"""Pure native codec checks: these tests never start the Seastar runtime."""
import gc
import asyncio
import json
import random
import struct
import sys
import weakref

import pytest

from oldman_server.native import _native as native


def compact(value):
    return json.dumps(value, separators=(",", ":")).encode("utf-8")


@pytest.mark.parametrize("value", [
    None, True, False, 0, -1, 2**63 - 1, -(2**63), 2**64, -(2**200),
    0.0, -0.0, 1.0, 1e-7, 1e20, 1e21, float("nan"), float("inf"),
    float("-inf"), "", "ascii\\\"/\b\f\n\r\t\x00\x1f\x7f", "café中文😀",
    "\ud800\udfff\ud800x\udc00\uffff\U00010000\U0010ffff", [], (), {}, [1, None, True, {"x": (1, 2)}],
    {"message": "Hello, World!", "n": 1},
    {None: "none", False: "false", True: "true", 1.5: "float", 2: "int"},
    {float("nan"): 1, float("inf"): 2, float("-inf"): 3, -0.0: 4},
    {"all_chars": "".join(chr(i) for i in range(1024))},
])
def test_encoder_matches_stdlib(value):
    assert native.encode_json(value) == compact(value)


def test_random_double_bit_patterns_match_stdlib():
    rng = random.Random(1909)
    values = [struct.unpack("!d", rng.randbytes(8))[0] for _ in range(2000)]
    assert native.encode_json(values) == compact(values)


def test_nested_random_values_match_stdlib():
    rng = random.Random(430)

    def value(depth):
        choices = [None, True, False, rng.randrange(-2**90, 2**90),
                   rng.uniform(-1e100, 1e100), "a😀\udcff\n" * rng.randrange(5)]
        if depth:
            choices.extend([[value(depth - 1) for _ in range(rng.randrange(5))],
                            {str(i): value(depth - 1) for i in range(rng.randrange(5))}])
        return rng.choice(choices)

    for _ in range(100):
        payload = value(4)
        assert native.encode_json(payload) == compact(payload)


def test_subclasses_preserve_stdlib_semantics():
    class Int(int):
        def __repr__(self):
            return "wrong"

    class Float(float):
        def __repr__(self):
            return "wrong"

    class Str(str):
        pass

    class List(list):
        def __iter__(self):
            return iter(["override"])

    class Dict(dict):
        def items(self):
            return [("override", 2)]

    for value in [Int(12), Float(1.25), Str("😀"), List([1]), Dict(x=1),
                  {"nested": [Int(12), Float(1.25), List([1]), Dict(x=1)]}]:
        assert native.encode_json(value) == compact(value)


@pytest.mark.parametrize("factory", [lambda: object(), lambda: {object(): 1},
                                      lambda: {"nested": b"bad"}, lambda: {1j: 2}])
def test_unsupported_matches_stdlib_error_type(factory):
    value = factory()
    with pytest.raises(TypeError):
        compact(value)
    with pytest.raises(TypeError):
        native.encode_json(value)


def test_cycles_rejected_and_shared_children_allowed():
    recursive_list = []
    recursive_list.append(recursive_list)
    recursive_dict = {}
    recursive_dict["self"] = recursive_dict
    for value in [recursive_list, recursive_dict]:
        with pytest.raises(ValueError, match="Circular"):
            native.encode_json(value)
    shared = [1, 2]
    assert native.encode_json([shared, shared]) == compact([shared, shared])


def test_recursion_limit_is_checked_without_poisoning_next_call():
    value = 0
    previous = 0
    # CPython 3.14 checks the available C stack, not sys.getrecursionlimit().
    # Establish a rejected input with the actual reference encoder. The native
    # encoder has different C frame sizes, so exact depth equality is not a
    # contract. Bound the search instead of allocating until memory is exhausted.
    for depth in (10000, 20000, 40000, 80000, 160000, 320000):
        for _ in range(depth - previous):
            value = [value]
        previous = depth
        try:
            compact(value)
        except RecursionError:
            break
    else:
        pytest.fail("stdlib encoder did not reject the bounded deep input")
    with pytest.raises(RecursionError):
        native.encode_json(value)
    assert native.encode_json({"healthy": True}) == compact({"healthy": True})


def test_output_byte_limit_includes_escaping_and_fallback():
    class Dict(dict):
        pass

    for value in [None, "😀", {"é": "\ud800"}, [1, 2], Dict(x="😀")]:
        expected = compact(value)
        assert native.encode_json(value, len(expected)) == expected
        with pytest.raises(ValueError, match="max_response_bytes"):
            native.encode_json(value, len(expected) - 1)


@pytest.mark.parametrize("kind", ["timeout", "cancel", "stop"])
def test_oversized_prefix_preserves_later_serializer_exception(kind):
    class Stop(BaseException):
        pass

    error_type = {"timeout": TimeoutError, "cancel": asyncio.CancelledError,
                  "stop": Stop}[kind]
    calls = []

    class Later(dict):
        def items(self):
            calls.append("later")
            raise error_type("later serializer")

    value = {"large": "x" * 64, "later": Later(x=1)}
    with pytest.raises(error_type, match="later serializer"):
        compact(value)
    assert calls == ["later"]
    calls.clear()
    with pytest.raises(error_type, match="later serializer"):
        native.encode_json(value, 32)
    assert calls == ["later"]
    calls.clear()

    batch = native.ApplicationResponseBatch(32)
    if kind == "stop":
        with pytest.raises(Stop, match="later serializer"):
            batch.append_result(1, 0, value)
    else:
        assert batch.append_result(1, 0, value) is True
    assert calls == ["later"]
    assert len(batch) == 1
    assert batch.stats() == dict(json_encoded=0, json_fallback=1, queued=1, flushed=0)
    assert batch.append_result(2, 0, {"healthy": True}) is True
    assert native.encode_json({"healthy": True}, 32) == b'{"healthy":true}'


@pytest.mark.parametrize("subclass_first", [False, True])
def test_oversized_json_runs_later_side_effect_once_then_rejects(subclass_first):
    calls = []

    class Later(dict):
        def items(self):
            calls.append("later")
            return super().items()

    # Exercise both OutputLimit and unsupported-subclass fallback entry points.
    value = ({"later": Later(x=1), "large": "x" * 64} if subclass_first else
             {"large": "x" * 64, "later": Later(x=1)})
    with pytest.raises(ValueError, match="max_response_bytes"):
        native.encode_json(value, 32)
    assert calls == ["later"]
    calls.clear()
    batch = native.ApplicationResponseBatch(32)
    assert batch.append_result(1, 0, value) is False
    assert calls == ["later"]
    assert len(batch) == 1
    assert batch.stats() == dict(json_encoded=0, json_fallback=1, queued=1, flushed=0)
    assert batch.append_result(2, 0, {"healthy": True}) is True
    assert native.encode_json({"healthy": True}, 32) == b'{"healthy":true}'


def test_integer_string_digit_limit_matches_stdlib():
    original = sys.get_int_max_str_digits()
    try:
        sys.set_int_max_str_digits(640)
        value = {"large": 10**700}
        with pytest.raises(ValueError):
            compact(value)
        with pytest.raises(ValueError):
            native.encode_json(value)
    finally:
        sys.set_int_max_str_digits(original)


def test_batch_owns_results_and_counts_success_failure_and_fallback():
    class Dict(dict):
        pass

    batch = native.ApplicationResponseBatch(128)
    assert len(batch) == 0
    assert batch.stats() == dict(json_encoded=0, json_fallback=0, queued=0, flushed=0)
    obj = Dict(x=[1, 2])
    ref = weakref.ref(obj)
    assert batch.append_result(1, 0, {"n": 1}) is True
    assert batch.append_result(2, 0, obj) is True
    del obj
    gc.collect()
    assert ref() is None
    for result in ["é", b"abc", (201, b"yes", (("X-Test", "café"),))]:
        assert batch.append_result(3, 0, result) is True
    assert batch.append_result(4, 0, object()) is False
    assert batch.append_result(5, 0, {"bad": object()}) is False
    assert len(batch) == 7
    assert batch.stats() == dict(json_encoded=2, json_fallback=2, queued=7, flushed=0)


@pytest.mark.parametrize("result", [
    None, [], (), (200, b"x"), (200, b"x", []), (True, b"", ()),
    (204, b"", ()), (304, b"", ()), (600, b"", ()), (199, b"", ()),
    (200, "text", ()), (200, b"", (("Content-Length", "1"),)),
    (200, b"", (("X-Test", "bad\r\nvalue"),)),
    (200, b"", (("X-Test", "😀"),)), "\ud800", b"x" * 33, "😀" * 9,
    {"large": "x" * 40},
])
def test_invalid_result_queues_exactly_one_fallback(result):
    batch = native.ApplicationResponseBatch(32)
    assert batch.append_result(10, 0, result) is False
    assert len(batch) == 1
    assert batch.append_result(11, 0, b"ok") is True
    assert len(batch) == 2


def test_serializer_baseexception_is_queued_then_propagated():
    class Stop(BaseException):
        pass

    class Dict(dict):
        def items(self):
            raise Stop("do not swallow")

    batch = native.ApplicationResponseBatch(128)
    with pytest.raises(Stop, match="do not swallow"):
        batch.append_result(1, 0, Dict(x=1))
    assert len(batch) == 1
    assert batch.stats() == dict(json_encoded=0, json_fallback=1, queued=1, flushed=0)
    assert batch.append_result(2, 0, {"ok": 1}) is True


def test_serializer_timeout_and_cancel_preserve_legacy_handled_status():
    class TimeoutDict(dict):
        def items(self):
            raise TimeoutError("serializer timed out")

    class CancelDict(dict):
        def items(self):
            raise asyncio.CancelledError("serializer cancelled")

    batch = native.ApplicationResponseBatch(128)
    assert batch.append_result(1, 0, TimeoutDict(x=1)) is True
    assert batch.append_result(2, 0, CancelDict(x=1)) is True
    assert len(batch) == 2
    assert batch.stats() == dict(json_encoded=0, json_fallback=2, queued=2, flushed=0)


@pytest.mark.parametrize("other_base", [Exception, TimeoutError])
def test_serializer_cancel_multiple_inheritance_remains_handled(other_base):
    class Both(asyncio.CancelledError, other_base):
        pass

    class Dict(dict):
        def items(self):
            raise Both("cancel takes precedence")

    batch = native.ApplicationResponseBatch(128)
    assert batch.append_result(1, 0, Dict(x=1)) is True
    assert len(batch) == 1
    assert batch.stats() == dict(json_encoded=0, json_fallback=1, queued=1, flushed=0)
    assert batch.append_result(2, 0, {"healthy": True}) is True


def test_string_and_bytes_subclasses_preserve_legacy_response_behavior():
    calls = []

    class Str(str):
        def encode(self, encoding):
            calls.append(encoding)
            return b"override"

    class Bytes(bytes):
        pass

    class BadStr(str):
        def encode(self, encoding):
            return Bytes(b"not exact")

    class LongStr(str):
        def encode(self, encoding):
            return b"x" * 33

    class Stop(BaseException):
        pass

    class StopStr(str):
        def encode(self, encoding):
            raise Stop("string encode")

    batch = native.ApplicationResponseBatch(32)
    assert batch.append_result(1, 0, Str("original")) is True
    assert calls == ["utf-8"]
    assert batch.append_result(2, 0, Bytes(b"invalid")) is False
    assert batch.append_result(3, 0, BadStr("invalid")) is False
    assert batch.append_result(4, 0, LongStr("invalid")) is False
    with pytest.raises(Stop, match="string encode"):
        batch.append_result(5, 0, StopStr("invalid"))
    assert len(batch) == 5


@pytest.mark.parametrize("request_id,shard", [(True, 0), (-1, 0), (1, True),
                                            (1, -1), (2**65, 0), (1, 2**40)])
def test_invalid_request_identity_never_queues(request_id, shard):
    batch = native.ApplicationResponseBatch(128)
    with pytest.raises((TypeError, ValueError, OverflowError)):
        batch.append_result(request_id, shard, b"ok")
    assert len(batch) == 0
