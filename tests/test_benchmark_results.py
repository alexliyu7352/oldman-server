import copy
import sys
from pathlib import Path
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import run_http_benchmarks as bench


def row():
    return {"key": "case", "mode": "hybrid", "size": 1024, "returncodes": [0,0],
        "client": {"errors":0,"requests":10,"new_connections":0,"total_connections":32},
        "server":{"delta":{"responses":10,"python_decisions":10,"body_bytes_to_python":0,"body_bytes_from_python":0}},
        "closed":{"closed":True,"stats":{"active_requests":0,"reserved_bytes":0,"pending_decisions":0,"queued_decisions":0}}}


def test_resumed_result_rejects_failure_inconsistency_and_unreleased_work():
    validate = getattr(bench, "validate_row", None)
    assert validate is not None, "cached rows have no integrity gate"
    good = row()
    validate(good, "case")
    variants = []
    bad=copy.deepcopy(good); bad["returncodes"]=[0,1]; variants.append(bad)
    bad=copy.deepcopy(good); bad["client"]["errors"]=1; variants.append(bad)
    bad=copy.deepcopy(good); bad["server"]["delta"]["responses"]=9; variants.append(bad)
    bad=copy.deepcopy(good); bad["closed"]["stats"]["active_requests"]=1; variants.append(bad)
    bad=copy.deepcopy(good); bad["key"]="stale_case"; variants.append(bad)
    for bad in variants:
        with pytest.raises((ValueError, AssertionError)):
            validate(bad, "case")


def test_resume_provenance_must_match_and_preserve_original_manifest():
    validate = getattr(bench, "validate_manifest", None)
    assert validate is not None, "resume has no provenance gate"
    old = {"protocol":2,"warmup_seconds":2,"measure_seconds":5,"repeats":3,"concurrency":32,
           "server_cpus":[0,1,2,3,4,5],"client_cpus":[6,7],"payloads":{"x":"abc"},
           "fingerprints":{"binary":"same"},"cases":27,"clock":"old"}
    new=copy.deepcopy(old); new["clock"]="new"
    validate(old,new)
    assert old["clock"] == "old"
    new["fingerprints"]["binary"]="changed"
    with pytest.raises((ValueError, AssertionError)):
        validate(old,new)
