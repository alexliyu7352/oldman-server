#pragma once

#include <pybind11/pybind11.h>
#include <algorithm>
#include <charconv>
#include <cmath>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace probe::json_codec {
namespace py = pybind11;

// This encoder owns only native scratch data. All inspection/conversion occurs
// with the GIL held; no Python objects are retained after encode() returns.
class Encoder {
    struct NeedsFallback {};
    struct OutputLimit {};
    size_t limit_;
    std::string out_;
    std::vector<PyObject*> active_;

    void append(std::string_view value) {
        if (value.size() > limit_ - out_.size())
            throw OutputLimit{};
        out_.append(value);
    }

    void character(char value) { append(std::string_view(&value, 1)); }

    void hex_escape(Py_UCS4 value) {
        constexpr char hex[] = "0123456789abcdef";
        const char escaped[] = {'\\', 'u', hex[(value >> 12) & 15],
            hex[(value >> 8) & 15], hex[(value >> 4) & 15], hex[value & 15]};
        append(std::string_view(escaped, sizeof escaped));
    }

    void string(PyObject* value) {
        if (PyUnicode_READY(value) < 0) throw py::error_already_set();
        character('"');
        const auto length = PyUnicode_GET_LENGTH(value);
        const auto kind = PyUnicode_KIND(value);
        const auto* data = PyUnicode_DATA(value);
        for (Py_ssize_t i = 0; i < length; ++i) {
            const auto c = PyUnicode_READ(kind, data, i);
            switch (c) {
            case '"': append("\\\""); break;
            case '\\': append("\\\\"); break;
            case '\b': append("\\b"); break;
            case '\f': append("\\f"); break;
            case '\n': append("\\n"); break;
            case '\r': append("\\r"); break;
            case '\t': append("\\t"); break;
            default:
                if (c >= 0x20 && c <= 0x7e) {
                    // ASCII is the overwhelmingly common case. Copy a run from
                    // compact one-byte Unicode directly into the owned body.
                    if (kind == PyUnicode_1BYTE_KIND) {
                        auto end = i + 1;
                        while (end < length) {
                            const auto next = PyUnicode_READ(kind, data, end);
                            if (next < 0x20 || next > 0x7e || next == '"' || next == '\\') break;
                            ++end;
                        }
                        append(std::string_view(static_cast<const char*>(data) + i,
                                                static_cast<size_t>(end - i)));
                        i = end - 1;
                    } else {
                        character(static_cast<char>(c));
                    }
                } else if (c <= 0xffff) {
                    hex_escape(c); // Includes unpaired Unicode surrogates.
                } else {
                    const auto astral = c - 0x10000;
                    hex_escape(0xd800 | (astral >> 10));
                    hex_escape(0xdc00 | (astral & 0x3ff));
                }
            }
        }
        character('"');
    }

    void integer(PyObject* value) {
        int overflow = 0;
        const auto number = PyLong_AsLongLongAndOverflow(value, &overflow);
        if (PyErr_Occurred()) throw py::error_already_set();
        if (!overflow) {
            char buffer[32];
            auto result = std::to_chars(buffer, buffer + sizeof buffer, number);
            append(std::string_view(buffer, static_cast<size_t>(result.ptr - buffer)));
            return;
        }
        // CPython's exact-int formatter preserves its integer digit limit.
        auto* raw = PyObject_Str(value);
        if (!raw) throw py::error_already_set();
        auto digits = py::reinterpret_steal<py::str>(raw);
        Py_ssize_t size;
        const char* text = PyUnicode_AsUTF8AndSize(digits.ptr(), &size);
        if (!text) throw py::error_already_set();
        append(std::string_view(text, static_cast<size_t>(size)));
    }

    void floating(PyObject* value) {
        const auto number = PyFloat_AS_DOUBLE(value);
        if (!std::isfinite(number)) {
            append(std::isnan(number) ? "NaN" : (number < 0 ? "-Infinity" : "Infinity"));
            return;
        }
        // Use CPython's repr formatter, as json.dumps does. In particular this
        // preserves shortest rounding, exponent spelling and negative zero.
        char* text = PyOS_double_to_string(number, 'r', 0, Py_DTSF_ADD_DOT_0, nullptr);
        if (!text) {
            if (!PyErr_Occurred()) PyErr_NoMemory();
            throw py::error_already_set();
        }
        try { append(text); }
        catch (...) { PyMem_Free(text); throw; }
        PyMem_Free(text);
    }

    void key(PyObject* value) {
        if (PyUnicode_CheckExact(value)) { string(value); return; }
        character('"');
        if (value == Py_True) append("true");
        else if (value == Py_False) append("false");
        else if (value == Py_None) append("null");
        else if (PyLong_CheckExact(value)) integer(value);
        else if (PyFloat_CheckExact(value)) floating(value);
        else throw NeedsFallback{};
        character('"');
    }

    void container(PyObject* value) {
        if (std::find(active_.begin(), active_.end(), value) != active_.end())
            throw py::value_error("Circular reference detected");
        if (Py_EnterRecursiveCall(" while encoding a JSON object")) throw py::error_already_set();
        try {
            active_.push_back(value);
        } catch (...) {
            Py_LeaveRecursiveCall();
            throw;
        }
        try {
            if (PyDict_CheckExact(value)) {
                character('{');
                Py_ssize_t position = 0;
                PyObject *item_key, *item_value;
                bool first = true;
                while (PyDict_Next(value, &position, &item_key, &item_value)) {
                    if (!first) character(',');
                    first = false;
                    key(item_key);
                    character(':');
                    item(item_value);
                }
                character('}');
            } else {
                character('[');
                const bool list = PyList_CheckExact(value);
                const auto size = list ? PyList_GET_SIZE(value) : PyTuple_GET_SIZE(value);
                for (Py_ssize_t i = 0; i < size; ++i) {
                    if (i) character(',');
                    item(list ? PyList_GET_ITEM(value, i) : PyTuple_GET_ITEM(value, i));
                }
                character(']');
            }
        } catch (...) {
            active_.pop_back();
            Py_LeaveRecursiveCall();
            throw;
        }
        active_.pop_back();
        Py_LeaveRecursiveCall();
    }

    void item(PyObject* value) {
        if (PyUnicode_CheckExact(value)) string(value);
        else if (value == Py_None) append("null");
        else if (value == Py_True) append("true");
        else if (value == Py_False) append("false");
        else if (PyLong_CheckExact(value)) integer(value);
        else if (PyFloat_CheckExact(value)) floating(value);
        else if (PyDict_CheckExact(value) || PyList_CheckExact(value) || PyTuple_CheckExact(value))
            container(value);
        else throw NeedsFallback{};
    }

    void fallback(py::handle value, uint64_t* fallback_count) {
        if (fallback_count) ++*fallback_count;
        out_.clear();
        // Complete stdlib serialization before checking the limit. A later
        // subclass can have visible side effects or raise a control exception,
        // which the legacy application observes even after an oversized prefix.
        auto encoded = py::module_::import("json").attr("dumps")(
            value, py::arg("separators") = py::make_tuple(",", ":"));
        Py_ssize_t size;
        const char* text = PyUnicode_AsUTF8AndSize(encoded.ptr(), &size);
        if (!text) throw py::error_already_set();
        if (static_cast<size_t>(size) > limit_)
            throw py::value_error("encoded JSON exceeds max_response_bytes");
        // Do not call append here: its internal OutputLimit marker belongs only
        // to the native attempt and must never escape or trigger a second pass.
        out_.assign(text, static_cast<size_t>(size));
    }

public:
    explicit Encoder(size_t limit) : limit_(limit) {}

    std::string encode(py::handle value, uint64_t* fallback_count = nullptr) {
        try {
            item(value.ptr());
        } catch (const NeedsFallback&) {
            // Subclasses and unsupported values are delegated as a whole, so
            // stdlib subclass iteration, exceptions and circular checks apply.
            fallback(value, fallback_count);
        } catch (const OutputLimit&) {
            // Keep the common path single-pass. Only oversize values repeat
            // through stdlib to preserve later serialization behavior.
            fallback(value, fallback_count);
        }
        return std::move(out_);
    }
};

inline std::string encode(py::handle value, size_t max_response_bytes,
                          uint64_t* fallback_count = nullptr) {
    return Encoder(max_response_bytes).encode(value, fallback_count);
}
} // namespace probe::json_codec
