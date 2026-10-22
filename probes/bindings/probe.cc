// Disposable synchronous fixture. Both bindings call this exact native core.
#include "server.hh"
#include <pybind11/pybind11.h>
#include <algorithm>
#include <limits>

namespace py = pybind11;
using probe::RequestDecision;
using probe::RequestEvent;

struct Fixture {
    uint64_t calls = 0, received = 0;
    std::vector<RequestDecision> last;
    std::vector<RequestEvent> drain_requests(size_t count) {
        std::vector<RequestEvent> events;
        for (size_t i = 0; i < std::min(count, size_t(6)); ++i) {
            events.push_back({(100000+i)*65536+i%2, unsigned(i%2), "GET", "fixed", 1024, false, {}});
        }
        return events;
    }
    void resolve_requests(std::vector<RequestDecision> decisions) {
        ++calls;
        received += decisions.size();
        last = std::move(decisions); // Own the converted data beyond the binding call.
    }
};

// CPython-owned references, including cleanup on allocation/validation failures.
struct Decref { void operator()(PyObject* p) const { Py_XDECREF(p); } };
using Owned = std::unique_ptr<PyObject, Decref>;
struct ReleasedGil {
    PyThreadState* state = PyEval_SaveThread();
    ~ReleasedGil() { PyEval_RestoreThread(state); }
};
static PyObject* cpp_error() {
    try { throw; }
    catch (const std::bad_alloc&) { PyErr_NoMemory(); }
    catch (const std::exception& e) { PyErr_SetString(PyExc_RuntimeError, e.what()); }
    catch (...) { PyErr_SetString(PyExc_RuntimeError, "unknown native exception"); }
    return nullptr;
}
struct CapiObject { PyObject_HEAD Fixture* fixture; };
static PyObject* capi_new(PyTypeObject* type, PyObject* args, PyObject* kwargs) {
    if (!PyArg_ParseTuple(args, ":CapiFixture")) return nullptr;
    if (kwargs && PyDict_Size(kwargs)) { PyErr_SetString(PyExc_TypeError, "no keywords accepted"); return nullptr; }
    auto* self = reinterpret_cast<CapiObject*>(type->tp_alloc(type, 0));
    if (!self) return nullptr;
    try { self->fixture = new Fixture; }
    catch (...) { Py_DECREF(self); return cpp_error(); }
    return reinterpret_cast<PyObject*>(self);
}
static void capi_dealloc(CapiObject* self) {
    auto* type = Py_TYPE(self);
    delete self->fixture;
    type->tp_free(reinterpret_cast<PyObject*>(self));
    Py_DECREF(type);
}
static PyObject* capi_drain(CapiObject* self, PyObject* count) {
    auto n = PyLong_AsSize_t(count);
    if (PyErr_Occurred()) return nullptr;
    try {
        auto events = self->fixture->drain_requests(n);
        Owned out(PyList_New(0)); // Same append strategy as the pybind11 path.
        if (!out) return nullptr;
        for (const auto& e : events) {
            Owned item(PyTuple_New(6));
            if (!item) return nullptr;
            PyObject* fields[] = {PyLong_FromUnsignedLongLong(e.id), PyLong_FromUnsignedLong(e.shard),
                PyUnicode_DecodeUTF8(e.method.data(), e.method.size(), nullptr),
                PyUnicode_DecodeUTF8(e.route.data(), e.route.size(), nullptr),
                PyLong_FromSize_t(e.body_size),
                e.copied ? PyBytes_FromStringAndSize(e.body.data(), e.body.size()) : Py_NewRef(Py_None)};
            bool failed = false;
            for (size_t i=0; i<6; ++i) {
                if (!fields[i]) failed = true;
                PyTuple_SET_ITEM(item.get(), i, fields[i]); // tuple owns each successful allocation
            }
            if (failed || PyList_Append(out.get(), item.get()) < 0) return nullptr;
        }
        return out.release();
    } catch (...) { return cpp_error(); }
}
static PyObject* capi_resolve(CapiObject* self, PyObject* items) {
    if (!PyList_Check(items)) { PyErr_SetString(PyExc_TypeError, "decisions must be a list"); return nullptr; }
    try {
        std::vector<RequestDecision> owned;
        owned.reserve(PyList_GET_SIZE(items));
        for (Py_ssize_t i=0; i<PyList_GET_SIZE(items); ++i) {
            auto* item = PyList_GET_ITEM(items, i);
            if (!PyTuple_Check(item) || PyTuple_GET_SIZE(item) != 4) {
                PyErr_SetString(PyExc_ValueError, "decision must contain id, shard, code, body"); return nullptr;
            }
            auto id = PyLong_AsUnsignedLongLong(PyTuple_GET_ITEM(item, 0));
            if (PyErr_Occurred()) return nullptr;
            auto shard = PyLong_AsUnsignedLong(PyTuple_GET_ITEM(item, 1));
            if (PyErr_Occurred()) return nullptr;
            auto code = PyLong_AsLong(PyTuple_GET_ITEM(item, 2));
            if (PyErr_Occurred()) return nullptr;
            if (shard > std::numeric_limits<unsigned>::max() || code < std::numeric_limits<int>::min() || code > std::numeric_limits<int>::max()) {
                PyErr_SetString(PyExc_OverflowError, "decision integer overflow"); return nullptr;
            }
            auto* body = PyTuple_GET_ITEM(item, 3);
            if (!PyBytes_Check(body)) { PyErr_SetString(PyExc_TypeError, "decision body must be bytes"); return nullptr; }
            owned.push_back({id, unsigned(shard), int(code),
                std::string(PyBytes_AS_STRING(body), PyBytes_GET_SIZE(body))});
        }
        { ReleasedGil release; self->fixture->resolve_requests(std::move(owned)); }
        Py_RETURN_NONE;
    } catch (...) { return cpp_error(); }
}
static py::tuple snapshot(Fixture& f) {
    py::list rows;
    for (const auto& d : f.last) rows.append(py::make_tuple(d.id, d.shard, d.code, py::bytes(d.body)));
    return py::make_tuple(f.calls, f.received, rows);
}
static PyObject* capi_snapshot(CapiObject* self, PyObject*) {
    try { return snapshot(*self->fixture).release().ptr(); }
    catch (py::error_already_set& e) { e.restore(); return nullptr; }
    catch (...) { return cpp_error(); }
}
static PyMethodDef methods[] = {
    {"drain_requests", reinterpret_cast<PyCFunction>(capi_drain), METH_O, nullptr},
    {"resolve_requests", reinterpret_cast<PyCFunction>(capi_resolve), METH_O, nullptr},
    {"snapshot", reinterpret_cast<PyCFunction>(capi_snapshot), METH_NOARGS, nullptr},
    {nullptr, nullptr, 0, nullptr}
};
static PyType_Slot slots[] = {
    {Py_tp_new, reinterpret_cast<void*>(capi_new)},
    {Py_tp_dealloc, reinterpret_cast<void*>(capi_dealloc)},
    {Py_tp_methods, methods}, {0, nullptr}
};
static PyType_Spec spec = {"_binding_probe.CapiFixture", sizeof(CapiObject), 0, Py_TPFLAGS_DEFAULT, slots};

PYBIND11_MODULE(_binding_probe, m) {
    py::class_<Fixture>(m, "PybindFixture")
        .def(py::init<>())
        // These two wrappers follow the production bridge's current operations.
        .def("drain_requests", [](Fixture& r, size_t count) {
            auto events = r.drain_requests(count);
            py::list out;
            for (const auto& e : events) {
                py::object body = e.copied ? py::object(py::bytes(e.body)) : py::object(py::none());
                out.append(py::make_tuple(e.id, e.shard, e.method, e.route, e.body_size, body));
            }
            return out;
        })
        .def("resolve_requests", [](Fixture& r, py::list items) {
            std::vector<RequestDecision> owned;
            owned.reserve(items.size());
            for (auto value : items) {
                auto item = py::cast<py::tuple>(value);
                if (item.size() != 4) throw py::value_error("decision must contain id, shard, code, body");
                if (!py::isinstance<py::bytes>(item[3])) throw py::type_error("decision body must be bytes");
                owned.push_back({py::cast<uint64_t>(item[0]), py::cast<unsigned>(item[1]),
                                 py::cast<int>(item[2]), py::cast<std::string>(item[3])});
            }
            py::gil_scoped_release release;
            r.resolve_requests(std::move(owned));
        })
        .def("snapshot", &snapshot);
    auto* type = PyType_FromSpec(&spec);
    if (!type) throw py::error_already_set();
    m.add_object("CapiFixture", py::reinterpret_steal<py::object>(type));
}
