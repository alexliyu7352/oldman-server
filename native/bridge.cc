#include "runtime.hh"
#include "multipart.hh"
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <limits>
#include <string_view>
namespace py = pybind11;

namespace {
py::str decode_latin1(std::string_view value) {
    auto* result = PyUnicode_DecodeLatin1(value.data(), static_cast<Py_ssize_t>(value.size()), nullptr);
    if (!result) throw py::error_already_set();
    return py::reinterpret_steal<py::str>(result);
}

struct ApplicationRequest {
    // These are ordinary owned allocations, never reactor objects or a RequestEvent
    // reservation. Python bytes/tuples are created only when a property is read.
    std::string method, target;
    std::vector<std::pair<std::string, std::string>> headers;
    std::string body;
    std::string http_version;
    std::pair<std::string, unsigned> client, server;
    uint64_t id = 0;     // the native request, for streamed responses
    unsigned shard = 0;
    bool more_body = false;  // the body is still arriving as application_data events
    uint64_t connection = 0; // (connection id << 16) | shard
    unsigned sequence = 0;   // 1-based position of the request on its connection
    std::string error;       // "<status>:<message>": a protocol error to render, no request
    bool unix_socket = false; // over a unix socket: no client address, server is the path

    ApplicationRequest() = default;
    explicit ApplicationRequest(probe::RequestEvent&& event)
        : method(std::move(event.method)), target(std::move(event.target)),
          headers(std::move(event.headers)), body(std::move(event.body)),
          http_version(std::move(event.http_version)),
          client(std::move(event.client)), server(std::move(event.server)),
          id(event.id), shard(event.shard), more_body(event.parameter == "more"),
          connection(event.connection), sequence(event.sequence), unix_socket(event.unix_socket) {
        if (event.parameter.starts_with("error:")) error = event.parameter.substr(6);
    }

    ApplicationRequest(py::handle method_value, py::handle target_value,
                       py::handle header_values, py::handle body_value) {
        if (!PyUnicode_CheckExact(method_value.ptr()))
            throw py::type_error("method must be str");
        if (!PyBytes_CheckExact(target_value.ptr()) || !PyBytes_CheckExact(body_value.ptr()))
            throw py::type_error("target and body must be bytes");
        if (!PyTuple_CheckExact(header_values.ptr()))
            throw py::type_error("headers must be a tuple of bytes pairs");
        const auto count = PyTuple_GET_SIZE(header_values.ptr());
        for (Py_ssize_t i = 0; i < count; ++i) {
            auto* pair = PyTuple_GET_ITEM(header_values.ptr(), i);
            if (!PyTuple_CheckExact(pair) || PyTuple_GET_SIZE(pair) != 2 ||
                !PyBytes_CheckExact(PyTuple_GET_ITEM(pair, 0)) ||
                !PyBytes_CheckExact(PyTuple_GET_ITEM(pair, 1)))
                throw py::type_error("headers must be a tuple of bytes pairs");
        }
        method = py::cast<std::string>(method_value);
        target.assign(PyBytes_AS_STRING(target_value.ptr()), PyBytes_GET_SIZE(target_value.ptr()));
        body.assign(PyBytes_AS_STRING(body_value.ptr()), PyBytes_GET_SIZE(body_value.ptr()));
        headers.reserve(static_cast<size_t>(count));
        for (Py_ssize_t i = 0; i < count; ++i) {
            auto* pair = PyTuple_GET_ITEM(header_values.ptr(), i);
            auto* name = PyTuple_GET_ITEM(pair, 0);
            auto* value = PyTuple_GET_ITEM(pair, 1);
            headers.emplace_back(std::string(PyBytes_AS_STRING(name), PyBytes_GET_SIZE(name)),
                                 std::string(PyBytes_AS_STRING(value), PyBytes_GET_SIZE(value)));
        }
    }

    py::tuple python_headers() const {
        py::tuple out(headers.size());
        for (size_t i = 0; i < headers.size(); ++i)
            out[i] = py::make_tuple(py::bytes(headers[i].first), py::bytes(headers[i].second));
        return out;
    }

    // Request.headers input: (name.decode("ascii").lower(), value.decode("utf-8",
    // "surrogateescape")) pairs, with the same exceptions, built in one pass. The
    // names are lowercase, as the asyncio server's parser makes them.
    py::list sanic_headers() const {
        py::list out(headers.size());
        std::string lower;
        for (size_t i = 0; i < headers.size(); ++i) {
            const auto& [name, value] = headers[i];
            lower.assign(name);
            for (auto& c : lower)
                if (c >= 'A' && c <= 'Z') c = static_cast<char>(c + ('a' - 'A'));
            auto key = py::reinterpret_steal<py::object>(PyUnicode_DecodeASCII(
                lower.data(), static_cast<Py_ssize_t>(lower.size()), nullptr));
            if (!key) throw py::error_already_set();
            auto text = py::reinterpret_steal<py::object>(PyUnicode_DecodeUTF8(
                value.data(), static_cast<Py_ssize_t>(value.size()), "surrogateescape"));
            if (!text) throw py::error_already_set();
            auto* pair = PyTuple_Pack(2, key.ptr(), text.ptr());
            if (!pair) throw py::error_already_set();
            PyList_SET_ITEM(out.ptr(), static_cast<Py_ssize_t>(i), pair);
        }
        return out;
    }

    py::str path() const {
        return decode_latin1(std::string_view(target).substr(0, target.find('?')));
    }

    py::str query_string() const {
        const auto separator = target.find('?');
        return decode_latin1(separator == std::string::npos
            ? std::string_view(target).substr(target.size())
            : std::string_view(target).substr(separator + 1));
    }

    // The values of the headers named `name` (ASCII case-insensitively), in
    // order; with `first`, only the first. A non-ASCII name raises
    // UnicodeEncodeError (no header can have it).
    std::vector<const std::string*> find_headers(py::handle name, bool first) const {
        if (!PyUnicode_Check(name.ptr())) throw py::type_error("header name must be str");
        auto* encoded = PyUnicode_AsASCIIString(name.ptr());
        if (!encoded) throw py::error_already_set();
        auto key = py::reinterpret_steal<py::bytes>(encoded);
        const std::string_view sought(PyBytes_AS_STRING(key.ptr()), PyBytes_GET_SIZE(key.ptr()));
        auto lower = [](unsigned char c) { return c >= 'A' && c <= 'Z' ? c + ('a' - 'A') : c; };
        std::vector<const std::string*> found;
        for (const auto& [header_name, value] : headers) {
            if (header_name.size() != sought.size()) continue;
            size_t i = 0;
            while (i < sought.size() && lower(header_name[i]) == lower(sought[i])) ++i;
            if (i == sought.size()) {
                found.push_back(&value);
                if (first) break;
            }
        }
        return found;
    }

    const std::string* find_header(py::handle name) const {
        const auto found = find_headers(name, true);
        return found.empty() ? nullptr : found.front();
    }

    // A header value as sanic_headers() decodes it.
    static py::object sanic_value(const std::string& value) {
        auto text = py::reinterpret_steal<py::object>(PyUnicode_DecodeUTF8(
            value.data(), static_cast<Py_ssize_t>(value.size()), "surrogateescape"));
        if (!text) throw py::error_already_set();
        return text;
    }

    // NativeHeaders lookups: the first value, or all values, of a header.
    py::object sanic_header(py::handle name, py::object fallback) const {
        const auto* value = find_header(name);
        return value ? sanic_value(*value) : fallback;
    }

    py::list sanic_header_all(py::handle name) const {
        py::list out;
        for (const auto* value : find_headers(name, false)) out.append(sanic_value(*value));
        return out;
    }

    py::object header(py::handle name, py::object fallback) const {
        const auto* value = find_header(name);
        return value ? py::object(decode_latin1(*value)) : fallback;
    }

    py::object header_bytes(py::handle name, py::object fallback) const {
        const auto* value = find_header(name);
        return value ? py::object(py::bytes(*value)) : fallback;
    }

    // validate_sanic_request() followed by the reads a native Sanic request
    // needs first, in one call: (target, http_version, method, body, more_body).
    py::tuple sanic_request() const {
        validate_sanic_request();
        return py::make_tuple(py::bytes(target), py::str(http_version), py::str(method),
                              py::bytes(body), py::bool_(more_body));
    }

    void validate_sanic_request() const {
        if (http_version.empty() || (client.first.empty() && !unix_socket) || server.first.empty())
            throw py::value_error("native request metadata is required");
        // Lazy Header creation must not postpone the adapter's ASCII-name
        // rejection until a handler happens to read the headers.
        for (const auto& [name, value] : headers) {
            for (unsigned char c : name) {
                if (c < 128) continue;
                auto* decoded = PyUnicode_DecodeASCII(name.data(), name.size(), nullptr);
                if (!decoded) throw py::error_already_set();
                Py_DECREF(decoded);
            }
        }
    }
};

// ApplicationRequest as Python sees it: a plain C type rather than a pybind11
// class. One is made for every request, and read a few times: without
// pybind11's type lookups, instance registry and call dispatcher.
struct RequestObject {
    PyObject_HEAD
    ApplicationRequest value;
};

PyTypeObject* request_type = nullptr;

const ApplicationRequest& request_of(PyObject* self) {
    return reinterpret_cast<RequestObject*>(self)->value;
}

// A Python result of `body`, or nullptr with its exception set.
template <class F>
PyObject* guarded(F&& body) {
    try {
        return body().release().ptr();
    } catch (py::error_already_set& error) {
        error.restore();
    } catch (const py::builtin_exception& error) {
        error.set_error();
    } catch (const std::bad_alloc&) {
        PyErr_NoMemory();
    } catch (const std::exception& error) {
        PyErr_SetString(PyExc_RuntimeError, error.what());
    }
    return nullptr;
}

template <class... Args>
py::object make_request(Args&&... args) {
    auto* self = PyObject_New(RequestObject, request_type);
    if (!self) throw py::error_already_set();
    try {
        new (&self->value) ApplicationRequest(std::forward<Args>(args)...);
    } catch (...) {
        new (&self->value) ApplicationRequest();  // for the deallocation
        Py_DECREF(self);
        throw;
    }
    return py::reinterpret_steal<py::object>(reinterpret_cast<PyObject*>(self));
}

void request_dealloc(PyObject* self) {
    auto* type = Py_TYPE(self);
    reinterpret_cast<RequestObject*>(self)->value.~ApplicationRequest();
    type->tp_free(self);
    Py_DECREF(type);
}

// ApplicationRequest(method, target, headers, body, *, http_version="",
// client=None, server=None). Socket metadata is optional so tests and
// in-process measurement can build complete Sanic requests without a
// listening runtime.
PyObject* request_new(PyTypeObject*, PyObject* args, PyObject* kwargs) {
    static const char* names[] = {"method", "target", "headers", "body", "http_version", "client", "server",
                                  nullptr};
    PyObject *method, *target, *headers, *body, *version = nullptr, *client = Py_None, *server = Py_None;
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "OOOO|$OOO:ApplicationRequest", const_cast<char**>(names),
                                     &method, &target, &headers, &body, &version, &client, &server))
        return nullptr;
    return guarded([&] {
        ApplicationRequest value(method, target, headers, body);
        try {
            if (version) value.http_version = py::cast<std::string>(version);
            if (client != Py_None) value.client = py::cast<std::pair<std::string, unsigned>>(client);
            if (server != Py_None) value.server = py::cast<std::pair<std::string, unsigned>>(server);
        } catch (const py::cast_error&) {
            throw py::type_error("http_version must be str; client and server (host, port) pairs");
        }
        return make_request(std::move(value));
    });
}

// A read-only attribute: Read(request) as a Python object.
template <auto Read>
PyObject* request_get(PyObject* self, void*) {
    return guarded([self] { return py::object(Read(request_of(self))); });
}

// A method without arguments.
template <auto Call>
PyObject* request_call(PyObject* self, PyObject*) {
    return guarded([self] { return py::object(Call(request_of(self))); });
}

// The header lookups' (name, default=None) arguments, by position or keyword.
bool lookup_arguments(PyObject* const* args, Py_ssize_t count, PyObject* keywords, bool with_default,
                      PyObject*& name, PyObject*& fallback) {
    name = nullptr;
    fallback = Py_None;
    if (count > (with_default ? 2 : 1)) {
        PyErr_SetString(PyExc_TypeError, "too many arguments");
        return false;
    }
    if (count > 0) name = args[0];
    if (count > 1) fallback = args[1];
    const auto named = keywords ? PyTuple_GET_SIZE(keywords) : 0;
    for (Py_ssize_t i = 0; i < named; ++i) {
        auto* key = PyTuple_GET_ITEM(keywords, i);
        if (!name && PyUnicode_CompareWithASCIIString(key, "name") == 0) {
            name = args[count + i];
        } else if (with_default && count < 2 && PyUnicode_CompareWithASCIIString(key, "default") == 0) {
            fallback = args[count + i];
        } else {
            PyErr_Format(PyExc_TypeError, "unexpected argument %R", key);
            return false;
        }
    }
    if (!name) {
        PyErr_SetString(PyExc_TypeError, "missing the header name");
        return false;
    }
    return true;
}

template <auto Lookup>
PyObject* request_lookup(PyObject* self, PyObject* const* args, Py_ssize_t count, PyObject* keywords) {
    PyObject *name, *fallback;
    if (!lookup_arguments(args, count, keywords, true, name, fallback)) return nullptr;
    return guarded([&] {
        return py::object(Lookup(request_of(self), name, py::reinterpret_borrow<py::object>(fallback)));
    });
}

PyObject* request_header_all(PyObject* self, PyObject* const* args, Py_ssize_t count, PyObject* keywords) {
    PyObject *name, *fallback;
    if (!lookup_arguments(args, count, keywords, false, name, fallback)) return nullptr;
    return guarded([&] { return request_of(self).sanic_header_all(name); });
}

PyObject* text_or_none(const std::string& value) {
    return guarded([&] { return value.empty() ? py::object(py::none()) : py::object(py::str(value)); });
}

PyObject* address_or_none(const std::pair<std::string, unsigned>& value) {
    return guarded([&] { return value.first.empty() ? py::object(py::none()) : py::cast(value); });
}

using Request = ApplicationRequest;

PyGetSetDef request_attributes[] = {
    {"method", request_get<[](const Request& r) { return py::str(r.method); }>, nullptr, nullptr, nullptr},
    {"target", request_get<[](const Request& r) { return py::bytes(r.target); }>, nullptr, nullptr, nullptr},
    {"headers", request_get<[](const Request& r) { return r.python_headers(); }>, nullptr, nullptr, nullptr},
    {"request_id", request_get<[](const Request& r) { return py::int_(r.id); }>, nullptr, nullptr, nullptr},
    {"shard", request_get<[](const Request& r) { return py::int_(r.shard); }>, nullptr, nullptr, nullptr},
    {"more_body", request_get<[](const Request& r) { return py::bool_(r.more_body); }>, nullptr, nullptr,
     nullptr},
    {"connection", request_get<[](const Request& r) { return py::int_(r.connection); }>, nullptr, nullptr,
     nullptr},
    {"sequence", request_get<[](const Request& r) { return py::int_(r.sequence); }>, nullptr, nullptr, nullptr},
    {"unix_socket", request_get<[](const Request& r) { return py::bool_(r.unix_socket); }>, nullptr, nullptr,
     nullptr},
    {"error", [](PyObject* self, void*) { return text_or_none(request_of(self).error); }, nullptr, nullptr,
     nullptr},
    {"body", request_get<[](const Request& r) { return py::bytes(r.body); }>, nullptr, nullptr, nullptr},
    {"http_version", request_get<[](const Request& r) { return py::str(r.http_version); }>, nullptr, nullptr,
     nullptr},
    {"client", [](PyObject* self, void*) { return address_or_none(request_of(self).client); }, nullptr,
     nullptr, nullptr},
    {"server", [](PyObject* self, void*) { return address_or_none(request_of(self).server); }, nullptr,
     nullptr, nullptr},
    {"path", request_get<[](const Request& r) { return r.path(); }>, nullptr, nullptr, nullptr},
    {"query_string", request_get<[](const Request& r) { return r.query_string(); }>, nullptr, nullptr,
     nullptr},
    {nullptr, nullptr, nullptr, nullptr, nullptr},
};

constexpr int lookup_flags = METH_FASTCALL | METH_KEYWORDS;

// The framework host's classes (oldman_server.native.host), for sanic_prepare
// and sanic_finish. set_sanic_types() takes them once; their slots are read
// and filled by offset.
struct SanicTypes {
    PyTypeObject* request = nullptr;  // Request
    PyTypeObject* exchange = nullptr;  // _BufferedExchange
    PyObject* unset = nullptr;  // _NATIVE_UNSET
    PyObject* stage_handler = nullptr;  // Stage.HANDLER
    PyObject* parsed_urls = nullptr;  // _PARSED_URLS: target bytes -> parse_url()
    PyObject* current = nullptr;  // Request._current
    PyObject* empty = nullptr;
    PyObject* minus_one = nullptr;
    PyObject *zero = nullptr, *one = nullptr, *two = nullptr;
    PyTypeObject* http_response = nullptr;  // HTTPResponse
    PyTypeObject* json_response = nullptr;  // JSONResponse (its body is a property)
    PyObject* stage_idle = nullptr;  // Stage.IDLE
    PyObject* sent_types = nullptr;  // _SENT_CONTENT_TYPES: content type -> _sent_content_type()
    PyObject* sent_content_type = nullptr;  // _sent_content_type
    struct {
        Py_ssize_t body, content_type, status, headers, stream;
    } h;
    struct {
        Py_ssize_t parsed_url, raw_url, id, name, app, native_request, headers, transport, conn_info,
            version, method, head, body, ctx, parsed_accept, parsed_args, parsed_not_grouped_args,
            parsed_cookies, parsed_credentials, parsed_files, parsed_form, parsed_forwarded, parsed_json,
            parsed_token, request_middleware_started, response_middleware_started, responded, route, stream,
            protocol, match_info;
    } r;
    struct {
        Py_ssize_t stage, response, request_body, body, max_body, result, failure, native, streaming,
            more_body, request_max_size, request_bytes, response_bytes_left;
    } x;
} sanic;

Py_ssize_t slot_offset(PyTypeObject* type, const char* name) {
    auto descriptor = py::reinterpret_steal<py::object>(
        PyObject_GetAttrString(reinterpret_cast<PyObject*>(type), name));
    if (!descriptor) throw py::error_already_set();
    if (!Py_IS_TYPE(descriptor.ptr(), &PyMemberDescr_Type))
        throw py::type_error(std::string(type->tp_name) + "." + name + " is not a slot");
    const auto* member = reinterpret_cast<PyMemberDescrObject*>(descriptor.ptr())->d_member;
    if (member->type != Py_T_OBJECT_EX) throw py::type_error(std::string(name) + " is not an object slot");
    return member->offset;
}

void set_sanic_types(py::handle request, py::handle exchange, py::handle unset, py::handle stage_handler,
                     py::handle parsed_urls, py::handle current, py::handle http_response,
                     py::handle json_response, py::handle stage_idle, py::handle sent_types,
                     py::handle sent_content_type) {
    if (!PyType_Check(request.ptr()) || !PyType_Check(exchange.ptr()) || !PyDict_CheckExact(parsed_urls.ptr())
            || !PyContextVar_CheckExact(current.ptr()) || !PyType_Check(http_response.ptr())
            || !PyType_Check(json_response.ptr()) || !PyDict_CheckExact(sent_types.ptr())
            || !PyCallable_Check(sent_content_type.ptr()))
        throw py::type_error("set_sanic_types(Request, _BufferedExchange, unset, Stage.HANDLER, dict, ContextVar, "
                             "HTTPResponse, JSONResponse, Stage.IDLE, dict, _sent_content_type)");
    auto* r = reinterpret_cast<PyTypeObject*>(request.ptr());
    auto* x = reinterpret_cast<PyTypeObject*>(exchange.ptr());
    auto* h = reinterpret_cast<PyTypeObject*>(http_response.ptr());
    sanic.h = {slot_offset(h, "body"), slot_offset(h, "content_type"), slot_offset(h, "status"),
               slot_offset(h, "_headers"), slot_offset(h, "stream")};
    sanic.r = {slot_offset(r, "_parsed_url"), slot_offset(r, "raw_url"), slot_offset(r, "_id"),
               slot_offset(r, "_name"), slot_offset(r, "app"), slot_offset(r, "_native_request"),
               slot_offset(r, "_headers"), slot_offset(r, "_transport"), slot_offset(r, "_conn_info"),
               slot_offset(r, "version"), slot_offset(r, "method"), slot_offset(r, "_head"),
               slot_offset(r, "body"), slot_offset(r, "_ctx"), slot_offset(r, "parsed_accept"),
               slot_offset(r, "_parsed_args"), slot_offset(r, "_parsed_not_grouped_args"),
               slot_offset(r, "parsed_cookies"), slot_offset(r, "parsed_credentials"),
               slot_offset(r, "parsed_files"), slot_offset(r, "parsed_form"), slot_offset(r, "parsed_forwarded"),
               slot_offset(r, "parsed_json"), slot_offset(r, "parsed_token"),
               slot_offset(r, "_request_middleware_started"), slot_offset(r, "_response_middleware_started"),
               slot_offset(r, "responded"), slot_offset(r, "route"), slot_offset(r, "stream"),
               slot_offset(r, "_protocol"), slot_offset(r, "_match_info")};
    sanic.x = {slot_offset(x, "stage"), slot_offset(x, "response"), slot_offset(x, "request_body"),
               slot_offset(x, "body"), slot_offset(x, "max_body"), slot_offset(x, "result"),
               slot_offset(x, "failure"), slot_offset(x, "native"), slot_offset(x, "streaming"),
               slot_offset(x, "more_body"), slot_offset(x, "request_max_size"), slot_offset(x, "request_bytes"),
               slot_offset(x, "response_bytes_left")};
    // Kept for the process's life, as the request type is.
    auto keep = [](py::handle value) { return Py_NewRef(value.ptr()); };
    sanic.request = reinterpret_cast<PyTypeObject*>(keep(request));
    sanic.exchange = reinterpret_cast<PyTypeObject*>(keep(exchange));
    sanic.unset = keep(unset);
    sanic.stage_handler = keep(stage_handler);
    sanic.parsed_urls = keep(parsed_urls);
    sanic.current = keep(current);
    sanic.empty = py::bytes().release().ptr();
    sanic.minus_one = py::int_(-1).release().ptr();
    sanic.zero = py::int_(0).release().ptr();
    sanic.one = py::int_(1).release().ptr();
    sanic.two = py::int_(2).release().ptr();
    sanic.http_response = reinterpret_cast<PyTypeObject*>(keep(http_response));
    sanic.json_response = reinterpret_cast<PyTypeObject*>(keep(json_response));
    sanic.stage_idle = keep(stage_idle);
    sanic.sent_types = keep(sent_types);
    sanic.sent_content_type = keep(sent_content_type);
}

inline PyObject* slot(PyObject* object, Py_ssize_t offset) {
    return *reinterpret_cast<PyObject**>(reinterpret_cast<char*>(object) + offset);
}

// A slot that may hold a value already.
inline void replace(PyObject* object, Py_ssize_t offset, PyObject* value) {
    Py_XSETREF(*reinterpret_cast<PyObject**>(reinterpret_cast<char*>(object) + offset), Py_NewRef(value));
}

// A slot of an object just allocated (NULL until filled).
inline void fill(PyObject* object, Py_ssize_t offset, PyObject* value) {
    *reinterpret_cast<PyObject**>(reinterpret_cast<char*>(object) + offset) = Py_NewRef(value);
}

// request.sanic_prepare(app, max_body, request_max_size): the Request and
// _BufferedExchange the framework host makes for this request, with the
// same slots Request._from_native_fields and _BufferedExchange.__init__
// assign (request.stream is the exchange), and the token of setting
// Request._current to the Request. None, with nothing made or set, when the
// target's parse is not cached or the target is too long: the Python path
// parses it, or raises.
PyObject* request_sanic_prepare(PyObject* self, PyObject* const* args, Py_ssize_t nargs) {
    if (nargs != 3) {
        PyErr_SetString(PyExc_TypeError, "sanic_prepare(app, max_body, request_max_size)");
        return nullptr;
    }
    if (!sanic.request) {
        PyErr_SetString(PyExc_RuntimeError, "set_sanic_types() was not called");
        return nullptr;
    }
    return guarded([&]() -> py::object {
        const Request& r = request_of(self);
        r.validate_sanic_request();
        if (r.target.size() > 65535) return py::none();
        py::bytes url(r.target);
        PyObject* parsed = PyDict_GetItemWithError(sanic.parsed_urls, url.ptr());
        if (!parsed) {
            if (PyErr_Occurred()) throw py::error_already_set();
            return py::none();
        }
        py::str version(r.http_version), method(r.method);
        py::bytes body(r.body);
        auto match_info = py::reinterpret_steal<py::object>(PyDict_New());
        auto request = py::reinterpret_steal<py::object>(sanic.request->tp_alloc(sanic.request, 0));
        auto exchange = py::reinterpret_steal<py::object>(sanic.exchange->tp_alloc(sanic.exchange, 0));
        if (!match_info || !request || !exchange) throw py::error_already_set();
        PyObject* q = request.ptr();
        PyObject* x = exchange.ptr();
        const auto& o = sanic.r;
        fill(q, o.parsed_url, parsed);
        fill(q, o.raw_url, url.ptr());
        fill(q, o.id, Py_None);
        fill(q, o.name, Py_None);
        fill(q, o.app, args[0]);
        fill(q, o.native_request, self);
        fill(q, o.headers, sanic.unset);
        fill(q, o.transport, sanic.unset);
        fill(q, o.conn_info, sanic.unset);
        fill(q, o.version, version.ptr());
        fill(q, o.method, method.ptr());
        fill(q, o.head, sanic.unset);
        fill(q, o.body, sanic.empty);
        fill(q, o.ctx, Py_None);
        fill(q, o.parsed_accept, Py_None);
        fill(q, o.parsed_args, sanic.unset);
        fill(q, o.parsed_not_grouped_args, sanic.unset);
        fill(q, o.parsed_cookies, Py_None);
        fill(q, o.parsed_credentials, Py_None);
        fill(q, o.parsed_files, Py_None);
        fill(q, o.parsed_form, Py_None);
        fill(q, o.parsed_forwarded, Py_None);
        fill(q, o.parsed_json, Py_None);
        fill(q, o.parsed_token, Py_None);
        fill(q, o.request_middleware_started, Py_False);
        fill(q, o.response_middleware_started, Py_False);
        fill(q, o.responded, Py_False);
        fill(q, o.route, Py_None);
        fill(q, o.stream, x);
        fill(q, o.protocol, Py_None);
        fill(q, o.match_info, match_info.ptr());
        const auto& e = sanic.x;
        PyObject* more_body = r.more_body ? Py_True : Py_False;
        fill(x, e.stage, sanic.stage_handler);
        fill(x, e.response, Py_None);
        fill(x, e.request_body, r.body.empty() ? more_body : Py_True);  // bool(body) or more_body
        fill(x, e.body, body.ptr());
        fill(x, e.max_body, args[1]);
        fill(x, e.result, Py_None);
        fill(x, e.failure, Py_None);
        fill(x, e.native, self);
        fill(x, e.streaming, Py_None);
        fill(x, e.more_body, more_body);
        fill(x, e.request_max_size, args[2]);
        fill(x, e.request_bytes, sanic.minus_one);
        fill(x, e.response_bytes_left, Py_None);
        auto prepared = py::reinterpret_steal<py::object>(PyTuple_New(3));
        if (!prepared) throw py::error_already_set();
        PyObject* token = PyContextVar_Set(sanic.current, q);  // last: nothing can fail after it
        if (!token) throw py::error_already_set();
        PyTuple_SET_ITEM(prepared.ptr(), 0, request.release().ptr());
        PyTuple_SET_ITEM(prepared.ptr(), 1, exchange.release().ptr());
        PyTuple_SET_ITEM(prepared.ptr(), 2, token);
        return prepared;
    });
}

// stats[key] += 1 for one of an Engine's counters; -1 with an exception set.
int count(PyObject* stats, PyObject* key) {
    PyObject* value = PyDict_GetItemWithError(stats, key);
    if (!value) {
        if (!PyErr_Occurred()) PyErr_SetObject(PyExc_KeyError, key);
        return -1;
    }
    static PyObject* const one = PyLong_FromLong(1);  // a small int: immortal
    PyObject* next = PyNumber_Add(value, one);
    if (!next) return -1;
    const int stored = PyDict_SetItem(stats, key, next);
    Py_DECREF(next);
    return stored;
}

// The serving Engine's request registries and answer queue, while it serves
// (set_application_state): sanic_finish settles a request there as
// Engine._application_finished does.
struct ApplicationState {
    PyObject *engine = nullptr, *tasks = nullptr, *inbound = nullptr, *timeouts = nullptr, *lost = nullptr,
             *terminal = nullptr, *returns = nullptr, *stats = nullptr, *call_soon = nullptr;
    PyObject *scheduled = nullptr, *flush = nullptr, *finished = nullptr;  // attribute and stats names
} application;

void clear_application_state(py::handle engine) {
    if (application.engine != engine.ptr()) return;
    for (auto* field : {&application.engine, &application.tasks, &application.inbound, &application.timeouts,
                        &application.lost, &application.terminal, &application.returns, &application.stats,
                        &application.call_soon})
        Py_CLEAR(*field);
}

void set_application_state(py::handle engine, py::handle tasks, py::handle inbound, py::handle timeouts,
                           py::handle lost, py::handle terminal, py::handle returns, py::handle stats,
                           py::handle call_soon) {
    if (!PyDict_CheckExact(tasks.ptr()) || !PyDict_CheckExact(inbound.ptr()) || !PySet_CheckExact(timeouts.ptr())
            || !PySet_CheckExact(lost.ptr()) || !PySet_CheckExact(terminal.ptr())
            || !PyList_CheckExact(returns.ptr()) || !PyDict_CheckExact(stats.ptr())
            || !PyCallable_Check(call_soon.ptr()))
        throw py::type_error("set_application_state(engine, dict, dict, set, set, set, list, dict, call_soon)");
    if (!application.scheduled) {
        application.scheduled = PyUnicode_InternFromString("_application_flush_scheduled");
        application.flush = PyUnicode_InternFromString("_flush_application");
        application.finished = PyUnicode_InternFromString("python_application_finished");
        if (!application.scheduled || !application.flush || !application.finished)
            throw py::error_already_set();
    }
    auto set = [](PyObject*& field, py::handle value) { Py_XSETREF(field, Py_NewRef(value.ptr())); };
    set(application.engine, engine);
    set(application.tasks, tasks);
    set(application.inbound, inbound);
    set(application.timeouts, timeouts);
    set(application.lost, lost);
    set(application.terminal, terminal);
    set(application.returns, returns);
    set(application.stats, stats);
    set(application.call_soon, call_soon);
}

// Engine._application_finished(id, shard, (status, body, headers)) for the
// Engine in `application`; -1 with an exception set on failure.
int settle(PyObject* id, PyObject* shard, PyObject* status, PyObject* body, PyObject* headers) {
    auto discard_key = [&](PyObject* dict) {
        if (!PyDict_GET_SIZE(dict)) return 0;
        if (PyDict_GetItemWithError(dict, id)) return PyDict_DelItem(dict, id);
        return PyErr_Occurred() ? -1 : 0;
    };
    auto discard = [&](PyObject* set) { return PySet_GET_SIZE(set) ? PySet_Discard(set, id) : 0; };
    if (discard_key(application.tasks) < 0 || discard_key(application.inbound) < 0
            || discard(application.timeouts) < 0 || discard(application.lost) < 0)
        return -1;
    const int answered = discard(application.terminal);  // already answered natively
    if (answered < 0) return -1;
    if (!answered) {
        PyObject* item = PyTuple_Pack(5, id, shard, status, body, headers);
        if (!item) return -1;
        const int appended = PyList_Append(application.returns, item);
        Py_DECREF(item);
        if (appended < 0) return -1;
        PyObject* scheduled = PyObject_GetAttr(application.engine, application.scheduled);
        if (!scheduled) return -1;
        const int truth = PyObject_IsTrue(scheduled);
        Py_DECREF(scheduled);
        if (truth < 0) return -1;
        if (!truth) {  // the Engine's flush as it is now (once per batch)
            if (PyObject_SetAttr(application.engine, application.scheduled, Py_True) < 0) return -1;
            PyObject* flush = PyObject_GetAttr(application.engine, application.flush);
            if (!flush) return -1;
            PyObject* handle = PyObject_CallOneArg(application.call_soon, flush);
            Py_DECREF(flush);
            if (!handle) return -1;
            Py_DECREF(handle);
        }
    }
    return count(application.stats, application.finished);
}

// request.sanic_finish(request, exchange, response, engine=None): the flat path's
// Request.respond(response) followed by send(end_stream=True), for the case
// _BufferedExchange.complete takes at once (a plain response, its headers
// never read, a bytes body within max_body, a status with a body): the
// state both leave, and 1; with the Engine set_application_state named, the
// request also settled there (its answer queued) and Request._current reset
// with `token`, and 2. 0, with nothing changed, for anything else (the Python
// path then runs, and raises where it raises).
PyObject* request_sanic_finish(PyObject* self, PyObject* const* args, Py_ssize_t nargs) {
    if (nargs < 3 || nargs > 5) {
        PyErr_SetString(PyExc_TypeError, "sanic_finish(request, exchange, response, engine=None, token=None)");
        return nullptr;
    }
    if (!sanic.http_response) {
        PyErr_SetString(PyExc_RuntimeError, "set_sanic_types() was not called");
        return nullptr;
    }
    PyObject* q = args[0];
    PyObject* x = args[1];
    PyObject* response = args[2];
    const auto& o = sanic.r;
    const auto& e = sanic.x;
    PyObject* const declined = sanic.zero;
    // Request.respond's checks and _BufferedExchange.complete's.
    if (Py_TYPE(q) != sanic.request || Py_TYPE(x) != sanic.exchange || slot(q, o.responded) != Py_False
            || slot(q, o.stream) != x || slot(x, e.response) != Py_None || slot(x, e.stage) != sanic.stage_handler
            || slot(x, e.streaming) != Py_None || slot(x, e.result) != Py_None || slot(x, e.failure) != Py_None)
        return Py_NewRef(declined);
    const bool plain = Py_TYPE(response) == sanic.http_response;
    if (!plain && Py_TYPE(response) != sanic.json_response) return Py_NewRef(declined);
    py::object body, status, content_type, headers;
    if (plain) {
        PyObject* values[] = {slot(response, sanic.h.body), slot(response, sanic.h.status),
                              slot(response, sanic.h.content_type), slot(response, sanic.h.headers)};
        for (auto* value : values)
            if (!value) return Py_NewRef(declined);  // unassigned: the Python path raises
        body = py::reinterpret_borrow<py::object>(values[0]);
        status = py::reinterpret_borrow<py::object>(values[1]);
        content_type = py::reinterpret_borrow<py::object>(values[2]);
        headers = py::reinterpret_borrow<py::object>(values[3]);
    } else {
        auto read = [&](const char* name) {
            return py::reinterpret_steal<py::object>(PyObject_GetAttrString(response, name));
        };
        body = read("body");
        status = body ? read("status") : py::object();
        content_type = status ? read("content_type") : py::object();
        headers = content_type ? read("_headers") : py::object();
        if (!headers) {
            PyErr_Clear();  // raised again where the Python path reads it
            return Py_NewRef(declined);
        }
    }
    if (!headers.is_none() || !PyBytes_CheckExact(body.ptr()) || !PyLong_CheckExact(status.ptr())
            || content_type.is_none())
        return Py_NewRef(declined);
    PyObject* max_body = slot(x, e.max_body);
    if (!PyLong_CheckExact(max_body)) return Py_NewRef(declined);
    int overflow = 0;
    const long long limit = PyLong_AsLongLongAndOverflow(max_body, &overflow);
    if (overflow < 0 || (overflow == 0 && PyBytes_GET_SIZE(body.ptr()) > limit)) return Py_NewRef(declined);
    const long code = PyLong_AsLongAndOverflow(status.ptr(), &overflow);
    if (overflow || code < 200 || code == 204 || code == 304) return Py_NewRef(declined);
    // _sent_content_type(response.content_type): (the response's headers, the sent pair).
    py::object entry;
    if (PyUnicode_CheckExact(content_type.ptr())) {
        PyObject* cached = PyDict_GetItemWithError(sanic.sent_types, content_type.ptr());
        if (!cached && PyErr_Occurred()) return nullptr;
        if (cached) entry = py::reinterpret_borrow<py::object>(cached);
    }
    if (!entry) {
        entry = py::reinterpret_steal<py::object>(PyObject_CallOneArg(sanic.sent_content_type, content_type.ptr()));
        if (!entry) {
            PyErr_Clear();  // raised again by the Python path
            return Py_NewRef(declined);
        }
    }
    if (!PyTuple_CheckExact(entry.ptr()) || PyTuple_GET_SIZE(entry.ptr()) != 2) {
        PyErr_SetString(PyExc_TypeError, "_sent_content_type must return a pair");
        return nullptr;
    }
    PyObject* result = PyTuple_Pack(3, status.ptr(), body.ptr(), PyTuple_GET_ITEM(entry.ptr(), 1));
    if (!result) return nullptr;
    PyObject* sent = PyTuple_GET_ITEM(entry.ptr(), 0);
    if (!plain && (PyObject_SetAttrString(response, "_headers", sent) < 0
                   || PyObject_SetAttrString(response, "stream", x) < 0)) {
        Py_DECREF(result);
        return nullptr;
    }
    if (plain) {
        replace(response, sanic.h.headers, sent);
        replace(response, sanic.h.stream, x);
    }
    replace(x, e.response, response);
    replace(q, o.responded, Py_True);
    Py_SETREF(*reinterpret_cast<PyObject**>(reinterpret_cast<char*>(x) + e.result), result);
    replace(x, e.stage, sanic.stage_idle);
    if (nargs < 4 || args[3] == Py_None || args[3] != application.engine) return Py_NewRef(sanic.one);
    const Request& r = request_of(self);
    auto id = py::reinterpret_steal<py::object>(PyLong_FromUnsignedLongLong(r.id));
    auto shard = py::reinterpret_steal<py::object>(PyLong_FromUnsignedLong(r.shard));
    if (!id || !shard
            || settle(id.ptr(), shard.ptr(), status.ptr(), body.ptr(), PyTuple_GET_ITEM(result, 2)) < 0)
        return nullptr;
    if (nargs == 5 && args[4] != Py_None && PyContextVar_Reset(sanic.current, args[4]) < 0) return nullptr;
    return Py_NewRef(sanic.two);
}

PyMethodDef request_methods[] = {
    {"sanic_finish", reinterpret_cast<PyCFunction>(reinterpret_cast<void (*)()>(request_sanic_finish)),
     METH_FASTCALL, nullptr},
    {"sanic_prepare", reinterpret_cast<PyCFunction>(reinterpret_cast<void (*)()>(request_sanic_prepare)),
     METH_FASTCALL, nullptr},
    {"sanic_headers", request_call<[](const Request& r) { return r.sanic_headers(); }>, METH_NOARGS, nullptr},
    {"sanic_header", reinterpret_cast<PyCFunction>(reinterpret_cast<void (*)()>(
         request_lookup<[](const Request& r, PyObject* name, py::object fallback) {
             return r.sanic_header(name, std::move(fallback));
         }>)), lookup_flags, nullptr},
    {"sanic_header_all", reinterpret_cast<PyCFunction>(reinterpret_cast<void (*)()>(request_header_all)),
     lookup_flags, nullptr},
    {"header", reinterpret_cast<PyCFunction>(reinterpret_cast<void (*)()>(
         request_lookup<[](const Request& r, PyObject* name, py::object fallback) {
             return r.header(name, std::move(fallback));
         }>)), lookup_flags, nullptr},
    {"header_bytes", reinterpret_cast<PyCFunction>(reinterpret_cast<void (*)()>(
         request_lookup<[](const Request& r, PyObject* name, py::object fallback) {
             return r.header_bytes(name, std::move(fallback));
         }>)), lookup_flags, nullptr},
    {"validate_sanic_request", request_call<[](const Request& r) {
         r.validate_sanic_request();
         return py::none();
     }>, METH_NOARGS, nullptr},
    {"sanic_request", request_call<[](const Request& r) { return r.sanic_request(); }>, METH_NOARGS, nullptr},
    {nullptr, nullptr, 0, nullptr},
};

// The type, named in the extension's module (kept for the process's life).
void add_request_type(py::module_& m) {
    static std::string name = py::str(m.attr("__name__")).cast<std::string>() + ".ApplicationRequest";
    static PyType_Slot slots[] = {
        {Py_tp_new, reinterpret_cast<void*>(request_new)},
        {Py_tp_dealloc, reinterpret_cast<void*>(request_dealloc)},
        {Py_tp_getset, request_attributes},
        {Py_tp_methods, request_methods},
        {0, nullptr},
    };
    static PyType_Spec spec = {name.c_str(), static_cast<int>(sizeof(RequestObject)), 0, Py_TPFLAGS_DEFAULT,
                               slots};
    auto* type = PyType_FromSpec(&spec);
    if (!type) throw py::error_already_set();
    request_type = reinterpret_cast<PyTypeObject*>(type);
    m.add_object("ApplicationRequest", py::reinterpret_borrow<py::object>(type));
}

uint64_t response_id(py::handle value) {
    if (!PyLong_CheckExact(value.ptr())) throw py::type_error("HTTP response id must be int");
    const auto result = PyLong_AsUnsignedLongLong(value.ptr());
    if (PyErr_Occurred()) throw py::error_already_set();
    return result;
}

unsigned response_shard(py::handle value) {
    if (!PyLong_CheckExact(value.ptr())) throw py::type_error("HTTP response shard must be int");
    const auto result = PyLong_AsUnsignedLong(value.ptr());
    if (PyErr_Occurred()) throw py::error_already_set();
    if (result > std::numeric_limits<unsigned>::max())
        throw py::value_error("HTTP response shard is out of range");
    return static_cast<unsigned>(result);
}

bool copy_latin1(PyObject* value, std::string& out) {
    const auto length = PyUnicode_GET_LENGTH(value);
    const auto kind = PyUnicode_KIND(value);
    const auto* data = PyUnicode_DATA(value);
    if (kind == PyUnicode_1BYTE_KIND) {
        out.assign(static_cast<const char*>(data), static_cast<size_t>(length));
        return true;
    }
    // Size has already been bounded. Inspect non-Latin-1 data without allocating
    // an intermediate Python bytes value or catching allocation/system failures.
    for (Py_ssize_t i = 0; i < length; ++i)
        if (PyUnicode_READ(kind, data, i) > 255) return false;
    out.resize(static_cast<size_t>(length));
    for (Py_ssize_t i = 0; i < length; ++i)
        out[i] = static_cast<char>(PyUnicode_READ(kind, data, i));
    return true;
}

// Writes the decision only when every field is valid, so a caller's invalid
// branch still holds its empty 500. upgrade: a 101 may start a tunnel.
bool checked_http_response_fields(PyObject* status, PyObject* body, PyObject* headers,
                                  size_t max_response_bytes, probe::RequestDecision& decision,
                                  bool bytes_headers = false, bool no_body_statuses = false,
                                  bool upgrade = false) {
    if (!PyLong_CheckExact(status) || !PyBytes_CheckExact(body) || !PyTuple_CheckExact(headers))
        return false;
    int overflow = 0;
    const auto code = PyLong_AsLongAndOverflow(status, &overflow);
    if (PyErr_Occurred()) throw py::error_already_set();
    if (overflow || (code < 200 && !(code == 101 && upgrade && no_body_statuses)) || code > 599)
        return false;
    if (!no_body_statuses && (code == 204 || code == 304)) return false;
    if (static_cast<size_t>(PyBytes_GET_SIZE(body)) > max_response_bytes) return false;
    const auto count = PyTuple_GET_SIZE(headers);
    if (count > 128) return false;

    // Check every shape and wire size before copying header strings or the body.
    // Latin-1 contributes one byte per Unicode code point when it is encodable.
    size_t remaining = 8192;
    for (Py_ssize_t i = 0; i < count; ++i) {
        auto* pair = PyTuple_GET_ITEM(headers, i);
        if (!PyTuple_CheckExact(pair) || PyTuple_GET_SIZE(pair) != 2) return false;
        for (Py_ssize_t j = 0; j < 2; ++j) {
            auto* value = PyTuple_GET_ITEM(pair, j);
            size_t length;
            if (bytes_headers) {
                if (!PyBytes_CheckExact(value)) return false;
                length = static_cast<size_t>(PyBytes_GET_SIZE(value));
            } else {
                if (!PyUnicode_CheckExact(value)) return false;
                if (PyUnicode_READY(value) < 0) throw py::error_already_set();
                length = static_cast<size_t>(PyUnicode_GET_LENGTH(value));
            }
            if (length > remaining) return false;
            remaining -= length;
        }
        if (remaining < 4) return false;
        remaining -= 4;
    }

    std::vector<std::pair<std::string, std::string>> owned_headers;
    owned_headers.reserve(static_cast<size_t>(count));
    for (Py_ssize_t i = 0; i < count; ++i) {
        auto* pair = PyTuple_GET_ITEM(headers, i);
        std::string name, value;
        if (bytes_headers) {
            auto* n = PyTuple_GET_ITEM(pair, 0);
            auto* v = PyTuple_GET_ITEM(pair, 1);
            name.assign(PyBytes_AS_STRING(n), static_cast<size_t>(PyBytes_GET_SIZE(n)));
            value.assign(PyBytes_AS_STRING(v), static_cast<size_t>(PyBytes_GET_SIZE(v)));
        } else if (!copy_latin1(PyTuple_GET_ITEM(pair, 0), name) ||
                   !copy_latin1(PyTuple_GET_ITEM(pair, 1), value)) return false;
        owned_headers.emplace_back(std::move(name), std::move(value));
    }
    if (!probe::valid_http_response(static_cast<int>(code), owned_headers, no_body_statuses, upgrade))
        return false;
    decision.body.assign(PyBytes_AS_STRING(body), static_cast<size_t>(PyBytes_GET_SIZE(body)));
    decision.code = static_cast<int>(code);
    decision.headers = std::move(owned_headers);
    return true;
}

bool checked_http_response(py::tuple item, size_t max_response_bytes,
                           probe::RequestDecision& decision, bool bytes_headers,
                           bool no_body_statuses = false, bool upgrade = false) {
    return checked_http_response_fields(item[2].ptr(), item[3].ptr(), item[4].ptr(),
                                         max_response_bytes, decision, bytes_headers, no_body_statuses,
                                         upgrade);
}

// The request inline_call runs the handler for, and whether the handler queued
// pieces of a streamed response for it meanwhile (Engine._queue_stream_item
// says so through inline_streamed): its answer then follows them through the
// queue. Used only with the GIL held, by the thread that polls the shard.
uint64_t inline_request = 0;
bool inline_request_streamed = false;

// ServerConfig::inline_call: a complete request handed to Python on its shard's
// own thread (the same-thread mode's poll(), which holds the GIL), as
// Engine._serve_now serves a plain function handler: the handler is called
// here, in a copy of the current context as a Task would run it. Python
// answers as a queued request's handler does, without the id and shard:
// (status, body, headers), or with a decision kind and its value (a HEAD
// response's Content-Length, or CACHED and its lifetime), checked as
// resolve_http_responses_checked checks them. Any other outcome (an exception,
// None, an awaitable, another shape, a streamed response) goes on in
// Engine._inline_outcome, which may answer later. Errors answer 500: nothing
// reaches the shard's handler.
int inline_call(const probe::ServerConfig& config, probe::RequestEvent& event,
                probe::RequestDecision& decision) {
    if (!PyGILState_Check()) return -1;  // the poller waits in a stop: the queue takes it
    auto* engine = static_cast<PyObject*>(config.inline_engine);
    auto* registry = static_cast<PyObject*>(config.inline_connections);
    auto* stats = static_cast<PyObject*>(config.inline_stats);
    static PyObject *closed_name = nullptr, *outcome_name = nullptr, *started = nullptr, *finished = nullptr;
    try {
        if (!finished) {  // each name once; `finished` last, when the others are there
            if (!closed_name) closed_name = PyUnicode_InternFromString("_closed");
            if (!outcome_name) outcome_name = PyUnicode_InternFromString("_inline_outcome");
            if (!started) started = PyUnicode_InternFromString("python_application_started");
            if (closed_name && outcome_name && started)
                finished = PyUnicode_InternFromString("python_application_finished");
            if (!finished) throw py::error_already_set();
        }
        // As drain_requests_owned: a connection's first request registers it.
        if (registry && event.sequence == 1 && event.connection) {
            auto key = py::reinterpret_steal<py::object>(PyLong_FromUnsignedLongLong(event.connection));
            if (!key || PyDict_SetItem(registry, key.ptr(), Py_None) < 0) throw py::error_already_set();
        }
        auto closed = py::reinterpret_steal<py::object>(PyObject_GetAttr(engine, closed_name));
        const int closing = closed ? PyObject_IsTrue(closed.ptr()) : -1;
        if (closing < 0) throw py::error_already_set();
        if (closing) {  // as a queued request to a closed Engine
            decision.code = 503;
            return 1;
        }
        const uint64_t id = event.id;
        const unsigned shard = event.shard;
        py::object request = make_request(std::move(event));
        auto context = py::reinterpret_steal<py::object>(PyContext_CopyCurrent());
        if (!context || PyContext_Enter(context.ptr()) < 0) throw py::error_already_set();
        const auto outer = std::exchange(inline_request, id);
        const bool outer_streamed = std::exchange(inline_request_streamed, false);
        auto result = py::reinterpret_steal<py::object>(
            PyObject_CallOneArg(static_cast<PyObject*>(config.inline_handler), request.ptr()));
        const bool streamed = std::exchange(inline_request_streamed, outer_streamed);
        inline_request = outer;
        if (PyContext_Exit(context.ptr()) < 0) throw py::error_already_set();
        const auto shape = result && PyTuple_CheckExact(result.ptr()) ? PyTuple_GET_SIZE(result.ptr()) : 0;
        if ((shape == 3 || shape == 5) && !streamed) {
            // Counted as Engine._plain_answer counts an answer.
            if (count(stats, started) < 0 || count(stats, finished) < 0) throw py::error_already_set();
        } else {
            auto error = py::reinterpret_steal<py::object>(result ? nullptr : PyErr_GetRaisedException());
            py::object id_value = py::int_(id), shard_value = py::int_(shard);
            PyObject* args[] = {engine, id_value.ptr(), shard_value.ptr(), request.ptr(),
                                result ? result.ptr() : Py_None, error ? error.ptr() : Py_None,
                                streamed ? Py_True : Py_False};
            result = py::reinterpret_steal<py::object>(PyObject_VectorcallMethod(
                outcome_name, args, 7 | PY_VECTORCALL_ARGUMENTS_OFFSET, nullptr));
            if (!result) throw py::error_already_set();
            if (result.is_none()) return 0;
        }
        auto* answer = result.ptr();
        const auto size = PyTuple_CheckExact(answer) ? PyTuple_GET_SIZE(answer) : 0;
        bool valid = (size == 3 || size == 5) &&
                     checked_http_response_fields(PyTuple_GET_ITEM(answer, 0), PyTuple_GET_ITEM(answer, 1),
                                                  PyTuple_GET_ITEM(answer, 2), config.max_response_bytes,
                                                  decision, config.inline_bytes_headers,
                                                  config.application_dispatch_all);
        if (valid && size == 5) {
            auto* kind = PyTuple_GET_ITEM(answer, 3);
            auto* value = PyTuple_GET_ITEM(answer, 4);
            valid = PyLong_CheckExact(kind) && PyLong_CheckExact(value);
            // The kind's high bits: the response's CORS policy (RequestDecision::cors).
            const int raw = valid ? py::cast<int>(py::handle(kind)) : 0;
            decision.cors = valid && raw >= 0 ? static_cast<unsigned>(raw >> 8) : 0;
            if (valid && (raw & 0xff) == probe::DecisionKind::cached)
                decision.cache_ms = py::cast<uint32_t>(py::handle(value));
            else if (valid && (raw & 0xff) == probe::DecisionKind::complete)
                decision.content_length = py::cast<int64_t>(py::handle(value));
            else
                valid = false;
        }
        if (!valid) decision = probe::RequestDecision{decision.id, decision.shard, 500, {}, true, {}};
        return 1;
    } catch (py::error_already_set& error) {
        error.discard_as_unraisable("the native host's direct call to the application");
    } catch (...) {
    }
    decision = probe::RequestDecision{decision.id, decision.shard, 500, {}, true, {}};
    return 1;
}

} // namespace

PYBIND11_MODULE(_native, m) {
    add_request_type(m);
    m.def("set_sanic_types", &set_sanic_types);
    m.def("set_application_state", &set_application_state);
    m.def("clear_application_state", &clear_application_state);
    // Engine._queue_stream_item: a piece of the response to request_id; when
    // inline_call runs its handler, the answer is to follow the pieces.
    m.def("inline_streamed", [](uint64_t request_id) {
        if (inline_request && request_id == inline_request) inline_request_streamed = true;
    });
    py::class_<probe::ServerConfig>(m, "ServerConfig")
        .def(py::init<>())
        .def_readwrite("http_port", &probe::ServerConfig::http_port)
        .def_readwrite("max_requests", &probe::ServerConfig::max_requests)
        .def_readwrite("max_bytes", &probe::ServerConfig::max_bytes)
        .def_readwrite("max_body_bytes", &probe::ServerConfig::max_body_bytes)
        .def_readwrite("decision_timeout_ms", &probe::ServerConfig::decision_timeout_ms)
        .def_readwrite("coalesce_notifications", &probe::ServerConfig::coalesce_notifications)
        .def_readwrite("generate_date_header", &probe::ServerConfig::generate_date_header)
        .def_readwrite("max_response_bytes", &probe::ServerConfig::max_response_bytes)
        .def_readwrite("application_dispatch_all", &probe::ServerConfig::application_dispatch_all)
        .def_readwrite("probe_headers", &probe::ServerConfig::probe_headers)
        .def_readwrite("http_host", &probe::ServerConfig::http_host)
        .def_readwrite("listen_fd", &probe::ServerConfig::listen_fd)
        .def_readwrite("unix_path", &probe::ServerConfig::unix_path)
        .def_readwrite("listen_shared", &probe::ServerConfig::listen_shared)
        .def_readwrite("accept_counts", &probe::ServerConfig::accept_counts)
        .def_readwrite("accept_slots", &probe::ServerConfig::accept_slots)
        .def_readwrite("accept_slot", &probe::ServerConfig::accept_slot)
        .def_readwrite("worker_listen_fd", &probe::ServerConfig::worker_listen_fd)
        .def_readwrite("worker_unix_path", &probe::ServerConfig::worker_unix_path)
        .def_readwrite("max_header_bytes", &probe::ServerConfig::max_header_bytes)
        .def_readwrite("keep_alive", &probe::ServerConfig::keep_alive)
        .def_readwrite("keep_alive_timeout_ms", &probe::ServerConfig::keep_alive_timeout_ms)
        .def_readwrite("write_timeout_ms", &probe::ServerConfig::write_timeout_ms)
        .def_readwrite("reason_phrases", &probe::ServerConfig::reason_phrases)
        .def_readwrite("body_window_bytes", &probe::ServerConfig::body_window_bytes)
        .def_readwrite("request_timeout_ms", &probe::ServerConfig::request_timeout_ms)
        .def_readwrite("framework_errors", &probe::ServerConfig::framework_errors)
        .def_readwrite("response_cache", &probe::ServerConfig::response_cache)
        .def_readwrite("cors_default", &probe::ServerConfig::cors_default)
        .def_readwrite("cors_route_policies", &probe::ServerConfig::cors_route_policies)
        // oldman_server.cors's table: (any_origin, (origin, ...), ((scheme, suffix), ...),
        // tail lines, vary, preflight lines, reflect_headers) for each policy.
        .def_property("cors_policies",
            [](const probe::ServerConfig& c) {
                py::list out;
                for (const auto& p : c.cors_policies)
                    out.append(py::make_tuple(p.any_origin, py::tuple(py::cast(std::vector<std::string>(
                                                  p.origins.begin(), p.origins.end()))),
                                              py::tuple(py::cast(p.wildcards)), p.tail, p.vary, p.preflight,
                                              p.reflect_headers));
                return py::tuple(out);
            },
            [](probe::ServerConfig& c, py::iterable items) {
                std::vector<probe::CorsPolicy> policies;
                for (auto item : items) {
                    auto [any_origin, origins, wildcards, tail, vary, preflight, reflect] =
                        item.cast<std::tuple<bool, std::vector<std::string>,
                                             std::vector<std::pair<std::string, std::string>>,
                                             std::string, bool, std::string, bool>>();
                    probe::CorsPolicy policy;
                    policy.any_origin = any_origin;
                    policy.origins.insert(origins.begin(), origins.end());
                    policy.wildcards = std::move(wildcards);
                    policy.tail = std::move(tail);
                    policy.vary = vary;
                    policy.preflight = std::move(preflight);
                    policy.reflect_headers = reflect;
                    policies.push_back(std::move(policy));
                }
                c.cors_policies = std::move(policies);
            })
        // The same-thread mode's direct call (inline_call): the Engine, its plain
        // function handler and stats dict, and the connections dict
        // drain_requests_owned registers connections in. Python keeps them
        // alive while the server runs: the configuration holds them borrowed.
        .def("set_inline", [](probe::ServerConfig& c, py::object engine, py::object handler, py::object stats,
                              py::object connections, bool bytes_headers) {
            if (!PyDict_CheckExact(stats.ptr())) throw py::type_error("stats must be a dict");
            if (!connections.is_none() && !PyDict_CheckExact(connections.ptr()))
                throw py::type_error("connections must be a dict");
            c.inline_call = &inline_call;
            c.inline_engine = engine.ptr();
            c.inline_handler = handler.ptr();
            c.inline_stats = stats.ptr();
            c.inline_connections = connections.is_none() ? nullptr : connections.ptr();
            c.inline_bytes_headers = bytes_headers;
        })
        // (path, status, body bytes, ((name, value), ...)[, cors]) for each path;
        // cors: its CORS policy, as RequestDecision::cors.
        .def_property("fixed_responses",
            [](const probe::ServerConfig& c) {
                py::list out;
                for (const auto& [path, response] : c.fixed_responses)
                    out.append(py::make_tuple(path, response.status, py::bytes(response.body),
                                              py::cast(response.headers)));
                return py::tuple(out);
            },
            [](probe::ServerConfig& c, py::iterable items) {
                std::map<std::string, probe::StoredResponse, std::less<>> fixed;
                for (auto item : items) {
                    auto fields = py::reinterpret_borrow<py::tuple>(item);
                    const unsigned cors = fields.size() > 4 ? fields[4].cast<unsigned>() : 0;
                    auto [path, status, body, headers] =
                        py::tuple(fields[py::slice(0, 4, 1)])
                            .cast<std::tuple<std::string, int, py::bytes, std::vector<std::pair<std::string, std::string>>>>();
                    if (!probe::valid_http_response(status, headers, true))
                        throw py::value_error("invalid fixed response for " + path);
                    fixed.insert_or_assign(std::move(path),
                                           probe::StoredResponse{status, std::string(body), std::move(headers), cors});
                }
                c.fixed_responses = std::move(fixed);
            });
    // Sanic's parse_multipart_form for the forms native/multipart.hh accepts:
    // (fields, files) dicts of lists, file parts as file_type(type, body, name);
    // None means Sanic's parser decides.
    m.def("parse_multipart", [](py::handle body, py::handle boundary, py::handle file_type) -> py::object {
        if (!PyBytes_CheckExact(body.ptr()) || !PyBytes_CheckExact(boundary.ptr())) return py::none();
        const auto parts = probe::multipart::parse(
            std::string_view(PyBytes_AS_STRING(body.ptr()), PyBytes_GET_SIZE(body.ptr())),
            std::string_view(PyBytes_AS_STRING(boundary.ptr()), PyBytes_GET_SIZE(boundary.ptr())));
        if (!parts) return py::none();
        py::dict fields, files;
        for (const auto& part : *parts) {
            py::object value;
            if (part.filename) {
                value = file_type(py::str(part.content_type), py::bytes(part.data.data(), part.data.size()),
                                  py::str(*part.filename));
            } else {
                auto* text = PyUnicode_DecodeUTF8(part.data.data(), static_cast<Py_ssize_t>(part.data.size()),
                                                  nullptr);
                if (!text) {  // Sanic's decode raises
                    PyErr_Clear();
                    return py::none();
                }
                value = py::reinterpret_steal<py::object>(text);
            }
            auto& target = part.filename ? files : fields;
            py::str name(part.name);
            if (target.contains(name)) target[name].cast<py::list>().append(value);
            else target[name] = py::make_tuple(value).cast<py::list>();
        }
        return py::make_tuple(fields, files);
    });
    py::class_<probe::Runtime>(m, "Runtime")
        .def(py::init<std::vector<int>, probe::ServerConfig, std::string, bool>(),
             py::arg("cpus"), py::arg("server") = probe::ServerConfig{}, py::arg("reactor_backend") = std::string{},
             py::arg("same_thread") = false)
        .def("drain_requests_owned", [](probe::Runtime& r, size_t count, py::object connections) {
            auto events = r.drain_requests(count);
            // Framework connections are tracked here, without Python events: the
            // first request of a connection registers it in `connections` (None
            // until Python keeps state for it) and its close removes it, calling
            // connection_lost() on any state kept.
            PyObject* registry = nullptr;
            if (!connections.is_none()) {
                if (!PyDict_CheckExact(connections.ptr())) throw py::type_error("connections must be a dict");
                registry = connections.ptr();
            }
            size_t visible = 0;
            for (const auto& e : events) visible += e.route != "application_closed";
            auto* list = PyList_New(static_cast<Py_ssize_t>(visible));
            if (!list) throw py::error_already_set();
            auto out = py::reinterpret_steal<py::list>(list);
            size_t slot = 0;
            for (auto& e : events) {
                py::object item;
                if (e.route == "application_closed") {
                    if (registry) {
                        auto key = py::reinterpret_steal<py::object>(PyLong_FromUnsignedLongLong(e.parent));
                        if (!key) throw py::error_already_set();
                        auto* kept = PyDict_GetItemWithError(registry, key.ptr());
                        if (!kept && PyErr_Occurred()) throw py::error_already_set();
                        if (kept) {
                            auto state = py::reinterpret_borrow<py::object>(kept);
                            if (PyDict_DelItem(registry, key.ptr()) < 0) throw py::error_already_set();
                            // None or False: no state was created for it.
                            if (state.ptr() != Py_None && state.ptr() != Py_False) {
                                try {
                                    state.attr("connection_lost")();
                                } catch (py::error_already_set& error) {
                                    // The events are off the native queue: the rest
                                    // of the batch must still reach Python.
                                    error.discard_as_unraisable("connection_lost of a native connection");
                                }
                            }
                        }
                    }
                    continue;
                }
                if (e.route == "application") {
                    if (registry && e.sequence == 1 && e.connection) {
                        auto key = py::reinterpret_steal<py::object>(PyLong_FromUnsignedLongLong(e.connection));
                        if (!key || PyDict_SetItem(registry, key.ptr(), Py_None) < 0)
                            throw py::error_already_set();
                    }
                    // A 4-tuple marks a request whose body is still arriving ("more")
                    // or a protocol error to render ("error").
                    if (e.parameter.empty()) {
                        item = py::make_tuple(e.id, e.shard, make_request(std::move(e)));
                    } else {
                        py::str marker(e.parameter == "more" ? "more" : "error");
                        item = py::make_tuple(e.id, e.shard, make_request(std::move(e)), marker);
                    }
                } else {
                    py::object body = e.copied ? py::object(py::bytes(e.body)) : py::object(py::none());
                    // Latin-1 decodes any byte: it cannot fail.
                    item = py::make_tuple(e.id, e.shard, decode_latin1(e.method), decode_latin1(e.route),
                                          e.body_size, std::move(body), decode_latin1(e.parameter), e.parent);
                }
                // Fresh, unpublished list: each slot receives one owned reference.
                PyList_SET_ITEM(out.ptr(), static_cast<Py_ssize_t>(slot++), item.release().ptr());
            }
            // Event reservations expire here; retaining a Python request retains
            // only its plain request data and cannot retain native admission.
            // Accepted risk: running out of memory (or an exception from a
            // signal handler) while building this list loses the batch's rest.
            return out;
        }, py::arg("count"), py::arg("connections") = py::none())
        .def("resolve_http_responses_checked", [](probe::Runtime& r, py::list items,
                                                  size_t max_response_bytes, bool bytes_headers,
                                                  bool no_body_statuses) {
            std::vector<probe::RequestDecision> owned;
            owned.reserve(items.size());
            size_t invalid = 0;
            for (auto value : items) {
                if (!PyTuple_CheckExact(value.ptr()))
                    throw py::type_error("HTTP response must be a tuple");
                auto item = py::reinterpret_borrow<py::tuple>(value);
                const auto size = item.size();
                if (size < 5 || size > 7)
                    throw py::value_error(
                        "HTTP response must contain id, shard, status, body, headers[, kind[, length]]");
                probe::RequestDecision decision{response_id(item[0]), response_shard(item[1]), 500, {}, true, {}};
                int kind = probe::DecisionKind::complete;
                unsigned cors = 0;
                if (size >= 6) {
                    if (!PyLong_CheckExact(item[5].ptr())) throw py::type_error("decision kind must be int");
                    kind = py::cast<int>(item[5]);
                    // The high bits: the response's CORS policy (RequestDecision::cors).
                    if (kind >= 0) {
                        cors = static_cast<unsigned>(kind >> 8);
                        kind &= 0xff;
                    }
                    if (kind < probe::DecisionKind::complete || kind > probe::DecisionKind::cached)
                        throw py::value_error("unknown decision kind");
                }
                if (kind == probe::DecisionKind::body_read) {
                    // The handler read this many bytes of the request body.
                    if (size != 7 || !PyLong_CheckExact(item[6].ptr()))
                        throw py::value_error("body_read needs the bytes read");
                    decision.kind = kind;
                    decision.code = 0;
                    decision.content_length = py::cast<int64_t>(item[6]);
                } else if (kind == probe::DecisionKind::cached) {
                    // A complete response the shard may keep for item[6] ms.
                    if (size != 7 || !PyLong_CheckExact(item[6].ptr()))
                        throw py::value_error("a cached response needs its lifetime");
                    if (checked_http_response(item, max_response_bytes, decision, bytes_headers,
                                              no_body_statuses, false))
                        decision.cache_ms = py::cast<uint32_t>(item[6]);
                    else
                        ++invalid;
                } else if (kind >= probe::DecisionKind::stream_data) {
                    // A later piece of a streamed response: its data only.
                    auto* data = item[3].ptr();
                    decision.kind = kind;
                    decision.code = 0;
                    if (PyBytes_CheckExact(data) &&
                        static_cast<size_t>(PyBytes_GET_SIZE(data)) <= max_response_bytes) {
                        decision.body.assign(PyBytes_AS_STRING(data), static_cast<size_t>(PyBytes_GET_SIZE(data)));
                    } else {
                        decision.kind = probe::DecisionKind::stream_abort;
                        ++invalid;
                    }
                } else if (checked_http_response(item, max_response_bytes, decision, bytes_headers,
                                                 no_body_statuses,
                                                 kind == probe::DecisionKind::stream_start)) {
                    // A 101 only starts a tunnel.
                    decision.kind = kind;
                    if (size == 7) decision.content_length = py::cast<int64_t>(item[6]);
                } else {
                    ++invalid;
                    if (kind == probe::DecisionKind::stream_start)
                        decision.retire = false;  // complete 500 plus a terminal event for Python
                }
                decision.cors = cors;
                owned.push_back(std::move(decision));
            }
            // Framing errors abort the entire batch; invalid user fields instead
            // produce one empty 500. Only plain owned decisions leave the GIL.
            py::gil_scoped_release release;
            r.resolve_requests(std::move(owned));
            return invalid;
        }, py::arg("items"), py::arg("max_response_bytes"),
           py::kw_only(), py::arg("bytes_headers") = false, py::arg("no_body_statuses") = false)
        .def("server_stats", [](const probe::Runtime& r) {
            auto s = r.server_stats();
            auto d = py::cast(s.totals).cast<py::dict>();
            d["shards"] = py::cast(s.shards);
            return d;
        })
        .def("fd", &probe::Runtime::fd)
        .def("fallback", &probe::Runtime::fallback)
        .def("clear_notification", &probe::Runtime::clear_notification)
        .def("submit_file", [](probe::Runtime& r, uint64_t id, int operation, std::string path,
                               uint64_t offset, int64_t length) {
            // The shard opens it as a C string, which would end at a NUL: refused
            // as Python's own file functions do.
            if (path.find('\0') != std::string::npos) throw py::value_error("embedded null byte");
            py::gil_scoped_release release;
            r.submit_file(id, operation, std::move(path), offset, length);
        })
        // (id, value, errno): value is the bytes read, or the 13 numbers of a
        // file_stat or file_open (os.stat order, the three times in nanoseconds).
        .def("drain_files", [](probe::Runtime& r, size_t count) {
            std::vector<probe::FileResult> items;
            { py::gil_scoped_release release; items = r.drain_files(count); }
            py::list result;
            for (auto& item : items) {
                py::object value;
                if (item.error) {
                    value = py::none();
                } else if (item.operation == probe::FileOperation::file_read) {
                    value = py::bytes(item.data);
                } else {
                    py::tuple numbers(item.stat.size());
                    for (size_t i = 0; i < item.stat.size(); ++i) {
                        const bool time = i >= 7 && i <= 9;
                        numbers[i] = time ? py::int_(static_cast<int64_t>(item.stat[i])) : py::int_(item.stat[i]);
                    }
                    value = std::move(numbers);
                }
                result.append(py::make_tuple(item.id, std::move(value), item.error));
            }
            return result;
        })
        .def("stats", [](const probe::Runtime& r) {
            auto s = r.stats();
            py::dict d;
            d["live_workers"] = s.live_workers;
            d["state"] = s.state;
            d["error"] = s.error;
            return d;
        })
        .def("shards", [](const probe::Runtime& r) {
            py::list out;
            for (const auto& s : r.shards()) {
                py::dict d;
                d["shard"] = s.shard; d["tid"] = s.tid;
                d["affinity"] = s.affinity; d["backend"] = s.backend;
                out.append(d);
            }
            return out;
        })
        // Bound class arguments are copied while the GIL is held: with
        // call_guard the copy would read the Python-owned object without it.
        .def("start_server", [](probe::Runtime& r, const probe::ServerConfig& server) {
            auto owned = server;
            py::gil_scoped_release release;
            return r.start_server(std::move(owned));
        }, py::arg("server"))
        .def("drain_server", &probe::Runtime::drain_server, py::arg("generation") = 0,
             py::call_guard<py::gil_scoped_release>())
        .def("stop_server", &probe::Runtime::stop_server, py::arg("generation") = 0,
             py::call_guard<py::gil_scoped_release>())
        .def("stop", &probe::Runtime::stop, py::call_guard<py::gil_scoped_release>())
        .def("join", &probe::Runtime::join, py::call_guard<py::gil_scoped_release>())
        .def("close_fd", &probe::Runtime::close_fd)
        // The shard's work runs on this thread, holding the GIL: it touches no
        // Python object.
        .def("poll", &probe::Runtime::poll)
        .def("reactor_fd", &probe::Runtime::reactor_fd);
}
