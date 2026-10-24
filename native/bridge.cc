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

} // namespace

PYBIND11_MODULE(_native, m) {
    py::class_<ApplicationRequest>(m, "ApplicationRequest")
        // Socket metadata is optional so tests and in-process measurement can
        // build complete Sanic requests without a listening runtime.
        .def(py::init([](py::handle method, py::handle target, py::handle headers, py::handle body,
                         std::string http_version,
                         std::optional<std::pair<std::string, unsigned>> client,
                         std::optional<std::pair<std::string, unsigned>> server) {
                 auto request = std::make_unique<ApplicationRequest>(method, target, headers, body);
                 request->http_version = std::move(http_version);
                 if (client) request->client = std::move(*client);
                 if (server) request->server = std::move(*server);
                 return request;
             }),
             py::arg("method"), py::arg("target"), py::arg("headers"), py::arg("body"), py::kw_only(),
             py::arg("http_version") = "", py::arg("client") = py::none(), py::arg("server") = py::none())
        .def_property_readonly("method", [](const ApplicationRequest& r) { return py::str(r.method); })
        .def_property_readonly("target", [](const ApplicationRequest& r) { return py::bytes(r.target); })
        .def_property_readonly("headers", &ApplicationRequest::python_headers)
        .def("sanic_headers", &ApplicationRequest::sanic_headers)
        .def("sanic_header", &ApplicationRequest::sanic_header, py::arg("name"), py::arg("default") = py::none())
        .def("sanic_header_all", &ApplicationRequest::sanic_header_all, py::arg("name"))
        .def_readonly("request_id", &ApplicationRequest::id)
        .def_readonly("shard", &ApplicationRequest::shard)
        .def_readonly("more_body", &ApplicationRequest::more_body)
        .def_readonly("connection", &ApplicationRequest::connection)
        .def_readonly("sequence", &ApplicationRequest::sequence)
        .def_readonly("unix_socket", &ApplicationRequest::unix_socket)
        .def_property_readonly("error", [](const ApplicationRequest& r) -> py::object {
            return r.error.empty() ? py::object(py::none()) : py::object(py::str(r.error));
        })
        .def_property_readonly("body", [](const ApplicationRequest& r) { return py::bytes(r.body); })
        .def_property_readonly("http_version", [](const ApplicationRequest& r) { return py::str(r.http_version); })
        .def_property_readonly("client", [](const ApplicationRequest& r) -> py::object {
            return r.client.first.empty() ? py::object(py::none()) : py::cast(r.client);
        })
        .def_property_readonly("server", [](const ApplicationRequest& r) -> py::object {
            return r.server.first.empty() ? py::object(py::none()) : py::cast(r.server);
        })
        .def_property_readonly("path", &ApplicationRequest::path)
        .def_property_readonly("query_string", &ApplicationRequest::query_string)
        .def("header", &ApplicationRequest::header, py::arg("name"), py::arg("default") = py::none())
        .def("header_bytes", &ApplicationRequest::header_bytes, py::arg("name"), py::arg("default") = py::none())
        .def("validate_sanic_request", &ApplicationRequest::validate_sanic_request)
        .def("sanic_request", &ApplicationRequest::sanic_request);
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
        .def_readwrite("body_window_bytes", &probe::ServerConfig::body_window_bytes)
        .def_readwrite("request_timeout_ms", &probe::ServerConfig::request_timeout_ms)
        .def_readwrite("framework_errors", &probe::ServerConfig::framework_errors);
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
        .def(py::init<std::vector<int>, probe::ServerConfig, std::string>(),
             py::arg("cpus"), py::arg("server") = probe::ServerConfig{}, py::arg("reactor_backend") = std::string{})
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
                        item = py::make_tuple(e.id, e.shard, ApplicationRequest(std::move(e)));
                    } else {
                        py::str marker(e.parameter == "more" ? "more" : "error");
                        item = py::make_tuple(e.id, e.shard, ApplicationRequest(std::move(e)), marker);
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
                if (size >= 6) {
                    if (!PyLong_CheckExact(item[5].ptr())) throw py::type_error("decision kind must be int");
                    kind = py::cast<int>(item[5]);
                    if (kind < probe::DecisionKind::complete || kind > probe::DecisionKind::body_read)
                        throw py::value_error("unknown decision kind");
                }
                if (kind == probe::DecisionKind::body_read) {
                    // The handler read this many bytes of the request body.
                    if (size != 7 || !PyLong_CheckExact(item[6].ptr()))
                        throw py::value_error("body_read needs the bytes read");
                    decision.kind = kind;
                    decision.code = 0;
                    decision.content_length = py::cast<int64_t>(item[6]);
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
        .def("close_fd", &probe::Runtime::close_fd);
}
