#include "server.hh"
#include "http1.hh"
#include <seastar/core/coroutine.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/timer.hh>
#include <seastar/core/abort_source.hh>
#include <seastar/core/with_timeout.hh>
#include <seastar/util/defer.hh>
#include <seastar/http/httpd.hh>
#include <seastar/http/handlers.hh>
#include <seastar/http/reply.hh>
#include <seastar/http/request.hh>
#include <seastar/http/client.hh>
#include <seastar/json/formatter.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/net/posix-stack.hh>
#include <seastar/net/unix_address.hh>
#include <algorithm>
#include <atomic>
#include <charconv>
#include <filesystem>
#include <mutex>
#include <optional>
#include <limits>
#include <deque>
#include <set>
#include <poll.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/syscall.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/un.h>

namespace probe {
namespace ss = seastar;
bool valid_http_response(int status,
    const std::vector<std::pair<std::string, std::string>>& headers, bool no_body_statuses,
    bool upgrade) {
    // Pinned Seastar httpd unconditionally adds framing, including to 204/304.
    // Only the HTTP/1.1 application host (no_body_statuses) writes them correctly.
    // That host sends the application's fields as Sanic does: any name, with its
    // own Content-Length / Transfer-Encoding / Connection in place of the
    // application's (http1::append_head); only the syntax is checked. 101 (a
    // protocol switch) only starts a framework host stream (upgrade): as the
    // status of a complete response it would claim a switch that never happens.
    if ((status < 200 && !(status == 101 && upgrade && no_body_statuses)) || status > 599 ||
        headers.size() > 128) return false;
    if (!no_body_statuses && (status == 204 || status == 304)) return false;
    size_t header_bytes = 0;
    for (const auto& [name, value] : headers) {
        if (name.size() > 8192 - header_bytes) return false;
        header_bytes += name.size();
        if (value.size() > 8192 - header_bytes) return false;
        header_bytes += value.size();
        if (8192 - header_bytes < 4) return false;
        header_bytes += 4; // colon, space, CRLF
        if (name.empty()) return false;
        std::string lower;
        lower.reserve(name.size());
        for (unsigned char c : name) {
            if (!((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
                  (c >= '0' && c <= '9') || std::string_view("!#$%&'*+-.^_`|~").find(c) != std::string_view::npos))
                return false;
            lower.push_back(c >= 'A' && c <= 'Z' ? c + ('a' - 'A') : c);
        }
        if (!no_body_statuses && (lower == "content-length" || lower == "transfer-encoding" || lower == "connection" ||
            lower == "keep-alive" || lower == "upgrade" || lower == "trailer" || lower == "te" ||
            lower == "proxy-connection" || lower == "date" || lower == "server" || lower.starts_with("x-probe-")))
            return false;
        for (unsigned char c : value)
            if ((c < 32 && c != '\t') || c == 127) return false;
    }
    return true;
}
namespace {
struct alignas(64) Counts {
    std::atomic<uint64_t> active_requests{0}, reserved_bytes{0}, peak_requests{0}, peak_bytes{0};
    std::atomic<uint64_t> accepted{0}, rejected{0}, responses{0}, errors{0}, pending_decisions{0};
    std::atomic<uint64_t> decisions{0}, decision_timeouts{0}, decision_wait_ns{0};
    std::atomic<uint64_t> python_terminal_events{0};
    std::atomic<uint64_t> native_policy_decisions{0}, native_policy_denied{0};
    std::atomic<uint64_t> body_bytes_to_python{0}, body_bytes_from_python{0}, response_bytes{0};
    std::atomic<uint64_t> compute_ns{0}, compute_cpu_ns{0}, worker_queue_ns{0};
    std::atomic<uint64_t> upstream_requests{0}, upstream_responses{0}, upstream_bytes{0}, upstream_wait_ns{0};
    std::atomic<uint64_t> tcp_connections{0}, tcp_frames{0}, udp_received{0}, udp_sent{0}, udp_dropped{0};
    std::atomic<uint64_t> connections_open{0};  // framework host connections
    size_t limit_count = 0, limit_bytes = 0;
    std::map<std::string,uint64_t> snapshot() const {
        std::map<std::string,uint64_t> d;
#define FIELD(x) d[#x] = x.load(std::memory_order_relaxed)
        FIELD(active_requests); FIELD(reserved_bytes); FIELD(peak_requests); FIELD(peak_bytes);
        FIELD(accepted); FIELD(rejected); FIELD(responses); FIELD(errors); FIELD(pending_decisions);
        FIELD(decisions); FIELD(decision_timeouts); FIELD(decision_wait_ns);
        FIELD(python_terminal_events);
        FIELD(native_policy_decisions); FIELD(native_policy_denied);
        FIELD(body_bytes_to_python); FIELD(body_bytes_from_python); FIELD(response_bytes);
        FIELD(compute_ns); FIELD(compute_cpu_ns); FIELD(worker_queue_ns);
        FIELD(upstream_requests); FIELD(upstream_responses); FIELD(upstream_bytes); FIELD(upstream_wait_ns);
        FIELD(tcp_connections); FIELD(tcp_frames); FIELD(udp_received); FIELD(udp_sent); FIELD(udp_dropped);
        FIELD(connections_open);
#undef FIELD
        return d;
    }
};
struct Lease {
    Counts& counts;
    size_t bytes;
    Lease(Counts& c, size_t b) : counts(c), bytes(b) {}
    ~Lease() { --counts.active_requests; counts.reserved_bytes -= bytes; }
};
struct Shared {
    ServerConfig config;
    unsigned shard_count;
    std::function<void()> notify;
    std::vector<std::unique_ptr<Counts>> counts;
    mutable std::mutex mutex;
    std::map<uint64_t,RequestEvent> events;
    // Drained application values belong to Python; their empty tree nodes stay
    // here until decide() reclaims them on the shard that allocated them.
    std::map<uint64_t,RequestEvent> pending_application_nodes;
    std::atomic<bool> running{false};
    std::atomic<uint64_t> decision_return_batches{0}, decision_return_items{0}, decision_return_posts{0};
    size_t file_size = 0;
    std::set<std::pair<std::string, std::string>> application_routes;
    Shared(ServerConfig c, unsigned n, std::function<void()> wake)
        : config(std::move(c)), shard_count(n), notify(std::move(wake)) {
        for (const auto& [method, path] : config.application_routes) {
            if (method != "GET" && method != "POST" && method != "PUT" && method != "PATCH" &&
                method != "DELETE" && method != "OPTIONS" && method != "HEAD")
                throw std::invalid_argument("unsupported application method");
            if (path.empty() || path.front() != '/' || path.find_first_of("?#") != std::string::npos)
                throw std::invalid_argument("application route must be an exact path");
            for (unsigned char ch : path)
                if (ch <= 32 || ch >= 127) throw std::invalid_argument("invalid application path");
            for (const auto* prefix : {"/native", "/hybrid", "/copy", "/policy", "/python", "/reference"})
                if (path == prefix || path.starts_with(std::string(prefix) + "/"))
                    throw std::invalid_argument("application route collides with experiment prefix");
            if (config.native_json && path == "/json")
                throw std::invalid_argument("application route collides with native JSON");
            if (!application_routes.emplace(method, path).second)
                throw std::invalid_argument("duplicate application route");
        }
        for (const auto& [method, path] : config.application_routes)
            if (method == "GET") application_routes.emplace("HEAD", path);
        for (unsigned i=0; i<n; ++i) {
            auto x = std::make_unique<Counts>();
            x->limit_count = config.max_requests / n + (i < config.max_requests % n);
            x->limit_bytes = config.max_bytes / n + (i < config.max_bytes % n);
            counts.push_back(std::move(x));
        }
        if (!config.file_path.empty()) file_size = std::filesystem::file_size(config.file_path);
    }
    // admitted: an event of a request Python already received (body data, write
    // acknowledgements, its deadline, a connection's close). Its memory is
    // bounded by that request's limits, so it does not take a queue slot.
    void publish(RequestEvent event, bool admitted = false) {
        {
            std::lock_guard lock(mutex);
            if (!admitted && events.size() >= config.max_requests)
                throw std::runtime_error("decision queue full");
            events.emplace(event.id, std::move(event));
        }
        notify();
    }
    void erase(uint64_t id) {
        std::lock_guard lock(mutex);
        if (!events.erase(id)) pending_application_nodes.erase(id);
    }
    // Removes a request event Python has not drained yet; false once drained.
    bool withdraw(uint64_t id) {
        std::lock_guard lock(mutex);
        return events.erase(id) > 0;
    }
};
struct Decision {
    int code;
    std::string body;
    uint64_t id = 0;
    bool retire = false;
    std::vector<std::pair<std::string, std::string>> headers;
    int kind = DecisionKind::complete;
    int64_t content_length = -1;
    int reject_status = 503;  // the answer when code is 3 (not answered by Python)
    bool timed_out = false;   // answered after the response deadline was reported
};
struct Waiter {
    ss::promise<Decision> promise;
    bool replied = false;
    // A streamed response keeps its waiter: later decisions queue here.
    std::deque<Decision> chunks;
    std::optional<ss::promise<>> more;
    // A tunnel's reading of the peer is paused by Python (backpressure).
    bool paused = false;
    std::optional<ss::promise<>> resumed;
};
// Request fields a framework handler receives, owned and independent of the
// HTTP implementation that parsed them.
struct ApplicationInput {
    std::string target, http_version;
    std::vector<std::pair<std::string, std::string>> headers;
    std::pair<std::string, unsigned> client, server;
    uint64_t connection = 0;
    unsigned sequence = 0;
    bool unix_socket = false;
};
// The protocol and address of a listening socket (TCP or AF_UNIX).
std::pair<int, ss::socket_address> listener_address(int fd) {
    sockaddr_storage storage{};
    socklen_t length = sizeof(storage);
    if (::getsockname(fd, reinterpret_cast<sockaddr*>(&storage), &length) < 0)
        throw std::system_error(errno, std::system_category(), "read the listening socket's address");
    ss::socket_address address;
    if (storage.ss_family == AF_INET) {
        address = ss::socket_address(reinterpret_cast<const sockaddr_in&>(storage));
    } else if (storage.ss_family == AF_INET6) {
        address = ss::socket_address(reinterpret_cast<const sockaddr_in6&>(storage));
    } else if (storage.ss_family == AF_UNIX) {
        const auto& unix_address = reinterpret_cast<const sockaddr_un&>(storage);
        address = ss::socket_address(ss::unix_domain_addr(std::string(
            unix_address.sun_path, strnlen(unix_address.sun_path, sizeof(unix_address.sun_path)))));
    } else {
        throw std::invalid_argument("the listening socket is neither TCP nor AF_UNIX");
    }
    return {address.is_af_unix() ? 0 : IPPROTO_TCP, address};
}
// A non-blocking duplicate of a listening socket another owner keeps.
ss::file_desc listener_copy(int fd) {
    const int copy = ::fcntl(fd, F_DUPFD_CLOEXEC, 0);  // the owner closes its own
    if (copy < 0) throw std::system_error(errno, std::system_category(), "duplicate the listening socket");
    auto owned = ss::file_desc::from_fd(copy);
    // Accepted without blocking (the flag is shared with the owner's fd).
    if (::fcntl(copy, F_SETFL, ::fcntl(copy, F_GETFL) | O_NONBLOCK) < 0)
        throw std::system_error(errno, std::system_category(), "make the listening socket non-blocking");
    return owned;
}
// Accepts on a listening socket (TCP or AF_UNIX) another owner bound, as
// Seastar's posix stack does with sockets it binds without SO_REUSEPORT:
// shard 0 accepts on a duplicate of it and hands connections to the others.
ss::server_socket adopt_listener(int fd) {
    const auto [protocol, address] = listener_address(fd);
    if (ss::this_shard_id() != 0)
        return ss::server_socket(std::make_unique<ss::net::posix_ap_server_socket_impl>(protocol, address));
    auto owned = listener_copy(fd);
    return ss::server_socket(std::make_unique<ss::net::posix_server_socket_impl>(
        protocol, address, ss::pollable_fd(std::move(owned)),
        ss::server_socket::load_balancing_algorithm::connection_distribution, 0, false));
}
// Addresses as Python's socket module gives them: no IPv6 scope suffix for scope 0.
std::string address_text(const ss::socket_address& address) {
    auto text = fmt::format("{}", address.addr());
    if (text.ends_with("%0")) text.resize(text.size() - 2);
    return text;
}
ApplicationInput application_input(const ss::http::request& request) {
    ApplicationInput input{std::string(request._url), std::string(request._version), {}, {}, {}};
    const auto& client = request.get_client_address();
    const auto& server = request.get_server_address();
    input.client = {fmt::format("{}", client.addr()), client.port()};
    input.server = {fmt::format("{}", server.addr()), server.port()};
    input.headers.reserve(request._headers.size());
    for (const auto& [name, value] : request._headers)
        input.headers.emplace_back(std::string(name), std::string(value));
    return input;
}
class Service;
class Handler : public ss::httpd::handler_base {
    Service& service;
    std::string mode, route;
public:
    Handler(Service& s, std::string m, std::string r) : service(s), mode(std::move(m)), route(std::move(r)) {}
    ss::future<std::unique_ptr<ss::http::reply>> handle(const ss::sstring& path,
        std::unique_ptr<ss::http::request> request, std::unique_ptr<ss::http::reply> reply) override;
};

class Service {
    Shared& shared;
    Counts& counts;
    NetworkServer::Compress compress;
    ss::httpd::http_server http{"python-native-http"};
    // Pinned routes does not own its default handler.
    std::unique_ptr<Handler> application_handler;
    std::map<uint64_t, std::unique_ptr<Waiter>> waiters;
    // Request and connection ids count on per shard across the servers a
    // process runs one after another: a late answer to a replaced server's
    // request never matches another server's request.
    static uint64_t& shard_counter(int which) {
        static thread_local uint64_t counters[2] = {0, 0};
        return counters[which];
    }
    uint64_t& next_id = shard_counter(0);
    bool closing = false;
    // Stopping gracefully (drain): no new connections, idle ones closed, the
    // others closed after their response.
    bool draining = false;
    bool accepts() const { return !closing && !draining; }
    NativePolicy policy;
    ss::semaphore compression_slot{1};
    std::vector<std::unique_ptr<ss::http::client>> upstreams;
    std::set<ss::abort_source*> upstream_active;
    struct UpstreamFailure { int status; };

    ss::future<int> fetch(unsigned origin, std::string& body) {
        if (closing) throw UpstreamFailure{503};
        ss::abort_source abort;
        bool expired = false;
        ss::timer<ss::lowres_clock> timeout([&] { expired = true; abort.request_abort(); });
        timeout.arm(std::chrono::milliseconds(shared.config.decision_timeout_ms));
        upstream_active.insert(&abort);
        auto start = clock_ns();
        auto done = ss::defer([&] () noexcept {
            timeout.cancel(); upstream_active.erase(&abort);
            counts.upstream_wait_ns += clock_ns() - start;
        });
        ++counts.upstream_requests;
        int status = 0;
        auto req = ss::http::request::make("GET", "127.0.0.1", "/native/fixed?size=8192");
        try {
            co_await upstreams.at(origin)->make_request(std::move(req),
                [this, &body, &status](const ss::http::reply& reply, ss::input_stream<char>&& stream) -> ss::future<> {
                    status = int(reply._status);
                    auto input = std::move(stream);
                    if (reply.content_length > shared.config.max_body_bytes) throw UpstreamFailure{502};
                    for (;;) {
                        auto chunk = co_await input.read();
                        if (chunk.empty()) break;
                        if (chunk.size() > shared.config.max_body_bytes - body.size()) throw UpstreamFailure{502};
                        body.append(chunk.get(), chunk.size());
                    }
                    if (reply.get_header("Content-Length") != "" && body.size() != reply.content_length)
                        throw UpstreamFailure{502};
                    // EOF is not sufficient: Seastar's chunk decoder can reach
                    // it before the final chunk/trailer. Only zero means complete.
                    if (reply.left_content_length != 0) throw UpstreamFailure{502};
                    co_await input.close();
                }, std::nullopt, &abort);
        } catch (...) {
            throw UpstreamFailure{closing ? 503 : expired ? 504 : 502};
        }
        if (closing || expired) throw UpstreamFailure{closing ? 503 : 504};
        ++counts.upstream_responses;
        counts.upstream_bytes += body.size();
        co_return status;
    }

    ss::future<std::unique_ptr<ss::http::reply>> handle_fetch(std::string mode,
        std::unique_ptr<ss::http::request> req, std::unique_ptr<ss::http::reply> rep) {
        if (upstreams.empty()) co_return error(std::move(rep), 404);
        if (req->content_length || !req->get_header("Transfer-Encoding").empty())
            co_return error(std::move(rep), 400);
        auto lease = reserve(shared.config.max_body_bytes);
        if (!lease) co_return error(std::move(rep), 503);
        uint64_t root = 0;
        unsigned terminal_status = 0;
        bool retired = false;
        auto finished = ss::defer([&] () noexcept {
            if (!root || retired) return;
            RequestEvent event{(++next_id << 16) | ss::this_shard_id(), ss::this_shard_id(),
                               "GET", "python_done", terminal_status};
            event.parent = root; event.reservation = lease;
            ++counts.python_terminal_events;
            shared.publish(std::move(event));
        });
        try {
            unsigned origin = 0;
            auto parameter = req->get_query_param("upstream");
            if (mode == "python") {
                auto answer = co_await decide("python_fetch", "GET", 0, nullptr,
                                             std::string(parameter), 0, &root);
                if (answer.code < 1000 || unsigned(answer.code - 1000) >= upstreams.size()) {
                    retired = answer.retire;
                    co_return error(std::move(rep), answer.code >= 400 && answer.code <= 599 ? answer.code : 503);
                }
                origin = answer.code - 1000;
            } else {
                if (parameter.empty()) parameter = "0";
                auto parsed = std::from_chars(parameter.data(), parameter.data()+parameter.size(), origin);
                if (parsed.ec != std::errc() || parsed.ptr != parameter.data()+parameter.size()
                    || origin >= upstreams.size()) co_return error(std::move(rep), 400);
            }
            auto body = std::make_shared<std::string>();
            const auto status = co_await fetch(origin, *body);
            std::string tag;
            if (mode == "python") {
                auto answer = co_await decide("python_result", "GET", body->size(), nullptr,
                                             std::to_string(status), root);
                retired = answer.retire;
                if (answer.code != 200)
                    co_return error(std::move(rep), answer.code >= 400 && answer.code <= 599 ? answer.code : 503);
                tag = std::move(answer.body);
            } else {
                if (status != 200 || body->empty()) co_return error(std::move(rep), 502);
                tag = std::to_string(status) + ":" + std::to_string(body->size());
            }
            if (tag.size() > 128 || tag.find_first_of("\r\n") != std::string::npos)
                co_return error(std::move(rep), 500);
            rep->add_header("X-App-Result", ss::sstring(tag));
            rep->write_body(std::nullopt, [body, lease](ss::output_stream<char>& out) {
                return write_payload(out, body, lease);
            });
            co_return std::move(rep);
        } catch (const UpstreamFailure& failure) {
            terminal_status = failure.status; ++counts.errors;
            co_return error(std::move(rep), failure.status);
        } catch (const ss::timed_out_error&) {
            terminal_status = 504; co_return error(std::move(rep), 504);
        } catch (...) {
            terminal_status = 500; ++counts.errors;
            co_return error(std::move(rep), 500);
        }
    }
    struct Peer {
        ss::connected_socket socket;
        ss::input_stream<char> input;
        ss::output_stream<char> output;
        explicit Peer(ss::connected_socket s)
            : socket(std::move(s)), input(socket.input()), output(socket.output()) {}
    };
    std::optional<ss::server_socket> tcp;
    // This pinned Seastar disables POSIX reuseport globally for load balance.
    // A Linux-only UDP adapter opts in explicitly, using its reactor-owned FD.
    // Flow distribution is measured; no upstream source is patched.
    std::optional<ss::pollable_fd> udp;
    ss::future<> accepting = ss::make_ready_future<>(), datagrams = ss::make_ready_future<>();
    ss::gate connections;
    std::map<uint64_t, std::unique_ptr<Peer>> peers;
    uint64_t& next_peer = shard_counter(1);

    ss::future<> tcp_flow(uint64_t id) {
        auto& peer = *peers.at(id);
        try {
            while (!closing) {
                auto header = co_await peer.input.read_exactly(4);
                if (header.empty()) break;
                if (header.size() != 4) throw std::runtime_error("partial frame header");
                uint32_t wire;
                std::memcpy(&wire, header.get(), 4);
                auto size = ntohl(wire);
                if (size > shared.config.max_body_bytes) { ++counts.rejected; break; }
                auto lease = reserve(size_t(size) * 2 + 8);
                if (!lease) break;
                auto body = co_await peer.input.read_exactly(size);
                if (body.size() != size) throw std::runtime_error("partial frame body");
                co_await peer.output.write(header.get(), 4);
                co_await peer.output.write(body.get(), body.size());
                co_await peer.output.flush();
                ++counts.tcp_frames;
                counts.response_bytes += size;
            }
        } catch (...) { if (!closing) ++counts.errors; }
        try { co_await peer.input.close(); } catch (...) {}
        try { co_await peer.output.close(); } catch (...) {}
        --counts.tcp_connections;
        peers.erase(id);
    }
    ss::future<> accept_tcp() {
        try {
            while (!closing) {
                auto accepted = co_await tcp->accept();
                if (closing) break;
                if (peers.size() >= counts.limit_count) { ++counts.rejected; continue; }
                auto id = ++next_peer;
                peers.emplace(id, std::make_unique<Peer>(std::move(accepted.connection)));
                ++counts.tcp_connections;
                (void)ss::with_gate(connections, [this, id] { return tcp_flow(id); }).handle_exception(
                    [this](std::exception_ptr) { ++counts.errors; });
            }
        } catch (...) { if (!closing) ++counts.errors; }
    }
    // ---- HTTP/1.1 host for framework dispatch (application_dispatch_all) ----
    // Parses and writes HTTP/1.1 directly on Seastar sockets: one buffered write
    // per batch of responses, Content-Length framing, repeated header names.
    struct AppConnection {
        ss::connected_socket socket;
        ss::input_stream<char> input;
        ss::output_stream<char> output;
        // serve_application's read buffer and parse position: bytes the peer
        // sent after a request that switches protocols belong to the tunnel.
        std::string* buffer = nullptr;
        size_t* consumed = nullptr;
        // The request Python handles on this connection (its id), while it
        // does: the peer leaving cancels it. tunnel: a WebSocket tunnel
        // reads the peer itself and reports its end.
        uint64_t handling = 0;
        bool tunnel = false;
        bool peer_gone = false;  // the peer closed (FIN) or reset (RST) it
        bool shut_here = false;  // this side shut the input (deadline, stop, the end)
        // Armed while a write waits for the peer to take it (write_timeout_ms):
        // a peer that stops reading has the connection shut, failing the write.
        ss::timer<ss::lowres_clock> write_timer;
        bool idle = false;  // waiting for a request, nothing of it received
        explicit AppConnection(ss::connected_socket s)
            : socket(std::move(s)), input(socket.input()), output(socket.output()) {}
        void shut_input() {
            shut_here = true;
            try { socket.shutdown_input(); } catch (...) {}  // ENOTCONN after a reset
        }
    };
    // The peer of a connection left: as Sanic's connection_lost, the request
    // Python is handling there is cancelled (application_lost; the event's
    // size is the connection key, whose state Python marks lost).
    void peer_left(AppConnection& connection, uint64_t connection_key) {
        if (connection.peer_gone) return;
        connection.peer_gone = true;
        if (!connection.handling || connection.tunnel || closing) return;
        RequestEvent lost{(++next_id << 16) | ss::this_shard_id(), ss::this_shard_id(),
                          {}, "application_lost", connection_key};
        lost.parent = connection.handling;
        shared.publish(std::move(lost), true);
    }
    std::optional<ss::server_socket> app_listener, worker_listener;
    ss::future<> app_accepting = ss::make_ready_future<>(), worker_accepting = ss::make_ready_future<>();
    // Shared listen_fd: shard 0's accept loop (accept_shared) and the eventfd
    // stop() writes to end it.
    ss::future<> shared_accepting = ss::make_ready_future<>();
    std::optional<ss::file_desc> shared_wake;
    // Shared listen_fd: the worker processes' accept counts, and this one's.
    AcceptSlot* accept_slots = nullptr;
    AcceptSlot* own_slot = nullptr;
    uint64_t own_pid = 0;
    ss::timer<ss::lowres_clock> accept_heartbeat;
    // Ends the pauses of the accept loops after a failed accept (stop()).
    ss::abort_source accept_pause;
    // After a failed accept: wait before accepting again, longer while the
    // failures go on (from 10 ms to 1 s; asyncio waits 1 s after EMFILE).
    // False when the server stops meanwhile.
    ss::future<bool> pause_accepting(unsigned failures) {
        const auto pause = std::chrono::milliseconds(10 << std::min(failures, 6u));
        try {
            co_await ss::sleep_abortable(std::min(pause, std::chrono::milliseconds(1000)), accept_pause);
        } catch (const ss::sleep_aborted&) {
            co_return false;
        }
        co_return accepts();
    }
    ss::gate app_connections;
    std::map<uint64_t, std::unique_ptr<AppConnection>> app_peers;
    http1::DateLine date_line;
    std::string probe_lines;  // configured diagnostic header lines, fixed per shard

    std::string_view date() {
        return shared.config.generate_date_header ? date_line.get() : std::string_view{};
    }
    // One write the peer must take within write_timeout_ms (see AppConnection).
    template <typename Write>
    ss::future<> timed_write(AppConnection& connection, Write write) {
        if (const auto timeout = shared.config.write_timeout_ms)
            connection.write_timer.arm(ss::lowres_clock::now() + std::chrono::milliseconds(timeout));
        std::exception_ptr failure;
        try {
            co_await write();
        } catch (...) {
            failure = std::current_exception();
        }
        connection.write_timer.cancel();
        if (failure) std::rethrow_exception(failure);
    }
    ss::future<> flush_output(AppConnection& connection, std::string& output) {
        if (output.empty()) co_return;
        co_await timed_write(connection, [&]() -> ss::future<> {
            co_await connection.output.write(output.data(), output.size());
            co_await connection.output.flush();
        });
        output.clear();
    }
    static uint64_t monotonic_ms() {
        timespec now{};
        ::clock_gettime(CLOCK_MONOTONIC, &now);
        return uint64_t(now.tv_sec) * 1000 + uint64_t(now.tv_nsec) / 1000000;
    }
    // A sibling counts while its heartbeat (every 50 ms) is this recent: one
    // that crashed or hangs holds the others back for at most this long.
    static constexpr uint64_t accept_stale_ms = 200;
    static uint64_t open_connections(const AcceptSlot& slot) {
        return slot.connections.load(std::memory_order_relaxed) & 0xffffffffu;
    }
    bool owns_slot() const {
        return own_slot->connections.load(std::memory_order_relaxed) >> 32 == own_pid;
    }
    // Count a connection opening (+1) or closing (-1) while the line is this
    // process's.
    void count_connection(int64_t delta) {
        auto word = own_slot->connections.load(std::memory_order_relaxed);
        while (word >> 32 == own_pid &&
               !own_slot->connections.compare_exchange_weak(word, word + uint64_t(delta),
                                                           std::memory_order_relaxed)) {}
    }
    // No sibling that is accepting has fewer open connections than this process.
    bool least_loaded() const {
        // A restarted successor took over this process's line, which counts
        // its connections, not ours: this process is about to stop, and takes
        // a connection only when no other one does.
        if (!owns_slot()) return false;
        const auto mine = open_connections(*own_slot);
        if (mine == 0) return true;
        const auto now = monotonic_ms();
        for (unsigned i = 0; i < shared.config.accept_slots; ++i) {
            const auto& slot = accept_slots[i];
            if (&slot == own_slot) continue;
            const auto beat = slot.heartbeat_ms.load(std::memory_order_acquire);
            if (beat && now < beat + accept_stale_ms && open_connections(slot) < mine) return false;
        }
        return true;
    }
    // A connection is waiting on the shared socket.
    bool connection_waiting() const {
        pollfd ready{shared.config.listen_fd, POLLIN, 0};
        return ::poll(&ready, 1, 0) > 0 && (ready.revents & POLLIN);
    }
    // Shard 0 of a worker process that shares listen_fd with others. Seastar's
    // accept can only be stopped by shutting the socket down, which stops it
    // for every process (a restarted worker's siblings then refused all
    // connections). This loop waits on a process-local epoll set holding the
    // socket and the eventfd stop() writes, accepts without blocking, and
    // hands each connection to the shard with the fewest, as Seastar's
    // connection_distribution does; every shard takes them from its ap socket.
    //
    // Without taking turns, the process whose shard is busy (polling instead
    // of sleeping) wins nearly every accept. Shard 0 accepts while it is least
    // loaded. Holding back, it checks again after 50 us, then less often (up
    // to every 1 ms): two processes that both accept quickly keep passing the
    // turn, and one that has fallen behind only notices at its next check
    // (checking every 200 us left connections queued, a median latency of
    // 15 ms at 20k connections/s). After 1 ms it takes a connection still
    // waiting: the others are not accepting (an idle shard wakes well within).
    ss::future<> accept_shared(int protocol, ss::socket_address address) {
        try {
            auto socket = listener_copy(shared.config.listen_fd);
            const int epoll = ::epoll_create1(EPOLL_CLOEXEC);
            if (epoll < 0) throw std::system_error(errno, std::system_category(), "epoll_create1");
            auto watched = ss::file_desc::from_fd(epoll);
            for (int fd : {socket.get(), shared_wake->get()}) {
                epoll_event event{};
                event.events = EPOLLIN;
                event.data.fd = fd;
                if (::epoll_ctl(epoll, EPOLL_CTL_ADD, fd, &event) < 0)
                    throw std::system_error(errno, std::system_category(), "epoll_ctl");
            }
            ss::pollable_fd ready(std::move(watched));
            ss::net::conntrack shards;
            unsigned failures = 0;
            while (accepts()) {
                if (own_slot) {
                    auto pause = std::chrono::microseconds(50);
                    const auto give_up = ss::steady_clock_type::now() + std::chrono::milliseconds(1);
                    while (accepts() && !least_loaded()) {
                        co_await ss::sleep(pause);
                        pause = std::min(pause * 2, std::chrono::microseconds(1000));
                        if (ss::steady_clock_type::now() >= give_up && connection_waiting()) break;
                    }
                }
                if (!accepts()) break;
                ss::socket_address remote;
                socklen_t length = sizeof(remote.u);
                const int fd = ::accept4(socket.get(), &remote.u.sa, &length, SOCK_NONBLOCK | SOCK_CLOEXEC);
                if (fd < 0) {
                    const int error = errno;
                    if (error == EAGAIN || error == EWOULDBLOCK || error == EINTR || error == ECONNABORTED) {
                        co_await ready.readable();  // a connection, or stop()
                    } else if (error == EBADF || error == EINVAL || error == ENOTSOCK || error == EFAULT) {
                        // The listening socket itself is unusable.
                        throw std::system_error(error, std::system_category(), "accept4");
                    } else {
                        // Out of descriptors or memory (EMFILE, ENFILE, ENOBUFS,
                        // ENOMEM), or what accept(2) says to retry on a TCP
                        // socket (EPROTO, ENETDOWN, EHOSTUNREACH, ..., EPERM from
                        // a firewall): this connection failed, the next may not.
                        ++counts.errors;
                        if (!co_await pause_accepting(failures++)) break;
                    }
                    continue;
                }
                failures = 0;
                remote.addr_length = length;
                auto handle = shards.get_handle();
                const auto shard = handle.cpu();
                (void)ss::smp::submit_to(shard, [protocol, address, connection = ss::file_desc::from_fd(fd),
                                                 remote, handle = std::move(handle)]() mutable {
                    ss::net::posix_ap_server_socket_impl::move_connected_socket(
                        protocol, address, ss::pollable_fd(std::move(connection)), remote, std::move(handle),
                        std::nullopt, ss::memory::malloc_allocator);
                });
            }
        } catch (...) {
            if (accepts()) {
                ++counts.errors;
                // This process no longer accepts: its siblings stop waiting
                // for its turn (its heartbeat would say it still does).
                if (own_slot) {
                    accept_heartbeat.cancel();
                    if (owns_slot()) own_slot->heartbeat_ms.store(0, std::memory_order_release);
                }
            }
        }
    }
    // unix_path (a ServerConfig member, so it outlives the connections): what
    // requests over an AF_UNIX listener report as the server address.
    ss::future<> accept_application(ss::server_socket& listener, const std::string& unix_path) {
        try {
            unsigned failures = 0;
            while (accepts()) {
                std::optional<ss::accept_result> result;
                try {
                    result = co_await listener.accept();
                } catch (...) {}
                if (!result) {
                    // Seastar's listener throws for every accept error: out of
                    // descriptors (EMFILE under load), a connection reset before
                    // it was accepted. The listener is still usable: the loop
                    // ended here, and the process never accepted again.
                    if (!accepts()) break;
                    ++counts.errors;
                    if (!co_await pause_accepting(failures++)) break;
                    continue;
                }
                failures = 0;
                auto accepted = std::move(*result);
                if (!accepts()) break;
                auto id = ++next_peer;
                app_peers.emplace(id, std::make_unique<AppConnection>(std::move(accepted.connection)));
                ++counts.connections_open;
                // Every open connection counts towards this process's load.
                if (own_slot) count_connection(1);
                (void)ss::with_gate(app_connections, [this, id, remote = accepted.remote_address, &unix_path] {
                    return serve_application(id, remote, unix_path);
                }).handle_exception([this](std::exception_ptr) { ++counts.errors; }).finally([this] {
                    if (own_slot) count_connection(-1);
                });
            }
        } catch (...) { if (accepts()) ++counts.errors; }
    }
    ss::future<> serve_application(uint64_t id, ss::socket_address remote, const std::string& unix_path) {
        auto& connection = *app_peers.at(id);
        const auto& config = shared.config;
        const auto local = connection.socket.local_address();
        // Over a unix socket Sanic reports no client address and the socket path.
        const bool unix_socket = local.is_af_unix();
        using Address = std::pair<std::string, unsigned>;
        const Address client = unix_socket ? Address{} : Address{address_text(remote), remote.port()};
        const Address server = unix_socket ? Address{unix_path, 0} : Address{address_text(local), local.port()};
        const uint64_t connection_key = (id << 16) | ss::this_shard_id();
        unsigned sequence = 0;
        std::string buffer, output;
        size_t consumed = 0;
        connection.buffer = &buffer;
        connection.consumed = &consumed;
        // Keep-alive idle and request-receive deadlines: the timer shuts the input
        // down, which ends the pending read; `expired` says which deadline it was.
        enum class Wait { none, idle, request } expired = Wait::none, waiting = Wait::none;
        ss::lowres_clock::time_point request_started{};
        ss::timer<ss::lowres_clock> deadline([&] {
            expired = waiting;
            connection.shut_input();
        });
        connection.write_timer.set_callback([&connection, this] {
            ++counts.errors;
            connection.shut_input();
            try { connection.socket.shutdown_output(); } catch (...) {}
        });
        // The peer leaving (FIN or RST) while Python handles a request here.
        // One watch for the connection's life, which ends when either side
        // shuts the input; this side's own shutdowns are not the peer leaving.
        auto watching = connection.socket.wait_input_shutdown().then_wrapped(
            [this, &connection, connection_key](ss::future<> done) {
                done.ignore_ready_future();
                if (!connection.shut_here) peer_left(connection, connection_key);
            });
        // mid_request: a body is being received even if nothing of it is buffered.
        auto read_more = [&](bool mid_request = false) -> ss::future<bool> {
            const bool idle = !mid_request && buffer.size() == consumed;
            if (idle && draining) co_return false;  // Sanic's close_if_idle
            waiting = idle ? Wait::idle : Wait::request;
            const auto timeout = idle ? config.keep_alive_timeout_ms : config.request_timeout_ms;
            if (timeout) {
                deadline.arm(idle ? ss::lowres_clock::now() + std::chrono::milliseconds(timeout)
                                  : request_started + std::chrono::milliseconds(timeout));
            }
            connection.idle = idle;
            auto chunk = co_await connection.input.read();
            connection.idle = false;
            deadline.cancel();
            if (chunk.empty()) co_return false;
            if (idle) request_started = ss::lowres_clock::now();
            buffer.append(chunk.get(), chunk.size());
            co_return true;
        };
        try {
            for (bool keep = true; keep && !closing;) {
                // Responses to pipelined requests are batched into one write,
                // up to 64 KiB (Sanic's write buffer limit): a client that
                // pipelines requests and does not read their responses holds
                // no more than that, until the write deadline.
                if (output.size() >= 65536) co_await flush_output(connection, output);
                http1::Head head;
                // No field count of its own: the size limit bounds the head, as in Sanic.
                const auto parsed = http1::parse_head(std::string_view(buffer).substr(consumed),
                                                      config.max_header_bytes,
                                                      std::numeric_limits<size_t>::max(), head);
                if (parsed == http1::Parse::incomplete) {
                    co_await flush_output(connection, output);
                    buffer.erase(0, consumed);
                    consumed = 0;
                    if (!co_await read_more()) break;
                    continue;
                }
                if (parsed != http1::Parse::complete && !config.framework_errors) {
                    http1::append_error(output, head.error_status, true, date(), probe_lines);
                    break;
                }
                if (parsed != http1::Parse::complete) {
                    // Sanic's error_response() over an empty request: the
                    // application's error handler renders it; then the connection closes.
                    ApplicationInput input{std::string(head.target), "1.1", {}, client, server,
                                           connection_key, ++sequence, unix_socket};
                    bool close = true;
                    co_await respond_application(connection, "NONE", std::move(input), {}, false, true,
                                                 close, output, 0,
                                                 fmt::format("error:{}:{}", head.error_status, head.error),
                                                 head.error_status);
                    break;
                }
                // Copy what Python owns now: reading the body may reallocate the buffer.
                std::string method(head.method);
                ApplicationInput input{std::string(head.target), std::string(head.version.substr(5)),
                                       {}, client, server, connection_key, ++sequence, unix_socket};
                input.headers.reserve(head.headers.size());
                for (const auto& [name, value] : head.headers) input.headers.emplace_back(name, value);
                const bool head_request = method == "HEAD";
                const bool http11 = head.version == "HTTP/1.1";
                bool close = !head.keep_alive || !config.keep_alive;
                const size_t start = consumed + head.size;
                const bool chunked = head.chunked;
                const size_t length = head.content_length;
                // A body Sanic would reject while reading it (Http.read): the request
                // still runs and its read raises; the connection closes afterwards.
                const char* body_error = nullptr;
                auto chunk_error = [](http1::Chunked state) {
                    return state == http1::Chunked::too_large ? "413:Request body exceeds the size limit"
                                                              : "400:Bad chunked encoding";
                };
                // What of the body has arrived. A complete body goes with the
                // request; otherwise the request goes now with this prefix and the
                // rest follows as application_data events while Python runs.
                std::string body;
                http1::ChunkedDecoder decoder;
                bool complete = false;
                const bool framework_errors = config.framework_errors;
                if (!chunked && length > config.max_body_bytes) {
                    if (!framework_errors) {
                        http1::append_error(output, 413, true, date(), probe_lines);
                        break;
                    }
                    body_error = "413:Request body exceeds the size limit";
                } else if (chunked && framework_errors) {
                    // Decoded while the handler runs, one event per chunk: a Sanic
                    // stream handler reads each chunk on its own.
                } else if (chunked) {
                    // The generic framework host hands Python complete bodies only.
                    size_t pos = start;
                    auto state = decoder.feed(buffer, pos, body, config.max_body_bytes);
                    bool continued = false;
                    while (state == http1::Chunked::incomplete) {
                        buffer.erase(start, pos - start);
                        pos = start;
                        if (head.expect_continue && http11 && !continued) {
                            output.append("HTTP/1.1 100 Continue\r\n\r\n");
                            continued = true;
                        }
                        co_await flush_output(connection, output);
                        if (!co_await read_more(true)) break;
                        state = decoder.feed(buffer, pos, body, config.max_body_bytes);
                    }
                    if (state == http1::Chunked::incomplete) break;  // peer closed or deadline
                    if (state != http1::Chunked::complete) {
                        http1::append_error(output, state == http1::Chunked::too_large ? 413 : 400,
                                            true, date(), probe_lines);
                        break;
                    }
                    complete = true;
                    buffer.erase(start, pos - start);
                } else {
                    if (!framework_errors && buffer.size() - start < length) {
                        // The generic framework host hands Python complete bodies only.
                        if (head.expect_continue && http11) output.append("HTTP/1.1 100 Continue\r\n\r\n");
                        co_await flush_output(connection, output);
                        while (buffer.size() - start < length && co_await read_more(true)) {}
                        if (buffer.size() - start < length) break;  // peer closed or deadline
                    }
                    const size_t available = std::min(length, buffer.size() - start);
                    body = buffer.substr(start, available);
                    buffer.erase(start, available);
                    complete = available == length;
                }
                consumed = start;
                request_started = ss::lowres_clock::now();  // for a pipelined successor
                if (complete) {
                    co_await respond_application(connection, method, std::move(input), std::move(body),
                                                 head_request, http11, close, output, 0);
                    keep = !close && !connection.peer_gone;
                    continue;
                }
                // Sanic stops keep-alive once a body fails to read.
                if (body_error) close = true;
                if (head.expect_continue && http11 && !body_error)
                    output.append("HTTP/1.1 100 Continue\r\n\r\n");
                co_await flush_output(connection, output);
                const uint64_t root = (++next_id << 16) | ss::this_shard_id();
                size_t delivered = body.size(), remaining = chunked || body_error ? 0 : length - body.size();
                bool body_ok = true;
                auto publish_data = [&](std::string piece, std::string parameter) {
                    RequestEvent data{(++next_id << 16) | ss::this_shard_id(), ss::this_shard_id(),
                                      method, "application_data", piece.size(), true, std::move(piece),
                                      std::move(parameter)};
                    data.parent = root;
                    counts.body_bytes_to_python += data.body.size();
                    shared.publish(std::move(data), true);
                };
                auto forward_body = [&]() -> ss::future<> {
                    if (body_error) {
                        body_ok = false;
                        publish_data({}, std::string("error:") + body_error);
                        co_return;
                    }
                    for (;;) {
                        std::string piece;
                        bool end = false;
                        bool more_here = false;  // another chunk may already be buffered
                        if (chunked) {
                            size_t pos = start;
                            const auto state = decoder.feed(buffer, pos, piece,
                                                            config.max_body_bytes - delivered,
                                                            framework_errors);
                            buffer.erase(start, pos - start);
                            if (state == http1::Chunked::bad_request || state == http1::Chunked::too_large) {
                                body_ok = false;
                                if (!framework_errors) {
                                    abort_request(root, state == http1::Chunked::too_large ? 413 : 400);
                                    co_return;
                                }
                                if (!piece.empty()) publish_data(std::move(piece), "");
                                publish_data({}, std::string("error:") + chunk_error(state));
                                close = true;  // read by the response head if not yet written
                                co_return;
                            }
                            end = state == http1::Chunked::complete;
                            more_here = state == http1::Chunked::chunk;
                        } else {
                            const size_t take = std::min(remaining, buffer.size() - start);
                            piece = buffer.substr(start, take);
                            buffer.erase(start, take);
                            remaining -= take;
                            end = remaining == 0;
                        }
                        delivered += piece.size();
                        if (!piece.empty() || end) publish_data(std::move(piece), end ? "end" : "");
                        if (end) co_return;
                        if (more_here) continue;
                        bool more = false;
                        try {
                            more = co_await read_more(true);
                        } catch (...) {}  // reset by the peer
                        if (!more) {
                            body_ok = false;
                            if (expired == Wait::request) {
                                // A response may have started: its handler's read
                                // ends with the timeout either way.
                                publish_data({}, "error:408:Request Timeout");
                                abort_request(root, 408);
                            } else {
                                // The peer closed or reset it mid-body: the handler
                                // is cancelled, also once its response started.
                                peer_left(connection, connection_key);
                                abort_request(root, 503);
                            }
                            co_return;
                        }
                    }
                };
                // Seastar coroutines run eagerly: respond_application publishes the
                // request before its first suspension, so the data events that
                // forward_body publishes afterwards always follow it.
                auto responding = respond_application(connection, method, std::move(input),
                                                      std::move(body), head_request, http11, close,
                                                      output, root);
                auto forwarding = forward_body();
                co_await std::move(responding);
                co_await std::move(forwarding);
                if (!body_ok) {
                    expired = Wait::none;  // the response already reported it
                    close = true;
                    break;
                }
                keep = !close && !connection.peer_gone;
            }
            if (expired == Wait::request) {
                // Sanic answers a request that did not arrive in time with 408.
                http1::append_error(output, 408, true, date(), probe_lines);
            }
            co_await flush_output(connection, output);
        } catch (...) { if (!closing) ++counts.errors; }
        if (sequence) {
            // Python keeps per-connection state (ConnInfo, protocol) until this,
            // however the connection ended (a reset or a failed write included).
            RequestEvent closed{(++next_id << 16) | ss::this_shard_id(), ss::this_shard_id(),
                                {}, "application_closed", 0};
            closed.parent = connection_key;
            shared.publish(std::move(closed), true);
        }
        deadline.cancel();
        if (!connection.shut_here) connection.shut_input();
        co_await std::move(watching);  // it refers to the connection
        // Closing flushes what is left: within the write deadline too.
        try {
            co_await timed_write(connection, [&] { return connection.output.close(); });
        } catch (...) {}
        try { co_await connection.input.close(); } catch (...) {}
        app_peers.erase(id);
        --counts.connections_open;
    }
    // streamed_id: nonzero when the body is still arriving; the request is
    // published under that id (application_data events refer to it).
    // parameter/error_status: a protocol error Python renders ("error:<status>:<message>");
    // the native fallback answers with that status.
    ss::future<> respond_application(AppConnection& connection, const std::string& method,
                                     ApplicationInput input, std::string body, bool head_request,
                                     bool http11, bool& close, std::string& output,
                                     uint64_t streamed_id, std::string parameter = {},
                                     int error_status = 0) {
        const auto size = body.size();
        // Admission covers the request (input twice) and the response head; response
        // bodies are bounded by max_response_bytes, not reserved up front.
        auto lease = reserve(2 * size + 8192);
        uint64_t root = 0;
        unsigned terminal_status = 500;
        bool retired = false;
        const auto connection_key = input.connection;
        auto finished = ss::defer([&] () noexcept {
            connection.handling = 0;
            if (!root || retired) return;
            RequestEvent event{(++next_id << 16) | ss::this_shard_id(), ss::this_shard_id(),
                               method, "application_done", terminal_status};
            event.parent = root;
            event.reservation = lease;
            ++counts.python_terminal_events;
            // Admitted: the framework host's in-request events may fill the queue.
            shared.publish(std::move(event), true);
        });
        auto reject = [&](int status) {
            terminal_status = status;
            http1::append_error(output, status, close, date(), probe_lines);
            ++counts.responses;
        };
        if (!lease) { reject(error_status ? error_status : 503); co_return; }
        std::optional<Decision> answer;
        try {
            if (closing) { reject(error_status ? error_status : 503); co_return; }
            // decide publishes the request (and sets root) before it waits.
            auto deciding = decide("application", method, size, &body,
                                   streamed_id ? "more" : std::move(parameter), 0, &root, &input,
                                   streamed_id, true);
            connection.handling = root;
            if (connection.peer_gone) {
                // Gone before the request reached Python (a reset while the
                // head or body arrived): it is cancelled as it starts.
                connection.peer_gone = false;
                peer_left(connection, connection_key);
            }
            answer = co_await std::move(deciding);
        } catch (...) {
            ++counts.errors;
            reject(error_status ? error_status : closing ? 503 : 500);
            co_return;
        }
        // The server is stopping: unless a pipelined request follows (served as
        // Sanic serves a busy connection), this is the connection's last response.
        if (draining && connection.buffer->size() == *connection.consumed) close = true;
        retired = answer->retire;
        const bool streamed = answer->kind == DecisionKind::stream_start;
        // Sanic closes the connection after a response timeout's error response.
        if (answer->timed_out) close = true;
        if (answer->code == 3) { reject(answer->reject_status); co_return; }
        if (!valid_http_response(answer->code, answer->headers, true, streamed) ||
            answer->body.size() > shared.config.max_response_bytes) {
            ++counts.errors;
            if (streamed) {
                // Python believes it is streaming: the terminal event cancels it.
                waiters.erase(root);
                retired = false;
            }
            reject(500);
            co_return;
        }
        terminal_status = answer->code;
        ++counts.responses;
        if (streamed) {
            co_await stream_application(connection, *answer, root, method, head_request, http11,
                                        close, output, retired);
            co_return;
        }
        const bool write_body = !head_request && http1::status_has_body(answer->code);
        counts.response_bytes += write_body ? answer->body.size() : 0;
        if (head_request && answer->content_length >= 0) {
            // Sanic keeps a HEAD response's own Content-Length (the GET size).
            http1::append_head(output, answer->code, answer->content_length, answer->headers, close,
                               date(), probe_lines);
            co_return;
        }
        if (!write_body || answer->body.size() < 16384) {
            http1::append_response(output, answer->code, answer->body, answer->headers, head_request,
                                   close, date(), probe_lines);
            co_return;
        }
        // Large bodies go to the socket as their own buffer instead of being
        // copied behind the head.
        http1::append_head(output, answer->code, answer->body.size(), answer->headers, close,
                           date(), probe_lines);
        co_await flush_output(connection, output);
        auto owned = std::make_unique<std::string>(std::move(answer->body));
        // Read the pointer and size before the deleter takes ownership: argument
        // evaluation order is unspecified.
        char* data = owned->data();
        const size_t length = owned->size();
        ss::temporary_buffer<char> whole(data, length, ss::make_object_deleter(std::move(owned)));
        // In pieces that share the body, each one write within the write
        // deadline: a slow peer that keeps reading gets all of it, one that
        // stops reading is cut off.
        constexpr size_t piece = 1 << 20;
        for (size_t at = 0; at < length; at += piece) {
            auto view = whole.share(at, std::min(piece, length - at));
            co_await timed_write(connection, [&]() { return connection.output.write(std::move(view)); });
        }
    }
    // Writes a streamed response: the start decision's head and data, then the
    // chunks Python queues on the waiter until stream_end or stream_abort.
    // Every flushed batch is acknowledged (application_written) so Python can
    // bound what it has in flight. A failed write or abort closes the connection
    // and, through respond_application's terminal event, cancels the handler.
    //
    // A 101 start switches the connection to a tunnel (WebSocket): after the
    // head, the chunks are written as raw bytes, and the peer's bytes (first any
    // the request buffer already holds) go to Python as application_data events
    // until EOF ("end"); Python pauses and resumes that reading. Without
    // timeouts, as Sanic's WebSocket connections. The connection closes after
    // the tunnel, which Python is told of with "lost".
    ss::future<> stream_application(AppConnection& connection, Decision& start, uint64_t root,
                                    const std::string& method, bool head_request, bool http11,
                                    bool& close, std::string& output, bool& retired) {
        auto found = waiters.find(root);
        if (found == waiters.end()) { close = true; retired = false; co_return; }
        auto& waiter = *found->second;
        enum class Framing { chunked, length, until_close, raw };
        const bool tunnel = start.code == 101;
        connection.tunnel = tunnel;
        const auto framing = tunnel ? Framing::raw : start.content_length >= 0 ? Framing::length :
                             http11 ? Framing::chunked : Framing::until_close;
        const bool has_body = tunnel || http1::status_has_body(start.code);
        const bool send_body = has_body && !head_request;
        if (framing == Framing::until_close || tunnel) close = true;
        if (tunnel) {
            http1::append_upgrade_head(output, start.code, start.headers);
        } else {
            std::string framing_line;
            if (has_body && framing == Framing::length)
                framing_line = fmt::format("Content-Length: {}\r\n", start.content_length);
            else if (has_body && framing == Framing::chunked)
                framing_line = "Transfer-Encoding: chunked\r\n";
            http1::append_stream_head(output, start.code, start.headers, close, date(), probe_lines,
                                      framing_line);
        }
        auto publish = [&](std::string data, const char* parameter) {
            RequestEvent event{(++next_id << 16) | ss::this_shard_id(), ss::this_shard_id(),
                               method, "application_data", data.size(), true, std::move(data), parameter};
            event.parent = root;
            counts.body_bytes_to_python += event.body.size();
            shared.publish(std::move(event), true);
        };
        bool tunnel_over = false;
        auto read_peer = [&]() -> ss::future<> {
            if (!tunnel) co_return;
            std::string early = connection.buffer->substr(*connection.consumed);
            connection.buffer->clear();
            *connection.consumed = 0;
            if (!early.empty()) publish(std::move(early), "");
            try {
                while (!tunnel_over) {
                    while (waiter.paused && !tunnel_over && !closing) {
                        waiter.resumed.emplace();
                        co_await waiter.resumed->get_future();
                        waiter.resumed.reset();
                    }
                    if (tunnel_over || closing) break;
                    auto chunk = co_await connection.input.read();
                    if (chunk.empty()) break;
                    publish(std::string(chunk.get(), chunk.size()), "");
                }
            } catch (...) {}
            if (!tunnel_over) publish({}, "end");
        };
        auto reading = read_peer();
        int64_t sent = 0;
        uint64_t unacked = 0;  // bytes Python queued and has not been told are written
        auto frame = [&](std::string_view data) {
            if (!send_body || data.empty()) return;
            if (framing == Framing::chunked) {
                output.append(fmt::format("{:x}\r\n", data.size()));
                output.append(data);
                output.append("\r\n");
            } else {
                output.append(data);
            }
            sent += static_cast<int64_t>(data.size());
        };
        auto put = [&](const std::string& data) {
            unacked += data.size();
            frame(data);
        };
        // stream_file: the file range goes out in chunk-size pieces, one per read
        // Sanic's file_stream would make; Python learns the outcome from
        // application_file (bytes written, or "error:<errno>:<message>").
        // False when the connection failed or the stream was aborted meanwhile.
        auto send_file = [&](const Decision& order) -> ss::future<bool> {
            uint64_t offset = 0, length = 0, chunk = 0;
            std::string_view spec(order.body);
            for (auto* field : {&offset, &length, &chunk}) {
                const auto space = spec.find(' ');
                std::from_chars(spec.data(), spec.data() + std::min(space, spec.size()), *field);
                spec.remove_prefix(std::min(space + 1, spec.size()));
            }
            const std::string path(spec);
            chunk = std::max<uint64_t>(chunk, 1);
            auto aborted = [&] {
                return closing || std::ranges::any_of(waiter.chunks, [](const Decision& next) {
                    return next.kind == DecisionKind::stream_abort;
                });
            };
            uint64_t written = 0;
            std::string error;
            bool connected = true;
            try {
                // As a C string the path would end at a NUL (Engine.stream_file refuses one).
                if (path.find('\0') != std::string::npos)
                    throw std::system_error(EINVAL, std::system_category(), "embedded null byte");
                auto file = co_await ss::open_file_dma(path, ss::open_flags::ro);
                ss::file_input_stream_options options;
                options.buffer_size = std::max<uint64_t>(65536, std::min<uint64_t>(chunk, 1 << 20));
                options.read_ahead = 1;
                auto input = ss::make_file_input_stream(file, offset, length, options);
                std::exception_ptr failure;
                try {
                    std::string pending;
                    while (written + pending.size() < length && !aborted()) {
                        auto data = co_await input.read();
                        if (data.empty()) break;  // the file became shorter
                        pending.append(data.get(), data.size());
                        size_t at = 0;
                        for (; pending.size() - at >= chunk; at += chunk) frame(std::string_view(pending).substr(at, chunk));
                        written += at;
                        pending.erase(0, at);
                        if (output.size() >= 65536) {
                            try {
                                co_await flush_output(connection, output);
                            } catch (...) {
                                connected = false;
                                break;
                            }
                        }
                    }
                    if (connected && !pending.empty() && !aborted()) {
                        frame(pending);
                        written += pending.size();
                    }
                } catch (...) {
                    failure = std::current_exception();
                }
                co_await input.close();  // the stream does not close the file
                co_await file.close();
                if (failure) std::rethrow_exception(failure);
            } catch (const std::system_error& e) {
                error = fmt::format("error:{}:{}", e.code().value(), e.code().message());
            } catch (const std::exception& e) {
                error = fmt::format("error:{}:{}", EIO, e.what());
            }
            RequestEvent done{(++next_id << 16) | ss::this_shard_id(), ss::this_shard_id(),
                              method, "application_file", written, false, {}, std::move(error)};
            done.parent = root;
            shared.publish(std::move(done), true);
            co_return connected && !aborted();
        };
        // Writes what is buffered and tells Python how much of what it queued
        // is written; false when the connection failed.
        auto flush_and_acknowledge = [&]() -> ss::future<bool> {
            try {
                co_await flush_output(connection, output);
            } catch (...) {
                co_return false;
            }
            if (unacked) {
                RequestEvent ack{(++next_id << 16) | ss::this_shard_id(), ss::this_shard_id(),
                                 method, "application_written", unacked};
                ack.parent = root;
                shared.publish(std::move(ack), true);
                unacked = 0;
            }
            co_return true;
        };
        put(start.body);
        bool ended = false, failed = false;
        for (;;) {
            if (!co_await flush_and_acknowledge()) {
                failed = true;
                break;
            }
            if (ended) break;
            while (waiter.chunks.empty() && !closing) {
                waiter.more.emplace();
                co_await waiter.more->get_future();
                waiter.more.reset();
            }
            if (closing) { failed = true; break; }
            while (!waiter.chunks.empty() && !ended && !failed) {
                auto chunk = std::move(waiter.chunks.front());
                waiter.chunks.pop_front();
                if (chunk.kind == DecisionKind::stream_abort) { failed = true; break; }
                if (chunk.kind == DecisionKind::stream_file) {
                    // Data queued before the file is acknowledged first: Python's
                    // window must not wait for the whole file.
                    if (tunnel || !co_await flush_and_acknowledge() || !co_await send_file(chunk))
                        failed = true;
                    continue;
                }
                put(chunk.body);
                ended = chunk.kind == DecisionKind::stream_end;
            }
            if (failed) break;
        }
        if (tunnel) {
            // Stop reading the peer: the input shutdown ends a pending read.
            tunnel_over = true;
            connection.shut_input();
            if (waiter.resumed) waiter.resumed->set_value();
        }
        co_await std::move(reading);
        if (tunnel) publish({}, "lost");
        waiters.erase(root);
        counts.response_bytes += sent;
        if (failed) {
            close = true;
            retired = false;
            co_return;
        }
        if (send_body && framing == Framing::chunked) output.append("0\r\n\r\n");
        if (send_body && framing == Framing::length && sent != start.content_length) close = true;
    }
    ss::future<> echo_datagrams() {
        std::array<char, 65536> buffer;
        try {
            while (!closing) {
                ss::socket_address source;
                iovec vec{buffer.data(), buffer.size()};
                msghdr message{};
                message.msg_name = &source.u.sa;
                message.msg_namelen = sizeof(source.u);
                message.msg_iov = &vec;
                message.msg_iovlen = 1;
                auto size = co_await udp->recvmsg(&message);
                source.addr_length = message.msg_namelen;
                ++counts.udp_received;
                if ((message.msg_flags & MSG_TRUNC) || size > shared.config.max_body_bytes) {
                    ++counts.udp_dropped; continue;
                }
                auto lease = reserve(size);
                if (!lease) { ++counts.udp_dropped; continue; }
                auto sent = co_await udp->sendto(source, buffer.data(), size);
                if (sent != size) { ++counts.udp_dropped; continue; }
                ++counts.udp_sent;
                counts.response_bytes += size;
            }
        } catch (...) { if (!closing) ++counts.errors; }
    }

    std::shared_ptr<Lease> reserve(size_t bytes) {
        if (closing || counts.active_requests >= counts.limit_count ||
            bytes > counts.limit_bytes - counts.reserved_bytes.load()) {
            ++counts.rejected;
            return {};
        }
        auto lease = std::make_shared<Lease>(counts, bytes);
        ++counts.active_requests; counts.reserved_bytes += bytes; ++counts.accepted;
        counts.peak_requests = std::max(counts.peak_requests.load(), counts.active_requests.load());
        counts.peak_bytes = std::max(counts.peak_bytes.load(), counts.reserved_bytes.load());
        return lease;
    }
    ss::future<Decision> decide(const std::string& route, const std::string& method,
                                size_t bytes, const std::string* copied,
                                std::string parameter = {}, uint64_t parent = 0, uint64_t* published_id = nullptr,
                                ApplicationInput* input = nullptr, uint64_t preassigned_id = 0,
                                bool notify_timeout = false) {
        if (closing) co_return Decision{503, {}};
        uint64_t id = preassigned_id ? preassigned_id : (++next_id << 16) | ss::this_shard_id();
        if (published_id) *published_id = id;
        auto waiter = std::make_unique<Waiter>();
        auto future = waiter->promise.get_future();
        waiters.emplace(id, std::move(waiter));
        ++counts.pending_decisions; ++counts.decisions;
        auto begin = clock_ns();
        std::exception_ptr failure;
        std::optional<Decision> result;
        try {
            RequestEvent event{id, ss::this_shard_id(), method, route, bytes, copied != nullptr, {}};
            event.parameter = std::move(parameter);
            event.parent = parent;
            if (input) {
                event.target = std::move(input->target);
                event.http_version = std::move(input->http_version);
                event.client = std::move(input->client);
                event.server = std::move(input->server);
                event.headers = std::move(input->headers);
                event.connection = input->connection;
                event.sequence = input->sequence;
                event.unix_socket = input->unix_socket;
            }
            if (copied) { event.body = *copied; counts.body_bytes_to_python += copied->size(); }
            shared.publish(std::move(event));
            const auto deadline = ss::lowres_clock::now() +
                std::chrono::milliseconds(shared.config.decision_timeout_ms);
            if (!notify_timeout) {
                result = co_await ss::with_timeout(deadline, std::move(future));
            } else {
                // The response deadline: Python is told and cancels the handler,
                // which may still answer (Sanic: 503 Response Timeout through its
                // error handler). Without an answer within the grace period the
                // host answers 504 itself.
                bool reported = false;
                ss::timer<ss::lowres_clock> timer;
                timer.set_callback([&, id] {
                    if (reported) {
                        abort_request(id, 504);
                        return;
                    }
                    ++counts.decision_timeouts;
                    if (shared.withdraw(id)) {
                        abort_request(id, 504);  // Python never received it
                        return;
                    }
                    reported = true;
                    RequestEvent timeout{(++next_id << 16) | ss::this_shard_id(), ss::this_shard_id(),
                                         method, "application_timeout", 0};
                    timeout.parent = id;
                    shared.publish(std::move(timeout), true);
                    timer.arm(ss::lowres_clock::now() + std::chrono::seconds(2));
                });
                timer.arm(deadline);
                result = co_await std::move(future);
                result->timed_out = reported;
            }
        } catch (const ss::timed_out_error&) {
            ++counts.decision_timeouts;
            failure = std::current_exception();
        } catch (...) { failure = std::current_exception(); }
        counts.decision_wait_ns += clock_ns() - begin;
        shared.erase(id);
        // A streamed response keeps its waiter for later chunks; respond_application
        // erases it when the stream ends.
        if (failure || !result || result->kind != DecisionKind::stream_start)
            waiters.erase(id); // destroying a timed-out promise occurs on its own shard
        --counts.pending_decisions;
        if (failure) std::rethrow_exception(failure);
        result->id = id;
        co_return std::move(*result);
    }
    static ss::future<> write_payload(ss::output_stream<char>& out, std::shared_ptr<std::string> body,
                                      std::shared_ptr<Lease> lease) {
        // Forward an owning view; retain both storage and byte reservation until
        // the output stream releases its final reference (including disconnect).
        ss::temporary_buffer<char> view(body->data(), body->size(),
            ss::make_deleter([body, lease] {}));
        co_await out.write(std::move(view));
        lease->counts.response_bytes += body->size();
        ++lease->counts.responses;
    }
    static ss::future<> write_file(ss::output_stream<char>& out, std::string path,
                                   std::shared_ptr<Lease> lease, size_t size) {
        auto file = co_await ss::open_file_dma(path, ss::open_flags::ro);
        ss::file_input_stream_options options;
        options.buffer_size = 65536;
        options.read_ahead = 1;
        auto input = ss::make_file_input_stream(std::move(file), options);
        co_await ss::copy(input, out).finally([&input] { return input.close(); });
        lease->counts.response_bytes += size;
        ++lease->counts.responses;
    }
    static std::unique_ptr<ss::http::reply> error(std::unique_ptr<ss::http::reply> reply, int status) {
        reply->set_status(static_cast<ss::http::reply::status_type>(status));
        reply->write_body("txt", ss::sstring("request rejected\n"));
        return reply;
    }
    ss::future<std::unique_ptr<ss::http::reply>> handle_application(
        std::unique_ptr<ss::http::request> req, std::unique_ptr<ss::http::reply> rep) {
        const std::string method(req->_method);
        const std::string target(req->_url);
        const auto path = target.substr(0, target.find('?'));
        const bool native = shared.config.native_json && path == "/json" &&
                            (method == "GET" || method == "HEAD");
        if (!native && !shared.config.application_dispatch_all &&
            !shared.application_routes.contains({method, path}))
            co_return error(std::move(rep), 404);
        if (req->_headers.contains("Transfer-Encoding")) co_return error(std::move(rep), 400);
        // The pinned parser uses strtol. Check its entire Content-Length value,
        // including sign/trailing junk, before trusting that framing for Python.
        const auto length = req->get_header("Content-Length");
        size_t size = 0;
        if (!length.empty()) {
            const auto parsed = std::from_chars(length.data(), length.data() + length.size(), size);
            if (parsed.ec != std::errc() || parsed.ptr != length.data() + length.size() ||
                size != req->content_length) co_return error(std::move(rep), 400);
        } else if (req->_headers.contains("Content-Length")) {
            co_return error(std::move(rep), 400);
        }
        if (size > shared.config.max_body_bytes) co_return error(std::move(rep), 413);
        // Input exists both locally and in the owned event. Error payloads also
        // fit the output reservation when the configured handler bound is tiny.
        const auto body_budget = std::max(shared.config.max_response_bytes, size_t(17));
        if (body_budget > std::numeric_limits<size_t>::max() - 8192)
            co_return error(std::move(rep), 503);
        const auto output_budget = body_budget + 8192;
        if (size > (std::numeric_limits<size_t>::max() - output_budget) / 2)
            co_return error(std::move(rep), 413);
        auto lease = reserve(2 * size + output_budget);
        if (!lease) co_return error(std::move(rep), 503);
        uint64_t root = 0;
        unsigned terminal_status = 500;
        bool retired = false;
        auto finished = ss::defer([&] () noexcept {
            if (!root || retired) return;
            RequestEvent event{(++next_id << 16) | ss::this_shard_id(), ss::this_shard_id(),
                               method, "application_done", terminal_status};
            event.parent = root;
            event.reservation = lease;
            ++counts.python_terminal_events;
            shared.publish(std::move(event));
        });
        auto respond = [&](int status, std::string payload,
                           const std::vector<std::pair<std::string, std::string>>& headers = {}) {
            terminal_status = status;
            rep->set_status(static_cast<ss::http::reply::status_type>(status));
            for (const auto& [name, value] : headers)
                rep->add_header(ss::sstring(name), ss::sstring(value));
            auto body = std::make_shared<std::string>(std::move(payload));
            rep->write_body(std::nullopt, [body, lease](ss::output_stream<char>& out) {
                return write_payload(out, body, lease);
            });
            // Seastar retains the writer but deliberately skips calling it
            // for HEAD. Its captured lease lives until reply destruction.
            if (method == "HEAD") ++counts.responses;
            return std::move(rep);
        };
        auto reject = [&](int status) { return respond(status, "request rejected\n"); };
        bool input_complete = false;
        try {
            std::string input;
            input.reserve(size);
            for (;;) {
                auto chunk = co_await req->content_stream->read();
                if (chunk.empty()) break;
                if (chunk.size() > size - input.size()) co_return reject(400);
                input.append(chunk.get(), chunk.size());
            }
            if (input.size() != size) co_return reject(400);
            input_complete = true;
            if (closing) co_return reject(503);
            Decision answer{200, {}};
            if (native) {
                // Fresh object and actual JSON serialization on every request.
                const std::map<std::string, std::string> object{{"message", "Hello, World!"}};
                answer.body = std::string(ss::json::formatter::to_json(object));
                answer.headers.emplace_back("Content-Type", "application/json");
            } else {
                auto fields = application_input(*req);
                answer = co_await decide("application", method, size, &input, {}, 0, &root, &fields);
                retired = answer.retire;
                if (answer.code == 3) co_return reject(503);
            }
            if (!valid_http_response(answer.code, answer.headers) ||
                answer.body.size() > shared.config.max_response_bytes) {
                ++counts.errors;
                co_return reject(500);
            }
            co_return respond(answer.code, std::move(answer.body), answer.headers);
        } catch (const ss::timed_out_error&) {
            co_return reject(504);
        } catch (...) {
            ++counts.errors;
            co_return reject(closing ? 503 : input_complete ? 500 : 400);
        }
    }
public:
    Service(Shared* state, NetworkServer::Compress fn)
        : shared(*state), counts(*state->counts[ss::this_shard_id()]), compress(std::move(fn)) {}
    ss::future<> start() {
        http.set_content_length_limit(shared.config.max_body_bytes);
        http.set_request_size_limit(8192);
        http.set_content_streaming(true);
        http.set_generate_date_header(shared.config.generate_date_header);
        using ss::httpd::operation_type;
        for (auto port : shared.config.upstream_ports) {
            auto factory = std::make_unique<ss::http::basic_connection_factory>(
                ss::socket_address(ss::net::inet_address("127.0.0.1"), uint16_t(port)));
            upstreams.push_back(std::make_unique<ss::http::client>(
                std::move(factory), std::max(size_t(1), counts.limit_count)));
        }
        if (!shared.config.application_dispatch_all) {
            for (const auto* mode : {"python", "reference"})
                http._routes.add(operation_type::GET, ss::httpd::url(ss::sstring("/") + mode + "/fetch"),
                                 new Handler(*this, mode, "fetch"));
            for (const auto* mode : {"native", "hybrid", "copy", "policy", "python", "reference"}) {
                for (const auto* route : {"fixed", "compress", "file"}) {
                    if ((std::string(mode) == "copy" || std::string(mode) == "python" ||
                         std::string(mode) == "reference") && std::string(route) != "compress") continue;
                    auto method = std::string(route) == "compress" ? operation_type::POST : operation_type::GET;
                    http._routes.add(method, ss::httpd::url(ss::sstring("/") + mode + "/" + route), new Handler(*this, mode, route));
                }
            }
        }
        if (!shared.application_routes.empty() || shared.config.native_json) {
            application_handler = std::make_unique<Handler>(*this, "application", "application");
            http._routes.add_default_handler(application_handler.get());
        }
        if (shared.config.application_dispatch_all) {
            const auto pid = ss::to_sstring(::getpid());
            if (shared.config.probe_headers == "all") {
                probe_lines = "X-Probe-Pid: " + std::string(pid) + "\r\nX-Probe-Shard: " +
                              std::to_string(ss::this_shard_id()) + "\r\nX-Probe-Policy: 0\r\n";
            } else if (shared.config.probe_headers == "pid") {
                probe_lines = "X-Probe-Pid: " + std::string(pid) + "\r\n";
            }
            if (shared.config.listen_fd >= 0 && shared.config.listen_shared) {
                // Every shard, shard 0 included, takes the connections accept_shared hands it.
                const auto [protocol, address] = listener_address(shared.config.listen_fd);
                app_listener.emplace(std::make_unique<ss::net::posix_ap_server_socket_impl>(protocol, address));
                const auto slot = shared.config.accept_slot;
                if (shared.config.accept_counts && slot >= 0 && unsigned(slot) < shared.config.accept_slots) {
                    accept_slots = reinterpret_cast<AcceptSlot*>(uintptr_t(shared.config.accept_counts));
                    own_slot = &accept_slots[slot];
                    own_pid = uint64_t(::getpid());
                    if (ss::this_shard_id() == 0) {
                        // A restarted worker takes over its predecessor's line.
                        own_slot->connections.store(own_pid << 32, std::memory_order_relaxed);
                        own_slot->heartbeat_ms.store(monotonic_ms(), std::memory_order_release);
                        accept_heartbeat.set_callback([this] {
                            if (owns_slot()) own_slot->heartbeat_ms.store(monotonic_ms(), std::memory_order_release);
                        });
                        accept_heartbeat.arm_periodic(std::chrono::milliseconds(50));
                    }
                }
                if (ss::this_shard_id() == 0) {
                    shared_wake = ss::file_desc::eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
                    shared_accepting = accept_shared(protocol, address);
                }
                app_accepting = accept_application(*app_listener, shared.config.unix_path);
            } else if (shared.config.listen_fd >= 0) {
                app_listener.emplace(adopt_listener(shared.config.listen_fd));
                app_accepting = accept_application(*app_listener, shared.config.unix_path);
            } else if (shared.config.http_port) {
                ss::listen_options options;
                options.reuse_address = true;
                options.lba = ss::server_socket::load_balancing_algorithm::connection_distribution;
                app_listener.emplace(ss::listen(ss::socket_address(ss::net::inet_address(shared.config.http_host),
                                                                   uint16_t(shared.config.http_port)), options));
                app_accepting = accept_application(*app_listener, shared.config.unix_path);
            }
            if (shared.config.worker_listen_fd >= 0) {
                worker_listener.emplace(adopt_listener(shared.config.worker_listen_fd));
                worker_accepting = accept_application(*worker_listener, shared.config.worker_unix_path);
            }
        } else if (shared.config.http_port) {
            ss::listen_options options;
            options.reuse_address = true;
            options.lba = ss::server_socket::load_balancing_algorithm::connection_distribution;
            // Use the OS default/autotuned HTTP socket send buffer, like aiohttp.
            co_await http.listen(ss::socket_address(ss::net::inet_address("127.0.0.1"), uint16_t(shared.config.http_port)), options);
        }
        if (shared.config.tcp_port) {
            ss::listen_options options;
            options.reuse_address = true;
            options.lba = ss::server_socket::load_balancing_algorithm::connection_distribution;
            options.so_sndbuf = 65536;
            tcp.emplace(ss::listen(ss::socket_address(ss::net::inet_address("127.0.0.1"), uint16_t(shared.config.tcp_port)), options));
            accepting = accept_tcp();
        }
        if (shared.config.udp_port) {
            auto fd = ss::file_desc::socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
            fd.setsockopt(SOL_SOCKET, SO_REUSEPORT, 1);
            ss::socket_address address(ss::net::inet_address("127.0.0.1"), uint16_t(shared.config.udp_port));
            fd.bind(address.u.sa, address.addr_length);
            udp.emplace(std::move(fd));
            datagrams = echo_datagrams();
        }
    }
    // The request body can no longer arrive (peer closed, deadline, bad framing):
    // stop waiting for Python; respond_application then answers 503 and its
    // terminal event cancels the handler.
    void abort_request(uint64_t id, int status) {
        auto it = waiters.find(id);
        if (it == waiters.end() || it->second->replied) return;
        it->second->replied = true;
        Decision decision{3, {}};
        decision.reject_status = status;
        it->second->promise.set_value(std::move(decision));
    }
    void resolve(uint64_t id, int code, std::string body, bool retire = false,
                 std::vector<std::pair<std::string, std::string>> headers = {},
                 int kind = DecisionKind::complete, int64_t content_length = -1) {
        auto it = waiters.find(id);
        if (it == waiters.end() || closing) return;
        auto& waiter = *it->second;
        counts.body_bytes_from_python += body.size();
        if (kind == DecisionKind::tunnel_pause || kind == DecisionKind::tunnel_resume) {
            waiter.paused = kind == DecisionKind::tunnel_pause;
            if (!waiter.paused && waiter.resumed) waiter.resumed->set_value();
            return;
        }
        if (kind >= DecisionKind::stream_data) {
            // Continuation of a streamed response that respond_application writes.
            if (!waiter.replied) return;
            waiter.chunks.push_back(Decision{code, std::move(body), 0, retire, {}, kind});
            if (waiter.more) waiter.more->set_value();
            return;
        }
        if (waiter.replied) return;
        waiter.replied = true;
        waiter.promise.set_value(Decision{code, std::move(body), 0, retire, std::move(headers),
                                          kind, content_length});
    }
    void install_policy(NativePolicy next) noexcept { policy = next; }
    // The framework host's listeners accept no more connections (again: a
    // no-op). A listener of this process's own is shut down, as Sanic's
    // server.close(): the kernel refuses new connections or, with
    // SO_REUSEPORT, gives them to the other processes' sockets. On a socket
    // shared with other processes, this process leaves accepting to them.
    void stop_accepting() {
        accept_pause.request_abort();
        if (app_listener) app_listener->abort_accept();  // on a shared socket, this shard's queue only
        if (worker_listener) worker_listener->abort_accept();
        if (shared_wake) {
            const uint64_t one = 1;
            (void)!::write(shared_wake->get(), &one, sizeof(one));  // ends accept_shared
        }
        if (own_slot && ss::this_shard_id() == 0) {
            // The siblings stop waiting for this process at once.
            accept_heartbeat.cancel();
            if (owns_slot()) own_slot->heartbeat_ms.store(0, std::memory_order_release);
        }
    }
    // A graceful stop's first step (GRACEFUL_SHUTDOWN_TIMEOUT), as Sanic's
    // server.close() and close_if_idle(): no new connections; idle ones close
    // now, the others after their response (sent with Connection: close).
    // Python waits for those to end, then stop() cuts off what is left.
    void drain() {
        if (!accepts()) return;
        draining = true;
        stop_accepting();
        for (auto& [id, peer] : app_peers)
            if (peer->idle) peer->shut_input();
    }
    ss::future<> stop() {
        closing = true;
        auto active = std::vector<ss::abort_source*>(upstream_active.begin(), upstream_active.end());
        for (auto* operation : active) operation->request_abort();
        if (tcp) tcp->abort_accept();
        stop_accepting();
        if (udp) {
            udp->shutdown(SHUT_RD, ss::pollable_fd::shutdown_kernel_only::no);
            udp->shutdown(SHUT_WR, ss::pollable_fd::shutdown_kernel_only::no);
        }
        for (auto& [id, peer] : peers) {
            peer->socket.shutdown_input(); peer->socket.shutdown_output();
        }
        for (auto& [id, peer] : app_peers) {
            peer->shut_input(); peer->socket.shutdown_output();
        }
        for (auto& [id, waiter] : waiters) {
            if (!waiter->replied) {
                waiter->replied = true;
                waiter->promise.set_value(Decision{3, {}});
            } else {
                // A streamed response or tunnel sees `closing`.
                if (waiter->more) waiter->more->set_value();
                if (waiter->resumed) waiter->resumed->set_value();
            }
        }
        co_await http.stop();
        for (auto& upstream : upstreams) co_await upstream->close();
        co_await std::move(accepting);
        co_await std::move(app_accepting);
        co_await std::move(shared_accepting);
        co_await std::move(worker_accepting);
        co_await connections.close();
        co_await app_connections.close();
        co_await std::move(datagrams);
        udp.reset();
    }
    ss::future<std::unique_ptr<ss::http::reply>> handle(std::string mode, std::string route,
        std::unique_ptr<ss::http::request> req, std::unique_ptr<ss::http::reply> rep) {
        const auto applied = policy; // Request keeps its admission version across awaits.
        rep->add_header("X-Probe-Pid", ss::to_sstring(::getpid()));
        rep->add_header("X-Probe-Shard", ss::to_sstring(ss::this_shard_id()));
        rep->add_header("X-Probe-Policy", ss::to_sstring(mode == "policy" ? applied.version : 0));
        rep->set_content_type("application/octet-stream");
        if (mode == "application") co_return co_await handle_application(std::move(req), std::move(rep));
        if (route == "fetch") co_return co_await handle_fetch(std::move(mode), std::move(req), std::move(rep));
        size_t size = 0;
        if (route == "fixed") {
            auto query = req->get_query_param("size");
            if (query.empty()) query = "1024";
            auto parsed = std::from_chars(query.data(), query.data()+query.size(), size);
            if (parsed.ec != std::errc() || parsed.ptr != query.data()+query.size()) co_return error(std::move(rep), 400);
        } else if (route == "compress") {
            size = req->content_length;
            // Pinned HTTP streaming API does not expose terminal-chunk validity.
            // Explicitly require a known body length instead of accepting a
            // truncated chunked request as a complete operation.
            if (req->get_header("Transfer-Encoding") != "" || req->get_header("Content-Length") == "")
                co_return error(std::move(rep), 411);
        } else {
            if (shared.config.file_path.empty()) co_return error(std::move(rep), 404);
            size = shared.file_size;
        }
        if (route != "file" && size > shared.config.max_body_bytes) co_return error(std::move(rep), 413);
        if (mode == "policy") {
            ++counts.native_policy_decisions;
            bool allowed = (route == "fixed" && applied.allow_fixed) ||
                           (route == "file" && applied.allow_file) ||
                           (route == "compress" && applied.allow_compress);
            if (!allowed || size > applied.max_payload_bytes) {
                ++counts.native_policy_denied;
                co_return error(std::move(rep), allowed ? 413 : 403);
            }
        }
        size_t bytes = route == "compress" ? compression_reservation(size) : size;
        if (mode == "copy") bytes += 2 * size;
        if (route == "file") bytes = 2 * 65536;
        auto lease = reserve(bytes);
        if (!lease) co_return error(std::move(rep), 503);
        uint64_t python_request = 0;
        unsigned terminal_status = 0;
        bool retired = false;
        auto finished = ss::defer([this, &python_request, &terminal_status, &retired, lease] () noexcept {
            if (!python_request || retired) return;
            RequestEvent event{(++next_id << 16) | ss::this_shard_id(), ss::this_shard_id(),
                "POST", "python_done", terminal_status};
            event.parent = python_request;
            event.reservation = lease;
            ++counts.python_terminal_events;
            // At most one event per admitted request; reservation remains live
            // until this event is drained, including on error and timeout paths.
            shared.publish(std::move(event));
        });
        try {
            int level = 6;
            if (mode == "python") {
                auto answer = co_await decide("python_compress", std::string(req->_method), size,
                    nullptr, std::string(req->get_query_param("level")), 0, &python_request);
                if (answer.code < 100 || answer.code > 109) {
                    retired = answer.retire;
                    co_return error(std::move(rep), answer.code >= 400 && answer.code <= 599 ? answer.code : 503);
                }
                level = answer.code - 100;
                python_request = answer.id;
            } else if (mode == "reference") {
                auto query = req->get_query_param("level");
                if (query.empty()) query = "6";
                auto parsed = std::from_chars(query.data(), query.data()+query.size(), level);
                if (parsed.ec != std::errc() || parsed.ptr != query.data()+query.size() || level < 0 || level > 9)
                    co_return error(std::move(rep), 400);
            }
            std::string input;
            if (route == "compress") {
                input.reserve(size);
                for (;;) {
                    auto buffer = co_await req->content_stream->read();
                    if (buffer.empty()) break;
                    if (buffer.size() > shared.config.max_body_bytes - input.size()) {
                        terminal_status = 413;
                        co_return error(std::move(rep), 413);
                    }
                    input.append(buffer.get(), buffer.size());
                }
                if (input.size() != req->content_length) {
                    terminal_status = 400;
                    co_return error(std::move(rep), 400);
                }
                size = input.size();
            }
            if (mode == "hybrid" || mode == "copy") {
                auto answer = co_await decide(route, std::string(req->_method), size, mode == "copy" ? &input : nullptr);
                if (answer.code != 1) co_return error(std::move(rep), answer.code == 0 ? 403 : answer.code == 3 ? 503 : 500);
                if (mode == "copy") {
                    if (answer.body.size() != input.size()) co_return error(std::move(rep), 500);
                    input = std::move(answer.body);
                }
            }
            if (route == "file") {
                rep->write_body(std::nullopt, [path=shared.config.file_path, lease, size](ss::output_stream<char>& out) {
                    return write_file(out, path, lease, size);
                });
            } else {
                auto body = std::make_shared<std::string>();
                if (route == "compress") {
                    // Bound active codec state per reactor; yield between chunks
                    // for I/O and messages without cycling through many zlib states.
                    WorkResult result;
                    if (shared.config.cooperative_compression) {
                        auto units = co_await ss::get_units(compression_slot, 1);
                        result = co_await compress(std::move(input), level);
                    } else {
                        result = co_await compress(std::move(input), level);
                    }
                    if (!result.error.empty()) throw std::runtime_error(result.error);
                    counts.compute_ns += result.metrics.algorithm_ns;
                    counts.compute_cpu_ns += result.metrics.compute_cpu_ns;
                    counts.worker_queue_ns += result.metrics.queue_ns;
                    rep->add_header("X-Probe-Worker", ss::to_sstring(result.metrics.work_tid));
                    *body = std::move(result.payload);
                    if (mode == "python") {
                        auto answer = co_await decide("python_result", "POST", body->size(), nullptr, {}, python_request);
                        retired = answer.retire;
                        if (answer.code != 200)
                            co_return error(std::move(rep), answer.code >= 400 && answer.code <= 599 ? answer.code : 503);
                        if (answer.body.size() > 128 || answer.body.find_first_of("\r\n") != std::string::npos)
                            co_return error(std::move(rep), 500);
                        rep->add_header("X-App-Result", ss::sstring(answer.body));
                    } else if (mode == "reference") {
                        if (body->size() > compression_reservation(size) - size)
                            co_return error(std::move(rep), 422);
                        rep->add_header("X-App-Result", ss::to_sstring(body->size()));
                    }
                } else body->assign(size, 'x');
                rep->write_body(std::nullopt, [body, lease](ss::output_stream<char>& out) {
                    return write_payload(out, body, lease);
                });
            }
            co_return std::move(rep);
        } catch (const ss::timed_out_error&) {
            terminal_status = 504;
            co_return error(std::move(rep), 504);
        } catch (const WorkerQueueFull&) {
            terminal_status = 503;
            ++counts.rejected;
            co_return error(std::move(rep), 503);
        } catch (...) {
            terminal_status = 500;
            ++counts.errors;
            co_return error(std::move(rep), 500);
        }
    }
};
ss::future<std::unique_ptr<ss::http::reply>> Handler::handle(const ss::sstring&,
    std::unique_ptr<ss::http::request> req, std::unique_ptr<ss::http::reply> rep) {
    return service.handle(mode, route, std::move(req), std::move(rep));
}
}

struct NetworkServer::Impl {
    Shared shared;
    ss::sharded<Service> services;
    ss::alien::instance& alien;
    Compress compress;
    bool started = false;
    std::optional<ss::shared_future<>> stopping;  // stop() may be called again
    ss::semaphore policy_updates{1}; // shard 0; serialize even canceled Python calls
    Impl(ServerConfig c, unsigned n, ss::alien::instance& a, std::function<void()> wake, Compress fn)
        : shared(std::move(c), n, std::move(wake)), alien(a), compress(std::move(fn)) {}
};
ss::future<> NetworkServer::install_policy(NativePolicy policy) {
    return ss::with_semaphore(p->policy_updates, 1, [this, policy] {
        return p->services.invoke_on_all(&Service::install_policy, policy);
    });
}
NetworkServer::NetworkServer(ServerConfig c, unsigned n, ss::alien::instance& a,
                              std::function<void()> wake, Compress compress)
    : p(std::make_unique<Impl>(std::move(c), n, a, std::move(wake), std::move(compress))) {}
NetworkServer::~NetworkServer() = default;
ss::future<> NetworkServer::start() {
    co_await p->services.start(&p->shared, p->compress);
    p->started = true;
    p->shared.running = true;
    co_await p->services.invoke_on_all(&Service::start);
}
ss::future<> NetworkServer::stop() {
    // Later and concurrent calls wait for the first: the services stop once.
    if (!p->stopping) {
        p->shared.running = false;
        p->stopping.emplace([](Impl& impl) -> ss::future<> {
            if (impl.started) {
                co_await impl.services.stop();
                impl.started = false;
            }
        }(*p));
    }
    return p->stopping->get_future();
}
ss::future<> NetworkServer::drain() {
    if (!p->started || p->stopping) return ss::make_ready_future<>();
    return p->services.invoke_on_all(&Service::drain);
}
std::vector<RequestEvent> NetworkServer::drain(size_t count) {
    std::vector<RequestEvent> result;
    std::lock_guard lock(p->shared.mutex);
    result.reserve(std::min(count, p->shared.events.size()));
    while (!p->shared.events.empty() && result.size() < count) {
        auto it = p->shared.events.begin();
        const bool application = it->second.route == "application";
        result.push_back(std::move(it->second));
        if (application) {
            p->shared.pending_application_nodes.insert(p->shared.events.extract(it));
        } else {
            // DONE has no reply and must remain drainable after reactor exit.
            p->shared.events.erase(it);
        }
    }
    return result;
}
bool NetworkServer::has_events() const {
    std::lock_guard lock(p->shared.mutex);
    return !p->shared.events.empty();
}
void NetworkServer::resolve(uint64_t id, unsigned shard, int code, std::string body, bool retire) {
    if (shard >= p->shared.shard_count || !p->shared.running) return;
    ss::alien::run_on(p->alien, shard, [impl=p.get(), id, code, body=std::move(body), retire]() mutable noexcept {
        if (impl->shared.running) impl->services.local().resolve(id, code, std::move(body), retire);
    });
}
void NetworkServer::resolve_batch(std::vector<RequestDecision> decisions) {
    if (decisions.empty() || !p->shared.running) return;
    std::vector<std::vector<RequestDecision>> groups(p->shared.shard_count);
    for (auto& decision : decisions) {
        if (decision.shard < groups.size()) groups[decision.shard].push_back(std::move(decision));
    }
    p->shared.decision_return_batches.fetch_add(1, std::memory_order_relaxed);
    for (unsigned shard = 0; shard < groups.size(); ++shard) {
        if (groups[shard].empty()) continue;
        auto count = groups[shard].size();
        ss::alien::run_on(p->alien, shard, [impl=p.get(), batch=std::move(groups[shard])]() mutable noexcept {
            if (!impl->shared.running) return;
            for (auto& decision : batch) {
                impl->services.local().resolve(decision.id, decision.code, std::move(decision.body),
                                               decision.retire, std::move(decision.headers),
                                               decision.kind, decision.content_length);
            }
        });
        p->shared.decision_return_items.fetch_add(count, std::memory_order_relaxed);
        p->shared.decision_return_posts.fetch_add(1, std::memory_order_relaxed);
    }
}
ServerSnapshot NetworkServer::snapshot() const {
    ServerSnapshot out;
    for (unsigned i=0; i<p->shared.shard_count; ++i) {
        auto counts = p->shared.counts[i]->snapshot();
        for (const auto& [key, value] : counts) out.totals[key] += value;
        counts["shard"] = i;
        out.shards.push_back(std::move(counts));
    }
    out.totals["decision_return_batches"] = p->shared.decision_return_batches.load(std::memory_order_relaxed);
    out.totals["decision_return_items"] = p->shared.decision_return_items.load(std::memory_order_relaxed);
    out.totals["decision_return_posts"] = p->shared.decision_return_posts.load(std::memory_order_relaxed);
    std::lock_guard lock(p->shared.mutex);
    out.totals["queued_decisions"] = p->shared.events.size();
    out.totals["pending_application_nodes"] = p->shared.pending_application_nodes.size();
    return out;
}
}
