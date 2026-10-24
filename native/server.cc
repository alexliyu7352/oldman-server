#include "server.hh"
#include "http1.hh"
#include <seastar/core/coroutine.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/timer.hh>
#include <seastar/core/abort_source.hh>
#include <seastar/core/with_timeout.hh>
#include <seastar/util/defer.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/net/posix-stack.hh>
#include <seastar/net/unix_address.hh>
#include <algorithm>
#include <atomic>
#include <charconv>
#include <mutex>
#include <optional>
#include <limits>
#include <deque>
#include <time.h>
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
    // Without no_body_statuses, 204/304 and the framing and diagnostic fields
    // are refused. The HTTP/1.1 application host (no_body_statuses) writes
    // them correctly and sends the application's fields as Sanic does: any
    // name, with its own Content-Length / Transfer-Encoding / Connection in
    // place of the application's (http1::append_head); only the syntax is
    // checked. 101 (a protocol switch) only starts a framework host stream
    // (upgrade): as the status of a complete response it would claim a switch
    // that never happens.
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
uint64_t clock_ns() {
    timespec t{};
    ::clock_gettime(CLOCK_MONOTONIC, &t);
    return uint64_t(t.tv_sec) * 1000000000 + t.tv_nsec;
}
struct alignas(64) Counts {
    std::atomic<uint64_t> active_requests{0}, reserved_bytes{0}, peak_requests{0}, peak_bytes{0};
    std::atomic<uint64_t> accepted{0}, rejected{0}, responses{0}, errors{0}, pending_decisions{0};
    std::atomic<uint64_t> decisions{0}, decision_timeouts{0}, decision_wait_ns{0};
    std::atomic<uint64_t> python_terminal_events{0};
    std::atomic<uint64_t> body_bytes_to_python{0}, body_bytes_from_python{0}, response_bytes{0};
    std::atomic<uint64_t> connections_open{0};  // framework host connections
    size_t limit_count = 0, limit_bytes = 0;
    std::map<std::string,uint64_t> snapshot() const {
        std::map<std::string,uint64_t> d;
#define FIELD(x) d[#x] = x.load(std::memory_order_relaxed)
        FIELD(active_requests); FIELD(reserved_bytes); FIELD(peak_requests); FIELD(peak_bytes);
        FIELD(accepted); FIELD(rejected); FIELD(responses); FIELD(errors); FIELD(pending_decisions);
        FIELD(decisions); FIELD(decision_timeouts); FIELD(decision_wait_ns);
        FIELD(python_terminal_events);
        FIELD(body_bytes_to_python); FIELD(body_bytes_from_python); FIELD(response_bytes);
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
// The queue to Python holds max_requests new requests (Shared::publish).
struct QueueFull : std::runtime_error {
    QueueFull() : std::runtime_error("decision queue full") {}
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
    Shared(ServerConfig c, unsigned n, std::function<void()> wake)
        : config(std::move(c)), shard_count(n), notify(std::move(wake)) {
        for (unsigned i=0; i<n; ++i) {
            auto x = std::make_unique<Counts>();
            x->limit_count = config.max_requests / n + (i < config.max_requests % n);
            x->limit_bytes = config.max_bytes / n + (i < config.max_bytes % n);
            counts.push_back(std::move(x));
        }
    }
    // admitted: an event of a request Python already received (body data, write
    // acknowledgements, its deadline, a connection's close). Its memory is
    // bounded by that request's limits, so it does not take a queue slot.
    // New requests in `events`: the queue's limit counts these only, not the
    // events of requests Python already has (body data, acknowledgements,
    // closes), which their own limits bound.
    size_t queued_requests = 0;  // under mutex
    void publish(RequestEvent event, bool admitted = false) {
        {
            std::lock_guard lock(mutex);
            if (!admitted && queued_requests >= config.max_requests) throw QueueFull{};
            event.admitted = admitted;
            if (!admitted) ++queued_requests;
            events.emplace(event.id, std::move(event));
        }
        notify();
    }
    // Under mutex: an event leaves `events`.
    void unqueue(const RequestEvent& event) {
        if (!event.admitted) --queued_requests;
    }
    void erase(uint64_t id) {
        std::lock_guard lock(mutex);
        if (auto it = events.find(id); it != events.end()) {
            unqueue(it->second);
            events.erase(it);
        } else {
            pending_application_nodes.erase(id);
        }
    }
    // Removes a request event Python has not drained yet; false once drained.
    bool withdraw(uint64_t id) {
        std::lock_guard lock(mutex);
        auto it = events.find(id);
        if (it == events.end()) return false;
        unqueue(it->second);
        events.erase(it);
        return true;
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
    // The response deadline (decide) until it is reported: request body data
    // arriving restarts it, as Sanic's RESPONSE_TIMEOUT counts from the last
    // data received.
    ss::timer<ss::lowres_clock>* response_deadline = nullptr;
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
class Service {
    Shared& shared;
    Counts& counts;
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
    uint64_t& next_peer = shard_counter(1);
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
        bool body_arriving = false;  // forward_body reads the connection
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
    // A request body forwarded to Python while its handler runs (forward_body):
    // what was published and the handler has not read yet (body_read decisions).
    struct BodyFlow {
        size_t unread = 0;
        bool publishing = true;  // until the response is out
        bool stopped = false;    // the connection closes after the response
        std::optional<ss::promise<>> room;
        void wake() {
            if (room) {
                room->set_value();
                room.reset();
            }
        }
    };
    std::map<uint64_t, BodyFlow*> body_flows;  // by request id
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
                // Counted now, not when its shard takes it: through a burst
                // this loop accepts without yielding, and with a count that
                // lags, each process holding the turn took all it could. (One
                // dropped as this process stops stays counted; its line no
                // longer counts then.)
                if (own_slot) count_connection(1);
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
    // counted: accept_shared counted the connections as it accepted them.
    ss::future<> accept_application(ss::server_socket& listener, const std::string& unix_path,
                                    bool counted = false) {
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
                if (own_slot && !counted) count_connection(1);
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
        enum class Wait { none, idle, request, response } expired = Wait::none, waiting = Wait::none;
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
        // What a read waits for, and so its deadline. Sanic times each stage
        // from the last data received: KEEP_ALIVE_TIMEOUT between requests;
        // REQUEST_TIMEOUT while a head arrives (here from its first byte, which
        // a peer sending a byte at a time cannot extend); RESPONSE_TIMEOUT
        // while the handler runs, a body arriving then included (each piece
        // also restarts the response deadline decide keeps while Python has
        // not answered; a streamed response has none); after the response,
        // the rest of the body as KEEP_ALIVE_TIMEOUT.
        enum class Reading { next, request, body, rest };
        auto read_more = [&](Reading reading = Reading::next) -> ss::future<bool> {
            const bool idle = reading == Reading::rest ||
                              (reading == Reading::next && buffer.size() == consumed);
            if (idle && draining) co_return false;  // Sanic's close_if_idle
            const bool body = reading == Reading::body;
            waiting = idle ? Wait::idle : body ? Wait::response : Wait::request;
            const auto timeout = body ? config.decision_timeout_ms
                                 : idle ? config.keep_alive_timeout_ms : config.request_timeout_ms;
            if (timeout) {
                deadline.arm(idle || body ? ss::lowres_clock::now() + std::chrono::milliseconds(timeout)
                                          : request_started + std::chrono::milliseconds(timeout));
            }
            connection.idle = idle && reading == Reading::next;
            auto chunk = co_await connection.input.read();
            connection.idle = false;
            deadline.cancel();
            if (chunk.empty()) co_return false;
            if (idle && reading == Reading::next) request_started = ss::lowres_clock::now();
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
                    // Python checks the size as the handler reads (Http.read: a
                    // stream handler has no limit). Too large to be read past
                    // after the response, as in Sanic: the connection ends then.
                    close = true;
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
                        if (!co_await read_more(Reading::request)) break;
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
                        while (buffer.size() - start < length && co_await read_more(Reading::request)) {}
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
                if (head.expect_continue && http11) output.append("HTTP/1.1 100 Continue\r\n\r\n");
                co_await flush_output(connection, output);
                const uint64_t root = (++next_id << 16) | ss::this_shard_id();
                size_t remaining = chunked ? 0 : length - body.size();
                bool body_ok = true;
                bool over = false;  // a chunked body declared more than max_body_bytes
                // At most body_window_bytes ahead of the handler (Sanic pauses
                // reading at REQUEST_BUFFER_SIZE); none once the response is out.
                BodyFlow flow;
                body_flows.emplace(root, &flow);
                auto unregister = ss::defer([&]() noexcept { body_flows.erase(root); });
                auto publish_data = [&](std::string piece, std::string parameter) {
                    RequestEvent data{(++next_id << 16) | ss::this_shard_id(), ss::this_shard_id(),
                                      method, "application_data", piece.size(), true, std::move(piece),
                                      std::move(parameter)};
                    data.parent = root;
                    counts.body_bytes_to_python += data.body.size();
                    flow.unread += data.body.size();
                    shared.publish(std::move(data), true);
                };
                auto forward_body = [&]() -> ss::future<> {
                    auto arrived = ss::defer([&]() noexcept { connection.body_arriving = false; });
                    for (;;) {
                        std::string piece;
                        bool end = false;
                        bool more_here = false;  // another chunk may already be buffered
                        if (chunked) {
                            size_t pos = start;
                            const auto state = decoder.feed(buffer, pos, piece,
                                                            std::numeric_limits<size_t>::max(), true);
                            buffer.erase(start, pos - start);
                            if (!over && decoder.declared() > config.max_body_bytes) {
                                // Past REQUEST_MAX_SIZE (Http.read checks each chunk's
                                // size): Python raises 413 unless the handler streams
                                // the body. Either way the connection ends after the
                                // response: Sanic does not read past the limit then.
                                over = true;
                                close = true;  // read by the response head if not yet written
                                if (flow.publishing) publish_data({}, "over");
                            }
                            if (state == http1::Chunked::bad_request) {
                                body_ok = false;
                                if (flow.publishing) {
                                    if (!piece.empty()) publish_data(std::move(piece), "");
                                    publish_data({}, "error:400:Bad chunked encoding");
                                }
                                close = true;
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
                        if (flow.publishing && (!piece.empty() || end))
                            publish_data(std::move(piece), end ? "end" : "");
                        if (end) co_return;
                        if (more_here) continue;
                        while (flow.publishing && flow.unread >= config.body_window_bytes && !closing) {
                            flow.room.emplace();
                            co_await flow.room->get_future();
                        }
                        if (flow.stopped || closing) {
                            body_ok = false;
                            co_return;
                        }
                        bool more = false;
                        try {
                            more = co_await read_more(flow.publishing ? Reading::body : Reading::rest);
                        } catch (...) {}  // reset by the peer
                        if (!more) {
                            body_ok = false;
                            // Stopped here, or the rest timed out after the response.
                            if (flow.stopped || !flow.publishing) co_return;
                            if (expired == Wait::response) {
                                // Sanic's response timeout: the handler is cancelled (a
                                // streamed response then ends, and the connection).
                                // While Python has not answered, decide's own deadline,
                                // restarted with the same piece, reports it.
                                auto response = waiters.find(root);
                                if (response == waiters.end() || !response->second->response_deadline) {
                                    RequestEvent timeout{(++next_id << 16) | ss::this_shard_id(),
                                                         ss::this_shard_id(), method, "application_timeout", 0};
                                    timeout.parent = root;
                                    shared.publish(std::move(timeout), true);
                                }
                                co_return;
                            }
                            // The peer closed or reset it mid-body: the handler is
                            // cancelled, also once its response started.
                            peer_left(connection, connection_key);
                            abort_request(root, 503);
                            co_return;
                        }
                        if (auto it = waiters.find(root); it != waiters.end() && it->second->response_deadline)
                            it->second->response_deadline->rearm(
                                ss::lowres_clock::now() + std::chrono::milliseconds(config.decision_timeout_ms));
                    }
                };
                // Seastar coroutines run eagerly: respond_application publishes the
                // request before its first suspension, so the data events that
                // forward_body publishes afterwards always follow it.
                connection.body_arriving = true;
                auto responding = respond_application(connection, method, std::move(input),
                                                      std::move(body), head_request, http11, close,
                                                      output, root);
                auto forwarding = forward_body();
                co_await std::move(responding);
                // The handler is done with the body: the rest is read past (as
                // Sanic does after a response), or not at all if the connection
                // ends now.
                flow.publishing = false;
                if (!forwarding.available()) {
                    // The response goes out now, as Sanic sends it, not once the
                    // rest of the body has come (forward_body does not write).
                    try {
                        co_await flush_output(connection, output);
                    } catch (...) {
                        close = true;
                    }
                    if (close) {
                        flow.stopped = true;
                        connection.shut_input();
                    }
                }
                flow.wake();
                co_await std::move(forwarding);
                if (!body_ok) {
                    expired = Wait::none;  // the response already reported it
                    close = true;
                    break;
                }
                keep = !close && !connection.peer_gone;
            }
            if (expired == Wait::request && config.framework_errors) {
                // Sanic answers a request that did not arrive in time with
                // RequestTimeout through its error handler.
                ApplicationInput input{{}, "1.1", {}, client, server, connection_key, ++sequence, unix_socket};
                bool close = true;
                co_await respond_application(connection, "NONE", std::move(input), {}, false, true, close,
                                             output, 0, "error:408:Request Timeout", 408);
            } else if (expired == Wait::request) {
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
        } catch (const QueueFull&) {
            // Overloaded: Python has max_requests new requests to take first.
            ++counts.rejected;
            reject(error_status ? error_status : 503);
            co_return;
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
        if (streamed && answer->code == 101 && connection.body_arriving) {
            // A tunnel while the request body still arrives (a WebSocket route
            // with ignore_body): its reader and forward_body would both read
            // the connection, and the body's rest would reach the tunnel.
            ++counts.errors;
            waiters.erase(root);
            retired = false;
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
        bool overrun = false;  // data past the declared Content-Length came
        auto frame = [&](std::string_view data) {
            if (!send_body || data.empty()) return;
            if (framing == Framing::chunked) {
                output.append(fmt::format("{:x}\r\n", data.size()));
                output.append(data);
                output.append("\r\n");
            } else {
                if (framing == Framing::length && static_cast<int64_t>(data.size()) > start.content_length - sent) {
                    // Never past the declared length (a client would read the rest
                    // as the next response): what fits goes out, then the
                    // connection ends. Python raises first, as Sanic does.
                    data = data.substr(0, static_cast<size_t>(std::max<int64_t>(start.content_length - sent, 0)));
                    overrun = true;
                }
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
            if (!co_await flush_and_acknowledge() || overrun) {
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
            while (!waiter.chunks.empty() && !ended && !failed && !overrun) {
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
            if (waiter.resumed) {
                waiter.resumed->set_value();
                waiter.resumed.reset();
            }
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
        auto& promise = waiter->promise;
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
            // Only now: a promise destroyed with its future attached (the
            // queue was full) would log "Exceptional future ignored".
            auto future = promise.get_future();
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
                    if (auto it = waiters.find(id); it != waiters.end()) it->second->response_deadline = nullptr;
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
                // Until this returns: a streamed response keeps its waiter, not the timer.
                waiters.at(id)->response_deadline = &timer;
                auto forget = ss::defer([this, id]() noexcept {
                    if (auto it = waiters.find(id); it != waiters.end()) it->second->response_deadline = nullptr;
                });
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
public:
    explicit Service(Shared* state)
        : shared(*state), counts(*state->counts[ss::this_shard_id()]) {}
    ss::future<> start() {
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
                app_accepting = accept_application(*app_listener, shared.config.unix_path, true);
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
        }
        co_return;
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
        if (kind == DecisionKind::body_read) {
            if (auto flow = body_flows.find(id); flow != body_flows.end()) {
                auto& unread = flow->second->unread;
                unread -= std::min(unread, size_t(std::max<int64_t>(content_length, 0)));
                flow->second->wake();
            }
            return;
        }
        auto it = waiters.find(id);
        if (it == waiters.end() || closing) return;
        auto& waiter = *it->second;
        counts.body_bytes_from_python += body.size();
        if (kind == DecisionKind::tunnel_pause || kind == DecisionKind::tunnel_resume) {
            waiter.paused = kind == DecisionKind::tunnel_pause;
            if (!waiter.paused && waiter.resumed) {
                waiter.resumed->set_value();
                waiter.resumed.reset();
            }
            return;
        }
        if (kind >= DecisionKind::stream_data) {
            // Continuation of a streamed response that respond_application writes.
            if (!waiter.replied) return;
            waiter.chunks.push_back(Decision{code, std::move(body), 0, retire, {}, kind});
            // Several pieces may come in one batch: wake the writer once.
            if (waiter.more) {
                waiter.more->set_value();
                waiter.more.reset();
            }
            return;
        }
        if (waiter.replied) return;
        waiter.replied = true;
        waiter.promise.set_value(Decision{code, std::move(body), 0, retire, std::move(headers),
                                          kind, content_length});
    }
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
        stop_accepting();
        for (auto& [id, peer] : app_peers) {
            peer->shut_input(); peer->socket.shutdown_output();
        }
        for (auto& [id, flow] : body_flows) flow->wake();
        for (auto& [id, waiter] : waiters) {
            if (!waiter->replied) {
                waiter->replied = true;
                waiter->promise.set_value(Decision{3, {}});
            } else {
                // A streamed response or tunnel sees `closing`.
                if (waiter->more) {
                    waiter->more->set_value();
                    waiter->more.reset();
                }
                if (waiter->resumed) {
                    waiter->resumed->set_value();
                    waiter->resumed.reset();
                }
            }
        }
        co_await std::move(app_accepting);
        co_await std::move(shared_accepting);
        co_await std::move(worker_accepting);
        co_await app_connections.close();
    }
};
}

struct NetworkServer::Impl {
    Shared shared;
    ss::sharded<Service> services;
    ss::alien::instance& alien;
    bool started = false;
    std::optional<ss::shared_future<>> stopping;  // stop() may be called again
    Impl(ServerConfig c, unsigned n, ss::alien::instance& a, std::function<void()> wake)
        : shared(std::move(c), n, std::move(wake)), alien(a) {}
};
NetworkServer::NetworkServer(ServerConfig c, unsigned n, ss::alien::instance& a,
                              std::function<void()> wake)
    : p(std::make_unique<Impl>(std::move(c), n, a, std::move(wake))) {}
NetworkServer::~NetworkServer() = default;
ss::future<> NetworkServer::start() {
    co_await p->services.start(&p->shared);
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
        p->shared.unqueue(it->second);
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
