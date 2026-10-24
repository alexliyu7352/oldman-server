#include "server.hh"
#include "http1.hh"
#include <asio.hpp>
#include <algorithm>
#include <atomic>
#include <charconv>
#include <chrono>
#include <cstring>
#include <deque>
#include <format>
#include <latch>
#include <limits>
#include <mutex>
#include <optional>
#include <set>
#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <time.h>
#include <unistd.h>

namespace probe {
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
using Socket = asio::generic::stream_protocol::socket;
using Acceptor = asio::basic_socket_acceptor<asio::generic::stream_protocol>;
using Endpoint = asio::generic::stream_protocol::endpoint;
// Completion without exceptions: (error_code, result...).
constexpr auto use_tuple = asio::as_tuple(asio::use_awaitable);

uint64_t clock_ns() {
    timespec t{};
    ::clock_gettime(CLOCK_MONOTONIC, &t);
    return uint64_t(t.tv_sec) * 1000000000 + t.tv_nsec;
}
uint64_t monotonic_ms() {
    timespec t{};
    ::clock_gettime(CLOCK_MONOTONIC, &t);
    return uint64_t(t.tv_sec) * 1000 + uint64_t(t.tv_nsec) / 1000000;
}

// Runs `f` when the scope ends.
template <typename F>
struct Defer {
    F f;
    ~Defer() { f(); }
};
template <typename F>
Defer(F) -> Defer<F>;

// One coroutine at a time waits until set(): the waiter is resumed through
// the io_context, never inside set() (whose caller may be in the middle of
// changing what the waiter looks at). A set() with nobody waiting is kept for
// the next wait(); every waiter rechecks what it waits for.
class Event {
    asio::io_context* io_;
    asio::any_completion_handler<void()> waiter_;
    bool set_ = false;
public:
    explicit Event(asio::io_context& io) : io_(&io) {}
    asio::awaitable<void> wait() {
        if (!set_) {
            co_await asio::async_initiate<decltype(asio::use_awaitable), void()>(
                [this](asio::any_completion_handler<void()> handler) { waiter_ = std::move(handler); },
                asio::use_awaitable);
        }
        set_ = false;
    }
    void set() {
        if (waiter_) asio::post(*io_, std::move(waiter_));
        else set_ = true;
    }
};

// Coroutines still running: close() waits for them.
struct Gate {
    explicit Gate(asio::io_context& io) : idle(io) {}
    size_t count = 0;
    Event idle;
    void enter() { ++count; }
    void leave() {
        if (--count == 0) idle.set();
    }
    asio::awaitable<void> close() {
        while (count) co_await idle.wait();
    }
};

// A timer of the shard's wheel: arming and cancelling are list operations, and
// it fires within one 10 ms tick of its time. Every request arms several
// (read, write and response deadlines); an asio::steady_timer for each cost
// about 0.26 us per request apiece.
class Wheel;
struct WheelEntry {
    WheelEntry *prev = nullptr, *next = nullptr;
    uint64_t due_ms = 0;
    std::function<void()> fire;
    Wheel* wheel = nullptr;
    bool armed() const { return prev != nullptr; }
    WheelEntry() = default;
    WheelEntry(const WheelEntry&) = delete;
    WheelEntry& operator=(const WheelEntry&) = delete;
    ~WheelEntry();
};
// 1024 slots of 10 ms and one ticker while any entry is armed. A slot is
// visited once all of its 10 ms have passed; an entry there fires if its time
// has come, or waits for the wheel's next turn.
class Wheel {
public:
    static constexpr uint64_t slot_ms = 10, slots = 1024;
    explicit Wheel(asio::io_context& io) : ticker_(io) {
        for (auto& head : heads_) head.prev = head.next = &head;
        due_.prev = due_.next = &due_;
    }
    ~Wheel() { ticker_.cancel(); }
    void arm(WheelEntry& entry, uint64_t delay_ms) { arm_at(entry, monotonic_ms() + delay_ms); }
    void arm_at(WheelEntry& entry, uint64_t due_ms) {
        cancel(entry);
        if (!ticking_) {
            ticking_ = true;
            visited_ms_ = monotonic_ms() / slot_ms * slot_ms;
            schedule();
        }
        entry.wheel = this;
        entry.due_ms = due_ms;
        // Never into a slot already visited on this turn.
        link(heads_[(std::max(due_ms, visited_ms_) / slot_ms) % slots], entry);
        ++count_;
    }
    void cancel(WheelEntry& entry) {
        if (!entry.armed()) return;
        unlink(entry);
        --count_;
    }
private:
    static void link(WheelEntry& head, WheelEntry& entry) {
        entry.next = &head;
        entry.prev = head.prev;
        head.prev->next = &entry;
        head.prev = &entry;
    }
    static void unlink(WheelEntry& entry) {
        entry.prev->next = entry.next;
        entry.next->prev = entry.prev;
        entry.prev = entry.next = nullptr;
    }
    void collect(WheelEntry& head, uint64_t now) {
        for (auto* entry = head.next; entry != &head;) {
            auto* next = entry->next;
            if (entry->due_ms <= now) {
                unlink(*entry);
                link(due_, *entry);
            }
            entry = next;
        }
    }
    void schedule() {
        ticker_.expires_after(std::chrono::milliseconds(slot_ms));
        // Destroying the wheel cancels this: an aborted wait touches nothing.
        ticker_.async_wait([this](asio::error_code ec) {
            if (ec) return;
            const auto now = monotonic_ms();
            if (now >= visited_ms_ + slot_ms * slots) {  // the thread was held up a whole turn
                for (auto& head : heads_) collect(head, now);
                visited_ms_ = now / slot_ms * slot_ms;
            }
            for (; visited_ms_ + slot_ms - 1 <= now; visited_ms_ += slot_ms)
                collect(heads_[(visited_ms_ / slot_ms) % slots], now);
            // The due entries are on their own list: firing one may cancel,
            // re-arm or destroy others.
            while (due_.next != &due_) {
                auto& entry = *due_.next;
                unlink(entry);
                --count_;
                entry.fire();
            }
            if (count_) schedule();
            else ticking_ = false;
        });
    }
    asio::steady_timer ticker_;
    std::array<WheelEntry, slots> heads_;
    WheelEntry due_;
    size_t count_ = 0;
    uint64_t visited_ms_ = 0;
    bool ticking_ = false;
};
WheelEntry::~WheelEntry() {
    if (armed() && wheel) wheel->cancel(*this);  // the wheel's own list heads have none
}

struct alignas(64) Counts {
    std::atomic<uint64_t> active_requests{0}, reserved_bytes{0}, peak_requests{0}, peak_bytes{0};
    std::atomic<uint64_t> accepted{0}, rejected{0}, responses{0}, errors{0}, pending_decisions{0};
    std::atomic<uint64_t> decisions{0}, decision_timeouts{0}, decision_wait_ns{0};
    std::atomic<uint64_t> python_terminal_events{0};
    std::atomic<uint64_t> body_bytes_to_python{0}, body_bytes_from_python{0}, response_bytes{0};
    std::atomic<uint64_t> connections_open{0};  // framework host connections
    // Connections handed to this shard and not closed yet (the accepting
    // shard's choice of the least loaded one).
    std::atomic<uint64_t> connections_assigned{0};
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
    // The shard runs on its poller's thread (Runtime's same-thread mode), which
    // takes the events after each poll: publishing does not notify.
    bool same_thread;
    std::vector<std::unique_ptr<Counts>> counts;
    mutable std::mutex mutex;
    std::map<uint64_t,RequestEvent> events;
    // Drained application values belong to Python; their empty tree nodes stay
    // here until decide() reclaims them on the shard that allocated them.
    std::map<uint64_t,RequestEvent> pending_application_nodes;
    std::atomic<bool> running{false};
    std::atomic<uint64_t> decision_return_batches{0}, decision_return_items{0}, decision_return_posts{0};
    Shared(ServerConfig c, unsigned n, std::function<void()> wake, bool same)
        : config(std::move(c)), shard_count(n), notify(std::move(wake)), same_thread(same) {
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
        if (!same_thread) notify();
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
    explicit Waiter(asio::io_context& io) : decided(io), more(io), resumed(io) {}
    std::optional<Decision> decision;
    Event decided;
    bool replied = false;
    // A streamed response keeps its waiter: later decisions queue here.
    std::deque<Decision> chunks;
    Event more;
    // A tunnel's reading of the peer is paused by Python (backpressure).
    bool paused = false;
    Event resumed;
    // The response deadline (decide) until it is reported: request body data
    // arriving restarts it, as Sanic's RESPONSE_TIMEOUT counts from the last
    // data received.
    WheelEntry* response_deadline = nullptr;
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
// The protocol of a listening socket (TCP or AF_UNIX) another owner bound.
asio::generic::stream_protocol listener_protocol(int fd) {
    sockaddr_storage storage{};
    socklen_t length = sizeof(storage);
    if (::getsockname(fd, reinterpret_cast<sockaddr*>(&storage), &length) < 0)
        throw std::system_error(errno, std::system_category(), "read the listening socket's address");
    const int family = storage.ss_family;
    if (family != AF_INET && family != AF_INET6 && family != AF_UNIX)
        throw std::invalid_argument("the listening socket is neither TCP nor AF_UNIX");
    return asio::generic::stream_protocol(family, family == AF_UNIX ? 0 : IPPROTO_TCP);
}
// A non-blocking duplicate of a listening socket another owner keeps.
int listener_copy(int fd) {
    const int copy = ::fcntl(fd, F_DUPFD_CLOEXEC, 0);  // the owner closes its own
    if (copy < 0) throw std::system_error(errno, std::system_category(), "duplicate the listening socket");
    // Accepted without blocking (the flag is shared with the owner's fd).
    if (::fcntl(copy, F_SETFL, ::fcntl(copy, F_GETFL) | O_NONBLOCK) < 0) {
        ::close(copy);
        throw std::system_error(errno, std::system_category(), "make the listening socket non-blocking");
    }
    return copy;
}
// http_host:http_port, bound here (tests and probes; app.run passes a socket).
int bind_listener(const std::string& host, unsigned port) {
    sockaddr_storage storage{};
    socklen_t length;
    auto* v4 = reinterpret_cast<sockaddr_in*>(&storage);
    auto* v6 = reinterpret_cast<sockaddr_in6*>(&storage);
    if (::inet_pton(AF_INET, host.c_str(), &v4->sin_addr) == 1) {
        v4->sin_family = AF_INET;
        v4->sin_port = htons(uint16_t(port));
        length = sizeof(sockaddr_in);
    } else if (::inet_pton(AF_INET6, host.c_str(), &v6->sin6_addr) == 1) {
        v6->sin6_family = AF_INET6;
        v6->sin6_port = htons(uint16_t(port));
        length = sizeof(sockaddr_in6);
    } else {
        throw std::invalid_argument("http_host must be an IP address");
    }
    const int fd = ::socket(storage.ss_family, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (fd < 0) throw std::system_error(errno, std::system_category(), "socket");
    const int one = 1;
    if (::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one)) < 0 ||
        ::bind(fd, reinterpret_cast<sockaddr*>(&storage), length) < 0 || ::listen(fd, 100) < 0) {
        const int error = errno;
        ::close(fd);
        throw std::system_error(error, std::system_category(), "listen on " + host + ":" + std::to_string(port));
    }
    return fd;
}
// An address as Python's socket module gives it: no IPv6 scope suffix for
// scope 0, and its port.
std::pair<std::string, unsigned> address_of(const Endpoint& endpoint) {
    const auto* address = endpoint.data();
    char text[INET6_ADDRSTRLEN] = {};
    if (address->sa_family == AF_INET) {
        const auto& v4 = *reinterpret_cast<const sockaddr_in*>(address);
        ::inet_ntop(AF_INET, &v4.sin_addr, text, sizeof(text));
        return {text, ntohs(v4.sin_port)};
    }
    if (address->sa_family == AF_INET6) {
        const auto& v6 = *reinterpret_cast<const sockaddr_in6*>(address);
        ::inet_ntop(AF_INET6, &v6.sin6_addr, text, sizeof(text));
        std::string result(text);
        if (v6.sin6_scope_id) result += "%" + std::to_string(v6.sin6_scope_id);
        return {result, ntohs(v6.sin6_port)};
    }
    return {};
}
class Service;
}  // namespace

struct NetworkServer::Impl : std::enable_shared_from_this<NetworkServer::Impl> {
    Shared shared;
    std::vector<asio::io_context*> contexts;
    Offload offload;
    std::vector<std::unique_ptr<Service>> services;  // services[i] lives on shard i
    bool started = false, stopped = false;
    std::mutex lifecycle;
    std::thread::id poller;  // same-thread mode
    Impl(ServerConfig c, std::vector<asio::io_context*> shards, std::function<void()> wake, Offload files,
         std::thread::id polling)
        : shared(std::move(c), unsigned(shards.size()), std::move(wake), polling != std::thread::id{}),
          contexts(std::move(shards)), offload(std::move(files)), services(contexts.size()), poller(polling) {}
    // A post from another thread reaches a same-thread shard's poller through
    // the notification (Asio wakes no reactor that nothing runs).
    void wake_poller() {
        if (shared.same_thread) shared.notify();
    }
    // Waits for what on_all posted; the poller itself waiting (Engine.abandon
    // stops the server from its thread) runs the shard meanwhile.
    void await_shards(std::latch& done) {
        if (shared.same_thread && std::this_thread::get_id() == poller) {
            while (!done.try_wait()) contexts.front()->run_one_for(std::chrono::milliseconds(1));
        } else {
            done.wait();
        }
    }
    // Runs fn(shard) on every shard and waits for all; the first failure is
    // rethrown once all have run.
    void on_all(const std::function<void(unsigned)>& fn);
    void on_all_async(const std::function<asio::awaitable<void>(unsigned)>& fn);
    unsigned least_loaded_shard() const;
    void hand_off(unsigned shard, Socket socket, Endpoint remote, const std::string* unix_path, bool counted);
};

namespace {
class Service {
    NetworkServer::Impl& owner;
    Shared& shared;
    Counts& counts;
    const unsigned shard;
    asio::io_context& io;
    Wheel wheel;  // first in, last out: every entry of it goes before it
    std::map<uint64_t, std::unique_ptr<Waiter>> waiters;
    // Request and connection ids count on per shard across the servers a
    // process runs one after another: a late answer to a replaced server's
    // request never matches another server's request.
    static uint64_t& shard_counter(int which) {
        static thread_local uint64_t counters[2] = {0, 0};
        return counters[which];
    }
    uint64_t& next_id;
    bool closing = false;
    // Stopping gracefully (drain): no new connections, idle ones closed, the
    // others closed after their response.
    bool draining = false;
    bool accepts() const { return !closing && !draining; }
    uint64_t& next_peer;
    // ---- HTTP/1.1 host for framework dispatch (application_dispatch_all) ----
    // Parses and writes HTTP/1.1 directly on the sockets: one buffered write
    // per batch of responses, Content-Length framing, repeated header names.
    struct AppConnection {
        explicit AppConnection(Socket s) : socket(std::move(s)) {}
        Socket socket;
        uint64_t key = 0;  // (id << 16) | shard
        char chunk[16384];  // each read lands here first
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
        WheelEntry write_timer;
        bool idle = false;  // waiting for a request, nothing of it received
        bool body_arriving = false;  // forward_body reads the connection
        // Shutting the input ends a pending read (it returns EOF).
        void shut_input() {
            shut_here = true;
            asio::error_code ignored;
            socket.shutdown(Socket::shutdown_receive, ignored);  // ENOTCONN after a reset
        }
        void shut_output() {
            asio::error_code ignored;
            socket.shutdown(Socket::shutdown_send, ignored);
        }
    };
    // The peer of a connection left: as Sanic's connection_lost, the request
    // Python is handling there is cancelled (application_lost; the event's
    // size is the connection key, whose state Python marks lost).
    void peer_left(AppConnection& connection, uint64_t connection_key) {
        if (connection.peer_gone) return;
        connection.peer_gone = true;
        if (!connection.handling || connection.tunnel || closing) return;
        RequestEvent lost{(++next_id << 16) | shard, shard, {}, "application_lost", connection_key};
        lost.parent = connection.handling;
        shared.publish(std::move(lost), true);
    }
    // The peer leaving (FIN or RST) while Python handles a request on a
    // connection: one epoll set of the shard holds every connection, once,
    // for EPOLLRDHUP, from its accept to its close. A connection leaves it
    // when it fires; closing the socket takes it out too. This side's own
    // input shutdowns fire it as well, and are not the peer leaving.
    int rdhup_set = -1;
    std::optional<asio::posix::stream_descriptor> rdhup_fd;
    // The set's wait, which stop() ends before the set closes: the io_uring
    // backend can complete a cancelled wait as ready, after the service.
    bool watching = true;
    Gate peer_watch;
    void watch_peers() {
        // Asio's registration of the set's fd is edge-triggered and a wait
        // never completes on readiness that came before it: the set is
        // drained after the wait is armed.
        peer_watch.enter();
        rdhup_fd->async_wait(asio::posix::stream_descriptor::wait_read, [this](asio::error_code ec) {
            peer_watch.leave();
            if (!ec && watching) watch_peers();
        });
        epoll_event events[64];
        int n;
        do {
            n = ::epoll_wait(rdhup_set, events, 64, 0);
            for (int i = 0; i < n; ++i) {
                auto it = app_peers.find(events[i].data.u64);
                if (it == app_peers.end()) continue;
                auto& connection = *it->second;
                ::epoll_ctl(rdhup_set, EPOLL_CTL_DEL, connection.socket.native_handle(), nullptr);
                if (!connection.shut_here) peer_left(connection, connection.key);
            }
        } while (n == 64);
    }
    // Shard 0's listeners: their acceptors (so stopping can end their
    // accepts), and whether stopping shuts the socket down for everyone (a
    // listener of this process's own).
    struct Listener {
        Acceptor acceptor;
        const std::string* unix_path;
        bool own;
    };
    std::vector<std::unique_ptr<Listener>> listeners;
    Gate accepting;
    // Shared listen_fd: the worker processes' accept counts, and this one's.
    AcceptSlot* accept_slots = nullptr;
    AcceptSlot* own_slot = nullptr;
    uint64_t own_pid = 0;
    std::optional<asio::steady_timer> accept_heartbeat;
    // The pauses of the accept loops after a failed accept (stop() ends them).
    std::set<asio::steady_timer*> accept_pauses;
    // After a failed accept: wait before accepting again, longer while the
    // failures go on (from 10 ms to 1 s; asyncio waits 1 s after EMFILE).
    // False when the server stops meanwhile.
    asio::awaitable<bool> pause_accepting(unsigned failures) {
        const auto pause = std::chrono::milliseconds(10 << std::min(failures, 6u));
        asio::steady_timer timer(io, std::min(pause, std::chrono::milliseconds(1000)));
        accept_pauses.insert(&timer);
        auto [ec] = co_await timer.async_wait(use_tuple);
        accept_pauses.erase(&timer);
        co_return !ec && accepts();
    }
    Gate app_connections;
    std::map<uint64_t, std::unique_ptr<AppConnection>> app_peers;
    // A request body forwarded to Python while its handler runs (forward_body):
    // what was published and the handler has not read yet (body_read decisions).
    struct BodyFlow {
        explicit BodyFlow(asio::io_context& io) : room(io) {}
        size_t unread = 0;
        bool publishing = true;  // until the response is out
        bool stopped = false;    // the connection closes after the response
        Event room;
        void wake() { room.set(); }
    };
    std::map<uint64_t, BodyFlow*> body_flows;  // by request id
    http1::DateLine date_line;
    std::string probe_lines;  // configured diagnostic header lines, fixed per shard

    std::string_view date() {
        return shared.config.generate_date_header ? date_line.get() : std::string_view{};
    }
    // What the peer sent next, appended to `buffer`: false at its end (EOF,
    // or this side shut the input). A failure (a reset) throws.
    asio::awaitable<bool> read_into(AppConnection& connection, std::string& buffer) {
        auto [ec, n] = co_await connection.socket.async_read_some(
            asio::buffer(connection.chunk, sizeof(connection.chunk)), use_tuple);
        if (ec == asio::error::eof) co_return false;
        if (ec) throw std::system_error(ec);
        buffer.append(connection.chunk, n);
        co_return true;
    }
    asio::awaitable<void> write_all(AppConnection& connection, std::string_view data) {
        auto [ec, n] = co_await asio::async_write(connection.socket, asio::buffer(data.data(), data.size()),
                                                  use_tuple);
        if (ec) throw std::system_error(ec);
    }
    // One write the peer must take within write_timeout_ms (see AppConnection).
    asio::awaitable<void> timed_write(AppConnection& connection, std::string_view data) {
        if (const auto timeout = shared.config.write_timeout_ms) wheel.arm(connection.write_timer, timeout);
        std::exception_ptr failure;
        try {
            co_await write_all(connection, data);
        } catch (...) {
            failure = std::current_exception();
        }
        wheel.cancel(connection.write_timer);
        if (failure) std::rethrow_exception(failure);
    }
    asio::awaitable<void> flush_output(AppConnection& connection, std::string& output) {
        if (output.empty()) co_return;
        co_await timed_write(connection, output);
        output.clear();
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
    void stop_heartbeat() {
        if (accept_heartbeat) accept_heartbeat->cancel();
        if (own_slot && owns_slot()) own_slot->heartbeat_ms.store(0, std::memory_order_release);
    }
    asio::awaitable<void> beat_heartbeat() {
        while (accepts()) {
            accept_heartbeat->expires_after(std::chrono::milliseconds(50));
            auto [ec] = co_await accept_heartbeat->async_wait(use_tuple);
            if (ec || !accepts()) break;
            if (owns_slot()) own_slot->heartbeat_ms.store(monotonic_ms(), std::memory_order_release);
        }
    }
    // Shard 0 of a worker process that shares listen_fd with others: accepts
    // on its own duplicate (cancelling an accept leaves the shared socket
    // alone, while shutting it down would stop it for every process) and
    // hands each connection to its least loaded shard.
    //
    // Without taking turns, the process whose shard is busy wins nearly every
    // accept. Shard 0 accepts while it is least loaded. Holding back, it
    // checks again after 50 us, then less often (up to every 1 ms): two
    // processes that both accept quickly keep passing the turn, and one that
    // has fallen behind only notices at its next check (checking every 200 us
    // left connections queued, a median latency of 15 ms at 20k
    // connections/s). After 1 ms it takes a connection still waiting: the
    // others are not accepting (an idle shard wakes well within).
    asio::awaitable<void> accept_shared(Listener& listener) {
        try {
            unsigned failures = 0;
            while (accepts()) {
                if (own_slot) {
                    auto pause = std::chrono::microseconds(50);
                    const auto give_up = std::chrono::steady_clock::now() + std::chrono::milliseconds(1);
                    asio::steady_timer timer(io);
                    while (accepts() && !least_loaded()) {
                        timer.expires_after(pause);
                        co_await timer.async_wait(use_tuple);
                        pause = std::min(pause * 2, std::chrono::microseconds(1000));
                        if (std::chrono::steady_clock::now() >= give_up && connection_waiting()) break;
                    }
                }
                if (!accepts()) break;
                if (!co_await accept_one(listener, true, failures)) break;
            }
        } catch (...) {
            if (accepts()) {
                ++counts.errors;
                // This process no longer accepts: its siblings stop waiting
                // for its turn (its heartbeat would say it still does).
                if (own_slot) stop_heartbeat();
            }
        }
    }
    // A listener of this process's own (listen_fd, NATIVE_WORKER_LISTEN, or
    // http_host:http_port): shard 0 accepts and hands each connection to its
    // least loaded shard.
    asio::awaitable<void> accept_own(Listener& listener) {
        try {
            unsigned failures = 0;
            while (accepts())
                if (!co_await accept_one(listener, false, failures)) break;
        } catch (...) { if (accepts()) ++counts.errors; }
    }
    // One accept: false when accepting ends. counted: the connection counts
    // towards this process's load now, not when its shard takes it (through
    // a burst the loop accepts without yielding, and with a count that lags,
    // each process holding the turn took all it could; one dropped as this
    // process stops stays counted: its line no longer counts then).
    // With none queued it waits for one and the loop accepts again: a wait,
    // not an asynchronous accept, which the io_uring backend retries while
    // the socket polls ready and has none (another process took it, or an
    // AF_UNIX listener was shut down) and loses a cancel that came meanwhile.
    asio::awaitable<bool> accept_one(Listener& listener, bool counted, unsigned& failures) {
        const auto target = owner.least_loaded_shard();
        Endpoint remote;
        asio::error_code ec;
        Socket socket = listener.acceptor.accept(*owner.contexts[target], remote, ec);
        if (ec == asio::error::would_block || ec == asio::error::try_again) {
            auto [waited] = co_await listener.acceptor.async_wait(Acceptor::wait_read, use_tuple);
            co_return waited != asio::error::operation_aborted && accepts();
        }
        if (ec) {
            if (!accepts()) co_return false;
            const int error = ec.value();
            if (error == EBADF || error == EINVAL || error == ENOTSOCK || error == EFAULT)
                throw std::system_error(ec, "accept");  // the listening socket itself is unusable
            // Out of descriptors or memory (EMFILE, ENFILE, ENOBUFS, ENOMEM),
            // or what accept(2) says to retry on a TCP socket (EPROTO,
            // ENETDOWN, EHOSTUNREACH, ..., EPERM from a firewall): this
            // connection failed, the next may not.
            ++counts.errors;
            co_return co_await pause_accepting(failures++);
        }
        failures = 0;
        if (!accepts()) co_return false;
        if (counted && own_slot) count_connection(1);
        owner.hand_off(target, std::move(socket), remote, listener.unix_path, counted);
        co_return true;
    }
public:
    Service(NetworkServer::Impl& server, unsigned id, asio::io_context& context)
        : owner(server), shared(server.shared), counts(*server.shared.counts[id]), shard(id), io(context),
          wheel(context), next_id(shard_counter(0)), next_peer(shard_counter(1)), peer_watch(context),
          accepting(context), app_connections(context) {}
    ~Service() {
        if (rdhup_set >= 0) {
            rdhup_fd.reset();  // closes the set
        }
    }
    // A connection accepted for this shard (by shard 0); unix_path (a
    // ServerConfig member, so it outlives the connections): what requests over
    // an AF_UNIX listener report as the server address. counted: shard 0
    // counted it towards this process's load as it accepted it.
    void adopt(Socket socket, Endpoint remote, const std::string& unix_path, bool counted) {
        if (!accepts()) {
            --counts.connections_assigned;
            return;
        }
        auto id = ++next_peer;
        auto [it, added] = app_peers.emplace(id, std::make_unique<AppConnection>(std::move(socket)));
        auto& connection = *it->second;
        connection.key = (id << 16) | shard;
        epoll_event event{};
        event.events = EPOLLRDHUP;
        event.data.u64 = id;
        ::epoll_ctl(rdhup_set, EPOLL_CTL_ADD, connection.socket.native_handle(), &event);
        ++counts.connections_open;
        // Every open connection counts towards this process's load.
        if (own_slot && !counted) count_connection(1);
        app_connections.enter();
        asio::co_spawn(io, serve_application(id, remote, unix_path), [this](std::exception_ptr failure) {
            if (failure) ++counts.errors;
            if (own_slot) count_connection(-1);
            --counts.connections_assigned;
            app_connections.leave();
        });
    }
    asio::awaitable<void> serve_application(uint64_t id, Endpoint remote, const std::string& unix_path) {
        auto& connection = *app_peers.at(id);
        const auto& config = shared.config;
        asio::error_code ignored;
        const auto local = connection.socket.local_endpoint(ignored);
        // Over a unix socket Sanic reports no client address and the socket path.
        const bool unix_socket = local.data()->sa_family == AF_UNIX;
        using Address = std::pair<std::string, unsigned>;
        const Address client = unix_socket ? Address{} : address_of(remote);
        const Address server = unix_socket ? Address{unix_path, 0} : address_of(local);
        const uint64_t connection_key = connection.key;
        unsigned sequence = 0;
        std::string buffer, output;
        size_t consumed = 0;
        connection.buffer = &buffer;
        connection.consumed = &consumed;
        // Keep-alive idle and request-receive deadlines: the timer shuts the input
        // down, which ends the pending read; `expired` says which deadline it was.
        enum class Wait { none, idle, request, response } expired = Wait::none, waiting = Wait::none;
        uint64_t request_started = 0;
        WheelEntry deadline;
        deadline.fire = [&] {
            expired = waiting;
            connection.shut_input();
        };
        connection.write_timer.fire = [&connection, this] {
            ++counts.errors;
            connection.shut_input();
            connection.shut_output();
        };
        // What a read waits for, and so its deadline. Sanic times each stage
        // from the last data received: KEEP_ALIVE_TIMEOUT between requests;
        // REQUEST_TIMEOUT while a head arrives (here from its first byte, which
        // a peer sending a byte at a time cannot extend); RESPONSE_TIMEOUT
        // while the handler runs, a body arriving then included (each piece
        // also restarts the response deadline decide keeps while Python has
        // not answered; a streamed response has none); after the response,
        // the rest of the body as KEEP_ALIVE_TIMEOUT.
        enum class Reading { next, request, body, rest };
        auto read_more = [&](Reading reading = Reading::next) -> asio::awaitable<bool> {
            const bool idle = reading == Reading::rest ||
                              (reading == Reading::next && buffer.size() == consumed);
            if (idle && draining) co_return false;  // Sanic's close_if_idle
            const bool body = reading == Reading::body;
            waiting = idle ? Wait::idle : body ? Wait::response : Wait::request;
            const auto timeout = body ? config.decision_timeout_ms
                                 : idle ? config.keep_alive_timeout_ms : config.request_timeout_ms;
            if (timeout) {
                if (idle || body) wheel.arm(deadline, timeout);
                else wheel.arm_at(deadline, request_started + timeout);
            }
            connection.idle = idle && reading == Reading::next;
            std::exception_ptr failure;
            bool more = false;
            try {
                more = co_await read_into(connection, buffer);
            } catch (...) {
                failure = std::current_exception();
            }
            connection.idle = false;
            wheel.cancel(deadline);
            if (failure) std::rethrow_exception(failure);
            if (!more) co_return false;
            if (idle && reading == Reading::next) request_started = monotonic_ms();
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
                                                 std::format("error:{}:{}", head.error_status, head.error),
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
                request_started = monotonic_ms();  // for a pipelined successor
                if (complete) {
                    co_await respond_application(connection, method, std::move(input), std::move(body),
                                                 head_request, http11, close, output, 0);
                    keep = !close && !connection.peer_gone;
                    continue;
                }
                if (head.expect_continue && http11) output.append("HTTP/1.1 100 Continue\r\n\r\n");
                co_await flush_output(connection, output);
                const uint64_t root = (++next_id << 16) | shard;
                size_t remaining = chunked ? 0 : length - body.size();
                bool body_ok = true;
                bool over = false;  // a chunked body declared more than max_body_bytes
                // At most body_window_bytes ahead of the handler (Sanic pauses
                // reading at REQUEST_BUFFER_SIZE); none once the response is out.
                BodyFlow flow(io);
                body_flows.emplace(root, &flow);
                Defer unregister{[&]() noexcept { body_flows.erase(root); }};
                auto publish_data = [&](std::string piece, std::string parameter) {
                    RequestEvent data{(++next_id << 16) | shard, shard, method, "application_data",
                                      piece.size(), true, std::move(piece), std::move(parameter)};
                    data.parent = root;
                    counts.body_bytes_to_python += data.body.size();
                    flow.unread += data.body.size();
                    shared.publish(std::move(data), true);
                };
                auto forward_body = [&]() -> asio::awaitable<void> {
                    Defer arrived{[&]() noexcept { connection.body_arriving = false; }};
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
                        while (flow.publishing && flow.unread >= config.body_window_bytes && !closing)
                            co_await flow.room.wait();
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
                                    RequestEvent timeout{(++next_id << 16) | shard, shard, method,
                                                         "application_timeout", 0};
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
                            wheel.arm(*it->second->response_deadline, config.decision_timeout_ms);
                    }
                };
                // respond_application publishes the request (with its id, root)
                // before it waits for Python: the data events forward_body
                // publishes start only after that, and so follow it.
                connection.body_arriving = true;
                Event published(io), responded(io), forwarded(io);
                bool responding_done = false, forwarding_done = false;
                std::exception_ptr responding_failure;
                asio::co_spawn(io, respond_application(connection, method, std::move(input), std::move(body),
                                                       head_request, http11, close, output, root, {}, 0,
                                                       &published),
                               [&](std::exception_ptr failure) {
                                   responding_failure = failure;
                                   responding_done = true;
                                   published.set();
                                   responded.set();
                               });
                co_await published.wait();
                asio::co_spawn(io, forward_body(), [&](std::exception_ptr) {
                    forwarding_done = true;
                    forwarded.set();
                });
                while (!responding_done) co_await responded.wait();
                if (responding_failure) {
                    while (!forwarding_done) co_await forwarded.wait();
                    std::rethrow_exception(responding_failure);
                }
                // The handler is done with the body: the rest is read past (as
                // Sanic does after a response), or not at all if the connection
                // ends now.
                flow.publishing = false;
                if (!forwarding_done) {
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
                while (!forwarding_done) co_await forwarded.wait();
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
            RequestEvent closed{(++next_id << 16) | shard, shard, {}, "application_closed", 0};
            closed.parent = connection_key;
            shared.publish(std::move(closed), true);
        }
        wheel.cancel(deadline);
        if (!connection.shut_here) connection.shut_input();
        // The peer gets this side's FIN before the socket closes.
        connection.shut_output();
        app_peers.erase(id);  // closes the socket
        --counts.connections_open;
    }
    // streamed_id: nonzero when the body is still arriving; the request is
    // published under that id (application_data events refer to it).
    // parameter/error_status: a protocol error Python renders ("error:<status>:<message>");
    // the native fallback answers with that status. published: set once the
    // request is published (or will not be).
    asio::awaitable<void> respond_application(AppConnection& connection, const std::string& method,
                                              ApplicationInput input, std::string body, bool head_request,
                                              bool http11, bool& close, std::string& output,
                                              uint64_t streamed_id, std::string parameter = {},
                                              int error_status = 0, Event* published = nullptr) {
        const auto size = body.size();
        // Admission covers the request (input twice) and the response head; response
        // bodies are bounded by max_response_bytes, not reserved up front.
        auto lease = reserve(2 * size + 8192);
        uint64_t root = 0;
        unsigned terminal_status = 500;
        bool retired = false;
        const auto connection_key = input.connection;
        Defer finished{[&]() noexcept {
            connection.handling = 0;
            if (!root || retired) return;
            RequestEvent event{(++next_id << 16) | shard, shard, method, "application_done", terminal_status};
            event.parent = root;
            event.reservation = lease;
            ++counts.python_terminal_events;
            // Admitted: the framework host's in-request events may fill the queue.
            shared.publish(std::move(event), true);
        }};
        bool announced = false;
        auto announce = [&] {
            if (published && !announced) published->set();
            announced = true;
        };
        Defer announced_anyway{[&]() noexcept { announce(); }};
        auto reject = [&](int status) {
            terminal_status = status;
            http1::append_error(output, status, close, date(), probe_lines);
            ++counts.responses;
        };
        if (!lease) { reject(error_status ? error_status : 503); co_return; }
        std::optional<Decision> answer;
        try {
            if (closing) { reject(error_status ? error_status : 503); co_return; }
            // Publishes the request (and sets root) before it waits.
            root = streamed_id ? streamed_id : (++next_id << 16) | shard;
            ask(root, method, size, &body, streamed_id ? "more" : std::move(parameter), &input);
            announce();
            connection.handling = root;
            if (connection.peer_gone) {
                // Gone before the request reached Python (a reset while the
                // head or body arrived): it is cancelled as it starts.
                connection.peer_gone = false;
                peer_left(connection, connection_key);
            }
            answer = co_await decide(root, method);
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
        // In pieces, each one write within the write deadline: a slow peer
        // that keeps reading gets all of it, one that stops reading is cut off.
        constexpr size_t piece = 1 << 20;
        const std::string_view whole(answer->body);
        for (size_t at = 0; at < whole.size(); at += piece)
            co_await timed_write(connection, whole.substr(at, piece));
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
    asio::awaitable<void> stream_application(AppConnection& connection, Decision& start, uint64_t root,
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
                framing_line = std::format("Content-Length: {}\r\n", start.content_length);
            else if (has_body && framing == Framing::chunked)
                framing_line = "Transfer-Encoding: chunked\r\n";
            http1::append_stream_head(output, start.code, start.headers, close, date(), probe_lines,
                                      framing_line);
        }
        auto publish = [&](std::string data, const char* parameter) {
            RequestEvent event{(++next_id << 16) | shard, shard, method, "application_data",
                               data.size(), true, std::move(data), parameter};
            event.parent = root;
            counts.body_bytes_to_python += event.body.size();
            shared.publish(std::move(event), true);
        };
        bool tunnel_over = false;
        auto read_peer = [&]() -> asio::awaitable<void> {
            std::string early = connection.buffer->substr(*connection.consumed);
            connection.buffer->clear();
            *connection.consumed = 0;
            if (!early.empty()) publish(std::move(early), "");
            try {
                std::string data;
                while (!tunnel_over) {
                    while (waiter.paused && !tunnel_over && !closing) co_await waiter.resumed.wait();
                    if (tunnel_over || closing) break;
                    data.clear();
                    if (!co_await read_into(connection, data)) break;
                    publish(std::move(data), "");
                }
            } catch (...) {}
            if (!tunnel_over) publish({}, "end");
        };
        Event read_ended(io);
        bool reading_done = !tunnel;
        if (tunnel)
            asio::co_spawn(io, read_peer(), [&](std::exception_ptr) {
                reading_done = true;
                read_ended.set();
            });
        int64_t sent = 0;
        uint64_t unacked = 0;  // bytes Python queued and has not been told are written
        bool overrun = false;  // data past the declared Content-Length came
        auto frame = [&](std::string_view data) {
            if (!send_body || data.empty()) return;
            if (framing == Framing::chunked) {
                output.append(std::format("{:x}\r\n", data.size()));
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
        auto send_file = [&](const Decision& order) -> asio::awaitable<bool> {
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
            const size_t read_size = std::max<uint64_t>(65536, std::min<uint64_t>(chunk, 1 << 20));
            File file{owner.offload, io};
            try {
                // As a C string the path would end at a NUL (Engine.stream_file refuses one).
                if (path.find('\0') != std::string::npos)
                    throw std::system_error(EINVAL, std::system_category(), "embedded null byte");
                co_await file.open(path);
                std::string pending;
                uint64_t at = offset;
                while (written + pending.size() < length && !aborted()) {
                    const auto want = std::min<uint64_t>(read_size, offset + length - at);
                    auto data = co_await file.read(at, want);
                    if (data.empty()) break;  // the file became shorter
                    at += data.size();
                    pending.append(data);
                    size_t done = 0;
                    for (; pending.size() - done >= chunk; done += chunk)
                        frame(std::string_view(pending).substr(done, chunk));
                    written += done;
                    pending.erase(0, done);
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
            } catch (const std::system_error& e) {
                error = std::format("error:{}:{}", e.code().value(), e.code().message());
            } catch (const std::exception& e) {
                error = std::format("error:{}:{}", EIO, e.what());
            }
            co_await file.close();
            RequestEvent done{(++next_id << 16) | shard, shard, method, "application_file", written,
                              false, {}, std::move(error)};
            done.parent = root;
            shared.publish(std::move(done), true);
            co_return connected && !aborted();
        };
        // Writes what is buffered and tells Python how much of what it queued
        // is written; false when the connection failed.
        auto flush_and_acknowledge = [&]() -> asio::awaitable<bool> {
            try {
                co_await flush_output(connection, output);
            } catch (...) {
                co_return false;
            }
            if (unacked) {
                RequestEvent ack{(++next_id << 16) | shard, shard, method, "application_written", unacked};
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
            while (waiter.chunks.empty() && !closing) co_await waiter.more.wait();
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
            waiter.resumed.set();
        }
        while (!reading_done) co_await read_ended.wait();
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
    // A file a streamed response reads: opened and read by the file threads
    // (a read that is not in the page cache blocks), each result posted back.
    struct File {
        Offload& offload;
        asio::io_context& io;
        int fd = -1;
        asio::awaitable<void> run(std::function<void()> job) {
            Event done(io);
            offload([&] {
                job();
                asio::post(io, [&] { done.set(); });
            });
            co_await done.wait();
        }
        asio::awaitable<void> open(const std::string& path) {
            int error = 0;
            co_await run([&] {
                fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
                if (fd < 0) error = errno;
            });
            if (error) throw std::system_error(error, std::system_category(), path);
        }
        asio::awaitable<std::string> read(uint64_t offset, size_t size) {
            std::string data;
            int error = 0;
            co_await run([&] {
                data.resize(size);
                ssize_t got;
                do { got = ::pread(fd, data.data(), size, off_t(offset)); } while (got < 0 && errno == EINTR);
                if (got < 0) {
                    error = errno;
                    got = 0;
                }
                data.resize(size_t(got));
            });
            if (error) throw std::system_error(error, std::system_category());
            co_return data;
        }
        asio::awaitable<void> close() {
            if (fd >= 0) {
                const int closing_fd = fd;
                fd = -1;
                co_await run([closing_fd] { ::close(closing_fd); });
            }
        }
    };

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
    // A request for Python, published under `id` with its waiter; decide()
    // waits for the answer. QueueFull when Python has max_requests new
    // requests to take first.
    void ask(uint64_t id, const std::string& method, size_t bytes, const std::string* copied,
             std::string parameter, ApplicationInput* input) {
        waiters.emplace(id, std::make_unique<Waiter>(io));
        ++counts.pending_decisions; ++counts.decisions;
        try {
            RequestEvent event{id, shard, method, "application", bytes, copied != nullptr, {}};
            event.parameter = std::move(parameter);
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
        } catch (...) {
            shared.erase(id);
            waiters.erase(id);
            --counts.pending_decisions;
            throw;
        }
    }
    // The response deadline: Python is told and cancels the handler, which may
    // still answer (Sanic: 503 Response Timeout through its error handler).
    // Without an answer within the grace period the host answers 504 itself.
    asio::awaitable<Decision> decide(uint64_t id, const std::string& method) {
        auto begin = clock_ns();
        auto& waiter = *waiters.at(id);
        bool reported = false;
        WheelEntry timer;
        timer.fire = [&, id] {
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
            RequestEvent timeout{(++next_id << 16) | shard, shard, method, "application_timeout", 0};
            timeout.parent = id;
            shared.publish(std::move(timeout), true);
            wheel.arm(timer, 2000);
        };
        wheel.arm(timer, shared.config.decision_timeout_ms);
        // Until this returns: a streamed response keeps its waiter, not the timer.
        waiter.response_deadline = &timer;
        while (!waiter.decision) co_await waiter.decided.wait();
        waiter.response_deadline = nullptr;
        wheel.cancel(timer);
        Decision result = std::move(*waiter.decision);
        waiter.decision.reset();
        result.timed_out = reported;
        counts.decision_wait_ns += clock_ns() - begin;
        shared.erase(id);
        // A streamed response keeps its waiter for later chunks; respond_application
        // erases it when the stream ends.
        if (result.kind != DecisionKind::stream_start) waiters.erase(id);
        --counts.pending_decisions;
        result.id = id;
        co_return result;
    }
    void settle(Waiter& waiter, Decision decision) {
        waiter.decision = std::move(decision);
        waiter.decided.set();
    }
public:
    void start() {
        rdhup_set = ::epoll_create1(EPOLL_CLOEXEC);
        if (rdhup_set < 0) throw std::system_error(errno, std::system_category(), "epoll_create1");
        rdhup_fd.emplace(io, rdhup_set);
        watch_peers();
        if (!shared.config.application_dispatch_all) return;
        const auto pid = std::to_string(::getpid());
        if (shared.config.probe_headers == "all") {
            probe_lines = "X-Probe-Pid: " + pid + "\r\nX-Probe-Shard: " + std::to_string(shard) +
                          "\r\nX-Probe-Policy: 0\r\n";
        } else if (shared.config.probe_headers == "pid") {
            probe_lines = "X-Probe-Pid: " + pid + "\r\n";
        }
        auto listen = [&](int fd, asio::generic::stream_protocol protocol, const std::string* unix_path, bool own,
                          bool shared_turns) {
            auto listener = std::make_unique<Listener>(Listener{Acceptor(io), unix_path, own});
            asio::error_code ec;
            listener->acceptor.assign(protocol, fd, ec);
            if (ec) {
                ::close(fd);
                throw std::system_error(ec, "adopt the listening socket");
            }
            listener->acceptor.non_blocking(true, ec);  // for accept_one's accept
            if (ec) throw std::system_error(ec, "adopt the listening socket");  // the acceptor closes it
            auto& adopted = *listener;
            listeners.push_back(std::move(listener));
            accepting.enter();
            asio::co_spawn(io, shared_turns ? accept_shared(adopted) : accept_own(adopted),
                           [this](std::exception_ptr) { accepting.leave(); });
        };
        if (shared.config.listen_fd >= 0 && shared.config.listen_shared) {
            const auto slot = shared.config.accept_slot;
            if (shared.config.accept_counts && slot >= 0 && unsigned(slot) < shared.config.accept_slots) {
                accept_slots = reinterpret_cast<AcceptSlot*>(uintptr_t(shared.config.accept_counts));
                own_slot = &accept_slots[slot];
                own_pid = uint64_t(::getpid());
                if (shard == 0) {
                    // A restarted worker takes over its predecessor's line.
                    own_slot->connections.store(own_pid << 32, std::memory_order_relaxed);
                    own_slot->heartbeat_ms.store(monotonic_ms(), std::memory_order_release);
                    accept_heartbeat.emplace(io);
                    accepting.enter();
                    asio::co_spawn(io, beat_heartbeat(), [this](std::exception_ptr) { accepting.leave(); });
                }
            }
            if (shard == 0) {
                const int fd = shared.config.listen_fd;
                listen(listener_copy(fd), listener_protocol(fd), &shared.config.unix_path, false, true);
            }
        } else if (shared.config.listen_fd >= 0) {
            if (shard == 0) {
                const int fd = shared.config.listen_fd;
                listen(listener_copy(fd), listener_protocol(fd), &shared.config.unix_path, true, false);
            }
        } else if (shared.config.http_port) {
            if (shard == 0) {
                const int fd = bind_listener(shared.config.http_host, shared.config.http_port);
                listen(fd, listener_protocol(fd), &shared.config.unix_path, true, false);
            }
        }
        if (shared.config.worker_listen_fd >= 0 && shard == 0) {
            const int fd = shared.config.worker_listen_fd;
            listen(listener_copy(fd), listener_protocol(fd), &shared.config.worker_unix_path, true, false);
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
        settle(*it->second, std::move(decision));
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
            if (!waiter.paused) waiter.resumed.set();
            return;
        }
        if (kind >= DecisionKind::stream_data) {
            // Continuation of a streamed response that respond_application writes.
            if (!waiter.replied) return;
            waiter.chunks.push_back(Decision{code, std::move(body), 0, retire, {}, kind});
            // Several pieces may come in one batch: wake the writer once.
            waiter.more.set();
            return;
        }
        if (waiter.replied) return;
        waiter.replied = true;
        settle(waiter, Decision{code, std::move(body), 0, retire, std::move(headers), kind, content_length});
    }
    // The framework host's listeners accept no more connections (again: a
    // no-op). A listener of this process's own is shut down, as Sanic's
    // server.close(): the kernel refuses new connections or, with
    // SO_REUSEPORT, gives them to the other processes' sockets. On a socket
    // shared with other processes, this process leaves accepting to them.
    void stop_accepting() {
        for (auto* pause : accept_pauses) pause->cancel();
        for (auto& listener : listeners) {
            asio::error_code ignored;
            if (listener->own) ::shutdown(listener->acceptor.native_handle(), SHUT_RD);
            listener->acceptor.cancel(ignored);
        }
        if (own_slot && shard == 0) {
            // The siblings stop waiting for this process at once.
            stop_heartbeat();
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
    asio::awaitable<void> stop() {
        closing = true;
        stop_accepting();
        for (auto& [id, peer] : app_peers) {
            peer->shut_input();
            peer->shut_output();
        }
        for (auto& [id, flow] : body_flows) flow->wake();
        for (auto& [id, waiter] : waiters) {
            if (!waiter->replied) {
                waiter->replied = true;
                settle(*waiter, Decision{3, {}});
            } else {
                // A streamed response or tunnel sees `closing`.
                waiter->more.set();
                waiter->resumed.set();
            }
        }
        co_await accepting.close();
        co_await app_connections.close();
        listeners.clear();
        if (rdhup_fd) {
            watching = false;
            asio::error_code ignored;
            rdhup_fd->cancel(ignored);
            co_await peer_watch.close();
            rdhup_fd.reset();
            rdhup_set = -1;
        }
    }
};
}  // namespace

void NetworkServer::Impl::on_all(const std::function<void(unsigned)>& fn) {
    std::latch done(std::ptrdiff_t(contexts.size()));
    std::vector<std::exception_ptr> failures(contexts.size());
    for (unsigned shard = 0; shard < contexts.size(); ++shard) {
        asio::post(*contexts[shard], [&, shard] {
            try { fn(shard); } catch (...) { failures[shard] = std::current_exception(); }
            done.count_down();
        });
    }
    wake_poller();
    await_shards(done);
    for (auto& failure : failures)
        if (failure) std::rethrow_exception(failure);
}
void NetworkServer::Impl::on_all_async(const std::function<asio::awaitable<void>(unsigned)>& fn) {
    std::latch done(std::ptrdiff_t(contexts.size()));
    std::vector<std::exception_ptr> failures(contexts.size());
    for (unsigned shard = 0; shard < contexts.size(); ++shard) {
        asio::post(*contexts[shard], [&, shard] {
            asio::co_spawn(*contexts[shard], fn(shard), [&, shard](std::exception_ptr failure) {
                failures[shard] = failure;
                done.count_down();
            });
        });
    }
    wake_poller();
    await_shards(done);
    for (auto& failure : failures)
        if (failure) std::rethrow_exception(failure);
}
// The shard with the fewest connections handed to it.
unsigned NetworkServer::Impl::least_loaded_shard() const {
    unsigned best = 0;
    uint64_t fewest = std::numeric_limits<uint64_t>::max();
    for (unsigned shard = 0; shard < contexts.size(); ++shard) {
        const auto load = shared.counts[shard]->connections_assigned.load(std::memory_order_relaxed);
        if (load < fewest) {
            fewest = load;
            best = shard;
        }
    }
    return best;
}
void NetworkServer::Impl::hand_off(unsigned shard, Socket socket, Endpoint remote, const std::string* unix_path,
                                   bool counted) {
    ++shared.counts[shard]->connections_assigned;
    asio::post(*contexts[shard], [impl = shared_from_this(), shard, socket = std::move(socket), remote,
                                  unix_path, counted]() mutable {
        if (auto& service = impl->services[shard]) service->adopt(std::move(socket), remote, *unix_path, counted);
        else --impl->shared.counts[shard]->connections_assigned;
    });
}

NetworkServer::NetworkServer(ServerConfig c, std::vector<asio::io_context*> shards, std::function<void()> wake,
                             Offload offload, std::thread::id poller)
    : p(std::make_shared<Impl>(std::move(c), std::move(shards), std::move(wake), std::move(offload),
                               poller)) {}
NetworkServer::~NetworkServer() = default;
void NetworkServer::start() {
    std::lock_guard lock(p->lifecycle);
    p->on_all([impl = p.get()](unsigned shard) {
        impl->services[shard] = std::make_unique<Service>(*impl, shard, *impl->contexts[shard]);
    });
    p->started = true;
    p->shared.running = true;
    p->on_all([impl = p.get()](unsigned shard) { impl->services[shard]->start(); });
}
void NetworkServer::stop() {
    // Later calls find it stopped: the services stop once.
    std::lock_guard lock(p->lifecycle);
    if (p->stopped) return;
    p->stopped = true;
    p->shared.running = false;
    if (!p->started) return;
    std::exception_ptr failure;
    try {
        p->on_all_async([impl = p.get()](unsigned shard) -> asio::awaitable<void> {
            if (impl->services[shard]) co_await impl->services[shard]->stop();
        });
    } catch (...) { failure = std::current_exception(); }
    // Each service goes on its own shard, as its sockets and timers belong there.
    p->on_all([impl = p.get()](unsigned shard) { impl->services[shard].reset(); });
    if (failure) std::rethrow_exception(failure);
}
void NetworkServer::drain() {
    std::lock_guard lock(p->lifecycle);
    if (!p->started || p->stopped) return;
    p->on_all([impl = p.get()](unsigned shard) { impl->services[shard]->drain(); });
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
            // DONE has no reply and must remain drainable after the shards stop.
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
        asio::post(*p->contexts[shard], [impl = p, shard, batch = std::move(groups[shard])]() mutable {
            auto& service = impl->services[shard];
            if (!impl->shared.running || !service) return;
            for (auto& decision : batch) {
                service->resolve(decision.id, decision.code, std::move(decision.body), decision.retire,
                                 std::move(decision.headers), decision.kind, decision.content_length);
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
