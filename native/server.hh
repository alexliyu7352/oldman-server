#pragma once
#include "work.hh"
#include <seastar/core/future.hh>
#include <seastar/core/alien.hh>
#include <atomic>
#include <functional>
#include <map>

namespace probe {
struct ServerConfig {
    unsigned http_port = 0, tcp_port = 0, udp_port = 0;
    size_t max_requests = 256, max_bytes = 64 * 1024 * 1024, max_body_bytes = 1024 * 1024;
    unsigned decision_timeout_ms = 1000;
    std::string file_path;
    bool coalesce_notifications = false;
    bool cooperative_compression = false;
    bool inline_compression = false;
    std::vector<unsigned> upstream_ports; // Bounded loopback HTTP experiment.
    std::vector<std::pair<std::string, std::string>> application_routes;
    bool generate_date_header = false;
    size_t max_response_bytes = 65536;
    // Diagnostic response headers on the framework host: "all" (pid, shard,
    // policy), "pid" (X-Probe-Pid only) or "none".
    std::string probe_headers = "all";
    // Framework host (application_dispatch_all) HTTP/1.1 settings. Timeouts of
    // 0 disable the deadline; the defaults keep the earlier probe behavior.
    std::string http_host = "127.0.0.1";
    // Instead of binding http_host:http_port: a listening socket (TCP or
    // AF_UNIX) that Python bound and keeps open, as Sanic does for its
    // server. Shard 0 accepts on a duplicate and hands connections to the
    // other shards. Requests over an AF_UNIX one report unix_path as the
    // server address.
    int listen_fd = -1;
    std::string unix_path;
    // Other worker processes accept on listen_fd too: none may shut it down
    // (Seastar's abort_accept does) when it stops.
    bool listen_shared = false;
    // Worker processes that accept on one shared listen_fd take turns by load:
    // the address of accept_slots AcceptSlots in shared memory (0: none) and
    // this process's slot (-1: not taking part). See accept_application.
    uint64_t accept_counts = 0;
    unsigned accept_slots = 0;
    int accept_slot = -1;
    // This process's own listening socket besides listen_fd (NATIVE_WORKER_LISTEN),
    // and for AF_UNIX the path its requests report as the server address.
    int worker_listen_fd = -1;
    std::string worker_unix_path;
    size_t max_header_bytes = 8192;
    bool keep_alive = true;
    unsigned keep_alive_timeout_ms = 0, request_timeout_ms = 0;
    // A write (of a response, or a piece of a large one) that the peer does
    // not take within this time closes the connection: a client that stops
    // reading holds the connection and the response's memory only that long.
    unsigned write_timeout_ms = 0;
    // Protocol errors go to Python as error requests and body error events (the
    // framework renders them, as Sanic's error handler does); otherwise the host
    // answers them with a plain error response.
    bool framework_errors = false;
    bool native_json = false;
    bool application_dispatch_all = false; // External framework owns all HTTP routing.
    bool enabled() const { return http_port || tcp_port || udp_port || listen_fd >= 0 || worker_listen_fd >= 0; }
};
// One worker process's line in the shared accept counts. connections: the
// owning process's pid in the high 32 bits and its open connections in the low
// 32. A restarted worker takes over the line while the one it replaces may
// still be closing connections; that one changes the line only while the pid
// is its own. heartbeat_ms: when the owner's shard 0 last showed it is
// accepting (CLOCK_MONOTONIC ms; 0 when it is not).
struct AcceptSlot {
    std::atomic<uint64_t> connections, heartbeat_ms;
    uint64_t padding[6];
};
static_assert(sizeof(AcceptSlot) == 64 && std::atomic<uint64_t>::is_always_lock_free);
struct RequestEvent {
    uint64_t id;
    unsigned shard;
    std::string method, route;
    size_t body_size;
    bool copied = false;
    std::string body;
    std::string parameter;
    uint64_t parent = 0;
    // Terminal notifications retain native admission until Python drains them.
    // The reservation contains atomics only and may be released on that thread.
    std::shared_ptr<void> reservation;
    std::string target;
    std::vector<std::pair<std::string, std::string>> headers;
    std::string http_version;
    std::pair<std::string, unsigned> client, server;
    // Framework requests: the connection ((id << 16) | shard) and the request's
    // 1-based position on it.
    uint64_t connection = 0;
    unsigned sequence = 0;
    bool unix_socket = false;  // client is empty and server the socket path
};
// Kinds of framework-host decisions: a complete response, or a streamed one
// (start with status/headers/first data, then data, end or abort).
// A stream_start with status 101 switches the connection to a tunnel (WebSocket):
// raw bytes both ways; tunnel_pause/resume stop and restart reading the peer.
// stream_file writes part of a file into an open stream (Sanic's file_stream):
// body "<offset> <length> <chunk size> <path>"; the shard reads it and reports
// application_file when done.
enum DecisionKind : int { complete = 0, stream_start = 1, stream_data = 2, stream_end = 3, stream_abort = 4,
                          tunnel_pause = 5, tunnel_resume = 6, stream_file = 7 };
struct RequestDecision {
    uint64_t id;
    unsigned shard;
    int code;
    std::string body;
    bool retire = false; // Python no longer needs a terminal notification.
    std::vector<std::pair<std::string, std::string>> headers;
    int kind = DecisionKind::complete;
    int64_t content_length = -1;  // stream_start: application Content-Length, or -1
};
// upgrade: the status of a framework host stream start, where a 101 begins a tunnel.
bool valid_http_response(int status,
    const std::vector<std::pair<std::string, std::string>>& headers, bool no_body_statuses = false,
    bool upgrade = false);
// A deliberately small, typed native operation contract, not a Python compiler.
// Each shard owns a copy; version acknowledges an update on every shard.
struct NativePolicy {
    uint64_t version = 0;
    bool allow_fixed = false, allow_file = false, allow_compress = false;
    size_t max_payload_bytes = 1024 * 1024;
};
struct ServerSnapshot {
    std::map<std::string, uint64_t> totals;
    std::vector<std::map<std::string, uint64_t>> shards;
};
class NetworkServer {
    struct Impl;
    std::unique_ptr<Impl> p;
public:
    using Compress = std::function<seastar::future<WorkResult>(std::string, int)>;
    NetworkServer(ServerConfig config, unsigned shards, seastar::alien::instance& alien,
                  std::function<void()> notify, Compress compress);
    ~NetworkServer();
    seastar::future<> start();
    seastar::future<> stop();
    // A graceful stop's first step: see Service::drain.
    seastar::future<> drain();
    seastar::future<> install_policy(NativePolicy policy);
    std::vector<RequestEvent> drain(size_t count);
    void resolve(uint64_t id, unsigned shard, int decision, std::string body, bool retire = false);
    void resolve_batch(std::vector<RequestDecision> decisions);
    bool has_events() const;
    ServerSnapshot snapshot() const;
};
}
