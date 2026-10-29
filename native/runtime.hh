#pragma once
#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>
#include "server.hh"

namespace probe {
// File operations of the framework host (the framework's file responses), run by the
// runtime's file threads (on the shards' CPUs), as they may block.
// file_open: open(path) for reading and fstat it (a check before streaming).
enum FileOperation : int { file_stat = 0, file_read = 1, file_open = 2 };
struct FileResult {
    uint64_t id;
    int operation;
    std::string data;  // file_read: the bytes
    // file_stat, file_open: mode, ino, dev, nlink, uid, gid, size, atime, mtime,
    // ctime (ns, as signed values), blksize, blocks (512-byte units), rdev.
    std::array<uint64_t, 13> stat{};
    int error = 0;     // errno; 0 on success
};
struct ShardInfo {
    unsigned shard;
    int tid;
    std::vector<int> affinity;
    std::string backend;
};
struct Stats {
    unsigned live_workers;
    unsigned state; // 0 starting, 1 ready, 2 stopping, 3 stopped, 4 failed
    std::string error;
};
// The runtime itself, built twice: on epoll, in this extension, and on
// io_uring, in a library of its own that Runtime loads when asked for (Asio
// picks its backend when it is compiled). See Runtime for the methods.
class RuntimeBackend {
public:
    virtual ~RuntimeBackend() = default;
    virtual int fd() const = 0;
    virtual void clear_notification() = 0;
    virtual Stats stats() const = 0;
    virtual std::vector<ShardInfo> shards() const = 0;
    virtual void submit_file(uint64_t id, int operation, std::string path, uint64_t offset, int64_t length) = 0;
    virtual std::vector<FileResult> drain_files(size_t count) = 0;
    virtual std::vector<RequestEvent> drain_requests(size_t count) = 0;
    virtual void resolve_requests(std::vector<RequestDecision> decisions) = 0;
    virtual ServerSnapshot server_stats() const = 0;
    virtual uint64_t start_server(ServerConfig server) = 0;
    virtual void drain_server(uint64_t generation) = 0;
    virtual void stop_server(uint64_t generation) = 0;
    virtual void stop() = 0;
    virtual void join() = 0;
    virtual void close_fd() = 0;
    virtual std::size_t poll() = 0;
    virtual int reactor_fd() const = 0;
};
class Runtime {
    std::unique_ptr<RuntimeBackend> p;
    std::string fallback_;
public:
    // One shard per CPU of `cpus`: a thread pinned there runs its io_context.
    // reactor_backend: "epoll" or empty (the default), or "io_uring": the
    // io_uring build where this system allows it, else epoll (fallback()
    // says why). Anything else is refused.
    // same_thread: no shard thread; the one shard's io_context is polled by
    // the thread that made the runtime (Python's), which waits on reactor_fd()
    // and calls poll() (`cpus`: that thread's CPU, for the file threads), on
    // either backend.
    explicit Runtime(std::vector<int> cpus, ServerConfig server = {}, std::string reactor_backend = {},
                     bool same_thread = false);
    ~Runtime();
    Runtime(const Runtime&) = delete;
    Runtime& operator=(const Runtime&) = delete;
    // Why io_uring was asked for and epoll serves; empty otherwise.
    const std::string& fallback() const { return fallback_; }
    int fd() const { return p->fd(); }
    // Reads the notification fd: the first call each time it is readable,
    // before draining files and requests.
    void clear_notification() { p->clear_notification(); }
    Stats stats() const { return p->stats(); }
    std::vector<ShardInfo> shards() const { return p->shards(); }
    // A FileOperation; its FileResult comes from drain_files after the
    // notification. file_read reads `length` bytes from `offset` (-1: to the end).
    void submit_file(uint64_t id, int operation, std::string path, uint64_t offset, int64_t length) {
        p->submit_file(id, operation, std::move(path), offset, length);
    }
    std::vector<FileResult> drain_files(size_t count) { return p->drain_files(count); }
    std::vector<RequestEvent> drain_requests(size_t count) { return p->drain_requests(count); }
    void resolve_requests(std::vector<RequestDecision> decisions) { p->resolve_requests(std::move(decisions)); }
    ServerSnapshot server_stats() const { return p->server_stats(); }
    // Persistent use: start or stop the network server while the shards keep
    // running (one runtime per process). Blocking; release the GIL around
    // them. A stopped server keeps its stats until the next start.
    // start_server returns the server's generation; stop_server(generation)
    // stops that server only if it is still the current one (0: the current).
    uint64_t start_server(ServerConfig server) { return p->start_server(std::move(server)); }
    // A graceful stop's first step (no new connections, idle ones closed);
    // stop_server ends it. The same generation rule as stop_server.
    void drain_server(uint64_t generation = 0) { p->drain_server(generation); }
    void stop_server(uint64_t generation = 0) { p->stop_server(generation); }
    void stop() { p->stop(); }
    void join() { p->join(); }
    void close_fd() { p->close_fd(); }
    // Same-thread mode: runs the shard's ready work (no blocking) and returns
    // how many handlers ran. Call it when reactor_fd() is readable, after
    // clear_notification(), and after resolve_requests(); then drain_requests.
    std::size_t poll() { return p->poll(); }
    int reactor_fd() const { return p->reactor_fd(); }
};
}
