#pragma once
#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>
#include "server.hh"

namespace probe {
// File operations of the framework host (Sanic's file responses), run on the
// shards with Seastar's file API: page-cache reads complete on the shard and
// reads that would block go to Seastar's syscall thread.
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
class Runtime {
    struct Impl;
    std::unique_ptr<Impl> p;
public:
    // reactor_backend: Seastar's --reactor-backend (epoll, linux-aio,
    // io_uring); empty lets Seastar take the first available of io_uring,
    // linux-aio and epoll.
    explicit Runtime(std::vector<int> cpus, ServerConfig server = {}, std::string reactor_backend = {});
    ~Runtime();
    Runtime(const Runtime&) = delete;
    Runtime& operator=(const Runtime&) = delete;
    int fd() const;
    // Reads the notification fd: the first call each time it is readable,
    // before draining files and requests.
    void clear_notification();
    Stats stats() const;
    std::vector<ShardInfo> shards() const;
    // A FileOperation on a shard; its FileResult comes from drain_files after
    // the notification. file_read reads `length` bytes from `offset` (-1: to the end).
    void submit_file(uint64_t id, int operation, std::string path, uint64_t offset, int64_t length);
    std::vector<FileResult> drain_files(size_t count);
    std::vector<RequestEvent> drain_requests(size_t count);
    void resolve_requests(std::vector<RequestDecision> decisions);
    ServerSnapshot server_stats() const;
    // Persistent use: start or stop the network server while Seastar keeps
    // running (Seastar itself starts once per process). Blocking; release the
    // GIL around them. A stopped server keeps its stats until the next start.
    // start_server returns the server's generation; stop_server(generation)
    // stops that server only if it is still the current one (0: the current).
    uint64_t start_server(ServerConfig server);
    // A graceful stop's first step (no new connections, idle ones closed);
    // stop_server ends it. The same generation rule as stop_server.
    void drain_server(uint64_t generation = 0);
    void stop_server(uint64_t generation = 0);
    void stop();
    void join();
    void close_fd();
};
}
