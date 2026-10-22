#pragma once
#include <stdexcept>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace probe {
class WorkerQueueFull : public std::runtime_error {
public:
    WorkerQueueFull() : std::runtime_error("native worker queue exhausted") {}
};
uint64_t clock_ns();
uint64_t thread_cpu_ns();
struct Metrics {
    uint64_t input_copy_ns = 0, output_copy_ns = 0, submitted_ns = 0;
    uint64_t dispatch_ns = 0, queue_ns = 0, algorithm_ns = 0, compute_cpu_ns = 0;
    uint64_t work_start_ns = 0, work_end_ns = 0, return_to_shard_ns = 0;
    uint64_t completion_ns = 0, notification_wait_ns = 0, cooperative_yields = 0;
    int work_tid = 0, shard_tid = 0;
};
struct WorkResult {
    std::string payload;
    Metrics metrics;
    std::string error;
};
size_t compression_reservation(size_t input_size);
void validate_level(int level);
class Compressor {
    struct Impl;
    std::unique_ptr<Impl> p;
public:
    Compressor(const std::string& input, int level);
    ~Compressor();
    bool step(); // true when complete; at most 32 KiB new input, no flush between chunks
    std::string finish();
};
WorkResult compress_native(const std::string& input, int level);
WorkResult wait_native(unsigned milliseconds);
struct WorkerInfo { int tid; std::vector<int> affinity; };
struct PoolStats { size_t queued, active, live; };
class WorkerPool {
    struct Impl;
    std::unique_ptr<Impl> p;
public:
    WorkerPool(const std::vector<int>& cpus, size_t limit);
    ~WorkerPool();
    void submit(std::move_only_function<void()> work);
    void close();
    PoolStats stats() const;
    std::vector<WorkerInfo> workers() const;
};
}
