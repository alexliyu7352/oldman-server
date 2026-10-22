#include "work.hh"
#include <zlib.h>
#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <future>
#include <limits>
#include <mutex>
#include <pthread.h>
#include <sched.h>
#include <stdexcept>
#include <sys/syscall.h>
#include <thread>
#include <time.h>
#include <unistd.h>

namespace probe {
uint64_t clock_ns() {
    timespec t{};
    ::clock_gettime(CLOCK_MONOTONIC, &t);
    return uint64_t(t.tv_sec) * 1000000000 + t.tv_nsec;
}
uint64_t thread_cpu_ns() {
    timespec t{};
    ::clock_gettime(CLOCK_THREAD_CPUTIME_ID, &t);
    return uint64_t(t.tv_sec) * 1000000000 + t.tv_nsec;
}
void validate_level(int level) {
    if (level < -1 || level > 9) throw std::invalid_argument("zlib level must be -1..9");
}
size_t compression_reservation(size_t size) {
    if (size > std::numeric_limits<uInt>::max() / 2)
        throw std::invalid_argument("probe input must be smaller than 2 GiB");
    return size + ::compressBound(size);
}
struct Compressor::Impl {
    z_stream stream{};
    const std::string& input;
    std::string output;
    size_t position = 0;
    bool done = false;
    Impl(const std::string& in, int level) : input(in) {
        validate_level(level);
        compression_reservation(input.size());
        // Allocate before initializing zlib: no initialized stream to leak if allocation fails.
        output.resize(::compressBound(input.size()));
        int rc = ::deflateInit(&stream, level);
        if (rc != Z_OK) throw std::runtime_error("deflateInit failed: " + std::to_string(rc));
        stream.next_out = reinterpret_cast<Bytef*>(output.data());
        stream.avail_out = output.size();
    }
    ~Impl() { ::deflateEnd(&stream); }
};
Compressor::Compressor(const std::string& input, int level) : p(std::make_unique<Impl>(input, level)) {}
Compressor::~Compressor() = default;
bool Compressor::step() {
    if (p->done) return true;
    size_t n = std::min<size_t>(32768, p->input.size() - p->position);
    p->stream.next_in = reinterpret_cast<Bytef*>(const_cast<char*>(p->input.data() + p->position));
    p->stream.avail_in = n;
    p->position += n;
    bool last = p->position == p->input.size();
    int rc = ::deflate(&p->stream, last ? Z_FINISH : Z_NO_FLUSH);
    if ((last && rc != Z_STREAM_END) || (!last && rc != Z_OK) || p->stream.avail_in)
        throw std::runtime_error("deflate step failed: " + std::to_string(rc));
    p->done = last;
    return p->done;
}
std::string Compressor::finish() {
    if (!p->done) throw std::logic_error("compression not finished");
    p->output.resize(p->stream.total_out);
    return std::move(p->output);
}
WorkResult compress_native(const std::string& input, int level) {
    WorkResult r;
    r.metrics.work_tid = ::syscall(SYS_gettid);
    r.metrics.work_start_ns = clock_ns();
    auto cpu = thread_cpu_ns();
    {
        Compressor compressor(input, level);
        while (!compressor.step()) {}
        r.payload = compressor.finish();
    }
    r.metrics.compute_cpu_ns = thread_cpu_ns() - cpu;
    r.metrics.work_end_ns = clock_ns();
    r.metrics.algorithm_ns = r.metrics.work_end_ns - r.metrics.work_start_ns;
    return r;
}
WorkResult wait_native(unsigned milliseconds) {
    if (milliseconds > 60000) throw std::invalid_argument("wait must be 0..60000 ms");
    WorkResult r;
    r.metrics.work_tid = ::syscall(SYS_gettid);
    r.metrics.work_start_ns = clock_ns();
    auto cpu = thread_cpu_ns();
    std::this_thread::sleep_for(std::chrono::milliseconds(milliseconds));
    r.metrics.compute_cpu_ns = thread_cpu_ns() - cpu;
    r.metrics.work_end_ns = clock_ns();
    r.metrics.algorithm_ns = r.metrics.work_end_ns - r.metrics.work_start_ns;
    return r;
}

struct WorkerPool::Impl {
    mutable std::mutex mutex;
    std::condition_variable wake;
    std::deque<std::move_only_function<void()>> queue;
    std::vector<std::thread> threads;
    std::vector<WorkerInfo> info;
    size_t limit, active = 0, live = 0;
    bool stopping = false;
    explicit Impl(size_t n) : limit(n) {}
    void run(int cpu, std::promise<void> ready) noexcept {
        try {
            cpu_set_t set;
            CPU_ZERO(&set);
            CPU_SET(cpu, &set);
            if (::pthread_setaffinity_np(::pthread_self(), sizeof(set), &set))
                throw std::runtime_error("worker affinity setup failed");
            ::pthread_setname_np(::pthread_self(), "probe-compute");
            CPU_ZERO(&set);
            if (::sched_getaffinity(0, sizeof(set), &set))
                throw std::runtime_error("worker affinity inspection failed");
            WorkerInfo w{int(::syscall(SYS_gettid)), {}};
            for (int i = 0; i < CPU_SETSIZE; ++i) if (CPU_ISSET(i, &set)) w.affinity.push_back(i);
            {
                std::lock_guard lock(mutex);
                info.push_back(std::move(w));
                ++live;
            }
            ready.set_value();
        } catch (...) {
            ready.set_exception(std::current_exception());
            return;
        }
        for (;;) {
            std::move_only_function<void()> job;
            {
                std::unique_lock lock(mutex);
                wake.wait(lock, [&] { return stopping || !queue.empty(); });
                if (queue.empty()) break;
                job = std::move(queue.front());
                queue.pop_front();
                ++active;
            }
            job(); // runtime jobs catch algorithm errors and deliver them to their shard
            {
                std::lock_guard lock(mutex);
                --active;
            }
        }
        std::lock_guard lock(mutex);
        --live;
    }
    void close() {
        {
            std::lock_guard lock(mutex);
            stopping = true;
        }
        wake.notify_all();
        for (auto& t : threads) if (t.joinable()) t.join();
    }
    ~Impl() { close(); }
};
WorkerPool::WorkerPool(const std::vector<int>& cpus, size_t limit) : p(std::make_unique<Impl>(limit)) {
    p->threads.reserve(cpus.size());
    for (int cpu : cpus) {
        if (cpu < 0 || cpu >= CPU_SETSIZE) throw std::invalid_argument("worker CPU out of range");
        std::promise<void> ready;
        auto f = ready.get_future();
        p->threads.emplace_back([impl=p.get(), cpu, ready=std::move(ready)]() mutable {
            impl->run(cpu, std::move(ready));
        });
        f.get();
    }
}
WorkerPool::~WorkerPool() = default;
void WorkerPool::submit(std::move_only_function<void()> work) {
    {
        std::lock_guard lock(p->mutex);
        if (p->stopping || p->queue.size() >= p->limit) throw WorkerQueueFull();
        p->queue.push_back(std::move(work));
    }
    p->wake.notify_one();
}
void WorkerPool::close() { p->close(); }
PoolStats WorkerPool::stats() const {
    std::lock_guard lock(p->mutex);
    return {p->queue.size(), p->active, p->live};
}
std::vector<WorkerInfo> WorkerPool::workers() const {
    std::lock_guard lock(p->mutex);
    return p->info;
}
}
