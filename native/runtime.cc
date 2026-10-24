#include "runtime.hh"
#include <asio.hpp>
#include <atomic>
#include <array>
#include <cerrno>
#include <condition_variable>
#include <cstdio>
#include <deque>
#include <dlfcn.h>
#include <latch>
#include <mutex>
#include <optional>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdexcept>
#include <sys/eventfd.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/utsname.h>
#include <fcntl.h>
#include <thread>
#include <unistd.h>

namespace probe {
namespace {
#ifdef OLDMAN_NATIVE_URING
constexpr const char* backend_name = "io_uring";
#else
constexpr const char* backend_name = "epoll";
#endif
// The same-thread mode's poller waits on the shard reactor's descriptor, which
// Asio keeps private (epoll_reactor::epoll_fd_; io_uring_service::rings_, one
// ring by default): the explicit instantiation below reads the member
// (access checking does not apply to its names).
static_assert(ASIO_VERSION == 103802, "check the reactor members read below in this Asio version");
template <auto Member, typename Tag>
struct Expose {
    friend auto member(Tag) { return Member; }
};
struct ReactorMember {
    friend auto member(ReactorMember);
};
#ifdef OLDMAN_NATIVE_URING
template struct Expose<&asio::detail::io_uring_service::rings_, ReactorMember>;
int reactor_descriptor_of(asio::io_context& context) {
    auto& service = asio::use_service<asio::detail::io_uring_service>(context);
    service.init_task();
    return (service.*member(ReactorMember{})).front().ring_.ring_fd;
}
#else
template struct Expose<&asio::detail::epoll_reactor::epoll_fd_, ReactorMember>;
int reactor_descriptor_of(asio::io_context& context) {
    auto& reactor = asio::use_service<asio::detail::epoll_reactor>(context);
    reactor.init_task();
    return reactor.*member(ReactorMember{});
}
#endif
// A shard's io_context, run by one thread. Its io_uring ring has 1024 entries
// rather than Asio's 16384: the kernel allocates a ring in physically
// contiguous pages, which a fragmented machine may lack for a large one (setup
// then fails with ENOMEM); with fewer entries Asio submits sooner, and the
// kernel keeps completions that overflow the queue.
std::unique_ptr<asio::io_context> make_context() {
    return std::make_unique<asio::io_context>(
        asio::config_from_string("scheduler.concurrency_hint=1\nreactor.io_uring_ring_size=1024"));
}
// Signals go to Python's main thread: the runtime's threads take none.
void block_signals() {
    sigset_t all;
    sigfillset(&all);
    pthread_sigmask(SIG_BLOCK, &all, nullptr);
}
std::vector<int> thread_affinity() {
    cpu_set_t set;
    CPU_ZERO(&set);
    std::vector<int> cpus;
    if (::sched_getaffinity(0, sizeof(set), &set) != 0) throw std::runtime_error("sched_getaffinity failed");
    for (int i = 0; i < CPU_SETSIZE; ++i)
        if (CPU_ISSET(i, &set)) cpus.push_back(i);
    return cpus;
}
void pin(const std::vector<int>& cpus) {
    cpu_set_t set;
    CPU_ZERO(&set);
    for (int cpu : cpus) CPU_SET(cpu, &set);
    if (::sched_setaffinity(0, sizeof(set), &set) != 0)
        throw std::system_error(errno, std::system_category(), "pin a native thread to its CPUs");
}

// Blocking file work (open, stat, read): threads on the shards' CPUs, so that
// the Python thread's CPU stays its own and no shard waits for a disk.
class FileThreads {
public:
    FileThreads(const std::vector<int>& cpus, unsigned count) {
        for (unsigned i = 0; i < count; ++i)
            threads_.emplace_back([this, cpus] {
                block_signals();
                try { pin(cpus); } catch (...) {}
                run();
            });
    }
    ~FileThreads() { close(); }
    void post(std::function<void()> job) {
        {
            std::lock_guard lock(mutex_);
            jobs_.push_back(std::move(job));
        }
        ready_.notify_one();
    }
    // Runs what is queued, then ends the threads.
    void close() {
        {
            std::lock_guard lock(mutex_);
            closing_ = true;
        }
        ready_.notify_all();
        for (auto& thread : threads_)
            if (thread.joinable()) thread.join();
    }
private:
    void run() {
        for (;;) {
            std::function<void()> job;
            {
                std::unique_lock lock(mutex_);
                ready_.wait(lock, [this] { return closing_ || !jobs_.empty(); });
                if (jobs_.empty()) return;
                job = std::move(jobs_.front());
                jobs_.pop_front();
            }
            job();
        }
    }
    std::mutex mutex_;
    std::condition_variable ready_;
    std::deque<std::function<void()>> jobs_;
    bool closing_ = false;
    std::vector<std::thread> threads_;
};

uint64_t ns(const timespec& t) { return uint64_t(int64_t(t.tv_sec) * 1'000'000'000 + t.tv_nsec); }
// os.stat order, the three times in nanoseconds.
std::array<uint64_t, 13> stat_numbers(const struct stat& st) {
    return {st.st_mode, st.st_ino, st.st_dev, st.st_nlink, st.st_uid, st.st_gid, uint64_t(st.st_size),
            ns(st.st_atim), ns(st.st_mtim), ns(st.st_ctim), uint64_t(st.st_blksize), uint64_t(st.st_blocks),
            st.st_rdev};
}
// A FileOperation, as Python's os.stat and open() / read() would do it.
FileResult execute_file(uint64_t id, int operation, const std::string& path, uint64_t offset, int64_t length) {
    FileResult result{id, operation};
    struct stat st{};
    if (operation == FileOperation::file_stat) {
        if (::stat(path.c_str(), &st) < 0) result.error = errno;
        else result.stat = stat_numbers(st);
        return result;
    }
    const int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
    if (fd < 0) {
        result.error = errno;
        return result;
    }
    if (::fstat(fd, &st) < 0) {
        result.error = errno;
    } else if (S_ISDIR(st.st_mode)) {
        result.error = EISDIR;  // open() refuses a directory, as Python's does
    } else {
        if (operation == FileOperation::file_open) result.stat = stat_numbers(st);
        const uint64_t size = operation == FileOperation::file_read ? uint64_t(st.st_size) : 0;
        const uint64_t end = length < 0 ? size : std::min<uint64_t>(size, offset + uint64_t(length));
        if (offset < end) {
            result.data.resize(end - offset);
            size_t got = 0;
            while (got < result.data.size()) {
                const auto n = ::pread(fd, result.data.data() + got, result.data.size() - got, off_t(offset + got));
                if (n < 0 && errno == EINTR) continue;
                if (n < 0) {
                    result.error = errno;
                    break;
                }
                if (n == 0) break;  // the file became shorter
                got += size_t(n);
            }
            result.data.resize(got);
        }
    }
    ::close(fd);
    return result;
}
struct Impl {
    std::vector<int> cpus;
    ServerConfig server_config;
    // No shard thread: the thread that made the runtime polls the one shard's
    // io_context (Runtime::poll) when its reactor's descriptor is readable.
    const bool same_thread;
    std::thread::id poller;  // the thread that made the runtime, in that mode
    int reactor_descriptor = -1;
    std::atomic<bool> pollable{false};
    // One io_context per shard, each run by its own thread on its CPU.
    std::vector<std::unique_ptr<asio::io_context>> contexts;
    std::vector<asio::executor_work_guard<asio::io_context::executor_type>> keep_running;
    std::vector<std::thread> shard_threads;
    std::unique_ptr<FileThreads> files;
    std::unique_ptr<NetworkServer> server;
    uint64_t server_generation = 0;  // of `server`; under mutex
    bool server_running = false;  // under lifecycle
    int notify_fd;
    std::atomic<bool> notify_pending{false};
    // The current server's setting that any thread reads without the mutex
    // (start_server replaces server_config under it).
    std::atomic<bool> coalesce{false};
    void adopt(const ServerConfig& config) {
        coalesce.store(config.coalesce_notifications, std::memory_order_relaxed);
    }
    std::atomic<uint64_t> notification_writes{0};
    std::thread thread;  // starts and stops the shards
    const pid_t owner = ::getpid();  // a forked child has a copy, not the threads
    // Held across each whole start_server / drain_server / stop_server call
    // and stop(): the server does not change under them.
    std::mutex lifecycle;
    mutable std::mutex mutex;
    std::deque<FileResult> file_results;
    std::vector<ShardInfo> shard_info;
    Stats status{};
    std::condition_variable stopping;
    bool stop_requested = false;  // under mutex

    Impl(std::vector<int> c, ServerConfig config, bool same)
        : cpus(std::move(c)), server_config(std::move(config)), same_thread(same),
          notify_fd(::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC)) {
        if (notify_fd < 0) throw std::runtime_error("eventfd creation failed");
        adopt(server_config);
        // The shards' io_contexts, made here rather than on their threads: a
        // backend the kernel refuses fails the construction, which the io_uring
        // build's entry point turns into the fallback to epoll. In the
        // same-thread mode this is the polling thread, which then waits on the
        // reactor before the control thread posts the server's start; the work
        // guards keep an idle poll() from stopping the context.
        try {
            for (size_t i = 0; i < cpus.size(); ++i) {
                contexts.push_back(make_context());
                keep_running.push_back(asio::make_work_guard(*contexts.back()));
            }
            if (same_thread) {
                poller = std::this_thread::get_id();
                reactor_descriptor = reactor_descriptor_of(*contexts.front());
                pollable = true;
            }
        } catch (...) {
            keep_running.clear();
            contexts.clear();
            ::close(notify_fd);
            throw;
        }
    }
    ~Impl() { if (notify_fd >= 0) ::close(notify_fd); }
    void notify() noexcept {
        if (coalesce.load(std::memory_order_relaxed) &&
            notify_pending.exchange(true, std::memory_order_acq_rel)) return;
        eventfd_t n = 1;
        int rc;
        do { rc = ::eventfd_write(notify_fd, n); } while (rc < 0 && errno == EINTR);
        if (rc == 0) notification_writes.fetch_add(1, std::memory_order_relaxed);
        // EAGAIN means a notification is already pending.
    }
    std::unique_ptr<NetworkServer> make_server(ServerConfig config) {
        std::vector<asio::io_context*> shards;
        for (auto& context : contexts) shards.push_back(context.get());
        return std::make_unique<NetworkServer>(
            std::move(config), std::move(shards), [this] { notify(); },
            [this](std::function<void()> job) {
                // The job posts its result to the shard, whose poller hears of
                // posts from other threads only through the notification.
                if (same_thread) files->post([this, job = std::move(job)] { job(); notify(); });
                else files->post(std::move(job));
            },
            poller);
    }
    // The shards: each pinned to its CPU and reporting its thread before it runs.
    void start_shards() {
        if (same_thread) return;  // no thread runs its context
        std::latch running(std::ptrdiff_t(cpus.size()));
        std::vector<std::exception_ptr> failures(cpus.size());
        shard_info.resize(cpus.size());
        for (unsigned i = 0; i < cpus.size(); ++i) {
            shard_threads.emplace_back([this, i, &running, &failures] {
                block_signals();
                try {
                    pin({cpus[i]});
                    auto info = ShardInfo{i, int(::syscall(SYS_gettid)), thread_affinity(), backend_name};
                    std::lock_guard lock(mutex);
                    shard_info[i] = std::move(info);
                } catch (...) {
                    failures[i] = std::current_exception();
                }
                auto& context = *contexts[i];
                running.count_down();
                context.run();
            });
        }
        running.wait();
        for (auto& failure : failures)
            if (failure) std::rethrow_exception(failure);
    }
    void stop_shards() {
        keep_running.clear();
        for (auto& context : contexts) context->stop();
        if (same_thread) {  // the poller may still call poll(): the context goes in close_fd()
            pollable = false;
            return;
        }
        for (auto& thread : shard_threads)
            if (thread.joinable()) thread.join();
        shard_threads.clear();
        contexts.clear();
    }
    void fail(const std::string& error) {
        if (server) {
            try { server->stop(); } catch (...) {}
        }
        stop_shards();
        if (files) files->close();
        std::lock_guard lock(mutex);
        server.reset();
        status.state = 4;
        status.live_workers = 0;
        status.error = error;
    }
    void run() noexcept {
        block_signals();
        try {
            files = std::make_unique<FileThreads>(cpus, unsigned(std::max<size_t>(2, cpus.size())));
            start_shards();
            if (server_config.enabled()) {
                auto network = make_server(server_config);
                try {
                    network->start();
                } catch (...) {
                    network->stop();
                    throw;
                }
                std::lock_guard lock(mutex);
                server = std::move(network);
            }
            {
                std::lock_guard lock(mutex);
                status.state = 1;
                status.live_workers = unsigned(cpus.size());
            }
            notify();
            {
                std::unique_lock lock(mutex);
                stopping.wait(lock, [this] { return stop_requested; });
            }
            if (server) server->stop();
            server_running = false;
            stop_shards();
            files->close();
            std::lock_guard lock(mutex);
            status.live_workers = 0;
            status.state = 3;
        } catch (const std::exception& e) {
            fail(e.what());
        } catch (...) {
            fail("unknown native startup exception");
        }
        notify();
    }
};

// The framework host is the only server: a listener needs framework dispatch,
// and framework dispatch HTTP or a listening socket.
void validate_server(const ServerConfig& config) {
    if (config.enabled() && !config.application_dispatch_all)
        throw std::invalid_argument("a server requires framework dispatch (application_dispatch_all)");
    if (config.application_dispatch_all && !config.http_port && config.listen_fd < 0)
        throw std::invalid_argument("framework dispatch requires an HTTP port or a listening socket");
}
class AsioRuntime final : public RuntimeBackend {
    std::unique_ptr<Impl> p;
public:
    AsioRuntime(std::vector<int> cpus, ServerConfig config, bool same_thread = false);
    ~AsioRuntime() override;
    int fd() const override;
    void clear_notification() override;
    Stats stats() const override;
    std::vector<ShardInfo> shards() const override;
    void submit_file(uint64_t id, int operation, std::string path, uint64_t offset, int64_t length) override;
    std::vector<FileResult> drain_files(size_t count) override;
    std::vector<RequestEvent> drain_requests(size_t count) override;
    void resolve_requests(std::vector<RequestDecision> decisions) override;
    ServerSnapshot server_stats() const override;
    uint64_t start_server(ServerConfig server) override;
    void drain_server(uint64_t generation) override;
    void stop_server(uint64_t generation) override;
    void stop() override;
    void join() override;
    void close_fd() override;
    std::size_t poll() override;
    int reactor_fd() const override;
};
AsioRuntime::AsioRuntime(std::vector<int> cpus, ServerConfig config, bool same_thread)
    : p(std::make_unique<Impl>(std::move(cpus), std::move(config), same_thread)) {
    p->thread = std::thread([this] { p->run(); });
}
AsioRuntime::~AsioRuntime() {
    if (p->owner != ::getpid()) {
        // A forked child's copy: the threads are the parent's. Joining them,
        // or destroying condition variables they wait on, hangs: the copy is
        // left alone.
        (void)p.release();
        return;
    }
    // Engine.aclose is mandatory. A forgotten live runtime cannot be detached safely.
    if (p->thread.joinable()) p->thread.join();
    close_fd();
}
int AsioRuntime::fd() const { return p->notify_fd; }
void AsioRuntime::submit_file(uint64_t id, int operation, std::string path, uint64_t offset, int64_t length) {
    if (operation < FileOperation::file_stat || operation > FileOperation::file_open)
        throw std::invalid_argument("unknown file operation");
    std::lock_guard lock(p->mutex);
    if (p->status.state != 1) throw std::runtime_error("runtime not accepting work");
    p->files->post([impl = p.get(), id, operation, path = std::move(path), offset, length] {
        auto result = execute_file(id, operation, path, offset, length);
        {
            std::lock_guard lock(impl->mutex);
            impl->file_results.push_back(std::move(result));
        }
        impl->notify();
    });
}
std::vector<FileResult> AsioRuntime::drain_files(size_t count) {
    std::vector<FileResult> out;
    bool more;
    {
        std::lock_guard lock(p->mutex);
        while (out.size() < count && !p->file_results.empty()) {
            out.push_back(std::move(p->file_results.front()));
            p->file_results.pop_front();
        }
        more = !p->file_results.empty();
    }
    if (more) p->notify();
    return out;
}
void AsioRuntime::clear_notification() {
    eventfd_t value;
    while (::eventfd_read(p->notify_fd, &value) < 0 && errno == EINTR) {}
    // Reset before inspecting either queue: a concurrent producer is either
    // covered by this drain or posts a new wake. Queue mutexes publish its data.
    // Also when not coalescing: a flag left from an earlier coalescing server
    // would otherwise swallow the next one's first wake.
    p->notify_pending.store(false, std::memory_order_release);
    bool more;
    {
        std::lock_guard lock(p->mutex);
        // In coalesced mode drain_requests() owns metadata rearming. Rearming
        // here, just before Python drains that queue, creates an empty callback.
        more = !p->coalesce.load(std::memory_order_relaxed) && p->server && p->server->has_events();
    }
    if (more) p->notify();
}
Stats AsioRuntime::stats() const {
    std::lock_guard lock(p->mutex);
    return p->status;
}
std::vector<ShardInfo> AsioRuntime::shards() const {
    std::lock_guard lock(p->mutex);
    return p->shard_info;
}
std::vector<RequestEvent> AsioRuntime::drain_requests(size_t count) {
    std::lock_guard lock(p->mutex);
    if (!p->server) return {};
    auto out = p->server->drain(count);
    if (p->server->has_events()) p->notify();
    return out;
}
void AsioRuntime::resolve_requests(std::vector<RequestDecision> decisions) {
    std::lock_guard lock(p->mutex);
    if (p->server && p->status.state < 2) p->server->resolve_batch(std::move(decisions));
}
ServerSnapshot AsioRuntime::server_stats() const {
    std::lock_guard lock(p->mutex);
    auto out = p->server ? p->server->snapshot() : ServerSnapshot{};
    out.totals["notification_writes"] = p->notification_writes.load(std::memory_order_relaxed);
    return out;
}
uint64_t AsioRuntime::start_server(ServerConfig config) {
    validate_server(config);
    std::lock_guard life(p->lifecycle);
    {
        std::lock_guard lock(p->mutex);
        if (p->status.state != 1) throw std::runtime_error("runtime is not running");
    }
    if (p->server_running) throw std::runtime_error("a network server is already running");
    auto network = p->make_server(config);
    try {
        network->start();
    } catch (...) {
        network->stop();
        throw;
    }
    std::unique_ptr<NetworkServer> previous;  // stopped already
    uint64_t generation;
    {
        std::lock_guard lock(p->mutex);
        previous = std::move(p->server);
        p->server = std::move(network);
        p->adopt(config);
        p->server_config = std::move(config);
        generation = ++p->server_generation;
    }
    p->server_running = true;
    return generation;
}
void AsioRuntime::drain_server(uint64_t generation) {
    std::lock_guard life(p->lifecycle);
    NetworkServer* network;
    {
        std::lock_guard lock(p->mutex);
        if (p->status.state != 1) return;
        if (generation && generation != p->server_generation) return;
        network = p->server.get();
    }
    if (network) network->drain();  // a no-op once it stops
}
void AsioRuntime::stop_server(uint64_t generation) {
    std::lock_guard life(p->lifecycle);
    NetworkServer* network;
    {
        std::lock_guard lock(p->mutex);
        if (p->status.state != 1) return;
        // An Engine stops the server it started, never a later one.
        if (generation && generation != p->server_generation) return;
        network = p->server.get();
    }
    p->server_running = false;
    if (network) network->stop();
}
void AsioRuntime::stop() {
    std::lock_guard life(p->lifecycle);
    std::lock_guard lock(p->mutex);
    if (p->status.state >= 2) return;
    if (p->status.state != 1) throw std::runtime_error("stop requires a ready runtime");
    p->status.state = 2;
    p->stop_requested = true;
    p->stopping.notify_all();
}
void AsioRuntime::join() { if (p->thread.joinable()) p->thread.join(); }
std::size_t AsioRuntime::poll() {
    if (!p->same_thread) throw std::logic_error("poll() is the same-thread mode's");
    return p->pollable.load(std::memory_order_acquire) ? p->contexts.front()->poll() : 0;
}
int AsioRuntime::reactor_fd() const { return p->reactor_descriptor; }
void AsioRuntime::close_fd() {
    if (p->notify_fd >= 0) {
        ::close(p->notify_fd);
        p->notify_fd = -1;
    }
    // Same-thread mode: the shard's io_context outlives the shards for its
    // poller; once the runtime has stopped and been joined, its descriptors go.
    if (p->same_thread && !p->thread.joinable() && !p->pollable) {
        p->keep_running.clear();
        p->contexts.clear();
        p->reactor_descriptor = -1;
    }
}
}  // namespace

#ifdef OLDMAN_NATIVE_URING
// The io_uring build's entry point, which Runtime finds with dlsym: a
// running runtime, or nullptr and why not when this system refuses io_uring.
extern "C" __attribute__((visibility("default")))
RuntimeBackend* oldman_native_uring_runtime(const std::vector<int>* cpus, const ServerConfig* config,
                                             bool same_thread, std::string* why) {
    // Asio's io_uring support assumes Linux 5.10 or later.
    unsigned major = 0, minor = 0;
    utsname system{};
    if (::uname(&system) == 0) std::sscanf(system.release, "%u.%u", &major, &minor);
    if (major < 5 || (major == 5 && minor < 10)) {
        *why = std::string("Linux ") + system.release + " predates 5.10";
        return nullptr;
    }
    try {
        // Sets up the shards' rings: refused by seccomp, io_uring_disabled, ENOMEM, ...
        return new AsioRuntime(*cpus, *config, same_thread);
    } catch (const std::exception& e) {
        *why = e.what();
        return nullptr;
    }
}
#else
static std::atomic<bool> ever_started{false};

// The io_uring build, next to this extension; nullptr (and why) without it.
static RuntimeBackend* load_uring(const std::vector<int>& cpus, const ServerConfig& config, bool same_thread,
                                  std::string& why) {
    Dl_info self{};
    if (!::dladdr(reinterpret_cast<void*>(&load_uring), &self) || !self.dli_fname) {
        why = "cannot locate the native extension";
        return nullptr;
    }
    std::string path(self.dli_fname);
    path = path.substr(0, path.rfind('/') + 1) + "liboldman_native_uring.so";
    void* library = ::dlopen(path.c_str(), RTLD_NOW | RTLD_LOCAL);  // stays loaded
    if (!library) {
        why = ::dlerror();
        return nullptr;
    }
    using Factory = RuntimeBackend* (*)(const std::vector<int>*, const ServerConfig*, bool, std::string*);
    auto factory = reinterpret_cast<Factory>(::dlsym(library, "oldman_native_uring_runtime"));
    if (!factory) {
        why = "the io_uring build has no entry point";
        return nullptr;
    }
    return factory(&cpus, &config, same_thread, &why);
}
Runtime::Runtime(std::vector<int> cpus, ServerConfig config, std::string reactor_backend, bool same_thread) {
    if (cpus.empty()) throw std::invalid_argument("CPUs required");
    if (!reactor_backend.empty() && reactor_backend != "epoll" && reactor_backend != "io_uring")
        throw std::invalid_argument("unknown reactor backend: " + reactor_backend + " (epoll or io_uring)");
    if (same_thread && cpus.size() != 1) throw std::invalid_argument("the same-thread mode takes one CPU");
    validate_server(config);
    if (ever_started.exchange(true)) throw std::runtime_error("one native runtime per process");
    if (reactor_backend == "io_uring") {
        p.reset(load_uring(cpus, config, same_thread, fallback_));
        if (!p && fallback_.empty()) fallback_ = "io_uring is unavailable";
    }
    if (!p) p = std::make_unique<AsioRuntime>(std::move(cpus), std::move(config), same_thread);
}
Runtime::~Runtime() = default;
#endif
}
