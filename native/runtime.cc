#include "runtime.hh"
#include <seastar/core/alien.hh>
#include <seastar/core/app-template.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/file.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/smp.hh>
#include <atomic>
#include <array>
#include <cerrno>
#include <deque>
#include <mutex>
#include <sched.h>
#include <sys/stat.h>
#include <signal.h>
#include <stdexcept>
#include <sys/eventfd.h>
#include <sys/syscall.h>
#include <thread>
#include <unistd.h>

namespace probe {
namespace ss = seastar;
static std::atomic<bool> ever_started{false};

struct Runtime::Impl {
    std::vector<int> cpus;
    std::string reactor_backend;
    ServerConfig server_config;
    std::unique_ptr<NetworkServer> server;
    uint64_t server_generation = 0;  // of `server`; under mutex
    bool server_running = false;  // shard 0 only
    int notify_fd;
    std::atomic<bool> notify_pending{false};
    // The current server's setting that any thread reads without the mutex
    // (start_server replaces server_config under it).
    std::atomic<bool> coalesce{false};
    void adopt(const ServerConfig& config) {
        coalesce.store(config.coalesce_notifications, std::memory_order_relaxed);
    }
    std::atomic<uint64_t> notification_writes{0};
    std::thread thread;
    const pid_t owner = ::getpid();  // a forked child has a copy, not the threads
    // Held across each whole start_server / drain_server / stop_server call
    // and stop(): the server and the alien instance they use do not change
    // under them (another thread's stop() making Seastar exit, say).
    std::mutex lifecycle;
    mutable std::mutex mutex;
    std::deque<FileResult> file_results;
    std::vector<ShardInfo> shard_info;
    Stats status{};
    ss::alien::instance* alien = nullptr;
    ss::promise<>* finish = nullptr; // accessed only on shard 0
    size_t next_shard = 0;  // of the next file operation
    std::array<struct sigaction, NSIG> original_signals{}, owned_signals{};
    std::array<bool, NSIG> signal_valid{}, signal_changed{};
    bool signal_snapshot_taken = false;

    static bool same_action(const struct sigaction& a, const struct sigaction& b) {
        if (a.sa_sigaction != b.sa_sigaction || a.sa_flags != b.sa_flags) return false;
        for (int s = 1; s < NSIG; ++s)
            if (sigismember(&a.sa_mask, s) != sigismember(&b.sa_mask, s)) return false;
        return true;
    }
    void capture_owned_signals() noexcept {
        for (int s = 1; s < NSIG; ++s) {
            if (signal_valid[s] && ::sigaction(s, nullptr, &owned_signals[s]) == 0)
                signal_changed[s] = !same_action(original_signals[s], owned_signals[s]);
        }
        signal_snapshot_taken = true;
    }
    void restore_signals() noexcept {
        for (int s = 1; s < NSIG; ++s) {
            struct sigaction current{};
            // Do not overwrite a handler the application installed while running.
            if (signal_changed[s] && ::sigaction(s, nullptr, &current) == 0 &&
                same_action(current, owned_signals[s]))
                ::sigaction(s, &original_signals[s], nullptr);
        }
    }

    Impl(std::vector<int> c, ServerConfig config, std::string backend)
        : cpus(std::move(c)), reactor_backend(std::move(backend)), server_config(std::move(config)),
          notify_fd(::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC)) {
        if (notify_fd < 0) throw std::runtime_error("eventfd creation failed");
        adopt(server_config);
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
    static ss::future<ShardInfo> inspect_shard() {
        co_await ss::sleep(std::chrono::milliseconds(1));
        cpu_set_t set;
        CPU_ZERO(&set);
        if (::sched_getaffinity(0, sizeof(set), &set) != 0)
            throw std::runtime_error("sched_getaffinity failed");
        ShardInfo info{ss::this_shard_id(), int(::syscall(SYS_gettid)), {},
                       std::string(ss::engine().get_backend_name())};
        for (int i = 0; i < CPU_SETSIZE; ++i)
            if (CPU_ISSET(i, &set)) info.affinity.push_back(i);
        co_return info;
    }
    ss::future<> execute_file(uint64_t id, int operation, std::string path, uint64_t offset, int64_t length) {
        FileResult result{id, operation};
        try {
            if (operation == FileOperation::file_stat) {
                const auto st = co_await ss::engine().file_stat(path, ss::follow_symlink::yes);
                auto ns = [](std::chrono::system_clock::time_point t) {
                    return uint64_t(std::chrono::duration_cast<std::chrono::nanoseconds>(t.time_since_epoch()).count());
                };
                result.stat = {st.mode, st.inode_number, st.device_id, st.number_of_links, st.uid, st.gid,
                               st.size, ns(st.time_accessed), ns(st.time_modified), ns(st.time_changed),
                               st.block_size, st.allocated_size / 512, st.rdev};
            } else {
                auto file = co_await ss::open_file_dma(path, ss::open_flags::ro);
                std::exception_ptr failure;
                try {
                    const auto st = co_await file.stat();
                    // Seastar opens directories too; Python's open() refuses them.
                    if (S_ISDIR(st.st_mode)) throw std::system_error(EISDIR, std::system_category());
                    auto ns = [](const timespec& t) { return uint64_t(int64_t(t.tv_sec) * 1'000'000'000 + t.tv_nsec); };
                    if (operation == FileOperation::file_open)
                        result.stat = {st.st_mode, st.st_ino, st.st_dev, st.st_nlink, st.st_uid, st.st_gid,
                                       uint64_t(st.st_size), ns(st.st_atim), ns(st.st_mtim), ns(st.st_ctim),
                                       uint64_t(st.st_blksize), uint64_t(st.st_blocks), st.st_rdev};
                    const uint64_t size = operation == FileOperation::file_read ? st.st_size : 0;
                    const uint64_t end = length < 0 ? size : std::min<uint64_t>(size, offset + uint64_t(length));
                    if (offset < end) {
                        // An aligned read: the buffer can extend past the range.
                        auto bytes = co_await file.dma_read_bulk<char>(offset, end - offset);
                        result.data.assign(bytes.get(), std::min<size_t>(bytes.size(), end - offset));
                    }
                } catch (...) {
                    failure = std::current_exception();
                }
                co_await file.close();
                if (failure) std::rethrow_exception(failure);
            }
        } catch (const std::system_error& e) {
            result.error = e.code().value() ? e.code().value() : EIO;
        } catch (...) {
            result.error = EIO;
        }
        {
            std::lock_guard lock(mutex);
            file_results.push_back(std::move(result));
        }
        notify();
    }
    ss::future<> main(ss::app_template& app) {
        ss::promise<> done;
        finish = &done;
        std::vector<ShardInfo> info;
        for (unsigned i = 0; i < cpus.size(); ++i)
            info.push_back(co_await ss::smp::submit_to(i, inspect_shard));
        capture_owned_signals();
        {
            std::lock_guard lock(mutex);
            alien = &app.alien();
            shard_info = std::move(info);
        }
        if (server_config.enabled()) {
            auto network = std::make_unique<NetworkServer>(server_config, cpus.size(), app.alien(),
                [this] { notify(); });
            {
                std::lock_guard lock(mutex);
                server = std::move(network);
            }
            std::exception_ptr failure;
            try { co_await server->start(); } catch (...) { failure = std::current_exception(); }
            if (failure) {
                co_await server->stop();
                finish = nullptr;
                std::rethrow_exception(failure);
            }
        }
        {
            std::lock_guard lock(mutex);
            status.state = 1;
            status.live_workers = cpus.size();
        }
        notify();
        co_await done.get_future();
        finish = nullptr;
        if (server) co_await server->stop();
        server_running = false;
    }
    ss::future<uint64_t> start_server_here(ServerConfig config) {
        auto network = std::make_unique<NetworkServer>(config, cpus.size(), *alien, [this] { notify(); });
        std::exception_ptr failure;
        try { co_await network->start(); } catch (...) { failure = std::current_exception(); }
        if (failure) {
            co_await network->stop();
            std::rethrow_exception(failure);
        }
        std::unique_ptr<NetworkServer> previous;  // stopped; destroyed here on shard 0
        uint64_t generation;
        {
            std::lock_guard lock(mutex);
            previous = std::move(server);
            server = std::move(network);
            adopt(config);
            server_config = std::move(config);
            generation = ++server_generation;
        }
        server_running = true;
        co_return generation;
    }
    ss::future<> drain_server_here(uint64_t generation) {
        NetworkServer* network;
        {
            std::lock_guard lock(mutex);
            if (generation && generation != server_generation) co_return;
            network = server.get();
        }
        if (network) co_await network->drain();  // a no-op once it stops
    }
    ss::future<> stop_server_here(uint64_t generation) {
        NetworkServer* network;
        {
            std::lock_guard lock(mutex);
            // An Engine stops the server it started, never a later one.
            if (generation && generation != server_generation) co_return;
            network = server.get();
        }
        server_running = false;
        if (network) co_await network->stop();
    }
    void run() noexcept {
        for (int s = 1; s < NSIG; ++s)
            signal_valid[s] = ::sigaction(s, nullptr, &original_signals[s]) == 0;
        try {
            ss::app_template::seastar_options opts;
            opts.name = "oldman-server";
            opts.auto_handle_sigint_sigterm = false;
            opts.smp_opts.memory_allocator = ss::memory_allocator::standard;
            ss::app_template app(std::move(opts));
            app.set_configuration_reader([] (auto&) {});
            std::string cpu_text;
            for (int cpu : cpus) {
                if (!cpu_text.empty()) cpu_text += ',';
                cpu_text += std::to_string(cpu);
            }
            // --kernel-page-cache: files are read through the page cache (not
            // O_DIRECT), as Python would; reads not in the cache go to Seastar's
            // syscall thread instead of blocking the reactor.
            std::vector<std::string> args{
                "oldman-server", "--smp", std::to_string(cpus.size()),
                "--cpuset", cpu_text,
                "--overprovisioned", "--thread-affinity", "1",
                "--enable-glibc-exception-scaling-workaround", "0",
                "--kernel-page-cache", "1", "--default-log-level", "warn"};
            if (!reactor_backend.empty()) {
                args.push_back("--reactor-backend");
                args.push_back(reactor_backend);
            }
            std::vector<char*> argv;
            for (auto& arg : args) argv.push_back(arg.data());
            // app.run does not retain its callable until its future completes.
            // This lambda is deliberately NOT a coroutine. The member coroutine
            // stores the stable Impl pointer and app reference in its own frame.
            int rc = app.run(int(argv.size()), argv.data(), [&] { return main(app); });
            {
                std::lock_guard lock(mutex);
                alien = nullptr;
                status.live_workers = 0;
                status.state = rc == 0 ? 3 : 4;
                if (rc) status.error = "Seastar app.run returned " + std::to_string(rc);
            }
        } catch (const std::exception& e) {
            std::lock_guard lock(mutex);
            status.state = 4;
            status.live_workers = 0;
            status.error = e.what();
        } catch (...) {
            std::lock_guard lock(mutex);
            status.state = 4;
            status.live_workers = 0;
            status.error = "unknown Seastar startup exception";
        }
        if (!signal_snapshot_taken) capture_owned_signals();
        restore_signals();
        notify();
    }
};

// The framework host is the only server: a listener needs framework dispatch,
// and framework dispatch HTTP or a listening socket.
static void validate_server(const ServerConfig& config) {
    if (config.enabled() && !config.application_dispatch_all)
        throw std::invalid_argument("a server requires framework dispatch (application_dispatch_all)");
    if (config.application_dispatch_all && !config.http_port && config.listen_fd < 0)
        throw std::invalid_argument("framework dispatch requires an HTTP port or a listening socket");
}
Runtime::Runtime(std::vector<int> cpus, ServerConfig config, std::string reactor_backend) {
    if (cpus.empty()) throw std::invalid_argument("CPUs required");
    validate_server(config);
    if (ever_started.exchange(true)) throw std::runtime_error("one Seastar lifecycle per process");
    p = std::make_unique<Impl>(std::move(cpus), std::move(config), std::move(reactor_backend));
    p->thread = std::thread([this] { p->run(); });
}
Runtime::~Runtime() {
    if (p->owner != ::getpid()) {
        // A forked child's copy: the threads (Seastar's) are
        // the parent's. Joining them, or destroying condition variables they
        // wait on, hangs: the copy is left alone.
        (void)p.release();
        return;
    }
    // Engine.aclose is mandatory. A forgotten live runtime cannot be detached safely.
    if (p->thread.joinable()) p->thread.join();
    close_fd();
}
int Runtime::fd() const { return p->notify_fd; }
void Runtime::submit_file(uint64_t id, int operation, std::string path, uint64_t offset, int64_t length) {
    if (operation < FileOperation::file_stat || operation > FileOperation::file_open)
        throw std::invalid_argument("unknown file operation");
    // Posted under the mutex: the alien instance is valid while
    // the state is 1, and run() clears it under the mutex before it goes.
    std::lock_guard lock(p->mutex);
    if (p->status.state != 1) throw std::runtime_error("runtime not accepting work");
    const unsigned shard = p->next_shard++ % p->cpus.size();
    ss::alien::run_on(*p->alien, shard,
        [impl = p.get(), id, operation, path = std::move(path), offset, length]() mutable noexcept {
            (void)impl->execute_file(id, operation, std::move(path), offset, length);
        });
}
std::vector<FileResult> Runtime::drain_files(size_t count) {
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
void Runtime::clear_notification() {
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
Stats Runtime::stats() const {
    std::lock_guard lock(p->mutex);
    return p->status;
}
std::vector<ShardInfo> Runtime::shards() const {
    std::lock_guard lock(p->mutex);
    return p->shard_info;
}
std::vector<RequestEvent> Runtime::drain_requests(size_t count) {
    std::lock_guard lock(p->mutex);
    if (!p->server) return {};
    auto out = p->server->drain(count);
    if (p->server->has_events()) p->notify();
    return out;
}
void Runtime::resolve_requests(std::vector<RequestDecision> decisions) {
    std::lock_guard lock(p->mutex);
    if (p->server && p->status.state < 2) p->server->resolve_batch(std::move(decisions));
}
ServerSnapshot Runtime::server_stats() const {
    std::lock_guard lock(p->mutex);
    auto out = p->server ? p->server->snapshot() : ServerSnapshot{};
    out.totals["notification_writes"] = p->notification_writes.load(std::memory_order_relaxed);
    return out;
}
uint64_t Runtime::start_server(ServerConfig config) {
    validate_server(config);
    std::lock_guard life(p->lifecycle);
    ss::alien::instance* alien = nullptr;
    {
        std::lock_guard lock(p->mutex);
        if (p->status.state != 1) throw std::runtime_error("runtime is not running");
        alien = p->alien;
    }
    return ss::alien::submit_to(*alien, 0, [impl = p.get(), config = std::move(config)]() mutable {
        if (impl->server_running)
            return ss::make_exception_future<uint64_t>(std::runtime_error("a network server is already running"));
        return impl->start_server_here(std::move(config));
    }).get();
}
void Runtime::drain_server(uint64_t generation) {
    std::lock_guard life(p->lifecycle);
    ss::alien::instance* alien = nullptr;
    {
        std::lock_guard lock(p->mutex);
        if (p->status.state != 1) return;
        alien = p->alien;
    }
    ss::alien::submit_to(*alien, 0, [impl = p.get(), generation] {
        return impl->drain_server_here(generation);
    }).get();
}
void Runtime::stop_server(uint64_t generation) {
    std::lock_guard life(p->lifecycle);
    ss::alien::instance* alien = nullptr;
    {
        std::lock_guard lock(p->mutex);
        if (p->status.state != 1) return;
        alien = p->alien;
    }
    ss::alien::submit_to(*alien, 0, [impl = p.get(), generation] {
        return impl->stop_server_here(generation);
    }).get();
}
void Runtime::stop() {
    std::lock_guard life(p->lifecycle);
    std::lock_guard lock(p->mutex);
    if (p->status.state >= 2) return;
    if (p->status.state != 1) throw std::runtime_error("stop requires a ready runtime");
    ss::alien::run_on(*p->alien, 0, [impl=p.get()]() noexcept { impl->finish->set_value(); });
    p->status.state = 2;
}
void Runtime::join() { if (p->thread.joinable()) p->thread.join(); }
void Runtime::close_fd() {
    if (p->notify_fd >= 0) {
        ::close(p->notify_fd);
        p->notify_fd = -1;
    }
}
}
