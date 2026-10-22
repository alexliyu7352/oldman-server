// Diagnostic interposer for the pinned GCC/Linux ABI. Never link into production.
#include "server.hh"
#include <atomic>
#include <cstdio>
#include <cstring>
#include <dlfcn.h>
#include <fcntl.h>
#include <malloc.h>
#include <pthread.h>
#include <time.h>
#include <unistd.h>

namespace {
constexpr size_t max_records = 131072, max_slots = 1024;
constexpr const char* drain_symbol = "_ZN5probe13NetworkServer5drainEm";
using Drain = std::vector<probe::RequestEvent> (*)(probe::NetworkServer*, size_t);
using New = void* (*)(size_t);
Drain real_drain = nullptr;
const char* origin = nullptr;
pthread_t owner;
std::atomic<bool> enabled{false};
std::atomic<uint64_t> errors{0};
size_t used = 0, period = 64;
struct Record {
    uint64_t count, size, capacity, allocations, bytes, sampled, allocation_ns, drain_ns;
};
struct Bucket {
    uint64_t calls, usable_min, usable_max, samples, ns;
    uintptr_t caller;
    uint64_t other_callers;
    uint64_t histogram[32];
};
Record records[max_records];
Bucket buckets[max_slots + 1];
thread_local Record* current = nullptr;

uint64_t now() {
    timespec ts;
    if (clock_gettime(CLOCK_MONOTONIC_RAW, &ts)) std::abort();
    return uint64_t(ts.tv_sec) * 1000000000 + ts.tv_nsec;
}
New allocator() {
    static auto fn = reinterpret_cast<New>(dlsym(RTLD_NEXT, "_Znwm"));
    if (!fn) std::abort();
    return fn;
}
}

void* operator new(size_t bytes) {
    auto allocate = allocator();
    auto* record = current;
    if (!record) return allocate(bytes);
    current = nullptr; // Do not double count a nested new_handler allocation.
    auto start = record->sampled ? now() : 0;
    void* pointer;
    try {
        pointer = allocate(bytes);
    } catch (...) {
        current = record;
        throw;
    }
    auto elapsed = record->sampled ? now() - start : 0;
    current = record;
    ++record->allocations;
    record->bytes += bytes;
    record->allocation_ns += elapsed;
    auto slots = bytes / sizeof(probe::RequestEvent);
    if (!slots || slots > max_slots || bytes % sizeof(probe::RequestEvent)) {
        ++errors;
        return pointer;
    }
    auto& bucket = buckets[slots];
    auto usable = malloc_usable_size(pointer);
    auto caller = reinterpret_cast<uintptr_t>(__builtin_return_address(0));
    if (!bucket.calls) { bucket.usable_min = usable; bucket.caller = caller; }
    ++bucket.calls;
    bucket.other_callers += caller != bucket.caller;
    bucket.usable_min = std::min(bucket.usable_min, uint64_t(usable));
    bucket.usable_max = std::max(bucket.usable_max, uint64_t(usable));
    if (record->sampled) {
        ++bucket.samples;
        bucket.ns += elapsed;
        auto bin = std::min(31u, elapsed ? 63u - unsigned(__builtin_clzll(elapsed)) : 0u);
        ++bucket.histogram[bin];
    }
    return pointer;
}

std::vector<probe::RequestEvent> probe::NetworkServer::drain(size_t count) {
    if (!real_drain) std::abort();
    if (!enabled.load(std::memory_order_relaxed)) return real_drain(this, count);
    if (!pthread_equal(owner, pthread_self()) || current || used == max_records) {
        ++errors;
        return real_drain(this, count);
    }
    Record record{};
    record.count = count;
    record.sampled = used % period == 0;
    auto start = record.sampled ? now() : 0;
    current = &record;
    try {
        auto batch = real_drain(this, count);
        current = nullptr;
        record.drain_ns = record.sampled ? now() - start : 0;
        record.size = batch.size();
        record.capacity = batch.capacity();
        records[used++] = record;
        return batch;
    } catch (...) {
        current = nullptr;
        ++errors;
        throw;
    }
}

extern "C" int drain_trace_init(const char* module, size_t sample_period) {
    if (!sample_period || real_drain) return -1;
    auto* handle = dlopen(module, RTLD_NOW | RTLD_NOLOAD);
    if (!handle) return -2;
    auto* function = dlsym(handle, drain_symbol);
    Dl_info info{};
    if (!function || !dladdr(function, &info) || !info.dli_fname ||
            std::strcmp(info.dli_fname, module)) return -3;
    real_drain = reinterpret_cast<Drain>(function);
    origin = info.dli_fname;
    owner = pthread_self();
    period = sample_period;
    return 0;
}
extern "C" void drain_trace_start() {
    if (!real_drain || enabled || !pthread_equal(owner, pthread_self())) std::abort();
    used = 0;
    errors = 0;
    std::memset(records, 0, sizeof(records));
    std::memset(buckets, 0, sizeof(buckets));
    enabled = true;
}
extern "C" void drain_trace_stop() { enabled = false; }
extern "C" int drain_trace_dump(const char* path) {
    if (enabled || !origin || !pthread_equal(owner, pthread_self())) return -1;
    int fd = open(path, O_WRONLY | O_CREAT | O_EXCL, 0600);
    if (fd < 0) return -2;
    FILE* file = fdopen(fd, "w");
    if (!file) { close(fd); return -3; }
    // Paths are reported by the Python wrapper; no unsafe JSON string escaping.
    std::fprintf(file, "{\"diagnostic_only\":true,\"event_bytes\":%zu,\"period\":%zu,\"errors\":%lu,"
                      "\"record_columns\":[\"count\",\"size\",\"capacity\",\"allocations\",\"bytes\",\"sampled\",\"allocation_ns\",\"drain_ns\"],\"records\":[",
                 sizeof(probe::RequestEvent), period, errors.load());
    for (size_t i = 0; i < used; ++i) {
        const auto& r = records[i];
        std::fprintf(file, "%s[%lu,%lu,%lu,%lu,%lu,%lu,%lu,%lu]", i ? "," : "",
            r.count, r.size, r.capacity, r.allocations, r.bytes, r.sampled, r.allocation_ns, r.drain_ns);
    }
    std::fprintf(file, "],\"allocations\":[");
    bool comma = false;
    for (size_t i = 1; i <= max_slots; ++i) {
        const auto& b = buckets[i];
        if (!b.calls) continue;
        Dl_info caller{};
        if (!dladdr(reinterpret_cast<void*>(b.caller), &caller) || !caller.dli_fname ||
                std::strcmp(caller.dli_fname, origin)) { fclose(file); return -4; }
        std::fprintf(file, "%s{\"slots\":%zu,\"calls\":%lu,\"usable_min\":%lu,\"usable_max\":%lu,"
            "\"samples\":%lu,\"ns\":%lu,\"caller_offset\":%lu,\"other_callers\":%lu,\"ns_log2_histogram\":[",
            comma ? "," : "", i, b.calls, b.usable_min, b.usable_max, b.samples, b.ns,
            b.caller - reinterpret_cast<uintptr_t>(caller.dli_fbase), b.other_callers);
        for (size_t j = 0; j < 32; ++j) std::fprintf(file, "%s%lu", j ? "," : "", b.histogram[j]);
        std::fprintf(file, "]}");
        comma = true;
    }
    std::fprintf(file, "]}\n");
    bool failed = ferror(file);
    return fclose(file) || failed ? -5 : 0;
}
