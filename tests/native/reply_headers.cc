#include <seastar/core/app-template.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/iostream.hh>
#include <seastar/http/reply.hh>
#include <cstdlib>
#include <iostream>
#include <new>
#include <stdexcept>
#include <string>
#include <vector>

static thread_local bool counting = false;
static thread_local size_t allocations = 0;
// This focused Linux/glibc test interposes malloc: sstring bypasses operator new.
extern "C" void* __libc_malloc(size_t);
extern "C" void* malloc(size_t size) noexcept {
    if (counting) ++allocations;
    return __libc_malloc(size);
}

namespace ss = seastar;
class sink final : public ss::data_sink_impl {
    std::string& bytes;
    bool fail;
public:
    sink(std::string& b, bool f) : bytes(b), fail(f) {}
    ss::future<> put(std::span<ss::temporary_buffer<char>> buffers) override {
        if (fail) return ss::make_exception_future<>(std::runtime_error("sink failed"));
        for (const auto& buffer : buffers) bytes.append(buffer.get(), buffer.size());
        return ss::make_ready_future<>();
    }
    ss::future<> close() override { return ss::make_ready_future<>(); }
};

class delayed_sink final : public ss::data_sink_impl {
    std::string& bytes;
    ss::promise<>& release;
    bool first = true;
public:
    delayed_sink(std::string& b, ss::promise<>& r) : bytes(b), release(r) {}
    ss::future<> put(std::span<ss::temporary_buffer<char>> buffers) override {
        if (!first) {
            for (const auto& buffer : buffers) bytes.append(buffer.get(), buffer.size());
            return ss::make_ready_future<>();
        }
        first = false;
        std::vector<ss::temporary_buffer<char>> retained;
        for (auto& buffer : buffers) retained.push_back(buffer.share());
        return release.get_future().then([this, retained = std::move(retained)] {
            for (const auto& buffer : retained) bytes.append(buffer.get(), buffer.size());
        });
    }
    ss::future<> close() override { return ss::make_ready_future<>(); }
};

ss::future<> check_delayed(bool fail) {
    ss::http::reply reply;
    reply.add_header("X-Long-Header", ss::sstring(512, 'v'));
    reply.set_cookie("after-header", "cookie-value");
    std::string expected = "seed";
    for (const auto& [key, value] : reply._headers)
        expected.append(key.data(), key.size()).append(": ").append(value.data(), value.size()).append("\r\n");
    for (const auto& [key, value] : reply._cookies)
        expected.append("Set-Cookie: ").append(key.data(), key.size()).append("=").append(value.data(), value.size()).append("\r\n");
    std::string bytes;
    ss::promise<> release;
    ss::output_stream<char> out(ss::data_sink(std::make_unique<delayed_sink>(bytes, release)), 16);
    co_await out.write("seed", 4);
    auto writing = reply.write_reply_headers(out);
    if (writing.available() || !bytes.empty()) throw std::runtime_error("delayed sink did not suspend");
    if (fail) release.set_exception(std::runtime_error("delayed sink failed"));
    else release.set_value();
    try {
        co_await std::move(writing);
        co_await out.flush();
        if (fail) throw std::logic_error("delayed sink failure was lost");
    } catch (const std::runtime_error& error) {
        if (!fail || std::string(error.what()) != "delayed sink failed") throw;
        if (!bytes.empty()) throw std::logic_error("wrote past failed header");
        std::cout << "delayed error: propagated sink failure\n";
        co_return;
    }
    co_await out.close();
    if (bytes != expected) throw std::runtime_error("delayed wire bytes or order changed");
    std::cout << "delayed success: exact bytes verified\n";
}

ss::future<> check_case(const char* name, ss::http::reply reply, bool fail = false) {
    std::string expected = "seed";
    for (const auto& [key, value] : reply._headers)
        expected.append(key.data(), key.size()).append(": ").append(value.data(), value.size()).append("\r\n");
    for (const auto& [key, value] : reply._cookies)
        expected.append("Set-Cookie: ").append(key.data(), key.size()).append("=").append(value.data(), value.size()).append("\r\n");
    std::string bytes;
    bytes.reserve(expected.size());
    ss::output_stream<char> out(ss::data_sink(std::make_unique<sink>(bytes, fail)), fail ? 16 : 65536);
    co_await out.write("seed", 4); // Allocate the reusable output buffer outside the measurement.
    allocations = 0;
    counting = !fail;
    auto written = reply.write_reply_headers(out);
    counting = false;
    const auto count = allocations;
    if (!fail && !written.available())
        throw std::runtime_error("allocation measurement unexpectedly suspended");
    try {
        co_await std::move(written);
        co_await out.flush();
        if (fail) throw std::logic_error("sink error was lost");
    } catch (const std::runtime_error& error) {
        if (!fail || std::string(error.what()) != "sink failed") throw;
        std::cout << name << ": propagated sink failure\n";
        co_return;
    }
    co_await out.close();
    if (bytes != expected) throw std::runtime_error("wire bytes or order changed");
    const auto lines = reply._headers.size() + reply._cookies.size();
    std::cout << name << ": " << count << " allocations for " << lines << " lines; exact bytes verified\n";
    if (count > lines) throw std::runtime_error("more than one allocation per complete header/cookie line");
}

ss::future<> checks() {
    co_await check_delayed(false);
    co_await check_delayed(true);
    ss::http::reply empty;
    co_await check_case("empty", std::move(empty));
    ss::http::reply small;
    small.add_header("X", "y");
    small.set_cookie("a", "b");
    co_await check_case("small", std::move(small));
    ss::http::reply regular;
    regular.add_header("Content-Type", "application/json");
    regular.add_header("Date", "Fri, 25 Sep 2026 00:00:00 GMT");
    regular.set_cookie("session", "opaque-value; HttpOnly; Path=/");
    co_await check_case("regular", std::move(regular));
    ss::http::reply large;
    large.add_header(ss::sstring(128, 'N'), ss::sstring(8192, 'v'));
    large.set_cookie("large", ss::sstring(8192, 'c'));
    co_await check_case("large", std::move(large));
    ss::http::reply failing;
    failing.add_header("Long-Header", ss::sstring(128, 'e'));
    failing.set_cookie("must-not-follow", "error");
    co_await check_case("error", std::move(failing), true);
}

int main(int argc, char** argv) {
    ss::app_template::seastar_options options;
    options.smp_opts.memory_allocator = ss::memory_allocator::standard;
    ss::app_template app(std::move(options));
    return app.run(argc, argv, checks);
}
