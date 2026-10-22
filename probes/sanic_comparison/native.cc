// Pure Seastar fixture: same response data and the same timed wait, no Python.
#include <seastar/core/app-template.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/sleep.hh>
#include <seastar/http/httpd.hh>
#include <seastar/http/handlers.hh>
#include <seastar/json/formatter.hh>
#include <array>
#include <atomic>
#include <future>
#include <iostream>
#include <map>
#include <thread>
#include <sys/syscall.h>
#include <unistd.h>

namespace ss = seastar;
struct alignas(64) Counts {
    std::atomic<uint64_t> started{0}, finished{0}, responses{0}, errors{0}, active{0}, bytes{0};
    int tid = 0;
};
static std::array<Counts,3> counters;
struct Lease {
    Counts& counts;
    explicit Lease(Counts& c):counts(c) { ++counts.started; ++counts.active; }
    ~Lease() { ++counts.finished; --counts.active; }
};
class Service;
class Handler final: public ss::httpd::handler_base {
    Service& service;
public:
    explicit Handler(Service& s): service(s) {}
    ss::future<std::unique_ptr<ss::http::reply>> handle(const ss::sstring&,
        std::unique_ptr<ss::http::request> req, std::unique_ptr<ss::http::reply> rep) override;
};
class Service {
    ss::httpd::http_server http{"sanic-native-reference"};
    Counts& counts = counters[ss::this_shard_id()];
public:
    ss::future<> start(unsigned port) {
        counts.tid = ::syscall(SYS_gettid);
        http.set_content_length_limit(65536);
        http.set_request_size_limit(8192);
        http.set_content_streaming(true);
        http.set_generate_date_header(true);
        using ss::httpd::operation_type;
        http._routes.add(operation_type::GET, ss::httpd::url("/small"), new Handler(*this));
        http._routes.add(operation_type::POST, ss::httpd::url("/body"), new Handler(*this));
        http._routes.add(operation_type::GET, ss::httpd::url("/sleep"), new Handler(*this));
        ss::listen_options options; options.reuse_address = true;
        co_await http.listen(ss::socket_address(ss::net::inet_address("127.0.0.1"),uint16_t(port)), options);
    }
    ss::future<> stop() {
        co_await http.stop();
    }
    ss::future<std::unique_ptr<ss::http::reply>> handle(
        std::unique_ptr<ss::http::request> req, std::unique_ptr<ss::http::reply> rep) {
        auto lease = std::make_shared<Lease>(counts);
        std::string body, content_type = "application/json";
        int status = 200;
        try {
            std::string input;
            for (;;) {
                auto chunk = co_await req->content_stream->read();
                if (chunk.empty()) break;
                if (input.size()+chunk.size()>65536) throw std::runtime_error("request body limit");
                input.append(chunk.get(),chunk.size());
            }
            co_await req->content_stream->close();
            if (req->_url == "/body") {
                body = std::string(req->get_header("Host"))+":"+
                       std::string(req->get_header("Content-Type"))+":"+input;
                content_type = "application/octet-stream";
            } else {
                // /sleep waits as long as the Python fixture (SLEEP_SECONDS).
                if (req->_url == "/sleep") co_await ss::sleep(std::chrono::milliseconds(1));
                std::map<std::string,std::string> value{{"message","Hello, World!"}};
                body = std::string(ss::json::formatter::to_json(value));
            }
        } catch (...) { ++counts.errors; status=500; body="reference error"; }
        rep->set_status(static_cast<ss::http::reply::status_type>(status));
        rep->add_header("Content-Type",content_type);
        rep->add_header("X-Probe-Pid",ss::to_sstring(::getpid()));
        rep->add_header("X-Probe-Shard",ss::to_sstring(ss::this_shard_id()));
        rep->write_body(std::nullopt,[this,body=std::move(body),lease](ss::output_stream<char>& out) -> ss::future<> {
            co_await out.write(body);
            co_await out.close();
            ++counts.responses; counts.bytes += body.size();
        });
        co_return rep;
    }
};
ss::future<std::unique_ptr<ss::http::reply>> Handler::handle(const ss::sstring&,
    std::unique_ptr<ss::http::request> req, std::unique_ptr<ss::http::reply> rep) {
    return service.handle(std::move(req),std::move(rep));
}
static std::map<std::string,uint64_t> stats() {
    std::map<std::string,uint64_t> result;
    for (auto& c:counters) {
        result["started"]+=c.started.load(); result["finished"]+=c.finished.load();
        result["responses"]+=c.responses.load(); result["active"]+=c.active.load();
        result["errors"]+=c.errors.load(); result["body_bytes"]+=c.bytes.load();
    }
    return result;
}
static void print_stats(const std::map<std::string,uint64_t>& values) {
    std::cout<<ss::json::formatter::to_json(values);
}
static ss::future<int> run_service(unsigned port,
    std::promise<void>& ready, std::atomic<bool>& stop) {
    ss::sharded<Service> services;
    co_await services.start();
    std::exception_ptr failure;
    try {
        co_await services.invoke_on_all(&Service::start,port);
        ready.set_value();
        while (!stop.load()) co_await ss::sleep(std::chrono::milliseconds(20));
    } catch (...) { failure=std::current_exception(); }
    // sharded::stop invokes Service::stop on each shard before destruction.
    co_await services.stop();
    if (failure) std::rethrow_exception(failure);
    co_return 0;
}
int main(int argc, char** argv) {
    if (argc!=3) return 2;
    unsigned port=std::stoul(argv[1]);
    std::atomic<bool> stop{false}; std::promise<void> ready;
    std::exception_ptr thread_error;
    std::thread runtime([&] {
        try {
            ss::app_template::seastar_options settings;
            settings.auto_handle_sigint_sigterm = false;
            settings.smp_opts.memory_allocator = ss::memory_allocator::standard;
            ss::app_template app(std::move(settings));
            app.set_configuration_reader([] (auto&) {});
            std::vector<std::string> options{argv[0],"--smp","3","--cpuset",argv[2],
                "--thread-affinity","1","--reactor-backend","epoll","--overprovisioned",
                "--default-log-level","warn","--enable-glibc-exception-scaling-workaround","0"};
            std::vector<char*> pointers; for (auto& option:options) pointers.push_back(option.data());
            // app.run releases the callable before the future completes.
            // Keep coroutine state in its function frame, not that callable.
            int result=app.run(pointers.size(),pointers.data(),[&] {
                return run_service(port,ready,stop);
            });
            if (result) throw std::runtime_error("Seastar run failed");
        } catch (...) {
            thread_error=std::current_exception();
            try { ready.set_exception(thread_error); } catch (...) {}
        }
    });
    try {
        ready.get_future().get();
        std::cout<<"{\"ready\":true,\"pids\":["<<getpid()<<"],\"service_pids\":["<<getpid()
                 <<"],\"workers\":[{\"pid\":"<<getpid()<<",\"shards\":[";
        for (unsigned i=0;i<3;++i) { if(i)std::cout<<','; std::cout<<"{\"tid\":"<<counters[i].tid<<'}'; }
        std::cout<<"]}]}"<<std::endl;
        std::map<std::string,uint64_t> before;
        for (std::string command;std::getline(std::cin,command);) {
            if(command=="quit")break;
            for (unsigned i=0;stats()["active"]&&i<5000;++i) std::this_thread::sleep_for(std::chrono::milliseconds(1));
            if(stats()["active"])throw std::runtime_error("reference failed to quiesce");
            if(command=="begin") { before=stats();std::cout<<"{\"begun\":true}"<<std::endl; }
            else if(command=="end") {
                auto after=stats(),delta=after;
                for(auto&[key,value]:delta)value-=before.at(key);
                std::cout<<"{\"workers\":[{\"pid\":"<<getpid()<<",\"delta\":";print_stats(delta);
                std::cout<<",\"after\":";print_stats(after);std::cout<<"}]}"<<std::endl;
            } else throw std::runtime_error("unknown command");
        }
        stop=true;runtime.join();
        if(thread_error)std::rethrow_exception(thread_error);
        std::cout<<"{\"closed\":true,\"workers\":[";print_stats(stats());std::cout<<"]}"<<std::endl;
    } catch(const std::exception& error) {
        stop=true;if(runtime.joinable())runtime.join();std::cerr<<error.what()<<'\n';return 1;
    }
}
