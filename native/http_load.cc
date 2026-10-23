// Persistent HTTP lanes, closed-loop or bounded scheduled arrivals.
#include <seastar/core/app-template.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/sleep.hh>
#include <seastar/http/client.hh>
#include <seastar/http/request.hh>
#include <seastar/net/inet_address.hh>
#include <boost/range/irange.hpp>
#include <algorithm>
#include <atomic>
#include <cmath>
#include <cstring>
#include <fstream>
#include <iostream>
#include <map>
#include <set>
#include <sstream>
#include <thread>
#include <time.h>
namespace ss=seastar;
using Clock=std::chrono::steady_clock;
struct Config {
    unsigned port, concurrency;
    std::set<int> pids;
    std::string path, method, input, expected;
    double rate;
    std::string policy_version;
    std::string result_tag;
};
struct Stats {
    uint64_t requests=0, errors=0, bytes=0, total_connections=0, new_connections=0;
    uint64_t scheduled=0, dropped=0, completed_in_window=0, status_errors=0, integrity_errors=0;
    std::vector<uint64_t> latency_ns, dispatch_ns;
    std::map<int,uint64_t> shards, workers, pids;
};
class NodelayFactory final: public ss::http::connection_factory {
    ss::socket_address address;
public:
    explicit NodelayFactory(unsigned port):address(ss::net::inet_address("127.0.0.1"),uint16_t(port)){}
    ss::future<ss::connected_socket> make(ss::abort_source*) override {
        auto socket=co_await ss::connect(address);
        socket.set_nodelay(true);
        co_return std::move(socket);
    }
};
class Load {
    Config config;
    std::vector<std::unique_ptr<ss::http::client>> clients;
    std::vector<bool> busy;
    Stats stats;
    unsigned active=0;
    ss::future<> consume(const ss::http::reply& reply, ss::input_stream<char> input, bool& ok) {
        bool success=int(reply._status)==200, bad=false;
        int pid=0;
        if (success) {
            try { pid=std::stoi(std::string(reply.get_header("X-Probe-Pid"))); }
            catch (...) { bad=true; }
            if (!config.pids.contains(pid)) bad=true;
            if (!config.policy_version.empty() && reply.get_header("X-Probe-Policy") != config.policy_version) bad=true;
            if (!config.result_tag.empty() && reply.get_header("X-App-Result") != config.result_tag) bad=true;
        }
        size_t offset=0;
        std::exception_ptr failure;
        try {
            for (;;) {
                auto b=co_await input.read();
                if (b.empty()) break;
                if (success && (offset>config.expected.size() || b.size()>config.expected.size()-offset ||
                    std::memcmp(b.get(),config.expected.data()+offset,b.size())!=0)) bad=true;
                offset+=b.size();
            }
        } catch (...) { failure=std::current_exception(); }
        co_await input.close();
        if (failure) std::rethrow_exception(failure);
        if (!success) { ++stats.status_errors; co_return; }
        if (bad || offset!=config.expected.size()) {
            ++stats.integrity_errors;
            throw std::runtime_error("response body/PID mismatch");
        }
        auto shard=reply.get_header("X-Probe-Shard"), worker=reply.get_header("X-Probe-Worker");
        if (!shard.empty()) ++stats.shards[std::stoi(std::string(shard))];
        if (!worker.empty()) ++stats.workers[std::stoi(std::string(worker))];
        ++stats.pids[pid]; stats.bytes+=offset; ok=true;
    }
    ss::future<> once(unsigned lane, Clock::time_point due, Clock::time_point deadline) {
        busy[lane]=true;
        ++active;
        stats.dispatch_ns.push_back(std::max<int64_t>(0,std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now()-due).count()));
        try {
            auto req=ss::http::request::make(config.method,"127.0.0.1",config.path);
            if (config.method=="POST") req.write_body("bin",ss::sstring(config.input));
            bool ok=false;
            co_await clients[lane]->make_request(std::move(req),[this,&ok](const ss::http::reply& r,ss::input_stream<char>&& in){return consume(r,std::move(in),ok);});
            if (ok) { ++stats.requests; if (Clock::now()<deadline) ++stats.completed_in_window; }
            else ++stats.errors;
        } catch (...) { ++stats.errors; }
        stats.latency_ns.push_back(std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now()-due).count());
        --active;
        busy[lane]=false;
    }
    ss::future<> closed_lane(unsigned lane,Clock::time_point deadline) {
        while (Clock::now()<deadline) { ++stats.scheduled; co_await once(lane,Clock::now(),deadline); }
    }
    ss::future<> open_phase(double seconds,Clock::time_point start,Clock::time_point deadline) {
        ss::gate pending;
        unsigned lane=0;
        // Use one integer count for the half-open arrival window. Repeated
        // division can put an exact endpoint a fraction of an ULP before it
        // (e.g.7938/264.6<30), creating one false dropped arrival. The tiny
        // request-count tolerance is far below the timer's nanosecond resolution.
        const auto offered=uint64_t(std::ceil(config.rate*seconds-1e-8));
        for (uint64_t ticket=ss::this_shard_id();ticket<offered;ticket+=ss::this_smp_shard_count()) {
            auto due=start+std::chrono::duration_cast<Clock::duration>(std::chrono::duration<double>(double(ticket)/config.rate));
            if (Clock::now()<due) co_await ss::sleep<Clock>(due-Clock::now());
            ++stats.scheduled;
            if (active>=clients.size() || Clock::now()>=deadline) ++stats.dropped;
            else {
                while (busy[lane]) lane=(lane+1)%clients.size();
                // once consumes request errors; gate retains all admitted work.
                (void)ss::with_gate(pending,[this,lane,due,deadline]{return once(lane,due,deadline);});
            }
            lane=(lane+1)%clients.size();
        }
        if (Clock::now()<deadline) co_await ss::sleep<Clock>(deadline-Clock::now());
        co_await pending.close();
    }
    uint64_t connections() const {
        uint64_t n=0; for (const auto& c:clients) n+=c->total_new_connections_nr(); return n;
    }
public:
    explicit Load(Config c):config(std::move(c)) {
        auto n=config.concurrency/ss::this_smp_shard_count()+(ss::this_shard_id()<config.concurrency%ss::this_smp_shard_count());
        for (unsigned i=0;i<n;++i) clients.push_back(std::make_unique<ss::http::client>(std::make_unique<NodelayFactory>(config.port),1));
        busy.resize(n,false);
    }
    ss::future<> prime() {
        // Prime every lane even when the arrival rate is low, for prefork fairness.
        return ss::parallel_for_each(boost::irange(0u,unsigned(clients.size())),[this](unsigned lane){return once(lane,Clock::now(),Clock::time_point::max());});
    }
    ss::future<> phase(double seconds,Clock::time_point start) {
        stats={}; auto before=connections();
        auto deadline=start+std::chrono::duration_cast<Clock::duration>(std::chrono::duration<double>(seconds));
        if (Clock::now()<start) co_await ss::sleep<Clock>(start-Clock::now());
        if (config.rate>0) co_await open_phase(seconds,start,deadline);
        else co_await ss::parallel_for_each(boost::irange(0u,unsigned(clients.size())),[this,deadline](unsigned lane){return closed_lane(lane,deadline);});
        stats.total_connections=connections(); stats.new_connections=stats.total_connections-before;
    }
    Stats get() const { return stats; }
    ss::future<> stop() { for (auto& c:clients) co_await c->close(); }
};
static std::string read_file(const std::string& path) {
    if (path.empty()) return {};
    std::ifstream f(path,std::ios::binary);
    if (!f) throw std::runtime_error("cannot open payload/expected file");
    return {std::istreambuf_iterator<char>(f),{}};
}
static double cpu_seconds() { timespec t{}; clock_gettime(CLOCK_PROCESS_CPUTIME_ID,&t); return t.tv_sec+t.tv_nsec/1e9; }
static void print_map(const std::map<int,uint64_t>& m) {
    std::cout<<'{'; bool first=true;
    for (auto [k,v]:m) { if (!first) std::cout<<','; first=false; std::cout<<'"'<<k<<"\":"<<v; }
    std::cout<<'}';
}
static ss::future<int> run(ss::app_template& app) {
    auto& o=app.configuration();
    Config cfg{o["port"].as<unsigned>(),o["concurrency"].as<unsigned>(),{},o["path"].as<std::string>(),o["method"].as<std::string>(),
        read_file(o["input"].as<std::string>()),read_file(o["expected"].as<std::string>()),o["rate"].as<double>(),
        o["policy-version"].as<std::string>(),o["result-tag"].as<std::string>()};
    std::istringstream pids(o["target-pids"].as<std::string>()); std::string pid;
    while (std::getline(pids,pid,',')) cfg.pids.insert(std::stoi(pid));
    if (o["target-pid"].as<int>()>0) cfg.pids.insert(o["target-pid"].as<int>());
    auto seconds=o["seconds"].as<double>(),warmup=o["warmup"].as<double>();
    if (cfg.concurrency<ss::this_smp_shard_count() || cfg.pids.empty() || cfg.rate<0 || !std::isfinite(cfg.rate) ||
        seconds<=0 || !std::isfinite(seconds) || warmup<0 || !std::isfinite(warmup)) throw std::runtime_error("invalid load settings");
    ss::sharded<Load> loads;
    co_await loads.start(cfg);
    co_await loads.invoke_on_all(&Load::prime);
    co_await loads.invoke_on_all(&Load::phase,warmup,Clock::now()+std::chrono::milliseconds(2));
    std::cout<<"MEASURE_READY"<<std::endl;
    {
        // Wait for the controller's start line off the reactor: a blocking read
        // here stalled shard 0, which then handled none of its connections'
        // events (a server closing an idle one, say) until the line came.
        std::string command;
        std::atomic<bool> read{false};
        std::thread reader([&] { std::getline(std::cin, command); read = true; });
        while (!read) co_await ss::sleep(std::chrono::milliseconds(1));
        reader.join();
    }
    auto cpu0=cpu_seconds(); auto start=Clock::now()+std::chrono::milliseconds(2);
    co_await loads.invoke_on_all(&Load::phase,seconds,start);
    auto elapsed=std::chrono::duration<double>(Clock::now()-start).count(),cpu=cpu_seconds()-cpu0;
    std::cout<<"MEASURE_DONE"<<std::endl;
    Stats total;
    for (unsigned s=0;s<ss::this_smp_shard_count();++s) {
        auto r=co_await loads.invoke_on(s,&Load::get);
#define ADD(f) total.f+=r.f
        ADD(requests); ADD(errors); ADD(bytes); ADD(total_connections); ADD(new_connections);
        ADD(scheduled); ADD(dropped); ADD(completed_in_window); ADD(status_errors); ADD(integrity_errors);
#undef ADD
        total.latency_ns.insert(total.latency_ns.end(),r.latency_ns.begin(),r.latency_ns.end());
        total.dispatch_ns.insert(total.dispatch_ns.end(),r.dispatch_ns.begin(),r.dispatch_ns.end());
        for (auto [k,v]:r.shards) total.shards[k]+=v;
        for (auto [k,v]:r.workers) total.workers[k]+=v;
        for (auto [k,v]:r.pids) total.pids[k]+=v;
    }
    co_await loads.stop();
    std::sort(total.latency_ns.begin(),total.latency_ns.end()); std::sort(total.dispatch_ns.begin(),total.dispatch_ns.end());
    auto pct=[](const auto& v,double p){return v.empty()?0.:v[size_t((v.size()-1)*p)]/1000.;};
    std::cout<<"{\"requests\":"<<total.requests<<",\"errors\":"<<total.errors
        <<",\"seconds\":"<<elapsed<<",\"arrival_seconds\":"<<seconds<<",\"drain_seconds\":"<<std::max(0.,elapsed-seconds)
        <<",\"cpu_seconds\":"<<cpu<<",\"cpu_cores\":"<<cpu/elapsed
        <<",\"rps\":"<<(cfg.rate>0?total.completed_in_window/seconds:total.requests/elapsed)
        <<",\"response_bytes\":"<<total.bytes<<",\"rate\":"<<cfg.rate
        <<",\"scheduled\":"<<total.scheduled<<",\"dropped\":"<<total.dropped
        <<",\"completed_in_window\":"<<total.completed_in_window<<",\"status_errors\":"<<total.status_errors
        <<",\"integrity_errors\":"<<total.integrity_errors
        <<",\"p50_us\":"<<pct(total.latency_ns,.5)<<",\"p99_us\":"<<pct(total.latency_ns,.99)<<",\"max_us\":"<<pct(total.latency_ns,1)
        <<",\"dispatch_p99_us\":"<<pct(total.dispatch_ns,.99)<<",\"dispatch_max_us\":"<<pct(total.dispatch_ns,1)
        <<",\"total_connections\":"<<total.total_connections<<",\"new_connections\":"<<total.new_connections
        <<",\"server_shards\":"; print_map(total.shards);
    std::cout<<",\"server_workers\":"; print_map(total.workers);
    std::cout<<",\"server_pids\":"; print_map(total.pids);
    std::cout<<'}'<<std::endl;
    co_return total.integrity_errors?1:0;
}
int main(int argc,char**argv) {
    namespace po=boost::program_options;
    ss::app_template app;
    app.add_options()("port",po::value<unsigned>()->required(),"server port")
        ("target-pid",po::value<int>()->default_value(0),"single server PID")
        ("target-pids",po::value<std::string>()->default_value(""),"comma-separated allowed PIDs")
        ("path",po::value<std::string>()->required(),"path")
        ("method",po::value<std::string>()->default_value("GET"),"method")
        ("input",po::value<std::string>()->default_value(""),"POST payload file")
        ("expected",po::value<std::string>()->required(),"expected response body")
        ("policy-version",po::value<std::string>()->default_value(""),"required X-Probe-Policy value; empty disables")
        ("result-tag",po::value<std::string>()->default_value(""),"required X-App-Result business result; empty disables")
        ("concurrency",po::value<unsigned>()->default_value(32),"persistent lanes and outstanding cap")
        ("rate",po::value<double>()->default_value(0),"scheduled arrivals/s; zero for closed-loop")
        ("warmup",po::value<double>()->default_value(2),"warmup seconds")
        ("seconds",po::value<double>()->default_value(5),"measurement seconds");
    return app.run(argc,argv,[&app]{return run(app);});
}
