// Persistent HTTP lanes, closed-loop or bounded scheduled arrivals: the load
// generator of the comparisons. One thread per --smp, each with its own
// io_context and its share of the lanes (one connection each), pinned to its
// CPU of --cpuset.
#include <asio.hpp>
#include <algorithm>
#include <cctype>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <cmath>
#include <cstring>
#include <fstream>
#include <future>
#include <iostream>
#include <map>
#include <memory>
#include <set>
#include <sstream>
#include <string>
#include <system_error>
#include <thread>
#include <vector>
#include <pthread.h>
#include <sched.h>
#include <time.h>
using Clock=std::chrono::steady_clock;
using asio::awaitable;
using asio::use_awaitable;
using asio::ip::tcp;
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
static Clock::duration seconds_of(double seconds) {
    return std::chrono::duration_cast<Clock::duration>(std::chrono::duration<double>(seconds));
}
static bool same_name(std::string_view a, std::string_view b) {
    return a.size()==b.size() && std::equal(a.begin(),a.end(),b.begin(),[](unsigned char x,unsigned char y){return std::tolower(x)==std::tolower(y);});
}
static bool to_int(std::string_view text, int& value) {
    auto [end,error]=std::from_chars(text.data(),text.data()+text.size(),value);
    return error==std::errc() && end==text.data()+text.size();
}
// What of a response head the checks need.
struct Head {
    int status=0;
    std::string_view pid, policy, result, shard, worker, length, encoding, connection;
};
static Head parse_head(std::string_view head) {
    Head h;
    if (head.size()<12 || !head.starts_with("HTTP/1.") || !to_int(head.substr(9,3),h.status))
        throw std::runtime_error("invalid status line");
    for (size_t at=head.find("\r\n")+2; at<head.size(); ) {
        auto end=head.find("\r\n",at);
        auto line=head.substr(at,end-at); at=end+2;
        if (line.empty()) break;
        auto colon=line.find(':');
        if (colon==line.npos) throw std::runtime_error("invalid header");
        auto name=line.substr(0,colon), value=line.substr(colon+1);
        while (!value.empty() && (value.front()==' ' || value.front()=='\t')) value.remove_prefix(1);
        while (!value.empty() && (value.back()==' ' || value.back()=='\t')) value.remove_suffix(1);
        for (auto [field,target]: {std::pair{"X-Probe-Pid",&h.pid},{"X-Probe-Policy",&h.policy},{"X-App-Result",&h.result},
                                   {"X-Probe-Shard",&h.shard},{"X-Probe-Worker",&h.worker},{"Content-Length",&h.length},
                                   {"Transfer-Encoding",&h.encoding},{"Connection",&h.connection}})
            if (same_name(name,field)) *target=value;
    }
    return h;
}
// The waits of a phase for the requests it started (Seastar's gate).
struct Pending {
    unsigned count=0;
    asio::steady_timer timer;
    explicit Pending(asio::io_context& context):timer(context) {}
    void leave() { if (--count==0) timer.cancel(); }
    awaitable<void> wait() {
        while (count) {
            timer.expires_at(Clock::time_point::max());
            asio::error_code error;
            co_await timer.async_wait(asio::redirect_error(use_awaitable,error));
        }
    }
};
class Load {
    struct Lane {
        tcp::socket socket;
        std::string buffer;
        bool busy=false;
    };
    const Config& config;
    asio::io_context& context;
    unsigned id, count;
    std::string request;
    std::vector<std::unique_ptr<Lane>> lanes;
    unsigned active=0;
    uint64_t connects=0;
    asio::steady_timer clock;
    awaitable<void> sleep_until(Clock::time_point when) {
        if (Clock::now()>=when) co_return;
        clock.expires_at(when);
        co_await clock.async_wait(use_awaitable);
    }
    // Reads one response: false for a status other than 200, an exception for
    // a body, PID or framing the check does not accept.
    awaitable<bool> consume(Lane& lane) {
        auto& buffer=lane.buffer;
        auto head_size=co_await asio::async_read_until(lane.socket,asio::dynamic_buffer(buffer),"\r\n\r\n",use_awaitable);
        auto h=parse_head(std::string_view(buffer).substr(0,head_size));
        bool success=h.status==200, bad=false, close=same_name(h.connection,"close");
        int pid=0;
        if (success) {
            if (!to_int(h.pid,pid) || !config.pids.contains(pid)) bad=true;
            if (!config.policy_version.empty() && h.policy!=config.policy_version) bad=true;
            if (!config.result_tag.empty() && h.result!=config.result_tag) bad=true;
        }
        int shard=-1, worker=-1;
        if (!h.shard.empty() && !to_int(h.shard,shard)) bad=true;
        if (!h.worker.empty() && !to_int(h.worker,worker)) bad=true;
        std::string body;
        size_t at=head_size;
        // At least `bytes` past `at` in the buffer; at least one more byte.
        auto need=[&](size_t bytes)->awaitable<void> {
            if (buffer.size()<at+bytes)
                co_await asio::async_read(lane.socket,asio::dynamic_buffer(buffer),asio::transfer_at_least(at+bytes-buffer.size()),use_awaitable);
        };
        auto more=[&]()->awaitable<void> {
            co_await asio::async_read(lane.socket,asio::dynamic_buffer(buffer),asio::transfer_at_least(1),use_awaitable);
        };
        std::string_view received;
        if (same_name(h.encoding,"chunked")) {
            for (;;) {
                auto line_end=buffer.find("\r\n",at);
                while (line_end==buffer.npos) {
                    co_await more();
                    line_end=buffer.find("\r\n",at);
                }
                size_t size=0;
                auto [end,error]=std::from_chars(buffer.data()+at,buffer.data()+line_end,size,16);
                if (error!=std::errc()) throw std::runtime_error("invalid chunk size");
                at=line_end+2;
                if (size==0) {
                    // No trailers: the empty line ends the message.
                    co_await need(2);
                    at+=2;
                    break;
                }
                co_await need(size+2);
                body.append(buffer,at,size); at+=size+2;
            }
            received=body;
        } else {
            int length=0;
            if (!h.encoding.empty() || !to_int(h.length,length) || length<0) throw std::runtime_error("unsupported framing");
            co_await need(size_t(length));
            received=std::string_view(buffer).substr(at,length); at+=length;
        }
        if (success && received!=config.expected) bad=true;
        auto bytes=received.size();
        buffer.erase(0,at);
        if (close) { asio::error_code ignored; lane.socket.close(ignored); }
        if (!success) { ++stats.status_errors; co_return false; }
        if (bad) {
            ++stats.integrity_errors;
            throw std::runtime_error("response body/PID mismatch");
        }
        if (shard>=0) ++stats.shards[shard];
        if (worker>=0) ++stats.workers[worker];
        ++stats.pids[pid]; stats.bytes+=bytes;
        co_return true;
    }
    // A lane taken for a request, as the request starts.
    void claim(Lane& lane, Clock::time_point due) {
        lane.busy=true;
        ++active;
        stats.dispatch_ns.push_back(std::max<int64_t>(0,std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now()-due).count()));
    }
    awaitable<void> once(Lane& lane, Clock::time_point due, Clock::time_point deadline) {
        bool ok=false;
        try {
            if (!lane.socket.is_open()) {
                lane.buffer.clear();
                co_await lane.socket.async_connect(tcp::endpoint(asio::ip::address_v4::loopback(),uint16_t(config.port)),use_awaitable);
                lane.socket.set_option(tcp::no_delay(true));
                ++connects;
            }
            co_await asio::async_write(lane.socket,asio::buffer(request),use_awaitable);
            ok=co_await consume(lane);
        } catch (...) {
            asio::error_code ignored;
            lane.socket.close(ignored);
        }
        if (ok) { ++stats.requests; if (Clock::now()<deadline) ++stats.completed_in_window; }
        else ++stats.errors;
        stats.latency_ns.push_back(std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now()-due).count());
        --active;
        lane.busy=false;
    }
    void launch(Pending& pending, unsigned lane, Clock::time_point due, Clock::time_point deadline) {
        claim(*lanes[lane],due);
        ++pending.count;
        asio::co_spawn(context,[this,&pending,lane,due,deadline]()->awaitable<void> {
            co_await once(*lanes[lane],due,deadline);
            pending.leave();
        },asio::detached);
    }
    awaitable<void> closed_lane(Lane& lane, Clock::time_point deadline) {
        while (Clock::now()<deadline) {
            ++stats.scheduled;
            auto due=Clock::now();
            claim(lane,due);
            co_await once(lane,due,deadline);
        }
    }
    awaitable<void> open_phase(double seconds, Clock::time_point start, Clock::time_point deadline) {
        Pending pending(context);
        unsigned lane=0;
        // Use one integer count for the half-open arrival window. Repeated
        // division can put an exact endpoint a fraction of an ULP before it
        // (e.g.7938/264.6<30), creating one false dropped arrival. The tiny
        // request-count tolerance is far below the timer's nanosecond resolution.
        const auto offered=uint64_t(std::ceil(config.rate*seconds-1e-8));
        for (uint64_t ticket=id; ticket<offered; ticket+=count) {
            auto due=start+seconds_of(double(ticket)/config.rate);
            co_await sleep_until(due);
            ++stats.scheduled;
            if (active>=lanes.size() || Clock::now()>=deadline) ++stats.dropped;
            else {
                while (lanes[lane]->busy) lane=(lane+1)%lanes.size();
                launch(pending,lane,due,deadline);
            }
            lane=(lane+1)%lanes.size();
        }
        co_await sleep_until(deadline);
        co_await pending.wait();
    }
public:
    Stats stats;
    Load(const Config& c, asio::io_context& ctx, unsigned id, unsigned count)
        :config(c), context(ctx), id(id), count(count), clock(ctx) {
        request=config.method+" "+config.path+" HTTP/1.1\r\nHost: 127.0.0.1\r\n";
        if (config.method=="POST")
            request+="Content-Type: application/octet-stream\r\nContent-Length: "+std::to_string(config.input.size())+"\r\n";
        request+="\r\n"+(config.method=="POST"?config.input:std::string());
        auto n=config.concurrency/count+(id<config.concurrency%count);
        for (unsigned i=0;i<n;++i) lanes.push_back(std::make_unique<Lane>(Lane{tcp::socket(ctx),{},false}));
    }
    awaitable<void> prime() {
        // Prime every lane even when the arrival rate is low, for prefork fairness.
        Pending pending(context);
        for (unsigned lane=0;lane<lanes.size();++lane) launch(pending,lane,Clock::now(),Clock::time_point::max());
        co_await pending.wait();
    }
    awaitable<void> phase(double seconds, Clock::time_point start) {
        stats={}; auto before=connects;
        auto deadline=start+seconds_of(seconds);
        co_await sleep_until(start);
        if (config.rate>0) co_await open_phase(seconds,start,deadline);
        else {
            Pending pending(context);
            for (auto& lane: lanes) {
                ++pending.count;
                asio::co_spawn(context,[this,&pending,&lane,deadline]()->awaitable<void> {
                    co_await closed_lane(*lane,deadline);
                    pending.leave();
                },asio::detached);
            }
            co_await pending.wait();
        }
        stats.total_connections=connects; stats.new_connections=connects-before;
    }
    awaitable<void> stop() {
        for (auto& lane: lanes) { asio::error_code ignored; lane->socket.close(ignored); }
        co_return;
    }
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
static std::vector<int> split_ints(const std::string& text) {
    std::vector<int> values; std::istringstream items(text); std::string item;
    while (std::getline(items,item,',')) values.push_back(std::stoi(item));
    return values;
}
static void pin(int cpu) {
    cpu_set_t set; CPU_ZERO(&set); CPU_SET(cpu,&set);
    if (int error=pthread_setaffinity_np(pthread_self(),sizeof set,&set)) throw std::system_error(error,std::generic_category(),"pin");
}
static int run(std::map<std::string,std::string>& o) {
    Config cfg{unsigned(std::stoul(o["port"])),unsigned(std::stoul(o["concurrency"])),{},o["path"],o["method"],
        read_file(o["input"]),read_file(o["expected"]),std::stod(o["rate"]),o["policy-version"],o["result-tag"]};
    for (int pid: split_ints(o["target-pids"])) cfg.pids.insert(pid);
    if (std::stoi(o["target-pid"])>0) cfg.pids.insert(std::stoi(o["target-pid"]));
    auto cpus=split_ints(o["cpuset"]);
    unsigned smp=o["smp"].empty()?std::max<size_t>(1,cpus.size()):std::stoul(o["smp"]);
    auto seconds=std::stod(o["seconds"]),warmup=std::stod(o["warmup"]);
    if (smp==0 || (!cpus.empty() && cpus.size()<smp) || cfg.concurrency<smp || cfg.pids.empty() || cfg.rate<0 ||
        !std::isfinite(cfg.rate) || seconds<=0 || !std::isfinite(seconds) || warmup<0 || !std::isfinite(warmup))
        throw std::runtime_error("invalid load settings");
    if (!cpus.empty()) {
        // This thread too (the controller's lines), within the budget.
        cpu_set_t set; CPU_ZERO(&set);
        for (int cpu: cpus) CPU_SET(cpu,&set);
        if (sched_setaffinity(0,sizeof set,&set)) throw std::system_error(errno,std::generic_category(),"cpuset");
    }
    std::vector<std::unique_ptr<asio::io_context>> contexts;
    std::vector<std::unique_ptr<Load>> loads;
    for (unsigned i=0;i<smp;++i) {
        contexts.push_back(std::make_unique<asio::io_context>(1));
        loads.push_back(std::make_unique<Load>(cfg,*contexts[i],i,smp));
    }
    std::vector<asio::executor_work_guard<asio::io_context::executor_type>> guards;
    std::vector<std::thread> threads;
    std::vector<std::promise<void>> pinned(smp);
    for (unsigned i=0;i<smp;++i) {
        guards.push_back(asio::make_work_guard(*contexts[i]));
        threads.emplace_back([&,i] {
            try { if (!cpus.empty()) pin(cpus[i]); pinned[i].set_value(); }
            catch (...) { pinned[i].set_exception(std::current_exception()); return; }
            contexts[i]->run();
        });
    }
    // On every thread at once; waits for all, rethrowing a failure.
    auto on_all=[&](auto make) {
        std::vector<std::future<void>> done;
        for (unsigned i=0;i<smp;++i) done.push_back(asio::co_spawn(*contexts[i],make(*loads[i]),asio::use_future));
        for (auto& d: done) d.get();
    };
    auto finish=[&] {
        for (auto& guard: guards) guard.reset();
        for (auto& thread: threads) thread.join();
    };
    try {
        for (auto& p: pinned) p.get_future().get();
        on_all([](Load& l){return l.prime();});
        auto warm=Clock::now()+std::chrono::milliseconds(2);
        on_all([&](Load& l){return l.phase(warmup,warm);});
    } catch (...) { for (auto& c: contexts) c->stop(); finish(); throw; }
    std::cout<<"MEASURE_READY"<<std::endl;
    // The threads keep serving their connections' events meanwhile.
    std::string command;
    std::getline(std::cin,command);
    auto cpu0=cpu_seconds(); auto start=Clock::now()+std::chrono::milliseconds(2);
    on_all([&](Load& l){return l.phase(seconds,start);});
    auto elapsed=std::chrono::duration<double>(Clock::now()-start).count(),cpu=cpu_seconds()-cpu0;
    std::cout<<"MEASURE_DONE"<<std::endl;
    Stats total;
    for (auto& load: loads) {
        auto& r=load->stats;
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
    on_all([](Load& l){return l.stop();});
    finish();
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
    return total.integrity_errors?1:0;
}
int main(int argc,char**argv) {
    // --name value; the defaults of the ones that have one.
    std::map<std::string,std::string> options{{"port",""},{"target-pid","0"},{"target-pids",""},{"path",""},
        {"method","GET"},{"input",""},{"expected",""},{"policy-version",""},{"result-tag",""},{"concurrency","32"},
        {"rate","0"},{"warmup","2"},{"seconds","5"},{"smp",""},{"cpuset",""}};
    try {
        for (int i=1;i<argc;i+=2) {
            std::string name=argv[i];
            if (!name.starts_with("--") || !options.contains(name.substr(2)) || i+1>=argc)
                throw std::runtime_error("unknown or incomplete option "+name);
            options[name.substr(2)]=argv[i+1];
        }
        for (auto required: {"port","path","expected"})
            if (options[required].empty()) throw std::runtime_error(std::string("--")+required+" is required");
        return run(options);
    } catch (const std::exception& e) {
        std::cerr<<"http_load: "<<e.what()<<std::endl;
        return 2;
    }
}
