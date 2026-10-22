// Same native transport, pool, algorithm and result checks as the Python async path.
#include "runtime.hh"
#include <iostream>
#include <poll.h>
#include <sstream>
#include <stdexcept>
#include <unistd.h>

static void print_map(const std::map<std::string, uint64_t>& values) {
    std::cout << '{'; bool first = true;
    for (const auto& [key, value] : values) {
        if (!first) std::cout << ',';
        first = false; std::cout << '"' << key << "\":" << value;
    }
    std::cout << '}';
}

int main(int argc, char** argv) {
    if (argc != 4 && argc != 5) return 2;
    std::vector<int> cpus; std::istringstream stream(argv[2]); std::string cpu;
    while (std::getline(stream, cpu, ',')) cpus.push_back(std::stoi(cpu));
    probe::ServerConfig config; config.http_port = std::stoi(argv[1]);
    config.cooperative_compression = argc == 5 && std::string(argv[4]) == "cooperative";
    config.inline_compression = argc == 5 && std::string(argv[4]) == "inline";
    config.max_requests = 128; config.max_bytes = 256*1024*1024;
    std::vector<int> workers; std::istringstream worker_stream(argv[3]);
    while (std::getline(worker_stream, cpu, ',')) workers.push_back(std::stoi(cpu));
    probe::Runtime runtime(cpus, 1024, config.max_bytes, workers, config);
    pollfd fd{runtime.fd(), POLLIN, 0};
    while (runtime.stats().state == 0) {
        if (::poll(&fd, 1, 10000) != 1) throw std::runtime_error("startup timeout");
        runtime.drain(64);
    }
    if (runtime.stats().state != 1) throw std::runtime_error(runtime.stats().error);
    std::cout << "{\"ready\":true,\"mode\":\"native\",\"pids\":[" << getpid()
              << "],\"service_pids\":[" << getpid() << "],\"workers\":[{\"shards\":[";
    bool first = true;
    for (const auto& shard : runtime.shards()) {
        if (!first) std::cout << ','; first = false;
        std::cout << "{\"shard\":" << shard.shard << ",\"tid\":" << shard.tid
                  << ",\"cpu\":" << shard.affinity.at(0) << '}';
    }
    std::cout << "]}]}" << std::endl;
    std::map<std::string, uint64_t> before;
    for (std::string command; std::getline(std::cin, command);) {
        if (command == "begin") {
            before = runtime.server_stats().totals;
            std::cout << "{\"begun\":true}" << std::endl;
        } else if (command == "end") {
            auto after = runtime.server_stats().totals, delta = after;
            for (auto& [key, value] : delta) value -= before[key];
            std::cout << "{\"workers\":[{\"delta\":"; print_map(delta);
            std::cout << ",\"after\":"; print_map(after); std::cout << "}]}" << std::endl;
        } else if (command == "quit") break;
        else throw std::runtime_error("unknown command");
    }
    runtime.stop(); runtime.join();
    std::cout << "{\"closed\":true,\"workers\":["; print_map(runtime.server_stats().totals);
    std::cout << "]}" << std::endl;
}
