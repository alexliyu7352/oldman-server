// Pure Seastar HTTP JSON baseline: no Python interpreter and three pinned reactors.
#include "runtime.hh"
#include <iostream>
#include <poll.h>
#include <sstream>
#include <stdexcept>
#include <unistd.h>

static void print_map(std::map<std::string, uint64_t> values) {
    values["python_application_started"] = 0;
    values["python_application_finished"] = 0;
    values["python_application_errors"] = 0;
    values["python_application_active"] = 0;
    std::cout << '{'; bool first = true;
    for (const auto& [key, value] : values) {
        if (!first) std::cout << ',';
        first = false; std::cout << '"' << key << "\":" << value;
    }
    std::cout << '}';
}

int main(int argc, char** argv) {
    if (argc != 3) return 2;
    std::vector<int> cpus;
    std::istringstream stream(argv[2]); std::string token;
    while (std::getline(stream, token, ',')) cpus.push_back(std::stoi(token));
    if (cpus.size() != 3) throw std::invalid_argument("native3 requires exactly three reactors");
    probe::ServerConfig config;
    config.http_port = std::stoi(argv[1]);
    config.max_requests = 1024;
    config.max_bytes = 64*1024*1024;
    config.max_body_bytes = 65536;
    config.max_response_bytes = 65536;
    config.decision_timeout_ms = 5000;
    config.generate_date_header = true;
    config.native_json = true;
    probe::Runtime runtime(cpus, 1024, config.max_bytes, {}, config);
    pollfd fd{runtime.fd(), POLLIN, 0};
    while (runtime.stats().state == 0) {
        if (::poll(&fd, 1, 10000) != 1) throw std::runtime_error("startup timeout");
        runtime.drain(64);
    }
    if (runtime.stats().state != 1) throw std::runtime_error(runtime.stats().error);
    std::cout << "{\"ready\":true,\"mode\":\"native3\",\"pids\":[" << getpid()
              << "],\"service_pids\":[" << getpid() << "],\"workers\":[{\"shards\":[";
    bool first = true;
    for (const auto& shard : runtime.shards()) {
        if (!first) std::cout << ',';
        first = false;
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
        else throw std::runtime_error("unknown control command");
    }
    runtime.stop(); runtime.join();
    std::cout << "{\"closed\":true,\"workers\":[";
    print_map(runtime.server_stats().totals);
    std::cout << "]}" << std::endl;
}
