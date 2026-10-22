#include "runtime.hh"
#include <iostream>
#include <poll.h>
#include <stdexcept>
#include <unistd.h>
int main(int argc, char** argv) {
    if (argc != 3) return 2;
    probe::Runtime runtime({std::stoi(argv[1]), std::stoi(argv[2])}, 16, 1024);
    pollfd fd{runtime.fd(), POLLIN, 0};
    auto wait = [&] {
        if (::poll(&fd, 1, 10000) != 1) throw std::runtime_error("native baseline timed out");
    };
    while (runtime.stats().state == 0) { wait(); runtime.drain(16); }
    if (runtime.stats().state != 1) throw std::runtime_error(runtime.stats().error);
    for (auto& info : runtime.shards()) {
        std::cout << "shard=" << info.shard << " tid=" << info.tid << " backend=" << info.backend << " affinity=";
        for (auto cpu : info.affinity) std::cout << cpu << ',';
        std::cout << '\n';
    }
    runtime.submit(1, "native-timer", 20);
    bool received = false;
    while (!received) {
        wait();
        for (auto& c : runtime.drain(16)) {
            if (c.payload != "native-timer" || c.pid != getpid()) return 3;
            received = true;
        }
    }
    runtime.stop();
    runtime.join();
    std::cout << "native_timer_roundtrip=passed live_workers=" << runtime.stats().live_workers << '\n';
}
