//
// C++ producer for the slick-stream-buffer-py interop tests.
//
// Usage:
//   cpp_producer <shm_name> <num_messages> <worker_id>
//                [create <capacity> <control_size>] [--hold] [--delay-us N]
//
//   create ...   create the segment (creator constructor); otherwise open existing
//   --hold       after publishing, wait for a line on stdin before exiting
//                (keeps the segment alive on Windows so Python can attach)
//   --delay-us N sleep N microseconds between messages
//
// Prints "PUBLISHED <n>" on success and exits 0.
//
#include <slick/stream_buffer.hpp>
#include "interop_common.hpp"

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <memory>
#include <string>
#include <thread>

int main(int argc, char** argv) {
    if (argc < 4) {
        std::fprintf(stderr,
            "Usage: %s <shm_name> <num_messages> <worker_id> "
            "[create <capacity> <control_size>] [--hold] [--delay-us N]\n", argv[0]);
        return 2;
    }

    const char* shm_name = argv[1];
    const int num_messages = std::atoi(argv[2]);
    const uint32_t worker_id = static_cast<uint32_t>(std::atoi(argv[3]));

    bool create = false;
    uint64_t capacity = 0;
    uint32_t control_size = 0;
    bool hold = false;
    int delay_us = 0;

    for (int i = 4; i < argc; ++i) {
        if (std::strcmp(argv[i], "create") == 0 && i + 2 < argc) {
            create = true;
            capacity = std::strtoull(argv[i + 1], nullptr, 10);
            control_size = static_cast<uint32_t>(std::strtoul(argv[i + 2], nullptr, 10));
            i += 2;
        } else if (std::strcmp(argv[i], "--hold") == 0) {
            hold = true;
        } else if (std::strcmp(argv[i], "--delay-us") == 0 && i + 1 < argc) {
            delay_us = std::atoi(argv[i + 1]);
            ++i;
        } else {
            std::fprintf(stderr, "Unknown argument: %s\n", argv[i]);
            return 2;
        }
    }

    try {
        std::unique_ptr<slick::stream_buffer> buf;
        if (create) {
            buf = std::make_unique<slick::stream_buffer>(capacity, control_size, shm_name);
        } else {
            buf = std::make_unique<slick::stream_buffer>(shm_name);
        }

        for (int i = 0; i < num_messages; ++i) {
            const std::size_t len = interop_msg_length(static_cast<uint32_t>(i));
            auto [ptr, sz] = buf->prepare(len);
            if (ptr == nullptr || sz != len) {
                std::fprintf(stderr, "prepare(%zu) failed at message %d\n", len, i);
                return 1;
            }
            interop_write_msg(ptr, worker_id, static_cast<uint32_t>(i));
            buf->commit(len);
            buf->consume(len);
            if (delay_us > 0) {
                std::this_thread::sleep_for(std::chrono::microseconds(delay_us));
            }
        }

        std::printf("PUBLISHED %d\n", num_messages);
        std::fflush(stdout);

        if (hold) {
            std::string line;
            std::getline(std::cin, line);
        }
        return 0;
    } catch (const std::exception& e) {
        std::fprintf(stderr, "cpp_producer error: %s\n", e.what());
        return 1;
    }
}
