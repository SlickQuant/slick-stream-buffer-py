//
// C++ consumer for the slick-stream-buffer-py interop tests.
//
// Usage:
//   cpp_consumer <shm_name> <num_messages> <worker_id> [--lossy] [--read-last] [--timeout-s N]
//
//   default      strict mode: read from cursor 0, expect exactly num_messages
//                sequential messages (msg_num == 0..num_messages-1)
//   --lossy      tolerate loss: verify every received message's content and
//                strictly increasing msg_num; stop once msg num_messages-1 is
//                seen (buffers that lapped skip records)
//   --read-last  poll read_last() until a valid message appears; print it
//
// Prints "CONSUMED <n>" (or "LAST <worker> <num> <len>") on success, exits 0.
//
#include <slick/stream_buffer.hpp>
#include "interop_common.hpp"

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <thread>

int main(int argc, char** argv) {
    if (argc < 4) {
        std::fprintf(stderr,
            "Usage: %s <shm_name> <num_messages> <worker_id> [--lossy] [--read-last] [--timeout-s N]\n",
            argv[0]);
        return 2;
    }

    const char* shm_name = argv[1];
    const int num_messages = std::atoi(argv[2]);
    const uint32_t worker_id = static_cast<uint32_t>(std::atoi(argv[3]));

    bool lossy = false;
    bool read_last = false;
    int timeout_s = 30;

    for (int i = 4; i < argc; ++i) {
        if (std::strcmp(argv[i], "--lossy") == 0) {
            lossy = true;
        } else if (std::strcmp(argv[i], "--read-last") == 0) {
            read_last = true;
        } else if (std::strcmp(argv[i], "--timeout-s") == 0 && i + 1 < argc) {
            timeout_s = std::atoi(argv[i + 1]);
            ++i;
        } else {
            std::fprintf(stderr, "Unknown argument: %s\n", argv[i]);
            return 2;
        }
    }

    try {
        slick::stream_buffer buf(shm_name);
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(timeout_s);

        if (read_last) {
            while (std::chrono::steady_clock::now() < deadline) {
                auto [data, len] = buf.read_last();
                if (data != nullptr) {
                    uint32_t msg_num = 0;
                    if (!interop_check_msg(data, len, worker_id, &msg_num)) {
                        std::fprintf(stderr, "read_last returned corrupt message (len=%u)\n", len);
                        return 1;
                    }
                    std::printf("LAST %u %u %u\n", worker_id, msg_num, len);
                    std::fflush(stdout);
                    return 0;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
            std::fprintf(stderr, "read_last timed out\n");
            return 1;
        }

        uint64_t cursor = 0;
        int received = 0;
        int64_t last_msg_num = -1;

        for (;;) {
            if (std::chrono::steady_clock::now() >= deadline) {
                std::fprintf(stderr, "timed out after %d messages (last msg_num %lld)\n",
                             received, static_cast<long long>(last_msg_num));
                return 1;
            }

            auto [data, len] = buf.read(cursor);
            if (data == nullptr) {
                std::this_thread::yield();
                continue;
            }

            uint32_t msg_num = 0;
            if (!interop_check_msg(data, len, worker_id, &msg_num)) {
                std::fprintf(stderr, "corrupt message after %d received (len=%u)\n", received, len);
                return 1;
            }

            if (lossy) {
                if (static_cast<int64_t>(msg_num) <= last_msg_num) {
                    std::fprintf(stderr, "msg_num went backwards: %u after %lld\n",
                                 msg_num, static_cast<long long>(last_msg_num));
                    return 1;
                }
            } else {
                if (msg_num != static_cast<uint32_t>(received)) {
                    std::fprintf(stderr, "expected msg_num %d, got %u\n", received, msg_num);
                    return 1;
                }
            }

            last_msg_num = msg_num;
            ++received;

            if (lossy) {
                if (msg_num == static_cast<uint32_t>(num_messages - 1)) {
                    break;  // final message observed
                }
            } else if (received == num_messages) {
                break;
            }
        }

        std::printf("CONSUMED %d\n", received);
        std::fflush(stdout);
        return 0;
    } catch (const std::exception& e) {
        std::fprintf(stderr, "cpp_consumer error: %s\n", e.what());
        return 1;
    }
}
