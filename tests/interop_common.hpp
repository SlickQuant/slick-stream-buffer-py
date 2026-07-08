#pragma once
//
// Framed test message shared by the C++/Python interop tests. The Python
// equivalent lives in tests/test_interop.py - both sides must agree exactly.
//
//   u32 worker_id (LE) | u32 msg_num (LE) | payload bytes
//
//   total length = 8 + (msg_num * 7) % 57      (varies 8..64 bytes)
//   payload fill = (msg_num * 31 + 7) & 0xFF
//
#include <cstdint>
#include <cstring>
#include <cstddef>

inline std::size_t interop_msg_length(uint32_t msg_num) {
    return 8 + (msg_num * 7) % 57;
}

inline uint8_t interop_fill_byte(uint32_t msg_num) {
    return static_cast<uint8_t>((msg_num * 31 + 7) & 0xFF);
}

inline void interop_write_msg(uint8_t* dst, uint32_t worker_id, uint32_t msg_num) {
    std::memcpy(dst, &worker_id, 4);
    std::memcpy(dst + 4, &msg_num, 4);
    std::memset(dst + 8, interop_fill_byte(msg_num), interop_msg_length(msg_num) - 8);
}

// Returns true when the message content is exactly as produced.
inline bool interop_check_msg(const uint8_t* data, uint32_t len,
                              uint32_t expected_worker, uint32_t* msg_num_out) {
    if (len < 8) {
        return false;
    }
    uint32_t worker_id, msg_num;
    std::memcpy(&worker_id, data, 4);
    std::memcpy(&msg_num, data + 4, 4);
    if (worker_id != expected_worker) {
        return false;
    }
    if (len != interop_msg_length(msg_num)) {
        return false;
    }
    const uint8_t fill = interop_fill_byte(msg_num);
    for (uint32_t i = 8; i < len; ++i) {
        if (data[i] != fill) {
            return false;
        }
    }
    *msg_num_out = msg_num;
    return true;
}
