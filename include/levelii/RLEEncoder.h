#pragma once

#include <cstdint>
#include <vector>
#include <cstring>

class RLEEncoder {
public:
    static std::vector<uint8_t> encode(const std::vector<uint8_t>& data) {
        std::vector<uint8_t> encoded;
        if (data.empty()) return encoded;

        encoded.reserve(data.size());
        size_t i = 0;

        while (i < data.size()) {
            uint8_t current_value = data[i];
            size_t run_length = 1;

            while (i + run_length < data.size() &&
                   data[i + run_length] == current_value &&
                   run_length < 255) {
                run_length++;
            }

            if (run_length >= 3) {
                encoded.push_back(0xFF);
                encoded.push_back(current_value);
                encoded.push_back(static_cast<uint8_t>(run_length));
                i += run_length;
            } else {
                for (size_t j = 0; j < run_length; j++) {
                    encoded.push_back(current_value);
                    if (current_value == 0xFF) {
                        encoded.push_back(0x00);
                    }
                }
                i += run_length;
            }
        }

        return encoded;
    }

    static std::vector<uint8_t> decode(const std::vector<uint8_t>& encoded) {
        std::vector<uint8_t> decoded;
        decoded.reserve(encoded.size());

        size_t i = 0;
        while (i < encoded.size()) {
            if (encoded[i] == 0xFF && i + 2 < encoded.size()) {
                uint8_t value = encoded[i + 1];
                uint8_t count = encoded[i + 2];
                for (int j = 0; j < count; j++) {
                    decoded.push_back(value);
                }
                i += 3;
            } else if (encoded[i] == 0xFF && i + 1 < encoded.size() && encoded[i + 1] == 0x00) {
                decoded.push_back(0xFF);
                i += 2;
            } else {
                decoded.push_back(encoded[i]);
                i++;
            }
        }

        return decoded;
    }

    static float compression_ratio(const std::vector<uint8_t>& original,
                                   const std::vector<uint8_t>& compressed) {
        if (original.empty()) return 1.0f;
        return 100.0f * static_cast<float>(compressed.size()) / static_cast<float>(original.size());
    }
};
