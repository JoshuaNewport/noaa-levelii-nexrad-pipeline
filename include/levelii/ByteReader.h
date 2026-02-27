#pragma once

#include <cstdint>
#include <cstring>
#include <optional>
#include <iostream>

namespace nexrad {

// Helper to read Big-Endian values
template<typename T>
inline T read_be(const uint8_t* data) {
#if defined(__GNUC__) || defined(__clang__)
    T value;
    std::memcpy(&value, data, sizeof(T));
    if constexpr (sizeof(T) == 1) {
        return value;
    } else if constexpr (sizeof(T) == 2) {
        return __builtin_bswap16(static_cast<uint16_t>(value));
    } else if constexpr (sizeof(T) == 4) {
        return __builtin_bswap32(static_cast<uint32_t>(value));
    } else if constexpr (sizeof(T) == 8) {
        return __builtin_bswap64(static_cast<uint64_t>(value));
    } else {
        // Fallback for other sizes
        T fallback = 0;
        for (size_t i = 0; i < sizeof(T); ++i) {
            fallback = (fallback << 8) | static_cast<T>(data[i]);
        }
        return fallback;
    }
#else
    T value = 0;
    for (size_t i = 0; i < sizeof(T); ++i) {
        value = (value << 8) | static_cast<T>(data[i]);
    }
    return value;
#endif
}

// Helper to read Big-Endian float
inline float read_be_float(const uint8_t* data) {
    uint32_t val = read_be<uint32_t>(data);
    float f;
    std::memcpy(&f, &val, 4);
    return f;
}

// Helper to read Little-Endian values
template<typename T>
inline T read_le(const uint8_t* data) {
#if defined(__GNUC__) || defined(__clang__)
    T value;
    std::memcpy(&value, data, sizeof(T));
    return value; // Assuming host is little-endian
#else
    T value = 0;
    for (size_t i = 0; i < sizeof(T); ++i) {
        value |= static_cast<T>(data[i]) << (i * 8);
    }
    return value;
#endif
}

// Safe struct reading with bounds checking
template<typename T>
inline std::optional<const T*> safe_read_struct(
    const uint8_t* data,
    size_t data_size,
    size_t offset,
    const char* struct_name = nullptr
) {
    if (!data || offset > data_size) {
        if (struct_name) {
            std::cerr << "❌ Invalid offset for " << struct_name << ": offset " 
                      << offset << " exceeds data_size " << data_size << std::endl;
        }
        return std::nullopt;
    }
    
    size_t remaining = data_size - offset;
    if (remaining < sizeof(T)) {
        if (struct_name) {
            std::cerr << "❌ Buffer underrun for " << struct_name << ": need " 
                      << sizeof(T) << " bytes, have " << remaining << std::endl;
        }
        return std::nullopt;
    }
    
    return reinterpret_cast<const T*>(data + offset);
}

// Safe pointer dereference with range validation
inline bool safe_pointer_dereference(
    size_t ptr_offset,
    size_t required_size,
    size_t payload_size,
    const char* block_type = nullptr
) {
    if (ptr_offset == 0) return false; // null pointer
    
    if (ptr_offset > payload_size) {
        if (block_type) {
            std::cerr << "❌ Block pointer out of range for " << block_type << ": " 
                      << ptr_offset << " > " << payload_size << std::endl;
        }
        return false;
    }
    
    if (ptr_offset + required_size > payload_size) {
        if (block_type) {
            std::cerr << "❌ Block size overflow for " << block_type << ": " 
                      << ptr_offset << " + " << required_size << " > " << payload_size << std::endl;
        }
        return false;
    }
    
    return true;
}

} // namespace nexrad
