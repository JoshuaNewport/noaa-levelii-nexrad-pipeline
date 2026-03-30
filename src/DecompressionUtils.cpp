/*
 * NEXRAD Decompression Utilities Implementation
 * Separated from main radar server for modularity
 */

#include "levelii/DecompressionUtils.h"
#include <bzlib.h>
#include <iostream>
#include <cstring>

namespace RadarDecompression {

namespace {

constexpr bool VERBOSE_LOGGING = false;

bool decompress_bz2_raw(const uint8_t* data, size_t size, 
                        std::vector<uint8_t>& decompressed) {
    if (size == 0) return false;
    
    decompressed.resize(size * 8);
    
    bz_stream stream;
    stream.bzalloc = nullptr;
    stream.bzfree = nullptr;
    stream.opaque = nullptr;
    stream.avail_in = size;
    stream.next_in = const_cast<char*>(reinterpret_cast<const char*>(data));
    stream.avail_out = decompressed.size();
    stream.next_out = reinterpret_cast<char*>(decompressed.data());
    
    int ret = BZ2_bzDecompressInit(&stream, 0, 0);
    if (ret != BZ_OK) return false;
    
    while (true) {
        ret = BZ2_bzDecompress(&stream);
        
        if (ret == BZ_STREAM_END) {
            decompressed.resize(stream.total_out_lo32);
            BZ2_bzDecompressEnd(&stream);
            return true;
        }
        
        if (ret != BZ_OK) {
            BZ2_bzDecompressEnd(&stream);
            return false;
        }
        
        if (stream.avail_out == 0) {
            size_t old_size = decompressed.size();
            size_t grow_size = old_size >> 1;
            decompressed.resize(old_size + grow_size);
            stream.avail_out = grow_size;
            stream.next_out = reinterpret_cast<char*>(decompressed.data() + old_size);
        }
    }
}

bool decompress_bz2(const std::vector<uint8_t>& compressed, 
                    std::vector<uint8_t>& decompressed) {
    return decompress_bz2_raw(compressed.data(), compressed.size(), decompressed);
}

bool decompress_ldm(const std::vector<uint8_t>& data, 
                    std::vector<uint8_t>& decompressed) {
    decompressed.clear();
    
    if (data.size() < VOLUME_HEADER_SIZE) {
        return false;
    }
    
    decompressed.reserve(data.size() * 8 + VOLUME_HEADER_SIZE);
    decompressed.insert(decompressed.end(), data.begin(), data.begin() + VOLUME_HEADER_SIZE);
    
    size_t offset = VOLUME_HEADER_SIZE;
    int stream_count = 0;
    
    while (offset + CONTROL_WORD_SIZE < data.size()) {
        int32_t control_word = 0;
        control_word |= static_cast<int32_t>(data[offset]) << 24;
        control_word |= static_cast<int32_t>(data[offset + 1]) << 16;
        control_word |= static_cast<int32_t>(data[offset + 2]) << 8;
        control_word |= static_cast<int32_t>(data[offset + 3]);
        
        size_t block_size = std::abs(control_word);
        offset += CONTROL_WORD_SIZE;
        
        if (block_size == 0) break;
        if (offset + block_size > data.size()) {
            block_size = data.size() - offset;
        }

        bz_stream stream;
        stream.bzalloc = nullptr;
        stream.bzfree = nullptr;
        stream.opaque = nullptr;
        stream.avail_in = block_size;
        stream.next_in = const_cast<char*>(reinterpret_cast<const char*>(data.data() + offset));
        
        size_t out_offset = decompressed.size();
        decompressed.resize(out_offset + block_size * 8);
        stream.avail_out = block_size * 8;
        stream.next_out = reinterpret_cast<char*>(decompressed.data() + out_offset);
        
        int ret = BZ2_bzDecompressInit(&stream, 0, 0);
        if (ret != BZ_OK) {
            decompressed.resize(out_offset);
            break; 
        }
        
        bool stream_ok = false;
        while (true) {
            ret = BZ2_bzDecompress(&stream);
            
            if (ret == BZ_STREAM_END) {
                decompressed.resize(out_offset + stream.total_out_lo32);
                BZ2_bzDecompressEnd(&stream);
                stream_count++;
                stream_ok = true;
                break;
            }
            
            if (ret != BZ_OK) {
                BZ2_bzDecompressEnd(&stream);
                decompressed.resize(out_offset);
                break;
            }
            
            if (stream.avail_out == 0) {
                size_t current_out_size = decompressed.size() - out_offset;
                size_t grow_size = current_out_size >> 1;
                decompressed.resize(decompressed.size() + grow_size);
                stream.avail_out = grow_size;
                stream.next_out = reinterpret_cast<char*>(decompressed.data() + out_offset + current_out_size);
            }
        }
        
        if (!stream_ok) break;
        offset += block_size;
    }
    
    return stream_count > 0;
}

} // anonymous namespace

bool auto_decompress(const std::vector<uint8_t>& data, 
                     std::vector<uint8_t>& decompressed) {
    if (data.empty()) {
        decompressed.clear();
        return false;
    }
    
    if (data.size() > 2 && data[0] == 'B' && data[1] == 'Z') {
        return decompress_bz2(data, decompressed);
    }
    
    if (data.size() >= VOLUME_HEADER_SIZE + CONTROL_WORD_SIZE) {
        if (!decompress_ldm(data, decompressed)) {
            if (data.size() > VOLUME_HEADER_SIZE + 2 && 
                data[VOLUME_HEADER_SIZE] == 'B' && data[VOLUME_HEADER_SIZE+1] == 'Z') {
                return decompress_bz2_raw(data.data() + VOLUME_HEADER_SIZE, 
                                          data.size() - VOLUME_HEADER_SIZE, 
                                          decompressed);
            }
            return decompress_bz2(data, decompressed);
        }
        return true;
    }
    
    decompressed = data;
    return true;
}

}  // namespace RadarDecompression
