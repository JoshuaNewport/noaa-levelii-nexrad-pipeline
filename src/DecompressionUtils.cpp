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

// Enable verbose logging for decompression
constexpr bool VERBOSE_LOGGING = false;

// ============================================================================
// Decompress bzip2 data (ULTRA-OPTIMIZED - TARGET: <100ms)
// ============================================================================
bool decompress_bz2(const std::vector<uint8_t>& compressed, 
                    std::vector<uint8_t>& decompressed) {
    if (compressed.empty()) return false;
    
    // OPTIMIZATION 1: Better initial size estimate for NEXRAD data
    // Typical NEXRAD bz2 compression ratio is ~10-12x
    // Pre-allocate 12x to minimize reallocations
    decompressed.resize(compressed.size() * 12);
    
    bz_stream stream;
    stream.bzalloc = nullptr;
    stream.bzfree = nullptr;
    stream.opaque = nullptr;
    stream.avail_in = compressed.size();
    stream.next_in = const_cast<char*>(reinterpret_cast<const char*>(compressed.data()));
    stream.avail_out = decompressed.size();
    stream.next_out = reinterpret_cast<char*>(decompressed.data());
    
    // OPTIMIZATION 2: Use small=0 for maximum speed (uses more memory but faster)
    int ret = BZ2_bzDecompressInit(&stream, 0, 0);
    if (ret != BZ_OK) return false;
    
    while (true) {
        ret = BZ2_bzDecompress(&stream);
        
        if (ret == BZ_STREAM_END) {
            // OPTIMIZATION 3: Just resize, don't shrink_to_fit (expensive!)
            decompressed.resize(stream.total_out_lo32);
            BZ2_bzDecompressEnd(&stream);
            return true;
        }
        
        if (ret != BZ_OK) {
            BZ2_bzDecompressEnd(&stream);
            return false;
        }
        
        // OPTIMIZATION 4: Grow by 1.5x for fewer reallocations while reducing memory waste
        if (stream.avail_out == 0) {
            size_t old_size = decompressed.size();
            size_t grow_size = old_size >> 1;  // 0.5x = 1.5x total
            decompressed.resize(old_size + grow_size);
            stream.avail_out = grow_size;
            stream.next_out = reinterpret_cast<char*>(decompressed.data() + old_size);
        }
    }
}

// ============================================================================
// Decompress LDM compressed NEXRAD file (ICD-COMPLIANT & OPTIMIZED)
// ============================================================================
bool decompress_ldm(const std::vector<uint8_t>& data, 
                    std::vector<uint8_t>& decompressed) {
    decompressed.clear();
    
    if (data.size() < VOLUME_HEADER_SIZE) {
        return false;
    }
    
    // Pre-allocate with better estimate (12x typical compression ratio)
    decompressed.reserve(data.size() * 12 + VOLUME_HEADER_SIZE);
    
    // 1. Copy Volume Header (24 bytes)
    decompressed.insert(decompressed.end(), data.begin(), data.begin() + VOLUME_HEADER_SIZE);
    
    size_t offset = VOLUME_HEADER_SIZE;
    int stream_count = 0;
    
    // 2. Process LDM Compressed Records
    // Each record: 4-byte big-endian control word + compressed block
    while (offset + CONTROL_WORD_SIZE < data.size()) {
        // Read 4-byte big-endian signed binary control word
        int32_t control_word = 0;
        control_word |= static_cast<int32_t>(data[offset]) << 24;
        control_word |= static_cast<int32_t>(data[offset + 1]) << 16;
        control_word |= static_cast<int32_t>(data[offset + 2]) << 8;
        control_word |= static_cast<int32_t>(data[offset + 3]);
        
        // ICD: "absolute value of the control word must be used for determining the size"
        size_t block_size = std::abs(control_word);
        offset += CONTROL_WORD_SIZE;
        
        if (block_size == 0) break;
        if (offset + block_size > data.size()) {
            // Safety check: if block size is invalid, try to use remaining data
            block_size = data.size() - offset;
        }

        bz_stream stream;
        stream.bzalloc = nullptr;
        stream.bzfree = nullptr;
        stream.opaque = nullptr;
        stream.avail_in = block_size;
        stream.next_in = const_cast<char*>(reinterpret_cast<const char*>(data.data() + offset));
        
        // Initial output buffer size (12x estimate)
        size_t out_offset = decompressed.size();
        decompressed.resize(out_offset + block_size * 12);
        stream.avail_out = block_size * 12;
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
                // Resize to actual decompressed size
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
            
            // Grow output buffer if needed (1.5x)
            if (stream.avail_out == 0) {
                size_t current_out_size = decompressed.size() - out_offset;
                size_t grow_size = current_out_size >> 1;
                decompressed.resize(decompressed.size() + grow_size);
                stream.avail_out = grow_size;
                stream.next_out = reinterpret_cast<char*>(decompressed.data() + out_offset + current_out_size);
            }
        }
        
        if (!stream_ok) break;
        
        // Move to next record
        offset += block_size;
    }
    
    if (VERBOSE_LOGGING) {
        // Silenced decompressed LDM blocks log
    }
    
    return stream_count > 0;
}

} // anonymous namespace

// ============================================================================
// Auto-detect and decompress NEXRAD data
// ============================================================================
bool auto_decompress(const std::vector<uint8_t>& data, 
                     std::vector<uint8_t>& decompressed) {
    if (data.empty()) {
        decompressed.clear();
        return false;
    }
    
    // Check if data is bzip2 compressed (starts with "BZ")
    if (data.size() > 2 && data[0] == 'B' && data[1] == 'Z') {
        return decompress_bz2(data, decompressed);
    }
    
    // Check for LDM compressed format
    if (data.size() >= VOLUME_HEADER_SIZE + CONTROL_WORD_SIZE) {
        // LDM format typically has AR2V magic or specific headers
        if (!decompress_ldm(data, decompressed)) {
            // Try treating as raw bzip2 even if not starting with BZ
            return decompress_bz2(data, decompressed);
        }
        return true;
    }
    
    // Data might already be uncompressed - copy it
    decompressed = data;
    return true;
}

}  // namespace RadarDecompression
