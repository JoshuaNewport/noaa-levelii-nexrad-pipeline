#include "levelii/ZlibUtils.h"
#include <zlib.h>
#include <iostream>
#include <cstring>

namespace ZlibUtils {

std::vector<uint8_t> gzip_compress(const uint8_t* data, size_t data_size) {
    std::vector<uint8_t> compressed;
    if (data_size == 0) return compressed;

    z_stream stream{};
    stream.zalloc = Z_NULL;
    stream.zfree = Z_NULL;
    stream.opaque = Z_NULL;
    
    if (deflateInit2(&stream, Z_BEST_COMPRESSION, Z_DEFLATED, 
                     15 + 16, 8, Z_DEFAULT_STRATEGY) != Z_OK) {
        return compressed;
    }
    
    stream.avail_in = data_size;
    stream.next_in = const_cast<uint8_t*>(data);
    
    const size_t chunk_size = 65536;
    std::vector<uint8_t> out_buffer(chunk_size);
    
    int ret;
    do {
        stream.avail_out = chunk_size;
        stream.next_out = out_buffer.data();
        ret = deflate(&stream, Z_FINISH);
        size_t have = chunk_size - stream.avail_out;
        compressed.insert(compressed.end(), out_buffer.begin(), out_buffer.begin() + have);
    } while (ret == Z_OK);
    
    deflateEnd(&stream);
    return (ret == Z_STREAM_END) ? compressed : std::vector<uint8_t>();
}

std::vector<uint8_t> gzip_decompress(const uint8_t* data, size_t data_size) {
    std::vector<uint8_t> decompressed;
    if (data_size == 0) return decompressed;

    z_stream stream{};
    stream.zalloc = Z_NULL;
    stream.zfree = Z_NULL;
    stream.opaque = Z_NULL;
    
    if (inflateInit2(&stream, 15 + 16) != Z_OK) {
        return decompressed;
    }
    
    stream.avail_in = data_size;
    stream.next_in = const_cast<uint8_t*>(data);
    
    const size_t chunk_size = 65536;
    std::vector<uint8_t> out_buffer(chunk_size);
    
    int ret;
    do {
        stream.avail_out = chunk_size;
        stream.next_out = out_buffer.data();
        ret = inflate(&stream, Z_NO_FLUSH);
        if (ret == Z_STREAM_ERROR || ret == Z_DATA_ERROR || ret == Z_MEM_ERROR) {
            inflateEnd(&stream);
            return std::vector<uint8_t>();
        }
        size_t have = chunk_size - stream.avail_out;
        decompressed.insert(decompressed.end(), out_buffer.begin(), out_buffer.begin() + have);
    } while (ret == Z_OK);
    
    inflateEnd(&stream);
    return (ret == Z_STREAM_END) ? decompressed : std::vector<uint8_t>();
}

} // namespace ZlibUtils
