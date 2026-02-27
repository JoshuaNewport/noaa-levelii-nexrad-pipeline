#include <iostream>
#include <fstream>
#include <vector>
#include <memory>
#include <iomanip>
#include <cstring>
#include "levelii/NEXRAD_Types.h"
#include "levelii/DecompressionUtils.h"
#include "levelii/ByteReader.h"

using nexrad::read_be;

int main(int argc, char** argv) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <radar_file>" << std::endl;
        return 1;
    }

    std::string filename = argv[1];
    std::ifstream file(filename, std::ios::binary | std::ios::ate);
    if (!file.is_open()) {
        std::cerr << "Could not open file: " << filename << std::endl;
        return 1;
    }

    std::streamsize size = file.tellg();
    file.seekg(0, std::ios::beg);
    std::vector<uint8_t> buffer(size);
    file.read((char*)buffer.data(), size);

    std::vector<uint8_t> decompressed;
    if (!RadarDecompression::auto_decompress(buffer, decompressed)) {
        std::cerr << "Failed to decompress" << std::endl;
        return 1;
    }

    const uint8_t* data = decompressed.data();
    size_t dsize = decompressed.size();
    size_t offset = 0;

    if (dsize >= 24 && (std::memcmp(data, "ARCHIVE2", 8) == 0 || std::memcmp(data, "AR2V", 4) == 0)) {
        offset = 24;
    }

    // Skip metadata
    offset += (134 * 2432);

    int last_seq = -1;
    int gaps = 0;
    int total_msg = 0;

    while (offset + 16 <= dsize) {
        // Find next header
        size_t found_offset = 0;
        for (size_t skip : {12UL, 0UL}) {
            if (offset + skip + 16 > dsize) continue;
            const nexrad::MessageHeader* hdr = reinterpret_cast<const nexrad::MessageHeader*>(data + offset + skip);
            uint8_t type = hdr->type;
            uint16_t sz = read_be<uint16_t>(reinterpret_cast<const uint8_t*>(&hdr->size));
            uint16_t julian = read_be<uint16_t>(reinterpret_cast<const uint8_t*>(&hdr->julian_date));
            if (type > 0 && type <= 32 && sz >= 8 && sz < 32768 && julian > 10000) {
                found_offset = offset + skip;
                break;
            }
        }

        if (found_offset == 0) {
            offset++;
            continue;
        }

        const nexrad::MessageHeader* hdr = reinterpret_cast<const nexrad::MessageHeader*>(data + found_offset);
        uint16_t seq = read_be<uint16_t>(reinterpret_cast<const uint8_t*>(&hdr->sequence_num));
        uint16_t sz_hw = read_be<uint16_t>(reinterpret_cast<const uint8_t*>(&hdr->size));
        uint8_t type = hdr->type;

        if (total_msg < 200 || gaps < 10) {
            std::cout << "Msg Seq=" << seq << " Type=" << (int)type << " SizeHW=" << sz_hw << " Offset=" << found_offset << std::endl;
        }

        if (last_seq != -1) {
            int expected = (last_seq + 1) % 65536;
            if (seq != expected) {
                std::cout << "⚠️  Gap detected: last=" << last_seq << " curr=" << seq 
                          << " (type=" << (int)type << " offset=" << found_offset << ")" << std::endl;
                gaps++;
            }
        }

        last_seq = seq;
        total_msg++;
        
        if (type == 31 || type == 29) {
            offset = found_offset + sz_hw * 2;
        } else {
            // slots of 2432 bytes, header starts at byte 12
            offset = (found_offset - 12) + 2432;
        }
        
        while (offset < dsize && data[offset] == 0) offset++;
    }

    std::cout << "\nSummary: " << total_msg << " messages, " << gaps << " gaps." << std::endl;
    return 0;
}
