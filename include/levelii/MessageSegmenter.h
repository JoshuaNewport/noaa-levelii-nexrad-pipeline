#pragma once

#include "levelii/NEXRAD_Types.h"
#include "levelii/ByteReader.h"
#include <vector>
#include <unordered_map>
#include <cstdint>

namespace nexrad {

class MessageSegmenter {
public:
    struct SegmentedMessage {
        uint16_t sequence_num;
        uint8_t type;
        std::vector<uint8_t> data;
    };

    /**
     * Adds a message segment.
     * @param header The message header of the segment.
     * @param segment_data Pointer to the data portion of the segment (excluding header).
     * @param segment_size Size of the data portion of the segment.
     * @param out_msg If this segment completes a message, it is populated here.
     * @return true if a full message is completed, false otherwise.
     */
    bool add_segment(const MessageHeader& header, const uint8_t* segment_data, size_t segment_size, SegmentedMessage& out_msg) {
        uint16_t num_segments = read_be<uint16_t>(reinterpret_cast<const uint8_t*>(&header.num_segments));
        uint16_t segment_num = read_be<uint16_t>(reinterpret_cast<const uint8_t*>(&header.segment_num));
        uint16_t seq_num = read_be<uint16_t>(reinterpret_cast<const uint8_t*>(&header.sequence_num));

        // Trivial case: single segment message
        if (num_segments <= 1) {
            out_msg.sequence_num = seq_num;
            out_msg.type = header.type;
            out_msg.data.assign(segment_data, segment_data + segment_size);
            return true;
        }

        // Segmented message
        auto& info = m_pending_messages[seq_num];
        
        // Safety: limit number of segments to prevent excessive memory allocation
        if (num_segments > 2000) {
            return false;
        }

        // If this is the first segment we see for this sequence number, initialize
        if (info.segments.empty()) {
            info.segments.resize(num_segments);
            info.type = header.type;
            info.segments_received = 0;
            info.total_data_size = 0;
        }

        // Validate segment number
        if (segment_num < 1 || segment_num > num_segments) {
            return false;
        }

        // If we haven't received this segment yet, store it
        if (info.segments[segment_num - 1].empty()) {
            info.segments[segment_num - 1].assign(segment_data, segment_data + segment_size);
            info.segments_received++;
            info.total_data_size += segment_size;
        }

        // Check if all segments are received
        if (info.segments_received == num_segments) {
            out_msg.sequence_num = seq_num;
            out_msg.type = info.type;
            out_msg.data.reserve(info.total_data_size);
            
            for (const auto& seg : info.segments) {
                out_msg.data.insert(out_msg.data.end(), seg.begin(), seg.end());
            }
            
            m_pending_messages.erase(seq_num);
            return true;
        }

        return false;
    }

    void clear() {
        m_pending_messages.clear();
    }

private:
    struct MessageInfo {
        uint8_t type;
        std::vector<std::vector<uint8_t>> segments;
        uint16_t segments_received;
        size_t total_data_size;
    };
    
    // Map of sequence number to message info
    std::unordered_map<uint16_t, MessageInfo> m_pending_messages;
};

} // namespace nexrad
