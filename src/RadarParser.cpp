#include "levelii/RadarParser.h"
#include "levelii/RadarFrame.h"
#include "levelii/DecompressionUtils.h"
#include "levelii/VolumetricGenerator.h"
#include "levelii/NEXRAD_Types.h"
#include "levelii/ByteReader.h"
#include "levelii/MessageSegmenter.h"
#include <iostream>
#include <vector>
#include <string>
#include <cstring>
#include <memory>
#include <unordered_map>
#include <algorithm>
#include <cmath>
#include <chrono>

namespace {

constexpr bool VERBOSE_LOGGING = false;
constexpr bool QUANTIZE_VALUES_DEFAULT = true;

using nexrad::read_be;
using nexrad::read_be_float;
using nexrad::read_le;

float quantize_value_internal(float value, bool quantize) {
    if (!quantize) return value;
    return std::round(value * 10.0f) * 0.1f;
}

uint8_t get_moment_type(const std::string& product_type) {
    if (product_type == "reflectivity") return 1;
    if (product_type == "velocity") return 2;
    if (product_type == "spectrum_width") return 3;
    if (product_type == "differential_reflectivity") return 4;
    if (product_type == "differential_phase") return 5;
    if (product_type == "cross_correlation_ratio" || product_type == "correlation_coefficient") return 6;
    return 1;
}

float group_elevation(float elevation) {
    // Round to nearest 0.1 degree for grouping radials into sweeps
    return std::round(elevation * 10.0f) / 10.0f;
}

std::string format_timestamp(uint32_t julian_day, uint32_t ms) {
    using namespace std::chrono;
    
    // Use system_clock epoch (1970-01-01 00:00:00 UTC)
    system_clock::time_point epoch; 
    
    // Add days and milliseconds as per ICD requirement
    // NEXRAD Julian Date is 1-based (1970-01-01 is day 1)
    auto tp = epoch + 
              hours(24 * (static_cast<long long>(julian_day) - 1)) + 
              milliseconds(static_cast<long long>(ms));
    
    time_t seconds = system_clock::to_time_t(tp);
    struct tm tm_info;
    gmtime_r(&seconds, &tm_info);
    
    char buffer[32];
    std::strftime(buffer, sizeof(buffer), "%Y%m%d_%H%M%S", &tm_info);
    return std::string(buffer);
}

} // anonymous namespace

class NEXRADParser {
public:
    static std::unordered_map<std::string, std::unique_ptr<RadarFrame>> parse(
        const std::vector<uint8_t>& data,
        const std::string& station_hint,
        const std::string& timestamp_hint,
        const std::vector<std::string>& product_types,
        std::vector<uint8_t>* decompressed_out = nullptr
    ) {
        auto parse_start = std::chrono::high_resolution_clock::now();
        std::unordered_map<std::string, std::unique_ptr<RadarFrame>> frames;
        std::unordered_map<std::string, uint8_t> product_to_moment;
        
        for (const auto& pt : product_types) {
            auto frame = std::make_unique<RadarFrame>();
            frame->station = station_hint;
            frame->timestamp = timestamp_hint;
            frame->product_type = pt;
            product_to_moment[pt] = get_moment_type(pt);
            frames[pt] = std::move(frame);
        }
        
        if (data.size() < sizeof(nexrad::VolumeHeader)) {
            if (VERBOSE_LOGGING) std::cerr << "❌ File too small for Volume Header" << std::endl;
            return frames;
        }
        
        const nexrad::VolumeHeader* vol_header = reinterpret_cast<const nexrad::VolumeHeader*>(data.data());
        
        char id[5] = {0};
        std::memcpy(id, vol_header->radar_id, 4);
        std::string actual_station = std::string(id);
        
        uint32_t julian_date = read_be<uint32_t>(reinterpret_cast<const uint8_t*>(&vol_header->julian_date));
        uint32_t ms = read_be<uint32_t>(reinterpret_cast<const uint8_t*>(&vol_header->milliseconds));
        std::string actual_timestamp = format_timestamp(julian_date, ms);
        
        for (auto& pair : frames) {
            pair.second->station = actual_station;
            pair.second->timestamp = actual_timestamp;
        }
        
        const uint8_t* parse_data = data.data();
        size_t parse_size = data.size();
        
        std::vector<uint8_t> local_decompressed;
        std::vector<uint8_t>& decompressed_data = decompressed_out ? *decompressed_out : local_decompressed;
        
        if (!RadarDecompression::auto_decompress(data, decompressed_data)) {
            return frames;
        }
        
        if (!decompressed_data.empty()) {
            parse_data = decompressed_data.data();
            parse_size = decompressed_data.size();
        }
        
        if (parse_size < sizeof(nexrad::VolumeHeader)) {
            return frames;
        }
        
        size_t offset = 0;
        int message_count = 0;
        int radial_count = 0;
        float min_elevation = 999.0f;
        std::unordered_map<int, int> elevation_ray_counts;
        
        int current_sweep_idx = -1;
        uint8_t current_elev_num = 0xFF;
        float current_sweep_elevation = -99.0f;
        
        nexrad::MessageSegmenter segmenter;
        bool is_archive2 = false;
        if (parse_size >= 24 && (std::memcmp(parse_data, "ARCHIVE2", 8) == 0 || 
                                 std::memcmp(parse_data, "AR2V", 4) == 0)) {
            offset = 24;
            is_archive2 = true;
        }

        if (is_archive2 && offset + (134 * 2432) <= parse_size) {
            for (int i = 0; i < 134; ++i) {
                size_t seg_offset = offset + (i * 2432);
                const nexrad::MessageHeader* msg_header = 
                    reinterpret_cast<const nexrad::MessageHeader*>(parse_data + seg_offset + 12);
                if (msg_header->type == 0) continue;
                const uint8_t* payload_start = parse_data + seg_offset + 12 + sizeof(nexrad::MessageHeader);
                size_t payload_size = 2432 - 12 - sizeof(nexrad::MessageHeader);
                nexrad::MessageSegmenter::SegmentedMessage complete_msg;
                segmenter.add_segment(*msg_header, payload_start, payload_size, complete_msg);
            }
            offset += (134 * 2432);
        }

        while (offset + sizeof(nexrad::MessageHeader) <= parse_size && message_count < 200000) {
            if (is_archive2) {
                while (offset < parse_size && parse_data[offset] == 0) offset++;
            }
            if (offset + sizeof(nexrad::MessageHeader) > parse_size) break;

            size_t msg_header_offset = offset;
            bool found_header = false;
            for (size_t skip : { 0UL, 12UL }) {
                if (offset + skip + sizeof(nexrad::MessageHeader) > parse_size) continue;
                const nexrad::MessageHeader* test_hdr = reinterpret_cast<const nexrad::MessageHeader*>(parse_data + offset + skip);
                uint8_t type = test_hdr->type;
                uint16_t size_hw = read_be<uint16_t>(reinterpret_cast<const uint8_t*>(&test_hdr->size));
                uint16_t julian = read_be<uint16_t>(reinterpret_cast<const uint8_t*>(&test_hdr->julian_date));
                if (type > 0 && type <= 32 && size_hw >= 8 && size_hw < 32768 && julian > 10000) {
                    msg_header_offset = offset + skip;
                    found_header = true;
                    break;
                }
            }

            if (!found_header && is_archive2) {
                for (size_t skip = 1; skip <= 4096; ++skip) {
                    if (offset + skip + sizeof(nexrad::MessageHeader) > parse_size) break;
                    const nexrad::MessageHeader* test_hdr = reinterpret_cast<const nexrad::MessageHeader*>(parse_data + offset + skip);
                    if (test_hdr->type > 0 && test_hdr->type <= 32) {
                         uint16_t size_hw = read_be<uint16_t>(reinterpret_cast<const uint8_t*>(&test_hdr->size));
                         uint16_t julian = read_be<uint16_t>(reinterpret_cast<const uint8_t*>(&test_hdr->julian_date));
                         if (size_hw >= 8 && size_hw < 32768 && julian > 10000) {
                             msg_header_offset = offset + skip;
                             found_header = true;
                             break;
                         }
                    }
                }
            }

            if (!found_header) {
                offset++;
                continue;
            }

            const nexrad::MessageHeader* msg_header = reinterpret_cast<const nexrad::MessageHeader*>(parse_data + msg_header_offset);
            uint8_t type = msg_header->type;
            uint16_t msg_size_halfwords = read_be<uint16_t>(reinterpret_cast<const uint8_t*>(&msg_header->size));
            size_t message_size_bytes = static_cast<size_t>(msg_size_halfwords) * 2;
            
            if (message_size_bytes < sizeof(nexrad::MessageHeader) || msg_header_offset + message_size_bytes > parse_size) {
                offset = msg_header_offset + 1;
                continue;
            }

            const uint8_t* msg_data_start_seg = parse_data + msg_header_offset + sizeof(nexrad::MessageHeader);
            size_t msg_data_size_seg = message_size_bytes - sizeof(nexrad::MessageHeader);

            nexrad::MessageSegmenter::SegmentedMessage complete_msg;
            if (!segmenter.add_segment(*msg_header, msg_data_start_seg, msg_data_size_seg, complete_msg)) {
                message_count++;
                offset = msg_header_offset + message_size_bytes;
                if (is_archive2 && message_size_bytes < 2420 && type != 31 && type != 29) {
                    offset = msg_header_offset + (2432 - 12);
                }
                continue;
            }

            const uint8_t* payload_ptr = complete_msg.data.data();
            size_t payload_size = complete_msg.data.size();
            uint8_t effective_type = complete_msg.type;
            
            offset = msg_header_offset + message_size_bytes;
            if (is_archive2 && message_size_bytes < 2420 && type != 31 && type != 29) {
                offset = msg_header_offset + (2432 - 12);
            }
            
            if (effective_type == 1) { // Legacy Digital Radar Data (Reflectivity)
                if (payload_size < 32) { message_count++; continue; }
                float azimuth = static_cast<float>(read_be<uint16_t>(payload_ptr + 8)) * (360.0f / 65536.0f);
                float elevation = static_cast<float>(read_be<uint16_t>(payload_ptr + 16)) * (360.0f / 65536.0f);
                if (azimuth < -0.1f || azimuth > 360.1f || elevation < -5.0f || elevation > 90.0f) { message_count++; continue; }
                
                if (elevation < min_elevation) min_elevation = elevation;
                uint8_t radial_status = payload_ptr[1];
                bool is_new_sweep = (radial_status == nexrad::STATUS_START_ELEVATION || 
                                     radial_status == nexrad::STATUS_START_VOLUME ||
                                     radial_status == nexrad::STATUS_START_ELEVATION_SEGMENTED ||
                                     current_sweep_idx == -1);

                if (is_new_sweep) {
                    current_sweep_idx++;
                    current_sweep_elevation = elevation;
                    for (auto& pair : frames) {
                        RadarFrame::Sweep sweep;
                        sweep.index = current_sweep_idx;
                        sweep.elevation_deg = elevation;
                        sweep.bins.reserve(60000 * 3);
                        pair.second->sweeps.push_back(sweep);
                    }
                }

                if (current_sweep_idx >= 0) {
                    int active_key = RadarFrame::get_tilt_key(current_sweep_elevation);
                    elevation_ray_counts[active_key]++;
                    
                    for (auto& pair : frames) {
                        auto& frame = *pair.second;
                        frame.sweeps[current_sweep_idx].ray_count++;
                        
                        if (product_to_moment[pair.first] == 1 && payload_size >= 46) {
                            uint16_t unam_rng_raw = read_be<uint16_t>(payload_ptr + 26);
                            if (unam_rng_raw > 0) {
                                frame.unambiguous_range_meters = static_cast<float>(unam_rng_raw) * 100.0f;
                                frame.max_range_meters = std::max(frame.max_range_meters, frame.unambiguous_range_meters);
                            }
                            uint16_t nyquist_raw = read_be<uint16_t>(payload_ptr + 28);
                            if (nyquist_raw > 0) {
                                float nyquist = static_cast<float>(nyquist_raw) * 0.1f;
                                frame.nyquist_velocity[active_key] = nyquist;
                                frame.sweeps[current_sweep_idx].nyquist_velocity = nyquist;
                            }

                            uint16_t num_gates = read_be<uint16_t>(payload_ptr + 24);
                            float first_gate_m = static_cast<float>(read_be<uint16_t>(payload_ptr + 20));
                            float gate_size_m = static_cast<float>(read_be<uint16_t>(payload_ptr + 22));
                            
                            if (num_gates > 0 && payload_size >= static_cast<size_t>(46 + num_gates)) {
                                const uint8_t* gate_data = payload_ptr + 46;
                                if (frame.ngates == 0 && num_gates > 10) {
                                    frame.ngates = num_gates;
                                    frame.gate_spacing_meters = gate_size_m;
                                    frame.range_spacing_meters = gate_size_m;
                                    frame.first_gate_meters = first_gate_m;
                                }
                                for (uint16_t g = 0; g < num_gates; g += DOWNSAMPLE_GATES) {
                                    uint8_t raw_value = gate_data[g];
                                    if (raw_value <= 1) continue;
                                    float value = (static_cast<float>(raw_value) - 66.0f) * 0.5f;
                                    if (value < -32.0f) continue;
                                    value = quantize_value_internal(value, QUANTIZE_VALUES_DEFAULT);
                                    float range_m = first_gate_m + static_cast<float>(g) * gate_size_m;
                                    frame.sweeps[current_sweep_idx].bins.push_back(azimuth);
                                    frame.sweeps[current_sweep_idx].bins.push_back(range_m);
                                    frame.sweeps[current_sweep_idx].bins.push_back(value);
                                }
                            }
                        }
                    }
                }
                radial_count++;
            } else if (effective_type == 31) { // Generic Digital Radar Data
                if (payload_size < sizeof(nexrad::Message31Header)) { message_count++; continue; }
                auto m31_opt = nexrad::safe_read_struct<nexrad::Message31Header>(payload_ptr, payload_size, 0, "Message31Header");
                if (!m31_opt) { message_count++; continue; }
                const nexrad::Message31Header* m31 = *m31_opt;
                
                uint16_t block_count = read_be<uint16_t>(reinterpret_cast<const uint8_t*>(&m31->block_count));
                if (block_count > 100) { message_count++; continue; }

                float azimuth = read_be_float(reinterpret_cast<const uint8_t*>(&m31->azimuth_angle));
                float elevation = read_be_float(reinterpret_cast<const uint8_t*>(&m31->elev_angle));
                if (azimuth < -0.1f || azimuth > 360.1f || elevation < -5.0f || elevation > 90.0f) { message_count++; continue; }

                uint8_t radial_status = m31->radial_status;
                uint8_t elev_num = m31->elev_number;

                bool is_new_sweep = (radial_status == nexrad::STATUS_START_ELEVATION || 
                                     radial_status == nexrad::STATUS_START_VOLUME ||
                                     radial_status == nexrad::STATUS_START_ELEVATION_SEGMENTED ||
                                     (elev_num != current_elev_num && current_sweep_idx >= 0) ||
                                     current_sweep_idx == -1);

                if (is_new_sweep) {
                    current_sweep_idx++;
                    current_elev_num = elev_num;
                    current_sweep_elevation = elevation;
                    if (radial_status == nexrad::STATUS_START_VOLUME) segmenter.clear();
                    for (auto& pair : frames) {
                        RadarFrame::Sweep sweep;
                        sweep.index = current_sweep_idx;
                        sweep.elevation_num = elev_num;
                        sweep.elevation_deg = elevation;
                        sweep.bins.reserve(60000 * 3);
                        pair.second->sweeps.push_back(sweep);
                    }
                }

                if (current_sweep_idx >= 0) {
                    if (elevation < min_elevation) min_elevation = elevation;
                    int active_key = RadarFrame::get_tilt_key(current_sweep_elevation);
                    elevation_ray_counts[active_key]++;
                    
                    for (auto& pair : frames) pair.second->sweeps[current_sweep_idx].ray_count++;

                    for (uint16_t b = 0; b < block_count; ++b) {
                        uint32_t b_off = read_be<uint32_t>(reinterpret_cast<const uint8_t*>(&m31->block_pointers[b]));
                        if (!nexrad::safe_pointer_dereference(b_off, sizeof(nexrad::DataBlock_Header), payload_size, "DBH")) continue;
                        const nexrad::DataBlock_Header* block_hdr = reinterpret_cast<const nexrad::DataBlock_Header*>(payload_ptr + b_off);
                        
                        if (strncmp(block_hdr->name, "VOL", 3) == 0) {
                            auto vol_opt = nexrad::safe_read_struct<nexrad::DataBlock_Volume>(payload_ptr, payload_size, b_off, "DBV");
                            if (vol_opt) {
                                uint16_t vcp = read_be<uint16_t>(reinterpret_cast<const uint8_t*>(&(*vol_opt)->vcp_number));
                                for (auto& pair : frames) pair.second->vcp_number = vcp;
                            }
                        } else if (strncmp(block_hdr->name, "RAD", 3) == 0) {
                            auto rad_opt = nexrad::safe_read_struct<nexrad::DataBlock_Radial>(payload_ptr, payload_size, b_off, "DBR");
                            if (rad_opt) {
                                float nyq = static_cast<float>(read_be<uint16_t>(reinterpret_cast<const uint8_t*>(&(*rad_opt)->nyquist_velocity))) * 0.01f;
                                uint16_t ur = read_be<uint16_t>(reinterpret_cast<const uint8_t*>(&(*rad_opt)->unambiguous_range));
                                for (auto& pair : frames) {
                                    auto& f = *pair.second;
                                    if (nyq > 0) { f.nyquist_velocity[active_key] = nyq; f.sweeps[current_sweep_idx].nyquist_velocity = nyq; }
                                    if (ur > 0) { f.unambiguous_range_meters = static_cast<float>(ur) * 100.0f; f.max_range_meters = std::max(f.max_range_meters, f.unambiguous_range_meters); }
                                }
                            }
                        } else if (block_hdr->type == 'D') {
                            auto moment_opt = nexrad::safe_read_struct<nexrad::DataBlock_Moment>(payload_ptr, payload_size, b_off, "DBM");
                            if (!moment_opt) continue;
                            const nexrad::DataBlock_Moment* moment = *moment_opt;
                            char dname[4] = {0}; std::memcpy(dname, moment->name, 3);
                            
                            uint16_t ng = read_be<uint16_t>(reinterpret_cast<const uint8_t*>(&moment->num_gates));
                            float fg = static_cast<float>(read_be<uint16_t>(reinterpret_cast<const uint8_t*>(&moment->first_gate)));
                            float gs = static_cast<float>(read_be<uint16_t>(reinterpret_cast<const uint8_t*>(&moment->gate_spacing)));
                            float sc = read_be_float(reinterpret_cast<const uint8_t*>(&moment->scale));
                            float ov = read_be_float(reinterpret_cast<const uint8_t*>(&moment->offset));
                            uint8_t ws = moment->data_word_size == 0 ? 8 : moment->data_word_size;
                            if (ng == 0 || ng > 8000 || gs == 0 || (ws != 8 && ws != 16)) continue;
                            
                            size_t dsize = static_cast<size_t>(ng) * (ws / 8);
                            if (b_off + sizeof(nexrad::DataBlock_Moment) + dsize > payload_size) continue;
                            const uint8_t* gdata = payload_ptr + b_off + sizeof(nexrad::DataBlock_Moment);

                            for (auto& pair : frames) {
                                uint8_t tm = product_to_moment[pair.first];
                                bool is_target = false;
                                if (tm == 1 && strncmp(dname, "REF", 3) == 0) is_target = true;
                                else if (tm == 2 && strncmp(dname, "VEL", 3) == 0) is_target = true;
                                else if (tm == 3 && strncmp(dname, "SW", 2) == 0) is_target = true;
                                else if (tm == 4 && strncmp(dname, "ZDR", 3) == 0) is_target = true;
                                else if (tm == 5 && strncmp(dname, "PHI", 3) == 0) is_target = true;
                                else if (tm == 6 && strncmp(dname, "RHO", 3) == 0) is_target = true;
                                
                                if (!is_target) continue;
                                auto& f = *pair.second;
                                if (f.ngates == 0 && ng > 10) { f.ngates = ng; f.gate_spacing_meters = gs; f.range_spacing_meters = gs; f.first_gate_meters = fg; }
                                
                                for (uint16_t g = 0; g < ng; g += DOWNSAMPLE_GATES) {
                                    uint16_t raw = (ws == 16) ? read_be<uint16_t>(gdata + g * 2) : gdata[g];
                                    if (raw <= 1) continue;
                                    float val = (static_cast<float>(raw) - ov) / sc;
                                    if (tm == 1 && val < -32.0f) continue;
                                    val = quantize_value_internal(val, QUANTIZE_VALUES_DEFAULT);
                                    float range_m = fg + static_cast<float>(g) * gs;
                                    f.sweeps[current_sweep_idx].bins.push_back(azimuth);
                                    f.sweeps[current_sweep_idx].bins.push_back(range_m);
                                    f.sweeps[current_sweep_idx].bins.push_back(val);
                                }
                            }
                        }
                    }
                }
                radial_count++;
            }
            message_count++;
        }
        
        for (auto& pair : frames) {
            auto& frame = *pair.second;
            for (const auto& sweep : frame.sweeps) frame.available_tilts.push_back(sweep.elevation_deg);
            std::sort(frame.available_tilts.begin(), frame.available_tilts.end());
            frame.available_tilts.erase(std::unique(frame.available_tilts.begin(), frame.available_tilts.end()), frame.available_tilts.end());
            
            if (frame.max_range_meters <= 0) frame.max_range_meters = 230000.0f;
            if (frame.unambiguous_range_meters <= 0) frame.unambiguous_range_meters = 230000.0f;
            frame.nsweeps = frame.sweeps.size();
            frame.nrays = radial_count;
            frame.sweep_ray_counts = elevation_ray_counts;
            if (!frame.sweeps.empty()) frame.elevation_deg = frame.sweeps[0].elevation_deg;
            else frame.elevation_deg = min_elevation;
            
            for (auto& sweep : frame.sweeps) sweep.bins.shrink_to_fit();
            if (!frame.sweeps.empty()) {
                try { VolumetricGenerator::generate_volumetric_3d(frame); } catch (...) {}
            }
        }
        return frames;
    }
};

std::unique_ptr<RadarFrame> parse_nexrad_level2(
    const std::vector<uint8_t>& data,
    const std::string& station,
    const std::string& timestamp,
    const std::string& product_type)
{
    auto results = NEXRADParser::parse(data, station, timestamp, {product_type});
    if (results.count(product_type)) {
        return std::move(results[product_type]);
    }
    return nullptr;
}

std::unordered_map<std::string, std::unique_ptr<RadarFrame>> parse_nexrad_level2_multi(
    const std::vector<uint8_t>& data,
    const std::string& station,
    const std::string& timestamp,
    const std::vector<std::string>& product_types,
    std::vector<uint8_t>* decompressed_buffer)
{
    return NEXRADParser::parse(data, station, timestamp, product_types, decompressed_buffer);
}
