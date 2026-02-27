#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <algorithm>
#include <cmath>
#include "levelii/RadarParser.h"
#include "levelii/RadarFrame.h"
#include "levelii/FrameStorageManager.h"

int main(int argc, char** argv) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <level2_file>" << std::endl;
        return 1;
    }

    std::string filename = argv[1];
    std::ifstream file(filename, std::ios::binary);
    if (!file) {
        std::cerr << "Could not open " << filename << std::endl;
        return 1;
    }

    file.seekg(0, std::ios::end);
    size_t size = file.tellg();
    file.seekg(0, std::ios::beg);
    std::vector<uint8_t> buffer(size);
    file.read((char*)buffer.data(), size);

    auto frame = parse_nexrad_level2(buffer, "TEST", "20260215_000000", "reflectivity");
    if (!frame) {
        std::cerr << "Failed to parse frame" << std::endl;
        return 1;
    }

    std::vector<float> sorted_tilts = frame->available_tilts;
    std::sort(sorted_tilts.begin(), sorted_tilts.end());

    const uint16_t num_rays = 720;
    const uint16_t num_gates = frame->ngates;
    const uint16_t num_tilts = static_cast<uint16_t>(sorted_tilts.size());

    std::vector<uint8_t> vol_grid(num_tilts * num_rays * num_gates, 0);

    for (size_t t_idx = 0; t_idx < sorted_tilts.size(); ++t_idx) {
        float tilt = sorted_tilts[t_idx];
        for (const auto& sweep : frame->sweeps) {
            if (std::abs(sweep.elevation_deg - tilt) < 0.05f) {
                for (size_t i = 0; i < sweep.bins.size(); i += 3) {
                    float az = sweep.bins[i];
                    float rng = sweep.bins[i+1];
                    float val = sweep.bins[i+2];

                    if (val <= -32.0f) continue;

                    uint8_t quantized = static_cast<uint8_t>(std::clamp((val + 32.0f) / (95.0f + 32.0f) * 255.0f, 0.0f, 255.0f));
                    if (quantized == 0) continue;

                    int r_idx = static_cast<int>(std::floor(az * 2.0f)) % num_rays;
                    int g_idx = static_cast<int>(std::floor((rng - frame->first_gate_meters) / frame->gate_spacing_meters));

                    if (g_idx >= 0 && g_idx < num_gates) {
                        size_t idx = (t_idx * num_rays * num_gates) + (r_idx * num_gates) + g_idx;
                        vol_grid[idx] = std::max(vol_grid[idx], quantized);
                    }
                }
            }
        }
    }

    std::vector<uint8_t> bitmask((vol_grid.size() + 7) / 8, 0);
    std::vector<uint8_t> values;
    for (size_t i = 0; i < vol_grid.size(); ++i) {
        if (vol_grid[i] > 0) {
            bitmask[i / 8] |= (1 << (7 - (i % 8)));
            values.push_back(vol_grid[i]);
        }
    }

    FrameStorageManager storage("./data");
    
    // 1. Save individual tilts
    for (float tilt : sorted_tilts) {
        std::vector<uint8_t> grid_2d(720 * num_gates, 0); // Always use 720 for consistency in this test
        for (const auto& sweep : frame->sweeps) {
            if (std::abs(sweep.elevation_deg - tilt) < 0.05f) {
                for (size_t i = 0; i < sweep.bins.size(); i += 3) {
                    float az = sweep.bins[i], rng = sweep.bins[i+1], val = sweep.bins[i+2];
                    if (val <= -32.0f) continue;
                    uint8_t quantized = static_cast<uint8_t>(std::clamp((val + 32.0f) / (95.0f + 32.0f) * 255.0f, 0.0f, 255.0f));
                    if (quantized == 0) continue;
                    int r_idx = static_cast<int>(std::floor(az * 2.0f)) % 720;
                    int g_idx = static_cast<int>(std::floor((rng - frame->first_gate_meters) / frame->gate_spacing_meters));
                    if (g_idx >= 0 && g_idx < num_gates) grid_2d[r_idx * num_gates + g_idx] = std::max(grid_2d[r_idx * num_gates + g_idx], quantized);
                }
            }
        }
        std::vector<uint8_t> bitmask_2d((grid_2d.size() + 7) / 8, 0);
        std::vector<uint8_t> values_2d;
        for (size_t i = 0; i < grid_2d.size(); ++i) {
            if (grid_2d[i] > 0) {
                bitmask_2d[i / 8] |= (1 << (7 - (i % 8)));
                values_2d.push_back(grid_2d[i]);
            }
        }
        storage.save_frame_bitmask("TEST", "reflectivity", "20260215_000000", tilt, 720, num_gates, frame->gate_spacing_meters, frame->first_gate_meters, bitmask_2d, values_2d);
    }

    // 2. Save volumetric 3D
    storage.save_volumetric_bitmask("TEST", "reflectivity", "20260215_000000", sorted_tilts, num_rays, num_gates, frame->gate_spacing_meters, frame->first_gate_meters, bitmask, values);

    std::cout << "Saved individual tilts and volumetric data (" << values.size() << " 3D points) to ./data" << std::endl;
    return 0;
}
