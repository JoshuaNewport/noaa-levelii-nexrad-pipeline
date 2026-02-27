#include <iostream>
#include <vector>
#include <cmath>
#include <cassert>
#include <filesystem>
#include "levelii/FrameStorageManager.h"

namespace fs = std::filesystem;

void test_bitmask_encoding() {
    std::cout << "\n=== BITMASK ENCODING TEST ===\n";
    
    std::vector<uint8_t> grid(16, 0);
    grid[0] = 42;
    grid[7] = 84;
    grid[8] = 99;
    
    std::vector<uint8_t> bitmask((grid.size() + 7) / 8, 0);
    std::vector<uint8_t> values;
    
    for (size_t b = 0; b < grid.size(); ++b) {
        if (grid[b] > 0) {
            bitmask[b / 8] |= (1 << (7 - (b % 8)));
            values.push_back(grid[b]);
        }
    }
    
    bool bitmask_correct = true;
    if (bitmask[0] != 0x81) {
        std::cout << "❌ Bitmask byte 0 incorrect: " << std::hex << (int)bitmask[0] 
                  << " (expected 0x81)\n" << std::dec;
        bitmask_correct = false;
    }
    if (bitmask[1] != 0x80) {
        std::cout << "❌ Bitmask byte 1 incorrect: " << std::hex << (int)bitmask[1] 
                  << " (expected 0x80)\n" << std::dec;
        bitmask_correct = false;
    }
    
    bool values_correct = (values.size() == 3) && (values[0] == 42) && (values[1] == 84) && (values[2] == 99);
    if (!values_correct) {
        std::cout << "❌ Values array incorrect (size=" << values.size() << ")\n";
    }
    
    if (bitmask_correct && values_correct) {
        std::cout << "✅ PASSED: Bitmask encoding is correct\n";
    }
}

void test_volumetric_frame_storage() {
    std::cout << "\n=== VOLUMETRIC FRAME STORAGE TEST ===\n";
    
    FrameStorageManager manager("./test_data");
    
    std::string station = "KTLX";
    std::string product = "reflectivity";
    std::string timestamp = "20260215_150000";
    std::vector<float> tilts = {0.5f, 0.9f, 1.3f, 1.8f, 2.4f};
    uint16_t num_rays = 720;
    uint16_t num_gates = 1200;
    float gate_spacing = 250.0f;
    float first_gate = 2125.0f;
    
    std::vector<uint8_t> bitmask(tilts.size() * num_rays * num_gates / 8, 0xAA);
    std::vector<uint8_t> values;
    for (size_t i = 0; i < 1000; ++i) values.push_back(static_cast<uint8_t>(i % 256));
    
    std::cout << "Testing save_volumetric_bitmask... ";
    bool saved = manager.save_volumetric_bitmask(
        station, product, timestamp, tilts,
        num_rays, num_gates, gate_spacing, first_gate,
        bitmask, values
    );
    
    if (!saved) {
        std::cout << "❌ FAILED\n";
        return;
    }
    std::cout << "✅ PASSED\n";
    
    std::cout << "Testing load_volumetric_bitmask... ";
    FrameStorageManager::CompressedFrameData loaded_data;
    bool loaded = manager.load_volumetric_bitmask(station, product, timestamp, loaded_data);
    
    if (!loaded) {
        std::cout << "❌ FAILED\n";
        return;
    }
    std::cout << "✅ PASSED\n";
    
    std::cout << "Verifying metadata... ";
    try {
        assert(loaded_data.metadata["s"] == station);
        assert(loaded_data.metadata["p"] == product);
        assert(loaded_data.metadata["t"] == timestamp);
        assert(loaded_data.metadata["f"] == "b");
        assert(loaded_data.metadata["tilts"].size() == tilts.size());
        std::cout << "✅ PASSED\n";
    } catch (const std::exception& e) {
        std::cout << "❌ FAILED: " << e.what() << "\n";
    }
}

void test_ray_wrapping() {
    std::cout << "\n=== RAY WRAPPING TEST ===\n";
    
    struct RayTest {
        float azimuth;
        float resolution_factor;
        int expected_ray_360;
        int expected_ray_720;
    };
    
    RayTest cases[] = {
        {0.0f, 1.0f, 0, 0},
        {0.5f, 1.0f, 0, 1},
        {1.0f, 1.0f, 1, 2},
        {179.9f, 1.0f, 179, 359},
        {180.0f, 1.0f, 180, 360},
        {359.5f, 1.0f, 359, 719},
        {359.9f, 1.0f, 359, 719},
    };
    
    bool all_passed = true;
    for (const auto& tc : cases) {
        int ray_360 = static_cast<int>(std::floor(tc.azimuth * 1.0f + 0.01f)) % 360;
        if (ray_360 < 0) ray_360 += 360;
        
        int ray_720 = static_cast<int>(std::floor(tc.azimuth * 2.0f + 0.01f)) % 720;
        if (ray_720 < 0) ray_720 += 720;
        
        bool passed = (ray_360 == tc.expected_ray_360) && (ray_720 == tc.expected_ray_720);
        
        std::cout << (passed ? "✅" : "❌") << " azimuth=" << tc.azimuth << "\n";
        all_passed = all_passed && passed;
    }
    
    if (all_passed) {
        std::cout << "✅ PASSED: Ray wrapping is correct\n";
    }
}

int main() {
    std::cout << "=" << std::string(60, '=') << "\n";
    std::cout << "FRAME STORAGE TEST SUITE FOR LEVELII PROCESSOR\n";
    std::cout << "=" << std::string(60, '=') << "\n";
    
    test_bitmask_encoding();
    test_volumetric_frame_storage();
    test_ray_wrapping();
    
    std::cout << "\n" << "=" << std::string(60, '=') << "\n";
    std::cout << "Frame storage tests completed.\n";
    std::cout << "=" << std::string(60, '=') << "\n";
    
    return 0;
}
