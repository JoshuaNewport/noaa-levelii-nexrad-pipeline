#include <iostream>
#include <vector>
#include <cmath>
#include <cassert>
#include <iomanip>
#include "levelii/RadarFrame.h"

static float dequantize_value(uint8_t quant, float min_val, float max_val) {
    float range = max_val - min_val;
    return min_val + (static_cast<float>(quant) / 255.0f) * range;
}

void test_quantization_roundtrip() {
    std::cout << "\n=== QUANTIZATION ROUNDTRIP TEST ===\n";
    
    float ref_min = -32.0f;
    float ref_max = 94.5f;
    float test_values[] = {-32.0f, -31.5f, 0.0f, 32.0f, 64.0f, 94.5f, 50.123f, -15.678f};
    
    bool all_passed = true;
    for (float orig : test_values) {
        uint8_t quant = quantize_value(orig, ref_min, ref_max);
        float dequant = dequantize_value(quant, ref_min, ref_max);
        float error = std::abs(dequant - orig);
        
        float expected_max_error = (ref_max - ref_min) / 255.0f;
        if (error > expected_max_error) {
            std::cout << "❌ FAILED: " << orig << " -> " << (int)quant << " -> " << dequant 
                      << " (error: " << error << ", max: " << expected_max_error << ")\n";
            all_passed = false;
        }
    }
    
    if (all_passed) {
        std::cout << "✅ PASSED: Quantization roundtrip is lossless (error < 1 level)\n";
    }
}

int main() {
    std::cout << "=" << std::string(60, '=') << "\n";
    std::cout << "QUANTIZATION TEST SUITE FOR LEVELII PROCESSOR\n";
    std::cout << "=" << std::string(60, '=') << "\n";
    
    test_quantization_roundtrip();
    
    std::cout << "\n" << "=" << std::string(60, '=') << "\n";
    std::cout << "Quantization test completed.\n";
    std::cout << "=" << std::string(60, '=') << "\n";
    
    return 0;
}
