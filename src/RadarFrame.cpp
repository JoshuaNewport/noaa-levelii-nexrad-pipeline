#include "levelii/RadarFrame.h"
#include <algorithm>
#include <cstring>
#include <cmath>

QuantizationParams get_quant_params(const std::string& product_type) {
    if (product_type == "velocity") {
        return {-100.0f, 100.0f, 0.0f, 230000.0f};
    } else if (product_type == "spectrum_width") {
        return {0.0f, 64.0f, 0.0f, 230000.0f};
    } else if (product_type == "differential_reflectivity") {
        return {-8.0f, 8.0f, 0.0f, 230000.0f};
    } else if (product_type == "differential_phase") {
        return {0.0f, 360.0f, 0.0f, 230000.0f};
    } else if (product_type == "cross_correlation_ratio" || product_type == "correlation_coefficient") {
        return {0.0f, 1.1f, 0.0f, 230000.0f};
    }
    // Default: reflectivity
    return {-32.0f, 94.5f, 0.0f, 230000.0f};
}

uint8_t quantize_value(float value, float min_val, float max_val) {
    float range = max_val - min_val;
    float normalized = (value - min_val) / range;
    normalized = (normalized < 0.0f) ? 0.0f : (normalized > 1.0f) ? 1.0f : normalized;
    return static_cast<uint8_t>(std::round(normalized * 255.0f));
}

uint16_t quantize_azimuth(float azimuth_deg) {
    float normalized = azimuth_deg / 360.0f;
    normalized = (normalized < 0.0f) ? 0.0f : (normalized > 1.0f) ? 1.0f : normalized;
    return static_cast<uint16_t>(std::round(normalized * 65535.0f));
}

uint16_t float_to_float16(float f) {
    uint32_t bits;
    std::memcpy(&bits, &f, sizeof(float));
    
    uint16_t sign = (bits >> 31) & 0x1;
    uint16_t exponent = (bits >> 23) & 0xFF;
    uint32_t mantissa = bits & 0x7FFFFF;
    
    if (exponent == 0xFF) {
        if (mantissa == 0) {
            return (sign << 15) | 0x7C00;
        } else {
            return (sign << 15) | 0x7E00 | (mantissa >> 13);
        }
    }
    
    if (exponent == 0) {
        return sign << 15;
    }
    
    int new_exponent = exponent - 127 + 15;
    
    if (new_exponent >= 31) {
        return (sign << 15) | 0x7C00;
    }
    
    if (new_exponent <= 0) {
        if (new_exponent < -10) {
            return sign << 15;
        }
        uint32_t new_mantissa = (mantissa | 0x800000) >> (14 - new_exponent);
        return (sign << 15) | (new_mantissa & 0x3FF);
    }
    
    uint32_t new_mantissa = mantissa >> 13;
    return (sign << 15) | (new_exponent << 10) | (new_mantissa & 0x3FF);
}

static const char base64_chars[] = 
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

static std::string base64_encode(const uint8_t* byte_data, size_t byte_size) {
    std::string encoded;
    encoded.reserve((byte_size + 2) / 3 * 4);
    
    for (size_t i = 0; i < byte_size; i += 3) {
        uint32_t triple = (byte_data[i] << 16);
        if (i + 1 < byte_size) triple |= (byte_data[i + 1] << 8);
        if (i + 2 < byte_size) triple |= byte_data[i + 2];
        
        encoded.push_back(base64_chars[(triple >> 18) & 0x3F]);
        encoded.push_back(base64_chars[(triple >> 12) & 0x3F]);
        encoded.push_back(i + 1 < byte_size ? base64_chars[(triple >> 6) & 0x3F] : '=');
        encoded.push_back(i + 2 < byte_size ? base64_chars[triple & 0x3F] : '=');
    }
    return encoded;
}

std::string RadarFrame::encode_volumetric_3d_binary() const {
    if (volumetric_3d.empty()) return "";
    
    std::vector<uint16_t> float16_data;
    float16_data.reserve(volumetric_3d.size());
    for (float f : volumetric_3d) {
        float16_data.push_back(float_to_float16(f));
    }
    
    const uint8_t* byte_data = reinterpret_cast<const uint8_t*>(float16_data.data());
    return base64_encode(byte_data, float16_data.size() * sizeof(uint16_t));
}
