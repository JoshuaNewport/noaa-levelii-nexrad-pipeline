#include "levelii/VolumetricGenerator.h"
#include <cmath>

#ifndef M_PI
#define M_PI 3.14159265358979323846
#endif

void VolumetricGenerator::generate_volumetric_3d(RadarFrame& frame) {
    if (frame.sweeps.empty()) {
        frame.has_volumetric_data = false;
        return;
    }
    
    frame.volumetric_3d.clear();
    frame.volumetric_3d.reserve(1000000);
    
    const float RE = 6371000.0f;  // Earth radius in meters
    const float IR = 4.0f / 3.0f; // 4/3 earth radius factor for refraction
    const float R_PRIME = RE * IR;
    const float radar_height_asl = frame.radar_height_asl_meters;
    const float base = R_PRIME + radar_height_asl;
    const float base_sq = base * base;
    
    for (const auto& sweep : frame.sweeps) {
        float elevation_rad = sweep.elevation_deg * static_cast<float>(M_PI) / 180.0f;
        float cos_elev = std::cos(elevation_rad);
        float sin_elev = std::sin(elevation_rad);
        float two_base_sin_elev = 2.0f * base * sin_elev;
        
        const auto& bins = sweep.bins;
        float last_azimuth = -999.0f;
        float sin_azimuth = 0.0f;
        float cos_azimuth = 0.0f;

        for (size_t i = 0; i + 2 < bins.size(); i += 3) {
            float azimuth_deg = bins[i];
            float range_meters = bins[i + 1];
            float value = bins[i + 2];
            
            if (value <= -100.0f) continue; // Skip no-data/very low values
            
            if (azimuth_deg != last_azimuth) {
                float azimuth_rad = azimuth_deg * static_cast<float>(M_PI) / 180.0f;
                sin_azimuth = std::sin(azimuth_rad);
                cos_azimuth = std::cos(azimuth_rad);
                last_azimuth = azimuth_deg;
            }
            
            // Standard 4/3 earth radius model for height above sea level
            float height_asl = std::sqrt(range_meters * range_meters + base_sq + 
                                        range_meters * two_base_sin_elev) - R_PRIME;
            
            // Ground distance s along the curved earth
            float arg = (range_meters * cos_elev) / (R_PRIME + height_asl);
            if (arg < -1.0f) arg = -1.0f;
            if (arg > 1.0f) arg = 1.0f;
            float s = R_PRIME * std::asin(arg);
            
            float x = s * sin_azimuth;
            float y = s * cos_azimuth;
            float z = height_asl - radar_height_asl; // Height relative to radar
            
            frame.volumetric_3d.push_back(x);
            frame.volumetric_3d.push_back(y);
            frame.volumetric_3d.push_back(z);
            frame.volumetric_3d.push_back(value);
        }
    }
    
    frame.has_volumetric_data = !frame.volumetric_3d.empty();
}
