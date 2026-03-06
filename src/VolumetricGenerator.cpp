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
    
    size_t total_potential_bins = 0;
    for (const auto& sweep : frame.sweeps) {
        total_potential_bins += sweep.bins.size() / 3;
    }
    frame.volumetric_3d.reserve(total_potential_bins * 4);
    
    const double RE = 6371000.0;  // Earth radius in meters
    const double IR = 4.0 / 3.0; // 4/3 earth radius factor for refraction
    const double R_PRIME = RE * IR;
    const double radar_height_asl = static_cast<double>(frame.radar_height_asl_meters);
    const double base = R_PRIME + radar_height_asl;
    const double base_sq = base * base;
    
    for (const auto& sweep : frame.sweeps) {
        double elevation_rad = static_cast<double>(sweep.elevation_deg) * M_PI / 180.0;
        double cos_elev = std::cos(elevation_rad);
        double sin_elev = std::sin(elevation_rad);
        
        const auto& bins = sweep.bins;
        double last_azimuth = -999.0;
        double sin_azimuth = 0.0;
        double cos_azimuth = 0.0;

        for (size_t i = 0; i + 2 < bins.size(); i += 3) {
            double azimuth_deg = static_cast<double>(bins[i]);
            double range_meters = static_cast<double>(bins[i + 1]);
            double value = static_cast<double>(bins[i + 2]);
            
            if (value <= -100.0) continue; // Skip no-data/very low values
            
            if (azimuth_deg != last_azimuth) {
                double azimuth_rad = azimuth_deg * M_PI / 180.0;
                sin_azimuth = std::sin(azimuth_rad);
                cos_azimuth = std::cos(azimuth_rad);
                last_azimuth = azimuth_deg;
            }
            
            // Standard 4/3 earth radius model for height above sea level
            // h = sqrt(r^2 + (Re'+H0)^2 + 2*r*(Re'+H0)*sin(elev)) - Re'
            double height_asl = std::sqrt(range_meters * range_meters + base_sq + 
                                        2.0 * range_meters * base * sin_elev) - R_PRIME;
            
            // Ground distance s along the curved earth (arc length)
            // Using atan2 for better precision and stability than asin
            // theta = atan2(r * cos(elev), Re' + H0 + r * sin(elev))
            double theta = std::atan2(range_meters * cos_elev, base + range_meters * sin_elev);
            double s = R_PRIME * theta;
            
            // Arc-length projection for horizontal coordinates
            float x = static_cast<float>(s * sin_azimuth);
            float y = static_cast<float>(s * cos_azimuth);
            
            // Height relative to the radar's origin plane (tangent plane)
            // z = (Re' + h) * cos(theta) - (Re' + H0)
            // This simplifies to z = r * sin(elev) but we keep the form for clarity
            double z_relative = (R_PRIME + height_asl) * std::cos(theta) - base;
            float z = static_cast<float>(z_relative);
            
            frame.volumetric_3d.push_back(x);
            frame.volumetric_3d.push_back(y);
            frame.volumetric_3d.push_back(z);
            frame.volumetric_3d.push_back(static_cast<float>(value));
        }
    }
    
    frame.has_volumetric_data = !frame.volumetric_3d.empty();
}
