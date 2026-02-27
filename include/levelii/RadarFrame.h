#pragma once

#include <string>
#include <vector>
#include <unordered_map>
#include <tuple>
#include <nlohmann/json.hpp>

using json = nlohmann::json;

// Configuration constants
constexpr int DOWNSAMPLE_GATES = 1;
constexpr float MIN_DBZ = -32.0f;

// Quantization utilities for compact binary format
struct QuantizationParams {
    float value_min;
    float value_max;
    float range_min;
    float range_max;
};

QuantizationParams get_quant_params(const std::string& product_type);
uint8_t quantize_value(float value, float min_val, float max_val);
uint16_t quantize_azimuth(float azimuth_deg);

struct RadarFrame {
    std::string station;
    std::string timestamp;
    std::string product_type;
    double radar_lat;
    double radar_lon;
    float max_range_meters;
    
    // Sweep structure for robust tracking
    struct Sweep {
        int index;                  // 0-based index in the volume
        uint8_t elevation_num;      // Elevation number from Message 31
        float elevation_deg;        // Actual elevation angle
        int ray_count;              // Number of rays in this sweep
        float nyquist_velocity;     // Nyquist velocity for this sweep
        std::vector<float> bins;    // Bins for this specific sweep
        
        Sweep() : index(0), elevation_num(0), elevation_deg(0.0f), ray_count(0), nyquist_velocity(0.0f) {}
    };
    std::vector<Sweep> sweeps;
    std::vector<float> available_tilts;  // List of available elevation angles
    
    // Key generator for tilt maps to avoid floating point precision issues
    // Uses 100x scaling (e.g. 0.5 deg -> 50)
    static inline int get_tilt_key(float elevation) {
        return static_cast<int>(std::round(elevation * 100.0f));
    }
    
    // Metadata
    int nsweeps;
    int ngates;
    int nrays;
    uint16_t vcp_number;
    float radar_height_asl_meters;
    float elevation_deg;
    float gate_spacing_meters;
    float range_spacing_meters;
    float first_gate_meters;  // Distance to first gate in meters
    
    // Per-sweep metadata
    std::unordered_map<int, int> sweep_ray_counts;  // elevation_key -> number of rays
    
    // Velocity dealiasing metadata
    std::unordered_map<int, float> nyquist_velocity;  // elevation_key -> Nyquist velocity (m/s)
    float unambiguous_range_meters = 0.0f;
    std::string prf_mode;  // "fixed", "staggered", "hybrid"
    
    // Dual-polarimetric metadata (for ZDR, PHI, RHO)
    struct DualPolMetadata {
        float zdr_calibration_db = 0.0f;
        float phidp_offset_deg = 0.0f;
        float rho_threshold = 0.9f;
    } dualpol_meta;
    
    // Volumetric 3D data: [x, y, z, value] in earth coordinates (meters from radar origin)
    std::vector<float> volumetric_3d;
    bool has_volumetric_data = false;
    
    RadarFrame() : radar_lat(0.0), radar_lon(0.0), max_range_meters(0.0f), 
                   nsweeps(0), ngates(0), nrays(0), vcp_number(0), 
                   radar_height_asl_meters(0.0f), elevation_deg(0.0f), 
                   gate_spacing_meters(0.0f), range_spacing_meters(0.0f), first_gate_meters(0.0f),
                   has_volumetric_data(false) {}
    
    std::string encode_volumetric_3d_binary() const;
};
