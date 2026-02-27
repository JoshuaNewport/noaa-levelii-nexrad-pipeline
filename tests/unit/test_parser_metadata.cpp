#include <iostream>
#include <fstream>
#include <vector>
#include <memory>
#include <iomanip>
#include <set>
#include <algorithm>
#include "levelii/RadarParser.h"
#include "levelii/RadarFrame.h"

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
    if (!file.read((char*)buffer.data(), size)) {
        std::cerr << "Could not read file: " << filename << std::endl;
        return 1;
    }

    std::cout << "Read " << size << " bytes from " << filename << std::endl;

    // Station and timestamp extraction from filename if possible, otherwise dummy
    std::string station = "KTLX";
    std::string timestamp = "20260209_162244";

    auto frame = parse_nexrad_level2(buffer, station, timestamp, "reflectivity");

    if (!frame) {
        std::cerr << "Failed to parse radar frame" << std::endl;
        return 1;
    }

    std::cout << "\n--- Radar Frame Metadata ---" << std::endl;
    std::cout << "Station: " << frame->station << std::endl;
    std::cout << "Timestamp: " << frame->timestamp << std::endl;
    std::cout << "Product: " << frame->product_type << std::endl;
    std::cout << "Lat/Lon: " << frame->radar_lat << ", " << frame->radar_lon << std::endl;
    std::cout << "Height ASL: " << frame->radar_height_asl_meters << " m" << std::endl;
    std::cout << "VCP: " << frame->vcp_number << std::endl;
    std::cout << "Number of sweeps: " << frame->nsweeps << std::endl;
    std::cout << "Number of gates: " << frame->ngates << std::endl;
    std::cout << "Number of rays: " << frame->nrays << std::endl;
    std::cout << "Gate spacing: " << frame->gate_spacing_meters << " m" << std::endl;
    std::cout << "First gate: " << frame->first_gate_meters << " m" << std::endl;

    std::cout << "\n--- Tilts Found (from available_tilts) ---" << std::endl;
    for (float tilt : frame->available_tilts) {
        std::cout << "Tilt: " << std::fixed << std::setprecision(2) << tilt << " deg";
        int tilt_key = RadarFrame::get_tilt_key(tilt);
        if (frame->sweep_ray_counts.count(tilt_key)) {
            std::cout << " (" << frame->sweep_ray_counts.at(tilt_key) << " rays)";
        }
        if (frame->nyquist_velocity.count(tilt_key)) {
            std::cout << " [Nyquist: " << frame->nyquist_velocity.at(tilt_key) << " m/s]";
        }
        std::cout << std::endl;
    }

    std::cout << "\n--- Sweeps Found (from sweeps vector) ---" << std::endl;
    for (const auto& sweep : frame->sweeps) {
        std::cout << "Sweep Index: " << sweep.index 
                  << " | Elev Num: " << (int)sweep.elevation_num 
                  << " | Angle: " << std::fixed << std::setprecision(2) << sweep.elevation_deg << " deg"
                  << " | Rays: " << sweep.ray_count
                  << " | Nyquist: " << sweep.nyquist_velocity << " m/s"
                  << " | Bins: " << (sweep.bins.size() / 3) << std::endl;
        
        if (sweep.index < 5 && sweep.ray_count > 0) {
            std::set<int> unique_az_milli;
            for (size_t i = 0; i < sweep.bins.size(); i += 3) {
                unique_az_milli.insert((int)(sweep.bins[i] * 1000.0f + 0.5f));
            }
            
            std::vector<float> azimuths;
            for (int azm : unique_az_milli) azimuths.push_back((float)azm / 1000.0f);
            
            float max_gap = 0;
            for (size_t i = 1; i < azimuths.size(); ++i) max_gap = std::max(max_gap, azimuths[i] - azimuths[i-1]);
            if (!azimuths.empty()) max_gap = std::max(max_gap, (360.0f - azimuths.back()) + azimuths.front());
            
            if (max_gap > 1.1f) {
                std::cout << "  ⚠️  Significant gap detected: " << max_gap << " deg (" << azimuths.size() << " unique azimuths)" << std::endl;
            }
        }
    }

    std::cout << "\n--- Data Summary ---" << std::endl;
    size_t total_bins = 0;
    for (const auto& sweep : frame->sweeps) {
        total_bins += sweep.bins.size() / 3; // each bin is (az, rng, val)
    }
    std::cout << "Total bins across all sweeps: " << total_bins << std::endl;

    if (!frame->sweeps.empty()) {
        const auto& sweep = frame->sweeps[0];
        const auto& bins = sweep.bins;
        std::cout << "\n--- Sample Data (First Sweep: " << sweep.elevation_deg << " deg) ---" << std::endl;
        size_t count = std::min(bins.size() / 3, (size_t)20);
        for (size_t i = 0; i < count; ++i) {
            std::cout << "Az: " << std::setw(6) << bins[i*3] 
                      << " | Rng: " << std::setw(8) << bins[i*3+1] 
                      << " | Val: " << std::setw(6) << bins[i*3+2] << std::endl;
        }
    }

    return 0;
}
