#include <iostream>
#include <fstream>
#include <vector>
#include <memory>
#include <unordered_map>
#include <cmath>
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

    std::vector<std::string> products = {
        "reflectivity", 
        "velocity", 
        "spectrum_width", 
        "differential_reflectivity", 
        "differential_phase", 
        "correlation_coefficient"
    };
    
    // Use station and timestamp hints that will be overwritten by actual data if found
    auto frames = parse_nexrad_level2_multi(buffer, "KXXX", "20240101_000000", products);

    std::cout << "Requested " << products.size() << " products, got " << frames.size() << " frames" << std::endl;

    if (frames.empty()) {
        std::cerr << "Error: No frames parsed at all!" << std::endl;
        return 1;
    }

    // Reflectivity is mandatory for a successful parse in this test
    if (frames.find("reflectivity") == frames.end()) {
        std::cerr << "Error: Reflectivity product not found" << std::endl;
        return 1;
    }

    bool all_products_found = true;
    for (const auto& product : products) {
        if (frames.find(product) == frames.end()) {
            std::cout << "Warning: Product " << product << " not found in results" << std::endl;
            // Velocity and others might be missing in some files, but we hope to find them in V06 files
            if (product == "reflectivity") return 1;
            all_products_found = false;
            continue;
        }
        
        auto& frame = frames[product];
        size_t total_bins = 0;
        for (const auto& sweep : frame->sweeps) {
            total_bins += sweep.bins.size() / 3;
        }
        
        std::cout << "Product: " << product 
                  << " | Sweeps: " << frame->nsweeps 
                  << " | Total Bins: " << total_bins << std::endl;
        
        if (frame->nsweeps == 0 || total_bins == 0) {
            std::cerr << "Error: Product " << product << " has no data!" << std::endl;
            if (product == "reflectivity") return 1;
        }

        // Check for realistic values (very basic check)
        if (!frame->sweeps.empty()) {
            for (const auto& sweep : frame->sweeps) {
                if (!sweep.bins.empty()) {
                    float val = sweep.bins[2];
                    std::cout << "  Sample (Sweep " << sweep.index << "): Az: " << sweep.bins[0] 
                              << " Rng: " << sweep.bins[1] << " Val: " << val << std::endl;
                    
                    // Simple range checks
                    if (product == "reflectivity" && (val < -33.0f || val > 95.0f)) {
                        std::cerr << "Warning: Unusual reflectivity value: " << val << std::endl;
                    }
                    if (product == "velocity" && (val < -100.0f || val > 100.0f)) {
                        std::cerr << "Warning: Unusual velocity value: " << val << std::endl;
                    }
                    break;
                }
            }
        }

        // Check Dual Pol Metadata (should be same for all frames in same volume)
        if (product == "differential_reflectivity" || product == "differential_phase" || product == "correlation_coefficient") {
             std::cout << "  DualPol Meta: sys_diff_refl=" << frame->dualpol_meta.sys_diff_refl 
                       << ", sys_diff_phase=" << frame->dualpol_meta.sys_diff_phase << std::endl;
        }
    }

    if (!all_products_found) {
        std::cout << "Some products were missing, but reflectivity was found." << std::endl;
    }

    std::cout << "All moments test completed successfully!" << std::endl;
    return 0;
}
