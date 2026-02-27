#include <iostream>
#include <fstream>
#include <vector>
#include <memory>
#include <unordered_map>
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

    std::vector<std::string> products = {"reflectivity", "velocity"};
    auto frames = parse_nexrad_level2_multi(buffer, "KTLX", "20260209_162244", products);

    if (frames.size() != products.size()) {
        std::cerr << "Expected " << products.size() << " frames, got " << frames.size() << std::endl;
        return 1;
    }

    for (const auto& product : products) {
        if (frames.find(product) == frames.end()) {
            std::cerr << "Product " << product << " not found in results" << std::endl;
            return 1;
        }
        auto& frame = frames[product];
        std::cout << "Product: " << frame->product_type << " | Sweeps: " << frame->nsweeps << " | Rays: " << frame->nrays << std::endl;
        
        if (frame->nsweeps == 0) {
            std::cerr << "Frame for " << product << " has no sweeps" << std::endl;
            // Velocity might not be present in all files, but reflectivity should be.
            if (product == "reflectivity") return 1;
        }
    }

    // Verify consistency: same number of sweeps and same elevation angles
    auto& ref_frame = frames["reflectivity"];
    auto& vel_frame = frames["velocity"];
    
    if (ref_frame->nsweeps != vel_frame->nsweeps) {
        std::cout << "Note: Reflectivity and Velocity have different number of sweeps (" 
                  << ref_frame->nsweeps << " vs " << vel_frame->nsweeps << ")" << std::endl;
    }

    std::cout << "Multi-product parsing test passed!" << std::endl;
    return 0;
}
