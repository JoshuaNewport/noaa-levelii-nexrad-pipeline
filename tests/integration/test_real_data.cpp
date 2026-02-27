#include <iostream>
#include <fstream>
#include <vector>
#include <memory>
#include <cassert>
#include <iomanip>
#include "levelii/RadarParser.h"
#include "levelii/RadarFrame.h"
#include "levelii/DecompressionUtils.h"

void run_test(const std::string& name, bool (*test_func)()) {
    std::cout << "Running test: " << name << "... ";
    if (test_func()) {
        std::cout << "✅ PASSED" << std::endl;
    } else {
        std::cout << "❌ FAILED" << std::endl;
    }
}

bool test_parser_metadata() {
    std::ifstream file("../KTLX20260209_162244_V06", std::ios::binary);
    if (!file.is_open()) return false;
    std::vector<uint8_t> buffer((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
    
    auto frame = parse_nexrad_level2(buffer, "DUMMY", "20260101_000000", "reflectivity");
    if (!frame) return false;
    
    if (frame->station != "KTLX") return false;
    if (frame->vcp_number != 35) return false;
    if (frame->available_tilts.empty()) return false;
    
    return true;
}

bool test_first_gate_consistency() {
    std::ifstream file("../KTLX20260209_162244_V06", std::ios::binary);
    if (!file.is_open()) return false;
    std::vector<uint8_t> buffer((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
    
    auto frame = parse_nexrad_level2(buffer, "KTLX", "20260209_162244", "reflectivity");
    if (!frame) return false;
    
    if (std::abs(frame->first_gate_meters - 2125.0f) > 1.0f) {
        return false;
    }
    
    return true;
}

bool test_kcrp_parsing() {
    std::ifstream file("../KCRP20260213_171946_V06", std::ios::binary);
    if (!file.is_open()) return false;
    std::vector<uint8_t> buffer((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
    
    auto frame = parse_nexrad_level2(buffer, "KCRP", "20260213_171946", "reflectivity");
    if (!frame) return false;
    
    if (frame->station != "KCRP") return false;
    if (frame->vcp_number != 215) return false;
    if (frame->available_tilts.size() < 14) return false;
    
    return true;
}

int main() {
    std::cout << "=== NEXRAD Level II Real Data Integration Tests ===" << std::endl;
    
    run_test("Parser Metadata (KTLX)", test_parser_metadata);
    run_test("First Gate Consistency", test_first_gate_consistency);
    run_test("KCRP Parsing & Ray Counts", test_kcrp_parsing);
    
    std::cout << "=========================================" << std::endl;
    return 0;
}
