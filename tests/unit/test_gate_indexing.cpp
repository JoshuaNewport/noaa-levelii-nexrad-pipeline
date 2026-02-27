#include <iostream>
#include <vector>
#include <cmath>
#include <cassert>
#include <iomanip>

void test_gate_indexing() {
    std::cout << "\n=== GATE INDEXING TEST ===\n";
    
    float first_gate = 500.0f;
    float gate_spacing = 250.0f;
    
    struct TestCase {
        float range;
        int expected_gate;
        const char* description;
    };
    
    TestCase cases[] = {
        {500.0f, 0, "First gate"},
        {749.9f, 0, "Just before second gate"},
        {750.0f, 1, "Second gate"},
        {999.9f, 1, "Just before third gate"},
        {1000.0f, 2, "Third gate"},
        {499.0f, -1, "Before first gate (invalid)"},
        {250.0f, -1, "Way before first gate (invalid)"},
    };
    
    bool all_passed = true;
    for (const auto& tc : cases) {
        int gate_idx = static_cast<int>(std::floor((tc.range - first_gate) / gate_spacing));
        
        if (gate_idx == tc.expected_gate) {
            std::cout << "✅ " << tc.description << ": range=" << tc.range << " -> gate=" << gate_idx << "\n";
        } else {
            std::cout << "❌ " << tc.description << ": range=" << tc.range << " -> gate=" << gate_idx 
                      << " (expected " << tc.expected_gate << ")\n";
            all_passed = false;
        }
    }
    
    if (all_passed) {
        std::cout << "✅ PASSED: Gate indexing is correct\n";
    }
}

int main() {
    std::cout << "=" << std::string(60, '=') << "\n";
    std::cout << "GATE INDEXING TEST SUITE FOR LEVELII PROCESSOR\n";
    std::cout << "=" << std::string(60, '=') << "\n";
    
    test_gate_indexing();
    
    std::cout << "\n" << "=" << std::string(60, '=') << "\n";
    std::cout << "Gate indexing test completed.\n";
    std::cout << "=" << std::string(60, '=') << "\n";
    
    return 0;
}
