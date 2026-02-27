#include <iostream>
#include <vector>
#include <cassert>
#include "levelii/ByteReader.h"
#include "levelii/NEXRAD_Types.h"

void test_safe_read_struct_valid() {
    std::cout << "Test: safe_read_struct with valid data..." << std::endl;
    
    nexrad::VolumeHeader header;
    header.julian_date = 0x00010001;
    header.milliseconds = 0x00000000;
    std::memcpy(header.radar_id, "KTLX", 4);
    
    std::vector<uint8_t> buffer(sizeof(nexrad::VolumeHeader) + 100);
    std::memcpy(buffer.data(), &header, sizeof(nexrad::VolumeHeader));
    
    auto result = nexrad::safe_read_struct<nexrad::VolumeHeader>(
        buffer.data(), buffer.size(), 0, "VolumeHeader"
    );
    
    assert(result.has_value());
    assert(result.value()->julian_date == header.julian_date);
    std::cout << "✓ Valid data read successfully" << std::endl;
}

void test_safe_read_struct_boundary() {
    std::cout << "Test: safe_read_struct at buffer boundary..." << std::endl;
    
    nexrad::VolumeHeader header;
    std::vector<uint8_t> buffer(sizeof(nexrad::VolumeHeader));
    std::memcpy(buffer.data(), &header, sizeof(nexrad::VolumeHeader));
    
    auto result = nexrad::safe_read_struct<nexrad::VolumeHeader>(
        buffer.data(), buffer.size(), 0
    );
    
    assert(result.has_value());
    std::cout << "✓ Boundary case handled correctly" << std::endl;
}

void test_safe_read_struct_overflow() {
    std::cout << "Test: safe_read_struct buffer overflow detection..." << std::endl;
    
    std::vector<uint8_t> buffer(10);
    
    auto result = nexrad::safe_read_struct<nexrad::VolumeHeader>(
        buffer.data(), buffer.size(), 0
    );
    
    assert(!result.has_value());
    std::cout << "✓ Buffer overflow correctly detected" << std::endl;
}

void test_safe_read_struct_invalid_offset() {
    std::cout << "Test: safe_read_struct invalid offset detection..." << std::endl;
    
    std::vector<uint8_t> buffer(100);
    
    auto result = nexrad::safe_read_struct<nexrad::VolumeHeader>(
        buffer.data(), buffer.size(), 200
    );
    
    assert(!result.has_value());
    std::cout << "✓ Invalid offset correctly detected" << std::endl;
}

void test_safe_read_struct_null_pointer() {
    std::cout << "Test: safe_read_struct null pointer detection..." << std::endl;
    
    auto result = nexrad::safe_read_struct<nexrad::VolumeHeader>(
        nullptr, 100, 0
    );
    
    assert(!result.has_value());
    std::cout << "✓ Null pointer correctly detected" << std::endl;
}

void test_safe_pointer_dereference_valid() {
    std::cout << "Test: safe_pointer_dereference with valid data..." << std::endl;
    
    size_t payload_size = 1000;
    size_t ptr_offset = 100;
    size_t required_size = 50;
    
    bool result = nexrad::safe_pointer_dereference(
        ptr_offset, required_size, payload_size, "TestBlock"
    );
    
    assert(result);
    std::cout << "✓ Valid pointer dereference allowed" << std::endl;
}

void test_safe_pointer_dereference_null_pointer() {
    std::cout << "Test: safe_pointer_dereference with null pointer..." << std::endl;
    
    size_t payload_size = 1000;
    size_t ptr_offset = 0;
    size_t required_size = 50;
    
    bool result = nexrad::safe_pointer_dereference(
        ptr_offset, required_size, payload_size, "TestBlock"
    );
    
    assert(!result);
    std::cout << "✓ Null pointer correctly rejected" << std::endl;
}

void test_safe_pointer_dereference_out_of_range() {
    std::cout << "Test: safe_pointer_dereference out of range..." << std::endl;
    
    size_t payload_size = 100;
    size_t ptr_offset = 200;
    size_t required_size = 50;
    
    bool result = nexrad::safe_pointer_dereference(
        ptr_offset, required_size, payload_size, "TestBlock"
    );
    
    assert(!result);
    std::cout << "✓ Out of range pointer correctly rejected" << std::endl;
}

void test_safe_pointer_dereference_overflow() {
    std::cout << "Test: safe_pointer_dereference size overflow..." << std::endl;
    
    size_t payload_size = 100;
    size_t ptr_offset = 50;
    size_t required_size = 100;
    
    bool result = nexrad::safe_pointer_dereference(
        ptr_offset, required_size, payload_size, "TestBlock"
    );
    
    assert(!result);
    std::cout << "✓ Size overflow correctly detected" << std::endl;
}

void test_safe_pointer_dereference_exact_boundary() {
    std::cout << "Test: safe_pointer_dereference exact boundary..." << std::endl;
    
    size_t payload_size = 100;
    size_t ptr_offset = 50;
    size_t required_size = 50;
    
    bool result = nexrad::safe_pointer_dereference(
        ptr_offset, required_size, payload_size, "TestBlock"
    );
    
    assert(result);
    std::cout << "✓ Exact boundary case handled correctly" << std::endl;
}

void test_read_be_helpers() {
    std::cout << "Test: Big-Endian read helpers..." << std::endl;
    
    uint8_t data[] = {0x12, 0x34, 0x56, 0x78};
    
    uint16_t val16 = nexrad::read_be<uint16_t>(data);
    assert(val16 == 0x1234);
    
    uint32_t val32 = nexrad::read_be<uint32_t>(data);
    assert(val32 == 0x12345678);
    
    std::cout << "✓ Big-Endian helpers working correctly" << std::endl;
}

int main() {
    std::cout << "\n=== Decompression Unit Tests ===" << std::endl;
    
    try {
        test_safe_read_struct_valid();
        test_safe_read_struct_boundary();
        test_safe_read_struct_overflow();
        test_safe_read_struct_invalid_offset();
        test_safe_read_struct_null_pointer();
        
        test_safe_pointer_dereference_valid();
        test_safe_pointer_dereference_null_pointer();
        test_safe_pointer_dereference_out_of_range();
        test_safe_pointer_dereference_overflow();
        test_safe_pointer_dereference_exact_boundary();
        
        test_read_be_helpers();
        
        std::cout << "\n=== All Tests Passed ✓ ===" << std::endl;
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "❌ Test failed with exception: " << e.what() << std::endl;
        return 1;
    }
}
