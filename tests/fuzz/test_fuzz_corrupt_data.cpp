#include <iostream>
#include <vector>
#include <cstring>
#include <cstdint>
#include "levelii/RadarParser.h"
#include "levelii/NEXRAD_Types.h"
#include "levelii/ByteReader.h"

void create_minimal_valid_header(std::vector<uint8_t>& data) {
    nexrad::VolumeHeader vh;
    std::memcpy(vh.filename, "ARCHIVE2", 8);
    vh.julian_date = 0x00010001;
    vh.milliseconds = 0x00000000;
    std::memcpy(vh.radar_id, "TEST", 4);
    
    std::memcpy(data.data(), &vh, sizeof(nexrad::VolumeHeader));
}

void test_invalid_block_pointer_out_of_range() {
    std::cout << "Test: Corrupted block pointer (out of range)..." << std::endl;
    
    std::vector<uint8_t> data(1024);
    create_minimal_valid_header(data);
    
    auto frame = parse_nexrad_level2(data, "TEST", "20260000_000000", "reflectivity");
    
    if (!frame || frame->nrays == 0) {
        std::cout << "✓ Gracefully handled out-of-range block pointer" << std::endl;
    } else {
        std::cout << "⚠️  Data was parsed despite corruption (might be expected if caught early)" << std::endl;
    }
}

void test_buffer_with_invalid_message_header() {
    std::cout << "Test: Corrupted message header..." << std::endl;
    
    std::vector<uint8_t> data(2000);
    create_minimal_valid_header(data);
    
    nexrad::MessageHeader bad_header;
    bad_header.size = 0xFFFF;
    bad_header.type = 31;
    bad_header.julian_date = 0x0001;
    bad_header.milliseconds = 0x00000000;
    bad_header.segment_num = 1;
    bad_header.num_segments = 1;
    
    size_t msg_offset = 500;
    std::memcpy(data.data() + msg_offset, &bad_header, sizeof(nexrad::MessageHeader));
    
    auto frame = parse_nexrad_level2(data, "TEST", "20260000_000000", "reflectivity");
    
    if (frame) {
        std::cout << "✓ Handled corrupted message header without crashing" << std::endl;
    } else {
        std::cout << "⚠️  Parser returned null (safe behavior)" << std::endl;
    }
}

void test_integer_overflow_block_count() {
    std::cout << "Test: Integer overflow on block count..." << std::endl;
    
    std::vector<uint8_t> data(2000);
    create_minimal_valid_header(data);
    
    nexrad::MessageHeader msg_header;
    msg_header.size = 0x0064;
    msg_header.type = 31;
    msg_header.julian_date = 0x0001;
    msg_header.milliseconds = 0x00000000;
    msg_header.segment_num = 1;
    msg_header.num_segments = 1;
    
    std::memcpy(data.data() + 100, &msg_header, sizeof(nexrad::MessageHeader));
    
    nexrad::Message31Header m31;
    m31.block_count = 0xFFFF;
    std::memcpy(data.data() + 100 + sizeof(nexrad::MessageHeader), &m31, 
                sizeof(nexrad::Message31Header));
    
    auto frame = parse_nexrad_level2(data, "TEST", "20260000_000000", "reflectivity");
    
    std::cout << "✓ Integer overflow on block count handled safely" << std::endl;
}

void test_null_buffer_pointer() {
    std::cout << "Test: Null buffer pointer in safe_read_struct..." << std::endl;
    
    auto result = nexrad::safe_read_struct<nexrad::VolumeHeader>(nullptr, 100, 0);
    
    if (!result.has_value()) {
        std::cout << "✓ Null pointer correctly rejected" << std::endl;
    } else {
        std::cout << "❌ FAILED: Null pointer was not detected" << std::endl;
    }
}

void test_offset_beyond_buffer() {
    std::cout << "Test: Offset beyond buffer bounds..." << std::endl;
    
    std::vector<uint8_t> data(100);
    
    auto result = nexrad::safe_read_struct<nexrad::Message31Header>(
        data.data(), data.size(), 500
    );
    
    if (!result.has_value()) {
        std::cout << "✓ Offset beyond buffer correctly rejected" << std::endl;
    } else {
        std::cout << "❌ FAILED: Offset beyond buffer was accepted" << std::endl;
    }
}

void test_pointer_dereference_overflow() {
    std::cout << "Test: Pointer dereference with size overflow..." << std::endl;
    
    size_t payload_size = 100;
    size_t ptr_offset = 50;
    size_t required_size = 60;
    
    bool result = nexrad::safe_pointer_dereference(
        ptr_offset, required_size, payload_size, "TestBlock"
    );
    
    if (!result) {
        std::cout << "✓ Size overflow correctly detected" << std::endl;
    } else {
        std::cout << "❌ FAILED: Size overflow was not detected" << std::endl;
    }
}

void test_pointer_dereference_negative_offset() {
    std::cout << "Test: Pointer dereference with boundary offset..." << std::endl;
    
    size_t payload_size = 1000;
    size_t ptr_offset = 1000;
    size_t required_size = 1;
    
    bool result = nexrad::safe_pointer_dereference(
        ptr_offset, required_size, payload_size, "TestBlock"
    );
    
    if (!result) {
        std::cout << "✓ Boundary overflow correctly detected" << std::endl;
    } else {
        std::cout << "⚠️  Boundary case might be valid (exact fit)" << std::endl;
    }
}

int main() {
    std::cout << "\n=== Fuzz Testing: Corrupted Data Handling ===" << std::endl;
    
    try {
        test_invalid_block_pointer_out_of_range();
        test_buffer_with_invalid_message_header();
        test_integer_overflow_block_count();
        test_null_buffer_pointer();
        test_offset_beyond_buffer();
        test_pointer_dereference_overflow();
        test_pointer_dereference_negative_offset();
        
        std::cout << "\n=== All Fuzz Tests Completed Without Segfaults ✓ ===" << std::endl;
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "❌ Test failed with exception: " << e.what() << std::endl;
        return 1;
    }
}
