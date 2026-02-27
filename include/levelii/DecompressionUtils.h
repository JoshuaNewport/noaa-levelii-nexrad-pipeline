/*
 * NEXRAD Decompression Utilities
 * Handles bzip2 and LDM decompression for NEXRAD Level 2 data
 * 
 * Separated from main radar server for modularity and reusability
 */

#pragma once

#include <vector>
#include <cstdint>
#include <string>

namespace RadarDecompression {

// ============================================================================
// NEXRAD Level 2 Archive II format constants
// ============================================================================
constexpr int VOLUME_HEADER_SIZE = 24;
constexpr int CTM_HEADER_SIZE = 12;  // Control Transfer Message header
constexpr int CONTROL_WORD_SIZE = 4;  // LDM control word size
constexpr int COMPRESSION_RECORD_SIZE = 12;  // Compression record to skip
constexpr uint32_t NEXRAD_MAGIC = 0x41523256;  // "AR2V"

// ============================================================================
// Decompression Functions
// ============================================================================

/**
 * Automatically detect and decompress NEXRAD data
 * Handles both bzip2 and LDM formats
 * 
 * @param data Input data (may be compressed or uncompressed)
 * @param decompressed Output decompressed data
 * @return true if decompression succeeded (or data was already uncompressed)
 */
bool auto_decompress(const std::vector<uint8_t>& data, 
                     std::vector<uint8_t>& decompressed);

}  // namespace RadarDecompression
