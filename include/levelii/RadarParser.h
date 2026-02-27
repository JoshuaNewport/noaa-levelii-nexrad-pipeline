#pragma once

#include <memory>
#include <vector>
#include <string>
#include <unordered_map>
#include "levelii/RadarFrame.h"

/**
 * @brief Parses raw NEXRAD Level II data into a structured RadarFrame.
 * 
 * This function handles decompression (if needed) and extracts meteorological
 * data moments (reflectivity, velocity, etc.) from the binary buffer.
 * 
 * @param data Raw binary data buffer from a Level II file.
 * @param station 4-letter station identifier (e.g., "KTLX").
 * @param timestamp ISO-formatted timestamp of the data collection.
 * @param product_type The specific data moment to extract (default: "reflectivity").
 * @return std::unique_ptr<RadarFrame> Structured frame containing sweeps and metadata, or nullptr on failure.
 */
std::unique_ptr<RadarFrame> parse_nexrad_level2(
    const std::vector<uint8_t>& data,
    const std::string& station,
    const std::string& timestamp,
    const std::string& product_type = "reflectivity"
);

/**
 * @brief Parses raw NEXRAD Level II data into multiple structured RadarFrames (one per product).
 * 
 * Highly optimized single-pass parsing that extracts multiple products from the same
 * binary buffer, avoiding redundant decompression and message scanning.
 * 
 * @param data Raw binary data buffer from a Level II file.
 * @param station 4-letter station identifier (e.g., "KTLX").
 * @param timestamp ISO-formatted timestamp of the data collection.
 * @param product_types List of products to extract (e.g., {"reflectivity", "velocity"}).
 * @return std::unordered_map<std::string, std::unique_ptr<RadarFrame>> Map from product name to its frame.
 */
std::unordered_map<std::string, std::unique_ptr<RadarFrame>> parse_nexrad_level2_multi(
    const std::vector<uint8_t>& data,
    const std::string& station,
    const std::string& timestamp,
    const std::vector<std::string>& product_types,
    std::vector<uint8_t>* decompressed_buffer = nullptr
);
