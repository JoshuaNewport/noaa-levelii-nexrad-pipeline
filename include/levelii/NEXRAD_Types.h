#pragma once

#include <cstdint>

/**
 * NEXRAD Level II Data Structures based on ICD 2620010J.
 * All multi-byte fields are Big-Endian as per the ICD, unless otherwise noted.
 */

#pragma pack(push, 1)

namespace nexrad {

/**
 * Volume Header (RDA to User / External)
 * 24 bytes total
 * This is the very first header in a Level II file.
 */
struct VolumeHeader {
    char filename[12];          // Archive II filename (9 bytes version + 3 bytes extension)
    uint32_t julian_date;       // NEXRAD-modified Julian date (Big Endian, days since Jan 1, 1970 where 1/1/1970=1)
    uint32_t milliseconds;      // Milliseconds past midnight (Big Endian)
    char radar_id[4];           // Radar identifier (ICAO)
};

/**
 * Message Header
 * 16 bytes total
 * Every message (e.g., Message 31) is preceded by this header.
 */
struct MessageHeader {
    uint16_t size;              // Message size in halfwords (Big Endian)
    uint8_t rda_redundancy;     // RDA Redundancy Channel
    uint8_t type;               // Message Type (31 for Generic Digital Radar Data)
    uint16_t sequence_num;      // ID Sequence Number (Big Endian)
    uint16_t julian_date;       // Julian Date (Big Endian)
    uint32_t milliseconds;      // Milliseconds of day (Big Endian)
    uint16_t num_segments;      // Number of message segments (Big Endian)
    uint16_t segment_num;       // Message segment number (Big Endian)
};

/**
 * Message 31: Generic Digital Radar Data Header
 * This follows the MessageHeader when type == 31.
 */
struct Message31Header {
    char radar_id[4];           // Radar Identifier
    uint32_t collection_time;   // Collection Time (ms of day) (Big Endian)
    uint16_t collection_date;   // Collection Date (Julian) (Big Endian)
    uint16_t azimuth_number;    // Azimuth Number (Big Endian)
    float azimuth_angle;        // Azimuth Angle (deg) (Big Endian float)
    uint8_t compression;        // Compression Indicator
    uint8_t spare;
    uint16_t radial_length;     // Radial Length (Big Endian)
    uint8_t az_spacing;         // Azimuth Resolution Spacing
    uint8_t radial_status;      // Radial Status
    uint8_t elev_number;        // Elevation Number
    uint8_t sector_num;         // Cut Sector Number
    float elev_angle;           // Elevation Angle (deg) (Big Endian float)
    uint8_t radial_blanking;    // Radial Blanking Status
    uint8_t az_indexing_mode;   // Azimuth Indexing Mode
    uint16_t block_count;       // Data Block Count (Big Endian)
    uint32_t block_pointers[1]; // Data Block Pointers (Big Endian, offset from Message 31 start). Variable length.
};

/**
 * Common Header for all Data Blocks
 */
struct DataBlock_Header {
    char type;                  // 'V', 'E', 'R', 'D'
    char name[3];               // "VOL", "ELV", "RAD", "REF", etc.
};

/**
 * Radial Status values for Message 31
 */
enum RadialStatus : uint8_t {
    STATUS_START_ELEVATION = 0,
    STATUS_INTERMEDIATE = 1,
    STATUS_END_ELEVATION = 2,
    STATUS_START_VOLUME = 3,
    STATUS_END_VOLUME = 4,
    STATUS_START_ELEVATION_SEGMENTED = 5
};

/**
 * Data Block: Volume ('V')
 * Contains site-specific metadata and VCP information.
 */
struct DataBlock_Volume {
    char type;                  // 'V'
    char name[3];               // "VOL"
    uint16_t size;              // Block size (Big Endian)
    uint8_t version_major;
    uint8_t version_minor;
    float lat;                  // Latitude (deg) (Big Endian float)
    float lon;                  // Longitude (deg) (Big Endian float)
    int16_t site_height;        // Site Height (m) (Big Endian)
    uint16_t feedhorn_height;   // Feedhorn Height (m) (Big Endian)
    float calibration;          // Calibration Constant (Big Endian float)
    float tx_power_h;           // Horizontal Transmit Power (Big Endian float)
    float tx_power_v;           // Vertical Transmit Power (Big Endian float)
    float sys_diff_refl;        // System Differential Reflectivity (Big Endian float)
    float sys_diff_phase;       // System Differential Phase (Big Endian float)
    uint16_t vcp_number;        // VCP Number (Big Endian)
    uint16_t processing_status; // Processing Status (Big Endian)
};

/**
 * Data Block: Elevation ('E')
 * Contains metadata specific to the current elevation cut.
 */
struct DataBlock_Elevation {
    char type;                  // 'E'
    char name[3];               // "ELV"
    uint16_t size;              // Block size (Big Endian)
    uint16_t atmos;             // Atmospheric attenuation factor (Big Endian)
    float calibration;          // Calibration Constant (Big Endian float)
};

/**
 * Data Block: Radial ('R')
 * Contains metadata for the current radial.
 */
struct DataBlock_Radial {
    char type;                  // 'R'
    char name[3];               // "RAD"
    uint16_t size;              // Block size (Big Endian)
    uint16_t unambiguous_range; // Unambiguous Range (km) (Big Endian)
    float noise_h;              // Horizontal Noise Level (Big Endian float)
    float noise_v;              // Vertical Noise Level (Big Endian float)
    uint16_t nyquist_velocity;  // Nyquist Velocity (m/s) (Big Endian)
    uint16_t spare;
};

/**
 * Data Block: Moment ('D')
 * Contains actual radar moment data (REF, VEL, etc.)
 */
struct DataBlock_Moment {
    char type;                  // 'D'
    char name[3];               // "REF", "VEL", "SW ", "ZDR", "PHI", "RHO"
    uint32_t reserved;          // Reserved (often contains block size in some versions)
    uint16_t num_gates;         // Number of Gates (Big Endian)
    uint16_t first_gate;        // Range to first gate (m) (Big Endian)
    uint16_t gate_spacing;      // Gate Spacing (m) (Big Endian)
    uint16_t threshold;         // SNR Threshold (Big Endian)
    uint16_t control_flags;     // Control Flags (Big Endian)
    uint8_t data_word_size;     // Data Word Size (8 or 16 bits) at offset 18
    uint8_t spare;              // Spare byte at offset 19
    float scale;                // Scale (Big Endian float) at offset 20
    float offset;               // Offset (Big Endian float) at offset 24
    // Followed by raw data (uint8_t or uint16_t based on data_word_size)
};

// Size constants for struct validation
namespace {
    constexpr size_t VOLUME_HEADER_SIZE = sizeof(VolumeHeader);
    constexpr size_t MESSAGE_HEADER_SIZE = sizeof(MessageHeader);
    constexpr size_t MESSAGE31_HEADER_MIN_SIZE = 102; // Without variable block_pointers
    constexpr size_t DATABLOCK_HEADER_SIZE = sizeof(DataBlock_Header);
    constexpr size_t DATABLOCK_VOLUME_SIZE = sizeof(DataBlock_Volume);
    constexpr size_t DATABLOCK_ELEVATION_SIZE = sizeof(DataBlock_Elevation);
    constexpr size_t DATABLOCK_RADIAL_SIZE = sizeof(DataBlock_Radial);
    constexpr size_t DATABLOCK_MOMENT_SIZE = sizeof(DataBlock_Moment);
}

} // namespace nexrad

#pragma pack(pop)
