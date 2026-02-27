/**
 * FrameStorageManager.h - Disk-based frame storage with automatic indexing
 * 
 * Manages persistent storage of radar frames with structure:
 * /levelii/STATION/YYYYMMDD_HHMMSS/product/tilt.RDA
 * 
 * Features:
 * - Automatic directory creation
 * - JSON index generation and maintenance
 * - Memory-efficient parsing (parse to disk, clear memory)
 * - Automatic cleanup of old frames
 */

#pragma once

#include <string>
#include <unordered_map>
#include <vector>
#include <nlohmann/json.hpp>
#include <filesystem>
#include <chrono>
#include <shared_mutex>
#include <queue>
#include <thread>
#include <atomic>
#include <condition_variable>

using json = nlohmann::json;
namespace fs = std::filesystem;

struct AsyncWriteTask {
    enum Type {
        BITMASK,
        VOLUMETRIC_BITMASK
    } type;
    
    std::string station;
    std::string product;
    std::string timestamp;
    float tilt;
    std::vector<uint8_t> data;
    std::vector<uint8_t> bitmask;
    std::vector<uint8_t> values;
    std::vector<float> tilts;
    uint16_t num_rays;
    uint16_t num_gates;
    float gate_spacing;
    float first_gate;
};

/**
 * @class FrameStorageManager
 * @brief Manages persistent storage and indexing of processed radar frames.
 * 
 * Data is stored hierarchically on disk and indexed via JSON files for fast lookup.
 * Supports asynchronous write operations to avoid blocking the main processing loop.
 */
class FrameStorageManager {
public:
    struct FrameMetadata {
        std::string station;
        std::string product;
        std::string timestamp;
        float tilt;
        size_t file_size;
        std::chrono::system_clock::time_point stored_time;
        std::string file_path;
    };

    struct CompressedFrameData {
        json metadata;
        std::vector<uint8_t> binary_data;
    };

    /**
     * @brief Construct a new Frame Storage Manager
     * @param base_path Root directory for all stored radar data.
     */
    explicit FrameStorageManager(const std::string& base_path = "./data/levelii");
    
    ~FrameStorageManager();
    
    /**
     * @brief Queue a frame for asynchronous write to disk.
     * @param task Description of the data and metadata to be written.
     */
    void enqueue_async_write(const AsyncWriteTask& task);
    
    /**
     * @brief Stop the async storage thread and wait for pending tasks to complete.
     */
    void shutdown_async_storage();

    /**
     * @brief Save a single frame using bitmask compression.
     */
    bool save_frame_bitmask(
        const std::string& station,
        const std::string& product,
        const std::string& timestamp,
        float tilt,
        uint16_t num_rays,
        uint16_t num_gates,
        float gate_spacing,
        float first_gate,
        const std::vector<uint8_t>& bitmask,
        const std::vector<uint8_t>& values
    );
    
    /**
     * @brief Save a full volumetric dataset using bitmask compression.
     * 
     * @param station 4-letter station ID.
     * @param product Product type (e.g., "reflectivity").
     * @param timestamp Data timestamp.
     * @param tilts list of elevation angles included.
     * @param num_rays Number of rays per sweep.
     * @param num_gates Number of gates per ray.
     * @param gate_spacing Distance between gates in meters.
     * @param first_gate Distance to first gate in meters.
     * @param bitmask Packed bitmask of valid data points.
     * @param values Vector of quantized data values.
     * @return true if saved successfully.
     */
    bool save_volumetric_bitmask(
        const std::string& station,
        const std::string& product,
        const std::string& timestamp,
        const std::vector<float>& tilts,
        uint16_t num_rays,
        uint16_t num_gates,
        float gate_spacing,
        float first_gate,
        const std::vector<uint8_t>& bitmask,
        const std::vector<uint8_t>& values
    );

    /**
     * @brief Load a frame's compressed bitmask data from disk.
     */
    bool load_frame_bitmask(
        const std::string& station,
        const std::string& product,
        const std::string& timestamp,
        float tilt,
        CompressedFrameData& out_data
    ) const;

    /**
     * @brief Load a volumetric dataset's compressed bitmask data from disk.
     */
    bool load_volumetric_bitmask(
        const std::string& station,
        const std::string& product,
        const std::string& timestamp,
        CompressedFrameData& out_data
    ) const;

    // Index management
    void update_index(const std::string& station, const std::string& product);
    json get_index(const std::string& station, const std::string& product) const;
    std::vector<FrameMetadata> list_frames(
        const std::string& station,
        const std::string& product
    ) const;
    
    bool has_timestamp_product(
        const std::string& station,
        const std::string& product,
        const std::string& timestamp
    ) const;
    
    // Cleanup operations
    void cleanup_old_frames(int max_frames_per_station = 30);
    void cleanup_old_frames_by_age(int max_age_minutes = 1440);
    
    // Path utilities
    std::string get_frame_path(
        const std::string& station,
        const std::string& product,
        const std::string& timestamp,
        float tilt
    ) const;
    
    std::string get_index_path(
        const std::string& station,
        const std::string& product
    ) const;
    
    // Statistics
    size_t get_total_disk_usage() const;
    int get_frame_count() const;
    
private:
    std::string base_path_;
    mutable std::shared_mutex index_mutex_;
    mutable std::unordered_map<std::string, json> index_cache_;
    
    // Async storage
    std::queue<AsyncWriteTask> write_queue_;
    mutable std::mutex write_queue_mutex_;
    std::condition_variable write_queue_cv_;
    std::thread storage_thread_;
    std::atomic<bool> async_storage_running_{false};
    std::atomic<bool> async_storage_stop_{false};
    
    void async_storage_loop();
    void process_write_task(const AsyncWriteTask& task);
    
    bool ensure_directory_exists(const std::string& path) const;
    std::string format_filename(
        const std::string& timestamp,
        float tilt
    ) const;
    std::vector<FrameMetadata> scan_directory(
        const std::string& station,
        const std::string& product
    ) const;
};
