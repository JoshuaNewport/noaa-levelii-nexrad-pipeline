/**
 * BackgroundFrameFetcher.h - Automatic background frame fetching and storage
 */

#pragma once

#include <string>
#include <vector>
#include <set>
#include <thread>
#include <mutex>
#include <atomic>
#include <functional>
#include <memory>
#include <future>
#include <queue>
#include <nlohmann/json.hpp>
#include "levelii/RadarFrame.h"
#include "levelii/ThreadPool.h"

// ✅ AWS SDK includes
#include <aws/s3/S3Client.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <condition_variable>

using json = nlohmann::json;

class FrameStorageManager;

/**
 * DiscoveryItem - Metadata for a discovered NEXRAD frame to be processed
 */
struct DiscoveryItem {
    std::string station;
    std::string key;
    std::string bucket;
    std::string timestamp;
};

/**
 * DiscoveryBatch - A group of discovery items, usually for the same station,
 * to be processed sequentially on a single thread.
 */
struct DiscoveryBatch {
    std::string station;
    std::vector<DiscoveryItem> items;
};

/**
 * StationStats - Statistics for a specific radar station
 */
struct StationStats {
    uint64_t frames_fetched = 0;
    uint64_t frames_failed = 0;
    uint64_t last_fetch_timestamp = 0;
    std::string last_frame_timestamp;
    
    // Optimization: avoid re-scanning old objects
    std::string last_processed_key;
    uint64_t last_scan_timestamp = 0;
};

class BufferPool {
public:
    explicit BufferPool(size_t num_buffers, size_t buffer_size);
    std::vector<uint8_t>* acquire();
    void release(std::vector<uint8_t>* buffer);
    void shutdown();
    bool is_shutdown() const { return stop_; }
    size_t total_buffers() const { return buffers_.size(); }
    size_t available_buffers() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return available_.size();
    }
    size_t buffer_size() const { return buffer_size_; }

private:
    size_t buffer_size_;
    std::vector<std::unique_ptr<std::vector<uint8_t>>> buffers_;
    std::queue<std::vector<uint8_t>*> available_;
    mutable std::mutex mutex_;
    std::condition_variable cv_;
    bool stop_{false};
};

class ScopedBuffer {
public:
    explicit ScopedBuffer(std::shared_ptr<BufferPool> pool) 
        : pool_(std::move(pool)), buffer_(pool_ ? pool_->acquire() : nullptr) {}
    
    ~ScopedBuffer() {
        release_buffer();
    }
    
    void reset() {
        release_buffer();
    }
    
    ScopedBuffer(const ScopedBuffer&) = delete;
    ScopedBuffer& operator=(const ScopedBuffer&) = delete;
    
    ScopedBuffer(ScopedBuffer&& other) noexcept 
        : pool_(std::move(other.pool_)), buffer_(other.buffer_) {
        other.buffer_ = nullptr;
    }
    
    ScopedBuffer& operator=(ScopedBuffer&& other) noexcept {
        if (this != &other) {
            release_buffer();
            pool_ = std::move(other.pool_);
            buffer_ = other.buffer_;
            other.buffer_ = nullptr;
        }
        return *this;
    }
    
    std::vector<uint8_t>& operator*() { return *buffer_; }
    const std::vector<uint8_t>& operator*() const { return *buffer_; }
    std::vector<uint8_t>* operator->() { return buffer_; }
    const std::vector<uint8_t>* operator->() const { return buffer_; }
    std::vector<uint8_t>* get() { return buffer_; }
    bool valid() const { return buffer_ != nullptr; }

private:
    std::shared_ptr<BufferPool> pool_;
    std::vector<uint8_t>* buffer_;
    
    void release_buffer() {
        if (pool_ && buffer_) {
            pool_->release(buffer_);
            buffer_ = nullptr;
        }
    }
};

struct FrameFetcherConfig {
    std::set<std::string> monitored_stations;
    std::vector<std::string> products = {
        "reflectivity",
        "velocity",
        "correlation_coefficient"
    };

    int scan_interval_seconds = 30;
    int max_frames_per_station = 30;
    int cleanup_interval_seconds = 300;
    bool auto_cleanup_enabled = true;
    bool catchup_enabled = true;
    bool generate_3d = false;

    int fetcher_thread_pool_size = 16;
    int buffer_pool_size = 128;
    size_t buffer_size = 64 * 1024 * 1024;
    int max_task_queue_size = 2000;
    
    int discovery_parallelism = 20;
    int max_discovery_queue_size = 500;
    
    int parallel_downloads = 8;
    int pipeline_depth = 32;
};

class BackgroundFrameFetcher {
public:
    explicit BackgroundFrameFetcher(
        std::shared_ptr<FrameStorageManager> storage,
        const FrameFetcherConfig& config,
        const std::string& data_path = "./data/levelii"
    );

    ~BackgroundFrameFetcher();

    void start();
    void stop();
    bool is_running() const { return is_running_; }

    void add_monitored_station(const std::string& station);
    void remove_monitored_station(const std::string& station);
    void set_monitored_stations(const std::set<std::string>& stations);
    std::set<std::string> get_monitored_stations() const;
    void set_logging_enabled(bool enabled) { logging_enabled_.store(enabled); }

    void reconfigure(const FrameFetcherConfig& new_config);
    FrameFetcherConfig get_config() const;
    json get_statistics() const;

private:
    std::shared_ptr<FrameStorageManager> storage_;
    FrameFetcherConfig config_;
    std::string data_path_;
    std::shared_ptr<ThreadPool> fetch_thread_pool_;
    std::shared_ptr<ThreadPool> discovery_thread_pool_;
    std::shared_ptr<ThreadPool> parse_thread_pool_;
    std::shared_ptr<BufferPool> buffer_pool_;

    std::thread discovery_loop_thread_;
    std::thread fetch_thread_;
    std::thread cleanup_thread_;
    std::atomic<bool> is_running_{false};
    std::atomic<bool> should_stop_{false};
    std::atomic<bool> logging_enabled_{false};

    mutable std::mutex state_mutex_;

    std::atomic<uint64_t> frames_fetched_{0};
    std::atomic<uint64_t> frames_failed_{0};
    std::atomic<uint64_t> last_fetch_timestamp_{0};
    
    std::queue<DiscoveryBatch> discovery_queue_;
    mutable std::mutex discovery_mutex_;
    std::condition_variable discovery_cv_;
    std::condition_variable discovery_full_cv_;
    

    
    std::map<std::string, StationStats> station_stats_;
    mutable std::mutex stats_mutex_;

    std::set<std::string> active_scans_;
    mutable std::mutex active_scans_mutex_;

    void discovery_loop();
    void fetch_loop();
    void process_loop();
    void cleanup_loop();

    void reinitialize_pools();

    void load_config_from_disk();
    void save_config_to_disk() const;
    void load_state_from_disk();
    void save_state_to_disk() const;

    void fetch_frame_for_station(const std::string& station);
    void process_discovery_batch(const DiscoveryBatch& batch, const FrameFetcherConfig& config, std::shared_ptr<BufferPool> buffer_pool);
    void process_single_frame(const DiscoveryItem& item, const FrameFetcherConfig& config, std::shared_ptr<BufferPool> buffer_pool);

    void log_info(const std::string& msg) const;
    void log_error(const std::string& msg) const;
};
