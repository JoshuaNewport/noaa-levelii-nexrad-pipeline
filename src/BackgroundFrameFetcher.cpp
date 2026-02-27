/**
 * BackgroundFrameFetcher.cpp - High-efficiency S3-based discovery
 */

#include "levelii/BackgroundFrameFetcher.h"
#include "levelii/AWSInitializer.h"
#include "levelii/FrameStorageManager.h"
#include "levelii/DecompressionUtils.h"
#include "levelii/RadarFrame.h"
#include "levelii/RadarParser.h"
#include "levelii/ThreadPool.h"
#include <iostream>
#include <chrono>
#include <algorithm>
#include <tuple>
#include <cstring>
#include <fstream>
#include <cstdlib>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/core/utils/DateTime.h>

namespace {
    static const char* NEXRAD_BUCKET = "unidata-nexrad-level2";

    /**
     * ScanGuard - RAII class to track active station scans
     */
    class ScanGuard {
    public:
        ScanGuard(std::string station, std::set<std::string>& active_scans, std::mutex& mutex)
            : station_(std::move(station)), active_scans_(active_scans), mutex_(mutex) {
            std::lock_guard<std::mutex> lock(mutex_);
            active_scans_.insert(station_);
        }
        ~ScanGuard() {
            std::lock_guard<std::mutex> lock(mutex_);
            active_scans_.erase(station_);
        }
    private:
        std::string station_;
        std::set<std::string>& active_scans_;
        std::mutex& mutex_;
    };
}

void BackgroundFrameFetcher::log_info(const std::string& msg) const {
    if (logging_enabled_.load()) {
        std::cout << "ℹ️  " << msg << std::endl;
    }
}

void BackgroundFrameFetcher::log_error(const std::string& msg) const {
    if (logging_enabled_.load()) {
        std::cerr << "❌ " << msg << std::endl;
    }
}

// ============================================================================
// BufferPool Implementation
// ============================================================================

BufferPool::BufferPool(size_t num_buffers, size_t buffer_size) : buffer_size_(buffer_size) {
    for (size_t i = 0; i < num_buffers; ++i) {
        auto buf = std::make_unique<std::vector<uint8_t>>();
        buf->reserve(buffer_size_);
        available_.push(buf.get());
        buffers_.push_back(std::move(buf));
    }
}

std::vector<uint8_t>* BufferPool::acquire() {
    std::unique_lock<std::mutex> lock(mutex_);
    cv_.wait(lock, [this] { return !available_.empty(); });
    auto* buf = available_.front();
    available_.pop();
    return buf;
}

void BufferPool::release(std::vector<uint8_t>* buffer) {
    std::lock_guard<std::mutex> lock(mutex_);
    available_.push(buffer);
    cv_.notify_one();
}

// ============================================================================
// BackgroundFrameFetcher Implementation
// ============================================================================

BackgroundFrameFetcher::BackgroundFrameFetcher(
    std::shared_ptr<FrameStorageManager> storage,
    const FrameFetcherConfig& config,
    const std::string& data_path)
    : storage_(storage), config_(config), data_path_(data_path) {
    load_config_from_disk();
    load_state_from_disk();
    reinitialize_pools();
}

BackgroundFrameFetcher::~BackgroundFrameFetcher() {
    stop();
}

void BackgroundFrameFetcher::start() {
    std::lock_guard<std::mutex> lock(state_mutex_);
    if (is_running_.load()) return;
    
    should_stop_.store(false);
    is_running_.store(true);
    
    discovery_loop_thread_ = std::thread([this]() { this->discovery_loop(); });
    fetch_thread_ = std::thread([this]() { this->fetch_loop(); });
    cleanup_thread_ = std::thread([this]() { this->cleanup_loop(); });
}

void BackgroundFrameFetcher::stop() {
    if (!is_running_.load()) return;
    
    should_stop_.store(true);
    is_running_.store(false);
    
    {
        std::lock_guard<std::mutex> lock(discovery_mutex_);
        discovery_cv_.notify_all();
    }

    if (discovery_loop_thread_.joinable()) {
        discovery_loop_thread_.join();
    }
    
    if (fetch_thread_.joinable()) {
        fetch_thread_.join();
    }
    
    if (cleanup_thread_.joinable()) {
        cleanup_thread_.join();
    }
    
    if (discovery_thread_pool_) discovery_thread_pool_->shutdown();
    if (fetch_thread_pool_) fetch_thread_pool_->shutdown();
}

void BackgroundFrameFetcher::add_monitored_station(const std::string& station) {
    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        config_.monitored_stations.insert(station);
    }
    save_config_to_disk();
}

void BackgroundFrameFetcher::remove_monitored_station(const std::string& station) {
    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        config_.monitored_stations.erase(station);
    }
    save_config_to_disk();
}

void BackgroundFrameFetcher::set_monitored_stations(const std::set<std::string>& stations) {
    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        config_.monitored_stations = stations;
    }
    save_config_to_disk();
}

std::set<std::string> BackgroundFrameFetcher::get_monitored_stations() const {
    std::lock_guard<std::mutex> lock(state_mutex_);
    return config_.monitored_stations;
}

void BackgroundFrameFetcher::reconfigure(const FrameFetcherConfig& new_config) {
    bool pools_changed = false;
    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        if (new_config.fetcher_thread_pool_size != config_.fetcher_thread_pool_size ||
            new_config.discovery_parallelism != config_.discovery_parallelism ||
            new_config.buffer_pool_size != config_.buffer_pool_size ||
            new_config.buffer_size != config_.buffer_size) {
            pools_changed = true;
        }
        config_ = new_config;
    }

    save_config_to_disk();

    if (pools_changed) {
        this->log_info("Configuration changed, reinitializing pools...");
        reinitialize_pools();
    }
}

FrameFetcherConfig BackgroundFrameFetcher::get_config() const {
    std::lock_guard<std::mutex> lock(state_mutex_);
    return config_;
}

void BackgroundFrameFetcher::reinitialize_pools() {
    std::shared_ptr<ThreadPool> old_fetch_pool;
    std::shared_ptr<ThreadPool> old_disc_pool;
    
    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        old_fetch_pool = fetch_thread_pool_;
        old_disc_pool = discovery_thread_pool_;
        
        fetch_thread_pool_ = std::make_shared<ThreadPool>(config_.fetcher_thread_pool_size);
        
        int disc_threads = config_.discovery_parallelism;
        const char* disc_env = std::getenv("NEXRAD_DISCOVERY_THREADS");
        if (disc_env) {
            try {
                disc_threads = std::stoi(disc_env);
                this->log_info("Overriding discovery_parallelism with " + std::to_string(disc_threads) + " from NEXRAD_DISCOVERY_THREADS");
            } catch (...) {}
        }
        
        discovery_thread_pool_ = std::make_shared<ThreadPool>(disc_threads);
        buffer_pool_ = std::make_shared<BufferPool>(config_.buffer_pool_size, config_.buffer_size);
        
        this->log_info("Initialized pools: " + std::to_string(config_.fetcher_thread_pool_size) + 
                 " fetch threads, " + std::to_string(disc_threads) + " discovery threads");
    }

    if (old_fetch_pool) old_fetch_pool->shutdown();
    if (old_disc_pool) old_disc_pool->shutdown();
}

void BackgroundFrameFetcher::discovery_loop() {
    this->log_info("High-efficiency S3 discovery loop started");

    while (!should_stop_.load()) {
        auto stations = get_monitored_stations();
        
        // Handle "ALL" stations mode
        if (stations.count("ALL")) {
            auto s3_client = AWSInitializer::instance().get_s3_client();
            if (s3_client) {
                auto now = std::chrono::system_clock::now();
                std::time_t t_now = std::chrono::system_clock::to_time_t(now);
                std::tm utc_tm;
                gmtime_r(&t_now, &utc_tm);

                char day_prefix[32];
                std::snprintf(day_prefix, sizeof(day_prefix), "%04d/%02d/%02d/",
                              utc_tm.tm_year + 1900, utc_tm.tm_mon + 1, utc_tm.tm_mday);

                Aws::S3::Model::ListObjectsV2Request list_req;
                list_req.WithBucket(NEXRAD_BUCKET).WithPrefix(day_prefix).WithDelimiter("/");

                auto list_outcome = s3_client->ListObjectsV2(list_req);
                if (list_outcome.IsSuccess()) {
                    for (const auto& prefix : list_outcome.GetResult().GetCommonPrefixes()) {
                        std::string p = prefix.GetPrefix();
                        // Prefix is "YYYY/MM/DD/STATION/"
                        size_t last_slash = p.find_last_of('/', p.size() - 2);
                        if (last_slash != std::string::npos) {
                            std::string station = p.substr(last_slash + 1, p.size() - last_slash - 2);
                            stations.insert(station);
                        }
                    }
                    stations.erase("ALL"); // Don't try to scan "ALL" as a station
                }
            }
        }
        
        if (!stations.empty()) {
            std::shared_ptr<ThreadPool> disc_pool;
            {
                std::lock_guard<std::mutex> lock(state_mutex_);
                disc_pool = discovery_thread_pool_;
            }

            if (disc_pool) {
                for (const auto& station : stations) {
                    if (should_stop_.load()) break;
                    
                    // Only enqueue if not already scanning
                    {
                        std::lock_guard<std::mutex> lock(active_scans_mutex_);
                        if (active_scans_.count(station)) continue;
                    }
                    
                    disc_pool->enqueue([this, station]() {
                        this->fetch_frame_for_station(station);
                    });
                }
            }
        }
        
        // Save state after each discovery cycle to persist last_processed_keys
        save_state_to_disk();
        
        int interval = 30;
        {
            std::lock_guard<std::mutex> lock(state_mutex_);
            interval = config_.scan_interval_seconds;
        }
        
        // Sleep in increments to respond to shutdown quickly
        for (int i = 0; i < interval * 10 && !should_stop_.load(); ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }
    
    this->log_info("Discovery loop stopped");
}

void BackgroundFrameFetcher::fetch_loop() {
    this->log_info("Fetch loop started");
    while (!should_stop_.load()) {
        DiscoveryBatch batch;
        {
            std::unique_lock<std::mutex> lock(discovery_mutex_);
            discovery_cv_.wait_for(lock, std::chrono::seconds(1), [this] { 
                return !discovery_queue_.empty() || should_stop_.load(); 
            });

            if (should_stop_.load()) break;
            if (discovery_queue_.empty()) continue;

            batch = std::move(discovery_queue_.front());
            discovery_queue_.pop();
        }

        std::shared_ptr<ThreadPool> pool;
        std::shared_ptr<BufferPool> buffer_pool;
        FrameFetcherConfig config;
        {
            std::lock_guard<std::mutex> lock(state_mutex_);
            pool = fetch_thread_pool_;
            buffer_pool = buffer_pool_;
            config = config_;
        }

        if (pool) {
            pool->enqueue([this, batch = std::move(batch), config, buffer_pool]() {
                std::string station = batch.station;
                try {
                    process_discovery_batch(batch, config, buffer_pool);
                } catch (const std::exception& e) {
                    this->log_error("Error processing batch for " + station + ": " + e.what());
                    frames_failed_.fetch_add(1);
                    
                    std::lock_guard<std::mutex> lock(stats_mutex_);
                    station_stats_[station].frames_failed++;
                    station_stats_[station].last_fetch_timestamp = std::chrono::system_clock::now().time_since_epoch().count();
                }
            });
        }
    }
    this->log_info("Fetch loop stopped");
}

void BackgroundFrameFetcher::cleanup_loop() {
    this->log_info("Cleanup thread started");
    while (!should_stop_.load()) {
        bool enabled = true;
        int interval = 300;
        {
            std::lock_guard<std::mutex> lock(state_mutex_);
            enabled = config_.auto_cleanup_enabled;
            interval = config_.cleanup_interval_seconds;
        }

        try {
            if (enabled && storage_) {
                this->log_info("Running periodic cleanup...");
                storage_->cleanup_old_frames();
            }
        } catch (...) {}

        for (int i = 0; i < interval && !should_stop_.load(); ++i) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
    }
    this->log_info("Cleanup thread stopped");
}

void BackgroundFrameFetcher::process_discovery_batch(const DiscoveryBatch& batch, const FrameFetcherConfig& config, std::shared_ptr<BufferPool> buffer_pool) {
    using namespace Aws::S3::Model;

    auto s3_client = AWSInitializer::instance().get_s3_client();
    if (!s3_client || !buffer_pool) return;

    for (const auto& item : batch.items) {
        if (should_stop_.load()) break;

        GetObjectRequest get_req;
        get_req.WithBucket(item.bucket).WithKey(item.key);

        auto get_outcome = s3_client->GetObject(get_req);
        if (!get_outcome.IsSuccess()) {
            this->log_error("Failed to get object " + item.key + ": " + get_outcome.GetError().GetMessage());
            frames_failed_.fetch_add(1);
            
            {
                std::lock_guard<std::mutex> lock(stats_mutex_);
                station_stats_[item.station].frames_failed++;
                station_stats_[item.station].last_fetch_timestamp = std::chrono::system_clock::now().time_since_epoch().count();
            }
            continue;
        }

        auto result = get_outcome.GetResultWithOwnership();
        auto& stream = result.GetBody();
        
        ScopedBuffer raw_data(buffer_pool);
        if (!raw_data.valid()) continue;
        raw_data->clear();
        
        char temp_buf[65536];
        while (stream.read(temp_buf, sizeof(temp_buf))) {
            raw_data->insert(raw_data->end(), temp_buf, temp_buf + stream.gcount());
        }
        if (stream.gcount() > 0) {
            raw_data->insert(raw_data->end(), temp_buf, temp_buf + stream.gcount());
        }

        if (raw_data->empty()) {
            continue;
        }

        ScopedBuffer decompressed_data(buffer_pool);
        if (!decompressed_data.valid()) continue;
        decompressed_data->clear();

        auto frames = parse_nexrad_level2_multi(*raw_data, item.station, item.timestamp, config.products, decompressed_data.get());
        for (auto& pair : frames) {
            const std::string& product = pair.first;
            auto& frame = pair.second;
            if (should_stop_.load()) break;

                try {
                    if (!frame || frame->available_tilts.empty()) continue;

                    std::vector<float> sorted_tilts = frame->available_tilts;
                    std::sort(sorted_tilts.begin(), sorted_tilts.end());

                    const uint16_t vol_num_rays = 720;
                    const float vol_res_factor = 2.0f;
                    const uint16_t vol_num_gates = frame->ngates;
                    const uint16_t vol_num_tilts = static_cast<uint16_t>(sorted_tilts.size());
                    
                    if (vol_num_gates == 0 || frame->gate_spacing_meters <= 0) continue;
                    
                    // Safety: limit allocation size
                    size_t total_elements = static_cast<size_t>(vol_num_tilts) * vol_num_rays * vol_num_gates;
                    if (total_elements > 200000000) { // 200M elements (~200MB)
                        continue;
                    }

                    // Pool processing buffers
                    ScopedBuffer vol_grid_buf(buffer_pool);
                    if (!vol_grid_buf.valid()) continue;
                    vol_grid_buf->assign(total_elements, 0);
                    std::vector<uint8_t>& vol_grid = *vol_grid_buf;
                    
                    auto params = get_quant_params(product);

                    for (size_t tilt_idx = 0; tilt_idx < sorted_tilts.size(); ++tilt_idx) {
                        float tilt = sorted_tilts[tilt_idx];
                        if (should_stop_.load()) break;

                        std::vector<float> tilt_data;
                        for (const auto& sweep : frame->sweeps) {
                            if (std::abs(sweep.elevation_deg - tilt) < 0.01f) {
                                tilt_data.insert(tilt_data.end(), sweep.bins.begin(), sweep.bins.end());
                            }
                        }
                        
                        if (tilt_data.empty()) continue;

                        uint16_t num_rays = 360;
                        float resolution_factor = 1.0f;
                        auto ray_count_it = frame->sweep_ray_counts.find(RadarFrame::get_tilt_key(tilt));
                        if (ray_count_it != frame->sweep_ray_counts.end() && ray_count_it->second > 400) {
                            num_rays = 720;
                            resolution_factor = 2.0f;
                        }
                        
                        ScopedBuffer grid_2d_buf(buffer_pool);
                        if (!grid_2d_buf.valid()) continue;
                        grid_2d_buf->assign(static_cast<size_t>(num_rays) * vol_num_gates, 0);
                        std::vector<uint8_t>& grid_2d = *grid_2d_buf;
                        
                        for (size_t j = 0; j + 2 < tilt_data.size(); j += 3) {
                            float azimuth = tilt_data[j];
                            float range = tilt_data[j+1];
                            float value = tilt_data[j+2];

                            uint8_t val = quantize_value(value, params.value_min, params.value_max);
                            if (val == 0) continue;

                            int gate_idx = static_cast<int>(std::floor((range - frame->first_gate_meters) / frame->gate_spacing_meters));
                            if (gate_idx < 0 || gate_idx >= static_cast<int>(vol_num_gates)) continue;

                            int ray_idx_2d = static_cast<int>(std::floor(azimuth * resolution_factor + 0.01f)) % num_rays;
                            if (ray_idx_2d < 0) ray_idx_2d += num_rays;
                            size_t idx_2d = static_cast<size_t>(ray_idx_2d) * vol_num_gates + gate_idx;
                            if (idx_2d < grid_2d.size()) {
                                grid_2d[idx_2d] = std::max(grid_2d[idx_2d], val);
                            }

                            int ray_idx_3d = static_cast<int>(std::floor(azimuth * vol_res_factor + 0.01f)) % vol_num_rays;
                            if (ray_idx_3d < 0) ray_idx_3d += vol_num_rays;
                            size_t idx_3d = (static_cast<size_t>(tilt_idx) * vol_num_rays * vol_num_gates) + 
                                            (static_cast<size_t>(ray_idx_3d) * vol_num_gates) + gate_idx;
                            if (idx_3d < vol_grid.size()) {
                                vol_grid[idx_3d] = std::max(vol_grid[idx_3d], val);
                                if (resolution_factor < 1.5f) {
                                    int adjacent_ray = (ray_idx_3d + 1) % vol_num_rays;
                                    size_t adj_idx = (static_cast<size_t>(tilt_idx) * vol_num_rays * vol_num_gates) + 
                                                     (static_cast<size_t>(adjacent_ray) * vol_num_gates) + gate_idx;
                                    if (adj_idx < vol_grid.size()) vol_grid[adj_idx] = std::max(vol_grid[adj_idx], val);
                                }
                            }
                        }

                        ScopedBuffer bitmask_2d_buf(buffer_pool);
                        ScopedBuffer values_2d_buf(buffer_pool);
                        if (!bitmask_2d_buf.valid() || !values_2d_buf.valid()) continue;
                        
                        bitmask_2d_buf->assign((grid_2d.size() + 7) / 8, 0);
                        values_2d_buf->clear();
                        
                        std::vector<uint8_t>& bitmask_2d = *bitmask_2d_buf;
                        std::vector<uint8_t>& values_2d = *values_2d_buf;

                        for (size_t b = 0; b < grid_2d.size(); ++b) {
                            if (grid_2d[b] > 0) {
                                bitmask_2d[b / 8] |= (1 << (7 - (b % 8)));
                                values_2d.push_back(grid_2d[b]);
                            }
                        }

                        if (storage_->save_frame_bitmask(item.station, product, item.timestamp, tilt, num_rays, vol_num_gates, frame->gate_spacing_meters, frame->first_gate_meters, bitmask_2d, values_2d)) {
                            frames_fetched_.fetch_add(1);
                            {
                                std::lock_guard<std::mutex> lock(stats_mutex_);
                                station_stats_[item.station].frames_fetched++;
                                station_stats_[item.station].last_fetch_timestamp = std::chrono::system_clock::now().time_since_epoch().count();
                                station_stats_[item.station].last_frame_timestamp = item.timestamp;
                            }
                        }
                    }

                    ScopedBuffer vol_bitmask_buf(buffer_pool);
                    ScopedBuffer vol_values_buf(buffer_pool);
                    if (vol_bitmask_buf.valid() && vol_values_buf.valid()) {
                        vol_bitmask_buf->assign((vol_grid.size() + 7) / 8, 0);
                        vol_values_buf->clear();
                        
                        std::vector<uint8_t>& vol_bitmask = *vol_bitmask_buf;
                        std::vector<uint8_t>& vol_values = *vol_values_buf;
                        
                        for (size_t b = 0; b < vol_grid.size(); ++b) {
                            if (vol_grid[b] > 0) {
                                vol_bitmask[b / 8] |= (1 << (7 - (b % 8)));
                                vol_values.push_back(vol_grid[b]);
                            }
                        }

                        if (!vol_values.empty()) {
                            storage_->save_volumetric_bitmask(item.station, product, item.timestamp, sorted_tilts, vol_num_rays, vol_num_gates, frame->gate_spacing_meters, frame->first_gate_meters, vol_bitmask, vol_values);
                        }
                    }
                } catch (const std::exception& e) {
                    this->log_error("Exception parsing/processing " + product + " for " + item.station + ": " + e.what());
                } catch (...) {
                    this->log_error("Unknown exception parsing/processing " + product + " for " + item.station);
                }
        }
        last_fetch_timestamp_.store(std::chrono::system_clock::now().time_since_epoch().count());
    }
}

void BackgroundFrameFetcher::fetch_frame_for_station(const std::string& station) {
    using namespace Aws::S3::Model;
    
    // Tracking active scans for 150+ stations efficiency
    ScanGuard guard(station, active_scans_, active_scans_mutex_);
    this->log_info("Starting discovery scan for station: " + station);

    auto s3_client = AWSInitializer::instance().get_s3_client();
    if (!s3_client) return;

    try {
        auto now = std::chrono::system_clock::now();
        std::time_t t_now = std::chrono::system_clock::to_time_t(now);
        std::tm utc_tm;
        gmtime_r(&t_now, &utc_tm);

        char date_prefix[64];
        std::snprintf(date_prefix, sizeof(date_prefix), "%04d/%02d/%02d/%s/",
                      utc_tm.tm_year + 1900, utc_tm.tm_mon + 1, utc_tm.tm_mday, station.c_str());

        std::string last_key;
        {
            std::lock_guard<std::mutex> lock(stats_mutex_);
            last_key = station_stats_[station].last_processed_key;
        }

        ListObjectsV2Request list_req;
        list_req.WithBucket(NEXRAD_BUCKET).WithPrefix(date_prefix);
        if (!last_key.empty()) {
            list_req.WithStartAfter(last_key);
        }

        auto list_outcome = s3_client->ListObjectsV2(list_req);
        if (!list_outcome.IsSuccess()) {
            this->log_error("Failed to list S3 objects for " + station + ": " + list_outcome.GetError().GetMessage());
            return;
        }

        auto objects = list_outcome.GetResult().GetContents();
        if (objects.empty()) return;

        // Sort by key (which is chronological for NEXRAD)
        std::sort(objects.begin(), objects.end(), [](const auto& a, const auto& b) {
            return a.GetKey() < b.GetKey();
        });

        int max_frames = 30;
        bool catchup = true;
        {
            std::lock_guard<std::mutex> lock(state_mutex_);
            max_frames = config_.max_frames_per_station;
            catchup = config_.catchup_enabled;
        }
        
        std::vector<Aws::S3::Model::Object> target_objects;
        if (last_key.empty()) {
            if (catchup) {
                // Take up to max_frames of the LATEST objects if we're catching up
                size_t count = std::min(objects.size(), static_cast<size_t>(max_frames));
                for (size_t i = objects.size() - count; i < objects.size(); ++i) {
                    target_objects.push_back(objects[i]);
                }
            } else {
                // Only take the absolute latest one if no catchup
                target_objects.push_back(objects.back());
            }
        } else {
            // Processing all new items found since last_key
            target_objects = objects;
        }

        DiscoveryBatch batch;
        batch.station = station;

        std::string new_last_key = last_key;
        for (const auto& obj : target_objects) {
            if (should_stop_.load()) break;
            
            std::string key = obj.GetKey();
            
            std::string filename = key.substr(key.find_last_of('/') + 1);
            if (filename.find("_MDM") != std::string::npos || filename.size() < 20) continue;
            
            std::string timestamp = filename.substr(4, 8) + "_" + filename.substr(filename.find('_') + 1, 6);

            // Skip if already stored
            bool all_exist = true;
            FrameFetcherConfig current_config;
            {
                std::lock_guard<std::mutex> lock(state_mutex_);
                current_config = config_;
            }
            for (const auto& prod : current_config.products) {
                if (!storage_->has_timestamp_product(station, prod, timestamp)) {
                    all_exist = false;
                    break;
                }
            }

            if (!all_exist) {
                DiscoveryItem item;
                item.station = station;
                item.key = key;
                item.bucket = NEXRAD_BUCKET;
                item.timestamp = timestamp;
                batch.items.push_back(item);
                
                // If batch gets too large, push it and start a new one to allow interleaving
                if (batch.items.size() >= 5) {
                    std::lock_guard<std::mutex> lock(discovery_mutex_);
                    discovery_queue_.push(std::move(batch));
                    discovery_cv_.notify_one();
                    
                    batch = DiscoveryBatch();
                    batch.station = station;
                }
            }
            new_last_key = key;
        }

        if (!batch.items.empty()) {
            std::lock_guard<std::mutex> lock(discovery_mutex_);
            discovery_queue_.push(std::move(batch));
            discovery_cv_.notify_one();
        }

        {
            std::lock_guard<std::mutex> lock(stats_mutex_);
            station_stats_[station].last_processed_key = new_last_key;
            station_stats_[station].last_scan_timestamp = std::chrono::system_clock::now().time_since_epoch().count();
        }
    } catch (const std::exception& e) {
        this->log_error("Exception fetching " + station + ": " + e.what());
    }
}

json BackgroundFrameFetcher::get_statistics() const {
    json stats = {
        {"is_running", is_running_.load()},
        {"frames_fetched", frames_fetched_.load()},
        {"frames_failed", frames_failed_.load()},
        {"last_fetch_timestamp", last_fetch_timestamp_.load()},
        {"monitored_stations", get_monitored_stations()},
        {"max_frames_per_station", config_.max_frames_per_station},
        {"catchup_enabled", config_.catchup_enabled},
        {"scan_interval", config_.scan_interval_seconds}
    };

    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        if (fetch_thread_pool_) {
            stats["thread_pool"] = {
                {"worker_count", fetch_thread_pool_->worker_count()},
                {"active_threads", fetch_thread_pool_->active_threads()},
                {"pending_tasks", fetch_thread_pool_->pending_tasks()}
            };
        }
        if (discovery_thread_pool_) {
            stats["discovery_pool"] = {
                {"worker_count", discovery_thread_pool_->worker_count()},
                {"active_threads", discovery_thread_pool_->active_threads()},
                {"pending_tasks", discovery_thread_pool_->pending_tasks()}
            };
        }
        
        {
            std::lock_guard<std::mutex> lock(active_scans_mutex_);
            stats["active_discovery_scans"] = {
                {"count", active_scans_.size()},
                {"stations", active_scans_}
            };
        }
    }

    {
        std::lock_guard<std::mutex> lock(stats_mutex_);
        json s_stats = json::object();
        for (const auto& [station, s] : station_stats_) {
            s_stats[station] = {
                {"frames_fetched", s.frames_fetched},
                {"frames_failed", s.frames_failed},
                {"last_fetch_timestamp", s.last_fetch_timestamp},
                {"last_frame_timestamp", s.last_frame_timestamp},
                {"last_scan_timestamp", s.last_scan_timestamp}
            };
        }
        stats["station_stats"] = s_stats;
    }

    if (storage_) {
        stats["total_disk_usage_bytes"] = storage_->get_total_disk_usage();
        stats["frame_count"] = storage_->get_frame_count();
    }
    return stats;
}

void BackgroundFrameFetcher::load_config_from_disk() {
    std::string path = data_path_ + "/config.json";
    std::ifstream f(path);
    if (!f.is_open()) return;

    try {
        json data;
        f >> data;
        std::lock_guard<std::mutex> lock(state_mutex_);
        if (data.contains("monitored_stations")) config_.monitored_stations = data["monitored_stations"].get<std::set<std::string>>();
        if (data.contains("scan_interval_seconds")) config_.scan_interval_seconds = data["scan_interval_seconds"];
        if (data.contains("max_frames_per_station")) config_.max_frames_per_station = data["max_frames_per_station"];
        if (data.contains("catchup_enabled")) config_.catchup_enabled = data["catchup_enabled"];
        if (data.contains("fetcher_thread_pool_size")) config_.fetcher_thread_pool_size = data["fetcher_thread_pool_size"];
        if (data.contains("discovery_parallelism")) config_.discovery_parallelism = data["discovery_parallelism"];
        if (data.contains("buffer_pool_size")) config_.buffer_pool_size = data["buffer_pool_size"];
        if (data.contains("buffer_size")) config_.buffer_size = data["buffer_size"];
        if (data.contains("products")) config_.products = data["products"].get<std::vector<std::string>>();
        this->log_info("Loaded configuration from " + path);
    } catch (...) {}
}

void BackgroundFrameFetcher::save_config_to_disk() const {
    std::string path = data_path_ + "/config.json";
    std::system(("mkdir -p " + data_path_).c_str());
    std::ofstream f(path);
    if (!f.is_open()) return;
    json data;
    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        data["monitored_stations"] = config_.monitored_stations;
        data["scan_interval_seconds"] = config_.scan_interval_seconds;
        data["max_frames_per_station"] = config_.max_frames_per_station;
        data["catchup_enabled"] = config_.catchup_enabled;
        data["fetcher_thread_pool_size"] = config_.fetcher_thread_pool_size;
        data["discovery_parallelism"] = config_.discovery_parallelism;
        data["buffer_pool_size"] = config_.buffer_pool_size;
        data["buffer_size"] = config_.buffer_size;
        data["products"] = config_.products;
    }
    f << data.dump(4);
}

void BackgroundFrameFetcher::load_state_from_disk() {
    std::string path = data_path_ + "/state.json";
    std::ifstream f(path);
    if (!f.is_open()) return;

    try {
        json data;
        f >> data;
        std::lock_guard<std::mutex> lock(stats_mutex_);
        if (data.contains("station_stats")) {
            for (auto it = data["station_stats"].begin(); it != data["station_stats"].end(); ++it) {
                const auto& station = it.key();
                const auto& s_data = it.value();
                if (s_data.contains("last_processed_key")) {
                    station_stats_[station].last_processed_key = s_data["last_processed_key"];
                }
                if (s_data.contains("frames_fetched")) station_stats_[station].frames_fetched = s_data["frames_fetched"];
                if (s_data.contains("frames_failed")) station_stats_[station].frames_failed = s_data["frames_failed"];
                if (s_data.contains("last_fetch_timestamp")) station_stats_[station].last_fetch_timestamp = s_data["last_fetch_timestamp"];
                if (s_data.contains("last_frame_timestamp")) station_stats_[station].last_frame_timestamp = s_data["last_frame_timestamp"];
            }
        }
        this->log_info("Loaded state from " + path);
    } catch (...) {}
}

void BackgroundFrameFetcher::save_state_to_disk() const {
    std::string path = data_path_ + "/state.json";
    std::system(("mkdir -p " + data_path_).c_str());
    std::ofstream f(path);
    if (!f.is_open()) return;

    json data;
    {
        std::lock_guard<std::mutex> lock(stats_mutex_);
        json s_stats = json::object();
        for (const auto& [station, s] : station_stats_) {
            s_stats[station] = {
                {"last_processed_key", s.last_processed_key},
                {"frames_fetched", s.frames_fetched},
                {"frames_failed", s.frames_failed},
                {"last_fetch_timestamp", s.last_fetch_timestamp},
                {"last_frame_timestamp", s.last_frame_timestamp}
            };
        }
        data["station_stats"] = s_stats;
    }
    f << data.dump(4);
}
