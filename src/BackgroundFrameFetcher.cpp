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
    cv_.wait(lock, [this] { return !available_.empty() || stop_; });
    if (stop_ && available_.empty()) return nullptr;
    auto* buf = available_.front();
    available_.pop();
    return buf;
}

void BufferPool::release(std::vector<uint8_t>* buffer) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (buffer) {
        if (buffer->capacity() > buffer_size_ * 2) {
            std::vector<uint8_t>().swap(*buffer);
            buffer->reserve(buffer_size_);
        } else {
            buffer->clear();
        }
        if (!stop_) {
            available_.push(buffer);
            cv_.notify_one();
        }
    }
}

void BufferPool::shutdown() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        stop_ = true;
    }
    cv_.notify_all();
}

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
        discovery_full_cv_.notify_all();
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
    
    std::shared_ptr<ThreadPool> disc_pool;
    std::shared_ptr<ThreadPool> fetch_pool;
    std::shared_ptr<BufferPool> buf_pool;

    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        disc_pool = std::move(discovery_thread_pool_);
        fetch_pool = std::move(fetch_thread_pool_);
        buf_pool = std::move(buffer_pool_);
    }

    if (disc_pool) disc_pool->shutdown();
    if (fetch_pool) fetch_pool->shutdown();
    if (buf_pool) buf_pool->shutdown();
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
    int fetch_threads, disc_threads, buffer_pool_size, buffer_size, max_queue_size;
    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        fetch_threads = config_.fetcher_thread_pool_size;
        disc_threads = config_.discovery_parallelism;
        buffer_pool_size = config_.buffer_pool_size;
        buffer_size = config_.buffer_size;
        max_queue_size = config_.max_task_queue_size;
    }

    const char* disc_env = std::getenv("NEXRAD_DISCOVERY_THREADS");
    if (disc_env) {
        try {
            disc_threads = std::stoi(disc_env);
            this->log_info("Overriding discovery_parallelism with " + std::to_string(disc_threads) + " from NEXRAD_DISCOVERY_THREADS");
        } catch (...) {}
    }

    int required_buffers = fetch_threads * 4;
    int actual_buffer_pool_size = std::max(buffer_pool_size, required_buffers);

    if (actual_buffer_pool_size > buffer_pool_size) {
        this->log_info("Increasing buffer_pool_size from " + std::to_string(buffer_pool_size) + 
                 " to " + std::to_string(actual_buffer_pool_size) + " to maintain adequate thread ratio");
    }

    auto new_fetch_pool = std::make_shared<ThreadPool>(fetch_threads, max_queue_size);
    auto new_disc_pool = std::make_shared<ThreadPool>(disc_threads, max_queue_size);
    auto new_buffer_pool = std::make_shared<BufferPool>(actual_buffer_pool_size, buffer_size);

    std::shared_ptr<ThreadPool> old_fetch_pool;
    std::shared_ptr<ThreadPool> old_disc_pool;
    std::shared_ptr<BufferPool> old_buffer_pool;
    
    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        old_fetch_pool = std::move(fetch_thread_pool_);
        old_disc_pool = std::move(discovery_thread_pool_);
        old_buffer_pool = std::move(buffer_pool_);
        
        fetch_thread_pool_ = new_fetch_pool;
        discovery_thread_pool_ = new_disc_pool;
        buffer_pool_ = new_buffer_pool;
        
        this->log_info("Initialized pools: " + std::to_string(fetch_threads) + 
                 " fetch threads, " + std::to_string(disc_threads) + " discovery threads, " +
                 std::to_string(actual_buffer_pool_size) + " buffers");
    }

    if (old_fetch_pool) old_fetch_pool->shutdown();
    if (old_disc_pool) old_disc_pool->shutdown();
    if (old_buffer_pool) old_buffer_pool->shutdown();
}

void BackgroundFrameFetcher::discovery_loop() {
    this->log_info("High-efficiency S3 discovery loop started");

    while (!should_stop_.load()) {
        auto stations = get_monitored_stations();
        
        if (stations.count("ALL")) {
            auto s3_client = AWSInitializer::instance().get_s3_client();
            if (s3_client) {
                auto now = std::chrono::system_clock::now();
                std::time_t t_now = std::chrono::system_clock::to_time_t(now);
                std::time_t t_yesterday = t_now - (24 * 60 * 60);
                
                std::tm utc_tm, yest_tm;
                gmtime_r(&t_now, &utc_tm);
                gmtime_r(&t_yesterday, &yest_tm);

                std::vector<std::string> prefixes;
                char buf[32];
                std::snprintf(buf, sizeof(buf), "%04d/%02d/%02d/",
                              yest_tm.tm_year + 1900, yest_tm.tm_mon + 1, yest_tm.tm_mday);
                prefixes.push_back(buf);
                std::snprintf(buf, sizeof(buf), "%04d/%02d/%02d/",
                              utc_tm.tm_year + 1900, utc_tm.tm_mon + 1, utc_tm.tm_mday);
                prefixes.push_back(buf);

                for (const auto& day_prefix : prefixes) {
                    Aws::S3::Model::ListObjectsV2Request list_req;
                    list_req.WithBucket(NEXRAD_BUCKET).WithPrefix(day_prefix).WithDelimiter("/");

                    auto list_outcome = s3_client->ListObjectsV2(list_req);
                    if (list_outcome.IsSuccess()) {
                        for (const auto& prefix : list_outcome.GetResult().GetCommonPrefixes()) {
                            std::string p = prefix.GetPrefix();
                            size_t last_slash = p.find_last_of('/', p.size() - 2);
                            if (last_slash != std::string::npos) {
                                std::string station = p.substr(last_slash + 1, p.size() - last_slash - 2);
                                stations.insert(station);
                            }
                        }
                    }
                }
                stations.erase("ALL");
            }
        }
        
        if (!stations.empty()) {
            std::shared_ptr<ThreadPool> disc_pool;
            {
                std::lock_guard<std::mutex> lock(state_mutex_);
                disc_pool = discovery_thread_pool_;
            }

            if (disc_pool) {
                int queued = 0;
                for (const auto& station : stations) {
                    if (should_stop_.load() || !disc_pool->is_running()) break;
                    
                    {
                        std::lock_guard<std::mutex> lock(active_scans_mutex_);
                        if (active_scans_.count(station)) continue;
                    }
                    
                    disc_pool->enqueue([this, station]() {
                        this->fetch_frame_for_station(station);
                    });
                    queued++;
                }
                if (queued > 0) {
                    this->log_info("Queued " + std::to_string(queued) + " discovery tasks");
                }
            }
        }
        
        save_state_to_disk();
        
        int interval = 30;
        {
            std::lock_guard<std::mutex> lock(state_mutex_);
            interval = config_.scan_interval_seconds;
        }
        
        for (int i = 0; i < interval * 10 && !should_stop_.load(); ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }
    
    this->log_info("Discovery loop stopped");
}

void BackgroundFrameFetcher::fetch_loop() {
    this->log_info("Fetch loop started");
    int idle_count = 0;
    while (!should_stop_.load()) {
        DiscoveryBatch batch;
        {
            std::unique_lock<std::mutex> lock(discovery_mutex_);
            discovery_cv_.wait_for(lock, std::chrono::seconds(1), [this] { 
                return !discovery_queue_.empty() || should_stop_.load(); 
            });

            if (should_stop_.load()) break;
            if (discovery_queue_.empty()) {
                idle_count++;
                if (idle_count % 10 == 0) {
                    this->log_info("Fetch loop idle, queue size: " + std::to_string(discovery_queue_.size()));
                }
                continue;
            }
            idle_count = 0;

            batch = std::move(discovery_queue_.front());
            discovery_queue_.pop();
            discovery_full_cv_.notify_one();
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
            this->log_info("Enqueuing batch for station: " + batch.station + " with " + std::to_string(batch.items.size()) + " items");
            pool->enqueue([this, batch = std::move(batch), config, buffer_pool]() {
                std::string station = batch.station;
                this->log_info("Processing batch for station: " + station);
                try {
                    process_discovery_batch(batch, config, buffer_pool);
                    this->log_info("Completed batch for station: " + station);
                } catch (const std::exception& e) {
                    this->log_error("Error processing batch for " + station + ": " + e.what());
                    frames_failed_.fetch_add(1);
                    
                    std::lock_guard<std::mutex> lock(stats_mutex_);
                    station_stats_[station].frames_failed++;
                    station_stats_[station].last_fetch_timestamp = std::chrono::system_clock::now().time_since_epoch().count();
                }
            });
        } else {
            this->log_error("Fetch thread pool is null!");
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

void BackgroundFrameFetcher::process_single_frame(const DiscoveryItem& item, const FrameFetcherConfig& config, std::shared_ptr<BufferPool> buffer_pool) {
    using namespace Aws::S3::Model;

    auto s3_client = AWSInitializer::instance().get_s3_client();
    if (!s3_client || !buffer_pool) return;

    GetObjectRequest get_req;
    get_req.WithBucket(item.bucket).WithKey(item.key);

    auto get_outcome = s3_client->GetObject(get_req);
    if (!get_outcome.IsSuccess()) {
        this->log_error("Failed to get object " + item.key + ": " + get_outcome.GetError().GetMessage());
        frames_failed_.fetch_add(1);
        
        std::lock_guard<std::mutex> lock(stats_mutex_);
        station_stats_[item.station].frames_failed++;
        station_stats_[item.station].last_fetch_timestamp = std::chrono::system_clock::now().time_since_epoch().count();
        return;
    }

    auto result = get_outcome.GetResultWithOwnership();
    auto& stream = result.GetBody();
    
    ScopedBuffer raw_data(buffer_pool);
    if (!raw_data.valid()) return;
    raw_data->clear();
    
    char temp_buf[65536];
    while (stream.read(temp_buf, sizeof(temp_buf))) {
        raw_data->insert(raw_data->end(), temp_buf, temp_buf + stream.gcount());
    }
    if (stream.gcount() > 0) {
        raw_data->insert(raw_data->end(), temp_buf, temp_buf + stream.gcount());
    }

    if (raw_data->empty()) {
        return;
    }

    ScopedBuffer decompressed_data(buffer_pool);
    if (!decompressed_data.valid()) return;
    decompressed_data->clear();

    auto frames = parse_nexrad_level2_multi(*raw_data, item.station, item.timestamp, config.products, decompressed_data.get(), config.generate_3d);
    
    raw_data.reset();

    for (auto& pair : frames) {
        if (should_stop_.load()) break;
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
            
            size_t total_elements = static_cast<size_t>(vol_num_tilts) * vol_num_rays * vol_num_gates;
            if (total_elements > 200000000) {
                continue;
            }

            ScopedBuffer vol_grid_buf(buffer_pool);
            if (!vol_grid_buf.valid()) continue;
            vol_grid_buf->assign(total_elements, 0);
            std::vector<uint8_t>& vol_grid = *vol_grid_buf;
            
            auto params = get_quant_params(product);

            for (size_t tilt_idx = 0; tilt_idx < sorted_tilts.size(); ++tilt_idx) {
                if (should_stop_.load()) break;
                float tilt = sorted_tilts[tilt_idx];

                bool has_sweeps = false;
                for (const auto& sweep : frame->sweeps) {
                    if (std::abs(sweep.elevation_deg - tilt) < 0.01f) {
                        has_sweeps = true;
                        break;
                    }
                }
                if (!has_sweeps) continue;

                uint16_t num_rays = 360;
                float resolution_factor = 1.0f;
                if (frame->elevation_ray_counts) {
                    auto ray_count_it = frame->elevation_ray_counts->find(RadarFrame::get_tilt_key(tilt));
                    if (ray_count_it != frame->elevation_ray_counts->end() && ray_count_it->second > 400) {
                        num_rays = 720;
                        resolution_factor = 2.0f;
                    }
                }
                
                ScopedBuffer grid_2d_buf(buffer_pool);
                if (!grid_2d_buf.valid()) continue;
                grid_2d_buf->assign(static_cast<size_t>(num_rays) * vol_num_gates, 0);
                std::vector<uint8_t>& grid_2d = *grid_2d_buf;
                
                for (const auto& sweep : frame->sweeps) {
                    if (should_stop_.load()) break;
                    if (std::abs(sweep.elevation_deg - tilt) < 0.01f) {
                        const auto& bins = sweep.bins;
                        size_t bin_count = bins.size();
                        
                        for (size_t j = 0; j + 2 < bin_count; j += 3) {
                            float azimuth = bins[j];
                            float range = bins[j+1];
                            float value = bins[j+2];

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
                    }
                }

                if (should_stop_.load()) break;

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

                if (should_stop_.load()) break;

                if (storage_->save_frame_bitmask(item.station, product, item.timestamp, tilt, num_rays, vol_num_gates, frame->gate_spacing_meters, frame->first_gate_meters, bitmask_2d, values_2d, frame->dualpol_meta, false)) {
                    frames_fetched_.fetch_add(1);
                    {
                        std::lock_guard<std::mutex> lock(stats_mutex_);
                        station_stats_[item.station].frames_fetched++;
                        station_stats_[item.station].last_fetch_timestamp = std::chrono::system_clock::now().time_since_epoch().count();
                        station_stats_[item.station].last_frame_timestamp = item.timestamp;
                    }
                }
            }

            if (should_stop_.load()) continue;

            ScopedBuffer vol_bitmask_buf(buffer_pool);
            ScopedBuffer vol_values_buf(buffer_pool);
            if (vol_bitmask_buf.valid() && vol_values_buf.valid()) {
                vol_bitmask_buf->assign((vol_grid.size() + 7) / 8, 0);
                vol_values_buf->clear();
                
                std::vector<uint8_t>& vol_bitmask = *vol_bitmask_buf;
                std::vector<uint8_t>& vol_values = *vol_values_buf;
                
                for (size_t b = 0; b < vol_grid.size(); ++b) {
                    if ((b & 0xFFFF) == 0 && should_stop_.load()) break;
                    if (vol_grid[b] > 0) {
                        vol_bitmask[b / 8] |= (1 << (7 - (b % 8)));
                        vol_values.push_back(vol_grid[b]);
                    }
                }

                if (!vol_values.empty() && !should_stop_.load()) {
                    storage_->save_volumetric_bitmask(item.station, product, item.timestamp, sorted_tilts, vol_num_rays, vol_num_gates, frame->gate_spacing_meters, frame->first_gate_meters, vol_bitmask, vol_values, frame->dualpol_meta, false);
                }
            }
            
            frame->clear_data();
            
        } catch (const std::exception& e) {
            this->log_error("Exception parsing/processing " + product + " for " + item.station + ": " + e.what());
        } catch (...) {
            this->log_error("Unknown exception parsing/processing " + product + " for " + item.station);
        }
    }
    
    last_fetch_timestamp_.store(std::chrono::system_clock::now().time_since_epoch().count());
}

void BackgroundFrameFetcher::process_discovery_batch(const DiscoveryBatch& batch, const FrameFetcherConfig& config, std::shared_ptr<BufferPool> buffer_pool) {
    for (const auto& item : batch.items) {
        if (should_stop_.load()) break;
        process_single_frame(item, config, buffer_pool);
    }

    for (const auto& product : config.products) {
        storage_->update_index(batch.station, product);
    }
}

void BackgroundFrameFetcher::fetch_frame_for_station(const std::string& station) {
    using namespace Aws::S3::Model;
    
    ScanGuard guard(station, active_scans_, active_scans_mutex_);
    this->log_info("Starting discovery scan for station: " + station);
    auto scan_start = std::chrono::high_resolution_clock::now();

    auto s3_client = AWSInitializer::instance().get_s3_client();
    if (!s3_client) return;

    try {
        auto now = std::chrono::system_clock::now();
        std::time_t t_now = std::chrono::system_clock::to_time_t(now);
        std::time_t t_yesterday = t_now - (24 * 60 * 60);

        std::tm utc_tm, yest_tm;
        gmtime_r(&t_now, &utc_tm);
        gmtime_r(&t_yesterday, &yest_tm);

        std::vector<std::string> prefixes;
        char buf[64];
        std::snprintf(buf, sizeof(buf), "%04d/%02d/%02d/%s/",
                      yest_tm.tm_year + 1900, yest_tm.tm_mon + 1, yest_tm.tm_mday, station.c_str());
        prefixes.push_back(buf);
        std::snprintf(buf, sizeof(buf), "%04d/%02d/%02d/%s/",
                      utc_tm.tm_year + 1900, utc_tm.tm_mon + 1, utc_tm.tm_mday, station.c_str());
        prefixes.push_back(buf);

        std::string last_key;
        {
            std::lock_guard<std::mutex> lock(stats_mutex_);
            last_key = station_stats_[station].last_processed_key;
        }

        std::vector<Aws::S3::Model::Object> objects;
        for (const auto& date_prefix : prefixes) {
            ListObjectsV2Request list_req;
            list_req.WithBucket(NEXRAD_BUCKET).WithPrefix(date_prefix);
            if (!last_key.empty()) {
                list_req.WithStartAfter(last_key);
            }

            auto list_outcome = s3_client->ListObjectsV2(list_req);
            if (list_outcome.IsSuccess()) {
                auto contents = list_outcome.GetResult().GetContents();
                objects.insert(objects.end(), contents.begin(), contents.end());
            } else {
                this->log_error("Failed to list S3 objects for " + station + " (" + date_prefix + "): " + list_outcome.GetError().GetMessage());
            }
        }

        if (objects.empty()) return;

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
                size_t count = std::min(objects.size(), static_cast<size_t>(max_frames));
                for (size_t i = objects.size() - count; i < objects.size(); ++i) {
                    target_objects.push_back(objects[i]);
                }
            } else {
                target_objects.push_back(objects.back());
            }
        } else {
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
                
                if (batch.items.size() >= 5) {
                    {
                        std::unique_lock<std::mutex> lock(discovery_mutex_);
                        if (config_.max_discovery_queue_size > 0) {
                            discovery_full_cv_.wait(lock, [this]() {
                                return should_stop_.load() || discovery_queue_.size() < config_.max_discovery_queue_size;
                            });
                        }
                        if (should_stop_.load()) break;
                        discovery_queue_.push(std::move(batch));
                    }
                    discovery_cv_.notify_one();
                    
                    batch = DiscoveryBatch();
                    batch.station = station;
                }
            }
            new_last_key = key;
        }

        if (!batch.items.empty() && !should_stop_.load()) {
            {
                std::unique_lock<std::mutex> lock(discovery_mutex_);
                if (config_.max_discovery_queue_size > 0) {
                    discovery_full_cv_.wait(lock, [this]() {
                        return should_stop_.load() || discovery_queue_.size() < config_.max_discovery_queue_size;
                    });
                }
                if (!should_stop_.load()) {
                    this->log_info("Pushing batch to discovery queue: " + batch.station + " with " + std::to_string(batch.items.size()) + " items, queue size: " + std::to_string(discovery_queue_.size() + 1));
                    discovery_queue_.push(std::move(batch));
                }
            }
            discovery_cv_.notify_one();
        }

        {
            std::lock_guard<std::mutex> lock(stats_mutex_);
            station_stats_[station].last_processed_key = new_last_key;
            station_stats_[station].last_scan_timestamp = std::chrono::system_clock::now().time_since_epoch().count();
        }
        
        auto scan_end = std::chrono::high_resolution_clock::now();
        auto scan_duration = std::chrono::duration_cast<std::chrono::milliseconds>(scan_end - scan_start).count();
        this->log_info("Discovery scan for " + station + " complete in " + std::to_string(scan_duration) + "ms");
    } catch (const std::exception& e) {
        this->log_error("Exception fetching " + station + ": " + e.what());
    }
}

json BackgroundFrameFetcher::get_statistics() const {
    json stats;
    
    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        stats = {
            {"is_running", is_running_.load()},
            {"frames_fetched", frames_fetched_.load()},
            {"frames_failed", frames_failed_.load()},
            {"last_fetch_timestamp", last_fetch_timestamp_.load()},
            {"monitored_stations", config_.monitored_stations},
            {"max_frames_per_station", config_.max_frames_per_station},
            {"catchup_enabled", config_.catchup_enabled},
            {"scan_interval", config_.scan_interval_seconds}
        };

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
        
        if (buffer_pool_) {
            stats["buffer_pool"] = {
                {"total_buffers", buffer_pool_->total_buffers()},
                {"available_buffers", buffer_pool_->available_buffers()},
                {"buffer_size", buffer_pool_->buffer_size()}
            };
        }

        {
            std::lock_guard<std::mutex> lock(active_scans_mutex_);
            stats["active_discovery_scans"] = {
                {"count", active_scans_.size()},
                {"stations", active_scans_}
            };
        }

        {
            std::lock_guard<std::mutex> lock(discovery_mutex_);
            stats["discovery_queue_size"] = discovery_queue_.size();
        }
    }

    {
        std::lock_guard<std::mutex> lock(stats_mutex_);
        json s_stats = json::object();
        
        std::vector<std::pair<std::string, StationStats>> sorted_stats(station_stats_.begin(), station_stats_.end());
        std::sort(sorted_stats.begin(), sorted_stats.end(), [](const auto& a, const auto& b) {
            return a.second.last_fetch_timestamp > b.second.last_fetch_timestamp;
        });
        
        size_t count = 0;
        for (const auto& [station, s] : sorted_stats) {
            if (count++ >= 50) break;
            s_stats[station] = {
                {"frames_fetched", s.frames_fetched},
                {"frames_failed", s.frames_failed},
                {"last_fetch_timestamp", s.last_fetch_timestamp},
                {"last_frame_timestamp", s.last_frame_timestamp},
                {"last_scan_timestamp", s.last_scan_timestamp}
            };
        }
        stats["station_stats"] = s_stats;
        stats["total_stations_tracked"] = station_stats_.size();
    }

    if (storage_) {
        stats["total_disk_usage_bytes"] = storage_->get_total_disk_usage();
        stats["frame_count"] = storage_->get_frame_count();
        stats["storage_pending_tasks"] = storage_->num_pending_tasks();
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
