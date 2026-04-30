/**
 * FrameStorageManager.cpp - Implementation
 */

#include "levelii/FrameStorageManager.h"
#include "levelii/ZlibUtils.h"
#include <iostream>
#include <fstream>
#include <sstream>
#include <iomanip>
#include <algorithm>
#include <unordered_map>
#include <cstring>
#include <mutex>

namespace {
    constexpr bool VERBOSE_LOGGING = false;

    void log_info(const std::string& msg) {
        if (VERBOSE_LOGGING) std::cout << "ℹ️  " << msg << std::endl;
    }

    void log_error(const std::string& msg) {
        std::cerr << "❌ " << msg << std::endl;
    }
}

FrameStorageManager::FrameStorageManager(const std::string& base_path)
    : base_path_(base_path) {
    ensure_directory_exists(base_path_);
    
    // Initialize SQLite database
    std::string db_path = base_path_ + "/index.db";
    db_ = std::make_unique<levelii::SQLiteDatabase>(db_path);

    // Initial scan to populate statistics
    size_t usage = 0;
    int count = 0;
    if (fs::exists(base_path_)) {
        for (const auto& entry : fs::recursive_directory_iterator(base_path_)) {
            if (entry.is_regular_file()) {
                usage += entry.file_size();
                if (entry.path().extension() == ".RDA") {
                    count++;
                }
            }
        }
    }
    total_disk_usage_.store(usage);
    total_frame_count_.store(count);
    
    async_storage_stop_.store(false);
    async_storage_running_.store(true);
    storage_thread_ = std::thread([this]() { this->async_storage_loop(); });
}

FrameStorageManager::~FrameStorageManager() {
    shutdown_async_storage();
}

void FrameStorageManager::enqueue_async_write(AsyncWriteTask&& task) {
    {
        std::unique_lock<std::mutex> lock(write_queue_mutex_);
        write_queue_full_cv_.wait(lock, [this]() {
            return write_queue_.size() < MAX_WRITE_QUEUE_SIZE || async_storage_stop_.load();
        });
        
        if (async_storage_stop_.load()) return;
        
        write_queue_.push(std::move(task));
    }
    write_queue_cv_.notify_one();
}

void FrameStorageManager::shutdown_async_storage() {
    if (!async_storage_running_.load()) return;
    
    {
        std::lock_guard<std::mutex> lock(write_queue_mutex_);
        async_storage_stop_.store(true);
        while (!write_queue_.empty()) {
            write_queue_.pop();
        }
    }
    write_queue_cv_.notify_all();
    write_queue_full_cv_.notify_all();
    
    if (storage_thread_.joinable()) {
        storage_thread_.join();
    }
    
    async_storage_running_.store(false);
}

void FrameStorageManager::async_storage_loop() {
    while (true) {
        AsyncWriteTask task;
        {
            std::unique_lock<std::mutex> lock(write_queue_mutex_);
            write_queue_cv_.wait(lock, [this]() {
                return async_storage_stop_.load() || !write_queue_.empty();
            });
            
            if (async_storage_stop_.load() && write_queue_.empty()) {
                break;
            }
            
            if (!write_queue_.empty()) {
                task = std::move(write_queue_.front());
                write_queue_.pop();
                write_queue_full_cv_.notify_one();
            } else {
                continue;
            }
        }
        
        process_write_task(task);
    }
}

void FrameStorageManager::process_write_task(const AsyncWriteTask& task) {
    try {
        switch (task.type) {
            case AsyncWriteTask::BITMASK:
                save_frame_bitmask(task.station, task.product, task.timestamp, task.tilt,
                                  task.num_rays, task.num_gates, task.gate_spacing, task.first_gate,
                                  task.bitmask, task.values, task.dualpol_meta);
                break;
            case AsyncWriteTask::VOLUMETRIC_BITMASK:
                save_volumetric_bitmask(task.station, task.product, task.timestamp,
                                       task.tilts, task.num_rays, task.num_gates,
                                       task.gate_spacing, task.first_gate,
                                       task.bitmask, task.values, task.dualpol_meta);
                break;
        }
    } catch (const std::exception& e) {
        log_error("Error processing async write task: " + std::string(e.what()));
    }
}

bool FrameStorageManager::ensure_directory_exists(const std::string& path) const {
    try {
        if (!fs::exists(path)) {
            fs::create_directories(path);
        }
        return fs::is_directory(path);
    } catch (const std::exception& e) {
        log_error("Failed to create directory " + path + ": " + e.what());
        return false;
    }
}

std::string FrameStorageManager::format_filename(const std::string& timestamp, float tilt) const {
    std::ostringstream oss;
    oss << std::fixed << std::setprecision(1) << tilt << ".RDA";
    return oss.str();
}

std::string FrameStorageManager::get_frame_path(const std::string& station, const std::string& product, const std::string& timestamp, float tilt) const {
    std::ostringstream oss;
    oss << base_path_ << "/" << station << "/" << product << "/" << timestamp << "/" << format_filename(timestamp, tilt);
    return oss.str();
}

bool FrameStorageManager::save_frame_bitmask(const std::string& station, const std::string& product, const std::string& timestamp, float tilt, uint16_t num_rays, uint16_t num_gates, float gate_spacing, float first_gate, const std::vector<uint8_t>& bitmask, const std::vector<uint8_t>& values, const RadarFrame::DualPolMetadata& dualpol_meta, bool auto_update_index) {
    std::string dir = base_path_ + "/" + station + "/" + product + "/" + timestamp;
    if (!ensure_directory_exists(dir)) return false;
    
    json metadata = {
        {"s", station}, {"p", product}, {"t", timestamp}, {"e", tilt},
        {"f", "b"}, {"r", num_rays}, {"g", num_gates}, {"gs", gate_spacing},
        {"fg", first_gate}, {"v", values.size()}
    };

    if (dualpol_meta.sys_diff_refl != 0.0f || dualpol_meta.sys_diff_phase != 0.0f) {
        metadata["dualpol"] = {
            {"sys_diff_refl", dualpol_meta.sys_diff_refl},
            {"sys_diff_phase", dualpol_meta.sys_diff_phase}
        };
    }
    
    std::string metadata_str = metadata.dump();
    uint32_t metadata_size = metadata_str.size();
    
    std::vector<uint8_t> uncompressed;
    uncompressed.reserve(4 + metadata_size + bitmask.size() + values.size());
    uncompressed.insert(uncompressed.end(), reinterpret_cast<uint8_t*>(&metadata_size), reinterpret_cast<uint8_t*>(&metadata_size) + 4);
    uncompressed.insert(uncompressed.end(), metadata_str.begin(), metadata_str.end());
    uncompressed.insert(uncompressed.end(), bitmask.begin(), bitmask.end());
    uncompressed.insert(uncompressed.end(), values.begin(), values.end());
    
    auto compressed = ZlibUtils::gzip_compress(uncompressed.data(), uncompressed.size());
    if (compressed.empty()) return false;
    
    std::ostringstream oss;
    oss << std::fixed << std::setprecision(1) << tilt << ".RDA";
    std::string file_path = dir + "/" + oss.str();
    
    bool existed = fs::exists(file_path);
    size_t old_size = existed ? fs::file_size(file_path) : 0;
    
    std::ofstream file(file_path, std::ios::binary);
    if (!file.is_open()) return false;
    file.write(reinterpret_cast<const char*>(compressed.data()), compressed.size());
    file.close();
    
    {
        std::lock_guard<std::mutex> lock(stats_mutex_);
        if (!existed) {
            total_frame_count_++;
        }
        total_disk_usage_ += (compressed.size() - old_size);
    }
    
    if (auto_update_index) {
        db_->insert_frame("levelii_frames", station, 0, product, timestamp, oss.str());
    }
    return true;
}

bool FrameStorageManager::load_frame_bitmask(const std::string& station, const std::string& product, const std::string& timestamp, float tilt, CompressedFrameData& out_data) const {
    std::ostringstream oss;
    oss << std::fixed << std::setprecision(1) << tilt << ".RDA";
    std::string file_path = base_path_ + "/" + station + "/" + product + "/" + timestamp + "/" + oss.str();
    
    if (!fs::exists(file_path)) return false;
    
    std::ifstream file(file_path, std::ios::binary);
    if (!file) return false;
    
    file.seekg(0, std::ios::end);
    size_t compressed_size = file.tellg();
    file.seekg(0, std::ios::beg);
    
    std::vector<uint8_t> compressed(compressed_size);
    file.read(reinterpret_cast<char*>(compressed.data()), compressed_size);
    file.close();
    
    auto decompressed = ZlibUtils::gzip_decompress(compressed.data(), compressed.size());
    if (decompressed.size() < 4) return false;
    
    uint32_t metadata_size;
    std::memcpy(&metadata_size, decompressed.data(), 4);
    if (4 + metadata_size > decompressed.size()) return false;
    
    try {
        std::string metadata_str(reinterpret_cast<const char*>(decompressed.data() + 4), metadata_size);
        out_data.metadata = json::parse(metadata_str);
        out_data.binary_data.assign(decompressed.begin() + 4 + metadata_size, decompressed.end());
    } catch (const std::exception& e) {
        log_error("Failed to parse bitmask metadata in " + file_path + ": " + e.what());
        return false;
    }
    
    return true;
}

bool FrameStorageManager::load_volumetric_bitmask(const std::string& station, const std::string& product, const std::string& timestamp, CompressedFrameData& out_data) const {
    std::string file_path = base_path_ + "/" + station + "/" + product + "/" + timestamp + "/volumetric.RDA";
    
    if (!fs::exists(file_path)) return false;
    
    std::ifstream file(file_path, std::ios::binary);
    if (!file) return false;
    
    file.seekg(0, std::ios::end);
    size_t compressed_size = file.tellg();
    file.seekg(0, std::ios::beg);
    
    std::vector<uint8_t> compressed(compressed_size);
    file.read(reinterpret_cast<char*>(compressed.data()), compressed_size);
    file.close();
    
    auto decompressed = ZlibUtils::gzip_decompress(compressed.data(), compressed.size());
    if (decompressed.size() < 4) return false;
    
    uint32_t metadata_size;
    std::memcpy(&metadata_size, decompressed.data(), 4);
    if (4 + metadata_size > decompressed.size()) return false;
    
    try {
        std::string metadata_str(reinterpret_cast<const char*>(decompressed.data() + 4), metadata_size);
        out_data.metadata = json::parse(metadata_str);
        out_data.binary_data.assign(decompressed.begin() + 4 + metadata_size, decompressed.end());
    } catch (const std::exception& e) {
        log_error("Failed to parse volumetric bitmask metadata in " + file_path + ": " + e.what());
        return false;
    }
    
    return true;
}

bool FrameStorageManager::save_volumetric_bitmask(const std::string& station, const std::string& product, const std::string& timestamp, const std::vector<float>& tilts, uint16_t num_rays, uint16_t num_gates, float gate_spacing, float first_gate, const std::vector<uint8_t>& bitmask, const std::vector<uint8_t>& values, const RadarFrame::DualPolMetadata& dualpol_meta, bool auto_update_index) {
    std::string dir = base_path_ + "/" + station + "/" + product + "/" + timestamp;
    if (!ensure_directory_exists(dir)) return false;
    
    json metadata = {
        {"s", station}, {"p", product}, {"t", timestamp},
        {"f", "b"}, {"tilts", tilts}, {"r", num_rays}, {"g", num_gates},
        {"gs", gate_spacing}, {"fg", first_gate}, {"v", values.size()}
    };

    if (dualpol_meta.sys_diff_refl != 0.0f || dualpol_meta.sys_diff_phase != 0.0f) {
        metadata["dualpol"] = {
            {"sys_diff_refl", dualpol_meta.sys_diff_refl},
            {"sys_diff_phase", dualpol_meta.sys_diff_phase}
        };
    }
    
    std::string metadata_str = metadata.dump();
    uint32_t metadata_size = metadata_str.size();
    
    std::vector<uint8_t> uncompressed;
    uncompressed.reserve(4 + metadata_size + bitmask.size() + values.size());
    uncompressed.insert(uncompressed.end(), reinterpret_cast<uint8_t*>(&metadata_size), reinterpret_cast<uint8_t*>(&metadata_size) + 4);
    uncompressed.insert(uncompressed.end(), metadata_str.begin(), metadata_str.end());
    uncompressed.insert(uncompressed.end(), bitmask.begin(), bitmask.end());
    uncompressed.insert(uncompressed.end(), values.begin(), values.end());
    
    auto compressed = ZlibUtils::gzip_compress(uncompressed.data(), uncompressed.size());
    if (compressed.empty()) return false;
    
    std::string file_path = dir + "/volumetric.RDA";
    
    bool existed = fs::exists(file_path);
    size_t old_size = existed ? fs::file_size(file_path) : 0;
    
    std::ofstream file(file_path, std::ios::binary);
    if (!file.is_open()) return false;
    file.write(reinterpret_cast<const char*>(compressed.data()), compressed.size());
    file.close();
    
    {
        std::lock_guard<std::mutex> lock(stats_mutex_);
        if (!existed) {
            total_frame_count_++;
        }
        total_disk_usage_ += (compressed.size() - old_size);
    }
    
    if (auto_update_index) {
        db_->insert_frame("levelii_frames", station, 0, product, timestamp, "volumetric.RDA");
    }
    return true;
}

void FrameStorageManager::update_index(const std::string& station, const std::string& product) {
    // Scan directory and update SQLite for any missing entries
    std::string product_dir = base_path_ + "/" + station + "/" + product;
    if (!fs::exists(product_dir)) return;

    for (const auto& ts_entry : fs::directory_iterator(product_dir)) {
        if (!ts_entry.is_directory()) continue;
        std::string timestamp = ts_entry.path().filename().string();
        for (const auto& file_entry : fs::directory_iterator(ts_entry)) {
            if (file_entry.is_regular_file() && file_entry.path().extension() == ".RDA") {
                db_->insert_frame("levelii_frames", station, 0, product, timestamp, file_entry.path().filename().string());
            }
        }
    }
}

json FrameStorageManager::get_index(const std::string& station, const std::string& product) const {
    std::string sql = "SELECT timestamp as t, filename as f FROM levelii_frames "
                      "WHERE station = '" + station + "' AND product_name = '" + product + "' "
                      "ORDER BY timestamp DESC;";
    
    json frames_array = db_->query(sql);
    
    // Transform to expected format: { "f": [ { "t": timestamp, "e": tilt }, ... ] }
    // Note: Level II tilt is usually derived from filename or stored separately.
    // In current schema we store filename. We can parse tilt from filename if needed.
    
    json result_frames = json::array();
    for (const auto& f : frames_array) {
        float tilt = 0.0f;
        try {
            std::string filename = f["f"];
            if (filename != "volumetric.RDA") {
                tilt = std::stof(filename.substr(0, filename.find(".RDA")));
            }
        } catch (...) {}
        result_frames.push_back({{"t", f["t"]}, {"e", tilt}});
    }

    json index = {
        {"s", station}, {"p", product}, 
        {"u", std::chrono::system_clock::now().time_since_epoch().count()},
        {"c", result_frames.size()}, {"f", result_frames}
    };
    
    return index;
}

std::vector<FrameStorageManager::FrameMetadata> FrameStorageManager::list_frames(const std::string& station, const std::string& product) const {
    std::string sql = "SELECT * FROM levelii_frames WHERE station = '" + station + "' AND product_name = '" + product + "' ORDER BY timestamp DESC;";
    json rows = db_->query(sql);
    
    std::vector<FrameMetadata> frames;
    for (const auto& row : rows) {
        FrameMetadata meta;
        meta.station = row["station"];
        meta.product = row["product_name"];
        meta.timestamp = row["timestamp"];
        std::string filename = row["filename"];
        try {
            if (filename != "volumetric.RDA") {
                meta.tilt = std::stof(filename.substr(0, filename.find(".RDA")));
            } else {
                meta.tilt = 0.0f;
            }
        } catch (...) { meta.tilt = 0.0f; }
        
        meta.file_path = base_path_ + "/" + meta.station + "/" + meta.product + "/" + meta.timestamp + "/" + filename;
        
        std::error_code ec;
        meta.file_size = fs::exists(meta.file_path, ec) ? fs::file_size(meta.file_path, ec) : 0;
        
        frames.push_back(meta);
    }
    return frames;
}

bool FrameStorageManager::has_timestamp_product(const std::string& station, const std::string& product, const std::string& timestamp) const {
    std::string sql = "SELECT 1 FROM levelii_frames WHERE station = '" + station + 
                      "' AND product_name = '" + product + "' AND timestamp = '" + timestamp + "' LIMIT 1;";
    json results = db_->query(sql);
    return !results.empty();
}

void FrameStorageManager::cleanup_old_frames(int max_frames_per_station) {
    if (!fs::exists(base_path_)) return;
    
    for (const auto& station_entry : fs::directory_iterator(base_path_)) {
        if (!station_entry.is_directory()) continue;
        
        std::unordered_map<std::string, std::vector<std::string>> products;
        for (const auto& prod_entry : fs::directory_iterator(station_entry)) {
            if (!prod_entry.is_directory()) continue;
            for (const auto& ts_entry : fs::directory_iterator(prod_entry)) {
                if (ts_entry.is_directory()) products[prod_entry.path().filename().string()].push_back(ts_entry.path().filename().string());
            }
        }
        
        for (auto& [prod, timestamps] : products) {
            std::sort(timestamps.rbegin(), timestamps.rend());
            if (timestamps.size() > static_cast<size_t>(max_frames_per_station)) {
                for (size_t i = max_frames_per_station; i < timestamps.size(); ++i) {
                    std::string prod_dir = station_entry.path().string() + "/" + prod + "/" + timestamps[i];
                    if (fs::exists(prod_dir)) {
                        size_t removed_usage = 0;
                        int removed_count = 0;
                        for (const auto& entry : fs::recursive_directory_iterator(prod_dir)) {
                            if (entry.is_regular_file()) {
                                removed_usage += entry.file_size();
                                if (entry.path().extension() == ".RDA") {
                                    removed_count++;
                                }
                            }
                        }
                        fs::remove_all(prod_dir);
                        {
                            std::lock_guard<std::mutex> lock(stats_mutex_);
                            total_disk_usage_ -= removed_usage;
                            total_frame_count_ -= removed_count;
                        }
                        
                        // Cleanup empty parent product directory
                        fs::path prod_dir = fs::path(prod_dir).parent_path();
                        if (fs::is_empty(prod_dir)) {
                            fs::remove(prod_dir);
                        }
                    }
                }
            }
            db_->purge_old_records("levelii_frames", station_entry.path().filename().string(), max_frames_per_station);
        }
        
        // Cleanup empty station directory (no timestamp directories left)
        bool has_timestamp_dirs = false;
        for (const auto& entry : fs::directory_iterator(station_entry)) {
            if (entry.is_directory()) {
                has_timestamp_dirs = true;
                break;
            }
        }
        
        if (!has_timestamp_dirs) {
            for (const auto& entry : fs::directory_iterator(station_entry)) {
                if (entry.is_regular_file()) {
                    fs::remove(entry.path());
                }
            }
            if (fs::is_empty(station_entry)) {
                fs::remove(station_entry);
            }
        }
    }
}

size_t FrameStorageManager::get_total_disk_usage() const {
    return total_disk_usage_.load();
}

int FrameStorageManager::get_frame_count() const {
    return total_frame_count_.load();
}
