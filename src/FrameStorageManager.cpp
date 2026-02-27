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
    
    async_storage_running_.store(true);
    async_storage_stop_.store(false);
    storage_thread_ = std::thread([this]() { this->async_storage_loop(); });
}

FrameStorageManager::~FrameStorageManager() {
    shutdown_async_storage();
}

void FrameStorageManager::enqueue_async_write(const AsyncWriteTask& task) {
    {
        std::lock_guard<std::mutex> lock(write_queue_mutex_);
        write_queue_.push(task);
    }
    write_queue_cv_.notify_one();
}

void FrameStorageManager::shutdown_async_storage() {
    if (!async_storage_running_.load()) return;
    
    {
        std::lock_guard<std::mutex> lock(write_queue_mutex_);
        async_storage_stop_.store(true);
    }
    write_queue_cv_.notify_all();
    
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
                                  task.bitmask, task.values);
                break;
            case AsyncWriteTask::VOLUMETRIC_BITMASK:
                save_volumetric_bitmask(task.station, task.product, task.timestamp,
                                       task.tilts, task.num_rays, task.num_gates,
                                       task.gate_spacing, task.first_gate,
                                       task.bitmask, task.values);
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
    oss << base_path_ << "/" << station << "/" << timestamp << "/" << product << "/" << format_filename(timestamp, tilt);
    return oss.str();
}

std::string FrameStorageManager::get_index_path(const std::string& station, const std::string& product) const {
    std::ostringstream oss;
    oss << base_path_ << "/" << station << "/index_" << product << ".json";
    return oss.str();
}

bool FrameStorageManager::save_frame_bitmask(const std::string& station, const std::string& product, const std::string& timestamp, float tilt, uint16_t num_rays, uint16_t num_gates, float gate_spacing, float first_gate, const std::vector<uint8_t>& bitmask, const std::vector<uint8_t>& values) {
    std::string dir = base_path_ + "/" + station + "/" + timestamp + "/" + product;
    if (!ensure_directory_exists(dir)) return false;
    
    json metadata = {
        {"s", station}, {"p", product}, {"t", timestamp}, {"e", tilt},
        {"f", "b"}, {"r", num_rays}, {"g", num_gates}, {"gs", gate_spacing},
        {"fg", first_gate}, {"v", values.size()}
    };
    
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
    
    std::ofstream file(file_path, std::ios::binary);
    if (!file.is_open()) return false;
    file.write(reinterpret_cast<const char*>(compressed.data()), compressed.size());
    file.close();
    
    update_index(station, product);
    return true;
}

bool FrameStorageManager::load_frame_bitmask(const std::string& station, const std::string& product, const std::string& timestamp, float tilt, CompressedFrameData& out_data) const {
    std::ostringstream oss;
    oss << std::fixed << std::setprecision(1) << tilt << ".RDA";
    std::string file_path = base_path_ + "/" + station + "/" + timestamp + "/" + product + "/" + oss.str();
    
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
    std::string file_path = base_path_ + "/" + station + "/" + timestamp + "/" + product + "/volumetric.RDA";
    
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

bool FrameStorageManager::save_volumetric_bitmask(const std::string& station, const std::string& product, const std::string& timestamp, const std::vector<float>& tilts, uint16_t num_rays, uint16_t num_gates, float gate_spacing, float first_gate, const std::vector<uint8_t>& bitmask, const std::vector<uint8_t>& values) {
    std::string dir = base_path_ + "/" + station + "/" + timestamp + "/" + product;
    if (!ensure_directory_exists(dir)) return false;
    
    json metadata = {
        {"s", station}, {"p", product}, {"t", timestamp},
        {"f", "b"}, {"tilts", tilts}, {"r", num_rays}, {"g", num_gates},
        {"gs", gate_spacing}, {"fg", first_gate}, {"v", values.size()}
    };
    
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
    
    std::ofstream file(file_path, std::ios::binary);
    if (!file.is_open()) return false;
    file.write(reinterpret_cast<const char*>(compressed.data()), compressed.size());
    file.close();
    
    update_index(station, product);
    return true;
}

void FrameStorageManager::update_index(const std::string& station, const std::string& product) {
    try {
        std::unique_lock<std::shared_mutex> lock(index_mutex_);
        auto frames = scan_directory(station, product);
        
        json index = {
            {"s", station}, {"p", product}, 
            {"u", std::chrono::system_clock::now().time_since_epoch().count()},
            {"c", frames.size()}, {"f", json::array()}
        };
        
        for (const auto& frame : frames) {
            index["f"].push_back({{"t", frame.timestamp}, {"e", frame.tilt}});
        }
        
        std::string json_str = index.dump();
        auto compressed = ZlibUtils::gzip_compress(reinterpret_cast<const uint8_t*>(json_str.c_str()), json_str.size());
        
        std::string index_path = get_index_path(station, product);
        ensure_directory_exists(fs::path(index_path).parent_path().string());
        
        std::ofstream file(index_path, std::ios::binary);
        if (file.is_open()) {
            file.write(reinterpret_cast<const char*>(compressed.data()), compressed.size());
            file.close();
        }
        
        index_cache_[station + "/" + product] = index;
    } catch (...) {}
}

json FrameStorageManager::get_index(const std::string& station, const std::string& product) const {
    std::shared_lock<std::shared_mutex> lock(index_mutex_);
    std::string key = station + "/" + product;
    if (index_cache_.count(key)) return index_cache_.at(key);
    
    std::string path = get_index_path(station, product);
    if (fs::exists(path)) {
        std::ifstream file(path, std::ios::binary);
        file.seekg(0, std::ios::end);
        size_t size = file.tellg();
        file.seekg(0, std::ios::beg);
        std::vector<uint8_t> compressed(size);
        file.read(reinterpret_cast<char*>(compressed.data()), size);
        auto decompressed = ZlibUtils::gzip_decompress(compressed.data(), compressed.size());
        return json::parse(std::string(decompressed.begin(), decompressed.end()));
    }
    return json::object();
}

std::vector<FrameStorageManager::FrameMetadata> FrameStorageManager::list_frames(const std::string& station, const std::string& product) const {
    return scan_directory(station, product);
}

std::vector<FrameStorageManager::FrameMetadata> FrameStorageManager::scan_directory(const std::string& station, const std::string& product) const {
    std::vector<FrameMetadata> frames;
    std::string station_dir = base_path_ + "/" + station;
    if (!fs::exists(station_dir)) return frames;
    
    for (const auto& ts_entry : fs::directory_iterator(station_dir)) {
        if (!ts_entry.is_directory()) continue;
        std::string product_dir = ts_entry.path().string() + "/" + product;
        if (!fs::exists(product_dir)) continue;
        
        for (const auto& file_entry : fs::directory_iterator(product_dir)) {
            if (!file_entry.is_regular_file()) continue;
            std::string ext = file_entry.path().extension().string();
            if (ext != ".RDA") continue;
            
            FrameMetadata meta;
            meta.station = station;
            meta.product = product;
            meta.file_path = file_entry.path().string();
            meta.file_size = fs::file_size(file_entry);
            meta.timestamp = ts_entry.path().filename().string();
            try {
                meta.tilt = std::stof(file_entry.path().stem().string());
            } catch (...) { meta.tilt = 0.0f; }
            frames.push_back(meta);
        }
    }
    std::sort(frames.begin(), frames.end(), [](const auto& a, const auto& b) { return a.timestamp > b.timestamp; });
    return frames;
}

void FrameStorageManager::cleanup_old_frames(int max_frames_per_station) {
    if (!fs::exists(base_path_)) return;
    
    for (const auto& station_entry : fs::directory_iterator(base_path_)) {
        if (!station_entry.is_directory()) continue;
        
        std::unordered_map<std::string, std::vector<std::string>> products;
        for (const auto& ts_entry : fs::directory_iterator(station_entry)) {
            if (!ts_entry.is_directory()) continue;
            for (const auto& prod_entry : fs::directory_iterator(ts_entry)) {
                if (prod_entry.is_directory()) products[prod_entry.path().filename().string()].push_back(ts_entry.path().filename().string());
            }
        }
        
        for (auto& [prod, timestamps] : products) {
            std::sort(timestamps.rbegin(), timestamps.rend());
            if (timestamps.size() > static_cast<size_t>(max_frames_per_station)) {
                for (size_t i = max_frames_per_station; i < timestamps.size(); ++i) {
                    fs::remove_all(station_entry.path().string() + "/" + timestamps[i] + "/" + prod);
                }
                update_index(station_entry.path().filename().string(), prod);
            }
        }
    }
}

bool FrameStorageManager::has_timestamp_product(const std::string& station, const std::string& product, const std::string& timestamp) const {
    std::string path = base_path_ + "/" + station + "/" + timestamp + "/" + product;
    return fs::exists(path) && fs::is_directory(path);
}

size_t FrameStorageManager::get_total_disk_usage() const {
    size_t total = 0;
    if (fs::exists(base_path_)) {
        for (const auto& entry : fs::recursive_directory_iterator(base_path_)) {
            if (entry.is_regular_file()) total += entry.file_size();
        }
    }
    return total;
}

int FrameStorageManager::get_frame_count() const {
    int count = 0;
    if (fs::exists(base_path_)) {
        for (const auto& entry : fs::recursive_directory_iterator(base_path_)) {
            if (entry.is_regular_file()) {
                std::string ext = entry.path().extension().string();
                if (ext == ".RDA") count++;
            }
        }
    }
    return count;
}
