#include "levelii/admin/AdminAPI.h"
#include "levelii/admin/WebServer.h"
#include "levelii/BackgroundFrameFetcher.h"
#include "levelii/FrameStorageManager.h"
#include <chrono>
#include <ctime>

static auto g_start_time = std::chrono::system_clock::now();

AdminAPI::AdminAPI(
    std::shared_ptr<BackgroundFrameFetcher> fetcher,
    std::shared_ptr<FrameStorageManager> storage
) : fetcher_(fetcher), storage_(storage) {}

void AdminAPI::register_routes(WebServer& server) {
    server.add_route("GET", "/api/stations", [this](const std::string&, const std::string&) {
        return handle_get_stations().dump();
    });

    server.add_route("POST", "/api/stations", [this](const std::string& body, const std::string&) {
        return handle_post_stations(body).dump();
    });

    server.add_route("DELETE", "/api/stations/:name", [this](const std::string&, const std::string& param) {
        return handle_delete_station(param).dump();
    });

    server.add_route("GET", "/api/metrics", [this](const std::string&, const std::string&) {
        return handle_get_metrics().dump();
    });

    server.add_route("GET", "/api/status", [this](const std::string&, const std::string&) {
        return handle_get_status().dump();
    });

    server.add_route("GET", "/api/config", [this](const std::string&, const std::string&) {
        return handle_get_config().dump();
    });

    server.add_route("POST", "/api/config", [this](const std::string& body, const std::string&) {
        return handle_post_config(body).dump();
    });

    server.add_route("POST", "/api/pause", [this](const std::string&, const std::string&) {
        return handle_post_pause().dump();
    });

    server.add_route("POST", "/api/resume", [this](const std::string&, const std::string&) {
        return handle_post_resume().dump();
    });
}

json AdminAPI::handle_get_stations() {
    if (!fetcher_) {
        return json::array();
    }
    auto stations = fetcher_->get_monitored_stations();
    json response = json::array();
    for (const auto& station : stations) {
        response.push_back({
            {"name", station},
            {"status", "active"}
        });
    }
    return response;
}

json AdminAPI::handle_post_stations(const std::string& body) {
    if (!fetcher_) {
        return json{{"error", "Fetcher not initialized"}};
    }
    try {
        auto data = json::parse(body);
        std::string station_name = data.value("name", "");
        
        if (station_name.empty()) {
            return json{{"error", "Station name required"}};
        }
        
        fetcher_->add_monitored_station(station_name);
        return json{
            {"success", true},
            {"station", station_name}
        };
    } catch (const std::exception& e) {
        return json{{"error", e.what()}};
    }
}

json AdminAPI::handle_delete_station(const std::string& name) {
    if (!fetcher_) {
        return json{{"error", "Fetcher not initialized"}};
    }
    if (name.empty()) {
        return json{{"error", "Station name required"}};
    }
    
    fetcher_->remove_monitored_station(name);
    return json{
        {"success", true},
        {"station", name}
    };
}

json AdminAPI::handle_get_metrics() {
    auto now = std::chrono::system_clock::now();
    auto uptime_seconds = std::chrono::duration_cast<std::chrono::seconds>(
        now - g_start_time
    ).count();
    
    uint64_t fetched = 0, failed = 0;
    json metrics{
        {"frames_fetched", 0},
        {"frames_failed", 0},
        {"success_rate", 0.0},
        {"disk_usage_mb", 0},
        {"disk_usage_gb", 0.0},
        {"frame_count", 0},
        {"avg_frames_per_minute", 0.0},
        {"uptime_seconds", uptime_seconds},
        {"last_fetch_timestamp", 0}
    };
    
    if (fetcher_) {
        auto stats = fetcher_->get_statistics();
        fetched = stats.value("frames_fetched", 0);
        failed = stats.value("frames_failed", 0);
        metrics["frames_fetched"] = fetched;
        metrics["frames_failed"] = failed;
        metrics["last_fetch_timestamp"] = stats.value("last_fetch_timestamp", 0);
        
        if (uptime_seconds > 0) {
            metrics["avg_frames_per_minute"] = (double(fetched) / uptime_seconds) * 60.0;
        }
        if ((fetched + failed) > 0) {
            metrics["success_rate"] = (double(fetched) / (fetched + failed)) * 100.0;
        }
    }
    
    if (storage_) {
        auto disk_usage = storage_->get_total_disk_usage();
        auto frame_count = storage_->get_frame_count();
        metrics["disk_usage_mb"] = disk_usage / (1024 * 1024);
        metrics["disk_usage_gb"] = (double)disk_usage / (1024 * 1024 * 1024);
        metrics["frame_count"] = frame_count;
    }
    
    return metrics;
}

json AdminAPI::handle_get_status() {
    bool running = fetcher_ ? fetcher_->is_running() : false;
    return json{
        {"status", "operational"},
        {"fetcher_running", running},
        {"timestamp", std::time(nullptr)}
    };
}

json AdminAPI::handle_get_config() {
    if (!fetcher_) return json{{"error", "Fetcher not initialized"}};
    
    auto config = fetcher_->get_config();
    return json{
        {"scan_interval_seconds", config.scan_interval_seconds},
        {"max_frames_per_station", config.max_frames_per_station},
        {"cleanup_interval_seconds", config.cleanup_interval_seconds},
        {"auto_cleanup_enabled", config.auto_cleanup_enabled},
        {"fetcher_thread_pool_size", config.fetcher_thread_pool_size},
        {"buffer_pool_size", config.buffer_pool_size},
        {"buffer_size_mb", config.buffer_size / (1024 * 1024)}
    };
}

json AdminAPI::handle_post_config(const std::string& body) {
    if (!fetcher_) return json{{"error", "Fetcher not initialized"}};
    
    try {
        auto data = json::parse(body);
        auto config = fetcher_->get_config();
        
        if (data.contains("scan_interval_seconds")) config.scan_interval_seconds = data["scan_interval_seconds"];
        if (data.contains("max_frames_per_station")) config.max_frames_per_station = data["max_frames_per_station"];
        if (data.contains("cleanup_interval_seconds")) config.cleanup_interval_seconds = data["cleanup_interval_seconds"];
        if (data.contains("auto_cleanup_enabled")) config.auto_cleanup_enabled = data["auto_cleanup_enabled"];
        if (data.contains("fetcher_thread_pool_size")) config.fetcher_thread_pool_size = data["fetcher_thread_pool_size"];
        if (data.contains("buffer_pool_size")) config.buffer_pool_size = data["buffer_pool_size"];
        if (data.contains("buffer_size_mb")) config.buffer_size = static_cast<size_t>(data["buffer_size_mb"]) * 1024 * 1024;
        
        fetcher_->reconfigure(config);
        return json{{"success", true}, {"config", handle_get_config()}};
    } catch (const std::exception& e) {
        return json{{"error", e.what()}};
    }
}

json AdminAPI::handle_post_pause() {
    if (!fetcher_) {
        return json{{"error", "Fetcher not initialized"}};
    }
    if (!fetcher_->is_running()) {
        return json{{"success", true}, {"status", "already paused"}};
    }
    fetcher_->stop();
    return json{{"success", true}, {"status", "paused"}, {"message", "All threads stopped successfully"}};
}

json AdminAPI::handle_post_resume() {
    if (!fetcher_) {
        return json{{"error", "Fetcher not initialized"}};
    }
    if (fetcher_->is_running()) {
        return json{{"error", "Fetcher already running"}};
    }
    fetcher_->start();
    return json{{"success", true}, {"status", "resumed"}};
}
