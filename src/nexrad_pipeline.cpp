/*
 * Level II Data Fetcher - Standalone background service
 * 
 * Handles NEXRAD Level II data fetching from AWS S3
 * in a separate process to avoid resource conflicts.
 * 
 * Run as a separate daemon/service alongside radar_server.
 */

#include <iostream>
#include <string>
#include <memory>
#include <csignal>
#include <unistd.h>
#include <atomic>

#include "levelii/BackgroundFrameFetcher.h"
#include "levelii/FrameStorageManager.h"
#include "levelii/admin/AdminServer.h"
#include "levelii/AWSInitializer.h"
#include "levelii/DecompressionUtils.h"
#include "levelii/TerminalUI.h"
#include <thread>
#include <unistd.h>

static std::atomic<bool> shutdown_requested(false);

void signal_handler(int signum) {
    std::cout << "\n🛑 Received signal " << signum << ", shutting down..." << std::endl;
    shutdown_requested = true;
}

std::string get_executable_directory() {
    char result[256];
    ssize_t count = readlink("/proc/self/exe", result, 255);
    if (count != -1) {
        result[count] = '\0';
        std::string exe_path(result);
        size_t pos = exe_path.find_last_of("/");
        return exe_path.substr(0, pos);
    }
    return ".";
}

int main(int argc, char* argv[]) {
    // Set a short timeout for EC2 metadata service to speed up SDK initialization
    // if we're not on EC2.
    setenv("AWS_METADATA_SERVICE_TIMEOUT", "1", 0);
    setenv("AWS_METADATA_SERVICE_NUM_ATTEMPTS", "1", 0);

    bool use_http = true;
    int cmd_threads = -1;
    int cmd_buffer_count = -1;
    int cmd_buffer_size = -1;
    bool catchup_disabled = true;
    bool terminal_ui_enabled = isatty(STDOUT_FILENO);

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--no-http") {
            use_http = false;
        } else if (arg == "--catchup") {
            catchup_disabled = false;
        //} else if (arg == "--no-ui") {
            //terminal_ui_enabled = false;
        } else if (arg == "--threads" && i + 1 < argc) {
            cmd_threads = std::stoi(argv[++i]);
        } else if (arg == "--buffer-count" && i + 1 < argc) {
            cmd_buffer_count = std::stoi(argv[++i]);
        } else if (arg == "--buffer-size" && i + 1 < argc) {
            cmd_buffer_size = std::stoi(argv[++i]);
        } else if (arg == "--help") {
            std::cout << "Usage: " << argv[0] << " [options]\n"
                      << "Options:\n"
                      << "  --no-http              Disable HTTP admin server\n"
                      << "  --catchup        Enables catch-up of historical frames on startup\n"
                      //<< "  --no-ui             Disable interactive terminal UI\n"
                      << "  --threads N         Number of worker threads\n"
                      << "  --buffer-count N    Number of pre-allocated buffers\n"
                      << "  --buffer-size N     Size of each buffer in MB\n"
                      << "  --help              Show this help message\n";
            return 0;
        }
    }

    std::cout << "🚀 Level II Data Fetcher Service Starting" << std::endl;
    if (use_http) {
        std::cout << "📡 HTTP server enabled" << std::endl;
    } else {
        std::cout << "📡 HTTP server disabled (use --http to enable)" << std::endl;
    }
    
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);
    signal(SIGHUP, signal_handler);
    
    try {
        std::string base_dir = get_executable_directory();
        std::string level2_data_path = base_dir + "/data/levelii";

        std::cout << "📁 Data directory:" << std::endl;
        std::cout << "   Level II: " << level2_data_path << std::endl;

        auto storage_manager = std::make_shared<FrameStorageManager>(level2_data_path);
        
        FrameFetcherConfig fetcher_config;
        
        const char* env_stations = std::getenv("NEXRAD_MONITORED_STATIONS");
        if (env_stations) {
            std::string stations_str(env_stations);
            if (stations_str == "ALL" || stations_str == "*") {
                fetcher_config.monitored_stations = {"ALL"}; 
                std::cout << "📡 Monitoring ALL stations (NEXRAD_MONITORED_STATIONS=" << stations_str << ")" << std::endl;
            } else {
                std::cout << "📡 Monitoring stations: " << stations_str << std::endl;
                size_t start = 0;
                size_t end = stations_str.find(',');
                while (end != std::string::npos) {
                    fetcher_config.monitored_stations.insert(stations_str.substr(start, end - start));
                    start = end + 1;
                    end = stations_str.find(',', start);
                }
                fetcher_config.monitored_stations.insert(stations_str.substr(start));
            }
        } else {
            fetcher_config.monitored_stations = {
                "KTLX", "KCRP", "KEWX"
            };
            std::cout << "📡 Monitoring default stations (KTLX, KCRP, KEWX)" << std::endl;
        }

        // Memory and Performance Scaling (Priority: CLI > Environment > Default)
        const char* env_threads = std::getenv("NEXRAD_THREADS");
        if (cmd_threads > 0) fetcher_config.fetcher_thread_pool_size = cmd_threads;
        else if (env_threads) fetcher_config.fetcher_thread_pool_size = std::stoi(env_threads);
        
        const char* env_buffer_count = std::getenv("NEXRAD_BUFFER_COUNT");
        if (cmd_buffer_count > 0) fetcher_config.buffer_pool_size = cmd_buffer_count;
        else if (env_buffer_count) fetcher_config.buffer_pool_size = std::stoi(env_buffer_count);
        
        const char* env_buffer_size = std::getenv("NEXRAD_BUFFER_SIZE_MB");
        if (cmd_buffer_size > 0) fetcher_config.buffer_size = static_cast<size_t>(cmd_buffer_size) * 1024 * 1024;
        else if (env_buffer_size) fetcher_config.buffer_size = static_cast<size_t>(std::stoi(env_buffer_size)) * 1024 * 1024;

        if (catchup_disabled) {
            fetcher_config.catchup_enabled = false;
        }

        std::cout << "⚙️  Performance Config: " 
                  << fetcher_config.fetcher_thread_pool_size << " threads, "
                  << fetcher_config.buffer_pool_size << " buffers ("
                  << fetcher_config.buffer_size / (1024 * 1024) << "MB each), "
                  << "catchup=" << (fetcher_config.catchup_enabled ? "on" : "off") << std::endl;
        fetcher_config.products = {
            "reflectivity",
            "velocity",
            "correlation_coefficient"
        };
        fetcher_config.scan_interval_seconds = 30;
        fetcher_config.max_frames_per_station = 30;
        fetcher_config.cleanup_interval_seconds = 300;
        fetcher_config.auto_cleanup_enabled = true;
        
        auto frame_fetcher = std::make_shared<BackgroundFrameFetcher>(
            storage_manager, 
            fetcher_config, 
            level2_data_path
        );
        
        std::shared_ptr<AdminServer> admin_server;
        if (use_http) {
            admin_server = std::make_shared<AdminServer>(frame_fetcher, storage_manager, 13480);
            admin_server->start();
            std::cout << "✓ Admin panel started on http://localhost:13480" << std::endl;
        }

        std::thread aws_init_thread([]() {
            AWSInitializer::instance().initialize_async();
        });

        frame_fetcher->start();
        if (terminal_ui_enabled) {
            frame_fetcher->set_logging_enabled(false);
        } else {
            std::cout << "✓ Level II fetcher started" << std::endl;
            std::cout << "\n✅ Level II data fetcher running. Press Ctrl+C to stop." << std::endl;
            std::cout << std::string(70, '=') << std::endl;
        }

        TerminalUI ui(frame_fetcher);
        if (terminal_ui_enabled) {
            ui.clear_screen();
        }

        while (!shutdown_requested) {
            if (terminal_ui_enabled) {
                ui.render();
            }
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }

        std::cout << "\n🛑 Shutting down Level II fetcher..." << std::endl;
        
        // 1. Join initialization thread first to ensure initialization is finished
        if (aws_init_thread.joinable()) {
            aws_init_thread.join();
        }
        
        // 2. Shut down admin server (stops web server and API)
        if (admin_server) {
            admin_server->shutdown_all();
            admin_server.reset();
        }
        frame_fetcher.reset();
        storage_manager.reset();
        
        // 4. Shutdown AWS SDK while we are still in main
        AWSInitializer::instance().shutdown();
        
        std::cout << "✅ Level II Data Fetcher Service stopped cleanly" << std::endl;
        
    } catch (const std::exception& e) {
        std::cerr << "❌ Fatal error: " << e.what() << std::endl;
        return 1;
    }
    
    return 0;
}
