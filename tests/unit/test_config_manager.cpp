/**
 * test_config_manager.cpp - Tests for BackgroundFrameFetcher configuration
 */

#include "levelii/BackgroundFrameFetcher.h"
#include "levelii/FrameStorageManager.h"
#include <iostream>
#include <cassert>
#include <set>

namespace {
    void log_info(const std::string& msg) {
        std::cout << "ℹ️  " << msg << std::endl;
    }

    void log_success(const std::string& msg) {
        std::cout << "✅ " << msg << std::endl;
    }

    void log_error(const std::string& msg) {
        std::cerr << "❌ " << msg << std::endl;
    }
}

int main() {
    log_info("=== Configuration Manager Tests ===\n");

    // Test 1: Monitored stations desynchronization
    {
        log_info("Test 1: Monitored stations desynchronization");
        FrameFetcherConfig config;
        config.monitored_stations = {"KTLX", "KAMA"};
        
        auto storage = std::make_shared<FrameStorageManager>("./test_data");
        BackgroundFrameFetcher fetcher(storage, config);
        
        // Add a station via API
        fetcher.add_monitored_station("KFWS");
        
        auto stations = fetcher.get_monitored_stations();
        if (stations.count("KFWS") == 0) {
            log_error("Test 1 failed: KFWS not added");
            return 1;
        }
        
        // Reconfigure (e.g., change scan interval)
        auto current_config = fetcher.get_config();
        current_config.scan_interval_seconds = 60;
        fetcher.reconfigure(current_config);
        
        // Check if KFWS is still there
        stations = fetcher.get_monitored_stations();
        if (stations.count("KFWS") == 0) {
            log_error("Test 1 failed: KFWS lost after reconfiguration (BUG REPRODUCED)");
            // We want this to fail initially to confirm the bug
        } else {
            log_success("Test 1 passed: KFWS preserved after reconfiguration");
        }
    }

    log_info("\n=== Configuration Manager tests completed ===");
    return 0;
}
