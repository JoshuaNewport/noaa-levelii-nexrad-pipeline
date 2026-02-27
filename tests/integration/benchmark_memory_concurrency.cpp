#include <iostream>
#include <fstream>
#include <vector>
#include <chrono>
#include <thread>
#include <atomic>
#include <iomanip>
#include <unistd.h>
#include "levelii/RadarParser.h"
#include "levelii/BackgroundFrameFetcher.h"
#include "levelii/FrameStorageManager.h"

// Simple memory usage helper for Linux
size_t get_rss() {
    std::ifstream stat_stream("/proc/self/stat", std::ios_base::in);
    std::string pid, comm, state, ppid, pgrp, session, tty_nr;
    std::string tpgid, flags, minflt, cminflt, majflt, cmajflt;
    std::string utime, stime, cutime, cstime, priority, nice;
    std::string O, itrealvalue, starttime;
    unsigned long vsize;
    long rss;
    stat_stream >> pid >> comm >> state >> ppid >> pgrp >> session >> tty_nr
                >> tpgid >> flags >> minflt >> cminflt >> majflt >> cmajflt
                >> utime >> stime >> cutime >> cstime >> priority >> nice
                >> O >> itrealvalue >> starttime >> vsize >> rss;
    stat_stream.close();
    long page_size_kb = sysconf(_SC_PAGE_SIZE) / 1024;
    return rss * page_size_kb;
}

int main(int argc, char** argv) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <sample_nexrad_file>" << std::endl;
        return 1;
    }

    std::ifstream file(argv[1], std::ios::binary);
    if (!file.is_open()) {
        std::cerr << "Could not open " << argv[1] << std::endl;
        return 1;
    }
    std::vector<uint8_t> sample_data((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
    file.close();

    const int num_stations = 150;
    const int frames_per_station = 1; // Process one frame per station for benchmark
    const int total_tasks = num_stations * frames_per_station;
    
    // Config mirroring our optimized settings
    FrameFetcherConfig config;
    config.fetcher_thread_pool_size = 8;
    config.buffer_pool_size = 64;
    config.buffer_size = 120 * 1024 * 1024;
    
    auto storage = std::make_shared<FrameStorageManager>("./test_data");
    auto buffer_pool = std::make_shared<BufferPool>(config.buffer_pool_size, config.buffer_size);
    auto thread_pool = std::make_shared<ThreadPool>(config.fetcher_thread_pool_size);

    std::atomic<int> completed_tasks{0};
    std::atomic<long long> total_parse_time_ms{0};

    size_t start_rss = get_rss();
    auto start_time = std::chrono::high_resolution_clock::now();

    std::cout << "🚀 Starting benchmark: " << total_tasks << " tasks on " 
              << config.fetcher_thread_pool_size << " threads..." << std::endl;
    std::cout << "Initial RSS: " << start_rss << " KB" << std::endl;

    for (int i = 0; i < total_tasks; ++i) {
        thread_pool->enqueue([&, i]() {
            auto task_start = std::chrono::high_resolution_clock::now();
            
            std::string station = "ST" + std::to_string(i % num_stations);
            std::string timestamp = "20260226_120000";
            
            ScopedBuffer raw_buf(buffer_pool);
            if (raw_buf.valid()) {
                raw_buf->assign(sample_data.begin(), sample_data.end());
                
                ScopedBuffer decomp_buf(buffer_pool);
                if (decomp_buf.valid()) {
                    auto frames = parse_nexrad_level2_multi(*raw_buf, station, timestamp, {"reflectivity"}, decomp_buf.get());
                }
            }
            
            auto task_end = std::chrono::high_resolution_clock::now();
            total_parse_time_ms += std::chrono::duration_cast<std::chrono::milliseconds>(task_end - task_start).count();
            completed_tasks++;
            
            if (completed_tasks % 10 == 0) {
                std::cout << "\rProgress: " << completed_tasks << "/" << total_tasks << " tasks done..." << std::flush;
            }
        });
    }

    // Wait for all tasks
    while (completed_tasks < total_tasks) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    size_t end_rss = get_rss();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

    std::cout << "\n\n=== Benchmark Results ===" << std::endl;
    std::cout << "Total Time: " << duration.count() << " ms" << std::endl;
    std::cout << "Average Task Time: " << (double)total_parse_time_ms / total_tasks << " ms" << std::endl;
    std::cout << "Throughput: " << (double)total_tasks / (duration.count() / 1000.0) << " tasks/sec" << std::endl;
    std::cout << "Final RSS: " << end_rss << " KB" << std::endl;
    std::cout << "Memory Growth: " << (long long)end_rss - (long long)start_rss << " KB" << std::endl;
    std::cout << "=========================" << std::endl;

    thread_pool->shutdown();
    return 0;
}
