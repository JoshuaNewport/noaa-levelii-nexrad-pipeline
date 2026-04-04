#include <iostream>
#include <vector>
#include <thread>
#include <atomic>
#include <chrono>
#include <memory>
#include "levelii/BackgroundFrameFetcher.h"
#include "levelii/DecompressionUtils.h"

// Reproducer for potential BufferPool/Decompression heap corruption

int main() {
    const int num_threads = 16;
    const int num_tasks = 200;
    const int initial_buffer_size = 1024 * 1024; // 1MB

    auto buffer_pool = std::make_shared<BufferPool>(32, initial_buffer_size);
    std::atomic<int> completed_tasks{0};
    std::atomic<bool> stop_repro{false};

    // Thread that constantly reconfigures the pool to stress it
    std::thread reconfig_thread([&]() {
        while (!stop_repro) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            // Simulate reconfiguration by just acquiring/releasing or potentially 
            // replacing the pool if we had access to BackgroundFrameFetcher
        }
    });

    std::vector<std::thread> workers;
    for (int t = 0; t < num_threads; ++t) {
        workers.emplace_back([&, t]() {
            while (completed_tasks < num_tasks && !stop_repro) {
                int task_id = completed_tasks.fetch_add(1);
                if (task_id >= num_tasks) break;

                // Simulate processing with various sizes to trigger reallocations in the pool
                size_t target_size = (task_id % 5 == 0) ? (initial_buffer_size * 5) : (initial_buffer_size / 2);
                
                ScopedBuffer buffer(buffer_pool);
                if (buffer.valid()) {
                    // Trigger reallocation if target_size > initial_buffer_size
                    buffer->resize(target_size, (uint8_t)(task_id & 0xFF));
                    
                    // Simulate some "processing" that might write to the buffer
                    for (size_t i = 0; i < buffer->size(); i += 4096) {
                        (*buffer)[i] = (uint8_t)(i & 0xFF);
                    }
                    
                    // Trigger more processing
                    std::this_thread::sleep_for(std::chrono::milliseconds(5));
                }
            }
        });
    }

    for (auto& w : workers) {
        if (w.joinable()) w.join();
    }
    stop_repro = true;
    if (reconfig_thread.joinable()) reconfig_thread.join();

    std::cout << "✅ Repro completed without crash. Completed " << completed_tasks << " tasks." << std::endl;
    return 0;
}
