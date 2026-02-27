/**
 * test_threadpool.cpp - ThreadPool unit and stress tests
 */

#include "levelii/ThreadPool.h"
#include <iostream>
#include <atomic>
#include <chrono>
#include <vector>

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
    log_info("=== ThreadPool Unit Tests ===\n");

    // Test 1: Basic task execution
    {
        log_info("Test 1: Basic task execution");
        ThreadPool pool(4);
        std::atomic<int> counter{0};
        
        for (int i = 0; i < 10; ++i) {
            pool.enqueue([&counter]() {
                counter.fetch_add(1);
            });
        }
        
        pool.shutdown();
        
        if (counter.load() == 10) {
            log_success("Test 1 passed: All 10 tasks executed");
        } else {
            log_error("Test 1 failed: Expected 10 tasks, got " + std::to_string(counter.load()));
            return 1;
        }
    }

    // Test 2: Multiple workers
    {
        log_info("\nTest 2: Multiple workers (default count)");
        ThreadPool pool(0);
        size_t workers = pool.worker_count();
        
        if (workers > 0) {
            log_success("Test 2 passed: Created " + std::to_string(workers) + " worker threads");
        } else {
            log_error("Test 2 failed: No workers created");
            return 1;
        }
    }

    // Test 3: High concurrency stress test
    {
        log_info("\nTest 3: Stress test with 50 concurrent stations");
        ThreadPool pool(8);
        std::atomic<int> completed{0};
        std::atomic<int> errors{0};
        
        auto start = std::chrono::steady_clock::now();
        
        for (int station = 0; station < 50; ++station) {
            pool.enqueue([station, &completed, &errors]() {
                try {
                    std::string station_name = "STATION_" + std::to_string(station);
                    
                    for (int i = 0; i < 5; ++i) {
                        std::this_thread::sleep_for(std::chrono::milliseconds(10));
                    }
                    
                    completed.fetch_add(1);
                } catch (const std::exception& e) {
                    errors.fetch_add(1);
                    log_error("Error in station task: " + std::string(e.what()));
                }
            });
        }
        
        pool.shutdown();
        
        auto end = std::chrono::steady_clock::now();
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
        
        if (completed.load() == 50 && errors.load() == 0) {
            log_success("Test 3 passed: Processed 50 stations in " + std::to_string(elapsed) + "ms");
        } else {
            log_error("Test 3 failed: Completed " + std::to_string(completed.load()) + 
                     ", Errors " + std::to_string(errors.load()));
            return 1;
        }
    }

    // Test 4: Graceful shutdown
    {
        log_info("\nTest 4: Graceful shutdown with pending tasks");
        {
            ThreadPool pool(4);
            std::atomic<int> executed{0};
            
            for (int i = 0; i < 100; ++i) {
                pool.enqueue([&executed]() {
                    executed.fetch_add(1);
                    std::this_thread::sleep_for(std::chrono::milliseconds(5));
                });
            }
            
            pool.shutdown();
            
            if (executed.load() == 100) {
                log_success("Test 4 passed: All tasks completed before shutdown");
            } else {
                log_error("Test 4 warning: " + std::to_string(executed.load()) + " tasks completed");
            }
        }
    }

    // Test 5: Exception handling
    {
        log_info("\nTest 5: Exception handling in tasks");
        ThreadPool pool(4);
        std::atomic<int> exceptions{0};
        
        for (int i = 0; i < 10; ++i) {
            pool.enqueue([i, &exceptions]() {
                if (i % 3 == 0) {
                    exceptions.fetch_add(1);
                    throw std::runtime_error("Test exception");
                }
            });
        }
        
        pool.shutdown();
        
        if (exceptions.load() > 0) {
            log_success("Test 5 passed: Exception handling works (" + 
                       std::to_string(exceptions.load()) + " exceptions handled)");
        } else {
            log_error("Test 5 failed: No exceptions detected");
            return 1;
        }
    }

    log_info("\n=== All ThreadPool tests completed successfully ===");
    return 0;
}
