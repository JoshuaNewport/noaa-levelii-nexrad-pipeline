/**
 * ThreadPool.cpp - Implementation
 */

#include "levelii/ThreadPool.h"
#include <iostream>

ThreadPool::ThreadPool(size_t worker_count) {
    if (worker_count == 0) {
        worker_count = std::max(1U, std::thread::hardware_concurrency() / 2);
    }

    stop_.store(false);

    for (size_t i = 0; i < worker_count; ++i) {
        workers_.emplace_back([this]() { this->worker_loop(); });
    }
}

ThreadPool::~ThreadPool() {
    shutdown();
}

void ThreadPool::enqueue(Task task) {
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        if (stop_.load()) {
            return;
        }
        task_queue_.push(std::move(task));
    }
    queue_cv_.notify_one();
}

void ThreadPool::shutdown() {
    if (stop_.load()) return;

    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        stop_.store(true);
    }
    queue_cv_.notify_all();

    for (auto& worker : workers_) {
        if (worker.joinable()) {
            worker.join();
        }
    }
}

void ThreadPool::worker_loop() {
    while (true) {
        Task task;
        {
            std::unique_lock<std::mutex> lock(queue_mutex_);
            queue_cv_.wait(lock, [this]() {
                return stop_.load() || !task_queue_.empty();
            });

            if (stop_.load() && task_queue_.empty()) {
                break;
            }

            if (!task_queue_.empty()) {
                task = std::move(task_queue_.front());
                task_queue_.pop();
            } else {
                continue;
            }
        }

        if (task) {
            active_threads_.fetch_add(1);
            try {
                task();
            } catch (const std::exception& e) {
                std::cerr << "❌ ThreadPool task exception: " << e.what() << std::endl;
            } catch (...) {
                std::cerr << "❌ ThreadPool task unknown exception" << std::endl;
            }
            active_threads_.fetch_sub(1);
        }
    }
}
