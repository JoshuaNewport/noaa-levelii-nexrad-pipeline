/**
 * ThreadPool.h - Generic thread pool for parallel task execution
 *
 * Implements a reusable thread pool with worker threads that process
 * tasks from a thread-safe queue. Supports:
 * - Configurable worker count (default: 4-8)
 * - Task queueing with arbitrary callables
 * - Graceful shutdown with work completion
 * - Memory-efficient design with move semantics
 */

#pragma once

#include <thread>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <functional>
#include <memory>
#include <vector>
#include <atomic>

class ThreadPool {
public:
    using Task = std::function<void()>;

    explicit ThreadPool(size_t worker_count = 0);

    ~ThreadPool();

    void enqueue(Task task);

    void shutdown();

    bool is_running() const { return is_running_.load(); }

    size_t worker_count() const { return workers_.size(); }
    size_t active_threads() const { return active_threads_.load(); }
    size_t pending_tasks() const {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        return task_queue_.size();
    }

private:
    std::vector<std::thread> workers_;
    std::queue<Task> task_queue_;
    mutable std::mutex queue_mutex_;
    std::condition_variable queue_cv_;
    std::atomic<bool> is_running_{false};
    std::atomic<bool> should_stop_{false};
    std::atomic<size_t> active_threads_{0};

    void worker_loop();
};
