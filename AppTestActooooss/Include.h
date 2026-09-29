#pragma once
#include <ws2tcpip.h>
#include <WinSock2.h>
#include <windows.h>


#include <condition_variable>
#include <cstddef>
#include <exception>
#include <functional>
#include <iostream>
#include <mutex>
#include <optional>
#include <queue>
#include <thread>
#include <vector>
#include <array>
#include <chrono>
#include <ctime>
#include <iomanip>
#include <sstream>
// Blocking, closable, thread-safe queue.
template <typename T>
class SafeQueue {
public:
    void push(T value) {
        {
            std::lock_guard<std::mutex> lock(m_);
            q_.push(std::move(value));
        }
        cv_.notify_one();
    }

    // Blocks until an item is available. Returns nullopt once closed AND drained.
    std::optional<T> pop() {
        std::unique_lock<std::mutex> lock(m_);
        cv_.wait(lock, [this] { return closed_ || !q_.empty(); });
        if (q_.empty()) return std::nullopt;
        T value = std::move(q_.front());
        q_.pop();
        return value;
    }

    void close() {
        {
            std::lock_guard<std::mutex> lock(m_);
            closed_ = true;
        }
        cv_.notify_all();
    }

    std::size_t size() const {
        std::lock_guard<std::mutex> lock(m_);
        return q_.size();
    }

private:
    mutable std::mutex m_;
    std::condition_variable cv_;
    std::queue<T> q_;
    bool closed_ = false;
};

// Fixed-size pool. Destructor finishes queued jobs, then joins.
class ThreadPool {
public:
    explicit ThreadPool(std::size_t threads = std::thread::hardware_concurrency()) {
        if (threads == 0) threads = 4;
        for (std::size_t i = 0; i < threads; ++i) {
            workers_.emplace_back([this] {
                while (auto job = jobs_.pop()) {
                    try {
                        (*job)();
                    }
                    catch (const std::exception& e) {
                        std::cerr << "[pool] job failed: " << e.what() << '\n';
                    }
                }
                });
        }
    }

    ~ThreadPool() {
        jobs_.close();
        for (auto& t : workers_) t.join();
    }

    ThreadPool(const ThreadPool&) = delete;
    ThreadPool& operator=(const ThreadPool&) = delete;

    void submit(std::function<void()> job) { jobs_.push(std::move(job)); }
    std::size_t pending() const { return jobs_.size(); }

private:
    SafeQueue<std::function<void()>> jobs_;
    std::vector<std::thread> workers_;
};
