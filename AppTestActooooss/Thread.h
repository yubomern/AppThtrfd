#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdio>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <queue>
#include <stdexcept>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

struct Data {
    std::string dataName;
    std::string measure;
    std::string config;
};

struct ProcessResult {
    int exitCode = -1;
    std::string output;
};

class Thread {
public:
    explicit Thread(std::size_t workerCount = 0);
    ~Thread();

    Thread(const Thread&) = delete;
    Thread& operator=(const Thread&) = delete;
    Thread(Thread&&) = delete;
    Thread& operator=(Thread&&) = delete;

    static Thread& getInstance();

    template <typename Function, typename... Args>
    auto submit(Function&& function, Args&&... args)
        -> std::future<std::invoke_result_t<Function, Args...>> {
        using Result = std::invoke_result_t<Function, Args...>;

        auto task = std::make_shared<std::packaged_task<Result()>>(
            std::bind(std::forward<Function>(function), std::forward<Args>(args)...));
        std::future<Result> future = task->get_future();

        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (stopping_) {
                throw std::runtime_error("cannot submit work to a stopped thread pool");
            }
            tasks_.emplace([task] { (*task)(); });
            pendingTasks_.fetch_add(1, std::memory_order_relaxed);
        }

        condition_.notify_one();
        return future;
    }

    std::future<ProcessResult> runPopenPython(std::string command);
    std::future<void> print(std::unique_ptr<Data> data);

    std::size_t workerCount() const noexcept;
    std::size_t pendingTasks() const noexcept;
    void waitIdle();
    void shutdown();

private:
    void workerLoop();
    static ProcessResult runPopenCapture(const std::string& command);

    mutable std::mutex mutex_;
    std::condition_variable condition_;
    std::condition_variable idleCondition_;
    std::vector<std::thread> workers_;
    std::queue<std::function<void()>> tasks_;
    std::atomic<std::size_t> pendingTasks_{ 0 };
    bool stopping_ = false;
};
