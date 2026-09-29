#include "Thread.h"




#include <chrono>
#include <ctime>
#include <iomanip>
#include <sstream>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

class ILoggerSink {
public:
    virtual ~ILoggerSink() = default;
    virtual void log(const std::string& level, const std::string& message) = 0;
};

class Logger {
public:
    static Logger& getInstance();
    Logger(const Logger&) = delete;
    Logger& operator=(const Logger&) = delete;

    void addSink(std::unique_ptr<ILoggerSink> sink);
    void log(const std::string& level, const std::string& message) const;
    void info(const std::string& message) const;
    void warn(const std::string& message) const;
    void error(const std::string& message) const;

private:
    Logger() = default;

    static std::unique_ptr<Logger> instance_;
    mutable std::mutex mutex_;
    std::vector<std::unique_ptr<ILoggerSink>> sinks_;
};

std::unique_ptr<Logger> Logger::instance_;

Logger& Logger::getInstance() {
    if (!instance_) {
        instance_ = std::make_unique<Logger>();
    }
    return *instance_;
}

void Logger::addSink(std::unique_ptr<ILoggerSink> sink) {
    if (sink) {
        sinks_.push_back(std::move(sink));
    }
}

void Logger::log(const std::string& level, const std::string& message) const {
    std::lock_guard<std::mutex> lock(mutex_);

    auto now = std::chrono::system_clock::now();
    auto time = std::chrono::system_clock::to_time_t(now);
    std::tm localTime{};

#ifdef _WIN32
    localtime_s(&localTime, &time);
#else
    localtime_r(&time, &localTime);
#endif

    std::ostringstream stream;
    stream << std::put_time(&localTime, "%Y-%m-%d %H:%M:%S");
    const std::string stamp = stream.str();
    const std::string formatted = "[" + stamp + "] [" + level + "] " + message;

    for (const auto& sink : sinks_) {
        sink->log(level, formatted);
    }
}

void Logger::info(const std::string& message) const {
    log("INFO", message);
}

void Logger::warn(const std::string& message) const {
    log("WARN", message);
}

void Logger::error(const std::string& message) const {
    log("ERROR", message);
}


#include <algorithm>
#include <array>
#include <iostream>

namespace {

    struct PipeCloser {
        int operator()(FILE* pipe) const noexcept {
            return pipe ? _pclose(pipe) : -1;
        }
    };

    using UniquePipe = std::unique_ptr<FILE, PipeCloser>;

    std::size_t defaultWorkerCount() {
        const unsigned int hardwareThreads = std::thread::hardware_concurrency();
        return std::max<std::size_t>(1, hardwareThreads == 0 ? 4 : hardwareThreads);
    }

} // namespace

Thread::Thread(std::size_t workerCount) {
    Logger::getInstance();
    const std::size_t count = workerCount == 0 ? defaultWorkerCount() : workerCount;
    workers_.reserve(count);
    for (std::size_t index = 0; index < count; ++index) {
        workers_.emplace_back(&Thread::workerLoop, this);
    }
}

Thread::~Thread() {
    shutdown();
}

Thread& Thread::getInstance() {
    static Thread instance;
    return instance;
}

std::future<ProcessResult> Thread::runPopenPython(std::string command) {
    return submit([command = std::move(command)] {
        return runPopenCapture(command);
        });
}

std::future<void> Thread::print(std::unique_ptr<Data> data) {
    if (!data) {
        throw std::invalid_argument("data must not be null");
    }

    return submit([data = std::move(data)] {
        const std::string message = data->dataName + " | " + data->measure + " | " + data->config;
        Logger::getInstance().info(message);
        std::cout << message << '\n';
        });
}

std::size_t Thread::workerCount() const noexcept {
    return workers_.size();
}

std::size_t Thread::pendingTasks() const noexcept {
    return pendingTasks_.load(std::memory_order_relaxed);
}

void Thread::waitIdle() {
    std::unique_lock<std::mutex> lock(mutex_);
    idleCondition_.wait(lock, [this] {
        return tasks_.empty() && pendingTasks_.load(std::memory_order_acquire) == 0;
        });
}

void Thread::shutdown() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (stopping_) {
            return;
        }
        stopping_ = true;
    }

    condition_.notify_all();
    for (std::thread& worker : workers_) {
        if (worker.joinable()) {
            worker.join();
        }
    }
    workers_.clear();
}

void Thread::workerLoop() {
    while (true) {
        std::function<void()> task;
        {
            std::unique_lock<std::mutex> lock(mutex_);
            condition_.wait(lock, [this] { return stopping_ || !tasks_.empty(); });
            if (stopping_ && tasks_.empty()) {
                return;
            }
            task = std::move(tasks_.front());
            tasks_.pop();
        }

        task();

        if (pendingTasks_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
            std::lock_guard<std::mutex> lock(mutex_);
            idleCondition_.notify_all();
        }
    }
}

ProcessResult Thread::runPopenCapture(const std::string& command) {
    ProcessResult result;
    UniquePipe pipe(_popen((command + " 2>&1").c_str(), "r"));
    if (!pipe) {
        result.output = "ERROR: failed to start process: " + command;
        Logger::getInstance().error(result.output);
        return result;
    }

    std::array<char, 4096> buffer{};
    while (fgets(buffer.data(), static_cast<int>(buffer.size()), pipe.get()) != nullptr) {
        result.output.append(buffer.data());
    }

    result.exitCode = _pclose(pipe.release());
    if (result.exitCode == 0) {
        Logger::getInstance().info("Process completed: " + command);
    }
    else {
        Logger::getInstance().error("Process failed (" + std::to_string(result.exitCode) + "): " + command);
    }
    return result;
}
