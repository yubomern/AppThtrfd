
#include "Thread.h"
#include "Include.h"
#include <exception>
#include <iostream>
#include <memory>

int main() {
    try {
       // Logger& logger = Logger::getInstance();
       // logger.addSink(LoggerFactory::createConsoleSink());
        //logger.addSink(LoggerFactory::createFileSink("thread.log"));

        Thread& pool = Thread::getInstance();
       // logger.info("Thread pool started with " + std::to_string(pool.workerCount()) + " workers");

        auto calculationFuture = pool.submit([](int first, int second) {
            return first + second;
            }, 20, 22);

        auto data = std::make_unique<Data>();
        data->dataName = "ThreadPool";
        data->measure = "active";
        data->config = std::to_string(pool.workerCount()) + " workers";
        auto printFuture = pool.print(std::move(data));

        auto pythonFuture = pool.runPopenPython("python --version");
        auto pingFuture = pool.runPopenPython("ping -n 2 127.0.0.1");

        std::cout << "Calculation result: " << calculationFuture.get() << '\n';
        printFuture.get();

        const ProcessResult pythonResult = pythonFuture.get();
        std::cout << "Python exit code: " << pythonResult.exitCode << '\n'
            << pythonResult.output;

        const ProcessResult pingResult = pingFuture.get();
        std::cout << "Ping exit code: " << pingResult.exitCode << '\n'
            << pingResult.output;

        pool.waitIdle();
       // logger.info("All asynchronous tasks completed");
        return pythonResult.exitCode == 0 && pingResult.exitCode == 0 ? 0 : 1;
    }
    catch (const std::exception& exception) {
        std::cerr << "runth error: " << exception.what() << '\n';
        return 1;
    }
}
