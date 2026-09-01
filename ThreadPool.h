#ifndef THREAD_POOL_H
#define THREAD_POOL_H

#include <future>
#include <vector>
#include <thread>
#include <queue>
#include <mutex>
#include <functional>
#include <condition_variable>

class ThreadPool {
private:
  std::vector<std::thread> workers;
  std::queue<std::function<void()>> jobs;
  std::mutex lock;
  std::condition_variable cv;
  bool stop = false;

  void worker_loop() {
    std::function<void()> fn;
    while (true) { 
        {
            std::unique_lock<std::mutex> lk(lock);
            cv.wait(lk, [this] {
              return stop || !jobs.empty(); 
            });

            if (stop && jobs.empty()) {
              return;
            }
              fn = jobs.front();
              jobs.pop();
            }

            fn();
        }
  }

public:
    ThreadPool(size_t numThreads) {
        for (size_t i = 0; i < numThreads; i++) {
            workers.emplace_back([this] {
                worker_loop();
            });
        }
    }

    ~ThreadPool() {
        {
            std::lock_guard<std::mutex> lk(lock);
            stop = true;
        }

        cv.notify_all();
        for (auto& thread : workers) {
            thread.join();
        }
    }

    template<class F, class... Args>
    auto submit(F&& f, Args&&... args) -> std::future<std::invoke_result_t<F, Args...>> {
        
        // Find Return Type of f(args...)
        using ReturnType = std::invoke_result_t<F, Args...>;

        // Create Packaged Task
        std::packaged_task<ReturnType()> task(std::bind(std::forward<F>(f), std::forward<Args>(args)...));

        // Create Shared Ptr for Enqueue
        auto taskPtr = std::make_shared<std::packaged_task<ReturnType()>>(std::move(task));

        std::future<ReturnType> fut = taskPtr->get_future();

        {
            std::lock_guard<std::mutex> lk(lock);
            jobs.push([taskPtr] {
                (*taskPtr)();
            });
        }

        cv.notify_one();
        return fut;
    }

};

#endif // THREAD_POOL_H_