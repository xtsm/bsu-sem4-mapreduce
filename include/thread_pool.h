#pragma once
#include <thread>
#include <functional>
#include <vector>
#include <mutex>
#include <condition_variable>

class ThreadPool {
 private:
  std::vector<std::thread> _threads;
  std::vector<bool> _finished;
  std::mutex _mutex;
  std::condition_variable _cv;
 public:
  ThreadPool();
  explicit ThreadPool(size_t thread_count);
  void Run(std::function<void()> runnable);
  void WaitForAll();
};
