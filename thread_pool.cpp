#include "include/thread_pool.h"
#include <mutex>
#include <iostream>

ThreadPool::ThreadPool(size_t thread_count) :
    _threads(thread_count),
    _finished(thread_count, true),
    _mutex(),
    _cv() {}

ThreadPool::ThreadPool() : ThreadPool(std::thread::hardware_concurrency()) {}

void ThreadPool::Run(std::function<void()> runnable) {
  std::unique_lock<std::mutex> lock(_mutex);
  _cv.wait(lock, [this]() {
    return std::find(_finished.begin(), _finished.end(), true)
        != _finished.end();
  });
  size_t slot_num = std::find(_finished.begin(), _finished.end(), true)
      - _finished.begin();
  _finished[slot_num] = false;
  if (_threads[slot_num].joinable()) {
    _threads[slot_num].join();
  }

  _threads[slot_num] = std::thread([this, slot_num, runnable]() {
    runnable();
    {
      std::lock_guard<std::mutex> lock(_mutex);
      _finished[slot_num] = true;
    }
    _cv.notify_one();
  });
}

void ThreadPool::WaitForAll() {
  std::unique_lock<std::mutex> lock(_mutex);
  _cv.wait(lock, [this]() {
    return std::find(_finished.begin(), _finished.end(), false)
        == _finished.end();
  });
  for (auto& thread : _threads) {
    if (thread.joinable()) {
      thread.join();
    }
  }
}
