#pragma once

#include <condition_variable>
#include <cstddef>
#include <deque>
#include <functional>
#include <mutex>
#include <thread>

#include "common/status.h"

namespace yewukv::storage {

// Executes accepted operations in one FIFO order. The capacity counts both
// the running operation and queued operations; Submit never waits for storage.
class Executor {
 public:
  using Task = std::function<void()>;

  explicit Executor(size_t capacity);
  ~Executor();
  Executor(const Executor&) = delete;
  Executor& operator=(const Executor&) = delete;

  Status Submit(Task task);
  void StopAccepting();
  // Complete all accepted tasks and join the worker. Idempotent; called after
  // the network loop stops but before its worker loops are destroyed.
  void DrainAndStop();

 private:
  void Run();

  const size_t capacity_;
  std::mutex mutex_;
  std::condition_variable ready_;
  std::deque<Task> tasks_;
  size_t outstanding_ = 0;
  bool accepting_ = true;
  std::thread worker_;
};

}  // namespace yewukv::storage
