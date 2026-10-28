#include "storage/executor.h"

#include <exception>
#include <stdexcept>
#include <utility>

#include "common/logging.h"

namespace yewukv::storage {

Executor::Executor(size_t capacity) : capacity_(capacity) {
  if (capacity == 0) throw std::invalid_argument("storage queue capacity must be positive");
  worker_ = std::thread([this] { Run(); });
}

Executor::~Executor() {
  DrainAndStop();
}

Status Executor::Submit(Task task) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (!accepting_) return Status::Shutdown("storage executor is stopping");
  if (outstanding_ == capacity_) return Status::Busy("storage queue is full");
  tasks_.push_back(std::move(task));
  ++outstanding_;
  ready_.notify_one();
  return Status::OK();
}

void Executor::StopAccepting() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    accepting_ = false;
  }
  ready_.notify_one();
}

void Executor::DrainAndStop() {
  StopAccepting();
  if (worker_.joinable()) worker_.join();
}

void Executor::Run() {
  for (;;) {
    Task task;
    {
      std::unique_lock<std::mutex> lock(mutex_);
      ready_.wait(lock, [this] { return !tasks_.empty() || !accepting_; });
      if (tasks_.empty()) return;
      task = std::move(tasks_.front());
      tasks_.pop_front();
    }
    try {
      task();
    } catch (const std::exception& error) {
      LOG_ERROR("storage", std::string("executor task failed: ") + error.what());
    } catch (...) {
      LOG_ERROR("storage", "executor task failed with unknown exception");
    }
    {
      std::lock_guard<std::mutex> lock(mutex_);
      --outstanding_;
    }
  }
}

}  // namespace yewukv::storage
