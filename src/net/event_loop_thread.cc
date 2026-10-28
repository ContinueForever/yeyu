#include "net/event_loop_thread.h"

namespace yewukv::net {

EventLoopThread::EventLoopThread(std::string name) : name_(std::move(name)) {}

EventLoopThread::~EventLoopThread() {
  RequestStop();
  if (thread_.joinable()) thread_.join();
}

void EventLoopThread::RequestStop() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (loop_) loop_->Quit();
}

EventLoop* EventLoopThread::StartLoop() {
  thread_ = std::thread([this] { ThreadFunc(); });
  std::unique_lock<std::mutex> lock(mutex_);
  cv_.wait(lock, [this] { return loop_ != nullptr; });
  return loop_;
}

void EventLoopThread::ThreadFunc() {
  EventLoop loop;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    loop_ = &loop;
    cv_.notify_one();
  }
  loop.Loop();
  std::lock_guard<std::mutex> lock(mutex_);
  loop_ = nullptr;
}

}  // namespace yewukv::net
