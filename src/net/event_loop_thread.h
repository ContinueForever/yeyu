#pragma once
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

#include "net/event_loop.h"

namespace yewukv::net {

// Runs one EventLoop in its own std::thread.
class EventLoopThread {
 public:
  explicit EventLoopThread(std::string name = "loop");
  ~EventLoopThread();

  EventLoop* StartLoop();
  void RequestStop();

 private:
  void ThreadFunc();

  std::string name_;
  std::thread thread_;
  EventLoop* loop_ = nullptr;
  std::mutex mutex_;
  std::condition_variable cv_;
};

}  // namespace yewukv::net
