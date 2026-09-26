#pragma once
#include <memory>
#include <string>
#include <vector>

#include "net/event_loop.h"
#include "net/event_loop_thread.h"

namespace yewukv::net {

// "one loop per thread": the acceptor runs in the base loop, accepted
// connections are round-robined across N worker loops.
class EventLoopThreadPool {
 public:
  EventLoopThreadPool(EventLoop* base, std::string name, int num_workers);

  void Start();
  void Stop();
  EventLoop* NextLoop();

 private:
  EventLoop* base_;
  std::string name_;
  int num_workers_;
  std::vector<std::unique_ptr<EventLoopThread>> threads_;
  std::vector<EventLoop*> loops_;
  uint32_t next_ = 0;
};

}  // namespace yewukv::net
