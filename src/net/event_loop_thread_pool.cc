#include "net/event_loop_thread_pool.h"

namespace yewukv::net {

EventLoopThreadPool::EventLoopThreadPool(EventLoop* base, std::string name, int num_workers)
    : base_(base), name_(std::move(name)), num_workers_(num_workers) {}

void EventLoopThreadPool::Start() {
  for (int i = 0; i < num_workers_; ++i) {
    auto t = std::make_unique<EventLoopThread>(name_ + "-" + std::to_string(i));
    loops_.push_back(t->StartLoop());
    threads_.push_back(std::move(t));
  }
}

EventLoop* EventLoopThreadPool::NextLoop() {
  if (loops_.empty()) return base_;
  EventLoop* l = loops_[next_ % loops_.size()];
  ++next_;
  return l;
}

void EventLoopThreadPool::Stop() {
  for (const auto& thread : threads_) thread->RequestStop();
  threads_.clear();  // destructors join after every loop was asked to quit
  loops_.clear();
  next_ = 0;
}

}  // namespace yewukv::net
