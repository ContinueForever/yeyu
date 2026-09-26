#pragma once
#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include "net/channel.h"
#include "net/poller.h"

namespace yewukv::net {

// One EventLoop per thread (Reactor). Owns a Poller and a queue of tasks that
// other threads hand over via RunInLoop/QueueInLoop; an eventfd wakes the loop.
class EventLoop {
 public:
  using Task = std::function<void()>;

  EventLoop();
  ~EventLoop();
  EventLoop(const EventLoop&) = delete;
  EventLoop& operator=(const EventLoop&) = delete;

  void Loop();
  void Quit();

  void RunInLoop(Task t);
  void QueueInLoop(Task t);

  void UpdateChannel(Channel* ch) { poller_.UpdateChannel(ch); }
  void RemoveChannel(Channel* ch) { poller_.RemoveChannel(ch); }

  bool InLoopThread() const { return tid_ == std::this_thread::get_id(); }
  void AssertInLoopThread() const;

 private:
  void HandleWakeup();
  void DrainWakeup();
  void DoPendingTasks();

  bool looping_ = false;
  std::atomic<bool> quit_{false};
  const std::thread::id tid_;
  Poller poller_;

  int wakeup_fd_;
  std::unique_ptr<Channel> wakeup_channel_;

  std::mutex mutex_;
  std::vector<Task> pending_;
};

}  // namespace yewukv::net
