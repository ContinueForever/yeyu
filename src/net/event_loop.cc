#include "net/event_loop.h"

#include <sys/eventfd.h>
#include <unistd.h>

#include "common/logging.h"

namespace yewukv::net {

namespace {
int CreateWakeupFd() {
  int fd = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
  if (fd < 0) LOG_ERROR("eventloop", "eventfd failed");
  return fd;
}
}  // namespace

EventLoop::EventLoop()
    : tid_(std::this_thread::get_id()),
      wakeup_fd_(CreateWakeupFd()),
      wakeup_channel_(std::make_unique<Channel>(this, wakeup_fd_)) {
  wakeup_channel_->SetReadCallback([this] { HandleWakeup(); });
  wakeup_channel_->EnableReading();
}

EventLoop::~EventLoop() {
  wakeup_channel_.reset();
  if (wakeup_fd_ >= 0) ::close(wakeup_fd_);
}

void EventLoop::AssertInLoopThread() const {
  if (!InLoopThread()) {
    LOG_ERROR("eventloop", "called from a different thread!");
  }
}

void EventLoop::Loop() {
  looping_ = true;
  std::vector<Channel*> active;
  while (!quit_.load(std::memory_order_relaxed)) {
    active.clear();
    poller_.Poll(1000, &active);
    for (Channel* ch : active) ch->HandleEvent();
    DoPendingTasks();
  }
  looping_ = false;
}

void EventLoop::Quit() {
  quit_.store(true, std::memory_order_relaxed);
  if (!InLoopThread()) {
    uint64_t one = 1;
    ssize_t r = ::write(wakeup_fd_, &one, sizeof(one));
    (void)r;
  }
}

void EventLoop::RunInLoop(Task t) {
  if (InLoopThread()) {
    t();
  } else {
    QueueInLoop(std::move(t));
  }
}

void EventLoop::QueueInLoop(Task t) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    pending_.push_back(std::move(t));
  }
  // Also wake for tasks queued by a task: they run on the next loop iteration.
  uint64_t one = 1;
  ssize_t r = ::write(wakeup_fd_, &one, sizeof(one));
  (void)r;
}

void EventLoop::HandleWakeup() {
  DrainWakeup();
}

void EventLoop::DrainWakeup() {
  uint64_t v = 0;
  while (::read(wakeup_fd_, &v, sizeof(v)) > 0) {
  }
}

void EventLoop::DoPendingTasks() {
  std::vector<Task> tasks;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    tasks.swap(pending_);
  }
  for (auto& t : tasks) t();
}

}  // namespace yewukv::net
