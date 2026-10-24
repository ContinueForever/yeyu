#include "net/channel.h"

#include <sys/epoll.h>

#include "net/event_loop.h"

namespace yewukv::net {

Channel::Channel(EventLoop* loop, int fd) : loop_(loop), fd_(fd) {}

void Channel::HandleEvent() {
  // A close callback may release the server's last reference to its connection.
  // Keep the owner (and this embedded Channel) alive until dispatch completes.
  std::shared_ptr<void> guard;
  if (tied_) {
    guard = owner_.lock();
    if (!guard) return;
  }
  if (events_ == kNoneEvent) return;
  if ((revents_ & EPOLLHUP) && !(revents_ & EPOLLIN)) {
    if (close_cb_) close_cb_();
    return;
  }
  if (revents_ & EPOLLERR) {
    if (error_cb_) error_cb_();
  }
  if (Reading() && (revents_ & (EPOLLIN | EPOLLPRI | EPOLLRDHUP))) {
    if (read_cb_) read_cb_();
  }
  if (Writing() && (revents_ & EPOLLOUT)) {
    if (write_cb_) write_cb_();
  }
}

void Channel::Update() {
  loop_->UpdateChannel(this);
}

}  // namespace yewukv::net
