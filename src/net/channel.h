#pragma once
#include <sys/epoll.h>

#include <functional>
#include <memory>

namespace yewukv::net {

class EventLoop;

// A Channel owns the interest set and callbacks for one fd. It does not own
// the fd; the owner (TcpConnection / Acceptor / timer) is responsible for close.
class Channel {
 public:
  using EventCallback = std::function<void()>;

  Channel(EventLoop* loop, int fd);

  void HandleEvent();
  void Tie(const std::shared_ptr<void>& owner) {
    owner_ = owner;
    tied_ = true;
  }

  void SetReadCallback(EventCallback cb) { read_cb_ = std::move(cb); }
  void SetWriteCallback(EventCallback cb) { write_cb_ = std::move(cb); }
  void SetCloseCallback(EventCallback cb) { close_cb_ = std::move(cb); }
  void SetErrorCallback(EventCallback cb) { error_cb_ = std::move(cb); }

  void EnableReading() {
    events_ |= kReadEvent;
    Update();
  }
  void EnableWriting() {
    events_ |= kWriteEvent;
    Update();
  }
  void DisableReading() {
    events_ &= ~kReadEvent;
    Update();
  }
  void DisableWriting() {
    events_ &= ~kWriteEvent;
    Update();
  }
  void DisableAll() {
    events_ = 0;
    Update();
  }

  bool Writing() const { return events_ & kWriteEvent; }
  bool Reading() const { return events_ & kReadEvent; }
  int Fd() const { return fd_; }
  uint32_t Events() const { return events_; }
  void SetRevents(uint32_t e) { revents_ = e; }
  EventLoop* OwnerLoop() const { return loop_; }

  static constexpr uint32_t kNoneEvent = 0;
  static constexpr uint32_t kReadEvent = EPOLLIN | EPOLLPRI;
  static constexpr uint32_t kWriteEvent = EPOLLOUT;

 private:
  void Update();

  EventLoop* loop_;
  const int fd_;
  uint32_t events_ = kNoneEvent;
  uint32_t revents_ = 0;
  std::weak_ptr<void> owner_;
  bool tied_ = false;

  EventCallback read_cb_;
  EventCallback write_cb_;
  EventCallback close_cb_;
  EventCallback error_cb_;
};

}  // namespace yewukv::net
