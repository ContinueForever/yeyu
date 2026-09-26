#pragma once
#include <functional>

#include "net/channel.h"
#include "net/event_loop.h"
#include "net/inet_address.h"

namespace yewukv::net {

// Listens on a port and hands accepted fds to NewConnectionCallback.
class Acceptor {
 public:
  using NewConnectionCallback = std::function<void(int fd, const InetAddress& peer)>;

  Acceptor(EventLoop* loop, InetAddress listen_addr, bool reuse_port = false);
  ~Acceptor();

  void SetNewConnectionCallback(NewConnectionCallback cb) { new_conn_cb_ = std::move(cb); }
  void Listen();
  void Stop();

 private:
  void HandleRead();

  EventLoop* loop_;
  int listen_fd_;
  InetAddress addr_;
  Channel channel_;
  NewConnectionCallback new_conn_cb_;
  bool listening_ = false;
};

}  // namespace yewukv::net
