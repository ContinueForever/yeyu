#pragma once
#include <sys/epoll.h>
#include <unordered_map>
#include <vector>

#include "net/channel.h"

namespace yewukv::net {

// Thin epoll(7) wrapper: owns the epoll fd and the active-event array.
class Poller {
 public:
  using ChannelList = std::vector<Channel*>;

  Poller();
  ~Poller();
  Poller(const Poller&) = delete;
  Poller& operator=(const Poller&) = delete;

  void UpdateChannel(Channel* ch);
  void RemoveChannel(Channel* ch);
  void Poll(int timeout_ms, ChannelList* active);

 private:
  static const char* OpName(int op);

  int epoll_fd_;
  std::vector<epoll_event> events_;
  std::unordered_map<int, Channel*> channels_;
};

}  // namespace yewukv::net
