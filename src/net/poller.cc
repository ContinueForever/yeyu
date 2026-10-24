#include "net/poller.h"

#include <unistd.h>

#include <cerrno>

#include "common/logging.h"

namespace yewukv::net {

Poller::Poller() : epoll_fd_(::epoll_create1(EPOLL_CLOEXEC)), events_(64) {}

Poller::~Poller() {
  if (epoll_fd_ >= 0) ::close(epoll_fd_);
}

void Poller::UpdateChannel(Channel* ch) {
  int op;
  auto it = channels_.find(ch->Fd());
  if (it == channels_.end() && ch->Events() == Channel::kNoneEvent) return;
  if (it == channels_.end()) {
    op = EPOLL_CTL_ADD;
    channels_[ch->Fd()] = ch;
  } else if (ch->Events() == Channel::kNoneEvent) {
    op = EPOLL_CTL_DEL;
    channels_.erase(it);
  } else {
    op = EPOLL_CTL_MOD;
  }

  epoll_event ev{};
  ev.events = ch->Events();
  if (ch->Reading()) ev.events |= EPOLLRDHUP;
  ev.data.ptr = ch;
  if (::epoll_ctl(epoll_fd_, op, ch->Fd(), &ev) < 0) {
    LOG_ERROR("poller", std::string("epoll_ctl ") + OpName(op) + " failed fd=" +
                            std::to_string(ch->Fd()) + " errno=" + std::to_string(errno));
  }
}

void Poller::RemoveChannel(Channel* ch) {
  if (channels_.erase(ch->Fd()) > 0) {
    ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, ch->Fd(), nullptr);
  }
}

void Poller::Poll(int timeout_ms, ChannelList* active) {
  int n = ::epoll_wait(epoll_fd_, events_.data(), static_cast<int>(events_.size()), timeout_ms);
  if (n > 0) {
    for (int i = 0; i < n; ++i) {
      auto* ch = static_cast<Channel*>(events_[i].data.ptr);
      ch->SetRevents(events_[i].events);
      active->push_back(ch);
    }
    if (n == static_cast<int>(events_.size())) events_.resize(events_.size() * 2);
  } else if (n < 0 && errno != EINTR) {
    LOG_ERROR("poller", "epoll_wait failed errno=" + std::to_string(errno));
  }
}

const char* Poller::OpName(int op) {
  switch (op) {
    case EPOLL_CTL_ADD:
      return "ADD";
    case EPOLL_CTL_MOD:
      return "MOD";
    case EPOLL_CTL_DEL:
      return "DEL";
    default:
      return "?";
  }
}

}  // namespace yewukv::net
