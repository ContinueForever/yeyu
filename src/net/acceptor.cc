#include "net/acceptor.h"

#include <sys/socket.h>

#include <cerrno>
#include <system_error>

#include "common/logging.h"
#include "net/socket_ops.h"

namespace yewukv::net {

Acceptor::Acceptor(EventLoop* loop, InetAddress listen_addr, bool reuse_port)
    : loop_(loop),
      listen_fd_(sockops::CreateNonBlockingListenFd()),
      addr_(std::move(listen_addr)),
      channel_(loop, listen_fd_) {
  if (listen_fd_ < 0) throw std::system_error(errno, std::generic_category(), "socket");
  sockops::SetReuseAddr(listen_fd_, true);
  sockops::SetReusePort(listen_fd_, reuse_port);
  sockops::SetTcpNoDelay(listen_fd_, true);
  channel_.SetReadCallback([this] { HandleRead(); });

  if (::bind(listen_fd_, addr_.Addr(), sizeof(sockaddr_in)) < 0) {
    int error = errno;
    sockops::Close(listen_fd_);
    throw std::system_error(error, std::generic_category(), "bind " + addr_.ToString());
  }
}

Acceptor::~Acceptor() {
  Stop();
}

void Acceptor::Stop() {
  if (listening_) channel_.DisableAll();
  listening_ = false;
  if (listen_fd_ >= 0) {
    sockops::Close(listen_fd_);
    listen_fd_ = -1;
  }
}

void Acceptor::Listen() {
  if (::listen(listen_fd_, SOMAXCONN) < 0) {
    throw std::system_error(errno, std::generic_category(), "listen " + addr_.ToString());
  }
  listening_ = true;
  channel_.EnableReading();
  LOG_INFO("acceptor", "listening on " + addr_.ToString());
}

void Acceptor::HandleRead() {
  if (!listening_) return;
  for (;;) {
    int saved = 0;
    int fd = sockops::Accept(listen_fd_, &saved);
    if (fd >= 0) {
      sockops::SetTcpNoDelay(fd, true);
      sockops::SetKeepAlive(fd, true);
      if (new_conn_cb_) {
        // Peer address is not captured by our Accept wrapper; report empty.
        InetAddress peer;
        new_conn_cb_(fd, peer);
      } else {
        sockops::Close(fd);
      }
    } else {
      if (saved == EINTR) continue;
      if (saved != EAGAIN && saved != EWOULDBLOCK) {
        LOG_WARN("acceptor", "accept failed errno=" + std::to_string(saved));
      }
      break;
    }
  }
}

}  // namespace yewukv::net
