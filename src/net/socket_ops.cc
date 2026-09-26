#include "net/socket_ops.h"

#include <cerrno>

#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

namespace yewukv::net::sockops {

int CreateNonBlockingListenFd() {
  int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, IPPROTO_TCP);
  return fd;
}

void SetReuseAddr(int fd, bool on) {
  int v = on ? 1 : 0;
  ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &v, sizeof(v));
}

void SetReusePort(int fd, bool on) {
  int v = on ? 1 : 0;
  ::setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &v, sizeof(v));
}

void SetTcpNoDelay(int fd, bool on) {
  int v = on ? 1 : 0;
  ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &v, sizeof(v));
}

void SetKeepAlive(int fd, bool on) {
  int v = on ? 1 : 0;
  ::setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &v, sizeof(v));
}

int Accept(int listen_fd, int* saved_errno) {
  sockaddr_in peer{};
  socklen_t len = sizeof(peer);
  int fd =
      ::accept4(listen_fd, reinterpret_cast<sockaddr*>(&peer), &len, SOCK_NONBLOCK | SOCK_CLOEXEC);
  if (fd < 0) *saved_errno = errno;
  return fd;
}

void Close(int fd) {
  ::close(fd);
}

}  // namespace yewukv::net::sockops
