#pragma once
// Thin RAII wrappers over POSIX socket calls used by the Reactor.
namespace yewukv::net::sockops {

int CreateNonBlockingListenFd();
void SetReuseAddr(int fd, bool on);
void SetReusePort(int fd, bool on);
void SetTcpNoDelay(int fd, bool on);
void SetKeepAlive(int fd, bool on);
int Accept(int listen_fd, int* saved_errno);
void Close(int fd);

}  // namespace yewukv::net::sockops
