#pragma once
#include <arpa/inet.h>
#include <netinet/in.h>
#include <string>

namespace yewukv::net {

class InetAddress {
 public:
  InetAddress() = default;
  explicit InetAddress(uint16_t port, bool loopback_only = false) {
    addr_.sin_family = AF_INET;
    addr_.sin_addr.s_addr = htonl(loopback_only ? INADDR_LOOPBACK : INADDR_ANY);
    addr_.sin_port = htons(port);
  }
  InetAddress(std::string ip, uint16_t port) {
    addr_.sin_family = AF_INET;
    addr_.sin_port = htons(port);
    ::inet_pton(AF_INET, ip.c_str(), &addr_.sin_addr);
  }

  const sockaddr_in& SockAddr() const { return addr_; }
  sockaddr* Addr() { return reinterpret_cast<sockaddr*>(&addr_); }
  socklen_t AddrLen() const { return sizeof(sockaddr_in); }

  uint16_t Port() const { return ntohs(addr_.sin_port); }
  std::string Ip() const {
    char buf[INET_ADDRSTRLEN];
    ::inet_ntop(AF_INET, &addr_.sin_addr, buf, sizeof(buf));
    return buf;
  }
  std::string ToString() const { return Ip() + ":" + std::to_string(Port()); }

 private:
  sockaddr_in addr_{};
};

}  // namespace yewukv::net
