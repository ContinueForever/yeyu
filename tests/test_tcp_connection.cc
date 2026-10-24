#include <gtest/gtest.h>

#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <thread>

#include "net/tcp_connection.h"

TEST(TcpConnection, IdleDeadlineAlsoClosesStalledDrain) {
  // A small send buffer and a peer that never reads guarantee pending output.
  // EOF or CloseAfterFlush must not exempt stalled output from idle eviction.
  int fds[2];
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds), 0);
  struct Peer {
    int fd;
    ~Peer() { ::close(fd); }
  } peer{fds[1]};
  yewukv::net::EventLoop loop;
  auto conn =
      std::make_shared<yewukv::net::TcpConnection>(&loop, fds[0], yewukv::net::InetAddress(0));
  int size = 4096;
  ASSERT_EQ(::setsockopt(fds[0], SOL_SOCKET, SO_SNDBUF, &size, sizeof(size)), 0);
  conn->Start();
  conn->Send(std::string(128 * 1024, 'x'));
  conn->CloseAfterFlush();
  EXPECT_TRUE(conn->Connected());
  conn->CloseIfIdle(std::chrono::seconds{1});
  EXPECT_TRUE(conn->Connected());
  std::this_thread::sleep_for(std::chrono::milliseconds{1100});
  conn->CloseIfIdle(std::chrono::seconds{1});
  EXPECT_FALSE(conn->Connected());
  // Repeated checks are harmless after descriptor removal.
  conn->CloseIfIdle(std::chrono::seconds{1});
}

TEST(TcpConnection, OutOfOrderCompletionsDrainInRequestOrderAfterHalfClose) {
  int fds[2];
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds), 0);
  struct Peer {
    int fd;
    ~Peer() { ::close(fd); }
  } peer{fds[1]};
  yewukv::net::EventLoop loop;
  auto conn =
      std::make_shared<yewukv::net::TcpConnection>(&loop, fds[0], yewukv::net::InetAddress(0));
  conn->Start();
  const auto first = conn->ReserveResponse();
  const auto second = conn->ReserveResponse();
  conn->CloseAfterFlush();
  conn->SendOrdered(second, "second\n");
  char buffer[32];
  EXPECT_EQ(::recv(peer.fd, buffer, sizeof(buffer), 0), -1);
  EXPECT_EQ(errno, EAGAIN);
  conn->SendOrdered(first, "first\n");
  const auto count = ::recv(peer.fd, buffer, sizeof(buffer), 0);
  ASSERT_EQ(count, 13);
  EXPECT_EQ(std::string(buffer, count), "first\nsecond\n");
  EXPECT_EQ(::recv(peer.fd, buffer, sizeof(buffer), 0), 0);
}

TEST(TcpConnection, WaitingForFirstReplyCannotAccumulateUnboundedCompletedResponses) {
  int fds[2];
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds), 0);
  yewukv::net::EventLoop loop;
  auto conn =
      std::make_shared<yewukv::net::TcpConnection>(&loop, fds[0], yewukv::net::InetAddress(0));
  conn->Start();
  conn->ReserveResponse();  // Deliberately never complete the first request.
  const auto second = conn->ReserveResponse();
  const auto third = conn->ReserveResponse();
  conn->SendOrdered(second, std::string(yewukv::net::TcpConnection::kMaxOutputBytes, 'x'));
  EXPECT_TRUE(conn->Connected());
  conn->SendOrdered(third, "one-byte-over-budget");
  EXPECT_FALSE(conn->Connected());
  ::close(fds[1]);
}

TEST(TcpConnection, OutstandingRequestCountIsBoundedEvenForTinyResponses) {
  int fds[2];
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds), 0);
  yewukv::net::EventLoop loop;
  auto conn =
      std::make_shared<yewukv::net::TcpConnection>(&loop, fds[0], yewukv::net::InetAddress(0));
  conn->Start();
  for (size_t i = 0; i < yewukv::net::TcpConnection::kMaxOutstandingResponses; ++i)
    EXPECT_NE(conn->ReserveResponse(), UINT64_MAX);
  EXPECT_TRUE(conn->Connected());
  EXPECT_EQ(conn->ReserveResponse(), UINT64_MAX);
  EXPECT_FALSE(conn->Connected());
  ::close(fds[1]);
}
