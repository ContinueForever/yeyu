#include <gtest/gtest.h>

#include <arpa/inet.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <cerrno>
#include <cstring>
#include <exception>
#include <functional>
#include <future>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>

#include "net/buffer.h"
#include "rpc/client.h"
#include "rpc/frame_codec.h"

namespace {

using namespace std::chrono_literals;
using yewukv::Result;
using yewukv::StatusCode;
using yewukv::rpc::FrameCodec;
using yewukv::rpc::KvClient;
using yewukv::rpc::KvRequest;
using yewukv::rpc::KvResponse;

class TestFd {
 public:
  explicit TestFd(int fd) : fd_(fd) {
    if (fd < 0) throw std::runtime_error(std::string("socket: ") + std::strerror(errno));
  }
  ~TestFd() { ::close(fd_); }
  TestFd(const TestFd&) = delete;
  TestFd& operator=(const TestFd&) = delete;
  int get() const { return fd_; }

 private:
  int fd_;
};

uint16_t BindLocal(int fd) {
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  if (::bind(fd, reinterpret_cast<const sockaddr*>(&address), sizeof(address)) < 0) {
    throw std::runtime_error(std::string("bind: ") + std::strerror(errno));
  }
  socklen_t size = sizeof(address);
  if (::getsockname(fd, reinterpret_cast<sockaddr*>(&address), &size) < 0) {
    throw std::runtime_error("getsockname failed");
  }
  return ntohs(address.sin_port);
}

void WaitReadable(int fd) {
  pollfd pfd{fd, POLLIN, 0};
  int ready;
  do {
    ready = ::poll(&pfd, 1, 2500);
  } while (ready < 0 && errno == EINTR);
  if (ready <= 0) throw std::runtime_error("fake peer timed out waiting for client input");
}

std::string ReadExactly(int fd, size_t bytes) {
  std::string output(bytes, '\0');
  size_t offset = 0;
  const auto deadline = std::chrono::steady_clock::now() + 2500ms;
  while (offset < bytes) {
    if (std::chrono::steady_clock::now() >= deadline) {
      throw std::runtime_error("fake peer timed out reading a frame");
    }
    const auto received = ::recv(fd, output.data() + offset, bytes - offset, 0);
    if (received > 0) {
      offset += static_cast<size_t>(received);
    } else if (received < 0 && errno == EINTR) {
      continue;
    } else {
      throw std::runtime_error("fake peer could not read the complete request");
    }
  }
  return output;
}

KvRequest ReadRequest(int fd) {
  auto header = ReadExactly(fd, FrameCodec::kHeaderBytes);
  uint32_t network_length;
  std::memcpy(&network_length, header.data(), sizeof(network_length));
  const uint32_t length = ntohl(network_length);
  if (length == 0 || length > FrameCodec::kMaxPayloadBytes) {
    throw std::runtime_error("client emitted an invalid frame length");
  }
  yewukv::net::Buffer buffer;
  buffer.Append(header);
  buffer.Append(ReadExactly(fd, length));
  std::string payload;
  if (FrameCodec::Decode(&buffer, &payload) != FrameCodec::DecodeStatus::kComplete ||
      buffer.ReadableBytes() != 0) {
    throw std::runtime_error("client emitted an invalid frame");
  }
  KvRequest request;
  if (!request.ParseFromString(payload) || request.request_id() == 0) {
    throw std::runtime_error("client emitted an invalid protobuf request");
  }
  return request;
}

void WriteAll(int fd, const std::string& data) {
  size_t offset = 0;
  const auto deadline = std::chrono::steady_clock::now() + 2500ms;
  while (offset < data.size()) {
    if (std::chrono::steady_clock::now() >= deadline) {
      throw std::runtime_error("fake peer timed out writing a response");
    }
    const auto sent = ::send(fd, data.data() + offset, data.size() - offset, MSG_NOSIGNAL);
    if (sent > 0) {
      offset += static_cast<size_t>(sent);
    } else if (sent < 0 && errno == EINTR) {
      continue;
    } else {
      throw std::runtime_error("fake peer could not send the response");
    }
  }
}

void WaitForClientClose(int fd) {
  char byte;
  ssize_t result;
  do {
    result = ::recv(fd, &byte, 1, 0);
  } while (result < 0 && errno == EINTR);
  if (result != 0) throw std::runtime_error("expected the RPC client to close its socket");
}

std::string ResponseFor(const KvRequest& request) {
  KvResponse response;
  response.set_request_id(request.request_id());
  response.set_value(request.get().key());
  return FrameCodec::Encode(response.SerializeAsString());
}

// Each test owns a private listener and one accepted socket. IO has bounded
// timeouts, and both normal completion and failed assertions join the worker.
class FakePeer {
 public:
  explicit FakePeer(std::function<void(int)> script)
      : listener_(::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0)),
        port_(BindLocal(listener_.get())) {
    if (::listen(listener_.get(), 1) < 0) throw std::runtime_error("listen failed");
    worker_ = std::thread([this, script = std::move(script)] {
      try {
        WaitReadable(listener_.get());
        TestFd client(::accept4(listener_.get(), nullptr, nullptr, SOCK_CLOEXEC));
        timeval timeout{2, 500000};
        if (::setsockopt(client.get(), SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) < 0 ||
            ::setsockopt(client.get(), SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout)) < 0) {
          throw std::runtime_error("setting fake peer socket timeout failed");
        }
        script(client.get());
      } catch (...) {
        error_ = std::current_exception();
      }
    });
  }

  ~FakePeer() {
    ::shutdown(listener_.get(), SHUT_RDWR);
    if (worker_.joinable()) worker_.join();
  }

  uint16_t port() const { return port_; }

  void Finish() {
    if (worker_.joinable()) worker_.join();
    if (error_) std::rethrow_exception(error_);
  }

 private:
  TestFd listener_;
  uint16_t port_;
  std::thread worker_;
  std::exception_ptr error_;
};

KvRequest Get(std::string key) {
  KvRequest request;
  request.set_request_id(999);  // CallAsync must replace a caller-supplied ID.
  request.mutable_get()->set_key(std::move(key));
  return request;
}

TEST(RpcClient, ConcurrentCallsMatchResponsesReturnedInReverseOrder) {
  FakePeer peer([](int fd) {
    const auto first = ReadRequest(fd);
    const auto second = ReadRequest(fd);
    if (first.request_id() == second.request_id() || first.request_id() == 999 ||
        second.request_id() == 999) {
      throw std::runtime_error("request IDs were not uniquely allocated");
    }
    const auto responses = ResponseFor(second) + ResponseFor(first);
    WriteAll(fd, responses.substr(0, 3));  // Include a split length prefix.
    WriteAll(fd, responses.substr(3));
  });
  KvClient client("127.0.0.1", peer.port());
  auto submit_first =
      std::async(std::launch::async, [&] { return client.CallAsync(Get("first"), 2s); });
  auto submit_second =
      std::async(std::launch::async, [&] { return client.CallAsync(Get("second"), 2s); });
  auto first = submit_first.get();
  auto second = submit_second.get();
  ASSERT_EQ(first.wait_for(3s), std::future_status::ready);
  ASSERT_EQ(second.wait_for(3s), std::future_status::ready);
  const auto first_result = first.get();
  const auto second_result = second.get();
  ASSERT_TRUE(first_result.ok()) << first_result.status().ToString();
  ASSERT_TRUE(second_result.ok()) << second_result.status().ToString();
  EXPECT_EQ(first_result.value().value(), "first");
  EXPECT_EQ(second_result.value().value(), "second");
  EXPECT_NE(first_result.value().request_id(), second_result.value().request_id());
  EXPECT_NO_THROW(peer.Finish());
}

TEST(RpcClient, MissingResponseTimesOutAndLateResponseDoesNotCompleteAnotherCall) {
  FakePeer peer([](int fd) {
    const auto expired = ReadRequest(fd);
    const auto current = ReadRequest(fd);
    WriteAll(fd, ResponseFor(expired) + ResponseFor(current));
  });
  KvClient client("127.0.0.1", peer.port());
  auto expired = client.CallAsync(Get("expired"), 300ms);
  ASSERT_EQ(expired.wait_for(2s), std::future_status::ready);
  EXPECT_EQ(expired.get().status().code(), StatusCode::kTimedOut);
  auto current = client.CallAsync(Get("current"), 2s);
  ASSERT_EQ(current.wait_for(3s), std::future_status::ready);
  const auto result = current.get();
  ASSERT_TRUE(result.ok()) << result.status().ToString();
  EXPECT_EQ(result.value().value(), "current");
  EXPECT_NO_THROW(peer.Finish());
}

TEST(RpcClient, PeerDisconnectFailsOutstandingCallsAndDoesNotReconnect) {
  FakePeer peer([](int fd) { ReadRequest(fd); });
  KvClient client("127.0.0.1", peer.port());
  auto pending = client.CallAsync(Get("missing"), 2s);
  ASSERT_EQ(pending.wait_for(3s), std::future_status::ready);
  EXPECT_EQ(pending.get().status().code(), StatusCode::kIOError);
  auto after_disconnect = client.CallAsync(Get("again"), 2s);
  ASSERT_EQ(after_disconnect.wait_for(500ms), std::future_status::ready);
  EXPECT_EQ(after_disconnect.get().status().code(), StatusCode::kIOError);
  EXPECT_NO_THROW(peer.Finish());
}

TEST(RpcClient, DestructorCompletesPendingCallWithShutdown) {
  std::promise<void> received;
  auto received_future = received.get_future();
  FakePeer peer([&](int fd) {
    ReadRequest(fd);
    received.set_value();
    WaitForClientClose(fd);
  });
  std::future<Result<KvResponse>> pending;
  {
    KvClient client("127.0.0.1", peer.port());
    pending = client.CallAsync(Get("pending"), 30s);
    ASSERT_EQ(received_future.wait_for(2s), std::future_status::ready);
  }
  ASSERT_EQ(pending.wait_for(500ms), std::future_status::ready);
  EXPECT_EQ(pending.get().status().code(), StatusCode::kShutdown);
  EXPECT_NO_THROW(peer.Finish());
}

TEST(RpcClient, ConnectionFailureReturnsIOError) {
  // A bound but non-listening port cannot be claimed by another process.
  TestFd reserved(::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0));
  KvClient client("127.0.0.1", BindLocal(reserved.get()));
  auto pending = client.CallAsync(Get("key"), 2s);
  ASSERT_EQ(pending.wait_for(3s), std::future_status::ready);
  EXPECT_EQ(pending.get().status().code(), StatusCode::kIOError);
}

TEST(RpcClient, PendingLimitRejectsExcessCallsWithoutSendingThem) {
  std::promise<void> received;
  auto received_future = received.get_future();
  FakePeer peer([&](int fd) {
    ReadRequest(fd);
    received.set_value();
    WaitForClientClose(fd);
  });
  KvClient::Options options;
  options.max_pending_requests = 1;
  {
    KvClient client("127.0.0.1", peer.port(), options);
    auto pending = client.CallAsync(Get("pending"), 30s);
    ASSERT_EQ(received_future.wait_for(2s), std::future_status::ready);
    auto excess = client.CallAsync(Get("excess"), 2s);
    ASSERT_EQ(excess.wait_for(500ms), std::future_status::ready);
    EXPECT_EQ(excess.get().status().code(), StatusCode::kBusy);
  }
  EXPECT_NO_THROW(peer.Finish());
}

TEST(RpcClient, InvalidAddressIsReportedWithoutBlockingDNS) {
  KvClient client("not-a-numeric-ipv4", 9000);
  auto pending = client.CallAsync(Get("key"), 2s);
  ASSERT_EQ(pending.wait_for(500ms), std::future_status::ready);
  EXPECT_EQ(pending.get().status().code(), StatusCode::kInvalidArgument);
}

}  // namespace
