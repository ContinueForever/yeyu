#include <gtest/gtest.h>

#include <arpa/inet.h>
#include <poll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <cstring>
#include <condition_variable>
#include <future>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>

#include "kv/server.h"
#include "net/buffer.h"
#include "rpc/frame_codec.h"
#include "rpc/kv_server.h"
#include "storage/engine.h"

namespace {
using namespace std::chrono_literals;

struct Fd {
  explicit Fd(int fd) : value(fd) {
    if (fd < 0) throw std::runtime_error("socket creation failed");
  }
  ~Fd() { ::close(value); }
  int value;
};

class BlockingEngine final : public yewukv::storage::KVEngine {
 public:
  yewukv::Status Put(std::string_view, std::string_view) override {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      entered_ = true;
    }
    cv_.notify_all();
    std::unique_lock<std::mutex> lock(mutex_);
    cv_.wait(lock, [this] { return released_; });
    return yewukv::Status::OK();
  }
  yewukv::Status Get(std::string_view, std::string*) override { return yewukv::Status::NotFound(); }
  yewukv::Status Delete(std::string_view) override { return yewukv::Status::OK(); }

  bool WaitEntered() {
    std::unique_lock<std::mutex> lock(mutex_);
    return cv_.wait_for(lock, 2s, [this] { return entered_; });
  }
  void Release() {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      released_ = true;
    }
    cv_.notify_all();
  }

 private:
  std::mutex mutex_;
  std::condition_variable cv_;
  bool entered_ = false;
  bool released_ = false;
};

uint16_t FreePort() {
  Fd sock(::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0));
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  if (::bind(sock.value, reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0) {
    throw std::runtime_error("bind failed");
  }
  socklen_t size = sizeof(address);
  if (::getsockname(sock.value, reinterpret_cast<sockaddr*>(&address), &size) != 0) {
    throw std::runtime_error("getsockname failed");
  }
  return ntohs(address.sin_port);
}

int Connect(uint16_t port) {
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  address.sin_port = htons(port);
  for (int attempt = 0; attempt < 200; ++attempt) {
    int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (fd < 0) throw std::runtime_error("socket failed");
    if (::connect(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) == 0) return fd;
    ::close(fd);
    std::this_thread::sleep_for(10ms);
  }
  throw std::runtime_error("server did not start");
}

void WriteAll(int fd, const std::string& bytes) {
  size_t offset = 0;
  while (offset < bytes.size()) {
    const auto count = ::send(fd, bytes.data() + offset, bytes.size() - offset, MSG_NOSIGNAL);
    if (count <= 0) throw std::runtime_error("send failed");
    offset += static_cast<size_t>(count);
  }
}

std::string ReadExactly(int fd, size_t length) {
  std::string bytes(length, '\0');
  size_t offset = 0;
  while (offset < length) {
    pollfd pfd{fd, POLLIN, 0};
    if (::poll(&pfd, 1, 2000) <= 0) throw std::runtime_error("read timed out");
    const auto count = ::recv(fd, bytes.data() + offset, length - offset, 0);
    if (count <= 0) throw std::runtime_error("connection closed early");
    offset += static_cast<size_t>(count);
  }
  return bytes;
}

yewukv::rpc::KvResponse ReadRpc(int fd) {
  const auto header = ReadExactly(fd, 4);
  uint32_t length;
  std::memcpy(&length, header.data(), 4);
  yewukv::rpc::KvResponse response;
  if (!response.ParseFromString(ReadExactly(fd, ntohl(length)))) {
    throw std::runtime_error("bad RPC response");
  }
  return response;
}

std::string RpcPut(uint64_t id) {
  yewukv::rpc::KvRequest request;
  request.set_request_id(id);
  request.mutable_put()->set_key("slow");
  request.mutable_put()->set_value("value");
  return yewukv::rpc::FrameCodec::Encode(request.SerializeAsString());
}

std::string RpcPing(uint64_t id) {
  yewukv::rpc::KvRequest request;
  request.set_request_id(id);
  request.mutable_ping();
  return yewukv::rpc::FrameCodec::Encode(request.SerializeAsString());
}

// The blocked storage operation makes the queue occupancy deterministic. A
// second connection must receive backpressure while the first connection's
// later reply must wait for the earlier PUT result.
void CheckBusyAndOrdering(bool rpc, int threads) {
  const uint16_t port = FreePort();
  Fd stop(::eventfd(0, EFD_CLOEXEC));
  auto engine = std::make_shared<BlockingEngine>();
  std::exception_ptr server_error;
  std::thread server([&] {
    try {
      if (rpc) {
        yewukv::rpc::RpcKvServer service(port, threads, engine, 60s, 1);
        service.Start(stop.value);
      } else {
        yewukv::kv::KvServer service(port, threads, engine, 60s, 1);
        service.Start(stop.value);
      }
    } catch (...) {
      server_error = std::current_exception();
    }
  });
  try {
    Fd first(Connect(port));
    WriteAll(first.value, rpc ? RpcPut(1) : "PUT slow value\n");
    if (!engine->WaitEntered()) throw std::runtime_error("storage did not block");
    Fd second(Connect(port));
    WriteAll(second.value, rpc ? RpcPing(3) : "PING\n");
    if (rpc) {
      const auto busy = ReadRpc(second.value);
      EXPECT_EQ(busy.request_id(), 3);
      EXPECT_EQ(busy.code(), yewukv::rpc::KvResponse::BUSY);
    } else {
      EXPECT_EQ(ReadExactly(second.value, 10), "-ERR busy\n");
    }
    WriteAll(first.value, rpc ? RpcPing(2) : "PING\n");
    pollfd pfd{first.value, POLLIN, 0};
    EXPECT_EQ(::poll(&pfd, 1, 100), 0);  // Busy response stays behind the PUT.
    // Stop accepting while the PUT is still running. Its reply and the later
    // BUSY response must drain before shutdown closes the write side.
    const uint64_t one = 1;
    ::write(stop.value, &one, sizeof(one));
    std::this_thread::sleep_for(20ms);
    engine->Release();
    if (rpc) {
      const auto put = ReadRpc(first.value);
      const auto busy = ReadRpc(first.value);
      EXPECT_EQ(put.request_id(), 1);
      EXPECT_EQ(put.code(), yewukv::rpc::KvResponse::OK);
      EXPECT_EQ(busy.request_id(), 2);
      EXPECT_EQ(busy.code(), yewukv::rpc::KvResponse::BUSY);
    } else {
      EXPECT_EQ(ReadExactly(first.value, 14), "+OK\n-ERR busy\n");
    }
  } catch (...) {
    engine->Release();
    const uint64_t one = 1;
    ::write(stop.value, &one, sizeof(one));
    server.join();
    throw;
  }
  const uint64_t one = 1;
  ::write(stop.value, &one, sizeof(one));
  server.join();
  if (server_error) std::rethrow_exception(server_error);
}

TEST(StorageServer, TextBusyAndResponseOrder) {
  for (int threads : {0, 4}) CheckBusyAndOrdering(false, threads);
}
TEST(StorageServer, RpcBusyAndResponseOrder) {
  for (int threads : {0, 4}) CheckBusyAndOrdering(true, threads);
}
}  // namespace
