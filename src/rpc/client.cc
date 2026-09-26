#include "rpc/client.h"

#include <arpa/inet.h>
#include <poll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <climits>
#include <cstring>
#include <deque>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <utility>

#include "net/buffer.h"
#include "rpc/frame_codec.h"

namespace yewukv::rpc {
namespace {

using Clock = std::chrono::steady_clock;

class OwnedFd {
 public:
  explicit OwnedFd(int fd = -1) : fd_(fd) {}
  ~OwnedFd() {
    if (fd_ >= 0) ::close(fd_);
  }
  OwnedFd(const OwnedFd&) = delete;
  OwnedFd& operator=(const OwnedFd&) = delete;
  int get() const { return fd_; }

 private:
  int fd_;
};

Status SocketError(const char* operation, int error) {
  return Status::IOError(std::string(operation) + ": " + std::strerror(error));
}

Clock::time_point Deadline(Clock::time_point now, std::chrono::milliseconds timeout) {
  const auto max_timeout =
      std::chrono::duration_cast<std::chrono::milliseconds>(Clock::time_point::max() - now);
  return now + std::min(timeout, max_timeout);
}

}  // namespace

struct KvClient::Impl {
  struct Pending {
    Clock::time_point deadline;
    std::promise<Result<KvResponse>> promise;
  };
  struct Outbound {
    uint64_t id;
    std::string frame;
    size_t offset = 0;
  };

  Impl(const std::string& address, uint16_t port, Options options)
      : options_(options), wake_fd_(::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC)) {
    address_.sin_family = AF_INET;
    address_.sin_port = htons(port);
    if (::inet_pton(AF_INET, address.c_str(), &address_.sin_addr) != 1 || port == 0) {
      FailAll(
          Status::InvalidArgument("RPC client requires a numeric IPv4 address and nonzero port"));
      return;
    }
    if (options_.max_pending_requests == 0 || options_.max_queued_bytes == 0 ||
        options_.connect_timeout.count() <= 0) {
      FailAll(Status::InvalidArgument("RPC client limits and connection timeout must be positive"));
      return;
    }
    if (wake_fd_.get() < 0) {
      FailAll(SocketError("eventfd", errno));
      return;
    }
    worker_ = std::thread([this] {
      try {
        Run();
      } catch (const std::exception& error) {
        std::lock_guard<std::mutex> lock(mutex_);
        FailAll(Status::IOError(std::string("RPC client worker failed: ") + error.what()));
      }
    });
  }

  ~Impl() {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      stopping_ = true;
    }
    Wake();
    if (worker_.joinable()) worker_.join();
  }

  std::future<Result<KvResponse>> Call(KvRequest request, std::chrono::milliseconds timeout) {
    const auto now = Clock::now();
    std::promise<Result<KvResponse>> promise;
    auto future = promise.get_future();
    auto reject = [&](Status status) {
      promise.set_value(Result<KvResponse>(std::move(status)));
      return std::move(future);
    };
    std::lock_guard<std::mutex> lock(mutex_);
    if (stopping_) return reject(Status::Shutdown("RPC client is stopping"));
    if (failed_) return reject(failure_);
    if (timeout.count() <= 0) return reject(Status::TimedOut("RPC request timed out"));
    if (pending_.size() >= options_.max_pending_requests) {
      return reject(Status::Busy("RPC pending request limit reached"));
    }
    // Never recycle an ID, including on overflow: an old response must never
    // accidentally complete a different call.
    if (next_id_ == 0) return reject(Status::Busy("RPC request ID space exhausted"));
    const uint64_t id = next_id_++;
    request.set_request_id(id);
    if (request.ByteSizeLong() > FrameCodec::kMaxPayloadBytes) {
      return reject(Status::InvalidArgument("RPC request exceeds the frame size limit"));
    }
    std::string payload;
    if (!request.SerializeToString(&payload)) {
      return reject(Status::InvalidArgument("RPC request serialization failed"));
    }
    std::string frame = FrameCodec::Encode(payload);
    if (frame.size() > options_.max_queued_bytes ||
        queued_bytes_ > options_.max_queued_bytes - frame.size()) {
      return reject(Status::Busy("RPC output queue limit reached"));
    }
    pending_.emplace(id, Pending{Deadline(now, timeout), std::move(promise)});
    queued_bytes_ += frame.size();
    outbound_.push_back(Outbound{id, std::move(frame), 0});
    Wake();
    return future;
  }

  void Wake() {
    if (wake_fd_.get() < 0) return;
    const uint64_t one = 1;
    ssize_t result;
    do {
      result = ::write(wake_fd_.get(), &one, sizeof(one));
    } while (result < 0 && errno == EINTR);
    // EAGAIN means an earlier notification is already waiting for the worker.
  }

  // The worker holds mutex_ while updating requests or performing bounded,
  // nonblocking IO. It never holds the mutex during poll.
  void FailAll(Status status) {
    failed_ = true;
    failure_ = std::move(status);
    for (auto& [id, pending] : pending_) pending.promise.set_value(failure_);
    pending_.clear();
    outbound_.clear();
    queued_bytes_ = 0;
  }

  void Expire(Clock::time_point now) {
    for (auto it = pending_.begin(); it != pending_.end();) {
      if (it->second.deadline <= now) {
        it->second.promise.set_value(Status::TimedOut("RPC request timed out"));
        it = pending_.erase(it);
      } else {
        ++it;
      }
    }
    for (auto it = outbound_.begin(); it != outbound_.end();) {
      if (it->offset == 0 && pending_.find(it->id) == pending_.end()) {
        queued_bytes_ -= it->frame.size();
        it = outbound_.erase(it);
      } else {
        // A partially sent frame must finish even after its call expires, or
        // subsequent requests would corrupt the shared stream's framing.
        ++it;
      }
    }
  }

  int PollTimeout(bool connected, Clock::time_point connect_deadline) const {
    auto deadline = connected ? Clock::time_point::max() : connect_deadline;
    for (const auto& [id, pending] : pending_) deadline = std::min(deadline, pending.deadline);
    if (deadline == Clock::time_point::max()) return -1;
    const auto wait = std::chrono::ceil<std::chrono::milliseconds>(deadline - Clock::now()).count();
    return static_cast<int>(std::clamp<int64_t>(wait, 0, INT_MAX));
  }

  bool WriteRequests(int fd) {
    size_t budget = 256 * 1024;
    while (!outbound_.empty() && budget > 0) {
      auto& output = outbound_.front();
      const size_t count = std::min(budget, output.frame.size() - output.offset);
      const ssize_t sent = ::send(fd, output.frame.data() + output.offset, count, MSG_NOSIGNAL);
      if (sent > 0) {
        output.offset += static_cast<size_t>(sent);
        queued_bytes_ -= static_cast<size_t>(sent);
        budget -= static_cast<size_t>(sent);
        if (output.offset == output.frame.size()) outbound_.pop_front();
      } else if (sent < 0 && errno == EINTR) {
        continue;
      } else if (sent < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
        return true;
      } else {
        FailAll(sent == 0 ? Status::IOError("RPC socket made no write progress")
                          : SocketError("RPC send", errno));
        return false;
      }
    }
    return true;
  }

  bool ReadResponses(int fd, net::Buffer* input) {
    size_t budget = 256 * 1024;
    char chunk[16384];
    while (budget > 0) {
      const ssize_t count = ::recv(fd, chunk, std::min(sizeof(chunk), budget), 0);
      if (count > 0) {
        budget -= static_cast<size_t>(count);
        input->Append(chunk, static_cast<size_t>(count));
        while (true) {
          std::string payload;
          const auto decoded = FrameCodec::Decode(input, &payload);
          if (decoded == FrameCodec::DecodeStatus::kIncomplete) break;
          if (decoded == FrameCodec::DecodeStatus::kInvalid) {
            FailAll(Status::Corruption("Invalid RPC response frame"));
            return false;
          }
          KvResponse response;
          if (!response.ParseFromString(payload) || response.request_id() == 0) {
            FailAll(Status::Corruption("Invalid RPC response protobuf or request ID"));
            return false;
          }
          auto found = pending_.find(response.request_id());
          if (found == pending_.end()) continue;  // A late response to an expired call.
          if (Clock::now() >= found->second.deadline) {
            found->second.promise.set_value(Status::TimedOut("RPC request timed out"));
          } else {
            found->second.promise.set_value(std::move(response));
          }
          pending_.erase(found);
        }
        // At most one incomplete frame plus a single recv chunk is buffered;
        // invalid length prefixes are rejected as soon as the header arrives.
      } else if (count == 0) {
        FailAll(Status::IOError("RPC peer disconnected"));
        return false;
      } else if (errno == EINTR) {
        continue;
      } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
        return true;
      } else {
        FailAll(SocketError("RPC recv", errno));
        return false;
      }
    }
    return true;
  }

  void Run() {
    OwnedFd socket(::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0));
    bool connected = false;
    const auto connect_deadline = Deadline(Clock::now(), options_.connect_timeout);
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (socket.get() < 0) {
        FailAll(SocketError("RPC socket", errno));
        return;
      }
      if (::connect(socket.get(), reinterpret_cast<const sockaddr*>(&address_), sizeof(address_)) ==
          0) {
        connected = true;
      } else if (errno != EINPROGRESS) {
        FailAll(SocketError("RPC connect", errno));
        return;
      }
    }
    net::Buffer input;
    while (true) {
      pollfd fds[2] = {{wake_fd_.get(), POLLIN, 0}, {socket.get(), POLLIN, 0}};
      int timeout;
      {
        std::lock_guard<std::mutex> lock(mutex_);
        if (stopping_) {
          FailAll(Status::Shutdown("RPC client is stopping"));
          return;
        }
        const auto now = Clock::now();
        Expire(now);
        if (!connected && now >= connect_deadline) {
          FailAll(Status::TimedOut("RPC connection timed out"));
          return;
        }
        if (!connected || !outbound_.empty()) fds[1].events |= POLLOUT;
        timeout = PollTimeout(connected, connect_deadline);
      }
      const int ready = ::poll(fds, 2, timeout);
      if (ready < 0 && errno == EINTR) continue;
      const int poll_error = errno;
      std::lock_guard<std::mutex> lock(mutex_);
      if (stopping_) {
        FailAll(Status::Shutdown("RPC client is stopping"));
        return;
      }
      if (ready < 0) {
        FailAll(SocketError("RPC poll", poll_error));
        return;
      }
      if (fds[0].revents & POLLIN) {
        uint64_t value;
        while (::read(wake_fd_.get(), &value, sizeof(value)) < 0 && errno == EINTR) {
        }
      }
      Expire(Clock::now());
      if (fds[1].revents & (POLLOUT | POLLERR | POLLHUP | POLLNVAL)) {
        int error = 0;
        socklen_t size = sizeof(error);
        if (::getsockopt(socket.get(), SOL_SOCKET, SO_ERROR, &error, &size) < 0) error = errno;
        if (error != 0 || (fds[1].revents & POLLNVAL)) {
          FailAll(SocketError("RPC connection", error != 0 ? error : EBADF));
          return;
        }
        connected = true;
      }
      if (connected && (fds[1].revents & (POLLIN | POLLHUP))) {
        if (!ReadResponses(socket.get(), &input)) return;
      }
      if (connected && (fds[1].revents & POLLOUT)) {
        if (!WriteRequests(socket.get())) return;
      }
    }
  }

  const Options options_;
  sockaddr_in address_{};
  OwnedFd wake_fd_;
  std::thread worker_;
  std::mutex mutex_;
  bool stopping_ = false;
  bool failed_ = false;
  Status failure_;
  uint64_t next_id_ = 1;
  size_t queued_bytes_ = 0;
  std::unordered_map<uint64_t, Pending> pending_;
  std::deque<Outbound> outbound_;
};

KvClient::KvClient(std::string address, uint16_t port)
    : KvClient(std::move(address), port, Options{}) {}

KvClient::KvClient(std::string address, uint16_t port, Options options)
    : impl_(std::make_unique<Impl>(address, port, options)) {}

KvClient::~KvClient() = default;

std::future<Result<KvResponse>> KvClient::CallAsync(KvRequest request,
                                                    std::chrono::milliseconds timeout) {
  return impl_->Call(std::move(request), timeout);
}

}  // namespace yewukv::rpc
