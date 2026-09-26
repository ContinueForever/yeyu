#include "net/tcp_connection.h"

#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>

#include "common/logging.h"
#include "net/socket_ops.h"

namespace yewukv::net {

namespace {
ssize_t WriteSocket(int fd, const char* data, size_t size) {
  ssize_t n;
  do {
    n = ::send(fd, data, size, MSG_NOSIGNAL);
  } while (n < 0 && errno == EINTR);
  return n;
}
}  // namespace

TcpConnection::TcpConnection(EventLoop* loop, int fd, InetAddress peer)
    : loop_(loop), fd_(fd), peer_(std::move(peer)), channel_(loop, fd) {
  channel_.SetReadCallback([this] { HandleRead(); });
  channel_.SetWriteCallback([this] { HandleWrite(); });
  channel_.SetCloseCallback([this] { HandleClose(); });
  channel_.SetErrorCallback([this] { HandleError(); });
}

TcpConnection::~TcpConnection() {
  if (fd_ >= 0) sockops::Close(fd_);
}

void TcpConnection::Start() {
  if (!Connected()) return;
  channel_.Tie(shared_from_this());
  channel_.EnableReading();
  if (conn_cb_) conn_cb_(shared_from_this());
}

void TcpConnection::Send(std::string_view data) {
  if (!Connected() || data.empty()) return;
  auto self = shared_from_this();
  if (data.size() > kMaxOutputBytes) {
    loop_->RunInLoop([self] {
      if (self->accepting_sends_) self->HandleClose();
    });
    return;
  }
  loop_->RunInLoop([self, data = std::string(data)] { self->SendInLoop(data); });
}

uint64_t TcpConnection::ReserveResponse() {
  loop_->AssertInLoopThread();
  return next_response_id_++;
}

void TcpConnection::SendOrdered(uint64_t id, std::string data) {
  // A disconnected peer must not keep completed values or its event loop alive.
  if (!Connected()) return;
  auto self = shared_from_this();
  loop_->RunInLoop([self, id, data = std::move(data)]() mutable {
    self->SendOrderedInLoop(id, std::move(data));
  });
}

void TcpConnection::SendOrderedInLoop(uint64_t id, std::string data) {
  if (!Connected() || id < next_reply_id_ || id >= next_response_id_) return;
  ready_responses_.emplace(id, std::move(data));
  while (Connected()) {
    auto found = ready_responses_.find(next_reply_id_);
    if (found == ready_responses_.end()) break;
    std::string reply = std::move(found->second);
    ready_responses_.erase(found);
    ++next_reply_id_;
    if (!reply.empty()) SendInLoop(reply);
  }
  FinishWrite();
}

void TcpConnection::SendInLoop(std::string_view data) {
  if (!Connected() || !accepting_sends_) return;
  if (data.size() > kMaxOutputBytes - output_.ReadableBytes()) {
    HandleClose();
    return;
  }
  ssize_t nwrote = 0;
  size_t remaining = data.size();
  // If no data pending in output buffer, try writing directly first.
  if (output_.ReadableBytes() == 0) {
    nwrote = WriteSocket(fd_, data.data(), data.size());
    if (nwrote >= 0) {
      if (nwrote > 0) last_activity_ = std::chrono::steady_clock::now();
      remaining -= static_cast<size_t>(nwrote);
    } else if (errno != EWOULDBLOCK && errno != EAGAIN) {
      LOG_ERROR("conn", "write error fd=" + std::to_string(fd_));
      HandleClose();
      return;
    }
  }
  if (remaining > 0) {
    size_t sent = data.size() - remaining;
    output_.Append(data.data() + sent, remaining);
    if (!channel_.Writing()) channel_.EnableWriting();
  }
}

void TcpConnection::HandleRead() {
  if (!Connected() || !channel_.Reading()) return;
  // During server shutdown, keep draining the socket without executing more
  // commands. Closing with unread kernel data would send RST and lose replies.
  if (close_after_flush_) input_.Reset();
  if (input_.ReadableBytes() >= kMaxInputBytes) {
    HandleClose();
    return;
  }
  int saved = 0;
  ssize_t n = input_.ReadFd(fd_, &saved, kMaxInputBytes - input_.ReadableBytes());
  if (n > 0) {
    last_activity_ = std::chrono::steady_clock::now();
    if (close_after_flush_) {
      input_.Reset();
    } else if (message_cb_) {
      message_cb_(shared_from_this(), &input_);
    }
  } else if (n == 0) {
    // A peer may finish sending requests while it is still reading responses.
    peer_eof_ = true;
    channel_.DisableReading();
    CloseAfterFlushInLoop();
  } else if (saved == EAGAIN || saved == EWOULDBLOCK || saved == EINTR) {
    // LT will notify us again after an interrupted read or spurious wakeup.
  } else {
    LOG_ERROR("conn", "read error fd=" + std::to_string(fd_) + " err=" + std::to_string(saved));
    HandleClose();
  }
}

void TcpConnection::HandleWrite() {
  if (!Connected() || !channel_.Writing()) return;
  ssize_t n = WriteSocket(fd_, output_.Peek(), output_.ReadableBytes());
  if (n > 0) {
    last_activity_ = std::chrono::steady_clock::now();
    output_.Consume(static_cast<size_t>(n));
    if (output_.ReadableBytes() == 0) {
      channel_.DisableWriting();
      FinishWrite();
    }
  } else if (n == 0 || (errno != EWOULDBLOCK && errno != EAGAIN)) {
    LOG_ERROR("conn", "handleWrite error fd=" + std::to_string(fd_));
    HandleClose();
  }
}

void TcpConnection::HandleClose() {
  if (!connected_.exchange(false)) return;
  auto self = shared_from_this();
  accepting_sends_ = false;
  ready_responses_.clear();
  channel_.DisableAll();
  sockops::Close(fd_);
  fd_ = -1;
  if (close_cb_) close_cb_(self);
}

void TcpConnection::HandleError() {
  if (!Connected()) return;
  int err = 0;
  socklen_t len = sizeof(err);
  if (::getsockopt(fd_, SOL_SOCKET, SO_ERROR, &err, &len) < 0) err = errno;
  LOG_ERROR("conn", "connection error fd=" + std::to_string(fd_) + " err=" + std::to_string(err));
  HandleClose();
}

void TcpConnection::Shutdown() {
  auto self = shared_from_this();
  loop_->RunInLoop([self] { self->ShutdownInLoop(); });
}

void TcpConnection::ShutdownInLoop() {
  if (!Connected()) return;
  accepting_sends_ = false;
  shutdown_requested_ = true;
  FinishWrite();
}

void TcpConnection::CloseAfterFlush() {
  auto self = shared_from_this();
  loop_->RunInLoop([self] { self->CloseAfterFlushInLoop(); });
}

void TcpConnection::CloseAfterFlushInLoop() {
  if (!Connected()) return;
  close_after_flush_ = true;
  FinishWrite();
}

void TcpConnection::FinishWrite() {
  if (!Connected() || output_.ReadableBytes() != 0) return;
  if (close_after_flush_ && next_reply_id_ != next_response_id_) return;
  if (close_after_flush_) accepting_sends_ = false;
  if (close_after_flush_ && peer_eof_) {
    HandleClose();
  } else if ((shutdown_requested_ || close_after_flush_) && !write_shutdown_) {
    int result;
    do {
      result = ::shutdown(fd_, SHUT_WR);
    } while (result < 0 && errno == EINTR);
    if (result < 0) {
      HandleClose();
    } else {
      write_shutdown_ = true;
    }
  }
}

void TcpConnection::CloseIfIdle(std::chrono::seconds timeout) {
  auto self = shared_from_this();
  loop_->RunInLoop([self, timeout] {
    if (timeout.count() > 0 && self->Connected() &&
        std::chrono::steady_clock::now() - self->last_activity_ >= timeout) {
      self->HandleClose();
    }
  });
}

void TcpConnection::ForceClose() {
  auto self = shared_from_this();
  loop_->RunInLoop([self] { self->HandleClose(); });
}

}  // namespace yewukv::net
