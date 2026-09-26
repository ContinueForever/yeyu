#include "net/tcp_server.h"

#include <sys/timerfd.h>
#include <unistd.h>

#include <cerrno>
#include <system_error>
#include <stdexcept>

#include "common/logging.h"
#include "net/socket_ops.h"

namespace yewukv::net {

TcpServer::TcpServer(uint16_t port, int worker_threads, std::chrono::seconds idle_timeout)
    : addr_(port),
      acceptor_(&base_loop_, addr_),
      pool_(&base_loop_, "io-worker", worker_threads),
      idle_timeout_(idle_timeout) {
  if (idle_timeout_.count() < 0) throw std::invalid_argument("idle timeout must be nonnegative");
  acceptor_.SetNewConnectionCallback(
      [this](int fd, const InetAddress& peer) { NewConnection(fd, peer); });
}

TcpServer::~TcpServer() {
  acceptor_.Stop();
  pool_.Stop();
  if (idle_timer_channel_) idle_timer_channel_->DisableAll();
  if (idle_timer_fd_ >= 0) ::close(idle_timer_fd_);
  if (shutdown_timer_channel_) shutdown_timer_channel_->DisableAll();
  if (shutdown_timer_fd_ >= 0) ::close(shutdown_timer_fd_);
}

void TcpServer::Start(int stop_fd) {
  if (idle_timeout_.count() > 0) {
    idle_timer_fd_ = ::timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
    if (idle_timer_fd_ < 0) {
      throw std::system_error(errno, std::generic_category(), "idle timerfd_create");
    }
    idle_timer_channel_ = std::make_unique<Channel>(&base_loop_, idle_timer_fd_);
    idle_timer_channel_->SetReadCallback([this] {
      uint64_t expirations;
      ssize_t n;
      do {
        n = ::read(idle_timer_fd_, &expirations, sizeof(expirations));
      } while (n < 0 && errno == EINTR);
      if (n != sizeof(expirations) || stopping_) return;
      for (const auto& conn : Connections()) conn->CloseIfIdle(idle_timeout_);
    });
    itimerspec interval{};
    interval.it_value.tv_sec = 1;
    interval.it_interval.tv_sec = 1;
    if (::timerfd_settime(idle_timer_fd_, 0, &interval, nullptr) < 0) {
      throw std::system_error(errno, std::generic_category(), "idle timerfd_settime");
    }
    idle_timer_channel_->EnableReading();
  }
  shutdown_timer_fd_ = ::timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
  if (shutdown_timer_fd_ < 0) {
    throw std::system_error(errno, std::generic_category(), "timerfd_create");
  }
  shutdown_timer_channel_ = std::make_unique<Channel>(&base_loop_, shutdown_timer_fd_);
  shutdown_timer_channel_->SetReadCallback([this] {
    uint64_t expirations;
    ssize_t n;
    do {
      n = ::read(shutdown_timer_fd_, &expirations, sizeof(expirations));
    } while (n < 0 && errno == EINTR);
    ForceCloseAll();
  });
  shutdown_timer_channel_->EnableReading();

  std::unique_ptr<Channel> stop_channel;
  if (stop_fd >= 0) {
    stop_channel = std::make_unique<Channel>(&base_loop_, stop_fd);
    stop_channel->SetReadCallback([this, &stop_channel] {
      stop_channel->DisableAll();
      StopInLoop();
    });
    stop_channel->EnableReading();
  }
  acceptor_.Listen();
  pool_.Start();
  base_loop_.Loop();
  if (stop_channel) stop_channel->DisableAll();
  shutdown_timer_channel_->DisableAll();
  if (idle_timer_channel_) idle_timer_channel_->DisableAll();
  // Storage can finish its accepted work while I/O worker loops still exist.
  if (before_workers_stop_cb_) before_workers_stop_cb_();
  pool_.Stop();
}

void TcpServer::Stop() {
  base_loop_.RunInLoop([this] { StopInLoop(); });
}

std::vector<TcpConnection::Pointer> TcpServer::Connections() {
  std::lock_guard<std::mutex> lock(conn_mutex_);
  return {connections_.begin(), connections_.end()};
}

void TcpServer::StopInLoop() {
  if (stopping_) return;
  stopping_ = true;
  acceptor_.Stop();
  if (stop_cb_) stop_cb_();
  for (const auto& conn : Connections()) conn->CloseAfterFlush();

  itimerspec deadline{};
  deadline.it_value.tv_sec = 1;
  if (::timerfd_settime(shutdown_timer_fd_, 0, &deadline, nullptr) < 0) {
    LOG_ERROR("server", "cannot arm shutdown deadline; closing connections");
    ForceCloseAll();
  }
  QuitIfStopped();
}

void TcpServer::ForceCloseAll() {
  for (const auto& conn : Connections()) conn->ForceClose();
  QuitIfStopped();
}

void TcpServer::QuitIfStopped() {
  std::lock_guard<std::mutex> lock(conn_mutex_);
  if (stopping_ && connections_.empty()) base_loop_.Quit();
}

void TcpServer::NewConnection(int fd, const InetAddress& peer) {
  if (stopping_) {
    sockops::Close(fd);
    return;
  }
  EventLoop* io = pool_.NextLoop();
  TcpConnection::Pointer conn;
  {
    std::lock_guard<std::mutex> lock(conn_mutex_);
    if (connections_.size() >= kMaxConnections) {
      sockops::Close(fd);
      return;
    }
    conn = std::make_shared<TcpConnection>(io, fd, peer);
    connections_.insert(conn);
  }
  conn->SetMessageCallback(message_cb_);
  conn->SetCloseCallback([this](const TcpConnection::Pointer& c) {
    // Other callbacks can close a Channel still in this epoll batch. Preserve
    // its owner until the entire batch has finished dispatching.
    c->Loop()->QueueInLoop([this, c] { RemoveConnection(c); });
  });
  // Start() must run on the connection's own loop thread.
  io->RunInLoop([conn] { conn->Start(); });
}

void TcpServer::RemoveConnection(const TcpConnection::Pointer& conn) {
  {
    std::lock_guard<std::mutex> lock(conn_mutex_);
    connections_.erase(conn);
  }
  base_loop_.QueueInLoop([this] { QuitIfStopped(); });
}

}  // namespace yewukv::net
