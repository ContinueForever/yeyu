#pragma once
#include <atomic>
#include <memory>
#include <unordered_set>

#include "net/acceptor.h"
#include "net/event_loop.h"
#include "net/event_loop_thread_pool.h"
#include "net/tcp_connection.h"

namespace yewukv::net {

class TcpServer {
 public:
  TcpServer(uint16_t port, int worker_threads = 0,
            std::chrono::seconds idle_timeout = std::chrono::seconds{60});
  ~TcpServer();

  void SetMessageCallback(TcpConnection::MessageCallback cb) { message_cb_ = std::move(cb); }
  void SetStopCallback(std::function<void()> cb) { stop_cb_ = std::move(cb); }
  void SetBeforeWorkersStopCallback(std::function<void()> cb) {
    before_workers_stop_cb_ = std::move(cb);
  }
  // Blocks until Stop() or stop_fd becomes readable. The caller owns stop_fd.
  // Queued responses get a one-second drain deadline before forced closure.
  void Start(int stop_fd = -1);
  void Stop();

  static constexpr size_t kMaxConnections = 1024;

 private:
  void NewConnection(int fd, const InetAddress& peer);
  void RemoveConnection(const TcpConnection::Pointer& conn);
  void StopInLoop();
  void ForceCloseAll();
  void QuitIfStopped();
  std::vector<TcpConnection::Pointer> Connections();

  EventLoop base_loop_;
  InetAddress addr_;
  Acceptor acceptor_;
  EventLoopThreadPool pool_;
  TcpConnection::MessageCallback message_cb_;
  std::function<void()> stop_cb_;
  std::function<void()> before_workers_stop_cb_;
  std::mutex conn_mutex_;
  std::unordered_set<TcpConnection::Pointer> connections_;
  bool stopping_ = false;  // base loop only
  const std::chrono::seconds idle_timeout_;
  int idle_timer_fd_ = -1;
  std::unique_ptr<Channel> idle_timer_channel_;
  int shutdown_timer_fd_ = -1;
  std::unique_ptr<Channel> shutdown_timer_channel_;
};

}  // namespace yewukv::net
