#pragma once
#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <map>
#include <string>

#include "net/buffer.h"
#include "net/channel.h"
#include "net/event_loop.h"
#include "net/inet_address.h"

namespace yewukv::net {

class EventLoop;

// One accepted connection: owns its fd and Channel, does non-blocking IO with
// application buffers. Callbacks run on the owning loop thread.
class TcpConnection : public std::enable_shared_from_this<TcpConnection> {
 public:
  using Pointer = std::shared_ptr<TcpConnection>;
  using MessageCallback = std::function<void(const Pointer&, Buffer*)>;
  using ConnectionCallback = std::function<void(const Pointer&)>;
  using CloseCallback = std::function<void(const Pointer&)>;

  static constexpr size_t kMaxRequestBytes = 64 * 1024;  // Includes the text protocol's newline.
  static constexpr size_t kMaxInputBytes = 128 * 1024;
  static constexpr size_t kMaxOutputBytes = 1024 * 1024;

  TcpConnection(EventLoop* loop, int fd, InetAddress peer);
  ~TcpConnection();

  void SetMessageCallback(MessageCallback cb) { message_cb_ = std::move(cb); }
  void SetConnectionCallback(ConnectionCallback cb) { conn_cb_ = std::move(cb); }
  void SetCloseCallback(CloseCallback cb) { close_cb_ = std::move(cb); }

  void Start();
  void Send(std::string_view data);
  // Service callbacks allocate IDs on this connection's event loop. A worker
  // may complete out of order; responses are emitted in request order.
  uint64_t ReserveResponse();
  void SendOrdered(uint64_t id, std::string data);
  // Reject new sends and perform SHUT_WR once queued output drains.
  void Shutdown();
  // Stop processing requests and reject new sends. Flush output, send EOF, and
  // discard further input until peer EOF; the server enforces a close deadline.
  void CloseAfterFlush();
  void ForceClose();
  // Recheck activity and close on the owning loop; safe from other threads.
  void CloseIfIdle(std::chrono::seconds timeout);

  EventLoop* Loop() const { return loop_; }
  int Fd() const { return fd_; }
  bool Connected() const { return connected_.load(); }
  const InetAddress& PeerAddress() const { return peer_; }

 private:
  void HandleRead();
  void HandleWrite();
  void HandleClose();
  void HandleError();
  void SendInLoop(std::string_view data);
  void SendOrderedInLoop(uint64_t id, std::string data);
  void ShutdownInLoop();
  void CloseAfterFlushInLoop();
  void FinishWrite();

  EventLoop* loop_;
  int fd_;
  InetAddress peer_;
  Channel channel_;
  Buffer input_;
  Buffer output_;
  std::atomic<bool> connected_{true};
  std::chrono::steady_clock::time_point last_activity_ = std::chrono::steady_clock::now();
  uint64_t next_response_id_ = 0;
  uint64_t next_reply_id_ = 0;
  std::map<uint64_t, std::string> ready_responses_;
  bool accepting_sends_ = true;
  bool shutdown_requested_ = false;
  bool write_shutdown_ = false;
  bool close_after_flush_ = false;
  bool peer_eof_ = false;

  MessageCallback message_cb_;
  ConnectionCallback conn_cb_;
  CloseCallback close_cb_;
};

}  // namespace yewukv::net
