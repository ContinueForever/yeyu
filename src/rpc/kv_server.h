#pragma once

#include <cstdint>
#include <memory>

#include "kv.pb.h"
#include "net/tcp_server.h"
#include "storage/engine.h"
#include "storage/executor.h"

namespace yewukv::rpc {

// Serves one length-prefixed protobuf protocol per TCP listener. Requests are
// correlated by request_id; disconnects do not cause any automatic replay.
class RpcKvServer {
 public:
  RpcKvServer(uint16_t port, int io_threads, std::shared_ptr<storage::KVEngine> engine,
              std::chrono::seconds idle_timeout = std::chrono::seconds{60},
              size_t storage_capacity = 4096);
  void Start(int stop_fd = -1);

 private:
  void OnMessage(const net::TcpConnection::Pointer& conn, net::Buffer* buffer);
  KvResponse Apply(const KvRequest& request);
  void SendResponse(const net::TcpConnection::Pointer& conn, uint64_t id, KvResponse response);

  std::shared_ptr<storage::KVEngine> engine_;
  storage::Executor executor_;
  net::TcpServer server_;
};

}  // namespace yewukv::rpc
