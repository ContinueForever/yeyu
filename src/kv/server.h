#pragma once
#include <cstdint>
#include <memory>

#include "kv/text_protocol.h"
#include "net/buffer.h"
#include "net/tcp_connection.h"
#include "net/tcp_server.h"
#include "storage/engine.h"
#include "storage/executor.h"

namespace yewukv::kv {

// Glues the Reactor network layer to the KV engine using the text protocol.
class KvServer {
 public:
  KvServer(uint16_t port, int io_threads, std::shared_ptr<storage::KVEngine> engine,
           std::chrono::seconds idle_timeout = std::chrono::seconds{60},
           size_t storage_capacity = 4096);
  void Start(int stop_fd = -1);

 private:
  void OnMessage(const net::TcpConnection::Pointer& conn, net::Buffer* buf);
  std::string Apply(const Command& cmd);

  std::shared_ptr<storage::KVEngine> engine_;
  storage::Executor executor_;
  net::TcpServer server_;
};

}  // namespace yewukv::kv
