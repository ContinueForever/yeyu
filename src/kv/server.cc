#include "kv/server.h"

namespace yewukv::kv {

KvServer::KvServer(uint16_t port, int io_threads, std::shared_ptr<storage::KVEngine> engine,
                   std::chrono::seconds idle_timeout, size_t storage_capacity)
    : engine_(std::move(engine)),
      executor_(storage_capacity),
      server_(port, io_threads, idle_timeout) {
  server_.SetStopCallback([this] { executor_.StopAccepting(); });
  server_.SetBeforeWorkersStopCallback([this] { executor_.DrainAndStop(); });
  server_.SetMessageCallback(
      [this](const net::TcpConnection::Pointer& c, net::Buffer* b) { OnMessage(c, b); });
}

void KvServer::Start(int stop_fd) {
  server_.Start(stop_fd);
}

void KvServer::OnMessage(const net::TcpConnection::Pointer& conn, net::Buffer* buf) {
  std::string_view view(buf->Peek(), buf->ReadableBytes());
  size_t consumed_total = 0;
  while (conn->Connected() && !view.empty()) {
    const size_t newline = view.find('\n');
    if ((newline == std::string_view::npos &&
         view.size() >= net::TcpConnection::kMaxRequestBytes) ||
        (newline != std::string_view::npos && newline >= net::TcpConnection::kMaxRequestBytes)) {
      conn->ForceClose();
      return;
    }
    Command cmd;
    size_t n = TryParse(view, &cmd);
    if (n == 0) break;  // wait for more bytes (half packet)
    const auto id = conn->ReserveResponse();
    std::weak_ptr<net::TcpConnection> weak = conn;
    const auto status = executor_.Submit([this, weak, id, cmd = std::move(cmd)] {
      std::string reply;
      try {
        reply = Apply(cmd);
      } catch (...) {
        reply = "-ERR storage operation failed\n";
      }
      if (auto peer = weak.lock()) peer->SendOrdered(id, std::move(reply));
    });
    if (status.code() == StatusCode::kBusy) {
      conn->SendOrdered(id, "-ERR busy\n");
    } else if (status.code() == StatusCode::kShutdown) {
      conn->SendOrdered(id, "");
      conn->CloseAfterFlush();
      return;
    }
    if (!conn->Connected()) return;
    view.remove_prefix(n);
    consumed_total += n;
  }
  if (consumed_total > 0) buf->Consume(consumed_total);
}

std::string KvServer::Apply(const Command& cmd) {
  switch (cmd.type) {
    case Command::Type::kPing:
      return "+PONG\n";
    case Command::Type::kPut: {
      auto s = engine_->Put(cmd.key, cmd.value);
      return s.ok() ? "+OK\n" : "-ERR " + s.ToString() + "\n";
    }
    case Command::Type::kDel: {
      auto s = engine_->Delete(cmd.key);
      return s.ok() ? ":1\n" : "-ERR " + s.ToString() + "\n";
    }
    case Command::Type::kGet: {
      std::string value;
      auto s = engine_->Get(cmd.key, &value);
      if (s.ok()) return "$" + std::to_string(value.size()) + "\n" + value + "\n";
      if (s.code() == StatusCode::kNotFound) return "$-1\n";
      return "-ERR " + s.ToString() + "\n";
    }
    case Command::Type::kInvalid:
      return "-ERR bad command\n";
  }
  return "-ERR unknown\n";
}

}  // namespace yewukv::kv
