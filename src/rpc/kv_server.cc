#include "rpc/kv_server.h"

#include <string>
#include <utility>

#include "rpc/frame_codec.h"

namespace yewukv::rpc {

namespace {
void SetStatus(const Status& status, KvResponse* response) {
  if (status.ok()) return;
  response->clear_value();
  // Status messages may contain arbitrary key bytes, whereas protobuf string
  // fields must be valid UTF-8. Keep wire diagnostics independent of key data.
  switch (status.code()) {
    case StatusCode::kNotFound:
      response->set_code(KvResponse::NOT_FOUND);
      response->set_error("key not found");
      break;
    case StatusCode::kBusy:
      response->set_code(KvResponse::BUSY);
      response->set_error(status.message().empty() ? "storage queue is full" : status.message());
      break;
    case StatusCode::kInvalidArgument:
      response->set_code(KvResponse::INVALID_ARGUMENT);
      response->set_error("invalid storage argument");
      break;
    default:
      response->set_code(KvResponse::INTERNAL);
      response->set_error("storage operation failed");
      break;
  }
}
}  // namespace

RpcKvServer::RpcKvServer(uint16_t port, int io_threads, std::shared_ptr<storage::KVEngine> engine,
                         std::chrono::seconds idle_timeout, size_t storage_capacity)
    : engine_(std::move(engine)),
      executor_(storage_capacity),
      server_(port, io_threads, idle_timeout) {
  server_.SetStopCallback([this] { executor_.StopAccepting(); });
  server_.SetBeforeWorkersStopCallback([this] { executor_.DrainAndStop(); });
  server_.SetMessageCallback([this](const net::TcpConnection::Pointer& conn, net::Buffer* buffer) {
    OnMessage(conn, buffer);
  });
}

void RpcKvServer::Start(int stop_fd) {
  server_.Start(stop_fd);
}

void RpcKvServer::OnMessage(const net::TcpConnection::Pointer& conn, net::Buffer* buffer) {
  while (conn->Connected()) {
    std::string payload;
    const auto decoded = FrameCodec::Decode(buffer, &payload);
    if (decoded == FrameCodec::DecodeStatus::kIncomplete) return;
    if (decoded == FrameCodec::DecodeStatus::kInvalid) {
      conn->ForceClose();
      return;
    }
    KvRequest request;
    if (!request.ParseFromString(payload)) {
      conn->ForceClose();
      return;
    }

    const auto id = conn->ReserveResponse();
    std::weak_ptr<net::TcpConnection> weak = conn;
    const uint64_t request_id = request.request_id();
    const auto status = executor_.Submit([this, weak, id, request = std::move(request)] {
      KvResponse response;
      try {
        response = Apply(request);
      } catch (...) {
        response.set_request_id(request.request_id());
        response.set_code(KvResponse::INTERNAL);
        response.set_error("storage operation failed");
      }
      if (auto peer = weak.lock()) SendResponse(peer, id, std::move(response));
    });
    if (status.code() == StatusCode::kShutdown) {
      conn->SendOrdered(id, "");
      conn->CloseAfterFlush();
      return;
    }
    if (!status.ok()) {
      KvResponse response;
      response.set_request_id(request_id);
      SetStatus(status, &response);
      SendResponse(conn, id, std::move(response));
    }
  }
}

void RpcKvServer::SendResponse(const net::TcpConnection::Pointer& conn, uint64_t id,
                               KvResponse response) {
  const size_t response_size = response.ByteSizeLong();
  if (response_size > FrameCodec::kMaxPayloadBytes ||
      response_size + FrameCodec::kHeaderBytes > net::TcpConnection::kMaxOutputBytes) {
    response.clear_value();
    response.set_code(KvResponse::INTERNAL);
    response.set_error("response exceeds maximum frame size");
  }
  std::string payload;
  if (!response.SerializeToString(&payload)) {
    conn->ForceClose();
    return;
  }
  conn->SendOrdered(id, FrameCodec::Encode(payload));
}

KvResponse RpcKvServer::Apply(const KvRequest& request) {
  KvResponse response;
  response.set_request_id(request.request_id());
  if (request.request_id() == 0) {
    response.set_code(KvResponse::INVALID_ARGUMENT);
    response.set_error("request_id must be nonzero");
    return response;
  }

  auto require_key = [&response](const std::string& key) {
    if (!key.empty()) return true;
    response.set_code(KvResponse::INVALID_ARGUMENT);
    response.set_error("key must not be empty");
    return false;
  };
  switch (request.operation_case()) {
    case KvRequest::kPut:
      if (require_key(request.put().key())) {
        SetStatus(engine_->Put(request.put().key(), request.put().value()), &response);
      }
      break;
    case KvRequest::kGet:
      if (require_key(request.get().key())) {
        std::string value;
        const auto status = engine_->Get(request.get().key(), &value);
        if (status.ok()) response.set_value(std::move(value));
        SetStatus(status, &response);
      }
      break;
    case KvRequest::kDel:
      if (require_key(request.del().key())) {
        SetStatus(engine_->Delete(request.del().key()), &response);
      }
      break;
    case KvRequest::kPing:
      break;
    case KvRequest::OPERATION_NOT_SET:
      response.set_code(KvResponse::INVALID_ARGUMENT);
      response.set_error("operation must be specified");
      break;
  }
  return response;
}

}  // namespace yewukv::rpc
