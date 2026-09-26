#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <future>
#include <memory>
#include <string>

#include "common/status.h"
#include "kv.pb.h"

namespace yewukv::rpc {

// One reusable TCP connection, with independently timed in-flight calls.
// Numeric IPv4 addresses only; construction starts a nonblocking connection.
// There is no reconnect/retry: a failed connection permanently fails this client.
// Result::ok() describes transport success; KvResponse::code() describes the KV
// operation. A timeout cannot prove that a write was not applied remotely.
class KvClient {
 public:
  struct Options {
    size_t max_pending_requests = 1024;
    size_t max_queued_bytes = 1024 * 1024;
    std::chrono::milliseconds connect_timeout{3000};
  };

  KvClient(std::string ipv4_address, uint16_t port);
  KvClient(std::string ipv4_address, uint16_t port, Options options);
  ~KvClient();

  KvClient(const KvClient&) = delete;
  KvClient& operator=(const KvClient&) = delete;
  KvClient(KvClient&&) = delete;
  KvClient& operator=(KvClient&&) = delete;

  // Safe to call from multiple threads. The supplied request_id is replaced by
  // an internally allocated nonzero ID. The deadline includes connection time
  // and local queuing; an expired call is not retried automatically.
  std::future<Result<KvResponse>> CallAsync(KvRequest request, std::chrono::milliseconds timeout);

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace yewukv::rpc
