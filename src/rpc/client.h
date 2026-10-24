#pragma once

#include <chrono>
#include <atomic>
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

  // Use a fresh instance per call. Atomics also cover a timed-out call whose partially
  // written frame can still finish after the future has become ready.
  struct CallTiming {
    std::atomic<uint64_t> request_id{0};
    std::atomic<uint64_t> io_tid{0};
    std::atomic<uint64_t> call_ns{0};
    std::atomic<uint64_t> first_write_ns{0};
    std::atomic<uint64_t> write_done_ns{0};
    std::atomic<uint64_t> decoded_ns{0};
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
  std::future<Result<KvResponse>> CallAsync(KvRequest request, std::chrono::milliseconds timeout,
                                            std::shared_ptr<CallTiming> timing = {});

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace yewukv::rpc
