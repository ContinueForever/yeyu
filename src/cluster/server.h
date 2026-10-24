#pragma once

#include <cstdint>
#include <cstddef>
#include <map>
#include <memory>
#include <string>

namespace yewukv::cluster {

struct Endpoint {
  std::string host;
  uint16_t port = 0;
};

struct Options {
  uint64_t id = 0;
  uint16_t port = 0;
  std::string listen_host = "127.0.0.1";
  std::map<uint64_t, Endpoint> members;
  std::string directory;
  uint32_t seed = 1;
  int request_timeout_ms = 1500;
  size_t proposal_batch_size = 32;
  size_t snapshot_entries = 128;
  size_t snapshot_bytes = 1024 * 1024;
  size_t max_state_bytes = 64 * 1024 * 1024;
  size_t event_queue_capacity = 4096;
  size_t pending_capacity = 1024;
  bool journal = true;
  bool read_index = true;
  bool adaptive_snapshots = true;
  size_t journal_checkpoint_bytes = 4 * 1024 * 1024;
  std::string trace_prefix;
  uint64_t trace_sample_every = 32;
  uint64_t trace_slow_ms = 10;
  uint64_t trace_max_events = 500000;
};

class Server {
 public:
  explicit Server(Options options);
  ~Server();
  Server(const Server&) = delete;
  Server& operator=(const Server&) = delete;
  bool Run(int stop_fd);

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace yewukv::cluster
