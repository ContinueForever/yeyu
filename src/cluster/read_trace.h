#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <mutex>
#include <string>
#include <thread>
#include <variant>

#include "raft/persistence_trace.h"

namespace yewukv::cluster {

uint64_t TraceNow();

struct ReadTrace {
  uint64_t request_id = 0;
  uint64_t client_port = 0;
  uint64_t term = 0;
  uint64_t context = 0;
  uint64_t confirmed_by = 0;
  uint64_t confirmed_sequence = 0;
  uint64_t index = 0;
  uint64_t minimum_index = 0;
  uint64_t code = 0;
  uint64_t received_tid = 0;
  uint64_t handled_tid = 0;
  uint64_t tcp_tid = 0;
  uint64_t received_ns = 0;
  uint64_t handled_ns = 0;
  uint64_t context_ns = 0;
  uint64_t quorum_ns = 0;
  uint64_t ready_ns = 0;
  uint64_t response_queued_ns = 0;
  uint64_t tcp_loop_ns = 0;
  uint64_t tcp_dispatch_done_ns = 0;
};

struct PeerTrace {
  uint64_t peer_id = 0;
  uint64_t sequence = 0;
  uint64_t context = 0;
  uint64_t term = 0;
  uint64_t request_id = 0;
  uint64_t success = 0;
  uint64_t scheduled_tid = 0;
  uint64_t worker_tid = 0;
  uint64_t rpc_tid = 0;
  uint64_t node_tid = 0;
  uint64_t scheduled_ns = 0;
  uint64_t worker_ns = 0;
  uint64_t call_ns = 0;
  uint64_t first_write_ns = 0;
  uint64_t write_done_ns = 0;
  uint64_t decoded_ns = 0;
  uint64_t returned_ns = 0;
  uint64_t node_received_ns = 0;
};

// The critical paths only enqueue numeric records. Slow/full trace output
// drops diagnostics; it never blocks consensus on file I/O. Each process opens
// a new O_EXCL file. A regular <prefix>.enabled file containing 0 or 1 controls
// recording, checked by the writer every 250 ms, outside the request threads.
class ReadTraceRecorder {
 public:
  struct Options {
    size_t queue_capacity = 8192;
    uint64_t max_events = 500000;
    uint64_t max_bytes = 256 * 1024 * 1024;
    uint64_t sample_every = 32;
    uint64_t slow_ns = 10 * 1000 * 1000;
  };

  ReadTraceRecorder(std::string prefix, uint64_t node, Options options);
  ~ReadTraceRecorder();
  ReadTraceRecorder(const ReadTraceRecorder&) = delete;
  ReadTraceRecorder& operator=(const ReadTraceRecorder&) = delete;

  bool Enabled() const { return enabled_.load() && !failed_.load() && !limited_.load(); }
  bool KeepRead(uint64_t ordinal, const ReadTrace& trace) const;
  void Record(ReadTrace trace) noexcept;
  void Record(PeerTrace trace) noexcept;
  void Record(raft::PersistenceTrace trace) noexcept;
  const std::string& path() const { return path_; }
  uint64_t accepted() const { return accepted_.load(); }
  uint64_t written() const { return written_.load(); }
  uint64_t dropped() const { return dropped_.load(); }
  bool failed() const { return failed_.load(); }
  bool limited() const { return limited_.load(); }

 private:
  using Event = std::variant<ReadTrace, PeerTrace, raft::PersistenceTrace>;
  void Enqueue(Event event) noexcept;
  void Run();
  void CheckControl();
  bool Write(const std::string& bytes);
  std::string Encode(const Event& event) const;

  Options options_;
  uint64_t node_;
  std::string control_;
  std::string path_;
  int fd_ = -1;
  uint64_t bytes_ = 0;
  bool metadata_written_ = false;
  std::atomic<bool> enabled_{true};
  std::atomic<bool> failed_{false};
  std::atomic<bool> limited_{false};
  std::atomic<uint64_t> accepted_{0};
  std::atomic<uint64_t> written_{0};
  std::atomic<uint64_t> dropped_{0};
  std::mutex mutex_;
  std::condition_variable ready_;
  std::deque<Event> events_;
  bool stopping_ = false;
  std::thread writer_;
};

}  // namespace yewukv::cluster
