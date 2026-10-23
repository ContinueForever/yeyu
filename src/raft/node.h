#pragma once

#include <array>
#include <cstdint>
#include <map>
#include <functional>
#include <memory>
#include <optional>
#include <random>
#include <vector>

#include "raft/file_store.h"

namespace yewukv::raft {

enum class MessageKind { kVote, kVoteReply, kAppend, kAppendReply, kSnapshot, kSnapshotReply };

struct SnapshotChunk {
  uint64_t index = 0;
  uint64_t term = 0;
  uint64_t offset = 0;
  uint64_t total = 0;
  std::string data;
  bool done = false;
};

struct Message {
  MessageKind kind = MessageKind::kVote;
  uint64_t from = 0;
  uint64_t to = 0;
  uint64_t sequence = 0;
  uint64_t term = 0;
  uint64_t last_index = 0;
  uint64_t last_term = 0;
  bool granted = false;
  AppendRequest append;
  bool success = false;
  uint64_t match_index = 0;
  uint64_t conflict_index = 0;
  uint64_t conflict_term = 0;
  SnapshotChunk snapshot;
  uint64_t read_context = 0;
};

struct ReadState {
  uint64_t context = 0;
  uint64_t index = 0;
  uint64_t term = 0;
};

// A single-owner, deterministic fixed-three-voter node. Tick is a logical clock;
// Receive returns at most one direct reply and queues any new outbound requests.
// Empty commands are reserved for leadership no-ops. The caller applies ONLY
// TakeCommitted(), in order. ReadIndex requires a fresh correlated quorum and
// a current-term committed entry; the caller additionally waits for application.
class Node {
 public:
  static constexpr size_t kMaxCommandBytes = 16 * 1024;
  static constexpr size_t kMaxAppendBytes = 32 * 1024;
  static constexpr size_t kMaxAppendEntries = 32;
  static constexpr size_t kSnapshotChunkBytes = 16 * 1024;

  struct Config {
    uint64_t id = 0;
    std::vector<uint64_t> voters;
    uint32_t seed = 1;
    uint32_t election_min_ticks = 12;
    uint32_t election_jitter_ticks = 13;
    uint32_t heartbeat_ticks = 2;
    uint32_t rpc_timeout_ticks = 10;
    std::function<bool(const std::string&)> validate_snapshot;
  };

  Node(Config config, DurableState recovered, HardStateStore& hard_store, LogStore& log_store);
  void Tick();
  Status Campaign();
  std::optional<Message> Receive(const Message& message);
  Result<uint64_t> Propose(std::string command);
  Result<std::vector<uint64_t>> ProposeBatch(std::vector<std::string> commands);
  std::vector<Message> TakeMessages();
  std::vector<LogEntry> TakeCommitted();
  std::optional<Snapshot> TakeInstalledSnapshot();
  Status Compact(uint64_t applied_index, std::string data);
  Result<uint64_t> ReadIndex();
  std::vector<ReadState> TakeReadStates();
  void CancelRead(uint64_t context);

  bool failed() const { return failed_ || election_.failed() || log_.failed(); }
  bool IsLeader() const { return !failed() && election_.role() == Role::kLeader; }
  Role role() const { return election_.role(); }
  uint64_t id() const { return config_.id; }
  uint64_t term() const { return election_.hard_state().term; }
  uint64_t leader_id() const { return leader_id_; }
  uint64_t LastIndex() const { return log_.LastIndex(); }
  uint64_t CommitIndex() const { return log_.CommitIndex(); }
  const std::vector<LogEntry>& entries() const { return log_.entries(); }
  const Snapshot& snapshot() const { return log_.snapshot(); }
  size_t RetainedBytes() const;

 private:
  struct Progress {
    uint64_t next = 1;
    uint64_t matched = 0;
    uint64_t sequence = 0;
    uint64_t sent_last = 0;
    uint64_t read_context = 0;
    uint32_t age = 0;
    bool inflight = false;
    bool send_pending = false;
    bool sending_snapshot = false;
    std::shared_ptr<const Snapshot> snapshot;
    size_t snapshot_offset = 0;
    size_t sent_snapshot_end = 0;
  };
  void ResetElection();
  void BecomeLeader();
  void SendAppend(uint64_t peer);
  void FlushAppend(uint64_t peer);
  void Broadcast();
  void AdvanceCommit();
  void Collect(std::vector<LogEntry> entries);
  std::optional<Message> ReceiveSnapshot(const Message& message);
  uint64_t NextSequence();
  bool IsPeer(uint64_t peer) const;

  Config config_;
  Election election_;
  RaftLog log_;
  std::minstd_rand random_;
  std::map<uint64_t, Progress> progress_;
  std::vector<Message> messages_;
  std::vector<LogEntry> committed_;
  std::optional<Snapshot> installed_snapshot_;
  struct IncomingSnapshot {
    uint64_t from = 0;
    uint64_t leader_term = 0;
    SnapshotChunk chunk;
  };
  std::optional<IncomingSnapshot> incoming_snapshot_;
  uint64_t sequence_ = 0;
  uint64_t leader_id_ = 0;
  uint32_t election_elapsed_ = 0;
  uint32_t election_timeout_ = 0;
  uint32_t heartbeat_elapsed_ = 0;
  bool failed_ = false;
  std::optional<ReadState> read_;
  std::vector<ReadState> read_states_;
};

}  // namespace yewukv::raft
