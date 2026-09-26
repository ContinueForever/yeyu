#pragma once

#include <array>
#include <cstdint>
#include <string>
#include <vector>

#include "common/status.h"

namespace yewukv::raft {

struct LogEntry {
  uint64_t index = 0;
  uint64_t term = 0;
  std::string command;
};

// ReplaceFrom must durably keep entries before first_index, replace the suffix
// with the supplied entries, and report OK only after the replacement is safe
// across restart. A failed call poisons the RaftLog until reconstructed.
class LogStore {
 public:
  virtual ~LogStore() = default;
  virtual Status ReplaceFrom(uint64_t first_index, const std::vector<LogEntry>& suffix) = 0;
};

struct AppendRequest {
  uint64_t prev_index = 0;
  uint64_t prev_term = 0;
  uint64_t leader_commit = 0;
  std::vector<LogEntry> entries;
};

struct AppendReply {
  bool success = false;
  uint64_t conflict_index = 0;
  uint64_t conflict_term = 0;
  uint64_t commit_index = 0;
  std::vector<LogEntry> newly_committed;
};

// Network-free log matching and commit rules for a fixed three-voter group.
// The caller must validate the sender's term/identity before HandleAppend and
// supply the current term and match indexes only from valid follower replies.
class RaftLog {
 public:
  RaftLog(std::vector<LogEntry> recovered, uint64_t commit_index, LogStore& store);

  Result<AppendReply> HandleAppend(const AppendRequest& request);
  // match_indexes[0] is the leader's own durable last index.
  Result<std::vector<LogEntry>> AdvanceLeaderCommit(uint64_t current_term,
                                                    const std::array<uint64_t, 3>& match_indexes);

  uint64_t LastIndex() const { return static_cast<uint64_t>(entries_.size()); }
  uint64_t LastTerm() const { return entries_.empty() ? 0 : entries_.back().term; }
  uint64_t CommitIndex() const { return commit_index_; }
  const std::vector<LogEntry>& entries() const { return entries_; }
  bool failed() const { return failed_; }

 private:
  Status CheckUsable() const;
  Status PersistSuffix(uint64_t first_index, const std::vector<LogEntry>& suffix);
  uint64_t TermAt(uint64_t index) const;
  std::vector<LogEntry> CommitThrough(uint64_t index);

  std::vector<LogEntry> entries_;
  uint64_t commit_index_ = 0;
  LogStore& store_;
  bool failed_ = false;
};

}  // namespace yewukv::raft
