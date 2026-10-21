#pragma once

#include <cstddef>
#include <functional>
#include <mutex>
#include <string>
#include <vector>

#include "raft/election.h"
#include "raft/log.h"

namespace yewukv::raft {

struct DurableState {
  HardState hard_state;
  std::vector<LogEntry> entries;
  Snapshot snapshot;
};

// A correctness-first store for one Raft node, in a dedicated directory.
// Each change replaces a checksummed image containing hard state, an optional
// snapshot and retained log. OK follows fdatasync, rename and directory fsync.
// This rewrites the full image; snapshot compaction bounds its retained log.
class FileStore final : public HardStateStore, public LogStore {
 public:
  static constexpr size_t kMaxStateBytes = 64 * 1024 * 1024;
  static constexpr size_t kMaxCommandBytes = 1024 * 1024;
  static constexpr size_t kMaxSnapshotBytes = 16 * 1024 * 1024;

  enum class PersistPoint {
    kBeforeFileSync,
    kBeforeRename,
    kBeforeDirectorySync,
    kAfterDirectorySync
  };
  struct Options {
    size_t max_state_bytes = kMaxStateBytes;
    // Optional deterministic fault/crash injection. Runs under the store lock,
    // including initial creation. Do not reenter the store from this callback.
    // Throwing poisons this instance, even if rename has already succeeded.
    std::function<void(PersistPoint)> persist_observer;
  };
  struct Metrics {
    uint64_t saves = 0;
    uint64_t bytes_written = 0;
    uint64_t persist_nanoseconds = 0;
    uint64_t snapshots = 0;
    uint64_t state_bytes = 0;
  };

  explicit FileStore(std::string directory);
  FileStore(std::string directory, Options options);
  ~FileStore() override;
  FileStore(const FileStore&) = delete;
  FileStore& operator=(const FileStore&) = delete;

  // Supply these recovered values to Election and RaftLog. Commit/apply indexes
  // are NOT persisted here; a node must establish commitment before applying.
  DurableState ReadState() const;
  bool failed() const;
  Metrics ReadMetrics() const;
  Status Save(const HardState& state) override;
  Status ReplaceFrom(uint64_t first_index, const std::vector<LogEntry>& suffix) override;
  Status SaveSnapshot(const Snapshot& snapshot, const std::vector<LogEntry>& suffix) override;

 private:
  void Recover();
  Status Persist(DurableState next);
  Status CheckUsable() const;
  void Close();

  Options options_;
  int directory_fd_ = -1;
  int lock_fd_ = -1;
  DurableState state_;
  bool failed_ = false;
  Metrics metrics_;
  mutable std::mutex mutex_;
};

}  // namespace yewukv::raft
