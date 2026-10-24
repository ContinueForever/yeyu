#pragma once

#include <cstddef>
#include <array>
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

// Durable state for one node. Image mode preserves the original implementation
// for compatibility and controlled benchmarks. Journal mode appends synced
// deltas and occasionally publishes an atomic, generation-bound checkpoint.
class FileStore final : public HardStateStore, public LogStore {
 public:
  static constexpr size_t kMaxStateBytes = 64 * 1024 * 1024;
  static constexpr size_t kMaxCommandBytes = 1024 * 1024;
  static constexpr size_t kMaxSnapshotBytes = 16 * 1024 * 1024;
  // Non-cumulative, disjoint buckets, followed by one overflow bucket.
  static constexpr std::array<uint64_t, 15> kLatencyBoundsNs = {
      10000,    50000,    100000,    500000,    1000000,   2000000,    5000000,   10000000,
      20000000, 50000000, 100000000, 200000000, 500000000, 1000000000, 5000000000};

  enum class PersistPoint {
    kBeforeFileSync,
    kBeforeRename,
    kBeforeDirectorySync,
    kAfterDirectorySync,
    kBeforeJournalWrite,
    kBeforeJournalSync,
    kAfterJournalSync
  };
  struct Options {
    size_t max_state_bytes = kMaxStateBytes;
    bool journal = false;
    size_t journal_checkpoint_bytes = 4 * 1024 * 1024;
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
    uint64_t checkpoint_saves = 0;
    uint64_t journal_saves = 0;
    uint64_t journal_bytes = 0;
    uint64_t checkpoint_bytes = 0;
    uint64_t encode_nanoseconds = 0;
    uint64_t sync_nanoseconds = 0;
    uint64_t write_nanoseconds = 0;
    uint64_t file_syncs = 0;
    uint64_t directory_syncs = 0;
    uint64_t crc_nanoseconds = 0;  // Subset of encode_nanoseconds.
    uint64_t file_sync_nanoseconds = 0;
    uint64_t directory_sync_nanoseconds = 0;
    uint64_t checkpoint_max_nanoseconds = 0;
    uint64_t journal_sync_max_nanoseconds = 0;
    std::array<uint64_t, 16> checkpoint_duration_buckets{};
    std::array<uint64_t, 16> journal_sync_duration_buckets{};
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
  Status Checkpoint(DurableState next);
  Status AppendDelta(uint32_t kind, const std::string& payload);
  void RecoverJournal();
  void RemoveOldJournals();
  uint64_t SyncFile(int fd, const char* operation);
  void SyncDirectory(const char* operation);
  Status CheckUsable() const;
  void Close();

  Options options_;
  int directory_fd_ = -1;
  int lock_fd_ = -1;
  int journal_fd_ = -1;
  uint64_t generation_ = 0;
  uint64_t journal_sequence_ = 0;
  DurableState state_;
  bool failed_ = false;
  Metrics metrics_;
  mutable std::mutex mutex_;
};

}  // namespace yewukv::raft
