#pragma once

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <optional>
#include <variant>

#include "raft/election.h"
#include "raft/log.h"

namespace yewukv::raft {

struct SuffixWrite {
  uint64_t first_index = 0;
  std::vector<LogEntry> entries;
};

struct SnapshotWrite {
  Snapshot snapshot;
  std::vector<LogEntry> suffix;
};

using PersistenceMutation = std::variant<HardState, SuffixWrite, SnapshotWrite>;

// Invoke the existing synchronous store contract. A returned failure may follow
// partial publication; callers must not retry the mutation on this instance.
Status ExecutePersistence(const PersistenceMutation& mutation, HardStateStore& hard_store,
                          LogStore& log_store);

// One volatile stream for one durable store. Submit admits owned input; it does
// NOT mean persistence succeeded. A single consumer executes each ready task
// once, then reports its actual store result. Complete does not perform I/O or
// verify the consumer's claim. Protocol effects require a matching completion.
//
// Thread-safe, with at most one task in flight. Capacity includes uncollected
// completions. The deterministic byte budget charges payload sizes plus fixed
// metadata, not allocator RSS or copies retained outside this queue. All users
// must finish before destruction; this queue creates no threads of its own.
class PersistenceQueue {
 public:
  struct Options {
    size_t max_operations = 64;
    size_t max_payload_bytes = 64 * 1024 * 1024;
  };
  struct Ticket {
    uint64_t stream = 0;
    uint64_t sequence = 0;
    bool operator==(const Ticket& other) const {
      return stream == other.stream && sequence == other.sequence;
    }
  };
  struct Work {
    Ticket ticket;
    std::shared_ptr<const PersistenceMutation> mutation;
  };
  struct Completion {
    Ticket ticket;
    Status status;
    // False means this queued operation was never dispatched. A dispatched
    // failure has an unknown durability outcome, even if its status is Busy.
    bool dispatched = false;
  };

  PersistenceQueue();
  explicit PersistenceQueue(Options options);
  PersistenceQueue(const PersistenceQueue&) = delete;
  PersistenceQueue& operator=(const PersistenceQueue&) = delete;

  Result<Ticket> Submit(const PersistenceMutation& mutation);
  std::optional<Work> TakeNext();
  std::optional<Work> WaitNext(std::chrono::milliseconds timeout);
  // Rejects duplicate, foreign and out-of-order reports without changing the
  // active task. A real failure poisons this stream and completes every queued
  // successor as NOT dispatched. Reopen the store with a fresh queue to resume.
  Status Complete(Ticket ticket, Status result);
  std::optional<Completion> TakeCompletion();
  // Single ordered owner only: waits for and consumes the front completion.
  // An invalid/non-front ticket fails immediately. No timeout cancels a write.
  Result<Completion> WaitCompletion(Ticket ticket);
  void StopAccepting();  // Accepted tasks still drain in order.
  size_t outstanding() const;
  size_t payload_bytes() const;
  bool failed() const;

 private:
  enum class Phase { kQueued, kRunning, kComplete };
  struct Entry {
    Ticket ticket;
    std::shared_ptr<const PersistenceMutation> mutation;
    size_t bytes = 0;
    Phase phase = Phase::kQueued;
    bool dispatched = false;
    Status result;
    // Allocate this before admission so failure propagation after a durable
    // operation needs no new completion slots or error-message allocations.
    Status not_dispatched = Status::Shutdown("predecessor persistence failed; not dispatched");
  };
  std::optional<Work> TakeNextLocked();
  Completion TakeCompletionLocked();
  bool HasQueuedLocked() const;

  Options options_;
  const uint64_t stream_;
  uint64_t sequence_ = 0;
  uint64_t active_ = 0;
  size_t bytes_ = 0;
  bool accepting_ = true;
  bool failed_ = false;
  std::deque<std::unique_ptr<Entry>> entries_;
  mutable std::mutex mutex_;
  std::condition_variable changed_;
};

// Compatibility/testing bridge for one ordered caller. Methods BLOCK and retain
// HardStateStore/LogStore's durable-before-OK meaning. Another thread must drive
// the queue. This is not an asynchronous Raft owner loop or a performance mode.
class QueuedStore final : public HardStateStore, public LogStore {
 public:
  explicit QueuedStore(PersistenceQueue& queue) : queue_(queue) {}
  Status Save(const HardState& state) override;
  Status ReplaceFrom(uint64_t first_index, const std::vector<LogEntry>& suffix) override;
  Status SaveSnapshot(const Snapshot& snapshot, const std::vector<LogEntry>& suffix) override;

 private:
  Status Persist(const PersistenceMutation& mutation);
  PersistenceQueue& queue_;
};

}  // namespace yewukv::raft
