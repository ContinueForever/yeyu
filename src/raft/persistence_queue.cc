#include "raft/persistence_queue.h"

#include <atomic>
#include <stdexcept>
#include <utility>

namespace yewukv::raft {
namespace {

// Tickets never identify a recovered disk generation. Their process-local
// lifetime ID prevents late completions from an old queue matching a new one.
uint64_t NextStream() {
  static std::atomic<uint64_t> next{1};
  auto value = next.load(std::memory_order_relaxed);
  for (;;) {
    if (value == UINT64_MAX) throw std::overflow_error("persistence stream IDs exhausted");
    if (next.compare_exchange_weak(value, value + 1, std::memory_order_relaxed)) return value;
  }
}

std::optional<size_t> Charge(const PersistenceMutation& mutation, size_t limit) {
  size_t bytes = 64;  // Fixed command/slot charge, independent of object layout.
  auto add = [&](size_t amount) {
    if (bytes > limit || amount > limit - bytes) return false;
    bytes += amount;
    return true;
  };
  auto entries = [&](const std::vector<LogEntry>& suffix) {
    for (const auto& entry : suffix)
      if (!add(24) || !add(entry.command.size())) return false;
    return true;
  };
  bool fits = bytes <= limit;
  if (const auto* suffix = std::get_if<SuffixWrite>(&mutation))
    fits = fits && add(8) && entries(suffix->entries);
  else if (const auto* snapshot = std::get_if<SnapshotWrite>(&mutation))
    fits = fits && add(24) && add(snapshot->snapshot.data.size()) && entries(snapshot->suffix);
  return fits ? std::optional<size_t>{bytes} : std::nullopt;
}

}  // namespace

Status ExecutePersistence(const PersistenceMutation& mutation, HardStateStore& hard_store,
                          LogStore& log_store) {
  try {
    if (const auto* hard = std::get_if<HardState>(&mutation)) return hard_store.Save(*hard);
    if (const auto* suffix = std::get_if<SuffixWrite>(&mutation))
      return log_store.ReplaceFrom(suffix->first_index, suffix->entries);
    const auto& snapshot = std::get<SnapshotWrite>(mutation);
    return log_store.SaveSnapshot(snapshot.snapshot, snapshot.suffix);
  } catch (const std::exception& error) {
    return Status::IOError(std::string("persistence consumer threw: ") + error.what());
  } catch (...) {
    return Status::IOError("persistence consumer threw");
  }
}

PersistenceQueue::PersistenceQueue() : PersistenceQueue(Options{}) {}

PersistenceQueue::PersistenceQueue(Options options) : options_(options), stream_(NextStream()) {
  if (!options_.max_operations || options_.max_payload_bytes < 64)
    throw std::invalid_argument("invalid persistence queue capacity");
}

Result<PersistenceQueue::Ticket> PersistenceQueue::Submit(const PersistenceMutation& mutation) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (failed_) return Status::IOError("persistence stream failed; reopen required");
  if (!accepting_) return Status::Shutdown("persistence stream stopped accepting");
  if (sequence_ == UINT64_MAX) return Status::Shutdown("persistence sequence exhausted");
  if (entries_.size() >= options_.max_operations) return Status::Busy("persistence queue full");
  const auto bytes = Charge(mutation, options_.max_payload_bytes - bytes_);
  if (!bytes) return Status::Busy("persistence payload budget exceeded");
  auto entry = std::make_unique<Entry>();
  entry->ticket = {stream_, sequence_ + 1};
  entry->bytes = *bytes;
  entry->mutation = std::make_shared<const PersistenceMutation>(mutation);
  const auto ticket = entry->ticket;
  entries_.push_back(std::move(entry));
  ++sequence_;
  bytes_ += *bytes;
  changed_.notify_all();
  return ticket;
}

bool PersistenceQueue::HasQueuedLocked() const {
  for (const auto& entry : entries_)
    if (entry->phase == Phase::kQueued) return true;
  return false;
}

std::optional<PersistenceQueue::Work> PersistenceQueue::TakeNextLocked() {
  if (failed_ || active_) return std::nullopt;
  for (auto& entry : entries_) {
    if (entry->phase != Phase::kQueued) continue;
    entry->phase = Phase::kRunning;
    entry->dispatched = true;
    active_ = entry->ticket.sequence;
    return Work{entry->ticket, entry->mutation};
  }
  return std::nullopt;
}

std::optional<PersistenceQueue::Work> PersistenceQueue::TakeNext() {
  std::lock_guard<std::mutex> lock(mutex_);
  return TakeNextLocked();
}

std::optional<PersistenceQueue::Work> PersistenceQueue::WaitNext(
    std::chrono::milliseconds timeout) {
  std::unique_lock<std::mutex> lock(mutex_);
  changed_.wait_for(lock, timeout,
                    [&] { return failed_ || (!active_ && (HasQueuedLocked() || !accepting_)); });
  return TakeNextLocked();
}

Status PersistenceQueue::Complete(Ticket ticket, Status result) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (ticket.stream != stream_ || !active_ || ticket.sequence != active_)
    return Status::InvalidArgument("completion does not match the in-flight persistence task");
  for (auto& entry : entries_) {
    if (entry->ticket.sequence != active_) continue;
    entry->result = std::move(result);
    entry->phase = Phase::kComplete;
    failed_ = !entry->result.ok();
    active_ = 0;
    break;
  }
  if (failed_) {
    accepting_ = false;
    for (auto& entry : entries_) {
      if (entry->phase != Phase::kQueued) continue;
      entry->result = std::move(entry->not_dispatched);
      entry->phase = Phase::kComplete;
    }
  }
  changed_.notify_all();
  return Status::OK();  // The report was accepted; inspect Completion::status.
}

PersistenceQueue::Completion PersistenceQueue::TakeCompletionLocked() {
  auto& front = *entries_.front();
  Completion result{front.ticket, std::move(front.result), front.dispatched};
  bytes_ -= front.bytes;
  entries_.pop_front();
  return result;
}

std::optional<PersistenceQueue::Completion> PersistenceQueue::TakeCompletion() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (entries_.empty() || entries_.front()->phase != Phase::kComplete) return std::nullopt;
  return TakeCompletionLocked();
}

Result<PersistenceQueue::Completion> PersistenceQueue::WaitCompletion(Ticket ticket) {
  std::unique_lock<std::mutex> lock(mutex_);
  if (entries_.empty() || !(ticket == entries_.front()->ticket))
    return Status::InvalidArgument("only the ordered owner may wait for the front task");
  changed_.wait(lock, [&] {
    return entries_.empty() || !(ticket == entries_.front()->ticket) ||
           entries_.front()->phase == Phase::kComplete;
  });
  if (entries_.empty() || !(ticket == entries_.front()->ticket))
    return Status::InvalidArgument("another owner consumed the persistence completion");
  return TakeCompletionLocked();
}

void PersistenceQueue::StopAccepting() {
  std::lock_guard<std::mutex> lock(mutex_);
  accepting_ = false;
  changed_.notify_all();
}

size_t PersistenceQueue::outstanding() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return entries_.size();
}

size_t PersistenceQueue::payload_bytes() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return bytes_;
}

bool PersistenceQueue::failed() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return failed_;
}

Status QueuedStore::Persist(const PersistenceMutation& mutation) {
  auto ticket = queue_.Submit(mutation);
  if (!ticket.ok()) return ticket.status();
  auto completion = queue_.WaitCompletion(ticket.value());
  if (!completion.ok()) return completion.status();
  return std::move(completion.value().status);
}

Status QueuedStore::Save(const HardState& state) {
  return Persist(state);
}

Status QueuedStore::ReplaceFrom(uint64_t first_index, const std::vector<LogEntry>& suffix) {
  return Persist(SuffixWrite{first_index, suffix});
}

Status QueuedStore::SaveSnapshot(const Snapshot& snapshot, const std::vector<LogEntry>& suffix) {
  return Persist(SnapshotWrite{snapshot, suffix});
}

}  // namespace yewukv::raft
