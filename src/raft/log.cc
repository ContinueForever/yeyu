#include "raft/log.h"

#include <algorithm>
#include <limits>
#include <iterator>
#include <stdexcept>
#include <utility>

namespace yewukv::raft {

RaftLog::RaftLog(std::vector<LogEntry> recovered, uint64_t commit_index, LogStore& store,
                 Snapshot snapshot)
    : entries_(std::move(recovered)),
      snapshot_(std::move(snapshot)),
      commit_index_(commit_index),
      store_(store) {
  if (snapshot_.index == UINT64_MAX || (snapshot_.index == 0) != (snapshot_.term == 0) ||
      (snapshot_.index == 0 && !snapshot_.data.empty()) ||
      entries_.size() >= UINT64_MAX - snapshot_.index || commit_index_ < snapshot_.index ||
      commit_index_ > LastIndex())
    throw std::invalid_argument("invalid snapshot or commit index");
  for (size_t i = 0; i < entries_.size(); ++i) {
    if (entries_[i].index != snapshot_.index + i + 1 || entries_[i].term == 0 ||
        entries_[i].term < (i == 0 ? snapshot_.term : entries_[i - 1].term))
      throw std::invalid_argument("Raft log must have contiguous indexes and ordered terms");
  }
}

Status RaftLog::CheckUsable() const {
  return failed_ ? Status::IOError("Raft log persistence failed; restart required") : Status::OK();
}

Status RaftLog::PersistSuffix(uint64_t first_index, const std::vector<LogEntry>& suffix) {
  Status status;
  try {
    status = store_.ReplaceFrom(first_index, suffix);
  } catch (const std::exception& error) {
    failed_ = true;
    return Status::IOError(std::string("Raft log save threw: ") + error.what());
  } catch (...) {
    failed_ = true;
    return Status::IOError("Raft log save threw");
  }
  if (!status.ok()) failed_ = true;
  return status;
}

uint64_t RaftLog::TermAt(uint64_t index) const {
  if (index == snapshot_.index) return snapshot_.term;
  if (index < snapshot_.index || index > LastIndex())
    throw std::out_of_range("Raft index outside retained log");
  return entries_[static_cast<size_t>(index - snapshot_.index - 1)].term;
}

std::vector<LogEntry> RaftLog::CommitThrough(uint64_t index) {
  if (index <= commit_index_) return {};
  std::vector<LogEntry> newly_committed(entries_.begin() + (commit_index_ - snapshot_.index),
                                        entries_.begin() + (index - snapshot_.index));
  commit_index_ = index;
  return newly_committed;
}

Result<RaftLog::AppendUpdate> RaftLog::PrepareAppend(const AppendRequest& request) {
  Status usable = CheckUsable();
  if (!usable.ok()) return usable;
  if ((request.prev_index == 0 && request.prev_term != 0) ||
      request.prev_index == std::numeric_limits<uint64_t>::max())
    return Status::InvalidArgument("invalid previous Raft log position");
  uint64_t expected = request.prev_index + 1;
  uint64_t preceding_term = request.prev_term;
  for (size_t i = 0; i < request.entries.size(); ++i) {
    const auto& entry = request.entries[i];
    if (entry.index == UINT64_MAX || entry.index != expected || entry.term == 0 ||
        entry.term < preceding_term)
      return Status::InvalidArgument("AppendEntries contains a gap or decreasing term");
    preceding_term = entry.term;
    if (i + 1 < request.entries.size()) {
      if (expected == std::numeric_limits<uint64_t>::max())
        return Status::InvalidArgument("Raft log index overflow");
      ++expected;
    }
  }

  if (request.prev_index < snapshot_.index)
    return AppendUpdate{{false, snapshot_.index + 1, snapshot_.term, commit_index_, {}}, 0, {}, 0};
  if (request.prev_index > LastIndex()) {
    return AppendUpdate{{false, LastIndex() + 1, 0, commit_index_, {}}, 0, {}, 0};
  }
  if (TermAt(request.prev_index) != request.prev_term) {
    const uint64_t conflict_term = TermAt(request.prev_index);
    uint64_t first = request.prev_index;
    while (first > snapshot_.index + 1 && TermAt(first - 1) == conflict_term) --first;
    return AppendUpdate{{false, first, conflict_term, commit_index_, {}}, 0, {}, 0};
  }

  size_t first_new = request.entries.size();
  for (size_t i = 0; i < request.entries.size(); ++i) {
    const auto& incoming = request.entries[i];
    if (incoming.index > LastIndex()) {
      first_new = i;
      break;
    }
    const auto& existing = entries_[static_cast<size_t>(incoming.index - snapshot_.index - 1)];
    if (incoming.term != existing.term) {
      first_new = i;
      break;
    }
    if (incoming.command != existing.command)
      return Status::Corruption("same Raft index and term contain different commands");
  }
  AppendUpdate update;
  if (first_new < request.entries.size()) {
    const uint64_t first_index = request.entries[first_new].index;
    if (first_index <= commit_index_)
      return Status::Corruption("AppendEntries would overwrite a committed entry");
    std::vector<LogEntry> suffix(request.entries.begin() + first_new, request.entries.end());
    const auto prefix = static_cast<size_t>(first_index - snapshot_.index - 1);
    // Keep the already persisted prefix in place. Prepare allocations before
    // saving; publishing a small append must not copy the entire retained log.
    entries_.reserve(prefix + suffix.size());
    update.first_index = first_index;
    update.suffix = std::move(suffix);
  }
  const auto last = request.entries.empty() ? request.prev_index : request.entries.back().index;
  update.reply.success = true;
  update.commit_through = std::min(request.leader_commit, last);
  return update;
}

Result<AppendReply> RaftLog::AdvanceAppend(AppendUpdate update, Status persisted) {
  if (!persisted.ok()) {
    failed_ = true;
    return persisted;
  }
  const auto usable = CheckUsable();
  if (!usable.ok()) return usable;
  if (update.first_index) {
    const auto prefix = static_cast<size_t>(update.first_index - snapshot_.index - 1);
    entries_.resize(prefix);
    entries_.insert(entries_.end(), std::make_move_iterator(update.suffix.begin()),
                    std::make_move_iterator(update.suffix.end()));
  }
  if (update.reply.success) update.reply.newly_committed = CommitThrough(update.commit_through);
  update.reply.commit_index = commit_index_;
  return std::move(update.reply);
}

Result<AppendReply> RaftLog::HandleAppend(const AppendRequest& request) {
  auto prepared = PrepareAppend(request);
  if (!prepared.ok()) return prepared.status();
  auto update = prepared.TakeValue();
  Status status;
  if (update.first_index) status = PersistSuffix(update.first_index, update.suffix);
  return AdvanceAppend(std::move(update), std::move(status));
}

Result<std::vector<LogEntry>> RaftLog::AdvanceLeaderCommit(
    uint64_t current_term, const std::array<uint64_t, 3>& match_indexes) {
  Status usable = CheckUsable();
  if (!usable.ok()) return usable;
  if (current_term == 0 || match_indexes[0] != LastIndex() ||
      std::any_of(match_indexes.begin(), match_indexes.end(),
                  [&](uint64_t index) { return index > LastIndex(); }))
    return Status::InvalidArgument("invalid current term or follower match index");
  for (uint64_t index = LastIndex(); index > commit_index_; --index) {
    if (TermAt(index) != current_term) continue;
    size_t replicas = 0;
    for (uint64_t match : match_indexes) {
      if (match >= index) ++replicas;
    }
    if (replicas >= 2) return CommitThrough(index);
  }
  return std::vector<LogEntry>{};
}

Status RaftLog::AdvanceSnapshot(SnapshotUpdate update, Status persisted) {
  if (!persisted.ok()) {
    failed_ = true;
    return persisted;
  }
  const auto usable = CheckUsable();
  if (!usable.ok()) return usable;
  if (update.persist) {
    snapshot_ = std::move(update.snapshot);
    entries_ = std::move(update.suffix);
    commit_index_ = std::max(commit_index_, snapshot_.index);
  }
  return Status::OK();
}

Status RaftLog::PublishSnapshot(const Snapshot& snapshot, std::vector<LogEntry> suffix) {
  Status status;
  try {
    status = store_.SaveSnapshot(snapshot, suffix);
  } catch (const std::exception& error) {
    status = Status::IOError(std::string("save snapshot: ") + error.what());
  } catch (...) {
    status = Status::IOError("save snapshot threw");
  }
  return AdvanceSnapshot({snapshot, std::move(suffix)}, std::move(status));
}

Result<RaftLog::SnapshotUpdate> RaftLog::PrepareCompact(uint64_t applied_index,
                                                        std::string data) const {
  const auto usable = CheckUsable();
  if (!usable.ok()) return usable;
  if (applied_index <= snapshot_.index || applied_index > commit_index_)
    return Status::InvalidArgument("snapshot must cover an applied committed prefix");
  return SnapshotUpdate{{applied_index, TermAt(applied_index), std::move(data)},
                        {entries_.begin() + (applied_index - snapshot_.index), entries_.end()}};
}

Result<RaftLog::SnapshotUpdate> RaftLog::PrepareInstallSnapshot(const Snapshot& snapshot) const {
  const auto usable = CheckUsable();
  if (!usable.ok()) return usable;
  if (snapshot.index <= commit_index_) return SnapshotUpdate{{}, {}, false};
  if (snapshot.index == UINT64_MAX || snapshot.term == 0 || snapshot.term < TermAt(commit_index_))
    return Status::InvalidArgument("invalid installed snapshot");
  std::vector<LogEntry> suffix;
  if (snapshot.index <= LastIndex() && TermAt(snapshot.index) == snapshot.term)
    suffix.assign(entries_.begin() + (snapshot.index - snapshot_.index), entries_.end());
  return SnapshotUpdate{snapshot, std::move(suffix)};
}

Status RaftLog::Compact(uint64_t applied_index, std::string data) {
  auto prepared = PrepareCompact(applied_index, std::move(data));
  if (!prepared.ok()) return prepared.status();
  auto update = prepared.TakeValue();
  return PublishSnapshot(update.snapshot, std::move(update.suffix));
}

Status RaftLog::InstallSnapshot(const Snapshot& snapshot) {
  auto prepared = PrepareInstallSnapshot(snapshot);
  if (!prepared.ok()) return prepared.status();
  auto update = prepared.TakeValue();
  return update.persist ? PublishSnapshot(update.snapshot, std::move(update.suffix)) : Status::OK();
}

}  // namespace yewukv::raft
