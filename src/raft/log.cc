#include "raft/log.h"

#include <algorithm>
#include <limits>
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

Result<AppendReply> RaftLog::HandleAppend(const AppendRequest& request) {
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
    return AppendReply{false, snapshot_.index + 1, snapshot_.term, commit_index_, {}};
  if (request.prev_index > LastIndex()) {
    return AppendReply{false, LastIndex() + 1, 0, commit_index_, {}};
  }
  if (TermAt(request.prev_index) != request.prev_term) {
    const uint64_t conflict_term = TermAt(request.prev_index);
    uint64_t first = request.prev_index;
    while (first > snapshot_.index + 1 && TermAt(first - 1) == conflict_term) --first;
    return AppendReply{false, first, conflict_term, commit_index_, {}};
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
  if (first_new < request.entries.size()) {
    const uint64_t first_index = request.entries[first_new].index;
    if (first_index <= commit_index_)
      return Status::Corruption("AppendEntries would overwrite a committed entry");
    std::vector<LogEntry> suffix(request.entries.begin() + first_new, request.entries.end());
    std::vector<LogEntry> replacement(
        entries_.begin(),
        entries_.begin() + static_cast<size_t>(first_index - snapshot_.index - 1));
    replacement.insert(replacement.end(), suffix.begin(), suffix.end());
    Status status = PersistSuffix(first_index, suffix);
    if (!status.ok()) return status;
    entries_.swap(replacement);
  }

  const uint64_t last_in_request =
      request.entries.empty() ? request.prev_index : request.entries.back().index;
  const uint64_t next_commit = std::min(request.leader_commit, last_in_request);
  AppendReply reply;
  reply.success = true;
  reply.newly_committed = CommitThrough(next_commit);
  reply.commit_index = commit_index_;
  return reply;
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

Status RaftLog::PublishSnapshot(const Snapshot& snapshot, std::vector<LogEntry> suffix) {
  Status status;
  try {
    status = store_.SaveSnapshot(snapshot, suffix);
  } catch (const std::exception& error) {
    status = Status::IOError(std::string("save snapshot: ") + error.what());
  } catch (...) {
    status = Status::IOError("save snapshot threw");
  }
  if (!status.ok()) {
    failed_ = true;
    return status;
  }
  snapshot_ = snapshot;
  entries_ = std::move(suffix);
  commit_index_ = std::max(commit_index_, snapshot.index);
  return Status::OK();
}

Status RaftLog::Compact(uint64_t applied_index, std::string data) {
  const auto usable = CheckUsable();
  if (!usable.ok()) return usable;
  if (applied_index <= snapshot_.index || applied_index > commit_index_)
    return Status::InvalidArgument("snapshot must cover an applied committed prefix");
  return PublishSnapshot({applied_index, TermAt(applied_index), std::move(data)},
                         {entries_.begin() + (applied_index - snapshot_.index), entries_.end()});
}

Status RaftLog::InstallSnapshot(const Snapshot& snapshot) {
  const auto usable = CheckUsable();
  if (!usable.ok()) return usable;
  if (snapshot.index <= commit_index_) return Status::OK();
  if (snapshot.index == UINT64_MAX || snapshot.term == 0 || snapshot.term < TermAt(commit_index_))
    return Status::InvalidArgument("invalid installed snapshot");
  std::vector<LogEntry> suffix;
  if (snapshot.index <= LastIndex() && TermAt(snapshot.index) == snapshot.term)
    suffix.assign(entries_.begin() + (snapshot.index - snapshot_.index), entries_.end());
  return PublishSnapshot(snapshot, std::move(suffix));
}

}  // namespace yewukv::raft
