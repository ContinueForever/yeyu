#include "raft/node.h"

#include <algorithm>
#include <iterator>
#include <limits>
#include <stdexcept>
#include <utility>

namespace yewukv::raft {

Node::Node(Config config, DurableState recovered, HardStateStore& hard_store, LogStore& log_store)
    : config_(std::move(config)),
      election_(config_.id, config_.voters, recovered.hard_state, hard_store),
      log_(std::move(recovered.entries), recovered.snapshot.index, log_store,
           std::move(recovered.snapshot)),
      random_(config_.seed + static_cast<uint32_t>(config_.id * 17)) {
  if (config_.heartbeat_ticks == 0 || config_.rpc_timeout_ticks == 0 ||
      config_.election_min_ticks <= config_.heartbeat_ticks || config_.election_jitter_ticks == 0 ||
      config_.election_jitter_ticks > UINT32_MAX - config_.election_min_ticks)
    throw std::invalid_argument("invalid Raft logical clock configuration");
  if (log_.snapshot().term > term() || log_.snapshot().data.size() > FileStore::kMaxSnapshotBytes ||
      (log_.snapshot().index != 0 && config_.validate_snapshot &&
       !config_.validate_snapshot(log_.snapshot().data)))
    throw std::invalid_argument("invalid recovered snapshot");
  for (const auto& entry : log_.entries()) {
    if (entry.term > term() || entry.command.size() > kMaxCommandBytes)
      throw std::invalid_argument("recovered log exceeds node term or command limit");
  }
  for (auto peer : config_.voters) {
    if (peer != config_.id) progress_.emplace(peer, Progress{});
  }
  ResetElection();
}

void Node::ResetElection() {
  election_elapsed_ = 0;
  election_timeout_ = config_.election_min_ticks + random_() % config_.election_jitter_ticks;
}

bool Node::IsPeer(uint64_t peer) const {
  return progress_.count(peer) != 0;
}

uint64_t Node::NextSequence() {
  if (sequence_ == std::numeric_limits<uint64_t>::max()) {
    failed_ = true;
    return 0;
  }
  return ++sequence_;
}

void Node::Collect(std::vector<LogEntry> entries) {
  committed_.insert(committed_.end(), std::make_move_iterator(entries.begin()),
                    std::make_move_iterator(entries.end()));
}

Status Node::Campaign() {
  if (failed()) return Status::IOError("Raft node failed; restart required");
  std::vector<OutboundVote> votes;
  const auto status = election_.Campaign(log_.LastIndex(), log_.LastTerm(), &votes);
  if (!status.ok()) return status;
  leader_id_ = 0;
  ResetElection();
  for (const auto& vote : votes) {
    Message message;
    message.kind = MessageKind::kVote;
    message.from = id();
    message.to = vote.peer_id;
    message.sequence = NextSequence();
    message.term = term();
    message.last_index = vote.request.last_log_index;
    message.last_term = vote.request.last_log_term;
    messages_.push_back(std::move(message));
  }
  return failed() ? Status::IOError("Raft sequence exhausted") : Status::OK();
}

void Node::Tick() {
  if (failed()) return;
  if (!IsLeader()) {
    if (++election_elapsed_ >= election_timeout_) Campaign();
    return;
  }
  for (auto& [peer, progress] : progress_) {
    if (progress.inflight && ++progress.age >= config_.rpc_timeout_ticks) progress.inflight = false;
  }
  if (++heartbeat_elapsed_ >= config_.heartbeat_ticks) {
    heartbeat_elapsed_ = 0;
    Broadcast();
  }
}

void Node::BecomeLeader() {
  leader_id_ = id();
  heartbeat_elapsed_ = 0;
  for (auto& [peer, progress] : progress_) {
    progress = Progress{};
    progress.next = log_.LastIndex() + 1;
  }
  // A current-term entry establishes commitment of the inherited prefix before
  // any client read can complete, including after an all-node restart.
  auto reply = log_.HandleAppend({log_.LastIndex(),
                                  log_.LastTerm(),
                                  log_.CommitIndex(),
                                  {{log_.LastIndex() + 1, term(), ""}}});
  if (!reply.ok() || !reply.value().success) {
    failed_ = true;
    return;
  }
  Broadcast();
}

void Node::SendAppend(uint64_t peer) {
  if (!IsLeader()) return;
  auto& progress = progress_.at(peer);
  if (progress.inflight) return;
  Message message;
  message.kind = MessageKind::kAppend;
  message.from = id();
  message.to = peer;
  message.sequence = NextSequence();
  message.term = term();
  if (progress.next <= log_.snapshot().index) {
    if (!progress.snapshot) {
      progress.snapshot = std::make_shared<Snapshot>(log_.snapshot());
      progress.snapshot_offset = 0;
    }
    const auto& snapshot = *progress.snapshot;
    message.kind = MessageKind::kSnapshot;
    message.snapshot.index = snapshot.index;
    message.snapshot.term = snapshot.term;
    message.snapshot.offset = progress.snapshot_offset;
    message.snapshot.total = snapshot.data.size();
    message.snapshot.data = snapshot.data.substr(progress.snapshot_offset, kSnapshotChunkBytes);
    progress.sent_snapshot_end = progress.snapshot_offset + message.snapshot.data.size();
    message.snapshot.done = progress.sent_snapshot_end == snapshot.data.size();
    progress.sending_snapshot = true;
    progress.sequence = message.sequence;
    progress.inflight = true;
    progress.age = 0;
    messages_.push_back(std::move(message));
    return;
  }
  progress.sending_snapshot = false;
  progress.snapshot.reset();
  message.append.prev_index = progress.next - 1;
  message.append.prev_term = log_.TermAt(progress.next - 1);
  message.append.leader_commit = log_.CommitIndex();
  size_t bytes = 0;
  for (size_t i = static_cast<size_t>(progress.next - log_.snapshot().index - 1);
       i < log_.entries().size(); ++i) {
    const auto& entry = log_.entries()[i];
    if (message.append.entries.size() == kMaxAppendEntries ||
        bytes + entry.command.size() + 24 > kMaxAppendBytes)
      break;
    message.append.entries.push_back(entry);
    bytes += entry.command.size() + 24;
  }
  progress.sequence = message.sequence;
  progress.sent_last = message.append.entries.empty() ? message.append.prev_index
                                                      : message.append.entries.back().index;
  progress.inflight = true;
  progress.age = 0;
  messages_.push_back(std::move(message));
}

void Node::Broadcast() {
  for (const auto& [peer, progress] : progress_) SendAppend(peer);
}

void Node::AdvanceCommit() {
  const auto previous = log_.CommitIndex();
  std::array<uint64_t, 3> matched{log_.LastIndex(), 0, 0};
  size_t i = 1;
  for (const auto& [peer, progress] : progress_) matched[i++] = progress.matched;
  auto result = log_.AdvanceLeaderCommit(term(), matched);
  if (!result.ok()) {
    failed_ = true;
    return;
  }
  Collect(result.TakeValue());
  if (log_.CommitIndex() != previous) Broadcast();
}

std::optional<Message> Node::Receive(const Message& message) {
  if (failed() || message.to != id() || !IsPeer(message.from) || message.term == 0 ||
      message.sequence == 0)
    return std::nullopt;
  const auto before_term = term();
  if (message.kind == MessageKind::kSnapshot) return ReceiveSnapshot(message);
  Message reply;
  reply.from = id();
  reply.to = message.from;
  reply.sequence = message.sequence;
  if (message.kind == MessageKind::kVote) {
    if ((message.last_index == 0) != (message.last_term == 0) || message.last_term > message.term)
      return std::nullopt;
    const auto result = election_.HandleVoteRequest(
        {message.term, message.from, message.last_index, message.last_term}, log_.LastIndex(),
        log_.LastTerm());
    if (!result.ok()) return std::nullopt;
    if (term() != before_term) leader_id_ = 0;
    if (result.value().granted) ResetElection();
    reply.kind = MessageKind::kVoteReply;
    reply.term = term();
    reply.granted = result.value().granted;
    return reply;
  }
  if (message.kind == MessageKind::kAppend) {
    reply.kind = MessageKind::kAppendReply;
    reply.term = term();
    if (message.term < term()) return reply;
    size_t bytes = 0;
    if (message.append.entries.size() > kMaxAppendEntries ||
        message.append.prev_term > message.term)
      return std::nullopt;
    for (const auto& entry : message.append.entries) {
      if (entry.term > message.term || entry.command.size() > kMaxCommandBytes) return std::nullopt;
      bytes += entry.command.size() + 24;
    }
    if (bytes > kMaxAppendBytes) return std::nullopt;
    if (!election_.ObserveLeader(message.from, message.term).ok()) return std::nullopt;
    leader_id_ = message.from;
    ResetElection();
    const auto result = log_.HandleAppend(message.append);
    if (!result.ok()) {
      // Invalid protocol data never produces an affirmative response. A true
      // persistence/corruption error stops this node rather than losing state.
      if (result.status().code() != StatusCode::kInvalidArgument) failed_ = true;
      return std::nullopt;
    }
    reply.term = term();
    reply.success = result.value().success;
    reply.conflict_index = result.value().conflict_index;
    reply.conflict_term = result.value().conflict_term;
    if (reply.success) {
      reply.match_index = message.append.entries.empty() ? message.append.prev_index
                                                         : message.append.entries.back().index;
      Collect(result.value().newly_committed);
    }
    return reply;
  }
  if (message.term > term()) {
    election_.HandleVoteResponse(message.from, {message.term, false});
    leader_id_ = 0;
    ResetElection();
    return std::nullopt;
  }
  if (message.term != term()) return std::nullopt;
  if (message.kind == MessageKind::kVoteReply) {
    const bool was_leader = IsLeader();
    election_.HandleVoteResponse(message.from, {message.term, message.granted});
    if (!was_leader && IsLeader()) BecomeLeader();
    return std::nullopt;
  }
  if (!IsLeader()) return std::nullopt;
  auto& progress = progress_.at(message.from);
  if (!progress.inflight || message.sequence != progress.sequence) return std::nullopt;
  if (message.kind == MessageKind::kSnapshotReply) {
    if (!progress.sending_snapshot || !progress.snapshot ||
        message.snapshot.index != progress.snapshot->index)
      return std::nullopt;
    progress.inflight = false;
    if (!message.success) {
      progress.snapshot_offset = 0;
    } else if (message.match_index == progress.snapshot->index &&
               message.snapshot.offset == progress.snapshot->data.size()) {
      progress.matched = std::max(progress.matched, progress.snapshot->index);
      progress.next = progress.matched + 1;
      progress.snapshot.reset();
      AdvanceCommit();
    } else if (message.snapshot.offset == progress.sent_snapshot_end &&
               message.snapshot.offset < progress.snapshot->data.size()) {
      progress.snapshot_offset = message.snapshot.offset;
    } else {
      return std::nullopt;
    }
    SendAppend(message.from);
    return std::nullopt;
  }
  if (message.kind != MessageKind::kAppendReply || progress.sending_snapshot) return std::nullopt;
  progress.inflight = false;
  if (message.success) {
    if (message.match_index != progress.sent_last) return std::nullopt;
    progress.matched = std::max(progress.matched, progress.sent_last);
    progress.next = progress.matched + 1;
    AdvanceCommit();
    if (progress.next <= log_.LastIndex()) SendAppend(message.from);
  } else {
    if (message.conflict_index > progress.next && message.conflict_index <= log_.LastIndex() + 1 &&
        message.conflict_index - 1 >= log_.snapshot().index &&
        log_.TermAt(message.conflict_index - 1) == message.conflict_term) {
      // A follower may compact an already committed prefix before this leader
      // receives its acknowledgement. Probe AFTER that matching boundary.
      progress.next = message.conflict_index;
    } else {
      progress.next =
          std::max(progress.matched + 1,
                   std::min(progress.next - 1, std::max(uint64_t{1}, message.conflict_index)));
    }
    SendAppend(message.from);
  }
  return std::nullopt;
}

Result<uint64_t> Node::Propose(std::string command) {
  auto result = ProposeBatch({std::move(command)});
  if (!result.ok()) return result.status();
  return result.value().front();
}

Result<std::vector<uint64_t>> Node::ProposeBatch(std::vector<std::string> commands) {
  if (failed()) return Status::IOError("Raft node failed; restart required");
  if (!IsLeader()) return Status::Busy("not leader");
  if (commands.empty() || commands.size() > kMaxAppendEntries)
    return Status::InvalidArgument("invalid Raft proposal batch size");
  AppendRequest append{log_.LastIndex(), log_.LastTerm(), log_.CommitIndex(), {}};
  std::vector<uint64_t> indexes;
  for (auto& command : commands) {
    if (command.empty() || command.size() > kMaxCommandBytes)
      return Status::InvalidArgument("invalid Raft command size");
    const uint64_t index = log_.LastIndex() + append.entries.size() + 1;
    indexes.push_back(index);
    append.entries.push_back({index, term(), std::move(command)});
  }
  const auto result = log_.HandleAppend(append);
  if (!result.ok()) return result.status();
  if (!result.value().success) {
    failed_ = true;
    return Status::Corruption("local Raft append rejected");
  }
  Broadcast();
  return indexes;
}

std::vector<Message> Node::TakeMessages() {
  return std::exchange(messages_, {});
}
std::vector<LogEntry> Node::TakeCommitted() {
  return std::exchange(committed_, {});
}

std::optional<Snapshot> Node::TakeInstalledSnapshot() {
  return std::exchange(installed_snapshot_, std::nullopt);
}

Status Node::Compact(uint64_t applied_index, std::string data) {
  if (failed()) return Status::IOError("Raft node failed; restart required");
  if (data.size() > FileStore::kMaxSnapshotBytes ||
      (config_.validate_snapshot && !config_.validate_snapshot(data)))
    return Status::InvalidArgument("invalid state-machine snapshot");
  return log_.Compact(applied_index, std::move(data));
}

size_t Node::RetainedBytes() const {
  size_t bytes = 0;
  for (const auto& entry : log_.entries()) bytes += 20 + entry.command.size();
  return bytes;
}

std::optional<Message> Node::ReceiveSnapshot(const Message& message) {
  const auto& chunk = message.snapshot;
  if (chunk.index == 0 || chunk.index == UINT64_MAX || chunk.term == 0 ||
      chunk.term > message.term || chunk.total > FileStore::kMaxSnapshotBytes ||
      chunk.offset > chunk.total || chunk.data.size() > chunk.total - chunk.offset ||
      chunk.data.size() > kSnapshotChunkBytes ||
      chunk.done != (chunk.offset + chunk.data.size() == chunk.total) ||
      (!chunk.done && chunk.data.empty()))
    return std::nullopt;
  if (chunk.index > log_.CommitIndex() && chunk.term < log_.TermAt(log_.CommitIndex()))
    return std::nullopt;
  Message reply;
  reply.kind = MessageKind::kSnapshotReply;
  reply.from = id();
  reply.to = message.from;
  reply.sequence = message.sequence;
  reply.term = term();
  reply.snapshot.index = chunk.index;
  if (message.term < term()) return reply;
  if (!election_.ObserveLeader(message.from, message.term).ok()) return std::nullopt;
  leader_id_ = message.from;
  ResetElection();
  reply.term = term();
  if (chunk.index <= log_.CommitIndex()) {
    reply.success = true;
    reply.match_index = chunk.index;
    reply.snapshot.offset = chunk.total;
    return reply;
  }
  const bool same = incoming_snapshot_ && incoming_snapshot_->from == message.from &&
                    incoming_snapshot_->leader_term == message.term &&
                    incoming_snapshot_->chunk.index == chunk.index &&
                    incoming_snapshot_->chunk.term == chunk.term &&
                    incoming_snapshot_->chunk.total == chunk.total;
  if (!same) {
    if (chunk.offset != 0) return reply;
    incoming_snapshot_ = IncomingSnapshot{
        message.from, message.term, {chunk.index, chunk.term, 0, chunk.total, {}, false}};
  }
  auto& data = incoming_snapshot_->chunk.data;
  if (chunk.offset == data.size()) {
    data += chunk.data;
  } else if (chunk.offset > data.size() || chunk.data.size() > data.size() - chunk.offset ||
             data.compare(chunk.offset, chunk.data.size(), chunk.data) != 0) {
    incoming_snapshot_.reset();
    return reply;
  }
  reply.snapshot.offset = data.size();
  if (data.size() == chunk.total) {
    if (config_.validate_snapshot && !config_.validate_snapshot(data)) {
      incoming_snapshot_.reset();
      reply.snapshot.offset = 0;
      return reply;
    }
    Snapshot snapshot{chunk.index, chunk.term, std::move(data)};
    incoming_snapshot_.reset();
    const auto status = log_.InstallSnapshot(snapshot);
    if (!status.ok()) {
      failed_ = true;
      return std::nullopt;
    }
    committed_.clear();
    installed_snapshot_ = std::move(snapshot);
    reply.match_index = chunk.index;
  }
  reply.success = true;
  return reply;
}

}  // namespace yewukv::raft
