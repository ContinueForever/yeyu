#include "raft/node.h"

#include <algorithm>
#include <utility>

namespace yewukv::raft {

Status Node::StartWrite(const PersistenceMutation& mutation, std::function<void(Status)> done) {
  if (pending_write_) return Status::Busy("a persistence barrier is pending");
  // Allocate the continuation before admission. Submission owns the disk input;
  // no store returns an early OK and no future is waited on by the owner.
  PendingWrite pending{{}, std::move(done), 0};
  auto admitted = queue_->Submit(mutation);
  if (!admitted.ok()) return admitted.status();
  pending.ticket = admitted.value();
  pending_write_ = std::move(pending);
  return Status::OK();
}

Status Node::StartElection(Election::Update update, std::function<void(Status)> done) {
  const auto epoch = leadership_epoch_;
  const auto hard = update.hard_state;
  const bool persist = update.persist;
  auto advance = [this, update = std::move(update), done = std::move(done),
                  epoch](Status io) mutable {
    auto status = election_.Advance(std::move(update), std::move(io));
    if (status.ok() && (epoch != leadership_epoch_ || term() < observed_term_)) {
      election_.StepDown();
      status = Status::Busy("election transition superseded while persistence was pending");
    }
    observed_term_ = std::max(observed_term_, term());
    done(std::move(status));
  };
  if (persist) return StartWrite(hard, std::move(advance));
  advance(Status::OK());
  return Status::OK();
}

Status Node::AppendAsync(RaftLog::AppendUpdate update,
                         std::function<void(Result<AppendReply>)> done) {
  const bool persist = update.first_index != 0;
  PersistenceMutation mutation = SuffixWrite{update.first_index, update.suffix};
  auto advance = [this, update = std::move(update), done = std::move(done)](Status io) mutable {
    done(log_.AdvanceAppend(std::move(update), std::move(io)));
  };
  if (persist) return StartWrite(mutation, std::move(advance));
  advance(Status::OK());
  return Status::OK();
}

Status Node::SnapshotAsync(RaftLog::SnapshotUpdate update, std::function<void(Status)> done) {
  const bool persist = update.persist;
  PersistenceMutation mutation = SnapshotWrite{update.snapshot, update.suffix};
  auto advance = [this, update = std::move(update), done = std::move(done)](Status io) mutable {
    done(log_.AdvanceSnapshot(std::move(update), std::move(io)));
  };
  if (persist) return StartWrite(mutation, std::move(advance));
  advance(Status::OK());
  return Status::OK();
}

void Node::Fence(uint64_t observed_term) {
  if (leadership_epoch_ == UINT64_MAX) {
    failed_ = true;
    return;
  }
  observed_term_ = std::max(observed_term_, observed_term);
  ++leadership_epoch_;
  election_.StepDown();
  leader_id_ = 0;
  read_.reset();
  read_states_.clear();
  messages_.clear();
  ResetElection();
}

void Node::AdvancePersistence() {
  if (!queue_ || failed()) return;
  if (auto completed = queue_->TakeCompletion()) {
    if (!pending_write_ || !(pending_write_->ticket == completed->ticket) ||
        !completed->dispatched) {
      failed_ = true;
      return;
    }
    auto pending = std::move(*pending_write_);
    pending_write_.reset();
    pending.done(std::move(completed->status));
  }
  // A higher-term message can fence leadership while the disk is busy. Record
  // the highest observed term even if its transport times out without retrying.
  if (!failed() && !pending_write_ && observed_term_ > term()) {
    auto prepared =
        election_.PrepareVoteResponse(progress_.begin()->first, {observed_term_, false});
    if (!prepared.ok() || !StartElection(prepared.TakeValue(), [](Status) {}).ok()) failed_ = true;
  }
}

Status Node::CampaignAsync() {
  if (pending_write_ || observed_term_ > term()) return Status::Busy("a term barrier is pending");
  auto prepared = election_.PrepareCampaign();
  if (!prepared.ok()) return prepared.status();
  const auto last = log_.LastIndex();
  const auto last_term = log_.LastTerm();
  return StartElection(prepared.TakeValue(), [this, last, last_term](Status status) {
    if (!status.ok()) return;
    leader_id_ = 0;
    read_.reset();
    read_states_.clear();
    ResetElection();
    for (const auto& [peer, progress] : progress_) {
      Message request;
      request.kind = MessageKind::kVote;
      request.from = id();
      request.to = peer;
      request.sequence = NextSequence();
      request.term = term();
      request.last_index = last;
      request.last_term = last_term;
      messages_.push_back(std::move(request));
    }
  });
}

bool Node::ValidAsyncMessage(const Message& message) const {
  if (message.to != id() || !IsPeer(message.from) || !message.term || !message.sequence)
    return false;
  if (message.kind == MessageKind::kVote)
    return (message.last_index == 0) == (message.last_term == 0) &&
           message.last_term <= message.term;
  if (message.kind == MessageKind::kAppend) {
    if (message.append.entries.size() > kMaxAppendEntries ||
        message.append.prev_term > message.term)
      return false;
    size_t bytes = 0;
    for (const auto& entry : message.append.entries) {
      if (entry.term > message.term || entry.command.size() > kMaxCommandBytes) return false;
      bytes += entry.command.size() + 24;
    }
    return bytes <= kMaxAppendBytes;
  }
  if (message.kind == MessageKind::kSnapshot) {
    const auto& chunk = message.snapshot;
    return chunk.index && chunk.index != UINT64_MAX && chunk.term && chunk.term <= message.term &&
           chunk.total <= FileStore::kMaxSnapshotBytes && chunk.offset <= chunk.total &&
           chunk.data.size() <= chunk.total - chunk.offset &&
           chunk.data.size() <= kSnapshotChunkBytes &&
           chunk.done == (chunk.offset + chunk.data.size() == chunk.total) &&
           (chunk.done || !chunk.data.empty()) &&
           (chunk.index <= log_.CommitIndex() || chunk.term >= log_.TermAt(log_.CommitIndex()));
  }
  return message.kind == MessageKind::kVoteReply || message.kind == MessageKind::kAppendReply ||
         message.kind == MessageKind::kSnapshotReply;
}

Status Node::ReceiveAsync(Message message, ReplyCallback callback) {
  if (!queue_) {
    callback(Receive(message));
    return Status::OK();
  }
  if (failed()) return Status::IOError("Raft node failed; restart required");
  if (!ValidAsyncMessage(message)) {
    callback(std::nullopt);
    return Status::OK();
  }
  const bool leader_message =
      message.kind == MessageKind::kAppend || message.kind == MessageKind::kSnapshot;
  if (message.term > observed_term_ ||
      (leader_message && message.term >= term() && election_.role() != Role::kFollower))
    Fence(message.term);
  if (pending_write_) {
    // Replies to requests already sent from the durable prefix may still
    // advance commitment and fresh read quorums. They never append to the log.
    if (IsLeader() && message.term == term() && !leader_message &&
        message.kind != MessageKind::kVote) {
      callback(Receive(message));
      return Status::OK();
    }
    return Status::Busy("a persistence barrier is pending");
  }
  if (message.term < observed_term_ && observed_term_ > term())
    return Status::Busy("a higher observed term must become durable");
  if (message.kind == MessageKind::kVote) {
    auto prepared = election_.PrepareVoteRequest(
        {message.term, message.from, message.last_index, message.last_term}, log_.LastIndex(),
        log_.LastTerm());
    if (!prepared.ok()) return prepared.status();
    const auto granted = prepared.value().granted;
    return StartElection(prepared.TakeValue(), [this, message = std::move(message), granted,
                                                callback = std::move(callback)](Status status) {
      if (!status.ok()) {
        callback(std::nullopt);
        return;
      }
      if (granted) ResetElection();
      Message reply;
      reply.kind = MessageKind::kVoteReply;
      reply.from = id();
      reply.to = message.from;
      reply.sequence = message.sequence;
      reply.term = term();
      reply.granted = granted;
      callback(std::move(reply));
    });
  }
  if (message.kind == MessageKind::kAppend || message.kind == MessageKind::kSnapshot) {
    if (message.term < term()) {
      Message reply;
      reply.kind = message.kind == MessageKind::kAppend ? MessageKind::kAppendReply
                                                        : MessageKind::kSnapshotReply;
      reply.from = id();
      reply.to = message.from;
      reply.sequence = message.sequence;
      reply.term = term();
      reply.snapshot.index = message.snapshot.index;
      callback(std::move(reply));
      return Status::OK();
    }
    auto prepared = election_.PrepareObserveLeader(message.from, message.term);
    if (!prepared.ok()) return prepared.status();
    return StartElection(
        prepared.TakeValue(), [this, message = std::move(message),
                               callback = std::move(callback)](Status status) mutable {
          if (!status.ok()) {
            callback(std::nullopt);
            return;
          }
          leader_id_ = message.from;
          ResetElection();
          // Keep a second copy of this small callback for admission failure at the
          // second barrier (the hard state may already be durable).
          auto failed_reply = callback;
          auto admitted = message.kind == MessageKind::kAppend
                              ? ReceiveAppendAsync(std::move(message), std::move(callback))
                              : ReceiveSnapshotAsync(std::move(message), std::move(callback));
          if (!admitted.ok()) failed_reply(std::nullopt);
        });
  }
  if (message.term > term()) {
    auto prepared = election_.PrepareVoteResponse(message.from, {message.term, false});
    if (!prepared.ok()) return prepared.status();
    return StartElection(prepared.TakeValue(),
                         [callback = std::move(callback)](Status) { callback(std::nullopt); });
  }
  callback(Receive(message));
  return Status::OK();
}

Status Node::ReceiveAppendAsync(Message message, ReplyCallback callback) {
  auto prepared = log_.PrepareAppend(message.append);
  if (!prepared.ok()) {
    if (prepared.status().code() != StatusCode::kInvalidArgument) failed_ = true;
    callback(std::nullopt);
    return Status::OK();
  }
  const auto epoch = leadership_epoch_;
  return AppendAsync(
      prepared.TakeValue(), [this, message = std::move(message), epoch,
                             callback = std::move(callback)](Result<AppendReply> result) {
        if (result.ok() && result.value().success)
          Collect(std::move(result.value().newly_committed));
        if (!result.ok() || epoch != leadership_epoch_ || message.term != term()) {
          callback(std::nullopt);
          return;
        }
        Message reply;
        reply.kind = MessageKind::kAppendReply;
        reply.from = id();
        reply.to = message.from;
        reply.sequence = message.sequence;
        reply.term = term();
        reply.read_context = message.read_context;
        reply.success = result.value().success;
        reply.conflict_index = result.value().conflict_index;
        reply.conflict_term = result.value().conflict_term;
        if (reply.success) {
          reply.match_index = message.append.entries.empty() ? message.append.prev_index
                                                             : message.append.entries.back().index;
        }
        callback(std::move(reply));
      });
}

Status Node::ReceiveSnapshotAsync(Message message, ReplyCallback callback) {
  const auto& chunk = message.snapshot;
  Message reply;
  reply.kind = MessageKind::kSnapshotReply;
  reply.from = id();
  reply.to = message.from;
  reply.sequence = message.sequence;
  reply.term = term();
  reply.snapshot.index = chunk.index;
  if (chunk.index <= log_.CommitIndex()) {
    reply.success = true;
    reply.match_index = chunk.index;
    reply.snapshot.offset = chunk.total;
    callback(std::move(reply));
    return Status::OK();
  }
  const bool same = incoming_snapshot_ && incoming_snapshot_->from == message.from &&
                    incoming_snapshot_->leader_term == message.term &&
                    incoming_snapshot_->chunk.index == chunk.index &&
                    incoming_snapshot_->chunk.term == chunk.term &&
                    incoming_snapshot_->chunk.total == chunk.total;
  if (!same) {
    if (chunk.offset != 0) {
      callback(std::move(reply));
      return Status::OK();
    }
    incoming_snapshot_ = IncomingSnapshot{
        message.from, message.term, {chunk.index, chunk.term, 0, chunk.total, {}, false}};
  }
  auto& data = incoming_snapshot_->chunk.data;
  if (chunk.offset == data.size()) {
    data += chunk.data;
  } else if (chunk.offset > data.size() || chunk.data.size() > data.size() - chunk.offset ||
             data.compare(chunk.offset, chunk.data.size(), chunk.data) != 0) {
    incoming_snapshot_.reset();
    callback(std::move(reply));
    return Status::OK();
  }
  reply.snapshot.offset = data.size();
  if (data.size() != chunk.total) {
    reply.success = true;
    callback(std::move(reply));
    return Status::OK();
  }
  if (config_.validate_snapshot && !config_.validate_snapshot(data)) {
    incoming_snapshot_.reset();
    reply.snapshot.offset = 0;
    callback(std::move(reply));
    return Status::OK();
  }
  Snapshot snapshot{chunk.index, chunk.term, std::move(data)};
  incoming_snapshot_.reset();
  auto prepared = log_.PrepareInstallSnapshot(snapshot);
  if (!prepared.ok()) {
    failed_ = true;
    callback(std::nullopt);
    return Status::OK();
  }
  const auto epoch = leadership_epoch_;
  return SnapshotAsync(prepared.TakeValue(),
                       [this, snapshot = std::move(snapshot), reply, epoch,
                        callback = std::move(callback)](Status status) mutable {
                         if (!status.ok()) {
                           callback(std::nullopt);
                           return;
                         }
                         // Installation must reach the state machine even if a higher term arrived
                         // meanwhile; the old transport's affirmative reply must still be
                         // suppressed.
                         committed_.clear();
                         installed_snapshot_ = std::move(snapshot);
                         if (epoch != leadership_epoch_) {
                           callback(std::nullopt);
                           return;
                         }
                         reply.match_index = reply.snapshot.index;
                         reply.success = true;
                         callback(std::move(reply));
                       });
}

}  // namespace yewukv::raft
