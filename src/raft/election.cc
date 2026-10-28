#include "raft/election.h"

#include <algorithm>
#include <limits>
#include <stdexcept>
#include <string>
#include <utility>

namespace yewukv::raft {

Election::Election(uint64_t node_id, std::vector<uint64_t> voters, HardState recovered,
                   HardStateStore& store)
    : node_id_(node_id), voters_(std::move(voters)), store_(store), hard_state_(recovered) {
  std::set<uint64_t> unique(voters_.begin(), voters_.end());
  if (voters_.size() != 3 || unique.size() != 3 || unique.count(0) != 0 ||
      unique.count(node_id_) == 0 ||
      (hard_state_.voted_for != 0 && unique.count(hard_state_.voted_for) == 0)) {
    throw std::invalid_argument("election requires three unique nonzero voters and a valid vote");
  }
}

Status Election::CheckUsable() const {
  return failed_ ? Status::IOError("Raft hard-state persistence failed; restart required")
                 : Status::OK();
}

Election::Update Election::Current() const {
  return {hard_state_, role_, votes_, false, false};
}

Status Election::Advance(Update update, Status persisted) {
  if (!persisted.ok()) {
    failed_ = true;
    return persisted;
  }
  hard_state_ = update.hard_state;
  role_ = update.role;
  votes_ = std::move(update.votes);
  return Status::OK();
}

void Election::StepDown() {
  role_ = Role::kFollower;
  votes_.clear();
}

Status Election::PersistUpdate(Update update) {
  Status status;
  try {
    if (update.persist) status = store_.Save(update.hard_state);
  } catch (const std::exception& error) {
    status = Status::IOError(std::string("Raft hard-state save threw: ") + error.what());
  } catch (...) {
    status = Status::IOError("Raft hard-state save threw");
  }
  return Advance(std::move(update), std::move(status));
}

bool Election::IsVoter(uint64_t id) const {
  return std::find(voters_.begin(), voters_.end(), id) != voters_.end();
}

Result<Election::Update> Election::PrepareCampaign() const {
  auto usable = CheckUsable();
  if (!usable.ok()) return usable;
  if (role_ == Role::kLeader) return Status::Busy("leader cannot campaign");
  if (hard_state_.term == std::numeric_limits<uint64_t>::max())
    return Status::Busy("Raft term exhausted");
  return Update{{hard_state_.term + 1, node_id_}, Role::kCandidate, {node_id_}, true, false};
}

Result<Election::Update> Election::PrepareVoteRequest(const VoteRequest& request,
                                                      uint64_t last_log_index,
                                                      uint64_t last_log_term) const {
  auto usable = CheckUsable();
  if (!usable.ok()) return usable;
  auto next = Current();
  if (!IsVoter(request.candidate_id) || request.term == 0 || request.term < hard_state_.term)
    return next;
  const bool higher_term = request.term > hard_state_.term;
  const bool fresh =
      request.last_log_term > last_log_term ||
      (request.last_log_term == last_log_term && request.last_log_index >= last_log_index);
  const auto previous_vote = higher_term ? 0 : hard_state_.voted_for;
  next.granted = fresh && (previous_vote == 0 || previous_vote == request.candidate_id);
  next.hard_state = {request.term, next.granted ? request.candidate_id : previous_vote};
  next.persist = next.hard_state.term != hard_state_.term ||
                 next.hard_state.voted_for != hard_state_.voted_for;
  if (higher_term) {
    next.role = Role::kFollower;
    next.votes.clear();
  }
  return next;
}

Result<Election::Update> Election::PrepareVoteResponse(uint64_t peer_id,
                                                       const VoteResponse& response) const {
  auto usable = CheckUsable();
  if (!usable.ok()) return usable;
  if (!IsVoter(peer_id) || peer_id == node_id_) return Status::InvalidArgument("unknown voter");
  auto next = Current();
  if (response.term > hard_state_.term) {
    next.hard_state = {response.term, 0};
    next.persist = true;
    next.role = Role::kFollower;
    next.votes.clear();
  } else if (role_ == Role::kCandidate && response.term == hard_state_.term && response.granted) {
    next.votes.insert(peer_id);
    if (next.votes.size() > voters_.size() / 2) next.role = Role::kLeader;
  }
  return next;
}

Result<Election::Update> Election::PrepareObserveLeader(uint64_t leader_id, uint64_t term) const {
  auto usable = CheckUsable();
  if (!usable.ok()) return usable;
  if (!IsVoter(leader_id) || leader_id == node_id_)
    return Status::InvalidArgument("unknown leader");
  if (term < hard_state_.term) return Status::Busy("stale leader term");
  auto next = Current();
  if (term > hard_state_.term) {
    next.hard_state = {term, 0};
    next.persist = true;
  }
  next.role = Role::kFollower;
  next.votes.clear();
  return next;
}

Status Election::Campaign(uint64_t last_log_index, uint64_t last_log_term,
                          std::vector<OutboundVote>* outbound) {
  if (outbound == nullptr) return Status::InvalidArgument("outbound votes pointer is null");
  outbound->clear();
  auto prepared = PrepareCampaign();
  if (!prepared.ok()) return prepared.status();
  const auto status = PersistUpdate(prepared.TakeValue());
  if (!status.ok()) return status;
  for (auto peer : voters_)
    if (peer != node_id_)
      outbound->push_back({peer, {hard_state_.term, node_id_, last_log_index, last_log_term}});
  return Status::OK();
}

Result<VoteResponse> Election::HandleVoteRequest(const VoteRequest& request,
                                                 uint64_t last_log_index, uint64_t last_log_term) {
  auto prepared = PrepareVoteRequest(request, last_log_index, last_log_term);
  if (!prepared.ok()) return prepared.status();
  const bool granted = prepared.value().granted;
  const auto status = PersistUpdate(prepared.TakeValue());
  if (!status.ok()) return status;
  return VoteResponse{hard_state_.term, granted};
}

Status Election::HandleVoteResponse(uint64_t peer_id, const VoteResponse& response) {
  auto prepared = PrepareVoteResponse(peer_id, response);
  return prepared.ok() ? PersistUpdate(prepared.TakeValue()) : prepared.status();
}

Status Election::ObserveLeader(uint64_t leader_id, uint64_t term) {
  auto prepared = PrepareObserveLeader(leader_id, term);
  return prepared.ok() ? PersistUpdate(prepared.TakeValue()) : prepared.status();
}

}  // namespace yewukv::raft
