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

Status Election::Persist(HardState next) {
  Status status;
  try {
    status = store_.Save(next);
  } catch (const std::exception& error) {
    failed_ = true;
    return Status::IOError(std::string("Raft hard-state save threw: ") + error.what());
  } catch (...) {
    failed_ = true;
    return Status::IOError("Raft hard-state save threw");
  }
  if (!status.ok()) {
    failed_ = true;
    return status;
  }
  hard_state_ = next;
  return Status::OK();
}

bool Election::IsVoter(uint64_t id) const {
  return std::find(voters_.begin(), voters_.end(), id) != voters_.end();
}

Status Election::Campaign(uint64_t last_log_index, uint64_t last_log_term,
                          std::vector<OutboundVote>* outbound) {
  Status usable = CheckUsable();
  if (!usable.ok()) return usable;
  if (outbound == nullptr) return Status::InvalidArgument("outbound votes pointer is null");
  outbound->clear();
  if (role_ == Role::kLeader) return Status::Busy("leader cannot campaign");
  if (hard_state_.term == std::numeric_limits<uint64_t>::max())
    return Status::Busy("Raft term exhausted");

  const HardState next{hard_state_.term + 1, node_id_};
  Status status = Persist(next);
  if (!status.ok()) return status;
  role_ = Role::kCandidate;
  votes_ = {node_id_};
  for (uint64_t peer : voters_) {
    if (peer != node_id_)
      outbound->push_back({peer, {next.term, node_id_, last_log_index, last_log_term}});
  }
  return Status::OK();
}

Result<VoteResponse> Election::HandleVoteRequest(const VoteRequest& request,
                                                 uint64_t last_log_index, uint64_t last_log_term) {
  Status usable = CheckUsable();
  if (!usable.ok()) return usable;
  if (!IsVoter(request.candidate_id) || request.term == 0)
    return VoteResponse{hard_state_.term, false};
  if (request.term < hard_state_.term) return VoteResponse{hard_state_.term, false};

  const bool higher_term = request.term > hard_state_.term;
  const bool log_is_fresh =
      request.last_log_term > last_log_term ||
      (request.last_log_term == last_log_term && request.last_log_index >= last_log_index);
  const uint64_t previous_vote = higher_term ? 0 : hard_state_.voted_for;
  const bool grant = log_is_fresh && (previous_vote == 0 || previous_vote == request.candidate_id);
  const HardState next{request.term, grant ? request.candidate_id : previous_vote};
  if (next.term != hard_state_.term || next.voted_for != hard_state_.voted_for) {
    Status status = Persist(next);
    if (!status.ok()) return status;
  }
  if (higher_term) {
    role_ = Role::kFollower;
    votes_.clear();
  }
  return VoteResponse{hard_state_.term, grant};
}

Status Election::HandleVoteResponse(uint64_t peer_id, const VoteResponse& response) {
  Status usable = CheckUsable();
  if (!usable.ok()) return usable;
  if (!IsVoter(peer_id) || peer_id == node_id_) return Status::InvalidArgument("unknown voter");
  if (response.term > hard_state_.term) {
    Status status = Persist({response.term, 0});
    if (!status.ok()) return status;
    role_ = Role::kFollower;
    votes_.clear();
    return Status::OK();
  }
  if (role_ != Role::kCandidate || response.term != hard_state_.term || !response.granted)
    return Status::OK();
  votes_.insert(peer_id);
  if (votes_.size() > voters_.size() / 2) role_ = Role::kLeader;
  return Status::OK();
}

Status Election::ObserveLeader(uint64_t leader_id, uint64_t term) {
  Status usable = CheckUsable();
  if (!usable.ok()) return usable;
  if (!IsVoter(leader_id) || leader_id == node_id_)
    return Status::InvalidArgument("unknown leader");
  if (term < hard_state_.term) return Status::Busy("stale leader term");
  if (term > hard_state_.term) {
    Status status = Persist({term, 0});
    if (!status.ok()) return status;
  }
  role_ = Role::kFollower;
  votes_.clear();
  return Status::OK();
}

}  // namespace yewukv::raft
