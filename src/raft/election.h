#pragma once

#include <cstdint>
#include <set>
#include <vector>

#include "common/status.h"

namespace yewukv::raft {

enum class Role { kFollower, kCandidate, kLeader };

struct HardState {
  uint64_t term = 0;
  uint64_t voted_for = 0;  // Zero means no vote in this term.
};

// Implementations must make the entire hard state durable before returning OK.
// A failed save makes the Election instance unusable until reconstructed.
class HardStateStore {
 public:
  virtual ~HardStateStore() = default;
  virtual Status Save(const HardState& state) = 0;
};

struct VoteRequest {
  uint64_t term = 0;
  uint64_t candidate_id = 0;
  uint64_t last_log_index = 0;
  uint64_t last_log_term = 0;
};

struct VoteResponse {
  uint64_t term = 0;
  bool granted = false;
};

struct OutboundVote {
  uint64_t peer_id = 0;
  VoteRequest request;
};

// Deterministic election-only core for a fixed three-voter group. Callers
// provide the last DURABLE log position. There are no timers or sockets here.
class Election {
 public:
  Election(uint64_t node_id, std::vector<uint64_t> voters, HardState recovered,
           HardStateStore& store);

  Status Campaign(uint64_t last_log_index, uint64_t last_log_term,
                  std::vector<OutboundVote>* outbound);
  Result<VoteResponse> HandleVoteRequest(const VoteRequest& request, uint64_t last_log_index,
                                         uint64_t last_log_term);
  Status HandleVoteResponse(uint64_t peer_id, const VoteResponse& response);
  // Call only after validating an AppendEntries message from a configured peer.
  Status ObserveLeader(uint64_t leader_id, uint64_t term);

  Role role() const { return role_; }
  const HardState& hard_state() const { return hard_state_; }
  bool failed() const { return failed_; }

 private:
  Status CheckUsable() const;
  Status Persist(HardState next);
  bool IsVoter(uint64_t id) const;

  const uint64_t node_id_;
  const std::vector<uint64_t> voters_;
  HardStateStore& store_;
  HardState hard_state_;
  Role role_ = Role::kFollower;
  std::set<uint64_t> votes_;
  bool failed_ = false;
};

}  // namespace yewukv::raft
