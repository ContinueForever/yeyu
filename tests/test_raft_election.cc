#include <gtest/gtest.h>

#include <stdexcept>
#include <vector>

#include "raft/election.h"

namespace {

class MemoryHardStateStore final : public yewukv::raft::HardStateStore {
 public:
  yewukv::Status Save(const yewukv::raft::HardState& state) override {
    if (throw_on_save) throw std::runtime_error("injected exception");
    if (fail) return yewukv::Status::IOError("injected persistence failure");
    writes.push_back(state);
    return yewukv::Status::OK();
  }

  bool fail = false;
  bool throw_on_save = false;
  std::vector<yewukv::raft::HardState> writes;
};

using yewukv::raft::Election;
using yewukv::raft::OutboundVote;
using yewukv::raft::Role;
using yewukv::raft::VoteRequest;

TEST(RaftElection, PersistsSelfVoteBeforeCampaignProducesRequests) {
  MemoryHardStateStore store;
  Election node(1, {1, 2, 3}, {}, store);
  std::vector<OutboundVote> outbound;
  ASSERT_TRUE(node.Campaign(9, 4, &outbound).ok());
  ASSERT_EQ(store.writes.size(), 1);
  EXPECT_EQ(store.writes[0].term, 1);
  EXPECT_EQ(store.writes[0].voted_for, 1);
  EXPECT_EQ(node.role(), Role::kCandidate);
  ASSERT_EQ(outbound.size(), 2);
  EXPECT_EQ(outbound[0].peer_id, 2);
  EXPECT_EQ(outbound[0].request.last_log_index, 9);
  EXPECT_EQ(outbound[0].request.last_log_term, 4);
  EXPECT_EQ(outbound[1].peer_id, 3);
}

TEST(RaftElection, NeverGrantsTwoVotesInOneTermEvenAfterRestart) {
  MemoryHardStateStore store;
  Election follower(1, {1, 2, 3}, {2, 0}, store);
  const VoteRequest first{2, 2, 7, 3};
  auto response = follower.HandleVoteRequest(first, 7, 3);
  ASSERT_TRUE(response.ok());
  EXPECT_TRUE(response.value().granted);
  ASSERT_EQ(store.writes.size(), 1);
  EXPECT_EQ(store.writes.back().voted_for, 2);

  response = follower.HandleVoteRequest({2, 3, 7, 3}, 7, 3);
  ASSERT_TRUE(response.ok());
  EXPECT_FALSE(response.value().granted);
  response = follower.HandleVoteRequest(first, 7, 3);
  ASSERT_TRUE(response.ok());
  EXPECT_TRUE(response.value().granted);
  EXPECT_EQ(store.writes.size(), 1);

  Election restarted(1, {1, 2, 3}, store.writes.back(), store);
  response = restarted.HandleVoteRequest({2, 3, 7, 3}, 7, 3);
  ASSERT_TRUE(response.ok());
  EXPECT_FALSE(response.value().granted);
}

TEST(RaftElection, ComparesLastLogTermBeforeIndexAndPersistsNewTerm) {
  MemoryHardStateStore store;
  Election follower(1, {1, 2, 3}, {3, 2}, store);
  auto response = follower.HandleVoteRequest({4, 3, 1000, 4}, 8, 5);
  ASSERT_TRUE(response.ok());
  EXPECT_FALSE(response.value().granted);
  EXPECT_EQ(follower.hard_state().term, 4);
  EXPECT_EQ(follower.hard_state().voted_for, 0);
  ASSERT_EQ(store.writes.size(), 1);

  response = follower.HandleVoteRequest({4, 3, 1, 6}, 8, 5);
  ASSERT_TRUE(response.ok());
  EXPECT_TRUE(response.value().granted);
  EXPECT_EQ(store.writes.back().voted_for, 3);
}

TEST(RaftElection, MajorityPromotesOnceAndHigherTermDemotes) {
  MemoryHardStateStore store;
  Election node(1, {1, 2, 3}, {}, store);
  std::vector<OutboundVote> outbound;
  ASSERT_TRUE(node.Campaign(0, 0, &outbound).ok());
  ASSERT_TRUE(node.HandleVoteResponse(2, {1, true}).ok());
  EXPECT_EQ(node.role(), Role::kLeader);
  ASSERT_TRUE(node.HandleVoteResponse(2, {1, true}).ok());
  EXPECT_EQ(node.role(), Role::kLeader);
  ASSERT_TRUE(node.HandleVoteResponse(3, {3, false}).ok());
  EXPECT_EQ(node.role(), Role::kFollower);
  EXPECT_EQ(node.hard_state().term, 3);
  EXPECT_EQ(node.hard_state().voted_for, 0);
  EXPECT_EQ(store.writes.back().term, 3);
  ASSERT_TRUE(node.HandleVoteResponse(2, {1, true}).ok());
  EXPECT_EQ(node.role(), Role::kFollower);
}

TEST(RaftElection, PersistenceFailurePreventsVotingOrLeadership) {
  MemoryHardStateStore store;
  store.fail = true;
  Election node(1, {1, 2, 3}, {}, store);
  std::vector<OutboundVote> outbound;
  EXPECT_FALSE(node.Campaign(0, 0, &outbound).ok());
  EXPECT_TRUE(outbound.empty());
  EXPECT_EQ(node.role(), Role::kFollower);
  EXPECT_EQ(node.hard_state().term, 0);
  EXPECT_TRUE(node.failed());
  EXPECT_FALSE(node.HandleVoteRequest({1, 2, 0, 0}, 0, 0).ok());

  MemoryHardStateStore other_store;
  Election other(1, {1, 2, 3}, {}, other_store);
  other_store.fail = true;
  auto response = other.HandleVoteRequest({1, 2, 0, 0}, 0, 0);
  EXPECT_FALSE(response.ok());
  EXPECT_TRUE(other.failed());
  EXPECT_EQ(other.hard_state().term, 0);

  MemoryHardStateStore throwing_store;
  Election throwing_node(1, {1, 2, 3}, {}, throwing_store);
  throwing_store.throw_on_save = true;
  outbound.push_back({2, {9, 1, 0, 0}});
  EXPECT_FALSE(throwing_node.Campaign(0, 0, &outbound).ok());
  EXPECT_TRUE(outbound.empty());
  EXPECT_TRUE(throwing_node.failed());
}

TEST(RaftElection, SameTermLeaderMessageDemotesCandidate) {
  MemoryHardStateStore store;
  Election node(1, {1, 2, 3}, {}, store);
  std::vector<OutboundVote> outbound;
  ASSERT_TRUE(node.Campaign(0, 0, &outbound).ok());
  ASSERT_TRUE(node.ObserveLeader(2, 1).ok());
  EXPECT_EQ(node.role(), Role::kFollower);
  EXPECT_EQ(node.hard_state().voted_for, 1);
  EXPECT_EQ(store.writes.size(), 1);
  EXPECT_EQ(node.ObserveLeader(2, 0).code(), yewukv::StatusCode::kBusy);
}

}  // namespace
