#include <gtest/gtest.h>

#include <array>
#include <deque>
#include <map>
#include <memory>
#include <random>
#include <set>
#include <stdexcept>
#include <utility>

#include "raft/node.h"

namespace {
using namespace yewukv::raft;

struct MemoryStore : HardStateStore, LogStore {
  DurableState state;
  bool fail = false;
  size_t log_saves = 0;
  yewukv::Status Save(const HardState& next) override {
    if (fail) return yewukv::Status::IOError("injected disk failure");
    state.hard_state = next;
    return yewukv::Status::OK();
  }
  yewukv::Status ReplaceFrom(uint64_t index, const std::vector<LogEntry>& suffix) override {
    if (fail) return yewukv::Status::IOError("injected disk failure");
    state.entries.resize(index - state.snapshot.index - 1);
    state.entries.insert(state.entries.end(), suffix.begin(), suffix.end());
    ++log_saves;
    return yewukv::Status::OK();
  }
  yewukv::Status SaveSnapshot(const Snapshot& snapshot,
                              const std::vector<LogEntry>& suffix) override {
    if (fail) return yewukv::Status::IOError("injected snapshot failure");
    state.snapshot = snapshot;
    state.entries = suffix;
    return yewukv::Status::OK();
  }
};

class Cluster {
 public:
  explicit Cluster(uint32_t seed = 1) : seed_(seed) {
    for (size_t i = 0; i < 3; ++i) Restart(i);
  }
  void Restart(size_t i) {
    nodes[i].reset();
    applied[i].clear();
    base[i] = stores[i].state.snapshot.index;
    Node::Config config;
    config.id = i + 1;
    config.voters = {1, 2, 3};
    config.seed = seed_;
    nodes[i] = std::make_unique<Node>(config, stores[i].state, stores[i], stores[i]);
  }
  void Collect(size_t i) {
    if (!nodes[i]) return;
    for (auto& message : nodes[i]->TakeMessages()) messages.push_back(std::move(message));
    if (auto snapshot = nodes[i]->TakeInstalledSnapshot()) {
      applied[i].clear();
      base[i] = snapshot->index;
    }
    for (const auto& entry : nodes[i]->TakeCommitted()) {
      ASSERT_EQ(entry.index, base[i] + applied[i].size() + 1);
      applied[i].push_back(entry);
      auto [it, inserted] = committed.emplace(entry.index, entry);
      if (!inserted) {
        ASSERT_EQ(it->second.term, entry.term);
        ASSERT_EQ(it->second.command, entry.command);
      }
    }
  }
  void Pump(bool disorder = false, uint32_t seed = 0) {
    std::minstd_rand random(seed + 1);
    size_t delivered = 0;
    while (!messages.empty()) {
      ASSERT_LT(++delivered, 20000);
      size_t selected = disorder ? random() % messages.size() : 0;
      auto message = std::move(messages[selected]);
      messages.erase(messages.begin() + selected);
      const auto from = message.from - 1;
      const auto to = message.to - 1;
      if (!nodes[from] || !nodes[to] || isolated.count(from) || isolated.count(to)) continue;
      if (disorder && random() % 13 == 0) continue;
      if (auto reply = nodes[to]->Receive(message)) {
        if (!muted_replies.count(to)) {
          messages.push_back(*reply);
          if (disorder && random() % 5 == 0) messages.push_back(*reply);
        }
      }
      Collect(to);
    }
  }
  void Tick(size_t count = 1, bool disorder = false) {
    for (size_t tick = 0; tick < count; ++tick) {
      for (size_t i = 0; i < 3; ++i) {
        if (nodes[i]) nodes[i]->Tick();
        Collect(i);
      }
      Pump(disorder, ticks_++);
      // A minority can retain an old leader role, but connected voters must
      // never elect two leaders in the same term, including across restarts.
      for (const auto& node : nodes) {
        if (node && node->IsLeader()) {
          auto [it, inserted] = leaders.emplace(node->term(), node->id());
          ASSERT_TRUE(inserted || it->second == node->id());
        }
      }
    }
  }
  int Leader() const {
    for (size_t i = 0; i < 3; ++i) {
      if (nodes[i] && !isolated.count(i) && nodes[i]->IsLeader()) return static_cast<int>(i);
    }
    return -1;
  }
  std::array<MemoryStore, 3> stores;
  std::array<std::unique_ptr<Node>, 3> nodes;
  std::array<std::vector<LogEntry>, 3> applied;
  std::array<uint64_t, 3> base{};
  std::set<size_t> isolated;
  std::set<size_t> muted_replies;
  std::deque<Message> messages;
  std::map<uint64_t, LogEntry> committed;
  std::map<uint64_t, uint64_t> leaders;

 private:
  uint32_t seed_;
  uint32_t ticks_ = 0;
};

TEST(RaftNode, ElectsOneLeaderAndCommitsCurrentTermNoopBeforeClientCommands) {
  Cluster cluster;
  cluster.Tick(50);
  const int leader = cluster.Leader();
  ASSERT_GE(leader, 0);
  ASSERT_GE(cluster.nodes[leader]->CommitIndex(), 1);
  auto proposed = cluster.nodes[leader]->Propose("put:key=value");
  ASSERT_TRUE(proposed.ok());
  cluster.Collect(leader);
  cluster.Pump();
  cluster.Tick(5);
  for (size_t i = 0; i < 3; ++i) {
    ASSERT_EQ(cluster.nodes[i]->CommitIndex(), proposed.value());
    EXPECT_EQ(cluster.applied[i].back().command, "put:key=value");
  }
}

TEST(RaftNode, IsolatedOldLeaderCannotCommitReadsOrWritesAndConflictIsReplaced) {
  Cluster cluster;
  cluster.Tick(50);
  const int old = cluster.Leader();
  ASSERT_GE(old, 0);
  const auto committed_before = cluster.nodes[old]->CommitIndex();
  cluster.isolated.insert(old);
  const auto rejected = cluster.nodes[old]->Propose("isolated-read-barrier");
  ASSERT_TRUE(rejected.ok());
  cluster.Collect(old);
  cluster.Tick(100);
  EXPECT_EQ(cluster.nodes[old]->CommitIndex(), committed_before);
  const int leader = cluster.Leader();
  ASSERT_GE(leader, 0);
  ASSERT_NE(leader, old);
  const auto accepted = cluster.nodes[leader]->Propose("majority-write");
  ASSERT_TRUE(accepted.ok());
  cluster.Collect(leader);
  cluster.Tick(5);
  ASSERT_GE(cluster.nodes[leader]->CommitIndex(), accepted.value());
  cluster.isolated.clear();
  cluster.Tick(60);
  EXPECT_FALSE(cluster.nodes[old]->IsLeader());
  for (size_t i = 0; i < 3; ++i) {
    EXPECT_EQ(cluster.nodes[i]->CommitIndex(), cluster.nodes[leader]->CommitIndex());
    for (const auto& entry : cluster.applied[i]) EXPECT_NE(entry.command, "isolated-read-barrier");
    EXPECT_EQ(cluster.applied[i].back().command, "majority-write");
  }
}

TEST(RaftNode, AcknowledgedPrefixSurvivesLeaderAndAllNodeRestarts) {
  Cluster cluster;
  cluster.Tick(50);
  int leader = cluster.Leader();
  ASSERT_GE(leader, 0);
  auto index = cluster.nodes[leader]->Propose("durable");
  ASSERT_TRUE(index.ok());
  cluster.Collect(leader);
  cluster.Tick(5);
  ASSERT_GE(cluster.nodes[leader]->CommitIndex(), index.value());
  cluster.nodes[leader].reset();
  cluster.Tick(80);
  ASSERT_GE(cluster.Leader(), 0);
  cluster.Restart(leader);
  cluster.Tick(30);
  for (size_t i = 0; i < 3; ++i) cluster.nodes[i].reset();
  for (size_t i = 0; i < 3; ++i) cluster.Restart(i);
  cluster.Tick(80);
  for (size_t i = 0; i < 3; ++i) {
    ASSERT_GE(cluster.applied[i].size(), index.value());
    EXPECT_EQ(cluster.applied[i][index.value() - 1].command, "durable");
  }
}

TEST(RaftNode, BackfillsLongLogsInBoundedBatches) {
  Cluster cluster;
  cluster.Tick(40);
  const int leader = cluster.Leader();
  ASSERT_GE(leader, 0);
  const size_t lagging = (leader + 1) % 3;
  cluster.nodes[lagging].reset();
  for (size_t i = 0; i < 100; ++i)
    ASSERT_TRUE(cluster.nodes[leader]->Propose(std::string(1000, 'a' + i % 26)).ok());
  cluster.Collect(leader);
  cluster.Tick(30);
  const auto committed = cluster.nodes[leader]->CommitIndex();
  ASSERT_GE(committed, 101);
  cluster.Restart(lagging);
  cluster.Tick(60);
  EXPECT_EQ(cluster.nodes[lagging]->CommitIndex(), committed);
  EXPECT_EQ(cluster.applied[lagging].size(), committed);
}

TEST(RaftNode, FixedSeedChaosPreservesCommittedHistoryAndConvergesAfterHealing) {
  for (uint32_t seed = 1; seed <= 100; ++seed) {
    SCOPED_TRACE(seed);
    Cluster cluster(seed);
    std::minstd_rand random(seed);
    for (size_t step = 0; step < 1000; ++step) {
      if (step % 31 == 0) {
        cluster.isolated.clear();
        cluster.isolated.insert(random() % 3);
      }
      if (step % 47 == 0) cluster.Restart(random() % 3);
      const int leader = cluster.Leader();
      if (leader >= 0) {
        ASSERT_TRUE(
            cluster.nodes[leader]->Propose(std::to_string(seed) + ":" + std::to_string(step)).ok());
        cluster.Collect(leader);
      }
      cluster.Tick(1, true);
      if (step % 29 == 0) {
        const size_t selected = random() % 3;
        const auto applied = cluster.base[selected] + cluster.applied[selected].size();
        if (applied > cluster.nodes[selected]->snapshot().index) {
          ASSERT_TRUE(
              cluster.nodes[selected]->Compact(applied, "state-" + std::to_string(applied)).ok());
        }
      }
    }
    cluster.isolated.clear();
    cluster.Tick(100);
    const int leader = cluster.Leader();
    ASSERT_GE(leader, 0);
    for (size_t i = 0; i < 3; ++i) {
      EXPECT_EQ(cluster.nodes[i]->CommitIndex(), cluster.nodes[leader]->CommitIndex());
      EXPECT_EQ(cluster.base[i] + cluster.applied[i].size(),
                cluster.base[leader] + cluster.applied[leader].size());
    }
  }
}

TEST(RaftNode, SendsChunkedSnapshotToOfflineFollowerAndContinuesReplication) {
  Cluster cluster;
  cluster.Tick(50);
  const auto leader = cluster.Leader();
  ASSERT_GE(leader, 0);
  const size_t lagging = (leader + 1) % 3;
  cluster.nodes[lagging].reset();
  for (size_t i = 0; i < 40; ++i) {
    ASSERT_TRUE(cluster.nodes[leader]->Propose("value-" + std::to_string(i)).ok());
    cluster.Collect(leader);
    cluster.Tick(2);
  }
  const auto covered = cluster.nodes[leader]->CommitIndex();
  const std::string state(Node::kSnapshotChunkBytes * 3 + 19, 's');
  ASSERT_TRUE(cluster.nodes[leader]->Compact(covered, state).ok());
  cluster.Restart(lagging);
  cluster.Tick(100, true);
  ASSERT_EQ(cluster.nodes[lagging]->snapshot().index, covered);
  EXPECT_EQ(cluster.nodes[lagging]->snapshot().data, state);
  ASSERT_TRUE(cluster.nodes[leader]->Propose("after-snapshot").ok());
  cluster.Collect(leader);
  cluster.Tick(30);
  EXPECT_EQ(cluster.nodes[lagging]->CommitIndex(), cluster.nodes[leader]->CommitIndex());
  EXPECT_EQ(cluster.applied[lagging].back().command, "after-snapshot");
  cluster.Restart(lagging);
  cluster.Tick(40);
  EXPECT_EQ(cluster.nodes[lagging]->snapshot().data, state);
  EXPECT_EQ(cluster.applied[lagging].back().command, "after-snapshot");
}

TEST(RaftNode, FollowerCompactionAheadOfLostAcknowledgementDoesNotLivelock) {
  Cluster cluster;
  cluster.Tick(50);
  const int leader = cluster.Leader();
  ASSERT_GE(leader, 0);
  const size_t follower = (leader + 1) % 3;
  cluster.muted_replies.insert(follower);
  ASSERT_TRUE(cluster.nodes[leader]->ProposeBatch(std::vector<std::string>(16, "value")).ok());
  cluster.Collect(leader);
  cluster.Tick(40);
  const auto covered = cluster.nodes[follower]->CommitIndex();
  ASSERT_EQ(covered, cluster.nodes[leader]->CommitIndex());
  ASSERT_GT(covered, 1);
  ASSERT_TRUE(cluster.nodes[follower]->Compact(covered, "state").ok());
  cluster.muted_replies.clear();
  cluster.Tick(40);
  ASSERT_TRUE(cluster.nodes[leader]->Propose("after-probe").ok());
  cluster.Collect(leader);
  cluster.Tick(30);
  EXPECT_EQ(cluster.nodes[follower]->CommitIndex(), covered + 1);
  EXPECT_EQ(cluster.applied[follower].back().command, "after-probe");
}

TEST(RaftNode, PersistenceFailurePreventsElectionOrAppendAcknowledgement) {
  MemoryStore store;
  Node node({1, {1, 2, 3}}, {}, store, store);
  store.fail = true;
  EXPECT_FALSE(node.Campaign().ok());
  EXPECT_TRUE(node.failed());
  EXPECT_TRUE(node.TakeMessages().empty());
  MemoryStore follower_store;
  Node follower({2, {1, 2, 3}}, {}, follower_store, follower_store);
  Message append;
  append.kind = MessageKind::kAppend;
  append.from = 1;
  append.to = 2;
  append.term = append.sequence = 1;
  append.append.entries = {{1, 1, "command"}};
  follower_store.fail = true;
  EXPECT_FALSE(follower.Receive(append).has_value());
  EXPECT_TRUE(follower.failed());
  EXPECT_EQ(follower.LastIndex(), 0);
}

TEST(RaftNode, RejectsUnknownPeersAndFutureTermEntriesWithoutMutatingState) {
  MemoryStore store;
  Node node({1, {1, 2, 3}}, {}, store, store);
  Message append;
  append.kind = MessageKind::kAppend;
  append.from = 9;
  append.to = 1;
  append.term = append.sequence = 1;
  EXPECT_FALSE(node.Receive(append).has_value());
  append.from = 2;
  append.append.entries = {{1, 2, "future"}};
  EXPECT_FALSE(node.Receive(append).has_value());
  EXPECT_EQ(node.term(), 0);
  EXPECT_EQ(node.LastIndex(), 0);
  EXPECT_FALSE(node.Propose("follower").ok());
}

TEST(RaftNode, BatchesDurableProposalsWithoutRelaxingMajorityCommit) {
  Cluster cluster;
  cluster.Tick(40);
  const int leader = cluster.Leader();
  ASSERT_GE(leader, 0);
  const auto saves = cluster.stores[leader].log_saves;
  const auto previous_commit = cluster.nodes[leader]->CommitIndex();
  auto result = cluster.nodes[leader]->ProposeBatch({"put-a", "read-a", "delete-a"});
  ASSERT_TRUE(result.ok());
  ASSERT_EQ(result.value().size(), 3);
  EXPECT_EQ(cluster.stores[leader].log_saves, saves + 1);
  EXPECT_EQ(cluster.nodes[leader]->CommitIndex(), previous_commit);
  EXPECT_FALSE(cluster.nodes[leader]->ProposeBatch({"valid", ""}).ok());
  EXPECT_FALSE(cluster.nodes[leader]->ProposeBatch(std::vector<std::string>(33, "too-many")).ok());
  EXPECT_EQ(cluster.stores[leader].log_saves, saves + 1);
  cluster.Collect(leader);
  cluster.Tick(5);
  for (size_t i = 0; i < 3; ++i) {
    EXPECT_EQ(cluster.nodes[i]->CommitIndex(), result.value().back());
    EXPECT_EQ(cluster.applied[i][result.value()[0] - 1].command, "put-a");
    EXPECT_EQ(cluster.applied[i][result.value()[1] - 1].command, "read-a");
    EXPECT_EQ(cluster.applied[i][result.value()[2] - 1].command, "delete-a");
  }
}

}  // namespace
