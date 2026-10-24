#include <gtest/gtest.h>

#include <stdexcept>
#include <vector>

#include "raft/log.h"

namespace {

class MemoryLogStore final : public yewukv::raft::LogStore {
 public:
  yewukv::Status ReplaceFrom(uint64_t first_index,
                             const std::vector<yewukv::raft::LogEntry>& suffix) override {
    if (fail) return yewukv::Status::IOError("injected log persistence failure");
    saved_from.push_back(first_index);
    saved_suffix.push_back(suffix);
    return yewukv::Status::OK();
  }

  bool fail = false;
  std::vector<uint64_t> saved_from;
  std::vector<std::vector<yewukv::raft::LogEntry>> saved_suffix;
};

using yewukv::raft::AppendRequest;
using yewukv::raft::LogEntry;
using yewukv::raft::RaftLog;

TEST(RaftLog, AppendsDurablyBeforeSuccessAndAdvancesFollowerCommit) {
  MemoryLogStore store;
  RaftLog log({}, 0, store);
  AppendRequest request{0, 0, 2, {{1, 1, "a"}, {2, 1, "b"}}};
  auto reply = log.HandleAppend(request);
  ASSERT_TRUE(reply.ok());
  ASSERT_TRUE(reply.value().success);
  ASSERT_EQ(store.saved_from.size(), 1);
  EXPECT_EQ(store.saved_from[0], 1);
  EXPECT_EQ(log.LastIndex(), 2);
  EXPECT_EQ(log.CommitIndex(), 2);
  ASSERT_EQ(reply.value().newly_committed.size(), 2);

  reply = log.HandleAppend(request);
  ASSERT_TRUE(reply.ok());
  EXPECT_TRUE(reply.value().newly_committed.empty());
  EXPECT_EQ(store.saved_from.size(), 1);
}

TEST(RaftLog, ReplacesOnlyConflictingUncommittedSuffix) {
  MemoryLogStore store;
  RaftLog log({{1, 1, "a"}, {2, 2, "old"}, {3, 2, "stale"}}, 1, store);
  auto reply = log.HandleAppend({1, 1, 2, {{2, 3, "new"}, {3, 3, "newer"}}});
  ASSERT_TRUE(reply.ok());
  EXPECT_TRUE(reply.value().success);
  ASSERT_EQ(store.saved_from.size(), 1);
  EXPECT_EQ(store.saved_from[0], 2);
  EXPECT_EQ(log.entries()[0].command, "a");
  EXPECT_EQ(log.entries()[1].command, "new");
  EXPECT_EQ(log.entries()[2].term, 3);
  EXPECT_EQ(log.CommitIndex(), 2);
  ASSERT_EQ(reply.value().newly_committed.size(), 1);
  EXPECT_EQ(reply.value().newly_committed[0].command, "new");
}

TEST(RaftLog, ReportsConflictHintsWithoutChangingLog) {
  MemoryLogStore store;
  RaftLog log({{1, 1, "a"}, {2, 2, "b"}, {3, 2, "c"}}, 1, store);
  auto reply = log.HandleAppend({5, 3, 0, {}});
  ASSERT_TRUE(reply.ok());
  EXPECT_FALSE(reply.value().success);
  EXPECT_EQ(reply.value().conflict_index, 4);
  EXPECT_EQ(reply.value().conflict_term, 0);

  reply = log.HandleAppend({3, 3, 0, {}});
  ASSERT_TRUE(reply.ok());
  EXPECT_FALSE(reply.value().success);
  EXPECT_EQ(reply.value().conflict_index, 2);
  EXPECT_EQ(reply.value().conflict_term, 2);
  EXPECT_TRUE(store.saved_from.empty());
}

TEST(RaftLog, RejectsCommittedOverwriteAndSameTermDifferentCommand) {
  MemoryLogStore store;
  RaftLog log({{1, 1, "a"}, {2, 2, "b"}}, 1, store);
  EXPECT_EQ(log.HandleAppend({0, 0, 0, {{1, 3, "overwrite"}}}).status().code(),
            yewukv::StatusCode::kCorruption);
  EXPECT_EQ(log.HandleAppend({0, 0, 0, {{1, 1, "different"}}}).status().code(),
            yewukv::StatusCode::kCorruption);
  EXPECT_EQ(log.entries()[0].command, "a");
  EXPECT_TRUE(store.saved_from.empty());
}

TEST(RaftLog, ShortHeartbeatCannotCommitUnmentionedFollowerSuffix) {
  MemoryLogStore store;
  RaftLog log({{1, 1, "a"}, {2, 2, "unconfirmed"}}, 0, store);
  auto reply = log.HandleAppend({0, 0, 2, {}});
  ASSERT_TRUE(reply.ok());
  EXPECT_TRUE(reply.value().success);
  EXPECT_EQ(log.CommitIndex(), 0);

  reply = log.HandleAppend({1, 1, 2, {}});
  ASSERT_TRUE(reply.ok());
  EXPECT_EQ(log.CommitIndex(), 1);
  ASSERT_EQ(reply.value().newly_committed.size(), 1);
  EXPECT_EQ(reply.value().newly_committed[0].command, "a");
}

TEST(RaftLog, LeaderOnlyAdvancesMajorityCommitThroughCurrentTermEntry) {
  MemoryLogStore store;
  RaftLog log({{1, 1, "a"}, {2, 1, "b"}, {3, 2, "c"}}, 0, store);
  auto committed = log.AdvanceLeaderCommit(2, {3, 2, 1});
  ASSERT_TRUE(committed.ok());
  EXPECT_TRUE(committed.value().empty());
  EXPECT_EQ(log.CommitIndex(), 0);

  committed = log.AdvanceLeaderCommit(2, {3, 3, 1});
  ASSERT_TRUE(committed.ok());
  ASSERT_EQ(committed.value().size(), 3);
  EXPECT_EQ(log.CommitIndex(), 3);
  EXPECT_EQ(committed.value()[2].command, "c");
}

TEST(RaftLog, PersistenceFailurePoisonsLogWithoutAcknowledgingAppend) {
  MemoryLogStore store;
  store.fail = true;
  RaftLog log({{1, 1, "a"}}, 0, store);
  auto reply = log.HandleAppend({1, 1, 0, {{2, 2, "b"}}});
  EXPECT_FALSE(reply.ok());
  EXPECT_TRUE(log.failed());
  EXPECT_EQ(log.LastIndex(), 1);
  EXPECT_FALSE(log.HandleAppend({1, 1, 0, {}}).ok());
}

TEST(RaftLog, RejectsMalformedRecoveredAndIncomingTermOrder) {
  MemoryLogStore store;
  EXPECT_THROW(RaftLog({{1, 3, "a"}, {2, 2, "b"}}, 0, store), std::invalid_argument);
  RaftLog log({{1, 3, "a"}}, 0, store);
  EXPECT_EQ(log.HandleAppend({1, 3, 0, {{2, 2, "older"}}}).status().code(),
            yewukv::StatusCode::kInvalidArgument);
  EXPECT_EQ(log.HandleAppend({1, 3, 0, {{3, 3, "gap"}}}).status().code(),
            yewukv::StatusCode::kInvalidArgument);
  EXPECT_TRUE(store.saved_from.empty());
}

}  // namespace
