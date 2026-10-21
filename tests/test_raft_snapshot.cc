#include <gtest/gtest.h>

#include <sys/wait.h>
#include <unistd.h>

#include <array>
#include <csignal>
#include <filesystem>
#include <stdexcept>
#include <system_error>

#include "raft/node.h"

namespace {
using namespace yewukv::raft;

class SnapshotDirectory {
 public:
  SnapshotDirectory() {
    char path[] = "/tmp/yewukv-snapshot-XXXXXX";
    const auto result = ::mkdtemp(path);
    if (!result) throw std::runtime_error("mkdtemp");
    path_ = result;
  }
  ~SnapshotDirectory() { std::filesystem::remove_all(path_); }
  const std::string& path() const { return path_; }

 private:
  std::string path_;
};

TEST(RaftSnapshot, CompactsCommittedPrefixAndRecoversSnapshotWithUncommittedSuffix) {
  SnapshotDirectory directory;
  {
    FileStore store(directory.path());
    ASSERT_TRUE(store.Save({5, 1}).ok());
    ASSERT_TRUE(
        store.ReplaceFrom(1, {{1, 1, "a"}, {2, 2, "b"}, {3, 3, "c"}, {4, 4, "uncommitted"}}).ok());
    RaftLog log(store.ReadState().entries, 3, store);
    EXPECT_FALSE(log.Compact(4, "unsafe").ok());
    ASSERT_TRUE(log.Compact(3, std::string("kv\0state", 8)).ok());
    EXPECT_EQ(log.snapshot().term, 3);
    EXPECT_EQ(log.LastIndex(), 4);
    ASSERT_EQ(log.entries().size(), 1);
    EXPECT_EQ(log.entries().front().index, 4);
    EXPECT_EQ(log.TermAt(3), 3);
    EXPECT_THROW(log.TermAt(2), std::out_of_range);
    EXPECT_FALSE(log.Compact(3, "duplicate").ok());
  }
  FileStore store(directory.path());
  const auto state = store.ReadState();
  ASSERT_EQ(state.snapshot.index, 3);
  EXPECT_EQ(state.snapshot.data, std::string("kv\0state", 8));
  RaftLog log(state.entries, state.snapshot.index, store, state.snapshot);
  EXPECT_EQ(log.CommitIndex(), 3);
  auto old = log.HandleAppend({2, 2, 4, {}});
  ASSERT_TRUE(old.ok());
  EXPECT_FALSE(old.value().success);
  EXPECT_EQ(old.value().conflict_index, 4);
  auto replacement = log.HandleAppend({3, 3, 4, {{4, 5, "replacement"}}});
  ASSERT_TRUE(replacement.ok());
  ASSERT_TRUE(replacement.value().success);
  ASSERT_EQ(replacement.value().newly_committed.size(), 1);
  EXPECT_EQ(replacement.value().newly_committed.front().command, "replacement");
}

TEST(RaftSnapshot, InstallationRetainsOnlySuffixWithMatchingBoundaryTerm) {
  SnapshotDirectory directory;
  FileStore store(directory.path());
  ASSERT_TRUE(store.Save({5, 0}).ok());
  ASSERT_TRUE(store.ReplaceFrom(1, {{1, 1, "a"}, {2, 2, "b"}, {3, 3, "c"}}).ok());
  RaftLog log(store.ReadState().entries, 1, store);
  ASSERT_TRUE(log.InstallSnapshot({2, 2, "state-b"}).ok());
  ASSERT_EQ(log.entries().size(), 1);
  EXPECT_EQ(log.entries().front().command, "c");
  ASSERT_TRUE(log.InstallSnapshot({3, 4, "different-history"}).ok());
  EXPECT_TRUE(log.entries().empty());
  EXPECT_EQ(log.CommitIndex(), 3);
  EXPECT_EQ(log.LastTerm(), 4);
  ASSERT_TRUE(log.InstallSnapshot({2, 2, "stale"}).ok());
  EXPECT_EQ(log.snapshot().data, "different-history");
}

TEST(RaftSnapshot, SnapshotCapacityRejectionDoesNotReplaceDurableImage) {
  SnapshotDirectory directory;
  FileStore::Options options;
  options.max_state_bytes = 128;
  FileStore store(directory.path(), options);
  ASSERT_TRUE(store.Save({1, 1}).ok());
  ASSERT_TRUE(store.ReplaceFrom(1, {{1, 1, "a"}}).ok());
  EXPECT_FALSE(store.SaveSnapshot({1, 1, std::string(128, 'x')}, {}).ok());
  EXPECT_EQ(store.ReadState().snapshot.index, 0);
  EXPECT_EQ(store.ReadState().entries.front().command, "a");
  ASSERT_TRUE(store.SaveSnapshot({1, 1, "small"}, {}).ok());
  ASSERT_TRUE(store.ReplaceFrom(2, {{2, 1, "after"}}).ok());
  EXPECT_FALSE(store.ReplaceFrom(1, {}).ok());
}

TEST(RaftSnapshot, SigkillAtEveryPublishBoundaryRecoversWholeSnapshotAndSuffix) {
  using Point = FileStore::PersistPoint;
  for (const auto point : {Point::kBeforeFileSync, Point::kBeforeRename,
                           Point::kBeforeDirectorySync, Point::kAfterDirectorySync}) {
    SCOPED_TRACE(static_cast<int>(point));
    SnapshotDirectory directory;
    {
      FileStore store(directory.path());
      ASSERT_TRUE(store.Save({3, 1}).ok());
      ASSERT_TRUE(store.ReplaceFrom(1, {{1, 1, "a"}, {2, 2, "b"}, {3, 3, "tail"}}).ok());
    }
    const pid_t child = ::fork();
    ASSERT_GE(child, 0);
    if (child == 0) {
      FileStore::Options options;
      options.persist_observer = [point](Point reached) {
        if (point == reached) ::raise(SIGKILL);
      };
      try {
        FileStore store(directory.path(), options);
        store.SaveSnapshot({2, 2, "a+b"}, {{3, 3, "tail"}});
      } catch (...) {
        ::_exit(3);
      }
      ::_exit(4);
    }
    int result = 0;
    ASSERT_EQ(::waitpid(child, &result, 0), child);
    ASSERT_TRUE(WIFSIGNALED(result));
    ASSERT_EQ(WTERMSIG(result), SIGKILL);
    FileStore recovered(directory.path());
    const auto state = recovered.ReadState();
    const bool published =
        point == Point::kBeforeDirectorySync || point == Point::kAfterDirectorySync;
    EXPECT_EQ(state.snapshot.index, published ? 2 : 0);
    EXPECT_EQ(state.snapshot.data, published ? "a+b" : "");
    ASSERT_EQ(state.entries.size(), published ? 1 : 3);
    EXPECT_EQ(state.entries.back().command, "tail");
    ASSERT_TRUE(recovered.ReplaceFrom(4, {{4, 3, "continued"}}).ok());
  }
}

Message Chunk(uint64_t sequence, uint64_t offset, const std::string& data) {
  Message message;
  message.kind = MessageKind::kSnapshot;
  message.from = 1;
  message.to = 2;
  message.term = 3;
  message.sequence = sequence;
  const auto payload = data.substr(offset, Node::kSnapshotChunkBytes);
  message.snapshot = {10, 2, offset, data.size(), payload, offset + payload.size() == data.size()};
  return message;
}

Node::Config FollowerConfig() {
  Node::Config config;
  config.id = 2;
  config.voters = {1, 2, 3};
  return config;
}

TEST(RaftSnapshot, ChunkRetriesAndReceiverRestartNeverPublishPartialState) {
  SnapshotDirectory directory;
  const std::string data(Node::kSnapshotChunkBytes * 2 + 31, 's');
  {
    FileStore store(directory.path());
    Node follower(FollowerConfig(), store.ReadState(), store, store);
    auto reply = follower.Receive(Chunk(1, 0, data));
    ASSERT_TRUE(reply && reply->success);
    EXPECT_EQ(reply->match_index, 0);
    EXPECT_EQ(store.ReadState().snapshot.index, 0);
    reply = follower.Receive(Chunk(2, 0, data));
    ASSERT_TRUE(reply && reply->success);
    EXPECT_EQ(reply->snapshot.offset, Node::kSnapshotChunkBytes);
  }
  FileStore store(directory.path());
  Node follower(FollowerConfig(), store.ReadState(), store, store);
  auto reply = follower.Receive(Chunk(3, Node::kSnapshotChunkBytes, data));
  ASSERT_TRUE(reply);
  EXPECT_FALSE(reply->success);
  for (size_t offset = 0; offset < data.size(); offset += Node::kSnapshotChunkBytes) {
    reply = follower.Receive(Chunk(4 + offset, offset, data));
    ASSERT_TRUE(reply && reply->success);
  }
  EXPECT_EQ(reply->match_index, 10);
  EXPECT_EQ(store.ReadState().snapshot.data, data);
  auto installed = follower.TakeInstalledSnapshot();
  ASSERT_TRUE(installed);
  EXPECT_EQ(installed->index, 10);
  EXPECT_FALSE(follower.TakeInstalledSnapshot());
  reply = follower.Receive(Chunk(999, Node::kSnapshotChunkBytes * 2, data));
  ASSERT_TRUE(reply && reply->success);
  EXPECT_FALSE(follower.TakeInstalledSnapshot());
}

TEST(RaftSnapshot, RejectsMalformedOrInvalidApplicationSnapshotsBeforePublication) {
  SnapshotDirectory directory;
  FileStore store(directory.path());
  auto config = FollowerConfig();
  config.validate_snapshot = [](const std::string& bytes) { return bytes == "valid"; };
  Node follower(config, store.ReadState(), store, store);
  auto invalid = Chunk(1, 0, "invalid");
  auto reply = follower.Receive(invalid);
  ASSERT_TRUE(reply);
  EXPECT_FALSE(reply->success);
  EXPECT_EQ(store.ReadState().snapshot.index, 0);
  invalid.snapshot.total = FileStore::kMaxSnapshotBytes + 1;
  EXPECT_FALSE(follower.Receive(invalid));
  auto valid = Chunk(2, 0, "valid");
  reply = follower.Receive(valid);
  ASSERT_TRUE(reply && reply->success);
  EXPECT_EQ(store.ReadState().snapshot.data, "valid");
}

TEST(RaftSnapshot, DiskFailurePreventsSnapshotInstallationAcknowledgement) {
  SnapshotDirectory directory;
  bool armed = false;
  FileStore::Options options;
  options.persist_observer = [&](FileStore::PersistPoint point) {
    if (armed && point == FileStore::PersistPoint::kBeforeFileSync)
      throw std::system_error(ENOSPC, std::generic_category());
  };
  FileStore store(directory.path(), options);
  ASSERT_TRUE(store.Save({3, 0}).ok());
  Node follower(FollowerConfig(), store.ReadState(), store, store);
  armed = true;
  EXPECT_FALSE(follower.Receive(Chunk(1, 0, "state")));
  EXPECT_TRUE(follower.failed());
  EXPECT_FALSE(follower.TakeInstalledSnapshot());
}
}  // namespace
