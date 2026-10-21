#include <fcntl.h>
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <stdexcept>
#include <string>
#include <system_error>
#include <utility>

#include <gtest/gtest.h>

#include "raft/file_store.h"

namespace {

using yewukv::StatusCode;
using yewukv::raft::Election;
using yewukv::raft::FileStore;
using yewukv::raft::OutboundVote;
using yewukv::raft::RaftLog;
using Point = FileStore::PersistPoint;

constexpr std::array<Point, 4> kPersistPoints{Point::kBeforeFileSync, Point::kBeforeRename,
                                              Point::kBeforeDirectorySync,
                                              Point::kAfterDirectorySync};

class TemporaryDirectory {
 public:
  TemporaryDirectory() {
    std::string pattern = (std::filesystem::temp_directory_path() / "yewukv-raft-XXXXXX").string();
    char* created = ::mkdtemp(pattern.data());
    if (!created) throw std::system_error(errno, std::generic_category(), "mkdtemp");
    path_ = created;
  }
  ~TemporaryDirectory() { std::filesystem::remove_all(path_); }
  const std::string& path() const { return path_; }
  std::string file(const char* name = "raft.state") const { return path_ + "/" + name; }

 private:
  std::string path_;
};

std::string ReadFile(const std::string& path) {
  std::ifstream file(path, std::ios::binary);
  if (!file) throw std::runtime_error("cannot read test state");
  return {std::istreambuf_iterator<char>(file), std::istreambuf_iterator<char>()};
}

void WriteFile(const std::string& path, const std::string& bytes) {
  std::ofstream file(path, std::ios::binary | std::ios::trunc);
  file.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
  file.close();
  ASSERT_TRUE(file.good());
}

// Mutate complete images while retaining a valid CRC, so decoder bounds and
// Raft invariants are checked independently of the checksum guard.
void Put64(std::string* bytes, size_t offset, uint64_t value) {
  for (int i = 7; i >= 0; --i) (*bytes)[offset + 7 - i] = static_cast<char>(value >> (i * 8));
}

void Rechecksum(std::string* bytes) {
  uint32_t crc = 0xffffffffU;
  for (size_t i = 0; i < bytes->size() - 4; ++i) {
    crc ^= static_cast<unsigned char>((*bytes)[i]);
    for (int bit = 0; bit < 8; ++bit) crc = (crc >> 1) ^ (0xedb88320U & (0U - (crc & 1U)));
  }
  crc = ~crc;
  for (int i = 3; i >= 0; --i)
    (*bytes)[bytes->size() - 4 + 3 - i] = static_cast<char>(crc >> (i * 8));
}

TEST(RaftFileStore, InitializesAndRecoversHardStateAndBinaryCommands) {
  TemporaryDirectory directory;
  const std::string binary("put\0key\xffvalue", 13);
  {
    FileStore store(directory.path());
    EXPECT_EQ(store.ReadState().hard_state.term, 0);
    EXPECT_TRUE(store.ReadState().entries.empty());
    ASSERT_TRUE(store.Save({4, 2}).ok());
    ASSERT_TRUE(store.ReplaceFrom(1, {{1, 1, binary}, {2, 4, ""}}).ok());
  }
  FileStore recovered(directory.path());
  const auto state = recovered.ReadState();
  EXPECT_EQ(state.hard_state.term, 4);
  EXPECT_EQ(state.hard_state.voted_for, 2);
  ASSERT_EQ(state.entries.size(), 2);
  EXPECT_EQ(state.entries[0].command, binary);
  EXPECT_EQ(state.entries[1].index, 2);
  EXPECT_EQ(state.entries[1].term, 4);
  EXPECT_TRUE(state.entries[1].command.empty());
  EXPECT_FALSE(recovered.failed());
}

TEST(RaftFileStore, ElectionCannotVoteTwiceAfterRealReopen) {
  TemporaryDirectory directory;
  {
    FileStore store(directory.path());
    Election election(1, {1, 2, 3}, store.ReadState().hard_state, store);
    auto vote = election.HandleVoteRequest({7, 2, 0, 0}, 0, 0);
    ASSERT_TRUE(vote.ok());
    ASSERT_TRUE(vote.value().granted);
  }
  FileStore store(directory.path());
  Election election(1, {1, 2, 3}, store.ReadState().hard_state, store);
  auto vote = election.HandleVoteRequest({7, 3, 0, 0}, 0, 0);
  ASSERT_TRUE(vote.ok());
  EXPECT_FALSE(vote.value().granted);
  EXPECT_EQ(vote.value().term, 7);
  vote = election.HandleVoteRequest({8, 3, 0, 0}, 0, 0);
  ASSERT_TRUE(vote.ok());
  EXPECT_TRUE(vote.value().granted);
}

TEST(RaftFileStore, LogCoreReplacesConflictingSuffixAcrossRestart) {
  TemporaryDirectory directory;
  {
    FileStore store(directory.path());
    ASSERT_TRUE(store.Save({2, 0}).ok());
    RaftLog log(store.ReadState().entries, 0, store);
    auto reply = log.HandleAppend({0, 0, 1, {{1, 1, "kept"}, {2, 2, "old"}, {3, 2, "tail"}}});
    ASSERT_TRUE(reply.ok());
    ASSERT_TRUE(reply.value().success);
    ASSERT_TRUE(store.Save({3, 0}).ok());
    reply = log.HandleAppend({1, 1, 2, {{2, 3, "replacement"}}});
    ASSERT_TRUE(reply.ok());
    ASSERT_TRUE(reply.value().success);
  }
  FileStore store(directory.path());
  RaftLog log(store.ReadState().entries, 0, store);
  EXPECT_EQ(log.LastIndex(), 2);
  EXPECT_EQ(log.LastTerm(), 3);
  EXPECT_EQ(log.CommitIndex(), 0);  // Durable does not mean committed.
  EXPECT_EQ(log.entries()[0].command, "kept");
  EXPECT_EQ(log.entries()[1].command, "replacement");
}

TEST(RaftFileStore, SupportsDurableSuffixTruncationAndAppend) {
  TemporaryDirectory directory;
  {
    FileStore store(directory.path());
    ASSERT_TRUE(store.Save({3, 0}).ok());
    ASSERT_TRUE(store.ReplaceFrom(1, {{1, 1, "a"}, {2, 2, "b"}, {3, 2, "c"}}).ok());
    ASSERT_TRUE(store.ReplaceFrom(2, {}).ok());
  }
  {
    FileStore store(directory.path());
    ASSERT_EQ(store.ReadState().entries.size(), 1);
    ASSERT_TRUE(store.ReplaceFrom(2, {{2, 3, "d"}}).ok());
  }
  FileStore store(directory.path());
  ASSERT_EQ(store.ReadState().entries.size(), 2);
  EXPECT_EQ(store.ReadState().entries[1].command, "d");
}

TEST(RaftFileStore, RejectsTermRegressionAndVoteChangesWithoutTouchingImage) {
  TemporaryDirectory directory;
  FileStore store(directory.path());
  ASSERT_TRUE(store.Save({5, 2}).ok());
  const auto original = ReadFile(directory.file());
  EXPECT_EQ(store.Save({4, 0}).code(), StatusCode::kInvalidArgument);
  EXPECT_EQ(store.Save({5, 3}).code(), StatusCode::kInvalidArgument);
  EXPECT_EQ(store.Save({5, 0}).code(), StatusCode::kInvalidArgument);
  EXPECT_EQ(store.Save({0, 2}).code(), StatusCode::kInvalidArgument);
  EXPECT_EQ(ReadFile(directory.file()), original);
  EXPECT_FALSE(store.failed());
  ASSERT_TRUE(store.Save({5, 2}).ok());
  ASSERT_TRUE(store.Save({6, 0}).ok());
  ASSERT_TRUE(store.Save({6, 3}).ok());
}

TEST(RaftFileStore, RejectsInvalidSuffixWithoutChangingPersistedState) {
  TemporaryDirectory directory;
  FileStore store(directory.path());
  ASSERT_TRUE(store.Save({4, 0}).ok());
  ASSERT_TRUE(store.ReplaceFrom(1, {{1, 2, "kept"}}).ok());
  const auto original = ReadFile(directory.file());
  EXPECT_EQ(store.ReplaceFrom(0, {}).code(), StatusCode::kInvalidArgument);
  EXPECT_EQ(store.ReplaceFrom(3, {}).code(), StatusCode::kInvalidArgument);
  EXPECT_EQ(store.ReplaceFrom(2, {{3, 3, "gap"}}).code(), StatusCode::kInvalidArgument);
  EXPECT_EQ(store.ReplaceFrom(2, {{2, 1, "older"}}).code(), StatusCode::kInvalidArgument);
  EXPECT_EQ(store.ReplaceFrom(2, {{2, 5, "future"}}).code(), StatusCode::kInvalidArgument);
  EXPECT_EQ(store.ReplaceFrom(2, {{2, 0, "zero"}}).code(), StatusCode::kInvalidArgument);
  const std::string oversized(FileStore::kMaxCommandBytes + 1, 'x');
  EXPECT_EQ(store.ReplaceFrom(2, {{2, 3, oversized}}).code(), StatusCode::kInvalidArgument);
  EXPECT_EQ(ReadFile(directory.file()), original);
  EXPECT_FALSE(store.failed());
}

TEST(RaftFileStore, EnforcesCapacityBeforeIOAndCanTruncateToMakeRoom) {
  TemporaryDirectory directory;
  FileStore::Options options;
  options.max_state_bytes = 64;
  FileStore store(directory.path(), options);
  ASSERT_TRUE(store.Save({1, 0}).ok());
  ASSERT_TRUE(store.ReplaceFrom(1, {{1, 1, "12345678"}}).ok());
  const auto original = ReadFile(directory.file());
  EXPECT_EQ(original.size(), 64);
  EXPECT_EQ(store.ReplaceFrom(2, {{2, 1, ""}}).code(), StatusCode::kBusy);
  EXPECT_EQ(ReadFile(directory.file()), original);
  EXPECT_FALSE(store.failed());
  ASSERT_TRUE(store.ReplaceFrom(1, {}).ok());
  ASSERT_TRUE(store.ReplaceFrom(1, {{1, 1, "smaller"}}).ok());
}

TEST(RaftFileStore, LocksDirectoryAndReleasesLockAfterCloseOrConstructorFailure) {
  TemporaryDirectory directory;
  {
    FileStore store(directory.path());
    EXPECT_THROW(FileStore other(directory.path()), std::system_error);
    ASSERT_TRUE(store.Save({1, 1}).ok());
  }
  const auto valid = ReadFile(directory.file());
  WriteFile(directory.file(), "broken");
  EXPECT_THROW(FileStore invalid(directory.path()), std::runtime_error);
  WriteFile(directory.file(), valid);
  FileStore recovered(directory.path());
  EXPECT_EQ(recovered.ReadState().hard_state.voted_for, 1);
}

TEST(RaftFileStore, IgnoresStaleTemporaryImageAndReplacesItOnNextWrite) {
  TemporaryDirectory directory;
  {
    FileStore store(directory.path());
    ASSERT_TRUE(store.Save({2, 1}).ok());
  }
  WriteFile(directory.file("raft.state.tmp"), "incomplete replacement");
  FileStore store(directory.path());
  EXPECT_EQ(store.ReadState().hard_state.term, 2);
  ASSERT_TRUE(store.Save({3, 2}).ok());
  EXPECT_FALSE(std::filesystem::exists(directory.file("raft.state.tmp")));
}

TEST(RaftFileStore, RejectsChecksumDamageAndEveryTruncatedImageWithoutRepair) {
  TemporaryDirectory directory;
  {
    FileStore store(directory.path());
    ASSERT_TRUE(store.Save({3, 2}).ok());
    ASSERT_TRUE(store.ReplaceFrom(1, {{1, 2, "command"}}).ok());
  }
  const auto original = ReadFile(directory.file());
  auto damaged = original;
  damaged[16] ^= 1;
  WriteFile(directory.file(), damaged);
  EXPECT_THROW(FileStore store(directory.path()), std::runtime_error);
  EXPECT_EQ(ReadFile(directory.file()), damaged);
  for (size_t size = 0; size < original.size(); ++size) {
    SCOPED_TRACE(size);
    const auto truncated = original.substr(0, size);
    WriteFile(directory.file(), truncated);
    EXPECT_THROW(FileStore store(directory.path()), std::runtime_error);
    EXPECT_EQ(ReadFile(directory.file()), truncated);
  }
}

TEST(RaftFileStore, RejectsStructurallyInvalidImagesEvenWithValidChecksums) {
  TemporaryDirectory directory;
  {
    FileStore store(directory.path());
    ASSERT_TRUE(store.Save({3, 2}).ok());
    ASSERT_TRUE(store.ReplaceFrom(1, {{1, 2, "a"}, {2, 3, "b"}}).ok());
  }
  const auto original = ReadFile(directory.file());
  // Count, first index, zero term, future term, decreasing term, term-zero vote.
  for (const auto& [offset, value] : std::array<std::pair<size_t, uint64_t>, 6>{
           {{24, UINT64_MAX}, {32, 2}, {40, 0}, {40, 4}, {61, 1}, {8, 0}}}) {
    SCOPED_TRACE(offset);
    auto damaged = original;
    Put64(&damaged, offset, value);
    Rechecksum(&damaged);
    WriteFile(directory.file(), damaged);
    EXPECT_THROW(FileStore store(directory.path()), std::runtime_error);
  }
  auto damaged = original;
  damaged[48] = '\x7f';  // Forged command length.
  Rechecksum(&damaged);
  WriteFile(directory.file(), damaged);
  EXPECT_THROW(FileStore store(directory.path()), std::runtime_error);
  damaged = original;
  damaged[0] = 'X';
  Rechecksum(&damaged);
  WriteFile(directory.file(), damaged);
  EXPECT_THROW(FileStore store(directory.path()), std::runtime_error);
  damaged = original;
  damaged[7] = 2;  // Unsupported format version.
  Rechecksum(&damaged);
  WriteFile(directory.file(), damaged);
  EXPECT_THROW(FileStore store(directory.path()), std::runtime_error);
  damaged = original;
  damaged.insert(damaged.size() - 4, "extra");
  Rechecksum(&damaged);
  WriteFile(directory.file(), damaged);
  EXPECT_THROW(FileStore store(directory.path()), std::runtime_error);
}

TEST(RaftFileStore, RejectsSymlinkAndNonRegularPublishedState) {
  TemporaryDirectory directory;
  const auto target = directory.file("target");
  WriteFile(target, "do not modify");
  std::filesystem::create_symlink(target, directory.file());
  EXPECT_THROW(FileStore store(directory.path()), std::system_error);
  EXPECT_EQ(ReadFile(target), "do not modify");
  std::filesystem::remove(directory.file());
  std::filesystem::create_directory(directory.file());
  EXPECT_THROW(FileStore store(directory.path()), std::runtime_error);
}

TEST(RaftFileStore, RejectsInvalidConfigurationAndOversizedPublishedImage) {
  TemporaryDirectory directory;
  FileStore::Options options;
  options.max_state_bytes = 35;
  EXPECT_THROW(FileStore store(directory.path(), options), std::invalid_argument);
  options.max_state_bytes = FileStore::kMaxStateBytes + 1;
  EXPECT_THROW(FileStore store(directory.path(), options), std::invalid_argument);
  EXPECT_THROW(FileStore store(""), std::invalid_argument);
  {
    FileStore store(directory.path());
  }
  ASSERT_EQ(::truncate(directory.file().c_str(), FileStore::kMaxStateBytes + 1), 0);
  EXPECT_THROW(FileStore store(directory.path()), std::runtime_error);
}

TEST(RaftFileStore, FilesystemFailurePoisonsElectionWithoutPublishingVotes) {
  TemporaryDirectory directory;
  FileStore store(directory.path());
  Election node(1, {1, 2, 3}, {}, store);
  std::filesystem::create_directory(directory.file("raft.state.tmp"));
  std::vector<OutboundVote> outbound;
  EXPECT_FALSE(node.Campaign(0, 0, &outbound).ok());
  EXPECT_TRUE(outbound.empty());
  EXPECT_TRUE(node.failed());
  EXPECT_TRUE(store.failed());
  EXPECT_FALSE(store.Save({2, 2}).ok());
  EXPECT_FALSE(store.ReplaceFrom(1, {}).ok());
  EXPECT_THROW(store.ReadState(), std::runtime_error);
}

TEST(RaftFileStore, AppendFailurePoisonsLogBeforeAnySuccessOrCommit) {
  TemporaryDirectory directory;
  bool armed = false;
  FileStore::Options options;
  options.persist_observer = [&](Point point) {
    if (armed && point == Point::kBeforeRename)
      throw std::system_error(EIO, std::generic_category());
  };
  {
    FileStore store(directory.path(), options);
    ASSERT_TRUE(store.Save({2, 0}).ok());
    RaftLog log(store.ReadState().entries, 0, store);
    armed = true;
    const auto reply = log.HandleAppend({0, 0, 1, {{1, 2, "must-not-commit"}}});
    EXPECT_FALSE(reply.ok());
    EXPECT_TRUE(log.failed());
    EXPECT_TRUE(store.failed());
    EXPECT_EQ(log.LastIndex(), 0);
    EXPECT_EQ(log.CommitIndex(), 0);
    EXPECT_FALSE(log.HandleAppend({0, 0, 0, {}}).ok());
  }
  FileStore store(directory.path());
  EXPECT_EQ(store.ReadState().hard_state.term, 2);
  EXPECT_TRUE(store.ReadState().entries.empty());
}

TEST(RaftFileStore, InjectedPersistenceFailuresRequireReopenAtEveryBoundary) {
  for (Point point : kPersistPoints) {
    SCOPED_TRACE(static_cast<int>(point));
    TemporaryDirectory directory;
    bool armed = false;
    FileStore::Options options;
    options.persist_observer = [&](Point reached) {
      if (armed && reached == point) throw std::system_error(EIO, std::generic_category());
    };
    {
      FileStore store(directory.path(), options);
      ASSERT_TRUE(store.Save({1, 1}).ok());
      armed = true;
      EXPECT_EQ(store.Save({2, 2}).code(), StatusCode::kIOError);
      EXPECT_TRUE(store.failed());
      EXPECT_FALSE(store.Save({3, 3}).ok());
    }
    FileStore recovered(directory.path());
    const bool renamed =
        point == Point::kBeforeDirectorySync || point == Point::kAfterDirectorySync;
    EXPECT_EQ(recovered.ReadState().hard_state.term, renamed ? 2 : 1);
    EXPECT_EQ(recovered.ReadState().hard_state.voted_for, renamed ? 2 : 1);
    ASSERT_TRUE(recovered.Save({3, 3}).ok());
  }
}

TEST(RaftFileStore, SigkillDuringSuffixReplacementRecoversAWholeOldOrNewImage) {
  for (Point point : kPersistPoints) {
    SCOPED_TRACE(static_cast<int>(point));
    TemporaryDirectory directory;
    {
      FileStore store(directory.path());
      ASSERT_TRUE(store.Save({4, 2}).ok());
      ASSERT_TRUE(store.ReplaceFrom(1, {{1, 1, "prefix"}, {2, 2, "old"}, {3, 2, "tail"}}).ok());
    }
    const pid_t child = ::fork();
    ASSERT_GE(child, 0);
    if (child == 0) {
      try {
        FileStore::Options options;
        options.persist_observer = [point](Point reached) {
          if (reached == point) ::raise(SIGKILL);
        };
        FileStore store(directory.path(), options);
        store.ReplaceFrom(2, {{2, 4, "new"}});
      } catch (...) {
        ::_exit(3);
      }
      ::_exit(4);
    }
    int status = 0;
    ASSERT_EQ(::waitpid(child, &status, 0), child);
    ASSERT_TRUE(WIFSIGNALED(status));
    EXPECT_EQ(WTERMSIG(status), SIGKILL);
    FileStore recovered(directory.path());
    const auto state = recovered.ReadState();
    EXPECT_EQ(state.hard_state.term, 4);
    EXPECT_EQ(state.hard_state.voted_for, 2);
    const bool renamed =
        point == Point::kBeforeDirectorySync || point == Point::kAfterDirectorySync;
    ASSERT_EQ(state.entries.size(), renamed ? 2 : 3);
    EXPECT_EQ(state.entries[0].command, "prefix");
    EXPECT_EQ(state.entries[1].command, renamed ? "new" : "old");
    if (!renamed) {
      EXPECT_EQ(state.entries[2].command, "tail");
    }
    ASSERT_TRUE(recovered.ReplaceFrom(2, {{2, 4, "continued"}}).ok());
  }
}

TEST(RaftFileStore, AcknowledgedVotesAndAppendsSurviveSigkill) {
  TemporaryDirectory directory;
  int ready[2];
  ASSERT_EQ(::pipe(ready), 0);
  const pid_t child = ::fork();
  ASSERT_GE(child, 0);
  if (child == 0) {
    ::close(ready[0]);
    try {
      FileStore store(directory.path());
      Election election(1, {1, 2, 3}, store.ReadState().hard_state, store);
      auto vote = election.HandleVoteRequest({8, 2, 0, 0}, 0, 0);
      if (!vote.ok() || !vote.value().granted) ::_exit(2);
      RaftLog log(store.ReadState().entries, 0, store);
      const auto reply = log.HandleAppend({0, 0, 0, {{1, 8, "acknowledged"}}});
      if (!reply.ok() || !reply.value().success) ::_exit(3);
      if (::write(ready[1], "Y", 1) != 1) ::_exit(4);
      for (;;) ::pause();
    } catch (...) {
      ::_exit(5);
    }
  }
  ::close(ready[1]);
  char acknowledged = 0;
  const auto count = ::read(ready[0], &acknowledged, 1);
  ::close(ready[0]);
  // Always reap the child, even if it exited before acknowledging.
  EXPECT_EQ(::kill(child, SIGKILL), 0);
  int status = 0;
  ASSERT_EQ(::waitpid(child, &status, 0), child);
  ASSERT_EQ(count, 1);
  ASSERT_EQ(acknowledged, 'Y');
  FileStore recovered(directory.path());
  const auto state = recovered.ReadState();
  ASSERT_EQ(state.entries.size(), 1);
  EXPECT_EQ(state.entries[0].command, "acknowledged");
  EXPECT_EQ(state.hard_state.term, 8);
  EXPECT_EQ(state.hard_state.voted_for, 2);
  Election election(1, {1, 2, 3}, state.hard_state, recovered);
  const auto vote = election.HandleVoteRequest({8, 3, 1, 8}, 1, 8);
  ASSERT_TRUE(vote.ok());
  EXPECT_FALSE(vote.value().granted);
}

}  // namespace
