#include <fcntl.h>
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

#include <filesystem>
#include <fstream>
#include <iterator>
#include <numeric>
#include <string>
#include <system_error>

#include <gtest/gtest.h>

#include "raft/file_store.h"

namespace {
using yewukv::raft::FileStore;
using Point = FileStore::PersistPoint;

class JournalDirectory {
 public:
  JournalDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "yewukv-journal-XXXXXX").string();
    char* made = ::mkdtemp(pattern.data());
    if (!made) throw std::system_error(errno, std::generic_category());
    path = made;
  }
  ~JournalDirectory() { std::filesystem::remove_all(path); }
  std::string file(const std::string& name = "raft.state") const { return path + "/" + name; }
  std::string journal() const {
    std::string result;
    for (const auto& entry : std::filesystem::directory_iterator(path)) {
      if (entry.path().filename().string().rfind("raft.wal.", 0) == 0) {
        if (!result.empty()) throw std::runtime_error("multiple active journals");
        result = entry.path();
      }
    }
    return result;
  }
  std::string path;
};

FileStore::Options JournalOptions() {
  FileStore::Options options;
  options.journal = true;
  return options;
}

std::string Read(const std::string& path) {
  std::ifstream file(path, std::ios::binary);
  return {std::istreambuf_iterator<char>(file), std::istreambuf_iterator<char>()};
}
void Write(const std::string& path, const std::string& bytes) {
  std::ofstream file(path, std::ios::binary | std::ios::trunc);
  file.write(bytes.data(), bytes.size());
  file.close();
  ASSERT_TRUE(file.good());
}

TEST(RaftJournal, MigratesLegacyImageAndRecoversBinarySuffixAndVote) {
  JournalDirectory directory;
  {
    FileStore old(directory.path);
    ASSERT_TRUE(old.Save({3, 2}).ok());
    ASSERT_TRUE(old.ReplaceFrom(1, {{1, 2, "old"}}).ok());
  }
  {
    FileStore store(directory.path, JournalOptions());
    ASSERT_EQ(store.ReadState().entries[0].command, "old");
    ASSERT_TRUE(store.Save({4, 1}).ok());
    ASSERT_TRUE(store.ReplaceFrom(2, {{2, 4, std::string("a\0b", 3)}}).ok());
  }
  {
    FileStore store(directory.path, JournalOptions());
    EXPECT_EQ(store.ReadState().hard_state.term, 4);
    EXPECT_EQ(store.ReadState().hard_state.voted_for, 1);
    ASSERT_EQ(store.ReadState().entries.size(), 2);
    EXPECT_EQ(store.ReadState().entries[1].command, std::string("a\0b", 3));
  }
  EXPECT_THROW(FileStore downgrade(directory.path), std::runtime_error);
}

TEST(RaftJournal, PhaseMetricsAccountForRolloverAndImagePublication) {
  for (bool journal : {false, true}) {
    JournalDirectory directory;
    FileStore::Options options;
    options.journal = journal;
    options.journal_checkpoint_bytes = 128;
    FileStore store(directory.path, options);
    ASSERT_TRUE(store.Save({1, 1}).ok());
    ASSERT_TRUE(store.ReplaceFrom(1, {{1, 1, std::string(65536, 'x')}}).ok());
    const auto before = store.ReadMetrics();
    ASSERT_TRUE(store.Save({2, 1}).ok());
    const auto after = store.ReadMetrics();
    EXPECT_EQ(std::accumulate(after.checkpoint_duration_buckets.begin(),
                              after.checkpoint_duration_buckets.end(), uint64_t{0}),
              after.checkpoint_saves);
    EXPECT_EQ(std::accumulate(after.journal_sync_duration_buckets.begin(),
                              after.journal_sync_duration_buckets.end(), uint64_t{0}),
              after.journal_saves);
    EXPECT_GT(after.checkpoint_saves, before.checkpoint_saves);
    EXPECT_GT(after.crc_nanoseconds, before.crc_nanoseconds);
    EXPECT_LE(after.crc_nanoseconds, after.encode_nanoseconds);
    EXPECT_GT(after.file_sync_nanoseconds, 0U);
    EXPECT_GT(after.directory_sync_nanoseconds, 0U);
    EXPECT_LE(after.file_sync_nanoseconds + after.directory_sync_nanoseconds,
              after.sync_nanoseconds);
    EXPECT_LE(after.checkpoint_max_nanoseconds, after.persist_nanoseconds);
    EXPECT_EQ(store.ReadState().entries.front().command, std::string(65536, 'x'));
  }
}

TEST(RaftJournal, AppendsOnlyDeltaAndLeavesLargeCheckpointUntouched) {
  JournalDirectory directory;
  FileStore store(directory.path, JournalOptions());
  ASSERT_TRUE(store.Save({1, 1}).ok());
  ASSERT_TRUE(store.ReplaceFrom(1, {{1, 1, ""}}).ok());
  ASSERT_TRUE(store.SaveSnapshot({1, 1, std::string(512 * 1024, 's')}, {}).ok());
  const auto checkpoint = Read(directory.file());
  const auto before = store.ReadMetrics();
  for (uint64_t i = 2; i <= 101; ++i) {
    ASSERT_TRUE(store.ReplaceFrom(i, {{i, 1, "small"}}).ok());
  }
  const auto after = store.ReadMetrics();
  EXPECT_EQ(Read(directory.file()), checkpoint);
  EXPECT_EQ(after.checkpoint_saves, before.checkpoint_saves);
  EXPECT_EQ(after.journal_saves - before.journal_saves, 100);
  EXPECT_LT(after.bytes_written - before.bytes_written, 10000);
  EXPECT_EQ(after.file_syncs - before.file_syncs, 100);
}

TEST(RaftJournal, ReplacementAndTruncationReplayWithoutResurrectingOldSuffix) {
  JournalDirectory directory;
  {
    FileStore store(directory.path, JournalOptions());
    ASSERT_TRUE(store.Save({4, 0}).ok());
    ASSERT_TRUE(store.ReplaceFrom(1, {{1, 1, "a"}, {2, 2, "old"}, {3, 2, "tail"}}).ok());
    ASSERT_TRUE(store.ReplaceFrom(2, {{2, 4, "replacement"}}).ok());
    ASSERT_TRUE(store.ReplaceFrom(3, {{3, 4, "temporary"}}).ok());
    ASSERT_TRUE(store.ReplaceFrom(3, {}).ok());
  }
  FileStore store(directory.path, JournalOptions());
  const auto state = store.ReadState();
  ASSERT_EQ(state.entries.size(), 2);
  EXPECT_EQ(state.entries[0].command, "a");
  EXPECT_EQ(state.entries[1].command, "replacement");
}

TEST(RaftJournal, EveryIncompleteFinalRecordIsTruncatedButAcknowledgedPrefixRemains) {
  JournalDirectory directory;
  size_t boundary;
  {
    FileStore store(directory.path, JournalOptions());
    ASSERT_TRUE(store.Save({2, 1}).ok());
    ASSERT_TRUE(store.ReplaceFrom(1, {{1, 1, "acknowledged"}}).ok());
    boundary = Read(directory.journal()).size();
    ASSERT_TRUE(store.ReplaceFrom(2, {{2, 2, "uncertain"}}).ok());
  }
  const auto path = directory.journal();
  const auto original = Read(path);
  for (size_t cut = boundary; cut < original.size(); ++cut) {
    SCOPED_TRACE(cut);
    Write(path, original.substr(0, cut));
    FileStore recovered(directory.path, JournalOptions());
    EXPECT_EQ(recovered.ReadState().hard_state.voted_for, 1);
    ASSERT_EQ(recovered.ReadState().entries.size(), 1);
    EXPECT_EQ(recovered.ReadState().entries[0].command, "acknowledged");
    EXPECT_EQ(Read(path).size(), boundary);
  }
}

TEST(RaftJournal, RejectsCompleteCorruptRecordIncludingFinalRecordAndHeaderLength) {
  JournalDirectory directory;
  size_t boundary;
  {
    FileStore store(directory.path, JournalOptions());
    ASSERT_TRUE(store.Save({2, 1}).ok());
    boundary = Read(directory.journal()).size();
    ASSERT_TRUE(store.ReplaceFrom(1, {{1, 2, "payload"}}).ok());
  }
  const auto path = directory.journal();
  const auto original = Read(path);
  for (size_t offset : {size_t{0}, size_t{12}, size_t{16}, size_t{28}, size_t{32},
                        original.size() - boundary - 1}) {
    auto damaged = original;
    damaged[boundary + offset] ^= 0x40;
    Write(path, damaged);
    EXPECT_THROW(FileStore recovered(directory.path, JournalOptions()), std::runtime_error);
    EXPECT_EQ(Read(path), damaged);
  }
}

TEST(RaftJournal, RollsCheckpointGenerationAndReclaimsObsoleteJournal) {
  JournalDirectory directory;
  auto options = JournalOptions();
  options.journal_checkpoint_bytes = 128;
  {
    FileStore store(directory.path, options);
    ASSERT_TRUE(store.Save({1, 1}).ok());
    for (uint64_t i = 1; i <= 30; ++i) {
      ASSERT_TRUE(store.ReplaceFrom(i, {{i, 1, "record"}}).ok());
    }
    EXPECT_GT(store.ReadMetrics().checkpoint_saves, 3);
    EXPECT_FALSE(directory.journal().empty());
  }
  FileStore store(directory.path, options);
  ASSERT_EQ(store.ReadState().entries.size(), 30);
  EXPECT_EQ(store.ReadState().entries.back().index, 30);
}

TEST(RaftJournal, MissingCheckpointOrActiveJournalFailsClosed) {
  JournalDirectory directory;
  {
    FileStore store(directory.path, JournalOptions());
    ASSERT_TRUE(store.Save({1, 1}).ok());
  }
  const auto image = Read(directory.file());
  const auto journal = directory.journal();
  std::filesystem::remove(directory.file());
  EXPECT_THROW(FileStore store(directory.path, JournalOptions()), std::runtime_error);
  Write(directory.file(), image);
  std::filesystem::remove(journal);
  EXPECT_THROW(FileStore store(directory.path, JournalOptions()), std::system_error);
}

TEST(RaftJournal, JournalFailurePoisonsInstanceUntilReopenAtAllAppendBoundaries) {
  for (auto point :
       {Point::kBeforeJournalWrite, Point::kBeforeJournalSync, Point::kAfterJournalSync}) {
    JournalDirectory directory;
    auto options = JournalOptions();
    bool armed = false;
    options.persist_observer = [&](Point reached) {
      if (armed && reached == point) throw std::system_error(EIO, std::generic_category());
    };
    {
      FileStore store(directory.path, options);
      ASSERT_TRUE(store.Save({1, 1}).ok());
      armed = true;
      EXPECT_FALSE(store.Save({2, 2}).ok());
      EXPECT_TRUE(store.failed());
      EXPECT_FALSE(store.Save({3, 3}).ok());
      EXPECT_THROW(store.ReadState(), std::runtime_error);
    }
    FileStore store(directory.path, JournalOptions());
    EXPECT_GE(store.ReadState().hard_state.term, 1);
    ASSERT_TRUE(store.Save({3, 3}).ok());
  }
}

TEST(RaftJournal, SigkillAtAppendBoundariesRecoversWholeOldOrNewSuffix) {
  for (auto point :
       {Point::kBeforeJournalWrite, Point::kBeforeJournalSync, Point::kAfterJournalSync}) {
    JournalDirectory directory;
    {
      FileStore store(directory.path, JournalOptions());
      ASSERT_TRUE(store.Save({4, 1}).ok());
      ASSERT_TRUE(store.ReplaceFrom(1, {{1, 1, "ack"}, {2, 2, "old"}, {3, 2, "tail"}}).ok());
    }
    const auto child = ::fork();
    ASSERT_GE(child, 0);
    if (child == 0) {
      auto options = JournalOptions();
      options.persist_observer = [point](Point reached) {
        if (point == reached) ::raise(SIGKILL);
      };
      FileStore store(directory.path, options);
      store.ReplaceFrom(2, {{2, 4, "new"}});
      ::_exit(3);
    }
    int status = 0;
    ASSERT_EQ(::waitpid(child, &status, 0), child);
    ASSERT_TRUE(WIFSIGNALED(status));
    ASSERT_EQ(WTERMSIG(status), SIGKILL);
    FileStore store(directory.path, JournalOptions());
    const auto state = store.ReadState();
    ASSERT_GE(state.entries.size(), 2);
    EXPECT_EQ(state.entries[0].command, "ack");
    if (state.entries[1].command == "new") {
      EXPECT_EQ(state.entries.size(), 2);
    } else {
      EXPECT_EQ(state.entries.size(), 3);
      EXPECT_EQ(state.entries[1].command, "old");
      EXPECT_EQ(state.entries[2].command, "tail");
    }
    ASSERT_TRUE(store.ReplaceFrom(2, {{2, 4, "continued"}}).ok());
  }
}

TEST(RaftJournal, SigkillAtCheckpointBoundariesKeepsCheckpointAndJournalPaired) {
  for (auto point : {Point::kBeforeFileSync, Point::kBeforeRename, Point::kBeforeDirectorySync,
                     Point::kAfterDirectorySync}) {
    JournalDirectory directory;
    {
      FileStore store(directory.path, JournalOptions());
      ASSERT_TRUE(store.Save({2, 1}).ok());
      ASSERT_TRUE(store.ReplaceFrom(1, {{1, 1, "a"}, {2, 2, "b"}}).ok());
    }
    const auto child = ::fork();
    ASSERT_GE(child, 0);
    if (child == 0) {
      auto options = JournalOptions();
      options.persist_observer = [point](Point reached) {
        if (point == reached) ::raise(SIGKILL);
      };
      FileStore store(directory.path, options);
      store.SaveSnapshot({1, 1, "snapshot-a"}, {{2, 2, "b"}});
      ::_exit(3);
    }
    int status = 0;
    ASSERT_EQ(::waitpid(child, &status, 0), child);
    ASSERT_TRUE(WIFSIGNALED(status));
    ASSERT_EQ(WTERMSIG(status), SIGKILL);
    FileStore store(directory.path, JournalOptions());
    const auto state = store.ReadState();
    if (state.snapshot.index == 0) {
      ASSERT_EQ(state.entries.size(), 2);
      EXPECT_EQ(state.entries[0].command, "a");
    } else {
      EXPECT_EQ(state.snapshot.index, 1);
      EXPECT_EQ(state.snapshot.data, "snapshot-a");
      ASSERT_EQ(state.entries.size(), 1);
    }
    EXPECT_EQ(state.entries.back().command, "b");
    ASSERT_TRUE(store.ReplaceFrom(3, {{3, 2, "after-restart"}}).ok());
    EXPECT_FALSE(directory.journal().empty());
  }
}
}  // namespace
