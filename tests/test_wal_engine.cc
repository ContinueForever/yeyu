#include <fcntl.h>
#include <unistd.h>

#include <filesystem>
#include <stdexcept>
#include <string>
#include <system_error>

#include <gtest/gtest.h>

#include "storage/wal_engine.h"

namespace {

class TemporaryDataDirectory {
 public:
  TemporaryDataDirectory() {
    std::string pattern = (std::filesystem::temp_directory_path() / "yewukv-wal-XXXXXX").string();
    char* created = ::mkdtemp(pattern.data());
    if (!created) throw std::system_error(errno, std::generic_category(), "mkdtemp");
    path_ = created;
  }
  ~TemporaryDataDirectory() { std::filesystem::remove_all(path_); }
  const std::string& path() const { return path_; }

 private:
  std::string path_;
};

void AppendBytes(const std::string& path, const std::string& bytes) {
  const int fd = ::open(path.c_str(), O_CREAT | O_WRONLY | O_APPEND, 0640);
  ASSERT_GE(fd, 0);
  ASSERT_EQ(::write(fd, bytes.data(), bytes.size()), static_cast<ssize_t>(bytes.size()));
  ASSERT_EQ(::fsync(fd), 0);
  ASSERT_EQ(::close(fd), 0);
}

}  // namespace

TEST(WalEngine, ReplaysBinaryValuesAndDeletes) {
  TemporaryDataDirectory directory;
  const std::string key("k\0\xff", 3);
  const std::string value("v\n\0\xfe", 4);
  {
    yewukv::storage::WalEngine engine(directory.path());
    ASSERT_TRUE(engine.Put(key, value).ok());
    ASSERT_TRUE(engine.Put("removed", "value").ok());
    ASSERT_TRUE(engine.Delete("removed").ok());
    EXPECT_EQ(engine.LastSequence(), 3);
  }
  {
    yewukv::storage::WalEngine engine(directory.path());
    std::string result;
    ASSERT_TRUE(engine.Get(key, &result).ok());
    EXPECT_EQ(result, value);
    EXPECT_EQ(engine.Get("removed", &result).code(), yewukv::StatusCode::kNotFound);
    EXPECT_EQ(engine.LastSequence(), 3);
    ASSERT_TRUE(engine.Put("after-replay", "ok").ok());
  }
  yewukv::storage::WalEngine engine(directory.path());
  EXPECT_EQ(engine.LastSequence(), 4);
}

TEST(WalEngine, TruncatesIncompleteTailAndContinuesSequence) {
  TemporaryDataDirectory directory;
  uint64_t valid_size = 0;
  {
    yewukv::storage::WalEngine engine(directory.path());
    ASSERT_TRUE(engine.Put("survivor", "value").ok());
    valid_size = engine.WalBytes();
  }
  const std::string wal = directory.path() + "/wal.log";
  AppendBytes(wal, "YWA");
  {
    yewukv::storage::WalEngine engine(directory.path());
    EXPECT_EQ(engine.WalBytes(), valid_size);
    std::string value;
    ASSERT_TRUE(engine.Get("survivor", &value).ok());
    EXPECT_EQ(value, "value");
    ASSERT_TRUE(engine.Put("later", "value2").ok());
    EXPECT_EQ(engine.LastSequence(), 2);
  }
  yewukv::storage::WalEngine engine(directory.path());
  EXPECT_EQ(engine.LastSequence(), 2);
}

TEST(WalEngine, RejectsCorruptCompleteRecordWithoutTruncation) {
  TemporaryDataDirectory directory;
  {
    yewukv::storage::WalEngine engine(directory.path());
    ASSERT_TRUE(engine.Put("key", "value").ok());
  }
  const std::string wal = directory.path() + "/wal.log";
  const auto size = std::filesystem::file_size(wal);
  const int fd = ::open(wal.c_str(), O_RDWR);
  ASSERT_GE(fd, 0);
  char corrupted = 'X';
  ASSERT_EQ(::pwrite(fd, &corrupted, 1, 24), 1);
  ASSERT_EQ(::fsync(fd), 0);
  ASSERT_EQ(::close(fd), 0);
  EXPECT_THROW(yewukv::storage::WalEngine engine(directory.path()), std::runtime_error);
  EXPECT_EQ(std::filesystem::file_size(wal), size);
}

TEST(WalEngine, EnforcesExclusiveDataDirectoryOwnership) {
  TemporaryDataDirectory directory;
  yewukv::storage::WalEngine first(directory.path());
  EXPECT_THROW(yewukv::storage::WalEngine second(directory.path()), std::system_error);
  ASSERT_TRUE(first.Put("key", "value").ok());
}

TEST(WalEngine, CompactionPreservesOnlyLiveValuesAndAllowsFurtherWrites) {
  TemporaryDataDirectory directory;
  uint64_t before = 0;
  uint64_t after = 0;
  {
    yewukv::storage::WalEngine engine(directory.path());
    ASSERT_TRUE(engine.Put("live", "old").ok());
    ASSERT_TRUE(engine.Put("deleted", "gone").ok());
    ASSERT_TRUE(engine.Delete("deleted").ok());
    ASSERT_TRUE(engine.Put("live", "new").ok());
    before = engine.WalBytes();
    // A previous crash may have left an incomplete replacement file.
    AppendBytes(directory.path() + "/wal.compact", "incomplete");
    ASSERT_TRUE(engine.Compact().ok());
    after = engine.WalBytes();
    EXPECT_LT(after, before);
    EXPECT_EQ(engine.LastSequence(), 1);
    ASSERT_TRUE(engine.Put("later", "value").ok());
  }
  {
    yewukv::storage::WalEngine engine(directory.path());
    std::string value;
    ASSERT_TRUE(engine.Get("live", &value).ok());
    EXPECT_EQ(value, "new");
    EXPECT_EQ(engine.Get("deleted", &value).code(), yewukv::StatusCode::kNotFound);
    ASSERT_TRUE(engine.Get("later", &value).ok());
    EXPECT_EQ(value, "value");
    EXPECT_EQ(engine.LastSequence(), 2);
    EXPECT_EQ(engine.WalBytes(), after + 24 + 5 + 5 + 4);
  }
}

TEST(WalEngine, IgnoresInterruptedCompactionAndCanRetry) {
  TemporaryDataDirectory directory;
  {
    yewukv::storage::WalEngine engine(directory.path());
    ASSERT_TRUE(engine.Put("stable", "committed").ok());
  }
  AppendBytes(directory.path() + "/wal.compact", "partial replacement");
  {
    yewukv::storage::WalEngine engine(directory.path());
    std::string value;
    ASSERT_TRUE(engine.Get("stable", &value).ok());
    EXPECT_EQ(value, "committed");
    ASSERT_TRUE(engine.Compact().ok());
  }
  EXPECT_FALSE(std::filesystem::exists(directory.path() + "/wal.compact"));
  yewukv::storage::WalEngine engine(directory.path());
  std::string value;
  ASSERT_TRUE(engine.Get("stable", &value).ok());
  EXPECT_EQ(value, "committed");
}

TEST(WalEngine, AutomaticallyCompactsWhenAppendReachesLimit) {
  TemporaryDataDirectory directory;
  {
    yewukv::storage::WalEngine engine(directory.path(), 70);
    ASSERT_TRUE(engine.Put("hot", "one").ok());
    ASSERT_TRUE(engine.Put("hot", "two").ok());
    ASSERT_TRUE(engine.Put("hot", "new").ok());
    EXPECT_EQ(engine.LastSequence(), 2);
    EXPECT_EQ(engine.WalBytes(), 68);
  }
  yewukv::storage::WalEngine engine(directory.path(), 70);
  std::string value;
  ASSERT_TRUE(engine.Get("hot", &value).ok());
  EXPECT_EQ(value, "new");
}

TEST(WalEngine, FullWalStillAllowsDurableDeletion) {
  TemporaryDataDirectory directory;
  {
    yewukv::storage::WalEngine engine(directory.path(), 64);
    ASSERT_TRUE(engine.Put("long-key", std::string(28, 'v')).ok());
    EXPECT_EQ(engine.WalBytes(), 64);
    ASSERT_TRUE(engine.Delete("long-key").ok());
    EXPECT_EQ(engine.WalBytes(), 0);
    ASSERT_TRUE(engine.Put("new", "v").ok());
  }
  yewukv::storage::WalEngine engine(directory.path(), 64);
  std::string value;
  EXPECT_EQ(engine.Get("long-key", &value).code(), yewukv::StatusCode::kNotFound);
  ASSERT_TRUE(engine.Get("new", &value).ok());
  EXPECT_EQ(value, "v");
}
