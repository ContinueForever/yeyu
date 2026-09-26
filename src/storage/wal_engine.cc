#include "storage/wal_engine.h"

#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstring>
#include <filesystem>
#include <limits>
#include <stdexcept>
#include <system_error>
#include <vector>

namespace yewukv::storage {
namespace {

constexpr std::array<char, 4> kMagic{'Y', 'W', 'A', 'L'};
constexpr uint8_t kVersion = 1;
constexpr uint8_t kPut = 1;
constexpr uint8_t kDelete = 2;
constexpr size_t kHeaderBytes = 24;
constexpr size_t kChecksumBytes = 4;
constexpr size_t kMaxRecordBytes =
    kHeaderBytes + WalEngine::kMaxKeyBytes + WalEngine::kMaxValueBytes + kChecksumBytes;

std::string ErrorText(const char* operation, int error = errno) {
  return std::string(operation) + ": " + std::strerror(error);
}

void Put16(char* out, uint16_t value) {
  out[0] = static_cast<char>(value >> 8);
  out[1] = static_cast<char>(value);
}
void Put32(char* out, uint32_t value) {
  for (int i = 3; i >= 0; --i) out[3 - i] = static_cast<char>(value >> (i * 8));
}
void Put64(char* out, uint64_t value) {
  for (int i = 7; i >= 0; --i) out[7 - i] = static_cast<char>(value >> (i * 8));
}
uint16_t Get16(const char* in) {
  return (static_cast<uint16_t>(static_cast<uint8_t>(in[0])) << 8) | static_cast<uint8_t>(in[1]);
}
uint32_t Get32(const char* in) {
  uint32_t value = 0;
  for (int i = 0; i < 4; ++i) value = (value << 8) | static_cast<uint8_t>(in[i]);
  return value;
}
uint64_t Get64(const char* in) {
  uint64_t value = 0;
  for (int i = 0; i < 8; ++i) value = (value << 8) | static_cast<uint8_t>(in[i]);
  return value;
}

uint32_t Crc32(std::string_view bytes) {
  uint32_t crc = 0xffffffffU;
  for (unsigned char byte : bytes) {
    crc ^= byte;
    for (int bit = 0; bit < 8; ++bit) {
      const uint32_t mask = 0U - (crc & 1U);
      crc = (crc >> 1) ^ (0xedb88320U & mask);
    }
  }
  return ~crc;
}

std::string EncodeRecord(uint8_t operation, uint64_t sequence, std::string_view key,
                         std::string_view value) {
  const size_t record_size = kHeaderBytes + key.size() + value.size() + kChecksumBytes;
  std::string record(record_size, '\0');
  std::copy(kMagic.begin(), kMagic.end(), record.begin());
  record[4] = static_cast<char>(kVersion);
  record[5] = static_cast<char>(operation);
  Put16(record.data() + 6, 0);
  Put64(record.data() + 8, sequence);
  Put32(record.data() + 16, static_cast<uint32_t>(key.size()));
  Put32(record.data() + 20, static_cast<uint32_t>(value.size()));
  std::copy(key.begin(), key.end(), record.begin() + kHeaderBytes);
  std::copy(value.begin(), value.end(), record.begin() + kHeaderBytes + key.size());
  Put32(record.data() + record.size() - kChecksumBytes,
        Crc32(std::string_view(record.data() + 4, record.size() - 8)));
  return record;
}

bool ReadAt(int fd, char* data, size_t size, off_t offset, size_t* bytes_read) {
  *bytes_read = 0;
  while (*bytes_read < size) {
    ssize_t count = ::pread(fd, data + *bytes_read, size - *bytes_read,
                            offset + static_cast<off_t>(*bytes_read));
    if (count > 0) {
      *bytes_read += static_cast<size_t>(count);
    } else if (count == 0) {
      return true;
    } else if (errno != EINTR) {
      return false;
    }
  }
  return true;
}

bool WriteAt(int fd, const char* data, size_t size, off_t offset) {
  size_t written = 0;
  while (written < size) {
    ssize_t count =
        ::pwrite(fd, data + written, size - written, offset + static_cast<off_t>(written));
    if (count > 0) {
      written += static_cast<size_t>(count);
    } else if (count < 0 && errno == EINTR) {
      continue;
    } else {
      if (count == 0) errno = EIO;
      return false;
    }
  }
  return true;
}

void SyncDirectoryChain(const std::filesystem::path& directory) {
  auto current = std::filesystem::absolute(directory).lexically_normal();
  while (true) {
    auto parent = current.parent_path();
    if (parent.empty()) parent = ".";
    int fd = ::open(parent.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (fd < 0) throw std::system_error(errno, std::generic_category(), "open parent directory");
    const int result = ::fsync(fd);
    const int saved = errno;
    ::close(fd);
    if (result < 0)
      throw std::system_error(saved, std::generic_category(), "fsync parent directory");
    if (parent == current || current == current.root_path()) break;
    current = std::move(parent);
  }
}

}  // namespace

WalEngine::WalEngine(std::string data_directory, uint64_t wal_limit_bytes)
    : data_directory_(std::move(data_directory)), wal_limit_bytes_(wal_limit_bytes) {
  try {
    if (data_directory_.empty()) throw std::invalid_argument("data directory must not be empty");
    if (wal_limit_bytes_ == 0 || wal_limit_bytes_ > kMaxWalBytes)
      throw std::invalid_argument("WAL limit must be between 1 byte and 1 GiB");
    const std::filesystem::path path(data_directory_);
    const bool existed = std::filesystem::exists(path);
    std::filesystem::create_directories(path);
    directory_fd_ = ::open(path.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (directory_fd_ < 0)
      throw std::system_error(errno, std::generic_category(), "open data directory");
    if (!existed) SyncDirectoryChain(path);

    lock_fd_ = ::openat(directory_fd_, "LOCK", O_CREAT | O_RDWR | O_CLOEXEC | O_NOFOLLOW, 0640);
    if (lock_fd_ < 0)
      throw std::system_error(errno, std::generic_category(), "open data directory lock");
    if (::flock(lock_fd_, LOCK_EX | LOCK_NB) < 0) {
      const int saved = errno;
      throw std::system_error(saved, std::generic_category(), "data directory is already in use");
    }

    const bool wal_existed = ::faccessat(directory_fd_, "wal.log", F_OK, AT_SYMLINK_NOFOLLOW) == 0;
    wal_fd_ = ::openat(directory_fd_, "wal.log", O_CREAT | O_RDWR | O_CLOEXEC | O_NOFOLLOW, 0640);
    if (wal_fd_ < 0) throw std::system_error(errno, std::generic_category(), "open WAL");
    struct stat info {};
    if (::fstat(wal_fd_, &info) < 0)
      throw std::system_error(errno, std::generic_category(), "stat WAL");
    if (!S_ISREG(info.st_mode)) throw std::runtime_error("WAL path is not a regular file");
    if (!wal_existed) {
      if (::fsync(directory_fd_) < 0)
        throw std::system_error(errno, std::generic_category(), "sync data directory");
    }
    Recover();
  } catch (...) {
    CloseResources();
    throw;
  }
}

WalEngine::~WalEngine() {
  CloseResources();
}

void WalEngine::CloseResources() {
  if (wal_fd_ >= 0) {
    ::close(wal_fd_);
    wal_fd_ = -1;
  }
  if (lock_fd_ >= 0) {
    ::flock(lock_fd_, LOCK_UN);
    ::close(lock_fd_);
    lock_fd_ = -1;
  }
  if (directory_fd_ >= 0) {
    ::close(directory_fd_);
    directory_fd_ = -1;
  }
}

void WalEngine::Recover() {
  struct stat info {};
  if (::fstat(wal_fd_, &info) < 0)
    throw std::system_error(errno, std::generic_category(), "stat WAL");
  if (info.st_size < 0 || static_cast<uint64_t>(info.st_size) > wal_limit_bytes_) {
    throw std::runtime_error("WAL exceeds the configured size limit");
  }
  const uint64_t size = static_cast<uint64_t>(info.st_size);
  uint64_t offset = 0;
  while (offset < size) {
    const uint64_t remaining = size - offset;
    if (remaining < kHeaderBytes) break;  // A torn final header is safe to discard.

    std::array<char, kHeaderBytes> header{};
    size_t bytes = 0;
    if (!ReadAt(wal_fd_, header.data(), header.size(), static_cast<off_t>(offset), &bytes)) {
      throw std::system_error(errno, std::generic_category(), "read WAL header");
    }
    if (bytes != header.size()) break;
    if (!std::equal(kMagic.begin(), kMagic.end(), header.begin())) {
      throw std::runtime_error("WAL corruption: invalid record magic at offset " +
                               std::to_string(offset));
    }
    if (static_cast<uint8_t>(header[4]) != kVersion || Get16(header.data() + 6) != 0) {
      throw std::runtime_error("WAL corruption: unsupported record header at offset " +
                               std::to_string(offset));
    }
    const uint8_t operation = static_cast<uint8_t>(header[5]);
    const uint64_t sequence = Get64(header.data() + 8);
    const uint32_t key_size = Get32(header.data() + 16);
    const uint32_t value_size = Get32(header.data() + 20);
    if ((operation != kPut && operation != kDelete) || key_size == 0 || key_size > kMaxKeyBytes ||
        value_size > kMaxValueBytes || (operation == kDelete && value_size != 0)) {
      throw std::runtime_error("WAL corruption: invalid operation or lengths at offset " +
                               std::to_string(offset));
    }
    const uint64_t record_size =
        kHeaderBytes + static_cast<uint64_t>(key_size) + value_size + kChecksumBytes;
    if (record_size > kMaxRecordBytes || record_size > remaining) break;
    std::string record(static_cast<size_t>(record_size), '\0');
    if (!ReadAt(wal_fd_, record.data(), record.size(), static_cast<off_t>(offset), &bytes)) {
      throw std::system_error(errno, std::generic_category(), "read WAL record");
    }
    if (bytes != record.size()) break;
    const uint32_t expected_crc = Get32(record.data() + record.size() - kChecksumBytes);
    if (Crc32(std::string_view(record.data() + 4, record.size() - 8)) != expected_crc) {
      throw std::runtime_error("WAL corruption: checksum mismatch at offset " +
                               std::to_string(offset));
    }
    if (sequence != sequence_ + 1 || sequence_ == std::numeric_limits<uint64_t>::max()) {
      throw std::runtime_error("WAL corruption: non-contiguous sequence at offset " +
                               std::to_string(offset));
    }
    const std::string_view key(record.data() + kHeaderBytes, key_size);
    const std::string_view value(record.data() + kHeaderBytes + key_size, value_size);
    Status status = operation == kPut ? mem_.Put(key, value) : mem_.Delete(key);
    if (!status.ok()) throw std::runtime_error("WAL recovery apply failed: " + status.ToString());
    sequence_ = sequence;
    offset += record_size;
  }
  if (offset != size) {
    if (::ftruncate(wal_fd_, static_cast<off_t>(offset)) < 0) {
      throw std::system_error(errno, std::generic_category(), "truncate incomplete WAL tail");
    }
    if (::fdatasync(wal_fd_) < 0)
      throw std::system_error(errno, std::generic_category(), "sync truncated WAL");
  }
  file_bytes_ = offset;
  if (::lseek(wal_fd_, static_cast<off_t>(file_bytes_), SEEK_SET) < 0) {
    throw std::system_error(errno, std::generic_category(), "seek WAL end");
  }
}

Status WalEngine::CheckUsable() const {
  if (!failed_) return Status::OK();
  return Status::IOError("WAL is in a failed state: " + failure_message_);
}

Status WalEngine::Append(uint8_t operation, std::string_view key, std::string_view value) {
  if (key.empty() || key.size() > kMaxKeyBytes || value.size() > kMaxValueBytes) {
    return Status::InvalidArgument("key/value exceeds WAL record limits");
  }
  const uint64_t record_size = kHeaderBytes + key.size() + value.size() + kChecksumBytes;
  if (record_size > wal_limit_bytes_ - file_bytes_) {
    const auto status = CompactLocked();
    if (!status.ok()) return status;
    if (record_size > wal_limit_bytes_ - file_bytes_)
      return Status::Busy("live data and new record exceed the WAL size limit");
  }
  if (sequence_ == std::numeric_limits<uint64_t>::max())
    return Status::Busy("WAL sequence is exhausted");

  const std::string record = EncodeRecord(operation, sequence_ + 1, key, value);

  if (!WriteAt(wal_fd_, record.data(), record.size(), static_cast<off_t>(file_bytes_))) {
    failed_ = true;
    failure_message_ = ErrorText("write WAL");
    return Status::IOError(failure_message_);
  }
  if (::fdatasync(wal_fd_) < 0) {
    failed_ = true;
    failure_message_ = ErrorText("fdatasync WAL");
    return Status::IOError(failure_message_);
  }
  file_bytes_ += record_size;
  ++sequence_;
  return Status::OK();
}

Status WalEngine::Compact() {
  std::lock_guard<std::mutex> lock(mutex_);
  const auto usable = CheckUsable();
  if (!usable.ok()) return usable;
  return CompactLocked();
}

Status WalEngine::CompactLocked(std::string_view deleted_key) {
  // The current WAL remains authoritative until a complete replacement is
  // synced and atomically renamed into place.
  auto snapshot = mem_.Snapshot();
  if (!deleted_key.empty()) {
    snapshot.erase(std::remove_if(snapshot.begin(), snapshot.end(),
                                  [&](const auto& entry) { return entry.first == deleted_key; }),
                   snapshot.end());
  }
  uint64_t compact_bytes = 0;
  for (const auto& [key, value] : snapshot) {
    const uint64_t size = kHeaderBytes + key.size() + value.size() + kChecksumBytes;
    if (size > wal_limit_bytes_ - compact_bytes)
      return Status::Busy("live data exceed the WAL size limit");
    compact_bytes += size;
  }
  constexpr char kTemporaryName[] = "wal.compact";
  if (::unlinkat(directory_fd_, kTemporaryName, 0) < 0 && errno != ENOENT)
    return Status::IOError(ErrorText("remove stale compact WAL"));
  const int compact_fd = ::openat(directory_fd_, kTemporaryName,
                                  O_CREAT | O_EXCL | O_RDWR | O_CLOEXEC | O_NOFOLLOW, 0640);
  if (compact_fd < 0) return Status::IOError(ErrorText("create compact WAL"));
  auto discard = [&](std::string error) {
    ::close(compact_fd);
    ::unlinkat(directory_fd_, kTemporaryName, 0);
    return Status::IOError(std::move(error));
  };
  uint64_t offset = 0;
  uint64_t next_sequence = 0;
  for (const auto& [key, value] : snapshot) {
    const std::string record = EncodeRecord(kPut, ++next_sequence, key, value);
    if (!WriteAt(compact_fd, record.data(), record.size(), static_cast<off_t>(offset)))
      return discard(ErrorText("write compact WAL"));
    offset += record.size();
  }
  if (::fdatasync(compact_fd) < 0) return discard(ErrorText("sync compact WAL"));
  if (::renameat(directory_fd_, kTemporaryName, directory_fd_, "wal.log") < 0)
    return discard(ErrorText("replace WAL"));

  const int old_fd = wal_fd_;
  wal_fd_ = compact_fd;
  file_bytes_ = offset;
  sequence_ = next_sequence;
  ::close(old_fd);
  if (::fsync(directory_fd_) < 0) {
    failed_ = true;
    failure_message_ = ErrorText("sync WAL replacement directory");
    return Status::IOError(failure_message_);
  }
  return Status::OK();
}

Status WalEngine::Put(std::string_view key, std::string_view value) {
  std::lock_guard<std::mutex> lock(mutex_);
  const auto usable = CheckUsable();
  if (!usable.ok()) return usable;
  const auto status = Append(kPut, key, value);
  if (!status.ok()) return status;
  return ApplyCommitted(kPut, key, value);
}

Status WalEngine::Get(std::string_view key, std::string* value) {
  std::lock_guard<std::mutex> lock(mutex_);
  const auto usable = CheckUsable();
  if (!usable.ok()) return usable;
  return mem_.Get(key, value);
}

Status WalEngine::Delete(std::string_view key) {
  std::lock_guard<std::mutex> lock(mutex_);
  const auto usable = CheckUsable();
  if (!usable.ok()) return usable;
  if (key.empty() || key.size() > kMaxKeyBytes)
    return Status::InvalidArgument("key/value exceeds WAL record limits");
  const uint64_t record_size = kHeaderBytes + key.size() + kChecksumBytes;
  if (record_size > wal_limit_bytes_ - file_bytes_) {
    // The replacement snapshot commits the deletion without needing room for
    // an extra Delete record, even when live data fills the WAL.
    const auto compact_status = CompactLocked(key);
    if (!compact_status.ok()) return compact_status;
    return ApplyCommitted(kDelete, key, {});
  }
  const auto status = Append(kDelete, key, {});
  if (!status.ok()) return status;
  return ApplyCommitted(kDelete, key, {});
}

Status WalEngine::ApplyCommitted(uint8_t operation, std::string_view key, std::string_view value) {
  try {
    Status status = operation == kPut ? mem_.Put(key, value) : mem_.Delete(key);
    if (status.ok()) return status;
    failure_message_ = "durable WAL record could not be applied to memory: " + status.ToString();
  } catch (const std::exception& error) {
    failure_message_ =
        std::string("durable WAL record could not be applied to memory: ") + error.what();
  } catch (...) {
    failure_message_ = "durable WAL record could not be applied to memory";
  }
  failed_ = true;
  return Status::IOError(failure_message_);
}

uint64_t WalEngine::LastSequence() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return sequence_;
}

uint64_t WalEngine::WalBytes() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return file_bytes_;
}

}  // namespace yewukv::storage
