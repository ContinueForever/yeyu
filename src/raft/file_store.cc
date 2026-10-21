#include "raft/file_store.h"

#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <stdexcept>
#include <string_view>
#include <system_error>
#include <utility>

namespace yewukv::raft {
namespace {

constexpr size_t kHeaderBytes = 32;
constexpr size_t kSnapshotHeaderBytes = 56;
constexpr size_t kEntryBytes = 20;
constexpr size_t kChecksumBytes = 4;
constexpr char kStateFile[] = "raft.state";
constexpr char kTemporaryFile[] = "raft.state.tmp";

class FileDescriptor {
 public:
  explicit FileDescriptor(int fd) : fd_(fd) {}
  ~FileDescriptor() {
    if (fd_ >= 0) ::close(fd_);
  }
  int get() const { return fd_; }
  FileDescriptor(const FileDescriptor&) = delete;
  FileDescriptor& operator=(const FileDescriptor&) = delete;

 private:
  int fd_;
};

void Require(bool condition, const char* message) {
  if (!condition) throw std::runtime_error(std::string("Raft state corruption: ") + message);
}

void CheckIO(bool condition, const char* operation) {
  if (!condition) throw std::system_error(errno, std::generic_category(), operation);
}

void Put32(char* out, uint32_t value) {
  for (int i = 3; i >= 0; --i) out[3 - i] = static_cast<char>(value >> (8 * i));
}

void Put64(char* out, uint64_t value) {
  for (int i = 7; i >= 0; --i) out[7 - i] = static_cast<char>(value >> (8 * i));
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
    for (int bit = 0; bit < 8; ++bit) crc = (crc >> 1) ^ (0xedb88320U & (0U - (crc & 1U)));
  }
  return ~crc;
}

// Sync every parent even if it already exists: it might have been created by
// an earlier interrupted initialization that never reached its own fsync.
void SyncParents(std::filesystem::path directory) {
  directory = std::filesystem::absolute(directory).lexically_normal();
  for (;;) {
    const auto parent = directory.parent_path();
    FileDescriptor fd(::open(parent.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC));
    CheckIO(fd.get() >= 0, "open Raft parent directory");
    CheckIO(::fsync(fd.get()) == 0, "sync Raft parent directory");
    if (parent == directory) break;
    directory = parent;
  }
}

size_t EncodedSize(const DurableState& state) {
  size_t bytes = (state.snapshot.index == 0 ? kHeaderBytes : kSnapshotHeaderBytes) +
                 state.snapshot.data.size() + kChecksumBytes;
  for (const auto& entry : state.entries) bytes += kEntryBytes + entry.command.size();
  return bytes;
}

std::string Encode(const DurableState& state) {
  std::string bytes(EncodedSize(state), '\0');
  std::memcpy(bytes.data(), "YRFT", 4);
  Put32(bytes.data() + 4, state.snapshot.index == 0 ? 1 : 2);
  Put64(bytes.data() + 8, state.hard_state.term);
  Put64(bytes.data() + 16, state.hard_state.voted_for);
  Put64(bytes.data() + 24, state.entries.size());
  size_t offset = kHeaderBytes;
  if (state.snapshot.index != 0) {
    Put64(bytes.data() + 32, state.snapshot.index);
    Put64(bytes.data() + 40, state.snapshot.term);
    Put64(bytes.data() + 48, state.snapshot.data.size());
    offset = kSnapshotHeaderBytes;
    std::memcpy(bytes.data() + offset, state.snapshot.data.data(), state.snapshot.data.size());
    offset += state.snapshot.data.size();
  }
  for (const auto& entry : state.entries) {
    Put64(bytes.data() + offset, entry.index);
    Put64(bytes.data() + offset + 8, entry.term);
    Put32(bytes.data() + offset + 16, static_cast<uint32_t>(entry.command.size()));
    offset += kEntryBytes;
    std::memcpy(bytes.data() + offset, entry.command.data(), entry.command.size());
    offset += entry.command.size();
  }
  Put32(bytes.data() + offset, Crc32(std::string_view(bytes.data(), offset)));
  return bytes;
}

DurableState Decode(const std::string& bytes) {
  const size_t end = bytes.size() - kChecksumBytes;
  Require(std::memcmp(bytes.data(), "YRFT", 4) == 0, "invalid magic");
  const uint32_t version = Get32(bytes.data() + 4);
  Require(version == 1 || version == 2, "unsupported version");
  Require(Get32(bytes.data() + end) == Crc32(std::string_view(bytes.data(), end)),
          "checksum mismatch");
  DurableState state;
  state.hard_state = {Get64(bytes.data() + 8), Get64(bytes.data() + 16)};
  Require(state.hard_state.term != 0 || state.hard_state.voted_for == 0, "vote in term zero");
  const uint64_t count = Get64(bytes.data() + 24);
  size_t offset = kHeaderBytes;
  uint64_t previous_term = 0;
  if (version == 2) {
    Require(end >= kSnapshotHeaderBytes, "truncated snapshot header");
    state.snapshot.index = Get64(bytes.data() + 32);
    state.snapshot.term = Get64(bytes.data() + 40);
    const uint64_t size = Get64(bytes.data() + 48);
    Require(state.snapshot.index != 0 && state.snapshot.index != UINT64_MAX &&
                state.snapshot.term != 0 && state.snapshot.term <= state.hard_state.term,
            "invalid snapshot position");
    Require(size <= FileStore::kMaxSnapshotBytes && size <= end - kSnapshotHeaderBytes,
            "invalid snapshot size");
    offset = kSnapshotHeaderBytes;
    state.snapshot.data = bytes.substr(offset, size);
    offset += size;
    previous_term = state.snapshot.term;
  }
  Require(count <= (end - offset) / kEntryBytes && count < UINT64_MAX - state.snapshot.index,
          "invalid entry count");
  for (uint64_t i = 0; i < count; ++i) {
    Require(end - offset >= kEntryBytes, "truncated log entry");
    const uint64_t index = Get64(bytes.data() + offset);
    const uint64_t term = Get64(bytes.data() + offset + 8);
    const uint32_t size = Get32(bytes.data() + offset + 16);
    offset += kEntryBytes;
    Require(index == state.snapshot.index + i + 1, "non-contiguous log indexes");
    Require(term != 0 && term >= previous_term && term <= state.hard_state.term,
            "invalid log term");
    Require(size <= FileStore::kMaxCommandBytes && size <= end - offset, "invalid command length");
    state.entries.push_back({index, term, bytes.substr(offset, size)});
    offset += size;
    previous_term = term;
  }
  Require(offset == end, "trailing bytes");
  return state;
}

}  // namespace

FileStore::FileStore(std::string directory) : FileStore(std::move(directory), Options{}) {}

FileStore::FileStore(std::string directory, Options options) : options_(std::move(options)) {
  if (directory.empty() || options_.max_state_bytes < kHeaderBytes + kChecksumBytes ||
      options_.max_state_bytes > kMaxStateBytes)
    throw std::invalid_argument("invalid Raft directory or state size limit");
  try {
    std::filesystem::create_directories(directory);
    directory_fd_ = ::open(directory.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    CheckIO(directory_fd_ >= 0, "open Raft directory");
    lock_fd_ = ::openat(directory_fd_, "LOCK", O_CREAT | O_RDWR | O_CLOEXEC | O_NOFOLLOW, 0600);
    CheckIO(lock_fd_ >= 0, "open Raft directory lock");
    CheckIO(::flock(lock_fd_, LOCK_EX | LOCK_NB) == 0, "Raft directory already in use");
    SyncParents(directory);
    Recover();
  } catch (...) {
    Close();
    throw;
  }
}

FileStore::~FileStore() {
  Close();
}

void FileStore::Close() {
  if (lock_fd_ >= 0) ::close(lock_fd_);
  if (directory_fd_ >= 0) ::close(directory_fd_);
  lock_fd_ = directory_fd_ = -1;
}

void FileStore::Recover() {
  FileDescriptor fd(
      ::openat(directory_fd_, kStateFile, O_RDONLY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK));
  if (fd.get() < 0) {
    CheckIO(errno == ENOENT, "open Raft state");
    const auto status = Persist({});
    if (!status.ok()) throw std::runtime_error(status.ToString());
    return;
  }
  struct stat info {};
  CheckIO(::fstat(fd.get(), &info) == 0, "stat Raft state");
  Require(S_ISREG(info.st_mode), "state is not a regular file");
  Require(info.st_size >= static_cast<off_t>(kHeaderBytes + kChecksumBytes) &&
              static_cast<uint64_t>(info.st_size) <= options_.max_state_bytes,
          "invalid file size");
  std::string bytes(static_cast<size_t>(info.st_size), '\0');
  size_t offset = 0;
  while (offset < bytes.size()) {
    const auto n = ::read(fd.get(), bytes.data() + offset, bytes.size() - offset);
    if (n < 0 && errno == EINTR) continue;
    CheckIO(n >= 0, "read Raft state");
    Require(n != 0, "truncated state file");
    offset += static_cast<size_t>(n);
  }
  state_ = Decode(bytes);
  metrics_.state_bytes = bytes.size();
  // A process may have died after rename and before syncing the directory.
  // Pin the recovered image before a same-state Save can return OK.
  CheckIO(::fsync(directory_fd_) == 0, "sync recovered Raft directory");
}

DurableState FileStore::ReadState() const {
  std::lock_guard<std::mutex> lock(mutex_);
  if (failed_) throw std::runtime_error("Raft file store failed; restart required");
  return state_;
}

bool FileStore::failed() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return failed_;
}

FileStore::Metrics FileStore::ReadMetrics() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return metrics_;
}

Status FileStore::CheckUsable() const {
  return failed_ ? Status::IOError("Raft file store failed; restart required") : Status::OK();
}

Status FileStore::Persist(DurableState next) {
  if (EncodedSize(next) > options_.max_state_bytes)
    return Status::Busy("Raft state size limit reached");
  const auto started = std::chrono::steady_clock::now();
  try {
    const std::string bytes = Encode(next);
    CheckIO(::unlinkat(directory_fd_, kTemporaryFile, 0) == 0 || errno == ENOENT,
            "remove stale Raft temporary file");
    FileDescriptor fd(::openat(directory_fd_, kTemporaryFile,
                               O_CREAT | O_EXCL | O_WRONLY | O_CLOEXEC | O_NOFOLLOW, 0600));
    CheckIO(fd.get() >= 0, "create Raft temporary file");
    size_t offset = 0;
    while (offset < bytes.size()) {
      const auto n = ::write(fd.get(), bytes.data() + offset, bytes.size() - offset);
      if (n < 0 && errno == EINTR) continue;
      if (n == 0) errno = EIO;
      CheckIO(n > 0, "write Raft state");
      offset += static_cast<size_t>(n);
    }
    auto observe = [this](PersistPoint point) {
      if (options_.persist_observer) options_.persist_observer(point);
    };
    observe(PersistPoint::kBeforeFileSync);
    CheckIO(::fdatasync(fd.get()) == 0, "sync Raft state");
    observe(PersistPoint::kBeforeRename);
    CheckIO(::renameat(directory_fd_, kTemporaryFile, directory_fd_, kStateFile) == 0,
            "replace Raft state");
    observe(PersistPoint::kBeforeDirectorySync);
    CheckIO(::fsync(directory_fd_) == 0, "sync Raft directory");
    observe(PersistPoint::kAfterDirectorySync);
    if (next.snapshot.index > state_.snapshot.index) ++metrics_.snapshots;
    state_ = std::move(next);
    ++metrics_.saves;
    metrics_.state_bytes = bytes.size();
    metrics_.bytes_written += bytes.size();
    metrics_.persist_nanoseconds +=
        static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                  std::chrono::steady_clock::now() - started)
                                  .count());
    return Status::OK();
  } catch (const std::exception& error) {
    failed_ = true;
    return Status::IOError(std::string("persist Raft state: ") + error.what());
  } catch (...) {
    failed_ = true;
    return Status::IOError("persist Raft state: unknown exception");
  }
}

Status FileStore::Save(const HardState& state) {
  std::lock_guard<std::mutex> lock(mutex_);
  const auto usable = CheckUsable();
  if (!usable.ok()) return usable;
  const auto& previous = state_.hard_state;
  if (state.term < previous.term || (state.term == 0 && state.voted_for != 0) ||
      (state.term == previous.term && previous.voted_for != 0 &&
       state.voted_for != previous.voted_for))
    return Status::InvalidArgument("Raft term/vote would regress or change an existing vote");
  if (state.term == previous.term && state.voted_for == previous.voted_for) return Status::OK();
  auto next = state_;
  next.hard_state = state;
  return Persist(std::move(next));
}

Status FileStore::ReplaceFrom(uint64_t first_index, const std::vector<LogEntry>& suffix) {
  std::lock_guard<std::mutex> lock(mutex_);
  const auto usable = CheckUsable();
  if (!usable.ok()) return usable;
  if (first_index <= state_.snapshot.index ||
      first_index > state_.snapshot.index + state_.entries.size() + 1)
    return Status::InvalidArgument("Raft replacement must follow an existing prefix");
  const size_t prefix = static_cast<size_t>(first_index - state_.snapshot.index - 1);
  uint64_t term = prefix == 0 ? state_.snapshot.term : state_.entries[prefix - 1].term;
  size_t bytes = (state_.snapshot.index == 0 ? kHeaderBytes : kSnapshotHeaderBytes) +
                 state_.snapshot.data.size() + kChecksumBytes;
  for (size_t i = 0; i < prefix; ++i) bytes += kEntryBytes + state_.entries[i].command.size();
  for (size_t i = 0; i < suffix.size(); ++i) {
    const auto& entry = suffix[i];
    if (i >= UINT64_MAX - first_index || entry.index != first_index + i || entry.term == 0 ||
        entry.term < term || entry.term > state_.hard_state.term ||
        entry.command.size() > kMaxCommandBytes)
      return Status::InvalidArgument("invalid Raft suffix index, term, or command size");
    if (kEntryBytes + entry.command.size() > options_.max_state_bytes - bytes)
      return Status::Busy("Raft state size limit reached");
    bytes += kEntryBytes + entry.command.size();
    term = entry.term;
  }
  DurableState next{state_.hard_state,
                    {state_.entries.begin(), state_.entries.begin() + prefix},
                    state_.snapshot};
  next.entries.insert(next.entries.end(), suffix.begin(), suffix.end());
  return Persist(std::move(next));
}

Status FileStore::SaveSnapshot(const Snapshot& snapshot, const std::vector<LogEntry>& suffix) {
  std::lock_guard<std::mutex> lock(mutex_);
  const auto usable = CheckUsable();
  if (!usable.ok()) return usable;
  if (snapshot.index <= state_.snapshot.index || snapshot.index == UINT64_MAX ||
      snapshot.term == 0 || snapshot.term < state_.snapshot.term ||
      snapshot.term > state_.hard_state.term || snapshot.data.size() > kMaxSnapshotBytes ||
      suffix.size() >= UINT64_MAX - snapshot.index)
    return Status::InvalidArgument("invalid snapshot index, term or size");
  uint64_t term = snapshot.term;
  for (size_t i = 0; i < suffix.size(); ++i) {
    const auto& entry = suffix[i];
    if (entry.index != snapshot.index + i + 1 || entry.term < term ||
        entry.term > state_.hard_state.term || entry.command.size() > kMaxCommandBytes)
      return Status::InvalidArgument("invalid snapshot suffix");
    term = entry.term;
  }
  return Persist({state_.hard_state, suffix, snapshot});
}

}  // namespace yewukv::raft
