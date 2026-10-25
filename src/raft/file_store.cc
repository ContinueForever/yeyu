#include "raft/file_store.h"

#include <fcntl.h>
#include <dirent.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <algorithm>
#include <array>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <iterator>
#include <stdexcept>
#include <string_view>
#include <system_error>
#include <utility>

namespace yewukv::raft {
namespace {

uint64_t TraceStamp(std::chrono::steady_clock::time_point point) noexcept {
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(point.time_since_epoch()).count());
}

constexpr size_t kHeaderBytes = 32;
constexpr size_t kSnapshotHeaderBytes = 56;
constexpr size_t kCheckpointHeaderBytes = 64;
constexpr size_t kJournalHeaderBytes = 32;
constexpr uint32_t kHardStateDelta = 1;
constexpr uint32_t kSuffixDelta = 2;
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

uint32_t Crc32(std::string_view bytes, uint64_t* elapsed_ns = nullptr) {
  const auto began = std::chrono::steady_clock::now();
  // Same IEEE polynomial, initial value and final complement as v1/v2/v3.
  // A table replaces eight dependent bit steps per byte; the disk format and
  // corruption checks stay byte-for-byte compatible with the old encoder.
  static constexpr auto table = [] {
    std::array<uint32_t, 256> values{};
    for (uint32_t i = 0; i < values.size(); ++i) {
      uint32_t crc = i;
      for (int bit = 0; bit < 8; ++bit) crc = (crc >> 1) ^ (0xedb88320U & (0U - (crc & 1U)));
      values[i] = crc;
    }
    return values;
  }();
  uint32_t crc = 0xffffffffU;
  for (unsigned char byte : bytes) crc = (crc >> 8) ^ table[(crc ^ byte) & 0xffU];
  if (elapsed_ns)
    *elapsed_ns += static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                             std::chrono::steady_clock::now() - began)
                                             .count());
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

std::string Encode(const DurableState& state, uint64_t generation = 0, uint64_t* crc_ns = nullptr) {
  const size_t old_header = state.snapshot.index == 0 ? kHeaderBytes : kSnapshotHeaderBytes;
  std::string bytes(EncodedSize(state) + (generation ? kCheckpointHeaderBytes - old_header : 0),
                    '\0');
  std::memcpy(bytes.data(), "YRFT", 4);
  Put32(bytes.data() + 4, generation ? 3 : (state.snapshot.index == 0 ? 1 : 2));
  Put64(bytes.data() + 8, state.hard_state.term);
  Put64(bytes.data() + 16, state.hard_state.voted_for);
  Put64(bytes.data() + 24, state.entries.size());
  size_t offset = kHeaderBytes;
  if (state.snapshot.index != 0 || generation != 0) {
    Put64(bytes.data() + 32, state.snapshot.index);
    Put64(bytes.data() + 40, state.snapshot.term);
    Put64(bytes.data() + 48, state.snapshot.data.size());
    offset = kSnapshotHeaderBytes;
    if (generation) {
      Put64(bytes.data() + 56, generation);
      offset = kCheckpointHeaderBytes;
    }
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
  Put32(bytes.data() + offset, Crc32(std::string_view(bytes.data(), offset), crc_ns));
  return bytes;
}

DurableState Decode(const std::string& bytes, uint64_t* generation) {
  const size_t end = bytes.size() - kChecksumBytes;
  Require(std::memcmp(bytes.data(), "YRFT", 4) == 0, "invalid magic");
  const uint32_t version = Get32(bytes.data() + 4);
  Require(version >= 1 && version <= 3, "unsupported version");
  *generation = 0;
  Require(Get32(bytes.data() + end) == Crc32(std::string_view(bytes.data(), end)),
          "checksum mismatch");
  DurableState state;
  state.hard_state = {Get64(bytes.data() + 8), Get64(bytes.data() + 16)};
  Require(state.hard_state.term != 0 || state.hard_state.voted_for == 0, "vote in term zero");
  const uint64_t count = Get64(bytes.data() + 24);
  size_t offset = kHeaderBytes;
  uint64_t previous_term = 0;
  if (version >= 2) {
    const size_t header = version == 3 ? kCheckpointHeaderBytes : kSnapshotHeaderBytes;
    Require(end >= header, "truncated snapshot header");
    state.snapshot.index = Get64(bytes.data() + 32);
    state.snapshot.term = Get64(bytes.data() + 40);
    const uint64_t size = Get64(bytes.data() + 48);
    Require(state.snapshot.index != UINT64_MAX &&
                (state.snapshot.index == 0) == (state.snapshot.term == 0) &&
                (state.snapshot.index != 0 || (version == 3 && size == 0)) &&
                state.snapshot.term <= state.hard_state.term,
            "invalid snapshot position");
    Require(size <= FileStore::kMaxSnapshotBytes && size <= end - header, "invalid snapshot size");
    offset = header;
    if (version == 3) {
      *generation = Get64(bytes.data() + 56);
      Require(*generation != 0, "zero checkpoint generation");
    }
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

uint64_t Nanoseconds(std::chrono::steady_clock::time_point begin) {
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - begin)
          .count());
}

void RecordLatency(uint64_t value, uint64_t* maximum, std::array<uint64_t, 16>* buckets) {
  *maximum = std::max(*maximum, value);
  size_t bucket = 0;
  while (bucket < FileStore::kLatencyBoundsNs.size() && value > FileStore::kLatencyBoundsNs[bucket])
    ++bucket;
  ++(*buckets)[bucket];
}

void WriteAll(int fd, std::string_view bytes) {
  while (!bytes.empty()) {
    const auto n = ::write(fd, bytes.data(), bytes.size());
    if (n < 0 && errno == EINTR) continue;
    if (n == 0) errno = EIO;
    CheckIO(n > 0, "write Raft durable data");
    bytes.remove_prefix(static_cast<size_t>(n));
  }
}

std::string JournalName(uint64_t generation) {
  return "raft.wal." + std::to_string(generation);
}

std::vector<std::string> JournalFiles(int directory) {
  DIR* stream = ::fdopendir(::openat(directory, ".", O_RDONLY | O_DIRECTORY | O_CLOEXEC));
  CheckIO(stream != nullptr, "list Raft journals");
  std::vector<std::string> files;
  while (auto* entry = ::readdir(stream)) {
    const std::string name = entry->d_name;
    if (name.rfind("raft.wal.", 0) == 0) files.push_back(name);
  }
  ::closedir(stream);
  return files;
}

std::string SuffixDelta(uint64_t index, const std::vector<LogEntry>& suffix) {
  size_t size = 16;
  for (const auto& entry : suffix) size += kEntryBytes + entry.command.size();
  std::string bytes(size, '\0');
  Put64(bytes.data(), index);
  Put64(bytes.data() + 8, suffix.size());
  size_t offset = 16;
  for (const auto& entry : suffix) {
    Put64(bytes.data() + offset, entry.index);
    Put64(bytes.data() + offset + 8, entry.term);
    Put32(bytes.data() + offset + 16, static_cast<uint32_t>(entry.command.size()));
    offset += kEntryBytes;
    std::memcpy(bytes.data() + offset, entry.command.data(), entry.command.size());
    offset += entry.command.size();
  }
  return bytes;
}

void ReplayDelta(DurableState* state, uint32_t kind, std::string_view bytes) {
  if (kind == kHardStateDelta) {
    Require(bytes.size() == 16, "invalid journal hard state");
    HardState next{Get64(bytes.data()), Get64(bytes.data() + 8)};
    const auto old = state->hard_state;
    Require(next.term >= old.term && (next.term != 0 || next.voted_for == 0) &&
                (next.term != old.term || old.voted_for == 0 || next.voted_for == old.voted_for),
            "journal term or vote regression");
    state->hard_state = next;
    return;
  }
  Require(kind == kSuffixDelta && bytes.size() >= 16, "invalid journal delta kind or size");
  const auto first = Get64(bytes.data());
  const auto count = Get64(bytes.data() + 8);
  Require(first > state->snapshot.index &&
              first <= state->snapshot.index + state->entries.size() + 1 &&
              count <= (bytes.size() - 16) / kEntryBytes && count < UINT64_MAX - first,
          "invalid journal suffix range");
  const auto prefix = static_cast<size_t>(first - state->snapshot.index - 1);
  uint64_t term = prefix ? state->entries[prefix - 1].term : state->snapshot.term;
  std::vector<LogEntry> suffix;
  size_t offset = 16;
  for (uint64_t i = 0; i < count; ++i) {
    Require(bytes.size() - offset >= kEntryBytes, "truncated journal entry");
    const auto index = Get64(bytes.data() + offset);
    const auto next_term = Get64(bytes.data() + offset + 8);
    const auto size = Get32(bytes.data() + offset + 16);
    offset += kEntryBytes;
    Require(index == first + i && next_term != 0 && next_term >= term &&
                next_term <= state->hard_state.term && size <= FileStore::kMaxCommandBytes &&
                size <= bytes.size() - offset,
            "invalid journal entry");
    suffix.push_back({index, next_term, std::string(bytes.substr(offset, size))});
    offset += size;
    term = next_term;
  }
  Require(offset == bytes.size(), "trailing journal bytes");
  state->entries.resize(prefix);
  state->entries.insert(state->entries.end(), std::make_move_iterator(suffix.begin()),
                        std::make_move_iterator(suffix.end()));
}

}  // namespace

class FileStore::TraceOperation {
 public:
  TraceOperation(FileStore& store, PersistenceKind kind) noexcept
      : store_(store), previous_(store.active_trace_) {
    try {
      if (!store.options_.trace_observer ||
          (store.options_.trace_enabled && !store.options_.trace_enabled()) ||
          store.trace_sequence_ == UINT64_MAX)
        return;
      enabled_ = true;
      record_.sequence = ++store.trace_sequence_;
      record_.kind = static_cast<uint64_t>(kind);
      record_.started_ns = TraceStamp(std::chrono::steady_clock::now());
      crc_before_ = store.metrics_.crc_nanoseconds;
      store.active_trace_ = &record_;
    } catch (...) {
      // Diagnostics must never affect a serving or recovering store.
    }
  }
  ~TraceOperation() noexcept {
    store_.active_trace_ = previous_;
    if (!enabled_) return;
    record_.finished_ns = TraceStamp(std::chrono::steady_clock::now());
    record_.crc_ns = store_.metrics_.crc_nanoseconds - crc_before_;
    try {
      store_.options_.trace_observer(record_);
    } catch (...) {
    }
  }
  void Bytes(uint64_t bytes) { record_.bytes = bytes; }
  void Success() { record_.success = 1; }
  TraceOperation(const TraceOperation&) = delete;
  TraceOperation& operator=(const TraceOperation&) = delete;

 private:
  FileStore& store_;
  PersistenceTrace* previous_;
  PersistenceTrace record_;
  uint64_t crc_before_ = 0;
  bool enabled_ = false;
};

class FileStore::TraceSpan {
 public:
  TraceSpan(FileStore& store, PersistenceStage stage) noexcept
      : TraceSpan(store, stage,
                  store.active_trace_ ? std::chrono::steady_clock::now()
                                      : std::chrono::steady_clock::time_point{}) {}
  TraceSpan(FileStore& store, PersistenceStage stage,
            std::chrono::steady_clock::time_point began) noexcept
      : record_(store.active_trace_),
        stage_(static_cast<uint64_t>(stage)),
        began_(record_ ? TraceStamp(began) : 0) {}
  ~TraceSpan() noexcept {
    if (!record_) return;
    if (record_->span_count == record_->spans.size()) {
      record_->spans_limited = 1;
      return;
    }
    record_->spans[record_->span_count++] = {stage_, began_,
                                             TraceStamp(std::chrono::steady_clock::now())};
  }
  TraceSpan(const TraceSpan&) = delete;
  TraceSpan& operator=(const TraceSpan&) = delete;

 private:
  PersistenceTrace* record_;
  uint64_t stage_;
  uint64_t began_;
};

FileStore::FileStore(std::string directory) : FileStore(std::move(directory), Options{}) {}

FileStore::FileStore(std::string directory, Options options) : options_(std::move(options)) {
  if (directory.empty() || options_.max_state_bytes < kHeaderBytes + kChecksumBytes ||
      options_.max_state_bytes > kMaxStateBytes || options_.journal_checkpoint_bytes < 128 ||
      options_.journal_checkpoint_bytes > kMaxStateBytes)
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
  if (journal_fd_ >= 0) ::close(journal_fd_);
  if (lock_fd_ >= 0) ::close(lock_fd_);
  if (directory_fd_ >= 0) ::close(directory_fd_);
  lock_fd_ = directory_fd_ = -1;
  journal_fd_ = -1;
}

void FileStore::Recover() {
  FileDescriptor fd(
      ::openat(directory_fd_, kStateFile, O_RDONLY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK));
  if (fd.get() < 0) {
    CheckIO(errno == ENOENT, "open Raft state");
    Require(JournalFiles(directory_fd_).empty(), "checkpoint missing while journals exist");
    const auto status = Persist({});
    if (!status.ok()) throw std::runtime_error(status.ToString());
    return;
  }
  struct stat info {};
  CheckIO(::fstat(fd.get(), &info) == 0, "stat Raft state");
  Require(S_ISREG(info.st_mode), "state is not a regular file");
  Require(
      info.st_size >= static_cast<off_t>(kHeaderBytes + kChecksumBytes) &&
          static_cast<uint64_t>(info.st_size) <= options_.max_state_bytes + kCheckpointHeaderBytes,
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
  state_ = Decode(bytes, &generation_);
  Require(EncodedSize(state_) <= options_.max_state_bytes, "recovered state exceeds capacity");
  Require(generation_ == 0 || options_.journal, "journal checkpoint requires journal mode");
  metrics_.state_bytes = EncodedSize(state_);
  metrics_.checkpoint_bytes = bytes.size();
  // A process may have died after rename and before syncing the directory.
  // Pin the recovered image before a same-state Save can return OK.
  CheckIO(::fsync(directory_fd_) == 0, "sync recovered Raft directory");
  if (generation_ != 0) {
    RecoverJournal();
    RemoveOldJournals();
  } else if (options_.journal) {
    const auto status = Checkpoint(state_);
    if (!status.ok()) throw std::runtime_error(status.ToString());
  }
}

void FileStore::RemoveOldJournals() {
  const auto active = JournalName(generation_);
  bool removed = false;
  for (const auto& name : JournalFiles(directory_fd_)) {
    if (name == active) continue;
    CheckIO(::unlinkat(directory_fd_, name.c_str(), 0) == 0, "remove obsolete Raft journal");
    removed = true;
  }
  if (removed) {
    const auto begin = std::chrono::steady_clock::now();
    SyncDirectory("sync Raft journal cleanup");
    metrics_.sync_nanoseconds += Nanoseconds(begin);
  }
}

void FileStore::RecoverJournal() {
  const auto name = JournalName(generation_);
  journal_fd_ = ::openat(directory_fd_, name.c_str(),
                         O_RDWR | O_APPEND | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK);
  CheckIO(journal_fd_ >= 0, "open checkpoint-bound Raft journal");
  struct stat info {};
  CheckIO(::fstat(journal_fd_, &info) == 0, "stat Raft journal");
  Require(S_ISREG(info.st_mode) && info.st_size >= 0 &&
              static_cast<uint64_t>(info.st_size) <=
                  options_.max_state_bytes + options_.journal_checkpoint_bytes + 128,
          "invalid journal file size");
  std::string bytes(static_cast<size_t>(info.st_size), '\0');
  size_t read = 0;
  while (read < bytes.size()) {
    const auto n = ::pread(journal_fd_, bytes.data() + read, bytes.size() - read, read);
    if (n < 0 && errno == EINTR) continue;
    CheckIO(n >= 0, "read Raft journal");
    Require(n != 0, "journal changed during recovery");
    read += static_cast<size_t>(n);
  }
  size_t offset = 0;
  while (bytes.size() - offset >= kJournalHeaderBytes) {
    const char* header = bytes.data() + offset;
    Require(std::memcmp(header, "YRJL", 4) == 0 && Get32(header + 4) == 1 &&
                Get32(header + 24) == 0 &&
                Get32(header + 28) == Crc32(std::string_view(header, 28)),
            "journal header checksum/format");
    const auto kind = Get32(header + 8);
    const auto size = Get32(header + 12);
    Require((kind == kHardStateDelta || kind == kSuffixDelta) &&
                size <= options_.max_state_bytes + 16 &&
                Get64(header + 16) == journal_sequence_ + 1,
            "journal kind, length or sequence");
    const size_t record = kJournalHeaderBytes + size + kChecksumBytes;
    if (record > bytes.size() - offset) break;
    Require(Get32(header + kJournalHeaderBytes + size) ==
                Crc32(std::string_view(header, kJournalHeaderBytes + size)),
            "journal record checksum");
    ReplayDelta(&state_, kind, std::string_view(header + kJournalHeaderBytes, size));
    Require(EncodedSize(state_) <= options_.max_state_bytes, "journal state exceeds capacity");
    ++journal_sequence_;
    offset += record;
  }
  // Only an incomplete physical tail may be discarded. Complete bad checksums,
  // lengths or sequence numbers fail closed, even for the last record.
  if (offset != bytes.size())
    CheckIO(::ftruncate(journal_fd_, static_cast<off_t>(offset)) == 0,
            "truncate incomplete Raft journal tail");
  CheckIO(::fdatasync(journal_fd_) == 0, "sync recovered Raft journal");
  metrics_.journal_bytes = offset;
  metrics_.state_bytes = EncodedSize(state_);
}

uint64_t FileStore::SyncFile(int fd, const char* operation) {
  const auto began = std::chrono::steady_clock::now();
  TraceSpan span(*this, PersistenceStage::kFileSync, began);
  CheckIO(::fdatasync(fd) == 0, operation);
  const auto elapsed = Nanoseconds(began);
  metrics_.file_sync_nanoseconds += elapsed;
  ++metrics_.file_syncs;
  return elapsed;
}

void FileStore::SyncDirectory(const char* operation) {
  const auto began = std::chrono::steady_clock::now();
  TraceSpan span(*this, PersistenceStage::kDirectorySync, began);
  CheckIO(::fsync(directory_fd_) == 0, operation);
  metrics_.directory_sync_nanoseconds += Nanoseconds(began);
  ++metrics_.directory_syncs;
}

Status FileStore::Checkpoint(DurableState next) {
  if (EncodedSize(next) > options_.max_state_bytes)
    return Status::Busy("Raft state size limit reached");
  if (generation_ == UINT64_MAX) return Status::IOError("Raft checkpoint generation exhausted");
  TraceOperation tracing(*this, PersistenceKind::kCheckpoint);
  const auto begin = std::chrono::steady_clock::now();
  int next_fd = -1;
  try {
    std::string bytes;
    {
      const auto encode_begin = std::chrono::steady_clock::now();
      TraceSpan span(*this, PersistenceStage::kEncode, encode_begin);
      bytes = Encode(next, generation_ + 1, &metrics_.crc_nanoseconds);
      metrics_.encode_nanoseconds += Nanoseconds(encode_begin);
    }
    tracing.Bytes(bytes.size());
    const auto name = JournalName(generation_ + 1);
    CheckIO(::unlinkat(directory_fd_, name.c_str(), 0) == 0 || errno == ENOENT,
            "remove orphan next journal");
    next_fd = ::openat(directory_fd_, name.c_str(),
                       O_CREAT | O_EXCL | O_RDWR | O_APPEND | O_CLOEXEC | O_NOFOLLOW, 0600);
    CheckIO(next_fd >= 0, "create next Raft journal");
    auto prepare_sync = std::chrono::steady_clock::now();
    SyncFile(next_fd, "sync new Raft journal");
    // Publish the empty journal's directory entry before a checkpoint can
    // reference it, including a crash between rename and the final dir sync.
    SyncDirectory("sync new Raft journal directory entry");
    metrics_.sync_nanoseconds += Nanoseconds(prepare_sync);
    CheckIO(::unlinkat(directory_fd_, kTemporaryFile, 0) == 0 || errno == ENOENT,
            "remove stale checkpoint");
    FileDescriptor fd(::openat(directory_fd_, kTemporaryFile,
                               O_CREAT | O_EXCL | O_WRONLY | O_CLOEXEC | O_NOFOLLOW, 0600));
    CheckIO(fd.get() >= 0, "create Raft checkpoint");
    {
      const auto write_begin = std::chrono::steady_clock::now();
      TraceSpan span(*this, PersistenceStage::kWrite, write_begin);
      WriteAll(fd.get(), bytes);
      metrics_.write_nanoseconds += Nanoseconds(write_begin);
    }
    auto observe = [this](PersistPoint point) {
      if (options_.persist_observer) options_.persist_observer(point);
    };
    observe(PersistPoint::kBeforeFileSync);
    const auto sync_begin = std::chrono::steady_clock::now();
    SyncFile(fd.get(), "sync Raft checkpoint");
    observe(PersistPoint::kBeforeRename);
    {
      TraceSpan span(*this, PersistenceStage::kRename);
      CheckIO(::renameat(directory_fd_, kTemporaryFile, directory_fd_, kStateFile) == 0,
              "publish Raft checkpoint");
    }
    observe(PersistPoint::kBeforeDirectorySync);
    SyncDirectory("sync Raft checkpoint directory");
    observe(PersistPoint::kAfterDirectorySync);
    metrics_.sync_nanoseconds += Nanoseconds(sync_begin);
    if (journal_fd_ >= 0) ::close(journal_fd_);
    journal_fd_ = next_fd;
    next_fd = -1;
    ++generation_;
    journal_sequence_ = 0;
    if (next.snapshot.index > state_.snapshot.index) ++metrics_.snapshots;
    state_ = std::move(next);
    ++metrics_.saves;
    ++metrics_.checkpoint_saves;
    metrics_.checkpoint_bytes = bytes.size();
    metrics_.state_bytes = EncodedSize(state_);
    metrics_.journal_bytes = 0;
    metrics_.bytes_written += bytes.size();
    RemoveOldJournals();
    const auto elapsed = Nanoseconds(begin);
    metrics_.persist_nanoseconds += elapsed;
    RecordLatency(elapsed, &metrics_.checkpoint_max_nanoseconds,
                  &metrics_.checkpoint_duration_buckets);
    tracing.Success();
    return Status::OK();
  } catch (const std::exception& error) {
    if (next_fd >= 0) ::close(next_fd);
    failed_ = true;
    return Status::IOError(std::string("checkpoint Raft state: ") + error.what());
  } catch (...) {
    if (next_fd >= 0) ::close(next_fd);
    failed_ = true;
    return Status::IOError("checkpoint Raft state failed");
  }
}

Status FileStore::AppendDelta(uint32_t kind, const std::string& payload) {
  if (metrics_.journal_bytes >= options_.journal_checkpoint_bytes) {
    const auto status = Checkpoint(state_);
    if (!status.ok()) return status;
  }
  TraceOperation tracing(*this, kind == kHardStateDelta ? PersistenceKind::kHardStateDelta
                                                        : PersistenceKind::kSuffixDelta);
  const auto begin = std::chrono::steady_clock::now();
  try {
    if (journal_sequence_ == UINT64_MAX)
      throw std::runtime_error("Raft journal sequence exhausted");
    std::string bytes;
    {
      const auto encode_begin = std::chrono::steady_clock::now();
      TraceSpan span(*this, PersistenceStage::kEncode, encode_begin);
      bytes.assign(kJournalHeaderBytes + payload.size() + kChecksumBytes, '\0');
      std::memcpy(bytes.data(), "YRJL", 4);
      Put32(bytes.data() + 4, 1);
      Put32(bytes.data() + 8, kind);
      Put32(bytes.data() + 12, static_cast<uint32_t>(payload.size()));
      Put64(bytes.data() + 16, journal_sequence_ + 1);
      Put32(bytes.data() + 28,
            Crc32(std::string_view(bytes.data(), 28), &metrics_.crc_nanoseconds));
      std::memcpy(bytes.data() + kJournalHeaderBytes, payload.data(), payload.size());
      Put32(bytes.data() + bytes.size() - 4,
            Crc32(std::string_view(bytes.data(), bytes.size() - 4), &metrics_.crc_nanoseconds));
      metrics_.encode_nanoseconds += Nanoseconds(encode_begin);
    }
    tracing.Bytes(bytes.size());
    if (options_.persist_observer) options_.persist_observer(PersistPoint::kBeforeJournalWrite);
    {
      const auto write_begin = std::chrono::steady_clock::now();
      TraceSpan span(*this, PersistenceStage::kWrite, write_begin);
      WriteAll(journal_fd_, bytes);
      metrics_.write_nanoseconds += Nanoseconds(write_begin);
    }
    if (options_.persist_observer) options_.persist_observer(PersistPoint::kBeforeJournalSync);
    const auto sync_begin = std::chrono::steady_clock::now();
    const auto journal_sync = SyncFile(journal_fd_, "sync Raft journal delta");
    metrics_.sync_nanoseconds += Nanoseconds(sync_begin);
    if (options_.persist_observer) options_.persist_observer(PersistPoint::kAfterJournalSync);
    ++journal_sequence_;
    ++metrics_.saves;
    ++metrics_.journal_saves;
    RecordLatency(journal_sync, &metrics_.journal_sync_max_nanoseconds,
                  &metrics_.journal_sync_duration_buckets);
    metrics_.journal_bytes += bytes.size();
    metrics_.bytes_written += bytes.size();
    metrics_.persist_nanoseconds += Nanoseconds(begin);
    tracing.Success();
    return Status::OK();
  } catch (const std::exception& error) {
    failed_ = true;
    return Status::IOError(std::string("append Raft journal: ") + error.what());
  } catch (...) {
    failed_ = true;
    return Status::IOError("append Raft journal failed");
  }
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
  if (options_.journal) return Checkpoint(std::move(next));
  if (EncodedSize(next) > options_.max_state_bytes)
    return Status::Busy("Raft state size limit reached");
  TraceOperation tracing(*this, PersistenceKind::kImage);
  const auto started = std::chrono::steady_clock::now();
  try {
    std::string bytes;
    {
      const auto encode_begin = std::chrono::steady_clock::now();
      TraceSpan span(*this, PersistenceStage::kEncode, encode_begin);
      bytes = Encode(next, 0, &metrics_.crc_nanoseconds);
      metrics_.encode_nanoseconds += Nanoseconds(encode_begin);
    }
    tracing.Bytes(bytes.size());
    CheckIO(::unlinkat(directory_fd_, kTemporaryFile, 0) == 0 || errno == ENOENT,
            "remove stale Raft temporary file");
    FileDescriptor fd(::openat(directory_fd_, kTemporaryFile,
                               O_CREAT | O_EXCL | O_WRONLY | O_CLOEXEC | O_NOFOLLOW, 0600));
    CheckIO(fd.get() >= 0, "create Raft temporary file");
    {
      size_t offset = 0;
      const auto write_begin = std::chrono::steady_clock::now();
      TraceSpan span(*this, PersistenceStage::kWrite, write_begin);
      while (offset < bytes.size()) {
        const auto n = ::write(fd.get(), bytes.data() + offset, bytes.size() - offset);
        if (n < 0 && errno == EINTR) continue;
        if (n == 0) errno = EIO;
        CheckIO(n > 0, "write Raft state");
        offset += static_cast<size_t>(n);
      }
      metrics_.write_nanoseconds += Nanoseconds(write_begin);
    }
    auto observe = [this](PersistPoint point) {
      if (options_.persist_observer) options_.persist_observer(point);
    };
    observe(PersistPoint::kBeforeFileSync);
    const auto sync_begin = std::chrono::steady_clock::now();
    SyncFile(fd.get(), "sync Raft state");
    observe(PersistPoint::kBeforeRename);
    {
      TraceSpan span(*this, PersistenceStage::kRename);
      CheckIO(::renameat(directory_fd_, kTemporaryFile, directory_fd_, kStateFile) == 0,
              "replace Raft state");
    }
    observe(PersistPoint::kBeforeDirectorySync);
    SyncDirectory("sync Raft directory");
    observe(PersistPoint::kAfterDirectorySync);
    metrics_.sync_nanoseconds += Nanoseconds(sync_begin);
    ++metrics_.checkpoint_saves;
    metrics_.checkpoint_bytes = bytes.size();
    if (next.snapshot.index > state_.snapshot.index) ++metrics_.snapshots;
    state_ = std::move(next);
    ++metrics_.saves;
    metrics_.state_bytes = bytes.size();
    metrics_.bytes_written += bytes.size();
    const auto elapsed = Nanoseconds(started);
    metrics_.persist_nanoseconds += elapsed;
    RecordLatency(elapsed, &metrics_.checkpoint_max_nanoseconds,
                  &metrics_.checkpoint_duration_buckets);
    tracing.Success();
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
  if (options_.journal) {
    std::string bytes(16, '\0');
    Put64(bytes.data(), state.term);
    Put64(bytes.data() + 8, state.voted_for);
    const auto status = AppendDelta(kHardStateDelta, bytes);
    if (status.ok()) state_.hard_state = state;
    return status;
  }
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
  if (options_.journal) {
    try {
      // Reserve before durable publication; allocation failure cannot leave a
      // successfully persisted mutation unapplied in this serving instance.
      state_.entries.reserve(prefix + suffix.size());
      std::vector<LogEntry> added = suffix;
      const auto encode_begin = std::chrono::steady_clock::now();
      const auto encoded = SuffixDelta(first_index, suffix);
      metrics_.encode_nanoseconds += Nanoseconds(encode_begin);
      const auto status = AppendDelta(kSuffixDelta, encoded);
      if (!status.ok()) return status;
      state_.entries.resize(prefix);
      state_.entries.insert(state_.entries.end(), std::make_move_iterator(added.begin()),
                            std::make_move_iterator(added.end()));
      metrics_.state_bytes = bytes;
      return Status::OK();
    } catch (...) {
      failed_ = true;
      return Status::IOError("apply durable journal suffix failed; restart required");
    }
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
