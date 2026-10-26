#pragma once

#include <array>
#include <cstdint>

namespace yewukv::raft {

// Diagnostic correlation only, scoped to the actual disk consumer thread.
inline thread_local uint64_t persistence_task_stream = 0;
inline thread_local uint64_t persistence_task_sequence = 0;
class PersistenceTaskScope {
 public:
  PersistenceTaskScope(uint64_t stream, uint64_t sequence)
      : stream_(persistence_task_stream), sequence_(persistence_task_sequence) {
    persistence_task_stream = stream;
    persistence_task_sequence = sequence;
  }
  ~PersistenceTaskScope() {
    persistence_task_stream = stream_;
    persistence_task_sequence = sequence_;
  }
  PersistenceTaskScope(const PersistenceTaskScope&) = delete;
  PersistenceTaskScope& operator=(const PersistenceTaskScope&) = delete;

 private:
  uint64_t stream_;
  uint64_t sequence_;
};

enum class PersistenceKind : uint64_t {
  kImage = 1,
  kCheckpoint = 2,
  kHardStateDelta = 3,
  kSuffixDelta = 4
};

enum class PersistenceStage : uint64_t {
  kEncode = 1,
  kWrite = 2,
  kFileSync = 3,
  kDirectorySync = 4,
  kRename = 5
};

// Numeric diagnostics only. All times use this process's steady_clock; CRC
// duration is contained in encoding, not an additional disjoint stage.
struct PersistenceTrace {
  struct Span {
    uint64_t stage = 0;
    uint64_t started_ns = 0;
    uint64_t finished_ns = 0;
  };
  uint64_t sequence = 0;
  uint64_t task_stream = 0;
  uint64_t task_sequence = 0;
  uint64_t kind = 0;
  uint64_t tid = 0;
  uint64_t started_ns = 0;
  uint64_t finished_ns = 0;
  uint64_t success = 0;
  uint64_t bytes = 0;
  uint64_t crc_ns = 0;
  uint64_t span_count = 0;
  uint64_t spans_limited = 0;
  std::array<Span, 12> spans{};
};

}  // namespace yewukv::raft
