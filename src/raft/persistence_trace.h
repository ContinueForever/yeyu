#pragma once

#include <array>
#include <cstdint>

namespace yewukv::raft {

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
