#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>

namespace yewukv::cluster {

// The byte threshold remains an unconditional compaction trigger. Adaptive
// mode only delays the entry-count trigger until enough log bytes amortize a
// large live image. No commit, apply or durable-publication rule changes.
inline bool ShouldSnapshot(uint64_t applied, uint64_t snapshot, size_t retained_bytes,
                           size_t live_bytes, size_t entry_limit, size_t byte_limit,
                           bool adaptive) {
  if (applied <= snapshot) return false;
  if (retained_bytes >= byte_limit) return true;
  return applied - snapshot >= entry_limit &&
         (!adaptive || retained_bytes >= std::min(byte_limit, live_bytes / 4));
}

}  // namespace yewukv::cluster
