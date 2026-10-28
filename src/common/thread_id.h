#pragma once

#include <sys/syscall.h>
#include <unistd.h>

#include <cstdint>

namespace yewukv {

// Linux TID matches sched tracepoints. Cache once per thread, not per request.
inline uint64_t CurrentThreadId() noexcept {
  thread_local const uint64_t id = [] {
    const auto tid = ::syscall(SYS_gettid);
    return tid > 0 ? static_cast<uint64_t>(tid) : uint64_t{0};
  }();
  return id;
}

}  // namespace yewukv
