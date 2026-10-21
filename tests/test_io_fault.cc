// Test-only interposer. It touches only writes/syncs of raft.state.tmp, and only
// while the per-process arm file exists. No production binary links this file.
#include <dlfcn.h>
#include <fcntl.h>
#include <unistd.h>

#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>

namespace {
bool Armed(int fd) {
  const char* arm = std::getenv("YEWUKV_TEST_IO_FAULT");
  if (!arm || ::access(arm, F_OK) != 0) return false;
  char path[64];
  char target[4096];
  std::snprintf(path, sizeof(path), "/proc/self/fd/%d", fd);
  const auto size = ::readlink(path, target, sizeof(target) - 1);
  if (size < 0) return false;
  target[size] = '\0';
  const char* suffix = "/raft.state.tmp";
  const size_t length = std::strlen(suffix);
  return static_cast<size_t>(size) >= length && std::strcmp(target + size - length, suffix) == 0;
}
}  // namespace

extern "C" ssize_t write(int fd, const void* data, size_t size) {
  using Write = ssize_t (*)(int, const void*, size_t);
  static const auto actual = reinterpret_cast<Write>(::dlsym(RTLD_NEXT, "write"));
  const char* mode = std::getenv("YEWUKV_TEST_IO_MODE");
  if (Armed(fd) && (!mode || std::strcmp(mode, "sync") != 0)) {
    errno = ENOSPC;
    return -1;
  }
  return actual(fd, data, size);
}

extern "C" int fdatasync(int fd) {
  using Sync = int (*)(int);
  static const auto actual = reinterpret_cast<Sync>(::dlsym(RTLD_NEXT, "fdatasync"));
  const char* mode = std::getenv("YEWUKV_TEST_IO_MODE");
  if (Armed(fd) && mode && std::strcmp(mode, "sync") == 0) {
    errno = EIO;
    return -1;
  }
  return actual(fd);
}
