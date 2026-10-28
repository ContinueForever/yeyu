#pragma once
// Growable byte buffer with a read offset, supporting partial reads/writes
// required by non-blocking IO (LT mode for now; ET mode needs read-until-EAGAIN).
#include <algorithm>
#include <cerrno>
#include <cstddef>
#include <cstring>
#include <limits>
#include <string>
#include <string_view>
#include <sys/uio.h>
#include <unistd.h>
#include <vector>

namespace yewukv::net {

class Buffer {
 public:
  static constexpr size_t kCheapPrepend = 8;
  static constexpr size_t kInitialSize = 1024;

  explicit Buffer(size_t initial = kInitialSize)
      : buf_(kCheapPrepend + initial), ridx_(kCheapPrepend), widx_(kCheapPrepend) {}

  size_t ReadableBytes() const { return widx_ - ridx_; }
  size_t WritableBytes() const { return buf_.size() - widx_; }

  const char* Peek() const { return data() + ridx_; }

  void Consume(size_t n) {
    if (n >= ReadableBytes()) {
      Reset();
    } else {
      ridx_ += n;
    }
  }

  void Reset() { ridx_ = widx_ = kCheapPrepend; }

  std::string Take(size_t n) {
    n = std::min(n, ReadableBytes());
    std::string s(Peek(), n);
    Consume(n);
    return s;
  }

  std::string TakeAll() { return Take(ReadableBytes()); }

  void Append(std::string_view s) { Append(s.data(), s.size()); }

  void Append(const char* p, size_t n) {
    EnsureWritable(n);
    std::memcpy(BeginWrite(), p, n);
    HasWritten(n);
  }

  // Read at most max_bytes; return bytes read, 0 on peer close, -1 on error.
  // A zero budget reports EAGAIN rather than a false peer EOF.
  ssize_t ReadFd(int fd, int* saved_errno, size_t max_bytes = std::numeric_limits<size_t>::max()) {
    if (max_bytes == 0) {
      *saved_errno = EAGAIN;
      return -1;
    }
    char extra[65536];
    iovec vec[2];
    size_t avail = std::min(WritableBytes(), max_bytes);
    vec[0].iov_base = BeginWrite();
    vec[0].iov_len = avail;
    vec[1].iov_base = extra;
    vec[1].iov_len = std::min(sizeof(extra), max_bytes - avail);
    ssize_t n = ::readv(fd, vec, avail < sizeof(extra) && vec[1].iov_len > 0 ? 2 : 1);
    if (n < 0) {
      *saved_errno = errno;
    } else if (static_cast<size_t>(n) <= avail) {
      HasWritten(static_cast<size_t>(n));
    } else {
      HasWritten(avail);
      Append(extra, static_cast<size_t>(n) - avail);
    }
    return n;
  }

  char* BeginWrite() { return data() + widx_; }
  void HasWritten(size_t n) { widx_ += n; }

 private:
  char* data() { return buf_.data(); }
  const char* data() const { return buf_.data(); }

  void EnsureWritable(size_t n) {
    if (WritableBytes() < n) MakeSpace(n);
  }

  void MakeSpace(size_t n) {
    if (WritableBytes() + (ridx_ - kCheapPrepend) < n) {
      buf_.resize(widx_ + n);
    } else {
      size_t used = ReadableBytes();
      std::memmove(data() + kCheapPrepend, Peek(), used);
      ridx_ = kCheapPrepend;
      widx_ = ridx_ + used;
    }
  }

  std::vector<char> buf_;
  size_t ridx_;
  size_t widx_;
};

}  // namespace yewukv::net
