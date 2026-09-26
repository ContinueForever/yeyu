#pragma once
// Lightweight Result/Status type used across all layers.
#include <string>
#include <utility>

namespace yewukv {

enum class StatusCode : int {
  kOk = 0,
  kNotFound = 1,
  kCorruption = 2,
  kNotSupported = 3,
  kInvalidArgument = 4,
  kIOError = 5,
  kBusy = 6,  // try again later (e.g. not leader)
  kShutdown = 7,
  kTimedOut = 8,
};

class Status {
 public:
  Status() = default;
  Status(StatusCode code, std::string msg) : code_(code), msg_(std::move(msg)) {}

  static Status OK() { return {}; }
  static Status NotFound(std::string m = "") { return {StatusCode::kNotFound, std::move(m)}; }
  static Status Corruption(std::string m) { return {StatusCode::kCorruption, std::move(m)}; }
  static Status NotSupported(std::string m) { return {StatusCode::kNotSupported, std::move(m)}; }
  static Status InvalidArgument(std::string m) {
    return {StatusCode::kInvalidArgument, std::move(m)};
  }
  static Status IOError(std::string m) { return {StatusCode::kIOError, std::move(m)}; }
  static Status Busy(std::string m = "") { return {StatusCode::kBusy, std::move(m)}; }
  static Status Shutdown(std::string m = "") { return {StatusCode::kShutdown, std::move(m)}; }
  static Status TimedOut(std::string m = "") { return {StatusCode::kTimedOut, std::move(m)}; }

  bool ok() const { return code_ == StatusCode::kOk; }
  StatusCode code() const { return code_; }
  const std::string& message() const { return msg_; }
  std::string ToString() const;

 private:
  StatusCode code_ = StatusCode::kOk;
  std::string msg_;
};

// Result<T> pairs a value with a Status.
template <typename T>
class Result {
 public:
  Result(T value) : status_(Status::OK()), value_(std::move(value)) {}
  Result(Status status) : status_(std::move(status)) {}

  bool ok() const { return status_.ok(); }
  const Status& status() const { return status_; }
  const T& value() const { return value_; }
  T& value() { return value_; }
  T&& TakeValue() { return std::move(value_); }

 private:
  Status status_;
  T value_{};
};

}  // namespace yewukv
