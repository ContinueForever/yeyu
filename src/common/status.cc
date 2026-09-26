#include "common/status.h"

namespace yewukv {

std::string Status::ToString() const {
  static const char* names[] = {"OK",           "NotFound",        "Corruption",
                                "NotSupported", "InvalidArgument", "IOError",
                                "Busy",         "Shutdown",        "TimedOut"};
  std::string s = names[static_cast<int>(code_)];
  if (!msg_.empty()) {
    s += ": ";
    s += msg_;
  }
  return s;
}

}  // namespace yewukv
