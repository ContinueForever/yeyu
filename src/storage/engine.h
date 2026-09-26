#pragma once
#include <optional>
#include <string>
#include <string_view>

#include "common/status.h"

namespace yewukv::storage {

// Ordered key/value engine abstraction.
// The network services execute calls through one bounded storage worker. The
// WalEngine uses this interface and currently applies recovered values to an
// in-memory map; future LSM storage can replace that map.
class KVEngine {
 public:
  virtual ~KVEngine() = default;
  virtual Status Put(std::string_view key, std::string_view value) = 0;
  virtual Status Get(std::string_view key, std::string* value) = 0;
  virtual Status Delete(std::string_view key) = 0;
};

}  // namespace yewukv::storage
