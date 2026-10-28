#pragma once
#include <map>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "storage/engine.h"

namespace yewukv::storage {

// Thread-safe ordered in-memory map. Temporary stand-in for the LSM engine.
class MemEngine : public KVEngine {
 public:
  Status Put(std::string_view key, std::string_view value) override;
  Status Get(std::string_view key, std::string* value) override;
  Status Delete(std::string_view key) override;

  size_t Size() const;
  std::vector<std::pair<std::string, std::string>> Snapshot() const;

 private:
  mutable std::mutex mutex_;
  std::map<std::string, std::string> data_;
};

}  // namespace yewukv::storage
