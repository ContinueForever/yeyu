#include "storage/mem_engine.h"

namespace yewukv::storage {

Status MemEngine::Put(std::string_view key, std::string_view value) {
  std::lock_guard<std::mutex> lock(mutex_);
  data_[std::string(key)] = std::string(value);
  return Status::OK();
}

Status MemEngine::Get(std::string_view key, std::string* value) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = data_.find(std::string(key));
  if (it == data_.end()) return Status::NotFound(std::string(key));
  *value = it->second;
  return Status::OK();
}

Status MemEngine::Delete(std::string_view key) {
  std::lock_guard<std::mutex> lock(mutex_);
  data_.erase(std::string(key));
  return Status::OK();
}

size_t MemEngine::Size() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return data_.size();
}

std::vector<std::pair<std::string, std::string>> MemEngine::Snapshot() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return {data_.begin(), data_.end()};
}

}  // namespace yewukv::storage
