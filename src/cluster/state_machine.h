#pragma once

#include <map>
#include <string>

#include "kv.pb.h"
#include "raft/file_store.h"

namespace yewukv::cluster {

// All replicas use the same deterministic live-state limit. An over-capacity
// PUT is a committed no-op returning BUSY, so DELETE can still reclaim space.
class StateMachine {
 public:
  explicit StateMachine(size_t byte_limit = raft::FileStore::kMaxSnapshotBytes)
      : byte_limit_(byte_limit) {}
  void Apply(const rpc::KvRequest& request, rpc::KvResponse* response);
  std::string Serialize() const;
  bool Restore(const std::string& bytes);
  static bool ValidateSnapshot(const std::string& bytes);
  size_t size() const { return values_.size(); }
  size_t encoded_bytes() const { return encoded_bytes_; }

 private:
  const size_t byte_limit_;   // A smaller limit is used only by isolated unit tests.
  size_t encoded_bytes_ = 2;  // KvSnapshot version=1, no entries.
  std::map<std::string, std::string> values_;
};

}  // namespace yewukv::cluster
