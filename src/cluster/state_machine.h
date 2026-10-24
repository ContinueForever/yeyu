#pragma once

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "kv.pb.h"
#include "raft/file_store.h"

namespace yewukv::cluster {

// All replicas use the same deterministic live-state limit. An over-capacity
// PUT is a committed no-op returning BUSY, so DELETE can still reclaim space.
class StateMachine {
 public:
  explicit StateMachine(size_t byte_limit = raft::FileStore::kMaxSnapshotBytes)
      : byte_limit_(byte_limit) {}
  struct Value {
    std::shared_ptr<const std::string> bytes;
    uint64_t revision = 0;
  };
  struct Frozen {
    std::vector<std::pair<std::string, Value>> entries;
    size_t encoded_bytes = 2;
    uint64_t revision = 0;
    bool revisioned = false;
    std::string Serialize() const;
  };
  void Apply(const rpc::KvRequest& request, rpc::KvResponse* response, uint64_t applied_index = 0);
  Frozen Freeze() const;
  std::string Serialize() const;
  bool Restore(const std::string& bytes);
  static bool ValidateSnapshot(const std::string& bytes);
  size_t size() const { return values_.size(); }
  size_t encoded_bytes() const { return encoded_bytes_; }

 private:
  const size_t byte_limit_;   // A smaller limit is used only by isolated unit tests.
  size_t encoded_bytes_ = 2;  // KvSnapshot version=1, no entries.
  std::map<std::string, Value> values_;
  uint64_t revision_ = 0;
  bool revisioned_ = false;
};

}  // namespace yewukv::cluster
