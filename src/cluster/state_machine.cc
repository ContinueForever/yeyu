#include "cluster/state_machine.h"

#include <stdexcept>
#include <utility>

namespace yewukv::cluster {
namespace {
size_t EntryBytes(const std::string& key, const std::string& value) {
  rpc::PutRequest entry;
  entry.set_key(key);
  entry.set_value(value);
  const size_t payload = entry.ByteSizeLong();
  size_t length_bytes = 1;
  for (size_t n = payload; n >= 128; n >>= 7) ++length_bytes;
  return 1 + length_bytes + payload;
}

bool Parse(const std::string& bytes, rpc::KvSnapshot* snapshot) {
  if (bytes.size() > raft::FileStore::kMaxSnapshotBytes || !snapshot->ParseFromString(bytes) ||
      snapshot->version() != 1)
    return false;
  std::string previous;
  for (const auto& entry : snapshot->entries()) {
    if (entry.key().empty() || entry.key().size() > 1024 || entry.value().size() > 8192 ||
        (!previous.empty() && entry.key() <= previous))
      return false;
    previous = entry.key();
  }
  return true;
}
}  // namespace

void StateMachine::Apply(const rpc::KvRequest& request, rpc::KvResponse* response) {
  if (request.has_put()) {
    const auto& put = request.put();
    auto old = values_.find(put.key());
    const auto previous = old == values_.end() ? 0 : EntryBytes(old->first, old->second);
    const auto replacement = EntryBytes(put.key(), put.value());
    if (request.state_machine_version() != 0 &&
        (encoded_bytes_ - previous > byte_limit_ ||
         replacement > byte_limit_ - (encoded_bytes_ - previous))) {
      response->set_code(rpc::KvResponse::BUSY);
      response->set_error("live KV snapshot capacity reached; delete keys or reduce values");
      return;
    }
    values_[put.key()] = put.value();
    encoded_bytes_ = encoded_bytes_ - previous + replacement;
  } else if (request.has_del()) {
    auto old = values_.find(request.del().key());
    if (old != values_.end()) {
      encoded_bytes_ -= EntryBytes(old->first, old->second);
      values_.erase(old);
    }
  } else if (request.has_get()) {
    auto found = values_.find(request.get().key());
    if (found == values_.end())
      response->set_code(rpc::KvResponse::NOT_FOUND);
    else
      response->set_value(found->second);
  }
}

std::string StateMachine::Serialize() const {
  rpc::KvSnapshot snapshot;
  snapshot.set_version(1);
  for (const auto& [key, value] : values_) {
    auto* entry = snapshot.add_entries();
    entry->set_key(key);
    entry->set_value(value);
  }
  std::string bytes;
  if (!snapshot.SerializeToString(&bytes) || bytes.size() != encoded_bytes_)
    throw std::runtime_error("KV snapshot size/encoding invariant failed");
  return bytes;
}

bool StateMachine::Restore(const std::string& bytes) {
  rpc::KvSnapshot snapshot;
  if (bytes.size() > byte_limit_ || !Parse(bytes, &snapshot)) return false;
  std::map<std::string, std::string> next;
  size_t encoded = 2;
  for (const auto& entry : snapshot.entries()) {
    encoded += EntryBytes(entry.key(), entry.value());
    next.emplace(entry.key(), entry.value());
  }
  values_ = std::move(next);
  encoded_bytes_ = encoded;
  return true;
}

bool StateMachine::ValidateSnapshot(const std::string& bytes) {
  rpc::KvSnapshot snapshot;
  return Parse(bytes, &snapshot);
}
}  // namespace yewukv::cluster
