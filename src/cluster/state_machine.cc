#include "cluster/state_machine.h"

#include <stdexcept>
#include <utility>

namespace yewukv::cluster {
namespace {
size_t VarintBytes(uint64_t value) {
  size_t bytes = 1;
  while (value >= 128) {
    value >>= 7;
    ++bytes;
  }
  return bytes;
}

size_t EntryBytes(const std::string& key, const std::string& value, uint64_t revision) {
  rpc::SnapshotEntry entry;
  entry.set_key(key);
  entry.set_value(value);
  entry.set_revision(revision);
  const size_t payload = entry.ByteSizeLong();
  return 1 + VarintBytes(payload) + payload;
}

size_t HeaderBytes(uint64_t revision) {
  return 2 + (revision ? 1 + VarintBytes(revision) : 0);
}

bool Parse(const std::string& bytes, rpc::KvSnapshot* snapshot) {
  if (bytes.size() > raft::FileStore::kMaxSnapshotBytes || !snapshot->ParseFromString(bytes) ||
      (snapshot->version() != 1 && snapshot->version() != 2) ||
      (snapshot->version() == 1 && snapshot->revision() != 0) ||
      (snapshot->version() == 2 && snapshot->revision() == 0))
    return false;
  std::string previous;
  for (const auto& entry : snapshot->entries()) {
    if (entry.key().empty() || entry.key().size() > 1024 || entry.value().size() > 8192 ||
        (!previous.empty() && entry.key() <= previous) ||
        (snapshot->version() == 1 && entry.revision() != 0) ||
        (snapshot->version() == 2 &&
         (entry.revision() == 0 || entry.revision() > snapshot->revision())))
      return false;
    previous = entry.key();
  }
  return true;
}
}  // namespace

void StateMachine::Apply(const rpc::KvRequest& request, rpc::KvResponse* response,
                         uint64_t applied_index) {
  const bool mutation = request.has_put() || request.has_del() || request.has_cas();
  if (mutation && request.state_machine_version() == 2 && !revisioned_) {
    if (!applied_index) throw std::runtime_error("revision migration requires committed index");
    // Every replica migrates at the same committed position, independent of
    // its legacy v1 snapshot boundary. A first legacy CAS can conflict safely.
    revisioned_ = true;
    revision_ = applied_index;
    encoded_bytes_ = HeaderBytes(revision_);
    for (auto& [key, value] : values_) {
      value.revision = applied_index;
      encoded_bytes_ += EntryBytes(key, *value.bytes, value.revision);
    }
  }
  if (mutation && revisioned_ && (!applied_index || applied_index < revision_))
    throw std::runtime_error("nonmonotonic committed mutation index");
  const uint64_t next_revision = revisioned_ ? applied_index : 0;
  if (request.has_put() || request.has_cas()) {
    const auto& key = request.has_put() ? request.put().key() : request.cas().key();
    const auto& bytes = request.has_put() ? request.put().value() : request.cas().value();
    auto old = values_.find(key);
    if (request.has_cas() &&
        ((old == values_.end() && request.cas().expected_revision() != 0) ||
         (old != values_.end() && (request.cas().expected_revision() == 0 ||
                                   request.cas().expected_revision() != old->second.revision)))) {
      response->set_code(rpc::KvResponse::CONFLICT);
      response->set_error("revision conflict; read the current value before retrying");
      if (old != values_.end()) {
        response->set_value(*old->second.bytes);
        response->set_revision(old->second.revision);
      }
      return;
    }
    const auto previous =
        old == values_.end() ? 0 : EntryBytes(old->first, *old->second.bytes, old->second.revision);
    const auto replacement = EntryBytes(key, bytes, next_revision);
    const auto base =
        encoded_bytes_ - previous - HeaderBytes(revision_) + HeaderBytes(next_revision);
    if (request.state_machine_version() != 0 &&
        (base > byte_limit_ || replacement > byte_limit_ - base)) {
      response->set_code(rpc::KvResponse::BUSY);
      response->set_error("live KV snapshot capacity reached; delete keys or reduce values");
      return;
    }
    values_[key] = {std::make_shared<const std::string>(bytes), next_revision};
    encoded_bytes_ = base + replacement;
    revision_ = next_revision;
    response->set_revision(next_revision);
  } else if (request.has_del()) {
    auto old = values_.find(request.del().key());
    if (old != values_.end()) {
      encoded_bytes_ -= EntryBytes(old->first, *old->second.bytes, old->second.revision);
      values_.erase(old);
    }
    encoded_bytes_ = encoded_bytes_ - HeaderBytes(revision_) + HeaderBytes(next_revision);
    revision_ = next_revision;
    response->set_revision(next_revision);
  } else if (request.has_get()) {
    auto found = values_.find(request.get().key());
    if (found == values_.end())
      response->set_code(rpc::KvResponse::NOT_FOUND);
    else {
      response->set_value(*found->second.bytes);
      response->set_revision(found->second.revision);
    }
  }
}

StateMachine::Frozen StateMachine::Freeze() const {
  Frozen frozen;
  frozen.encoded_bytes = encoded_bytes_;
  frozen.revision = revision_;
  frozen.revisioned = revisioned_;
  frozen.entries.reserve(values_.size());
  for (const auto& entry : values_) frozen.entries.push_back(entry);
  return frozen;
}

std::string StateMachine::Frozen::Serialize() const {
  rpc::KvSnapshot snapshot;
  snapshot.set_version(revisioned ? 2 : 1);
  snapshot.set_revision(revision);
  for (const auto& [key, value] : entries) {
    auto* entry = snapshot.add_entries();
    entry->set_key(key);
    entry->set_value(*value.bytes);
    entry->set_revision(value.revision);
  }
  std::string bytes;
  if (!snapshot.SerializeToString(&bytes) || bytes.size() != encoded_bytes)
    throw std::runtime_error("KV snapshot size/encoding invariant failed");
  return bytes;
}

std::string StateMachine::Serialize() const {
  return Freeze().Serialize();
}

bool StateMachine::Restore(const std::string& bytes) {
  rpc::KvSnapshot snapshot;
  if (bytes.size() > byte_limit_ || !Parse(bytes, &snapshot)) return false;
  std::map<std::string, Value> next;
  size_t encoded = HeaderBytes(snapshot.revision());
  for (const auto& entry : snapshot.entries()) {
    encoded += EntryBytes(entry.key(), entry.value(), entry.revision());
    next.emplace(entry.key(),
                 Value{std::make_shared<const std::string>(entry.value()), entry.revision()});
  }
  values_ = std::move(next);
  encoded_bytes_ = encoded;
  revision_ = snapshot.revision();
  revisioned_ = snapshot.version() == 2;
  return true;
}

bool StateMachine::ValidateSnapshot(const std::string& bytes) {
  rpc::KvSnapshot snapshot;
  return Parse(bytes, &snapshot);
}
}  // namespace yewukv::cluster
