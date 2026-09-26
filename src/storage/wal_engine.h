#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <mutex>

#include "storage/engine.h"
#include "storage/mem_engine.h"

namespace yewukv::storage {

// Append-only, checksummed WAL in a directory exclusively owned by this
// engine instance. Successful Put/Delete return only after fdatasync succeeds.
// The directory must remain on a filesystem that honors fdatasync/fsync.
class WalEngine final : public KVEngine {
 public:
  static constexpr uint64_t kMaxWalBytes = 1024ULL * 1024 * 1024;
  static constexpr uint32_t kMaxKeyBytes = 1024 * 1024;
  static constexpr uint32_t kMaxValueBytes = 15 * 1024 * 1024;

  explicit WalEngine(std::string data_directory, uint64_t wal_limit_bytes = kMaxWalBytes);
  ~WalEngine() override;
  WalEngine(const WalEngine&) = delete;
  WalEngine& operator=(const WalEngine&) = delete;

  Status Put(std::string_view key, std::string_view value) override;
  Status Get(std::string_view key, std::string* value) override;
  Status Delete(std::string_view key) override;

  uint64_t LastSequence() const;
  uint64_t WalBytes() const;
  Status Compact();

 private:
  Status Append(uint8_t operation, std::string_view key, std::string_view value);
  Status ApplyCommitted(uint8_t operation, std::string_view key, std::string_view value);
  Status CheckUsable() const;
  Status CompactLocked(std::string_view deleted_key = {});
  void Recover();
  void CloseResources();

  int directory_fd_ = -1;
  int lock_fd_ = -1;
  int wal_fd_ = -1;
  std::string data_directory_;
  uint64_t wal_limit_bytes_;
  MemEngine mem_;
  mutable std::mutex mutex_;
  uint64_t sequence_ = 0;
  uint64_t file_bytes_ = 0;
  bool failed_ = false;
  std::string failure_message_;
};

}  // namespace yewukv::storage
