#pragma once

#include <atomic>
#include <functional>
#include <mutex>
#include <thread>

#include "raft/file_store.h"
#include "raft/persistence_queue.h"

namespace yewukv::raft {

// One ordered disk thread. Start only after recovery/identity validation and
// stop after the owner has stopped submitting. Stop drains accepted work and
// joins before the FileStore can be destroyed; it never waits for its owner.
class PersistenceWorker {
 public:
  PersistenceWorker(PersistenceQueue& queue, FileStore& store, std::function<void()> wake_owner);
  ~PersistenceWorker();
  PersistenceWorker(const PersistenceWorker&) = delete;
  PersistenceWorker& operator=(const PersistenceWorker&) = delete;
  void Start();
  void Stop();
  // A completed-operation cache, never the FileStore's I/O mutex.
  FileStore::Metrics ReadMetrics() const;

 private:
  void Run();
  PersistenceQueue& queue_;
  FileStore& store_;
  std::function<void()> wake_owner_;
  std::atomic<bool> stopping_{false};
  std::thread thread_;
  mutable std::mutex mutex_;
  FileStore::Metrics metrics_;
};

}  // namespace yewukv::raft
