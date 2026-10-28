#include "raft/persistence_worker.h"

#include <chrono>
#include <stdexcept>
#include <utility>

namespace yewukv::raft {

PersistenceWorker::PersistenceWorker(PersistenceQueue& queue, FileStore& store,
                                     std::function<void()> wake_owner)
    : queue_(queue),
      store_(store),
      wake_owner_(std::move(wake_owner)),
      metrics_(store.ReadMetrics()) {}

PersistenceWorker::~PersistenceWorker() {
  Stop();
}

void PersistenceWorker::Start() {
  if (thread_.joinable() || stopping_) throw std::logic_error("persistence worker cannot restart");
  thread_ = std::thread([this] { Run(); });
}

void PersistenceWorker::Stop() {
  stopping_ = true;
  queue_.StopAccepting();
  if (thread_.joinable())
    thread_.join();
  else
    Run();  // Also drain input accepted before a failed/unstarted Start.
}

FileStore::Metrics PersistenceWorker::ReadMetrics() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return metrics_;
}

void PersistenceWorker::Run() {
  for (;;) {
    auto work = queue_.WaitNext(std::chrono::milliseconds(50));
    if (!work) {
      if (stopping_ || queue_.failed()) return;
      continue;
    }
    PersistenceTaskScope task(work->ticket.stream, work->ticket.sequence);
    auto result = ExecutePersistence(*work->mutation, store_, store_);
    const auto metrics = store_.ReadMetrics();
    {
      std::lock_guard<std::mutex> lock(mutex_);
      metrics_ = metrics;
    }
    // This worker owns the only dispatched ticket; Complete cannot allocate a
    // new slot and the owner is notified only after both disk result and cache.
    if (!queue_.Complete(work->ticket, std::move(result)).ok()) std::terminate();
    try {
      if (wake_owner_) wake_owner_();
    } catch (...) { /* diagnostics/wakeup must not lose completion */
    }
  }
}

}  // namespace yewukv::raft
