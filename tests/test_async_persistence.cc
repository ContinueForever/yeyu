#include <gtest/gtest.h>

#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <future>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <thread>
#include <unistd.h>

#include "raft/node.h"
#include "raft/persistence_worker.h"

namespace {
using namespace yewukv;
using namespace yewukv::raft;
using namespace std::chrono_literals;

Node::Config Config() {
  Node::Config config;
  config.id = 1;
  config.voters = {1, 2, 3};
  return config;
}

struct MemoryStore : HardStateStore, LogStore {
  DurableState state;
  Status Save(const HardState& hard) override {
    state.hard_state = hard;
    return Status::OK();
  }
  Status ReplaceFrom(uint64_t first, const std::vector<LogEntry>& suffix) override {
    state.entries.resize(first - state.snapshot.index - 1);
    state.entries.insert(state.entries.end(), suffix.begin(), suffix.end());
    return Status::OK();
  }
  Status SaveSnapshot(const Snapshot& snapshot, const std::vector<LogEntry>& suffix) override {
    state.snapshot = snapshot;
    state.entries = suffix;
    return Status::OK();
  }
};

Message Request(MessageKind kind, uint64_t term = 1, uint64_t from = 2) {
  Message message;
  message.kind = kind;
  message.from = from;
  message.to = 1;
  message.term = term;
  message.sequence = 71;
  return message;
}

struct Owner {
  MemoryStore store;
  PersistenceQueue queue;
  Node node;
  explicit Owner(size_t bytes = 64 * 1024 * 1024)
      : queue({1, bytes}), node(Config(), {}, store, store, &queue) {}
  void Persist() {
    auto work = queue.TakeNext();
    ASSERT_TRUE(work);
    auto status = ExecutePersistence(*work->mutation, store, store);
    ASSERT_TRUE(status.ok()) << status.ToString();
    ASSERT_TRUE(queue.Complete(work->ticket, std::move(status)).ok());
    node.AdvancePersistence();
  }
  void Reply(const Message& request) {
    Message reply = Request(MessageKind::kAppendReply, request.term, request.to);
    reply.sequence = request.sequence;
    reply.success = true;
    reply.match_index = request.append.entries.empty() ? request.append.prev_index
                                                       : request.append.entries.back().index;
    reply.read_context = request.read_context;
    ASSERT_TRUE(node.ReceiveAsync(reply, [](auto) {}).ok());
  }
  void DrainReplication() {
    for (size_t round = 0; round < 8; ++round) {
      auto messages = node.TakeMessages();
      if (messages.empty()) return;
      for (const auto& request : messages) {
        ASSERT_EQ(request.kind, MessageKind::kAppend);
        Reply(request);
      }
    }
    FAIL() << "replication did not become idle";
  }
  void Lead() {
    ASSERT_TRUE(node.Campaign().ok());
    Persist();
    ASSERT_EQ(node.TakeMessages().size(), 2u);
    auto vote = Request(MessageKind::kVoteReply);
    vote.granted = true;
    ASSERT_TRUE(node.ReceiveAsync(vote, [](auto) {}).ok());
    ASSERT_TRUE(node.IsLeader());
    ASSERT_TRUE(node.PersistencePending());
    EXPECT_TRUE(node.TakeMessages().empty());
    Persist();
    DrainReplication();
    ASSERT_EQ(node.CommitIndex(), 1u);
    node.TakeCommitted();
  }
};

TEST(AsyncPersistence, CampaignPublishesNothingBeforeMatchingDurableCompletion) {
  Owner owner;
  ASSERT_TRUE(owner.node.Campaign().ok());
  EXPECT_EQ(owner.node.term(), 0u);
  EXPECT_EQ(owner.node.role(), Role::kFollower);
  EXPECT_TRUE(owner.node.TakeMessages().empty());
  auto work = owner.queue.TakeNext();
  ASSERT_TRUE(work);
  EXPECT_EQ(std::get<HardState>(*work->mutation).voted_for, 1u);
  auto wrong = work->ticket;
  ++wrong.stream;
  EXPECT_FALSE(owner.queue.Complete(wrong, Status::OK()).ok());
  owner.node.AdvancePersistence();
  EXPECT_EQ(owner.node.term(), 0u);
  ASSERT_TRUE(ExecutePersistence(*work->mutation, owner.store, owner.store).ok());
  ASSERT_TRUE(owner.queue.Complete(work->ticket, Status::OK()).ok());
  EXPECT_EQ(owner.node.term(), 0u);  // Worker completion alone cannot mutate the core.
  owner.node.AdvancePersistence();
  EXPECT_EQ(owner.node.term(), 1u);
  EXPECT_EQ(owner.node.role(), Role::kCandidate);
  EXPECT_EQ(owner.node.TakeMessages().size(), 2u);
  EXPECT_FALSE(owner.queue.Complete(work->ticket, Status::OK()).ok());
}

TEST(AsyncPersistence, VoteFailureNeverGrantsAndPoisonsTheNode) {
  Owner owner;
  size_t callbacks = 0;
  std::optional<Message> reply;
  ASSERT_TRUE(owner.node
                  .ReceiveAsync(Request(MessageKind::kVote),
                                [&](auto result) {
                                  ++callbacks;
                                  reply = std::move(result);
                                })
                  .ok());
  EXPECT_EQ(callbacks, 0u);
  auto work = owner.queue.TakeNext();
  ASSERT_TRUE(work);
  // Dispatched Busy is an uncertain disk failure, not an admission rejection.
  ASSERT_TRUE(owner.queue.Complete(work->ticket, Status::Busy("uncertain write")).ok());
  owner.node.AdvancePersistence();
  EXPECT_EQ(callbacks, 1u);
  EXPECT_FALSE(reply);
  EXPECT_TRUE(owner.node.failed());
  EXPECT_TRUE(owner.node.TakeMessages().empty());
  EXPECT_FALSE(owner.node.Campaign().ok());
}

TEST(AsyncPersistence, FollowerAppendRequiresHardStateThenLogBarrier) {
  Owner owner;
  auto message = Request(MessageKind::kAppend, 3);
  message.append = {0, 0, 1, {{1, 3, "put"}}};
  size_t callbacks = 0;
  std::optional<Message> reply;
  ASSERT_TRUE(owner.node
                  .ReceiveAsync(message,
                                [&](auto result) {
                                  ++callbacks;
                                  reply = result;
                                })
                  .ok());
  EXPECT_EQ(callbacks, 0u);
  EXPECT_EQ(owner.node.term(), 0u);
  owner.Persist();
  EXPECT_EQ(owner.node.term(), 3u);
  EXPECT_EQ(owner.node.LastIndex(), 0u);
  EXPECT_EQ(callbacks, 0u);
  EXPECT_TRUE(owner.node.TakeCommitted().empty());
  auto work = owner.queue.TakeNext();
  ASSERT_TRUE(work);
  ASSERT_TRUE(std::holds_alternative<SuffixWrite>(*work->mutation));
  ASSERT_TRUE(ExecutePersistence(*work->mutation, owner.store, owner.store).ok());
  ASSERT_TRUE(owner.queue.Complete(work->ticket, Status::OK()).ok());
  EXPECT_EQ(callbacks, 0u);
  EXPECT_EQ(owner.node.LastIndex(), 0u);
  owner.node.AdvancePersistence();
  ASSERT_EQ(callbacks, 1u);
  ASSERT_TRUE(reply);
  EXPECT_TRUE(reply->success);
  EXPECT_EQ(reply->match_index, 1u);
  EXPECT_EQ(owner.node.TakeCommitted().size(), 1u);
}

TEST(AsyncPersistence, HigherObservedTermFencesOldVoteAndIsPersistedWithoutRetry) {
  Owner owner;
  std::optional<Message> reply;
  bool called = false;
  ASSERT_TRUE(owner.node
                  .ReceiveAsync(Request(MessageKind::kVote, 2),
                                [&](auto result) {
                                  called = true;
                                  reply = result;
                                })
                  .ok());
  const auto rejected = owner.node.ReceiveAsync(Request(MessageKind::kVoteReply, 7, 3),
                                                [](auto) { ADD_FAILURE() << "not admitted"; });
  ASSERT_EQ(rejected.code(), StatusCode::kBusy);
  owner.Persist();
  EXPECT_TRUE(called);
  EXPECT_FALSE(reply);
  EXPECT_EQ(owner.node.term(), 2u);
  EXPECT_EQ(owner.node.role(), Role::kFollower);
  EXPECT_TRUE(owner.node.PersistencePending());
  EXPECT_TRUE(owner.node.TakeMessages().empty());
  owner.Persist();
  EXPECT_EQ(owner.node.term(), 7u);
  EXPECT_EQ(owner.store.state.hard_state.voted_for, 0u);
  EXPECT_FALSE(owner.node.PersistencePending());
}

TEST(AsyncPersistence, HigherTermDuringAppendKeepsCommittedApplyButSuppressesOldReply) {
  Owner owner;
  auto append = Request(MessageKind::kAppend, 2);
  append.append = {0, 0, 1, {{1, 2, "committed"}}};
  bool called = false;
  std::optional<Message> reply;
  ASSERT_TRUE(owner.node
                  .ReceiveAsync(append,
                                [&](auto result) {
                                  called = true;
                                  reply = result;
                                })
                  .ok());
  owner.Persist();  // Hard state; log still pending.
  EXPECT_EQ(owner.node.ReceiveAsync(Request(MessageKind::kVoteReply, 3, 3), [](auto) {}).code(),
            StatusCode::kBusy);
  owner.Persist();
  EXPECT_TRUE(called);
  EXPECT_FALSE(reply);
  const auto committed = owner.node.TakeCommitted();
  ASSERT_EQ(committed.size(), 1u);
  EXPECT_EQ(committed.front().command, "committed");
  EXPECT_EQ(owner.node.CommitIndex(), 1u);
  owner.Persist();
  EXPECT_EQ(owner.node.term(), 3u);
}

TEST(AsyncPersistence, PendingProposalKeepsDurableHeartbeatAndFreshReadQuorumUsable) {
  Owner owner;
  owner.Lead();
  const auto proposed = owner.node.Propose("new");
  ASSERT_TRUE(proposed.ok());
  EXPECT_EQ(proposed.value(), 2u);
  EXPECT_EQ(owner.node.LastIndex(), 1u);
  EXPECT_EQ(owner.node.LastAdmittedIndex(), 2u);
  EXPECT_EQ(owner.node.Propose("later").status().code(), StatusCode::kBusy);
  const auto context = owner.node.ReadIndex();
  ASSERT_TRUE(context.ok());
  auto messages = owner.node.TakeMessages();
  ASSERT_FALSE(messages.empty());
  for (const auto& message : messages) {
    EXPECT_TRUE(message.append.entries.empty());
    EXPECT_EQ(message.append.prev_index, 1u);
    owner.Reply(message);
  }
  EXPECT_EQ(owner.node.CommitIndex(), 1u);
  auto reads = owner.node.TakeReadStates();
  ASSERT_EQ(reads.size(), 1u);
  EXPECT_EQ(reads.front().index, 1u);
  // The server must additionally gate this read on LastAdmittedIndex == 2.
  owner.Persist();
  EXPECT_EQ(owner.node.LastIndex(), 2u);
  owner.DrainReplication();
  EXPECT_EQ(owner.node.CommitIndex(), 2u);
  EXPECT_EQ(owner.node.TakeCommitted().front().command, "new");
}

TEST(AsyncPersistence, HigherTermDuringLocalAppendNeverRestoresLeadership) {
  Owner owner;
  owner.Lead();
  ASSERT_TRUE(owner.node.Propose("uncertain").ok());
  const auto status = owner.node.ReceiveAsync(Request(MessageKind::kVoteReply, 9), [](auto) {});
  ASSERT_EQ(status.code(), StatusCode::kBusy);
  EXPECT_FALSE(owner.node.IsLeader());
  EXPECT_FALSE(owner.node.ReadIndex().ok());
  EXPECT_TRUE(owner.node.TakeMessages().empty());
  owner.Persist();
  EXPECT_EQ(owner.node.LastIndex(), 2u);
  EXPECT_FALSE(owner.node.IsLeader());
  EXPECT_TRUE(owner.node.TakeMessages().empty());
  EXPECT_TRUE(owner.node.TakeCommitted().empty());
  owner.Persist();
  EXPECT_EQ(owner.node.term(), 9u);
  EXPECT_FALSE(owner.node.IsLeader());
}

TEST(AsyncPersistence, SameTermLeaderMessageAlsoFencesPendingLocalAppend) {
  Owner owner;
  owner.Lead();
  ASSERT_TRUE(owner.node.Propose("uncertain").ok());
  auto heartbeat = Request(MessageKind::kAppend);
  heartbeat.append = {1, 1, 1, {}};
  ASSERT_EQ(owner.node.ReceiveAsync(heartbeat, [](auto) {}).code(), StatusCode::kBusy);
  EXPECT_FALSE(owner.node.IsLeader());
  owner.Persist();
  EXPECT_FALSE(owner.node.IsLeader());
  EXPECT_TRUE(owner.node.TakeMessages().empty());
}

TEST(AsyncPersistence, AdmissionRejectionDoesNotPoisonOrReserveAnIndex) {
  Owner owner(96);
  owner.Lead();  // Fits a no-op, cannot fit a nonempty suffix.
  const auto proposed = owner.node.Propose("does-not-fit");
  EXPECT_EQ(proposed.status().code(), StatusCode::kBusy);
  EXPECT_FALSE(owner.node.failed());
  EXPECT_TRUE(owner.node.IsLeader());
  EXPECT_EQ(owner.node.LastIndex(), 1u);
  EXPECT_EQ(owner.node.LastAdmittedIndex(), 1u);
  EXPECT_FALSE(owner.node.PersistencePending());
  EXPECT_EQ(owner.queue.outstanding(), 0u);
  EXPECT_TRUE(owner.node.TakeMessages().empty());
}

TEST(AsyncPersistence, SnapshotCapturesCurrentSuffixAndBlocksLaterMutationsUntilDurable) {
  Owner owner;
  owner.Lead();
  ASSERT_TRUE(owner.node.Propose("suffix-before-publication").ok());
  owner.Persist();
  owner.DrainReplication();
  owner.node.TakeCommitted();
  ASSERT_TRUE(owner.node.Compact(1, "encoded-earlier-at-index-1").ok());
  EXPECT_EQ(owner.node.snapshot().index, 0u);
  EXPECT_EQ(owner.node.Propose("must-wait").status().code(), StatusCode::kBusy);
  owner.Persist();
  EXPECT_EQ(owner.node.snapshot().index, 1u);
  ASSERT_EQ(owner.node.entries().size(), 1u);
  EXPECT_EQ(owner.node.entries().front().index, 2u);
  EXPECT_EQ(owner.node.entries().front().command, "suffix-before-publication");
  EXPECT_EQ(owner.store.state.entries.front().command, "suffix-before-publication");
}

TEST(AsyncPersistence, InstalledSnapshotWaitsForDiskAndSurvivesSupersedingTerm) {
  Owner owner;
  auto message = Request(MessageKind::kSnapshot, 2);
  message.snapshot = {10, 2, 0, 5, "state", true};
  bool called = false;
  std::optional<Message> reply;
  ASSERT_TRUE(owner.node
                  .ReceiveAsync(message,
                                [&](auto result) {
                                  called = true;
                                  reply = result;
                                })
                  .ok());
  owner.Persist();
  EXPECT_FALSE(called);
  EXPECT_FALSE(owner.node.TakeInstalledSnapshot());
  EXPECT_EQ(owner.node.ReceiveAsync(Request(MessageKind::kVoteReply, 3), [](auto) {}).code(),
            StatusCode::kBusy);
  owner.Persist();
  EXPECT_TRUE(called);
  EXPECT_FALSE(reply);
  auto snapshot = owner.node.TakeInstalledSnapshot();
  ASSERT_TRUE(snapshot);
  EXPECT_EQ(snapshot->data, "state");
  EXPECT_EQ(owner.node.CommitIndex(), 10u);
  owner.Persist();
  EXPECT_EQ(owner.node.term(), 3u);
}

class Directory {
 public:
  Directory() {
    char path[] = "/tmp/yewukv-async-XXXXXX";
    auto result = ::mkdtemp(path);
    if (!result) throw std::runtime_error("mkdtemp");
    name = result;
  }
  ~Directory() { std::filesystem::remove_all(name); }
  std::string name;
};

TEST(AsyncPersistence, RealStoreFailureAfterJournalSyncNeverPublishesProtocolEffects) {
  Directory directory;
  bool armed = false;
  {
    FileStore::Options options;
    options.journal = true;
    options.persist_observer = [&](auto point) {
      if (armed && point == FileStore::PersistPoint::kAfterJournalSync)
        throw std::runtime_error("after durable bytes, before publication");
    };
    FileStore store(directory.name, options);
    PersistenceQueue queue;
    Node node(Config(), store.ReadState(), store, store, &queue);
    armed = true;
    ASSERT_TRUE(node.Campaign().ok());
    auto work = queue.TakeNext();
    ASSERT_TRUE(work);
    auto status = ExecutePersistence(*work->mutation, store, store);
    ASSERT_FALSE(status.ok());
    ASSERT_TRUE(queue.Complete(work->ticket, std::move(status)).ok());
    node.AdvancePersistence();
    EXPECT_TRUE(node.failed());
    EXPECT_EQ(node.term(), 0u);
    EXPECT_TRUE(node.TakeMessages().empty());
  }
  FileStore::Options options;
  options.journal = true;
  FileStore recovered(directory.name, options);
  EXPECT_EQ(recovered.ReadState().hard_state.term, 1u);
  EXPECT_EQ(recovered.ReadState().hard_state.voted_for, 1u);
}

TEST(AsyncPersistence, CompletedButUnadvancedDiskWriteRecoversInBothFormats) {
  for (bool journal : {false, true}) {
    Directory directory;
    FileStore::Options options;
    options.journal = journal;
    {
      FileStore store(directory.name, options);
      PersistenceQueue queue;
      Node node(Config(), store.ReadState(), store, store, &queue);
      ASSERT_TRUE(node.Campaign().ok());
      auto work = queue.TakeNext();
      ASSERT_TRUE(work);
      ASSERT_TRUE(ExecutePersistence(*work->mutation, store, store).ok());
      ASSERT_TRUE(queue.Complete(work->ticket, Status::OK()).ok());
      EXPECT_EQ(node.term(), 0u);
      EXPECT_TRUE(node.TakeMessages().empty());
      // Simulate losing the owner before AdvancePersistence.
    }
    FileStore recovered(directory.name, options);
    Node node(Config(), recovered.ReadState(), recovered, recovered);
    EXPECT_EQ(node.term(), 1u);
    EXPECT_FALSE(node.IsLeader());
  }
}

TEST(AsyncPersistence, SnapshotPublicationRecoversTheLatestSuffixInBothFormats) {
  for (bool journal : {false, true}) {
    Directory directory;
    FileStore::Options options;
    options.journal = journal;
    {
      FileStore store(directory.name, options);
      ASSERT_TRUE(store.Save({1, 0}).ok());
      ASSERT_TRUE(store.ReplaceFrom(1, {{1, 1, ""}, {2, 1, "earlier"}}).ok());
      PersistenceQueue queue;
      Node node(Config(), store.ReadState(), store, store, &queue);
      auto complete = [&] {
        auto work = queue.TakeNext();
        ASSERT_TRUE(work);
        ASSERT_TRUE(ExecutePersistence(*work->mutation, store, store).ok());
        ASSERT_TRUE(queue.Complete(work->ticket, Status::OK()).ok());
        node.AdvancePersistence();
      };
      // The encoder captured index 1, then index 3 became durable and committed.
      auto append = Request(MessageKind::kAppend);
      append.append = {2, 1, 3, {{3, 1, "newer-than-encoding"}}};
      bool replied = false;
      ASSERT_TRUE(
          node.ReceiveAsync(append, [&](auto reply) { replied = reply && reply->success; }).ok());
      complete();
      ASSERT_TRUE(replied);
      ASSERT_EQ(node.CommitIndex(), 3u);
      ASSERT_TRUE(node.Compact(1, "encoded-at-1").ok());
      EXPECT_EQ(node.snapshot().index, 0u);
      complete();
      EXPECT_EQ(node.snapshot().index, 1u);
      EXPECT_EQ(node.entries().back().command, "newer-than-encoding");
    }
    FileStore reopened(directory.name, options);
    auto state = reopened.ReadState();
    EXPECT_EQ(state.snapshot.index, 1u);
    EXPECT_EQ(state.snapshot.data, "encoded-at-1");
    ASSERT_EQ(state.entries.size(), 2u);
    EXPECT_EQ(state.entries[0].index, 2u);
    EXPECT_EQ(state.entries[1].index, 3u);
    EXPECT_EQ(state.entries[1].command, "newer-than-encoding");
  }
}

TEST(AsyncPersistence, UnstartedWorkerShutdownAlsoDrainsAcceptedInput) {
  Directory directory;
  FileStore store(directory.name);
  PersistenceQueue queue;
  ASSERT_TRUE(queue.Submit(HardState{4, 2}).ok());
  PersistenceWorker worker(queue, store, [] {});
  worker.Stop();
  EXPECT_EQ(store.ReadState().hard_state.term, 4u);
  auto completed = queue.TakeCompletion();
  ASSERT_TRUE(completed);
  EXPECT_TRUE(completed->dispatched);
  EXPECT_TRUE(completed->status.ok());
}

struct Gate {
  std::mutex mutex;
  std::condition_variable changed;
  bool armed = false, entered = false, released = false;
  void Observe() {
    std::unique_lock<std::mutex> lock(mutex);
    if (!armed) return;
    entered = true;
    changed.notify_all();
    changed.wait(lock, [&] { return released; });
  }
  bool Wait() {
    std::unique_lock<std::mutex> lock(mutex);
    return changed.wait_for(lock, 2s, [&] { return entered; });
  }
  void Release() {
    std::lock_guard<std::mutex> lock(mutex);
    released = true;
    changed.notify_all();
  }
};
struct ReleaseOnExit {
  Gate& gate;
  ~ReleaseOnExit() { gate.Release(); }
};

TEST(AsyncPersistence, WorkerNeverHoldsMetricsCacheDuringIoAndShutdownDrains) {
  Directory directory;
  Gate gate;
  std::vector<PersistenceTrace> traces;
  FileStore::Options options;
  options.journal = true;
  options.persist_observer = [&](auto point) {
    if (point == FileStore::PersistPoint::kBeforeJournalSync) gate.Observe();
  };
  options.trace_observer = [&](const auto& trace) { traces.push_back(trace); };
  FileStore store(directory.name, options);
  PersistenceQueue queue;
  Node node(Config(), store.ReadState(), store, store, &queue);
  PersistenceWorker worker(queue, store, [] {});
  // Declared after the worker so every failed ASSERT releases the disk first.
  ReleaseOnExit release{gate};
  gate.armed = true;
  ASSERT_TRUE(node.Campaign().ok());
  const auto ticket = node.PendingTicket();
  ASSERT_TRUE(ticket);
  worker.Start();
  ASSERT_TRUE(gate.Wait());
  const auto started = std::chrono::steady_clock::now();
  const auto metrics = worker.ReadMetrics();
  EXPECT_LT(std::chrono::steady_clock::now() - started, 200ms);
  EXPECT_EQ(metrics.journal_saves, 0u);
  EXPECT_EQ(node.term(), 0u);
  EXPECT_TRUE(node.TakeMessages().empty());
  gate.Release();
  worker.Stop();  // Accepted work is durable even if the owner has not advanced.
  EXPECT_EQ(node.term(), 0u);
  EXPECT_FALSE(queue.Submit(HardState{2, 0}).ok());
  node.AdvancePersistence();
  EXPECT_EQ(node.term(), 1u);
  bool correlated = false;
  for (const auto& trace : traces)
    if (trace.task_stream == ticket->stream && trace.task_sequence == ticket->sequence)
      correlated = true;
  EXPECT_TRUE(correlated);
  EXPECT_EQ(worker.ReadMetrics().journal_saves, 1u);
}

}  // namespace
