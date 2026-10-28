#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <future>
#include <iterator>
#include <map>
#include <set>
#include <stdexcept>
#include <string>

#include "raft/node.h"
#include "raft/persistence_queue.h"

namespace {
using namespace std::chrono_literals;
using yewukv::Status;
using yewukv::StatusCode;
using namespace yewukv::raft;

Node::Config Configuration() {
  Node::Config config;
  config.id = 1;
  config.voters = {1, 2, 3};
  return config;
}

struct Directory {
  Directory() {
    char pattern[] = "/tmp/yewukv-persistence-queue-XXXXXX";
    const auto* value = ::mkdtemp(pattern);
    if (!value) throw std::runtime_error("mkdtemp failed");
    path = value;
  }
  ~Directory() { std::filesystem::remove_all(path); }
  std::string path;
};

// Keep the currently dispatched token so a fatal test assertion can release
// the blocked owner before its std::future destructor joins the owner thread.
struct Driver {
  explicit Driver(PersistenceQueue& value) : queue(value) {}
  bool Next() {
    active = queue.WaitNext(2s);
    return active.has_value();
  }
  Status Execute(FileStore& store) { return ExecutePersistence(*active->mutation, store, store); }
  Status Complete(Status status) {
    const auto ticket = active->ticket;
    active.reset();
    return queue.Complete(ticket, std::move(status));
  }
  void Abort() {
    queue.StopAccepting();
    if (active) Complete(Status::IOError("test teardown"));
    while ((active = queue.TakeNext())) Complete(Status::IOError("test teardown"));
  }
  PersistenceQueue& queue;
  std::optional<PersistenceQueue::Work> active;
};
struct StopOnExit {
  Driver& driver;
  ~StopOnExit() { driver.Abort(); }
};

std::map<std::string, std::string> Files(const Directory& directory) {
  std::map<std::string, std::string> result;
  for (const auto& file : std::filesystem::directory_iterator(directory.path)) {
    if (!file.is_regular_file()) continue;
    std::ifstream stream(file.path(), std::ios::binary);
    result[file.path().filename().string()] = {std::istreambuf_iterator<char>(stream), {}};
  }
  return result;
}

TEST(PersistenceQueue, OwnsImmutableInputsAndDispatchesOneTaskInSubmissionOrder) {
  PersistenceQueue queue;
  auto first = queue.Submit(HardState{2, 1});
  PersistenceMutation input = SuffixWrite{1, {{1, 2, "original"}}};
  auto second = queue.Submit(input);
  ASSERT_TRUE(first.ok());
  ASSERT_TRUE(second.ok());
  std::get<SuffixWrite>(input).entries[0].command = "changed after admission";
  auto work = queue.TakeNext();
  ASSERT_TRUE(work);
  EXPECT_TRUE(work->ticket == first.value());
  EXPECT_FALSE(queue.TakeNext());
  EXPECT_FALSE(queue.TakeCompletion());
  EXPECT_EQ(queue.Complete(second.value(), Status::OK()).code(), StatusCode::kInvalidArgument);
  EXPECT_TRUE(queue.Complete(first.value(), Status::OK()).ok());
  work = queue.TakeNext();
  ASSERT_TRUE(work);
  EXPECT_TRUE(work->ticket == second.value());
  EXPECT_EQ(std::get<SuffixWrite>(*work->mutation).entries[0].command, "original");
  ASSERT_TRUE(queue.Complete(second.value(), Status::OK()).ok());
  for (const auto ticket : {first.value(), second.value()}) {
    auto completion = queue.TakeCompletion();
    ASSERT_TRUE(completion);
    EXPECT_TRUE(completion->ticket == ticket);
    EXPECT_TRUE(completion->status.ok());
    EXPECT_TRUE(completion->dispatched);
  }
  EXPECT_EQ(queue.outstanding(), 0);
  EXPECT_EQ(queue.payload_bytes(), 0);
}

TEST(PersistenceQueue, CapacityIncludesQueuedRunningAndUncollectedCompletions) {
  PersistenceQueue queue({1, 128});
  auto first = queue.Submit(HardState{1, 1});
  ASSERT_TRUE(first.ok());
  EXPECT_EQ(queue.Submit(HardState{2, 1}).status().code(), StatusCode::kBusy);
  ASSERT_TRUE(queue.TakeNext());
  EXPECT_EQ(queue.Submit(HardState{2, 1}).status().code(), StatusCode::kBusy);
  ASSERT_TRUE(queue.Complete(first.value(), Status::OK()).ok());
  EXPECT_EQ(queue.Submit(HardState{2, 1}).status().code(), StatusCode::kBusy);
  EXPECT_EQ(queue.payload_bytes(), 64);
  ASSERT_TRUE(queue.TakeCompletion());
  auto next = queue.Submit(HardState{2, 1});
  ASSERT_TRUE(next.ok());
  EXPECT_EQ(next.value().sequence, first.value().sequence + 1);
}

TEST(PersistenceQueue, PayloadBudgetRejectsBeforeCopyAndAccountsForSnapshotAndSuffix) {
  EXPECT_THROW(PersistenceQueue(PersistenceQueue::Options{0, 100}), std::invalid_argument);
  EXPECT_THROW(PersistenceQueue(PersistenceQueue::Options{1, 63}), std::invalid_argument);
  PersistenceQueue queue({4, 100});
  const PersistenceMutation suffix = SuffixWrite{1, {{1, 1, "four"}}};
  auto submitted = queue.Submit(suffix);  // 64 + 8 + 24 + 4.
  ASSERT_TRUE(submitted.ok());
  EXPECT_EQ(queue.payload_bytes(), 100);
  EXPECT_EQ(queue.Submit(HardState{1, 1}).status().code(), StatusCode::kBusy);
  ASSERT_TRUE(queue.TakeNext());
  ASSERT_TRUE(queue.Complete(submitted.value(), Status::OK()).ok());
  ASSERT_TRUE(queue.TakeCompletion());
  EXPECT_EQ(queue.Submit(SnapshotWrite{{1, 1, std::string(13, 'x')}, {}}).status().code(),
            StatusCode::kBusy);  // 64 + 24 + 13.
  EXPECT_EQ(queue.outstanding(), 0);
  EXPECT_EQ(queue.payload_bytes(), 0);
}

TEST(PersistenceQueue, RejectsForeignDuplicateAndOutOfOrderCompletionWithoutAdvancing) {
  PersistenceQueue queue;
  PersistenceQueue replacement;
  auto first = queue.Submit(HardState{1, 1});
  auto second = queue.Submit(HardState{2, 1});
  auto foreign = replacement.Submit(HardState{1, 1});
  ASSERT_TRUE(first.ok() && second.ok() && foreign.ok());
  EXPECT_EQ(first.value().sequence, foreign.value().sequence);
  EXPECT_NE(first.value().stream, foreign.value().stream);
  ASSERT_TRUE(queue.TakeNext());
  for (auto bad : {foreign.value(), second.value(), PersistenceQueue::Ticket{}}) {
    EXPECT_EQ(queue.Complete(bad, Status::OK()).code(), StatusCode::kInvalidArgument);
    EXPECT_FALSE(queue.TakeNext());
    EXPECT_FALSE(queue.TakeCompletion());
  }
  ASSERT_TRUE(queue.Complete(first.value(), Status::OK()).ok());
  EXPECT_EQ(queue.Complete(first.value(), Status::OK()).code(), StatusCode::kInvalidArgument);
  EXPECT_FALSE(queue.failed());
  auto work = queue.TakeNext();
  ASSERT_TRUE(work);
  EXPECT_TRUE(work->ticket == second.value());
  EXPECT_EQ(queue.WaitCompletion(second.value()).status().code(), StatusCode::kInvalidArgument);
}

TEST(PersistenceQueue, FailureCompletesQueuedSuccessorsAsNotDispatchedAndRequiresReopen) {
  PersistenceQueue queue;
  auto a = queue.Submit(HardState{1, 1});
  auto b = queue.Submit(SuffixWrite{1, {{1, 1, "value"}}});
  auto c = queue.Submit(SnapshotWrite{{1, 1, "snapshot"}, {}});
  ASSERT_TRUE(a.ok() && b.ok() && c.ok());
  ASSERT_TRUE(queue.TakeNext());
  ASSERT_TRUE(queue.Complete(a.value(), Status::IOError("partial publication possible")).ok());
  auto failed = queue.TakeCompletion();
  ASSERT_TRUE(failed);
  EXPECT_TRUE(failed->dispatched);
  EXPECT_EQ(failed->status.code(), StatusCode::kIOError);
  for (const auto ticket : {b.value(), c.value()}) {
    auto skipped = queue.TakeCompletion();
    ASSERT_TRUE(skipped);
    EXPECT_TRUE(skipped->ticket == ticket);
    EXPECT_FALSE(skipped->dispatched);
    EXPECT_EQ(skipped->status.code(), StatusCode::kShutdown);
  }
  EXPECT_TRUE(queue.failed());
  EXPECT_FALSE(queue.TakeNext());
  EXPECT_EQ(queue.Submit(HardState{2, 1}).status().code(), StatusCode::kIOError);
  EXPECT_EQ(queue.payload_bytes(), 0);
}

TEST(PersistenceQueue, StopRejectsNewWorkButDrainsAcceptedTasksAndWakesIdleConsumer) {
  PersistenceQueue queue;
  auto first = queue.Submit(HardState{1, 1});
  auto second = queue.Submit(HardState{2, 1});
  ASSERT_TRUE(first.ok() && second.ok());
  queue.StopAccepting();
  queue.StopAccepting();
  EXPECT_EQ(queue.Submit(HardState{3, 1}).status().code(), StatusCode::kShutdown);
  for (auto ticket : {first.value(), second.value()}) {
    auto work = queue.WaitNext(0ms);
    ASSERT_TRUE(work);
    EXPECT_TRUE(work->ticket == ticket);
    ASSERT_TRUE(queue.Complete(ticket, Status::OK()).ok());
    ASSERT_TRUE(queue.TakeCompletion());
  }
  EXPECT_FALSE(queue.WaitNext(0ms));
  EXPECT_FALSE(queue.failed());
  PersistenceQueue idle;
  auto consumer = std::async(std::launch::async, [&] { return idle.WaitNext(5s); });
  idle.StopAccepting();
  EXPECT_EQ(consumer.wait_for(2s), std::future_status::ready);
  EXPECT_FALSE(consumer.get());
}

TEST(PersistenceQueue, ConsumerConvertsBothStandardAndUnknownStoreExceptionsToFailure) {
  class ThrowingStore final : public HardStateStore, public LogStore {
   public:
    Status Save(const HardState&) override { throw std::runtime_error("injected"); }
    Status ReplaceFrom(uint64_t, const std::vector<LogEntry>&) override { throw 7; }
  } store;
  EXPECT_EQ(ExecutePersistence(HardState{1, 1}, store, store).code(), StatusCode::kIOError);
  EXPECT_EQ(ExecutePersistence(SuffixWrite{1, {}}, store, store).code(), StatusCode::kIOError);
  EXPECT_EQ(ExecutePersistence(SnapshotWrite{{1, 1, "x"}, {}}, store, store).code(),
            StatusCode::kInvalidArgument);
}

TEST(PersistenceQueue, CampaignCannotExposeVoteRequestsBeforeDurabilityAndCompletion) {
  Directory directory;
  FileStore store(directory.path);
  PersistenceQueue queue;
  QueuedStore bridge(queue);
  Node node(Configuration(), store.ReadState(), bridge, bridge);
  Driver driver(queue);
  auto call = std::async(std::launch::async, [&] { return node.Campaign(); });
  StopOnExit cleanup{driver};
  ASSERT_TRUE(driver.Next());
  EXPECT_TRUE(std::holds_alternative<HardState>(*driver.active->mutation));
  EXPECT_EQ(store.ReadState().hard_state.term, 0);
  EXPECT_EQ(call.wait_for(0ms), std::future_status::timeout);
  auto saved = driver.Execute(store);
  ASSERT_TRUE(saved.ok());
  EXPECT_EQ(store.ReadState().hard_state.voted_for, 1);
  EXPECT_EQ(call.wait_for(0ms), std::future_status::timeout);
  ASSERT_TRUE(driver.Complete(std::move(saved)).ok());
  ASSERT_TRUE(call.get().ok());
  EXPECT_EQ(node.term(), 1);
  EXPECT_EQ(node.TakeMessages().size(), 2);
  EXPECT_EQ(queue.outstanding(), 0);
}

TEST(PersistenceQueue, GrantedVoteReplyWaitsForTermAndVotePublicationNotification) {
  Directory directory;
  FileStore store(directory.path);
  PersistenceQueue queue;
  QueuedStore bridge(queue);
  Node node(Configuration(), store.ReadState(), bridge, bridge);
  Driver driver(queue);
  Message vote;
  vote.kind = MessageKind::kVote;
  vote.from = 2;
  vote.to = 1;
  vote.sequence = 7;
  vote.term = 3;
  auto call = std::async(std::launch::async, [&] { return node.Receive(vote); });
  StopOnExit cleanup{driver};
  ASSERT_TRUE(driver.Next());
  auto saved = driver.Execute(store);
  ASSERT_TRUE(saved.ok());
  EXPECT_EQ(store.ReadState().hard_state.term, 3);
  EXPECT_EQ(store.ReadState().hard_state.voted_for, 2);
  EXPECT_EQ(call.wait_for(0ms), std::future_status::timeout);
  ASSERT_TRUE(driver.Complete(std::move(saved)).ok());
  auto reply = call.get();
  ASSERT_TRUE(reply);
  EXPECT_TRUE(reply->granted);
  EXPECT_EQ(reply->term, 3);
  EXPECT_EQ(reply->sequence, 7);
}

TEST(PersistenceQueue, AppendReplyAndCommittedEntriesWaitForHardStateAndLogCompletions) {
  Directory directory;
  FileStore store(directory.path);
  PersistenceQueue queue;
  QueuedStore bridge(queue);
  Node node(Configuration(), store.ReadState(), bridge, bridge);
  Driver driver(queue);
  Message append;
  append.kind = MessageKind::kAppend;
  append.from = 2;
  append.to = 1;
  append.sequence = 8;
  append.term = 2;
  append.append = {0, 0, 1, {{1, 2, "value"}}};
  auto call = std::async(std::launch::async, [&] { return node.Receive(append); });
  StopOnExit cleanup{driver};
  ASSERT_TRUE(driver.Next());
  EXPECT_TRUE(std::holds_alternative<HardState>(*driver.active->mutation));
  ASSERT_TRUE(driver.Complete(driver.Execute(store)).ok());
  ASSERT_TRUE(driver.Next());
  EXPECT_TRUE(std::holds_alternative<SuffixWrite>(*driver.active->mutation));
  EXPECT_TRUE(store.ReadState().entries.empty());
  EXPECT_EQ(call.wait_for(0ms), std::future_status::timeout);
  auto saved = driver.Execute(store);
  ASSERT_TRUE(saved.ok());
  EXPECT_EQ(store.ReadState().entries.size(), 1);
  EXPECT_EQ(call.wait_for(0ms), std::future_status::timeout);
  ASSERT_TRUE(driver.Complete(std::move(saved)).ok());
  auto reply = call.get();
  ASSERT_TRUE(reply);
  EXPECT_TRUE(reply->success);
  EXPECT_EQ(reply->match_index, 1);
  auto committed = node.TakeCommitted();
  ASSERT_EQ(committed.size(), 1);
  EXPECT_EQ(committed.front().command, "value");
}

TEST(PersistenceQueue, SnapshotBoundaryWaitsForAtomicPublicationCompletion) {
  Directory directory;
  FileStore store(directory.path);
  ASSERT_TRUE(store.Save({2, 1}).ok());
  ASSERT_TRUE(store.ReplaceFrom(1, {{1, 2, "value"}}).ok());
  PersistenceQueue queue;
  QueuedStore bridge(queue);
  const auto recovered = store.ReadState();
  RaftLog log(recovered.entries, 0, bridge);
  ASSERT_TRUE(log.AdvanceLeaderCommit(2, {1, 1, 0}).ok());
  Driver driver(queue);
  auto call = std::async(std::launch::async, [&] { return log.Compact(1, "snapshot"); });
  StopOnExit cleanup{driver};
  ASSERT_TRUE(driver.Next());
  EXPECT_TRUE(std::holds_alternative<SnapshotWrite>(*driver.active->mutation));
  auto saved = driver.Execute(store);
  ASSERT_TRUE(saved.ok());
  EXPECT_EQ(store.ReadState().snapshot.index, 1);
  EXPECT_EQ(call.wait_for(0ms), std::future_status::timeout);
  ASSERT_TRUE(driver.Complete(std::move(saved)).ok());
  ASSERT_TRUE(call.get().ok());
  EXPECT_EQ(log.snapshot().index, 1);
  EXPECT_EQ(log.snapshot().data, "snapshot");
  EXPECT_TRUE(log.entries().empty());
}

TEST(PersistenceQueue, TypedMutationsPreserveImageAndJournalBytesAndReopenState) {
  for (const bool journal : {false, true}) {
    SCOPED_TRACE(journal);
    Directory baseline;
    Directory queued;
    FileStore::Options options;
    options.journal = journal;
    options.journal_checkpoint_bytes = 128;
    const std::vector<PersistenceMutation> mutations = {
        HardState{3, 1}, SuffixWrite{1, {{1, 3, "one"}, {2, 3, "two"}}},
        SnapshotWrite{{1, 3, "snapshot"}, {{2, 3, "two"}}}, SuffixWrite{3, {{3, 3, "three"}}},
        HardState{4, 0}};
    {
      FileStore direct(baseline.path, options);
      FileStore disk(queued.path, options);
      PersistenceQueue queue;
      for (const auto& mutation : mutations) {
        ASSERT_TRUE(ExecutePersistence(mutation, direct, direct).ok());
        auto submitted = queue.Submit(mutation);
        ASSERT_TRUE(submitted.ok());
        auto work = queue.TakeNext();
        ASSERT_TRUE(work);
        ASSERT_TRUE(
            queue.Complete(work->ticket, ExecutePersistence(*work->mutation, disk, disk)).ok());
        auto result = queue.TakeCompletion();
        ASSERT_TRUE(result && result->status.ok());
      }
      EXPECT_EQ(Files(baseline), Files(queued));
    }
    FileStore reopened(queued.path, options);
    auto state = reopened.ReadState();
    EXPECT_EQ(state.hard_state.term, 4);
    EXPECT_EQ(state.snapshot.index, 1);
    ASSERT_EQ(state.entries.size(), 2);
    EXPECT_EQ(state.entries.back().command, "three");
  }
}

TEST(PersistenceQueue, PostSyncFailureCannotConfirmElectionOrDispatchQueuedSuccessor) {
  Directory directory;
  {
    bool armed = false;
    FileStore::Options options;
    options.journal = true;
    options.persist_observer = [&](FileStore::PersistPoint point) {
      if (armed && point == FileStore::PersistPoint::kAfterJournalSync)
        throw std::runtime_error("injected after durable sync");
    };
    FileStore disk(directory.path, options);
    armed = true;
    PersistenceQueue queue;
    QueuedStore bridge(queue);
    Node node(Configuration(), disk.ReadState(), bridge, bridge);
    Driver driver(queue);
    auto call = std::async(std::launch::async, [&] { return node.Campaign(); });
    StopOnExit cleanup{driver};
    ASSERT_TRUE(driver.Next());
    auto successor = queue.Submit(SuffixWrite{1, {{1, 1, "must not run"}}});
    ASSERT_TRUE(successor.ok());
    auto saved = driver.Execute(disk);
    EXPECT_EQ(saved.code(), StatusCode::kIOError);
    ASSERT_TRUE(driver.Complete(std::move(saved)).ok());
    EXPECT_FALSE(call.get().ok());
    EXPECT_TRUE(node.failed());
    EXPECT_TRUE(node.TakeMessages().empty());
    EXPECT_TRUE(disk.failed());
    auto skipped = queue.TakeCompletion();
    ASSERT_TRUE(skipped);
    EXPECT_TRUE(skipped->ticket == successor.value());
    EXPECT_FALSE(skipped->dispatched);
    EXPECT_FALSE(queue.TakeNext());
    EXPECT_TRUE(queue.failed());
  }
  // The first write may have reached disk despite its failed acknowledgement.
  FileStore::Options options;
  options.journal = true;
  FileStore reopened(directory.path, options);
  EXPECT_EQ(reopened.ReadState().hard_state.term, 1);
  EXPECT_TRUE(reopened.ReadState().entries.empty());
}

TEST(PersistenceQueue, ConcurrentAdmissionAssignsUniqueOrderedTicketsWithoutLosingCompletions) {
  PersistenceQueue queue({32, 4096});
  std::vector<std::future<yewukv::Result<PersistenceQueue::Ticket>>> submissions;
  for (uint64_t i = 1; i <= 16; ++i)
    submissions.push_back(
        std::async(std::launch::async, [&, i] { return queue.Submit(HardState{i, 1}); }));
  std::set<uint64_t> sequences;
  for (auto& submitted : submissions) {
    auto result = submitted.get();
    ASSERT_TRUE(result.ok());
    EXPECT_TRUE(sequences.insert(result.value().sequence).second);
  }
  EXPECT_EQ(queue.outstanding(), 16);
  for (uint64_t i = 1; i <= 16; ++i) {
    auto work = queue.TakeNext();
    ASSERT_TRUE(work);
    EXPECT_EQ(work->ticket.sequence, i);
    ASSERT_TRUE(queue.Complete(work->ticket, Status::OK()).ok());
    auto completed = queue.TakeCompletion();
    ASSERT_TRUE(completed && completed->status.ok());
    EXPECT_EQ(completed->ticket.sequence, i);
  }
  EXPECT_EQ(queue.outstanding(), 0);
  EXPECT_EQ(queue.payload_bytes(), 0);
}

TEST(PersistenceQueue, StoppingAnInFlightTaskDoesNotCompleteItOrDispatchItsSuccessor) {
  PersistenceQueue queue;
  auto first = queue.Submit(HardState{1, 1});
  auto second = queue.Submit(HardState{2, 1});
  ASSERT_TRUE(first.ok() && second.ok());
  auto work = queue.TakeNext();
  ASSERT_TRUE(work);
  queue.StopAccepting();
  EXPECT_FALSE(queue.TakeCompletion());
  EXPECT_FALSE(queue.TakeNext());
  EXPECT_FALSE(queue.WaitNext(0ms));
  ASSERT_TRUE(queue.Complete(first.value(), Status::OK()).ok());
  ASSERT_TRUE(queue.TakeCompletion());
  work = queue.TakeNext();
  ASSERT_TRUE(work);
  EXPECT_TRUE(work->ticket == second.value());
  ASSERT_TRUE(queue.Complete(second.value(), Status::OK()).ok());
  ASSERT_TRUE(queue.TakeCompletion());
}

}  // namespace
