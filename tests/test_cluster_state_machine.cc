#include <gtest/gtest.h>
#include <thread>

#include "cluster/state_machine.h"
#include "cluster/snapshot_policy.h"

namespace {
using yewukv::cluster::StateMachine;
using yewukv::rpc::KvRequest;
using yewukv::rpc::KvResponse;

TEST(SnapshotPolicy, LargeLiveStateAmortizesEntryTriggeredImages) {
  using yewukv::cluster::ShouldSnapshot;
  EXPECT_FALSE(ShouldSnapshot(128, 0, 20000, 15 * 1024 * 1024, 128, 1024 * 1024, true));
  EXPECT_TRUE(ShouldSnapshot(128, 0, 20000, 15 * 1024 * 1024, 128, 1024 * 1024, false));
  EXPECT_FALSE(ShouldSnapshot(128, 0, 65535, 256 * 1024, 128, 1024 * 1024, true));
  EXPECT_TRUE(ShouldSnapshot(128, 0, 65536, 256 * 1024, 128, 1024 * 1024, true));
  EXPECT_TRUE(ShouldSnapshot(128, 0, 20000, 8192, 128, 1024 * 1024, true));
}

TEST(SnapshotPolicy, ByteTriggerAndApplicationBoundaryRemainHardConditions) {
  using yewukv::cluster::ShouldSnapshot;
  for (bool adaptive : {false, true}) {
    EXPECT_TRUE(ShouldSnapshot(11, 10, 1024 * 1024, 15 * 1024 * 1024, 128, 1024 * 1024, adaptive));
    EXPECT_FALSE(ShouldSnapshot(10, 10, 1024 * 1024, 15 * 1024 * 1024, 128, 1024 * 1024, adaptive));
    EXPECT_FALSE(ShouldSnapshot(11, 10, 100, 8192, 128, 1024 * 1024, adaptive));
  }
}

KvRequest Put(const std::string& key, const std::string& value) {
  KvRequest request;
  request.set_state_machine_version(1);
  request.mutable_put()->set_key(key);
  request.mutable_put()->set_value(value);
  return request;
}

KvResponse Apply(StateMachine& state, const KvRequest& request) {
  KvResponse response;
  state.Apply(request, &response);
  return response;
}

TEST(KvStateMachine, CapacityRejectsNewOrGrowingValuesAndDeletionReclaimsSpace) {
  StateMachine state(40);
  ASSERT_EQ(Apply(state, Put("a", std::string(24, 'x'))).code(), KvResponse::OK);
  const auto original = state.Serialize();
  EXPECT_EQ(Apply(state, Put("b", std::string(24, 'y'))).code(), KvResponse::BUSY);
  EXPECT_EQ(Apply(state, Put("a", std::string(40, 'z'))).code(), KvResponse::BUSY);
  EXPECT_EQ(state.Serialize(), original);
  EXPECT_EQ(Apply(state, Put("a", "")).code(), KvResponse::OK);
  EXPECT_EQ(Apply(state, Put("b", std::string(24, 'y'))).code(), KvResponse::OK);
  KvRequest remove;
  remove.mutable_del()->set_key("b");
  EXPECT_EQ(Apply(state, remove).code(), KvResponse::OK);
  EXPECT_EQ(Apply(state, Put("c", std::string(24, 'z'))).code(), KvResponse::OK);
  EXPECT_EQ(state.encoded_bytes(), state.Serialize().size());
}

TEST(KvStateMachine, SnapshotRestoresDeterministicCapacityDecisionsIncludingEmptyBinaryValues) {
  StateMachine first(300), second(300);
  ASSERT_EQ(Apply(first, Put(std::string("a\0b", 3), "")).code(), KvResponse::OK);
  ASSERT_EQ(Apply(first, Put("z", std::string(128, 'z'))).code(), KvResponse::OK);
  ASSERT_TRUE(second.Restore(first.Serialize()));
  const auto request = Put("large", std::string(200, 'x'));
  EXPECT_EQ(Apply(first, request).code(), KvResponse::BUSY);
  EXPECT_EQ(Apply(second, request).code(), KvResponse::BUSY);
  EXPECT_EQ(first.Serialize(), second.Serialize());
  KvRequest get;
  get.mutable_get()->set_key(std::string("a\0b", 3));
  auto response = Apply(second, get);
  EXPECT_EQ(response.code(), KvResponse::OK);
  EXPECT_TRUE(response.value().empty());
}

TEST(KvStateMachine, InvalidSnapshotLeavesLiveStateUntouched) {
  StateMachine state;
  ASSERT_EQ(Apply(state, Put("safe", "value")).code(), KvResponse::OK);
  const auto original = state.Serialize();
  yewukv::rpc::KvSnapshot malformed;
  malformed.set_version(1);
  for (const auto& key : {"z", "a"}) malformed.add_entries()->set_key(key);
  EXPECT_FALSE(state.Restore(malformed.SerializeAsString()));
  EXPECT_FALSE(state.Restore("truncated"));
  EXPECT_EQ(state.Serialize(), original);
  StateMachine limited(4);
  EXPECT_FALSE(limited.Restore(original));
  EXPECT_EQ(limited.size(), 0);
}

TEST(KvStateMachine, SnapshotBytesMatchProtobufLengthBoundariesAndRepeatedUpdates) {
  StateMachine state;
  for (const auto size : {0, 1, 120, 127, 128, 1024, 8192}) {
    ASSERT_EQ(Apply(state, Put("key-" + std::to_string(size), std::string(size, 'x'))).code(),
              KvResponse::OK);
    EXPECT_EQ(state.encoded_bytes(), state.Serialize().size());
  }
  ASSERT_EQ(Apply(state, Put("key-8192", "smaller")).code(), KvResponse::OK);
  EXPECT_EQ(state.encoded_bytes(), state.Serialize().size());
}

TEST(KvStateMachine, ReplaysLegacySuccessfulWritesWithoutApplyingNewCapacityRetroactively) {
  StateMachine state(40);
  auto legacy = Put("legacy", std::string(80, 'x'));
  legacy.set_state_machine_version(0);
  EXPECT_EQ(Apply(state, legacy).code(), KvResponse::OK);
  KvRequest get;
  get.mutable_get()->set_key("legacy");
  EXPECT_EQ(Apply(state, get).value(), std::string(80, 'x'));
  EXPECT_EQ(Apply(state, Put("new", "value")).code(), KvResponse::BUSY);
  KvRequest remove;
  remove.mutable_del()->set_key("legacy");
  EXPECT_EQ(Apply(state, remove).code(), KvResponse::OK);
  EXPECT_EQ(Apply(state, Put("new", "value")).code(), KvResponse::OK);
}

KvRequest Cas(const std::string& key, uint64_t revision, const std::string& value) {
  KvRequest request;
  request.set_state_machine_version(2);
  request.mutable_cas()->set_key(key);
  request.mutable_cas()->set_expected_revision(revision);
  request.mutable_cas()->set_value(value);
  return request;
}

KvResponse At(StateMachine& state, KvRequest request, uint64_t index) {
  KvResponse response;
  state.Apply(request, &response, index);
  return response;
}

TEST(KvRevision, CompareAndSwapAllowsOneWinnerAndConflictsDoNotOverwrite) {
  StateMachine state;
  EXPECT_EQ(At(state, Cas("key", 0, "first"), 10).revision(), 10);
  EXPECT_EQ(At(state, Cas("key", 10, "winner"), 11).revision(), 11);
  const auto conflict = At(state, Cas("key", 10, "loser"), 12);
  EXPECT_EQ(conflict.code(), KvResponse::CONFLICT);
  EXPECT_EQ(conflict.value(), "winner");
  EXPECT_EQ(conflict.revision(), 11);
  EXPECT_EQ(At(state, Cas("key", 0, "create-again"), 13).code(), KvResponse::CONFLICT);
  EXPECT_EQ(At(state, Cas("absent", 11, "bad"), 14).code(), KvResponse::CONFLICT);
}

TEST(KvRevision, DeleteRecreateAndSnapshotPreserveProtectionAgainstABA) {
  StateMachine state;
  ASSERT_EQ(At(state, Cas("key", 0, "same"), 20).code(), KvResponse::OK);
  KvRequest remove;
  remove.set_state_machine_version(2);
  remove.mutable_del()->set_key("key");
  ASSERT_EQ(At(state, remove, 21).code(), KvResponse::OK);
  ASSERT_EQ(At(state, Cas("key", 0, "same"), 22).revision(), 22);
  StateMachine recovered;
  ASSERT_TRUE(recovered.Restore(state.Serialize()));
  EXPECT_EQ(At(recovered, Cas("key", 20, "stale"), 23).code(), KvResponse::CONFLICT);
  EXPECT_EQ(At(recovered, Cas("key", 22, "updated"), 24).revision(), 24);
  EXPECT_EQ(recovered.Serialize().size(), recovered.encoded_bytes());
}

TEST(KvRevision, LegacyMigrationConvergesAcrossDifferentSnapshotBoundaries) {
  StateMachine replayed;
  ASSERT_EQ(Apply(replayed, Put("legacy", "before")).code(), KvResponse::OK);
  StateMachine snapshotted;
  ASSERT_TRUE(snapshotted.Restore(replayed.Serialize()));
  for (auto* state : {&replayed, &snapshotted}) {
    const auto first = At(*state, Cas("legacy", 0, "unsafe"), 100);
    EXPECT_EQ(first.code(), KvResponse::CONFLICT);
    EXPECT_EQ(first.value(), "before");
    EXPECT_EQ(first.revision(), 100);
    EXPECT_EQ(At(*state, Cas("legacy", 100, "after"), 101).code(), KvResponse::OK);
  }
  EXPECT_EQ(replayed.Serialize(), snapshotted.Serialize());
}

TEST(KvRevision, CapacityAndMalformedRevisionSnapshotLeaveValuesIntact) {
  StateMachine state(50);
  ASSERT_EQ(At(state, Cas("safe", 0, "value"), 127).code(), KvResponse::OK);
  const auto before = state.Serialize();
  EXPECT_EQ(At(state, Cas("safe", 127, std::string(60, 'x')), 128).code(), KvResponse::BUSY);
  EXPECT_EQ(state.Serialize(), before);
  yewukv::rpc::KvSnapshot invalid;
  ASSERT_TRUE(invalid.ParseFromString(before));
  invalid.mutable_entries(0)->set_revision(invalid.revision() + 1);
  EXPECT_FALSE(state.Restore(invalid.SerializeAsString()));
  EXPECT_EQ(state.Serialize(), before);
}

TEST(KvRevision, FrozenSnapshotOwnsValuesAfterMutationDeletionAndDestruction) {
  StateMachine::Frozen frozen;
  std::string original;
  {
    StateMachine state;
    ASSERT_EQ(At(state, Cas("key", 0, std::string(8192, 'a')), 1).code(), KvResponse::OK);
    frozen = state.Freeze();
    original = frozen.Serialize();
    ASSERT_EQ(At(state, Cas("key", 1, "new"), 2).code(), KvResponse::OK);
    KvRequest remove;
    remove.set_state_machine_version(2);
    remove.mutable_del()->set_key("key");
    At(state, remove, 3);
    EXPECT_EQ(frozen.Serialize(), original);
  }
  StateMachine restored;
  ASSERT_TRUE(restored.Restore(frozen.Serialize()));
  EXPECT_EQ(frozen.Serialize(), original);
  EXPECT_EQ(At(restored, Cas("key", 1, "alive"), 4).code(), KvResponse::OK);
}

TEST(KvRevision, BackgroundSerializationDoesNotObserveConcurrentOwnerMutations) {
  StateMachine state;
  ASSERT_EQ(At(state, Cas("key", 0, "old"), 1).code(), KvResponse::OK);
  auto frozen = state.Freeze();
  const auto expected = frozen.Serialize();
  std::string actual;
  std::thread encoder([&] {
    for (int i = 0; i != 100; ++i) actual = frozen.Serialize();
  });
  for (uint64_t i = 2; i != 102; ++i) {
    auto put = Put("key", std::to_string(i));
    put.set_state_machine_version(2);
    At(state, put, i);
  }
  encoder.join();
  EXPECT_EQ(actual, expected);
}
}  // namespace
