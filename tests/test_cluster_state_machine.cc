#include <gtest/gtest.h>

#include "cluster/state_machine.h"

namespace {
using yewukv::cluster::StateMachine;
using yewukv::rpc::KvRequest;
using yewukv::rpc::KvResponse;

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
}  // namespace
