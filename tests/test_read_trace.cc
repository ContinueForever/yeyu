#include <gtest/gtest.h>

#include <unistd.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "cluster/read_trace.h"

namespace {
using yewukv::cluster::PeerTrace;
using yewukv::cluster::ReadTrace;
using yewukv::cluster::ReadTraceRecorder;
using namespace std::chrono_literals;

class TraceDirectory {
 public:
  TraceDirectory() {
    auto name = (std::filesystem::temp_directory_path() / "yewukv-trace-XXXXXX").string();
    const auto* made = ::mkdtemp(name.data());
    if (!made) throw std::runtime_error("create trace test directory");
    path = made;
  }
  ~TraceDirectory() { std::filesystem::remove_all(path); }
  std::string prefix() const { return path + "/trace"; }
  std::string path;
};

std::string Contents(const std::string& path) {
  std::ifstream input(path);
  return {std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()};
}

TEST(ReadTrace, EventLimitIsBoundedAndShutdownDrainsAcceptedRecords) {
  TraceDirectory directory;
  ReadTraceRecorder::Options options;
  options.max_events = 2;
  std::string path;
  {
    ReadTraceRecorder recorder(directory.prefix(), 2, options);
    path = recorder.path();
    ReadTrace read;
    read.request_id = 11;
    read.received_ns = 100;
    read.ready_ns = 200;
    recorder.Record(read);
    PeerTrace peer;
    peer.sequence = 23;
    recorder.Record(peer);
    recorder.Record(read);
    EXPECT_EQ(recorder.accepted(), 2);
    EXPECT_TRUE(recorder.limited());
    EXPECT_GE(recorder.dropped(), 1);
  }
  const auto output = Contents(path);
  EXPECT_NE(output.find("\"request_id\":11"), std::string::npos);
  EXPECT_NE(output.find("\"sequence\":23"), std::string::npos);
  EXPECT_NE(output.find("\"event\":\"summary\""), std::string::npos);
  EXPECT_NE(output.find("\"written\":2"), std::string::npos);
  EXPECT_NE(output.find("\"limited\":1"), std::string::npos);
}

TEST(ReadTrace, RuntimeControlPausesAndResumesWithoutChangingTheService) {
  TraceDirectory directory;
  const auto control = directory.prefix() + ".enabled";
  std::ofstream(control) << "0\n";
  std::string path;
  {
    ReadTraceRecorder recorder(directory.prefix(), 1, {});
    path = recorder.path();
    EXPECT_FALSE(recorder.Enabled());
    recorder.Record(ReadTrace{});
    EXPECT_EQ(recorder.accepted(), 0);
    std::ofstream(control) << "1\n";
    const auto deadline = std::chrono::steady_clock::now() + 2s;
    while (!recorder.Enabled() && std::chrono::steady_clock::now() < deadline)
      std::this_thread::sleep_for(10ms);
    ASSERT_TRUE(recorder.Enabled());
    recorder.Record(ReadTrace{});
    std::ofstream(control) << "0\n";
    while (recorder.Enabled() && std::chrono::steady_clock::now() < deadline)
      std::this_thread::sleep_for(10ms);
    ASSERT_FALSE(recorder.Enabled());
    recorder.Record(ReadTrace{});
    EXPECT_EQ(recorder.accepted(), 1);
  }
  const auto output = Contents(path);
  EXPECT_NE(output.find("\"event\":\"mode\""), std::string::npos);
  EXPECT_NE(output.find("\"written\":1"), std::string::npos);
}

TEST(ReadTrace, ConcurrentProducersKeepNumericRecordsAndAccountForQueueDrops) {
  TraceDirectory directory;
  ReadTraceRecorder::Options options;
  options.queue_capacity = 1;
  std::string path;
  uint64_t accepted, dropped;
  {
    ReadTraceRecorder recorder(directory.prefix(), 3, options);
    path = recorder.path();
    std::vector<std::thread> producers;
    for (int thread = 0; thread < 8; ++thread)
      producers.emplace_back([&recorder, thread] {
        for (int i = 0; i < 1000; ++i) {
          PeerTrace trace;
          trace.request_id = static_cast<uint64_t>(thread * 1000 + i + 1);
          recorder.Record(trace);
        }
      });
    for (auto& producer : producers) producer.join();
    accepted = recorder.accepted();
    dropped = recorder.dropped();
    EXPECT_EQ(accepted + dropped, 8000);
    EXPECT_FALSE(recorder.failed());
    EXPECT_FALSE(recorder.limited());
  }
  const auto output = Contents(path);
  EXPECT_NE(output.find("\"written\":" + std::to_string(accepted)), std::string::npos);
  EXPECT_NE(output.find("\"dropped\":" + std::to_string(dropped)), std::string::npos);
  std::istringstream input(output);
  std::string line;
  size_t records = 0;
  while (std::getline(input, line)) {
    ASSERT_FALSE(line.empty());
    EXPECT_EQ(line.front(), '{');
    EXPECT_EQ(line.back(), '}');
    records += line.find("\"event\":\"peer\"") != std::string::npos;
  }
  EXPECT_EQ(records, accepted);
}

TEST(ReadTrace, ByteLimitBoundsTheFileAndMarksIncompleteDiagnostics) {
  TraceDirectory directory;
  ReadTraceRecorder::Options options;
  options.max_bytes = 4096;
  std::string path;
  {
    ReadTraceRecorder recorder(directory.prefix(), 1, options);
    path = recorder.path();
    for (int i = 0; i < 100; ++i) recorder.Record(ReadTrace{});
  }
  EXPECT_LE(std::filesystem::file_size(path), options.max_bytes + 1024);
  const auto output = Contents(path);
  EXPECT_NE(output.find("\"limited\":1"), std::string::npos);
  EXPECT_NE(output.find("\"event\":\"summary\""), std::string::npos);
}

TEST(ReadTrace, PersistenceSelectionSharesLimitsAndKeepsCheckpointsSlowAndFailedOperations) {
  TraceDirectory directory;
  ReadTraceRecorder::Options options;
  options.max_events = 4;
  options.sample_every = 32;
  options.slow_ns = 100;
  std::string path;
  {
    ReadTraceRecorder recorder(directory.prefix(), 1, options);
    path = recorder.path();
    yewukv::raft::PersistenceTrace event;
    event.kind = static_cast<uint64_t>(yewukv::raft::PersistenceKind::kCheckpoint);
    event.sequence = 1;
    event.success = 1;
    event.started_ns = 100;
    event.finished_ns = 110;
    event.span_count = 1;
    event.spans[0] = {1, 101, 109};
    recorder.Record(event);
    event.kind = static_cast<uint64_t>(yewukv::raft::PersistenceKind::kSuffixDelta);
    event.sequence = 2;
    recorder.Record(event);
    EXPECT_EQ(recorder.accepted(), 1);
    event.sequence = 32;
    recorder.Record(event);
    event.sequence = 3;
    event.finished_ns = 300;
    recorder.Record(event);
    event.sequence = 4;
    event.finished_ns = 110;
    event.success = 0;
    recorder.Record(event);
    EXPECT_EQ(recorder.accepted(), 4);
    recorder.Record(PeerTrace{});
    EXPECT_TRUE(recorder.limited());
  }
  const auto output = Contents(path);
  EXPECT_NE(output.find("\"schema\":2"), std::string::npos);
  EXPECT_NE(output.find("\"spans\":[[1,101,109]]"), std::string::npos);
  EXPECT_NE(output.find("\"sequence\":32"), std::string::npos);
  EXPECT_EQ(output.find("\"event\":\"persist\",\"node\":1,\"sequence\":2,"), std::string::npos);
  EXPECT_NE(output.find("\"written\":4"), std::string::npos);
}
}  // namespace
