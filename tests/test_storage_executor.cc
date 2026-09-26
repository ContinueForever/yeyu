#include <gtest/gtest.h>

#include <chrono>
#include <condition_variable>
#include <future>
#include <mutex>
#include <thread>
#include <vector>

#include "storage/executor.h"

namespace {
using namespace std::chrono_literals;
using yewukv::StatusCode;
using yewukv::storage::Executor;

TEST(StorageExecutor, BoundedFifoAndDrain) {
  Executor executor(2);
  std::promise<void> started;
  auto running = started.get_future();
  std::promise<void> release;
  auto released = release.get_future();
  std::vector<int> order;
  ASSERT_TRUE(executor
                  .Submit([&] {
                    started.set_value();
                    released.wait();
                    order.push_back(1);
                  })
                  .ok());
  ASSERT_EQ(running.wait_for(2s), std::future_status::ready);
  ASSERT_TRUE(executor.Submit([&] { order.push_back(2); }).ok());
  EXPECT_EQ(executor.Submit([&] { order.push_back(3); }).code(), StatusCode::kBusy);
  executor.StopAccepting();
  EXPECT_EQ(executor.Submit([&] { order.push_back(4); }).code(), StatusCode::kShutdown);
  release.set_value();
  executor.DrainAndStop();
  EXPECT_EQ(order, (std::vector<int>{1, 2}));
  executor.DrainAndStop();  // Idempotent.
}

TEST(StorageExecutor, CapacityCountsRunningOperation) {
  Executor executor(1);
  std::promise<void> started;
  std::promise<void> release;
  auto released = release.get_future();
  ASSERT_TRUE(executor
                  .Submit([&] {
                    started.set_value();
                    released.wait();
                  })
                  .ok());
  ASSERT_EQ(started.get_future().wait_for(2s), std::future_status::ready);
  EXPECT_EQ(executor.Submit([] {}).code(), StatusCode::kBusy);
  release.set_value();
  executor.DrainAndStop();
}
}  // namespace
