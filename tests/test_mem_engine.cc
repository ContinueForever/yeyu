#include <gtest/gtest.h>

#include "storage/mem_engine.h"

using yewukv::storage::MemEngine;

TEST(MemEngine, PutGetDelete) {
  MemEngine e;
  EXPECT_TRUE(e.Put("k", "v").ok());
  std::string out;
  EXPECT_TRUE(e.Get("k", &out).ok());
  EXPECT_EQ(out, "v");
  EXPECT_TRUE(e.Delete("k").ok());
  EXPECT_TRUE(e.Get("k", &out).code() == yewukv::StatusCode::kNotFound);
}
