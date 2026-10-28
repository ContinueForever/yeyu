#include <gtest/gtest.h>

#include "net/buffer.h"

using yewukv::net::Buffer;

TEST(Buffer, AppendAndTake) {
  Buffer b;
  b.Append("hello");
  EXPECT_EQ(b.ReadableBytes(), 5u);
  b.Append(" world");
  EXPECT_EQ(b.Take(5), "hello");
  EXPECT_EQ(b.TakeAll(), " world");
  EXPECT_EQ(b.ReadableBytes(), 0u);
}

TEST(Buffer, ConsumeResets) {
  Buffer b(4);
  b.Append("abcdef");
  b.Consume(3);
  EXPECT_EQ(b.TakeAll(), "def");
}
