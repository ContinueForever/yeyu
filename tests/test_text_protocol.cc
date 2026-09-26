#include <gtest/gtest.h>

#include "kv/text_protocol.h"

using yewukv::kv::Command;
using yewukv::kv::TryParse;

TEST(TextProtocol, PutGetDelPing) {
  Command c;
  EXPECT_EQ(TryParse("PUT k1 v1\n", &c), 10u);
  EXPECT_EQ(c.type, Command::Type::kPut);
  EXPECT_EQ(c.key, "k1");
  EXPECT_EQ(c.value, "v1");

  EXPECT_EQ(TryParse("GET k1\n", &c), 7u);
  EXPECT_EQ(c.type, Command::Type::kGet);
  EXPECT_EQ(c.key, "k1");

  EXPECT_EQ(TryParse("DEL k1\r\n", &c), 8u);
  EXPECT_EQ(c.type, Command::Type::kDel);

  EXPECT_EQ(TryParse("PING\n", &c), 5u);
  EXPECT_EQ(c.type, Command::Type::kPing);
}

TEST(TextProtocol, HalfPacketReturnsZero) {
  Command c;
  EXPECT_EQ(TryParse("GET ke", &c), 0u);
  EXPECT_EQ(c.type, Command::Type::kInvalid);
}

TEST(TextProtocol, MultipleCommandsInOneBuffer) {
  std::string_view buf = "PING\nPUT a b\n";
  Command c;
  size_t n1 = TryParse(buf, &c);
  ASSERT_EQ(n1, 5u);
  buf.remove_prefix(n1);
  size_t n2 = TryParse(buf, &c);
  ASSERT_EQ(n2, 8u);
  EXPECT_EQ(c.type, Command::Type::kPut);
  EXPECT_EQ(c.key, "a");
  EXPECT_EQ(c.value, "b");
}

TEST(TextProtocol, Malformed) {
  Command c;
  size_t n = TryParse("FOO bar\n", &c);
  EXPECT_EQ(n, 8u);
  EXPECT_EQ(c.type, Command::Type::kInvalid);
}
