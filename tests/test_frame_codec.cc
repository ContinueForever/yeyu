#include <gtest/gtest.h>

#include <stdexcept>
#include <string>
#include <string_view>

#include "rpc/frame_codec.h"

using yewukv::net::Buffer;
using yewukv::rpc::FrameCodec;
using DecodeStatus = FrameCodec::DecodeStatus;

TEST(FrameCodec, EncodesNetworkOrderLengthAndBinaryPayload) {
  const std::string payload("\0A\n\xff", 4);
  const std::string frame = FrameCodec::Encode(payload);
  EXPECT_EQ(frame.substr(0, FrameCodec::kHeaderBytes), std::string("\0\0\0\4", 4));
  EXPECT_EQ(frame.substr(FrameCodec::kHeaderBytes), payload);

  Buffer input;
  input.Append(frame);
  std::string decoded;
  ASSERT_EQ(FrameCodec::Decode(&input, &decoded), DecodeStatus::kComplete);
  EXPECT_EQ(decoded, payload);
  EXPECT_EQ(input.ReadableBytes(), 0u);
}

TEST(FrameCodec, WaitsForSplitHeaderWithoutConsumingOrChangingOutput) {
  const std::string frame = FrameCodec::Encode("hello");
  Buffer input;
  std::string payload = "unchanged";
  EXPECT_EQ(FrameCodec::Decode(&input, &payload), DecodeStatus::kIncomplete);
  for (size_t i = 0; i < FrameCodec::kHeaderBytes; ++i) {
    input.Append(frame.data() + i, 1);
    EXPECT_EQ(FrameCodec::Decode(&input, &payload), DecodeStatus::kIncomplete);
    EXPECT_EQ(input.ReadableBytes(), i + 1);
    EXPECT_EQ(payload, "unchanged");
  }
  input.Append(frame.data() + FrameCodec::kHeaderBytes, 5);
  ASSERT_EQ(FrameCodec::Decode(&input, &payload), DecodeStatus::kComplete);
  EXPECT_EQ(payload, "hello");
}

TEST(FrameCodec, WaitsForSplitPayload) {
  const std::string frame = FrameCodec::Encode("hello");
  Buffer input;
  input.Append(frame.data(), frame.size() - 1);
  std::string payload = "unchanged";
  EXPECT_EQ(FrameCodec::Decode(&input, &payload), DecodeStatus::kIncomplete);
  EXPECT_EQ(input.ReadableBytes(), frame.size() - 1);
  EXPECT_EQ(payload, "unchanged");

  input.Append(frame.data() + frame.size() - 1, 1);
  ASSERT_EQ(FrameCodec::Decode(&input, &payload), DecodeStatus::kComplete);
  EXPECT_EQ(payload, "hello");
  EXPECT_EQ(input.ReadableBytes(), 0u);
}

TEST(FrameCodec, ConsumesOneOfMultipleFramesAndKeepsTrailingPartialFrame) {
  const std::string first = FrameCodec::Encode("one");
  const std::string second = FrameCodec::Encode("two");
  const std::string third = FrameCodec::Encode("three");
  Buffer input;
  input.Append(first + second + third.substr(0, 6));
  std::string payload;

  ASSERT_EQ(FrameCodec::Decode(&input, &payload), DecodeStatus::kComplete);
  EXPECT_EQ(payload, "one");
  EXPECT_EQ(input.ReadableBytes(), second.size() + 6);
  ASSERT_EQ(FrameCodec::Decode(&input, &payload), DecodeStatus::kComplete);
  EXPECT_EQ(payload, "two");
  EXPECT_EQ(input.ReadableBytes(), 6u);
  EXPECT_EQ(FrameCodec::Decode(&input, &payload), DecodeStatus::kIncomplete);
  EXPECT_EQ(payload, "two");
  EXPECT_EQ(input.ReadableBytes(), 6u);

  input.Append(third.data() + 6, third.size() - 6);
  ASSERT_EQ(FrameCodec::Decode(&input, &payload), DecodeStatus::kComplete);
  EXPECT_EQ(payload, "three");
  EXPECT_EQ(input.ReadableBytes(), 0u);
}

TEST(FrameCodec, RejectsZeroLengthWithoutConsuming) {
  Buffer input;
  input.Append(std::string(4, '\0'));
  std::string payload = "unchanged";
  EXPECT_EQ(FrameCodec::Decode(&input, &payload), DecodeStatus::kInvalid);
  EXPECT_EQ(input.ReadableBytes(), 4u);
  EXPECT_EQ(payload, "unchanged");
}

TEST(FrameCodec, RejectsOversizedLengthAsSoonAsHeaderArrives) {
  for (const std::string& header :
       {std::string("\0\1\0\1", 4), std::string("\xff\xff\xff\xff", 4)}) {
    Buffer input;
    input.Append(header);
    std::string payload = "unchanged";
    EXPECT_EQ(FrameCodec::Decode(&input, &payload), DecodeStatus::kInvalid);
    EXPECT_EQ(input.ReadableBytes(), 4u);
    EXPECT_EQ(payload, "unchanged");
  }
}

TEST(FrameCodec, AcceptsMaximumPayloadAtExactBoundary) {
  std::string payload(FrameCodec::kMaxPayloadBytes, 'x');
  payload[1] = '\0';
  const std::string frame = FrameCodec::Encode(payload);
  EXPECT_EQ(frame.substr(0, 4), std::string("\0\1\0\0", 4));
  Buffer input;
  input.Append(frame.data(), frame.size() - 1);
  std::string decoded;
  EXPECT_EQ(FrameCodec::Decode(&input, &decoded), DecodeStatus::kIncomplete);
  input.Append(frame.data() + frame.size() - 1, 1);
  ASSERT_EQ(FrameCodec::Decode(&input, &decoded), DecodeStatus::kComplete);
  EXPECT_EQ(decoded, payload);
  EXPECT_EQ(input.ReadableBytes(), 0u);
}

TEST(FrameCodec, RejectsInvalidPayloadLengthsOnEncode) {
  EXPECT_THROW(FrameCodec::Encode(std::string_view{}), std::invalid_argument);
  EXPECT_THROW(FrameCodec::Encode(std::string(FrameCodec::kMaxPayloadBytes + 1, 'x')),
               std::invalid_argument);
}
