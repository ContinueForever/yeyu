#pragma once

#include <cstddef>
#include <string>
#include <string_view>

#include "net/buffer.h"

namespace yewukv::rpc {

// Four-byte network-order payload length followed by opaque payload bytes.
// Protobuf parsing and request validation belong to the caller.
class FrameCodec {
 public:
  static constexpr size_t kHeaderBytes = 4;
  static constexpr size_t kMaxPayloadBytes = 64 * 1024;

  enum class DecodeStatus { kComplete, kIncomplete, kInvalid };

  // Consume exactly one frame on success. Incomplete or invalid input leaves
  // both the buffer and payload unchanged; invalid lengths should close the peer.
  static DecodeStatus Decode(net::Buffer* input, std::string* payload);

  // Throw std::invalid_argument for empty payloads or payloads over the limit.
  static std::string Encode(std::string_view payload);
};

}  // namespace yewukv::rpc
