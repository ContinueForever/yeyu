#include "rpc/frame_codec.h"

#include <arpa/inet.h>

#include <cstdint>
#include <cstring>
#include <stdexcept>

namespace yewukv::rpc {

FrameCodec::DecodeStatus FrameCodec::Decode(net::Buffer* input, std::string* payload) {
  if (input->ReadableBytes() < kHeaderBytes) return DecodeStatus::kIncomplete;

  uint32_t network_length;
  std::memcpy(&network_length, input->Peek(), kHeaderBytes);
  const size_t length = ntohl(network_length);
  if (length == 0 || length > kMaxPayloadBytes) return DecodeStatus::kInvalid;
  if (input->ReadableBytes() - kHeaderBytes < length) return DecodeStatus::kIncomplete;

  payload->assign(input->Peek() + kHeaderBytes, length);
  input->Consume(kHeaderBytes + length);
  return DecodeStatus::kComplete;
}

std::string FrameCodec::Encode(std::string_view payload) {
  if (payload.empty() || payload.size() > kMaxPayloadBytes) {
    throw std::invalid_argument("RPC payload must contain 1..65536 bytes");
  }
  const uint32_t network_length = htonl(static_cast<uint32_t>(payload.size()));
  std::string frame(kHeaderBytes, '\0');
  std::memcpy(frame.data(), &network_length, kHeaderBytes);
  frame.append(payload.data(), payload.size());
  return frame;
}

}  // namespace yewukv::rpc
