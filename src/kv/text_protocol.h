#pragma once
#include <string>
#include <string_view>

namespace yewukv::kv {

// Minimal line-based text protocol used by the M1 vertical slice:
//   PUT <key> <value>\n
//   GET <key>\n
//   DEL <key>\n
//   PING\n
// The text endpoint remains useful for local bring-up and regression tests.
struct Command {
  enum class Type { kPut, kGet, kDel, kPing, kInvalid };
  Type type = Type::kInvalid;
  std::string key;
  std::string value;
};

// Attempt to parse one complete "\n"-terminated command from [data, data+len).
// On success returns consumed bytes (including '\n') and fills cmd; returns 0
// if no complete line is buffered yet (half-packet case).
size_t TryParse(std::string_view buf, Command* cmd);

}  // namespace yewukv::kv
