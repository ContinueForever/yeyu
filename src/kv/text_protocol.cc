#include "kv/text_protocol.h"

#include <algorithm>

namespace yewukv::kv {

namespace {
std::string_view TrimCr(std::string_view s) {
  if (!s.empty() && s.back() == '\r') s.remove_suffix(1);
  return s;
}
}  // namespace

size_t TryParse(std::string_view buf, Command* cmd) {
  size_t nl = buf.find('\n');
  if (nl == std::string_view::npos) return 0;

  std::string_view line = TrimCr(buf.substr(0, nl));
  auto reply_err = [&] {
    cmd->type = Command::Type::kInvalid;
    return nl + 1;
  };

  auto sp = line.find(' ');
  std::string_view op = (sp == std::string_view::npos) ? line : line.substr(0, sp);
  std::string_view rest = (sp == std::string_view::npos) ? "" : line.substr(sp + 1);

  if (op == "PING") {
    cmd->type = Command::Type::kPing;
  } else if (op == "GET" || op == "DEL") {
    rest = TrimCr(rest);
    if (rest.empty() || rest.find(' ') != std::string_view::npos) return reply_err();
    cmd->type = (op == "GET") ? Command::Type::kGet : Command::Type::kDel;
    cmd->key = std::string(rest);
  } else if (op == "PUT") {
    size_t gap = rest.find(' ');
    if (gap == std::string_view::npos || gap == 0) return reply_err();
    cmd->type = Command::Type::kPut;
    cmd->key = std::string(rest.substr(0, gap));
    cmd->value = std::string(rest.substr(gap + 1));
  } else {
    return reply_err();
  }
  return nl + 1;
}

}  // namespace yewukv::kv
