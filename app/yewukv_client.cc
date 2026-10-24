// yewukv-client [--host 127.0.0.1] [--port 9000] [--timeout-ms 2000]
//               ping | put KEY VALUE | get KEY | del KEY
#include <charconv>
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "rpc/client.h"

namespace {
struct Options {
  std::string host = "127.0.0.1";
  uint16_t port = 9000;
  int timeout_ms = 2000;
  std::vector<std::string> command;
  std::vector<std::pair<std::string, uint16_t>> nodes;
};

int ParseNumber(const std::string& text, int min, int max, const std::string& option) {
  int value = 0;
  auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), value);
  if (error != std::errc{} || end != text.data() + text.size() || value < min || value > max) {
    throw std::invalid_argument("invalid " + option + ": " + text);
  }
  return value;
}

Options ParseArgs(int argc, char** argv) {
  Options options;
  for (int index = 1; index < argc; ++index) {
    std::string arg = argv[index];
    if (arg == "--host" || arg == "--port" || arg == "--timeout-ms" || arg == "--nodes") {
      if (index + 1 == argc) throw std::invalid_argument("missing value for " + arg);
      std::string value = argv[++index];
      if (arg == "--host") {
        options.host = std::move(value);
      } else if (arg == "--port") {
        options.port = static_cast<uint16_t>(ParseNumber(value, 1, 65535, arg));
      } else if (arg == "--timeout-ms") {
        options.timeout_ms = ParseNumber(value, 1, 60000, arg);
      } else {
        size_t offset = 0;
        while (offset < value.size()) {
          const auto end = value.find(',', offset);
          const auto address = value.substr(offset, end - offset);
          const auto colon = address.find(':');
          if (colon == std::string::npos || colon == 0)
            throw std::invalid_argument("--nodes requires IPv4:port,IPv4:port,...");
          options.nodes.emplace_back(
              address.substr(0, colon),
              static_cast<uint16_t>(ParseNumber(address.substr(colon + 1), 1, 65535, arg)));
          if (end == std::string::npos) break;
          offset = end + 1;
        }
        if (options.nodes.empty()) throw std::invalid_argument("--nodes is empty");
      }
    } else {
      for (; index < argc; ++index) options.command.emplace_back(argv[index]);
      break;
    }
  }
  return options;
}

yewukv::rpc::KvRequest MakeRequest(const std::vector<std::string>& command) {
  using yewukv::rpc::KvRequest;
  KvRequest request;
  if (command.size() == 1 && command[0] == "ping") {
    request.mutable_ping();
  } else if (command.size() == 1 && command[0] == "status") {
    request.mutable_status();
  } else if (command.size() == 3 && command[0] == "put") {
    request.mutable_put()->set_key(command[1]);
    request.mutable_put()->set_value(command[2]);
  } else if (command.size() == 2 && (command[0] == "get" || command[0] == "get-meta")) {
    request.mutable_get()->set_key(command[1]);
  } else if (command.size() == 4 && command[0] == "cas") {
    uint64_t revision = 0;
    const auto& value = command[2];
    const auto [end, error] = std::from_chars(value.data(), value.data() + value.size(), revision);
    if (error != std::errc{} || end != value.data() + value.size())
      throw std::invalid_argument("CAS revision must be an unsigned 64-bit integer");
    request.mutable_cas()->set_key(command[1]);
    request.mutable_cas()->set_expected_revision(revision);
    request.mutable_cas()->set_value(command[3]);
  } else if (command.size() == 2 && command[0] == "del") {
    request.mutable_del()->set_key(command[1]);
  } else {
    throw std::invalid_argument(
        "usage: yewukv-client [--host IP] [--port N] "
        "[--nodes IP:port,IP:port,...] [--timeout-ms N] "
        "ping | status | put KEY VALUE | get KEY | get-meta KEY | del KEY | cas KEY REVISION "
        "VALUE");
  }
  return request;
}

yewukv::Result<yewukv::rpc::KvResponse> Call(const Options& options,
                                             const yewukv::rpc::KvRequest& request) {
  using namespace std::chrono;
  using yewukv::rpc::KvClient;
  using yewukv::rpc::KvResponse;
  if (options.nodes.empty()) {
    KvClient client(options.host, options.port);
    return client.CallAsync(request, milliseconds(options.timeout_ms)).get();
  }
  const auto deadline = steady_clock::now() + milliseconds(options.timeout_ms);
  while (steady_clock::now() < deadline) {
    std::optional<std::pair<std::string, uint16_t>> selected;
    uint64_t selected_term = 0;
    for (const auto& endpoint : options.nodes) {
      auto remaining = duration_cast<milliseconds>(deadline - steady_clock::now());
      if (remaining.count() <= 0) break;
      KvClient client(endpoint.first, endpoint.second);
      yewukv::rpc::KvRequest status;
      status.mutable_status();
      const auto result = client.CallAsync(status, std::min(remaining, milliseconds(150))).get();
      if (result.ok() && result.value().code() == KvResponse::OK && result.value().is_leader() &&
          (!selected || result.value().raft_term() > selected_term)) {
        selected = endpoint;
        selected_term = result.value().raft_term();
      }
    }
    if (selected) {
      auto remaining = duration_cast<milliseconds>(deadline - steady_clock::now());
      if (remaining.count() <= 0) break;
      KvClient client(selected->first, selected->second);
      auto result = client.CallAsync(request, remaining).get();
      // Only NOT_LEADER proves rejection before append. Transport failure or
      // UNAVAILABLE may follow a committed write and must never trigger replay.
      if (!result.ok() || result.value().code() != KvResponse::NOT_LEADER) return result;
    }
    std::this_thread::sleep_for(milliseconds(20));
  }
  return yewukv::Status::TimedOut("no leader discovered before deadline");
}
}  // namespace

int main(int argc, char** argv) {
  try {
    Options options = ParseArgs(argc, argv);
    auto request = MakeRequest(options.command);
    auto result = Call(options, request);
    if (!result.ok()) {
      std::cerr << result.status().ToString() << '\n';
      return 1;
    }
    const auto& response = result.value();
    if (response.code() == yewukv::rpc::KvResponse::CONFLICT) {
      std::cerr << "revision conflict; current revision=" << response.revision() << '\n';
      return 3;
    }
    if (response.code() == yewukv::rpc::KvResponse::NOT_FOUND) {
      std::cerr << "not found\n";
      return 2;
    }
    if (response.code() != yewukv::rpc::KvResponse::OK) {
      std::cerr << (response.error().empty() ? "RPC error" : response.error()) << '\n';
      return 1;
    }
    if (options.command[0] == "get-meta") {
      static constexpr char digits[] = "0123456789abcdef";
      std::cout << "{\"revision\":" << response.revision() << ",\"value_hex\":\"";
      for (unsigned char byte : response.value())
        std::cout << digits[byte >> 4] << digits[byte & 15];
      std::cout << "\"}\n";
    } else if (options.command[0] == "cas") {
      std::cout << "{\"revision\":" << response.revision() << "}\n";
    } else if (options.command[0] == "get" || options.command[0] == "status") {
      std::cout << response.value() << '\n';
    } else {
      std::cout << (options.command[0] == "ping" ? "PONG" : "OK") << '\n';
    }
    return 0;
  } catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    return 1;
  }
}
