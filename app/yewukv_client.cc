// yewukv-client [--host 127.0.0.1] [--port 9000] [--timeout-ms 2000]
//               ping | put KEY VALUE | get KEY | del KEY
#include <charconv>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

#include "rpc/client.h"

namespace {
struct Options {
  std::string host = "127.0.0.1";
  uint16_t port = 9000;
  int timeout_ms = 2000;
  std::vector<std::string> command;
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
    if (arg == "--host" || arg == "--port" || arg == "--timeout-ms") {
      if (index + 1 == argc) throw std::invalid_argument("missing value for " + arg);
      std::string value = argv[++index];
      if (arg == "--host") {
        options.host = std::move(value);
      } else if (arg == "--port") {
        options.port = static_cast<uint16_t>(ParseNumber(value, 1, 65535, arg));
      } else {
        options.timeout_ms = ParseNumber(value, 1, 60000, arg);
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
  } else if (command.size() == 3 && command[0] == "put") {
    request.mutable_put()->set_key(command[1]);
    request.mutable_put()->set_value(command[2]);
  } else if (command.size() == 2 && command[0] == "get") {
    request.mutable_get()->set_key(command[1]);
  } else if (command.size() == 2 && command[0] == "del") {
    request.mutable_del()->set_key(command[1]);
  } else {
    throw std::invalid_argument(
        "usage: yewukv-client [--host IP] [--port N] "
        "[--timeout-ms N] ping | put KEY VALUE | get KEY | del KEY");
  }
  return request;
}
}  // namespace

int main(int argc, char** argv) {
  try {
    Options options = ParseArgs(argc, argv);
    auto request = MakeRequest(options.command);
    yewukv::rpc::KvClient client(options.host, options.port);
    auto result =
        client.CallAsync(std::move(request), std::chrono::milliseconds(options.timeout_ms)).get();
    if (!result.ok()) {
      std::cerr << result.status().ToString() << '\n';
      return 1;
    }
    const auto& response = result.value();
    if (response.code() == yewukv::rpc::KvResponse::NOT_FOUND) {
      std::cerr << "not found\n";
      return 2;
    }
    if (response.code() != yewukv::rpc::KvResponse::OK) {
      std::cerr << (response.error().empty() ? "RPC error" : response.error()) << '\n';
      return 1;
    }
    if (options.command[0] == "get") {
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
