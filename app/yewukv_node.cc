#include <arpa/inet.h>
#include <sys/signalfd.h>
#include <unistd.h>

#include <charconv>
#include <csignal>
#include <iostream>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>

#include "cluster/server.h"

namespace {
uint64_t Number(const std::string& text, uint64_t maximum) {
  uint64_t value = 0;
  const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), value);
  if (error != std::errc{} || end != text.data() + text.size() || value == 0 || value > maximum)
    throw std::invalid_argument("invalid numeric option: " + text);
  return value;
}
}  // namespace

int main(int argc, char** argv) {
  try {
    yewukv::cluster::Options options;
    for (int i = 1; i < argc; ++i) {
      const std::string option = argv[i];
      if (i + 1 == argc) throw std::invalid_argument("missing option value");
      const std::string value = argv[++i];
      if (option == "--id")
        options.id = Number(value, UINT64_MAX);
      else if (option == "--port")
        options.port = static_cast<uint16_t>(Number(value, 65535));
      else if (option == "--data-dir")
        options.directory = value;
      else if (option == "--seed")
        options.seed = static_cast<uint32_t>(Number(value, UINT32_MAX));
      else if (option == "--request-timeout-ms")
        options.request_timeout_ms = static_cast<int>(Number(value, 60000));
      else if (option == "--proposal-batch-size")
        options.proposal_batch_size = static_cast<size_t>(Number(value, 32));
      else if (option == "--snapshot-entries")
        options.snapshot_entries = static_cast<size_t>(Number(value, 1000000000));
      else if (option == "--snapshot-bytes")
        options.snapshot_bytes = static_cast<size_t>(Number(value, 64 * 1024 * 1024));
      else if (option == "--max-state-bytes")
        options.max_state_bytes = static_cast<size_t>(Number(value, 64 * 1024 * 1024));
      else if (option == "--event-queue")
        options.event_queue_capacity = static_cast<size_t>(Number(value, 8192));
      else if (option == "--pending-limit")
        options.pending_capacity = static_cast<size_t>(Number(value, 8192));
      else if (option == "--journal-checkpoint-bytes")
        options.journal_checkpoint_bytes = static_cast<size_t>(Number(value, 64 * 1024 * 1024));
      else if (option == "--snapshot-policy") {
        if (value != "adaptive" && value != "fixed")
          throw std::invalid_argument("--snapshot-policy must be adaptive or fixed");
        options.adaptive_snapshots = value == "adaptive";
      } else if (option == "--raft-store") {
        if (value != "journal" && value != "image")
          throw std::invalid_argument("--raft-store must be journal or image");
        options.journal = value == "journal";
      } else if (option == "--read-mode") {
        if (value != "read-index" && value != "log")
          throw std::invalid_argument("--read-mode must be read-index or log");
        options.read_index = value == "read-index";
      } else if (option == "--members") {
        std::stringstream input(value);
        std::string member;
        while (std::getline(input, member, ',')) {
          const auto at = member.find('@');
          const auto colon = member.find(':');
          if (at == std::string::npos || colon == std::string::npos || colon <= at + 1)
            throw std::invalid_argument("members format: 1@127.0.0.1:9101,2@127.0.0.1:9102,...");
          const auto id = Number(member.substr(0, at), UINT64_MAX);
          const auto host = member.substr(at + 1, colon - at - 1);
          in_addr address{};
          if (::inet_pton(AF_INET, host.c_str(), &address) != 1)
            throw std::invalid_argument("member address must be numeric IPv4");
          const auto port = static_cast<uint16_t>(Number(member.substr(colon + 1), 65535));
          if (!options.members.emplace(id, yewukv::cluster::Endpoint{host, port}).second)
            throw std::invalid_argument("duplicate member ID");
        }
      } else
        throw std::invalid_argument("unknown option: " + option);
    }
    std::set<std::pair<std::string, uint16_t>> endpoints;
    for (const auto& [id, endpoint] : options.members)
      endpoints.emplace(endpoint.host, endpoint.port);
    if (options.members.size() != 3 || endpoints.size() != 3 ||
        options.members.count(options.id) == 0 || options.port == 0 || options.directory.empty())
      throw std::invalid_argument("provide --id --port --data-dir and three unique --members");
    sigset_t signals;
    ::sigemptyset(&signals);
    ::sigaddset(&signals, SIGINT);
    ::sigaddset(&signals, SIGTERM);
    if (::sigprocmask(SIG_BLOCK, &signals, nullptr) != 0)
      throw std::runtime_error("block shutdown signals");
    const int stop = ::signalfd(-1, &signals, SFD_NONBLOCK | SFD_CLOEXEC);
    if (stop < 0) throw std::runtime_error("create shutdown signalfd");
    yewukv::cluster::Server server(std::move(options));
    const bool healthy = server.Run(stop);
    ::close(stop);
    return healthy ? 0 : 1;
  } catch (const std::exception& error) {
    std::cerr << "yewukv-node: " << error.what() << '\n';
    return 1;
  }
}
