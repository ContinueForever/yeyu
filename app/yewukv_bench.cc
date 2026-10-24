// Bounded Linux TCP load generator. One connection and in-flight call per worker.
#include <arpa/inet.h>
#include <poll.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <condition_variable>
#include <csignal>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "kv.pb.h"

namespace {
using Clock = std::chrono::steady_clock;
using namespace std::chrono_literals;
using yewukv::rpc::KvRequest;
using yewukv::rpc::KvResponse;
std::atomic<bool> cancelled{false};
static_assert(std::atomic<bool>::is_always_lock_free,
              "signal cancellation requires lock-free atomics");
uint64_t Ns(Clock::duration elapsed) {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed).count();
}
struct Options {
  std::string host = "127.0.0.1", prefix, output;
  int port = 19101, clients = 8, duration_ms = 1000, warmup_ms = 1000;
  int write_percent = 50, value_size = 128, offered_rps = 0, max_samples = 1000000;
};
int Number(const std::string& text, int low, int high) {
  int result = 0;
  auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), result);
  if (error != std::errc{} || end != text.data() + text.size() || result < low || result > high)
    throw std::invalid_argument("invalid numeric option");
  return result;
}
Options Parse(int argc, char** argv) {
  Options o;
  for (int i = 1; i < argc; i += 2) {
    if (i + 1 == argc) throw std::invalid_argument("options require values");
    const std::string arg = argv[i], v = argv[i + 1];
    if (arg == "--host")
      o.host = v;
    else if (arg == "--prefix")
      o.prefix = v;
    else if (arg == "--output")
      o.output = v;
    else if (arg == "--port")
      o.port = Number(v, 1, 65535);
    else if (arg == "--clients")
      o.clients = Number(v, 1, 128);
    else if (arg == "--duration-ms")
      o.duration_ms = Number(v, 100, 3600000);
    else if (arg == "--warmup-ms")
      o.warmup_ms = Number(v, 0, 60000);
    else if (arg == "--write-percent")
      o.write_percent = Number(v, 0, 100);
    else if (arg == "--value-size")
      o.value_size = Number(v, 1, 8192);
    else if (arg == "--offered-rps")
      o.offered_rps = Number(v, 0, 1000000);
    else if (arg == "--max-samples")
      o.max_samples = Number(v, 128, 10000000);
    else
      throw std::invalid_argument("unknown option: " + arg);
  }
  in_addr address{};
  if (::inet_pton(AF_INET, o.host.c_str(), &address) != 1 || o.output.empty() || o.prefix.empty() ||
      o.prefix.size() > 64 ||
      o.prefix.find_first_not_of(
          "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_") != std::string::npos)
    throw std::invalid_argument("require numeric IPv4, new output directory and safe --prefix");
  return o;
}
class Connection {
 public:
  explicit Connection(const Options& o) {
    fd_ = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC | SOCK_NONBLOCK, 0);
    if (fd_ < 0) throw std::runtime_error("socket");
    sockaddr_in target{};
    target.sin_family = AF_INET;
    target.sin_port = htons(static_cast<uint16_t>(o.port));
    ::inet_pton(AF_INET, o.host.c_str(), &target.sin_addr);
    try {
      const auto deadline = Clock::now() + 2s;
      if (::connect(fd_, reinterpret_cast<sockaddr*>(&target), sizeof(target)) != 0) {
        if (errno != EINPROGRESS) throw std::runtime_error("connect");
        Wait(POLLOUT, deadline);
        int error = 0;
        socklen_t size = sizeof(error);
        if (::getsockopt(fd_, SOL_SOCKET, SO_ERROR, &error, &size) != 0 || error)
          throw std::runtime_error("connect completion");
      }
      int one = 1;
      if (::setsockopt(fd_, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one)) != 0)
        throw std::runtime_error("TCP_NODELAY");
    } catch (...) {
      ::close(fd_);
      throw;
    }
  }
  ~Connection() { ::close(fd_); }
  Connection(const Connection&) = delete;
  Connection& operator=(const Connection&) = delete;
  KvResponse Call(const std::string& key, const std::string& value, bool put) {
    KvRequest request;
    request.set_request_id(++id_);
    if (put) {
      request.mutable_put()->set_key(key);
      request.mutable_put()->set_value(value);
    } else
      request.mutable_get()->set_key(key);
    const auto payload = request.SerializeAsString();
    const auto length = htonl(static_cast<uint32_t>(payload.size()));
    std::string bytes(reinterpret_cast<const char*>(&length), sizeof(length));
    bytes += payload;
    const auto deadline = Clock::now() + 2s;
    Transfer(bytes.data(), bytes.size(), true, deadline);
    uint32_t reply_size = 0;
    Transfer(reinterpret_cast<char*>(&reply_size), 4, false, deadline);
    reply_size = ntohl(reply_size);
    if (!reply_size || reply_size > 65536) throw std::runtime_error("invalid frame");
    std::string reply(reply_size, '\0');
    Transfer(reply.data(), reply.size(), false, deadline);
    KvResponse response;
    if (!response.ParseFromString(reply) || response.request_id() != id_)
      throw std::runtime_error("invalid response");
    return response;
  }

 private:
  void Wait(short event, Clock::time_point deadline) {
    for (;;) {
      const auto remaining =
          std::chrono::duration_cast<std::chrono::milliseconds>(deadline - Clock::now()).count();
      if (remaining <= 0) throw std::runtime_error("RPC deadline");
      pollfd selected{fd_, event, 0};
      const int n = ::poll(&selected, 1, static_cast<int>(remaining));
      if (n > 0) return;
      if (n < 0 && errno == EINTR) continue;
      throw std::runtime_error("RPC poll deadline/error");
    }
  }
  void Transfer(char* bytes, size_t count, bool send, Clock::time_point deadline) {
    while (count) {
      Wait(send ? POLLOUT : POLLIN, deadline);
      const auto n = send ? ::send(fd_, bytes, count, MSG_NOSIGNAL | MSG_DONTWAIT)
                          : ::recv(fd_, bytes, count, MSG_DONTWAIT);
      if (n < 0 && (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK)) continue;
      if (n <= 0) throw std::runtime_error("RPC transfer");
      bytes += n;
      count -= static_cast<size_t>(n);
    }
  }
  int fd_ = -1;
  uint64_t id_ = 0;
};
struct Sample {
  uint64_t latency, scheduled, started, completed;
  bool put;
  int code;
};
struct Worker {
  std::vector<Sample> samples;
  uint64_t offered = 0;
  bool setup_failed = false, limited = false;
};
struct Shared {
  std::mutex mutex;
  std::condition_variable ready;
  int count = 0;
  bool released = false;
  Clock::time_point start;
  std::atomic<bool> abort{false};
};
void SleepUntil(Clock::time_point until, const Shared& shared) {
  while (!cancelled && !shared.abort) {
    const auto remaining = until - Clock::now();
    if (remaining <= Clock::duration::zero()) return;
    std::this_thread::sleep_for(std::min(remaining, Clock::duration(100ms)));
  }
}
void Run(int id, const Options& o, Shared& shared, Worker& output) {
  bool arrived = false;
  try {
    Connection connection(o);
    std::array<std::string, 8> keys, values;
    for (int i = 0; i < 8; ++i) {
      if (cancelled || shared.abort) throw std::runtime_error("cancelled setup");
      keys[i] = o.prefix + "-" + std::to_string(id) + "-" + std::to_string(i);
      values[i] = std::string(o.value_size, static_cast<char>('a' + (id + i) % 26));
      if (connection.Call(keys[i], values[i], true).code() != KvResponse::OK)
        throw std::runtime_error("preload rejected");
    }
    auto until = Clock::now() + std::chrono::milliseconds(o.warmup_ms);
    while (Clock::now() < until) {
      if (cancelled || shared.abort) throw std::runtime_error("cancelled warmup");
      const auto response = connection.Call(keys[0], "", false);
      if (response.code() != KvResponse::OK || response.value() != values[0])
        throw std::runtime_error("warmup validation");
    }
    {
      std::unique_lock<std::mutex> lock(shared.mutex);
      ++shared.count;
      arrived = true;
      shared.ready.notify_all();
      shared.ready.wait(lock, [&] { return shared.released; });
    }
    SleepUntil(shared.start, shared);
    const uint64_t duration = static_cast<uint64_t>(o.duration_ms) * 1000000;
    const bool paced = o.offered_rps != 0;
    uint64_t slot = 0, sequence = 0, random = 0x9e3779b9ULL + id;
    const size_t limit = (static_cast<size_t>(o.max_samples) + o.clients - 1) / o.clients;
    output.samples.reserve(std::min<size_t>(limit, 4096));
    while (!shared.abort && !cancelled) {
      uint64_t scheduled = Ns(Clock::now() - shared.start);
      if (paced) {
        if (slot >= output.offered) break;
        scheduled = (static_cast<uint64_t>(id) + slot * o.clients) * 1000000000ULL / o.offered_rps;
        SleepUntil(shared.start + std::chrono::nanoseconds(scheduled), shared);
      }
      if (cancelled || shared.abort) break;
      const auto begin = Clock::now();
      const uint64_t began = Ns(begin - shared.start);
      if (began >= duration) break;
      if (output.samples.size() == limit) {
        output.limited = true;
        shared.abort = true;
        break;
      }
      random ^= random << 13;
      random ^= random >> 7;
      random ^= random << 17;
      const bool put = random % 100 < static_cast<uint64_t>(o.write_percent);
      const size_t key = (random / 100) % 8;
      std::string value(o.value_size, static_cast<char>('a' + (++sequence % 26)));
      for (size_t i = 0; i < std::min<size_t>(8, value.size()); ++i)
        value[i] = static_cast<char>(sequence >> (8 * i));
      if (value.size() > 8) value[8] = static_cast<char>(id);
      int code = -1;
      try {
        auto response = connection.Call(keys[key], value, put);
        code = static_cast<int>(response.code());
        if (code == 0 && !put && response.value() != values[key]) code = -2;
        if (code == 0 && put) values[key] = std::move(value);
      } catch (...) {
        code = -1;
      }
      const auto end = Clock::now();
      output.samples.push_back(
          {Ns(end - begin), scheduled, began, Ns(end - shared.start), put, code});
      if (code != 0) {
        shared.abort = true;
        break;
      }  // Never replay an unknown write.
      if (paced) {
        const uint64_t next_global =
            ((Ns(end - shared.start) + 1) * o.offered_rps + 999999999ULL) / 1000000000ULL;
        const uint64_t next_owned = next_global > static_cast<uint64_t>(id)
                                        ? (next_global - id + o.clients - 1) / o.clients
                                        : 0;
        slot = std::max(slot + 1, next_owned);
      }
    }
    if (cancelled) shared.abort = true;
    if (!shared.abort) SleepUntil(shared.start + std::chrono::nanoseconds(duration), shared);
    if (!paced) output.offered = output.samples.size();
  } catch (...) {
    output.setup_failed = true;
    shared.abort = true;
    if (!arrived) {
      std::lock_guard<std::mutex> lock(shared.mutex);
      ++shared.count;
      shared.ready.notify_all();
    }
  }
}
}  // namespace

int main(int argc, char** argv) {
  try {
    std::signal(SIGTERM, [](int) { cancelled.store(true, std::memory_order_relaxed); });
    const auto o = Parse(argc, argv);
    if (!std::filesystem::create_directory(o.output))
      throw std::runtime_error("output must be new");
    Shared shared;
    std::vector<Worker> results(o.clients);
    if (o.offered_rps) {
      const uint64_t duration = static_cast<uint64_t>(o.duration_ms) * 1000000;
      const uint64_t total = (duration * o.offered_rps + 999999999ULL) / 1000000000ULL;
      for (int i = 0; i < o.clients; ++i) {
        results[i].offered = static_cast<uint64_t>(i) < total ? (total - 1 - i) / o.clients + 1 : 0;
      }
    }
    std::vector<std::thread> threads;
    try {
      for (int i = 0; i < o.clients; ++i)
        threads.emplace_back(Run, i, std::cref(o), std::ref(shared), std::ref(results[i]));
      std::unique_lock<std::mutex> lock(shared.mutex);
      shared.ready.wait(lock, [&] { return shared.count == o.clients; });
      shared.start = Clock::now() + 100ms;
      shared.released = true;
      shared.ready.notify_all();
    } catch (...) {
      shared.abort = true;
      {
        std::lock_guard<std::mutex> lock(shared.mutex);
        shared.released = true;
      }
      shared.ready.notify_all();
      for (auto& t : threads) t.join();
      throw;
    }
    for (auto& t : threads) t.join();
    uint64_t attempts = 0, failed = 0, offered = 0, completed = 0;
    bool setup_failed = false, limited = false;
    for (int id = 0; id < o.clients; ++id) {
      const auto& worker = results[id];
      setup_failed |= worker.setup_failed;
      limited |= worker.limited;
      const auto planned = o.offered_rps ? worker.offered : worker.samples.size();
      offered += planned;
      std::ofstream file(std::filesystem::path(o.output) /
                         ("client-" + std::to_string(id) + ".json"));
      file << "{\"client\":" << id << ",\"offered_slots\":" << planned << ",\"samples\":[";
      for (size_t i = 0; i < worker.samples.size(); ++i) {
        const auto& s = worker.samples[i];
        if (i) file << ',';
        file << '[' << s.latency << ',' << s.scheduled << ',' << s.started << ',' << s.completed
             << ',' << s.put << ',' << s.code << ']';
        ++attempts;
        failed += s.code != 0;
        completed = std::max(completed, s.completed);
      }
      file << "]}\n";
      if (!file) throw std::runtime_error("write samples");
    }
    const bool valid = !setup_failed && !limited && !shared.abort;
    std::ofstream file(std::filesystem::path(o.output) / "result.json");
    file << "{\"schema\":2,\"valid\":" << (valid ? "true" : "false")
         << ",\"columns\":[\"latency_ns\",\"scheduled_offset_ns\",\"started_offset_ns\","
            "\"completed_offset_ns\",\"put\",\"code\"]"
         << ",\"start_monotonic_ns\":" << Ns(shared.start.time_since_epoch())
         << ",\"max_samples\":" << o.max_samples << ",\"clients\":" << o.clients
         << ",\"duration_ms\":" << o.duration_ms << ",\"warmup_ms\":" << o.warmup_ms
         << ",\"write_percent\":" << o.write_percent << ",\"value_size\":" << o.value_size
         << ",\"offered_rps\":" << o.offered_rps << ",\"prefix\":\"" << o.prefix
         << "\",\"attempts\":" << attempts << ",\"successes\":" << attempts - failed
         << ",\"failures\":" << failed << ",\"offered_slots\":" << offered
         << ",\"missed_slots\":" << offered - attempts << ",\"elapsed_ns\":"
         << std::max(completed, static_cast<uint64_t>(o.duration_ms) * 1000000)
         << ",\"setup_failed\":" << (setup_failed ? "true" : "false")
         << ",\"cancelled\":" << (cancelled ? "true" : "false")
         << ",\"sample_limit_reached\":" << (limited ? "true" : "false") << "}\n";
    if (!file) throw std::runtime_error("write result");
    std::cout << "attempts=" << attempts << " failures=" << failed
              << " missed=" << offered - attempts << '\n';
    return valid ? 0 : 2;
  } catch (const std::exception& e) {
    std::cerr << e.what() << '\n';
    return 1;
  }
}
