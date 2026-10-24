// yewukv-server --port 9000 --io-threads 4 --protocol text|rpc
//               --idle-timeout 60 --storage-queue 4096 --data-dir data
#include <sys/signalfd.h>
#include <unistd.h>

#include <cerrno>
#include <charconv>
#include <csignal>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <system_error>
#include <utility>

#include "kv/server.h"
#include "rpc/kv_server.h"
#include "storage/wal_engine.h"

namespace {
struct Options {
  uint16_t port = 9000;
  int io_threads = 4;
  int idle_timeout_seconds = 60;
  int storage_queue = 4096;
  std::string data_directory = "data";
  std::string protocol = "text";
};

int ParseNumber(const std::string& text, int min, int max, const std::string& option) {
  int value = 0;
  auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), value);
  if (error != std::errc{} || end != text.data() + text.size() || value < min || value > max) {
    throw std::invalid_argument(option + " must be an integer in [" + std::to_string(min) + ", " +
                                std::to_string(max) + "]");
  }
  return value;
}

Options ParseArgs(int argc, char** argv) {
  Options options;
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg != "--port" && arg != "--io-threads" && arg != "--protocol" &&
        arg != "--idle-timeout" && arg != "--storage-queue" && arg != "--data-dir") {
      throw std::invalid_argument("unknown option: " + arg);
    }
    if (i + 1 == argc) throw std::invalid_argument("missing value for " + arg);
    std::string value = argv[++i];
    if (arg == "--port") {
      options.port = static_cast<uint16_t>(ParseNumber(value, 1, 65535, arg));
    } else if (arg == "--io-threads") {
      options.io_threads = ParseNumber(value, 0, 64, arg);
    } else if (arg == "--storage-queue") {
      options.storage_queue = ParseNumber(value, 1, 8192, arg);
    } else if (arg == "--data-dir") {
      if (value.empty()) throw std::invalid_argument("--data-dir must not be empty");
      options.data_directory = std::move(value);
    } else if (arg == "--idle-timeout") {
      options.idle_timeout_seconds = ParseNumber(value, 0, 86400, arg);
    } else {
      if (value != "text" && value != "rpc") {
        throw std::invalid_argument("--protocol must be text or rpc");
      }
      options.protocol = std::move(value);
    }
  }
  return options;
}

class ShutdownSignal {
 public:
  ShutdownSignal() {
    sigset_t signals;
    ::sigemptyset(&signals);
    ::sigaddset(&signals, SIGINT);
    ::sigaddset(&signals, SIGTERM);
    // Workers inherit the mask. Readiness in the base loop handles shutdown;
    // no C++ objects, locks or allocations are touched in a signal handler.
    if (::sigprocmask(SIG_BLOCK, &signals, nullptr) < 0) {
      throw std::system_error(errno, std::generic_category(), "sigprocmask");
    }
    fd_ = ::signalfd(-1, &signals, SFD_NONBLOCK | SFD_CLOEXEC);
    if (fd_ < 0) throw std::system_error(errno, std::generic_category(), "signalfd");
  }
  ~ShutdownSignal() { ::close(fd_); }
  ShutdownSignal(const ShutdownSignal&) = delete;
  ShutdownSignal& operator=(const ShutdownSignal&) = delete;
  int Fd() const { return fd_; }

 private:
  int fd_ = -1;
};
}  // namespace

int main(int argc, char** argv) {
  try {
    Options options = ParseArgs(argc, argv);
    ShutdownSignal shutdown_signal;
    auto engine = std::make_shared<yewukv::storage::WalEngine>(options.data_directory);
    if (options.protocol == "rpc") {
      yewukv::rpc::RpcKvServer server(options.port, options.io_threads, engine,
                                      std::chrono::seconds{options.idle_timeout_seconds},
                                      static_cast<size_t>(options.storage_queue));
      server.Start(shutdown_signal.Fd());
    } else {
      yewukv::kv::KvServer server(options.port, options.io_threads, engine,
                                  std::chrono::seconds{options.idle_timeout_seconds},
                                  static_cast<size_t>(options.storage_queue));
      server.Start(shutdown_signal.Fd());
    }
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "yewukv: " << error.what() << '\n';
    return 1;
  }
}
