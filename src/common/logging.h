#pragma once
// Minimal structured logging. Replace with spdlog later if desired.
#include <cstdio>
#include <cstring>
#include <ctime>
#include <string>
#include <string_view>

namespace yewukv::log {

enum class Level { kDebug, kInfo, kWarn, kError };

inline Level& CurrentLevel() {
  static Level level = Level::kInfo;
  return level;
}

inline void SetLevel(Level l) {
  CurrentLevel() = l;
}

inline void Emit(Level l, std::string_view tag, const std::string& msg) {
  static const char* tags[] = {"DEBUG", "INFO ", "WARN ", "ERROR"};
  struct timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  struct tm tm;
  localtime_r(&ts.tv_sec, &tm);
  char tbuf[16];
  strftime(tbuf, sizeof(tbuf), "%H:%M:%S", &tm);
  FILE* out = (l == Level::kError) ? stderr : stdout;
  fprintf(out, "%s.%03ld %s [%s] %s\n", tbuf, ts.tv_nsec / 1000000, tags[static_cast<int>(l)],
          tag.data(), msg.c_str());
  fflush(out);
}

}  // namespace yewukv::log

#define LOG_INFO(tag, msg) ::yewukv::log::Emit(::yewukv::log::Level::kInfo, tag, msg)
#define LOG_WARN(tag, msg) ::yewukv::log::Emit(::yewukv::log::Level::kWarn, tag, msg)
#define LOG_ERROR(tag, msg) ::yewukv::log::Emit(::yewukv::log::Level::kError, tag, msg)
#define LOG_DEBUG(tag, msg) ::yewukv::log::Emit(::yewukv::log::Level::kDebug, tag, msg)
