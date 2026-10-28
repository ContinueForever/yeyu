#include "cluster/read_trace.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <initializer_list>
#include <sstream>
#include <stdexcept>
#include <utility>

#include "common/thread_id.h"

namespace yewukv::cluster {
namespace {
using Clock = std::chrono::steady_clock;

std::string Fields(const char* kind, uint64_t node,
                   std::initializer_list<std::pair<const char*, uint64_t>> fields) {
  std::ostringstream out;
  out << "{\"event\":\"" << kind << "\",\"node\":" << node;
  for (const auto& [name, value] : fields) out << ",\"" << name << "\":" << value;
  out << "}\n";
  return out.str();
}
}  // namespace

uint64_t TraceNow() {
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now().time_since_epoch())
          .count());
}

ReadTraceRecorder::ReadTraceRecorder(std::string prefix, uint64_t node, Options options)
    : options_(options), node_(node), control_(prefix + ".enabled") {
  if (prefix.empty() || node == 0 || options.queue_capacity == 0 || options.max_events == 0 ||
      options.max_bytes < 4096 || options.sample_every == 0)
    throw std::invalid_argument("invalid read trace options");
  path_ = prefix + "-" + std::to_string(::getpid()) + "-" + std::to_string(TraceNow()) + ".jsonl";
  fd_ = ::open(path_.c_str(), O_CREAT | O_EXCL | O_WRONLY | O_CLOEXEC | O_NOFOLLOW, 0600);
  if (fd_ < 0) throw std::runtime_error("create read trace file");
  try {
    CheckControl();
    const auto realtime =
        static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                  std::chrono::system_clock::now().time_since_epoch())
                                  .count());
    if (!Write(Fields("metadata", node_,
                      {{"schema", 2},
                       {"pid", static_cast<uint64_t>(::getpid())},
                       {"steady_ns", TraceNow()},
                       {"realtime_ns", realtime},
                       {"enabled", enabled_.load()},
                       {"sample_every", options_.sample_every},
                       {"slow_ns", options_.slow_ns},
                       {"max_events", options_.max_events},
                       {"max_bytes", options_.max_bytes}})))
      throw std::runtime_error("write read trace metadata");
    metadata_written_ = true;
    writer_ = std::thread([this] { Run(); });
  } catch (...) {
    ::close(fd_);
    fd_ = -1;
    throw;
  }
}

ReadTraceRecorder::~ReadTraceRecorder() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    stopping_ = true;
  }
  ready_.notify_one();
  if (writer_.joinable()) writer_.join();
  if (fd_ >= 0) ::close(fd_);
}

bool ReadTraceRecorder::KeepRead(uint64_t ordinal, const ReadTrace& trace) const {
  return Enabled() && (ordinal % options_.sample_every == 0 ||
                       (trace.ready_ns >= trace.received_ns &&
                        trace.ready_ns - trace.received_ns >= options_.slow_ns));
}

void ReadTraceRecorder::Record(ReadTrace trace) noexcept {
  Enqueue(std::move(trace));
}
void ReadTraceRecorder::Record(PeerTrace trace) noexcept {
  Enqueue(std::move(trace));
}

void ReadTraceRecorder::Record(raft::PersistenceTrace trace) noexcept {
  if (!Enabled()) return;
  const bool checkpoint = trace.kind == static_cast<uint64_t>(raft::PersistenceKind::kImage) ||
                          trace.kind == static_cast<uint64_t>(raft::PersistenceKind::kCheckpoint);
  if (!checkpoint && trace.success && trace.sequence % options_.sample_every != 0 &&
      (trace.finished_ns < trace.started_ns ||
       trace.finished_ns - trace.started_ns < options_.slow_ns))
    return;
  trace.tid = CurrentThreadId();
  if (trace.span_count > trace.spans.size()) {
    trace.span_count = trace.spans.size();
    trace.spans_limited = 1;
  }
  Enqueue(std::move(trace));
}

void ReadTraceRecorder::Enqueue(Event event) noexcept {
  try {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!Enabled() || stopping_) return;
    if (events_.size() >= options_.queue_capacity || accepted_ >= options_.max_events) {
      ++dropped_;
      if (accepted_ >= options_.max_events) limited_ = true;
      return;
    }
    events_.push_back(std::move(event));
    ++accepted_;
    ready_.notify_one();
  } catch (...) {
    ++dropped_;
    failed_ = true;
  }
}

void ReadTraceRecorder::CheckControl() {
  const int control = ::open(control_.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK);
  if (control < 0) return;
  struct stat info {};
  char value = 0;
  if (::fstat(control, &info) == 0 && S_ISREG(info.st_mode) && ::read(control, &value, 1) == 1 &&
      (value == '0' || value == '1')) {
    const bool active = value == '1';
    if (enabled_.exchange(active) != active && metadata_written_ && !limited_) {
      const auto line = Fields("mode", node_, {{"steady_ns", TraceNow()}, {"enabled", active}});
      if (bytes_ + line.size() <= options_.max_bytes)
        Write(line);
      else
        limited_ = true;
    }
  }
  ::close(control);
}

bool ReadTraceRecorder::Write(const std::string& bytes) {
  if (failed_) return false;
  size_t offset = 0;
  while (offset < bytes.size()) {
    const auto n = ::write(fd_, bytes.data() + offset, bytes.size() - offset);
    if (n > 0)
      offset += static_cast<size_t>(n);
    else if (n < 0 && errno == EINTR)
      continue;
    else {
      failed_ = true;
      return false;
    }
  }
  bytes_ += bytes.size();
  return true;
}

std::string ReadTraceRecorder::Encode(const Event& event) const {
  if (const auto* r = std::get_if<raft::PersistenceTrace>(&event)) {
    auto line = Fields("persist", node_,
                       {{"sequence", r->sequence},
                        {"task_stream", r->task_stream},
                        {"task_sequence", r->task_sequence},
                        {"kind", r->kind},
                        {"tid", r->tid},
                        {"started_ns", r->started_ns},
                        {"finished_ns", r->finished_ns},
                        {"success", r->success},
                        {"bytes", r->bytes},
                        {"crc_ns", r->crc_ns},
                        {"spans_limited", r->spans_limited}});
    line.resize(line.size() - 2);
    std::ostringstream out;
    out << line << ",\"spans\":[";
    for (size_t i = 0; i < r->span_count; ++i) {
      const auto& span = r->spans[i];
      if (i) out << ',';
      out << '[' << span.stage << ',' << span.started_ns << ',' << span.finished_ns << ']';
    }
    out << "]}\n";
    return out.str();
  }
  if (const auto* r = std::get_if<ReadTrace>(&event))
    return Fields("read", node_,
                  {{"request_id", r->request_id},
                   {"client_port", r->client_port},
                   {"term", r->term},
                   {"context", r->context},
                   {"confirmed_by", r->confirmed_by},
                   {"confirmed_sequence", r->confirmed_sequence},
                   {"index", r->index},
                   {"minimum_index", r->minimum_index},
                   {"persistence_stream", r->persistence_stream},
                   {"persistence_sequence", r->persistence_sequence},
                   {"code", r->code},
                   {"received_tid", r->received_tid},
                   {"handled_tid", r->handled_tid},
                   {"tcp_tid", r->tcp_tid},
                   {"received_ns", r->received_ns},
                   {"handled_ns", r->handled_ns},
                   {"context_ns", r->context_ns},
                   {"quorum_ns", r->quorum_ns},
                   {"ready_ns", r->ready_ns},
                   {"response_queued_ns", r->response_queued_ns},
                   {"tcp_loop_ns", r->tcp_loop_ns},
                   {"tcp_dispatch_done_ns", r->tcp_dispatch_done_ns}});
  const auto& r = std::get<PeerTrace>(event);
  return Fields("peer", node_,
                {{"peer_id", r.peer_id},
                 {"sequence", r.sequence},
                 {"context", r.context},
                 {"term", r.term},
                 {"request_id", r.request_id},
                 {"success", r.success},
                 {"scheduled_tid", r.scheduled_tid},
                 {"worker_tid", r.worker_tid},
                 {"rpc_tid", r.rpc_tid},
                 {"node_tid", r.node_tid},
                 {"scheduled_ns", r.scheduled_ns},
                 {"worker_ns", r.worker_ns},
                 {"call_ns", r.call_ns},
                 {"first_write_ns", r.first_write_ns},
                 {"write_done_ns", r.write_done_ns},
                 {"decoded_ns", r.decoded_ns},
                 {"returned_ns", r.returned_ns},
                 {"node_received_ns", r.node_received_ns}});
}

void ReadTraceRecorder::Run() {
  auto next_control = Clock::now();
  try {
    for (;;) {
      std::deque<Event> batch;
      bool stop = false;
      {
        std::unique_lock<std::mutex> lock(mutex_);
        ready_.wait_for(lock, std::chrono::milliseconds(250),
                        [this] { return stopping_ || !events_.empty(); });
        batch.swap(events_);
        stop = stopping_;
      }
      if (Clock::now() >= next_control) {
        CheckControl();
        next_control = Clock::now() + std::chrono::milliseconds(250);
      }
      std::string output;
      uint64_t output_events = 0;
      for (const auto& event : batch) {
        auto line = Encode(event);
        if (bytes_ + output.size() + line.size() > options_.max_bytes) {
          limited_ = true;
          ++dropped_;
          continue;
        }
        output += line;
        ++output_events;
      }
      if (!output.empty()) {
        if (Write(output))
          written_ += output_events;
        else
          dropped_ += output_events;
      }
      if (stop) break;
    }
    Write(Fields("summary", node_,
                 {{"steady_ns", TraceNow()},
                  {"accepted", accepted_},
                  {"written", written_},
                  {"dropped", dropped_},
                  {"limited", limited_},
                  {"io_failed", failed_}}));
  } catch (...) {
    failed_ = true;
  }
}

}  // namespace yewukv::cluster
