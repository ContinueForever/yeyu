#include "cluster/server.h"
#include "raft/persistence_worker.h"

#include "cluster/state_machine.h"
#include "cluster/snapshot_policy.h"
#include "cluster/read_trace.h"
#include "common/thread_id.h"

#include <fcntl.h>
#include <unistd.h>

#include <atomic>
#include <algorithm>
#include <cerrno>
#include <cstdio>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <fstream>
#include <functional>
#include <future>
#include <iostream>
#include <mutex>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <thread>
#include <utility>

#include "net/tcp_server.h"
#include "raft/node.h"
#include "rpc/client.h"
#include "rpc/frame_codec.h"

namespace yewukv::cluster {
namespace {
using Clock = std::chrono::steady_clock;
using namespace std::chrono_literals;
using raft::Message;
using raft::MessageKind;
using rpc::KvRequest;
using rpc::KvResponse;

raft::PeerMessage EncodeMessage(const Message& message) {
  raft::PeerMessage wire;
  wire.set_from(message.from);
  wire.set_to(message.to);
  wire.set_sequence(message.sequence);
  switch (message.kind) {
    case MessageKind::kVote: {
      auto* vote = wire.mutable_vote();
      vote->set_term(message.term);
      vote->set_candidate_id(message.from);
      vote->set_last_log_index(message.last_index);
      vote->set_last_log_term(message.last_term);
      break;
    }
    case MessageKind::kVoteReply:
      wire.mutable_vote_reply()->set_term(message.term);
      wire.mutable_vote_reply()->set_vote_granted(message.granted);
      break;
    case MessageKind::kAppend: {
      auto* append = wire.mutable_append();
      append->set_term(message.term);
      append->set_leader_id(message.from);
      append->set_prev_log_index(message.append.prev_index);
      append->set_prev_log_term(message.append.prev_term);
      append->set_leader_commit(message.append.leader_commit);
      append->set_read_context(message.read_context);
      for (const auto& entry : message.append.entries) {
        auto* encoded = append->add_entries();
        encoded->set_index(entry.index);
        encoded->set_term(entry.term);
        encoded->set_data(entry.command);
      }
      break;
    }
    case MessageKind::kAppendReply:
      wire.mutable_append_reply()->set_term(message.term);
      wire.mutable_append_reply()->set_success(message.success);
      wire.mutable_append_reply()->set_conflict_index(message.conflict_index);
      wire.mutable_append_reply()->set_conflict_term(message.conflict_term);
      wire.mutable_append_reply()->set_match_index(message.match_index);
      wire.mutable_append_reply()->set_read_context(message.read_context);
      break;
    case MessageKind::kSnapshot: {
      auto* snapshot = wire.mutable_snapshot();
      snapshot->set_term(message.term);
      snapshot->set_leader_id(message.from);
      snapshot->set_last_included_index(message.snapshot.index);
      snapshot->set_last_included_term(message.snapshot.term);
      snapshot->set_offset(message.snapshot.offset);
      snapshot->set_total_size(message.snapshot.total);
      snapshot->set_data(message.snapshot.data);
      snapshot->set_done(message.snapshot.done);
      break;
    }
    case MessageKind::kSnapshotReply: {
      auto* snapshot = wire.mutable_snapshot_reply();
      snapshot->set_term(message.term);
      snapshot->set_success(message.success);
      snapshot->set_last_included_index(message.snapshot.index);
      snapshot->set_next_offset(message.snapshot.offset);
      snapshot->set_match_index(message.match_index);
      break;
    }
  }
  return wire;
}

std::optional<Message> DecodeMessage(const raft::PeerMessage& wire) {
  Message message;
  message.from = wire.from();
  message.to = wire.to();
  message.sequence = wire.sequence();
  switch (wire.body_case()) {
    case raft::PeerMessage::kVote:
      if (wire.vote().candidate_id() != wire.from() || wire.vote().pre_vote()) return std::nullopt;
      message.kind = MessageKind::kVote;
      message.term = wire.vote().term();
      message.last_index = wire.vote().last_log_index();
      message.last_term = wire.vote().last_log_term();
      break;
    case raft::PeerMessage::kVoteReply:
      message.kind = MessageKind::kVoteReply;
      message.term = wire.vote_reply().term();
      message.granted = wire.vote_reply().vote_granted();
      break;
    case raft::PeerMessage::kAppend:
      if (wire.append().leader_id() != wire.from()) return std::nullopt;
      message.kind = MessageKind::kAppend;
      message.term = wire.append().term();
      message.append.prev_index = wire.append().prev_log_index();
      message.append.prev_term = wire.append().prev_log_term();
      message.append.leader_commit = wire.append().leader_commit();
      message.read_context = wire.append().read_context();
      if (wire.append().entries_size() > static_cast<int>(raft::Node::kMaxAppendEntries))
        return std::nullopt;
      for (const auto& entry : wire.append().entries()) {
        if (entry.type() != raft::Entry::NORMAL) return std::nullopt;
        message.append.entries.push_back({entry.index(), entry.term(), entry.data()});
      }
      break;
    case raft::PeerMessage::kAppendReply:
      message.kind = MessageKind::kAppendReply;
      message.term = wire.append_reply().term();
      message.success = wire.append_reply().success();
      message.match_index = wire.append_reply().match_index();
      message.conflict_index = wire.append_reply().conflict_index();
      message.conflict_term = wire.append_reply().conflict_term();
      message.read_context = wire.append_reply().read_context();
      break;
    case raft::PeerMessage::kSnapshot:
      if (wire.snapshot().leader_id() != wire.from()) return std::nullopt;
      message.kind = MessageKind::kSnapshot;
      message.term = wire.snapshot().term();
      message.snapshot = {wire.snapshot().last_included_index(),
                          wire.snapshot().last_included_term(),
                          wire.snapshot().offset(),
                          wire.snapshot().total_size(),
                          wire.snapshot().data(),
                          wire.snapshot().done()};
      break;
    case raft::PeerMessage::kSnapshotReply:
      message.kind = MessageKind::kSnapshotReply;
      message.term = wire.snapshot_reply().term();
      message.success = wire.snapshot_reply().success();
      message.snapshot.index = wire.snapshot_reply().last_included_index();
      message.snapshot.offset = wire.snapshot_reply().next_offset();
      message.match_index = wire.snapshot_reply().match_index();
      break;
    default:
      return std::nullopt;
  }
  return message;
}

bool ValidCommand(const KvRequest& request) {
  if (request.request_id() == 0 || request.ByteSizeLong() > raft::Node::kMaxCommandBytes ||
      request.state_machine_version() > 2)
    return false;
  switch (request.operation_case()) {
    case KvRequest::kPut:
      return !request.put().key().empty() && request.put().key().size() <= 1024 &&
             request.put().value().size() <= 8192;
    case KvRequest::kGet:
      return !request.get().key().empty() && request.get().key().size() <= 1024;
    case KvRequest::kCas:
      return !request.cas().key().empty() && request.cas().key().size() <= 1024 &&
             request.cas().value().size() <= 8192;
    case KvRequest::kDel:
      return !request.del().key().empty() && request.del().key().size() <= 1024;
    case KvRequest::kPing:
      return true;
    default:
      return false;
  }
}

raft::Node::Config NodeConfig(const Options& options) {
  raft::Node::Config config;
  config.id = options.id;
  config.seed = options.seed;
  config.validate_snapshot = StateMachine::ValidateSnapshot;
  for (const auto& [id, endpoint] : options.members) config.voters.push_back(id);
  return config;
}

std::shared_ptr<ReadTraceRecorder> MakeTrace(const Options& options) {
  if (options.trace_prefix.empty()) return {};
  ReadTraceRecorder::Options selected;
  selected.sample_every = options.trace_sample_every;
  selected.slow_ns = options.trace_slow_ms * 1000000;
  selected.max_events = options.trace_max_events;
  return std::make_shared<ReadTraceRecorder>(options.trace_prefix, options.id, selected);
}

raft::FileStore::Options StoreConfig(const Options& options,
                                     const std::shared_ptr<ReadTraceRecorder>& trace) {
  raft::FileStore::Options config;
  config.max_state_bytes = options.max_state_bytes;
  config.journal = options.journal;
  config.journal_checkpoint_bytes = options.journal_checkpoint_bytes;
  if (trace) {
    config.trace_enabled = [trace] { return trace->Enabled(); };
    config.trace_observer = [trace](const raft::PersistenceTrace& event) { trace->Record(event); };
  }
  return config;
}

// Bind an existing directory to one immutable node/membership configuration.
// FileStore already holds the directory lock when this function runs.
void BindIdentity(const Options& options, const raft::DurableState& state) {
  std::ostringstream identity;
  identity << "yewukv-fixed-cluster-v1\nnode=" << options.id << '\n';
  for (const auto& [id, endpoint] : options.members)
    identity << id << '=' << endpoint.host << ':' << endpoint.port << '\n';
  const std::string path = options.directory + "/cluster.identity";
  const int directory = ::open(options.directory.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
  if (directory < 0) throw std::runtime_error("open cluster identity directory");
  try {
    const int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK);
    if (fd >= 0) {
      char buffer[4096];
      const auto n = ::read(fd, buffer, sizeof(buffer));
      ::close(fd);
      if (n < 0 || std::string(buffer, static_cast<size_t>(n)) != identity.str())
        throw std::runtime_error("cluster identity differs from persisted node configuration");
    } else {
      if (errno != ENOENT || state.hard_state.term != 0 || !state.entries.empty())
        throw std::runtime_error("missing cluster identity for nonempty Raft state");
      const std::string temporary = options.directory + "/cluster.identity.tmp";
      if (::unlink(temporary.c_str()) != 0 && errno != ENOENT)
        throw std::runtime_error("remove temporary cluster identity");
      const int output = ::open(temporary.c_str(), O_CREAT | O_EXCL | O_WRONLY | O_CLOEXEC, 0600);
      if (output < 0) throw std::runtime_error("create cluster identity");
      const std::string bytes = identity.str();
      const bool written =
          ::write(output, bytes.data(), bytes.size()) == static_cast<ssize_t>(bytes.size());
      const bool synced = ::fdatasync(output) == 0;
      ::close(output);
      if (!written || !synced || ::rename(temporary.c_str(), path.c_str()) != 0)
        throw std::runtime_error("persist cluster identity");
    }
    if (::fsync(directory) != 0) throw std::runtime_error("sync cluster identity directory");
    ::close(directory);
  } catch (...) {
    ::close(directory);
    throw;
  }
}
}  // namespace

struct Server::Impl {
  struct Pending {
    std::weak_ptr<net::TcpConnection> connection;
    uint64_t ordered;
    uint64_t request_id;
    Clock::time_point deadline;
  };
  struct Peer {
    uint64_t id;
    Endpoint endpoint;
    std::mutex mutex;
    std::condition_variable ready;
    std::optional<Message> queued;
    uint64_t queued_ns = 0;
    uint64_t queued_tid = 0;
    std::thread thread;
  };
  struct Read {
    Pending pending;
    KvRequest request;
    uint64_t minimum_index;
    uint64_t term;
    uint64_t context = 0;
    std::optional<uint64_t> index;
    std::optional<ReadTrace> trace;
    uint64_t trace_ordinal = 0;
  };
  struct DeferredPeer {
    Pending pending;
    Message message;
    Clock::time_point received;
  };
  // Fixed-three membership: one waiting RPC per remote voter. The sender's
  // transport timeout is 400 ms; leave time to deliver a bounded rejection.
  static constexpr size_t kDeferredPeerCapacity = 2;
  static constexpr auto kPeerDeferTimeout = 300ms;

  Options options;
  std::shared_ptr<ReadTraceRecorder> trace;
  raft::FileStore store;
  raft::PersistenceQueue persistence_queue{{1, 2 * raft::FileStore::kMaxStateBytes}};
  raft::Node node;
  std::unique_ptr<raft::PersistenceWorker> disk_worker;
  net::TcpServer server;
  StateMachine state_machine;
  std::map<uint64_t, Pending> pending;
  uint64_t pending_term = 0;
  std::vector<std::pair<Pending, std::string>> staged;
  std::deque<Read> reads;
  std::deque<DeferredPeer> deferred_peers;
  uint64_t applied_index = 0;
  std::vector<std::unique_ptr<Peer>> peers;
  std::mutex mutex;
  std::condition_variable ready;
  bool persistence_ready = false;  // Protected by mutex, including notification.
  std::deque<std::function<void()>> tasks;
  std::atomic<bool> stopping{false};
  std::atomic<bool> fatal{false};
  std::thread worker;
  uint64_t reported_term = 0;
  raft::Role reported_role = raft::Role::kFollower;
  std::atomic<uint64_t> busy_rejections{0};
  uint64_t installed_snapshots = 0;
  uint64_t completed_commands = 0;
  uint64_t quorum_timeouts = 0;
  uint64_t read_index_completed = 0;
  uint64_t read_quorum_rounds = 0;
  uint64_t read_overlapped_requests = 0;
  uint64_t append_requests = 0;
  uint64_t read_append_requests = 0;
  uint64_t read_append_with_entries = 0;
  uint64_t peer_deferred = 0;
  uint64_t peer_deferred_admitted = 0;
  uint64_t peer_deferred_expired = 0;
  uint64_t peer_deferred_abandoned = 0;
  uint64_t peer_deferred_rejected = 0;
  uint64_t peer_deferred_wait_ns = 0;
  uint64_t peer_deferred_wait_max_ns = 0;
  std::atomic<uint64_t> peer_busy_received{0};
  std::atomic<uint64_t> peer_rpc_failed{0};
  uint64_t snapshot_pause_ns = 0;
  uint64_t snapshot_pause_max_ns = 0;
  struct EncodedSnapshot {
    uint64_t index;
    std::string bytes;
    uint64_t encode_ns;
  };
  std::future<EncodedSnapshot> snapshot_job;
  std::atomic<bool> snapshot_ready{false};
  uint64_t snapshot_jobs = 0;
  uint64_t snapshot_discarded = 0;
  uint64_t snapshot_encode_ns = 0;
  uint64_t snapshot_freeze_max_ns = 0;
  uint64_t snapshot_publish_max_ns = 0;
  uint64_t trace_read_ordinal = 0;

  explicit Impl(Options selected)
      : options(std::move(selected)),
        trace(MakeTrace(options)),
        store(options.directory, StoreConfig(options, trace)),
        node(NodeConfig(options), store.ReadState(), store, store,
             options.background_persistence ? &persistence_queue : nullptr),
        server(net::InetAddress(options.listen_host, options.port)) {
    if (options.request_timeout_ms < 100 || options.request_timeout_ms > 60000 ||
        options.proposal_batch_size == 0 ||
        options.proposal_batch_size > raft::Node::kMaxAppendEntries ||
        options.members.at(options.id).port != options.port || options.snapshot_entries == 0 ||
        options.snapshot_bytes == 0 || options.max_state_bytes < 4096 ||
        options.event_queue_capacity == 0 || options.pending_capacity == 0)
      throw std::invalid_argument("invalid cluster port or request timeout");
    BindIdentity(options, store.ReadState());
    if (options.background_persistence)
      disk_worker = std::make_unique<raft::PersistenceWorker>(persistence_queue, store, [this] {
        std::lock_guard<std::mutex> lock(mutex);
        persistence_ready = true;
        ready.notify_one();
      });
    if (node.snapshot().index != 0) {
      if (!state_machine.Restore(node.snapshot().data))
        throw std::runtime_error("invalid recovered KV snapshot");
      applied_index = node.snapshot().index;
    }
    for (const auto& entry : node.entries()) {
      if (entry.command.empty()) continue;
      KvRequest command;
      if (!command.ParseFromString(entry.command) || !ValidCommand(command))
        throw std::runtime_error("invalid recovered KV command");
    }
    for (const auto& [id, endpoint] : options.members) {
      if (id == options.id) continue;
      auto peer = std::make_unique<Peer>();
      peer->id = id;
      peer->endpoint = endpoint;
      peers.push_back(std::move(peer));
    }
    server.SetMessageCallback([this](const auto& connection, net::Buffer* input) {
      while (connection->Connected()) {
        std::string payload;
        const auto decoded = rpc::FrameCodec::Decode(input, &payload);
        if (decoded == rpc::FrameCodec::DecodeStatus::kIncomplete) return;
        KvRequest request;
        if (decoded == rpc::FrameCodec::DecodeStatus::kInvalid ||
            !request.ParseFromString(payload)) {
          connection->ForceClose();
          return;
        }
        const auto ordered = connection->ReserveResponse();
        const auto request_id = request.request_id();
        std::optional<ReadTrace> timing;
        if (request.has_get() && trace && trace->Enabled()) {
          timing.emplace();
          timing->received_ns = TraceNow();
          timing->received_tid = CurrentThreadId();
          timing->request_id = request_id;
          timing->client_port = connection->PeerAddress().Port();
        }
        if (!Enqueue([this, weak = std::weak_ptr<net::TcpConnection>(connection), ordered,
                      request = std::move(request),
                      timing] { Handle(weak, ordered, request, timing); })) {
          KvResponse response;
          ++busy_rejections;
          response.set_request_id(request_id);
          response.set_code(KvResponse::BUSY);
          response.set_error("node event queue is full");
          Respond(connection, ordered, std::move(response));
        }
      }
    });
  }

  bool Enqueue(std::function<void()> task) {
    std::lock_guard<std::mutex> lock(mutex);
    if (stopping || tasks.size() >= options.event_queue_capacity) return false;
    tasks.push_back(std::move(task));
    ready.notify_one();
    return true;
  }

  static void Respond(std::weak_ptr<net::TcpConnection> weak, uint64_t ordered, KvResponse response,
                      std::shared_ptr<ReadTraceRecorder> recorder = {},
                      std::optional<ReadTrace> timing = {}) {
    if (auto connection = weak.lock()) {
      std::string bytes;
      if (!response.SerializeToString(&bytes) || bytes.empty() ||
          bytes.size() > rpc::FrameCodec::kMaxPayloadBytes) {
        connection->ForceClose();
        return;
      }
      auto frame = rpc::FrameCodec::Encode(bytes);
      if (timing && recorder) {
        timing->response_queued_ns = TraceNow();
        connection->Loop()->RunInLoop(
            [connection, ordered, frame = std::move(frame), recorder, timing = *timing]() mutable {
              timing.tcp_loop_ns = TraceNow();
              timing.tcp_tid = CurrentThreadId();
              connection->SendOrdered(ordered, std::move(frame));
              timing.tcp_dispatch_done_ns = TraceNow();
              recorder->Record(timing);
            });
      } else
        connection->SendOrdered(ordered, std::move(frame));
    }
  }

  KvResponse Response(uint64_t request_id) const {
    KvResponse response;
    response.set_request_id(request_id);
    response.set_leader_id(node.leader_id());
    response.set_is_leader(node.IsLeader());
    response.set_raft_term(node.term());
    return response;
  }

  Status ReceivePeer(const DeferredPeer& request) {
    // ReceiveAsync must inspect every message before admission, including when
    // the waiting slots are full: a valid higher term fences leadership now.
    // Keep our owned copy only when the disk barrier rejects admission.
    return node.ReceiveAsync(
        request.message, [this, operation = request.pending](std::optional<Message> reply) {
          auto response = Response(operation.request_id);
          if (reply)
            *response.mutable_peer() = EncodeMessage(*reply);
          else
            response.set_code(KvResponse::UNAVAILABLE);
          Respond(operation.connection, operation.ordered, std::move(response));
        });
  }

  void RejectPeer(const DeferredPeer& request, const Status& status) {
    auto response = Response(request.pending.request_id);
    response.set_code(status.code() == StatusCode::kBusy ? KvResponse::BUSY
                                                         : KvResponse::UNAVAILABLE);
    response.set_error(status.ToString());
    Respond(request.pending.connection, request.pending.ordered, std::move(response));
  }

  void HandlePeer(DeferredPeer request) {
    const auto status = ReceivePeer(request);
    if (status.ok()) return;
    if (status.code() == StatusCode::kBusy && disk_worker &&
        deferred_peers.size() < kDeferredPeerCapacity &&
        std::none_of(deferred_peers.begin(), deferred_peers.end(), [&](const auto& previous) {
          return previous.message.from == request.message.from;
        })) {
      deferred_peers.push_back(std::move(request));
      ++peer_deferred;
      return;
    }
    if (status.code() == StatusCode::kBusy) ++peer_deferred_rejected;
    RejectPeer(request, status);
  }

  void DrainPeers() {
    const auto now = Clock::now();
    for (auto it = deferred_peers.begin(); it != deferred_peers.end();) {
      auto connection = it->pending.connection.lock();
      if (!connection || !connection->Connected()) {
        ++peer_deferred_abandoned;
        it = deferred_peers.erase(it);
      } else if (now >= it->pending.deadline) {
        ++peer_deferred_expired;
        RejectPeer(*it, Status::IOError("peer request expired before disk admission"));
        it = deferred_peers.erase(it);
      } else
        ++it;
    }
    while (!deferred_peers.empty() && !node.PersistencePending()) {
      const auto status = ReceivePeer(deferred_peers.front());
      if (status.code() == StatusCode::kBusy) break;
      auto request = std::move(deferred_peers.front());
      deferred_peers.pop_front();
      if (!status.ok()) {
        RejectPeer(request, status);
        continue;
      }
      ++peer_deferred_admitted;  // Admitted; its reply still waits for disk.
      const auto waited = static_cast<uint64_t>(
          std::chrono::duration_cast<std::chrono::nanoseconds>(now - request.received).count());
      peer_deferred_wait_ns += waited;
      peer_deferred_wait_max_ns = std::max(peer_deferred_wait_max_ns, waited);
    }
  }

  void Handle(std::weak_ptr<net::TcpConnection> weak, uint64_t ordered, const KvRequest& request,
              std::optional<ReadTrace> timing = {}) {
    if (timing) {
      timing->handled_ns = TraceNow();
      timing->handled_tid = CurrentThreadId();
    }
    auto response = Response(request.request_id());
    if (request.request_id() == 0) {
      response.set_code(KvResponse::INVALID_ARGUMENT);
      response.set_error("request_id must be nonzero");
    } else if (request.has_peer()) {
      auto message = DecodeMessage(request.peer());
      if (!message ||
          (message->kind != MessageKind::kVote && message->kind != MessageKind::kAppend &&
           message->kind != MessageKind::kSnapshot)) {
        response.set_code(KvResponse::INVALID_ARGUMENT);
      } else {
        if (message->kind == MessageKind::kAppend) {
          for (const auto& entry : message->append.entries) {
            if (entry.command.empty()) continue;
            KvRequest command;
            if (!command.ParseFromString(entry.command) || !ValidCommand(command)) {
              response.set_code(KvResponse::INVALID_ARGUMENT);
              response.set_error("invalid replicated KV command");
              Respond(weak, ordered, std::move(response));
              return;
            }
          }
        }
        const auto received = Clock::now();
        HandlePeer({Pending{weak, ordered, request.request_id(), received + kPeerDeferTimeout},
                    std::move(*message), received});
        return;
      }
    } else if (request.has_status()) {
      const char* role = node.IsLeader()                         ? "leader"
                         : node.role() == raft::Role::kCandidate ? "candidate"
                                                                 : "follower";
      std::ostringstream json;
      const auto metrics = disk_worker ? disk_worker->ReadMetrics() : store.ReadMetrics();
      json << "{\"id\":" << options.id << ",\"role\":\"" << role << "\",\"term\":" << node.term()
           << ",\"leader_id\":" << node.leader_id() << ",\"last_index\":" << node.LastIndex()
           << ",\"commit_index\":" << node.CommitIndex() << ",\"applied_index\":" << applied_index
           << ",\"pending\":" << pending.size() << ",\"keys\":" << state_machine.size()
           << ",\"disk_saves\":" << metrics.saves << ",\"disk_bytes\":" << metrics.bytes_written
           << ",\"disk_persist_ns\":" << metrics.persist_nanoseconds
           << ",\"snapshot_index\":" << node.snapshot().index
           << ",\"snapshot_bytes\":" << node.snapshot().data.size()
           << ",\"live_snapshot_bytes\":" << state_machine.encoded_bytes()
           << ",\"retained_entries\":" << node.entries().size()
           << ",\"retained_log_bytes\":" << node.RetainedBytes()
           << ",\"state_bytes\":" << metrics.state_bytes
           << ",\"snapshots_saved\":" << metrics.snapshots
           << ",\"snapshots_installed\":" << installed_snapshots
           << ",\"busy_rejections\":" << busy_rejections.load()
           << ",\"completed_commands\":" << completed_commands
           << ",\"quorum_timeouts\":" << quorum_timeouts << ",\"read_pending\":" << reads.size()
           << ",\"read_index_completed\":" << read_index_completed
           << ",\"read_quorum_rounds\":" << read_quorum_rounds
           << ",\"read_overlapped_requests\":" << read_overlapped_requests
           << ",\"append_requests\":" << append_requests
           << ",\"read_append_requests\":" << read_append_requests
           << ",\"read_append_with_entries\":" << read_append_with_entries
           << ",\"peer_deferred\":" << peer_deferred
           << ",\"peer_deferred_pending\":" << deferred_peers.size()
           << ",\"peer_deferred_capacity\":" << kDeferredPeerCapacity
           << ",\"peer_deferred_admitted\":" << peer_deferred_admitted
           << ",\"peer_deferred_expired\":" << peer_deferred_expired
           << ",\"peer_deferred_abandoned\":" << peer_deferred_abandoned
           << ",\"peer_deferred_rejected\":" << peer_deferred_rejected
           << ",\"peer_deferred_wait_ns\":" << peer_deferred_wait_ns
           << ",\"peer_deferred_wait_max_ns\":" << peer_deferred_wait_max_ns
           << ",\"peer_busy_received\":" << peer_busy_received.load()
           << ",\"peer_rpc_failed\":" << peer_rpc_failed.load()
           << ",\"snapshot_pause_ns\":" << snapshot_pause_ns
           << ",\"snapshot_pause_max_ns\":" << snapshot_pause_max_ns
           << ",\"snapshot_jobs\":" << snapshot_jobs
           << ",\"snapshot_discarded\":" << snapshot_discarded
           << ",\"snapshot_encode_ns\":" << snapshot_encode_ns
           << ",\"snapshot_freeze_max_ns\":" << snapshot_freeze_max_ns
           << ",\"snapshot_publish_max_ns\":" << snapshot_publish_max_ns
           << ",\"snapshot_encoder\":\""
           << (options.background_snapshot_encoding ? "background" : "sync") << '"'
           << ",\"snapshot_policy\":\"" << (options.adaptive_snapshots ? "adaptive" : "fixed")
           << '"' << ",\"storage_mode\":\"" << (options.journal ? "journal" : "image") << '"'
           << ",\"read_mode\":\"" << (options.read_index ? "read-index" : "log") << '"'
           << ",\"disk_checkpoint_saves\":" << metrics.checkpoint_saves
           << ",\"disk_journal_saves\":" << metrics.journal_saves
           << ",\"journal_bytes\":" << metrics.journal_bytes
           << ",\"checkpoint_bytes\":" << metrics.checkpoint_bytes
           << ",\"disk_encode_ns\":" << metrics.encode_nanoseconds
           << ",\"disk_sync_ns\":" << metrics.sync_nanoseconds
           << ",\"disk_write_ns\":" << metrics.write_nanoseconds
           << ",\"disk_file_syncs\":" << metrics.file_syncs
           << ",\"disk_directory_syncs\":" << metrics.directory_syncs
           << ",\"disk_crc_ns\":" << metrics.crc_nanoseconds
           << ",\"disk_file_sync_ns\":" << metrics.file_sync_nanoseconds
           << ",\"disk_directory_sync_ns\":" << metrics.directory_sync_nanoseconds
           << ",\"disk_checkpoint_max_ns\":" << metrics.checkpoint_max_nanoseconds
           << ",\"disk_journal_sync_max_ns\":" << metrics.journal_sync_max_nanoseconds;
      json << ",\"disk_executor\":\"" << (disk_worker ? "background" : "sync") << '"'
           << ",\"persistence_pending\":" << (node.PersistencePending() ? 1 : 0)
           << ",\"admitted_last_index\":" << node.LastAdmittedIndex()
           << ",\"staged\":" << staged.size()
           << ",\"persistence_queue_operations\":" << persistence_queue.outstanding()
           << ",\"persistence_queue_bytes\":" << persistence_queue.payload_bytes();
      auto array = [&json](const auto& values) {
        json << '[';
        for (size_t i = 0; i < values.size(); ++i) {
          if (i) json << ',';
          json << values[i];
        }
        json << ']';
      };
      json << ",\"disk_latency_bounds_ns\":";
      array(raft::FileStore::kLatencyBoundsNs);
      json << ",\"disk_checkpoint_duration_buckets\":";
      array(metrics.checkpoint_duration_buckets);
      json << ",\"disk_journal_sync_duration_buckets\":";
      array(metrics.journal_sync_duration_buckets);
      json << ",\"trace_enabled\":" << (trace && trace->Enabled() ? 1 : 0)
           << ",\"trace_written\":" << (trace ? trace->written() : 0)
           << ",\"trace_dropped\":" << (trace ? trace->dropped() : 0)
           << ",\"trace_limited\":" << (trace && trace->limited() ? 1 : 0)
           << ",\"trace_failed\":" << (trace && trace->failed() ? 1 : 0) << '}';
      response.set_value(json.str());
    } else if (!ValidCommand(request)) {
      response.set_code(KvResponse::INVALID_ARGUMENT);
      response.set_error("invalid operation or key/value size (key <= 1024, value <= 8192 bytes)");
    } else if (!node.IsLeader()) {
      response.set_code(KvResponse::NOT_LEADER);
      response.set_error("not leader; discover leader before submitting");
    } else if (pending.size() + staged.size() + reads.size() >= options.pending_capacity) {
      ++busy_rejections;
      response.set_code(KvResponse::BUSY);
      response.set_error("too many pending proposals");
    } else if (options.read_index && request.has_get()) {
      if (timing) {
        timing->term = node.term();
        timing->minimum_index = node.LastAdmittedIndex() + staged.size();
        if (timing->minimum_index > node.LastIndex() && staged.empty()) {
          if (auto ticket = node.PendingTicket()) {
            timing->persistence_stream = ticket->stream;
            timing->persistence_sequence = ticket->sequence;
          }
        }
      }
      reads.push_back(
          Read{Pending{weak, ordered, request.request_id(),
                       Clock::now() + std::chrono::milliseconds(options.request_timeout_ms)},
               request, node.LastAdmittedIndex() + staged.size(), node.term(), 0, std::nullopt,
               timing, timing ? ++trace_read_ordinal : 0});
      return;
    } else {
      auto command = request;
      command.set_state_machine_version(2);
      staged.emplace_back(
          Pending{weak, ordered, request.request_id(),
                  Clock::now() + std::chrono::milliseconds(options.request_timeout_ms)},
          command.SerializeAsString());
      return;
    }
    Respond(weak, ordered, std::move(response));
  }

  void Apply() {
    if (auto snapshot = node.TakeInstalledSnapshot()) {
      if (snapshot->index <= applied_index || !state_machine.Restore(snapshot->data))
        throw std::runtime_error("invalid installed state-machine snapshot");
      applied_index = snapshot->index;
      ++installed_snapshots;
    }
    for (const auto& entry : node.TakeCommitted()) {
      if (entry.index != applied_index + 1) throw std::runtime_error("state-machine apply gap");
      applied_index = entry.index;
      if (entry.command.empty()) continue;
      ++completed_commands;
      KvRequest request;
      if (!request.ParseFromString(entry.command) || !ValidCommand(request))
        throw std::runtime_error("invalid committed KV command");
      auto response = Response(request.request_id());
      state_machine.Apply(request, &response, entry.index);
      auto found = pending.find(entry.index);
      if (found != pending.end()) {
        response.set_request_id(found->second.request_id);
        Respond(found->second.connection, found->second.ordered, std::move(response));
        pending.erase(found);
      }
    }
  }

  void Finish() {
    // A new leader may replace an old pending index with a different command.
    // Cancel old-leader waiters BEFORE applying the new leader's committed log.
    if (!node.IsLeader() || (!pending.empty() && pending_term != node.term())) {
      for (const auto& [index, operation] : pending) {
        auto response = Response(operation.request_id);
        response.set_code(KvResponse::UNAVAILABLE);
        response.set_error("leadership lost; operation outcome is unknown");
        Respond(operation.connection, operation.ordered, std::move(response));
      }
      pending.clear();
    }
    Apply();
    if (snapshot_job.valid() && snapshot_job.wait_for(0ms) == std::future_status::ready) {
      snapshot_ready = false;
    }
    if (!node.PersistencePending() && snapshot_job.valid() &&
        snapshot_job.wait_for(0ms) == std::future_status::ready) {
      auto encoded = snapshot_job.get();
      snapshot_ready = false;
      snapshot_encode_ns += encoded.encode_ns;
      if (encoded.index > node.snapshot().index) {
        const auto started = Clock::now();
        const auto status = node.Compact(encoded.index, std::move(encoded.bytes));
        if (!status.ok()) throw std::runtime_error(status.ToString());
        const auto elapsed = static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - started).count());
        snapshot_pause_ns += elapsed;
        snapshot_pause_max_ns = std::max(snapshot_pause_max_ns, elapsed);
        snapshot_publish_max_ns = std::max(snapshot_publish_max_ns, elapsed);
      } else
        ++snapshot_discarded;
    }
    if (!node.PersistencePending() && !snapshot_job.valid() &&
        state_machine.encoded_bytes() <= raft::FileStore::kMaxSnapshotBytes &&
        ShouldSnapshot(applied_index, node.snapshot().index, node.RetainedBytes(),
                       state_machine.encoded_bytes(), options.snapshot_entries,
                       options.snapshot_bytes, options.adaptive_snapshots)) {
      const auto started = Clock::now();
      if (options.background_snapshot_encoding) {
        auto frozen = state_machine.Freeze();
        const auto index = applied_index;
        snapshot_ready = false;
        snapshot_job = std::async(std::launch::async, [this, index, frozen = std::move(frozen)]() {
          try {
            const auto began = Clock::now();
            auto bytes = frozen.Serialize();
            const auto elapsed = static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - began).count());
            snapshot_ready = true;
            ready.notify_one();
            return EncodedSnapshot{index, std::move(bytes), elapsed};
          } catch (...) {
            snapshot_ready = true;
            ready.notify_one();
            throw;
          }
        });
        ++snapshot_jobs;
      } else {
        const auto status = node.Compact(applied_index, state_machine.Serialize());
        if (!status.ok()) throw std::runtime_error(status.ToString());
      }
      const auto elapsed = static_cast<uint64_t>(
          std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - started).count());
      snapshot_pause_ns += elapsed;
      snapshot_pause_max_ns = std::max(snapshot_pause_max_ns, elapsed);
      if (options.background_snapshot_encoding)
        snapshot_freeze_max_ns = std::max(snapshot_freeze_max_ns, elapsed);
      else
        snapshot_publish_max_ns = std::max(snapshot_publish_max_ns, elapsed);
    }
    // Start fresh quorum confirmation before proposing this batch's writes.
    // Confirmation overlaps replication; completion still waits for BOTH the
    // ReadIndex and the request's earlier-write application fence.
    FinishReads();
    const auto now = Clock::now();
    for (auto it = staged.begin(); it != staged.end();) {
      if (now >= it->first.deadline || it->first.connection.expired()) {
        auto response = Response(it->first.request_id);
        response.set_code(KvResponse::BUSY);
        response.set_error("proposal deadline expired before admission");
        Respond(it->first.connection, it->first.ordered, std::move(response));
        it = staged.erase(it);
      } else
        ++it;
    }
    if (!staged.empty()) {
      if (!node.IsLeader()) {
        for (const auto& operation : staged) {
          auto response = Response(operation.first.request_id);
          response.set_code(KvResponse::NOT_LEADER);
          response.set_error("leadership changed before proposal");
          Respond(operation.first.connection, operation.first.ordered, std::move(response));
        }
        staged.clear();
      } else if (!node.PersistencePending()) {
        // Earlier requests may have accumulated while the disk was busy. Only
        // submit one bounded batch; later requests retain their original fence.
        const auto count = std::min(staged.size(), options.proposal_batch_size);
        std::vector<std::string> commands;
        for (size_t i = 0; i < count; ++i) commands.push_back(staged[i].second);
        auto proposed = node.ProposeBatch(std::move(commands));
        if (proposed.ok()) {
          pending_term = node.term();
          for (size_t i = 0; i < count; ++i)
            pending.emplace(proposed.value()[i], std::move(staged[i].first));
          if (auto ticket = node.PendingTicket())
            for (auto& read : reads)
              if (read.trace && read.minimum_index >= proposed.value().front() &&
                  read.minimum_index <= proposed.value().back()) {
                read.trace->persistence_stream = ticket->stream;
                read.trace->persistence_sequence = ticket->sequence;
              }
        } else {
          for (size_t i = 0; i < count; ++i) {
            const auto& operation = staged[i];
            auto response = Response(operation.first.request_id);
            response.set_code(KvResponse::UNAVAILABLE);
            response.set_error("proposal persistence failed; operation outcome is unknown");
            Respond(operation.first.connection, operation.first.ordered, std::move(response));
          }
        }
        staged.erase(staged.begin(), staged.begin() + count);
      }
    }
    for (auto it = pending.begin(); it != pending.end();) {
      if (!node.IsLeader() || now >= it->second.deadline) {
        ++quorum_timeouts;
        auto response = Response(it->second.request_id);
        response.set_code(KvResponse::UNAVAILABLE);
        response.set_error("leadership lost or quorum timeout; operation outcome is unknown");
        Respond(it->second.connection, it->second.ordered, std::move(response));
        it = pending.erase(it);
      } else
        ++it;
    }
    if (node.failed()) throw std::runtime_error("Raft node persistence/protocol failure");
    if (reported_term != node.term() || reported_role != node.role()) {
      reported_term = node.term();
      reported_role = node.role();
      const char* role = node.IsLeader()                         ? "leader"
                         : node.role() == raft::Role::kCandidate ? "candidate"
                                                                 : "follower";
      std::clog << "node=" << options.id << " event=role role=" << role << " term=" << node.term()
                << " last=" << node.LastIndex() << " commit=" << node.CommitIndex() << '\n';
    }
    for (auto& message : node.TakeMessages()) {
      if (message.kind == MessageKind::kAppend) {
        ++append_requests;
        if (message.read_context != 0) {
          ++read_append_requests;
          if (!message.append.entries.empty()) ++read_append_with_entries;
        }
      }
      for (auto& peer : peers) {
        if (peer->id != message.to) continue;
        std::lock_guard<std::mutex> lock(peer->mutex);
        peer->queued_ns = trace && trace->Enabled() ? TraceNow() : 0;
        peer->queued_tid = peer->queued_ns ? CurrentThreadId() : 0;
        peer->queued = std::move(message);
        peer->ready.notify_one();
        break;
      }
    }
  }

  void FinishReads() {
    for (const auto& state : node.TakeReadStates()) {
      for (auto& read : reads)
        if (read.context == state.context && read.term == state.term) {
          read.index = state.index;
          if (read.trace) {
            read.trace->quorum_ns = TraceNow();
            read.trace->index = state.index;
            read.trace->confirmed_by = state.confirmed_by;
            read.trace->confirmed_sequence = state.confirmed_sequence;
          }
        }
    }
    const auto now = Clock::now();
    for (auto it = reads.begin(); it != reads.end();) {
      const bool unavailable =
          !node.IsLeader() || it->term != node.term() || now >= it->pending.deadline;
      const bool ready_to_read =
          it->index && applied_index >= *it->index && applied_index >= it->minimum_index;
      if (!unavailable && !ready_to_read && !it->pending.connection.expired()) {
        ++it;
        continue;
      }
      auto response = Response(it->pending.request_id);
      if (unavailable) {
        ++quorum_timeouts;
        response.set_code(KvResponse::UNAVAILABLE);
        response.set_error("read quorum unavailable or leadership changed");
      } else if (ready_to_read) {
        state_machine.Apply(it->request, &response);
        ++read_index_completed;
      }
      std::optional<ReadTrace> timing;
      if (it->trace) {
        it->trace->ready_ns = TraceNow();
        it->trace->code = response.code();
        if (trace->KeepRead(it->trace_ordinal, *it->trace)) timing = it->trace;
      }
      Respond(it->pending.connection, it->pending.ordered, std::move(response), trace, timing);
      const auto context = it->context;
      it = reads.erase(it);
      if (context != 0 && std::none_of(reads.begin(), reads.end(), [context](const Read& read) {
            return read.context == context;
          }))
        node.CancelRead(context);
    }
    if (!node.IsLeader() || reads.empty() ||
        std::any_of(reads.begin(), reads.end(),
                    [](const Read& read) { return read.context != 0 && !read.index; }))
      return;
    if (std::none_of(reads.begin(), reads.end(),
                     [](const Read& read) { return read.context == 0; }))
      return;
    auto context = node.ReadIndex();
    if (!context.ok()) return;  // Includes the initial current-term commit gate.
    ++read_quorum_rounds;
    // Only requests already received before this quorum round may share it.
    for (auto& read : reads)
      if (read.context == 0) {
        read.context = context.value();
        if (read.trace) {
          read.trace->context = context.value();
          read.trace->context_ns = TraceNow();
        }
        if (read.minimum_index > applied_index) ++read_overlapped_requests;
      }
  }

  void Work() {
    auto next_tick = Clock::now() + 50ms;
    try {
      while (!stopping) {
        std::vector<std::function<void()>> batch;
        {
          std::unique_lock<std::mutex> lock(mutex);
          ready.wait_until(lock, next_tick, [this] {
            return stopping || !tasks.empty() || snapshot_ready.load() || persistence_ready;
          });
          if (stopping) break;
          persistence_ready = false;
          while (!tasks.empty() && batch.size() < options.proposal_batch_size) {
            batch.push_back(std::move(tasks.front()));
            tasks.pop_front();
          }
        }
        node.AdvancePersistence();
        // Resume bounded peer requests before starting another local snapshot
        // or client disk write. Completion wakes this owner without a tick.
        DrainPeers();
        for (auto& task : batch) task();
        DrainPeers();
        if (Clock::now() >= next_tick) {
          node.Tick();
          next_tick = Clock::now() + 50ms;
        }
        Finish();
      }
    } catch (const std::exception& error) {
      fatal = true;
      std::cerr << "cluster node " << options.id << " stopped: " << error.what() << '\n';
      server.Stop();
    }
  }

  void PeerWork(Peer& peer) {
    std::unique_ptr<rpc::KvClient> client;
    while (!stopping) {
      Message message;
      std::optional<PeerTrace> timing;
      {
        std::unique_lock<std::mutex> lock(peer.mutex);
        peer.ready.wait(lock, [&] { return stopping || peer.queued.has_value(); });
        if (stopping) return;
        message = std::move(*peer.queued);
        if (peer.queued_ns && trace && trace->Enabled()) {
          timing.emplace();
          timing->scheduled_ns = peer.queued_ns;
          timing->worker_ns = TraceNow();
          timing->worker_tid = CurrentThreadId();
          timing->scheduled_tid = peer.queued_tid;
          timing->peer_id = peer.id;
          timing->sequence = message.sequence;
          timing->context = message.read_context;
          timing->term = message.term;
        }
        peer.queued.reset();
      }
      try {
        if (!client)
          client = std::make_unique<rpc::KvClient>(peer.endpoint.host, peer.endpoint.port);
        KvRequest request;
        *request.mutable_peer() = EncodeMessage(message);
        auto transport = timing ? std::make_shared<rpc::KvClient::CallTiming>() : nullptr;
        auto result = client->CallAsync(std::move(request), 400ms, transport).get();
        if (timing) {
          timing->request_id = transport->request_id.load();
          timing->rpc_tid = transport->io_tid.load();
          timing->call_ns = transport->call_ns.load();
          timing->first_write_ns = transport->first_write_ns.load();
          timing->write_done_ns = transport->write_done_ns.load();
          timing->decoded_ns = transport->decoded_ns.load();
          timing->returned_ns = TraceNow();
        }
        if (!result.ok() || result.value().code() != KvResponse::OK || !result.value().has_peer()) {
          if (result.ok() && result.value().code() == KvResponse::BUSY) ++peer_busy_received;
          ++peer_rpc_failed;
          if (timing) trace->Record(*timing);
          client.reset();
          continue;
        }
        auto reply = DecodeMessage(result.value().peer());
        if (!reply || reply->from != peer.id || reply->to != options.id ||
            reply->sequence != message.sequence ||
            (message.kind == MessageKind::kVote && reply->kind != MessageKind::kVoteReply) ||
            (message.kind == MessageKind::kAppend && reply->kind != MessageKind::kAppendReply) ||
            (message.kind == MessageKind::kSnapshot &&
             reply->kind != MessageKind::kSnapshotReply)) {
          ++peer_rpc_failed;
          if (timing) trace->Record(*timing);
          client.reset();
          continue;
        }
        if (timing) timing->success = 1;
        if (!Enqueue([this, reply = std::move(*reply), timing]() mutable {
              if (timing) {
                timing->node_received_ns = TraceNow();
                timing->node_tid = CurrentThreadId();
              }
              node.ReceiveAsync(std::move(reply), [](std::optional<Message>) {});
              if (timing) trace->Record(*timing);
            }) &&
            timing)
          trace->Record(*timing);
      } catch (...) {
        ++peer_rpc_failed;
        if (timing) trace->Record(*timing);
        client.reset();
      }
    }
  }

  void Stop() {
    {
      std::lock_guard<std::mutex> lock(mutex);
      stopping = true;
    }
    ready.notify_all();
    for (auto& peer : peers) {
      std::lock_guard<std::mutex> lock(peer->mutex);
      peer->ready.notify_all();
    }
    if (worker.joinable()) worker.join();
    if (disk_worker) disk_worker->Stop();
    if (snapshot_job.valid()) snapshot_job.wait();
    for (auto& peer : peers) {
      if (peer->thread.joinable()) peer->thread.join();
    }
  }
};

Server::Server(Options options) : impl_(std::make_unique<Impl>(std::move(options))) {}
Server::~Server() {
  impl_->Stop();
}

bool Server::Run(int stop_fd) {
  try {
    if (impl_->disk_worker) impl_->disk_worker->Start();
    impl_->worker = std::thread([this] { impl_->Work(); });
    for (auto& peer : impl_->peers) {
      peer->thread = std::thread([this, selected = peer.get()] { impl_->PeerWork(*selected); });
    }
    impl_->server.Start(stop_fd);
  } catch (...) {
    impl_->Stop();
    throw;
  }
  impl_->Stop();
  return !impl_->fatal;
}

}  // namespace yewukv::cluster
