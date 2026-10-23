#include "cluster/server.h"

#include "cluster/state_machine.h"
#include "cluster/snapshot_policy.h"

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
      request.state_machine_version() > 1)
    return false;
  switch (request.operation_case()) {
    case KvRequest::kPut:
      return !request.put().key().empty() && request.put().key().size() <= 1024 &&
             request.put().value().size() <= 8192;
    case KvRequest::kGet:
      return !request.get().key().empty() && request.get().key().size() <= 1024;
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

raft::FileStore::Options StoreConfig(const Options& options) {
  raft::FileStore::Options config;
  config.max_state_bytes = options.max_state_bytes;
  config.journal = options.journal;
  config.journal_checkpoint_bytes = options.journal_checkpoint_bytes;
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
    std::thread thread;
  };
  struct Read {
    Pending pending;
    KvRequest request;
    uint64_t minimum_index;
    uint64_t term;
    uint64_t context = 0;
    std::optional<uint64_t> index;
  };

  Options options;
  raft::FileStore store;
  raft::Node node;
  net::TcpServer server;
  StateMachine state_machine;
  std::map<uint64_t, Pending> pending;
  std::vector<std::pair<Pending, std::string>> staged;
  std::deque<Read> reads;
  uint64_t applied_index = 0;
  std::vector<std::unique_ptr<Peer>> peers;
  std::mutex mutex;
  std::condition_variable ready;
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
  uint64_t snapshot_pause_ns = 0;
  uint64_t snapshot_pause_max_ns = 0;

  explicit Impl(Options selected)
      : options(std::move(selected)),
        store(options.directory, StoreConfig(options)),
        node(NodeConfig(options), store.ReadState(), store, store),
        server(options.port, 0, std::chrono::seconds{60}, true) {
    if (options.request_timeout_ms < 100 || options.request_timeout_ms > 60000 ||
        options.proposal_batch_size == 0 ||
        options.proposal_batch_size > raft::Node::kMaxAppendEntries ||
        options.members.at(options.id).port != options.port || options.snapshot_entries == 0 ||
        options.snapshot_bytes == 0 || options.max_state_bytes < 4096 ||
        options.event_queue_capacity == 0 || options.pending_capacity == 0)
      throw std::invalid_argument("invalid cluster port or request timeout");
    BindIdentity(options, store.ReadState());
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
        if (!Enqueue([this, weak = std::weak_ptr<net::TcpConnection>(connection), ordered,
                      request = std::move(request)] { Handle(weak, ordered, request); })) {
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

  static void Respond(std::weak_ptr<net::TcpConnection> weak, uint64_t ordered,
                      KvResponse response) {
    if (auto connection = weak.lock()) {
      std::string bytes;
      if (!response.SerializeToString(&bytes) || bytes.empty() ||
          bytes.size() > rpc::FrameCodec::kMaxPayloadBytes) {
        connection->ForceClose();
        return;
      }
      connection->SendOrdered(ordered, rpc::FrameCodec::Encode(bytes));
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

  void Handle(std::weak_ptr<net::TcpConnection> weak, uint64_t ordered, const KvRequest& request) {
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
        const auto reply = node.Receive(*message);
        if (!reply) {
          response.set_code(KvResponse::UNAVAILABLE);
        } else {
          *response.mutable_peer() = EncodeMessage(*reply);
        }
      }
    } else if (request.has_status()) {
      const char* role = node.IsLeader()                         ? "leader"
                         : node.role() == raft::Role::kCandidate ? "candidate"
                                                                 : "follower";
      std::ostringstream json;
      const auto metrics = store.ReadMetrics();
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
           << ",\"snapshot_pause_ns\":" << snapshot_pause_ns
           << ",\"snapshot_pause_max_ns\":" << snapshot_pause_max_ns << ",\"snapshot_policy\":\""
           << (options.adaptive_snapshots ? "adaptive" : "fixed") << '"' << ",\"storage_mode\":\""
           << (options.journal ? "journal" : "image") << '"' << ",\"read_mode\":\""
           << (options.read_index ? "read-index" : "log") << '"'
           << ",\"disk_checkpoint_saves\":" << metrics.checkpoint_saves
           << ",\"disk_journal_saves\":" << metrics.journal_saves
           << ",\"journal_bytes\":" << metrics.journal_bytes
           << ",\"checkpoint_bytes\":" << metrics.checkpoint_bytes
           << ",\"disk_encode_ns\":" << metrics.encode_nanoseconds
           << ",\"disk_sync_ns\":" << metrics.sync_nanoseconds
           << ",\"disk_write_ns\":" << metrics.write_nanoseconds
           << ",\"disk_file_syncs\":" << metrics.file_syncs
           << ",\"disk_directory_syncs\":" << metrics.directory_syncs << '}';
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
      reads.push_back(
          Read{Pending{weak, ordered, request.request_id(),
                       Clock::now() + std::chrono::milliseconds(options.request_timeout_ms)},
               request, node.LastIndex() + staged.size(), node.term(), 0, std::nullopt});
      return;
    } else {
      auto command = request;
      command.set_state_machine_version(1);
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
      state_machine.Apply(request, &response);
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
    if (!node.IsLeader()) {
      for (const auto& [index, operation] : pending) {
        auto response = Response(operation.request_id);
        response.set_code(KvResponse::UNAVAILABLE);
        response.set_error("leadership lost; operation outcome is unknown");
        Respond(operation.connection, operation.ordered, std::move(response));
      }
      pending.clear();
    }
    Apply();
    if (state_machine.encoded_bytes() <= raft::FileStore::kMaxSnapshotBytes &&
        ShouldSnapshot(applied_index, node.snapshot().index, node.RetainedBytes(),
                       state_machine.encoded_bytes(), options.snapshot_entries,
                       options.snapshot_bytes, options.adaptive_snapshots)) {
      const auto started = Clock::now();
      const auto status = node.Compact(applied_index, state_machine.Serialize());
      if (!status.ok()) throw std::runtime_error(status.ToString());
      const auto elapsed = static_cast<uint64_t>(
          std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - started).count());
      snapshot_pause_ns += elapsed;
      snapshot_pause_max_ns = std::max(snapshot_pause_max_ns, elapsed);
    }
    // Start fresh quorum confirmation before proposing this batch's writes.
    // Confirmation overlaps replication; completion still waits for BOTH the
    // ReadIndex and the request's earlier-write application fence.
    FinishReads();
    if (!staged.empty()) {
      if (!node.IsLeader()) {
        for (const auto& operation : staged) {
          auto response = Response(operation.first.request_id);
          response.set_code(KvResponse::NOT_LEADER);
          response.set_error("leadership changed before proposal");
          Respond(operation.first.connection, operation.first.ordered, std::move(response));
        }
      } else {
        std::vector<std::string> commands;
        for (auto& operation : staged) commands.push_back(std::move(operation.second));
        auto proposed = node.ProposeBatch(std::move(commands));
        if (proposed.ok()) {
          for (size_t i = 0; i < staged.size(); ++i)
            pending.emplace(proposed.value()[i], std::move(staged[i].first));
        } else {
          for (const auto& operation : staged) {
            auto response = Response(operation.first.request_id);
            response.set_code(KvResponse::UNAVAILABLE);
            response.set_error("proposal persistence failed; operation outcome is unknown");
            Respond(operation.first.connection, operation.first.ordered, std::move(response));
          }
        }
      }
      staged.clear();
    }
    const auto now = Clock::now();
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
        peer->queued = std::move(message);
        peer->ready.notify_one();
        break;
      }
    }
  }

  void FinishReads() {
    for (const auto& state : node.TakeReadStates()) {
      for (auto& read : reads)
        if (read.context == state.context && read.term == state.term) read.index = state.index;
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
      Respond(it->pending.connection, it->pending.ordered, std::move(response));
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
          ready.wait_until(lock, next_tick, [this] { return stopping || !tasks.empty(); });
          if (stopping) break;
          while (!tasks.empty() && batch.size() < options.proposal_batch_size) {
            batch.push_back(std::move(tasks.front()));
            tasks.pop_front();
          }
        }
        for (auto& task : batch) task();
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
      {
        std::unique_lock<std::mutex> lock(peer.mutex);
        peer.ready.wait(lock, [&] { return stopping || peer.queued.has_value(); });
        if (stopping) return;
        message = std::move(*peer.queued);
        peer.queued.reset();
      }
      try {
        if (!client)
          client = std::make_unique<rpc::KvClient>(peer.endpoint.host, peer.endpoint.port);
        KvRequest request;
        *request.mutable_peer() = EncodeMessage(message);
        auto result = client->CallAsync(std::move(request), 400ms).get();
        if (!result.ok() || result.value().code() != KvResponse::OK || !result.value().has_peer()) {
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
          client.reset();
          continue;
        }
        Enqueue([this, reply = std::move(*reply)] { node.Receive(reply); });
      } catch (...) {
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
