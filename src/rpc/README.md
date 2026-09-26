# rpc (M1)

The `--protocol rpc` server mode uses a four-byte big-endian length prefix and
one Protobuf message per frame (`proto/kv.proto`). Payloads must contain
1–65536 bytes. The decoder handles partial and coalesced frames and rejects
invalid sizes before allocating a payload.

`KvClient` keeps one TCP connection and returns one future per call. It assigns
nonzero request IDs, matches responses to pending calls, and reports timeouts
and disconnects as local `Status` values. A transport-successful response has
its own application `KvResponse.code` (`OK`, `NOT_FOUND`, `INVALID_ARGUMENT`,
`INTERNAL`, `BUSY`). Automatic retry and reconnect are deliberately absent:
after a timed-out write, the client cannot know whether the server applied it. Later
durable retry semantics require a deduplication key in replicated state.

The RPC server submits requests to a bounded, single-thread storage executor.
The default `WalEngine` syncs each mutation before returning `OK`; queue or WAL
capacity pressure returns `BUSY` without applying the request. `KvClient` accepts numeric IPv4 addresses. The
original text protocol remains the default server mode.
