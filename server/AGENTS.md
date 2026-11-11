# `server/` — Layer 12 PostgreSQL wire protocol (Simple Query only)

Top of the layer DAG; may include anything below. The runtime uses
`common/platform` event polling and sockets on Linux, macOS and Windows;
the server CMake target is not restricted to Linux.

## Key files

- `postgres_protocol.{hpp,cpp}` — thin v3 encoder/decoder: startup packet,
  auth-ok, ParameterStatus, BackendKeyData, ReadyForQuery, Error/EmptyQuery,
  RowDescription/DataRow, CommandComplete, `SplitSqlStatements` (shared with
  the CLI so statement splitting never diverges). Text format,
  int8/float8/date-centric. Startup advertises `standard_conforming_strings=off`
  because the GoogleSQL frontend interprets backslash escapes in string literals.
- `postgres_server.{hpp,cpp}` + `postgres_server_main.cpp` — platform event loop +
  read-worker pool (`PostgresServerOptions`: `port 54321`, `max_connections
  1024`, `max_message 16 MiB`,   `idle_timeout 3600s`, `force_recovery` for torn
  WAL tails). Executes **`Q` (Simple Query) only** — `Parse/Bind/Describe/
  Execute` (extended protocol) are rejected with an ErrorResponse +
  ReadyForQuery, while `Sync` is tolerated as a no-op that only answers
  ReadyForQuery and `Flush` is a silent no-op. Read-only Query messages may
  be offloaded to the shared worker pool; ordinary INSERT/UPDATE/DELETE and
  BEGIN/COMMIT/ROLLBACK messages also use that pool, with one in-flight task
  per session. TransactionContext ownership moves to the task and returns in
  its completion; socket lifetime and input/output buffers stay owned by epoll.
  A ready reply up to 4 KiB may be sent nonblocking on the completing worker
  when no earlier output was queued. A shared lifetime mutex excludes close /
  descriptor reuse; partial sends return their suffix to epoll, and the
  session remains in flight until completion processing. Single-statement tasks
  publish commits on SQL workers, then hold the entire response in a separate
  durability queue until both own and observed dependency LSNs are durable.
  Read-only multi-SELECT messages share one snapshot and can defer their
  combined dependency barrier too. Partial rows before a later statement
  error retain that barrier even after abort. Explicit-block SELECT replies
  observe the same dependency barrier. Sync
  failures replace the held response with an error; never acknowledge them
  as committed. The pending-task count includes this queue for DDL admission.
  Disconnect cleanup and
  shutdown drain abort unfinished blocks on workers. The legacy
  `read_worker_threads` / `--read-workers` names configure this shared pool.
  Sessions that created a TEMP view or SQL
  UDF stay on the event-loop thread (the frontend registries are
  thread-local). DoS caps: pre-auth input 1024 B, queued output 64 MiB,
  oversized results abort with an error past 1M rows.

Catalog changes stay on epoll and fail with SQLSTATE 55006 while a foreign
transaction block or worker task/completion/cleanup is outstanding. There is
no online-index catch-up protocol: do not remove this quiescence admission
without replacing that contract. NoWait covers row intents AND index-undo
reservation drains, so a bounded pool cannot fill with waiters whose owners'
commit messages are queued behind them.

## Contract to preserve

Simple-Query multi-statement + streaming responses. Do not claim full
protocol compatibility — extended-protocol support does not exist.
`Execute`/`Prepare` on `SqlEngine` is the only DB entry; no direct storage
calls from here.

Test: `./build/postgres_server_test`, `postgres_protocol_test`,
`postgres_server_extra_test`.
Fuzz: `server/postgres_protocol_fuzzer.cpp` (+ `_replay`,
needs `TINYLAMB_ENABLE_FUZZ`).

A peer FIN does not discard queued Query messages: drain complete buffered
messages through worker completions before closing. EPOLL RDHUP notifications
can be stale after a worker re-arms input; read the socket rather than assuming
there are no remaining bytes. Exact certified literal reply packets can be
served directly from the receive buffer; partial/multiple frames use the
normal framing path, and continuous full-size packets yield after 32 packets.
