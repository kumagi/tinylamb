# `server/` — Layer 12 PostgreSQL wire protocol (Simple Query only)

Top of the layer DAG; may include anything below. Linux-only runtime
(epoll/eventfd/accept4) — `tinylamb_server` binary is not built on macOS.

## Key files

- `postgres_protocol.{hpp,cpp}` — thin v3 encoder/decoder: startup packet,
  auth-ok, ParameterStatus, BackendKeyData, ReadyForQuery, Error/EmptyQuery,
  RowDescription/DataRow, CommandComplete, `SplitSqlStatements` (shared with
  the CLI so statement splitting never diverges). Text format,
  int8/float8/date-centric.
- `postgres_server.{hpp,cpp}` + `postgres_server_main.cpp` — epoll +
  read-worker pool (`PostgresServerOptions`: `port 54321`, `max_connections
  1024`, `max_message 16 MiB`,   `idle_timeout 3600s`, `force_recovery` for torn
  WAL tails). Executes **`Q` (Simple Query) only** — `Parse/Bind/Describe/
  Execute/Sync` (extended protocol) are rejected with an error +
  ReadyForQuery. DoS caps: pre-auth input 1024 B, queued output 64 MiB,
  oversized results abort with an error past 1M rows.

## Contract to preserve

Simple-Query multi-statement + streaming responses. Do not claim full
protocol compatibility — extended-protocol support does not exist.
`Execute`/`Prepare` on `SqlEngine` is the only DB entry; no direct storage
calls from here.

Test: `./build/postgres_server_test`, `postgres_protocol_test`,
`postgres_server_extra_test`.
Fuzz: `server/postgres_protocol_fuzzer.cpp` (+ `_replay`,
needs `TINYLAMB_ENABLE_FUZZ`).
