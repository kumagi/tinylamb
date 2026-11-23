# `common/` — Layer 1 foundation (no tinylamb deps)

Generic building blocks for the whole engine. Must not include any upper
layer (`check_layering.py` enforces; sole exception is the zero-dependency
enum header `type/value_type.hpp`, allowlisted).

## Key files

- `status_or.hpp` — `StatusOr<T>` + 3-arg macros `ASSIGN_OR_RETURN(type,
  value, expr)`, `ASSIGN_OR_ASSERT_FAIL[_CONST]`, `ASSIGN_OR_CRASH[_CONST]`,
  `COERCE`. **DB logic must not let exceptions escape across `Status`
  boundaries**; return `Status`/`StatusOr`. Transitional exception paths
  remain (`exc_shim.hpp` / `exception_status.hpp` adapters, internal codec
  bounds checks caught and translated at boundary callers), so "never throws"
  is a boundary contract, not a statement about the whole call stack.
- `constants.hpp` — canonical aliases: `lsn_t`/`txn_id_t`/`page_id_t` = u64,
  `slot_t` = u16, plus `StatusCode` (including `kMemoryLimitExceeded` and `kNoSpace`).
- `memory_budget.{hpp,cpp}` — `ProcessMemoryBudget` global memory arbiter
  (`kStorage`/`kColumnar`/`kQuery`/`kTx` pools, per-pool floors, RSS safety
  threshold, `ReclaimStage` callbacks `kColumnar` -> `kCleanStorage` ->
  `kSpillableQuery`). See ADR 0003.
- `query_grant.{hpp,cpp}` — `QueryMemoryCoordinator`, `QueryMemoryGrant`,
  `ActiveConsumerGuard`, `LocalBatchCache`, and `ComputeEffectiveDop`
  (query admission control and operator budget division). See ADR 0004.
- `spill/spill_store.{hpp,cpp}` — block-multiplexed `SpillStore` over at most
  one lazily opened unlinked temp file descriptor (`OpenUnlinkedTempFile`,
  opened on first Append and closed when empty), 64 KiB blocks, free-block
  reuse, `TINYLAMB_TEMP_LIMIT_BYTES` cap (`kNoSpace`). See ADR 0005.
- `incarnation.hpp`, `late_materialization.hpp`, `query_parallelism.hpp`,
  `parallel_work.{hpp,cpp}`, `sorted_distinct_runs.{hpp,cpp}` — per-table
  incarnation state, parallel task execution, and external sorted distinct helpers.
- `encoder.hpp`/`decoder.hpp`, `serdes.{hpp,cpp}` — streaming `<<`/`>>` +
  primitives, big-endian fixed. Persistence base for WAL, pages, index keys.
- `log_message.hpp` — `LOG(INFO/WARN/FATAL)` with file:line:func.
- `crc32c.hpp` — WAL/page checksums.
- `join_kind.hpp`, `set_operation.hpp`, `byte_stream.hpp`.
- `runtime_filter.{hpp,cpp}` — sound runtime ("dynamic") join filters over one
  INT64/DATE key domain: `ExactSetRuntimeFilter` (information passing),
  `MinMaxRuntimeFilter` (SIP range half), `BloomRuntimeFilter` (SIP membership
  half), `RejectAllRuntimeFilter`, and the conjunctive `RuntimeFilterGroup`
  built by `RuntimeFilterBuilder`. Invariant: `Reject(k) == true` implies no
  build row carries an equal non-NULL key `k`; Bloom false positives are allowed,
  false negatives never are. Lives at the lowest layer so the storage scan
  iterator (table/full_scan_iterator.cpp) can reject a probe row before row
  decode.

## `platform/` — OS abstraction (Linux + macOS + Windows)

Windows means Clang (clang-cl) or GCC (MinGW-w64): `platform/extended_float.hpp`
needs `unsigned __int128` / `__builtin_clzll` and hard-errors (T622) on
anything without 128-bit integers, e.g. MSVC cl.exe.

All OS- and libc-specific code goes through `common/platform/`; the rest of
the tree must not include Linux-only headers (`<sys/epoll.h>`,
`<sys/eventfd.h>`, `<endian.h>`, `<asm-generic/…>`, `<bits/…>`, `<sched.h>`
affinity) or call Linux-only APIs (`fdatasync`, `accept4`, `pipe2`,
`SOCK_CLOEXEC`/`SOCK_NONBLOCK`, `sched_*affinity`, `posix_fadvise`), nor
rely on libstdc++-only library features (`std::chrono::locate_zone`,
`std::osyncstream`). Each header documents the per-platform behaviour.

- `endian.hpp` — `htobe64`/`be64toh` family (glibc `<endian.h>`, Apple
  `OSByteOrder`, or a portable fallback).
- `file_io.hpp` — `DataSync` (fdatasync / `F_FULLFSYNC`), `AdviseDontNeed`,
  `Preallocate`, `OpenUnlinkedTempFile` (`O_TMPFILE` / unlinked `mkstemp`).
- `process.{hpp,cpp}` — `CurrentProcessRssBytes` and process memory probes.
- `socket.hpp` — `OpenSocket`/`AcceptSocket`/`OpenPipe` with atomic or
  fcntl-applied `FdFlags`, `SendNoSignal`, `InitSpawnAttributes`
  (`POSIX_SPAWN_CLOEXEC_DEFAULT` on macOS).
- `event_poller.hpp` — level-triggered `EventPoller` (epoll / kqueue / WSAPoll) and the
  cross-thread `Waker` (eventfd / pipe / Windows loopback socket pair).
- `cpu_affinity.hpp` — `CpuSet` thread pinning; unsupported (no-op) on macOS.
- `time_zone.hpp` — IANA zone lookup (C++20 tzdb on Linux, `CFTimeZone` on
  macOS; links CoreFoundation).
- `synced_stream.hpp` — `SyncedOStream` (`std::osyncstream` or a locked
  fallback).

Test: `./build/platform_test`. Note that this binary runs on the current host
only: the Windows branches in `event_poller.cpp` (WSAPoll) and `file_io.cpp`
(and similarly platform-specific macOS paths) are not exercised on a Linux
runner, so a green `platform_test` is not evidence of cross-platform safety.
Treat those branches as untested-until-built-on-target.

## Before reading code here

`Status` codes (`kSuccess/kNoSpace/kConflicts/kDuplicates/kNotExists/
kTooBigData/kDeleted/kCorrupt…`) and the macro forms above are the vocabulary
of every layer — learn them first and the rest reads faster.

Tests: `./build/log_message_test`, `crc32c_test`, `decoder_test`,
`serdes_test`, `runtime_filter_test`, `memory_budget_test`, `query_grant_test`,
`spill_store_test`, `ooc_scale_baseline_test` (label `scale`),
`sorted_distinct_runs_test`, `parallel_work_test`, `status_or_test`,
`string_util_test`, `util_extra_test`, `regex_guard_test`, and
`platform_test` (including `range_selection_test` where available).
