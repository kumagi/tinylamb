# `common/` — Layer 1 foundation (no tinylamb deps)

Generic building blocks for the whole engine. Must not include any upper
layer (`check_layering.py` enforces; sole exception is the zero-dependency
enum header `type/value_type.hpp`, allowlisted).

## Key files

- `status_or.hpp` — `StatusOr<T>` + 3-arg macros `ASSIGN_OR_RETURN(type,
  value, expr)`, `ASSIGN_OR_ASSERT_FAIL[_CONST]`, `ASSIGN_OR_CRASH[_CONST]`,
  `COERCE`. **DB logic never throws**; return `Status`/`StatusOr`.
- `constants.hpp` — canonical aliases: `lsn_t`/`txn_id_t`/`page_id_t` = u64,
  `slot_t` = u16.
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

## `platform/` — OS abstraction (Linux + macOS)

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
  `Preallocate`.
- `socket.hpp` — `OpenSocket`/`AcceptSocket`/`OpenPipe` with atomic or
  fcntl-applied `FdFlags`, `SendNoSignal`, `InitSpawnAttributes`
  (`POSIX_SPAWN_CLOEXEC_DEFAULT` on macOS).
- `event_poller.hpp` — level-triggered `EventPoller` (epoll / kqueue) and the
  cross-thread `Waker` (eventfd / pipe).
- `cpu_affinity.hpp` — `CpuSet` thread pinning; unsupported (no-op) on macOS.
- `time_zone.hpp` — IANA zone lookup (C++20 tzdb on Linux, `CFTimeZone` on
  macOS; links CoreFoundation).
- `synced_stream.hpp` — `SyncedOStream` (`std::osyncstream` or a locked
  fallback).

Test: `./build/platform_test`.

## Before reading code here

`Status` codes (`kSuccess/kNoSpace/kConflicts/kDuplicates/kNotExists/
kTooBigData/kDeleted/kCorrupt…`) and the macro forms above are the vocabulary
of every layer — learn them first and the rest reads faster.

Test: `./build/log_message_test`, `crc32c_test`, `serdes_test`,
`runtime_filter_test`.
