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
- `vm_cache*.{hpp,cpp}` — mmap-backed VM cache; convergence target for the
  S3-FIFO migration (`s3-fifo.md`). Do not confuse with `page/PagePool`.
- `ring_buffer.hpp` — SPSC lock-free queue. `join_kind.hpp`,
  `set_operation.hpp`, `byte_stream.hpp`, `converter.hpp`, `debug.*`.

## Before reading code here

`Status` codes (`kSuccess/kNoSpace/kConflicts/kDuplicates/kNotExists/
kTooBigData/kDeleted/kCorrupt…`) and the macro forms above are the vocabulary
of every layer — learn them first and the rest reads faster.

Test: `./build/vm_cache_test`, `log_message_test`, `crc32c_test`, `debug_test`.
