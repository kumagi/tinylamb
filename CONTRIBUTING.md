# Contributing

## Error handling

| Situation | Mechanism |
|-----------|-----------|
| Expected failures (I/O, constraints, corrupt input) | Return `Status` / `StatusOr<T>`; use `kCorrupt` for checksum or WAL damage |
| Programmer bugs (impossible states) | `assert` in debug; `LOG(FATAL)` + abort only when continuing would corrupt data |
| User SQL errors | Surface through `SqlEngine::LastError()` / PostgreSQL error responses |

**Do not add new C++ exceptions** to engine code paths. Existing test-only or
boundary throws (e.g. spill file I/O) should not spread into the storage or
executor layers.

## Logging

Use `LOG(LogLevel::kInfo)` (or the `INFO`/`DEBUG`/… macros). Do not introduce
new bare `FATAL`/`ERROR` integer constants outside `log_message.hpp`.

## Tests

Run `cmake --build build -j && ctest --test-dir build -j --output-on-failure`
before opening a PR. Concurrency-sensitive changes should pass under TSAN when
available (`TINYLAMB_ENABLE_TSAN=ON`).
