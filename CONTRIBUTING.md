# Contributing

## Error handling

| Situation | Mechanism |
|-----------|-----------|
| Expected failures (I/O, constraints, corrupt input) | Return `Status` / `StatusOr<T>`; use `kCorrupt` for checksum or WAL damage |
| Programmer bugs (impossible states) | `CHECK` / `CHECK_MSG` (`common/log_message.hpp`: logs `FATAL` + aborts) and `assert` in debug |
| User SQL errors | Surface through `SqlEngine::LastError()` / PostgreSQL error responses |

**Do not add new C++ exceptions** to engine code paths. Existing test-only or
boundary adapters (`common/exc_shim.hpp` / `Execution` helpers outside library
paths) should not spread into storage, expression, or executor layers.

## Logging

Use `LOG(LogLevel::kInfo)` (or the `INFO`/`DEBUG`/… macros). Do not introduce
new bare `FATAL`/`ERROR` integer constants outside `log_message.hpp`.

## Tests & Lint

Run `cmake --build build -j && ctest --test-dir build -j$(nproc) --output-on-failure`,
`python3 scripts/check_layering.py`, and `python3 scripts/check_ooc_invariants.py`
before opening a PR. Concurrency-sensitive changes should pass under TSAN when
available (`TINYLAMB_ENABLE_TSAN=ON`).
