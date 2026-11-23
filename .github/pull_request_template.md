## What / Why

<!-- What does this change and why? Link the TODO.md item / issue if any. -->

## How it was verified

<!-- Commands you ran and what they showed. For perf changes: warm median
     before/after (evo-x2 preferred), and the SF used. -->

## Checklist

- [ ] `cmake --build build -j && ctest --test-dir build -j$(nproc) --output-on-failure -LE scale` passes
- [ ] `python3 scripts/check_layering.py` and `python3 scripts/check_ooc_invariants.py` exit 0
- [ ] `clang-format-22 -style=file` is clean on touched files
- [ ] No new C++ exceptions: errors go through `Status` / `StatusOr<T>` (see CONTRIBUTING.md)
- [ ] Concurrency-sensitive change → passes under `TINYLAMB_ENABLE_TSAN=ON`
- [ ] Bug fix → a regression test (or fuzz reproducer) is added
- [ ] Performance-relevant → `BenchmarkHistory.md` updated with warm medians
