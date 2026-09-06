# `parser/` + `legacy/` — archived, off the execution path

Historical unit tests only. The canonical SQL frontend is `query/` + the
external `execute_query --mode=parse` child process. **Do not build on this
code**; see `legacy/parser/README.md` (A2-3: keep + mark archived).

- Headers shared with tests: `parser/*.hpp` (`parser.hpp`, `token.hpp`,
  `tokenizer.hpp`, `pratt_parser.hpp`; `parser/ast.hpp` is a compat shim for
  `query/statement.hpp`, slated for removal once remaining references —
  `parser/parser.hpp`, `parser/pratt_parser.hpp`, `parser/ast_extra_test.cpp`,
  `query/googlesql_ast_fuzzer.hpp` — are replaced).
- Archived sources: `legacy/parser/*.cpp` (tokenizer, recursive-descent +
  Pratt parsers, `parser_fuzzer.cpp`).
- Nothing outside `parser/*_test.cpp`, `parser/ast_extra_test.cpp`, and
  `legacy/parser/` references these (`grep '#include "parser/'` across
  `common/…/server/` returns nothing; CMake builds no parser target).

If you are fixing a test failure here, check whether the test itself should
be migrated to the `query/` frontend instead.
