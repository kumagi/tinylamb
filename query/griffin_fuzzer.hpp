/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_GRIFFIN_FUZZER_HPP
#define TINYLAMB_GRIFFIN_FUZZER_HPP

#include <cstddef>
#include <cstdint>
#include <random>
#include <string>
#include <vector>

namespace tinylamb {

// Griffin-style grammar-free DBMS fuzzing (Fu et al., ASE '22), adapted to
// tinylamb's in-process metadata. Instead of a SQL grammar, iterations are
// guided by a metadata snapshot harvested from the live catalog
// (Database::ListTables + Table::GetSchema): tables with their typed columns.
//
// One iteration:
//   1. Reshuffle statements from the built-in seed cases (static pool, so a
//      seed fully determines the iteration and replay is self-contained).
//   2. Metadata-guided substitution: identifiers appearing in SELECT/DML
//      statements are swapped for catalog entries of the same kind (table ->
//      table, column -> same-typed column), raising the share of mutations
//      the engine accepts instead of rejecting on semantics.
//   3. Oracles:
//        (1) crash oracle: everything runs under ASAN/UBSAN/-UNDEBUG in fuzz
//            builds; a crash or an escaped exception is the finding.
//        (2) evaluation-path differential: every SELECT that executed is
//            re-run with TINYLAMB_DISABLE_BYTECODE set, forcing the AST
//            evaluation path, and the sorted result multisets are compared
//            against the bytecode/JIT fast path.
//
// A violation serializes to a self-contained `.test` file that replays via
// ParseGriffinTest + ReplayGriffinTrace.

struct GriffinTrace {
  uint64_t seed = 0;
  std::vector<std::string> statements;  // the executed session, in order
  std::string failure;                  // one-line summary of the mismatch
  bool ran_select = false;  // guards against vacuous, all-skipped sessions

  bool operator==(const GriffinTrace&) const = default;
};

// Runs one iteration from `rng`. Returns "" when every oracle held, otherwise
// a diagnostic. `trace` always receives the executed case.
std::string RunGriffinIteration(std::mt19937& rng, bool verbose,
                                GriffinTrace* trace = nullptr);

// Re-runs the recorded session directly (no regeneration) and, in parallel,
// regenerates the iteration from `trace.seed` to detect a tampered file.
// Returns "" when the recorded failure no longer reproduces; otherwise the
// fresh diagnostic.
std::string ReplayGriffinTrace(const GriffinTrace& trace, bool verbose = false);

// Self-contained text format ("-- key: value" lines, one "-- stmt: " per
// executed statement).
std::string SerializeGriffinTest(const GriffinTrace& trace);
// Returns false on malformed input.
bool ParseGriffinTest(std::string_view text, GriffinTrace* trace);

// libFuzzer harness: interprets `data` as a seed for the built-in seed pool.
// On a violation, writes `griffin_fuzz-repro-<seed>.test` into the working
// directory (after verifying it replays) and aborts.
void GriffinFuzzTry(const uint8_t* data, size_t size, bool verbose);

}  // namespace tinylamb

#endif  // TINYLAMB_GRIFFIN_FUZZER_HPP
