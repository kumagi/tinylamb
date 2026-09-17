/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#ifndef TINYLAMB_SQL_SESSION_FUZZER_HPP
#define TINYLAMB_SQL_SESSION_FUZZER_HPP

#include <cstddef>
#include <cstdint>
#include <random>
#include <string>
#include <string_view>
#include <vector>

namespace tinylamb {

// State-tracking session fuzzing.  Unlike the metamorphic oracles in
// sql_oracle_fuzzer (which check properties of single queries), this harness
// verifies the *whole database state* after every statement: it maintains a
// row-level mirror of every table in C++ and compares a full-table dump
// against the engine after each INSERT / UPDATE / DELETE / DDL step.
//
// DML and CREATE/DROP TABLE run as SQL.  CREATE INDEX is not reachable
// through the SQL frontend (the GoogleSQL AST visitor does not implement
// it yet), so the harness drives it through the Database C++ API; the
// recorded pseudo-DDL replays through the same API.
//
// A mismatch means some layer (MVCC visibility, WAL reflect, page
// allocation, B+Tree/LSM index maintenance, catalog) lost a committed
// mutation or invented one.

struct SessionCheck {
  std::string table;                  // dump this table...
  size_t after_step{0};               // ...after steps[after_step]
  std::vector<std::string> expected;  // sorted Row::ToString dump
  // Dual-transaction sessions: which context issues the check (0 or 1).
  // Each context's snapshot isolates it from the other's uncommitted work.
  int txn{0};

  bool operator==(const SessionCheck&) const = default;
};

// One executed step: SQL, C++-API DDL recorded as pseudo-SQL, or a commit
// boundary (PreCommit + fresh context).  The commit flag must be part of the
// trace: the state-tracking checks run across transaction boundaries, and a
// replay that kept every statement in one transaction would silently skip
// the MVCC/commit paths that produced the failure.
struct SessionStep {
  bool is_ddl{false};
  bool is_commit{false};
  bool is_abort{false};
  // Crash boundary: discard the buffer pool, reopen the same database, and
  // let ARIES recovery reconstruct the committed state (any in-flight
  // transaction is loser-undone — mirror-wise a crash is an abort).
  bool is_crash{false};
  // Fuzzy checkpoint: writes BEGIN/END checkpoint records + the master
  // record so a later crash recovers from the DPT instead of LSN 0.  No
  // visible-state change — the in-flight transaction stays open.
  bool is_checkpoint{false};
  // Dual-transaction sessions: "-- begin2" opens context 1's first
  // transaction (context 0 begins implicitly at session start; commit/abort
  // re-begin their own context, crash re-begins both).
  bool is_begin{false};
  // Dual-transaction sessions: which context the step runs under (0 or 1).
  int txn{0};
  // The statement was observed to fail at generation time (write-intent
  // conflict or insert slot race).  Replay must fail the same way — a
  // flipped outcome means nondeterministic conflict handling.
  bool must_fail{false};
  std::string sql{};

  bool operator==(const SessionStep&) const = default;
};

struct SessionTrace {
  std::vector<SessionStep> steps;
  std::vector<SessionCheck> checks;

  bool operator==(const SessionTrace&) const = default;
};

struct SessionStats {
  int statements{0};
  int checks{0};
  int indexes_created{0};
  int tables_dropped{0};
  int commits{0};
  int aborts{0};
  int crashes{0};
  int checkpoints{0};
  int conflicts{0};
};

// Runs one seeded session.  Returns "" when every check held, otherwise a
// diagnostic naming the statement, the table, and both row dumps.  `trace`
// always receives the full recipe; replay it via ReplaySessionTrace.
std::string RunSessionIteration(std::mt19937& rng, bool verbose,
                                SessionStats* stats = nullptr,
                                SessionTrace* trace = nullptr);

// Replays a trace on a fresh throwaway database; "" = all checks hold.
std::string ReplaySessionTrace(const SessionTrace& trace, bool verbose = false);

// Self-contained text format ("-- key: value" lines, positional).
std::string SerializeSessionTest(uint64_t seed, const SessionTrace& trace,
                                 const std::string& failure_summary);
bool ParseSessionTest(std::string_view text, uint64_t* seed,
                      SessionTrace* trace, std::string* failure_summary);

// libFuzzer harness (data = seed).  On failure writes
// sql_session_fuzz-repro-<seed>.test and aborts.
void SessionFuzzTry(const uint8_t* data, size_t size, bool verbose);

}  // namespace tinylamb

#endif  // TINYLAMB_SQL_SESSION_FUZZER_HPP
