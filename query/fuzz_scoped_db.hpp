/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#ifndef TINYLAMB_FUZZ_SCOPED_DB_HPP
#define TINYLAMB_FUZZ_SCOPED_DB_HPP

#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/log_message.hpp"
#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "database/database.hpp"
#include "index/index_schema.hpp"
#include "table/table.hpp"
#include "type/column_name.hpp"
#include "type/schema.hpp"

namespace tinylamb {

// On aarch64 Linux, Clang's `__builtin_trap()` (used by `-fsanitize=bounds`
// local-bounds checks) emits `brk` which raises SIGTRAP.  libFuzzer hooks
// SIGILL/SIGABRT/SIGSEGV/SIGBUS/SIGFPE but not SIGTRAP, so an untrapped
// SIGTRAP exits with code 133 without writing a `crash-<sha1>` artifact or a
// stack trace.  Bridging SIGTRAP to `abort()` lets libFuzzer's SIGABRT handler
// symbolize the stack and persist the crashing input.
inline void InstallFuzzSigtrapBridge() {
#if defined(__unix__) && defined(SIGTRAP)
  static const bool kInstalled = []() {
    struct sigaction sa{};
    sa.sa_handler = [](int /*sig*/) {
      std::cerr << "\n[fuzz-sigtrap] caught SIGTRAP (__builtin_trap / bounds); "
                   "forwarding to SIGABRT for libFuzzer\n"
                << std::flush;
      std::abort();
    };
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = SA_NODEFER;
    sigaction(SIGTRAP, &sa, nullptr);
    return true;
  }();
  (void)kInstalled;
#endif
}

// Throwaway database for one fuzz iteration: deletes the .db/.log/master
// files when the scope exits.  Without this every iteration strands a fresh
// file pair next to the fuzzer's working directory; long sweeps eventually
// exhaust the disk (observed: "Disk quota exceeded" turning Database::Create
// into a LOG(FATAL) abort, which libFuzzer reports as a bogus crash).
class ScopedDb {
 public:
  explicit ScopedDb(std::string_view prefix) : name_(ResolveDbName(prefix)) {
    InstallFuzzSigtrapBridge();
    if (std::getenv("TINYLAMB_FUZZ_FORCE_SCOPED_DB_CREATE_FAILURE") ==
        nullptr) {
      StatusOr<std::unique_ptr<Database>> created = Database::Create(name_);
      if (created.HasValue()) {
        db_ = created.MoveValue();
      }
    }
  }

  ScopedDb(const ScopedDb&) = delete;
  ScopedDb& operator=(const ScopedDb&) = delete;
  ScopedDb(ScopedDb&&) = delete;
  ScopedDb& operator=(ScopedDb&&) = delete;

  ~ScopedDb() {
    if (db_ != nullptr) {
      db_->DeleteAll();
    }
    db_.reset();
  }

  void Reset() { db_.reset(); }

  void Cleanup() {
    if (db_ != nullptr) {
      db_->DeleteAll();
      db_.reset();
      return;
    }
    StatusOr<std::unique_ptr<Database>> created = Database::Create(name_);
    if (created.HasValue()) {
      created.Value()->DeleteAll();
    }
  }

  // Emulates a crash mid-session: discard every unflushed pool image and
  // reopen the same database files so recovery (REDO + loser UNDO) runs
  // against exactly what reached the disk.  The old Database must be torn
  // down before Create so no shutdown-time writeback resurrects images
  // that never made it out of the pool.
  bool CrashAndReopen() {
    if (db_ != nullptr) {
      db_->EmulateCrash();
    }
    db_.reset();
    StatusOr<std::unique_ptr<Database>> reopened = Database::Create(name_);
    if (!reopened.HasValue()) {
      // Recovery diagnostics: a silent false here turns long fuzz sweeps
      // into "replay crash reopen failed" mysteries.
      LOG(ERROR) << "ScopedDb::CrashAndReopen: reopen failed: "
                 << ToString(reopened.GetStatus());
      return false;
    }
    db_ = reopened.MoveValue();
    return true;
  }

  [[nodiscard]] Database* operator->() const { return db_.get(); }
  [[nodiscard]] Database& operator*() const { return *db_; }
  [[nodiscard]] Database* get() const { return db_.get(); }
  [[nodiscard]] explicit operator bool() const { return db_ != nullptr; }
  [[nodiscard]] const std::string& name() const { return name_; }

 private:
  static std::string ResolveDbName(std::string_view prefix) {
    const std::string base = std::string(prefix) + "-" + RandomString(8);
    const char* db_dir = std::getenv("TINYLAMB_FUZZ_DB_DIR");
    if (db_dir == nullptr || *db_dir == '\0') {
      return base;
    }
    std::string dir(db_dir);
    while (!dir.empty() && dir.back() == '/') {
      dir.pop_back();
    }
    return dir.empty() ? base : (dir + "/" + base);
  }

  std::string name_;
  std::unique_ptr<Database> db_;
};

// The SQL frontend has no CREATE INDEX syntax, so fuzzers carry index specs
// as text ("CREATE [UNIQUE] INDEX n ON t(cols)[ INCLUDE(cols)];") and apply
// them through the catalog API here. Keeping the spec textual preserves the
// self-contained .test regression format.
inline bool ApplyIndexSpec(Database& db, TransactionContext& ctx,
                           std::string_view ddl) {
  static const bool kTraceFuzzSql =
      std::getenv("TINYLAMB_FUZZ_TRACE_SQL") != nullptr;
  if (kTraceFuzzSql) {
    std::cerr << "[fuzz-sql] " << ddl << "\n" << std::flush;
  }
  IndexMode mode = IndexMode::kNonUnique;
  if (ddl.starts_with("CREATE UNIQUE INDEX ")) {
    mode = IndexMode::kUnique;
    ddl.remove_prefix(std::string_view("CREATE UNIQUE INDEX ").size());
  } else if (ddl.starts_with("CREATE INDEX ")) {
    ddl.remove_prefix(std::string_view("CREATE INDEX ").size());
  } else {
    return false;
  }
  const size_t on = ddl.find(" ON ");
  const size_t open =
      on == std::string_view::npos ? std::string_view::npos : ddl.find('(', on);
  if (on == std::string_view::npos || open == std::string_view::npos) {
    return false;
  }
  const std::string idx_name(ddl.substr(0, on));
  const std::string table(ddl.substr(on + 4, open - (on + 4)));
  auto col_names = [](std::string_view list) {
    std::vector<std::string> cols;
    size_t p = 0;
    while (p < list.size()) {
      const size_t comma = list.find(',', p);
      cols.emplace_back(list.substr(p, comma == std::string_view::npos
                                           ? std::string_view::npos
                                           : comma - p));
      if (comma == std::string_view::npos) {
        break;
      }
      p = comma + 1;
    }
    return cols;
  };
  const size_t close = ddl.find(')', open);
  if (close == std::string_view::npos) {
    return false;
  }
  std::vector<std::string> key_cols =
      col_names(ddl.substr(open + 1, close - open - 1));
  std::vector<std::string> include_cols;
  static constexpr std::string_view kIncludeKw = " INCLUDE(";
  if (const size_t inc = ddl.find(kIncludeKw, close);
      inc != std::string_view::npos) {
    const size_t inc_open = inc + kIncludeKw.size() - 1;
    const size_t inc_close = ddl.find(')', inc_open);
    if (inc_close == std::string_view::npos) {
      return false;
    }
    include_cols =
        col_names(ddl.substr(inc_open + 1, inc_close - inc_open - 1));
  }
  StatusOr<Table> tbl = db.GetTable(ctx, table);
  if (!tbl.HasValue()) {
    return false;
  }
  auto slots_of = [&tbl](const std::vector<std::string>& cols,
                         std::vector<slot_t>* slots) {
    for (const std::string& c : cols) {
      const int off = tbl.Value().GetSchema().Offset(ColumnName(c));
      if (off < 0) {
        return false;
      }
      slots->push_back(static_cast<slot_t>(off));
    }
    return true;
  };
  std::vector<slot_t> key;
  std::vector<slot_t> include;
  if (key_cols.empty() || !slots_of(key_cols, &key) ||
      !slots_of(include_cols, &include)) {
    return false;
  }
  return db.CreateIndex(ctx, table, IndexSchema(idx_name, key, include, mode))
      .ok();
}

// Formats a single-line summary suitable for `-- failure: ` headers in `.test`
// files by collapsing newlines into ` | `.
inline std::string FormatFuzzFailureSummary(std::string_view prefix,
                                            std::string_view report) {
  std::string out(prefix);
  if (report.empty()) {
    return out;
  }
  out += " | ";
  bool prev_space = false;
  for (char ch : report) {
    if (ch == '\r' || ch == '\n') {
      if (!out.ends_with(" | ")) {
        out += " | ";
      }
      prev_space = true;
    } else if (ch == ' ' || ch == '\t') {
      if (!prev_space) {
        out.push_back(' ');
        prev_space = true;
      }
    } else {
      out.push_back(ch);
      prev_space = false;
    }
    if (out.size() >= 600) {
      out += "...";
      break;
    }
  }
  while (out.ends_with(" | ") || out.ends_with(" ")) {
    if (out.ends_with(" | ")) {
      out.resize(out.size() - 3);
    } else {
      out.pop_back();
    }
  }
  return out;
}

// Persists a verified fuzz finding (.test + .report.txt) to the working
// directory and optionally to `TINYLAMB_FUZZ_FINDINGS_DIR`.  When
// `TINYLAMB_FUZZ_CONTINUE=1` is set, returns after persisting so continuous
// fuzzing campaigns can keep collecting failures without aborting the worker.
inline void EmitFuzzFinding(std::string_view tag, uint64_t seed,
                            const std::string& filename,
                            const std::string& test_text,
                            std::string_view report, std::string_view verify) {
  auto write_pair = [&](const std::string& base_path) {
    {
      std::ofstream file(base_path);
      file << test_text;
    }
    std::string report_path = base_path;
    if (report_path.ends_with(".test")) {
      report_path.resize(report_path.size() - 5);
    }
    report_path += ".report.txt";
    std::ofstream rfile(report_path);
    rfile << "[" << tag << "] seed=" << seed << "\n"
          << report << "regression test written to " << base_path
          << (verify.empty() ? "" : "\n" + std::string(verify)) << "\n";
  };

  write_pair(filename);
  if (const char* dir = std::getenv("TINYLAMB_FUZZ_FINDINGS_DIR");
      dir != nullptr && dir[0] != '\0') {
    std::string target = dir;
    if (!target.ends_with('/')) {
      target.push_back('/');
    }
    target += filename;
    write_pair(target);
  }

  std::cerr << "[" << tag << "] seed=" << seed << "\n"
            << report << "regression test written to " << filename
            << (verify.empty() ? "" : "\n" + std::string(verify)) << "\n"
            << std::flush;

  if (const char* cont = std::getenv("TINYLAMB_FUZZ_CONTINUE");
      cont != nullptr && cont[0] != '\0' && std::string_view(cont) != "0") {
    return;
  }
  abort();
}

}  // namespace tinylamb

#endif  // TINYLAMB_FUZZ_SCOPED_DB_HPP
