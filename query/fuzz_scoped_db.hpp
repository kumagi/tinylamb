/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#ifndef TINYLAMB_FUZZ_SCOPED_DB_HPP
#define TINYLAMB_FUZZ_SCOPED_DB_HPP

#include <sys/types.h>
#include <unistd.h>

#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/platform/process.hpp"
#include "common/log_message.hpp"
#include "common/platform/os.hpp"
#include "query/fuzz_flaky_marker.hpp"
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
#if defined(TINYLAMB_OS_POSIX) && defined(SIGTRAP)
  static const bool kInstalled = []() {
    struct sigaction sa{};
    sa.sa_handler = [](int /*sig*/) {
      // T4841: std::cerr is NOT async-signal-safe -- its lock may be held
      // by another thread mid-write when the trap lands, deadlocking the
      // bridge before libFuzzer can record the crash. Use write(2).
      static constexpr char kMsg[] =  // NOLINT(modernize-avoid-c-arrays)
          "\n[fuzz-sigtrap] caught SIGTRAP (__builtin_trap / bounds); "
          "forwarding to SIGABRT for libFuzzer\n";
      const ssize_t written = ::write(2, kMsg, sizeof(kMsg) - 1);
      (void)written;
      std::abort();
    };
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = SA_NODEFER;
    if (::sigaction(SIGTRAP, &sa, nullptr) != 0) {
      // Install failure must not pass silently: the SIGTRAP contract above
      // would be a lie, so fail loudly at harness startup instead.
      static constexpr char kFail[] =  // NOLINT(modernize-avoid-c-arrays)
          "[fuzz-sigtrap] sigaction(SIGTRAP) failed; aborting\n";
      const ssize_t written = ::write(2, kFail, sizeof(kFail) - 1);
      (void)written;
      std::abort();
    }
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
    if (::tinylamb::platform::GetEnv("TINYLAMB_FUZZ_FORCE_SCOPED_DB_CREATE_FAILURE") ==
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
    // A failed Create/CrashAndReopen leaves db_ null but can still strand
    // files; remove them by name so the next iteration starts clean.
    RemoveDbFiles();
  }

  void RemoveDbFiles() {
    std::error_code ec;
    // Same artifact family as Database::DeleteAll() /
    // RemoveCreatedDatabaseFiles(): a failed Create/CrashAndReopen leaves db_
    // null while the page file, WAL segments and LSM/TLCS companions may
    // already be on disk. Sweeping only the base suffixes strands them
    // (multi-MiB WAL segments per iteration) and defeats this class' disk-quota
    // purpose.
    for (const char* suffix :
         {".db", ".tmp", ".db.tmp", ".db.ckpt", ".db.ckpt.tmp", ".ckpt",
          ".ckpt.tmp", ".log", ".log.meta", ".log.meta.tmp", ".last_checkpoint",
          ".last_checkpoint.tmp"}) {
      std::filesystem::remove(name_ + suffix, ec);
    }

    // WAL segment series `<name>.log.<index>` (RemoveWalSegmentFiles in
    // database.cpp): contiguous digits from 0, recycled by trimming a prefix.
    const std::filesystem::path wal_base(name_ + ".log");
    const std::filesystem::path wal_dir = wal_base.has_parent_path()
                                              ? wal_base.parent_path()
                                              : std::filesystem::path(".");
    const std::string wal_prefix = wal_base.filename().string() + ".";
    std::error_code iter_ec;
    for (std::filesystem::directory_iterator it(wal_dir, iter_ec), end;
         !iter_ec && it != end; it.increment(iter_ec)) {
      const std::string fname = it->path().filename().string();
      if (!fname.starts_with(wal_prefix)) {
        continue;
      }
      const std::string digits = fname.substr(wal_prefix.size());
      bool all_digits = !digits.empty();
      for (char c : digits) {
        if (c < '0' || c > '9') {
          all_digits = false;
          break;
        }
      }
      if (all_digits) {
        std::filesystem::remove(it->path(), ec);
      }
    }
    if (iter_ec) {
      // A failed enumeration (permissions, concurrent deletion, bad parent)
      // would silently strand every remaining WAL segment and defeat this
      // class' disk-quota purpose.
      LOG(WARN) << "ScopedDb::RemoveDbFiles: WAL segment scan failed: "
                << iter_ec.message();
    }

    // LSM/block/TLCS companions (RemoveCompanionStorageFiles in database.cpp)
    // for both the page file (<name>.db) and the bare base name.
    const auto remove_companions = [](const std::string& file) {
      const std::filesystem::path p(file);
      const std::filesystem::path parent = p.parent_path().empty()
                                               ? std::filesystem::path(".")
                                               : p.parent_path();
      const std::string base = p.filename().string();
      std::error_code remove_ec;
      std::filesystem::remove(file + ".blocks", remove_ec);
      std::filesystem::remove(file + ".lsm", remove_ec);
      std::error_code companion_iter_ec;
      for (std::filesystem::directory_iterator it(parent, companion_iter_ec),
           end;
           !companion_iter_ec && it != end; it.increment(companion_iter_ec)) {
        const std::string fname = it->path().filename().string();
        if (fname.starts_with(base + ".blocks.") ||
            fname.starts_with(base + ".lsm.") ||
            fname.starts_with(base + ".tlcs")) {
          std::filesystem::remove(it->path(), remove_ec);
        }
      }
      if (remove_ec) {
        LOG(WARN) << "ScopedDb::RemoveDbFiles: companion remove failed for "
                  << file << ": " << remove_ec.message();
      }
      if (companion_iter_ec) {
        LOG(WARN) << "ScopedDb::RemoveDbFiles: companion scan failed for "
                  << file << ": " << companion_iter_ec.message();
      }
    };
    remove_companions(name_ + ".db");
    remove_companions(name_);
  }

  void Reset() { db_.reset(); }

  void Cleanup() {
    if (db_ != nullptr) {
      db_->DeleteAll();
      db_.reset();
    }
    // When db_ is null (a failed Create/CrashAndReopen/CloseAndReopen), do not
    // issue another Create just to call DeleteAll on it: under quota pressure
    // that retry creates a fresh partial page/WAL set on every iteration and
    // makes subsequent Creates fail more often. The name-based sweep below
    // already removes every artifact family, including partially created ones
    // (same contract as the destructor).
    RemoveDbFiles();
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
    last_reopen_status_ = reopened.GetStatus();
    if (!reopened.HasValue()) {
      // Recovery diagnostics: a silent false here turns long fuzz sweeps
      // into "replay crash reopen failed" mysteries.
      LOG(ERROR) << "ScopedDb::CrashAndReopen: reopen failed: "
                 << ToString(reopened.GetStatus());
      // Keep the harness reusable after a failed recovery attempt. Database::
      // Create may have opened and modified some files before returning its
      // error, and this object has no Database left to clean them up.
      RemoveDbFiles();
      return false;
    }
    db_ = reopened.MoveValue();
    return true;
  }

  // Clean shutdown + reopen: the Database destructor runs its normal
  // shutdown path (final checkpoint / compacted block file / WAL close), and
  // the reopen must see exactly the committed state.  Callers must have
  // ended every transaction first.
  bool CloseAndReopen() {
    db_.reset();
    StatusOr<std::unique_ptr<Database>> reopened = Database::Create(name_);
    last_reopen_status_ = reopened.GetStatus();
    if (!reopened.HasValue()) {
      LOG(ERROR) << "ScopedDb::CloseAndReopen: reopen failed: "
                 << ToString(reopened.GetStatus());
      RemoveDbFiles();
      return false;
    }
    db_ = reopened.MoveValue();
    return true;
  }

  // A failed Create/CloseAndReopen leaves db_ null. Dereferencing it is a
  // caller precondition violation (check `bool(scoped_db)` first); turn the
  // otherwise-UB into a diagnosable programmer-error tripwire instead.
  [[nodiscard]] Database* operator->() const {
    CHECK_MSG(db_ != nullptr, "ScopedDb::operator-> on an empty ScopedDb");
    return db_.get();
  }
  [[nodiscard]] Database& operator*() const {
    CHECK_MSG(db_ != nullptr, "ScopedDb::operator* on an empty ScopedDb");
    return *db_;
  }
  [[nodiscard]] Database* get() const { return db_.get(); }
  [[nodiscard]] explicit operator bool() const { return db_ != nullptr; }
  [[nodiscard]] const std::string& name() const { return name_; }
  // Status of the latest CrashAndReopen / CloseAndReopen (why it failed).
  [[nodiscard]] const Status& last_reopen_status() const {
    return last_reopen_status_;
  }

 private:
  static std::string ResolveDbName(std::string_view prefix) {
    std::string base = std::string(prefix) + "-" + RandomString(8);
    const char* db_dir = ::tinylamb::platform::GetEnv("TINYLAMB_FUZZ_DB_DIR");
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
  Status last_reopen_status_{Status::kSuccess};
};

// Fuzzers carry index specs as their own slot-based pseudo-DDL
// ("CREATE [UNIQUE] INDEX n ON t(cols)[ INCLUDE(cols)];") — distinct from
// the SQL CREATE INDEX spelling — and apply them through the catalog API
// here. Keeping the spec textual preserves the self-contained .test
// regression format.
inline bool ApplyIndexSpec(Database& db, TransactionContext& ctx,
                           std::string_view ddl) {
  static const bool kTraceFuzzSql =
      ::tinylamb::platform::GetEnv("TINYLAMB_FUZZ_TRACE_SQL") != nullptr;
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
  if (on == std::string_view::npos) {
    return false;
  }
  // Search for the column-list '(' strictly after the " ON " separator so a
  // malformed spec (e.g. "i ON(" with no table name) cannot make
  // open - table_start underflow into a huge substr length.
  const size_t table_start = on + 4;
  const size_t open = ddl.find('(', table_start);
  if (open == std::string_view::npos) {
    return false;
  }
  const std::string idx_name(ddl.substr(0, on));
  const std::string_view table_raw =
      ddl.substr(table_start, open - table_start);
  // "CREATE INDEX i ON  t(a)" can carry extra whitespace between ON and the
  // table name; trim it or GetTable sees " t" and silently loses the index
  // coverage seed.
  const size_t tstart = table_raw.find_first_not_of(" \t\r\n");
  const size_t tend = table_raw.find_last_not_of(" \t\r\n");
  const std::string table(tstart == std::string_view::npos
                              ? std::string_view()
                              : table_raw.substr(tstart, tend - tstart + 1));
  auto col_names = [](std::string_view list) {
    std::vector<std::string> cols;
    cols.reserve(list.empty() ? 0 : std::ranges::count(list, ',') + 1);
    size_t p = 0;
    while (p < list.size()) {
      const size_t comma = list.find(',', p);
      std::string col(list.substr(p, comma == std::string_view::npos
                                         ? std::string_view::npos
                                         : comma - p));
      const size_t start = col.find_first_not_of(" \t\r\n");
      const size_t end = col.find_last_not_of(" \t\r\n");
      col = start == std::string::npos ? std::string()
                                       : col.substr(start, end - start + 1);
      cols.push_back(std::move(col));
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
    slots->reserve(cols.size());
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
      // Enforce the documented 600-character cap *including* the ellipsis.
      // Checking after a full append could overshoot by the appended chars
      // (up to 3 for " | ") plus the ellipsis itself. Back up to a UTF-8
      // character boundary: resize(597) can otherwise split a multi-byte
      // sequence and leave invalid bytes in the `-- failure:` header.
      size_t cut = 597;
      while (cut > 0 &&
             (static_cast<unsigned char>(out[cut]) & 0xC0) == 0x80) {
        --cut;
      }
      out.resize(cut);
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

// A verify string containing this dedicated sentinel means the serialized
// trace replayed clean; such findings are flaky and must never be persisted or
// abort a worker. The marker is deliberately not a common word such as
// "flaky": the rest of `verify` is a free-form replay report (or SQL text),
// and a bare substring test there would silently drop genuine findings whose
// report happens to contain that word. Only the "replay held" branch of each
// fuzzer embeds the sentinel, via MakeFlakyVerify().
inline std::string MakeFlakyVerify(std::string_view detail) {
  std::string out(detail);
  out.push_back(' ');
  out.append(kFlakyVerifyMarker);
  return out;
}

// Persists a fuzz finding to the working directory and optionally to
// `TINYLAMB_FUZZ_FINDINGS_DIR`. A skipped verification saves an .unverified
// trace and lets the worker continue. Verified findings use .test and stop
// the worker unless `TINYLAMB_FUZZ_CONTINUE` is enabled.
inline void EmitFuzzFinding(std::string_view tag, uint64_t seed,
                            std::string filename,
                            const std::string& test_text,
                            std::string_view report, std::string_view verify,
                            bool replay_skipped = false) {
  // A finding whose serialized trace replays clean is most likely flaky;
  // do not persist it as a regression (it would be a permanently green test)
  // and do not abort the worker over a non-reproducing failure.
  if (verify.find(kFlakyVerifyMarker) != std::string_view::npos) {
    std::cerr << "[" << tag
              << "] skipping flaky finding (replay holds): " << filename << "\n"
              << std::flush;
    return;
  }
  if (replay_skipped) {
    if (filename.ends_with(".test")) {
      filename.resize(filename.size() - 5);
    }
    filename += ".unverified";
  }
  const std::string_view saved_message = replay_skipped
                                             ? "unverified trace written to "
                                             : "regression test written to ";
  auto write_pair = [&](const std::string& base_path) -> bool {
    bool ok = true;
    {
      std::ofstream file(base_path);
      file << test_text;
      file.flush();
      ok = file.good();
    }
    std::string report_path = base_path;
    if (report_path.ends_with(".test")) {
      report_path.resize(report_path.size() - 5);
    }
    report_path += ".report.txt";
    std::ofstream rfile(report_path);
    rfile << "[" << tag << "] seed=" << seed << "\n"
          << report << saved_message << base_path
          << (verify.empty() ? "" : "\n" + std::string(verify)) << "\n";
    rfile.flush();
    return ok && rfile.good();
  };

  if (!write_pair(filename)) {
    std::cerr << "[" << tag << "] WARNING: failed to write regression test to "
              << filename << "\n"
              << std::flush;
  }
  if (const char* dir = ::tinylamb::platform::GetEnv("TINYLAMB_FUZZ_FINDINGS_DIR");
      dir != nullptr && dir[0] != '\0') {
    std::string target = dir;
    if (!target.ends_with('/')) {
      target.push_back('/');
    }
    // Create the directory on demand: a supervisor-set findings dir that does
    // not exist yet must not silently swallow the remote copy.
    std::error_code mkerr;
    std::filesystem::create_directories(target, mkerr);
    target += filename;
    if (!write_pair(target)) {
      std::cerr << "[" << tag << "] WARNING: failed to write finding copy to "
                << target << " (TINYLAMB_FUZZ_FINDINGS_DIR=" << dir
                << ", mkdir error: " << mkerr.message()
                << "); only the local copy may exist\n"
                << std::flush;
    }
  }

  std::cerr << "[" << tag << "] seed=" << seed << "\n"
            << report << saved_message << filename
            << (verify.empty() ? "" : "\n" + std::string(verify)) << "\n"
            << std::flush;

  const char* cont = ::tinylamb::platform::GetEnv("TINYLAMB_FUZZ_CONTINUE");
  if (replay_skipped ||
      (cont != nullptr && cont[0] != '\0' && std::string_view(cont) != "0")) {
    return;
  }
  abort();
}

}  // namespace tinylamb

#endif  // TINYLAMB_FUZZ_SCOPED_DB_HPP
