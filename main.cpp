/**
 * Copyright 2023 KUMAZAKI Hiroki
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <algorithm>
#include <atomic>
#include <csignal>
#include <exception>
#include <iostream>
#include <iterator>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/status_or.hpp"
#include "database/database.hpp"
#include "executor/executor_base.hpp"
#include "query/sql_engine.hpp"
#include "recovery/recovery_manager.hpp"
#include "server/postgres_protocol.hpp"
#include "type/row.hpp"

namespace {
// T2799: the server installs SIGINT/SIGTERM handlers; the CLI had none, so
// Ctrl-C during a long script died by default disposition without ever
// calling context.Abort(), leaving the transaction's tail for the next
// recovery. The handler sets a flag (checked between statements, where the
// abort must run) and a SECOND interrupt restores the default disposition
// and re-raises, so an unresponsive run can always be killed outright.
std::atomic<bool> g_interrupted{false};

void HandleInterrupt(int signo) {
  if (g_interrupted.exchange(true)) {
    std::signal(SIGINT, SIG_DFL);  // NOLINT(cert-err33-c)
    std::signal(SIGTERM, SIG_DFL);  // NOLINT(cert-err33-c)
    std::raise(signo);  // NOLINT(cert-err33-c)
  }
}
}  // namespace

int main(int argc, char** argv) {
  // --force: a torn/unparsable WAL tail is truncated to its intact prefix
  // instead of treating the corruption as fatal (the default).
  bool force = false;
  // T2796: unknown leading-dash tokens used to fall through as the database
  // file name -- `tinylamb --help x.db` opened (and created) a DB literally
  // named "--help" while the typo `--forse` silently skipped forcing.
  // Reject unknown options the way postgres_server_main.cpp does, and give
  // --help a real answer.
  std::vector<std::string> args;
  for (int i = 1; i < argc; ++i) {
    const std::string arg(argv[i]);
    if (arg == "--force") {
      force = true;
    } else if (arg == "--help" || arg == "-h") {
      std::cout << "usage: tinylamb [--force] <database-file> < input.sql\n"
                << "  --force  truncate a torn WAL tail instead of failing\n";
      return 0;
    } else if (!arg.empty() && arg.front() == '-') {
      std::cerr << "unknown option: " << arg << '\n'
                << "usage: tinylamb [--force] <database-file> < input.sql\n";
      return 2;
    } else {
      args.push_back(arg);
    }
  }
  if (args.size() != 1) {
    std::cerr << "usage: tinylamb [--force] <database-file> < input.sql\n";
    return 2;
  }
  if (std::signal(SIGINT, HandleInterrupt) == SIG_ERR ||
      std::signal(SIGTERM, HandleInterrupt) == SIG_ERR) {
    std::cerr << "failed to install signal handlers; Ctrl-C will not abort "
                 "the transaction cleanly\n";
  }
  tinylamb::RecoveryManager::SetTornTailTruncationAllowed(force);

  std::string sql((std::istreambuf_iterator<char>(std::cin)),
                  std::istreambuf_iterator<char>());
  tinylamb::StatusOr<std::vector<std::string>> split =
      tinylamb::pgwire::SplitSqlStatements(sql);
  if (!split.HasValue()) {
    std::cerr << split.GetStatus() << '\n';
    return 1;
  }
  const std::vector<std::string> statements = std::move(split).MoveValue();
  if (statements.empty()) {
    std::cerr << "no SQL was provided on standard input\n";
    return 2;
  }

  // Same WAL geometry as the server entry point (T2853): a new database
  // preallocates its first WAL segment no matter which binary created it.
  auto database_or_status =
      tinylamb::Database::Create(args[0], 1, /*preallocate_new_wal=*/true);
  if (!database_or_status.HasValue()) {
    std::cerr << "failed to open database " << args[0] << ": "
              << database_or_status.GetStatus() << '\n';
    return 1;
  }
  tinylamb::Database& database = *database_or_status.Value();
  tinylamb::TransactionContext context = database.BeginContext();
  tinylamb::SqlEngine engine(database);
  // One implicit transaction wraps every statement of the script; the first
  // failure aborts the whole run (the process exits with a non-zero status).
  // D4 (docs/design.md): output is buffered and only printed after the
  // commit's durability barrier, so no row reaches the user before the
  // commits it observed are durable.
  std::vector<std::string> pending_output;
  const auto interrupted = [&context](const char* phase) {
    std::cerr << "interrupted " << phase
              << "; rolling back implicit "
                 "transaction\n";
    const tinylamb::Status abort_status = context.Abort();
    if (abort_status != tinylamb::Status::kSuccess) {
      std::cerr << "rollback after interrupt failed: " << abort_status << '\n';
    }
    return 130;  // 128 + SIGINT, the shell convention.
  };
  for (const std::string& statement : statements) {
    if (g_interrupted.load()) {
      return interrupted("before a statement");
    }
    try {
      tinylamb::StatusOr<tinylamb::QueryResult> executed =
          engine.Execute(context, statement);
      // An empty diagnostic means the external parse child was transiently
      // unavailable (fork throttling under load, test runner in parallel).
      // One retry keeps long scripts from dying on a scheduler hiccup.
      if (!executed.HasValue() && engine.LastError().empty()) {
        executed = engine.Execute(context, statement);
      }
      if (!executed.HasValue()) {
        const std::string& last_error = engine.LastError();
        std::cerr << "SQL error: " << last_error;
        if (last_error.empty()) {
          std::cerr << executed.GetStatus();
        }
        std::cerr << '\n';
        context.Abort();
        return 1;
      }
      std::string buffer;
      executed.Value().ForEach([&buffer](const tinylamb::Row& row) {
        std::ostringstream line;
        line << row << '\n';
        buffer += line.str();
      });
      // Executors latch drain-time failures on their status channel
      // (Next()==false reads as EOF): an unchecked overflow would print
      // as a successful empty result and exit 0.
      if (const tinylamb::Status drain_status = executed.Value().GetStatus();
          drain_status != tinylamb::Status::kSuccess) {
        std::cerr << "SQL error: " << drain_status << '\n';
        context.Abort();
        return 1;
      }
      pending_output.push_back(std::move(buffer));
    } catch (const std::exception& error) {
      std::cerr << "error executing statement: " << error.what() << '\n';
      context.Abort();
      return 1;
    }
  }
  // T2799: an interrupt that landed during the last statement still gets a
  // clean Abort() before the durability barrier rather than a surprise
  // commit (the buffered output would print, then the signal-default death
  // would hit during PreCommit).
  if (g_interrupted.load()) {
    return interrupted("before commit");
  }
  // TransactionManager::PreCommit reports failures as a Status (it catches
  // internally and never rethrows), so a non-success status is the only
  // commit-failure channel here.
  const tinylamb::Status commit_status = context.PreCommit();
  if (commit_status != tinylamb::Status::kSuccess) {
    // NOTE: a non-success status can also mean the versions were already
    // published and only the durability barrier failed; in that case Abort()
    // is a no-op and re-running the script would double-apply. We still
    // report failure here (the caller must inspect the scripts' effects).
    std::cerr << "transaction commit failed (check whether writes were "
                 "already published): " << commit_status << "\n";
    context.Abort();
    return 1;
  }
  // Durability barrier passed; the buffered rows may now reach the user.
  for (const std::string& chunk : pending_output) {
    std::cout << chunk;
  }
  std::cout.flush();
  // T2794: write failures that arrive without SIGPIPE (/dev/full, NFS
  // errors, a stdout fd the parent already closed) only set the stream's
  // failbit. A result set that never reached the consumer must not exit 0
  // -- that silently seals a truncated output into a "successful" ETL run.
  if (!std::cout.good()) {
    std::cerr << "failed to write result output to standard output\n";
    return 1;
  }
  return 0;
}
