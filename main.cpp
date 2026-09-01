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
#include <cstddef>
#include <cctype>
#include <exception>
#include <iostream>
#include <iterator>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "recovery/recovery_manager.hpp"
#include "common/status_or.hpp"
#include "common/constants.hpp"
#include "database/database.hpp"
#include "executor/executor_base.hpp"
#include "query/sql_engine.hpp"
#include "server/postgres_protocol.hpp"
#include "type/row.hpp"

int main(int argc, char** argv) {
  // --force: a torn/unparsable WAL tail is truncated to its intact prefix
  // instead of treating the corruption as fatal (the default).
  bool force = false;
  std::vector<std::string> args(argv + 1, argv + argc);
  auto flag = std::remove(args.begin(), args.end(), "--force");
  if (flag != args.end()) {
    force = true;
    args.erase(flag, args.end());
  }
  if (args.size() != 1) {
    std::cerr << "usage: tinylamb [--force] <database-file> < input.sql\n";
    return 2;
  }
  tinylamb::RecoveryManager::SetTornTailTruncationAllowed(force);

  std::string sql((std::istreambuf_iterator<char>(std::cin)),
                  std::istreambuf_iterator<char>());
  const std::vector<std::string> statements =
      tinylamb::pgwire::SplitSqlStatements(sql);
  if (statements.empty()) {
    std::cerr << "no SQL was provided on standard input\n";
    return 2;
  }

  tinylamb::Database database(args[0]);
  tinylamb::TransactionContext context = database.BeginContext();
  tinylamb::SqlEngine engine(database);
  // One implicit transaction wraps every statement of the script; the first
  // failure aborts the whole run instead of terminating the process.
  for (const std::string& statement : statements) {
    try {
      tinylamb::StatusOr<tinylamb::QueryResult> executed =
          engine.Execute(context, statement);
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
      executed.Value().ForEach(
          [](const tinylamb::Row& row) { std::cout << row << '\n'; });
    } catch (const std::exception& error) {
      std::cerr << "error executing statement: " << error.what() << '\n';
      context.Abort();
      return 1;
    }
  }
  try {
    if (context.PreCommit() != tinylamb::Status::kSuccess) {
      // Keep the cleanup symmetric with the Prepare failure path above.
      std::cerr << "transaction commit failed\n";
      context.Abort();
      return 1;
    }
  } catch (const std::exception& error) {
    // TransactionManager::PreCommit already released the locks and marked the
    // transaction aborted before rethrowing, so no extra Abort() here.
    std::cerr << "error during commit: " << error.what() << '\n';
    return 1;
  }
  return 0;
}
