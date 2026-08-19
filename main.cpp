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

#include <iostream>
#include <iterator>
#include <string>

#include "database/database.hpp"
#include "query/sql_engine.hpp"
#include "type/row.hpp"

int main(int argc, char** argv) {
  if (argc != 2) {
    std::cerr << "usage: tinylamb <database-file> < input.sql\n";
    return 2;
  }

  std::string sql((std::istreambuf_iterator<char>(std::cin)),
                  std::istreambuf_iterator<char>());
  if (sql.empty()) {
    std::cerr << "no SQL was provided on standard input\n";
    return 2;
  }

  tinylamb::Database database(argv[1]);
  tinylamb::TransactionContext context = database.BeginContext();
  tinylamb::SqlEngine engine(database);
  tinylamb::StatusOr<tinylamb::Executor> prepared =
      engine.Prepare(context, sql);
  if (!prepared.HasValue()) {
    std::cerr << "SQL error: " << engine.LastError();
    if (engine.LastError().empty()) {
      std::cerr << prepared.GetStatus();
    }
    std::cerr << '\n';
    context.Abort();
    return 1;
  }

  tinylamb::Row row;
  tinylamb::Executor executor = std::move(prepared.Value());
  while (executor->Next(&row, nullptr)) {
    std::cout << row << '\n';
  }
  if (context.PreCommit() != tinylamb::Status::kSuccess) {
    std::cerr << "transaction commit failed\n";
    return 1;
  }
  return 0;
}
