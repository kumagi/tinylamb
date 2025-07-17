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
#include <string>
#include <vector>

#include "common/constants.hpp"
#include "common/log_message.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "parser/ast.hpp"
#include "parser/parser.hpp"
#include "parser/tokenizer.hpp"
#include "plan/optimizer.hpp"
#include "query/query_data.hpp"

int main(int argc, char** argv) {
  if (argc < 2) {
    LOG(FATAL) << "Set DB file";
    return 1;
  }
  tinylamb::Database db(argv[1]);

  // Sample SQL query
  std::string sql = "SELECT id, name FROM users WHERE id = 1;";

  // 1. Tokenize
  tinylamb::Tokenizer tokenizer(sql);
  std::vector<tinylamb::Token> tokens = tokenizer.Tokenize();

  // 2. Parse
  tinylamb::Parser parser(tokens);
  std::unique_ptr<tinylamb::Statement> statement = parser.Parse();

  // 3. Convert to QueryData and Optimize
  if (statement->Type() == tinylamb::StatementType::kSelect) {
    tinylamb::SelectStatement* select_stmt =
        dynamic_cast<tinylamb::SelectStatement*>(statement.get());
    if (select_stmt) {
      tinylamb::QueryData query_data;
      query_data.from_ = select_stmt->FromClause();
      query_data.where_ = select_stmt->WhereClause();
      query_data.select_ = select_stmt->SelectList();

      // Create a dummy TransactionContext for optimization
      tinylamb::TransactionContext ctx = db.BeginContext();

      tinylamb::StatusOr<tinylamb::Plan> plan_or_status =
          tinylamb::Optimizer::Optimize(query_data, ctx);

      if (plan_or_status.HasValue()) {
        tinylamb::Plan optimized_plan = plan_or_status.Value();
        std::cout << "Optimized Plan:\n" << optimized_plan << std::endl;
      } else {
        std::cerr << "Optimization failed: " << plan_or_status.GetStatus()
                  << std::endl;
      }
    } else {
      std::cerr << "Failed to cast to SelectStatement." << std::endl;
    }
  } else {
    std::cout << "Parsed statement is not a SELECT statement." << std::endl;
  }

  return 0;
}