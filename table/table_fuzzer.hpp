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

#ifndef TINYLAMB_TABLE_FUZZER_HPP
#define TINYLAMB_TABLE_FUZZER_HPP
#include <memory>
#include <string>
#include <unordered_map>

#include "common/byte_stream.hpp"
#include "common/random_string.hpp"
#include "database/database.hpp"
#include "database/page_storage.hpp"
#include "index/index_schema.hpp"
#include "table.hpp"
#include "type/constraint.hpp"
#include "type/schema.hpp"

namespace tinylamb {

// Byte-driven table fuzzer.  The input directly encodes an Insert / Delete /
// Verify operation stream with row values taken from the same buffer, so
// libFuzzer can steer value shapes and interleavings toward row-page and
// secondary-index edge cases instead of sampling a PRNG.
void Try(const uint8_t* data, size_t size, bool verbose) {
  ByteStream stream(data, size);
  std::string db_name = RandomString();
  Database db(db_name);

  Schema schema("FuzzerTable", {Column("f_id", ValueType::kInt64,
                                       Constraint(Constraint::kIndex)),
                                Column("name", ValueType::kVarChar),
                                Column("double", ValueType::kDouble)});
  {
    TransactionContext ctx = db.BeginContext();
    db.CreateTable(ctx, schema);
    db.CreateIndex(ctx, "FuzzerTable", {"num_idx", {0}});
    db.CreateIndex(ctx, "FuzzerTable", {"str_idx", {1}});
    assert(ctx.txn_.PreCommit() == Status::kSuccess);
  }

  std::unordered_map<RowPosition, Row> rows;
  constexpr size_t kMaxOps = 200;
  for (size_t i = 0; i < kMaxOps && stream.Remaining(); ++i) {
    TransactionContext ctx = db.BeginContext();
    ASSIGN_OR_CRASH(Table, table, db.GetTable(ctx, "FuzzerTable"));
    switch (stream.Pick(3)) {
      case 0: {  // Insert
        Row new_row({Value(static_cast<int64_t>(stream.Pick(1000))),
                     Value(std::string(stream.Bytes(stream.Pick(64)))),
                     Value(static_cast<double>(stream.Pick(1000)))});
        if (verbose) {
          LOG(DEBUG) << "Insert: " << new_row;
        }
        StatusOr<RowPosition> rp = table.Insert(ctx.txn_, new_row);
        if (rp.GetStatus() == Status::kDuplicates) {
          // A duplicate unique-index key is legitimately rejected; the
          // rejected row is rolled back, so nothing is modeled.
          break;
        }
        ASSIGN_OR_CRASH(RowPosition, pos, rp);
        rows[pos] = new_row;
        break;
      }
      case 1: {  // Delete a row from the model.
        if (!rows.empty()) {
          auto iter = rows.begin();
          std::advance(iter, stream.Pick(rows.size()));
          if (verbose) {
            LOG(TRACE) << "Delete: " << iter->first << " : " << iter->second;
          }
          Status s = table.Delete(ctx.txn_, iter->first);
          assert(s == Status::kSuccess);
          rows.erase(iter);
        }
        break;
      }
      default: {  // Verify the whole model against the table.
        for (const auto& [rp, expected_row] : rows) {
          ASSIGN_OR_CRASH(Row, read_row, table.Read(ctx.txn_, rp));
          assert(expected_row == read_row);
        }
        break;
      }
    }
    ctx.txn_.PreCommit();
  }
  db.DeleteAll();
}

}  // namespace tinylamb
#endif  // TINYLAMB_TABLE_FUZZER_HPP
