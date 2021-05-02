//
// Created by kumagi on 2022/04/19.
//

#ifndef TINYLAMB_TABLE_FUZZER_HPP
#define TINYLAMB_TABLE_FUZZER_HPP
#include <memory>

#include "common/random_string.hpp"
#include "database/database.hpp"
#include "database/page_storage.hpp"
#include "index/index_schema.hpp"
#include "table.hpp"
#include "type/constraint.hpp"
#include "type/schema.hpp"

namespace tinylamb {

void Try(uint64_t seed, bool verbose) {
  std::mt19937_64 rand(seed);
  auto RandomString = [&](size_t len = 16) {
    static const char alphanum[] =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";
    std::string ret;
    ret.reserve(len);
    for (size_t i = 0; i < len; ++i) {
      ret.push_back(alphanum[rand() % (sizeof(alphanum) - 1)]);
    }
    return ret;
  };
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

  const size_t kRows = 1;
  std::unordered_map<RowPosition, Row> rows;
  {
    for (size_t i = 0; i < kRows; ++i) {
      TransactionContext ctx = db.BeginContext();
      ASSIGN_OR_CRASH(Table, table, db.GetTable(ctx, "FuzzerTable"));
      Row new_row({Value(static_cast<int>(i)),
                   Value(RandomString(rand() % 300 + 10)),
                   Value(static_cast<double>(rand() % 1000))});
      ASSIGN_OR_CRASH(RowPosition, rp, table.Insert(ctx.txn_, new_row));
      if (verbose) {
        LOG(DEBUG) << "Insert: " << new_row;
      }
      rows.emplace(rp, new_row);
      ctx.txn_.PreCommit();
    }
  }
  if (verbose) {
    LOG(INFO) << "Insert finish";
  }
  for (size_t i = 0; i < kRows * 30; ++i) {
    TransactionContext ctx = db.BeginContext();
    ASSIGN_OR_CRASH(Table, table, db.GetTable(ctx, "FuzzerTable"));
    auto iter = rows.begin();
    size_t offset = rand() % rows.size();
    std::advance(iter, offset);
    if (verbose) {
      LOG(TRACE) << "Delete: " << iter->first << " : " << iter->second;
    }
    Status s = table.Delete(ctx.txn_, iter->first);
    if (s != Status::kSuccess) LOG(FATAL) << s;
    assert(s == Status::kSuccess);
    if (verbose) {
      // std::cerr << table << "\n";
    }
    rows.erase(iter);

    for (const auto& row : rows) {
      ASSIGN_OR_CRASH(Row, read_row, table.Read(ctx.txn_, row.first));
      if (row.second != read_row) {
        LOG(ERROR) << row.second << " vs " << read_row;
      }
      assert(s == Status::kSuccess);
      assert(row.second == read_row);
    }

    Row new_row({Value((int)offset), Value(RandomString(rand() % 4900 + 5000)),
                 Value((double)(rand() % 800))});
    if (verbose) {
      LOG(TRACE) << "Insert: " << new_row;
    }
    ASSIGN_OR_CRASH(RowPosition, rp, table.Insert(ctx.txn_, new_row));
    rows[rp] = new_row;
    for (const auto& row : rows) {
      ASSIGN_OR_CRASH(Row, read_row, table.Read(ctx.txn_, row.first));
      if (row.second != read_row) {
        LOG(ERROR) << "Row: " << i;
        LOG(ERROR) << row.second << " vs " << read_row;
      }
      assert(s == Status::kSuccess);
      assert(row.second == read_row);
    }
    ctx.txn_.PreCommit();
  }

  for (const auto& row : rows) {
    TransactionContext ctx = db.BeginContext();
    ASSIGN_OR_CRASH(Table, table, db.GetTable(ctx, "FuzzerTable"));
    ASSIGN_OR_CRASH(Row, read_row, table.Read(ctx.txn_, row.first));
    assert(row.second == read_row);
    Status s = table.Delete(ctx.txn_, row.first);
    if (s != Status::kSuccess) {
      LOG(ERROR) << s;
    }
    ctx.txn_.PreCommit();
  }
  std::remove(db.Storage().DBName().c_str());
  std::remove(db.Storage().LogName().c_str());
  std::remove(db.Storage().MasterRecordName().c_str());
}

}  // namespace tinylamb

#endif  // TINYLAMB_TABLE_FUZZER_HPP
