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
    Transaction txn = db.Begin();
    db.CreateTable(txn, schema);
    db.CreateIndex(txn, "FuzzerTable", {"num_idx", {0}});
    db.CreateIndex(txn, "FuzzerTable", {"str_idx", {1}});
    assert(txn.PreCommit() == Status::kSuccess);
  }

  const size_t kRows = 1;
  std::unordered_map<RowPosition, Row> rows;
  {
    for (size_t i = 0; i < kRows; ++i) {
      auto txn = db.Begin();
      Table table;
      db.GetTable(txn, "FuzzerTable", &table);
      Row new_row({Value((int)i), Value(RandomString(rand() % 300 + 10)),
                   Value((double)(rand() % 1000))});
      RowPosition rp;
      table.Insert(txn, new_row, &rp);
      if (verbose) {
        LOG(DEBUG) << "Insert: " << new_row;
      }
      rows.emplace(rp, new_row);
      txn.PreCommit();
    }
  }
  if (verbose) {
    LOG(INFO) << "Insert finish";
  }
  for (size_t i = 0; i < kRows * 30; ++i) {
    auto txn = db.Begin();
    Table table;
    db.GetTable(txn, "FuzzerTable", &table);
    auto iter = rows.begin();
    size_t offset = rand() % rows.size();
    std::advance(iter, offset);
    if (verbose) {
      LOG(TRACE) << "Delete: " << iter->first << " : " << iter->second;
    }
    Status s = table.Delete(txn, iter->first);
    if (s != Status::kSuccess) LOG(FATAL) << s;
    assert(s == Status::kSuccess);
    if (verbose) {
      // std::cerr << table << "\n";
    }
    rows.erase(iter);

    for (const auto& row : rows) {
      Row read_row;
      Status s = table.Read(txn, row.first, &read_row);
      if (s != Status::kSuccess) {
        LOG(ERROR) << s;
      }
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
    RowPosition rp;
    assert(table.Insert(txn, new_row, &rp));
    rows[rp] = new_row;
    for (const auto& row : rows) {
      Row read_row;
      s = table.Read(txn, row.first, &read_row);
      if (s != Status::kSuccess) {
        LOG(ERROR) << s;
      }
      if (row.second != read_row) {
        LOG(ERROR) << "Row: " << i;
        LOG(ERROR) << row.second << " vs " << read_row;
      }
      assert(s == Status::kSuccess);
      assert(row.second == read_row);
    }
    txn.PreCommit();
  }

  for (const auto& row : rows) {
    auto txn = db.Begin();
    Table table;
    db.GetTable(txn, "FuzzerTable", &table);
    Row read_row;
    assert(table.Read(txn, row.first, &read_row) == Status::kSuccess);
    assert(row.second == read_row);
    Status s = table.Delete(txn, row.first);
    if (s != Status::kSuccess) {
      LOG(ERROR) << s;
    }
    txn.PreCommit();
  }
  std::remove(db.Storage().DBName().c_str());
  std::remove(db.Storage().LogName().c_str());
  std::remove(db.Storage().MasterRecordName().c_str());
}

}  // namespace tinylamb

#endif  // TINYLAMB_TABLE_FUZZER_HPP
