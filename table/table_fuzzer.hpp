//
// Created by kumagi on 2022/04/19.
//

#ifndef TINYLAMB_TABLE_FUZZER_HPP
#define TINYLAMB_TABLE_FUZZER_HPP
#include <memory>

#include "common/random_string.hpp"
#include "page/page_manager.hpp"
#include "recovery/checkpoint_manager.hpp"
#include "recovery/logger.hpp"
#include "recovery/recovery_manager.hpp"
#include "table/table.hpp"
#include "transaction/lock_manager.hpp"
#include "transaction/transaction_manager.hpp"
#include "type/row.hpp"
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
  std::string log_name = db_name + ".log";
  std::string master_record_name = db_name + ".master.log";

  LockManager lm;
  PageManager p(db_name + ".db", 20);
  Logger l(log_name);
  RecoveryManager r(log_name, p.GetPool());
  TransactionManager tm(&lm, &l, &r);
  CheckpointManager cm(master_record_name, &tm, p.GetPool(), 1);

  Schema schema("FuzzerTable", {Column("f_id", ValueType::kInt64,
                                       Constraint(Constraint::kIndex)),
                                Column("name", ValueType::kVarChar),
                                Column("double", ValueType::kDouble)});

  page_id_t row_page, num_idx, str_idx;
  {
    Transaction txn = tm.Begin();
    PageRef page = p.AllocateNewPage(txn, PageType::kRowPage);
    PageRef num_idx_root = p.AllocateNewPage(txn, PageType::kLeafPage);
    PageRef str_idx_root = p.AllocateNewPage(txn, PageType::kLeafPage);
    row_page = page->PageID();
    num_idx = num_idx_root->PageID();
    str_idx = str_idx_root->PageID();
    assert(txn.PreCommit() == Status::kSuccess);
  }
  std::vector<Index> idx;
  idx.push_back({"num_idx", {0}, num_idx});
  idx.push_back({"str_idx", {1}, str_idx});

  Table table(&p, schema, row_page, idx);
  const size_t kRows = 5;
  std::unordered_map<RowPosition, Row> rows;
  {
    for (size_t i = 0; i < kRows; ++i) {
      Transaction txn = tm.Begin();
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

  for (size_t i = 0; i < kRows * 30; ++i) {
    Transaction txn = tm.Begin();
    auto iter = rows.begin();
    size_t offset = rand() % rows.size();
    std::advance(iter, offset);
    if (verbose) {
      LOG(TRACE) << "Delete: " << iter->first << " : " << iter->second;
    }
    Status s = table.Delete(txn, iter->first);
    if (s != Status::kSuccess) LOG(FATAL) << s;
    assert(s == Status::kSuccess);
    // assert(table.SanityCheckForTest(&page_manager));
    if (verbose) {
      // std::cerr << table << "\n";
    }
    rows.erase(iter);
    LOG(INFO) << "Insert finished";

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
      Status s = table.Read(txn, row.first, &read_row);
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
    auto txn = tm.Begin();
    Row read_row;
    assert(table.Read(txn, row.first, &read_row) == Status::kSuccess);
    assert(row.second == read_row);
    assert(table.Delete(txn, row.first) == Status::kSuccess);
    txn.PreCommit();
  }
  std::remove((db_name + ".db").c_str());
  std::remove(log_name.c_str());
  std::remove(master_record_name.c_str());
}

}  // namespace tinylamb

#endif  // TINYLAMB_TABLE_FUZZER_HPP
