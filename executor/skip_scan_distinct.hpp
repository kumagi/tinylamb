/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_SKIP_SCAN_DISTINCT_HPP
#define TINYLAMB_EXECUTOR_SKIP_SCAN_DISTINCT_HPP

#include <cstddef>
#include <iosfwd>
#include <memory>
#include <string>
#include <vector>

#include "common/constants.hpp"
#include "executor/data_chunk.hpp"
#include "executor/executor_base.hpp"
#include "expression/expression.hpp"
#include "index/lsm_index.hpp"
#include "page/row_position.hpp"
#include "transaction/transaction_manager.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {

class Table;
class Index;
class Transaction;

// Index skip-scan distinct executor that jumps directly to the next distinct
// key using LsmIndexIterator seek instead of scanning duplicates.
class SkipScanDistinct : public ExecutorBase {
 public:
  SkipScanDistinct(Transaction& txn, const Table& table, const Index& index,
                   const Value& begin, const Value& end, bool ascending,
                   Expression where, Schema sc, size_t prefix_cols = 0,
                   TransactionManager::IndexReadGuard index_read_guard = {});

  SkipScanDistinct(Transaction& txn, const Table& table, const Index& index,
                   const std::vector<Value>& begin_key,
                   const std::vector<Value>& end_key, bool ascending,
                   Expression where, Schema sc, size_t prefix_cols = 0,
                   TransactionManager::IndexReadGuard index_read_guard = {});

  SkipScanDistinct(const SkipScanDistinct&) = delete;
  SkipScanDistinct(SkipScanDistinct&&) = delete;
  SkipScanDistinct& operator=(const SkipScanDistinct&) = delete;
  SkipScanDistinct& operator=(SkipScanDistinct&&) = delete;
  ~SkipScanDistinct() override = default;

  size_t NextBatch(DataChunk* destination,
                   size_t max_rows = kDefaultVectorSize) override;
  void Dump(std::ostream& o, int indent) const override;
  void Explain(std::ostream& o, int indent) const override;

 private:
  void SeekNextDistinct(const std::string& prev_encoded_key,
                        const Row& prev_key);
  [[nodiscard]] Status ResolveCurrentRow();

  Transaction& txn_;
  const Table& table_;
  const Index& index_;
  std::vector<Value> begin_key_;
  std::vector<Value> end_key_;
  bool ascending_;
  Expression where_;
  Schema schema_;
  size_t prefix_cols_{0};
  bool is_unique_{false};

  TransactionManager::IndexReadGuard index_read_guard_;
  LsmIndex bpt_;
  LsmIndexIterator iter_;
  RowPosition current_pos_;
  Row current_index_key_;
  Row current_row_;
  bool finished_{false};
};

using SkipScanDistinctExecutor = SkipScanDistinct;

}  // namespace tinylamb

#endif  // TINYLAMB_EXECUTOR_SKIP_SCAN_DISTINCT_HPP
