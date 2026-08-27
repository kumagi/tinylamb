/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_ANALYZE_SCAN_HPP
#define TINYLAMB_ANALYZE_SCAN_HPP

#include <cstddef>
#include <memory>
#include <ostream>
#include <string>
#include <string_view>
#include <vector>

#include "database/transaction_context.hpp"
#include "executor/executor_base.hpp"
#include "page/row_position.hpp"
#include "table/table.hpp"
#include "table/table_statistics.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"

namespace tinylamb {

class Database;

class AnalyzeScan : public ExecutorBase {
 public:
  AnalyzeScan(TransactionContext& ctx, std::string_view schema_name,
              const Table& table, size_t sample_size = 1024,
              size_t bucket_count = 16, Database* db = nullptr);
  AnalyzeScan(TransactionContext& ctx, std::string_view schema_name,
              Executor child, const Schema& schema, size_t sample_size = 1024,
              size_t bucket_count = 16, Database* db = nullptr);
  ~AnalyzeScan() override = default;

  bool Next(Row* dst, RowPosition* rp) override;
  void Dump(std::ostream& o, int indent) const override;

  [[nodiscard]] size_t RowsAnalyzed() const { return rows_analyzed_; }
  [[nodiscard]] const TableStatistics& ResultStatistics() const {
    return result_statistics_;
  }

 private:
  TransactionContext* ctx_;
  std::string schema_name_;
  const Table* table_{nullptr};
  Executor child_;
  Schema schema_;
  size_t sample_size_;
  size_t bucket_count_;
  Database* db_{nullptr};
  bool executed_{false};
  size_t rows_analyzed_{0};
  TableStatistics result_statistics_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_ANALYZE_SCAN_HPP
