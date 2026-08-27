/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_CHUNKED_SCAN_HPP
#define TINYLAMB_EXECUTOR_CHUNKED_SCAN_HPP

#include <cstddef>
#include <iosfwd>
#include <memory>
#include <optional>
#include <vector>

#include "executor/data_chunk.hpp"
#include "executor/executor_base.hpp"
#include "expression/expression.hpp"
#include "index/index.hpp"
#include "table/iterator.hpp"
#include "table/table.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {

class Transaction;

// Morsel-driven table and index scan iterator directly populating DataChunk
// batches for vectorized execution without per-row allocation.
class ChunkedScan : public ExecutorBase {
 public:
  // Table scan constructor (partitioned by page morsels).
  ChunkedScan(Transaction& txn, Table& table, Schema schema,
              std::vector<slot_t> projection = {},
              std::optional<Expression> filter = std::nullopt,
              size_t pages_per_morsel = 8);

  // Index scan constructor.
  ChunkedScan(Transaction& txn, Table& table, const Index& index, Schema schema,
              const Value& begin = Value(), const Value& end = Value(),
              bool ascending = true, std::vector<slot_t> projection = {},
              std::optional<Expression> filter = std::nullopt);

  ~ChunkedScan() override = default;

  bool Next(Row* dst, RowPosition* rp) override;

  size_t NextBatch(DataChunk* destination,
                   size_t max_rows = kDefaultVectorSize) override;

  void Dump(std::ostream& o, int indent) const override;
  void Explain(std::ostream& o, int indent) const override;

  [[nodiscard]] const Schema& GetSchema() const { return schema_; }
  [[nodiscard]] size_t MorselCount() const { return morsels_.size(); }
  [[nodiscard]] size_t CurrentMorselIndex() const { return current_morsel_idx_; }

 private:
  void InitializeTableMorsels();
  size_t FillFromIndex(DataChunk* destination, size_t max_rows);
  size_t FillFromTableMorsels(DataChunk* destination, size_t max_rows);

  Transaction& txn_;
  Table& table_;
  Schema schema_;
  std::vector<slot_t> projection_;
  std::optional<Expression> filter_;
  size_t pages_per_morsel_{8};

  // Table scan state
  std::vector<Table::ScanMorsel> morsels_;
  size_t current_morsel_idx_{0};
  std::optional<Iterator> current_iter_;
  bool table_scan_exhausted_{false};

  // Index scan state
  const Index* index_{nullptr};
  Value index_begin_;
  Value index_end_;
  bool ascending_{true};
  std::optional<Iterator> index_iter_;
  bool is_index_scan_{false};

  // Row-by-row fallback buffering
  DataChunk buffer_;
  size_t buffer_offset_{0};
};

}  // namespace tinylamb

#endif  // TINYLAMB_EXECUTOR_CHUNKED_SCAN_HPP
