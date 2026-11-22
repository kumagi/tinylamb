/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_DISTINCT_EXECUTOR_HPP
#define TINYLAMB_DISTINCT_EXECUTOR_HPP
#include <unordered_set>
#include <vector>

#include "executor/executor_base.hpp"
#include "expression/expression.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"

namespace tinylamb {

struct DistinctRowEqual {
  bool operator()(const Row& left, const Row& right) const;
};

// Hash consistent with DistinctRowEqual: a case-insensitive VARCHAR hashes
// its folded text, so 'ABC' and 'abc' (equal once folded) share a bucket.
// std::hash<Row> hashes raw bytes and let CI peers slip into the set twice.
struct DistinctRowHash {
  size_t operator()(const Row& row) const;
};

using DistinctRowSet =
    std::unordered_set<Row, DistinctRowHash, DistinctRowEqual>;

class DistinctExecutor : public ExecutorBase {
 public:
  // `distinct_on` evaluates key expressions per row (DISTINCT ON). `key_cols`
  // names positional columns forming the duplicate key while the whole row
  // (including columns past the key, e.g. hidden ORDER BY carriers) is still
  // emitted - the finishing path's way to dedupe on the visible prefix only.
  explicit DistinctExecutor(Executor source, Schema schema = {},
                            std::vector<Expression> distinct_on = {},
                            std::vector<slot_t> key_cols = {})
      : source_(std::move(source)),
        schema_(std::move(schema)),
        distinct_on_(std::move(distinct_on)),
        key_cols_(std::move(key_cols)) {}
  size_t NextBatch(DataChunk* destination,
                   size_t max_rows = kDefaultVectorSize) override;
  void Dump(std::ostream& output, int indent) const override;

 private:
  Executor source_;
  Schema schema_;
  std::vector<Expression> distinct_on_;
  std::vector<slot_t> key_cols_;
  DataChunk input_batch_;
  size_t input_offset_{0};
  // T1793: measured counters, surfaced by Dump only after a drain.
  size_t input_rows_{0};
  size_t emitted_rows_{0};
  DistinctRowSet seen_;
  DistinctRowSet seen_keys_;
};

class SortDistinctExecutor final : public ExecutorBase {
 public:
  explicit SortDistinctExecutor(Executor source) : source_(std::move(source)) {}
  size_t NextBatch(DataChunk* destination,
                   size_t max_rows = kDefaultVectorSize) override;
  void Dump(std::ostream& output, int indent) const override;

 private:
  Executor source_;
  DataChunk input_batch_;
  size_t input_offset_{0};
  Row previous_;
  bool have_previous_{false};
  // T1793: see DistinctExecutor::Dump (counters surfaced after a drain).
  size_t input_rows_{0};
  size_t emitted_rows_{0};
};
}  // namespace tinylamb
#endif
