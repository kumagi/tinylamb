/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_TOPN_EXECUTOR_HPP
#define TINYLAMB_TOPN_EXECUTOR_HPP

#include <cstddef>
#include <memory>
#include <optional>
#include <vector>

#include "executor/executor_base.hpp"
#include "expression/expression.hpp"
#include "page/row_position.hpp"
#include "type/collation.hpp"
#include "type/schema.hpp"

namespace tinylamb {

class TopNExecutor final : public ExecutorBase {
 public:
  struct Key {
    Expression expression;
    bool ascending{true};
    std::optional<bool> nulls_first{std::nullopt};
    // Collation the key's comparison runs under, resolved from the key
    // expression by the planner (COLLATE(...)). The heap compares evaluated
    // values through the same SortKeyOrder contract the sort operators use,
    // so a case-insensitive ORDER BY keeps its LIMIT over the right rows.
    Collation collation{Collation::kNone};
  };

  TopNExecutor(Executor source, Schema schema, std::vector<Key> keys,
               size_t limit, size_t offset, bool with_ties = false)
      : source_(std::move(source)),
        schema_(std::move(schema)),
        keys_(std::move(keys)),
        limit_(limit),
        offset_(offset),
        with_ties_(with_ties) {}

  size_t NextBatch(DataChunk* destination,
                   size_t max_rows = kDefaultVectorSize) override;
  void Dump(std::ostream& output, int indent) const override;

 private:
  Status Materialize();

  struct Candidate {
    Row row;
    RowPosition position;
    std::vector<Value> keys;
    size_t sequence{0};
  };

  Executor source_;
  Schema schema_;
  std::vector<Key> keys_;
  size_t limit_{0};
  size_t offset_{0};
  bool with_ties_{false};
  std::vector<Candidate> rows_;
  size_t output_index_{0};
  size_t output_end_{0};
  size_t input_rows_{0};
  size_t heap_capacity_{0};
  bool materialized_{false};
  bool fused_scan_{false};
};

}  // namespace tinylamb

#endif  // TINYLAMB_TOPN_EXECUTOR_HPP
