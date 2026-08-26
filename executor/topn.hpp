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
#include "type/schema.hpp"

namespace tinylamb {

class TopNExecutor final : public ExecutorBase {
 public:
  struct Key {
    Expression expression;
    bool ascending{true};
    std::optional<bool> nulls_first;
  };

  TopNExecutor(Executor source, Schema schema, std::vector<Key> keys,
               size_t limit, size_t offset, bool with_ties = false)
      : source_(std::move(source)),
        schema_(std::move(schema)),
        keys_(std::move(keys)),
        limit_(limit),
        offset_(offset),
        with_ties_(with_ties) {}

  bool Next(Row* dst, RowPosition* position) override;
  void Dump(std::ostream& output, int indent) const override;

 private:
  void Materialize();

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
  bool materialized_{false};
};

}  // namespace tinylamb

#endif  // TINYLAMB_TOPN_EXECUTOR_HPP
