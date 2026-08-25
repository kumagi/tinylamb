/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_MERGE_APPEND_EXECUTOR_HPP
#define TINYLAMB_MERGE_APPEND_EXECUTOR_HPP

#include <optional>
#include <utility>
#include <vector>

#include "executor/executor_base.hpp"
#include "executor/sort.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"

namespace tinylamb {

// Merges already sorted child streams while retaining UNION ALL multiplicity.
// Children are independently sorted by the caller when they do not already
// provide the requested order.
class MergeAppendExecutor final : public ExecutorBase {
 public:
  MergeAppendExecutor(std::vector<Executor> sources,
                      std::vector<Schema> schemas, Schema output_schema,
                      std::vector<SortExecutor::Key> keys)
      : sources_(std::move(sources)),
        schemas_(std::move(schemas)),
        output_schema_(std::move(output_schema)),
        keys_(std::move(keys)) {}

  bool Next(Row* destination, RowPosition* position) override;
  void Dump(std::ostream& output, int indent) const override;

 private:
  struct Head {
    Row row;
    RowPosition position;
    size_t source{0};
  };

  [[nodiscard]] Value KeyValue(const Head& head,
                               const SortExecutor::Key& key) const;
  [[nodiscard]] bool Before(const Head& left, const Head& right) const;
  void Initialize();

  std::vector<Executor> sources_;
  std::vector<Schema> schemas_;
  Schema output_schema_;
  std::vector<SortExecutor::Key> keys_;
  std::vector<Head> heads_;
  bool initialized_{false};
};

}  // namespace tinylamb

#endif  // TINYLAMB_MERGE_APPEND_EXECUTOR_HPP
