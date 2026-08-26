/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_DETAIL_RELATION_HPP
#define TINYLAMB_EXECUTOR_DETAIL_RELATION_HPP

#include <cstddef>
#include <memory>
#include <vector>

#include "executor/spill_file.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"

namespace tinylamb::relational_detail {

constexpr size_t kSpillPartitions = 32;
struct ExecutionRuntime;

struct Relation {
  Schema schema;
  std::vector<Row> rows;
  std::shared_ptr<SpillFile> spill;
  std::shared_ptr<SpillFile> spill_tail_;
  // USING join merge metadata: names whose duplicates across the joined
  // sides coalesce for bare references and star expansion.  Set by Join().
  std::shared_ptr<const std::vector<std::string>> using_columns;
  size_t charged_bytes_{0};
  size_t hash_joins{0};
  size_t hybrid_hash_joins{0};
  size_t in_memory_hash_joins{0};
  size_t nested_loop_joins{0};
  size_t join_comparisons{0};
  size_t peak_intermediate_rows{0};
  size_t spilled_rows_{0};

  explicit Relation(ExecutionRuntime* runtime = nullptr) : runtime_(runtime) {}
  Relation(const Relation&) = delete;
  Relation& operator=(const Relation&) = delete;
  Relation(Relation&& other) noexcept;
  Relation& operator=(Relation&& other) noexcept;
  ~Relation();

  void ReleaseCharge();
  void EnsureSpill();
  void AddRow(Row row);
  void FinishSpill();
  // Drop every buffered or spilled row (and its memory charge). Callers must
  // have copied the contents out beforehand (e.g. via ForEachRow); re-adding
  // rows afterwards starts a fresh spill cycle instead of appending to the
  // finished spill files.
  void ResetContents();
  void set_runtime(ExecutionRuntime* runtime) { runtime_ = runtime; }
  [[nodiscard]] ExecutionRuntime* runtime() const { return runtime_; }

  template <typename Fn>
  void ForEachRow(Fn&& fn) {
    for (const Row& row : rows) {
      fn(row);
    }
    if (spill) {
      spill->ForEachRow(fn);
    }
    if (spill_tail_) {
      spill_tail_->ForEachRow(fn);
    }
  }

  template <typename Fn>
  void ForEachRow(Fn&& fn) const {
    for (const Row& row : rows) {
      fn(row);
    }
    if (spill) {
      const_cast<SpillFile*>(spill.get())->ForEachRow(fn);
    }
    if (spill_tail_) {
      const_cast<Relation*>(this)->FinishSpill();
      const_cast<SpillFile*>(spill_tail_.get())->ForEachRow(fn);
    }
  }

  [[nodiscard]] bool HasSpill() const {
    return spill != nullptr || spill_tail_ != nullptr;
  }

  [[nodiscard]] size_t TotalRows() const {
    size_t total = rows.size();
    if (spill) total += spill->Count();
    if (spill_tail_) total += spill_tail_->Count();
    return total;
  }

 private:
  ExecutionRuntime* runtime_{nullptr};
};

using RelationPtr = std::shared_ptr<Relation>;

Relation MaterializeRelation(const Relation& source);

void CopyExecutionStats(Relation* destination, const Relation& source);

void NoteRelationSpill(ExecutionRuntime* runtime);

}  // namespace tinylamb::relational_detail

#endif  // TINYLAMB_EXECUTOR_DETAIL_RELATION_HPP
