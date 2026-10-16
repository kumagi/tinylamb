/**
 * Copyright 2023 KUMAZAKI Hiroki
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef TINYLAMB_PRODUCT_PLAN_HPP
#define TINYLAMB_PRODUCT_PLAN_HPP

#include <optional>

#include "expression/expression.hpp"
#include "plan/plan.hpp"
#include "table/table_statistics.hpp"

namespace tinylamb {

// Physical join configuration types owned by the executor layer
// (executor/hash_join_mode.hpp, executor/join_kind.hpp). The plan layer only
// carries their values opaquely; the relational factory translates them when
// emitting executors.
enum class HashJoinMode : uint8_t;
enum class JoinKind : uint8_t;

class ExecutorBase;

// Logical probe descriptor for the index nested-loop (lookup join) shape.
// `left_columns` / `right_columns` are aligned with the right index's leading
// key columns -- in the index's own key order, not in the order the equi-join
// happens to list its keys. Positions [0, range_position) are equality-pinned
// and form the seek prefix; when `range_position` is set, that position
// carries a range bound compared as
// `right_columns[range_position] <range_operation>
// left_columns[range_position]`. Column names only: the relational factory
// resolves them into slot offsets when it emits the executor.
struct LookupSeek {
  std::vector<ColumnName> left_columns;
  std::vector<ColumnName> right_columns;
  std::optional<size_t> range_position;
  BinaryOperation range_operation{BinaryOperation::kLessThan};
  // Two-sided (interval) probe: a second bound on the SAME key position, on
  // the opposite side of |range_operation| -- one lower and one upper bound,
  // compared as `right <second_range_operation> second_range_left_column`.
  // Its value comes from this left column of the probe row (the first
  // bound's value rides in `left_columns[range_position]`).
  std::optional<ColumnName> second_range_left_column;
  std::optional<BinaryOperation> second_range_operation;
};

// True when `operation`, read as `right <operation> bound`, is a lower bound
// of the seek interval (the seek starts at the bound); false for the upper
// bounds (`right < bound` / `right <= bound`).
[[nodiscard]] bool IsLowerLookupRange(BinaryOperation operation);

// Range-probe whitelist for the index nested-loop join. The seek range is only
// an enclosure (the executor re-checks the comparison per candidate), so a
// bound is admissible exactly when its memcomparable encoding orders the way
// Value::operator< does for the same type. INT64 and DATE are the signed
// integer orderings the band join and the range-sliced index scan already rely
// on; every other type (FLOAT64 with its NaN/-0.0 holes, VARCHAR collation,
// arrays) is left to the residual predicate instead of risking a seek that
// excludes rows the comparison accepts.
[[nodiscard]] bool SupportsLookupRangeType(ValueType type);

// Derives the index nested-loop probe from the right index's key order: the
// leading key columns the equi-join pins, walked in the index's own order.
// Returns nullopt when no leading key column is a join key (no point lookup
// exists) or when the two key vectors are not the same length.
[[nodiscard]] std::optional<LookupSeek> DeriveLookupSeek(
    const Index& index, const Schema& right_schema,
    const std::vector<ColumnName>& right_columns,
    const std::vector<ColumnName>& left_columns);

class ProductPlan final : public PlanBase {
 public:
  ProductPlan(Plan left_src, std::vector<ColumnName> left_cols, Plan right_src,
              std::vector<ColumnName> right_cols);
  ProductPlan(Plan left_src, std::vector<ColumnName> left_cols, Plan right_src,
              std::vector<ColumnName> right_cols, HashJoinMode hash_mode,
              std::optional<size_t> estimated_rows = std::nullopt);
  // Explicit hash join kind. Semi/anti kinds emit only the left child's
  // columns; outer kinds retain both schemas and add NULL padding.
  ProductPlan(Plan left_src, std::vector<ColumnName> left_cols, Plan right_src,
              std::vector<ColumnName> right_cols, HashJoinMode hash_mode,
              JoinKind kind,
              std::optional<size_t> estimated_rows = std::nullopt);
  // Same, always executing in memory (the decorrelation rewrite in the
  // optimizer emits this shape).
  ProductPlan(Plan left_src, std::vector<ColumnName> left_cols, Plan right_src,
              std::vector<ColumnName> right_cols, JoinKind kind,
              std::optional<size_t> estimated_rows = std::nullopt);
  ProductPlan(Plan left_src, std::vector<ColumnName> left_cols,
              const Table& right_tbl, const Index& idx,
              std::vector<ColumnName> right_cols,
              const TableStatistics& right_ts,
              std::optional<size_t> estimated_rows = std::nullopt);
  // Index nested-loop (lookup join) whose right side is exposed under a
  // relation-renamed schema:
  // `right_cols` resolve against the physical table while `declared_output`
  // names the emitted columns (Phase 8 self-joins).
  ProductPlan(Plan left_src, std::vector<ColumnName> left_cols,
              const Table& right_tbl, const Index& idx,
              std::vector<ColumnName> right_cols,
              const TableStatistics& right_ts, Schema declared_output,
              std::optional<size_t> estimated_rows = std::nullopt);
  ProductPlan(Plan left_src, Plan right_src);
  // Cross / nested-loop shape with an explicit join kind. Inner lowers to a
  // cross product (or NestedLoopJoin when a residual note is attached);
  // LeftOuter lowers to a null-padding NestedLoopJoin. Semi/anti/full/right
  // residuals lower to the blocked BatchNestedLoopJoin (the tuple nested
  // loop only implements inner/left-outer).
  ProductPlan(Plan left_src, Plan right_src, JoinKind kind,
              std::optional<size_t> estimated_rows = std::nullopt);
  ProductPlan(const ProductPlan&) = delete;
  ProductPlan(ProductPlan&&) = delete;
  ProductPlan& operator=(const ProductPlan&) = delete;
  ProductPlan& operator=(ProductPlan&&) = delete;
  ~ProductPlan() override = default;

  Executor EmitExecutor(TransactionContext& ctx) const override;
  [[nodiscard]] bool ContainsParameterSlots() const override;
  bool InstallParameterSlots(const std::vector<Value>& parameters,
                             size_t* cursor,
                             std::vector<Expression>* slots) override;

  [[nodiscard]] const Table* ScanSource() const override { return nullptr; }
  [[nodiscard]] const Schema& GetSchema() const override;
  [[nodiscard]] const TableStatistics& GetStats() const override {
    return stats_;
  }

  [[nodiscard]] size_t AccessRowCount() const override;
  [[nodiscard]] size_t EmitRowCount() const override;
  [[nodiscard]] bool IsOrderedBy(
      const std::vector<Expression>& expressions,
      const std::vector<bool>& ascending) const override;
  [[nodiscard]] bool IsOrderedBy(
      const std::vector<Expression>& expressions,
      const std::vector<bool>& ascending,
      const std::vector<std::optional<bool>>& nulls_first) const override;
  [[nodiscard]] HashJoinMode GetHashJoinMode() const { return hash_mode_; }
  [[nodiscard]] JoinKind Kind() const { return kind_; }
  [[nodiscard]] const Plan& Left() const { return left_src_; }
  [[nodiscard]] const Plan& Right() const { return right_src_; }
  [[nodiscard]] const std::vector<ColumnName>& LeftKeys() const {
    return left_cols_;
  }
  [[nodiscard]] const std::vector<ColumnName>& RightKeys() const {
    return right_cols_;
  }
  // Per-key null-safety (IS NOT DISTINCT FROM equijoins) and the residual
  // predicate associated with this join.
  void SetJoinNotes(std::vector<bool> key_null_safe, Expression residual_note) {
    key_null_safe_ = std::move(key_null_safe);
    residual_note_ = std::move(residual_note);
  }
  [[nodiscard]] const std::vector<bool>& KeyNullSafe() const {
    return key_null_safe_;
  }
  [[nodiscard]] const Expression& ResidualNote() const {
    return residual_note_;
  }
  // Block-nested-loop strategy marker. The cross/nested-loop shape (empty
  // key columns + residual note) lowers to the tuple NestedLoopJoin by
  // default; the batch_nested_loop implementation rule sets this to select
  // the blocked BatchNestedLoopJoin executor instead (same logical info,
  // different physical strategy — like HashJoinMode kInMemory/kHybrid).
  void PreferBatchNestedLoop() { batch_nested_loop_ = true; }
  [[nodiscard]] bool PrefersBatchNestedLoop() const {
    return batch_nested_loop_;
  }
  // ---- Dynamic (runtime) join filter ----------------------------------------
  // Marks this hash join as a dynamic filter producer: the factory makes the
  // `build_left` side the build, and the executor publishes a sound filter
  // over its join keys to the probe scan before it pulls probe rows
  // (Phase 2 "Dynamic Filter Join / SIP / Bloom").
  //
  // Only the *orientation* is logical information here; which filter shape
  // (exact set / min-max / Bloom) the build domain earns, and whether the
  // probe child can prune at all, are executor decisions. The implementation
  // rule sets this only for an inner, non null-safe, single INT64/DATE equi
  // join whose probe group carries a dynamic-filter key annotation and whose
  // estimated build side is materially smaller than the probe side.
  void PreferDynamicFilter(bool build_left) {
    dynamic_filter_ = true;
    dynamic_filter_build_left_ = build_left;
  }
  [[nodiscard]] bool PrefersDynamicFilter() const { return dynamic_filter_; }
  [[nodiscard]] bool DynamicFilterBuildsLeft() const {
    return dynamic_filter_build_left_;
  }
  // ---- Hash semi reduction --------------------------------------------------
  // Marks this semi/anti hash join as one whose BUILD side may be shrunk before
  // it is hashed (Phase 2 "Hash Semi Reduction"). A semi/anti join emits only
  // the probe side, so its build is a pure membership test: keeping one row per
  // distinct join key, and keeping only keys the probe side actually carries,
  // cannot change a single output row.
  //
  // The marker names no filter shape and no key list: the executor decides the
  // domain, dedups the build, and falls back to the unrestricted intake when
  // the probe domain turns out to be larger than it is worth. The
  // implementation rule sets it only for a semi/anti equi-join without a
  // residual whose probe group carries the semi-reduction key annotation and
  // where the build side is large enough for the reduction to pay.
  void PreferSemiReduction() { semi_reduction_ = true; }
  [[nodiscard]] bool PrefersSemiReduction() const { return semi_reduction_; }
  // ---- Index nested-loop (lookup join) probe -------------------------------
  // The probe is derived from the right index's key order as soon as the
  // table/index shape is known, so every index-join ProductPlan probes a real
  // key prefix. A composite index whose trailing key columns are not join keys
  // used to seek with the join-key order, landing between unrelated stored
  // keys and silently dropping matches.
  [[nodiscard]] const std::optional<LookupSeek>& Lookup() const {
    return lookup_seek_;
  }
  // Extends the derived equality prefix with a range bound on the first
  // unpinned index key column. Returns false (and leaves the plan unchanged)
  // when the range cannot be probed safely -- the caller then keeps the
  // conjunct as a residual predicate, which is an explicit fallback rather
  // than a dropped predicate:
  //  * `range_key_offset` is not the first unpinned index key column, so the
  //    bound would not narrow the seek;
  //  * the left/right column types differ, so the per-row re-check could not
  //    decide the comparison;
  //  * the right (index key) column type is outside SupportsLookupRangeType.
  //
  // A second call with the same `range_key_offset` adds the opposite side and
  // turns the one-sided probe into an interval probe
  // `lower <= right <= upper` (boundaries re-checked per candidate).  A
  // same-side second bound is rejected: it would not narrow the interval and
  // belongs to the residual predicate.
  bool SetLookupRange(size_t range_key_offset, ColumnName left_column,
                      ColumnName right_column, BinaryOperation operation);
  void Dump(std::ostream& o, int indent) const override;
  [[nodiscard]] std::string ToString() const override;

 private:
  void DeriveLookupSeek();
  [[nodiscard]] std::string LookupProbeText() const;

  Plan left_src_;
  Plan right_src_;
  std::vector<ColumnName> left_cols_;
  std::vector<ColumnName> right_cols_;
  const Table* right_tbl_;
  const Index* right_idx_;
  const TableStatistics* right_ts_;
  HashJoinMode hash_mode_{};
  JoinKind kind_{};
  std::vector<bool> key_null_safe_;
  Expression residual_note_;
  bool batch_nested_loop_{false};
  bool dynamic_filter_{false};
  bool dynamic_filter_build_left_{false};
  bool semi_reduction_{false};
  std::optional<LookupSeek> lookup_seek_;
  Schema output_schema_;
  TableStatistics stats_;
};

[[nodiscard]] size_t EstimatedJoinRowCount(
    JoinKind kind, size_t left_rows, size_t right_rows,
    std::optional<size_t> match_rows = std::nullopt);

// ---- Hash semi reduction (Phase 2 "Hash Semi Reduction") --------------------
// An existence-only (semi / anti / null-aware anti) join emits just the probe
// side, so its hash build is a pure membership test over the join key. The
// helpers below estimate how many build rows the probe side's key domain can
// still admit, and whether cutting the build is worth the bookkeeping. Both the
// Cascades implementation rule and the subquery-decorrelation path -- which
// builds the same ProductPlan shape directly -- gate on them, so the two paths
// can never disagree about whether a semi join is worth reducing.

// Estimated distinct values of one join key column, or 0 when the plan carries
// no statistics for it. 0 is a legal (coarser) answer, never an unsafe one.
[[nodiscard]] double JoinKeyDomainEstimate(const Plan& plan,
                                           const ColumnName& column);
// Widest single-key estimate over `columns` (a composite key cannot have more
// distinct values than its widest component).
[[nodiscard]] double JoinKeyDomainEstimate(
    const Plan& plan, const std::vector<ColumnName>& columns);
// Build rows the probe key domain can still admit.
[[nodiscard]] double SemiReductionKeptRows(double probe_rows, double build_rows,
                                           double probe_domain,
                                           double build_domain);
// The reduction has to be worth its own bookkeeping: the probe side is hashed
// once more (into the domain) and every build row pays a membership test, so
// the shape only wins when it removes a real share of a materially larger
// build side.
[[nodiscard]] bool SemiReductionIsProfitable(double probe_rows,
                                             double build_rows,
                                             double probe_domain,
                                             double build_domain);
// The same question about a concrete pair of child plans.
[[nodiscard]] bool SemiReductionProfitable(
    const Plan& probe, const std::vector<ColumnName>& probe_keys,
    const Plan& build, const std::vector<ColumnName>& build_keys);

// Opaque accessors for the executor-layer enum values: the plan layer sees
// only the forward-declared JoinKind, so callers pick a side by name instead
// of naming enumerators.
[[nodiscard]] bool IsSemiJoinKind(JoinKind kind);
[[nodiscard]] bool IsAntiJoinKind(JoinKind kind);
[[nodiscard]] bool IsNullAwareAntiJoinKind(JoinKind kind);
[[nodiscard]] bool IsLeftOuterJoinKind(JoinKind kind);
[[nodiscard]] bool IsRightOuterJoinKind(JoinKind kind);
[[nodiscard]] bool IsFullOuterJoinKind(JoinKind kind);
[[nodiscard]] JoinKind SemiJoinKind();
[[nodiscard]] JoinKind AntiJoinKind();
[[nodiscard]] JoinKind NullAwareAntiJoinKind();
[[nodiscard]] JoinKind LeftOuterJoinKind();
[[nodiscard]] JoinKind RightOuterJoinKind();
[[nodiscard]] JoinKind FullOuterJoinKind();

}  // namespace tinylamb

#endif  // TINYLAMB_PRODUCT_PLAN_HPP
