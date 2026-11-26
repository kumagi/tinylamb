/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/detail/fused_grouped_join.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <cctype>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <functional>
#include <iostream>
#include <iterator>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <span>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/parallel_work.hpp"
#include "common/platform/extended_float.hpp"
#include "common/platform/int128_conversion.hpp"
#include "common/platform/process.hpp"
#include "common/platform/range_selection.hpp"
#include "common/query_parallelism.hpp"
#include "common/status_or.hpp"
#include "common/string_util.hpp"
#include "database/transaction_context.hpp"
#include "executor/detail/analytics_factorized_join.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/detail/flat_int_key_map.hpp"
#include "executor/detail/fused_group_emit.hpp"
#include "executor/detail/fused_group_probe.hpp"
#include "executor/detail/fused_dense_aggregates.hpp"
#include "executor/detail/fused_raw_aggregates.hpp"
#include "executor/detail/fused_compact_radix.hpp"
#include "executor/detail/fused_group_dictionary.hpp"
#include "executor/detail/fused_varchar_groups.hpp"
#include "executor/detail/fused_grouped_join_internal.hpp"
#include "executor/detail/planning_columnar_keys.hpp"
#include "executor/detail/relation.hpp"
#include "executor/detail/scan_filter.hpp"
#include "executor/detail/subquery_runtime.hpp"
#include "executor/hash_join_mode.hpp"
#include "executor/query_memory.hpp"
#include "executor/spill_file.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/binary_expression.hpp"
#include "expression/bytecode.hpp"
#include "expression/case_expression.hpp"
#include "expression/cast_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/function_call_expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/jit.hpp"
#include "expression/named_expression.hpp"
#include "expression/parameter_slot.hpp"
#include "expression/query_expression.hpp"
#include "expression/rewrite.hpp"
#include "expression/unary_expression.hpp"
#include "query/statement.hpp"
#include "table/lstable_bridge.hpp"
#include "table/table.hpp"
#include "table/table_statistics.hpp"
#include "type/collation.hpp"
#include "type/column_name.hpp"
#include "type/date.hpp"
#include "type/schema.hpp"
#include "type/type.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb::relational_detail {

namespace {









// One probe row updates all aggregate cells under the same group lock.
// Release on every Status/error path as well as normal completion.
class FusedGroupLock {
 public:
  explicit FusedGroupLock(uint8_t* state) noexcept : state_(state) {
    if (state_ != nullptr) {
      while (__atomic_exchange_n(state_, 2, __ATOMIC_ACQUIRE) == 2) {
      }
    }
  }
  ~FusedGroupLock() {
    if (state_ != nullptr) {
      __atomic_store_n(state_, 1, __ATOMIC_RELEASE);
    }
  }
  FusedGroupLock(const FusedGroupLock&) = delete;
  FusedGroupLock& operator=(const FusedGroupLock&) = delete;
  FusedGroupLock(FusedGroupLock&&) = delete;
  FusedGroupLock& operator=(FusedGroupLock&&) = delete;

 private:
  uint8_t* state_;
};



// Bulk raw-count state: one map entry per distinct build key replaces the
// whole bucket chain.  `r_idx` keeps the most recently inserted build row for
// that key -- the row a bucket-head walk would have filled a new group's
// representative from.


}  // namespace

// Tries to fuse the final join of a grouped statement with the grouping
// itself: matched rows (and, for LEFT joins, unmatched left rows padded with
// NULLs) feed group keys and aggregate inputs straight into per-worker
// accumulators, and the joined relation is never materialized.  The emit
// mirrors Project's generic grouped path: per group, accumulators finish
// into an AggregateResultMap and the select list evaluates against the
// representative row.
//
// An empty optional means "shape not fusable" and the caller falls back to
// the materializing join; a real Status propagates.  Gates that need the
// joined schema live here: a resident build side, no null-safe keys, an
// equality join, and every per-row expression (join residuals, group keys,
// aggregate children) compiled to context-free bytecode.
StatusOr<std::optional<Relation>> TryFusedGroupedJoin(
    TransactionContext& context, const Relation& left,
    const Relation& right_arg, const std::vector<Expression>& predicates,
    JoinType join_type, const Scope* outer, const CteMap& ctes,
    GroupedJoinFusionSpec* fusion,
    const DeferredLSTableProbe* deferred_left_probe) {
  static const bool disabled =
      ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_GROUPED_JOIN_FUSION") !=
      nullptr;
  const bool debug_fusion =
      ::tinylamb::platform::GetEnv("TINYLAMB_FUSION_DEBUG") != nullptr;
  const auto not_fusable = [&](const char* why) {
    if (debug_fusion) {
      std::cerr << "[fusion] not fusable: " << why << "\n";
    }
    return std::optional<Relation>{};
  };
  const size_t effective_left_rows =
      deferred_left_probe != nullptr && deferred_left_probe->store != nullptr
          ? deferred_left_probe->store->total_rows
          : left.TotalRows();
  if (debug_fusion) {
    std::cerr << "[fusion] try: eligible="
              << (fusion != nullptr && fusion->eligible)
              << " join_type=" << static_cast<int>(join_type)
              << " left_rows=" << effective_left_rows << "\n";
  }
  auto materialize_columnar_side =
      [&](const Relation& side,
          std::optional<Relation>& holder) -> StatusOr<const Relation*> {
    if (!side.HasColumnarOnly()) {
      return &side;
    }
    Relation mat;
    mat.schema = side.schema;
    mat.rows.reserve(side.TotalRows());
    bool uniform_ok = true;
    RETURN_IF_FAIL(MaterializeColumnarTableToRows(*side.columnar, side.schema,
                                                  &mat.rows, &uniform_ok));
    mat.uniform_cell_types = uniform_ok && !mat.rows.empty();
    holder.emplace(std::move(mat));
    return &*holder;
  };
  if (!disabled && fusion != nullptr && fusion->eligible &&
      !fusion->aggregates.empty() && !fusion->require_exact_integer_totals &&
      join_type == JoinType::kFull && deferred_left_probe == nullptr) {
    std::optional<Relation> left_full_holder;
    std::optional<Relation> right_full_holder;
    ASSIGN_OR_RETURN(const Relation*, full_left_ptr,
                     materialize_columnar_side(left, left_full_holder));
    ASSIGN_OR_RETURN(const Relation*, full_right_ptr,
                     materialize_columnar_side(right_arg, right_full_holder));
    const Relation& full_left = *full_left_ptr;
    const Relation& full_right = *full_right_ptr;
    ASSIGN_OR_RETURN(
        std::optional<Relation>, full_res,
        (TryNullSignatureFullAggregate(context, full_left, full_right,
                                       predicates, outer, ctes, *fusion)));
    if (full_res.has_value()) {
      for (Row& r : full_res->rows) {
        for (Value& v : r.values_) {
          if (v.type == ValueType::kVarChar && v.owned_data.empty() &&
              !v.value.varchar_value.empty()) {
            v.owned_data.assign(v.value.varchar_value);
            v.value.varchar_value = v.owned_data;
          }
        }
      }
    }
    return full_res;
  }
  if (disabled || fusion == nullptr || !fusion->eligible ||
      (fusion->aggregates.empty() && fusion->group_keys.empty()) ||
      (join_type != JoinType::kInner && join_type != JoinType::kLeft)) {
    return not_fusable("head");
  }
  if (right_arg.HasSpill() || effective_left_rows == 0) {
    return not_fusable("spill");
  }
  std::optional<Relation> right_holder;
  const size_t total_right_rows = right_arg.TotalRows();
  bool right_col_only = right_arg.HasColumnarOnly() &&
                        total_right_rows < std::numeric_limits<uint32_t>::max();
  std::shared_ptr<const ColumnarTable> right_col_owner =
      right_col_only ? right_arg.columnar : nullptr;
  RightColLocator right_locator;
  if (right_col_only) {
    right_locator.Init(right_arg.columnar.get());
  }
  const Relation* right_ptr = &right_arg;
  // Build rows pointer list (populated if `right` is row-backed or falls back
  // from native columnar mode).
  std::vector<const Row*> build_rows;
  const auto fallback_to_right_rows = [&]() -> StatusOr<const Relation*> {
    if (right_col_only) {
      ASSIGN_OR_RETURN(const Relation*, rmat,
                       materialize_columnar_side(right_arg, right_holder));
      right_ptr = rmat;
      right_col_only = false;
      right_col_owner.reset();
      build_rows.reserve(right_ptr->rows.size());
      for (const Row& row : right_ptr->rows) {
        build_rows.push_back(&row);
      }
    }
    return right_ptr;
  };
  if (right_arg.HasColumnarOnly() && !right_col_only) {
    ASSIGN_OR_RETURN(const Relation*, rmat,
                     materialize_columnar_side(right_arg, right_holder));
    right_ptr = rmat;
  }
  const Relation& right = right_arg;
  if (!right_col_only) {
    build_rows.reserve(right_ptr->rows.size());
    for (const Row& row : right_ptr->rows) {
      build_rows.push_back(&row);
    }
  }
  const auto join_begin = std::chrono::steady_clock::now();
  Relation result(context.execution_runtime());
  result.schema = left.schema + right.schema;
  // A grouping without accumulators observes the set of joined tuples, not
  // their multiplicities. Repeated integral build rows therefore need only
  // one bucket entry, including when residual predicates reject every pair.
  // Keep only direct column projections and comparisons: arbitrary scalar
  // functions or SQL string/float equality can distinguish equivalent keys.
  const auto direct_column = [](const Expression& expression) {
    return expression && expression->Type() == TypeTag::kColumnValue;
  };
  const auto integral_schema = [&] {
    for (size_t i = 0; i < right.schema.ColumnCount(); ++i) {
      const auto type = right.schema.GetColumn(i).Type();
      if (type != ValueType::kInt64 && type != ValueType::kDate) {
        return false;
      }
    }
    return true;
  };
  if (fusion->aggregates.empty() && total_right_rows >= 1024 &&
      integral_schema() &&
      std::ranges::all_of(fusion->group_keys, direct_column) &&
      std::ranges::all_of(fusion->select_items, direct_column) &&
      std::ranges::all_of(predicates, [&](const Expression& predicate) {
        if (!predicate || predicate->Type() != TypeTag::kBinaryExp) {
          return false;
        }
        const auto& binary = predicate->AsBinaryExpression();
        const auto op = binary.Op();
        return (op == BinaryOperation::kEquals ||
                op == BinaryOperation::kNotEquals ||
                op == BinaryOperation::kLessThan ||
                op == BinaryOperation::kLessThanEquals ||
                op == BinaryOperation::kGreaterThan ||
                op == BinaryOperation::kGreaterThanEquals) &&
               direct_column(binary.Left()) && direct_column(binary.Right());
      })) {
    const auto hash_row = [](const Row* row) { return std::hash<Row>{}(*row); };
    const auto equal_row = [](const Row* a, const Row* b) { return *a == *b; };
    if (total_right_rows <= SIZE_MAX / 64) {
      if (auto charge = TryReserveQueryMemory(total_right_rows * 64)) {
        constexpr size_t kSampleRows = 256;
        bool sample_dup = false;
        if (right_col_only) {
          std::vector<Row> sample_storage(kSampleRows);
          const size_t ncols = right.schema.ColumnCount();
          std::unordered_set<const Row*, decltype(hash_row),
                             decltype(equal_row)>
              seen(0, hash_row, equal_row);
          for (size_t i = 0; i < kSampleRows; ++i) {
            sample_storage[i].values_.resize(ncols);
            for (size_t c = 0; c < ncols; ++c) {
              right_locator.WriteValue(c, static_cast<uint32_t>(i),
                                       &sample_storage[i].values_[c]);
            }
            seen.insert(&sample_storage[i]);
          }
          sample_dup = seen.size() <= kSampleRows * 3 / 4;
        } else {
          std::unordered_set<const Row*, decltype(hash_row),
                             decltype(equal_row)>
              seen(0, hash_row, equal_row);
          for (size_t i = 0; i < kSampleRows; ++i) {
            seen.insert(build_rows[i]);
          }
          sample_dup = seen.size() <= kSampleRows * 3 / 4;
        }
        if (sample_dup) {
          ASSIGN_OR_RETURN(const Relation*, rmat_ptr, fallback_to_right_rows());
          const Relation& rmat = *rmat_ptr;
          if (std::ranges::all_of(rmat.rows, [](const Row& row) {
                return std::ranges::all_of(row.values_, [](const Value& value) {
                  return value.IsNull() || value.type == ValueType::kInt64 ||
                         value.type == ValueType::kDate;
                });
              })) {
            std::unordered_set<const Row*, decltype(hash_row),
                               decltype(equal_row)>
                seen(0, hash_row, equal_row);
            seen.reserve(rmat.rows.size());
            size_t kept = 0;
            for (const Row* row : build_rows) {
              if (seen.insert(row).second) {
                build_rows[kept++] = row;
              }
            }
            build_rows.resize(kept);
          }
        }
      }
    }
  }
  const bool singleton_right_relation =
      (join_type == JoinType::kInner && total_right_rows == 1 &&
       predicates.empty() && !left.HasSpill());
  if (singleton_right_relation) {
    RETURN_IF_FAIL(fallback_to_right_rows().GetStatus());
  }
  std::vector<EqualityKey> equality_keys =
      EqualityKeys(left.schema, right.schema, predicates);
  const size_t raw_key_count = equality_keys.size();
  RETURN_IF_FAIL(DisableRawCollatedKeys(left, right, &equality_keys));
  if (equality_keys.size() != raw_key_count) {
    return not_fusable("collated join key");
  }
  const std::vector<Expression> residuals =
      HasMixedKeySignednessOrType(left, right, equality_keys)
          ? predicates
          : ResidualJoinPredicates(left.schema, right.schema, predicates,
                                   /*allow_binary_varchar=*/true);
  if (equality_keys.empty() && !singleton_right_relation) {
    return not_fusable("no equality key");
  }
  const bool has_null_safe_key = std::ranges::any_of(
      equality_keys, [](const EqualityKey& key) { return key.null_safe; });
  if (has_null_safe_key) {
    return not_fusable("null-safe key");
  }

  const size_t left_width = left.schema.ColumnCount();
  const size_t right_width = right.schema.ColumnCount();
  const auto compile_or = [&](const Expression& expression,
                              BytecodeCompiler::Context context_kind)
      -> std::optional<BytecodeProgram> {
    if (!expression) {
      return BytecodeProgram{};
    }
    return BytecodeCompiler::CompileRowCached(expression, result.schema,
                                              context_kind);
  };
  std::vector<BytecodeProgram> residual_programs;
  residual_programs.reserve(residuals.size());
  for (const Expression& predicate : residuals) {
    std::optional<BytecodeProgram> program =
        compile_or(predicate, BytecodeCompiler::Context::kFilter);
    if (!program.has_value()) {
      return not_fusable("residual compile");
    }
    residual_programs.push_back(std::move(*program));
  }
  std::vector<BytecodeProgram> key_programs;
  key_programs.reserve(fusion->group_keys.size());
  for (const Expression& key : fusion->group_keys) {
    std::optional<BytecodeProgram> program =
        compile_or(key, BytecodeCompiler::Context::kValue);
    if (!program.has_value()) {
      return not_fusable("key compile");
    }
    key_programs.push_back(std::move(*program));
  }
  std::vector<BytecodeProgram> child_programs;
  child_programs.reserve(fusion->aggregates.size());
  for (size_t aggregate_index = 0; aggregate_index < fusion->aggregates.size();
       ++aggregate_index) {
    if (fusion->count_star[aggregate_index]) {
      child_programs.emplace_back();
      continue;
    }
    std::optional<BytecodeProgram> program =
        compile_or(fusion->aggregates[aggregate_index]->Child(),
                   BytecodeCompiler::Context::kValue);
    if (!program.has_value()) {
      return not_fusable("child compile");
    }
    child_programs.push_back(std::move(*program));
  }
  if (deferred_left_probe != nullptr && !disabled &&
      ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_COLLAPSED_RIGHT_JOIN") ==
          nullptr &&
      outer == nullptr && ctes.empty() && !fusion->anti_unmatched_only &&
      !fusion->rollup_count_distribution && fusion->output_sink == nullptr &&
      join_type == JoinType::kInner && equality_keys.size() == 1 &&
      !equality_keys[0].null_safe && equality_keys[0].left_offset == 0 &&
      equality_keys[0].right_offset == 0 && residuals.empty() &&
      !right_arg.HasSpill() && effective_left_rows >= 4096 &&
      total_right_rows >= 1024 && !fusion->aggregates.empty() &&
      fusion->aggregates.size() <= 8) {
    const auto rk = static_cast<slot_t>(equality_keys[0].right);
    const Column& rcol = right_arg.schema.GetColumn(rk);
    const bool r_int =
        (rcol.Type() == ValueType::kInt64 || rcol.Type() == ValueType::kDate) &&
        !rcol.IsUnsigned() && !rcol.IsInterval();
    const bool r_str = rcol.Type() == ValueType::kVarChar && !rcol.IsInterval();
    if (r_int || r_str) {
      constexpr size_t kFanoutSample = 256;
      const size_t stride =
          std::max<size_t>(1, total_right_rows / kFanoutSample);
      std::unordered_set<int64_t> int_seen;
      std::unordered_set<std::string> str_seen;
      size_t non_null_sampled = 0;
      Value tmp;
      for (size_t i = 0, sampled = 0;
           i < total_right_rows && sampled < kFanoutSample;
           i += stride, ++sampled) {
        const Value* vp = nullptr;
        if (right_col_only) {
          right_locator.WriteValue(rk, static_cast<uint32_t>(i), &tmp);
          vp = &tmp;
        } else {
          vp = &(*build_rows[i])[rk];
        }
        if (vp->IsNull()) {
          continue;
        }
        ++non_null_sampled;
        if (r_int && vp->type == rcol.Type() && !vp->IsUnsigned() &&
            !vp->IsInterval()) {
          int_seen.insert(vp->value.int_value);
        } else if (r_str && vp->type == ValueType::kVarChar &&
                   !vp->IsCaseInsensitive() && !vp->IsInterval()) {
          str_seen.emplace(vp->value.varchar_value);
        }
      }
      const size_t distinct_sampled = r_int ? int_seen.size() : str_seen.size();
      if (non_null_sampled >= 32 &&
          distinct_sampled * 5 < non_null_sampled * 4) {
        return not_fusable("high-fanout build: prefer collapsed-right");
      }
    }
  }
  if (deferred_left_probe == nullptr) {
    ASSIGN_OR_RETURN(
        std::optional<Relation>, collapsed_grouped,
        (TryCollapsedRightGroupedJoin(context, left, right_arg, equality_keys,
                                      join_type, residuals, key_programs,
                                      child_programs, outer, ctes, fusion)));
    if (collapsed_grouped.has_value()) {
      return collapsed_grouped;
    }
  }

  // Pair-loop shape classification (see ProgramShape): direct-column reads,
  // boxing-free fast programs, and canonical fallbacks.
  std::vector<ProgramShape> residual_shapes;
  residual_shapes.reserve(residual_programs.size());
  for (const BytecodeProgram& program : residual_programs) {
    residual_shapes.push_back(ClassifyProgram(program));
  }
  // Build-side residual hoist: a residual conjunct that loads only
  // right-side cells (Q13's `o_comment NOT LIKE ...` under a customer
  // LEFT JOIN orders) does not depend on the probe row, so it can be
  // evaluated once per build row -- inside the worker-parallel relation
  // filter -- instead of once per candidate pair in the fused probe loop.
  // Soundness: removing a right row whose right-only conjunct fails is
  // equivalent to leaving it in and rejecting every pair it forms; for
  // kInner those pairs vanish either way, and for kLeft the matched-false
  // contract null-extends the probe row exactly when all of its bucket
  // entries are gone.  The throw-safe gate keeps the hoisted evaluation
  // from surfacing errors the per-pair path would only reach on
  // hash-matched rows.
  static const bool hoist_disabled =
      ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_BUILD_RESIDUAL_HOIST") !=
      nullptr;
  if (!hoist_disabled && !residual_programs.empty() && right.TotalRows() > 0) {
    // Evaluate each right-only residual once per build row (Q13's
    // `o_comment NOT LIKE ...` is row-invariant on the probe side) and drop
    // failing rows from the build pointer list -- no relation copy, and the
    // pair loop loses the per-pair residual dispatch.  Soundness: a dropped
    // right row is one whose every candidate pair would be residual-
    // rejected; kInner loses nothing and kLeft's matched-false contract
    // null-extends the probe row exactly the same.  Any evaluation error
    // abandons the hoist (the per-pair path keeps its exact error behavior).
    std::vector<size_t> hoisted_indices;
    hoisted_indices.reserve(residual_programs.size());
    for (size_t i = 0; i < residual_programs.size(); ++i) {
      if (!ExpressionCannotThrowForPlanner(residuals[i])) {
        continue;
      }
      std::vector<slot_t> loads;
      CollectLoadCells(residual_programs[i], &loads);
      if (!loads.empty() &&
          std::all_of(loads.begin(), loads.end(),
                      [&](slot_t cell) { return cell >= left_width; })) {
        hoisted_indices.push_back(i);
      }
    }
    if (!hoisted_indices.empty()) {
      ASSIGN_OR_RETURN(const Relation*, rmat_ptr, fallback_to_right_rows());
      const Relation& rmat = *rmat_ptr;
      std::vector<slot_t> hoist_cells;
      std::vector<ProgramShape> hoist_shapes;
      hoist_shapes.reserve(hoisted_indices.size());
      for (const size_t i : hoisted_indices) {
        CollectLoadCells(residual_programs[i], &hoist_cells);
        hoist_shapes.push_back(residual_shapes[i]);
      }
      Row hoist_scratch;
      hoist_scratch.values_.resize(left_width + right_width);
      bool hoist_error = false;
      std::vector<bool> keep(rmat.rows.size(), true);
      for (size_t ri = 0; ri < rmat.rows.size(); ++ri) {
        const Row& row = rmat.rows[ri];
        for (const slot_t cell : hoist_cells) {
          hoist_scratch.values_[cell] = row.values_[cell - left_width];
        }
        for (size_t i = 0; i < hoisted_indices.size(); ++i) {
          StatusOr<Value> verdict =
              EvaluateProgramValue(residual_programs[hoisted_indices[i]],
                                   hoist_shapes[i], hoist_scratch);
          if (!verdict.HasValue()) {
            hoist_error = true;
            break;
          }
          if (!Truthy(verdict.Value())) {
            keep[ri] = false;
            break;
          }
        }
        if (hoist_error) {
          break;
        }
      }
      if (!hoist_error) {
        std::vector<const Row*> filtered;
        filtered.reserve(build_rows.size());
        for (auto& build_row : build_rows) {
          // `keep` is indexed by the original row position in `rmat.rows`;
          // `build_rows` may have been compacted by an earlier dedup pass, so
          // its own index is a different space. Recover the original position
          // from the row pointer before consulting the verdict.
          const auto ri = static_cast<size_t>(build_row - rmat.rows.data());
          if (ri < keep.size() && keep[ri]) {
            filtered.push_back(build_row);
          }
        }
        build_rows = std::move(filtered);
        // Drop the hoisted programs (and their shapes), highest index first.
        for (size_t i = hoisted_indices.size(); i-- > 0;) {
          residual_programs.erase(
              residual_programs.begin() +
              static_cast<std::ptrdiff_t>(hoisted_indices[i]));
          residual_shapes.erase(
              residual_shapes.begin() +
              static_cast<std::ptrdiff_t>(hoisted_indices[i]));
        }
      }
    }
  }
  const auto right_build_empty = [&]() noexcept -> bool {
    return right_col_only ? total_right_rows == 0 : build_rows.empty();
  };
  const auto right_build_count = [&]() noexcept -> size_t {
    return right_col_only ? total_right_rows : build_rows.size();
  };
  std::vector<ProgramShape> key_shapes;
  key_shapes.reserve(key_programs.size());
  for (size_t k = 0; k < key_programs.size(); ++k) {
    key_shapes.push_back(ClassifyProgram(key_programs[k], fusion->group_keys[k],
                                         &result.schema));
  }
  bool verified_unique_right_join_key = false;
  if (join_type == JoinType::kInner &&
      !HasMixedKeySignednessOrType(left, right, equality_keys) &&
      equality_keys.size() == 1 && !right.HasSpill() && !right_build_empty()) {
    const EqualityKey& eq = equality_keys[0];
    if (!eq.null_safe && eq.left_offset == 0 && eq.right_offset == 0) {
      std::optional<size_t> determining_key;
      for (size_t k = 0; k < key_programs.size(); ++k) {
        if (std::cmp_equal(key_shapes[k].direct_column,
                           left_width + eq.right) ||
            std::cmp_equal(key_shapes[k].direct_column, eq.left)) {
          determining_key = k;
          if (std::cmp_equal(key_shapes[k].direct_column,
                             left_width + eq.right)) {
            break;
          }
        }
      }
      if (determining_key.has_value()) {
        const Column& rcol = right.schema.GetColumn(eq.right);
        const bool is_int_col =
            !rcol.IsUnsigned() && (rcol.Type() == ValueType::kInt64 ||
                                   rcol.Type() == ValueType::kDate);
        bool right_key_unique =
            rcol.GetConstraint().ctype == Constraint::kPrimaryKey;
        if (!right_key_unique && is_int_col && right_build_count() <= 131072) {
          FlatIntKeyMap<uint8_t> seen_keys;
          right_key_unique = true;
          if (right_col_only) {
            for (const ColumnarChunk& rch : right_arg.columnar->chunks) {
              const ColumnarColumn& rcc = rch.columns[eq.right];
              for (size_t ri = 0; ri < rch.rows; ++ri) {
                const ColumnarColumn* leaf = &rcc;
                size_t lr = ri;
                if (leaf->encoding == ColumnarColumn::Encoding::kGathered)
                    [[unlikely]] {
                  if (!rcc.ResolveLeaf(ri, &leaf, &lr)) {
                    continue;
                  }
                } else if (leaf->NonGatheredIsNull(lr)) {
                  continue;
                }
                int64_t kv = 0;
                if (leaf->is_uniform) [[likely]] {
                  if (leaf->type != rcol.Type() || leaf->is_unsigned) {
                    right_key_unique = false;
                    break;
                  }
                  kv = leaf->IntAt(lr);
                } else {
                  const Value& v = leaf->fallback[lr];
                  if (v.IsNull()) {
                    continue;
                  }
                  if (v.type != rcol.Type() || v.IsUnsigned()) {
                    right_key_unique = false;
                    break;
                  }
                  kv = v.value.int_value;
                }
                bool inserted = false;
                seen_keys.TryEmplace(kv, &inserted);
                if (!inserted) {
                  right_key_unique = false;
                  break;
                }
              }
              if (!right_key_unique) {
                break;
              }
            }
          } else {
            for (const Row* r : build_rows) {
              const Value& v = r->values_[eq.right];
              if (v.IsNull()) {
                continue;
              }
              if (v.type != rcol.Type() || v.IsUnsigned()) {
                right_key_unique = false;
                break;
              }
              bool inserted = false;
              seen_keys.TryEmplace(v.value.int_value, &inserted);
              if (!inserted) {
                right_key_unique = false;
                break;
              }
            }
          }
        }
        if (right_key_unique) {
          if (is_int_col) {
            verified_unique_right_join_key = true;
          }
          if (key_programs.size() >= 2) {
            std::vector<uint8_t> elide_key(key_programs.size(), 0);
            bool any_elided = false;
            for (size_t k = 0; k < key_programs.size(); ++k) {
              if (k == *determining_key) {
                continue;
              }
              const Expression& expr = fusion->group_keys[k];
              if (!expr || ContainsQuery(expr) || ContainsParameterSlot(expr) ||
                  !ExpressionCannotThrowForPlanner(expr)) {
                continue;
              }
              std::vector<slot_t> loads;
              CollectLoadCells(key_programs[k], &loads);
              if (std::all_of(loads.begin(), loads.end(),
                              [&](slot_t c) { return c >= left_width; })) {
                elide_key[k] = 1;
                any_elided = true;
              }
            }
            if (any_elided) {
              std::vector<BytecodeProgram> kept_programs;
              std::vector<ProgramShape> kept_shapes;
              kept_programs.reserve(key_programs.size());
              kept_shapes.reserve(key_shapes.size());
              for (size_t k = 0; k < key_programs.size(); ++k) {
                if (elide_key[k] == 0) {
                  kept_programs.push_back(std::move(key_programs[k]));
                  kept_shapes.push_back(std::move(key_shapes[k]));
                }
              }
              key_programs = std::move(kept_programs);
              key_shapes = std::move(kept_shapes);
            }
          }
        }
      }
    }
  }
  std::vector<ProgramShape> child_shapes;
  child_shapes.reserve(child_programs.size());
  for (size_t i = 0; i < child_programs.size(); ++i) {
    const Expression child_expr =
        fusion->count_star[i] ? Expression() : fusion->aggregates[i]->Child();
    child_shapes.push_back(
        ClassifyProgram(child_programs[i], child_expr, &result.schema));
  }

  // Count-only fusions (Q13: COUNT(o_orderkey) per customer with every other
  // aggregate absent) accumulate a raw per-group integer instead of driving
  // the accumulator machinery 915K times: no per-pair Value boxing, no
  // per-group state construction, and an integer-add merge.  Two shapes
  // qualify, and they differ on unmatched LEFT rows: COUNT(*) counts the
  // null-extended row (+1), while COUNT(not-null column) counts nothing
  // (+0).  A not-null column child (bare load of a kNotNull/kPrimaryKey
  // column) can never contribute a NULL, so its COUNT equals COUNT(*) over
  // matched pairs.  Mixed shapes stay on the generic path.
  // A grouping-only join needs the same key-presence state as COUNT(*).
  // Its count is internal; no aggregate column is emitted.
  const auto is_stat_raw_kind = [](RawAggKind kind) noexcept {
    return kind == RawAggKind::kStddevSamp || kind == RawAggKind::kStddevPop ||
           kind == RawAggKind::kVarSamp || kind == RawAggKind::kVarPop;
  };
  FusedRawAggregates raw_aggregates;
  if (const char* reason = raw_aggregates.Classify({
      .left = left,
      .right_arg = right_arg,
      .join_type = join_type,
      .fusion = fusion,
      .deferred_left_probe = deferred_left_probe,
      .effective_left_rows = effective_left_rows,
      .right_col_only = right_col_only,
      .right_ptr = right_ptr,
      .right = right,
      .result = result,
      .left_width = left_width,
      .right_width = right_width,
      .child_programs = child_programs,
      .right_build_count = right_build_count(),
      .child_shapes = child_shapes,
  })) {
    return not_fusable(reason);
  }
  // For INNER joins over exact INT64/DATE equality keys, align join-key group
  // columns with the side that owns the remaining group keys so either
  // right-only pre-assigned build groups (Q3, Q18) or left-only group caching
  // applies.
  if (join_type == JoinType::kInner &&
      !HasMixedKeySignednessOrType(left, right, equality_keys)) {
    // This flag starts as "right side is spill-free" (the precondition for
    // safe pre-allocation) and is cleared as soon as a left-side key is seen.
    bool preallocation_safe = !right.HasSpill();
    bool has_non_join_right_key = false;
    for (size_t k = 0; k < key_programs.size(); ++k) {
      bool is_join_eq_col = false;
      if (key_shapes[k].direct_column >= 0) {
        const auto c = static_cast<size_t>(key_shapes[k].direct_column);
        for (const EqualityKey& eq : equality_keys) {
          if (eq.left_offset == 0 && eq.right_offset == 0 && !eq.null_safe &&
              (c == eq.left || c == left_width + eq.right)) {
            const Column& lc = left.schema.GetColumn(eq.left);
            const Column& rc = right.schema.GetColumn(eq.right);
            if ((lc.Type() == ValueType::kInt64 ||
                 lc.Type() == ValueType::kDate) &&
                lc.Type() == rc.Type() && lc.IsUnsigned() == rc.IsUnsigned()) {
              is_join_eq_col = true;
              break;
            }
          }
        }
      }
      if (is_join_eq_col) {
        continue;
      }
      if (key_shapes[k].direct_column >= 0) {
        if (std::cmp_less(key_shapes[k].direct_column, left_width)) {
          preallocation_safe = false;
        } else {
          has_non_join_right_key = true;
        }
      } else if (key_shapes[k].extract_date_col >= 0) {
        if (std::cmp_less(key_shapes[k].extract_date_col, left_width)) {
          preallocation_safe = false;
        } else {
          has_non_join_right_key = true;
        }
      } else {
        std::vector<slot_t> loads;
        CollectLoadCells(key_programs[k], &loads);
        if (std::any_of(loads.begin(), loads.end(),
                        [&](slot_t c) { return c < left_width; })) {
          preallocation_safe = false;
        } else if (!loads.empty()) {
          has_non_join_right_key = true;
        }
      }
    }
    const bool prefer_right_group_key =
        preallocation_safe &&
        (has_non_join_right_key ||
         (raw_aggregates.skip_accumulator_states && residual_programs.empty() &&
          right.TotalRows() * 2 <= effective_left_rows));
    for (ProgramShape& kshape : key_shapes) {
      if (prefer_right_group_key) {
        if (kshape.direct_column >= 0 &&
            std::cmp_less(kshape.direct_column, left_width)) {
          const auto lcol = static_cast<size_t>(kshape.direct_column);
          for (const EqualityKey& eq : equality_keys) {
            if (eq.left == lcol && eq.left_offset == 0 &&
                eq.right_offset == 0 && !eq.null_safe) {
              const Column& lc = left.schema.GetColumn(eq.left);
              const Column& rc = right.schema.GetColumn(eq.right);
              if ((lc.Type() == ValueType::kInt64 ||
                   lc.Type() == ValueType::kDate) &&
                  lc.Type() == rc.Type() &&
                  lc.IsUnsigned() == rc.IsUnsigned()) {
                kshape.direct_column =
                    static_cast<int32_t>(left_width + eq.right);
                break;
              }
            }
          }
        }
      } else if (std::cmp_greater_equal(kshape.direct_column, left_width)) {
        const size_t rcol =
            static_cast<size_t>(kshape.direct_column) - left_width;
        for (const EqualityKey& eq : equality_keys) {
          if (eq.right == rcol && eq.left_offset == 0 && eq.right_offset == 0 &&
              !eq.null_safe) {
            const Column& lc = left.schema.GetColumn(eq.left);
            const Column& rc = right.schema.GetColumn(eq.right);
            if ((lc.Type() == ValueType::kInt64 ||
                 lc.Type() == ValueType::kDate) &&
                lc.Type() == rc.Type() && lc.IsUnsigned() == rc.IsUnsigned()) {
              kshape.direct_column = static_cast<int32_t>(eq.left);
              break;
            }
          }
        }
      }
    }
  }
  bool left_only_group_keys = !key_programs.empty();
  bool right_only_group_keys = !right.HasSpill();
  for (size_t k = 0; k < key_programs.size(); ++k) {
    if (key_shapes[k].direct_column >= 0) {
      if (std::cmp_greater_equal(key_shapes[k].direct_column, left_width)) {
        left_only_group_keys = false;
      } else {
        right_only_group_keys = false;
      }
    } else if (key_shapes[k].extract_date_col >= 0) {
      if (std::cmp_greater_equal(key_shapes[k].extract_date_col, left_width)) {
        left_only_group_keys = false;
      } else {
        right_only_group_keys = false;
      }
    } else {
      std::vector<slot_t> loads;
      CollectLoadCells(key_programs[k], &loads);
      if (std::any_of(loads.begin(), loads.end(),
                      [&](slot_t c) { return c >= left_width; })) {
        left_only_group_keys = false;
      }
      if (std::any_of(loads.begin(), loads.end(),
                      [&](slot_t c) { return c < left_width; })) {
        right_only_group_keys = false;
      }
    }
  }
  bool all_direct_group_keys = !left.HasSpill() && !key_programs.empty();
  for (size_t k = 0; k < key_programs.size(); ++k) {
    if (key_shapes[k].direct_column < 0) {
      all_direct_group_keys = false;
      break;
    }
  }
  std::vector<size_t> int_key_indices;
  int_key_indices.reserve(key_programs.size());
  bool has_unique_int_key = false;
  for (size_t k = 0; k < key_programs.size(); ++k) {
    if (key_shapes[k].direct_column >= 0) {
      const auto col_idx = static_cast<size_t>(key_shapes[k].direct_column);
      if (col_idx < result.schema.ColumnCount()) {
        const Column& col = result.schema.GetColumn(col_idx);
        if ((col.Type() == ValueType::kInt64 ||
             col.Type() == ValueType::kDate) &&
            !col.IsUnsigned()) {
          int_key_indices.push_back(k);
          if (col.GetConstraint().ctype == Constraint::kPrimaryKey) {
            has_unique_int_key = true;
          } else if (join_type == JoinType::kInner &&
                     !HasMixedKeySignednessOrType(left, right, equality_keys)) {
            for (const EqualityKey& eq : equality_keys) {
              if (eq.left_offset == 0 && eq.right_offset == 0 &&
                  !eq.null_safe) {
                if (col_idx == eq.left &&
                    (verified_unique_right_join_key ||
                     right.schema.GetColumn(eq.right).GetConstraint().ctype ==
                         Constraint::kPrimaryKey)) {
                  has_unique_int_key = true;
                  break;
                }
                if (col_idx == left_width + eq.right &&
                    (verified_unique_right_join_key ||
                     left.schema.GetColumn(eq.left).GetConstraint().ctype ==
                         Constraint::kPrimaryKey)) {
                  has_unique_int_key = true;
                  break;
                }
              }
            }
          }
        }
      }
    }
  }
  const bool hash_int_keys_only =
      !int_key_indices.empty() &&
      (int_key_indices.size() == key_programs.size() || has_unique_int_key);
  // Scratch-mask copy: the pair loop fills only the cells the residual,
  // non-direct key, and non-direct child programs actually load (group
  // representatives — which the final select list reads in full — are re-filled
  // completely on the rare new-group path).  Direct-column keys and children
  // read straight from left_row/right_row without touching scratch, avoiding
  // per-pair VARCHAR copies on group hits (Q10, Q9, Q3, Q5, Q7, Q18, Q21).
  std::vector<slot_t> scratch_cells;
  for (const BytecodeProgram& program : residual_programs) {
    CollectLoadCells(program, &scratch_cells);
  }
  for (size_t k = 0; k < key_programs.size(); ++k) {
    if (!key_shapes[k].HasPairDirect()) {
      CollectLoadCells(key_programs[k], &scratch_cells);
    }
  }
  for (size_t i = 0; i < child_programs.size(); ++i) {
    if (!fusion->count_star[i] && !child_shapes[i].HasPairDirect()) {
      CollectLoadCells(child_programs[i], &scratch_cells);
    }
  }
  std::sort(scratch_cells.begin(), scratch_cells.end());
  scratch_cells.erase(std::unique(scratch_cells.begin(), scratch_cells.end()),
                      scratch_cells.end());
  const bool mask_scratch = scratch_cells.size() < left_width + right_width;

  std::vector<slot_t> left_columns;
  std::vector<slot_t> right_columns;
  left_columns.reserve(equality_keys.size());
  right_columns.reserve(equality_keys.size());
  for (const EqualityKey& key : equality_keys) {
    left_columns.push_back(static_cast<slot_t>(key.left));
    right_columns.push_back(static_cast<slot_t>(key.right));
  }
  const bool integer_key = SingleIntegerJoinKey(left.schema, left_columns) &&
                           SingleIntegerJoinKey(right.schema, right_columns);
  const bool composite_int_key =
      !integer_key && CompositeIntJoinKey(left.schema, left_columns,
                                          right.schema, right_columns);
  if (debug_fusion) {
    std::cerr << "[fusion] join_keys=" << equality_keys.size()
              << " integer=" << integer_key
              << " composite=" << composite_int_key
              << " right_columnar=" << right_col_only << "\n";
    for (const auto& eq : equality_keys) {
      std::cerr << "[fusion] equality=" << left.schema.GetColumn(eq.left).Name()
                << " -> " << right.schema.GetColumn(eq.right).Name() << "\n";
    }
  }
  // Biased keys hash with their constants folded in; the fused residual
  // programs still verify every matched pair against the original
  // predicates, so the bias only prunes buckets.
  std::vector<int64_t> left_key_offsets;
  std::vector<int64_t> right_key_offsets;
  left_key_offsets.reserve(equality_keys.size());
  right_key_offsets.reserve(equality_keys.size());
  for (const EqualityKey& key : equality_keys) {
    left_key_offsets.push_back(key.left_offset);
    right_key_offsets.push_back(key.right_offset);
  }

  std::vector<int64_t> validated_packed_keys;
  std::shared_ptr<QueryMemoryCharge> validated_packed_keys_charge;
  if (right_col_only) {
    auto program_loads_right = [&](const BytecodeProgram& prog) {
      std::vector<slot_t> cells;
      CollectLoadCells(prog, &cells);
      return std::ranges::any_of(cells,
                                 [&](slot_t c) { return c >= left_width; });
    };
    bool need_right_rows = left.HasSpill() || total_right_rows == 0;
    if (!need_right_rows) {
      for (const BytecodeProgram& rp : residual_programs) {
        if (program_loads_right(rp)) {
          need_right_rows = true;
          break;
        }
      }
    }
    if (!need_right_rows) {
      for (const BytecodeProgram& cp : child_programs) {
        if (program_loads_right(cp)) {
          need_right_rows = true;
          break;
        }
      }
    }
    if (!need_right_rows && !all_direct_group_keys) {
      for (size_t k = 0; k < key_programs.size(); ++k) {
        if (key_shapes[k].direct_column < 0 &&
            program_loads_right(key_programs[k])) {
          need_right_rows = true;
          break;
        }
      }
    }
    if (!need_right_rows && !integer_key) {
      if (!composite_int_key) {
        need_right_rows = true;
      } else if (right_columns.size() == 2) {
        const slot_t rc0 = right_columns[0];
        const slot_t rc1 = right_columns[1];
        const int64_t roff0 = right_key_offsets[0];
        const int64_t roff1 = right_key_offsets[1];
        bool can_pack = true;
        const size_t count = right_build_count();
        if (count > 0 && count <= 4ULL * 1024 * 1024) {
          validated_packed_keys_charge =
              TryReserveQueryMemory(count * sizeof(int64_t));
          if (validated_packed_keys_charge) {
            validated_packed_keys.assign(count, kInvalidFusedPacked2x32);
          }
        }
        size_t base_row = 0;
        for (const ColumnarChunk& rch : right_arg.columnar->chunks) {
          const ColumnarColumn& c0 = rch.columns[rc0];
          const ColumnarColumn& c1 = rch.columns[rc1];
          for (size_t r = 0; r < rch.rows; ++r) {
            int64_t v0 = 0;
            int64_t v1 = 0;
            if (!c0.TryReadNonNullInt(r, &v0) ||
                !c1.TryReadNonNullInt(r, &v1)) {
              continue;
            }
            const int64_t k0 = AddJoinKeyBias(v0, roff0);
            const int64_t k1 = AddJoinKeyBias(v1, roff1);
            if (!FitsFusedInt32(k0) || !FitsFusedInt32(k1) ||
                PackFusedInt32Pair(k0, k1) == kInvalidFusedPacked2x32) {
              can_pack = false;
              break;
            }
            if (!validated_packed_keys.empty()) {
              validated_packed_keys[base_row + r] = PackFusedInt32Pair(k0, k1);
            }
          }
          if (!can_pack) {
            break;
          }
          base_row += rch.rows;
        }
        if (!can_pack) {
          std::vector<int64_t>().swap(validated_packed_keys);
          validated_packed_keys_charge.reset();
          need_right_rows = true;
        }
      }
    }
    if (need_right_rows) {
      RETURN_IF_FAIL(fallback_to_right_rows().GetStatus());
    }
  }

  const size_t total_left = effective_left_rows;
  // Single-INT64/DATE group-key fast path: when the fusion groups by one
  // plain column of that domain, the pair loop probes a flat int table with
  // the raw cell payload (NULL keys keep the generic map, which groups
  // NULLs).  Q13's customer-key grouping pays the generic vector hash once
  // per matched pair otherwise.
  bool int_group_key = false;
  slot_t int_group_slot = 0;
  ValueType int_group_type = ValueType::kNull;
  if (key_programs.size() == 1 && key_shapes[0].direct_column >= 0) {
    const Column& column = result.schema.GetColumn(
        static_cast<size_t>(key_shapes[0].direct_column));
    const ValueType type = column.Type();
    if ((type == ValueType::kInt64 && !column.IsUnsigned()) ||
        type == ValueType::kDate) {
      int_group_key = true;
      int_group_slot = static_cast<slot_t>(key_shapes[0].direct_column);
      int_group_type = type;
    }
  }
  // Classification shared by every per-group accumulator construction below;
  // identical for all groups of one aggregate.
  std::vector<AggregateAccumulator::Shape> aggregate_shapes;
  aggregate_shapes.reserve(fusion->aggregates.size());
  for (const AggregateExpression* aggregate : fusion->aggregates) {
    aggregate_shapes.push_back(
        AggregateAccumulator::AnalyzeShape(aggregate, nullptr));
  }
  // Bulk raw-count: once every residual is hoisted off the pair path and the
  // group key is one probe-side INT64/DATE column, a count-only fusion needs
  // no per-pair work at all -- each surviving bucket entry contributes
  // exactly +1 (COUNT(*) and COUNT(not-null column) coincide inside a
  // bucket) -- so the probe collapses to one equal_range, one group-map
  // probe and one integer add per outer row.  The per-pair scratch fills,
  // flat-map probes and build-row dereferences all disappear (Q13's customer
  // LEFT JOIN orders COUNT(o_orderkey) walked ~915K pairs at ~330 ns each).
  static const bool bulk_count_disabled =
      ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_BULK_COUNT") != nullptr;
  const bool anti_unmatched_only =
      join_type == JoinType::kLeft && fusion->anti_unmatched_only;
  const bool bulk_raw_count =
      !singleton_right_relation && !left.HasSpill() && !bulk_count_disabled &&
      !anti_unmatched_only && raw_aggregates.raw_count_fusion && residual_programs.empty() &&
      int_group_key && int_group_slot < left_width && integer_key;
  static const Value kNullCell{};
  const auto right_row_ptr = [&](uint32_t r_idx) noexcept -> const Row* {
    return (!right_col_only && r_idx != kInvalidRightIdx)
               ? &right_ptr->rows[r_idx]
               : nullptr;
  };
  const FusedRightCellReader read_right_cell{.right_locator = right_locator,
                                             .right_ptr = right_ptr,
                                             .right_col_only = right_col_only,
                                             .kNullCell = kNullCell};
  const auto right_cell_is_null = [&](uint32_t r_idx,
                                      size_t rcol) noexcept -> bool {
    if (r_idx == kInvalidRightIdx) {
      return true;
    }
    if (right_col_only) {
      return right_locator.IsNull(rcol, r_idx);
    }
    return right_ptr->rows[r_idx].values_[rcol].IsNull();
  };
  const auto get_pair_cell = [&](const Row* left_row, const Row* right_row,
                                 slot_t col) -> const Value& {
    if (col < left_width) {
      return left_row != nullptr ? left_row->values_[col] : kNullCell;
    }
    return right_row != nullptr ? right_row->values_[col - left_width]
                                : kNullCell;
  };
  const auto eval_case_cond = [&](const ProgramShape& shape,
                                  const Row* left_row, const Row* right_row,
                                  bool* cond_true) noexcept -> bool {
    if (shape.case_cond == ProgramShape::CaseCondKind::kIntComparisons) {
      bool matches = true;
      for (const auto& test : shape.case_int_comparisons) {
        const auto& a =
            get_pair_cell(left_row, right_row, static_cast<slot_t>(test.left));
        const auto& b = test.subtract >= 0
                            ? get_pair_cell(left_row, right_row,
                                            static_cast<slot_t>(test.subtract))
                            : kNullCell;
        if (a.IsNull() || (test.subtract >= 0 && b.IsNull())) {
          matches = false;
          continue;
        }
        if (test.is_date) {
          if (a.type != ValueType::kDate || test.subtract >= 0) {
            return false;
          }
        } else if (a.type != ValueType::kInt64 || a.IsUnsigned() ||
                   (test.subtract >= 0 &&
                    (b.type != ValueType::kInt64 || b.IsUnsigned()))) {
          return false;
        }
        int64_t value = a.value.int_value;
        if (test.subtract >= 0 &&
            __builtin_sub_overflow(value, b.value.int_value, &value)) {
          return false;
        }
        switch (test.op) {
          case BinaryOperation::kEquals:
            matches &= value == test.constant;
            break;
          case BinaryOperation::kLessThan:
            matches &= value < test.constant;
            break;
          case BinaryOperation::kLessThanEquals:
            matches &= value <= test.constant;
            break;
          case BinaryOperation::kGreaterThan:
            matches &= value > test.constant;
            break;
          case BinaryOperation::kGreaterThanEquals:
            matches &= value >= test.constant;
            break;
          default:
            return false;
        }
      }
      *cond_true = matches;
      return true;
    }
    const Value& cc = get_pair_cell(left_row, right_row,
                                    static_cast<slot_t>(shape.case_cond_col));
    if (cc.IsNull()) {
      *cond_true = false;
      return true;
    }
    if (shape.case_cond == ProgramShape::CaseCondKind::kColEqInt) {
      if (cc.type == ValueType::kInt64 && !cc.IsUnsigned()) [[likely]] {
        *cond_true = (cc.value.int_value == shape.case_cond_int);
        return true;
      }
      return false;
    }
    if (cc.type == ValueType::kVarChar && cc.Collation() == 0 &&
        !cc.IsCaseInsensitive()) [[likely]] {
      const std::string_view sv = cc.value.varchar_value;
      switch (shape.case_cond) {
        case ProgramShape::CaseCondKind::kColEqStr:
          *cond_true = (sv == shape.case_cond_str0);
          return true;
        case ProgramShape::CaseCondKind::kColEq2StrOr:
          *cond_true =
              (sv == shape.case_cond_str0 || sv == shape.case_cond_str1);
          return true;
        case ProgramShape::CaseCondKind::kColNe2StrAnd:
          *cond_true =
              (sv != shape.case_cond_str0 && sv != shape.case_cond_str1);
          return true;
        case ProgramShape::CaseCondKind::kColLikePrefix:
          *cond_true = (sv.size() >= shape.case_cond_str0.size() &&
                        std::memcmp(sv.data(), shape.case_cond_str0.data(),
                                    shape.case_cond_str0.size()) == 0);
          return true;
        case ProgramShape::CaseCondKind::kNone:
        case ProgramShape::CaseCondKind::kColEqInt:
        case ProgramShape::CaseCondKind::kIntComparisons:
          break;
      }
    }
    return false;
  };
  std::shared_ptr<QueryMemoryCharge> right_case_charge;
  std::array<std::vector<uint8_t>, kRawAggregateCapacity> right_case_conditions;
  std::array<bool, kRawAggregateCapacity> right_case_complete{};
  const size_t right_case_rows =
      right_col_only ? total_right_rows : right_ptr->rows.size();
  if (raw_aggregates.raw_sum_fusion && effective_left_rows >= 1024 &&
      effective_left_rows >= right_case_rows && right_case_rows > 0 &&
      !right_ptr->HasSpill() &&
      right_case_rows <= SIZE_MAX / fusion->aggregates.size() &&
      std::ranges::any_of(child_shapes, [](const auto& shape) {
        return shape.case_cond != ProgramShape::CaseCondKind::kNone;
      })) {
    right_case_charge =
        TryReserveQueryMemory(right_case_rows * fusion->aggregates.size());
    if (right_case_charge) {
      for (size_t i = 0; i < child_shapes.size(); ++i) {
        const auto& shape = child_shapes[i];
        bool right_only =
            shape.case_cond != ProgramShape::CaseCondKind::kNone &&
            shape.case_cond_col >= 0 &&
            std::cmp_greater_equal(shape.case_cond_col, left_width);
        if (shape.case_cond == ProgramShape::CaseCondKind::kIntComparisons) {
          right_only = std::ranges::all_of(
              shape.case_int_comparisons, [&](const auto& test) {
                return std::cmp_greater_equal(test.left, left_width) &&
                       (test.subtract < 0 ||
                        std::cmp_greater_equal(test.subtract, left_width));
              });
        }
        if (!right_only) {
          continue;
        }
        auto& conditions = right_case_conditions[i];
        conditions.reserve(right_case_rows);
        bool all_known = true;
        if (right_col_only) {
          Row col_r_tmp;
          for (const ColumnarChunk& rch : right_arg.columnar->chunks) {
            for (size_t rr = 0; rr < rch.rows; ++rr) {
              rch.MaterializeRowInto(rr, &col_r_tmp);
              bool value = false;
              const uint8_t cond =
                  eval_case_cond(shape, nullptr, &col_r_tmp, &value)
                      ? static_cast<uint8_t>(value)
                      : uint8_t{2};
              all_known &= (cond < 2);
              conditions.push_back(cond);
            }
          }
        } else {
          for (const auto& row : right_ptr->rows) {
            bool value = false;
            const uint8_t cond = eval_case_cond(shape, nullptr, &row, &value)
                                     ? static_cast<uint8_t>(value)
                                     : uint8_t{2};
            all_known &= (cond < 2);
            conditions.push_back(cond);
          }
        }
        right_case_complete[i] = all_known;
      }
    }
  }
  const auto cached_case_cond = [&](size_t aggregate, const Row* left_row,
                                    const Row* right_row, bool* value) {
    const auto& conditions = right_case_conditions[aggregate];
    if (!right_col_only && right_row != nullptr && !conditions.empty()) {
      const auto index =
          static_cast<size_t>(right_row - right_ptr->rows.data());
      if (conditions[index] < 2) {
        *value = conditions[index] != 0;
        return true;
      }
    }
    return eval_case_cond(child_shapes[aggregate], left_row, right_row, value);
  };
  const auto case_always_null = [&](size_t aggregate, const Row* right_row) {
    const auto& conditions = right_case_conditions[aggregate];
    return !right_col_only && child_shapes[aggregate].case_else_null &&
           right_row != nullptr && !conditions.empty() &&
           conditions[static_cast<size_t>(right_row -
                                          right_ptr->rows.data())] == 0;
  };
  static const auto kFastYearLut = [] {
    std::array<int16_t, 25568> lut{};
    for (int d = 0; d < 25568; ++d) {
      const std::chrono::year_month_day ymd{
          std::chrono::sys_days{std::chrono::days{d}}};
      lut[static_cast<size_t>(d)] =
          static_cast<int16_t>(static_cast<int>(ymd.year()));
    }
    return lut;
  }();
  const auto eval_pair_program = [&](const BytecodeProgram& program,
                                     const ProgramShape& shape, Row& scratch,
                                     const Row* left_row,
                                     const Row* right_row) -> StatusOr<Value> {
    if (shape.direct_column >= 0) {
      return get_pair_cell(left_row, right_row,
                           static_cast<slot_t>(shape.direct_column));
    }
    if (shape.extract_date_col >= 0) {
      const Value& c = get_pair_cell(
          left_row, right_row, static_cast<slot_t>(shape.extract_date_col));
      if (c.IsNull()) {
        return Value();
      }
      if (c.type == ValueType::kDate && c.value.int_value >= kDateMinDays &&
          c.value.int_value <= kDateMaxDays) [[likely]] {
        if (shape.extract_date_part == 0 && c.value.int_value >= 0 &&
            std::cmp_less(c.value.int_value, kFastYearLut.size())) [[likely]] {
          return Value(static_cast<int64_t>(
              kFastYearLut[static_cast<size_t>(c.value.int_value)]));
        }
        const std::chrono::year_month_day ymd{std::chrono::sys_days{
            std::chrono::days{static_cast<int>(c.value.int_value)}}};
        if (shape.extract_date_part == 0) {
          const int64_t year = static_cast<int>(ymd.year());
          return Value(year);
        }
        if (shape.extract_date_part == 1) {
          return Value(
              static_cast<int64_t>(static_cast<unsigned>(ymd.month())));
        }
        return Value(static_cast<int64_t>(static_cast<unsigned>(ymd.day())));
      }
      scratch.values_[static_cast<size_t>(shape.extract_date_col)] = c;
      return EvaluateProgramValue(program, shape, scratch);
    }
    if (shape.mul_col0 >= 0) {
      const Value& c0 = get_pair_cell(left_row, right_row,
                                      static_cast<slot_t>(shape.mul_col0));
      const Value& c1 = get_pair_cell(left_row, right_row,
                                      static_cast<slot_t>(shape.mul_col1));
      if (c0.IsNull() || c1.IsNull()) {
        return Value();
      }
      double d0 = 0.0, d1 = 0.0;
      if (ReadFusedDouble(c0, &d0) && ReadFusedDouble(c1, &d1)) [[likely]] {
        return Value(d0 * d1);
      }
      scratch.values_[static_cast<size_t>(shape.mul_col0)] = c0;
      scratch.values_[static_cast<size_t>(shape.mul_col1)] = c1;
      return EvaluateProgramValue(program, shape, scratch);
    }
    if (shape.coalesce_mul_col0 >= 0) {
      const Value& c0 = get_pair_cell(
          left_row, right_row, static_cast<slot_t>(shape.coalesce_mul_col0));
      const Value& c1 = get_pair_cell(
          left_row, right_row, static_cast<slot_t>(shape.coalesce_mul_col1));
      if (c0.IsNull() || c1.IsNull()) {
        return Value(0.0);
      }
      double d0 = 0.0, d1 = 0.0;
      if (ReadFusedDouble(c0, &d0) && ReadFusedDouble(c1, &d1)) [[likely]] {
        return Value(d0 * d1);
      }
      scratch.values_[static_cast<size_t>(shape.coalesce_mul_col0)] = c0;
      scratch.values_[static_cast<size_t>(shape.coalesce_mul_col1)] = c1;
      return EvaluateProgramValue(program, shape, scratch);
    }
    if (shape.mul_one_minus_col0 >= 0) {
      const Value& c0 = get_pair_cell(
          left_row, right_row, static_cast<slot_t>(shape.mul_one_minus_col0));
      const Value& c1 = get_pair_cell(
          left_row, right_row, static_cast<slot_t>(shape.mul_one_minus_col1));
      if (c0.IsNull() || c1.IsNull()) {
        return Value();
      }
      double d0 = 0.0, d1 = 0.0;
      if (ReadFusedDouble(c0, &d0) && ReadFusedDouble(c1, &d1)) [[likely]] {
        return Value(d0 * (shape.mul_one_minus_const - d1));
      }
      scratch.values_[static_cast<size_t>(shape.mul_one_minus_col0)] = c0;
      scratch.values_[static_cast<size_t>(shape.mul_one_minus_col1)] = c1;
      return EvaluateProgramValue(program, shape, scratch);
    }
    if (shape.profit_col0 >= 0) {
      const Value& c0 = get_pair_cell(left_row, right_row,
                                      static_cast<slot_t>(shape.profit_col0));
      const Value& c1 = get_pair_cell(left_row, right_row,
                                      static_cast<slot_t>(shape.profit_col1));
      const Value& c2 = get_pair_cell(left_row, right_row,
                                      static_cast<slot_t>(shape.profit_col2));
      const Value& c3 = get_pair_cell(left_row, right_row,
                                      static_cast<slot_t>(shape.profit_col3));
      if (c0.IsNull() || c1.IsNull() || c2.IsNull() || c3.IsNull()) {
        return Value();
      }
      double d0 = 0.0, d1 = 0.0, d2 = 0.0, d3 = 0.0;
      if (ReadFusedDouble(c0, &d0) && ReadFusedDouble(c1, &d1) &&
          ReadFusedDouble(c2, &d2) && ReadFusedDouble(c3, &d3)) [[likely]] {
        return Value((d0 * (shape.profit_const - d1)) - (d2 * d3));
      }
      scratch.values_[static_cast<size_t>(shape.profit_col0)] = c0;
      scratch.values_[static_cast<size_t>(shape.profit_col1)] = c1;
      scratch.values_[static_cast<size_t>(shape.profit_col2)] = c2;
      scratch.values_[static_cast<size_t>(shape.profit_col3)] = c3;
      return EvaluateProgramValue(program, shape, scratch);
    }
    if (shape.case_cond != ProgramShape::CaseCondKind::kNone &&
        shape.case_val != ProgramShape::CaseValKind::kNone) {
      bool cond_true = false;
      if (eval_case_cond(shape, left_row, right_row, &cond_true)) [[likely]] {
        if (!cond_true) {
          if (shape.case_else_null) {
            return Value();
          }
          return (shape.case_val == ProgramShape::CaseValKind::kIntConst1 ||
                  shape.case_val == ProgramShape::CaseValKind::kDirectInt64Col)
                     ? Value(int64_t{0})
                     : Value(0.0);
        }
        if (shape.case_val == ProgramShape::CaseValKind::kIntConst1) {
          return Value(int64_t{1});
        }
        if (shape.case_val == ProgramShape::CaseValKind::kDirectInt64Col) {
          const Value& v0 = get_pair_cell(
              left_row, right_row, static_cast<slot_t>(shape.case_val_col0));
          if (v0.IsNull()) {
            return Value();
          }
          if (v0.type == ValueType::kInt64 && !v0.IsUnsigned()) [[likely]] {
            return Value(v0.value.int_value);
          }
        }
        if (shape.case_val == ProgramShape::CaseValKind::kDirectDoubleCol) {
          const Value& v0 = get_pair_cell(
              left_row, right_row, static_cast<slot_t>(shape.case_val_col0));
          if (v0.IsNull()) {
            return Value();
          }
          double d0 = 0.0;
          if (ReadFusedDouble(v0, &d0)) [[likely]] {
            return Value(d0);
          }
        } else if (shape.case_val == ProgramShape::CaseValKind::kMulOneMinus) {
          const Value& v0 = get_pair_cell(
              left_row, right_row, static_cast<slot_t>(shape.case_val_col0));
          const Value& v1 = get_pair_cell(
              left_row, right_row, static_cast<slot_t>(shape.case_val_col1));
          if (v0.IsNull() || v1.IsNull()) {
            return Value();
          }
          double d0 = 0.0, d1 = 0.0;
          if (ReadFusedDouble(v0, &d0) && ReadFusedDouble(v1, &d1)) [[likely]] {
            return Value(d0 * (shape.case_val_const - d1));
          }
        }
      }
      if (shape.case_cond_col >= 0) {
        scratch.values_[static_cast<size_t>(shape.case_cond_col)] =
            get_pair_cell(left_row, right_row,
                          static_cast<slot_t>(shape.case_cond_col));
      }
      for (const auto& test : shape.case_int_comparisons) {
        scratch.values_[static_cast<size_t>(test.left)] =
            get_pair_cell(left_row, right_row, static_cast<slot_t>(test.left));
        if (test.subtract >= 0) {
          scratch.values_[static_cast<size_t>(test.subtract)] = get_pair_cell(
              left_row, right_row, static_cast<slot_t>(test.subtract));
        }
      }
      if (shape.case_val_col0 >= 0) {
        scratch.values_[static_cast<size_t>(shape.case_val_col0)] =
            get_pair_cell(left_row, right_row,
                          static_cast<slot_t>(shape.case_val_col0));
      }
      if (shape.case_val_col1 >= 0) {
        scratch.values_[static_cast<size_t>(shape.case_val_col1)] =
            get_pair_cell(left_row, right_row,
                          static_cast<slot_t>(shape.case_val_col1));
      }
      return EvaluateProgramValue(program, shape, scratch);
    }
    return EvaluateProgramValue(program, shape, scratch);
  };
  std::vector<std::vector<double>> right_profit_doubles(child_shapes.size());
  if (!right_col_only && raw_aggregates.raw_sum_fusion && !right_ptr->HasSpill() &&
      !right_ptr->rows.empty() && right_ptr->rows.size() <= 262144) {
    for (size_t i = 0; i < child_shapes.size(); ++i) {
      const ProgramShape& sh = child_shapes[i];
      if (sh.profit_col0 < 0 ||
          std::cmp_greater_equal(sh.profit_col0, left_width) ||
          std::cmp_greater_equal(sh.profit_col1, left_width)) {
        continue;
      }
      const bool col2_right =
          std::cmp_greater_equal(sh.profit_col2, left_width) &&
          std::cmp_less(sh.profit_col3, left_width);
      const bool col3_right =
          std::cmp_greater_equal(sh.profit_col3, left_width) &&
          std::cmp_less(sh.profit_col2, left_width);
      if (!col2_right && !col3_right) {
        continue;
      }
      const auto rcol =
          static_cast<size_t>((col2_right ? sh.profit_col2 : sh.profit_col3) -
                              static_cast<int32_t>(left_width));
      if (rcol >= right.schema.ColumnCount()) {
        continue;
      }
      std::vector<double> vals(right_ptr->rows.size());
      bool all_ok = true;
      for (size_t ri = 0; ri < right_ptr->rows.size(); ++ri) {
        const Value& rv = right_ptr->rows[ri].values_[rcol];
        double dv = 0.0;
        if (rv.IsNull() || !ReadFusedDouble(rv, &dv)) {
          all_ok = false;
          break;
        }
        vals[ri] = dv;
      }
      if (all_ok) {
        right_profit_doubles[i] = std::move(vals);
      }
    }
  }
  // Straight DOUBLE arithmetic needs neither a Value stack nor a joined
  // scratch row. Keep the exact expression order and NULL flags, and leave
  // integer subexpressions, functions and control flow to the canonical VM.
  std::vector<bool> double_arithmetic(child_programs.size(), false);
  for (size_t i = 0; i < child_programs.size(); ++i) {
    const auto& program = child_programs[i];
    size_t depth = 0;
    bool eligible = program.ResultType() == ValueType::kDouble;
    for (const auto& instruction : program.Instructions()) {
      if (instruction.opcode == BytecodeOp::kLoadColumn) {
        eligible = eligible &&
                   instruction.operand < result.schema.ColumnCount() &&
                   result.schema.GetColumn(instruction.operand).Type() ==
                       ValueType::kDouble;
        ++depth;
      } else if (instruction.opcode == BytecodeOp::kLoadConstant) {
        const auto& value = program.Constants()[instruction.operand];
        eligible = eligible && !value.IsNull() && !value.IsUnsigned() &&
                   (value.type == ValueType::kDouble ||
                    value.type == ValueType::kInt64);
        ++depth;
      } else if (instruction.opcode == BytecodeOp::kBinaryDouble &&
                 depth >= 2 &&
                 (instruction.binary == BinaryOperation::kAdd ||
                  instruction.binary == BinaryOperation::kSubtract ||
                  instruction.binary == BinaryOperation::kMultiply ||
                  instruction.binary == BinaryOperation::kDivide)) {
        --depth;
      } else {
        eligible = false;
      }
      if (!eligible || depth > 16) {
        eligible = false;
        break;
      }
    }
    double_arithmetic[i] = eligible && depth == 1;
  }
  const auto eval_pair_double = [&](size_t agg_idx, Row& scratch,
                                    const Row* left_row, const Row* right_row,
                                    double* out, bool* is_null) -> Status {
    const ProgramShape& shape = child_shapes[agg_idx];
    if (shape.direct_column >= 0) {
      const Value& c = get_pair_cell(left_row, right_row,
                                     static_cast<slot_t>(shape.direct_column));
      if (c.IsNull()) {
        *is_null = true;
        return Status::kSuccess;
      }
      if (ReadFusedDouble(c, out)) [[likely]] {
        *is_null = false;
        return Status::kSuccess;
      }
    } else if (shape.mul_col0 >= 0) {
      const Value& c0 = get_pair_cell(left_row, right_row,
                                      static_cast<slot_t>(shape.mul_col0));
      const Value& c1 = get_pair_cell(left_row, right_row,
                                      static_cast<slot_t>(shape.mul_col1));
      if (c0.IsNull() || c1.IsNull()) {
        *is_null = true;
        return Status::kSuccess;
      }
      double d0 = 0.0, d1 = 0.0;
      if (ReadFusedDouble(c0, &d0) && ReadFusedDouble(c1, &d1)) [[likely]] {
        *out = d0 * d1;
        *is_null = false;
        return Status::kSuccess;
      }
    } else if (shape.coalesce_mul_col0 >= 0) {
      const Value& c0 = get_pair_cell(
          left_row, right_row, static_cast<slot_t>(shape.coalesce_mul_col0));
      const Value& c1 = get_pair_cell(
          left_row, right_row, static_cast<slot_t>(shape.coalesce_mul_col1));
      if (c0.IsNull() || c1.IsNull()) {
        *out = 0.0;
        *is_null = false;
        return Status::kSuccess;
      }
      double d0 = 0.0, d1 = 0.0;
      if (ReadFusedDouble(c0, &d0) && ReadFusedDouble(c1, &d1)) [[likely]] {
        *out = d0 * d1;
        *is_null = false;
        return Status::kSuccess;
      }
    } else if (shape.mul_one_minus_col0 >= 0) {
      const Value& c0 = get_pair_cell(
          left_row, right_row, static_cast<slot_t>(shape.mul_one_minus_col0));
      const Value& c1 = get_pair_cell(
          left_row, right_row, static_cast<slot_t>(shape.mul_one_minus_col1));
      if (c0.IsNull() || c1.IsNull()) {
        *is_null = true;
        return Status::kSuccess;
      }
      double d0 = 0.0, d1 = 0.0;
      if (ReadFusedDouble(c0, &d0) && ReadFusedDouble(c1, &d1)) [[likely]] {
        *out = d0 * (shape.mul_one_minus_const - d1);
        *is_null = false;
        return Status::kSuccess;
      }
    } else if (shape.profit_col0 >= 0) {
      if (!right_col_only && !right_profit_doubles[agg_idx].empty() &&
          left_row != nullptr && right_row != nullptr) [[likely]] {
        const auto r_idx =
            static_cast<size_t>(right_row - right_ptr->rows.data());
        const Value& c0 =
            left_row->values_[static_cast<size_t>(shape.profit_col0)];
        const Value& c1 =
            left_row->values_[static_cast<size_t>(shape.profit_col1)];
        const bool col2_right =
            std::cmp_greater_equal(shape.profit_col2, left_width);
        const Value& left_factor = left_row->values_[static_cast<size_t>(
            col2_right ? shape.profit_col3 : shape.profit_col2)];
        if (c0.IsNull() || c1.IsNull() || left_factor.IsNull()) [[unlikely]] {
          *is_null = true;
          return Status::kSuccess;
        }
        double d0 = 0.0, d1 = 0.0, left_number = 0.0;
        if (ReadFusedDouble(c0, &d0) && ReadFusedDouble(c1, &d1) &&
            ReadFusedDouble(left_factor, &left_number)) [[likely]] {
          const double dr = right_profit_doubles[agg_idx][r_idx];
          *out = (d0 * (shape.profit_const - d1)) -
                 (col2_right ? (dr * left_number) : (left_number * dr));
          *is_null = false;
          return Status::kSuccess;
        }
      }
      const Value& c0 = get_pair_cell(left_row, right_row,
                                      static_cast<slot_t>(shape.profit_col0));
      const Value& c1 = get_pair_cell(left_row, right_row,
                                      static_cast<slot_t>(shape.profit_col1));
      const Value& c2 = get_pair_cell(left_row, right_row,
                                      static_cast<slot_t>(shape.profit_col2));
      const Value& c3 = get_pair_cell(left_row, right_row,
                                      static_cast<slot_t>(shape.profit_col3));
      if (c0.IsNull() || c1.IsNull() || c2.IsNull() || c3.IsNull()) {
        *is_null = true;
        return Status::kSuccess;
      }
      double d0 = 0.0, d1 = 0.0, d2 = 0.0, d3 = 0.0;
      if (ReadFusedDouble(c0, &d0) && ReadFusedDouble(c1, &d1) &&
          ReadFusedDouble(c2, &d2) && ReadFusedDouble(c3, &d3)) [[likely]] {
        *out = (d0 * (shape.profit_const - d1)) - (d2 * d3);
        *is_null = false;
        return Status::kSuccess;
      }
    } else if (shape.case_cond != ProgramShape::CaseCondKind::kNone &&
               shape.case_val != ProgramShape::CaseValKind::kNone) {
      bool cond_true = false;
      if (cached_case_cond(agg_idx, left_row, right_row, &cond_true))
          [[likely]] {
        if (!cond_true) {
          *out = 0.0;
          *is_null = shape.case_else_null;
          return Status::kSuccess;
        }
        if (shape.case_val == ProgramShape::CaseValKind::kDirectDoubleCol) {
          const Value& v0 = get_pair_cell(
              left_row, right_row, static_cast<slot_t>(shape.case_val_col0));
          if (v0.IsNull()) {
            *is_null = true;
            return Status::kSuccess;
          }
          if (ReadFusedDouble(v0, out)) [[likely]] {
            *is_null = false;
            return Status::kSuccess;
          }
        } else if (shape.case_val == ProgramShape::CaseValKind::kMulOneMinus) {
          const Value& v0 = get_pair_cell(
              left_row, right_row, static_cast<slot_t>(shape.case_val_col0));
          const Value& v1 = get_pair_cell(
              left_row, right_row, static_cast<slot_t>(shape.case_val_col1));
          if (v0.IsNull() || v1.IsNull()) {
            *is_null = true;
            return Status::kSuccess;
          }
          double d0 = 0.0, d1 = 0.0;
          if (ReadFusedDouble(v0, &d0) && ReadFusedDouble(v1, &d1)) [[likely]] {
            *out = d0 * (shape.case_val_const - d1);
            *is_null = false;
            return Status::kSuccess;
          }
        }
      }
    }
    if (double_arithmetic[agg_idx]) {
      std::array<double, 16> values{};
      std::array<bool, 16> nulls{};
      size_t depth = 0;
      bool supported = true;
      const auto& program = child_programs[agg_idx];
      for (const auto& instruction : program.Instructions()) {
        if (instruction.opcode == BytecodeOp::kLoadColumn) {
          const auto& value =
              get_pair_cell(left_row, right_row, instruction.operand);
          nulls[depth] = value.IsNull();
          if (!nulls[depth]) {
            if (value.type != ValueType::kDouble) {
              supported = false;
              break;
            }
            values[depth] = value.value.double_value;
          }
          ++depth;
        } else if (instruction.opcode == BytecodeOp::kLoadConstant) {
          const auto& value = program.Constants()[instruction.operand];
          values[depth] = value.type == ValueType::kDouble
                              ? value.value.double_value
                              : static_cast<double>(value.value.int_value);
          nulls[depth++] = false;
        } else {
          const size_t rhs = --depth;
          const size_t lhs = depth - 1;
          nulls[lhs] = nulls[lhs] || nulls[rhs];
          if (nulls[lhs]) {
            continue;
          }
          switch (instruction.binary) {
            case BinaryOperation::kAdd:
              values[lhs] += values[rhs];
              break;
            case BinaryOperation::kSubtract:
              values[lhs] -= values[rhs];
              break;
            case BinaryOperation::kMultiply:
              values[lhs] *= values[rhs];
              break;
            case BinaryOperation::kDivide: {
              if (values[rhs] == 0.0) {
                return StatusError(StatusCode::kIsInfinity, "division by zero");
              }
              const double quotient = values[lhs] / values[rhs];
              if (std::isfinite(values[lhs]) && std::isfinite(values[rhs]) &&
                  std::isinf(quotient)) {
                return StatusError(StatusCode::kIsInfinity, "double overflow");
              }
              values[lhs] = quotient;
              break;
            }
            default:
              break;
          }
        }
      }
      if (supported) {
        *out = values[0];
        *is_null = nulls[0];
        return Status::kSuccess;
      }
    }
    ASSIGN_OR_RETURN(Value, child,
                     eval_pair_program(child_programs[agg_idx], shape, scratch,
                                       left_row, right_row));
    if (child.IsNull()) {
      *is_null = true;
      return Status::kSuccess;
    }
    if (ReadFusedDouble(child, out)) {
      *is_null = false;
      return Status::kSuccess;
    }
    return StatusError(StatusCode::kInvalidArgument,
                       "numeric aggregate operand is not double/int64");
  };
  const auto eval_pair_ext = [&](size_t agg_idx, Row& scratch,
                                 const Row* left_row, const Row* right_row,
                                 platform::ExtendedFloat* out,
                                 bool* is_null) -> Status {
    const ProgramShape& shape = child_shapes[agg_idx];
    if (shape.direct_column >= 0) {
      const Value& c = get_pair_cell(left_row, right_row,
                                     static_cast<slot_t>(shape.direct_column));
      if (c.IsNull()) {
        *is_null = true;
        return Status::kSuccess;
      }
      if (c.type == ValueType::kDouble) {
        *out = static_cast<platform::ExtendedFloat>(c.value.double_value);
        *is_null = false;
        return Status::kSuccess;
      }
      if ((c.type == ValueType::kInt64 || c.type == ValueType::kDate) &&
          !c.IsUnsigned()) {
        *out = static_cast<platform::ExtendedFloat>(c.value.int_value);
        *is_null = false;
        return Status::kSuccess;
      }
    }
    double dv = 0.0;
    RETURN_IF_FAIL(
        eval_pair_double(agg_idx, scratch, left_row, right_row, &dv, is_null));
    *out = static_cast<platform::ExtendedFloat>(dv);
    return Status::kSuccess;
  };
  const auto read_group_pair_cell = [&](const FusedGroup& grp, size_t col_idx,
                                        Value* col_scratch) -> const Value& {
    if (col_idx < left_width) {
      if (grp.rep_left != nullptr) {
        return grp.rep_left->values_[col_idx];
      }
      if (grp.rep_col_chunk != std::numeric_limits<uint32_t>::max()) {
        left.columnar->chunks[grp.rep_col_chunk]
            .columns[col_idx]
            .WriteValueInto(grp.rep_col_row, col_scratch);
        return *col_scratch;
      }
      return kNullCell;
    }
    return read_right_cell(grp.rep_right_idx, col_idx - left_width,
                           col_scratch);
  };
  const auto ensure_group_key = [&](FusedGroup& grp) {
    if (!grp.key.values_.empty() || !all_direct_group_keys ||
        (grp.rep_left == nullptr &&
         grp.rep_col_chunk == std::numeric_limits<uint32_t>::max() &&
         grp.rep_right_idx == kInvalidRightIdx)) {
      return;
    }
    grp.key.values_.resize(key_programs.size());
    Value col_tmp;
    for (size_t k = 0; k < key_programs.size(); ++k) {
      const Value& raw = read_group_pair_cell(
          grp, static_cast<size_t>(key_shapes[k].direct_column), &col_tmp);
      grp.key.values_[k] = CanonicalDistinctValue(raw);
    }
  };
  std::vector<ValueType> def_base_types;
  std::vector<uint8_t> def_overwritten_by_pk;
  std::vector<PreparedDeferredPkLookup> prepared_pk_lookups;
  std::vector<PreparedDeferredPkGroup> def_pk_groups;
  std::array<int8_t, 16> def_col_to_pk_lookup{};
  def_col_to_pk_lookup.fill(-1);
  SharedBaseRelationCache* const shared_pk_cache =
      context.execution_runtime() != nullptr
          ? context.execution_runtime()->shared_base_cache.get()
          : nullptr;
  if (deferred_left_probe != nullptr) {
    if (join_type != JoinType::kInner || left.HasSpill() ||
        deferred_left_probe->store == nullptr ||
        deferred_left_probe->pk_lookups.size() > 4) {
      return not_fusable("deferred_left_probe unsupported");
    }
    const size_t base_proj_cols = deferred_left_probe->projection.size();
    if (base_proj_cols == 0 || base_proj_cols > 16 ||
        base_proj_cols > left_width) {
      return not_fusable("deferred_left_probe width");
    }
    def_base_types.resize(base_proj_cols, ValueType::kNull);
    def_overwritten_by_pk.assign(base_proj_cols, 0);
    for (const DeferredLSTablePkLookup& lkp : deferred_left_probe->pk_lookups) {
      if (lkp.store == nullptr || lkp.pk_col >= base_proj_cols ||
          lkp.target_proj_col >= left_width ||
          lkp.store_pk_col >= lkp.store->column_min_int.size() ||
          lkp.payload_col >= lkp.store->column_min_int.size() ||
          (lkp.payload_type != ValueType::kInt64 &&
           lkp.payload_type != ValueType::kDate &&
           lkp.payload_type != ValueType::kDouble &&
           lkp.payload_type != ValueType::kVarChar)) {
        return not_fusable("deferred_left_probe pk_lookup");
      }
      if (lkp.target_proj_col < base_proj_cols) {
        def_overwritten_by_pk[lkp.target_proj_col] = 1;
      }
      PreparedDeferredPkLookup prep;
      prep.pk_col = lkp.pk_col;
      prep.target_proj_col = lkp.target_proj_col;
      prep.payload_type = lkp.payload_type;
      prep.payload_unsigned = lkp.payload_unsigned;
      prep.blocks.reserve(lkp.store->blocks.size());
      prep.blk_max_keys.reserve(lkp.store->blocks.size());
      for (const LSTablePageBlock& blk : lkp.store->blocks) {
        if (blk.row_count > 0) {
          if (lkp.store_pk_col >= blk.columns.size() ||
              lkp.payload_col >= blk.columns.size()) {
            return not_fusable("deferred_left_probe pk blk");
          }
          const LSTableColumnBlock& pk_b = blk.columns[lkp.store_pk_col];
          const LSTableColumnBlock& pay_b = blk.columns[lkp.payload_col];
          const bool is_dense =
              (static_cast<uint64_t>(pk_b.max_int) -
                   static_cast<uint64_t>(pk_b.min_int) + 1ULL ==
               blk.row_count);
          prep.blocks.push_back(PreparedDeferredPkBlock{
              .min_k = pk_b.min_int,
              .max_k = pk_b.max_int,
              .row_count = blk.row_count,
              .dense_contiguous = static_cast<uint8_t>(is_dense),
              .has_null_payload = static_cast<uint8_t>(pay_b.has_null),
              .pk_cells = pk_b.int_cells.data(),
              .pay_cells = pay_b.int_cells.data(),
              .pay_blk = &pay_b});
          prep.blk_max_keys.push_back(pk_b.max_int);
        }
      }
      if (prep.blocks.empty()) {
        return not_fusable("deferred_left_probe empty pk blocks");
      }
      prep.min_k = lkp.store->column_min_int[lkp.store_pk_col];
      prep.max_k = lkp.store->column_max_int[lkp.store_pk_col];
      prep.span = std::max<uint64_t>(
          1ULL, static_cast<uint64_t>(prep.max_k) -
                    static_cast<uint64_t>(prep.min_k) + 1ULL);
      const auto nb_count = static_cast<uint32_t>(prep.blocks.size());
      uint32_t cur_nb = 0;
      for (size_t b = 0; b < PreparedDeferredPkLookup::kDirBuckets; ++b) {
        const int64_t b_min =
            prep.min_k +
            static_cast<int64_t>((static_cast<__uint128_t>(prep.span) * b) >>
                                 PreparedDeferredPkLookup::kDirShift);
        while (cur_nb + 1 < nb_count && prep.blk_max_keys[cur_nb] < b_min) {
          ++cur_nb;
        }
        prep.bucket_lo[b] = cur_nb;
      }
      prep.bucket_lo[PreparedDeferredPkLookup::kDirBuckets] = nb_count - 1;
      prep.store_payload_col = lkp.payload_col;
      const size_t li = prepared_pk_lookups.size();
      size_t gi = def_pk_groups.size();
      for (size_t g = 0; g < def_pk_groups.size(); ++g) {
        if (def_pk_groups[g].pk_col == lkp.pk_col &&
            def_pk_groups[g].store == lkp.store.get() &&
            def_pk_groups[g].store_pk_col == lkp.store_pk_col) {
          gi = g;
          break;
        }
      }
      if (gi == def_pk_groups.size()) {
        PreparedDeferredPkGroup grp;
        grp.pk_col = lkp.pk_col;
        grp.store = lkp.store.get();
        grp.store_owner = lkp.store;
        grp.store_pk_col = lkp.store_pk_col;
        grp.min_k = prep.min_k;
        grp.max_k = prep.max_k;
        grp.span = prep.span;
        grp.bucket_lo = prep.bucket_lo;
        grp.blk_max_keys = prep.blk_max_keys;
        grp.blocks.reserve(prep.blocks.size());
        uint32_t running_rows = 0;
        for (const auto& pb : prep.blocks) {
          grp.blocks.push_back(PreparedDeferredPkGroup::Block{
              .min_k = pb.min_k,
              .max_k = pb.max_k,
              .row_count = pb.row_count,
              .row_base = running_rows,
              .dense_contiguous = pb.dense_contiguous,
              .pk_cells = pb.pk_cells});
          running_rows += pb.row_count;
        }
        grp.total_rows = running_rows;
        def_pk_groups.push_back(std::move(grp));
      }
      prep.group_idx = gi;
      def_pk_groups[gi].lookup_indices.push_back(li);
      if (prep.target_proj_col < 16) {
        def_col_to_pk_lookup[prep.target_proj_col] = static_cast<int8_t>(li);
      }
      prepared_pk_lookups.push_back(std::move(prep));
    }
    constexpr uint64_t kMaxDensePkSpan = 262144;
    for (PreparedDeferredPkGroup& grp : def_pk_groups) {
      if (grp.total_rows == 0 || grp.max_k < grp.min_k ||
          grp.span > kMaxDensePkSpan) {
        continue;
      }
      std::string dense_key;
      struct DenseSlotGuard {
        SharedBaseRelationCache* cache{nullptr};
        std::string key;
        bool active{false};
        DenseSlotGuard() = default;
        DenseSlotGuard(SharedBaseRelationCache* c, std::string k, bool a)
            : cache(c), key(std::move(k)), active(a) {}
        DenseSlotGuard(const DenseSlotGuard&) = delete;
        DenseSlotGuard& operator=(const DenseSlotGuard&) = delete;
        DenseSlotGuard(DenseSlotGuard&&) = delete;
        DenseSlotGuard& operator=(DenseSlotGuard&&) = delete;
        ~DenseSlotGuard() {
          if (active && cache != nullptr) {
            cache->AbandonPkDense(key);
          }
        }
      } dense_guard(shared_pk_cache, {}, false);
      if (shared_pk_cache != nullptr && grp.store != nullptr) {
        dense_key =
            "D:" + std::to_string(reinterpret_cast<uintptr_t>(grp.store)) +
            ":" + std::to_string(grp.store->layout_generation) + ":" +
            std::to_string(grp.store_pk_col) + ":" + std::to_string(grp.min_k) +
            ":" + std::to_string(grp.max_k) + ":" +
            std::to_string(grp.total_rows);
        std::shared_ptr<
            const std::vector<PreparedDeferredPkGroup::DensePkEntry>>
            hit;
        if (!shared_pk_cache->AcquirePkDenseSlot(dense_key, &hit)) {
          if (hit != nullptr && hit->size() == static_cast<size_t>(grp.span)) {
            grp.shared_dense = std::move(hit);
            grp.dense_ptr = grp.shared_dense->data();
            grp.dense_size = grp.shared_dense->size();
            continue;
          }
        } else {
          dense_guard.key = dense_key;
          dense_guard.active = true;
        }
      }
      const size_t dense_bytes = static_cast<size_t>(grp.span) *
                                 sizeof(PreparedDeferredPkGroup::DensePkEntry);
      grp.dense_index_charge = TryReserveQueryMemory(dense_bytes);
      if (!grp.dense_index_charge) {
        continue;
      }
      grp.dense_index.resize(static_cast<size_t>(grp.span));
      bool dense_ok = true;
      bool blocks_disjoint_sorted =
          grp.blocks.size() >= 2 && grp.total_rows >= 16384;
      int64_t prev_max_k = std::numeric_limits<int64_t>::min();
      bool have_prev_blk = false;
      for (const auto& bm : grp.blocks) {
        if (bm.pk_cells == nullptr) {
          dense_ok = false;
          break;
        }
        if (bm.row_count > 0) {
          if (have_prev_blk && bm.min_k <= prev_max_k) {
            blocks_disjoint_sorted = false;
          }
          prev_max_k = bm.max_k;
          have_prev_blk = true;
        }
      }
      if (dense_ok) {
        if (blocks_disjoint_sorted) {
          const size_t dense_workers = std::min<size_t>(
              ParallelWorkerCount(grp.total_rows / 8192), grp.blocks.size());
          if (dense_workers > 1) {
            std::atomic<uint32_t> next_blk{0};
            const auto blk_count = static_cast<uint32_t>(grp.blocks.size());
            RETURN_IF_FAIL(RunParallelTasks(dense_workers, [&](size_t) {
              uint32_t nb = 0;
              while ((nb = next_blk.fetch_add(1, std::memory_order_relaxed)) <
                     blk_count) {
                const auto& bm = grp.blocks[nb];
                const int64_t* const pks = bm.pk_cells;
                for (uint32_t rin = 0; rin < bm.row_count; ++rin) {
                  const int64_t pk = pks[rin];
                  if (pk >= grp.min_k && pk <= grp.max_k) {
                    const auto idx =
                        static_cast<size_t>(static_cast<uint64_t>(pk) -
                                            static_cast<uint64_t>(grp.min_k));
                    auto& entry = grp.dense_index[idx];
                    if (entry.nb == std::numeric_limits<uint32_t>::max()) {
                      entry.nb = nb;
                      entry.rin = rin;
                      entry.subgid = bm.row_base + rin;
                    }
                  }
                }
              }
            }));
          } else {
            blocks_disjoint_sorted = false;
          }
        }
        if (!blocks_disjoint_sorted) {
          for (uint32_t nb = 0; nb < grp.blocks.size(); ++nb) {
            const auto& bm = grp.blocks[nb];
            const int64_t* const pks = bm.pk_cells;
            for (uint32_t rin = 0; rin < bm.row_count; ++rin) {
              const int64_t pk = pks[rin];
              if (pk >= grp.min_k && pk <= grp.max_k) {
                const auto idx =
                    static_cast<size_t>(static_cast<uint64_t>(pk) -
                                        static_cast<uint64_t>(grp.min_k));
                auto& entry = grp.dense_index[idx];
                if (entry.nb == std::numeric_limits<uint32_t>::max()) {
                  entry.nb = nb;
                  entry.rin = rin;
                  // Stash row_base + rin temporarily; remapped to subgid if
                  // built.
                  entry.subgid = bm.row_base + rin;
                }
              }
            }
          }
        }
      }
      if (!dense_ok) {
        grp.dense_index.clear();
        grp.dense_index_charge.reset();
      } else if (dense_guard.active && shared_pk_cache != nullptr) {
        auto shared = std::make_shared<
            std::vector<PreparedDeferredPkGroup::DensePkEntry>>(
            std::move(grp.dense_index));
        shared_pk_cache->PublishPkDense(dense_key, shared);
        dense_guard.active = false;
        grp.shared_dense = std::move(shared);
        grp.dense_ptr = grp.shared_dense->data();
        grp.dense_size = grp.shared_dense->size();
      } else {
        grp.dense_ptr = grp.dense_index.data();
        grp.dense_size = grp.dense_index.size();
      }
    }
    for (const slot_t lk : left_columns) {
      if (lk >= base_proj_cols || def_overwritten_by_pk[lk] != 0) {
        return not_fusable("deferred_left_probe join key overwritten");
      }
    }
    for (size_t c = 0; c < base_proj_cols; ++c) {
      if (def_overwritten_by_pk[c] == 0) {
        const Column& col = left.schema.GetColumn(c);
        if (col.IsUnsigned() || (col.Type() != ValueType::kInt64 &&
                                 col.Type() != ValueType::kDate &&
                                 col.Type() != ValueType::kDouble)) {
          return not_fusable("deferred_left_probe base col type");
        }
        def_base_types[c] = col.Type();
      }
    }
  }
  const PreparedPkCellReader read_pk_cell{prepared_pk_lookups};
  FusedGroupKeyLayout layout;
  RETURN_IF_FAIL(layout.Prepare({
      .left = left,
      .right_arg = right_arg,
      .join_type = join_type,
      .fusion = fusion,
      .deferred_left_probe = deferred_left_probe,
      .total_right_rows = total_right_rows,
      .right_col_only = right_col_only,
      .right_col_owner = right_col_owner,
      .right_locator = right_locator,
      .right_ptr = right_ptr,
      .right = right,
      .result = result,
      .singleton_right_relation = singleton_right_relation,
      .equality_keys = equality_keys,
      .left_width = left_width,
      .key_programs = key_programs,
      .key_shapes = key_shapes,
      .count_star_only = raw_aggregates.count_star_only,
      .raw_count_fusion = raw_aggregates.raw_count_fusion,
      .raw_sum_fusion = raw_aggregates.raw_sum_fusion,
      .all_direct_group_keys = all_direct_group_keys,
      .int_group_key = int_group_key,
      .read_right_cell = read_right_cell,
      .prepared_pk_lookups = prepared_pk_lookups,
      .def_pk_groups = def_pk_groups,
      .def_col_to_pk_lookup = def_col_to_pk_lookup,
      .shared_pk_cache = shared_pk_cache,
      .read_pk_cell = read_pk_cell,
  }));
  // A selective COUNT HAVING needs only a counter and borrowed key owners
  // while probing. Delay the much larger general aggregate state until the
  // worker counts have merged and the HAVING predicate has accepted a group.
  // Lean proof: lean/TinylambOpt/PredicateAndRelationalLowering.lean
  //   (packed_2x32_join_key_bijection_sound)
  const bool compact_raw_left_key = [&]() {
    if (!layout.split_key_unpack_only || !layout.split_left_slot ||
        deferred_left_probe == nullptr ||
        *layout.split_left_slot >= deferred_left_probe->projection.size() ||
        !prepared_pk_lookups.empty() || !residual_programs.empty()) {
      return false;
    }
    const auto& store = *deferred_left_probe->store;
    const slot_t col = deferred_left_probe->projection[*layout.split_left_slot];
    return store.HasColumn(col) && col < store.column_has_null.size() &&
           !store.column_has_null[col] && col < store.column_min_int.size() &&
           col < store.column_max_int.size() &&
           store.column_min_int[col] <= store.column_max_int[col] &&
           std::in_range<int32_t>(store.column_min_int[col]) &&
           std::in_range<int32_t>(store.column_max_int[col]);
  }();
  const bool compact_split_key_eligible =
      layout.use_split_key_gid && layout.split_key_unpack_only &&
      (!layout.split_has_left_probe || compact_raw_left_key) && layout.right_row_count > 0 &&
      (!layout.split_uses_right_subgid ||
       layout.split_right_rep_count == layout.split_right_subgid_count);
  const bool compact_counts =
      raw_aggregates.raw_count_fusion && raw_aggregates.count_star_only && !bulk_raw_count &&
      all_direct_group_keys && !right_ptr->HasSpill() &&
      join_type == JoinType::kInner &&
      ((deferred_left_probe == nullptr && !left.HasColumnarOnly()) ||
       (integer_key && compact_split_key_eligible)) &&
      (layout.compact_integer_threshold.has_value() ||
       (fusion->post_having_active && !fusion->post_having_is_self_sum_factor &&
        fusion->post_having_agg_slot == 0 &&
        fusion->post_having_op == BinaryOperation::kGreaterThan &&
        fusion->post_having_constant_or_factor >= 1.0));
  const bool compact_split_key_only =
      compact_counts && compact_split_key_eligible;

  const auto compact_count_passes = [&](int64_t count) {
    return layout.compact_integer_threshold
               ? count > *layout.compact_integer_threshold
               : static_cast<double>(count) >
                     fusion->post_having_constant_or_factor;
  };
  const auto compact_key_equal = [&](const CompactCountGroup& a,
                                     const CompactCountGroup& b) {
    Value ax_tmp;
    Value ay_tmp;
    return std::ranges::all_of(key_shapes, [&](const ProgramShape& shape) {
      const auto column = static_cast<slot_t>(shape.direct_column);
      const auto& x =
          column < left_width
              ? get_pair_cell(a.left, nullptr, column)
              : read_right_cell(a.right_idx, column - left_width, &ax_tmp);
      const auto& y =
          column < left_width
              ? get_pair_cell(b.left, nullptr, column)
              : read_right_cell(b.right_idx, column - left_width, &ay_tmp);
      return x.IsCaseInsensitive() || y.IsCaseInsensitive()
                 ? DistinctValueEqual{}(CanonicalDistinctValue(x),
                                        CanonicalDistinctValue(y))
                 : DistinctValueEqual{}(x, y);
    });
  };
  if (debug_fusion && compact_counts) {
    std::cerr << "[fusion] compact counts rows=" << left.rows.size() << '\n';
  }

  const auto read_pair_cell = [&](const Row* left_row, uint32_t r_idx,
                                  slot_t col,
                                  Value* col_scratch) -> const Value& {
    if (col < left_width) {
      return left_row != nullptr ? left_row->values_[col] : kNullCell;
    }
    return read_right_cell(r_idx, col - left_width, col_scratch);
  };
  const auto resolve_group_index = [&](FusedWorkerLocal& local, Row& scratch,
                                       const Row* left_row, uint32_t r_idx,
                                       bool masked, size_t* cached_group,
                                       size_t* out_group_index) -> Status {
    const Row* right_row = right_row_ptr(r_idx);
    size_t group_index = 0;
    if (cached_group != nullptr && *cached_group != kNoCachedGroup) {
      group_index = *cached_group;
    } else {
      Value fast_key_tmp;
      const Value* fast_key = nullptr;
      if (int_group_key) {
        const Value& cell =
            read_pair_cell(left_row, r_idx, int_group_slot, &fast_key_tmp);
        if (!cell.IsNull() && cell.type == int_group_type &&
            !cell.IsUnsigned()) {
          fast_key = &cell;
        }
      }
      size_t* fast_slot = nullptr;
      uint32_t* fast_lut_slot = nullptr;
      bool new_group = false;
      uint64_t computed_key_hash = 0;
      bool has_computed_key_hash = false;
      if (fast_key != nullptr) {
        bool inserted = false;
        fast_slot =
            local.int_offsets.TryEmplace(fast_key->value.int_value, &inserted);
        if (!inserted) {
          group_index = *fast_slot;
        } else {
          if (!all_direct_group_keys) {
            local.key_scratch.assign(1, *fast_key);
          }
          new_group = true;
        }
      } else if (layout.use_split_key_gid) {
        const uint32_t r_subgid =
            !layout.split_uses_right_subgid
                ? 0U
                : (r_idx == kInvalidRightIdx
                       ? std::numeric_limits<uint32_t>::max()
                       : layout.split_right_subgid_ptr[r_idx]);
        const bool pk_ok = !layout.split_uses_pk_subgid || local.have_cur_pk_subgids;
        int64_t l_val = 0;
        bool l_ok = !layout.split_has_left_probe;
        if (layout.split_has_left_probe) {
          const ProgramShape& l_shape = key_shapes[layout.split_left_k];
          if (left_row != nullptr && l_shape.direct_column >= 0) {
            const Value& lv =
                left_row->values_[static_cast<size_t>(l_shape.direct_column)];
            if (!lv.IsNull() &&
                (lv.type == ValueType::kInt64 || lv.type == ValueType::kDate) &&
                !lv.IsUnsigned() &&
                lv.value.int_value >=
                    static_cast<int64_t>(std::numeric_limits<int32_t>::min()) &&
                lv.value.int_value <=
                    static_cast<int64_t>(std::numeric_limits<int32_t>::max()))
                [[likely]] {
              l_val = lv.value.int_value;
              l_ok = true;
            }
          } else if (left_row != nullptr && l_shape.extract_date_col >= 0) {
            const Value& lv =
                left_row
                    ->values_[static_cast<size_t>(l_shape.extract_date_col)];
            if (!lv.IsNull() && lv.type == ValueType::kDate &&
                lv.value.int_value >= kDateMinDays &&
                lv.value.int_value <= kDateMaxDays) [[likely]] {
              if (l_shape.extract_date_part == 0 && lv.value.int_value >= 0 &&
                  std::cmp_less(lv.value.int_value, kFastYearLut.size()))
                  [[likely]] {
                l_val = kFastYearLut[static_cast<size_t>(lv.value.int_value)];
              } else {
                const std::chrono::year_month_day ymd{std::chrono::sys_days{
                    std::chrono::days{static_cast<int>(lv.value.int_value)}}};
                if (l_shape.extract_date_part == 0) {
                  const int64_t year = static_cast<int>(ymd.year());
                  l_val = year;
                } else if (l_shape.extract_date_part == 1) {
                  l_val =
                      static_cast<int64_t>(static_cast<unsigned>(ymd.month()));
                } else {
                  l_val =
                      static_cast<int64_t>(static_cast<unsigned>(ymd.day()));
                }
              }
              l_ok = true;
            }
          }
        }
        if (r_subgid != std::numeric_limits<uint32_t>::max() && pk_ok && l_ok)
            [[likely]] {
          if (layout.use_fast_2key_year_lut && r_subgid < 64 && l_val >= 1900 &&
              l_val < 2028) [[likely]] {
            if (!local.local_2key_gid_init) [[unlikely]] {
              local.local_2key_gid.fill(std::numeric_limits<uint32_t>::max());
              local.local_2key_gid_init = true;
            }
            const size_t lut_idx = (static_cast<size_t>(r_subgid) << 7) |
                                   static_cast<size_t>(l_val - 1900);
            const uint32_t cached_gid = local.local_2key_gid[lut_idx];
            if (cached_gid != std::numeric_limits<uint32_t>::max()) [[likely]] {
              *out_group_index = cached_gid;
              return Status::kSuccess;
            }
            fast_lut_slot = &local.local_2key_gid[lut_idx];
          }
          uint64_t packed_u64 = 0;
          if (layout.split_uses_right_subgid) {
            packed_u64 |= static_cast<uint64_t>(r_subgid) << layout.split_right_shift;
          }
          if (layout.split_uses_pk_subgid) {
            for (size_t gi = 0; gi < def_pk_groups.size(); ++gi) {
              if (def_pk_groups[gi].use_subgid) {
                packed_u64 |= static_cast<uint64_t>(local.cur_pk_subgids[gi])
                              << def_pk_groups[gi].subgid_shift;
              }
            }
          }
          if (layout.split_has_left_probe) {
            packed_u64 |= static_cast<uint64_t>(
                static_cast<uint32_t>(static_cast<int32_t>(l_val)));
          }
          const auto packed_gk = std::bit_cast<int64_t>(packed_u64);
          bool inserted = false;
          fast_slot = local.int_offsets.TryEmplace(packed_gk, &inserted);
          if (!inserted) [[likely]] {
            if (fast_lut_slot != nullptr) {
              *fast_lut_slot = static_cast<uint32_t>(*fast_slot);
            }
            *out_group_index = *fast_slot;
            return Status::kSuccess;
          }
          if (!all_direct_group_keys) {
            local.key_scratch.resize(key_programs.size());
            Value rv_tmp;
            for (size_t k = 0; k < key_shapes.size(); ++k) {
              if (layout.split_has_left_probe && k == layout.split_left_k) {
                const ProgramShape& l_shape = key_shapes[layout.split_left_k];
                if (l_shape.direct_column >= 0 &&
                    left_row->values_[static_cast<size_t>(
                                          l_shape.direct_column)]
                            .type == ValueType::kDate) {
                  local.key_scratch[layout.split_left_k] = Value::DateFromDays(l_val);
                } else {
                  local.key_scratch[layout.split_left_k] = Value(l_val);
                }
              } else if (std::cmp_greater_equal(key_shapes[k].direct_column,
                                                left_width)) {
                const auto column =
                    static_cast<size_t>(key_shapes[k].direct_column) -
                    left_width;
                local.key_scratch[k] = CanonicalDistinctValue(
                    read_right_cell(r_idx, column, &rv_tmp));
              } else {
                local.key_scratch[k] = CanonicalDistinctValue(
                    left_row->values_[static_cast<size_t>(
                        key_shapes[k].direct_column)]);
              }
            }
          }
          computed_key_hash = packed_u64;
          has_computed_key_hash = true;
          new_group = true;
        } else {
          local.key_scratch.resize(key_programs.size());
          for (size_t k = 0; k < key_programs.size(); ++k) {
            // A NULL or wide probe key cannot use the packed key. The build
            // side may still be columnar, so its absent Row is not SQL NULL.
            if (key_shapes[k].direct_column >= 0) {
              Value cell_scratch;
              local.key_scratch[k] = CanonicalDistinctValue(read_pair_cell(
                  left_row, r_idx,
                  static_cast<slot_t>(key_shapes[k].direct_column),
                  &cell_scratch));
            } else {
              ASSIGN_OR_RETURN(Value, kv,
                               eval_pair_program(key_programs[k], key_shapes[k],
                                                 scratch, left_row, right_row));
              local.key_scratch[k] = CanonicalDistinctValue(kv);
            }
          }
          if (auto found = local.offsets.find(local.key_scratch);
              found != local.offsets.end()) {
            group_index = found->second;
          } else {
            new_group = true;
          }
        }
      } else if (layout.use_left_packed_key && left_row != nullptr &&
                 (!layout.left_pack_uses_right || r_idx != kInvalidRightIdx) &&
                 (!layout.left_pack_uses_dict ||
                  (left_row >= left.rows.data() &&
                   left_row < left.rows.data() + left.rows.size()))) {
        uint64_t packed_u64 = 0;
        if (layout.left_pack_uses_right) {
          const uint32_t r_subgid = layout.split_right_subgid_ptr[r_idx];
          packed_u64 |= static_cast<uint64_t>(r_subgid) << layout.split_right_shift;
        }
        if (layout.left_pack_uses_dict) {
          const auto li = static_cast<size_t>(left_row - left.rows.data());
          packed_u64 |= static_cast<uint64_t>(layout.left_pack_dict_subgid[li])
                        << layout.left_pack_dict_shift;
        }
        for (const LeftPackedIntCol& lpi : layout.left_pack_ints) {
          const Value& v = left_row->values_[lpi.col];
          const uint64_t code =
              v.IsNull()
                  ? lpi.null_code
                  : static_cast<uint64_t>(v.value.int_value - lpi.min_val);
          packed_u64 |= code << lpi.shift;
        }
        bool inserted = false;
        fast_slot = local.int_offsets.TryEmplace(
            std::bit_cast<int64_t>(packed_u64), &inserted);
        if (!inserted) [[likely]] {
          *out_group_index = *fast_slot;
          return Status::kSuccess;
        }
        computed_key_hash = packed_u64;
        has_computed_key_hash = true;
        new_group = true;
      } else if (layout.packed_2x32_group_key) {
        Value v0_tmp;
        Value v1_tmp;
        const Value& v0 =
            read_pair_cell(left_row, r_idx, layout.packed_2x32_slot0, &v0_tmp);
        const Value& v1 =
            read_pair_cell(left_row, r_idx, layout.packed_2x32_slot1, &v1_tmp);
        const auto fits_i32 = [](const Value& v, ValueType expected) noexcept {
          return !v.IsNull() && v.type == expected && !v.IsUnsigned() &&
                 v.value.int_value >=
                     static_cast<int64_t>(
                         std::numeric_limits<int32_t>::min()) &&
                 v.value.int_value <=
                     static_cast<int64_t>(std::numeric_limits<int32_t>::max());
        };
        if (fits_i32(v0, layout.packed_2x32_type0) && fits_i32(v1, layout.packed_2x32_type1))
            [[likely]] {
          const uint64_t u0 =
              static_cast<uint32_t>(static_cast<int32_t>(v0.value.int_value));
          const uint64_t u1 =
              static_cast<uint32_t>(static_cast<int32_t>(v1.value.int_value));
          const uint64_t packed_u64 = (u0 << 32) | u1;
          bool inserted = false;
          fast_slot = local.int_offsets.TryEmplace(
              std::bit_cast<int64_t>(packed_u64), &inserted);
          if (!inserted) [[likely]] {
            *out_group_index = *fast_slot;
            return Status::kSuccess;
          }
          computed_key_hash = packed_u64;
          has_computed_key_hash = true;
          new_group = true;
        } else {
          if (local.key_scratch.size() != 2) {
            local.key_scratch.resize(2);
          }
          local.key_scratch[0] = v0;
          local.key_scratch[1] = v1;
          if (auto found = local.offsets.find(local.key_scratch);
              found != local.offsets.end()) {
            group_index = found->second;
          } else {
            new_group = true;
          }
        }
      } else if (!key_programs.empty()) {
        uint64_t key_hash = 0x9e3779b97f4a7c15ULL;
        const bool fast_direct_int_hash =
            hash_int_keys_only && all_direct_group_keys && !int_group_key;
        if (fast_direct_int_hash) {
          Value kv_tmp;
          for (const size_t ik : int_key_indices) {
            const Value& kv = read_pair_cell(
                left_row, r_idx,
                static_cast<slot_t>(key_shapes[ik].direct_column), &kv_tmp);
            const uint64_t h = kv.IsNull()
                                   ? 0x517cc1b727220a95ULL
                                   : static_cast<uint64_t>(kv.value.int_value) *
                                         0x9e3779b97f4a7c15ULL;
            key_hash = (key_hash << 7) | (key_hash >> 57);
            key_hash ^= h ^ (h >> 32);
            key_hash *= 0x100000001b3ULL;
          }
        } else {
          if (local.key_scratch.size() != key_programs.size()) {
            local.key_scratch.resize(key_programs.size());
          }
          if (local.key_ptrs.size() != key_programs.size()) {
            local.key_ptrs.resize(key_programs.size());
          }
          Value extract_tmp;
          for (size_t key_index = 0; key_index < key_programs.size();
               ++key_index) {
            const Value* kp = nullptr;
            if (key_shapes[key_index].direct_column >= 0) {
              const Value& raw = read_pair_cell(
                  left_row, r_idx,
                  static_cast<slot_t>(key_shapes[key_index].direct_column),
                  &local.key_scratch[key_index]);
              if (raw.IsCaseInsensitive() ||
                  (raw.type == ValueType::kDouble &&
                   (std::isnan(raw.value.double_value) ||
                    raw.value.double_value == 0.0))) [[unlikely]] {
                local.key_scratch[key_index] = CanonicalDistinctValue(raw);
                kp = &local.key_scratch[key_index];
              } else {
                kp = &raw;
              }
            } else if (key_shapes[key_index].extract_date_col >= 0) {
              const Value& c = read_pair_cell(
                  left_row, r_idx,
                  static_cast<slot_t>(key_shapes[key_index].extract_date_col),
                  &extract_tmp);
              if (!c.IsNull() && c.type == ValueType::kDate &&
                  c.value.int_value >= kDateMinDays &&
                  c.value.int_value <= kDateMaxDays) [[likely]] {
                int64_t extracted = 0;
                if (key_shapes[key_index].extract_date_part == 0 &&
                    c.value.int_value >= 0 &&
                    std::cmp_less(c.value.int_value, kFastYearLut.size()))
                    [[likely]] {
                  extracted =
                      kFastYearLut[static_cast<size_t>(c.value.int_value)];
                } else {
                  const std::chrono::year_month_day ymd{std::chrono::sys_days{
                      std::chrono::days{static_cast<int>(c.value.int_value)}}};
                  if (key_shapes[key_index].extract_date_part == 0) {
                    const int64_t year = static_cast<int>(ymd.year());
                    extracted = year;
                  } else if (key_shapes[key_index].extract_date_part == 1) {
                    extracted = static_cast<int64_t>(
                        static_cast<unsigned>(ymd.month()));
                  } else {
                    extracted =
                        static_cast<int64_t>(static_cast<unsigned>(ymd.day()));
                  }
                }
                local.key_scratch[key_index] = Value(extracted);
                kp = &local.key_scratch[key_index];
              } else {
                ASSIGN_OR_RETURN(
                    Value, key_value,
                    eval_pair_program(key_programs[key_index],
                                      key_shapes[key_index], scratch, left_row,
                                      right_row));
                local.key_scratch[key_index] =
                    CanonicalDistinctValue(key_value);
                kp = &local.key_scratch[key_index];
              }
            } else {
              ASSIGN_OR_RETURN(Value, key_value,
                               eval_pair_program(key_programs[key_index],
                                                 key_shapes[key_index], scratch,
                                                 left_row, right_row));
              local.key_scratch[key_index] = CanonicalDistinctValue(key_value);
              kp = &local.key_scratch[key_index];
            }
            local.key_ptrs[key_index] = kp;
            if (!hash_int_keys_only) {
              key_hash = (key_hash << 7) | (key_hash >> 57);
              uint64_t vh = 0;
              if (!kp->IsNull() &&
                  (kp->type == ValueType::kInt64 ||
                   kp->type == ValueType::kDate) &&
                  !kp->IsUnsigned()) [[likely]] {
                vh = static_cast<uint64_t>(kp->value.int_value) *
                     0x9e3779b97f4a7c15ULL;
                vh ^= vh >> 32;
              } else {
                // Shared dimension strings already retain their canonical hash.
                // Reuse it instead of hashing the bytes for every fact match.
                vh = std::hash<Value>()(*kp);
              }
              key_hash ^= vh;
              key_hash *= 0x100000001b3ULL;
            }
          }
          if (hash_int_keys_only) {
            for (const size_t ik : int_key_indices) {
              const Value& kv = *local.key_ptrs[ik];
              const uint64_t h =
                  kv.IsNull() ? 0x517cc1b727220a95ULL
                              : static_cast<uint64_t>(kv.value.int_value) *
                                    0x9e3779b97f4a7c15ULL;
              key_hash = (key_hash << 7) | (key_hash >> 57);
              key_hash ^= h ^ (h >> 32);
              key_hash *= 0x100000001b3ULL;
            }
          }
        }
        computed_key_hash = key_hash;
        has_computed_key_hash = true;
        if (!int_group_key) {
          bool hash_inserted = false;
          size_t* hash_slot = local.int_offsets.TryEmplace(
              static_cast<int64_t>(key_hash), &hash_inserted);
          if (!hash_inserted) {
            const size_t cand_group = *hash_slot;
            FusedGroup& cand_grp = local.groups[cand_group];
            const bool lazy_cand =
                cand_grp.key.values_.empty() && all_direct_group_keys;
            bool match = lazy_cand ||
                         (cand_grp.key.values_.size() == key_programs.size());
            Value cand_col_tmp;
            Value probe_col_tmp;
            auto cand_key_cell = [&](size_t k) -> const Value& {
              if (!lazy_cand) {
                return cand_grp.key.values_[k];
              }
              return read_group_pair_cell(
                  cand_grp, static_cast<size_t>(key_shapes[k].direct_column),
                  &cand_col_tmp);
            };
            auto probe_key_cell = [&](size_t k) -> const Value& {
              if (fast_direct_int_hash) {
                return read_pair_cell(
                    left_row, r_idx,
                    static_cast<slot_t>(key_shapes[k].direct_column),
                    &probe_col_tmp);
              }
              return *local.key_ptrs[k];
            };
            if (match && !int_key_indices.empty()) {
              for (const size_t ik : int_key_indices) {
                const Value& x = probe_key_cell(ik);
                const Value& y = cand_key_cell(ik);
                if (x.type != y.type ||
                    (!x.IsNull() && x.value.int_value != y.value.int_value)) {
                  match = false;
                  break;
                }
              }
            }
            if (match && int_key_indices.size() < key_programs.size()) {
              for (size_t k = 0; k < key_programs.size(); ++k) {
                const Value& x = probe_key_cell(k);
                const Value& y = cand_key_cell(k);
                if (&x == &y) {
                  continue;
                }
                if (x.type != y.type) {
                  match = false;
                  break;
                }
                if (x.type == ValueType::kDouble) {
                  const double p = x.value.double_value;
                  const double q = y.value.double_value;
                  if (std::isnan(p) && std::isnan(q)) {
                    continue;
                  }
                  if (!(p == q)) {
                    match = false;
                    break;
                  }
                  continue;
                }
                if (!(x == y)) {
                  match = false;
                  break;
                }
              }
            }
            if (match) [[likely]] {
              group_index = cand_group;
            } else {
              ensure_group_key(cand_grp);
              if (local.key_scratch.size() != key_programs.size()) {
                local.key_scratch.resize(key_programs.size());
              }
              Value p_tmp;
              for (size_t k = 0; k < key_programs.size(); ++k) {
                if (fast_direct_int_hash) {
                  local.key_scratch[k] = CanonicalDistinctValue(read_pair_cell(
                      left_row, r_idx,
                      static_cast<slot_t>(key_shapes[k].direct_column),
                      &p_tmp));
                } else if (local.key_ptrs[k] != &local.key_scratch[k]) {
                  local.key_scratch[k] = *local.key_ptrs[k];
                }
              }
              if (auto found = local.offsets.find(local.key_scratch);
                  found != local.offsets.end()) {
                group_index = found->second;
              } else {
                new_group = true;
              }
            }
          } else {
            if (!all_direct_group_keys) {
              for (size_t k = 0; k < local.key_ptrs.size(); ++k) {
                if (local.key_ptrs[k] != &local.key_scratch[k]) {
                  local.key_scratch[k] = *local.key_ptrs[k];
                }
              }
            }
            new_group = true;
            fast_slot = hash_slot;
          }
        } else {
          for (size_t k = 0; k < local.key_ptrs.size(); ++k) {
            if (local.key_ptrs[k] != &local.key_scratch[k]) {
              local.key_scratch[k] = *local.key_ptrs[k];
            }
          }
          if (auto found = local.offsets.find(local.key_scratch);
              found != local.offsets.end()) {
            group_index = found->second;
          } else {
            new_group = true;
          }
        }
      } else {
        local.key_scratch.clear();
        if (auto found = local.offsets.find(local.key_scratch);
            found != local.offsets.end()) {
          group_index = found->second;
        } else {
          new_group = true;
        }
      }
      if (new_group) {
        group_index = local.groups.size();
        FusedGroup group;
        if (raw_aggregates.raw_sum_fusion) {
          local.raw_arena.Initialize(group, raw_aggregates.raw_slot_count);
        }
        if (!raw_aggregates.skip_accumulator_states) {
          group.state_offset = local.states.size();
          for (size_t i = 0; i < fusion->aggregates.size(); ++i) {
            local.states.emplace_back(fusion->aggregates[i],
                                      aggregate_shapes[i]);
          }
        }
        group.key_hash = computed_key_hash;
        group.has_key_hash = has_computed_key_hash;
        if (left.HasSpill()) {
          if (masked && left_row != nullptr) {
            for (size_t i = 0; i < left_width; ++i) {
              scratch.values_[i] = left_row->values_[i];
            }
            Value right_tmp;
            for (size_t i = 0; i < right_width; ++i) {
              scratch.values_[left_width + i] =
                  read_right_cell(r_idx, i, &right_tmp);
            }
          }
          group.representative = std::make_unique<Row>(scratch);
        } else {
          if ((deferred_left_probe != nullptr || left.HasColumnarOnly()) &&
              left_row != nullptr &&
              !((layout.packed_2x32_unpack_only || layout.split_key_unpack_only) &&
                has_computed_key_hash)) {
            local.owned_rep_left.push_back(*left_row);
            group.rep_left = &local.owned_rep_left.back();
          } else {
            group.rep_left =
                (deferred_left_probe != nullptr || left.HasColumnarOnly())
                    ? nullptr
                    : left_row;
          }
          group.rep_right_idx = r_idx;
        }
        if (fast_slot != nullptr) {
          if (!all_direct_group_keys) {
            group.key = Row(std::move(local.key_scratch));
          }
          local.groups.push_back(std::move(group));
          *fast_slot = group_index;
          if (fast_lut_slot != nullptr) {
            *fast_lut_slot = static_cast<uint32_t>(group_index);
          }
        } else {
          group.key = Row(local.key_scratch);
          local.groups.push_back(std::move(group));
          local.offsets.emplace(Row(std::move(local.key_scratch)), group_index);
        }
      }
      if (cached_group != nullptr && left_only_group_keys) {
        *cached_group = group_index;
      }
    }
    *out_group_index = group_index;
    return Status::kSuccess;
  };
  bool compact_log_allowed = false;
  const auto init_compact_shards = [&](FusedWorkerLocal& local) {
    if (local.compact_split_shards.empty()) {
      local.compact_split_shards.resize(kCompactSplitShards);
      const size_t per_shard_est =
          (effective_left_rows / (kCompactSplitShards)) + 32;
      const size_t reserve_cap = std::min<size_t>(per_shard_est, 4096);
      for (auto& sh : local.compact_split_shards) {
        sh.reserve(reserve_cap);
      }
    }
  };
  const auto record_compact_split = [&](FusedWorkerLocal& local,
                                        uint64_t packed_u64) -> Status {
    if (compact_log_allowed && !local.compact_count_log &&
        ((local.compact_count_inputs == 1024 &&
          local.compact_counts.size() >= 820) ||
         (local.compact_count_inputs == 16384 &&
          local.compact_counts.size() >= 6144))) {
      local.compact_count_log = true;
      init_compact_shards(local);
    }
    ++local.compact_count_inputs;
    if (local.compact_count_log) {
      init_compact_shards(local);
      uint64_t h = packed_u64 + 0x9e3779b97f4a7c15ULL;
      h = (h ^ (h >> 30U)) * 0xbf58476d1ce4e5b9ULL;
      h = (h ^ (h >> 27U)) * 0x94d049bb133111ebULL;
      h ^= h >> 31U;
      local.compact_split_shards[h & (kCompactSplitShards - 1)].push_back(
          packed_u64);
      return Status::kSuccess;
    }
    bool inserted = false;
    size_t* slot = local.int_offsets.TryEmplace(
        std::bit_cast<int64_t>(packed_u64), &inserted);
    if (!inserted) {
      ++local.compact_counts[*slot].count;
      return Status::kSuccess;
    }
    CompactCountGroup probe;
    probe.hash = packed_u64;
    probe.count = 1;
    *slot = local.compact_counts.size();
    local.compact_counts.push_back(probe);
    return Status::kSuccess;
  };
  const auto accumulate_into = [&](FusedWorkerLocal& local, Row& scratch,
                                   const Row* left_row, uint32_t r_idx,
                                   bool masked,
                                   size_t* cached_group = nullptr) -> Status {
    const Row* right_row = right_row_ptr(r_idx);
    if (compact_counts) {
      if (compact_split_key_only && r_idx != kInvalidRightIdx) {
        uint64_t packed_u64 = 0;
        bool pk_ok = true;
        if (layout.split_uses_pk_subgid) {
          pk_ok = local.have_cur_pk_subgids;
          for (size_t gi = 0; pk_ok && gi < def_pk_groups.size(); ++gi) {
            if (def_pk_groups[gi].use_subgid) {
              if (local.cur_pk_subgids[gi] ==
                  std::numeric_limits<uint32_t>::max()) [[unlikely]] {
                pk_ok = false;
              } else {
                packed_u64 |= static_cast<uint64_t>(local.cur_pk_subgids[gi])
                              << def_pk_groups[gi].subgid_shift;
              }
            }
          }
        }
        const uint32_t r_subgid =
            !layout.split_uses_right_subgid ? 0U : layout.split_right_subgid_ptr[r_idx];
        if (pk_ok && r_subgid != std::numeric_limits<uint32_t>::max()) {
          if (layout.split_uses_right_subgid) {
            packed_u64 |= static_cast<uint64_t>(r_subgid) << layout.split_right_shift;
          }
          if (compact_raw_left_key) {
            Value scratch_key;
            const auto& key =
                read_pair_cell(left_row, r_idx, *layout.split_left_slot, &scratch_key);
            if (key.IsNull() || key.type != layout.split_left_type ||
                key.IsUnsigned() ||
                !std::in_range<int32_t>(key.value.int_value)) {
              return StatusError(StatusCode::kCorrupt,
                                 "packed COUNT key differs from scan metadata");
            }
            packed_u64 |= static_cast<uint32_t>(
                static_cast<int32_t>(key.value.int_value));
          }
          return record_compact_split(local, packed_u64);
        }
      }
      CompactCountGroup probe;
      probe.left = left_row;
      probe.right_idx = r_idx;
      bool packed = false;
      if (layout.use_split_key_gid && layout.split_has_left_probe && !layout.split_uses_pk_subgid &&
          layout.split_uses_right_subgid &&
          key_shapes[layout.split_left_k].direct_column >= 0 && left_row != nullptr &&
          r_idx != kInvalidRightIdx) {
        const auto& value = (*left_row)[static_cast<size_t>(
            key_shapes[layout.split_left_k].direct_column)];
        if (!value.IsNull() && !value.IsUnsigned() &&
            (value.type == ValueType::kInt64 ||
             value.type == ValueType::kDate) &&
            std::in_range<int32_t>(value.value.int_value)) {
          const uint32_t dimension = layout.split_right_subgid_ptr[r_idx];
          if (dimension != std::numeric_limits<uint32_t>::max()) {
            probe.hash = (static_cast<uint64_t>(dimension) << 32U) |
                         static_cast<uint32_t>(value.value.int_value);
            packed = true;
          }
        }
      }
      if (!packed) {
        probe.hash = 0x9e3779b97f4a7c15ULL;
        Value v_tmp;
        for (const auto& shape : key_shapes) {
          const auto& value =
              read_pair_cell(left_row, r_idx,
                             static_cast<slot_t>(shape.direct_column), &v_tmp);
          probe.hash = std::rotl(probe.hash, 7);
          probe.hash ^= HashFusedKeyValue(value);
          probe.hash *= 0x100000001b3ULL;
        }
      }
      // When nearly every pair is a new group, local hashing barely reduces
      // the transfer. Keep the sampled partials, then append records for the
      // owning merge partition to fold exactly once. The sample only chooses
      // an algorithm; full key equality still decides every final group.
      if (compact_log_allowed && local.compact_count_inputs == 1024) {
        local.compact_count_log = local.compact_counts.size() >= 820;
      }
      ++local.compact_count_inputs;
      if (local.compact_count_log) {
        probe.count = 1;
        local.compact_counts.push_back(probe);
        return Status::kSuccess;
      }
      bool inserted = false;
      size_t* slot = local.int_offsets.TryEmplace(
          std::bit_cast<int64_t>(probe.hash), &inserted);
      if (!inserted) {
        for (size_t index = *slot; index != SIZE_MAX;
             index = local.compact_counts[index].next) {
          if (compact_key_equal(local.compact_counts[index], probe)) {
            ++local.compact_counts[index].count;
            return Status::kSuccess;
          }
        }
        probe.next = *slot;
      }
      probe.count = 1;
      *slot = local.compact_counts.size();
      local.compact_counts.push_back(probe);
      return Status::kSuccess;
    }
    size_t group_index = 0;
    RETURN_IF_FAIL(resolve_group_index(local, scratch, left_row, r_idx, masked,
                                       cached_group, &group_index));
    FusedGroup& group = local.groups[group_index];
    if (raw_aggregates.count_star_only) {
      group.raw_count += 1;
      return Status::kSuccess;
    }
    if (raw_aggregates.count_nonnull_only) {
      // COUNT(right column): unmatched LEFT rows and NULL cells contribute 0.
      // COUNT(left NOT NULL column) is unconditional instead: the left value
      // is still present for a null-extended row, so that row counts too.
      if (raw_aggregates.unconditional_matched_count ||
          (r_idx != kInvalidRightIdx &&
           (!raw_aggregates.count_right_column.has_value() ||
            !right_cell_is_null(r_idx, *raw_aggregates.count_right_column)))) {
        group.raw_count += 1;
      }
      return Status::kSuccess;
    }
    if (raw_aggregates.raw_sum_fusion) {
      Value v_tmp;
      for (size_t s = 0; s < raw_aggregates.raw_slot_count; ++s) {
        const size_t src = raw_aggregates.raw_slot_origin[s];
        if (case_always_null(src, right_row)) {
          continue;
        }
        switch (raw_aggregates.slot_agg_kinds[s]) {
          case RawAggKind::kCountStar:
            group.RawCounts()[s] += 1;
            break;
          case RawAggKind::kConstantExtremum:
          case RawAggKind::kCountValue: {
            if (child_shapes[src].direct_column >= 0) {
              const Value& v = read_pair_cell(
                  left_row, r_idx,
                  static_cast<slot_t>(child_shapes[src].direct_column), &v_tmp);
              if (!v.IsNull()) {
                group.RawCounts()[s] += 1;
              }
            } else {
              ASSIGN_OR_RETURN(
                  Value, child,
                  eval_pair_program(child_programs[src], child_shapes[src],
                                    scratch, left_row, right_row));
              if (!child.IsNull()) {
                group.RawCounts()[s] += 1;
              }
            }
            break;
          }
          case RawAggKind::kSumInt64: {
            int64_t iv = 0;
            bool is_null = false;
            if (child_shapes[src].direct_column >= 0) {
              const Value& v = read_pair_cell(
                  left_row, r_idx,
                  static_cast<slot_t>(child_shapes[src].direct_column), &v_tmp);
              is_null = v.IsNull();
              if (!is_null) {
                iv = v.value.int_value;
              }
            } else if (child_shapes[src].case_cond !=
                           ProgramShape::CaseCondKind::kNone &&
                       (child_shapes[src].case_val ==
                            ProgramShape::CaseValKind::kIntConst1 ||
                        child_shapes[src].case_val ==
                            ProgramShape::CaseValKind::kDirectInt64Col)) {
              bool cond_true = false;
              if (cached_case_cond(src, left_row, right_row, &cond_true))
                  [[likely]] {
                if (!cond_true) {
                  is_null = child_shapes[src].case_else_null;
                  iv = 0;
                } else if (child_shapes[src].case_val ==
                           ProgramShape::CaseValKind::kIntConst1) {
                  iv = 1;
                } else {
                  const Value& v = read_pair_cell(
                      left_row, r_idx,
                      static_cast<slot_t>(child_shapes[src].case_val_col0),
                      &v_tmp);
                  is_null = v.IsNull();
                  if (!is_null) {
                    iv = v.value.int_value;
                  }
                }
              } else {
                ASSIGN_OR_RETURN(
                    Value, child,
                    eval_pair_program(child_programs[src], child_shapes[src],
                                      scratch, left_row, right_row));
                is_null = child.IsNull();
                if (!is_null) {
                  iv = child.value.int_value;
                }
              }
            } else {
              ASSIGN_OR_RETURN(
                  Value, child,
                  eval_pair_program(child_programs[src], child_shapes[src],
                                    scratch, left_row, right_row));
              is_null = child.IsNull();
              if (!is_null) {
                iv = child.value.int_value;
              }
            }
            if (!is_null) {
              int64_t next = 0;
              if (__builtin_add_overflow(group.RawInts()[s], iv, &next)) {
                group.RawInfFlags()[s] = 1;
              } else {
                group.RawInts()[s] = next;
              }
              group.RawCounts()[s] += 1;
            }
            break;
          }
          case RawAggKind::kSumDouble:
          case RawAggKind::kAvgDouble: {
            double dv = 0.0;
            bool is_null = false;
            RETURN_IF_FAIL(eval_pair_double(src, scratch, left_row, right_row,
                                            &dv, &is_null));
            if (!is_null) {
              const double next = group.RawDoubles()[s] + dv;
              if (std::isinf(next) && !std::isinf(group.RawDoubles()[s]) &&
                  !std::isinf(dv)) {
                group.RawInfFlags()[s] = 1;
              }
              group.RawDoubles()[s] = next;
              group.RawCounts()[s] += 1;
            }
            break;
          }
          case RawAggKind::kStddevSamp:
          case RawAggKind::kStddevPop:
          case RawAggKind::kVarSamp:
          case RawAggKind::kVarPop: {
            platform::ExtendedFloat x = 0;
            bool is_null = false;
            RETURN_IF_FAIL(
                eval_pair_ext(src, scratch, left_row, right_row, &x, &is_null));
            if (!is_null) {
              group.RawStatX()[s] += x;
              group.RawStatXX()[s] += x * x;
              group.RawCounts()[s] += 1;
            }
            break;
          }
          case RawAggKind::kCountDistinctInt64: {
            int64_t iv = 0;
            bool is_null = false;
            if (child_shapes[src].direct_column >= 0) {
              const Value& v = read_pair_cell(
                  left_row, r_idx,
                  static_cast<slot_t>(child_shapes[src].direct_column), &v_tmp);
              is_null = v.IsNull();
              if (!is_null) {
                iv = v.value.int_value;
              }
            } else {
              ASSIGN_OR_RETURN(
                  Value, child,
                  eval_pair_program(child_programs[src], child_shapes[src],
                                    scratch, left_row, right_row));
              is_null = child.IsNull();
              if (!is_null) {
                iv = child.value.int_value;
              }
            }
            if (!is_null) {
              if (!group.raw_int_set) {
                group.raw_int_set = std::make_unique<FlatIntKeyMap<uint8_t>>();
              }
              group.raw_int_set->TryEmplace(iv);
            }
            break;
          }
          case RawAggKind::kNone:
            break;
        }
      }
      return Status::kSuccess;
    }
    Value v_tmp;
    for (size_t i = 0; i < fusion->aggregates.size(); ++i) {
      AggregateAccumulator& accumulator = local.states[group.state_offset + i];
      if (fusion->count_star[i]) {
        RETURN_IF_FAIL(accumulator.Add(Value(1)));
        continue;
      }
      if (child_shapes[i].direct_column >= 0) {
        RETURN_IF_FAIL(accumulator.Add(read_pair_cell(
            left_row, r_idx, static_cast<slot_t>(child_shapes[i].direct_column),
            &v_tmp)));
        continue;
      }
      ASSIGN_OR_RETURN(Value, child,
                       eval_pair_program(child_programs[i], child_shapes[i],
                                         scratch, left_row, right_row));
      RETURN_IF_FAIL(accumulator.Add(child));
    }
    return Status::kSuccess;
  };
  // Fills the pair scratch from the two sides.  `r_idx == kInvalidRightIdx`
  // pads the right half with NULLs (unmatched left rows).  Masked fills touch
  // only the cells the compiled programs load.
  const auto fill_scratch = [&](Row& scratch, const Row& left_row,
                                uint32_t r_idx) {
    if (mask_scratch) {
      Value tmp;
      for (const slot_t cell : scratch_cells) {
        scratch.values_[cell] =
            cell < left_width ? left_row.values_[cell]
                              : read_right_cell(r_idx, cell - left_width, &tmp);
      }
      return;
    }
    for (size_t i = 0; i < left_width; ++i) {
      scratch.values_[i] = left_row.values_[i];
    }
    Value tmp;
    for (size_t i = 0; i < right_width; ++i) {
      scratch.values_[left_width + i] = read_right_cell(r_idx, i, &tmp);
    }
  };
  // Returns true when the pair passed every residual and was accumulated.
  // A residual-rejected pair is NOT a match: LEFT semantics must still
  // null-extend the outer row when every bucket pair for it was rejected.
  // Direct raw aggregates read the probe and build rows themselves. Copying
  // every joined cell into the scratch row is then pure overhead.
  // Lean proof: lean/TinylambOpt/PredicateAndRelationalLowering.lean
  // (precomputed_build_disjunct_residual_sound)
  // When a single inner-join residual is a disjunction `L(left) OR R(right)`
  // of non-throwing single-side terms, precompute `R(right)` once per build
  // row and evaluate `L(left)` directly on `left_row` only when `R(right)` is
  // false, avoiding per-pair scratch VARCHAR copies when keys/aggs are direct.
  bool use_split_disj_residual = false;
  std::optional<BytecodeProgram> left_disj_program;
  ProgramShape left_disj_shape;
  std::vector<uint8_t> precomputed_right_disj;
  const bool non_residual_scratch_unused =
      all_direct_group_keys && raw_aggregates.raw_sum_fusion &&
      std::ranges::all_of(child_shapes, [](const ProgramShape& shape) {
        return shape.direct_column >= 0;
      });
  if (!hoist_disabled && join_type == JoinType::kInner &&
      !anti_unmatched_only && residual_programs.size() == 1 &&
      residuals.size() == 1 && right_build_count() > 0 &&
      right_build_count() <= 524288) {
    const std::vector<Expression> disj = SplitDisjuncts(residuals[0]);
    if (disj.size() >= 2) {
      std::vector<Expression> left_parts;
      std::vector<Expression> right_parts;
      bool split_ok = true;
      for (const Expression& d : disj) {
        if (!d || !ExpressionCannotThrowForPlanner(d)) {
          split_ok = false;
          break;
        }
        auto dprog = BytecodeCompiler::CompileRowCached(
            d, result.schema, BytecodeCompiler::Context::kFilter);
        if (!dprog.has_value()) {
          split_ok = false;
          break;
        }
        std::vector<slot_t> dloads;
        CollectLoadCells(*dprog, &dloads);
        if (dloads.empty()) {
          split_ok = false;
          break;
        }
        const bool all_left = std::ranges::all_of(
            dloads, [&](slot_t c) { return c < left_width; });
        const bool all_right = std::ranges::all_of(
            dloads, [&](slot_t c) { return c >= left_width; });
        if (all_left) {
          left_parts.push_back(d);
        } else if (all_right) {
          right_parts.push_back(d);
        } else {
          split_ok = false;
          break;
        }
      }
      if (split_ok && !left_parts.empty() && !right_parts.empty()) {
        auto combine_or = [](const std::vector<Expression>& parts) {
          Expression acc = parts[0];
          for (size_t i = 1; i < parts.size(); ++i) {
            acc = BinaryExpressionExp(acc, BinaryOperation::kOr, parts[i]);
          }
          return acc;
        };
        const Expression left_or = combine_or(left_parts);
        const Expression right_or = combine_or(right_parts);
        auto lprog = BytecodeCompiler::CompileRowCached(
            left_or, left.schema, BytecodeCompiler::Context::kFilter);
        auto rprog = BytecodeCompiler::CompileRowCached(
            right_or, result.schema, BytecodeCompiler::Context::kFilter);
        if (lprog.has_value() && rprog.has_value()) {
          const ProgramShape rshape = ClassifyProgram(*rprog);
          std::vector<slot_t> rcells;
          CollectLoadCells(*rprog, &rcells);
          const size_t rcnt = right_build_count();
          precomputed_right_disj.assign(rcnt, 0);
          Row rscratch;
          rscratch.values_.resize(left_width + right_width);
          Value rtmp;
          bool r_ok = true;
          for (size_t ri = 0; ri < rcnt; ++ri) {
            for (const slot_t cell : rcells) {
              rscratch.values_[cell] = read_right_cell(
                  static_cast<uint32_t>(ri), cell - left_width, &rtmp);
            }
            StatusOr<Value> rv = EvaluateProgramValue(*rprog, rshape, rscratch);
            if (!rv.HasValue()) {
              r_ok = false;
              break;
            }
            precomputed_right_disj[ri] = Truthy(rv.Value()) ? 1U : 0U;
          }
          if (r_ok) {
            left_disj_shape = ClassifyProgram(*lprog);
            left_disj_program = std::move(lprog);
            use_split_disj_residual = true;
          } else {
            precomputed_right_disj.clear();
          }
        }
      }
    }
  }
  const bool scratch_unused =
      (residual_programs.empty() || use_split_disj_residual) &&
      non_residual_scratch_unused;
  const auto process_pair =
      [&](FusedWorkerLocal& local, const Row& left_row, uint32_t r_idx,
          Row& scratch, size_t* cached_group = nullptr) -> StatusOr<bool> {
    if (anti_unmatched_only && residual_programs.empty()) {
      return true;
    }
    if (use_split_disj_residual) {
      if (precomputed_right_disj[r_idx] == 0) {
        ASSIGN_OR_RETURN(Value, lverdict,
                         EvaluateProgramValue(*left_disj_program,
                                              left_disj_shape, left_row));
        if (!Truthy(lverdict)) {
          return false;
        }
      }
      if (!scratch_unused) {
        fill_scratch(scratch, left_row, r_idx);
      }
      RETURN_IF_FAIL(accumulate_into(local, scratch, &left_row, r_idx,
                                     mask_scratch, cached_group));
      return true;
    }
    if (!scratch_unused) {
      fill_scratch(scratch, left_row, r_idx);
    }
    for (size_t i = 0; i < residual_programs.size(); ++i) {
      ASSIGN_OR_RETURN(Value, verdict,
                       EvaluateProgramValue(residual_programs[i],
                                            residual_shapes[i], scratch));
      if (!Truthy(verdict)) {
        return false;
      }
    }
    if (anti_unmatched_only) {
      // Lean proof: lean/TinylambOpt/PredicateAndRelationalLowering.lean
      //   (fused_antijoin_groupby_sound)
      // A matched pair under LEFT JOIN ... WHERE right.key IS NULL marks the
      // outer row as matched, so its NULL-extended (anti) row is suppressed.
      // No group is accumulated here; `true` just reports the match upward.
      return true;
    }
    RETURN_IF_FAIL(accumulate_into(local, scratch, &left_row, r_idx,
                                   mask_scratch, cached_group));
    return true;
  };
  const auto unmatched_left = [&](FusedWorkerLocal& local, Row& scratch,
                                  const Row& left_row) -> Status {
    fill_scratch(scratch, left_row, kInvalidRightIdx);
    return accumulate_into(local, scratch, &left_row, kInvalidRightIdx,
                           mask_scratch);
  };

  bool has_double_raw_agg = false;
  bool has_int_raw_agg = false;
  bool has_stat_raw_agg = false;
  if (raw_aggregates.raw_sum_fusion) {
    for (size_t s = 0; s < raw_aggregates.raw_slot_count && s < raw_aggregates.slot_agg_kinds.size(); ++s) {
      if (raw_aggregates.slot_agg_kinds[s] == RawAggKind::kSumDouble ||
          raw_aggregates.slot_agg_kinds[s] == RawAggKind::kAvgDouble) {
        has_double_raw_agg = true;
      } else if (is_stat_raw_kind(raw_aggregates.slot_agg_kinds[s])) {
        has_stat_raw_agg = true;
      } else if (raw_aggregates.slot_agg_kinds[s] == RawAggKind::kSumInt64) {
        has_int_raw_agg = true;
      }
    }
  }
  // Lean proof:
  // lean/TinylambOpt/ResearchProofs/FanoutWeightedBucketWorkSplit.lean (sound)
  size_t estimated_fanout_pairs = total_left;
  double estimated_avg_depth = 1.0;
  if (deferred_left_probe == nullptr && !left.HasSpill() &&
      !right_arg.HasSpill() && total_left > 0 && total_left < 96000 &&
      !singleton_right_relation && right_build_count() >= 256 &&
      (left.HasColumnarOnly() || left.rows.size() == total_left) &&
      ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_FANOUT_WORK_SPLIT") ==
          nullptr) {
    const size_t n_samples = std::min<size_t>(total_left, 64);
    const size_t r_total = right_build_count();
    const size_t r_limit = std::min<size_t>(r_total, 65536);
    const double r_scale = r_limit > 0 ? static_cast<double>(r_total) /
                                             static_cast<double>(r_limit)
                                       : 1.0;
    size_t sampled_probe = 0;
    uint64_t matched_hits = 0;
    if (integer_key && left_columns.size() == 1 && right_columns.size() == 1) {
      std::unordered_map<int64_t, uint32_t> probe_sample;
      probe_sample.reserve(n_samples * 2);
      Row col_row;
      Value col_cell;
      const auto sample_left_at = [&](size_t target_idx,
                                      int64_t* out_k) -> bool {
        if (!left.HasColumnarOnly()) {
          const Value& v = left.rows[target_idx][left_columns[0]];
          if (v.IsNull()) {
            return false;
          }
          *out_k = AddJoinKeyBias(v.value.int_value, left_key_offsets[0]);
          return true;
        }
        size_t rem = target_idx;
        for (const auto& ch : left.columnar->chunks) {
          if (rem < ch.rows) {
            ch.columns[left_columns[0]].WriteValueInto(rem, &col_cell);
            if (col_cell.IsNull()) {
              return false;
            }
            *out_k =
                AddJoinKeyBias(col_cell.value.int_value, left_key_offsets[0]);
            return true;
          }
          rem -= ch.rows;
        }
        return false;
      };
      for (size_t s = 0; s < n_samples; ++s) {
        int64_t k = 0;
        if (sample_left_at((s * total_left) / n_samples, &k)) {
          ++probe_sample[k];
          ++sampled_probe;
        }
      }
      if (sampled_probe > 0) {
        if (!right_col_only) {
          for (size_t ri = 0; ri < r_limit; ++ri) {
            const Row& r = *build_rows[(ri * r_total) / r_limit];
            const Value& v = r[right_columns[0]];
            if (v.IsNull()) {
              continue;
            }
            auto it = probe_sample.find(
                AddJoinKeyBias(v.value.int_value, right_key_offsets[0]));
            if (it != probe_sample.end()) {
              matched_hits += it->second;
            }
          }
        } else {
          for (size_t ri = 0; ri < r_limit; ++ri) {
            const Value& v =
                read_right_cell(static_cast<uint32_t>((ri * r_total) / r_limit),
                                right_columns[0], &col_cell);
            if (v.IsNull()) {
              continue;
            }
            auto it = probe_sample.find(
                AddJoinKeyBias(v.value.int_value, right_key_offsets[0]));
            if (it != probe_sample.end()) {
              matched_hits += it->second;
            }
          }
        }
      }
    } else if (!equality_keys.empty()) {
      std::unordered_map<std::string, uint32_t> probe_sample;
      probe_sample.reserve(n_samples * 2);
      Row col_left_row;
      Row col_right_row;
      col_left_row.values_.resize(left_width);
      col_right_row.values_.resize(right_width);
      const auto sample_left_row = [&](size_t target_idx) -> const Row* {
        if (!left.HasColumnarOnly()) {
          return &left.rows[target_idx];
        }
        size_t rem = target_idx;
        for (const auto& ch : left.columnar->chunks) {
          if (rem < ch.rows) {
            ch.MaterializeRowInto(rem, &col_left_row);
            return &col_left_row;
          }
          rem -= ch.rows;
        }
        return nullptr;
      };
      for (size_t s = 0; s < n_samples; ++s) {
        const Row* lr = sample_left_row((s * total_left) / n_samples);
        if (lr != nullptr && !HasNullKey(*lr, left_columns)) {
          ++probe_sample[EncodeJoinKey(*lr, left_columns, &left_key_offsets)];
          ++sampled_probe;
        }
      }
      if (sampled_probe > 0) {
        for (size_t ri = 0; ri < r_limit; ++ri) {
          const size_t actual_ri = (ri * r_total) / r_limit;
          const Row* rr = nullptr;
          if (!right_col_only) {
            rr = build_rows[actual_ri];
          } else {
            Value tmp;
            for (const slot_t rc : right_columns) {
              col_right_row.values_[rc] =
                  read_right_cell(static_cast<uint32_t>(actual_ri), rc, &tmp);
            }
            rr = &col_right_row;
          }
          if (HasNullKey(*rr, right_columns)) {
            continue;
          }
          auto it = probe_sample.find(
              EncodeJoinKey(*rr, right_columns, &right_key_offsets));
          if (it != probe_sample.end()) {
            matched_hits += it->second;
          }
        }
      }
    }
    if (sampled_probe > 0 && matched_hits > 0) {
      estimated_avg_depth =
          std::max(1.0, (static_cast<double>(matched_hits) * r_scale) /
                            static_cast<double>(sampled_probe));
      estimated_fanout_pairs = static_cast<size_t>(
          static_cast<double>(total_left) * estimated_avg_depth);
    }
  }
  const size_t workers =
      left.HasSpill()
          ? 1
          : (estimated_fanout_pairs >= 65536 && estimated_avg_depth >= 2.0
                 ? ParallelWorkerCount((estimated_fanout_pairs / 32768) + 1)
                 : ((int_group_key && int_group_slot < left_width &&
                     !bulk_raw_count && total_left < 96000)
                        ? 1
                        : ParallelWorkerCount((total_left / 16384) + 1)));
  std::vector<FusedWorkerLocal> locals(workers);
  for (FusedWorkerLocal& l : locals) {
    l.raw_arena.has_stat = has_stat_raw_agg;
    l.raw_arena.has_double = !raw_aggregates.raw_sum_fusion || has_double_raw_agg;
    l.raw_arena.has_int = !raw_aggregates.raw_sum_fusion || has_int_raw_agg;
  }
  std::shared_ptr<QueryMemoryCharge> group_capacity_charge;
  if (all_direct_group_keys && !right_only_group_keys &&
      deferred_left_probe == nullptr && !left.HasSpill() &&
      !left.HasColumnarOnly() && left.rows.size() >= 32768) {
    // Estimate high-cardinality probe keys from a small, distributed sample.
    // Only capacity depends on this estimate: collisions, repeated keys and
    // build-side multiplicity still go through the normal equality checks.
    std::array<uint64_t, 1024> hashes{};
    for (size_t sample = 0; sample < hashes.size(); ++sample) {
      const auto& row = left.rows[(sample * left.rows.size()) / hashes.size()];
      uint64_t hash = 0x9e3779b97f4a7c15ULL;
      for (const auto& shape : key_shapes) {
        const auto column = static_cast<size_t>(shape.direct_column);
        if (column < left_width) {
          hash = std::rotl(hash, 7) ^ DistinctValueHash {}(row[column]);
          hash *= 0x100000001b3ULL;
        }
      }
      hashes[sample] = hash;
    }
    std::ranges::sort(hashes);
    const auto distinct = static_cast<size_t>(
        std::unique(hashes.begin(), hashes.end()) - hashes.begin());
    if (distinct >= (hashes.size() * 4) / 5) {
      const size_t capacity =
          std::min<size_t>(262144, (left.rows.size() / workers) + 1);
      // Include open-addressing slack and the group-vector storage. Raw
      // aggregate blocks retain their existing incremental allocation path.
      const size_t bytes = workers * capacity *
                           (64 + (compact_counts ? sizeof(CompactCountGroup)
                                                 : sizeof(FusedGroup)));
      group_capacity_charge = TryReserveQueryMemory(bytes);
      if (group_capacity_charge) {
        for (auto& local : locals) {
          local.int_offsets.Reserve(capacity);
          if (compact_counts) {
            local.compact_counts.reserve(capacity);
          } else {
            local.groups.reserve(capacity);
          }
        }
      }
    }
  }
  std::vector<Row> scratch_storage(locals.size());
  for (Row& scratch : scratch_storage) {
    scratch.values_.resize(left_width + right_width);
  }
  constexpr size_t kProbeChunkRows = 8192;
  const size_t probe_chunk_rows =
      (workers > 1 && total_left > 0 && total_left < workers * kProbeChunkRows)
          ? std::max<size_t>(64, total_left / (workers * 8))
          : kProbeChunkRows;
  const size_t chunk_count =
      (total_left + probe_chunk_rows - 1) / probe_chunk_rows;
  // Lean proof: lean/TinylambOpt/PredicateAndRelationalLowering.lean
  // (packed_2x32_join_key_bijection_sound)
  // Partition ColumnarTable probe chunks into morsels so large or high-fanout
  // ColumnarTables fan out across all worker threads instead of serializing
  // on Worker 0.
  std::vector<ColProbeMorsel> col_morsels;
  if (left.HasColumnarOnly()) {
    const auto& chunks = left.columnar->chunks;
    size_t est_morsels = 0;
    for (const auto& ch : chunks) {
      est_morsels += (ch.rows + probe_chunk_rows - 1) / probe_chunk_rows;
    }
    col_morsels.reserve(est_morsels);
    for (size_t ci = 0; ci < chunks.size(); ++ci) {
      const size_t nrows = chunks[ci].rows;
      for (size_t rb = 0; rb < nrows; rb += probe_chunk_rows) {
        col_morsels.push_back(ColProbeMorsel{
            .ci = static_cast<uint32_t>(ci),
            .r_begin = static_cast<uint32_t>(rb),
            .r_end =
                static_cast<uint32_t>(std::min(nrows, rb + probe_chunk_rows)),
        });
      }
    }
  }

  // Build the hash table over the right side once, before the workers spawn
  // (the build side is read-only during the probe).
  std::optional<FlatJoinIdxBuckets<int64_t>> int_buckets;
  std::optional<FlatJoinIdxBuckets<CompositeIntKey, CompositeIntKeyHash>>
      composite_buckets;
  // A unique, compact leading component can index a composite key directly.
  // Keep all components for exact equality checks; uniqueness is verified
  // below.
  std::vector<CompositeIntKey> dense_composite_keys;
  std::vector<uint32_t> dense_composite_rows;
  std::shared_ptr<QueryMemoryCharge> composite_keys_charge,
      composite_rows_charge;
  int64_t dense_composite_min = 0;
  bool use_dense_composite = false;
  std::optional<FlatJoinIdxBuckets<std::string>> key_buckets;
  std::optional<FlatIntKeyMap<BulkCountValue>> bulk_counts;
  std::vector<BulkCountValue> dense_bulk_counts;
  int64_t dense_bulk_min = 0;
  bool use_dense_bulk = false;
  std::vector<std::vector<BuildKey>> key_cache;
  std::shared_ptr<QueryMemoryCharge> key_cache_charge;
  std::vector<uint32_t> dense_int_idxs;
  std::shared_ptr<QueryMemoryCharge> dense_int_charge;
  std::vector<uint64_t> ranked_int_bits;
  std::vector<uint32_t> ranked_int_prefix, ranked_int_rows;
  uint64_t ranked_int_span = 0;
  int64_t dense_int_min = 0;
  bool use_dense_int_rows = false;
  bool build_int_keys_unique = true;
  bool packed_2x32_int_key = false;
  std::vector<UniqueIntSlot> unique_int_table;
  size_t unique_int_mask = 0;
  bool use_unique_int_table = false;

  // Decode each component once, including gathered columns and NULLs.
  const auto visit_right_int_keys = [&](auto&& on_row) {
    if (right_col_only) {
      const slot_t rc0 = right_columns[0];
      const int64_t bias0 = right_key_offsets[0];
      uint32_t base_r = 0;
      for (const ColumnarChunk& ch : right_arg.columnar->chunks) {
        const ColumnarColumn& kc = ch.columns[rc0];
        const bool fast =
            kc.is_uniform && !kc.is_unsigned &&
            (kc.type == ValueType::kInt64 || kc.type == ValueType::kDate);
        for (size_t r = 0; r < ch.rows; ++r) {
          int64_t raw_k = 0;
          if (!ReadFusedIntCell(kc, r, fast, &raw_k)) {
            continue;
          }
          if (!on_row(base_r + static_cast<uint32_t>(r),
                      AddJoinKeyBias(raw_k, bias0))) {
            return;
          }
        }
        base_r += static_cast<uint32_t>(ch.rows);
      }
      return;
    }
    for (const Row* row_ptr : build_rows) {
      if (HasNullKey(*row_ptr, right_columns)) {
        continue;
      }
      const int64_t bkey = AddJoinKeyBias(
          IntegerJoinKey(*row_ptr, right_columns[0]), right_key_offsets[0]);
      if (!on_row(static_cast<uint32_t>(row_ptr - right_ptr->rows.data()),
                  bkey)) {
        return;
      }
    }
  };
  const auto visit_right_packed_2x32_keys = [&](auto&& on_row) {
    if (!validated_packed_keys.empty()) {
      for (size_t row = 0; row < validated_packed_keys.size(); ++row) {
        const int64_t key = validated_packed_keys[row];
        if (key != kInvalidFusedPacked2x32 &&
            !on_row(static_cast<uint32_t>(row), key)) {
          return;
        }
      }
      return;
    }
    if (right_col_only) {
      const slot_t rc0 = right_columns[0];
      const slot_t rc1 = right_columns[1];
      const int64_t bias0 = right_key_offsets[0];
      const int64_t bias1 = right_key_offsets[1];
      uint32_t base_r = 0;
      for (const ColumnarChunk& ch : right_arg.columnar->chunks) {
        const ColumnarColumn& kc0 = ch.columns[rc0];
        const ColumnarColumn& kc1 = ch.columns[rc1];
        const bool fast0 =
            kc0.is_uniform && !kc0.is_unsigned &&
            (kc0.type == ValueType::kInt64 || kc0.type == ValueType::kDate);
        const bool fast1 =
            kc1.is_uniform && !kc1.is_unsigned &&
            (kc1.type == ValueType::kInt64 || kc1.type == ValueType::kDate);
        for (size_t r = 0; r < ch.rows; ++r) {
          int64_t v0 = 0;
          int64_t v1 = 0;
          if (!ReadFusedIntCell(kc0, r, fast0, &v0) ||
              !ReadFusedIntCell(kc1, r, fast1, &v1)) {
            continue;
          }
          const int64_t bkey = PackFusedInt32Pair(AddJoinKeyBias(v0, bias0),
                                               AddJoinKeyBias(v1, bias1));
          if (!on_row(base_r + static_cast<uint32_t>(r), bkey)) {
            return;
          }
        }
        base_r += static_cast<uint32_t>(ch.rows);
      }
      return;
    }
    for (const Row* row_ptr : build_rows) {
      const Row& row = *row_ptr;
      if (HasNullKey(row, right_columns)) {
        continue;
      }
      const int64_t k0 = AddJoinKeyBias(IntegerJoinKey(row, right_columns[0]),
                                        right_key_offsets[0]);
      const int64_t k1 = AddJoinKeyBias(IntegerJoinKey(row, right_columns[1]),
                                        right_key_offsets[1]);
      if (!on_row(static_cast<uint32_t>(row_ptr - right_ptr->rows.data()),
                  PackFusedInt32Pair(k0, k1))) {
        return;
      }
    }
  };
  const auto build_buckets = [&]() {
    const size_t b_count = right_build_count();
    if (bulk_raw_count) {
      const auto visit_bulk = [&](auto&& on_bulk) {
        if (right_col_only) {
          const slot_t rc0 = right_columns[0];
          const int64_t bias0 = right_key_offsets[0];
          const std::optional<slot_t> count_rc = raw_aggregates.count_right_column;
          uint32_t base_r = 0;
          for (const ColumnarChunk& ch : right_arg.columnar->chunks) {
            const ColumnarColumn& kc = ch.columns[rc0];
            const ColumnarColumn* cnt_c =
                count_rc.has_value() ? &ch.columns[*count_rc] : nullptr;
            const bool fast =
                kc.is_uniform && !kc.is_unsigned &&
                (kc.type == ValueType::kInt64 || kc.type == ValueType::kDate);
            for (size_t r = 0; r < ch.rows; ++r) {
              int64_t raw_k = 0;
              if (!ReadFusedIntCell(kc, r, fast, &raw_k)) {
                continue;
              }
              const int64_t add =
                  (cnt_c == nullptr || !cnt_c->IsNull(r)) ? 1 : 0;
              on_bulk(base_r + static_cast<uint32_t>(r),
                      AddJoinKeyBias(raw_k, bias0), add);
            }
            base_r += static_cast<uint32_t>(ch.rows);
          }
          return;
        }
        for (const Row* row_ptr : build_rows) {
          const Row& row = *row_ptr;
          if (HasNullKey(row, right_columns)) {
            continue;
          }
          const int64_t bkey = AddJoinKeyBias(
              IntegerJoinKey(row, right_columns[0]), right_key_offsets[0]);
          const int64_t add = (!raw_aggregates.count_right_column.has_value() ||
                               !row.values_[*raw_aggregates.count_right_column].IsNull())
                                  ? 1
                                  : 0;
          on_bulk(static_cast<uint32_t>(row_ptr - right_ptr->rows.data()), bkey,
                  add);
        }
      };
      if (!left.HasSpill() && !left.HasColumnarOnly() && !left.rows.empty()) {
        int64_t min_k = std::numeric_limits<int64_t>::max();
        int64_t max_k = std::numeric_limits<int64_t>::min();
        for (const Row& lrow : left.rows) {
          if (HasNullKey(lrow, left_columns)) {
            continue;
          }
          const int64_t k = AddJoinKeyBias(
              IntegerJoinKey(lrow, left_columns[0]), left_key_offsets[0]);
          min_k = std::min(min_k, k);
          max_k = std::max(max_k, k);
        }
        if (min_k <= max_k) {
          const uint64_t span = static_cast<uint64_t>(max_k) -
                                static_cast<uint64_t>(min_k) + 1ULL;
          if (span <= 2'000'000ULL &&
              span <=
                  (static_cast<uint64_t>(left.rows.size()) * 8ULL) + 1024ULL) {
            dense_bulk_min = min_k;
            dense_bulk_counts.resize(static_cast<size_t>(span));
            use_dense_bulk = true;
            visit_bulk([&](uint32_t r_idx, int64_t build_key, int64_t add) {
              if (build_key < min_k || build_key > max_k) {
                return;
              }
              BulkCountValue& slot = dense_bulk_counts[static_cast<size_t>(
                  static_cast<uint64_t>(build_key) -
                  static_cast<uint64_t>(min_k))];
              ++slot.pairs;
              slot.count += add;
              slot.r_idx = r_idx;
            });
            return;
          }
        }
      }
      // The duplicate count and the bucket-head row stand in for the whole
      // bucket chain: the probe never touches an Entry or a build Row.
      bulk_counts.emplace();
      bulk_counts->Reserve(b_count);
      visit_bulk([&](uint32_t r_idx, int64_t build_key, int64_t add) {
        bool inserted = false;
        BulkCountValue* slot = bulk_counts->TryEmplace(build_key, &inserted);
        slot->count = inserted ? add : slot->count + add;
        slot->pairs = inserted ? 1 : slot->pairs + 1;
        slot->r_idx = r_idx;
      });
      return;
    }
    if (integer_key) {
      // Gathered columns can require several indirections per cell. Decode
      // build keys once when range discovery and direct-index construction
      // will make multiple passes over the same column.
      std::vector<std::pair<int64_t, int64_t>> key_bounds;
      if (deferred_left_probe != nullptr && right_col_only &&
          b_count >= 262144 && b_count <= 4194304) {
        key_cache_charge = TryReserveQueryMemory(b_count * sizeof(BuildKey));
        if (key_cache_charge) {
          const auto& chunks = right_arg.columnar->chunks;
          std::vector<uint32_t> starts(chunks.size() + 1, 0);
          for (size_t i = 0; i < chunks.size(); ++i) {
            starts[i + 1] = starts[i] + chunks[i].rows;
          }
          const size_t tasks =
              std::min(workers, std::max<size_t>(1, chunks.size()));
          key_cache.resize(tasks);
          key_bounds.assign(tasks, {INT64_MAX, INT64_MIN});
          const Status decoded = RunParallelTasks(tasks, [&](size_t w) {
            const size_t begin = chunks.size() * w / tasks;
            const size_t end = chunks.size() * (w + 1) / tasks;
            auto& keys = key_cache[w];
            auto& [low, high] = key_bounds[w];
            keys.reserve(starts[end] - starts[begin]);
            for (size_t ci = begin; ci < end; ++ci) {
              const auto& column = chunks[ci].columns[right_columns[0]];
              const bool fast = column.is_uniform && !column.is_unsigned &&
                                (column.type == ValueType::kInt64 ||
                                 column.type == ValueType::kDate);
              for (size_t r = 0; r < chunks[ci].rows; ++r) {
                int64_t raw = 0;
                if (!ReadFusedIntCell(column, r, fast, &raw)) {
                  continue;
                }
                const int64_t key = AddJoinKeyBias(raw, right_key_offsets[0]);
                keys.push_back({key, starts[ci] + static_cast<uint32_t>(r)});
                low = std::min(low, key);
                high = std::max(high, key);
              }
            }
          });
          if (!decoded.ok()) {
            key_cache.clear();
            key_cache_charge.reset();
          }
        }
      }
      const auto visit_build_int_keys = [&](auto&& on_row) {
        if (!key_cache_charge) {
          visit_right_int_keys(on_row);
          return;
        }
        for (const auto& chunk : key_cache) {
          for (const auto& entry : chunk) {
            if (!on_row(entry.row, entry.key)) {
              return;
            }
          }
        }
      };
      if (!left.HasSpill() && b_count > 0) {
        int64_t min_k = std::numeric_limits<int64_t>::max();
        int64_t max_k = std::numeric_limits<int64_t>::min();
        if (key_cache_charge) {
          for (const auto& [low, high] : key_bounds) {
            min_k = std::min(min_k, low);
            max_k = std::max(max_k, high);
          }
        } else {
          visit_build_int_keys([&](uint32_t, int64_t bkey) {
            min_k = std::min(min_k, bkey);
            max_k = std::max(max_k, bkey);
            return true;
          });
        }
        if (min_k <= max_k) {
          const uint64_t span = static_cast<uint64_t>(max_k) -
                                static_cast<uint64_t>(min_k) + 1ULL;
          // A wide sparse domain needs a membership bitmap and ranks, not
          // one row slot for every possible key. Rank selects the row index
          // with one popcount while keeping this direct lookup cache-sized.
          if (deferred_left_probe != nullptr && !fusion->group_keys.empty() &&
              !fusion->aggregates.empty() && span > 1'048'576ULL &&
              span <= 134'217'728ULL &&
              span <= static_cast<uint64_t>(b_count) * 64ULL) {
            const auto words = static_cast<size_t>((span + 63) / 64);
            const size_t bytes = (words * 12) + (b_count * 4);
            dense_int_charge = TryReserveQueryMemory(bytes);
            if (dense_int_charge) {
              ranked_int_bits.assign(words, 0);
              bool unique = true;
              if (key_cache_charge && key_cache.size() > 1) {
                // Adjacent chunk partitions may share bitmap words, and
                // duplicate keys can occur in different partitions. Atomic
                // OR preserves both membership and the uniqueness proof.
                std::atomic<bool> duplicate{false};
                const Status indexed =
                    RunParallelTasks(key_cache.size(), [&](size_t w) {
                      for (const auto& entry : key_cache[w]) {
                        const uint64_t off = static_cast<uint64_t>(entry.key) -
                                             static_cast<uint64_t>(min_k);
                        const uint64_t bit = uint64_t{1} << (off & 63);
                        std::atomic_ref<uint64_t> word(
                            ranked_int_bits[off >> 6]);
                        if ((word.fetch_or(bit, std::memory_order_relaxed) &
                             bit) != 0) {
                          duplicate.store(true, std::memory_order_relaxed);
                        }
                      }
                    });
                unique =
                    indexed.ok() && !duplicate.load(std::memory_order_relaxed);
              } else {
                visit_build_int_keys([&](uint32_t, int64_t key) {
                  const uint64_t off =
                      static_cast<uint64_t>(key) - static_cast<uint64_t>(min_k);
                  auto& word = ranked_int_bits[off >> 6];
                  const uint64_t bit = uint64_t{1} << (off & 63);
                  if ((word & bit) != 0) {
                    unique = false;
                    return false;
                  }
                  word |= bit;
                  return true;
                });
              }
              if (unique) {
                dense_int_min = min_k;
                ranked_int_span = span;
                use_dense_int_rows = true;
                build_int_keys_unique = true;
                return;
              }
              std::vector<uint64_t>().swap(ranked_int_bits);
              dense_int_charge.reset();
            }
          }
          if (span <= 1'048'576ULL) {
            dense_int_min = min_k;
            dense_int_idxs.assign(static_cast<size_t>(span), kInvalidRightIdx);
            bool unique = true;
            visit_build_int_keys([&](uint32_t r_idx, int64_t bkey) {
              const auto idx = static_cast<size_t>(
                  static_cast<uint64_t>(bkey) - static_cast<uint64_t>(min_k));
              if (dense_int_idxs[idx] != kInvalidRightIdx) {
                unique = false;
                return false;
              }
              dense_int_idxs[idx] = r_idx;
              return true;
            });
            if (unique) {
              use_dense_int_rows = true;
              build_int_keys_unique = true;
              return;
            }
            std::vector<uint32_t>().swap(dense_int_idxs);
            dense_int_charge.reset();
          }
        }
        if (!right_ptr->HasSpill() && b_count <= 524288) {
          size_t cap = 16;
          while (cap < b_count * 2) {
            cap <<= 1;
          }
          unique_int_table.assign(cap, UniqueIntSlot{});
          unique_int_mask = cap - 1;
          bool unique = true;
          visit_build_int_keys([&](uint32_t r_idx, int64_t bkey) {
            size_t slot = HashUniqueIntKey(bkey) & unique_int_mask;
            while (unique_int_table[slot].r_idx !=
                   std::numeric_limits<uint32_t>::max()) {
              if (unique_int_table[slot].key == bkey) {
                unique = false;
                return false;
              }
              slot = (slot + 1) & unique_int_mask;
            }
            unique_int_table[slot].key = bkey;
            unique_int_table[slot].r_idx = r_idx;
            return true;
          });
          if (unique) {
            use_unique_int_table = true;
            build_int_keys_unique = true;
            return;
          }
          unique_int_table.clear();
        }
      }
      int_buckets.emplace();
      int_buckets->reserve(b_count);
      visit_build_int_keys([&](uint32_t r_idx, int64_t bkey) {
        if (!int_buckets->emplace(bkey, r_idx)) {
          build_int_keys_unique = false;
        }
        return true;
      });
    } else if (composite_int_key) {
      bool can_pack = (right_columns.size() == 2);
      if (can_pack && !right_col_only) {
        for (const Row* row_ptr : build_rows) {
          const Row& row = *row_ptr;
          if (HasNullKey(row, right_columns)) {
            continue;
          }
          const int64_t k0 = AddJoinKeyBias(
              IntegerJoinKey(row, right_columns[0]), right_key_offsets[0]);
          const int64_t k1 = AddJoinKeyBias(
              IntegerJoinKey(row, right_columns[1]), right_key_offsets[1]);
          if (!FitsFusedInt32(k0) || !FitsFusedInt32(k1) ||
              PackFusedInt32Pair(k0, k1) == kInvalidFusedPacked2x32) {
            can_pack = false;
            break;
          }
        }
      }
      if (can_pack) {
        packed_2x32_int_key = true;
        if (!left.HasSpill() && !right_ptr->HasSpill() && b_count > 0 &&
            b_count <= 524288) {
          size_t cap = 16;
          while (cap < b_count * 2) {
            cap <<= 1;
          }
          unique_int_table.assign(cap, UniqueIntSlot{});
          unique_int_mask = cap - 1;
          bool unique = true;
          visit_right_packed_2x32_keys([&](uint32_t r_idx, int64_t bkey) {
            size_t slot = HashUniqueIntKey(bkey) & unique_int_mask;
            while (unique_int_table[slot].r_idx !=
                   std::numeric_limits<uint32_t>::max()) {
              if (unique_int_table[slot].key == bkey) {
                unique = false;
                return false;
              }
              slot = (slot + 1) & unique_int_mask;
            }
            unique_int_table[slot].key = bkey;
            unique_int_table[slot].r_idx = r_idx;
            return true;
          });
          if (unique) {
            use_unique_int_table = true;
            build_int_keys_unique = true;
            return;
          }
          unique_int_table.clear();
        }
        int_buckets.emplace();
        int_buckets->reserve(b_count);
        visit_right_packed_2x32_keys([&](uint32_t r_idx, int64_t bkey) {
          int_buckets->emplace(bkey, r_idx);
          return true;
        });
      } else {
        if (right_col_only && left.HasColumnarOnly() && b_count > 0 &&
            b_count <= 4ULL * 1024 * 1024) {
          composite_keys_charge =
              TryReserveQueryMemory(b_count * sizeof(CompositeIntKey));
          if (composite_keys_charge) {
            dense_composite_keys.resize(b_count);
            int64_t minimum = std::numeric_limits<int64_t>::max();
            int64_t maximum = std::numeric_limits<int64_t>::min();
            size_t base = 0;
            bool any = false;
            for (const auto& chunk : right_arg.columnar->chunks) {
              for (size_t r = 0; r < chunk.rows; ++r) {
                CompositeIntKey key;
                if (!ReadFusedCompositeKey(chunk, right_columns,
                                            right_key_offsets, r, &key)) {
                  continue;
                }
                dense_composite_keys[base + r] = key;
                minimum = std::min(minimum, key.parts[0]);
                maximum = std::max(maximum, key.parts[0]);
                any = true;
              }
              base += chunk.rows;
            }
            const uint64_t span =
                // NOLINTNEXTLINE(bugprone-implicit-widening-of-multiplication-result)
                // operands bounded
                static_cast<uint64_t>(maximum) - static_cast<uint64_t>(minimum);
            if (any && span < 16ULL * 1024 * 1024 &&
                span <
                    b_count *
                        16) {  // NOLINT(bugprone-implicit-widening-of-multiplication-result)
              composite_rows_charge =
                  TryReserveQueryMemory((span + 1) * sizeof(uint32_t));
              if (composite_rows_charge) {
                dense_composite_rows.assign(span + 1, kInvalidRightIdx);
                bool unique = true;
                for (size_t r = 0; r < b_count; ++r) {
                  const auto& key = dense_composite_keys[r];
                  if (key.count == 0) {
                    continue;
                  }
                  const uint64_t offset = static_cast<uint64_t>(key.parts[0]) -
                                          static_cast<uint64_t>(minimum);
                  if (dense_composite_rows[offset] != kInvalidRightIdx) {
                    unique = false;
                    break;
                  }
                  dense_composite_rows[offset] = r;
                }
                if (unique) {
                  dense_composite_min = minimum;
                  use_dense_composite = true;
                  return;
                }
              }
            }
            std::vector<uint32_t>().swap(dense_composite_rows);
            composite_rows_charge.reset();
          }
        }
        composite_buckets.emplace();
        composite_buckets->reserve(b_count);
        if (!dense_composite_keys.empty()) {
          // Reuse decoded keys when the optional dense layout was unsuitable.
          for (size_t r = 0; r < dense_composite_keys.size(); ++r) {
            if (dense_composite_keys[r].count != 0) {
              composite_buckets->emplace(dense_composite_keys[r], r);
            }
          }
          std::vector<CompositeIntKey>().swap(dense_composite_keys);
          composite_keys_charge.reset();
        } else if (right_col_only) {
          uint32_t base = 0;
          for (const auto& chunk : right_arg.columnar->chunks) {
            for (size_t r = 0; r < chunk.rows; ++r) {
              CompositeIntKey key;
              if (!ReadFusedCompositeKey(chunk, right_columns,
                                          right_key_offsets, r, &key)) {
                continue;
              }
              composite_buckets->emplace(key, base + static_cast<uint32_t>(r));
            }
            base += chunk.rows;
          }
        } else {
          for (const Row* row_ptr : build_rows) {
            const Row& row = *row_ptr;
            if (HasNullKey(row, right_columns)) {
              continue;
            }
            composite_buckets->emplace(
                EncodeCompositeIntKey(row, right_columns, &right_key_offsets),
                static_cast<uint32_t>(row_ptr - right_ptr->rows.data()));
          }
        }
      }
    } else {
      key_buckets.emplace();
      key_buckets->reserve(b_count);
      for (const Row* row_ptr : build_rows) {
        const Row& row = *row_ptr;
        if (HasNullKey(row, right_columns)) {
          continue;
        }
        key_buckets->emplace(
            EncodeJoinKey(row, right_columns, &right_key_offsets),
            static_cast<uint32_t>(row_ptr - right_ptr->rows.data()));
      }
    }
  };
  if (!singleton_right_relation) {
    build_buckets();
  }
  // Lean proof: lean/TinylambOpt/ResearchProofs/FanoutWeightestd::cmp_equal(
  // (so, For high-f)anout skewed probes under workers > 1, partition probe rows
  // by cstd::cmp_equal(h ma, count(k)) via b)inary search so every worker
  // handles balanced pair work while keeping disjoint probe-row ownership.
  std::vector<size_t> fanout_key_bounds;
  if (workers > 1 && estimated_avg_depth >= 4.0 && !singleton_right_relation &&
      !left.HasSpill() && !left.HasColumnarOnly() && total_left > 0 &&
      total_left <= 65536 && key_buckets.has_value() &&
      ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_FANOUT_WORK_SPLIT") ==
          nullptr) {
    std::vector<uint64_t> prefix_weight(total_left + 1, 0);
    for (size_t ri = 0; ri < total_left; ++ri) {
      const Row& lr = left.rows[ri];
      uint64_t w = 1;
      if (!HasNullKey(lr, left_columns)) {
        const auto [first, last] = key_buckets->equal_range(
            EncodeJoinKey(lr, left_columns, &left_key_offsets));
        uint64_t depth = 0;
        for (auto it = first; it != last; ++it) {
          ++depth;
        }
        if (depth > 1) {
          w = depth;
        }
      }
      prefix_weight[ri + 1] = prefix_weight[ri] + w;
    }
    const uint64_t total_w = prefix_weight.back();
    if (total_w >= workers) {
      fanout_key_bounds.resize(workers + 1, 0);
      fanout_key_bounds[workers] = total_left;
      for (size_t wi = 1; wi < workers; ++wi) {
        const uint64_t target = (total_w * wi) / workers;
        auto it = std::lower_bound(prefix_weight.begin(), prefix_weight.end(),
                                   target);
        fanout_key_bounds[wi] = static_cast<size_t>(it - prefix_weight.begin());
      }
    }
  }

  const auto phase_buckets_done = std::chrono::steady_clock::now();

  // Fast path: 2-key dense VARCHAR x VARCHAR + direct INT64 / CASE-INT64 SUM
  // aggregation over a dense unique build key. Handles both (left, right) and
  // (left, left) key placements (using zero-copy kGathered source dictionaries
  // when available) and both left-side and right-side CASE condition columns.
  // Lean proof: lean/TinylambOpt/VectorizedJoinAndPruning.lean
  //   (dense_2key_varchar_case_sum_sound)
  if (join_type == JoinType::kInner && integer_key && use_dense_int_rows &&
      !dense_int_idxs.empty() && left_columns.size() == 1 &&
      left_key_offsets.size() == 1 && left.HasColumnarOnly() &&
      left.columnar != nullptr && left.columnar->total_rows >= 4096 &&
      !left.HasSpill() && !right_ptr->HasSpill() && residual_programs.empty() &&
      !fusion->having && !fusion->post_having_active &&
      fusion->topn_limit == 0 && !fusion->rollup_count_distribution &&
      result.using_columns == nullptr && fusion->output_sink == nullptr &&
      fusion->columnar_output && def_pk_groups.empty() &&
      key_programs.size() == 2 && raw_aggregates.raw_sum_fusion && !raw_aggregates.raw_count_fusion &&
      !raw_aggregates.has_count_distinct && !fusion->aggregates.empty() &&
      fusion->aggregates.size() <= 4 && layout.right_row_count > 0 &&
      layout.right_row_count <= 262144) {
    DenseVarcharGroups varchar_groups{
      .raw_aggregates = raw_aggregates,
        .context = context,
        .left = left,
        .fusion = fusion,
        .debug_fusion = debug_fusion,
        .right = right,
        .join_begin = join_begin,
        .result = result,
        .left_width = left_width,
        .key_shapes = key_shapes,
        .child_shapes = child_shapes,
        .left_columns = left_columns,
        .left_key_offsets = left_key_offsets,
        .read_right_cell = read_right_cell,
        .right_row_count = layout.right_row_count,
        .workers = workers,
        .dense_int_idxs = dense_int_idxs,
        .dense_int_min = dense_int_min,
    };
    ASSIGN_OR_RETURN(bool, aggregated, varchar_groups.TryAggregate());
    if (aggregated) {
      return std::optional<Relation>(std::move(result));
    }
  }

  // A proven unique build bounds the append log by the number of probe
  // rows. Charge vector growth slack before enabling it, and retain local
  // aggregation when the query memory budget cannot cover the log.
  static const bool compact_log_disabled =
      ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_COMPACT_COUNT_LOG") !=
      nullptr;
  const size_t log_entry_bytes =
      compact_split_key_only ? sizeof(uint64_t) : sizeof(CompactCountGroup);
  const size_t est_compact_log_rows = std::max(total_left, effective_left_rows);
  auto compact_log_charge =
      compact_counts && !compact_log_disabled && workers > 1 &&
              !left.HasSpill() &&
              (use_dense_int_rows || use_unique_int_table) &&
              est_compact_log_rows <= SIZE_MAX / (2 * log_entry_bytes)
          ? TryReserveQueryMemory(est_compact_log_rows * 2 * log_entry_bytes)
          : nullptr;
  compact_log_allowed = compact_log_charge != nullptr;


  std::vector<uint32_t> right_row_gid;
  FusedRawArena pre_raw_arena;
  pre_raw_arena.has_stat = has_stat_raw_agg;
  pre_raw_arena.has_double = !raw_aggregates.raw_sum_fusion || has_double_raw_agg;
  pre_raw_arena.has_int = !raw_aggregates.raw_sum_fusion || has_int_raw_agg;
  std::vector<FusedGroup> pre_groups;
  bool identity_right_gid = false;
  const size_t active_build_count = right_build_count();
  // Dense per-worker states scale with bytes, not a fixed number of groups.
  // Larger unique build domains used to construct millions of FusedGroups
  // only to discard them at the fixed 512K cutoff below.
  const size_t direct_slots =
      raw_aggregates.raw_sum_fusion ? raw_aggregates.raw_slot_count : fusion->aggregates.size();
  const size_t direct_state_bytes =
      raw_aggregates.raw_count_fusion
          ? 9
          : 1 + (direct_slots *
                 (9 + (has_double_raw_agg ? 8 : 0) + (has_int_raw_agg ? 8 : 0) +
                  (has_stat_raw_agg ? 2 * sizeof(platform::ExtendedFloat)
                                    : 0)));
  size_t direct_group_limit = 524288;
  std::shared_ptr<QueryMemoryCharge> extended_direct_charge;
  if (layout.right_row_count > direct_group_limit && workers > 0 &&
      layout.right_row_count <= (size_t{256} << 20) / workers / direct_state_bytes &&
      integer_key && build_int_keys_unique && right_only_group_keys &&
      all_direct_group_keys && !raw_aggregates.has_count_distinct) {
    extended_direct_charge =
        TryReserveQueryMemory(workers * layout.right_row_count * direct_state_bytes);
    if (extended_direct_charge) {
      direct_group_limit = layout.right_row_count;
    }
  }
  bool use_preassigned_build_groups =
      join_type == JoinType::kInner && !left.HasSpill() &&
      !right_ptr->HasSpill() && right_only_group_keys &&
      raw_aggregates.skip_accumulator_states && residual_programs.empty() && !bulk_raw_count &&
      active_build_count > 0;
  const auto for_each_build_nonnull_idx = [&](auto&& on_idx) -> Status {
    if (right_col_only) {
      if (integer_key) {
        const slot_t rc0 = right_columns[0];
        uint32_t base_r = 0;
        for (const ColumnarChunk& ch : right_arg.columnar->chunks) {
          const ColumnarColumn& kc = ch.columns[rc0];
          const bool fast =
              kc.is_uniform && !kc.is_unsigned &&
              (kc.type == ValueType::kInt64 || kc.type == ValueType::kDate);
          for (size_t r = 0; r < ch.rows; ++r) {
            int64_t dummy = 0;
            if (ReadFusedIntCell(kc, r, fast, &dummy)) {
              RETURN_IF_FAIL(on_idx(base_r + static_cast<uint32_t>(r)));
            }
          }
          base_r += static_cast<uint32_t>(ch.rows);
        }
        return Status::kSuccess;
      }
      if (right_columns.size() > 2) {
        uint32_t base = 0;
        Value key_scratch;
        for (const auto& chunk : right_arg.columnar->chunks) {
          for (size_t r = 0; r < chunk.rows; ++r) {
            if (!ColumnarHasNullOrNaN(chunk, right_columns, r, &key_scratch)) {
              RETURN_IF_FAIL(on_idx(base + static_cast<uint32_t>(r)));
            }
          }
          base += chunk.rows;
        }
        return Status::kSuccess;
      }
      const slot_t rc0 = right_columns[0];
      const slot_t rc1 = right_columns[1];
      uint32_t base_r = 0;
      for (const ColumnarChunk& ch : right_arg.columnar->chunks) {
        const ColumnarColumn& kc0 = ch.columns[rc0];
        const ColumnarColumn& kc1 = ch.columns[rc1];
        const bool fast0 =
            kc0.is_uniform && !kc0.is_unsigned &&
            (kc0.type == ValueType::kInt64 || kc0.type == ValueType::kDate);
        const bool fast1 =
            kc1.is_uniform && !kc1.is_unsigned &&
            (kc1.type == ValueType::kInt64 || kc1.type == ValueType::kDate);
        for (size_t r = 0; r < ch.rows; ++r) {
          int64_t d0 = 0;
          int64_t d1 = 0;
          if (ReadFusedIntCell(kc0, r, fast0, &d0) &&
              ReadFusedIntCell(kc1, r, fast1, &d1)) {
            RETURN_IF_FAIL(on_idx(base_r + static_cast<uint32_t>(r)));
          }
        }
        base_r += static_cast<uint32_t>(ch.rows);
      }
      return Status::kSuccess;
    }
    for (const Row* row_ptr : build_rows) {
      if (!HasNullKey(*row_ptr, right_columns)) {
        RETURN_IF_FAIL(
            on_idx(static_cast<uint32_t>(row_ptr - right_ptr->rows.data())));
      }
    }
    return Status::kSuccess;
  };
  if (use_preassigned_build_groups) {
    if (key_programs.empty()) {
      right_row_gid.assign(layout.right_row_count, kInvalidGid);
      FusedGroup g;
      g.rep_left = (deferred_left_probe != nullptr || left.HasColumnarOnly() ||
                    left.rows.empty())
                       ? nullptr
                       : left.rows.data();
      g.rep_right_idx =
          right_col_only
              ? 0U
              : static_cast<uint32_t>(build_rows[0] - right_ptr->rows.data());
      pre_groups.push_back(std::move(g));
      RETURN_IF_FAIL(for_each_build_nonnull_idx([&](uint32_t r_idx) -> Status {
        right_row_gid[r_idx] = 0;
        return Status::kSuccess;
      }));
    } else if (integer_key && build_int_keys_unique && all_direct_group_keys &&
               !raw_aggregates.has_count_distinct && layout.right_row_count <= direct_group_limit &&
               std::ranges::any_of(key_shapes, [&](const ProgramShape& ks) {
                 return std::cmp_equal(ks.direct_column,
                                       left_width + right_columns[0]);
               })) {
      identity_right_gid = true;
    } else if (integer_key && build_int_keys_unique && all_direct_group_keys &&
               std::ranges::any_of(key_shapes, [&](const ProgramShape& ks) {
                 return std::cmp_equal(ks.direct_column,
                                       left_width + right_columns[0]);
               })) {
      right_row_gid.assign(layout.right_row_count, kInvalidGid);
      pre_groups.reserve(active_build_count);
      RETURN_IF_FAIL(for_each_build_nonnull_idx([&](uint32_t r_idx) -> Status {
        const auto gid = static_cast<uint32_t>(pre_groups.size());
        right_row_gid[r_idx] = gid;
        FusedGroup g;
        g.rep_right_idx = r_idx;
        pre_groups.push_back(std::move(g));
        return Status::kSuccess;
      }));
    } else {
      // Lean proof: lean/TinylambOpt/VectorizedJoinAndPruning.lean
      //   (parallel_hash_partition_preassign_iff)
      // Parallel zero-copy hash-partitioned group pre-assignment when all
      // group keys are direct uniform INT64/DATE/binary-VARCHAR columns on the
      // columnar right side (e.g. TPC-H Q10, Q16). Avoids single-threaded
      // Value/Row materialization and std::string copies.
      bool parallel_preassign_ok =
          all_direct_group_keys && right_col_only && workers >= 2 &&
          layout.right_row_count >= 16384 && !key_shapes.empty() &&
          key_shapes.size() <= 8 && !right_ptr->columnar->chunks.empty();
      std::array<slot_t, 8> pg_rcols{};
      std::array<ValueType, 8> pg_types{};
      if (parallel_preassign_ok) {
        for (size_t k = 0; k < key_shapes.size(); ++k) {
          if (std::cmp_less(key_shapes[k].direct_column, left_width)) {
            parallel_preassign_ok = false;
            break;
          }
          const auto rc =
              static_cast<slot_t>(key_shapes[k].direct_column - left_width);
          const Column& col_meta = right_ptr->schema.GetColumn(rc);
          const ValueType vt = col_meta.Type();
          if (vt != ValueType::kInt64 && vt != ValueType::kDate &&
              (vt != ValueType::kVarChar || col_meta.IsInterval())) {
            parallel_preassign_ok = false;
            break;
          }
          pg_rcols[k] = rc;
          pg_types[k] = vt;
        }
        if (parallel_preassign_ok) {
          for (const ColumnarChunk& ch : right_ptr->columnar->chunks) {
            for (size_t k = 0; k < key_shapes.size(); ++k) {
              const ColumnarColumn& cc = ch.columns[pg_rcols[k]];
              if (!cc.is_uniform ||
                  (cc.type != pg_types[k] && cc.type != ValueType::kNull)) {
                parallel_preassign_ok = false;
                break;
              }
            }
            if (!parallel_preassign_ok) {
              break;
            }
          }
        }
      }
      if (parallel_preassign_ok) {
        right_row_gid.assign(layout.right_row_count, kInvalidGid);
        const size_t n_keys = key_shapes.size();
        const auto& rchunks = right_ptr->columnar->chunks;
        size_t n_parts = 1;
        while (n_parts < workers && n_parts < 16) {
          n_parts <<= 1U;
        }
        const size_t part_mask = n_parts - 1;
        struct PreEntry {
          uint32_t r_idx;
          uint64_t hash;
        };
        std::vector<std::vector<std::vector<PreEntry>>> w_parts(
            workers, std::vector<std::vector<PreEntry>>(n_parts));

        auto hash_row_at = [&](const ColumnarChunk& ch,
                               size_t rr) noexcept -> uint64_t {
          uint64_t h = 0x9e3779b97f4a7c15ULL;
          for (size_t k = 0; k < n_keys; ++k) {
            const ColumnarColumn& cc = ch.columns[pg_rcols[k]];
            uint64_t kh = 0;
            if (cc.IsNull(rr)) {
              kh = 0x517cc1b727220a95ULL;
            } else if (pg_types[k] == ValueType::kVarChar) {
              const std::string_view sv = cc.StringAt(rr);
              // FNV-1a 64-bit over bytes (zero allocation).
              uint64_t sh = 14695981039346656037ULL;
              for (const char c : sv) {
                sh ^= static_cast<uint8_t>(c);
                sh *= 1099511628211ULL;
              }
              kh = sh ^ (sv.size() * 0x9e3779b97f4a7c15ULL);
            } else {
              auto z = static_cast<uint64_t>(cc.IntAt(rr));
              z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
              z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
              kh = z ^ (z >> 31);
            }
            h ^= kh + 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2);
          }
          return h;
        };

        // Phase 1: Parallel hash + partition across workers.
        RETURN_IF_FAIL(RunParallelTasks(workers, [&](size_t tid) {
          const size_t begin = (layout.right_row_count * tid) / workers;
          const size_t end = (layout.right_row_count * (tid + 1)) / workers;
          auto& local_parts = w_parts[tid];
          for (size_t p = 0; p < n_parts; ++p) {
            local_parts[p].reserve(((end - begin) / n_parts) + 64);
          }
          size_t r_cur = begin;
          while (r_cur < end) {
            const auto [ci, rr0] =
                right_locator.Locate(static_cast<uint32_t>(r_cur));
            const ColumnarChunk& ch = rchunks[ci];
            const size_t take = std::min(end - r_cur, ch.rows - rr0);
            for (size_t d = 0; d < take; ++d) {
              const size_t rr = rr0 + d;
              bool null_jk = false;
              for (const slot_t jk_col : right_columns) {
                if (ch.columns[jk_col].IsNull(rr)) {
                  null_jk = true;
                  break;
                }
              }
              if (null_jk) {
                continue;
              }
              const uint64_t h = hash_row_at(ch, rr);
              local_parts[(h >> 32) & part_mask].push_back(
                  PreEntry{static_cast<uint32_t>(r_cur + d), h});
            }
            r_cur += take;
          }
        }));

        // Phase 2: Independent per-partition zero-copy deduplication.
        std::vector<std::vector<uint32_t>> part_reps(n_parts);
        struct RowGidPair {
          uint32_t r_idx;
          uint32_t lg;
        };
        std::vector<std::vector<RowGidPair>> part_assigns(n_parts);
        RETURN_IF_FAIL(RunParallelTasks(n_parts, [&](size_t p) {
          size_t total = 0;
          for (size_t tid = 0; tid < workers; ++tid) {
            total += w_parts[tid][p].size();
          }
          if (total == 0) {
            return;
          }
          size_t cap = 16;
          while (cap < total * 2) {
            cap <<= 1U;
          }
          const size_t mask = cap - 1;
          std::vector<uint32_t> ht(cap, 0);
          std::vector<uint64_t> rep_hashes;
          auto& reps = part_reps[p];
          auto& assigns = part_assigns[p];
          reps.reserve(std::min<size_t>(total, 65536));
          rep_hashes.reserve(std::min<size_t>(total, 65536));
          assigns.reserve(total);

          auto keys_equal = [&](uint32_t r_a, uint32_t r_b) noexcept -> bool {
            const auto [ca, ra] = right_locator.Locate(r_a);
            const auto [cb, rb] = right_locator.Locate(r_b);
            const ColumnarChunk& cha = rchunks[ca];
            const ColumnarChunk& chb = rchunks[cb];
            for (size_t k = 0; k < n_keys; ++k) {
              const ColumnarColumn& cca = cha.columns[pg_rcols[k]];
              const ColumnarColumn& ccb = chb.columns[pg_rcols[k]];
              const bool na = cca.IsNull(ra);
              const bool nb = ccb.IsNull(rb);
              if (na != nb) {
                return false;
              }
              if (na) {
                continue;
              }
              if (pg_types[k] == ValueType::kVarChar) {
                if (cca.StringAt(ra) != ccb.StringAt(rb)) {
                  return false;
                }
              } else {
                if (cca.IntAt(ra) != ccb.IntAt(rb)) {
                  return false;
                }
              }
            }
            return true;
          };

          for (size_t tid = 0; tid < workers; ++tid) {
            for (const PreEntry& e : w_parts[tid][p]) {
              size_t slot = static_cast<size_t>(e.hash) & mask;
              uint32_t found_lg = kInvalidGid;
              while (ht[slot] != 0) {
                const uint32_t lg = ht[slot] - 1;
                if (rep_hashes[lg] == e.hash && keys_equal(reps[lg], e.r_idx)) {
                  found_lg = lg;
                  break;
                }
                slot = (slot + 1) & mask;
              }
              if (found_lg == kInvalidGid) {
                found_lg = static_cast<uint32_t>(reps.size());
                reps.push_back(e.r_idx);
                rep_hashes.push_back(e.hash);
                ht[slot] = found_lg + 1;
              }
              assigns.push_back(RowGidPair{e.r_idx, found_lg});
            }
            std::vector<PreEntry>().swap(w_parts[tid][p]);
          }
        }));

        // Phase 3: Prefix sum + parallel global gid & pre_groups write.
        std::vector<uint32_t> part_base(n_parts, 0);
        size_t total_groups = 0;
        for (size_t p = 0; p < n_parts; ++p) {
          part_base[p] = static_cast<uint32_t>(total_groups);
          total_groups += part_reps[p].size();
        }
        pre_groups.resize(total_groups);
        RETURN_IF_FAIL(RunParallelTasks(n_parts, [&](size_t p) {
          const uint32_t base = part_base[p];
          const auto& reps = part_reps[p];
          for (size_t lg = 0; lg < reps.size(); ++lg) {
            pre_groups[base + lg].rep_right_idx = reps[lg];
          }
          for (const RowGidPair& a : part_assigns[p]) {
            right_row_gid[a.r_idx] = base + a.lg;
          }
        }));
      } else {
        right_row_gid.assign(layout.right_row_count, kInvalidGid);
        FusedWorkerLocal pre_local;
        pre_local.raw_arena.has_stat = has_stat_raw_agg;
        pre_local.raw_arena.has_double = !raw_aggregates.raw_sum_fusion || has_double_raw_agg;
        pre_local.raw_arena.has_int = !raw_aggregates.raw_sum_fusion || has_int_raw_agg;
        pre_local.int_offsets.Reserve(active_build_count);
        pre_local.groups.reserve(active_build_count);
        Row& pre_scratch = scratch_storage[0];
        const Status pre_st =
            for_each_build_nonnull_idx([&](uint32_t r_idx) -> Status {
              if (!all_direct_group_keys) {
                Value pre_tmp;
                for (const slot_t cell : scratch_cells) {
                  if (cell >= left_width) {
                    pre_scratch.values_[cell] =
                        read_right_cell(r_idx, cell - left_width, &pre_tmp);
                  }
                }
              }
              size_t gid = 0;
              RETURN_IF_FAIL(resolve_group_index(
                  pre_local, pre_scratch, nullptr, r_idx, true, nullptr, &gid));
              right_row_gid[r_idx] = static_cast<uint32_t>(gid);
              return Status::kSuccess;
            });
        if (!pre_st.ok()) {
          use_preassigned_build_groups = false;
          right_row_gid.clear();
          pre_groups.clear();
        } else {
          pre_groups = std::move(pre_local.groups);
          pre_raw_arena = std::move(pre_local.raw_arena);
        }
      }
    }
  }
  const size_t pre_group_count =
      identity_right_gid ? layout.right_row_count : pre_groups.size();
  const bool use_direct_gid_agg = use_preassigned_build_groups &&
                                  !raw_aggregates.has_count_distinct && pre_group_count > 0 &&
                                  pre_group_count <= direct_group_limit;
  const bool use_partitioned_count_distinct =
      use_preassigned_build_groups && raw_aggregates.has_count_distinct &&
      fusion->aggregates.size() == 1 &&
      raw_aggregates.raw_agg_kinds[0] == RawAggKind::kCountDistinctInt64 &&
      pre_group_count > 0;
  if (use_preassigned_build_groups && !use_direct_gid_agg &&
      !use_partitioned_count_distinct) {
    use_preassigned_build_groups = false;
    identity_right_gid = false;
    right_row_gid.clear();
    pre_groups.clear();
  }
  struct GidValPair {
    uint32_t gid{0};
    int64_t val{0};
    bool operator<(const GidValPair& o) const noexcept {
      return gid != o.gid ? gid < o.gid : val < o.val;
    }
    bool operator==(const GidValPair& o) const noexcept {
      return gid == o.gid && val == o.val;
    }
  };
  const size_t agg_count =
      raw_aggregates.raw_sum_fusion ? raw_aggregates.raw_slot_count : fusion->aggregates.size();
  bool all_slot0_sum_double = raw_aggregates.raw_sum_fusion && agg_count == 1 &&
                              raw_aggregates.slot_agg_kinds[0] == RawAggKind::kSumDouble;
  if (all_slot0_sum_double) {
    for (size_t oi = 0; oi < fusion->aggregates.size(); ++oi) {
      if (raw_aggregates.raw_agg_slot[oi] == 0 &&
          raw_aggregates.raw_agg_kinds[oi] != RawAggKind::kSumDouble) {
        all_slot0_sum_double = false;
        break;
      }
    }
  }
  const bool single_double_sum =
      all_slot0_sum_double &&
      (child_shapes[raw_aggregates.raw_slot_origin[0]].direct_column >= 0 ||
       child_shapes[raw_aggregates.raw_slot_origin[0]].mul_col0 >= 0 ||
       (raw_aggregates.raw_slot_origin[0] < double_arithmetic.size() &&
        double_arithmetic[raw_aggregates.raw_slot_origin[0]]));
  // One shared group array takes a lock on every probe row. Multi-aggregate
  // probes keep a private partial per worker and fold them once. A lone
  // SUM(double) stays on the shared input-ordered log so its prefixes do
  // not change. Huge domains keep the shared array.
  const bool many_direct_groups =
      use_direct_gid_agg && pre_group_count > 4096 && workers > 1;
  const size_t private_partial_bytes =
      workers * pre_group_count * direct_state_bytes;
  // A lone SUM(double) keeps one shared array when the probe must follow
  // input order. A deferred fact probe has no row order to preserve, so each
  // worker keeps its own partial and the cells are added without a lock.
  // Floating-point SUM/AVG partials are not associative: with dynamic chunk
  // scheduling each worker folds a different subset, so the merge order (and
  // thus the answer) changes per run. Keep every DOUBLE aggregate on the
  // single shared, input-ordered fold so the result is deterministic and
  // matches the canonical accumulator. `single_double_sum` below is the lone
  // SUM(double) special case; `has_double_raw_agg` covers AVG/multi-agg shapes.
  const bool private_partials =
      many_direct_groups && !has_double_raw_agg &&
      (!single_double_sum || deferred_left_probe != nullptr ||
       left.HasColumnarOnly()) &&
      private_partial_bytes <= (size_t{256} << 20);
  const bool use_shared_direct_gid = many_direct_groups && !private_partials;
  const size_t direct_gid_worker_count =
      use_direct_gid_agg ? (use_shared_direct_gid ? 1 : workers) : 0;
  std::vector<DirectGidWorker> direct_gid_workers;
  const auto init_direct_gid_workers =
      [&](size_t worker_count, size_t domain_count,
          bool packed_double = false) -> Status {
    direct_gid_workers.resize(worker_count);
    const auto init_one = [&](DirectGidWorker& dw) {
      if (packed_double) {
        dw.double_cells.assign(domain_count, DirectDoubleCell{});
        return;
      }
      dw.matched.assign(domain_count, 0);
      if (raw_aggregates.raw_count_fusion) {
        dw.counts.assign(domain_count, 0);
      } else {
        if (has_double_raw_agg) {
          dw.doubles.assign(domain_count * agg_count, 0.0);
        }
        if (has_int_raw_agg) {
          dw.ints.assign(domain_count * agg_count, 0);
        }
        if (has_stat_raw_agg) {
          dw.ext_sx.assign(domain_count * agg_count,
                           platform::ExtendedFloat{0});
          dw.ext_sxx.assign(domain_count * agg_count,
                            platform::ExtendedFloat{0});
        }
        dw.counts.assign(domain_count * agg_count, 0);
        dw.inf_flags.assign(domain_count * agg_count, 0);
      }
    };
    if (worker_count > 1 && domain_count >= 8192) {
      return RunParallelTasks(
          worker_count, [&](size_t wi) { init_one(direct_gid_workers[wi]); });
    }
    for (DirectGidWorker& dw : direct_gid_workers) {
      init_one(dw);
    }
    return Status::kSuccess;
  };
  std::vector<std::vector<std::vector<GidValPair>>> cd_worker_parts(
      use_partitioned_count_distinct ? workers : 0);
  std::vector<std::vector<uint8_t>> cd_worker_matched(
      use_partitioned_count_distinct ? workers : 0);
  if (use_partitioned_count_distinct) {
    for (size_t w = 0; w < workers; ++w) {
      cd_worker_parts[w].resize(workers);
      cd_worker_matched[w].assign(pre_group_count, 0);
    }
  }
  std::shared_ptr<QueryMemoryCharge> ordered_sum_charge;
  std::vector<OrderedSumInput> ordered_sum_inputs;
  if (use_shared_direct_gid && single_double_sum && integer_key &&
      build_int_keys_unique && deferred_left_probe == nullptr &&
      !left.HasColumnarOnly() &&
      left.rows.size() <= SIZE_MAX / sizeof(OrderedSumInput)) {
    ordered_sum_charge =
        TryReserveQueryMemory(left.rows.size() * sizeof(OrderedSumInput));
    if (ordered_sum_charge) {
      ordered_sum_inputs.resize(left.rows.size());
    }
  }
  const auto accumulate_preassigned_pair = [&](size_t w, Row& scratch,
                                               const Row& left_row,
                                               uint32_t r_idx) -> Status {
    const Row* right_row = right_row_ptr(r_idx);
    const uint32_t gid = identity_right_gid ? r_idx : right_row_gid[r_idx];
    if (gid == kInvalidGid) [[unlikely]] {
      return Status::kSuccess;
    }
    // The group id is already the build row's. COUNT(*) never reads a cell,
    // so copying key strings into the scratch row on every fact row is wasted
    // (Q23's item-description grouping).
    if (!raw_aggregates.count_star_only && (!mask_scratch || !scratch_cells.empty())) {
      Value cell_tmp;
      for (const slot_t cell : scratch_cells) {
        scratch.values_[cell] =
            cell < left_width
                ? left_row.values_[cell]
                : read_right_cell(r_idx, cell - left_width, &cell_tmp);
      }
    }
    if (use_partitioned_count_distinct) {
      cd_worker_matched[w][gid] = 1;
      int64_t iv = 0;
      bool is_null = false;
      if (child_shapes[0].direct_column >= 0) {
        Value v_tmp;
        const Value& v = read_pair_cell(
            &left_row, r_idx,
            static_cast<slot_t>(child_shapes[0].direct_column), &v_tmp);
        is_null = v.IsNull();
        if (!is_null) {
          iv = v.value.int_value;
        }
      } else {
        ASSIGN_OR_RETURN(Value, child,
                         eval_pair_program(child_programs[0], child_shapes[0],
                                           scratch, &left_row, right_row));
        is_null = child.IsNull();
        if (!is_null) {
          iv = child.value.int_value;
        }
      }
      if (!is_null) {
        cd_worker_parts[w][gid % workers].push_back(
            GidValPair{.gid = gid, .val = iv});
      }
      return Status::kSuccess;
    }
    if (ordered_sum_charge) {
      // Unique build keys produce at most one entry for this input row.
      // Publish by input position, independently of morsel scheduling.
      auto& item =
          ordered_sum_inputs[static_cast<size_t>(&left_row - left.rows.data())];
      bool is_null = false;
      RETURN_IF_FAIL(eval_pair_double(raw_aggregates.raw_slot_origin[0], scratch, &left_row,
                                      right_row, &item.value, &is_null));
      item.gid = gid;
      item.nonnull = !is_null;
      return Status::kSuccess;
    }
    DirectGidWorker& dw = direct_gid_workers[use_shared_direct_gid ? 0 : w];
    if (fusion->aggregates.empty()) {
      if (use_shared_direct_gid) {
        __atomic_store_n(&dw.matched[gid], 1, __ATOMIC_RELAXED);
      } else {
        dw.matched[gid] = 1;
      }
      return Status::kSuccess;
    }
    if (raw_aggregates.count_star_only) {
      if (use_shared_direct_gid) {
        __atomic_store_n(&dw.matched[gid], 1, __ATOMIC_RELAXED);
        __atomic_fetch_add(&dw.counts[gid], 1, __ATOMIC_RELAXED);
      } else {
        dw.matched[gid] = 1;
        dw.counts[gid] += 1;
      }
      return Status::kSuccess;
    }
    if (raw_aggregates.count_nonnull_only) {
      const bool non_null = !raw_aggregates.count_right_column.has_value() ||
                            !right_cell_is_null(r_idx, *raw_aggregates.count_right_column);
      if (use_shared_direct_gid) {
        __atomic_store_n(&dw.matched[gid], 1, __ATOMIC_RELAXED);
        if (non_null) {
          __atomic_fetch_add(&dw.counts[gid], 1, __ATOMIC_RELAXED);
        }
      } else {
        dw.matched[gid] = 1;
        if (non_null) {
          dw.counts[gid] += 1;
        }
      }
      return Status::kSuccess;
    }
    FusedGroupLock group_lock(use_shared_direct_gid ? &dw.matched[gid]
                                                    : nullptr);
    if (!use_shared_direct_gid) {
      dw.matched[gid] = 1;
    }
    if (single_double_sum) {
      double value = 0.0;
      bool is_null = false;
      RETURN_IF_FAIL(eval_pair_double(raw_aggregates.raw_slot_origin[0], scratch, &left_row,
                                      right_row, &value, &is_null));
      if (!is_null) {
        const double previous = dw.doubles[gid];
        const double next = previous + value;
        if (std::isinf(next) && !std::isinf(previous) && !std::isinf(value)) {
          dw.inf_flags[gid] = 1;
        }
        dw.doubles[gid] = next;
        // A lone SUM observes presence only; AVG and COUNT keep exact counts
        // in the general loop. The merge also needs only zero/nonzero here.
        dw.counts[gid] = 1;
      }
      return Status::kSuccess;
    }
    const size_t base = static_cast<size_t>(gid) * agg_count;
    Value v_tmp;
    for (size_t s = 0; s < agg_count; ++s) {
      const size_t src = raw_aggregates.raw_slot_origin[s];
      if (case_always_null(src, right_row)) {
        continue;
      }
      switch (raw_aggregates.slot_agg_kinds[s]) {
        case RawAggKind::kCountStar:
          dw.counts[base + s] += 1;
          break;
        case RawAggKind::kConstantExtremum:
        case RawAggKind::kCountValue: {
          bool non_null = false;
          if (child_shapes[src].direct_column >= 0) {
            const Value& v = read_pair_cell(
                &left_row, r_idx,
                static_cast<slot_t>(child_shapes[src].direct_column), &v_tmp);
            non_null = !v.IsNull();
          } else {
            ASSIGN_OR_RETURN(
                Value, child,
                eval_pair_program(child_programs[src], child_shapes[src],
                                  scratch, &left_row, right_row));
            non_null = !child.IsNull();
          }
          if (non_null) {
            dw.counts[base + s] += 1;
          }
          break;
        }
        case RawAggKind::kSumInt64: {
          int64_t iv = 0;
          bool is_null = false;
          if (child_shapes[src].direct_column >= 0) {
            const Value& v = read_pair_cell(
                &left_row, r_idx,
                static_cast<slot_t>(child_shapes[src].direct_column), &v_tmp);
            is_null = v.IsNull();
            if (!is_null) {
              iv = v.value.int_value;
            }
          } else if (child_shapes[src].case_cond !=
                         ProgramShape::CaseCondKind::kNone &&
                     (child_shapes[src].case_val ==
                          ProgramShape::CaseValKind::kIntConst1 ||
                      child_shapes[src].case_val ==
                          ProgramShape::CaseValKind::kDirectInt64Col)) {
            bool cond_true = false;
            if (cached_case_cond(src, &left_row, right_row, &cond_true))
                [[likely]] {
              if (!cond_true) {
                is_null = child_shapes[src].case_else_null;
                iv = 0;
              } else if (child_shapes[src].case_val ==
                         ProgramShape::CaseValKind::kIntConst1) {
                iv = 1;
              } else {
                const Value& v = read_pair_cell(
                    &left_row, r_idx,
                    static_cast<slot_t>(child_shapes[src].case_val_col0),
                    &v_tmp);
                is_null = v.IsNull();
                if (!is_null) {
                  iv = v.value.int_value;
                }
              }
            } else {
              ASSIGN_OR_RETURN(
                  Value, child,
                  eval_pair_program(child_programs[src], child_shapes[src],
                                    scratch, &left_row, right_row));
              is_null = child.IsNull();
              if (!is_null) {
                iv = child.value.int_value;
              }
            }
          } else {
            ASSIGN_OR_RETURN(
                Value, child,
                eval_pair_program(child_programs[src], child_shapes[src],
                                  scratch, &left_row, right_row));
            is_null = child.IsNull();
            if (!is_null) {
              iv = child.value.int_value;
            }
          }
          if (!is_null) {
            int64_t next = 0;
            if (__builtin_add_overflow(dw.ints[base + s], iv, &next)) {
              dw.inf_flags[base + s] = 1;
            } else {
              dw.ints[base + s] = next;
            }
            dw.counts[base + s] += 1;
          }
          break;
        }
        case RawAggKind::kSumDouble:
        case RawAggKind::kAvgDouble: {
          double dv = 0.0;
          bool is_null = false;
          RETURN_IF_FAIL(eval_pair_double(src, scratch, &left_row, right_row,
                                          &dv, &is_null));
          if (!is_null) {
            const double prev = dw.doubles[base + s];
            const double next = prev + dv;
            if (std::isinf(next) && !std::isinf(prev) && !std::isinf(dv)) {
              dw.inf_flags[base + s] = 1;
            }
            dw.doubles[base + s] = next;
            dw.counts[base + s] += 1;
          }
          break;
        }
        case RawAggKind::kStddevSamp:
        case RawAggKind::kStddevPop:
        case RawAggKind::kVarSamp:
        case RawAggKind::kVarPop: {
          platform::ExtendedFloat x = 0;
          bool is_null = false;
          RETURN_IF_FAIL(
              eval_pair_ext(src, scratch, &left_row, right_row, &x, &is_null));
          if (!is_null) {
            dw.ext_sx[base + s] += x;
            dw.ext_sxx[base + s] += x * x;
            dw.counts[base + s] += 1;
          }
          break;
        }
        case RawAggKind::kCountDistinctInt64:
        case RawAggKind::kNone:
          break;
      }
    }
    return Status::kSuccess;
  };

  // A global numeric aggregate whose dimension was absorbed into the
  // runtime filter has no join key left. The fact stays columnar and every
  // surviving row charges the single group. Grouped queries keep the keyed
  // probe; a failed proof materializes the fact as before.
  const bool def_local_preds_ok =
      deferred_left_probe != nullptr &&
      (deferred_left_probe->local_preds.empty() ||
       deferred_left_probe->local_preds_covered_by_peeks);
  const bool scalar_fact_charge =
      deferred_left_probe != nullptr && singleton_right_relation &&
      fusion->group_keys.empty() && key_programs.empty() &&
      equality_keys.empty() && deferred_left_probe->pk_lookups.empty() &&
      def_local_preds_ok && deferred_left_probe->store != nullptr;
  const bool distinct_fact_singleton =
      deferred_left_probe != nullptr && singleton_right_relation &&
      equality_keys.empty() && deferred_left_probe->pk_lookups.empty() &&
      def_local_preds_ok && deferred_left_probe->store != nullptr &&
      fusion->aggregates.empty() && join_type == JoinType::kInner &&
      !fusion->having && !fusion->post_having_active &&
      fusion->topn_limit == 0 && !fusion->rollup_count_distribution &&
      fusion->output_order.empty() && result.using_columns == nullptr &&
      residual_programs.empty() && !key_programs.empty() &&
      key_programs.size() <= 2;
  if (deferred_left_probe != nullptr) {
    if ((singleton_right_relation && !scalar_fact_charge &&
         !distinct_fact_singleton) ||
        (!integer_key && !packed_2x32_int_key && !scalar_fact_charge &&
         !distinct_fact_singleton)) {
      return not_fusable("deferred_left_probe unsupported");
    }
  }

  // Preassigned groups already name the build row. Aggregates that only read
  // left-side numeric columns can charge those cells without a scratch row
  // or a bytecode evaluation.
  std::array<LeftNumericAgg, kRawAggregateCapacity> left_numeric{};
  bool left_cell_agg_ready = raw_aggregates.raw_sum_fusion && !ordered_sum_charge &&
                             !use_partitioned_count_distinct && agg_count > 0 &&
                             agg_count <= kRawAggregateCapacity;
  bool uses_double_arith_cell_op = false;
  std::vector<CompiledDefDoubleArith> compiled_double_arith(
      fusion->aggregates.size());
  const auto numeric_left_col = [&](int32_t col) {
    if (col < 0 ||
        std::cmp_greater_equal(static_cast<size_t>(col), left_width)) {
      return false;
    }
    const Column& column = left.schema.GetColumn(static_cast<size_t>(col));
    if (column.IsUnsigned()) {
      return false;
    }
    const ValueType type = column.Type();
    return type == ValueType::kInt64 || type == ValueType::kDouble;
  };
  const auto integer_left_col = [&](int32_t col) {
    if (col < 0 ||
        std::cmp_greater_equal(static_cast<size_t>(col), left_width)) {
      return false;
    }
    const Column& column = left.schema.GetColumn(static_cast<size_t>(col));
    return !column.IsUnsigned() && column.Type() == ValueType::kInt64;
  };
  // Lean proof: lean/TinylambOpt/PredicateAndRelationalLowering.lean
  //   (conditional_case_double_arith_cell_eval_sound)
  const auto compile_def_double_arith_program =
      [&](const BytecodeProgram& prog, CompiledDefDoubleArith* plan) -> bool {
    if (deferred_left_probe == nullptr) {
      return false;
    }
    for (const auto& inst : prog.Instructions()) {
      if (inst.opcode != BytecodeOp::kLoadColumn) {
        continue;
      }
      if (inst.operand < def_base_types.size()) {
        if (def_overwritten_by_pk[inst.operand] != 0 ||
            (def_base_types[inst.operand] != ValueType::kDouble &&
             def_base_types[inst.operand] != ValueType::kInt64) ||
            (def_base_types[inst.operand] == ValueType::kInt64 &&
             left.schema.GetColumn(inst.operand).IsUnsigned())) {
          return false;
        }
      } else if (!right_ptr->HasSpill() && inst.operand >= left_width &&
                 (inst.operand - left_width) < right.schema.ColumnCount() &&
                 right.schema.GetColumn(inst.operand - left_width).Type() ==
                     ValueType::kDouble) {
        continue;
      } else {
        return false;
      }
    }
    const auto& insts = prog.Instructions();
    const auto& consts = prog.Constants();
    plan->steps.clear();
    plan->steps.reserve(insts.size());
    bool can_simplify = true;
    for (const auto& inst : insts) {
      CompiledDefDoubleArith::Step st;
      if (inst.opcode == BytecodeOp::kLoadColumn) {
        if (inst.operand < def_base_types.size()) {
          st.kind = def_base_types[inst.operand] == ValueType::kInt64 ? 8 : 0;
          st.operand = inst.operand;
        } else {
          st.kind = 1;
          st.operand = static_cast<uint32_t>(inst.operand - left_width);
        }
      } else if (inst.opcode == BytecodeOp::kLoadConstant) {
        st.kind = 2;
        const Value& cv = consts[inst.operand];
        st.cst = cv.type == ValueType::kDouble
                     ? cv.value.double_value
                     : static_cast<double>(cv.value.int_value);
      } else if (inst.opcode == BytecodeOp::kBinaryDouble ||
                 inst.opcode == BytecodeOp::kBinaryInt64) {
        switch (inst.binary) {
          case BinaryOperation::kAdd:
            st.kind = 3;
            break;
          case BinaryOperation::kSubtract:
            st.kind = 4;
            break;
          case BinaryOperation::kMultiply:
            st.kind = 5;
            break;
          case BinaryOperation::kDivide:
            st.kind = 6;
            break;
          default:
            can_simplify = false;
            break;
        }
      } else {
        can_simplify = false;
      }
      if (st.kind == 6 && !plan->steps.empty() &&
          plan->steps.back().kind == 2 &&
          std::isfinite(plan->steps.back().cst) &&
          std::abs(plan->steps.back().cst) >= 1.0) {
        st.kind = 7;
        st.cst = plan->steps.back().cst;
        plan->steps.pop_back();
      }
      plan->steps.push_back(st);
    }
    if (can_simplify && plan->steps.size() == 8 && plan->steps[0].kind == 0 &&
        plan->steps[1].kind == 0 && plan->steps[2].kind == 4 &&
        plan->steps[3].kind == 0 && plan->steps[4].kind == 4 &&
        plan->steps[5].kind == 0 && plan->steps[6].kind == 3 &&
        plan->steps[7].kind == 7) {
      plan->pattern = CompiledDefDoubleArith::Pattern::kSubSubAddDivConst;
      plan->fast_cols = {plan->steps[0].operand, plan->steps[1].operand,
                         plan->steps[3].operand, plan->steps[5].operand};
      plan->fast_div = plan->steps[7].cst;
    } else if (can_simplify && plan->steps.size() == 4 &&
               plan->steps[0].kind == 0 && plan->steps[1].kind == 0 &&
               plan->steps[2].kind == 4 && plan->steps[3].kind == 7) {
      plan->pattern = CompiledDefDoubleArith::Pattern::kSubDivConst;
      plan->fast_cols[0] = plan->steps[0].operand;
      plan->fast_cols[1] = plan->steps[1].operand;
      plan->fast_div = plan->steps[3].cst;
    } else if (can_simplify && plan->steps.size() == 3 &&
               plan->steps[0].kind == 0 && plan->steps[1].kind == 0 &&
               (plan->steps[2].kind == 4 || plan->steps[2].kind == 3)) {
      plan->pattern = plan->steps[2].kind == 4
                          ? CompiledDefDoubleArith::Pattern::kSub
                          : CompiledDefDoubleArith::Pattern::kAdd;
      plan->fast_cols[0] = plan->steps[0].operand;
      plan->fast_cols[1] = plan->steps[1].operand;
    }
    return true;
  };
  if (left_cell_agg_ready) {
    const auto try_match_diff_range_case = [&](size_t agg_i,
                                               LeftNumericAgg* out_op) -> bool {
      const Expression& ch_expr = fusion->aggregates[agg_i]->Child();
      if (deferred_left_probe == nullptr || !ch_expr ||
          ch_expr->Type() != TypeTag::kCaseExp) {
        return false;
      }
      const CaseExpression& ce = ch_expr->AsCaseExpression();
      if (ce.when_clauses_.size() != 1 || !ce.when_clauses_[0].first ||
          !ce.when_clauses_[0].second ||
          ce.when_clauses_[0].second->Type() != TypeTag::kConstantValue ||
          !ce.else_clause_ ||
          ce.else_clause_->Type() != TypeTag::kConstantValue) {
        return false;
      }
      const Value& tv =
          ce.when_clauses_[0].second->AsConstantValue().GetValue();
      const Value& ev = ce.else_clause_->AsConstantValue().GetValue();
      if (tv.IsNull() || tv.type != ValueType::kInt64 || tv.IsUnsigned() ||
          tv.value.int_value != 1 || ev.IsNull() ||
          ev.type != ValueType::kInt64 || ev.IsUnsigned() ||
          ev.value.int_value != 0) {
        return false;
      }
      std::vector<Expression> cond_parts;
      const Expression& cond_root = ce.when_clauses_[0].first;
      if (cond_root->Type() == TypeTag::kBinaryExp &&
          cond_root->AsBinaryExpression().Op() == BinaryOperation::kAnd) {
        cond_parts.push_back(cond_root->AsBinaryExpression().Left());
        cond_parts.push_back(cond_root->AsBinaryExpression().Right());
      } else {
        cond_parts.push_back(cond_root);
      }
      int32_t d_col0 = -1;
      int32_t d_col1 = -1;
      bool has_lo = false;
      bool has_hi = false;
      bool lo_incl = false;
      bool hi_incl = true;
      int64_t lo_val = 0;
      int64_t hi_val = 0;
      for (const Expression& cp : cond_parts) {
        if (!cp || cp->Type() != TypeTag::kBinaryExp) {
          return false;
        }
        const auto& cb = cp->AsBinaryExpression();
        const BinaryOperation bop = cb.Op();
        if (bop != BinaryOperation::kLessThan &&
            bop != BinaryOperation::kLessThanEquals &&
            bop != BinaryOperation::kGreaterThan &&
            bop != BinaryOperation::kGreaterThanEquals) {
          return false;
        }
        if (!cb.Left() || !cb.Right() ||
            cb.Left()->Type() != TypeTag::kBinaryExp ||
            cb.Left()->AsBinaryExpression().Op() !=
                BinaryOperation::kSubtract ||
            cb.Right()->Type() != TypeTag::kConstantValue) {
          return false;
        }
        const auto& sub = cb.Left()->AsBinaryExpression();
        const Value& cv = cb.Right()->AsConstantValue().GetValue();
        if (!sub.Left() || !sub.Right() ||
            sub.Left()->Type() != TypeTag::kColumnValue ||
            sub.Right()->Type() != TypeTag::kColumnValue || cv.IsNull() ||
            cv.type != ValueType::kInt64 || cv.IsUnsigned()) {
          return false;
        }
        const auto o0 = LocalColumnOffset(
            left.schema, sub.Left()->AsColumnValue().GetColumnName());
        const auto o1 = LocalColumnOffset(
            left.schema, sub.Right()->AsColumnValue().GetColumnName());
        if (!o0.has_value() || !o1.has_value()) {
          return false;
        }
        const auto c0 = static_cast<int32_t>(*o0);
        const auto c1 = static_cast<int32_t>(*o1);
        if (!integer_left_col(c0) || !integer_left_col(c1)) {
          return false;
        }
        if (d_col0 < 0) {
          d_col0 = c0;
          d_col1 = c1;
        } else if (d_col0 != c0 || d_col1 != c1) {
          return false;
        }
        if (bop == BinaryOperation::kGreaterThan ||
            bop == BinaryOperation::kGreaterThanEquals) {
          if (has_lo) {
            return false;
          }
          has_lo = true;
          lo_incl = (bop == BinaryOperation::kGreaterThanEquals);
          lo_val = cv.value.int_value;
        } else {
          if (has_hi) {
            return false;
          }
          has_hi = true;
          hi_incl = (bop == BinaryOperation::kLessThanEquals);
          hi_val = cv.value.int_value;
        }
      }
      if (d_col0 < 0 || d_col1 < 0 || (!has_lo && !has_hi)) {
        return false;
      }
      out_op->op = 15;
      out_op->col0 = d_col0;
      out_op->col1 = d_col1;
      out_op->diff_has_lo = has_lo;
      out_op->diff_has_hi = has_hi;
      out_op->diff_lo_incl = lo_incl;
      out_op->diff_hi_incl = hi_incl;
      out_op->diff_lo = lo_val;
      out_op->diff_hi = hi_val;
      uses_double_arith_cell_op = true;
      return true;
    };
    for (size_t s = 0; s < agg_count; ++s) {
      const size_t i = raw_aggregates.raw_slot_origin[s];
      if (child_shapes[i].profit_col0 >= 0) {
        left_cell_agg_ready = false;
        break;
      }
      LeftNumericAgg& op = left_numeric[s];
      op.orig_agg = static_cast<int32_t>(i);
      if (raw_aggregates.slot_agg_kinds[s] == RawAggKind::kSumInt64 &&
          try_match_diff_range_case(i, &op)) {
        continue;
      }
      if (child_shapes[i].case_cond != ProgramShape::CaseCondKind::kNone) {
        const ProgramShape& sh = child_shapes[i];
        if (right_case_conditions[i].empty() || !right_case_complete[i]) {
          // Deferred PK-column CASE: WHEN pk_int_col = const THEN double_arith
          // ELSE 0/NULL END (e.g. TPC-DS Q66 12-month pivot on date_dim.d_moy).
          // Lean proof: lean/TinylambOpt/VectorizedJoinAndPruning.lean
          //   (pk_eq_case_double_arith_sound)
          if (deferred_left_probe != nullptr &&
              sh.case_cond == ProgramShape::CaseCondKind::kColEqInt &&
              sh.case_cond_col >= 0 &&
              static_cast<size_t>(sh.case_cond_col) <
                  def_col_to_pk_lookup.size() &&
              (raw_aggregates.slot_agg_kinds[s] == RawAggKind::kSumDouble ||
               raw_aggregates.slot_agg_kinds[s] == RawAggKind::kAvgDouble) &&
              sh.case_val == ProgramShape::CaseValKind::kDoubleArith &&
              sh.case_val_program != nullptr) {
            const int8_t pk_li =
                def_col_to_pk_lookup[static_cast<size_t>(sh.case_cond_col)];
            if (pk_li >= 0 &&
                static_cast<size_t>(pk_li) < prepared_pk_lookups.size() &&
                !prepared_pk_lookups[static_cast<size_t>(pk_li)]
                     .payload_unsigned &&
                prepared_pk_lookups[static_cast<size_t>(pk_li)].payload_type ==
                    ValueType::kInt64 &&
                compile_def_double_arith_program(*sh.case_val_program,
                                                 &compiled_double_arith[i])) {
              op.op = 16;
              op.case_else_null = sh.case_else_null;
              op.case_val_kind = 4;
              op.pk_lookup_idx = static_cast<uint8_t>(pk_li);
              op.pk_eq_val = sh.case_cond_int;
              uses_double_arith_cell_op = true;
              continue;
            }
          }
          left_cell_agg_ready = false;
          break;
        }
        op.case_else_null = sh.case_else_null;
        op.right_cond = right_case_conditions[i].data();
        bool val_ok = false;
        switch (sh.case_val) {
          case ProgramShape::CaseValKind::kDirectDoubleCol:
            if (numeric_left_col(sh.case_val_col0)) {
              op.col0 = sh.case_val_col0;
              op.case_val_kind = 2;
              val_ok = true;
            }
            break;
          case ProgramShape::CaseValKind::kMulOneMinus:
            if (numeric_left_col(sh.case_val_col0) &&
                numeric_left_col(sh.case_val_col1)) {
              op.col0 = sh.case_val_col0;
              op.col1 = sh.case_val_col1;
              op.scale = sh.case_val_const;
              op.case_val_kind = 3;
              val_ok = true;
            }
            break;
          case ProgramShape::CaseValKind::kIntConst1:
            op.case_val_kind = 1;
            val_ok = true;
            break;
          case ProgramShape::CaseValKind::kDoubleArith:
            if (sh.case_val_program != nullptr &&
                compile_def_double_arith_program(*sh.case_val_program,
                                                 &compiled_double_arith[i])) {
              op.case_val_kind = 4;
              uses_double_arith_cell_op = true;
              val_ok = true;
            }
            break;
          case ProgramShape::CaseValKind::kDirectInt64Col:
          case ProgramShape::CaseValKind::kNone:
            break;
        }
        if (!val_ok) {
          left_cell_agg_ready = false;
          break;
        }
        switch (raw_aggregates.slot_agg_kinds[s]) {
          case RawAggKind::kCountValue:
            if (op.case_val_kind == 4) {
              // This operand needs arithmetic evaluation, including its
              // error semantics, before deciding whether it counts.
              left_cell_agg_ready = false;
            } else {
              op.op = 13;
            }
            break;
          case RawAggKind::kSumInt64:
            if (op.case_val_kind == 1) {
              op.op = 12;
            } else {
              left_cell_agg_ready = false;
            }
            break;
          case RawAggKind::kSumDouble:
          case RawAggKind::kAvgDouble:
            if (op.case_val_kind == 2) {
              op.op = 10;
            } else if (op.case_val_kind == 3) {
              op.op = 11;
            } else if (op.case_val_kind == 4) {
              op.op = 14;
            } else {
              left_cell_agg_ready = false;
            }
            break;
          default:
            left_cell_agg_ready = false;
            break;
        }
        if (!left_cell_agg_ready) {
          break;
        }
        continue;
      }
      switch (raw_aggregates.slot_agg_kinds[s]) {
        case RawAggKind::kCountStar:
          op.op = 0;
          break;
        case RawAggKind::kConstantExtremum:
          if (raw_aggregates.raw_constants[i].IsNull()) {
            op.op = 5;
          } else if (join_type == JoinType::kInner) {
            // Every joined pair carries the uniform value, so "some pair
            // exists" decides the extremum.
            op.op = 0;
          } else {
            // An outer join's NULL-padded pair carries NULL, not the uniform
            // value: counting it as a pair would turn MIN/MAX of a group
            // with no match into the constant.  Let the general path count
            // non-NULL cells.
            left_cell_agg_ready = false;
          }
          break;
        case RawAggKind::kCountValue:
          if (numeric_left_col(child_shapes[i].direct_column)) {
            op.op = 4;
            op.col0 = child_shapes[i].direct_column;
          } else if (numeric_left_col(child_shapes[i].mul_col0) &&
                     numeric_left_col(child_shapes[i].mul_col1)) {
            // COUNT(a * b): the product is NULL exactly when either factor is.
            op.op = 7;
            op.col0 = child_shapes[i].mul_col0;
            op.col1 = child_shapes[i].mul_col1;
          } else {
            left_cell_agg_ready = false;
          }
          break;
        case RawAggKind::kSumInt64:
          if (!integer_left_col(child_shapes[i].direct_column)) {
            left_cell_agg_ready = false;
          } else {
            op.op = 3;
            op.col0 = child_shapes[i].direct_column;
          }
          break;
        case RawAggKind::kSumDouble:
        case RawAggKind::kAvgDouble:
          if (child_shapes[i].mul_col0 >= 0 &&
              numeric_left_col(child_shapes[i].mul_col0) &&
              numeric_left_col(child_shapes[i].mul_col1)) {
            op.op = 1;
            op.col0 = child_shapes[i].mul_col0;
            op.col1 = child_shapes[i].mul_col1;
          } else if (child_shapes[i].coalesce_mul_col0 >= 0 &&
                     numeric_left_col(child_shapes[i].coalesce_mul_col0) &&
                     numeric_left_col(child_shapes[i].coalesce_mul_col1)) {
            op.op = 17;
            op.col0 = child_shapes[i].coalesce_mul_col0;
            op.col1 = child_shapes[i].coalesce_mul_col1;
          } else if (child_shapes[i].mul_one_minus_col0 >= 0 &&
                     numeric_left_col(child_shapes[i].mul_one_minus_col0) &&
                     numeric_left_col(child_shapes[i].mul_one_minus_col1)) {
            op.op = 6;
            op.col0 = child_shapes[i].mul_one_minus_col0;
            op.col1 = child_shapes[i].mul_one_minus_col1;
            op.scale = child_shapes[i].mul_one_minus_const;
          } else if (numeric_left_col(child_shapes[i].direct_column)) {
            op.op = 2;
            op.col0 = child_shapes[i].direct_column;
          } else if (deferred_left_probe != nullptr &&
                     i < double_arithmetic.size() && double_arithmetic[i]) {
            if (compile_def_double_arith_program(child_programs[i],
                                                 &compiled_double_arith[i])) {
              op.op = 9;
              uses_double_arith_cell_op = true;
            } else {
              left_cell_agg_ready = false;
            }
          } else {
            left_cell_agg_ready = false;
          }
          break;
        case RawAggKind::kStddevSamp:
        case RawAggKind::kStddevPop:
        case RawAggKind::kVarSamp:
        case RawAggKind::kVarPop:
          if (numeric_left_col(child_shapes[i].direct_column)) {
            op.op = 8;
            op.col0 = child_shapes[i].direct_column;
            op.is_int_col = integer_left_col(child_shapes[i].direct_column);
          } else {
            left_cell_agg_ready = false;
          }
          break;
        default:
          left_cell_agg_ready = false;
          break;
      }
      if (!left_cell_agg_ready) {
        break;
      }
    }
  }
  bool left_numeric_probe = left_cell_agg_ready && !uses_double_arith_cell_op;
  // Lean proof:
  // lean/TinylambOpt/ResearchProofs/QueryRewrites/SurrogateKeyRollupsAndSetCounts.lean
  //   (DisjointRangeBucketHistogram_sound)
  // Detect a 5-bucket disjoint shipping-delay histogram over the same (col0 -
  // col1) pair (TPC-DS Q62 / Q99: <=30, (30,60], (60,90], (90,120], >120) so
  // each matching probe row decodes (col0, col1) once, subtracts once, and
  // increments the single matching bucket via ClassifyDisjointRangeBucket5.
  bool disjoint_bucket5_ready = false;
  int32_t disjoint_bucket5_col0 = -1;
  int32_t disjoint_bucket5_col1 = -1;
  std::array<uint8_t, 5> disjoint_bucket5_slots{};
  if (left_cell_agg_ready && agg_count == 5) {
    bool ok5 = true;
    int32_t c0 = -1;
    int32_t c1 = -1;
    uint8_t seen_mask = 0;
    for (size_t s = 0; s < 5; ++s) {
      const LeftNumericAgg& op = left_numeric[s];
      if (op.op != 15) {
        ok5 = false;
        break;
      }
      if (s == 0) {
        c0 = op.col0;
        c1 = op.col1;
      } else if (op.col0 != c0 || op.col1 != c1) {
        ok5 = false;
        break;
      }
      uint8_t b = 255;
      if (!op.diff_has_lo && op.diff_has_hi && op.diff_hi_incl &&
          op.diff_hi == 30) {
        b = 0;
      } else if (op.diff_has_lo && !op.diff_lo_incl && op.diff_lo == 30 &&
                 op.diff_has_hi && op.diff_hi_incl && op.diff_hi == 60) {
        b = 1;
      } else if (op.diff_has_lo && !op.diff_lo_incl && op.diff_lo == 60 &&
                 op.diff_has_hi && op.diff_hi_incl && op.diff_hi == 90) {
        b = 2;
      } else if (op.diff_has_lo && !op.diff_lo_incl && op.diff_lo == 90 &&
                 op.diff_has_hi && op.diff_hi_incl && op.diff_hi == 120) {
        b = 3;
      } else if (op.diff_has_lo && !op.diff_lo_incl && op.diff_lo == 120 &&
                 !op.diff_has_hi) {
        b = 4;
      }
      if (b >= 5 || (seen_mask & (1U << b)) != 0U) {
        ok5 = false;
        break;
      }
      seen_mask |= static_cast<uint8_t>(1U << b);
      disjoint_bucket5_slots[b] = static_cast<uint8_t>(s);
    }
    if (ok5 && seen_mask == 0x1FU) {
      disjoint_bucket5_ready = true;
      disjoint_bucket5_col0 = c0;
      disjoint_bucket5_col1 = c1;
    }
  }
  if (!use_direct_gid_agg || use_shared_direct_gid) {
    left_numeric_probe = false;
  }
  if (scalar_fact_charge) {
    bool columns_ready = left_numeric_probe && prepared_pk_lookups.empty();
    for (size_t i = 0; columns_ready && i < agg_count; ++i) {
      const LeftNumericAgg& op = left_numeric[i];
      const auto in_proj = [&](int32_t col) {
        return col < 0 ||
               (static_cast<size_t>(col) <
                    deferred_left_probe->projection.size() &&
                def_overwritten_by_pk[static_cast<size_t>(col)] == 0);
      };
      columns_ready = in_proj(op.col0) && in_proj(op.col1);
    }
    if (!columns_ready) {
      return not_fusable("scalar fact charge");
    }
  }
  const bool def_compact_split_probe = deferred_left_probe != nullptr &&
                                       compact_split_key_only &&
                                       residual_programs.empty();
  bool def_group_cell_probe =
      deferred_left_probe != nullptr && !use_preassigned_build_groups &&
      !compact_counts && residual_programs.empty() && raw_aggregates.raw_sum_fusion &&
      left_cell_agg_ready && !raw_aggregates.has_count_distinct && all_direct_group_keys &&
      (int_group_key || layout.packed_2x32_group_key ||
       (layout.use_split_key_gid && !layout.use_fast_2key_year_lut));
  if (def_group_cell_probe) {
    const size_t base_proj_n = deferred_left_probe->projection.size();
    for (size_t i = 0; i < agg_count; ++i) {
      const LeftNumericAgg& op = left_numeric[i];
      if ((op.col0 >= 0 &&
           (std::cmp_greater_equal(op.col0, base_proj_n) ||
            def_overwritten_by_pk[static_cast<size_t>(op.col0)] != 0)) ||
          (op.col1 >= 0 &&
           (std::cmp_greater_equal(op.col1, base_proj_n) ||
            def_overwritten_by_pk[static_cast<size_t>(op.col1)] != 0))) {
        def_group_cell_probe = false;
        break;
      }
    }
  }
  if (def_group_cell_probe || def_compact_split_probe) {
    std::array<uint8_t, 16> eager_col{};
    for (const slot_t lk : left_columns) {
      if (lk < 16) {
        eager_col[lk] = 1;
      }
    }
    if (def_group_cell_probe) {
      for (size_t i = 0; i < agg_count; ++i) {
        const LeftNumericAgg& op = left_numeric[i];
        if (op.col0 >= 0 && op.col0 < 16) {
          eager_col[static_cast<size_t>(op.col0)] = 1;
        }
        if (op.col1 >= 0 && op.col1 < 16) {
          eager_col[static_cast<size_t>(op.col1)] = 1;
        }
        if (op.op == 9 || op.op == 14) {
          for (const auto& st :
               compiled_double_arith[static_cast<size_t>(op.orig_agg)].steps) {
            if ((st.kind == 0 || st.kind == 8) && st.operand < 16) {
              eager_col[st.operand] = 1;
            }
          }
        }
      }
    }
    for (PreparedDeferredPkGroup& grp : def_pk_groups) {
      if (!grp.use_subgid) {
        continue;
      }
      bool all_lazy = true;
      for (const size_t li : grp.lookup_indices) {
        const slot_t tc = prepared_pk_lookups[li].target_proj_col;
        if (tc < 16 && eager_col[tc] != 0) {
          all_lazy = false;
          break;
        }
      }
      grp.lazy_payloads = all_lazy;
    }
  }
  std::vector<size_t> dense_pk_group_indices;
  dense_pk_group_indices.reserve(def_pk_groups.size());
  size_t dense_split_domain = 0;
  std::shared_ptr<QueryMemoryCharge> dense_split_charge;
  bool use_dense_split_direct_gid =
      def_group_cell_probe && layout.use_split_key_gid && !layout.split_has_left_probe &&
      layout.split_key_unpack_only && join_type == JoinType::kInner &&
      !fusion->aggregates.empty() && layout.right_row_count > 0 &&
      (!layout.split_uses_right_subgid ||
       layout.split_right_rep_count == layout.split_right_subgid_count);
  bool use_packed_double_cells = use_dense_split_direct_gid &&
                                 !use_direct_gid_agg && !raw_aggregates.raw_count_fusion &&
                                 agg_count == 1 &&
                                 (raw_aggregates.slot_agg_kinds[0] == RawAggKind::kSumDouble ||
                                  raw_aggregates.slot_agg_kinds[0] == RawAggKind::kAvgDouble);
  if (use_dense_split_direct_gid) {
    uint64_t dom = layout.split_uses_right_subgid ? layout.split_right_subgid_count : 1ULL;
    for (size_t gi = 0; gi < def_pk_groups.size(); ++gi) {
      if (!def_pk_groups[gi].use_subgid) {
        continue;
      }
      const uint32_t card = def_pk_groups[gi].subgid_count;
      if (card == 0 || dom > 524288ULL / card) {
        dom = 0;
        break;
      }
      dom *= card;
      dense_pk_group_indices.push_back(gi);
    }
    if (dom == 0 || dom > 524288ULL) {
      use_dense_split_direct_gid = false;
      use_packed_double_cells = false;
    } else {
      const size_t per_group_bytes =
          use_packed_double_cells
              ? sizeof(DirectDoubleCell)
              : (1 + (agg_count *
                      (8 + 1 + (has_double_raw_agg ? 8 : 0) +
                       (has_int_raw_agg ? 8 : 0) +
                       (has_stat_raw_agg ? 2 * sizeof(platform::ExtendedFloat)
                                         : 0))));
      const size_t bytes = workers * static_cast<size_t>(dom) *
                           std::max<size_t>(per_group_bytes, 16);
      const size_t dense_byte_cap = (!raw_aggregates.raw_count_fusion && has_stat_raw_agg)
                                        ? (size_t{192} << 20)
                                        : (size_t{256} << 20);
      if (bytes <= dense_byte_cap) {
        dense_split_charge = TryReserveQueryMemory(bytes);
      }
      if (dense_split_charge) {
        dense_split_domain = static_cast<size_t>(dom);
        RETURN_IF_FAIL(init_direct_gid_workers(workers, dense_split_domain,
                                               use_packed_double_cells));
      } else {
        use_dense_split_direct_gid = false;
        use_packed_double_cells = false;
      }
    }
  }
  JitDenseSplitPlan jit_dense_split;
  if (!use_dense_split_direct_gid && !use_direct_gid_agg &&
      def_group_cell_probe && layout.use_split_key_gid && !layout.split_has_left_probe &&
      layout.split_key_unpack_only && join_type == JoinType::kInner &&
      deferred_left_probe != nullptr && !raw_aggregates.raw_count_fusion && raw_aggregates.raw_sum_fusion &&
      agg_count == 1 && fusion->aggregates.size() == 1 &&
      (raw_aggregates.slot_agg_kinds[0] == RawAggKind::kSumDouble ||
       raw_aggregates.slot_agg_kinds[0] == RawAggKind::kAvgDouble) &&
      use_dense_int_rows && !dense_int_idxs.empty() &&
      ranked_int_bits.empty() && left_columns.size() == 1 &&
      left_key_offsets[0] == 0 && right_col_only && layout.right_row_count > 0 &&
      (!layout.split_uses_right_subgid ||
       layout.split_right_rep_count == layout.split_right_subgid_count) &&
      def_pk_groups.size() == 1 && def_pk_groups[0].use_subgid &&
      def_pk_groups[0].dense_ptr != nullptr &&
      def_pk_groups[0].dense_size == def_pk_groups[0].span &&
      def_pk_groups[0].total_rows > 0 &&
      def_pk_groups[0].max_k >= def_pk_groups[0].min_k &&
      def_pk_groups[0].subgid_rep_ptr != nullptr &&
      def_pk_groups[0].subgid_count > 0 && fusion->columnar_output &&
      fusion->output_sink == nullptr && result.using_columns == nullptr &&
      !fusion->having && !fusion->post_having_active &&
      fusion->topn_limit == 0 && !fusion->rollup_count_distribution &&
      !result.HasSpill() && !fusion->select_items.empty() &&
      ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_JIT_DENSE_SPLIT") ==
          nullptr) {
    const auto& pk0 = def_pk_groups[0];
    const uint64_t r_card =
        layout.split_uses_right_subgid ? layout.split_right_subgid_count : 1ULL;
    const uint64_t dom =
        (pk0.subgid_count > 0 && r_card <= 2097152ULL / pk0.subgid_count)
            ? r_card * pk0.subgid_count
            : 0ULL;
    const size_t base_proj_n = deferred_left_probe->projection.size();
    const slot_t jk0_slot = left_columns[0];
    if (dom > 524288ULL && dom <= 2097152ULL && jk0_slot < base_proj_n &&
        pk0.pk_col < base_proj_n) {
      bool expr_ok = true;
      std::array<bool, 4> need_v{false, false, false, false};
      std::array<ValueType, 4> expected_vt{
          ValueType::kDouble, ValueType::kDouble, ValueType::kDouble,
          ValueType::kDouble};
      const LeftNumericAgg& lop = left_numeric[0];
      if (lop.op == 2 && lop.col0 >= 0 &&
          std::cmp_less(lop.col0, base_proj_n) &&
          !left.schema.GetColumn(static_cast<size_t>(lop.col0)).IsUnsigned()) {
        jit_dense_split.v_cols[0] = static_cast<size_t>(lop.col0);
        need_v[0] = true;
        if (def_base_types[jit_dense_split.v_cols[0]] == ValueType::kDouble) {
          jit_dense_split.expr_op = JitDenseSplitRadixAgg::ExprOp::kColDouble;
          expected_vt[0] = ValueType::kDouble;
        } else if (def_base_types[jit_dense_split.v_cols[0]] ==
                   ValueType::kInt64) {
          jit_dense_split.expr_op = JitDenseSplitRadixAgg::ExprOp::kColInt64;
          expected_vt[0] = ValueType::kInt64;
        } else {
          expr_ok = false;
        }
      } else if ((lop.op == 1 || lop.op == 17) && lop.col0 >= 0 &&
                 lop.col1 >= 0 && std::cmp_less(lop.col0, base_proj_n) &&
                 std::cmp_less(lop.col1, base_proj_n) &&
                 !left.schema.GetColumn(static_cast<size_t>(lop.col0))
                      .IsUnsigned() &&
                 !left.schema.GetColumn(static_cast<size_t>(lop.col1))
                      .IsUnsigned()) {
        const auto c0 = static_cast<size_t>(lop.col0);
        const auto c1 = static_cast<size_t>(lop.col1);
        const ValueType t0 = def_base_types[c0];
        const ValueType t1 = def_base_types[c1];
        const bool is_coal = (lop.op == 17);
        need_v[0] = true;
        need_v[1] = true;
        if (t0 == ValueType::kDouble && t1 == ValueType::kInt64) {
          jit_dense_split.v_cols[0] = c0;
          jit_dense_split.v_cols[1] = c1;
          expected_vt[0] = ValueType::kDouble;
          expected_vt[1] = ValueType::kInt64;
          jit_dense_split.expr_op =
              is_coal ? JitDenseSplitRadixAgg::ExprOp::kCoalesceMulZero
                      : JitDenseSplitRadixAgg::ExprOp::kMulDoubleInt;
        } else if (t0 == ValueType::kInt64 && t1 == ValueType::kDouble) {
          jit_dense_split.v_cols[0] = c1;
          jit_dense_split.v_cols[1] = c0;
          expected_vt[0] = ValueType::kDouble;
          expected_vt[1] = ValueType::kInt64;
          jit_dense_split.expr_op =
              is_coal ? JitDenseSplitRadixAgg::ExprOp::kCoalesceMulZero
                      : JitDenseSplitRadixAgg::ExprOp::kMulDoubleInt;
        } else if (t0 == ValueType::kDouble && t1 == ValueType::kDouble) {
          jit_dense_split.v_cols[0] = c0;
          jit_dense_split.v_cols[1] = c1;
          expected_vt[0] = ValueType::kDouble;
          expected_vt[1] = ValueType::kDouble;
          jit_dense_split.expr_op =
              is_coal ? JitDenseSplitRadixAgg::ExprOp::kCoalesceMulDoubleZero
                      : JitDenseSplitRadixAgg::ExprOp::kMulDoubleDouble;
        } else if (t0 == ValueType::kInt64 && t1 == ValueType::kInt64) {
          jit_dense_split.v_cols[0] = c0;
          jit_dense_split.v_cols[1] = c1;
          expected_vt[0] = ValueType::kInt64;
          expected_vt[1] = ValueType::kInt64;
          jit_dense_split.expr_op =
              is_coal ? JitDenseSplitRadixAgg::ExprOp::kCoalesceMulIntZero
                      : JitDenseSplitRadixAgg::ExprOp::kMulIntInt;
        } else {
          expr_ok = false;
        }
      } else if (lop.op == 9) {
        const auto& da =
            compiled_double_arith[static_cast<size_t>(lop.orig_agg)];
        size_t nv = 0;
        switch (da.pattern) {
          case CompiledDefDoubleArith::Pattern::kSub:
            jit_dense_split.expr_op = JitDenseSplitRadixAgg::ExprOp::kSubDouble;
            nv = 2;
            break;
          case CompiledDefDoubleArith::Pattern::kAdd:
            jit_dense_split.expr_op = JitDenseSplitRadixAgg::ExprOp::kAddDouble;
            nv = 2;
            break;
          case CompiledDefDoubleArith::Pattern::kSubDivConst:
            jit_dense_split.expr_op =
                JitDenseSplitRadixAgg::ExprOp::kSubDivConst;
            jit_dense_split.div_const = da.fast_div;
            nv = 2;
            expr_ok = (da.fast_div != 0.0);
            break;
          case CompiledDefDoubleArith::Pattern::kSubSubAddDivConst:
            jit_dense_split.expr_op =
                JitDenseSplitRadixAgg::ExprOp::kSubSubAddDivConst;
            jit_dense_split.div_const = da.fast_div;
            nv = 4;
            expr_ok = (da.fast_div != 0.0);
            break;
          default:
            expr_ok = false;
            break;
        }
        for (size_t vi = 0; expr_ok && vi < nv; ++vi) {
          const uint32_t c = da.fast_cols[vi];
          if (c >= base_proj_n || def_base_types[c] != ValueType::kDouble) {
            expr_ok = false;
            break;
          }
          jit_dense_split.v_cols[vi] = c;
          need_v[vi] = true;
          expected_vt[vi] = ValueType::kDouble;
        }
      } else {
        expr_ok = false;
      }
      if (expr_ok) {
        const LSTableStore& store = *deferred_left_probe->store;
        const auto col_raw_ok = [&](size_t proj_col, ValueType req_vt,
                                    bool check_exact_vt) {
          const slot_t full_c = deferred_left_probe->projection[proj_col];
          const bool lazy_col = store.ColumnIsLazy(full_c);
          return std::ranges::all_of(
              store.blocks, [&](const LSTablePageBlock& blk) {
                if (blk.row_count == 0) {
                  return true;
                }
                if (full_c >= blk.columns.size()) {
                  return false;
                }
                const LSTableColumnBlock& cb = blk.columns[full_c];
                if (cb.type == ValueType::kNull) {
                  return true;
                }
                if (!lazy_col && cb.int_cells.size() < blk.row_count) {
                  return false;
                }
                if (!lazy_col && cb.has_null &&
                    cb.Nulls().size() < blk.row_count) {
                  return false;
                }
                if (check_exact_vt) {
                  if (cb.type != req_vt) {
                    return false;
                  }
                } else {
                  if (cb.type != ValueType::kInt64 &&
                      cb.type != ValueType::kDate) {
                    return false;
                  }
                }
                return true;
              });
        };
        expr_ok = col_raw_ok(jk0_slot, ValueType::kInt64, false) &&
                  col_raw_ok(pk0.pk_col, ValueType::kInt64, false);
        for (size_t vi = 0; expr_ok && vi < 4; ++vi) {
          if (need_v[vi] &&
              !col_raw_ok(jit_dense_split.v_cols[vi], expected_vt[vi], true)) {
            expr_ok = false;
          }
        }
        jit_dense_split.need_v = need_v;
      }
      if (expr_ok) {
        // Pre-summarize right columns once to preserve exact uniform/null
        // flags.
        struct RightSummary {
          bool all_uniform{true};
          bool any_null{false};
          bool all_null{true};
          ValueType vt{ValueType::kNull};
          bool is_unsigned{false};
          bool is_interval{false};
        };
        const size_t rw = right.schema.ColumnCount();
        std::vector<RightSummary> rsums(rw);
        std::vector<bool> r_init(rw, false);
        for (const ColumnarChunk& rch : right_arg.columnar->chunks) {
          if (rch.rows == 0) {
            continue;
          }
          for (size_t rc = 0; rc < rw && rc < rch.columns.size(); ++rc) {
            const ColumnarColumn& col = rch.columns[rc];
            RightSummary& s = rsums[rc];
            if (col.has_null || col.type == ValueType::kNull ||
                (col.encoding == ColumnarColumn::Encoding::kConstant &&
                 col.const_is_null)) {
              s.any_null = true;
            }
            if (!col.is_uniform) {
              s.all_uniform = false;
              s.all_null = false;
              continue;
            }
            if (col.type != ValueType::kNull) {
              s.all_null = false;
              if (!r_init[rc]) {
                s.vt = col.type;
                s.is_unsigned = col.is_unsigned;
                s.is_interval = col.is_interval;
                r_init[rc] = true;
              } else if (s.vt != col.type || s.is_unsigned != col.is_unsigned ||
                         s.is_interval != col.is_interval) {
                s.all_uniform = false;
              }
            }
          }
        }
        jit_dense_split.sel_items.reserve(fusion->select_items.size());
        for (size_t si = 0; si < fusion->select_items.size(); ++si) {
          const Expression& expr = fusion->select_items[si];
          std::string nm =
              si < fusion->select_names.size() ? fusion->select_names[si] : "";
          if (!expr) {
            expr_ok = false;
            break;
          }
          if (expr->Type() == TypeTag::kConstantValue &&
              !ContainsParameterSlot(expr)) {
            const Value& cv = expr->AsConstantValue().GetValue();
            if (cv.IsCaseInsensitive() || cv.type == ValueType::kArray ||
                (!cv.IsNull() && !cv.IsUnsigned() && cv.Collation() != 0)) {
              expr_ok = false;
              break;
            }
            jit_dense_split.sel_items.push_back(JitDenseSplitSelItem{
                .kind = 1,
                .constant = cv,
                .idx = 0,
                .name = std::move(nm),
                .vt = cv.IsNull() ? ValueType::kNull : cv.type,
                .is_unsigned = cv.IsUnsigned(),
                .is_interval = cv.IsInterval(),
                .has_null = cv.IsNull(),
                .is_uniform = true});
            continue;
          }
          if (expr->Type() == TypeTag::kAggregateExp &&
              &expr->AsAggregateExpression() == fusion->aggregates[0]) {
            jit_dense_split.sel_items.push_back(
                JitDenseSplitSelItem{.kind = 0,
                                     .constant = Value(),
                                     .idx = 0,
                                     .name = std::move(nm),
                                     .vt = ValueType::kDouble,
                                     .is_unsigned = false,
                                     .is_interval = false,
                                     .has_null = false,
                                     .is_uniform = true});
            continue;
          }
          if (expr->Type() == TypeTag::kColumnValue) {
            const auto off = LocalColumnOffset(
                result.schema, expr->AsColumnValue().GetColumnName());
            if (!off.has_value()) {
              expr_ok = false;
              break;
            }
            if (*off >= left_width && (*off - left_width) < rw) {
              const size_t rc = *off - left_width;
              const RightSummary& rs = rsums[rc];
              if (!rs.all_uniform) {
                expr_ok = false;
                break;
              }
              const Column& rcol_def = right.schema.GetColumn(rc);
              const ValueType out_vt =
                  rs.all_null
                      ? ValueType::kNull
                      : (rs.vt != ValueType::kNull ? rs.vt : rcol_def.Type());
              jit_dense_split.sel_items.push_back(
                  JitDenseSplitSelItem{.kind = 2,
                                       .constant = Value(),
                                       .idx = rc,
                                       .name = std::move(nm),
                                       .vt = out_vt,
                                       .is_unsigned = rs.is_unsigned,
                                       .is_interval = rs.is_interval,
                                       .has_null = rs.any_null,
                                       .is_uniform = rs.all_uniform});
              continue;
            }
            if (*off < left_width && *off < 16 &&
                def_col_to_pk_lookup[*off] >= 0) {
              const auto li = static_cast<size_t>(
                  static_cast<unsigned char>(def_col_to_pk_lookup[*off]));
              const PreparedDeferredPkLookup& lkp = prepared_pk_lookups[li];
              if (lkp.group_idx == 0 && !lkp.blocks.empty() &&
                  (lkp.payload_type != ValueType::kVarChar ||
                   pk0.store_owner != nullptr) &&
                  (lkp.payload_type == ValueType::kInt64 ||
                   lkp.payload_type == ValueType::kDate ||
                   lkp.payload_type == ValueType::kDouble ||
                   lkp.payload_type == ValueType::kVarChar)) {
                jit_dense_split.sel_items.push_back(
                    JitDenseSplitSelItem{.kind = 3,
                                         .constant = Value(),
                                         .idx = li,
                                         .name = std::move(nm),
                                         .vt = lkp.payload_type,
                                         .is_unsigned = lkp.payload_unsigned,
                                         .is_interval = false,
                                         .has_null = false,
                                         .is_uniform = true});
                continue;
              }
            }
            std::optional<size_t> eq_rc;
            if (*off < left_width && *off < left.schema.ColumnCount() &&
                (!layout.split_uses_right_subgid ||
                 layout.split_right_subgid_count == layout.right_row_count)) {
              const Column& lc = left.schema.GetColumn(*off);
              if (lc.Type() != ValueType::kNull &&
                  lc.Type() != ValueType::kVarChar) {
                for (const EqualityKey& eq : equality_keys) {
                  if (eq.left == *off && eq.left_offset == 0 &&
                      eq.right_offset == 0 && !eq.null_safe && eq.right < rw) {
                    const Column& rc_def = right.schema.GetColumn(eq.right);
                    if (lc.Type() == rc_def.Type() &&
                        lc.IsUnsigned() == rc_def.IsUnsigned() &&
                        rc_def.Type() != ValueType::kVarChar) {
                      eq_rc = eq.right;
                      break;
                    }
                  }
                }
              }
            }
            if (eq_rc.has_value()) {
              const size_t rc = *eq_rc;
              const RightSummary& rs = rsums[rc];
              if (!rs.all_uniform) {
                expr_ok = false;
                break;
              }
              const Column& rcol_def = right.schema.GetColumn(rc);
              const ValueType out_vt =
                  rs.all_null
                      ? ValueType::kNull
                      : (rs.vt != ValueType::kNull ? rs.vt : rcol_def.Type());
              jit_dense_split.sel_items.push_back(
                  JitDenseSplitSelItem{.kind = 2,
                                       .constant = Value(),
                                       .idx = rc,
                                       .name = std::move(nm),
                                       .vt = out_vt,
                                       .is_unsigned = rs.is_unsigned,
                                       .is_interval = rs.is_interval,
                                       .has_null = rs.any_null,
                                       .is_uniform = rs.all_uniform});
              continue;
            }
          }
          expr_ok = false;
          break;
        }
      }
      if (expr_ok) {
        const bool is_avg = (raw_aggregates.slot_agg_kinds[0] == RawAggKind::kAvgDouble);
        const bool raise_inf = raw_aggregates.slot_raises_inf[0];
        auto sk = JitDenseSplitRadixAgg::CompileScatter(kDenseSplitPartShift);
        auto ak = JitDenseSplitRadixAgg::CompileAccum(is_avg, raise_inf);
        const size_t res_bytes =
            ((dense_int_idxs.size() + pk0.dense_size) * sizeof(uint32_t)) +
            (workers * (size_t{32768} * 16 + size_t{262144} * 12));
        auto charge = TryReserveQueryMemory(res_bytes);
        if (sk.has_value() && ak.has_value() && charge != nullptr) {
          jit_dense_split.active = true;
          jit_dense_split.dom = dom;
          jit_dense_split.pk0_card = pk0.subgid_count;
          jit_dense_split.is_avg = is_avg;
          jit_dense_split.raise_inf = raise_inf;
          jit_dense_split.mem_charge = std::move(charge);
          jit_dense_split.scatter_k = sk;
          jit_dense_split.accum_k = ak;
        }
      }
      if (debug_fusion) {
        std::cerr << "[fusion] jit_dense_split active="
                  << jit_dense_split.active << " dom=" << dom
                  << " r_card=" << r_card << " pk0_card=" << pk0.subgid_count
                  << " expr_ok=" << expr_ok << "\n";
      }
    }
  }
  if (layout.use_left_packed_key && layout.left_col_only && layout.left_pack_uses_dict &&
      (use_preassigned_build_groups || compact_counts ||
       !residual_programs.empty() || !raw_aggregates.raw_sum_fusion || raw_aggregates.has_count_distinct ||
       !all_direct_group_keys || !left_cell_agg_ready ||
       uses_double_arith_cell_op || right_ptr->HasSpill() ||
       singleton_right_relation || !(integer_key || packed_2x32_int_key))) {
    layout.use_left_packed_key = false;
    layout.left_pack_unpack_only = false;
  }
  bool use_radix_agg = false;
  size_t radix_primary = 0;
  bool radix_tuple_is_int = false;
  // Lean proof: lean/TinylambOpt/PredicateAndRelationalLowering.lean
  // (packed_2x32_join_key_bijection_sound)
  const bool small_build_packed_2x32 =
      (layout.packed_2x32_group_key || (layout.use_split_key_gid && layout.split_key_unpack_only)) &&
      layout.right_row_count <= 512 && effective_left_rows <= 4000000;
  const bool bounded_packed_radix =
      small_build_packed_2x32 && layout.packed_2x32_group_key &&
      layout.packed_2x32_unpack_only && build_int_keys_unique &&
      deferred_left_probe != nullptr;
  std::shared_ptr<QueryMemoryCharge> bounded_radix_charge;
  if (workers > 1 && effective_left_rows >= 2000000 &&
      (!small_build_packed_2x32 || bounded_packed_radix) && raw_aggregates.raw_sum_fusion &&
      !use_preassigned_build_groups && !use_direct_gid_agg &&
      !use_dense_split_direct_gid && !jit_dense_split.active &&
      !ordered_sum_charge && !scalar_fact_charge && !compact_counts &&
      !raw_aggregates.has_count_distinct && residual_programs.empty() &&
      all_direct_group_keys && !fusion->group_keys.empty() && !fusion->having &&
      !fusion->post_having_active && fusion->topn_limit == 0 &&
      !fusion->rollup_count_distribution && left_cell_agg_ready &&
      agg_count >= 1 && join_type == JoinType::kInner &&
      !right_ptr->HasSpill() && !layout.use_fast_2key_year_lut &&
      ((layout.use_left_packed_key && layout.left_pack_unpack_only) ||
       (layout.use_split_key_gid && layout.split_key_unpack_only) ||
       (layout.packed_2x32_group_key && layout.packed_2x32_unpack_only)) &&
      ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_RADIX_AGG") == nullptr) {
    const bool radix_def_path =
        (deferred_left_probe != nullptr && def_group_cell_probe &&
         (def_pk_groups.empty() ||
          (!uses_double_arith_cell_op && !use_dense_split_direct_gid)));
    const bool radix_col_path =
        (deferred_left_probe == nullptr && left.HasColumnarOnly() &&
         !singleton_right_relation && !uses_double_arith_cell_op &&
         (integer_key || packed_2x32_int_key) && !bulk_raw_count &&
         (layout.use_left_packed_key ||
          (layout.use_split_key_gid && !layout.split_uses_pk_subgid &&
           (!layout.split_has_left_probe ||
            key_shapes[layout.split_left_k].direct_column >= 0)) ||
          layout.packed_2x32_group_key));
    if (radix_def_path || radix_col_path) {
      bool found_any_primary = false;
      for (size_t s = 0; s < agg_count; ++s) {
        const uint8_t sop = left_numeric[s].op;
        if (sop == 1 || sop == 2 || sop == 3 || sop == 6 || sop == 8 ||
            sop == 9 || sop == 17) {
          radix_primary = s;
          break;
        }
        if (!found_any_primary && sop != 0 && sop != 5) {
          radix_primary = s;
          found_any_primary = true;
        }
      }
      const LeftNumericAgg& pop = left_numeric[radix_primary];
      bool ops_ok = (pop.op <= 9 || pop.op == 17);
      bool any_int_req = (pop.op == 3 || (pop.op == 8 && pop.is_int_col));
      for (size_t s = 0; ops_ok && s < agg_count; ++s) {
        const LeftNumericAgg& sop = left_numeric[s];
        if (sop.op == 0 || sop.op == 5) {
          continue;
        }
        if (sop.op >= 10 && sop.op != 17) {
          ops_ok = false;
          break;
        }
        if (s == radix_primary) {
          continue;
        }
        if ((pop.op == 2 || pop.op == 3 || pop.op == 4 || pop.op == 8) &&
            (sop.op == 2 || sop.op == 3 || sop.op == 4 || sop.op == 8) &&
            sop.col0 == pop.col0 && sop.col0 >= 0) {
          if (sop.op == 3 || (sop.op == 8 && sop.is_int_col)) {
            any_int_req = true;
          }
          continue;
        }
        ops_ok = false;
      }
      if (ops_ok && any_int_req && !integer_left_col(pop.col0)) {
        ops_ok = false;
      }
      if (ops_ok) {
        if (bounded_packed_radix) {
          // Lean proof: lean/TinylambOpt/PredicateAndRelationalLowering.lean
          //   (radix_bucket_preserves_exact_key_order)
          // The unique build emits at most one tuple per fact row. Reserve for
          // all tuples and vector growth before switching away from local maps.
          bounded_radix_charge = TryReserveQueryMemory(
              effective_left_rows * 2 *
              (sizeof(uint64_t) + sizeof(double) + sizeof(uint8_t)));
        }
        use_radix_agg =
            !bounded_packed_radix || bounded_radix_charge != nullptr;
        radix_tuple_is_int = any_int_req;
      }
    }
  }
  std::vector<std::array<RadixSoABucket, kRadixParts>> worker_radix_buckets;
  if (use_radix_agg) {
    worker_radix_buckets.resize(workers);
  } else if (!group_capacity_charge &&
             (deferred_left_probe != nullptr || left.HasColumnarOnly()) &&
             !left.HasSpill() && effective_left_rows >= 32768 &&
             (int_group_key || layout.use_split_key_gid || layout.packed_2x32_group_key)) {
    const size_t max_reserve =
        effective_left_rows >= 2000000 ? size_t{65536} : size_t{8192};
    const size_t capacity =
        std::min<size_t>(max_reserve, (effective_left_rows / workers) + 1);
    const size_t bytes = workers * capacity *
                         (64 + (compact_counts ? sizeof(CompactCountGroup)
                                               : sizeof(FusedGroup)));
    group_capacity_charge = TryReserveQueryMemory(bytes);
    if (group_capacity_charge) {
      for (auto& local : locals) {
        local.int_offsets.Reserve(
            compact_counts ? std::min<size_t>(1024, capacity) : capacity);
        if (compact_counts) {
          local.compact_counts.reserve(std::min<size_t>(1024, capacity));
        } else {
          local.groups.reserve(capacity);
        }
      }
    }
  }
  std::vector<FusedRawArena> radix_part_arenas;
  std::vector<std::vector<FusedGroup>> radix_part_groups;

  // Decide the partitioning method once, before workers start. A failed
  // optional allocation must not make some workers split rows while others
  // split keys, which would overlap or omit parts of the probe.
  std::shared_ptr<const LSTableIntegerRuns> flat_run_index;
  if (deferred_left_probe != nullptr &&
      (left_numeric_probe ||
       (left_cell_agg_ready && !uses_double_arith_cell_op &&
        identity_right_gid && fusion->topn_limit > 0 &&
        fusion->topn_limit <= 256)) &&
      single_double_sum && !ranked_int_bits.empty() && integer_key &&
      prepared_pk_lookups.empty() && deferred_left_probe->null_peeks.empty() &&
      left_key_offsets[0] == 0 &&
      (left_numeric[0].op == 1 || left_numeric[0].op == 2 ||
       left_numeric[0].op == 6)) {
    const auto& store = *deferred_left_probe->store;
    const auto flat_column_ready = [&](size_t col) {
      return store.HasColumn(static_cast<slot_t>(col)) &&
             col < store.column_has_null.size() &&
             !store.column_has_null[col] &&
             store.IntegerColumn(static_cast<slot_t>(col)).size() ==
                 store.total_rows;
    };
    const auto op_column_ready = [&](int32_t col) {
      return col >= 0 &&
             static_cast<size_t>(col) <
                 deferred_left_probe->projection.size() &&
             !def_overwritten_by_pk[static_cast<size_t>(col)];
    };
    if (op_column_ready(left_numeric[0].col0) &&
        (left_numeric[0].op == 2 || op_column_ready(left_numeric[0].col1)) &&
        std::ranges::all_of(deferred_left_probe->projection,
                            flat_column_ready) &&
        std::ranges::all_of(
            deferred_left_probe->integer_peeks,
            [&](const auto& pred) { return flat_column_ready(pred.column); })) {
      flat_run_index =
          store.IntegerRuns(deferred_left_probe->projection[left_columns[0]]);
    }
  }
  std::shared_ptr<const LSTableIntegerRunBounds> flat_run_bounds;
  if (flat_run_index && deferred_left_probe->integer_peeks.size() == 1 &&
      deferred_left_probe->integer_peeks[0].op ==
          BinaryOperation::kGreaterThan) {
    flat_run_bounds = deferred_left_probe->store->IntegerRunBounds(
        flat_run_index->column, deferred_left_probe->integer_peeks[0].column);
  }
  // Indexed fact probes consume the already decoded build keys directly.
  // Only the scanning fallback needs rank-to-row arrays over the bitmap.
  if (!ranked_int_bits.empty() &&
      (!flat_run_index || !left_numeric_probe || !key_cache_charge)) {
    ranked_int_prefix.resize(ranked_int_bits.size());
    uint32_t count = 0;
    for (size_t i = 0; i < ranked_int_bits.size(); ++i) {
      ranked_int_prefix[i] = count;
      count += static_cast<uint32_t>(std::popcount(ranked_int_bits[i]));
    }
    ranked_int_rows.resize(count);
    const auto add = [&](uint32_t row, int64_t key) {
      const uint64_t off =
          static_cast<uint64_t>(key) - static_cast<uint64_t>(dense_int_min);
      const uint64_t lower = (uint64_t{1} << (off & 63)) - 1;
      const uint32_t rank =
          ranked_int_prefix[off >> 6] + static_cast<uint32_t>(std::popcount(
                                            ranked_int_bits[off >> 6] & lower));
      ranked_int_rows[rank] = row;
      return true;
    };
    if (key_cache_charge) {
      for (const auto& chunk : key_cache) {
        for (const auto& entry : chunk) {
          add(entry.row, entry.key);
        }
      }
    } else {
      visit_right_int_keys(add);
    }
  }
  if (!flat_run_index) {
    key_cache.clear();
    key_cache_charge.reset();
  }
  // The indexed probe assigns each unique build key (and its entire fact
  // run) to one worker. Identity groups therefore have exactly one writer:
  // one state array suffices without locks or a merge of worker partials.
  const bool disjoint_direct_groups = flat_run_index && identity_right_gid;
  if (debug_fusion && disjoint_direct_groups) {
    std::cerr << "[fusion] topk-check: dgid=" << use_direct_gid_agg
              << " sds=" << single_double_sum
              << " kc=" << static_cast<int>(key_cache_charge != nullptr)
              << " lim=" << fusion->topn_limit
              << " ord=" << fusion->output_order.size() << " asc0="
              << (fusion->output_order.empty()
                      ? -1
                      : static_cast<int>(fusion->output_order[0].ascending))
              << " op=" << static_cast<int>(left_numeric[0].op)
              << " peeks=" << deferred_left_probe->integer_peeks.size()
              << " o0="
              << (fusion->output_order.empty() ||
                          !fusion->output_order[0].expression
                      ? ""
                      : fusion->output_order[0].expression->ToString())
              << "\n";
  }
  // Lean proof: lean/TinylambOpt/VectorizedJoinAndPruning.lean
  //   (parallel_disjoint_topk_member_sound, finite_sum_implies_no_inf)
  // Streaming parallel Top-K for disjoint-group indexed probes with
  // ORDER BY SUM(...) DESC LIMIT K (e.g. TPC-H Q3): skips allocating/zeroing
  // the 30 MiB per-group state arrays and eliminates 2.4M random DRAM writes
  // by keeping only each worker's top-K (plus ties at the K-th value).
  if (disjoint_direct_groups && use_direct_gid_agg && single_double_sum &&
      deferred_left_probe != nullptr && deferred_left_probe->store != nullptr &&
      flat_run_index != nullptr && key_cache_charge && fusion->topn_limit > 0 &&
      fusion->topn_limit <= 256 && !fusion->output_order.empty() &&
      !fusion->output_order[0].ascending && fusion->having == nullptr &&
      !fusion->post_having_active && fusion->output_sink == nullptr &&
      !fusion->rollup_count_distribution && residual_programs.empty() &&
      prepared_pk_lookups.empty() && def_local_preds_ok &&
      deferred_left_probe->null_peeks.empty() &&
      deferred_left_probe->integer_peeks.size() == 1 &&
      deferred_left_probe->integer_peeks[0].op ==
          BinaryOperation::kGreaterThan &&
      !right_ptr->HasSpill() && agg_count == 1 &&
      (left_numeric[0].op == 6 || left_numeric[0].op == 1 ||
       left_numeric[0].op == 2)) {
    const auto& store = *deferred_left_probe->store;
    const auto& proj = deferred_left_probe->projection;
    const LeftNumericAgg& lop = left_numeric[0];
    auto flat_col_ptr = [&](slot_t base_col) -> const int64_t* {
      const auto col = store.IntegerColumn(base_col);
      if (!store.HasColumn(base_col) ||
          base_col >= store.column_has_null.size() ||
          store.column_has_null[base_col] != 0 ||
          col.size() != store.total_rows) {
        return nullptr;
      }
      return col.data();
    };
    const int64_t* pred_cells =
        flat_col_ptr(deferred_left_probe->integer_peeks[0].column);
    const int64_t pred_thresh = deferred_left_probe->integer_peeks[0].constant;
    const int64_t* flat_a =
        (lop.col0 >= 0 && static_cast<size_t>(lop.col0) < proj.size())
            ? flat_col_ptr(proj[static_cast<size_t>(lop.col0)])
            : nullptr;
    const int64_t* flat_b =
        (lop.op == 2)
            ? nullptr
            : ((lop.col1 >= 0 && static_cast<size_t>(lop.col1) < proj.size())
                   ? flat_col_ptr(proj[static_cast<size_t>(lop.col1)])
                   : nullptr);
    const bool a_dbl =
        lop.col0 >= 0 &&
        left.schema.GetColumn(static_cast<size_t>(lop.col0)).Type() ==
            ValueType::kDouble;
    const bool b_dbl =
        lop.op == 2 ||
        (lop.col1 >= 0 &&
         left.schema.GetColumn(static_cast<size_t>(lop.col1)).Type() ==
             ValueType::kDouble);
    // Check select_items: one agg0, all others either join key or right cols.
    size_t agg_sel_idx = fusion->select_items.size();
    struct TopKSelBind {
      int kind{0};  // 0 = agg0, 1 = join_key, 2 = right_col
      slot_t rcol{0};
    };
    std::vector<TopKSelBind> sel_binds;
    bool sel_ok = (pred_cells != nullptr && flat_a != nullptr &&
                   (lop.op == 2 || flat_b != nullptr) && a_dbl && b_dbl &&
                   !fusion->select_items.empty());
    const ColumnName left_jk_name =
        left.schema.GetColumn(left_columns[0]).Name();
    if (sel_ok) {
      sel_binds.reserve(fusion->select_items.size());
      for (size_t si = 0; si < fusion->select_items.size(); ++si) {
        const Expression& item = fusion->select_items[si];
        if (!item) {
          sel_ok = false;
          break;
        }
        if (item->Type() == TypeTag::kAggregateExp &&
            &item->AsAggregateExpression() == fusion->aggregates[0]) {
          agg_sel_idx = si;
          sel_binds.push_back({.kind = 0, .rcol = 0});
        } else if (item->Type() == TypeTag::kColumnValue) {
          const ColumnName& cn = item->AsColumnValue().GetColumnName();
          if (const auto roff = LocalColumnOffset(right_arg.schema, cn)) {
            sel_binds.push_back(
                {.kind = 2, .rcol = static_cast<slot_t>(*roff)});
          } else if (cn.name == left_jk_name.name &&
                     (cn.schema.empty() || cn.schema == left_jk_name.schema)) {
            sel_binds.push_back({.kind = 1, .rcol = 0});
          } else {
            sel_ok = false;
            break;
          }
        } else {
          sel_ok = false;
          break;
        }
      }
    }
    // Verify ORDER BY term 0 is DESC on agg0.
    bool order0_is_agg0 = false;
    if (sel_ok && agg_sel_idx < fusion->select_items.size()) {
      const Expression& o0 = fusion->output_order[0].expression;
      if (o0 != nullptr) {
        const bool is_agg =
            o0->Type() == TypeTag::kAggregateExp &&
            &o0->AsAggregateExpression() == fusion->aggregates[0];
        const bool is_named_agg = o0->Type() == TypeTag::kColumnValue &&
                                  agg_sel_idx < fusion->select_names.size() &&
                                  !fusion->select_names[agg_sel_idx].empty() &&
                                  o0->AsColumnValue().GetColumnName().name ==
                                      fusion->select_names[agg_sel_idx];
        order0_is_agg0 = is_agg || is_named_agg;
      }
    }
    if (debug_fusion) {
      std::cerr << "[fusion] topk-inner: sel_ok=" << sel_ok
                << " ord0=" << order0_is_agg0
                << " pred=" << (pred_cells != nullptr)
                << " fa=" << (flat_a != nullptr)
                << " fb=" << (flat_b != nullptr) << " adbl=" << a_dbl
                << " bdbl=" << b_dbl << " agg_sel=" << agg_sel_idx << "\n";
    }
    if (sel_ok && order0_is_agg0) {
      struct TopKCand {
        double sum;
        int64_t key;
        uint32_t r_idx;
      };
      const size_t k_lim = fusion->topn_limit;
      const uint8_t op_code = lop.op;
      const double scale = lop.scale;
      const size_t nworkers = workers;
      std::vector<std::vector<TopKCand>> w_cands(nworkers);
      std::vector<uint64_t> w_cmps(nworkers, 0);
      std::atomic<bool> topk_bail{false};

      auto run_topk_worker = [&](size_t w) {
        auto& heap = w_cands[w];
        heap.reserve(k_lim + 16);
        std::vector<TopKCand> ties;
        double cutoff = -std::numeric_limits<double>::infinity();
        bool heap_full = false;
        auto min_cmp = [](const TopKCand& a, const TopKCand& b) {
          return a.sum > b.sum;
        };
        auto offer = [&](double sum, int64_t key, uint32_t r_idx) {
          if (!heap_full) {
            heap.push_back(TopKCand{.sum = sum, .key = key, .r_idx = r_idx});
            if (heap.size() == k_lim) {
              std::make_heap(heap.begin(), heap.end(), min_cmp);
              cutoff = heap.front().sum;
              heap_full = true;
            }
          } else if (sum > cutoff) {
            std::pop_heap(heap.begin(), heap.end(), min_cmp);
            heap.back() = TopKCand{.sum = sum, .key = key, .r_idx = r_idx};
            std::push_heap(heap.begin(), heap.end(), min_cmp);
            const double new_cut = heap.front().sum;
            if (new_cut > cutoff) {
              cutoff = new_cut;
              ties.clear();
            }
          } else if (sum == cutoff) {
            ties.push_back(TopKCand{.sum = sum, .key = key, .r_idx = r_idx});
          }
        };

        struct PendingRun {
          int64_t key;
          uint32_t r_idx;
          uint32_t begin;
          uint32_t end;
        };
        std::array<PendingRun, 128> pending{};
        size_t pending_count = 0;
        uint64_t cmp = 0;
        const auto flush_runs = [&]() {
          for (size_t i = 0; i < pending_count; ++i) {
            const auto off = static_cast<uint64_t>(pending[i].key) -
                             static_cast<uint64_t>(flat_run_index->minimum);
            if (flat_run_bounds && off < flat_run_bounds->packed.size()) {
              __builtin_prefetch(flat_run_bounds->packed.data() + off, 0, 1);
            } else if (off < flat_run_index->offsets.size() - 1) {
              __builtin_prefetch(flat_run_index->offsets.data() + off, 0, 1);
              __builtin_prefetch(flat_run_index->offsets.data() + off + 1, 0,
                                 1);
            }
          }
          for (size_t i = 0; i < pending_count; ++i) {
            auto& run = pending[i];
            if (flat_run_bounds &&
                !flat_run_bounds->MayExceed(run.key, pred_thresh)) {
              run.begin = run.end = 0;
              continue;
            }
            std::tie(run.begin, run.end) = flat_run_index->Find(run.key);
            if (run.begin == run.end) {
              continue;
            }
            __builtin_prefetch(pred_cells + run.begin, 0, 1);
            __builtin_prefetch(flat_a + run.begin, 0, 1);
            if (flat_b != nullptr) {
              __builtin_prefetch(flat_b + run.begin, 0, 1);
            }
          }
          for (size_t i = 0; i < pending_count; ++i) {
            const auto& run = pending[i];
            if (run.begin == run.end) {
              continue;
            }
            double run_sum = 0.0;
            uint32_t run_hits = 0;
            if (op_code == 6 && flat_b != nullptr) {
              for (size_t r = run.begin; r < run.end; ++r) {
                if (pred_cells[r] > pred_thresh) {
                  run_sum += std::bit_cast<double>(flat_a[r]) *
                             (scale - std::bit_cast<double>(flat_b[r]));
                  ++run_hits;
                }
              }
            } else if (op_code == 1 && flat_b != nullptr) {
              for (size_t r = run.begin; r < run.end; ++r) {
                if (pred_cells[r] > pred_thresh) {
                  run_sum += std::bit_cast<double>(flat_a[r]) *
                             std::bit_cast<double>(flat_b[r]);
                  ++run_hits;
                }
              }
            } else {
              for (size_t r = run.begin; r < run.end; ++r) {
                if (pred_cells[r] > pred_thresh) {
                  run_sum += std::bit_cast<double>(flat_a[r]);
                  ++run_hits;
                }
              }
            }
            if (run_hits > 0) {
              cmp += run_hits;
              if (!std::isfinite(run_sum)) [[unlikely]] {
                topk_bail.store(true, std::memory_order_relaxed);
                return;
              }
              offer(run_sum, run.key, run.r_idx);
            }
          }
          pending_count = 0;
        };

        for (size_t ci = w; ci < key_cache.size(); ci += nworkers) {
          if (topk_bail.load(std::memory_order_relaxed)) {
            return;
          }
          for (const auto& entry : key_cache[ci]) {
            pending[pending_count++] = {
                .key = entry.key, .r_idx = entry.row, .begin = 0, .end = 0};
            if (pending_count == pending.size()) {
              flush_runs();
              if (topk_bail.load(std::memory_order_relaxed)) {
                return;
              }
            }
          }
        }
        flush_runs();
        for (const auto& t : ties) {
          heap.push_back(t);
        }
        w_cmps[w] = cmp;
      };

      if (nworkers == 1) {
        run_topk_worker(0);
      } else {
        RETURN_IF_FAIL(RunParallelTasks(nworkers, run_topk_worker));
      }
      if (!topk_bail.load(std::memory_order_relaxed)) {
        std::vector<Column> out_cols;
        out_cols.reserve(fusion->select_items.size());
        for (size_t si = 0; si < fusion->select_items.size(); ++si) {
          std::string nm = si < fusion->select_names.size() &&
                                   !fusion->select_names[si].empty()
                               ? fusion->select_names[si]
                               : ("col_" + std::to_string(si));
          ValueType vt = ValueType::kDouble;
          if (sel_binds[si].kind == 1) {
            vt = left.schema.GetColumn(left_columns[0]).Type();
          } else if (sel_binds[si].kind == 2) {
            vt = right_arg.schema.GetColumn(sel_binds[si].rcol).Type();
          }
          out_cols.emplace_back(std::move(nm), vt);
        }
        result.schema = Schema("", std::move(out_cols));
        const ValueType jk_type = left.schema.GetColumn(left_columns[0]).Type();
        for (size_t w = 0; w < nworkers; ++w) {
          for (const TopKCand& c : w_cands[w]) {
            Row row;
            row.values_.reserve(sel_binds.size());
            for (const TopKSelBind& b : sel_binds) {
              if (b.kind == 0) {
                row.values_.emplace_back(c.sum);
              } else if (b.kind == 1) {
                row.values_.push_back(jk_type == ValueType::kDate
                                          ? Value::DateFromDays(c.key)
                                          : Value(c.key));
              } else {
                Value rv;
                if (right_col_only) {
                  size_t rem = c.r_idx;
                  for (const auto& rch : right_arg.columnar->chunks) {
                    if (rem < rch.rows) {
                      rch.columns[b.rcol].WriteValueInto(rem, &rv);
                      break;
                    }
                    rem -= rch.rows;
                  }
                } else {
                  rv = build_rows[c.r_idx]->values_[b.rcol];
                }
                row.values_.push_back(std::move(rv));
              }
            }
            result.AddRow(std::move(row));
          }
        }
        result.hash_joins = left.hash_joins + right.hash_joins + 1;
        result.in_memory_hash_joins =
            left.in_memory_hash_joins + right.in_memory_hash_joins + 1;
        result.nested_loop_joins =
            left.nested_loop_joins + right.nested_loop_joins;
        result.hybrid_hash_joins =
            left.hybrid_hash_joins + right.hybrid_hash_joins;
        result.join_comparisons =
            left.join_comparisons + right.join_comparisons;
        for (uint64_t cm : w_cmps) {
          result.join_comparisons += cm;
        }
        result.peak_intermediate_rows = result.rows.size();
        if (context.execution_runtime() != nullptr) {
          context.execution_runtime()->join_ms += ElapsedMs(join_begin);
          ++context.execution_runtime()->grouped_join_fusions;
        }
        fusion->fired = true;
        return result;
      }
    }
  }
  if (!left_numeric_probe) {
    flat_run_index.reset();
    flat_run_bounds.reset();
  }
  if (use_direct_gid_agg) {
    const size_t state_workers =
        disjoint_direct_groups ? 1 : direct_gid_worker_count;
    if (disjoint_direct_groups && extended_direct_charge && workers > 1) {
      extended_direct_charge->Release((workers - 1) * pre_group_count *
                                      direct_state_bytes);
    }
    RETURN_IF_FAIL(init_direct_gid_workers(state_workers, pre_group_count));
  }
  const auto phase_states_done = std::chrono::steady_clock::now();

  // Lean proof: lean/TinylambOpt/VectorizedJoinAndPruning.lean
  //   (parallel_direct_array_merge_eq, finite_sum_implies_no_inf)
  // Monotonic-left-key streaming fast path for deferred resident LSTableStore
  // probes against a unique dense right-side integer key set (e.g. TPC-H Q11):
  //   SELECT L.gcol, SUM(L.c0 * L.c1) FROM L INNER JOIN R ON L.jcol = R.pk
  //   GROUP BY L.gcol
  // Streams L in parallel with 2 registers (cur_k, cur_sum), zero hash tables,
  // merges only worker-boundary keys, and emits a 2-column ColumnarTable.
  if (deferred_left_probe != nullptr && deferred_left_probe->store != nullptr &&
      join_type == JoinType::kInner && integer_key && use_dense_int_rows &&
      !dense_int_idxs.empty() && residual_programs.empty() &&
      prepared_pk_lookups.empty() && deferred_left_probe->local_preds.empty() &&
      deferred_left_probe->integer_peeks.empty() &&
      deferred_left_probe->null_peeks.empty() &&
      deferred_left_probe->pk_lookups.empty() && key_shapes.size() == 1 &&
      key_shapes[0].direct_column >= 0 &&
      std::cmp_less(key_shapes[0].direct_column, left_width) &&
      agg_count == 1 && single_double_sum &&
      (left_numeric[0].op == 1 || left_numeric[0].op == 2 ||
       left_numeric[0].op == 6) &&
      fusion->having == nullptr && fusion->output_sink == nullptr &&
      (!fusion->post_having_active ||
       (fusion->post_having_agg_slot == 0 &&
        (fusion->post_having_is_self_sum_factor ||
         std::isfinite(fusion->post_having_constant_or_factor)))) &&
      !fusion->rollup_count_distribution && fusion->select_items.size() == 2 &&
      fusion->select_items[0] != nullptr &&
      fusion->select_items[0]->Type() == TypeTag::kColumnValue &&
      fusion->select_items[1] != nullptr &&
      fusion->select_items[1]->Type() == TypeTag::kAggregateExp &&
      &fusion->select_items[1]->AsAggregateExpression() ==
          fusion->aggregates[0]) {
    const auto& store = *deferred_left_probe->store;
    const auto& proj = deferred_left_probe->projection;
    const auto g_pcol = static_cast<size_t>(key_shapes[0].direct_column);
    const auto j_pcol = static_cast<size_t>(left_columns[0]);
    const LeftNumericAgg& lop = left_numeric[0];
    auto block_col_ok = [&](size_t pcol, slot_t* base_out) {
      if (pcol >= proj.size()) {
        return false;
      }
      const slot_t b = proj[pcol];
      if (!store.HasColumn(b) || b >= store.column_has_null.size() ||
          store.column_has_null[b] != 0 || store.blocks.empty()) {
        return false;
      }
      *base_out = b;
      return true;
    };
    slot_t g_base = 0;
    slot_t j_base = 0;
    slot_t c0_base = 0;
    slot_t c1_base = 0;
    const bool cols_ok =
        block_col_ok(g_pcol, &g_base) && block_col_ok(j_pcol, &j_base) &&
        lop.col0 >= 0 &&
        block_col_ok(static_cast<size_t>(lop.col0), &c0_base) &&
        (lop.op == 2 ||
         (lop.col1 >= 0 &&
          block_col_ok(static_cast<size_t>(lop.col1), &c1_base))) &&
        g_base < store.column_is_monotonic.size() &&
        store.column_is_monotonic[g_base] != 0 &&
        (left.schema.GetColumn(g_pcol).Type() == ValueType::kInt64 ||
         left.schema.GetColumn(g_pcol).Type() == ValueType::kDate) &&
        !left.schema.GetColumn(g_pcol).IsUnsigned();
    // Verify select_items[0] matches group_keys[0].
    const bool sel0_ok =
        cols_ok && fusion->group_keys.size() == 1 &&
        fusion->group_keys[0] != nullptr &&
        fusion->group_keys[0]->Type() == TypeTag::kColumnValue &&
        fusion->select_items[0]->AsColumnValue().GetColumnName() ==
            fusion->group_keys[0]->AsColumnValue().GetColumnName();
    if (sel0_ok) {
      DenseFilteredGroups specialized{
          .context = context,
          .left = left,
          .fusion = fusion,
          .join_begin = join_begin,
          .workers = workers,
          .dense_int_idxs = dense_int_idxs,
          .dense_int_min = dense_int_min,
          .store = store,
          .g_pcol = g_pcol,
          .lop = lop,
          .g_base = g_base,
          .j_base = j_base,
          .c0_base = c0_base,
          .c1_base = c1_base,
      };
      ASSIGN_OR_RETURN(std::optional<Relation>, emitted, specialized.TryRun());
      if (emitted) {
        return emitted;
      }
    }
  }

  if (jit_dense_split.active) {
    return DenseSplitExecution{
        .context = context,
        .left = left,
        .right = right,
        .fusion = *fusion,
        .probe = *deferred_left_probe,
        .right_col_owner = right_col_owner,
        .right_locator = right_locator,
        .join_begin = join_begin,
        .result = result,
        .left_columns = left_columns,
        .left_key_offsets = left_key_offsets,
        .prepared_pk_lookups = prepared_pk_lookups,
        .pk0 = def_pk_groups[0],
        .split_uses_right_subgid = layout.split_uses_right_subgid,
        .split_right_subgid_ptr = layout.split_right_subgid_ptr,
        .split_right_rep_ptr = layout.split_right_rep_ptr,
        .workers = workers,
        .dense_int_idxs = dense_int_idxs,
        .dense_int_min = dense_int_min,
        .packed_2x32_int_key = packed_2x32_int_key,
        .jit_dense_split = jit_dense_split,
    }
        .Run();
  }

  {
    ASSIGN_OR_RETURN(
        DistinctLSTableProbeOutcome, distinct_probe_outcome,
        TryDistinctLSTableProbe(DistinctLSTableProbeContext{
            .context = context,
            .left = left,
            .right = right,
            .fusion = fusion,
            .result = result,
            .int_buckets = int_buckets,
            .equality_keys = equality_keys,
            .residual_programs = residual_programs,
            .deferred_left_probe = deferred_left_probe,
            .key_programs = key_programs,
            .key_shapes = key_shapes,
            .left_width = left_width,
            .def_overwritten_by_pk = def_overwritten_by_pk,
            .left_columns = left_columns,
            .left_key_offsets = left_key_offsets,
            .join_begin = join_begin,
            .dense_int_idxs = dense_int_idxs,
            .dense_int_min = dense_int_min,
            .unique_int_table = unique_int_table,
            .unique_int_mask = unique_int_mask,
            .worker_count = locals.size(),
            .join_type = join_type,
            .use_dense_int_rows = use_dense_int_rows,
            .use_unique_int_table = use_unique_int_table,
            .singleton_right_relation = singleton_right_relation,
            .integer_key = integer_key,
            .packed_2x32_int_key = packed_2x32_int_key,
            .prepared_pk_lookups_empty = prepared_pk_lookups.empty(),
            .def_local_preds_ok = def_local_preds_ok,
            .distinct_fact_singleton = distinct_fact_singleton,
            .debug_fusion = debug_fusion}));
    if (distinct_probe_outcome.handled) {
      return std::move(distinct_probe_outcome.result);
    }
  }
  // Lean proof: lean/TinylambOpt/VectorizedJoinAndPruning.lean
  //   (packed_u64_pair_injective, finite_sum_implies_no_inf)
  // Zero-materialization columnar fast path for 2-key packed-2x32 unique joins
  // with (right_direct_key, EXTRACT(year FROM left_date_col)) grouping and
  // SUM(c0 * (k - c1) - c2 * c3) profit arithmetic (e.g. TPC-H Q9).
  if (debug_fusion && packed_2x32_int_key && use_unique_int_table) {
    std::cerr << "[fusion] profit-check: lcol=" << layout.left_col_only
              << " rcol=" << right_col_only << " sds=" << single_double_sum
              << " prof="
              << (child_shapes.empty() ? -2 : child_shapes[0].profit_col0)
              << " ks=" << key_shapes.size()
              << " sel=" << fusion->select_items.size() << " k0d="
              << (!key_shapes.empty() ? key_shapes[0].direct_column : -2)
              << " k0e="
              << (!key_shapes.empty() ? key_shapes[0].extract_date_col : -2)
              << " k1d="
              << (key_shapes.size() > 1 ? key_shapes[1].direct_column : -2)
              << " k1e="
              << (key_shapes.size() > 1 ? key_shapes[1].extract_date_col : -2)
              << " s0="
              << (!fusion->select_items.empty() && fusion->select_items[0]
                      ? fusion->select_items[0]->ToString()
                      : "")
              << " g0="
              << (!fusion->group_keys.empty() && fusion->group_keys[0]
                      ? fusion->group_keys[0]->ToString()
                      : "")
              << " s1="
              << (fusion->select_items.size() > 1 && fusion->select_items[1]
                      ? fusion->select_items[1]->ToString()
                      : "")
              << " g1="
              << (fusion->group_keys.size() > 1 && fusion->group_keys[1]
                      ? fusion->group_keys[1]->ToString()
                      : "")
              << "\n";
  }
  if (layout.left_col_only && packed_2x32_int_key && use_unique_int_table &&
      join_type == JoinType::kInner && residual_programs.empty() &&
      single_double_sum && !raw_aggregates.has_count_distinct && agg_count == 1 &&
      !right_ptr->HasSpill() && !left.HasSpill() && child_shapes.size() == 1 &&
      child_shapes[0].profit_col0 >= 0 && key_shapes.size() == 2 &&
      fusion->having == nullptr && !fusion->post_having_active &&
      fusion->output_sink == nullptr && !fusion->rollup_count_distribution &&
      fusion->select_items.size() == 3 && fusion->select_items[0] != nullptr &&
      fusion->select_items[1] != nullptr &&
      fusion->select_items[2] != nullptr &&
      fusion->select_items[2]->Type() == TypeTag::kAggregateExp &&
      &fusion->select_items[2]->AsAggregateExpression() ==
          fusion->aggregates[0] &&
      fusion->group_keys.size() == 2 && fusion->group_keys[0] != nullptr &&
      fusion->group_keys[1] != nullptr &&
      fusion->select_items[0]->ToString() ==
          fusion->group_keys[0]->ToString() &&
      fusion->select_items[1]->ToString() ==
          fusion->group_keys[1]->ToString()) {
    int rk = -1;
    int lk = -1;
    for (int k = 0; k < 2; ++k) {
      if (std::cmp_greater_equal(key_shapes[k].direct_column, left_width)) {
        rk = k;
      } else if (key_shapes[k].extract_date_col >= 0 &&
                 std::cmp_less(key_shapes[k].extract_date_col, left_width) &&
                 key_shapes[k].extract_date_part == 0) {
        lk = k;
      }
    }
    if (rk >= 0 && lk >= 0 && rk != lk) {
      DenseProfitGroups specialized{
          .context = context,
          .left = left,
          .right_arg = right_arg,
          .fusion = fusion,
          .right_col_only = right_col_only,
          .build_rows = build_rows,
          .right = right,
          .join_begin = join_begin,
          .result = result,
          .left_width = left_width,
          .key_shapes = key_shapes,
          .child_shapes = child_shapes,
          .left_columns = left_columns,
          .left_key_offsets = left_key_offsets,
          .kFastYearLut = kFastYearLut,
          .locals = locals,
          .unique_int_table = unique_int_table,
          .unique_int_mask = unique_int_mask,
          .rk = rk,
          .lk = lk,
      };
      ASSIGN_OR_RETURN(bool, emitted, specialized.TryRun());
      if (emitted) {
        return std::optional<Relation>(std::move(result));
      }
    }
  }
  FusedGroupProbe probe_task{
      .layout = layout,
      .raw_aggregates = raw_aggregates,
      .source = {
          .left = left,
          .fusion = fusion,
          .deferred_left_probe = deferred_left_probe,
          .right_ptr = right_ptr,
          .left_width = left_width,
          .residual_programs = residual_programs,
          .key_shapes = key_shapes,
          .left_columns = left_columns,
          .left_key_offsets = left_key_offsets,
          .total_left = total_left,
          .aggregate_shapes = aggregate_shapes,
          .read_right_cell = read_right_cell,
          .def_base_types = def_base_types,
          .def_overwritten_by_pk = def_overwritten_by_pk,
          .prepared_pk_lookups = prepared_pk_lookups,
          .def_pk_groups = def_pk_groups,
          .def_col_to_pk_lookup = def_col_to_pk_lookup,
          .workers = workers,
          .probe_chunk_rows = probe_chunk_rows,
          .chunk_count = chunk_count,
          .col_morsels = col_morsels,
          .int_group_slot = int_group_slot,
          .join_type = join_type,
          .singleton_right_relation = singleton_right_relation,
          .all_direct_group_keys = all_direct_group_keys,
          .integer_key = integer_key,
          .composite_int_key = composite_int_key,
          .int_group_key = int_group_key,
          .int_group_type = int_group_type,
          .anti_unmatched_only = anti_unmatched_only,
          .bulk_raw_count = bulk_raw_count,
          .compact_raw_left_key = compact_raw_left_key,
          .compact_counts = compact_counts,
          .compact_log_allowed = compact_log_allowed,
      },
      .keys = {
          .int_buckets = int_buckets,
          .composite_buckets = composite_buckets,
          .dense_composite_keys = dense_composite_keys,
          .dense_composite_rows = dense_composite_rows,
          .dense_composite_min = dense_composite_min,
          .key_buckets = key_buckets,
          .bulk_counts = bulk_counts,
          .dense_bulk_counts = dense_bulk_counts,
          .dense_bulk_min = dense_bulk_min,
          .key_cache = key_cache,
          .key_cache_charge = key_cache_charge,
          .dense_int_idxs = dense_int_idxs,
          .ranked_int_bits = ranked_int_bits,
          .ranked_int_prefix = ranked_int_prefix,
          .ranked_int_rows = ranked_int_rows,
          .ranked_int_span = ranked_int_span,
          .dense_int_min = dense_int_min,
          .unique_int_table = unique_int_table,
          .unique_int_mask = unique_int_mask,
          .fanout_key_bounds = fanout_key_bounds,
          .right_row_gid = right_row_gid,
          .use_dense_composite = use_dense_composite,
          .use_dense_bulk = use_dense_bulk,
          .use_dense_int_rows = use_dense_int_rows,
          .packed_2x32_int_key = packed_2x32_int_key,
          .use_unique_int_table = use_unique_int_table,
          .identity_right_gid = identity_right_gid,
          .use_preassigned_build_groups = use_preassigned_build_groups,
      },
      .direct = {
          .agg_count = agg_count,
          .direct_gid_workers = direct_gid_workers,
          .ordered_sum_charge = ordered_sum_charge,
          .use_direct_gid_agg = use_direct_gid_agg,
          .use_partitioned_count_distinct = use_partitioned_count_distinct,
          .single_double_sum = single_double_sum,
          .use_shared_direct_gid = use_shared_direct_gid,
          .scalar_fact_charge = scalar_fact_charge,
      },
      .deferred = {
          .left_numeric = left_numeric,
          .compiled_double_arith = compiled_double_arith,
          .disjoint_bucket5_slots = disjoint_bucket5_slots,
          .dense_pk_group_indices = dense_pk_group_indices,
          .radix_primary = radix_primary,
          .worker_radix_buckets = worker_radix_buckets,
          .flat_run_index = flat_run_index,
          .flat_run_bounds = flat_run_bounds,
          .disjoint_bucket5_col0 = disjoint_bucket5_col0,
          .disjoint_bucket5_col1 = disjoint_bucket5_col1,
          .left_cell_agg_ready = left_cell_agg_ready,
          .uses_double_arith_cell_op = uses_double_arith_cell_op,
          .left_numeric_probe = left_numeric_probe,
          .disjoint_bucket5_ready = disjoint_bucket5_ready,
          .def_compact_split_probe = def_compact_split_probe,
          .def_group_cell_probe = def_group_cell_probe,
          .use_dense_split_direct_gid = use_dense_split_direct_gid,
          .use_packed_double_cells = use_packed_double_cells,
          .use_radix_agg = use_radix_agg,
          .radix_tuple_is_int = radix_tuple_is_int,
          .disjoint_direct_groups = disjoint_direct_groups,
      },
      .callbacks = {
          .record_compact_split = std::ref(record_compact_split),
          .process_pair = std::ref(process_pair),
          .unmatched_left = std::ref(unmatched_left),
          .accumulate_preassigned_pair = std::ref(accumulate_preassigned_pair),
      },
  };

  const bool track_group_phases =
      ::tinylamb::platform::GetEnv("TINYLAMB_GROUP_PHASES") != nullptr &&
      effective_left_rows >= 100000;
  std::chrono::steady_clock::time_point phase_probe_begin = join_begin;
  std::chrono::steady_clock::time_point phase_merge_begin = join_begin;
  std::chrono::steady_clock::time_point phase_merge_done = join_begin;
  std::chrono::steady_clock::time_point phase_emit_begin = join_begin;
  phase_probe_begin = std::chrono::steady_clock::now();
  if (locals.size() == 1) {
    // Small grouped joins need no helper or transfer. Besides thread startup,
    // this preserves the caller's expression scratch capacity across queries.
    locals[0].error = probe_task.Run(0, locals[0], scratch_storage[0]);
  } else {
    RETURN_IF_FAIL(RunParallelTasks(locals.size(), [&](size_t w) {
      locals[w].error = probe_task.Run(w, locals[w], scratch_storage[w]);
    }));
  }
  for (const FusedWorkerLocal& local : locals) {
    RETURN_IF_FAIL(local.error);
  }
  if (ordered_sum_charge) {
    // A group belongs to one fold worker. Visiting entries in input order
    // preserves every floating-point prefix while avoiding a lock per row.
    RETURN_IF_FAIL(RunParallelTasks(workers, [&](size_t worker) {
      auto& state = direct_gid_workers[0];
      const size_t chunk = (pre_group_count + workers - 1) / workers;
      const size_t begin = worker * chunk;
      const size_t end = std::min(pre_group_count, begin + chunk);
      for (const auto& item : ordered_sum_inputs) {
        if (item.gid < begin || item.gid >= end) {
          continue;
        }
        const size_t gid = item.gid;
        state.matched[gid] = 1;
        if (!item.nonnull) {
          continue;
        }
        const double previous = state.doubles[gid];
        const double next = previous + item.value;
        if (std::isinf(next) && !std::isinf(previous) &&
            !std::isinf(item.value)) {
          state.inf_flags[gid] = 1;
        }
        state.doubles[gid] = next;
        state.counts[gid] = 1;
      }
    }));
  }
  result.hash_joins = left.hash_joins + right.hash_joins + 1;
  result.in_memory_hash_joins =
      left.in_memory_hash_joins + right.in_memory_hash_joins + 1;
  result.nested_loop_joins = left.nested_loop_joins + right.nested_loop_joins;
  result.hybrid_hash_joins = left.hybrid_hash_joins + right.hybrid_hash_joins;
  result.join_comparisons = left.join_comparisons + right.join_comparisons;
  for (const FusedWorkerLocal& local : locals) {
    result.join_comparisons += local.comparisons;
  }
  phase_merge_begin = std::chrono::steady_clock::now();

  // Merge partials in worker order.  Groups are visited through each
  // worker's insertion-ordered vector (not its hash map) so the emission
  // order is deterministic; a group first seen by an earlier worker merges
  // later partials into its adopted state.
  const auto same_group_key = [&](FusedGroup& a, FusedGroup& b) -> bool {
    if (!a.key.values_.empty() && !b.key.values_.empty()) {
      return GroupKeyEqual{}(a.key, b.key);
    }
    if (!all_direct_group_keys) {
      ensure_group_key(a);
      ensure_group_key(b);
      return GroupKeyEqual{}(a.key, b.key);
    }
    Value tmp_a;
    Value tmp_b;
    const auto key_cell_of = [&](const FusedGroup& g, size_t k,
                                 Value* scratch_v) -> const Value& {
      if (!g.key.values_.empty()) {
        return g.key.values_[k];
      }
      return read_group_pair_cell(
          g, static_cast<size_t>(key_shapes[k].direct_column), scratch_v);
    };
    if (!int_key_indices.empty()) {
      for (const size_t ik : int_key_indices) {
        const Value& x = key_cell_of(a, ik, &tmp_a);
        const Value& y = key_cell_of(b, ik, &tmp_b);
        if (x.type != y.type ||
            (!x.IsNull() && x.value.int_value != y.value.int_value)) {
          return false;
        }
      }
    }
    if (int_key_indices.size() < key_programs.size()) {
      for (size_t k = 0; k < key_programs.size(); ++k) {
        const Value& x = key_cell_of(a, k, &tmp_a);
        const Value& y = key_cell_of(b, k, &tmp_b);
        if (&x == &y) {
          continue;
        }
        if (x.type != y.type) {
          return false;
        }
        if (x.type == ValueType::kDouble) {
          const double p = x.value.double_value;
          const double q = y.value.double_value;
          if (std::isnan(p) && std::isnan(q)) {
            continue;
          }
          if (!(p == q)) {
            return false;
          }
          continue;
        }
        if (!(x == y)) {
          return false;
        }
      }
    }
    return true;
  };
  GroupKeyMap offsets;
  FlatIntKeyMap<size_t> int_merge_offsets;
  std::vector<FusedGroup> groups;
  FusedRawArena merged_raw_arena;
  merged_raw_arena.has_stat = has_stat_raw_agg;
  merged_raw_arena.has_double = !raw_aggregates.raw_sum_fusion || has_double_raw_agg;
  merged_raw_arena.has_int = !raw_aggregates.raw_sum_fusion || has_int_raw_agg;
  std::vector<FusedRawArena> extra_merged_arenas;
  std::deque<AggregateAccumulator> states;
  if (compact_counts) {
    if (compact_split_key_only) {
      const uint64_t r_mask =
          (!layout.split_uses_right_subgid || layout.split_right_subgid_count <= 1)
              ? 0ULL
              : ((uint64_t{1} << std::bit_width(layout.split_right_subgid_count - 1)) -
                 1ULL);
      const auto make_split_group = [&](uint64_t packed_u64, int64_t cnt) {
        FusedGroup group;
        group.key_hash = packed_u64;
        group.has_key_hash = true;
        group.rep_left = nullptr;
        if (layout.split_uses_right_subgid) {
          const auto r_subgid =
              static_cast<size_t>((packed_u64 >> layout.split_right_shift) & r_mask);
          group.rep_right_idx = layout.split_right_rep_ptr[r_subgid];
        } else if (layout.right_row_count > 0) {
          group.rep_right_idx = 0;
        }
        group.raw_count = cnt;
        return group;
      };
      const bool any_sharded =
          std::ranges::any_of(locals, [](const auto& local) {
            return !local.compact_split_shards.empty();
          });
      if (!any_sharded) {
        if (locals.size() == 1) {
          for (const auto& entry : locals.front().compact_counts) {
            if (compact_count_passes(entry.count)) {
              groups.push_back(make_split_group(entry.hash, entry.count));
            }
          }
        } else {
          std::vector<uint64_t> merged_keys;
          std::vector<int64_t> merged_counts;
          FlatIntKeyMap<size_t> compact_offsets;
          size_t hint = 0;
          for (const auto& local : locals) {
            hint += local.compact_counts.size();
          }
          merged_keys.reserve(hint);
          merged_counts.reserve(hint);
          compact_offsets.Reserve(hint);
          for (const auto& local : locals) {
            for (const auto& entry : local.compact_counts) {
              bool inserted = false;
              size_t* slot = compact_offsets.TryEmplace(
                  std::bit_cast<int64_t>(entry.hash), &inserted);
              if (!inserted) {
                merged_counts[*slot] += entry.count;
              } else {
                *slot = merged_keys.size();
                merged_keys.push_back(entry.hash);
                merged_counts.push_back(entry.count);
              }
            }
          }
          for (size_t i = 0; i < merged_keys.size(); ++i) {
            if (compact_count_passes(merged_counts[i])) {
              groups.push_back(
                  make_split_group(merged_keys[i], merged_counts[i]));
            }
          }
        }
      } else {
        struct PreSplitEntry {
          uint64_t key;
          int64_t count;
        };
        std::array<std::vector<PreSplitEntry>, kCompactSplitShards>
            pre_entries{};
        size_t total_entries = 0;
        for (auto& local : locals) {
          for (const auto& entry : local.compact_counts) {
            uint64_t h = entry.hash + 0x9e3779b97f4a7c15ULL;
            h = (h ^ (h >> 30U)) * 0xbf58476d1ce4e5b9ULL;
            h = (h ^ (h >> 27U)) * 0x94d049bb133111ebULL;
            h ^= h >> 31U;
            pre_entries[h & (kCompactSplitShards - 1)].push_back(
                PreSplitEntry{.key = entry.hash, .count = entry.count});
            ++total_entries;
          }
          std::vector<CompactCountGroup>().swap(local.compact_counts);
          local.int_offsets = FlatIntKeyMap<size_t>();
          for (const auto& shard : local.compact_split_shards) {
            total_entries += shard.size();
          }
        }
        const size_t shard_workers = std::min<size_t>(
            kCompactSplitShards,
            ParallelWorkerCount(std::max<size_t>(1, total_entries / 2048)));
        std::array<std::vector<PreSplitEntry>, kCompactSplitShards>
            shard_accepted{};
        RETURN_IF_FAIL(RunParallelTasks(shard_workers, [&](size_t sw) {
          if (total_entries < std::numeric_limits<uint32_t>::max()) {
            struct CompactShardSlot {
              uint64_t key{0};
              uint32_t gen{0};
              uint32_t count{0};
            };
            std::vector<CompactShardSlot> table;
            std::vector<uint32_t> occupied;
            uint32_t gen = 0;
            for (size_t s = sw; s < kCompactSplitShards; s += shard_workers) {
              size_t shard_n = pre_entries[s].size();
              for (const auto& local : locals) {
                if (s < local.compact_split_shards.size()) {
                  shard_n += local.compact_split_shards[s].size();
                }
              }
              if (shard_n == 0) {
                continue;
              }
              const size_t min_cap =
                  std::max<size_t>(64, shard_n + (shard_n / 2) + 16);
              if (min_cap > table.size()) {
                table.assign(std::bit_ceil(min_cap), CompactShardSlot{});
                gen = 1;
              } else {
                ++gen;
                if (gen == 0) {
                  std::fill(table.begin(), table.end(), CompactShardSlot{});
                  gen = 1;
                }
              }
              const size_t mask = table.size() - 1;
              occupied.clear();
              const auto add_key = [&](uint64_t key, uint32_t add) {
                uint64_t h =
                    (key + 0x9e3779b97f4a7c15ULL) * 0xbf58476d1ce4e5b9ULL;
                h ^= h >> 30U;
                size_t idx = static_cast<size_t>(h) & mask;
                while (true) {
                  CompactShardSlot& slot_entry = table[idx];
                  if (slot_entry.gen != gen) {
                    slot_entry.gen = gen;
                    slot_entry.key = key;
                    slot_entry.count = add;
                    occupied.push_back(static_cast<uint32_t>(idx));
                    return;
                  }
                  if (slot_entry.key == key) {
                    slot_entry.count += add;
                    return;
                  }
                  idx = (idx + 1) & mask;
                }
              };
              for (const auto& pre : pre_entries[s]) {
                add_key(pre.key, static_cast<uint32_t>(pre.count));
              }
              for (auto& local : locals) {
                if (s >= local.compact_split_shards.size()) {
                  continue;
                }
                for (const uint64_t k : local.compact_split_shards[s]) {
                  add_key(k, 1U);
                }
                std::vector<uint64_t>().swap(local.compact_split_shards[s]);
              }
              for (const uint32_t idx : occupied) {
                const auto signed_cnt = static_cast<int64_t>(table[idx].count);
                if (compact_count_passes(signed_cnt)) {
                  shard_accepted[s].push_back(PreSplitEntry{
                      .key = table[idx].key, .count = signed_cnt});
                }
              }
            }
            return;
          }
          FlatIntKeyMap<uint64_t> counts_map;
          for (size_t s = sw; s < kCompactSplitShards; s += shard_workers) {
            size_t shard_n = pre_entries[s].size();
            for (const auto& local : locals) {
              if (s < local.compact_split_shards.size()) {
                shard_n += local.compact_split_shards[s].size();
              }
            }
            if (shard_n == 0) {
              continue;
            }
            counts_map.Clear();
            counts_map.Reserve(shard_n);
            for (const auto& pre : pre_entries[s]) {
              *counts_map.TryEmplace(std::bit_cast<int64_t>(pre.key)) +=
                  static_cast<uint64_t>(pre.count);
            }
            for (auto& local : locals) {
              if (s >= local.compact_split_shards.size()) {
                continue;
              }
              for (const uint64_t k : local.compact_split_shards[s]) {
                ++(*counts_map.TryEmplace(std::bit_cast<int64_t>(k)));
              }
              std::vector<uint64_t>().swap(local.compact_split_shards[s]);
            }
            counts_map.ForEach([&](int64_t k, uint64_t cnt) {
              const auto signed_cnt = static_cast<int64_t>(cnt);
              if (compact_count_passes(signed_cnt)) {
                shard_accepted[s].push_back(PreSplitEntry{
                    .key = std::bit_cast<uint64_t>(k), .count = signed_cnt});
              }
            });
          }
        }));
        for (size_t s = 0; s < kCompactSplitShards; ++s) {
          for (const auto& entry : shard_accepted[s]) {
            groups.push_back(make_split_group(entry.key, entry.count));
          }
        }
      }
      fusion->post_having_applied = true;
    } else {
      size_t partial_count = 0;
      for (const auto& local : locals) {
        partial_count += local.compact_counts.size();
      }
      const size_t merge_workers =
          std::min(locals.size(), ParallelWorkerCount(partial_count / 4096));
      static const bool merge_disabled =
          ::tinylamb::platform::GetEnv(
              "TINYLAMB_DISABLE_FUSED_PARTITION_MERGE") != nullptr;
      auto merge_charge = !merge_disabled && merge_workers > 1 &&
                                  partial_count <= SIZE_MAX / 256
                              ? TryReserveQueryMemory(partial_count * 256)
                              : nullptr;
      if (merge_charge) {
        static const bool prefilter_disabled =
            ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_COUNT_PREFILTER") !=
            nullptr;
        uint8_t prefilter_limit = 0;
        if (!prefilter_disabled) {
          if (layout.compact_integer_threshold && *layout.compact_integer_threshold >= 3 &&
              *layout.compact_integer_threshold <= 254) {
            prefilter_limit =
                static_cast<uint8_t>(*layout.compact_integer_threshold + 1);
          } else if (!layout.compact_integer_threshold &&
                     fusion->post_having_constant_or_factor >= 3.0 &&
                     fusion->post_having_constant_or_factor < 255.0) {
            prefilter_limit = static_cast<uint8_t>(
                static_cast<unsigned>(fusion->post_having_constant_or_factor) +
                1U);
          }
        }
        struct Ref {
          const CompactCountGroup* group;
          size_t rank;
        };
        struct RankedCount {
          CompactCountGroup group;
          size_t rank;
        };
        std::vector<size_t> rank_bases(locals.size(), 0);
        for (size_t w = 1; w < locals.size(); ++w) {
          rank_bases[w] =
              rank_bases[w - 1] + locals[w - 1].compact_counts.size();
        }
        std::vector<std::vector<std::vector<Ref>>> worker_buckets(
            locals.size());
        RETURN_IF_FAIL(RunParallelTasks(locals.size(), [&](size_t w) {
          const auto& counts = locals[w].compact_counts;
          auto& wb = worker_buckets[w];
          wb.resize(merge_workers);
          const size_t approx = (counts.size() / merge_workers) + 16;
          for (auto& b : wb) {
            b.reserve(approx);
          }
          size_t r = rank_bases[w];
          for (const auto& group : counts) {
            const size_t partition =
                (group.hash ^ (group.hash >> 32U)) % merge_workers;
            wb[partition].push_back(Ref{.group = &group, .rank = r++});
          }
        }));
        std::vector<std::vector<RankedCount>> accepted(merge_workers);
        RETURN_IF_FAIL(RunParallelTasks(merge_workers, [&](size_t p) {
          size_t part_size = 0;
          for (size_t w = 0; w < locals.size(); ++w) {
            part_size += worker_buckets[w][p].size();
          }
          std::vector<Ref> bucket_p;
          bucket_p.reserve(part_size);
          for (size_t w = 0; w < locals.size(); ++w) {
            const auto& wb = worker_buckets[w][p];
            bucket_p.insert(bucket_p.end(), wb.begin(), wb.end());
          }
          std::vector<Ref> candidates;
          const auto* refs = &bucket_p;
          if (prefilter_limit != 0 && bucket_p.size() >= 8192) {
            // Lean proof: lean/TinylambOpt/PredicateAndRelationalLowering.lean
            //   (capped_bucket_count_keeps_candidate)
            // Counter collisions can only increase a frequency. Saturating at
            // the acceptance bound therefore rejects only groups that cannot
            // satisfy COUNT's HAVING predicate. Every candidate still uses
            // the exact key comparison, exact count and original final
            // predicate. The four arrays use at most four bytes per input
            // reference, covered by the merge reservation above.
            const size_t width = std::bit_ceil(bucket_p.size() / 2);
            const size_t mask = width - 1;
            std::vector<uint8_t> counts(width * 4);
            const auto mixed_hash = [](uint64_t hash) {
              hash ^= hash >> 30U;
              hash *= 0xbf58476d1ce4e5b9ULL;
              hash ^= hash >> 27U;
              hash *= 0x94d049bb133111ebULL;
              return hash ^ (hash >> 31U);
            };
            for (const auto& ref : bucket_p) {
              uint64_t hash = mixed_hash(ref.group->hash);
              const auto add = static_cast<unsigned>(
                  std::min<int64_t>(ref.group->count, prefilter_limit));
              for (size_t lane = 0; lane < 4; ++lane) {
                auto& cell = counts[(lane * width) + (hash & mask)];
                cell = static_cast<uint8_t>(std::min<unsigned>(
                    prefilter_limit, static_cast<unsigned>(cell) + add));
                hash = std::rotl(hash, 16);
              }
            }
            candidates.reserve(std::min<size_t>(4096, bucket_p.size()));
            for (const auto& ref : bucket_p) {
              uint64_t hash = mixed_hash(ref.group->hash);
              bool possible = true;
              for (size_t lane = 0; lane < 4; ++lane) {
                if (counts[(lane * width) + (hash & mask)] < prefilter_limit) {
                  possible = false;
                  break;
                }
                hash = std::rotl(hash, 16);
              }
              if (possible) {
                candidates.push_back(ref);
              }
            }
            refs = &candidates;
          }
          FlatIntKeyMap<size_t> index;
          std::vector<RankedCount> merged;
          merged.reserve(refs->size());
          index.Reserve(refs->size());
          for (const auto& ref : *refs) {
            const auto& group = *ref.group;
            bool inserted = false;
            size_t* slot =
                index.TryEmplace(std::bit_cast<int64_t>(group.hash), &inserted);
            size_t matching = SIZE_MAX;
            if (!inserted) {
              for (size_t candidate = *slot; candidate != SIZE_MAX;
                   candidate = merged[candidate].group.next) {
                if (compact_key_equal(merged[candidate].group, group)) {
                  matching = candidate;
                  break;
                }
              }
            }
            if (matching != SIZE_MAX) {
              merged[matching].group.count += group.count;
            } else {
              auto added = group;
              added.next = inserted ? SIZE_MAX : *slot;
              *slot = merged.size();
              merged.push_back(RankedCount{.group = added, .rank = ref.rank});
            }
          }
          for (auto& group : merged) {
            if (compact_count_passes(group.group.count)) {
              accepted[p].push_back(group);
            }
          }
        }));
        std::vector<RankedCount> ordered;
        ordered.reserve(accepted.size());
        for (auto& part : accepted) {
          ordered.insert(ordered.end(), std::make_move_iterator(part.begin()),
                         std::make_move_iterator(part.end()));
        }
        std::ranges::sort(ordered, {}, &RankedCount::rank);
        for (const auto& entry : ordered) {
          FusedGroup group;
          group.rep_left = entry.group.left;
          group.rep_right_idx = entry.group.right_idx;
          group.raw_count = entry.group.count;
          groups.push_back(std::move(group));
        }
      } else {
        // A logged first worker can contain repeated keys. Rebuild its index
        // too when the parallel merge cannot reserve memory.
        const bool logged = std::ranges::any_of(
            locals, [](const auto& local) { return local.compact_count_log; });
        auto merged = logged ? std::vector<CompactCountGroup>{}
                             : std::move(locals.front().compact_counts);
        auto compact_offsets = logged ? FlatIntKeyMap<size_t>{}
                                      : std::move(locals.front().int_offsets);
        for (size_t w = logged ? 0 : 1; w < locals.size(); ++w) {
          for (const auto& group : locals[w].compact_counts) {
            bool inserted = false;
            size_t* slot = compact_offsets.TryEmplace(
                std::bit_cast<int64_t>(group.hash), &inserted);
            size_t matching = SIZE_MAX;
            if (!inserted) {
              for (size_t index = *slot; index != SIZE_MAX;
                   index = merged[index].next) {
                if (compact_key_equal(merged[index], group)) {
                  matching = index;
                  break;
                }
              }
            }
            if (matching != SIZE_MAX) {
              merged[matching].count += group.count;
            } else {
              auto added = group;
              added.next = inserted ? SIZE_MAX : *slot;
              *slot = merged.size();
              merged.push_back(added);
            }
          }
        }
        for (const auto& count : merged) {
          if (compact_count_passes(count.count)) {
            FusedGroup group;
            group.rep_left = count.left;
            group.rep_right_idx = count.right_idx;
            group.raw_count = count.count;
            groups.push_back(std::move(group));
          }
        }
      }
    }
    fusion->post_having_applied = true;
  }
  if (use_direct_gid_agg || use_dense_split_direct_gid) {
    const size_t gid_domain =
        use_dense_split_direct_gid ? dense_split_domain : pre_group_count;
    const size_t dw_count = direct_gid_workers.size();
    const auto merge_gid_range = [&](size_t begin, size_t end,
                                     FusedRawArena& arena,
                                     std::vector<FusedGroup>& out_groups) {
      out_groups.reserve(std::min<size_t>(end - begin, 16384));
      if (use_packed_double_cells) {
        for (size_t gid = begin; gid < end; ++gid) {
          double sum = 0.0;
          uint64_t total_cnt = 0;
          uint8_t any_matched = 0;
          uint8_t any_inf = 0;
          for (size_t w = 0; w < dw_count; ++w) {
            const DirectDoubleCell& c = direct_gid_workers[w].double_cells[gid];
            if ((c.meta & 1ULL) == 0) {
              continue;
            }
            if (!any_matched) {
              sum = c.sum;
              any_matched = 1;
            } else {
              const double next = sum + c.sum;
              if (std::isinf(next) && !std::isinf(sum) && !std::isinf(c.sum)) {
                any_inf = 1;
              }
              sum = next;
            }
            total_cnt += (c.meta >> 2);
            any_inf |= static_cast<uint8_t>((c.meta >> 1) & 1ULL);
          }
          if (any_matched == 0) {
            continue;
          }
          FusedGroup grp;
          auto tmp_gid = static_cast<uint32_t>(gid);
          uint64_t packed_u64 = 0;
          for (size_t idx = dense_pk_group_indices.size(); idx > 0; --idx) {
            const size_t gi = dense_pk_group_indices[idx - 1];
            const uint32_t card = def_pk_groups[gi].subgid_count;
            const uint32_t psg = tmp_gid % card;
            tmp_gid /= card;
            packed_u64 |= static_cast<uint64_t>(psg)
                          << def_pk_groups[gi].subgid_shift;
          }
          const uint32_t r_subgid = tmp_gid;
          if (layout.split_uses_right_subgid) {
            packed_u64 |= static_cast<uint64_t>(r_subgid) << layout.split_right_shift;
          }
          grp.key_hash = packed_u64;
          grp.has_key_hash = true;
          grp.rep_left = nullptr;
          grp.rep_right_idx =
              layout.split_uses_right_subgid
                  ? layout.split_right_rep_ptr[r_subgid]
                  : (layout.right_row_count > 0 ? 0U : kInvalidRightIdx);
          arena.Initialize(grp, raw_aggregates.raw_slot_count);
          grp.RawDoubles()[0] = sum;
          grp.RawCounts()[0] = static_cast<int64_t>(total_cnt);
          grp.RawInfFlags()[0] = any_inf;
          out_groups.push_back(std::move(grp));
        }
        return;
      }
      for (size_t gid = begin; gid < end; ++gid) {
        bool any_matched = false;
        for (size_t w = 0; w < dw_count; ++w) {
          if (direct_gid_workers[w].matched[gid] != 0) {
            any_matched = true;
            break;
          }
        }
        if (!any_matched) {
          continue;
        }
        FusedGroup grp;
        if (use_dense_split_direct_gid) {
          auto tmp_gid = static_cast<uint32_t>(gid);
          uint64_t packed_u64 = 0;
          for (size_t idx = dense_pk_group_indices.size(); idx > 0; --idx) {
            const size_t gi = dense_pk_group_indices[idx - 1];
            const uint32_t card = def_pk_groups[gi].subgid_count;
            const uint32_t psg = tmp_gid % card;
            tmp_gid /= card;
            packed_u64 |= static_cast<uint64_t>(psg)
                          << def_pk_groups[gi].subgid_shift;
          }
          const uint32_t r_subgid = tmp_gid;
          if (layout.split_uses_right_subgid) {
            packed_u64 |= static_cast<uint64_t>(r_subgid) << layout.split_right_shift;
          }
          grp.key_hash = packed_u64;
          grp.has_key_hash = true;
          grp.rep_left = nullptr;
          grp.rep_right_idx =
              layout.split_uses_right_subgid
                  ? layout.split_right_rep_ptr[r_subgid]
                  : (layout.right_row_count > 0 ? 0U : kInvalidRightIdx);
        } else if (identity_right_gid) {
          grp.rep_right_idx = static_cast<uint32_t>(gid);
        } else {
          grp = std::move(pre_groups[gid]);
        }
        if (raw_aggregates.raw_sum_fusion) {
          arena.Initialize(grp, raw_aggregates.raw_slot_count);
        }
        if (raw_aggregates.raw_count_fusion) {
          int64_t total_cnt = 0;
          for (size_t w = 0; w < dw_count; ++w) {
            total_cnt += direct_gid_workers[w].counts[gid];
          }
          grp.raw_count = total_cnt;
        } else {
          const size_t base = gid * agg_count;
          for (size_t w = 0; w < dw_count; ++w) {
            const DirectGidWorker& dw = direct_gid_workers[w];
            if (dw.matched[gid] == 0) {
              continue;
            }
            for (size_t s = 0; s < agg_count; ++s) {
              switch (raw_aggregates.slot_agg_kinds[s]) {
                case RawAggKind::kCountStar:
                case RawAggKind::kConstantExtremum:
                case RawAggKind::kCountValue:
                  grp.RawCounts()[s] += dw.counts[base + s];
                  break;
                case RawAggKind::kSumInt64: {
                  int64_t next = 0;
                  if (__builtin_add_overflow(grp.RawInts()[s],
                                             dw.ints[base + s], &next)) {
                    grp.RawInfFlags()[s] = 1;
                  } else {
                    grp.RawInts()[s] = next;
                  }
                  grp.RawCounts()[s] += dw.counts[base + s];
                  grp.RawInfFlags()[s] |= dw.inf_flags[base + s];
                  break;
                }
                case RawAggKind::kSumDouble:
                case RawAggKind::kAvgDouble: {
                  const double next =
                      grp.RawDoubles()[s] + dw.doubles[base + s];
                  if (std::isinf(next) && !std::isinf(grp.RawDoubles()[s]) &&
                      !std::isinf(dw.doubles[base + s])) {
                    grp.RawInfFlags()[s] = 1;
                  }
                  grp.RawDoubles()[s] = next;
                  grp.RawCounts()[s] += dw.counts[base + s];
                  grp.RawInfFlags()[s] |= dw.inf_flags[base + s];
                  break;
                }
                case RawAggKind::kStddevSamp:
                case RawAggKind::kStddevPop:
                case RawAggKind::kVarSamp:
                case RawAggKind::kVarPop:
                  grp.RawStatX()[s] += dw.ext_sx[base + s];
                  grp.RawStatXX()[s] += dw.ext_sxx[base + s];
                  grp.RawCounts()[s] += dw.counts[base + s];
                  grp.RawInfFlags()[s] |= dw.inf_flags[base + s];
                  break;
                case RawAggKind::kCountDistinctInt64:
                case RawAggKind::kNone:
                  break;
              }
            }
          }
        }
        out_groups.push_back(std::move(grp));
      }
    };
    const size_t gid_workers =
        gid_domain >= 16384 && workers > 1
            ? std::min(workers, ParallelWorkerCount(gid_domain / 4096))
            : 1;
    if (gid_workers > 1) {
      extra_merged_arenas.resize(gid_workers);
      for (FusedRawArena& a : extra_merged_arenas) {
        a.has_stat = has_stat_raw_agg;
        a.has_double = !raw_aggregates.raw_sum_fusion || has_double_raw_agg;
        a.has_int = !raw_aggregates.raw_sum_fusion || has_int_raw_agg;
      }
      std::vector<std::vector<FusedGroup>> slice_groups(gid_workers);
      RETURN_IF_FAIL(RunParallelTasks(gid_workers, [&](size_t mw) {
        const size_t begin = (gid_domain * mw) / gid_workers;
        const size_t end = (gid_domain * (mw + 1)) / gid_workers;
        merge_gid_range(begin, end, extra_merged_arenas[mw], slice_groups[mw]);
      }));
      size_t total_merged = 0;
      for (const auto& sg : slice_groups) {
        total_merged += sg.size();
      }
      groups.reserve(total_merged);
      for (auto& sg : slice_groups) {
        groups.insert(groups.end(), std::make_move_iterator(sg.begin()),
                      std::make_move_iterator(sg.end()));
      }
    } else {
      merge_gid_range(0, gid_domain, merged_raw_arena, groups);
    }
    if (dw_count > 1) {
      RETURN_IF_FAIL(RunParallelTasks(dw_count, [&](size_t w) {
        DirectGidWorker{}.matched.swap(direct_gid_workers[w].matched);
        DirectGidWorker{}.doubles.swap(direct_gid_workers[w].doubles);
        DirectGidWorker{}.ints.swap(direct_gid_workers[w].ints);
        DirectGidWorker{}.counts.swap(direct_gid_workers[w].counts);
        DirectGidWorker{}.inf_flags.swap(direct_gid_workers[w].inf_flags);
        DirectGidWorker{}.ext_sx.swap(direct_gid_workers[w].ext_sx);
        DirectGidWorker{}.ext_sxx.swap(direct_gid_workers[w].ext_sxx);
        std::vector<DirectDoubleCell>{}.swap(
            direct_gid_workers[w].double_cells);
      }));
    }
    std::vector<DirectGidWorker>().swap(direct_gid_workers);
  } else if (use_partitioned_count_distinct) {
    std::vector<int64_t> cd_counts(pre_group_count, 0);
    auto sort_partition = [&](size_t p) {
      size_t total_pairs = 0;
      for (size_t w = 0; w < workers; ++w) {
        total_pairs += cd_worker_parts[w][p].size();
      }
      if (total_pairs == 0) {
        return;
      }
      // Lean proof: lean/TinylambOpt/VectorizedJoinAndPruning.lean
      //   (parallel_independent_group_dedup_eq)
      // Bucket-scatter `(gid, val)` pairs by `gid` into contiguous per-group
      // `int64_t` slices, then sort+dedup each tiny slice in L1 cache instead
      // of sorting 16-byte `(gid, val)` pairs across the whole partition.
      const size_t n_local = (pre_group_count + workers - 1) / workers;
      std::vector<uint32_t> offsets(n_local + 1, 0);
      for (size_t w = 0; w < workers; ++w) {
        for (const GidValPair& pair : cd_worker_parts[w][p]) {
          ++offsets[(pair.gid / workers) + 1];
        }
      }
      for (size_t i = 1; i <= n_local; ++i) {
        offsets[i] += offsets[i - 1];
      }
      std::vector<int64_t> vals(total_pairs);
      std::vector<uint32_t> cursor(
          offsets.begin(),
          offsets.begin() + static_cast<std::ptrdiff_t>(n_local));
      for (size_t w = 0; w < workers; ++w) {
        auto& vec = cd_worker_parts[w][p];
        for (const GidValPair& pair : vec) {
          vals[cursor[pair.gid / workers]++] = pair.val;
        }
        std::vector<GidValPair>().swap(vec);
      }
      for (size_t lg = 0; lg < n_local; ++lg) {
        const uint32_t b = offsets[lg];
        const uint32_t e = offsets[lg + 1];
        if (b == e) {
          continue;
        }
        const size_t gid = (lg * workers) + p;
        if (gid >= pre_group_count) {
          continue;
        }
        if (e - b == 1) {
          cd_counts[gid] = 1;
          continue;
        }
        int64_t* slice = vals.data() + b;
        const size_t len = e - b;
        std::sort(slice, slice + len);
        int64_t cnt = 1;
        for (size_t i = 1; i < len; ++i) {
          cnt += static_cast<int64_t>(slice[i] != slice[i - 1]);
        }
        cd_counts[gid] = cnt;
      }
    };
    RETURN_IF_FAIL(RunParallelTasks(workers, sort_partition));
    groups.reserve(pre_group_count);
    for (size_t gid = 0; gid < pre_group_count; ++gid) {
      bool any_matched = (cd_counts[gid] > 0);
      if (!any_matched) {
        for (size_t w = 0; w < workers; ++w) {
          if (cd_worker_matched[w][gid] != 0) {
            any_matched = true;
            break;
          }
        }
      }
      if (!any_matched) {
        continue;
      }
      FusedGroup grp = std::move(pre_groups[gid]);
      merged_raw_arena.Initialize(grp, fusion->aggregates.size());
      grp.RawCounts()[0] = cd_counts[gid];
      groups.push_back(std::move(grp));
    }
  }
  const auto merge_raw = [&](FusedGroup& destination, FusedGroup& group) {
    for (size_t s = 0; s < raw_aggregates.raw_slot_count; ++s) {
      switch (raw_aggregates.slot_agg_kinds[s]) {
        case RawAggKind::kCountStar:
        case RawAggKind::kConstantExtremum:
        case RawAggKind::kCountValue:
          destination.RawCounts()[s] += group.RawCounts()[s];
          break;
        case RawAggKind::kSumInt64: {
          int64_t next = 0;
          if (__builtin_add_overflow(destination.RawInts()[s],
                                     group.RawInts()[s], &next)) {
            destination.RawInfFlags()[s] = 1;
          } else {
            destination.RawInts()[s] = next;
          }
          destination.RawCounts()[s] += group.RawCounts()[s];
          destination.RawInfFlags()[s] |= group.RawInfFlags()[s];
          break;
        }
        case RawAggKind::kSumDouble:
        case RawAggKind::kAvgDouble: {
          const double next =
              destination.RawDoubles()[s] + group.RawDoubles()[s];
          if (std::isinf(next) && !std::isinf(destination.RawDoubles()[s]) &&
              !std::isinf(group.RawDoubles()[s])) {
            destination.RawInfFlags()[s] = 1;
          }
          destination.RawDoubles()[s] = next;
          destination.RawCounts()[s] += group.RawCounts()[s];
          destination.RawInfFlags()[s] |= group.RawInfFlags()[s];
          break;
        }
        case RawAggKind::kStddevSamp:
        case RawAggKind::kStddevPop:
        case RawAggKind::kVarSamp:
        case RawAggKind::kVarPop:
          destination.RawStatX()[s] += group.RawStatX()[s];
          destination.RawStatXX()[s] += group.RawStatXX()[s];
          destination.RawCounts()[s] += group.RawCounts()[s];
          destination.RawInfFlags()[s] |= group.RawInfFlags()[s];
          break;
        case RawAggKind::kCountDistinctInt64:
          if (group.raw_int_set) {
            if (!destination.raw_int_set) {
              destination.raw_int_set = std::move(group.raw_int_set);
            } else {
              group.raw_int_set->ForEach([&](int64_t v, uint8_t) {
                destination.raw_int_set->TryEmplace(v);
              });
            }
          }
          break;
        case RawAggKind::kNone:
          break;
      }
    }
  };
  std::vector<FusedGroup*> group_ptrs;
  bool use_group_ptrs = false;
  if (use_radix_agg) {
    CompactRadixGroups compact_radix{
      .raw_aggregates = raw_aggregates,
      .layout = layout,
      .context = context,
      .left = left,
      .right_arg = right_arg,
      .fusion = fusion,
      .right_col_only = right_col_only,
      .right_col_owner = right_col_owner,
      .right_locator = right_locator,
      .right = right,
      .join_begin = join_begin,
      .result = result,
      .equality_keys = equality_keys,
      .left_width = left_width,
      .prepared_pk_lookups = prepared_pk_lookups,
      .def_pk_groups = def_pk_groups,
      .def_col_to_pk_lookup = def_col_to_pk_lookup,
      .has_stat_raw_agg = has_stat_raw_agg,
      .workers = workers,
      .locals = locals,
      .agg_count = agg_count,
      .left_numeric = left_numeric,
      .radix_tuple_is_int = radix_tuple_is_int,
      .bounded_radix_charge = bounded_radix_charge,
      .worker_radix_buckets = worker_radix_buckets,
      .track_group_phases = track_group_phases,
    };
    ASSIGN_OR_RETURN(bool, emitted, compact_radix.TryEmit());
    if (emitted) {
      return std::optional<Relation>(std::move(result));
    }
    radix_part_arenas.resize(kRadixParts);
    radix_part_groups.resize(kRadixParts);
    for (size_t p = 0; p < kRadixParts; ++p) {
      radix_part_arenas[p].has_stat = has_stat_raw_agg;
      radix_part_arenas[p].has_double = !raw_aggregates.raw_sum_fusion || has_double_raw_agg;
      radix_part_arenas[p].has_int = !raw_aggregates.raw_sum_fusion || has_int_raw_agg;
    }
    const bool has_split_rep_right =
        layout.split_right_rep_ptr != nullptr &&
        ((layout.split_key_unpack_only && layout.split_uses_right_subgid) ||
         (layout.left_pack_unpack_only && layout.left_pack_uses_right));
    const uint64_t radix_r_mask =
        !has_split_rep_right
            ? 0ULL
            : ((layout.left_pack_unpack_only && layout.left_pack_uses_right)
                   ? layout.left_pack_right_mask
                   : (layout.split_right_subgid_count <= 1
                          ? 0ULL
                          : ((uint64_t{1}
                              << std::bit_width(layout.split_right_subgid_count - 1)) -
                             1ULL)));
    std::atomic<size_t> next_part{0};
    const size_t part_workers = std::min<size_t>(workers, kRadixParts);
    RETURN_IF_FAIL(RunParallelTasks(part_workers, [&](size_t) {
      while (true) {
        const size_t p = next_part.fetch_add(1, std::memory_order_relaxed);
        if (p >= kRadixParts) {
          break;
        }
        size_t part_total = 0;
        for (auto& worker_radix_bucket : worker_radix_buckets) {
          part_total += worker_radix_bucket[p].keys.size();
        }
        if (part_total == 0) {
          continue;
        }
        const size_t part_init =
            std::min<size_t>(std::max<size_t>(part_total / 2, 64), 65536);
        FlatIntKeyMap<size_t> part_map;
        part_map.Reserve(part_init);
        FusedRawArena& arena = radix_part_arenas[p];
        std::vector<FusedGroup>& pgroups = radix_part_groups[p];
        pgroups.reserve(part_init);
        for (auto& worker_radix_bucket : worker_radix_buckets) {
          const RadixSoABucket& bucket = worker_radix_bucket[p];
          const size_t bsz = bucket.keys.size();
          for (size_t idx = 0; idx < bsz; ++idx) {
            const uint64_t t_key = bucket.keys[idx];
            const uint8_t t_valid = bucket.valids[idx];
            const int64_t t_ival = radix_tuple_is_int ? bucket.ivals[idx] : 0;
            const double t_dval = radix_tuple_is_int ? 0.0 : bucket.dvals[idx];
            bool inserted = false;
            size_t* slot =
                part_map.TryEmplace(std::bit_cast<int64_t>(t_key), &inserted);
            size_t gi = 0;
            if (!inserted) [[likely]] {
              gi = *slot;
            } else {
              gi = pgroups.size();
              FusedGroup grp;
              arena.Initialize(grp, raw_aggregates.raw_slot_count);
              grp.key_hash = t_key;
              grp.has_key_hash = true;
              if (has_split_rep_right) {
                const auto r_subgid = static_cast<size_t>(
                    (t_key >> layout.split_right_shift) & radix_r_mask);
                if (r_subgid < layout.split_right_rep_count) {
                  grp.rep_right_idx = layout.split_right_rep_ptr[r_subgid];
                }
              } else if (layout.right_row_count > 0) {
                grp.rep_right_idx = 0;
              }
              pgroups.push_back(std::move(grp));
              *slot = gi;
            }
            FusedGroup& g = pgroups[gi];
            for (size_t s = 0; s < agg_count; ++s) {
              const uint8_t sop = left_numeric[s].op;
              if (sop == 0) {
                g.RawCounts()[s] += 1;
                continue;
              }
              if (t_valid == 0 || sop == 5) {
                continue;
              }
              if (radix_tuple_is_int) {
                if (sop == 3) {
                  int64_t next = 0;
                  if (__builtin_add_overflow(g.RawInts()[s], t_ival, &next)) {
                    g.RawInfFlags()[s] = 1;
                  } else {
                    g.RawInts()[s] = next;
                  }
                  g.RawCounts()[s] += 1;
                } else if (sop == 2) {
                  const auto dv = static_cast<double>(t_ival);
                  const double prev = g.RawDoubles()[s];
                  const double next = prev + dv;
                  if (std::isinf(next) && !std::isinf(prev) &&
                      !std::isinf(dv)) {
                    g.RawInfFlags()[s] = 1;
                  }
                  g.RawDoubles()[s] = next;
                  g.RawCounts()[s] += 1;
                } else if (sop == 4) {
                  g.RawCounts()[s] += 1;
                } else if (sop == 8) {
                  const auto x = static_cast<platform::ExtendedFloat>(t_ival);
                  g.RawStatX()[s] += x;
                  g.RawStatXX()[s] += x * x;
                  g.RawCounts()[s] += 1;
                }
              } else {
                const double dv = t_dval;
                if (sop == 1 || sop == 2 || sop == 6 || sop == 9 || sop == 17) {
                  const double prev = g.RawDoubles()[s];
                  const double next = prev + dv;
                  if (std::isinf(next) && !std::isinf(prev) &&
                      !std::isinf(dv)) {
                    g.RawInfFlags()[s] = 1;
                  }
                  g.RawDoubles()[s] = next;
                  g.RawCounts()[s] += 1;
                } else if (sop == 4 || sop == 7) {
                  g.RawCounts()[s] += 1;
                } else if (sop == 8) {
                  const auto x = static_cast<platform::ExtendedFloat>(dv);
                  g.RawStatX()[s] += x;
                  g.RawStatXX()[s] += x * x;
                  g.RawCounts()[s] += 1;
                }
              }
            }
          }
        }
      }
    }));
    std::vector<std::array<RadixSoABucket, kRadixParts>>().swap(
        worker_radix_buckets);
    bounded_radix_charge.reset();
    size_t fallback_worker_groups = 0;
    for (const FusedWorkerLocal& local : locals) {
      fallback_worker_groups += local.groups.size();
    }
    if (fallback_worker_groups == 0) {
      size_t radix_total_groups = 0;
      for (const auto& pgroups : radix_part_groups) {
        radix_total_groups += pgroups.size();
      }
      group_ptrs.reserve(radix_total_groups);
      for (auto& pgroups : radix_part_groups) {
        for (FusedGroup& g : pgroups) {
          group_ptrs.push_back(&g);
        }
      }
      use_group_ptrs = true;
      if (result.runtime() != nullptr) {
        ++result.runtime()->partitioned_aggregate_merges;
      }
    } else {
      for (auto& pgroups : radix_part_groups) {
        for (FusedGroup& g : pgroups) {
          locals.front().groups.push_back(std::move(g));
        }
        std::vector<FusedGroup>().swap(pgroups);
      }
    }
  }
  size_t total_worker_groups = 0;
  if (!use_group_ptrs) {
    for (const FusedWorkerLocal& local : locals) {
      total_worker_groups += local.groups.size();
    }
    auto extract_single_int_key = [&](const FusedGroup& group,
                                      int64_t* out_key) -> bool {
      Value col_tmp;
      const Value* cell = nullptr;
      if (group.key.values_.size() == 1) {
        cell = group.key.values_.data();
      } else if (all_direct_group_keys &&
                 (group.rep_left != nullptr ||
                  group.rep_col_chunk != std::numeric_limits<uint32_t>::max() ||
                  group.rep_right_idx != kInvalidRightIdx)) {
        cell = &read_group_pair_cell(group, int_group_slot, &col_tmp);
      }
      if (cell != nullptr && !cell->IsNull() && cell->type == int_group_type &&
          !cell->IsUnsigned()) {
        *out_key = cell->value.int_value;
        return true;
      }
      return false;
    };
    auto mix_int_partition = [](int64_t key) noexcept -> uint64_t {
      auto x = static_cast<uint64_t>(key) + 0x9e3779b97f4a7c15ULL;
      x = (x ^ (x >> 30U)) * 0xbf58476d1ce4e5b9ULL;
      x = (x ^ (x >> 27U)) * 0x94d049bb133111ebULL;
      return x ^ (x >> 31U);
    };
    const size_t merge_workers = std::min(
        locals.size(), ParallelWorkerCount(total_worker_groups / 8192));
    const bool partitionable_hash =
        !int_group_key &&
        std::ranges::all_of(locals, [](const FusedWorkerLocal& local) {
          return std::ranges::all_of(local.groups, [](const FusedGroup& group) {
            return group.has_key_hash;
          });
        });
    const bool partitionable_int = int_group_key && all_direct_group_keys;
    const bool partitionable =
        merge_workers > 1 && groups.empty() && all_direct_group_keys &&
        (raw_aggregates.raw_sum_fusion || raw_aggregates.raw_count_fusion || fusion->aggregates.empty()) &&
        (partitionable_hash || partitionable_int) &&
        ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_RAW_PARTITION_MERGE") ==
            nullptr;
    auto partition_charge =
        partitionable && total_worker_groups <= SIZE_MAX / 128
            ? TryReserveQueryMemory(total_worker_groups * 128)
            : nullptr;
    if (partition_charge) {
      struct GroupRef {
        FusedGroup* group;
        size_t rank;
      };
      struct MergedRef {
        GroupRef ref;
        size_t next;
      };
      const size_t local_count = locals.size();
      std::vector<size_t> rank_bases(local_count + 1, 0);
      for (size_t w = 0; w < local_count; ++w) {
        rank_bases[w + 1] = rank_bases[w] + locals[w].groups.size();
      }
      const auto group_partition = [&](const FusedGroup& group) -> size_t {
        if (int_group_key) {
          int64_t k = 0;
          if (!extract_single_int_key(group, &k)) {
            return 0;
          }
          return static_cast<size_t>(mix_int_partition(k) % merge_workers);
        }
        return (group.key_hash ^ (group.key_hash >> 32U)) % merge_workers;
      };
      std::vector<size_t> worker_part_counts(local_count * merge_workers, 0);
      RETURN_IF_FAIL(RunParallelTasks(local_count, [&](size_t w) {
        size_t* counts = worker_part_counts.data() + (w * merge_workers);
        for (const auto& group : locals[w].groups) {
          ++counts[group_partition(group)];
        }
      }));
      std::vector<size_t> worker_part_offsets(local_count * merge_workers, 0);
      std::vector<std::vector<GroupRef>> partitions(merge_workers);
      for (size_t p = 0; p < merge_workers; ++p) {
        size_t off = 0;
        for (size_t w = 0; w < local_count; ++w) {
          worker_part_offsets[(w * merge_workers) + p] = off;
          off += worker_part_counts[(w * merge_workers) + p];
        }
        partitions[p].resize(off);
      }
      RETURN_IF_FAIL(RunParallelTasks(local_count, [&](size_t w) {
        size_t* offsets_w = worker_part_offsets.data() + (w * merge_workers);
        const size_t base_rank = rank_bases[w];
        auto& wgroups = locals[w].groups;
        for (size_t i = 0; i < wgroups.size(); ++i) {
          auto& group = wgroups[i];
          const size_t p = group_partition(group);
          partitions[p][offsets_w[p]++] = {.group = &group,
                                           .rank = base_rank + i};
        }
      }));
      std::vector<std::vector<MergedRef>> merged(merge_workers);
      std::vector<FusedGroup*> first_groups(total_worker_groups, nullptr);
      const bool exact_packed_hash =
          layout.use_split_key_gid || layout.packed_2x32_group_key || layout.use_left_packed_key;
      RETURN_IF_FAIL(RunParallelTasks(merge_workers, [&](size_t p) {
        FlatIntKeyMap<size_t> index;
        index.Reserve(partitions[p].size());
        GroupKeyMap fallback_offsets;
        auto& output = merged[p];
        output.reserve(partitions[p].size());
        for (const auto& ref : partitions[p]) {
          bool inserted = false;
          size_t match = SIZE_MAX;
          size_t next = SIZE_MAX;
          size_t* head = nullptr;
          if (int_group_key) {
            int64_t k = 0;
            if (extract_single_int_key(*ref.group, &k)) {
              head = index.TryEmplace(k, &inserted);
              if (!inserted) {
                match = *head;
              }
            } else {
              ensure_group_key(*ref.group);
              auto [it, ins] =
                  fallback_offsets.emplace(ref.group->key, output.size());
              inserted = ins;
              if (!ins) {
                match = it->second;
              }
            }
          } else {
            head = index.TryEmplace(std::bit_cast<int64_t>(ref.group->key_hash),
                                    &inserted);
            if (!inserted) {
              if (exact_packed_hash) {
                match = *head;
              } else {
                for (size_t i = *head; i != SIZE_MAX; i = output[i].next) {
                  if (same_group_key(*output[i].ref.group, *ref.group)) {
                    match = i;
                    break;
                  }
                }
              }
            }
            next = inserted ? SIZE_MAX : *head;
          }
          if (match == SIZE_MAX) {
            if (head != nullptr) {
              *head = output.size();
            }
            output.push_back({.ref = ref, .next = next});
            first_groups[ref.rank] = ref.group;
          } else if (raw_aggregates.raw_count_fusion) {
            output[match].ref.group->raw_count += ref.group->raw_count;
          } else if (raw_aggregates.raw_sum_fusion) {
            merge_raw(*output[match].ref.group, *ref.group);
          }
        }
      }));
      // Preserve the historical first-worker/first-group emission order as
      // well as the accumulation order within each group (including FLOAT
      // prefixes). Keep FusedGroup storage in worker locals until after
      // emission so each worker frees its own groups in parallel without
      // move-copying structs.
      std::vector<size_t> chunk_counts(merge_workers, 0);
      RETURN_IF_FAIL(RunParallelTasks(merge_workers, [&](size_t c) {
        const size_t begin = (total_worker_groups * c) / merge_workers;
        const size_t end = (total_worker_groups * (c + 1)) / merge_workers;
        size_t cnt = 0;
        for (size_t i = begin; i < end; ++i) {
          if (first_groups[i] != nullptr) {
            ++cnt;
          }
        }
        chunk_counts[c] = cnt;
      }));
      std::vector<size_t> chunk_offsets(merge_workers, 0);
      size_t unique_groups = 0;
      for (size_t c = 0; c < merge_workers; ++c) {
        chunk_offsets[c] = unique_groups;
        unique_groups += chunk_counts[c];
      }
      group_ptrs.resize(unique_groups);
      use_group_ptrs = true;
      RETURN_IF_FAIL(RunParallelTasks(merge_workers, [&](size_t c) {
        const size_t begin = (total_worker_groups * c) / merge_workers;
        const size_t end = (total_worker_groups * (c + 1)) / merge_workers;
        size_t out = chunk_offsets[c];
        for (size_t i = begin; i < end; ++i) {
          if (first_groups[i] != nullptr) {
            group_ptrs[out++] = first_groups[i];
          }
        }
      }));
      if (locals.size() > 1) {
        RETURN_IF_FAIL(RunParallelTasks(locals.size(), [&](size_t w) {
          locals[w].offsets = GroupKeyMap();
          locals[w].int_offsets = FlatIntKeyMap<size_t>();
          std::vector<CompactCountGroup>().swap(locals[w].compact_counts);
        }));
      }
      // Group structs and raw state blocks remain owned by locals until
      // emission completes.
      total_worker_groups = 0;
      if (result.runtime() != nullptr) {
        ++result.runtime()->partitioned_aggregate_merges;
      }
    }
  }
  if (!use_group_ptrs) {
    if (total_worker_groups > 0) {
      groups.reserve(total_worker_groups);
    }
    for (FusedWorkerLocal& local : locals) {
      if (groups.empty() && offsets.empty() && local.offsets.empty() &&
          !local.groups.empty()) {
        groups = std::move(local.groups);
        int_merge_offsets = std::move(local.int_offsets);
        if (!raw_aggregates.skip_accumulator_states) {
          states = std::move(local.states);
        } else {
          local.states.clear();
        }
        if (total_worker_groups > groups.size()) {
          int_merge_offsets.Reserve(total_worker_groups);
          groups.reserve(total_worker_groups);
        }
        continue;
      }
      if (groups.empty() && total_worker_groups > 0) {
        int_merge_offsets.Reserve(total_worker_groups);
      }
      for (FusedGroup& group : local.groups) {
        bool inserted = false;
        size_t target_idx = 0;
        Value single_int_tmp;
        const Value* single_int_cell = nullptr;
        if (int_group_key) {
          if (group.key.values_.size() == 1) {
            single_int_cell = group.key.values_.data();
          } else if (all_direct_group_keys &&
                     (group.rep_left != nullptr ||
                      group.rep_col_chunk !=
                          std::numeric_limits<uint32_t>::max() ||
                      group.rep_right_idx != kInvalidRightIdx)) {
            single_int_cell =
                &read_group_pair_cell(group, int_group_slot, &single_int_tmp);
          }
        }
        if (single_int_cell != nullptr && !single_int_cell->IsNull() &&
            single_int_cell->type == int_group_type &&
            !single_int_cell->IsUnsigned()) {
          size_t* slot = int_merge_offsets.TryEmplace(
              single_int_cell->value.int_value, &inserted);
          if (inserted) {
            *slot = groups.size();
          }
          target_idx = *slot;
        } else if (!int_group_key && group.has_key_hash) {
          bool hash_inserted = false;
          size_t* slot = int_merge_offsets.TryEmplace(
              static_cast<int64_t>(group.key_hash), &hash_inserted);
          if (hash_inserted) {
            *slot = groups.size();
            inserted = true;
            target_idx = *slot;
            // Split keys pack an exact dictionary ID and a signed 32-bit
            // probe value. Their integer slots are collision-free; fallback
            // keys live exclusively in offsets and never enter this map.
          } else if (layout.use_split_key_gid || layout.packed_2x32_group_key ||
                     layout.use_left_packed_key ||
                     same_group_key(groups[*slot], group)) [[likely]] {
            inserted = false;
            target_idx = *slot;
          } else {
            ensure_group_key(groups[*slot]);
            ensure_group_key(group);
            auto [iter, ins] = offsets.emplace(group.key, groups.size());
            inserted = ins;
            target_idx = iter->second;
          }
        } else {
          ensure_group_key(group);
          auto [iter, ins] = offsets.emplace(group.key, groups.size());
          inserted = ins;
          target_idx = iter->second;
        }
        if (inserted) {
          if (!raw_aggregates.skip_accumulator_states) {
            const size_t local_offset = group.state_offset;
            for (size_t i = 0; i < fusion->aggregates.size(); ++i) {
              states.push_back(std::move(local.states[local_offset + i]));
            }
            group.state_offset = states.size() - fusion->aggregates.size();
          }
          groups.push_back(std::move(group));
        } else if (raw_aggregates.raw_count_fusion) {
          groups[target_idx].raw_count += group.raw_count;
        } else if (raw_aggregates.raw_sum_fusion) {
          merge_raw(groups[target_idx], group);
        } else {
          FusedGroup& destination = groups[target_idx];
          for (size_t i = 0; i < fusion->aggregates.size(); ++i) {
            AggregateAccumulator& target = states[destination.state_offset + i];
            RETURN_IF_FAIL(target.Merge(local.states[group.state_offset + i]));
          }
        }
      }
      local.groups.clear();
      local.states.clear();
    }
  }
  // A keyless (global) aggregate over an empty join output still emits one
  // row: accumulators finish to their empty-input values (SUM -> NULL,
  // COUNT -> 0), exactly like the scalar AggregateExecutor.  Without this the
  // fused shape would drop the row entirely.
  if (fusion->group_keys.empty() && groups.empty() && !use_group_ptrs) {
    FusedGroup group;
    if (raw_aggregates.raw_sum_fusion) {
      merged_raw_arena.Initialize(group, raw_aggregates.raw_slot_count);
    }
    if (!raw_aggregates.skip_accumulator_states) {
      group.state_offset = 0;
      for (size_t i = 0; i < fusion->aggregates.size(); ++i) {
        states.emplace_back(fusion->aggregates[i], aggregate_shapes[i]);
      }
    }
    group.representative = std::make_unique<Row>(
        std::vector<Value>(result.schema.ColumnCount(), Value()));
    groups.push_back(std::move(group));
  }
  FusedGroupEmitter emitter{
      .raw_aggregates = raw_aggregates,
      .layout = layout,
      .context = context,
      .left = left,
      .right_arg = right_arg,
      .ctes = ctes,
      .outer = outer,
      .fusion = fusion,
      .deferred_left_probe = deferred_left_probe,
      .effective_left_rows = effective_left_rows,
      .right_col_owner = right_col_owner,
      .right_locator = right_locator,
      .right_ptr = right_ptr,
      .right = right,
      .join_begin = join_begin,
      .result = result,
      .equality_keys = equality_keys,
      .left_width = left_width,
      .right_width = right_width,
      .key_shapes = key_shapes,
      .kNullCell = kNullCell,
      .read_right_cell = read_right_cell,
      .def_overwritten_by_pk = def_overwritten_by_pk,
      .prepared_pk_lookups = prepared_pk_lookups,
      .def_pk_groups = def_pk_groups,
      .def_col_to_pk_lookup = def_col_to_pk_lookup,
      .read_pk_cell = read_pk_cell,
      .phase_buckets_done = phase_buckets_done,
      .phase_states_done = phase_states_done,
      .phase_probe_begin = phase_probe_begin,
      .phase_merge_begin = phase_merge_begin,
      .phase_merge_done = phase_merge_done,
      .phase_emit_begin = phase_emit_begin,
      .groups = groups,
      .states = states,
      .group_ptrs = group_ptrs,
      .join_type = join_type,
      .right_col_only = right_col_only,
      .has_double_raw_agg = has_double_raw_agg,
      .has_int_raw_agg = has_int_raw_agg,
      .has_stat_raw_agg = has_stat_raw_agg,
      .track_group_phases = track_group_phases,
      .use_group_ptrs = use_group_ptrs,
  };
  RETURN_IF_FAIL(emitter.Emit());
  if (!result.HasColumnarOnly() && !result.HasSpill() && !result.rows.empty() &&
      (left.HasColumnarOnly() || right_holder.has_value() ||
       right_col_owner != nullptr ||
       (deferred_left_probe != nullptr &&
        !deferred_left_probe->pk_lookups.empty()))) {
    if (context.execution_runtime() != nullptr) {
      if (left.HasColumnarOnly() && left.columnar != nullptr) {
        context.execution_runtime()->retained_scan_stores.push_back(
            left.columnar);
      }
      if (right_col_owner != nullptr) {
        context.execution_runtime()->retained_scan_stores.push_back(
            right_col_owner);
      } else if (right_holder.has_value() && right_arg.columnar != nullptr) {
        context.execution_runtime()->retained_scan_stores.push_back(
            right_arg.columnar);
      }
      if (deferred_left_probe != nullptr) {
        for (const DeferredLSTablePkLookup& lkp :
             deferred_left_probe->pk_lookups) {
          if (lkp.store != nullptr) {
            context.execution_runtime()->retained_scan_stores.push_back(
                lkp.store);
          }
        }
      }
    } else {
      for (Row& row : result.rows) {
        for (Value& cell : row.values_) {
          if (cell.type == ValueType::kVarChar && cell.owned_data.empty() &&
              !cell.value.varchar_value.empty()) {
            cell.owned_data.assign(cell.value.varchar_value);
            cell.value.varchar_value = cell.owned_data;
          }
        }
      }
    }
  }
  const size_t final_group_count = use_group_ptrs ? group_ptrs.size() : groups.size();
  if (!use_group_ptrs && workers > 1 && groups.size() >= 16384) {
    RETURN_IF_FAIL(RunParallelTasks(workers, [&](size_t w) {
      const size_t begin = (groups.size() * w) / workers;
      const size_t end = (groups.size() * (w + 1)) / workers;
      for (size_t gi = begin; gi < end; ++gi) {
        std::vector<Value>().swap(groups[gi].key.values_);
        groups[gi].representative.reset();
      }
    }));
  }
  std::vector<FusedGroup*>().swap(group_ptrs);
  std::vector<FusedGroup>().swap(groups);
  std::deque<AggregateAccumulator>().swap(states);
  int_merge_offsets = FlatIntKeyMap<size_t>();
  offsets.clear();
  if (locals.size() > 1) {
    RETURN_IF_FAIL(RunParallelTasks(locals.size(), [&](size_t w) {
      locals[w].raw_arena.blocks.clear();
      std::deque<AggregateAccumulator>().swap(locals[w].states);
      std::deque<Row>().swap(locals[w].owned_rep_left);
      std::vector<FusedGroup>().swap(locals[w].groups);
      locals[w].offsets = GroupKeyMap();
      locals[w].int_offsets = FlatIntKeyMap<size_t>();
      std::vector<CompactCountGroup>().swap(locals[w].compact_counts);
      std::vector<std::vector<uint64_t>>().swap(locals[w].compact_split_shards);
    }));
  }
  if (!radix_part_groups.empty() && workers > 1) {
    RETURN_IF_FAIL(RunParallelTasks(workers, [&](size_t w) {
      const size_t begin = (radix_part_groups.size() * w) / workers;
      const size_t end = (radix_part_groups.size() * (w + 1)) / workers;
      for (size_t p = begin; p < end; ++p) {
        std::vector<FusedGroup>().swap(radix_part_groups[p]);
        radix_part_arenas[p].blocks.clear();
      }
    }));
  }
  result.peak_intermediate_rows =
      std::max(result.peak_intermediate_rows, result.TotalRows());
  if (context.execution_runtime() != nullptr) {
    ExecutionRuntime& runtime = *context.execution_runtime();
    runtime.join_ms += ElapsedMs(join_begin);
    ++runtime.grouped_join_fusions;
  }
  if (track_group_phases) {
    std::cerr << "[fusion] emit_ms=" << ElapsedMs(phase_emit_begin)
              << " total_ms=" << ElapsedMs(join_begin)
              << " groups=" << final_group_count << "\n";
  }
  if (debug_fusion) {
    std::cerr << "[fusion] fused groups=" << final_group_count
              << " dense=" << use_dense_int_rows
              << " unique=" << use_unique_int_table
              << " radix=" << use_radix_agg
              << " def_cell=" << def_group_cell_probe
              << " dense_split=" << use_dense_split_direct_gid
              << " split_unpack=" << layout.split_key_unpack_only
              << " left_pack_unpack=" << layout.left_pack_unpack_only
              << " packed2x32_unpack=" << layout.packed_2x32_unpack_only
              << " ordered_sum=" << static_cast<bool>(ordered_sum_charge)
              << " shared_gid=" << use_shared_direct_gid
              << " workers=" << workers
              << " single_double=" << single_double_sum
              << " count_star=" << raw_aggregates.count_star_only
              << " preassigned=" << use_preassigned_build_groups
              << " right_keys=" << right_only_group_keys
              << " residual=" << residual_programs.size()
              << " numprobe=" << left_numeric_probe
              << " deferred=" << (deferred_left_probe != nullptr) << " pk="
              << (deferred_left_probe == nullptr
                      ? 0
                      : deferred_left_probe->pk_lookups.size())
              << " raw=" << raw_aggregates.raw_sum_fusion << " aggs=" << agg_count
              << " darith="
              << (!double_arithmetic.empty() && double_arithmetic[0])
              << " width=" << left_width << " keys=";
    for (size_t ki = 0; ki < key_programs.size() && ki < 10; ++ki) {
      std::cerr << key_shapes[ki].direct_column << ",";
    }
    std::cerr << " kinds=";
    for (size_t ai = 0; ai < agg_count && ai < 4; ++ai) {
      std::cerr << static_cast<int>(raw_aggregates.raw_agg_kinds[ai]) << ":"
                << child_shapes[ai].mul_col0 << "/" << child_shapes[ai].mul_col1
                << "/" << child_shapes[ai].direct_column << ",";
    }
    std::cerr << " pre_groups=" << pre_group_count
              << " ranked=" << ranked_int_rows.size() << " monotonic="
              << (deferred_left_probe != nullptr && !left_columns.empty() &&
                  left_columns[0] < deferred_left_probe->projection.size() &&
                  deferred_left_probe->projection[left_columns[0]] <
                      deferred_left_probe->store->column_is_monotonic.size() &&
                  deferred_left_probe->store->column_is_monotonic
                          [deferred_left_probe->projection[left_columns[0]]] !=
                      0)
              << " left=" << effective_left_rows << "\n";
  }
  return std::optional<Relation>(std::move(result));
}

}  // namespace tinylamb::relational_detail
