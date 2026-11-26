/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <iostream>
#include <iterator>
#include <limits>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/parallel_work.hpp"
#include "common/platform/process.hpp"
#include "common/query_parallelism.hpp"
#include "common/runtime_filter.hpp"
#include "common/status_or.hpp"
#include "common/string_util.hpp"
#include "database/transaction_context.hpp"
#include "executor/detail/build_input_internal.hpp"
#include "executor/detail/columnar_table.hpp"
#include "executor/detail/columnar_tlcs_fast_path.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/detail/flat_int_key_map.hpp"
#include "executor/detail/fused_grouped_join.hpp"
#include "executor/detail/planning_heuristics.hpp"
#include "executor/detail/relation.hpp"
#include "executor/detail/scan_filter.hpp"
#include "executor/detail/scan_tuple_filter.hpp"
#include "executor/detail/subquery_runtime.hpp"
#include "expression/binary_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/expression.hpp"
#include "expression/query_expression.hpp"
#include "expression/rewrite.hpp"
#include "expression/unary_expression.hpp"
#include "table/lstable_bridge.hpp"
#include "table/table.hpp"
#include "table/table_statistics.hpp"
#include "table/tlcs_file.hpp"
#include "type/column_name.hpp"
#include "type/schema.hpp"
#include "type/type.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb::relational_detail {

Schema ConcatRelationSchemas(const std::vector<Relation>& relations) {
  Schema combined;
  for (const Relation& relation : relations) {
    combined = combined.ColumnCount() == 0 ? relation.schema
                                           : combined + relation.schema;
  }
  return combined;
}

const ScanTupleFilter* InputTupleFilter(const SelectStatement& statement,
                                        const GroupedJoinFusionSpec* fusion,
                                        size_t source) {
  return fusion != nullptr && fusion->output_sink != nullptr &&
                 fusion->output_sink->input_filter &&
                 fusion->output_sink->input_filter->source ==
                     TableKeyFilterKey(statement.Sources()[source])
             ? fusion->output_sink->input_filter.get()
             : nullptr;
}

StatusOr<Relation> BuildOrderedJoinInput(
    TransactionContext& context, const SelectStatement& statement,
    const Scope* outer, const CteMap& ctes, bool* where_fully_applied,
    GroupedJoinFusionSpec* fusion, bool columnar_output,
    std::vector<Relation>& relations, const std::vector<bool>& base_sources,
    std::vector<std::vector<slot_t>>& projections) {
  // The ordered path keeps the syntactic join shape, but WHERE conjuncts
  // are still placed as early as they become referenceable, mirroring the
  // inner-join optimizer below: (a) single-source conjuncts filter the
  // owning scan, and (b) conjuncts whose last referenced table is a comma
  // (cross) source become that join's condition, turning what would be an
  // unbounded cross product into an equi/residual join.  Placement only
  // moves a conjunct to the first step where every source it references is
  // already joined -- never below the outer join that supplies one of its
  // columns -- and the full WHERE still runs above the joined output, so
  // outer-join semantics are unchanged.  Volatile or query-bearing
  // conjuncts are left for the top-level filter, since applying them twice
  // must not change how often they evaluate.
  // TINYLAMB_DISABLE_JOIN_REORDER=1 restores the strict syntactic visit
  // order (A/B hatch; results must be identical either way).
  static const bool disable_join_reorder =
      ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_JOIN_REORDER") != nullptr;
  const Schema ordered_all_schema = ConcatRelationSchemas(relations);
  std::vector<PredicateInfo> ordered_predicates =
      AnalyzePredicates(statement.WhereClause(), relations);
  const size_t where_predicate_count = ordered_predicates.size();
  // INNER ON predicates constrain the whole inner prefix, not a fixed
  // edge of its syntactic chain. Place them when their operands become
  // available so a dimension can join through a WHERE equality before
  // the fact table mentioned by its original ON. Later LEFT joins keep
  // this prefix preserved; RIGHT/FULL joins must retain the original ON.
  std::vector<bool> lifted_inner_on(relations.size(), false);
  size_t inner_end = 1;
  while (inner_end < statement.Sources().size()) {
    const auto& source = statement.Sources()[inner_end];
    if ((source.join_type != JoinType::kInner &&
         source.join_type != JoinType::kCross) ||
        source.unnest || source.is_lateral || source.from_nested_join ||
        !source.using_columns.empty()) {
      break;
    }
    ++inner_end;
  }
  if (!disable_join_reorder && inner_end > 2 &&
      !StatementContainsVolatileFunction(statement) &&
      std::ranges::all_of(statement.Sources(), [](const auto& source) {
        return source.join_type != JoinType::kRight &&
               source.join_type != JoinType::kFull && !source.unnest &&
               !source.is_lateral && !source.from_nested_join &&
               source.using_columns.empty();
      })) {
    std::vector<PredicateInfo> inner_predicates;
    bool eligible = true;
    for (size_t i = 1; i < inner_end; ++i) {
      auto predicates =
          AnalyzePredicates(statement.Sources()[i].join_condition, relations);
      for (auto& predicate : predicates) {
        if (!predicate.resolved || predicate.contains_query ||
            predicate.sources.empty() ||
            !SafeToReduceEvaluationCountWithSchema(predicate.expression,
                                                   ordered_all_schema) ||
            !std::ranges::all_of(predicate.sources, [&](size_t owner) {
              return owner < inner_end;
            })) {
          eligible = false;
        }
        inner_predicates.push_back(std::move(predicate));
      }
    }
    if (eligible) {
      for (size_t i = 1; i < inner_end; ++i) {
        lifted_inner_on[i] = true;
      }
      ordered_predicates.insert(
          ordered_predicates.end(),
          std::make_move_iterator(inner_predicates.begin()),
          std::make_move_iterator(inner_predicates.end()));
    }
  }
  // An INNER ON equality also constrains both preserved inputs. Keep the
  // original ON condition; the extra edge is only a necessary prefilter.
  for (const auto& source : statement.Sources()) {
    if (source.join_type != JoinType::kInner) {
      continue;
    }
    for (const auto& term : SplitConjuncts(source.join_condition)) {
      if (IsColumnEqualityPredicate(term) &&
          term->AsBinaryExpression().Op() == BinaryOperation::kEquals) {
        auto edges = AnalyzePredicates(term, relations);
        ordered_predicates.insert(ordered_predicates.end(), edges.begin(),
                                  edges.end());
      }
    }
  }
  // Disjunctions that imply the same equi-join pair on every branch gain
  // synthetic join edges so the greedy reorder and the hash keys see the
  // same connectivity the Cascades memo infers (TPC-DS q13); the original
  // disjunction is untouched and keeps re-verifying matched pairs.
  std::vector<PredicateInfo> disjunct_edges =
      SynthesizeDisjunctEqualityPredicates(ordered_predicates, relations);
  ordered_predicates.insert(ordered_predicates.end(),
                            std::make_move_iterator(disjunct_edges.begin()),
                            std::make_move_iterator(disjunct_edges.end()));
  static const bool debug_ordered =
      ::tinylamb::platform::GetEnv("TINYLAMB_DEBUG_ORDERED") != nullptr;
  if (debug_ordered) {
    for (size_t p = 0; p < ordered_predicates.size(); ++p) {
      const PredicateInfo& predicate = ordered_predicates[p];
      std::cerr << "[ordered] p" << p << " resolved=" << predicate.resolved
                << " query=" << predicate.contains_query << " sources=[";
      for (size_t s : predicate.sources) {
        std::cerr << s << ',';
      }
      std::cerr << "] expr=" << predicate.expression->ToString() << '\n';
    }
  }
  std::vector<bool> predicate_placed(ordered_predicates.size(), false);
  const auto placeable = [&](const PredicateInfo& predicate) {
    return predicate.resolved && !predicate.contains_query &&
           SafeToReduceEvaluationCountWithSchema(predicate.expression,
                                                 ordered_all_schema);
  };
  // Single-source conjuncts may only pre-filter a PRESERVED side.
  // Pre-filtering a null-supplying side converts some matched rows into
  // NULL-padded rows, which changes the outcome of WHERE terms that
  // distinguish padded rows -- `WHERE b.id IS NULL` over
  // `a LEFT JOIN b` (outer_to_anti) would otherwise resurrect the rows
  // whose match was filtered away.  A source is null-supplying when it is
  // the right side of a LEFT/FULL join; a RIGHT/FULL join additionally
  // makes every earlier source null-supplying.
  std::vector<char> filter_safe(relations.size(), 1);
  std::vector<size_t> last_null_extension(relations.size(), 0);
  for (size_t i = 1; i < statement.Sources().size(); ++i) {
    const JoinType type = statement.Sources()[i].join_type;
    if (type == JoinType::kLeft || type == JoinType::kFull) {
      filter_safe[i] = 0;
      last_null_extension[i] = i;
    }
    if (type == JoinType::kRight || type == JoinType::kFull) {
      for (size_t j = 0; j < i; ++j) {
        filter_safe[j] = 0;
        last_null_extension[j] = i;
      }
    }
  }
  const auto all_where_placed = [&] {
    for (size_t p = 0; p < where_predicate_count; ++p) {
      if (!predicate_placed[p]) {
        return false;
      }
    }
    return true;
  };
  std::vector<std::vector<Expression>> ordered_local(relations.size());
  for (size_t p = 0; p < ordered_predicates.size(); ++p) {
    const PredicateInfo& predicate = ordered_predicates[p];
    if (!placeable(predicate) || predicate.sources.size() != 1) {
      continue;
    }
    const size_t owner = *predicate.sources.begin();
    if (!static_cast<bool>(filter_safe[owner])) {
      continue;
    }
    ordered_local[owner].push_back(predicate.expression);
    predicate_placed[p] = true;
  }
  // An ON term local to the null-supplying side of LEFT JOIN can reject
  // build rows before materialization. Keep the original ON term as the
  // semantic backstop; never use a WHERE term here or cross a FULL join.
  for (size_t i = 1; i < relations.size(); ++i) {
    if (statement.Sources()[i].join_type != JoinType::kLeft) {
      continue;
    }
    for (const Expression& predicate :
         SplitConjuncts(statement.Sources()[i].join_condition)) {
      if (IsNonThrowingScanFilter(predicate, relations[i].schema)) {
        ordered_local[i].push_back(predicate);
      }
    }
  }
  if (debug_ordered) {
    for (size_t i = 0; i < ordered_local.size(); ++i) {
      if (!ordered_local[i].empty()) {
        std::cerr << "[ordered] local[" << i
                  << "] count=" << ordered_local[i].size()
                  << " base=" << base_sources[i] << '\n';
      }
    }
  }
  // Materialize selective preserved dimensions before the fact scan, even
  // when an outer join fixes the join order. A strict WHERE equality to
  // another preserved source makes its key domain a necessary condition;
  // no predicate is pushed through a null-supplying boundary.
  std::vector<bool> loaded(relations.size(), false);
  static const bool relative_filters_disabled =
      ::tinylamb::platform::GetEnv(
          "TINYLAMB_DISABLE_RELATIVE_ORDERED_FILTERS") != nullptr;
  std::vector<size_t> ordered_rows(relations.size());
  size_t largest_scan = 0;
  for (size_t i = 0; i < relations.size(); ++i) {
    if (base_sources[i]) {
      const auto stats = context.GetStats(statement.Sources()[i].table);
      if (stats.HasValue()) {
        ordered_rows[i] = stats.Value()->Rows();
        largest_scan = std::max(largest_scan, ordered_rows[i]);
      }
    }
  }
  std::vector<std::shared_ptr<QueryMemoryCharge>> ordered_filter_charges;
  const bool allow_ord_col =
      (columnar_output || (fusion != nullptr && fusion->eligible)) &&
      std::ranges::none_of(statement.Sources(), [](const SelectSource& src) {
        return !src.using_columns.empty();
      });
  const auto ordered_binding = [&](size_t i) {
    std::optional<ScanRuntimeFilter> binding;
    std::set<std::array<size_t, 3>> bound_edges;
    const auto try_attach_edge = [&](size_t other, slot_t left_col,
                                     slot_t right_col,
                                     size_t max_other_rows) -> bool {
      const size_t other_rows = relations[other].TotalRows();
      if (other == i || !loaded[other] || relations[other].HasSpill() ||
          other_rows == 0 || other_rows > max_other_rows ||
          (binding && binding->extra.size() >= 7)) {
        return false;
      }
      const auto& left = relations[i].schema.GetColumn(left_col);
      const auto& right = relations[other].schema.GetColumn(right_col);
      if (left.Type() != right.Type() || left.IsUnsigned() ||
          right.IsUnsigned() ||
          (left.Type() != ValueType::kInt64 &&
           left.Type() != ValueType::kDate)) {
        return false;
      }
      const auto projected = std::ranges::find(projections[i], left_col);
      if (projected == projections[i].end()) {
        return false;
      }
      if (!relative_filters_disabled &&
          !bound_edges.insert({other, left_col, right_col}).second) {
        return false;
      }
      std::shared_ptr<QueryMemoryCharge> filter_charge;
      if (other_rows > 4096) {
        const size_t tracked =
            std::min(other_rows, RuntimeFilterBuilder::kMaxTrackedKeys);
        // Retain the reservation through this BuildInput. It covers
        // the temporary keys and the largest persistent filter shape;
        // a failed reservation only declines this optional reduction.
        filter_charge = TryReserveQueryMemory(
            (tracked * 64) + (RuntimeFilterGroup::kMaxDenseSpan / 8) + 8);
        if (!filter_charge) {
          return false;
        }
      }
      RuntimeFilterBuilder builder;
      builder.Reserve(other_rows);
      if (relations[other].HasColumnarOnly()) {
        for (const ColumnarChunk& chunk : relations[other].columnar->chunks) {
          if (chunk.rows == 0) {
            continue;
          }
          const ColumnarColumn& cc = chunk.columns[right_col];
          if (cc.is_uniform && !cc.is_unsigned &&
              (cc.type == ValueType::kInt64 || cc.type == ValueType::kDate)) {
            for (size_t r = 0; r < chunk.rows; ++r) {
              if (!cc.IsNull(r)) {
                builder.Observe(cc.IntAt(r));
              }
            }
          } else {
            Value val;
            for (size_t r = 0; r < chunk.rows; ++r) {
              cc.WriteValueInto(r, &val);
              if (!val.IsNull()) {
                builder.Observe(val.value.int_value);
              }
            }
          }
        }
      } else {
        for (const auto& row : relations[other].rows) {
          const auto& key = row[right_col];
          if (!key.IsNull()) {
            builder.Observe(key.value.int_value);
          }
        }
      }
      auto group = builder.Build(false);
      if (!group || group->empty()) {
        return false;
      }
      if (filter_charge) {
        ordered_filter_charges.push_back(std::move(filter_charge));
      }
      ScanRuntimeFilter filter{.group = std::move(group),
                               .column = static_cast<slot_t>(std::distance(
                                   projections[i].begin(), projected)),
                               .type = left.Type()};
      if (binding) {
        binding->extra.push_back(std::move(filter));
      } else {
        binding = std::move(filter);
      }
      return true;
    };
    if (outer == nullptr && filter_safe[i] != 0 &&
        !statement.Sources()[i].sample) {
      struct OrderedFilterCandidate {
        size_t other{0};
        slot_t left_col{0};
        slot_t right_col{0};
        size_t rows{0};
      };
      std::vector<OrderedFilterCandidate> candidates;
      const size_t max_other_rows = std::max<size_t>(
          4096, relative_filters_disabled ? 0 : ordered_rows[i] / 2);
      for (const auto& predicate : ordered_predicates) {
        if (!predicate.resolved || predicate.contains_query ||
            predicate.sources.size() != 2 || !predicate.sources.contains(i) ||
            !IsColumnEqualityPredicate(predicate.expression) ||
            predicate.expression->AsBinaryExpression().Op() !=
                BinaryOperation::kEquals) {
          continue;
        }
        for (const size_t other : predicate.sources) {
          if (other == i || !loaded[other] || filter_safe[other] == 0) {
            continue;
          }
          const auto keys =
              EqualityKeys(relations[i].schema, relations[other].schema,
                           {predicate.expression});
          if (keys.size() != 1) {
            continue;
          }
          const size_t other_rows = relations[other].TotalRows();
          if (!relations[other].HasSpill() && other_rows > 0 &&
              other_rows <= max_other_rows) {
            candidates.push_back(OrderedFilterCandidate{
                .other = other,
                .left_col = static_cast<slot_t>(keys[0].left),
                .right_col = static_cast<slot_t>(keys[0].right),
                .rows = other_rows,
            });
          }
        }
      }
      std::ranges::stable_sort(candidates, [](const OrderedFilterCandidate& a,
                                              const OrderedFilterCandidate& b) {
        return a.rows < b.rows;
      });
      for (const OrderedFilterCandidate& cand : candidates) {
        try_attach_edge(cand.other, cand.left_col, cand.right_col,
                        max_other_rows);
      }
    } else if (outer == nullptr &&
               statement.Sources()[i].join_type == JoinType::kLeft &&
               last_null_extension[i] == i && !statement.Sources()[i].sample &&
               statement.Sources()[i].using_columns.empty() &&
               statement.Sources()[i].join_condition != nullptr) {
      // Null-supplying LEFT JOIN semijoin reduction: an equijoin in the ON
      // clause only matches rows whose key exists on the preserved side (and
      // transitively any preserved filter_safe partner equated to that key).
      struct LeftFilterCandidate {
        size_t src{0};
        slot_t left_col{0};
        slot_t src_col{0};
        size_t rows{0};
      };
      std::vector<LeftFilterCandidate> candidates;
      const size_t max_other_rows = std::max<size_t>(
          4096, relative_filters_disabled ? 0 : (ordered_rows[i] * 3) / 4);
      for (const Expression& conjunct :
           SplitConjuncts(statement.Sources()[i].join_condition)) {
        if (!conjunct || !IsColumnEqualityPredicate(conjunct) ||
            conjunct->AsBinaryExpression().Op() != BinaryOperation::kEquals) {
          continue;
        }
        const std::vector<PredicateInfo> analyzed =
            AnalyzePredicates(conjunct, relations);
        if (analyzed.size() != 1 || !analyzed[0].resolved ||
            analyzed[0].contains_query || analyzed[0].sources.size() != 2 ||
            !analyzed[0].sources.contains(i)) {
          continue;
        }
        size_t other = *analyzed[0].sources.begin();
        if (other == i) {
          other = *std::next(analyzed[0].sources.begin());
        }
        if (other >= i ||
            (filter_safe[other] == 0 && last_null_extension[other] != other)) {
          continue;
        }
        const auto keys = EqualityKeys(relations[i].schema,
                                       relations[other].schema, {conjunct});
        if (keys.size() != 1 || keys[0].null_safe || keys[0].left_offset != 0 ||
            keys[0].right_offset != 0) {
          continue;
        }
        const size_t other_rows = relations[other].TotalRows();
        if (loaded[other] && !relations[other].HasSpill() && other_rows > 0 &&
            other_rows <= max_other_rows) {
          candidates.push_back(
              LeftFilterCandidate{.src = other,
                                  .left_col = static_cast<slot_t>(keys[0].left),
                                  .src_col = static_cast<slot_t>(keys[0].right),
                                  .rows = other_rows});
        }
        if (filter_safe[other] != 0) {
          std::vector<std::pair<size_t, slot_t>> queue{
              {other, static_cast<slot_t>(keys[0].right)}};
          std::set<std::pair<size_t, slot_t>> visited{
              {other, static_cast<slot_t>(keys[0].right)}};
          for (size_t q = 0; q < queue.size(); ++q) {
            const auto [cur_rel, cur_col] = queue[q];
            for (const auto& pred : ordered_predicates) {
              if (!pred.resolved || pred.contains_query ||
                  pred.sources.size() != 2 || !pred.sources.contains(cur_rel) ||
                  !IsColumnEqualityPredicate(pred.expression) ||
                  pred.expression->AsBinaryExpression().Op() !=
                      BinaryOperation::kEquals) {
                continue;
              }
              size_t partner = *pred.sources.begin();
              if (partner == cur_rel) {
                partner = *std::next(pred.sources.begin());
              }
              if (partner == i || filter_safe[partner] == 0) {
                continue;
              }
              const auto pkeys =
                  EqualityKeys(relations[cur_rel].schema,
                               relations[partner].schema, {pred.expression});
              if (pkeys.size() != 1 || pkeys[0].null_safe ||
                  pkeys[0].left_offset != 0 || pkeys[0].right_offset != 0 ||
                  pkeys[0].left != cur_col) {
                continue;
              }
              const auto partner_col = static_cast<slot_t>(pkeys[0].right);
              if (!visited.insert({partner, partner_col}).second) {
                continue;
              }
              queue.emplace_back(partner, partner_col);
              const size_t partner_rows = relations[partner].TotalRows();
              if (loaded[partner] && !relations[partner].HasSpill() &&
                  partner_rows > 0 && partner_rows <= max_other_rows) {
                candidates.push_back(LeftFilterCandidate{
                    .src = partner,
                    .left_col = static_cast<slot_t>(keys[0].left),
                    .src_col = partner_col,
                    .rows = partner_rows});
              }
            }
          }
        }
      }
      std::ranges::stable_sort(candidates, [](const LeftFilterCandidate& a,
                                              const LeftFilterCandidate& b) {
        return a.rows < b.rows;
      });
      std::unordered_set<slot_t> bound_left_cols;
      bound_left_cols.reserve(candidates.size());
      for (const LeftFilterCandidate& cand : candidates) {
        if (bound_left_cols.contains(cand.left_col)) {
          continue;
        }
        if (try_attach_edge(cand.src, cand.left_col, cand.src_col,
                            max_other_rows)) {
          bound_left_cols.insert(cand.left_col);
        }
      }
    }
    return binding;
  };
  // Carry a selective key domain through preserved dimension chains before
  // loading large fact tables. This is a semijoin reduction only: retain all
  // original joins and predicates, including duplicates and null extension.
  // A dimension without a local predicate is useful when an already loaded
  // dimension constrains it through a strict equality (for example, two
  // dates belonging to the same week).
  while (outer == nullptr) {
    size_t next = relations.size();
    size_t next_rows = SIZE_MAX;
    std::optional<ScanRuntimeFilter> next_binding;
    for (size_t i = 0; i < relations.size(); ++i) {
      if (loaded[i] || !base_sources[i] || filter_safe[i] == 0 ||
          statement.Sources()[i].sample ||
          !std::ranges::all_of(ordered_local[i], [&](const auto& predicate) {
            return IsNonThrowingScanFilter(predicate, relations[i].schema);
          })) {
        continue;
      }
      const auto stats = context.GetStats(statement.Sources()[i].table);
      if (!stats.HasValue() || stats.Value()->Rows() == 0 ||
          (stats.Value()->Rows() > 100000 &&
           (relative_filters_disabled || ordered_local[i].empty() ||
            stats.Value()->Rows() > 500000 ||
            stats.Value()->Rows() > largest_scan / 4)) ||
          stats.Value()->Rows() >= next_rows) {
        continue;
      }
      auto binding = ordered_binding(i);
      if (!binding && ordered_local[i].empty()) {
        continue;
      }
      next = i;
      next_rows = stats.Value()->Rows();
      next_binding = std::move(binding);
    }
    if (next == relations.size()) {
      break;
    }
    const auto* next_tf = InputTupleFilter(statement, fusion, next);
    ASSIGN_OR_RETURN(
        Relation, dimension,
        (LoadSource(
            context, statement.Sources()[next], nullptr, ctes,
            &projections[next],
            ordered_local[next].empty() ? nullptr : &ordered_local[next],
            next_binding ? &*next_binding : nullptr, next_tf,
            allow_ord_col && next_tf == nullptr)));
    relations[next] = std::move(dimension);
    loaded[next] = true;
  }
  // Source 0 stays loaded: it is the initial probe. A later numeric fact is
  // left unloaded and probed from its columnar store, but ordering still
  // sees the table statistics so an unloaded fact is not treated as empty.
  std::vector<size_t> deferred_order_rows(relations.size(), 0);
  for (size_t i = 0; i < relations.size(); ++i) {
    if (loaded[i]) {
      continue;
    }
    if (base_sources[i]) {
      // A TABLESAMPLE source must go through LoadSource -> ApplyTableSample;
      // the deferred stream-probe reads the columnar store directly and
      // would silently scan the whole table, inflating counts and aggregates.
      const auto stats = context.GetStats(statement.Sources()[i].table);
      if (i != 0 && !statement.Sources()[i].sample && stats.HasValue() &&
          stats.Value()->Rows() >= 1000000 && !projections[i].empty() &&
          projections[i].size() <= 8) {
        bool numeric = true;
        for (const slot_t column : projections[i]) {
          if (column >= relations[i].schema.ColumnCount()) {
            numeric = false;
            break;
          }
          const Column& col = relations[i].schema.GetColumn(column);
          const auto type = col.Type();
          if (col.IsUnsigned() ||
              (type != ValueType::kInt64 && type != ValueType::kDate &&
               type != ValueType::kDouble)) {
            numeric = false;
            break;
          }
        }
        if (numeric) {
          deferred_order_rows[i] = stats.Value()->Rows();
          continue;
        }
      }
      auto binding = ordered_binding(i);
      const auto* tf = InputTupleFilter(statement, fusion, i);
      ASSIGN_OR_RETURN(
          Relation, loaded_rel,
          (LoadSource(context, statement.Sources()[i], outer, ctes,
                      &projections[i],
                      ordered_local[i].empty() ? nullptr : &ordered_local[i],
                      binding ? &*binding : nullptr, tf,
                      allow_ord_col && tf == nullptr)));
      relations[i] = std::move(loaded_rel);
      loaded[i] = true;
    } else if (!ordered_local[i].empty()) {
      RETURN_IF_FAIL(FilterRelation(context, &relations[i], ordered_local[i],
                                    outer, ctes));
    }
  }
  if (outer == nullptr) {
    ReduceUnmatchedOuterProbes(context, statement, relations,
                               ordered_predicates, ctes);
  }
  // Maximal runs of inner/cross sources reorder greedily by connectivity:
  // inner joins are associative and commutative, so visiting them in the
  // syntactic chain order is not required.  Outer/USING/nested/lateral
  // sources keep their syntactic position (their null-extension and merge
  // semantics are order dependent) and freeze the run boundaries.  The
  // mirror heuristic drives the choice: prefer candidates that connect to
  // the already-joined prefix, smallest estimated intermediate size first,
  // so dimension chains join before the fact-to-fact joins that need
  // their filters (TPC-DS q72 joins the 2002 date slice before inventory).
  std::vector<size_t> visit_order;
  visit_order.reserve(relations.size());
  {
    const auto reorderable = [&](size_t i) {
      const SelectSource& s = statement.Sources()[i];
      return (s.join_type == JoinType::kInner ||
              s.join_type == JoinType::kCross) &&
             !s.from_nested_join && !s.unnest && s.using_columns.empty();
    };
    // Connective conjuncts: the ON conditions of run members plus every
    // unplaced multi-source WHERE conjunct, analyzed per source set.
    struct ConnectiveConjunct {
      std::unordered_set<size_t> sources;
    };
    std::vector<ConnectiveConjunct> connectives;
    const auto collect = [&](const Expression& conjunct) {
      if (!conjunct) {
        return;
      }
      const std::vector<PredicateInfo> analyzed =
          AnalyzePredicates(conjunct, relations);
      if (analyzed.size() == 1 && analyzed[0].resolved) {
        connectives.push_back({analyzed[0].sources});
      }
    };
    for (size_t i = 1; i < relations.size(); ++i) {
      if (reorderable(i)) {
        for (const Expression& conjunct :
             SplitConjuncts(statement.Sources()[i].join_condition)) {
          collect(conjunct);
        }
      }
    }
    for (size_t p = 0; p < ordered_predicates.size(); ++p) {
      const PredicateInfo& predicate = ordered_predicates[p];
      if (!predicate_placed[p] && placeable(predicate) &&
          predicate.sources.size() >= 2) {
        connectives.push_back({predicate.sources});
      }
    }
    const auto connected_to = [&](size_t candidate,
                                  const std::unordered_set<size_t>& joined) {
      // The candidate is not part of `joined` yet: a conjunct connects it
      // when every source it references is either already joined or the
      // candidate itself, and at least one referenced source is new.
      std::unordered_set<size_t> with_candidate = joined;
      with_candidate.insert(candidate);
      const auto connects_candidate = [&](const ConnectiveConjunct& conjunct) {
        return conjunct.sources.contains(candidate) &&
               conjunct.sources.size() >= 2 &&
               IsSubset(conjunct.sources, with_candidate) &&
               !IsSubset(conjunct.sources, joined);
      };
      return std::ranges::any_of(connectives, connects_candidate);
    };
    // A source may only join when its own ON condition binds against the
    // prefix plus itself: joining it earlier would leave the ON
    // referencing a relation that is not an input of the join.  ON
    // conjuncts that never resolved (outer-scope qualifiers, subqueries)
    // keep their historic residual handling and do not block.
    std::vector<std::unordered_set<size_t>> on_sources(relations.size());
    for (size_t i = 1; i < relations.size(); ++i) {
      if (lifted_inner_on[i]) {
        continue;
      }
      for (const Expression& conjunct :
           SplitConjuncts(statement.Sources()[i].join_condition)) {
        if (!conjunct) {
          continue;
        }
        const std::vector<PredicateInfo> analyzed =
            AnalyzePredicates(conjunct, relations);
        if (analyzed.size() == 1 && analyzed[0].resolved) {
          on_sources[i].insert(analyzed[0].sources.begin(),
                               analyzed[0].sources.end());
        }
      }
    }
    const auto on_bindable = [&](size_t candidate,
                                 const std::unordered_set<size_t>& joined) {
      std::unordered_set<size_t> with_candidate = joined;
      with_candidate.insert(candidate);
      return IsSubset(on_sources[candidate], with_candidate);
    };
    struct OrderedEqEdge {
      size_t a{0};
      size_t b{0};
      slot_t col_a{0};
      slot_t col_b{0};
    };
    std::vector<OrderedEqEdge> eq_edges;
    auto collect_eq_edge = [&](const Expression& conjunct) {
      if (!conjunct || !IsColumnEqualityPredicate(conjunct) ||
          conjunct->AsBinaryExpression().Op() != BinaryOperation::kEquals) {
        return;
      }
      const std::vector<PredicateInfo> analyzed =
          AnalyzePredicates(conjunct, relations);
      if (analyzed.size() != 1 || !analyzed[0].resolved ||
          analyzed[0].sources.size() != 2) {
        return;
      }
      auto it = analyzed[0].sources.begin();
      const size_t s0 = *it++;
      const size_t s1 = *it;
      const auto keys =
          EqualityKeys(relations[s0].schema, relations[s1].schema, {conjunct});
      if (keys.size() == 1 && !keys[0].null_safe && keys[0].left_offset == 0 &&
          keys[0].right_offset == 0) {
        eq_edges.push_back(
            OrderedEqEdge{.a = s0,
                          .b = s1,
                          .col_a = static_cast<slot_t>(keys[0].left),
                          .col_b = static_cast<slot_t>(keys[0].right)});
      }
    };
    for (size_t i = 1; i < relations.size(); ++i) {
      if (reorderable(i)) {
        for (const Expression& conjunct :
             SplitConjuncts(statement.Sources()[i].join_condition)) {
          collect_eq_edge(conjunct);
        }
      }
    }
    for (size_t p = 0; p < ordered_predicates.size(); ++p) {
      const PredicateInfo& predicate = ordered_predicates[p];
      if (!predicate_placed[p] && placeable(predicate) &&
          predicate.sources.size() == 2) {
        collect_eq_edge(predicate.expression);
      }
    }
    auto is_expansive_candidate =
        [&](size_t cand, const std::unordered_set<size_t>& joined) -> bool {
      if (!loaded[cand] || deferred_order_rows[cand] != 0 ||
          relations[cand].HasSpill()) {
        return false;
      }
      const size_t total = relations[cand].TotalRows();
      if (total <= 1 || total > 65536) {
        return false;
      }
      std::vector<slot_t> cand_cols;
      cand_cols.reserve(eq_edges.size());
      for (const OrderedEqEdge& edge : eq_edges) {
        if (edge.a == cand && joined.contains(edge.b)) {
          if (std::ranges::find(cand_cols, edge.col_a) == cand_cols.end()) {
            cand_cols.push_back(edge.col_a);
          }
        } else if (edge.b == cand && joined.contains(edge.a)) {
          if (std::ranges::find(cand_cols, edge.col_b) == cand_cols.end()) {
            cand_cols.push_back(edge.col_b);
          }
        }
      }
      if (cand_cols.size() != 1) {
        return false;
      }
      const slot_t kc = cand_cols[0];
      const Column& col_meta = relations[cand].schema.GetColumn(kc);
      if (col_meta.GetConstraint().IsUnique()) {
        return false;
      }
      if (col_meta.IsUnsigned() || (col_meta.Type() != ValueType::kInt64 &&
                                    col_meta.Type() != ValueType::kDate)) {
        return false;
      }
      constexpr size_t kMaxSample = 4096;
      std::vector<int64_t> vals;
      vals.reserve(std::min(total, kMaxSample));
      if (relations[cand].HasColumnarOnly()) {
        for (const ColumnarChunk& ch : relations[cand].columnar->chunks) {
          const ColumnarColumn& cc = ch.columns[kc];
          for (size_t r = 0; r < ch.rows && vals.size() < kMaxSample; ++r) {
            int64_t v = 0;
            if (cc.TryReadNonNullInt(r, &v)) {
              vals.push_back(v);
            }
          }
          if (vals.size() >= kMaxSample) {
            break;
          }
        }
      } else {
        const size_t limit = std::min(relations[cand].rows.size(), kMaxSample);
        for (size_t r = 0; r < limit; ++r) {
          const Value& v = relations[cand].rows[r][kc];
          if (!v.IsNull()) {
            vals.push_back(v.value.int_value);
          }
        }
      }
      if (vals.size() <= 1) {
        return false;
      }
      const size_t non_null_rows = vals.size();
      std::ranges::sort(vals);
      const auto unique_end = std::ranges::unique(vals).begin();
      const size_t distinct = static_cast<size_t>(unique_end - vals.begin());
      return distinct > 0 && non_null_rows > distinct + (distinct / 2);
    };

    size_t run_begin = 1;
    while (run_begin < relations.size()) {
      if (disable_join_reorder) {
        visit_order.push_back(run_begin);
        ++run_begin;
        continue;
      }
      if (!reorderable(run_begin)) {
        visit_order.push_back(run_begin);
        ++run_begin;
        continue;
      }
      size_t run_end = run_begin;
      while (run_end < relations.size() && reorderable(run_end)) {
        ++run_end;
      }
      // Start the chain from the prefix-connected member with the fewest
      // rows (the min() estimate cannot see hash fanout, so simulating
      // every start ties on peak; the smallest filtered member is the
      // robust anchor).  Then greedily append connected candidates.
      std::unordered_set<size_t> prefix;
      prefix.reserve(run_begin);
      for (size_t i = 0; i < run_begin; ++i) {
        prefix.insert(i);
      }
      size_t start = run_begin;
      bool have_start = false;
      bool start_connected = false;
      for (size_t candidate = run_begin; candidate < run_end; ++candidate) {
        if (!on_bindable(candidate, prefix)) {
          continue;
        }
        const bool connected = connected_to(candidate, prefix);
        const size_t rows = deferred_order_rows[candidate] != 0
                                ? deferred_order_rows[candidate]
                                : relations[candidate].TotalRows();
        const size_t start_rows = deferred_order_rows[start] != 0
                                      ? deferred_order_rows[start]
                                      : relations[start].TotalRows();
        if (!have_start || (connected && !start_connected) ||
            (connected == start_connected && rows < start_rows)) {
          have_start = true;
          start = candidate;
          start_connected = connected;
        }
      }
      if (!have_start) {
        // No member binds against the prefix (a disconnected cross
        // fragment): fall back to the syntactic order for this run.
        for (size_t idx = run_begin; idx < run_end; ++idx) {
          visit_order.push_back(idx);
        }
        run_begin = run_end;
        continue;
      }
      std::vector<size_t> order{start};
      order.reserve(run_end - run_begin);
      std::unordered_set<size_t> sim_joined = prefix;
      sim_joined.reserve(relations.size());
      sim_joined.insert(start);
      std::vector<size_t> remaining;
      remaining.reserve(run_end - run_begin);
      for (size_t i = run_begin; i < run_end; ++i) {
        if (i != start) {
          remaining.push_back(i);
        }
      }
      while (!remaining.empty()) {
        bool any_bindable = false;
        for (size_t candidate : remaining) {
          any_bindable = any_bindable || on_bindable(candidate, sim_joined);
        }
        if (!any_bindable) {
          // The remaining fragment's ON conditions reference only its own
          // members (a disconnected inner fragment).  Reordering cannot
          // help; preserve the syntactic order, which is the historic
          // shape for this case.
          for (const size_t idx : remaining) {
            order.push_back(idx);
          }
          break;
        }
        size_t next = remaining.front();
        bool next_bindable = on_bindable(next, sim_joined);
        bool next_connected = next_bindable && connected_to(next, sim_joined);
        bool next_expands =
            next_connected && is_expansive_candidate(next, sim_joined);
        for (size_t candidate : remaining) {
          const bool cand_bindable = on_bindable(candidate, sim_joined);
          const bool cand_connected =
              cand_bindable && connected_to(candidate, sim_joined);
          const bool cand_expands =
              cand_connected && is_expansive_candidate(candidate, sim_joined);
          const size_t raw_cand_rows = deferred_order_rows[candidate] != 0
                                           ? deferred_order_rows[candidate]
                                           : relations[candidate].TotalRows();
          const size_t raw_next_rows = deferred_order_rows[next] != 0
                                           ? deferred_order_rows[next]
                                           : relations[next].TotalRows();
          const size_t cand_rows =
              cand_expands ? (1000000ULL + raw_cand_rows) : raw_cand_rows;
          const size_t next_rows =
              next_expands ? (1000000ULL + raw_next_rows) : raw_next_rows;
          const bool cheaper = cand_rows < next_rows;
          if ((cand_connected && !next_connected) ||
              (cand_connected == next_connected && cand_bindable &&
               !next_bindable) ||
              (cand_connected == next_connected &&
               cand_bindable == next_bindable && cheaper)) {
            next = candidate;
            next_connected = cand_connected;
            next_bindable = cand_bindable;
          }
        }
        order.push_back(next);
        sim_joined.insert(next);
        std::erase(remaining, next);
      }
      if (debug_ordered) {
        std::cerr << "[ordered] run [" << run_begin << ',' << run_end
                  << ") start=" << start << " order=";
        for (const size_t idx : order) {
          const size_t r = deferred_order_rows[idx] != 0
                               ? deferred_order_rows[idx]
                               : relations[idx].TotalRows();
          std::cerr << idx << "(r=" << r << ",b=" << on_sources[idx].size()
                    << "),";
        }
        std::cerr << '\n';
      }
      for (const size_t idx : order) {
        visit_order.push_back(idx);
      }
      run_begin = run_end;
    }
  }
  Relation result = std::move(relations.front());
  std::unordered_set<size_t> available{0};
  static const bool prune_columns_disabled =
      ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_ORDERED_COLUMN_PRUNE") !=
      nullptr;
  const bool can_prune_columns =
      !prune_columns_disabled && !statement.HasDistinctOn() &&
      !statement.ForUpdate() &&
      std::ranges::all_of(statement.Sources(), [](const auto& source) {
        return source.using_columns.empty() && !source.is_lateral &&
               !source.unnest && !source.from_nested_join;
      });
  const auto prune_consumed_columns = [&]() -> Status {
    if (!can_prune_columns || available.size() == relations.size() ||
        result.HasSpill() || result.using_columns) {
      return Status::kSuccess;
    }
    // A final fused join/aggregate reads cells in place and emits groups;
    // it does not construct the wide joined rows this pass would avoid.
    if (fusion != nullptr && fusion->eligible &&
        available.size() + 1 == relations.size()) {
      return Status::kSuccess;
    }
    SelectStatement future(statement);
    std::vector<SelectSource> remaining_sources;
    remaining_sources.reserve(statement.Sources().size());
    for (size_t r = 0; r < statement.Sources().size(); ++r) {
      if (!available.contains(r)) {
        auto source = statement.Sources()[r];
        if (lifted_inner_on[r]) {
          source.join_condition.reset();
        }
        remaining_sources.push_back(std::move(source));
      }
    }
    future.SetSources(std::move(remaining_sources));
    std::vector<Expression> remaining_predicates;
    remaining_predicates.reserve(ordered_predicates.size() + 1);
    // If any WHERE term stays above BuildInput, its caller re-evaluates
    // the original whole WHERE. Keep that complete dependency set until
    // every term has been placed, including query-bearing predicates.
    if (!all_where_placed() && statement.WhereClause()) {
      remaining_predicates.push_back(statement.WhereClause());
    }
    for (size_t p = 0; p < ordered_predicates.size(); ++p) {
      if (!predicate_placed[p]) {
        remaining_predicates.push_back(ordered_predicates[p].expression);
      }
    }
    future.SetWhereClause(CombineConjuncts(remaining_predicates));
    const auto columns = RequiredColumns(future, result.schema);
    if (columns.size() == result.schema.ColumnCount()) {
      return Status::kSuccess;
    }
    if (result.HasColumnarOnly()) {
      bool all_uniform = true;
      result.columnar =
          ProjectColumnarTableBySlots(*result.columnar, columns, &all_uniform);
      result.uniform_cell_types = all_uniform;
      result.schema = ProjectSchema(result.schema, columns);
      return Status::kSuccess;
    }
    const size_t workers = ParallelWorkerCount(result.rows.size() / 8192);
    RETURN_IF_FAIL(RunParallelTasks(workers, [&](size_t w) {
      const size_t begin = result.rows.size() * w / workers;
      const size_t end = result.rows.size() * (w + 1) / workers;
      for (size_t r = begin; r < end; ++r) {
        auto& values = result.rows[r].values_;
        // RequiredColumns is ascending, so forward compaction never
        // overwrites an unread cell. Retain the buffer for the next join.
        for (size_t c = 0; c < columns.size(); ++c) {
          if (c != columns[c]) {
            values[c] = std::move(values[columns[c]]);
          }
        }
        values.resize(columns.size());
      }
    }));
    // Keeping the old reservation is conservative until this Relation is
    // consumed; no uncharged allocation or change to row order is needed.
    result.schema = ProjectSchema(result.schema, columns);
    return Status::kSuccess;
  };
  const auto try_stream_probe =
      [&](size_t index,
          const SelectSource& probed) -> StatusOr<std::optional<Relation>> {
    if (probed.join_type != JoinType::kInner || probed.sample ||
        !probed.using_columns.empty() || result.HasSpill() ||
        result.TotalRows() == 0 || result.using_columns ||
        projections[index].empty() ||
        deferred_order_rows[index] < result.TotalRows() * 4) {
      return std::optional<Relation>{};
    }
    const Schema projected =
        ProjectSchema(relations[index].schema, projections[index]);
    const Schema probe_pair_schema = result.schema + projected;
    const auto predicates = SplitConjuncts(probed.join_condition);
    if (predicates.empty() ||
        !std::ranges::all_of(predicates, [&](const Expression& pred) {
          return SafeToReduceEvaluationCountWithSchema(pred, probe_pair_schema);
        })) {
      return std::optional<Relation>{};
    }
    const auto keys = EqualityKeys(result.schema, projected, predicates);
    if (keys.empty() || keys.size() > CompositeIntKey::kMaxParts ||
        std::ranges::any_of(keys, [](const EqualityKey& key) {
          return key.null_safe || key.left_offset != 0 || key.right_offset != 0;
        })) {
      return std::optional<Relation>{};
    }
    std::vector<slot_t> left_columns;
    left_columns.reserve(keys.size());
    std::vector<slot_t> right_store;
    right_store.reserve(keys.size());
    for (const EqualityKey& key : keys) {
      const Column& left_col = result.schema.GetColumn(key.left);
      const Column& right_col = projected.GetColumn(key.right);
      if (left_col.IsUnsigned() || right_col.IsUnsigned() ||
          left_col.Type() != right_col.Type() ||
          (left_col.Type() != ValueType::kInt64 &&
           left_col.Type() != ValueType::kDate) ||
          key.right >= projections[index].size()) {
        return std::optional<Relation>{};
      }
      left_columns.push_back(static_cast<slot_t>(key.left));
      right_store.push_back(projections[index][key.right]);
    }
    if (result.TotalRows() >= std::numeric_limits<uint32_t>::max()) {
      return std::optional<Relation>{};
    }
    const std::vector<Expression> residual =
        ResidualJoinPredicates(result.schema, projected, predicates);
    const Schema out_schema = result.schema + projected;
    const size_t left_width = result.schema.ColumnCount();
    const size_t right_width = projections[index].size();

    struct FastIntColPairPred {
      slot_t left_col{0};
      slot_t right_store_col{0};
      BinaryOperation op{BinaryOperation::kLessThan};
    };
    std::vector<FastIntColPairPred> fast_residuals;
    fast_residuals.reserve(residual.size());
    auto find_unique_col = [&](const Schema& s, const ColumnName& name,
                               size_t* out_idx) -> int {
      int hits = 0;
      for (size_t i = 0; i < s.ColumnCount(); ++i) {
        const Column& col = s.GetColumn(i);
        if (!IdentifierEquals(col.Name().name, name.name)) {
          continue;
        }
        if (!name.schema.empty() &&
            !IdentifierEquals(col.Name().schema, name.schema)) {
          continue;
        }
        *out_idx = i;
        ++hits;
      }
      return hits;
    };
    auto flip_cmp_op = [](BinaryOperation op, BinaryOperation* flipped) {
      switch (op) {
        case BinaryOperation::kEquals:
        case BinaryOperation::kNotEquals:
          *flipped = op;
          return true;
        case BinaryOperation::kLessThan:
          *flipped = BinaryOperation::kGreaterThan;
          return true;
        case BinaryOperation::kLessThanEquals:
          *flipped = BinaryOperation::kGreaterThanEquals;
          return true;
        case BinaryOperation::kGreaterThan:
          *flipped = BinaryOperation::kLessThan;
          return true;
        case BinaryOperation::kGreaterThanEquals:
          *flipped = BinaryOperation::kLessThanEquals;
          return true;
        default:
          return false;
      }
    };
    for (const Expression& pred : residual) {
      if (!pred || pred->Type() != TypeTag::kBinaryExp) {
        continue;
      }
      const auto& bin = pred->AsBinaryExpression();
      if (!bin.Left() || !bin.Right() ||
          bin.Left()->Type() != TypeTag::kColumnValue ||
          bin.Right()->Type() != TypeTag::kColumnValue) {
        continue;
      }
      BinaryOperation flipped_op{};
      if (!flip_cmp_op(bin.Op(), &flipped_op)) {
        continue;
      }
      const ColumnName& ln = bin.Left()->AsColumnValue().GetColumnName();
      const ColumnName& rn = bin.Right()->AsColumnValue().GetColumnName();
      size_t ll = 0;
      size_t lr = 0;
      size_t right_left = 0;
      size_t rr = 0;
      const int h_ll = find_unique_col(result.schema, ln, &ll);
      const int h_lr = find_unique_col(projected, ln, &lr);
      const int h_rl = find_unique_col(result.schema, rn, &right_left);
      const int h_rr = find_unique_col(projected, rn, &rr);
      size_t lcol = 0;
      size_t rcol = 0;
      BinaryOperation norm_op = bin.Op();
      if (h_ll == 1 && h_lr == 0 && h_rl == 0 && h_rr == 1) {
        lcol = ll;
        rcol = rr;
      } else if (h_ll == 0 && h_lr == 1 && h_rl == 1 && h_rr == 0) {
        lcol = right_left;
        rcol = lr;
        norm_op = flipped_op;
      } else {
        continue;
      }
      const Column& lc = result.schema.GetColumn(lcol);
      const Column& rc = projected.GetColumn(rcol);
      if (lc.IsUnsigned() || rc.IsUnsigned() || lc.Type() != rc.Type() ||
          (lc.Type() != ValueType::kInt64 && lc.Type() != ValueType::kDate) ||
          rcol >= right_width) {
        continue;
      }
      fast_residuals.push_back(FastIntColPairPred{
          .left_col = static_cast<slot_t>(lcol),
          .right_store_col = projections[index][rcol],
          .op = norm_op,
      });
    }
    const bool all_residuals_fast =
        !residual.empty() && fast_residuals.size() == residual.size();
    const bool columnar_left =
        result.HasColumnarOnly() && result.columnar != nullptr;
    bool right_types_supported = true;
    for (size_t c = 0; c < right_width; ++c) {
      const ValueType vt = projected.GetColumn(c).Type();
      if (vt != ValueType::kInt64 && vt != ValueType::kDate &&
          vt != ValueType::kDouble && vt != ValueType::kVarChar) {
        right_types_supported = false;
        break;
      }
    }
    const bool emit_columnar = columnar_left && right_types_supported &&
                               ordered_local[index].empty() &&
                               (residual.empty() || all_residuals_fast);

    std::vector<Row> mat_left_rows;
    if (columnar_left && !emit_columnar) {
      RETURN_IF_FAIL(MaterializeColumnarTableToRows(
          *result.columnar, result.schema, &mat_left_rows, nullptr));
    }
    const std::vector<Row>& left_rows =
        columnar_left ? mat_left_rows : result.rows;
    const size_t total_left_rows =
        columnar_left ? result.columnar->total_rows : left_rows.size();
    if (total_left_rows == 0) {
      return std::optional<Relation>{};
    }

    struct LeftChunkLoc {
      uint32_t chunk{0};
      uint32_t row{0};
    };
    std::vector<LeftChunkLoc> left_locs;
    std::vector<std::vector<int64_t>> left_key_vals;
    std::vector<std::vector<uint8_t>> left_key_nulls;
    std::vector<std::vector<int64_t>> left_fast_vals;
    std::vector<std::vector<uint8_t>> left_fast_nulls;
    if (emit_columnar) {
      const ColumnarTable& lt = *result.columnar;
      left_locs.resize(total_left_rows);
      size_t pos = 0;
      for (size_t ci = 0; ci < lt.chunks.size(); ++ci) {
        const auto nr = static_cast<uint32_t>(lt.chunks[ci].rows);
        for (uint32_t r = 0; r < nr; ++r) {
          left_locs[pos++] =
              LeftChunkLoc{.chunk = static_cast<uint32_t>(ci), .row = r};
        }
      }
      auto extract_left_int_col = [&](slot_t col_idx,
                                      std::vector<int64_t>* vals_out,
                                      std::vector<uint8_t>* nulls_out) {
        vals_out->resize(total_left_rows);
        nulls_out->assign(total_left_rows, 0);
        size_t base = 0;
        for (const ColumnarChunk& fc : lt.chunks) {
          if (fc.rows == 0) {
            continue;
          }
          const ColumnarColumn& col = fc.columns[col_idx];
          if (col.IsFlat() && col.is_uniform && !col.has_null &&
              col.type != ValueType::kNull && col.ints.size() >= fc.rows) {
            std::memcpy(vals_out->data() + base, col.ints.data(),
                        fc.rows * sizeof(int64_t));
          } else if (col.encoding == ColumnarColumn::Encoding::kConstant &&
                     col.is_uniform) {
            if (col.const_is_null || col.type == ValueType::kNull) {
              std::fill_n(nulls_out->data() + base, fc.rows, uint8_t{1});
            } else {
              std::fill_n(vals_out->data() + base, fc.rows, col.const_int);
            }
          } else {
            for (size_t r = 0; r < fc.rows; ++r) {
              int64_t v = 0;
              if (!col.TryReadNonNullInt(r, &v)) {
                (*nulls_out)[base + r] = 1;
              } else {
                (*vals_out)[base + r] = v;
              }
            }
          }
          base += fc.rows;
        }
      };
      left_key_vals.resize(left_columns.size());
      left_key_nulls.resize(left_columns.size());
      for (size_t p = 0; p < left_columns.size(); ++p) {
        extract_left_int_col(left_columns[p], &left_key_vals[p],
                             &left_key_nulls[p]);
      }
      left_fast_vals.resize(fast_residuals.size());
      left_fast_nulls.resize(fast_residuals.size());
      for (size_t fp = 0; fp < fast_residuals.size(); ++fp) {
        extract_left_int_col(fast_residuals[fp].left_col, &left_fast_vals[fp],
                             &left_fast_nulls[fp]);
      }
    }

    struct PartBits {
      slot_t store_column{0};
      int64_t lo{0};
      uint64_t span{0};
      uint64_t set_bits{0};
      unsigned pack_shift{0};
      bool all_set{false};
      std::vector<uint64_t> words;
    };
    std::vector<PartBits> parts(right_store.size());
    for (size_t p = 0; p < right_store.size(); ++p) {
      parts[p].store_column = right_store[p];
      bool any = false;
      int64_t lo = 0;
      int64_t hi = 0;
      if (emit_columnar) {
        const int64_t* vp = left_key_vals[p].data();
        const uint8_t* np = left_key_nulls[p].data();
        for (size_t r = 0; r < total_left_rows; ++r) {
          if (np[r] != 0) {
            continue;
          }
          const int64_t key = vp[r];
          if (!any) {
            lo = hi = key;
            any = true;
          } else {
            lo = std::min(lo, key);
            hi = std::max(hi, key);
          }
        }
      } else {
        for (const Row& row : left_rows) {
          const Value& value = row[left_columns[p]];
          if (value.IsNull()) {
            continue;
          }
          const int64_t key = value.value.int_value;
          if (!any) {
            lo = hi = key;
            any = true;
          } else {
            lo = std::min(lo, key);
            hi = std::max(hi, key);
          }
        }
      }
      if (!any) {
        return std::optional<Relation>{};
      }
      const uint64_t span =
          static_cast<uint64_t>(hi) - static_cast<uint64_t>(lo);
      if (span > 4000000ULL) {
        return std::optional<Relation>{};
      }
      parts[p].lo = lo;
      parts[p].span = span;
      parts[p].words.assign(static_cast<size_t>(span >> 6) + 1, 0);
    }
    unsigned total_pack_bits = 0;
    for (auto& part : parts) {
      part.pack_shift = total_pack_bits;
      total_pack_bits += static_cast<unsigned>(std::bit_width(part.span));
    }
    const bool use_packed_key = (parts.size() >= 2 && total_pack_bits <= 63);
    FlatIntKeyMap<uint32_t> pair_head;
    std::vector<uint32_t> pair_next;
    FlatJoinValueBuckets<CompositeIntKey, uint32_t, CompositeIntKeyHash>
        buckets;
    if (use_packed_key) {
      pair_head.Reserve(std::max<size_t>(total_left_rows, 16));
      pair_next.assign(total_left_rows, UINT32_MAX);
    } else {
      buckets.reserve(total_left_rows * 2);
    }
    size_t bloom_bits = 1024;
    while (bloom_bits < total_left_rows * 8 && bloom_bits < (1U << 21)) {
      bloom_bits *= 2;
    }
    const size_t bloom_mask = bloom_bits - 1;
    std::vector<uint64_t> composite_bloom(
        use_packed_key ? 0 : (bloom_bits >> 6), 0);
    size_t build_keys = 0;
    const auto mix_hash64 = [](uint64_t x) noexcept -> uint64_t {
      x ^= x >> 33;
      x *= 0xff51afd7ed558ccdULL;
      x ^= x >> 33;
      return x;
    };
    if (use_packed_key) {
      const size_t nparts = parts.size();
      for (size_t r = 0; r < total_left_rows; ++r) {
        uint64_t packed_u64 = 0;
        bool has_null = false;
        if (emit_columnar) {
          for (size_t p = 0; p < nparts; ++p) {
            if (left_key_nulls[p][r] != 0) {
              has_null = true;
              break;
            }
            const uint64_t off = static_cast<uint64_t>(left_key_vals[p][r]) -
                                 static_cast<uint64_t>(parts[p].lo);
            parts[p].words[off >> 6] |= uint64_t{1} << (off & 63);
            packed_u64 |= off << parts[p].pack_shift;
          }
        } else {
          const Row& row = left_rows[r];
          for (size_t p = 0; p < nparts; ++p) {
            const Value& v = row[left_columns[p]];
            if (v.IsNull()) {
              has_null = true;
              break;
            }
            const uint64_t off = static_cast<uint64_t>(v.value.int_value) -
                                 static_cast<uint64_t>(parts[p].lo);
            parts[p].words[off >> 6] |= uint64_t{1} << (off & 63);
            packed_u64 |= off << parts[p].pack_shift;
          }
        }
        if (has_null) {
          continue;
        }
        bool inserted = false;
        uint32_t* head =
            pair_head.TryEmplace(static_cast<int64_t>(packed_u64), &inserted);
        const auto r32 = static_cast<uint32_t>(r);
        if (inserted) {
          *head = r32;
        } else {
          pair_next[r32] = *head;
          *head = r32;
        }
        ++build_keys;
      }
    } else {
      const auto insert_build_key = [&](uint32_t r_idx,
                                        const CompositeIntKey& key) {
        const uint64_t kh = mix_hash64(CompositeIntKeyHash{}(key));
        const size_t b0 = static_cast<size_t>(kh) & bloom_mask;
        const size_t b1 = static_cast<size_t>(kh >> 24) & bloom_mask;
        composite_bloom[b0 >> 6] |= uint64_t{1} << (b0 & 63);
        composite_bloom[b1 >> 6] |= uint64_t{1} << (b1 & 63);
        buckets.emplace_hashed(key, r_idx, kh);
        ++build_keys;
        for (size_t p = 0; p < parts.size(); ++p) {
          const uint64_t off = static_cast<uint64_t>(key.parts[p]) -
                               static_cast<uint64_t>(parts[p].lo);
          parts[p].words[off >> 6] |= uint64_t{1} << (off & 63);
        }
      };
      if (emit_columnar) {
        for (size_t r = 0; r < total_left_rows; ++r) {
          CompositeIntKey key;
          key.count = static_cast<uint8_t>(left_columns.size());
          bool has_null = false;
          for (size_t p = 0; p < left_columns.size(); ++p) {
            if (left_key_nulls[p][r] != 0) {
              has_null = true;
              break;
            }
            key.parts[p] = left_key_vals[p][r];
          }
          if (has_null) {
            continue;
          }
          insert_build_key(static_cast<uint32_t>(r), key);
        }
      } else {
        for (size_t r = 0; r < left_rows.size(); ++r) {
          const Row& row = left_rows[r];
          if (HasNullKey(row, left_columns)) {
            continue;
          }
          insert_build_key(static_cast<uint32_t>(r),
                           EncodeCompositeIntKey(row, left_columns));
        }
      }
    }
    if (build_keys == 0) {
      return std::optional<Relation>{};
    }
    for (PartBits& pb : parts) {
      uint64_t cnt = 0;
      for (uint64_t w : pb.words) {
        cnt += static_cast<uint64_t>(std::popcount(w));
      }
      pb.set_bits = cnt;
      pb.all_set = (cnt == pb.span + 1);
    }
    std::array<size_t, CompositeIntKey::kMaxParts> check_order{};
    for (size_t p = 0; p < parts.size(); ++p) {
      check_order[p] = p;
    }
    // Order parts by lowest bitmap fill ratio first so sparse selective keys
    // prune before dense small-domain keys (e.g. inv_date_sk before wh_sk).
    std::sort(
        check_order.begin(), check_order.begin() + parts.size(),
        [&](size_t a, size_t b) {
          if (parts[a].all_set != parts[b].all_set) {
            return !parts[a].all_set;
          }
          const __uint128_t lhs = static_cast<__uint128_t>(parts[a].set_bits) *
                                  static_cast<__uint128_t>(parts[b].span + 1);
          const __uint128_t rhs = static_cast<__uint128_t>(parts[b].set_bits) *
                                  static_cast<__uint128_t>(parts[a].span + 1);
          if (lhs != rhs) {
            return lhs < rhs;
          }
          if (parts[a].set_bits != parts[b].set_bits) {
            return parts[a].set_bits < parts[b].set_bits;
          }
          return parts[a].span < parts[b].span;
        });

    std::vector<uint64_t> pair_bits;
    // Lean proof:
    // lean/TinylambOpt/ResearchProofs/PairBitmapDenseRemap.lean (sound)
    std::vector<uint32_t> pair_outer_dense;
    std::shared_ptr<QueryMemoryCharge> pair_charge;
    size_t pair_pa = 0;
    size_t pair_pb = 0;
    uint64_t pair_stride1 = 0;
    bool use_exact_pair_bits = false;
    if (use_packed_key) {
      uint64_t best_score = 0;
      for (size_t i = 0; i < parts.size(); ++i) {
        for (size_t j = i + 1; j < parts.size(); ++j) {
          size_t outer_p = i;
          size_t inner_p = j;
          if (parts[j].set_bits < parts[i].set_bits ||
              (parts[j].set_bits == parts[i].set_bits &&
               parts[j].span < parts[i].span)) {
            outer_p = j;
            inner_p = i;
          }
          const uint64_t outer_span1 = parts[outer_p].span + 1;
          const uint64_t d_inner = parts[inner_p].span + 1;
          const bool can_dense = (parts[outer_p].set_bits < outer_span1 &&
                                  outer_span1 <= (4ULL << 20));
          const uint64_t d_outer =
              can_dense ? parts[outer_p].set_bits : outer_span1;
          if (d_outer <= 1 || d_inner <= 1 ||
              d_outer > (64ULL << 20) / d_inner) {
            continue;
          }
          const bool both_sparse =
              (!parts[outer_p].all_set && !parts[inner_p].all_set);
          const uint64_t prod =
              parts[outer_p].set_bits * parts[inner_p].set_bits;
          const uint64_t score =
              (both_sparse ? (uint64_t{1} << 62) : 0ULL) + prod;
          if (!use_exact_pair_bits || score > best_score) {
            use_exact_pair_bits = true;
            best_score = score;
            pair_pa = outer_p;
            pair_pb = inner_p;
            pair_stride1 = d_inner;
          }
        }
      }
      if (use_exact_pair_bits) {
        const uint64_t outer_span1 = parts[pair_pa].span + 1;
        const bool need_dense = (parts[pair_pa].set_bits < outer_span1 &&
                                 outer_span1 <= (4ULL << 20));
        const uint64_t d_outer =
            need_dense ? parts[pair_pa].set_bits : outer_span1;
        const uint64_t total_bits = d_outer * pair_stride1;
        const auto pair_words = static_cast<size_t>((total_bits + 63ULL) >> 6);
        const size_t charge_bytes =
            (pair_words * sizeof(uint64_t)) +
            (need_dense ? static_cast<size_t>(outer_span1) * sizeof(uint32_t)
                        : 0);
        pair_charge = TryReserveQueryMemory(charge_bytes);
        if (!pair_charge) {
          use_exact_pair_bits = false;
        } else {
          pair_bits.assign(pair_words, 0);
          if (need_dense) {
            pair_outer_dense.assign(static_cast<size_t>(outer_span1),
                                    UINT32_MAX);
            uint32_t dense_next = 0;
            const size_t nw = parts[pair_pa].words.size();
            for (size_t wi = 0; wi < nw; ++wi) {
              uint64_t w = parts[pair_pa].words[wi];
              const size_t base = wi << 6;
              while (w != 0) {
                const auto b = static_cast<unsigned>(std::countr_zero(w));
                pair_outer_dense[base + b] = dense_next++;
                w &= (w - 1);
              }
            }
          }
          const int64_t lo_pa = parts[pair_pa].lo;
          const int64_t lo_pb = parts[pair_pb].lo;
          for (size_t r = 0; r < total_left_rows; ++r) {
            int64_t ka = 0;
            int64_t kb = 0;
            if (emit_columnar) {
              if (left_key_nulls[pair_pa][r] != 0 ||
                  left_key_nulls[pair_pb][r] != 0) {
                continue;
              }
              ka = left_key_vals[pair_pa][r];
              kb = left_key_vals[pair_pb][r];
            } else {
              const Value& va = left_rows[r][left_columns[pair_pa]];
              const Value& vb = left_rows[r][left_columns[pair_pb]];
              if (va.IsNull() || vb.IsNull()) {
                continue;
              }
              ka = va.value.int_value;
              kb = vb.value.int_value;
            }
            const uint64_t opa =
                static_cast<uint64_t>(ka) - static_cast<uint64_t>(lo_pa);
            const uint64_t opb =
                static_cast<uint64_t>(kb) - static_cast<uint64_t>(lo_pb);
            const uint64_t row_idx =
                pair_outer_dense.empty() ? opa : pair_outer_dense[opa];
            if (row_idx == UINT32_MAX) {
              continue;
            }
            const uint64_t bit = (row_idx * pair_stride1) + opb;
            pair_bits[bit >> 6] |= uint64_t{1} << (bit & 63);
          }
        }
      }
    }
    // Lean proof:
    // lean/TinylambOpt/ResearchProofs/TwoProbePackedBloom.lean (sound)
    std::vector<uint64_t> packed_bloom;
    size_t packed_bloom_mask = 0;
    std::shared_ptr<QueryMemoryCharge> packed_bloom_charge;
    if (use_packed_key && !use_exact_pair_bits && build_keys > 0) {
      const size_t packed_bloom_bits = std::bit_ceil(std::clamp<size_t>(
          build_keys * 16, size_t{1024}, size_t{16ULL << 20}));
      const size_t bloom_words = packed_bloom_bits >> 6;
      packed_bloom_charge =
          TryReserveQueryMemory(bloom_words * sizeof(uint64_t));
      if (packed_bloom_charge) {
        packed_bloom.assign(bloom_words, 0);
        packed_bloom_mask = packed_bloom_bits - 1;
        pair_head.ForEach([&](int64_t packed_key, uint32_t) {
          const uint64_t kh = mix_hash64(static_cast<uint64_t>(packed_key));
          const size_t b0 = static_cast<size_t>(kh) & packed_bloom_mask;
          const size_t b1 = static_cast<size_t>(kh >> 24) & packed_bloom_mask;
          packed_bloom[b0 >> 6] |= uint64_t{1} << (b0 & 63);
          packed_bloom[b1 >> 6] |= uint64_t{1} << (b1 & 63);
        });
      }
    }

    auto table = context.GetTable(probed.table);
    if (!table.HasValue()) {
      return std::optional<Relation>{};
    }
    std::vector<slot_t> probe_proj;
    const auto add_probe_col = [&](slot_t col) {
      if (std::ranges::find(probe_proj, col) == probe_proj.end()) {
        probe_proj.push_back(col);
      }
    };
    for (const slot_t col : projections[index]) {
      add_probe_col(col);
    }
    for (const auto& part : parts) {
      add_probe_col(part.store_column);
    }
    for (const auto& fr : fast_residuals) {
      add_probe_col(fr.right_store_col);
    }
    auto store =
        GetOrBuildLSTableStore(context.txn_, *table.Value(), true,
                               !probe_proj.empty() ? &probe_proj : nullptr,
                               /*allow_block_lazy=*/true);
    if (context.txn_.HasAnyPageVersionChains()) {
      return std::optional<Relation>{};
    }
    if (store == nullptr || store->blocks.empty()) {
      if (!emit_columnar) {
        return std::optional<Relation>{};
      }
      auto eval_fast_int_stream = [](int64_t l, int64_t r,
                                     BinaryOperation op) -> bool {
        switch (op) {
          case BinaryOperation::kEquals:
            return l == r;
          case BinaryOperation::kNotEquals:
            return l != r;
          case BinaryOperation::kLessThan:
            return l < r;
          case BinaryOperation::kLessThanEquals:
            return l <= r;
          case BinaryOperation::kGreaterThan:
            return l > r;
          case BinaryOperation::kGreaterThanEquals:
            return l >= r;
          default:
            return false;
        }
      };
      const ColumnarTable& lt = *result.columnar;
      std::vector<ValueType> left_col_type(left_width);
      std::vector<uint8_t> left_col_unsigned(left_width, 0);
      std::vector<uint8_t> left_col_interval(left_width, 0);
      std::vector<uint8_t> left_col_uniform(left_width, 1);
      std::vector<uint8_t> left_col_has_null(left_width, 0);
      for (size_t c = 0; c < left_width; ++c) {
        const Column& sc = result.schema.GetColumn(c);
        left_col_type[c] = sc.Type();
        left_col_unsigned[c] = sc.IsUnsigned() ? 1 : 0;
        left_col_interval[c] = sc.IsInterval() ? 1 : 0;
        if (sc.Type() == ValueType::kNull) {
          left_col_uniform[c] = 0;
        }
        for (const ColumnarChunk& fc : lt.chunks) {
          if (fc.rows == 0) {
            continue;
          }
          const ColumnarColumn& lc = fc.columns[c];
          if (lc.has_null || lc.type == ValueType::kNull) {
            left_col_has_null[c] = 1;
          }
          if (!lc.is_uniform ||
              (lc.type != ValueType::kNull && lc.type != sc.Type())) {
            left_col_uniform[c] = 0;
          }
        }
      }
      std::shared_ptr<const TlcsFile> clean_tlcs =
          ResolveCleanBackingTlcs(*table.Value(), context);
      if (clean_tlcs == nullptr || clean_tlcs->BlockCount() == 0 ||
          clean_tlcs->TotalRows() == 0) {
        // Page-based (PAX/row-page) parallel morsel streaming probe using
        // DecodeNextTypedBatch + RuntimeFilter on the most selective join
        // key. Avoids materializing huge uncompacted fact tables (e.g.
        // 133M-row `inventory` in TPC-DS Q72) before hash-joining.
        const auto t_m0 = std::chrono::steady_clock::now();
        Status morsel_status = Status::kSuccess;
        std::vector<Table::ScanMorsel> morsels =
            table.Value()->BuildScanMorsels(context.txn_, 8, &morsel_status);
        const auto t_m1 = std::chrono::steady_clock::now();
        if (!morsel_status.ok() || morsels.empty()) {
          return std::optional<Relation>{};
        }
        std::vector<size_t> part_proj_slot(parts.size(), 0);
        for (size_t p = 0; p < parts.size(); ++p) {
          auto it =
              std::ranges::find(projections[index], parts[p].store_column);
          if (it == projections[index].end()) {
            return std::optional<Relation>{};
          }
          part_proj_slot[p] =
              static_cast<size_t>(it - projections[index].begin());
        }
        std::vector<size_t> fast_proj_slot(fast_residuals.size(), 0);
        for (size_t fp = 0; fp < fast_residuals.size(); ++fp) {
          auto it = std::ranges::find(projections[index],
                                      fast_residuals[fp].right_store_col);
          if (it == projections[index].end()) {
            return std::optional<Relation>{};
          }
          fast_proj_slot[fp] =
              static_cast<size_t>(it - projections[index].begin());
        }
        std::vector<ValueType> proj_types(right_width);
        std::vector<bool> proj_unsigned(right_width, false);
        std::vector<size_t> proj_vc_ordinal(right_width, 0);
        size_t vc_ord = 0;
        for (size_t rc = 0; rc < right_width; ++rc) {
          const Column& col = projected.GetColumn(rc);
          proj_types[rc] = col.Type();
          proj_unsigned[rc] = col.IsUnsigned();
          if (col.Type() == ValueType::kVarChar) {
            proj_vc_ordinal[rc] = vc_ord++;
          }
        }
        // Lean proof: lean/TinylambOpt/VectorizedJoinAndPruning.lean
        //   (multi_col_page_zone_map_prune_sound)
        // Prune pages conjunctively across ALL join-key columns with cached
        // per-page [min, max] zone maps (e.g. on TPC-DS Q72 `inventory`,
        // `inv_warehouse_sk` has span 9 and prunes 0% of pages while
        // `inv_date_sk` has span 365 and prunes 80% of pages). On the first
        // clean scan before any bounds are cached, compute bounds for all
        // join-key columns in one pass.
        std::shared_ptr<const RuntimeFilterGroup> rf_group;
        std::optional<slot_t> rf_full_col;
        ValueType rf_col_type = ValueType::kNull;
        const std::optional<std::vector<slot_t>> proj_opt(projections[index]);
        std::shared_ptr<const TablePageDirectory> page_dir;
        std::vector<std::shared_ptr<TablePageDirectory::IntBoundsVec>>
            building_all_bounds;
        size_t zm_p = check_order[0];
        for (size_t p = 0; p < parts.size(); ++p) {
          if (parts[p].set_bits <= RuntimeFilterBuilder::kMaxTrackedKeys &&
              (parts[zm_p].set_bits > RuntimeFilterBuilder::kMaxTrackedKeys ||
               parts[p].set_bits < parts[zm_p].set_bits)) {
            zm_p = p;
          }
        }
        std::vector<uint8_t> part_pruned_none(parts.size(), 0);
        if (context.txn_.IsReadOnly() && !context.txn_.IsFailed() &&
            !table.Value()->RowsRewrittenInPlace() &&
            !context.txn_.HasAnyPageVersionChains()) {
          auto dir = table.Value()->GetCleanPageDirectory();
          if (dir != nullptr && dir->layout_generation != 0 &&
              dir->layout_generation == table.Value()->LayoutGeneration() &&
              dir->first_pid == table.Value()->FirstPageId() &&
              !dir->page_ids.empty()) {
            page_dir = std::move(dir);
            const size_t npages = page_dir->page_ids.size();
            std::vector<std::shared_ptr<const TablePageDirectory::IntBoundsVec>>
                cached_bounds(parts.size());
            bool any_cached = false;
            bool any_missing = false;
            for (size_t p = 0; p < parts.size(); ++p) {
              auto b = page_dir->GetColIntBounds(parts[p].store_column);
              if (b != nullptr && b->size() == npages) {
                cached_bounds[p] = std::move(b);
                any_cached = true;
              } else {
                any_missing = true;
              }
            }
            auto part_rejects_range = [&](size_t p, int64_t bmin,
                                          int64_t bmax) -> bool {
              const int64_t lo = parts[p].lo;
              const uint64_t span = parts[p].span;
              const auto& words = parts[p].words;
              if (bmin > bmax || bmax < lo ||
                  (bmin > lo &&
                   static_cast<uint64_t>(bmin) - static_cast<uint64_t>(lo) >
                       span)) {
                return true;
              }
              const uint64_t off_lo =
                  bmin <= lo
                      ? 0ULL
                      : static_cast<uint64_t>(bmin) - static_cast<uint64_t>(lo);
              const uint64_t off_hi =
                  std::min<uint64_t>(span, static_cast<uint64_t>(bmax) -
                                               static_cast<uint64_t>(lo));
              if (off_hi - off_lo <= 65536ULL) {
                const auto w_lo = static_cast<size_t>(off_lo >> 6);
                const auto w_hi = static_cast<size_t>(off_hi >> 6);
                if (w_lo == w_hi) {
                  uint64_t mask = (~uint64_t{0}) << (off_lo & 63);
                  mask &= (~uint64_t{0}) >>
                          (63U - static_cast<unsigned>(off_hi & 63));
                  return (words[w_lo] & mask) == 0;
                }
                const uint64_t head_mask = (~uint64_t{0}) << (off_lo & 63);
                const uint64_t tail_mask =
                    (~uint64_t{0}) >>
                    (63U - static_cast<unsigned>(off_hi & 63));
                if ((words[w_lo] & head_mask) != 0 ||
                    (words[w_hi] & tail_mask) != 0) {
                  return false;
                }
                for (size_t wi = w_lo + 1; wi < w_hi; ++wi) {
                  if (words[wi] != 0) {
                    return false;
                  }
                }
                return true;
              }
              return false;
            };
            if (any_cached) {
              // Prune pages using all cached per-page [min, max] zone maps
              // and pick the most selective column for the row-level RF.
              std::vector<size_t> part_surviving(parts.size(), npages);
              std::vector<Table::ScanMorsel> pruned;
              for (size_t pi = 0; pi < npages; ++pi) {
                bool rejected = false;
                for (size_t p = 0; p < parts.size(); ++p) {
                  if (cached_bounds[p] == nullptr) {
                    continue;
                  }
                  const auto [bmin, bmax] = (*cached_bounds[p])[pi];
                  if (part_rejects_range(p, bmin, bmax)) {
                    rejected = true;
                    --part_surviving[p];
                  }
                }
                if (rejected) {
                  continue;
                }
                if (pruned.empty() || pruned.back().size() == 8) {
                  pruned.emplace_back();
                  pruned.back().reserve(8);
                }
                pruned.back().push_back(page_dir->page_ids[pi]);
              }
              morsels = std::move(pruned);
              size_t best_surv = npages + 1;
              for (size_t p = 0; p < parts.size(); ++p) {
                if (cached_bounds[p] != nullptr &&
                    part_surviving[p] == npages) {
                  part_pruned_none[p] = 1;
                }
                if (cached_bounds[p] != nullptr &&
                    parts[p].set_bits <=
                        RuntimeFilterBuilder::kMaxTrackedKeys &&
                    part_surviving[p] < best_surv) {
                  best_surv = part_surviving[p];
                  zm_p = p;
                }
              }
            } else if (any_missing) {
              building_all_bounds.resize(parts.size());
              for (size_t p = 0; p < parts.size(); ++p) {
                building_all_bounds[p] =
                    std::make_shared<TablePageDirectory::IntBoundsVec>(
                        npages, std::pair<int64_t, int64_t>{
                                    std::numeric_limits<int64_t>::max(),
                                    std::numeric_limits<int64_t>::min()});
              }
            }
          }
        }
        struct StreamExtraRf {
          std::shared_ptr<const RuntimeFilterGroup> group;
          slot_t col{0};
          ValueType type{ValueType::kNull};
        };
        std::vector<StreamExtraRf> extra_rfs;
        auto build_part_rf =
            [&](size_t p_idx) -> std::shared_ptr<const RuntimeFilterGroup> {
          if (parts[p_idx].set_bits > RuntimeFilterBuilder::kMaxTrackedKeys) {
            return nullptr;
          }
          std::vector<int64_t> rf_keys;
          rf_keys.reserve(static_cast<size_t>(parts[p_idx].set_bits));
          const int64_t lo = parts[p_idx].lo;
          for (size_t wi = 0; wi < parts[p_idx].words.size(); ++wi) {
            uint64_t w = parts[p_idx].words[wi];
            while (w != 0) {
              const auto bit = static_cast<unsigned>(std::countr_zero(w));
              rf_keys.push_back(lo + static_cast<int64_t>((wi << 6) | bit));
              w &= w - 1;
            }
          }
          return RuntimeFilterBuilder::FromKeys(rf_keys);
        };
        rf_group = build_part_rf(zm_p);
        if (rf_group != nullptr) {
          rf_full_col = parts[zm_p].store_column;
          rf_col_type =
              table.Value()->GetSchema().GetColumn(*rf_full_col).Type();
        }
        for (size_t p_idx = 0; p_idx < parts.size(); ++p_idx) {
          if (p_idx == zm_p) {
            continue;
          }
          // Skip dense all-set keys that rejected 0% of pages (e.g.
          // `inv_warehouse_sk` in TPC-DS Q72 where all 10 warehouses match):
          // peeking them per-row in DecodeNextTypedBatch adds pure overhead.
          if (parts[p_idx].all_set && part_pruned_none[p_idx] != 0) {
            continue;
          }
          auto eg = build_part_rf(p_idx);
          if (eg != nullptr) {
            const slot_t ecol = parts[p_idx].store_column;
            const ValueType etype =
                table.Value()->GetSchema().GetColumn(ecol).Type();
            extra_rfs.push_back(StreamExtraRf{
                .group = std::move(eg), .col = ecol, .type = etype});
          }
        }
        if (morsels.empty()) {
          Relation empty_out(context.execution_runtime());
          empty_out.schema = out_schema;
          ++empty_out.hash_joins;
          return std::optional<Relation>(std::move(empty_out));
        }
        const bool build_all_zm = !building_all_bounds.empty();
        const size_t work_items =
            build_all_zm ? page_dir->page_ids.size() : morsels.size();
        size_t page_workers = std::min<size_t>(
            work_items,
            QueryParallelWorkerLimit(std::min<size_t>(16, work_items)));
        page_workers = std::max<size_t>(1, page_workers);
        std::atomic<size_t> next_morsel{0};
        std::vector<std::vector<ColumnarChunk>> worker_chunks(page_workers);
        std::vector<Status> page_errors(page_workers, Status::kSuccess);
        RETURN_IF_FAIL(RunParallelTasks(page_workers, [&](size_t w) {
          Status& err = page_errors[w];
          TypedCellBatch batch;
          batch.Configure(right_width, proj_types, proj_unsigned);
          std::vector<uint32_t> cur_left_rows;
          cur_left_rows.reserve(kColumnarChunkRows);
          ColumnarChunk cur_chunk;
          bool chunk_open = false;
          auto open_chunk = [&]() {
            cur_chunk = ColumnarChunk();
            cur_chunk.InitFromSchema(out_schema, kColumnarChunkRows);
            cur_chunk.EnsureArena();
            cur_left_rows.clear();
            chunk_open = true;
          };
          auto flush_chunk = [&]() {
            if (!chunk_open || cur_left_rows.empty()) {
              return;
            }
            const size_t n = cur_left_rows.size();
            cur_chunk.rows = n;
            auto left_gi = std::make_shared<ChunkGatherIndex>();
            left_gi->source_table = result.columnar;
            left_gi->src_chunks.resize(n);
            left_gi->src_rows.resize(n);
            for (size_t k = 0; k < n; ++k) {
              const LeftChunkLoc& loc = left_locs[cur_left_rows[k]];
              left_gi->src_chunks[k] = loc.chunk;
              left_gi->src_rows[k] = loc.row;
            }
            for (size_t c = 0; c < left_width; ++c) {
              if (left_col_uniform[c] != 0) {
                cur_chunk.columns[c].SetGathered(
                    left_gi, c, left_col_type[c], left_col_unsigned[c] != 0,
                    left_col_interval[c] != 0, true, left_col_has_null[c] != 0);
              } else {
                Value val;
                for (size_t k = 0; k < n; ++k) {
                  const LeftChunkLoc& loc = left_locs[cur_left_rows[k]];
                  lt.chunks[loc.chunk].columns[c].WriteValueInto(loc.row, &val);
                  cur_chunk.columns[c].AppendValue(val, cur_chunk.EnsureArena(),
                                                   k);
                }
              }
            }
            worker_chunks[w].push_back(std::move(cur_chunk));
            cur_left_rows.clear();
            chunk_open = false;
          };
          Value cell;
          auto emit_match = [&](uint32_t lr, size_t r) {
            const size_t base = r * right_width;
            for (size_t fp = 0; fp < fast_residuals.size(); ++fp) {
              if (left_fast_nulls[fp][lr] != 0) {
                return;
              }
              const size_t fslot = fast_proj_slot[fp];
              if (batch.nulls[base + fslot] != 0) {
                return;
              }
              if (!eval_fast_int_stream(left_fast_vals[fp][lr],
                                        batch.cells[base + fslot],
                                        fast_residuals[fp].op)) {
                return;
              }
            }
            if (!chunk_open) {
              open_chunk();
            }
            const size_t k = cur_left_rows.size();
            cur_left_rows.push_back(lr);
            for (size_t rc = 0; rc < right_width; ++rc) {
              if (batch.nulls[base + rc] != 0) {
                cell = Value();
              } else if (proj_types[rc] == ValueType::kDouble) {
                cell.SetDouble(std::bit_cast<double>(batch.cells[base + rc]));
              } else if (proj_types[rc] == ValueType::kDate) {
                cell = Value::DateFromDays(batch.cells[base + rc]);
              } else if (proj_types[rc] == ValueType::kVarChar) {
                cell.CopyFromStringView(batch.Varchar(r, proj_vc_ordinal[rc]));
              } else {
                cell = Value(batch.cells[base + rc]);
                if (proj_unsigned[rc]) {
                  cell.SetUnsigned();
                }
              }
              cur_chunk.columns[left_width + rc].AppendValue(
                  cell, cur_chunk.EnsureArena(), k);
            }
            if (cur_left_rows.size() == kColumnarChunkRows) {
              flush_chunk();
            }
          };
          auto process_batch = [&](size_t n, int64_t* pmins_out,
                                   int64_t* pmaxs_out) {
            const int64_t* const cells_ptr = batch.cells.data();
            const uint8_t* const nulls_ptr = batch.nulls.data();
            if (parts.size() == 2 && use_packed_key && pmins_out == nullptr) {
              const size_t p0 = check_order[0];
              const size_t p1 = check_order[1];
              const size_t ps0 = part_proj_slot[p0];
              const size_t ps1 = part_proj_slot[p1];
              const int64_t lo0 = parts[p0].lo;
              const uint64_t span0 = parts[p0].span;
              const bool all0 = parts[p0].all_set;
              const uint64_t* __restrict__ w0 = parts[p0].words.data();
              const unsigned sh0 = parts[p0].pack_shift;
              const int64_t lo1 = parts[p1].lo;
              const uint64_t span1 = parts[p1].span;
              const bool all1 = parts[p1].all_set;
              const uint64_t* __restrict__ w1 = parts[p1].words.data();
              const unsigned sh1 = parts[p1].pack_shift;
              const uint64_t* __restrict__ pb_ptr =
                  use_exact_pair_bits ? pair_bits.data() : nullptr;
              const uint32_t* __restrict__ pb_dense_ptr =
                  pair_outer_dense.empty() ? nullptr : pair_outer_dense.data();
              const uint64_t stride = pair_stride1;
              const bool pa_is_p0 = (pair_pa == p0);
              const uint64_t* __restrict__ pbloom_ptr =
                  (!use_exact_pair_bits && !packed_bloom.empty())
                      ? packed_bloom.data()
                      : nullptr;
              const size_t pbloom_mask = packed_bloom_mask;
              for (size_t r = 0; r < n; ++r) {
                const size_t base = r * right_width;
                if (nulls_ptr[base + ps0] != 0) {
                  continue;
                }
                const uint64_t o0 =
                    static_cast<uint64_t>(cells_ptr[base + ps0]) -
                    static_cast<uint64_t>(lo0);
                if (o0 > span0 ||
                    (!all0 && ((w0[o0 >> 6] >> (o0 & 63)) & 1ULL) == 0)) {
                  continue;
                }
                if (nulls_ptr[base + ps1] != 0) {
                  continue;
                }
                const uint64_t o1 =
                    static_cast<uint64_t>(cells_ptr[base + ps1]) -
                    static_cast<uint64_t>(lo1);
                if (o1 > span1 ||
                    (!all1 && ((w1[o1 >> 6] >> (o1 & 63)) & 1ULL) == 0)) {
                  continue;
                }
                if (pb_ptr != nullptr) {
                  const uint64_t oa = pa_is_p0 ? o0 : o1;
                  const uint64_t ob = pa_is_p0 ? o1 : o0;
                  const uint64_t row_idx =
                      pb_dense_ptr != nullptr ? pb_dense_ptr[oa] : oa;
                  if (row_idx == UINT32_MAX) {
                    continue;
                  }
                  const uint64_t bit = (row_idx * stride) + ob;
                  if (((pb_ptr[bit >> 6] >> (bit & 63)) & 1ULL) == 0) {
                    continue;
                  }
                }
                const uint64_t packed_u64 = (o0 << sh0) | (o1 << sh1);
                if (pbloom_ptr != nullptr) {
                  const uint64_t kh = mix_hash64(packed_u64);
                  const size_t b0 = static_cast<size_t>(kh) & pbloom_mask;
                  const size_t b1 = static_cast<size_t>(kh >> 24) & pbloom_mask;
                  if (((pbloom_ptr[b0 >> 6] >> (b0 & 63)) & 1ULL) == 0 ||
                      ((pbloom_ptr[b1 >> 6] >> (b1 & 63)) & 1ULL) == 0) {
                    continue;
                  }
                }
                const uint32_t* head =
                    pair_head.Find(static_cast<int64_t>(packed_u64));
                if (head == nullptr) {
                  continue;
                }
                for (uint32_t lr = *head; lr != UINT32_MAX;
                     lr = pair_next[lr]) {
                  emit_match(lr, r);
                }
              }
              return;
            }
            if (parts.size() == 3 && use_packed_key && pmins_out == nullptr) {
              const size_t p0 = check_order[0];
              const size_t p1 = check_order[1];
              const size_t p2 = check_order[2];
              const size_t ps0 = part_proj_slot[p0];
              const size_t ps1 = part_proj_slot[p1];
              const size_t ps2 = part_proj_slot[p2];
              const int64_t lo0 = parts[p0].lo;
              const uint64_t span0 = parts[p0].span;
              const bool all0 = parts[p0].all_set;
              const uint64_t* __restrict__ w0 = parts[p0].words.data();
              const unsigned sh0 = parts[p0].pack_shift;
              const int64_t lo1 = parts[p1].lo;
              const uint64_t span1 = parts[p1].span;
              const bool all1 = parts[p1].all_set;
              const uint64_t* __restrict__ w1 = parts[p1].words.data();
              const unsigned sh1 = parts[p1].pack_shift;
              const int64_t lo2 = parts[p2].lo;
              const uint64_t span2 = parts[p2].span;
              const bool all2 = parts[p2].all_set;
              const uint64_t* __restrict__ w2 = parts[p2].words.data();
              const unsigned sh2 = parts[p2].pack_shift;
              const uint64_t* __restrict__ pb_ptr =
                  use_exact_pair_bits ? pair_bits.data() : nullptr;
              const uint32_t* __restrict__ pb_dense_ptr =
                  pair_outer_dense.empty() ? nullptr : pair_outer_dense.data();
              const uint64_t stride = pair_stride1;
              const uint64_t* __restrict__ pbloom_ptr =
                  !packed_bloom.empty() ? packed_bloom.data() : nullptr;
              const size_t pbloom_mask = packed_bloom_mask;
              for (size_t r = 0; r < n; ++r) {
                const size_t base = r * right_width;
                if (nulls_ptr[base + ps0] != 0) {
                  continue;
                }
                const uint64_t o0 =
                    static_cast<uint64_t>(cells_ptr[base + ps0]) -
                    static_cast<uint64_t>(lo0);
                if (o0 > span0 ||
                    (!all0 && ((w0[o0 >> 6] >> (o0 & 63)) & 1ULL) == 0)) {
                  continue;
                }
                if (nulls_ptr[base + ps1] != 0) {
                  continue;
                }
                const uint64_t o1 =
                    static_cast<uint64_t>(cells_ptr[base + ps1]) -
                    static_cast<uint64_t>(lo1);
                if (o1 > span1 ||
                    (!all1 && ((w1[o1 >> 6] >> (o1 & 63)) & 1ULL) == 0)) {
                  continue;
                }
                if (nulls_ptr[base + ps2] != 0) {
                  continue;
                }
                const uint64_t o2 =
                    static_cast<uint64_t>(cells_ptr[base + ps2]) -
                    static_cast<uint64_t>(lo2);
                if (o2 > span2 ||
                    (!all2 && ((w2[o2 >> 6] >> (o2 & 63)) & 1ULL) == 0)) {
                  continue;
                }
                if (pb_ptr != nullptr) {
                  const uint64_t oa =
                      (pair_pa == p0) ? o0 : ((pair_pa == p1) ? o1 : o2);
                  const uint64_t ob =
                      (pair_pb == p0) ? o0 : ((pair_pb == p1) ? o1 : o2);
                  const uint64_t row_idx =
                      pb_dense_ptr != nullptr ? pb_dense_ptr[oa] : oa;
                  if (row_idx == UINT32_MAX) {
                    continue;
                  }
                  const uint64_t bit = (row_idx * stride) + ob;
                  if (((pb_ptr[bit >> 6] >> (bit & 63)) & 1ULL) == 0) {
                    continue;
                  }
                }
                const uint64_t packed_u64 =
                    (o0 << sh0) | (o1 << sh1) | (o2 << sh2);
                if (pbloom_ptr != nullptr) {
                  const uint64_t kh = mix_hash64(packed_u64);
                  const size_t b0 = static_cast<size_t>(kh) & pbloom_mask;
                  const size_t b1 = static_cast<size_t>(kh >> 24) & pbloom_mask;
                  if (((pbloom_ptr[b0 >> 6] >> (b0 & 63)) & 1ULL) == 0 ||
                      ((pbloom_ptr[b1 >> 6] >> (b1 & 63)) & 1ULL) == 0) {
                    continue;
                  }
                }
                const uint32_t* head =
                    pair_head.Find(static_cast<int64_t>(packed_u64));
                if (head == nullptr) {
                  continue;
                }
                for (uint32_t lr = *head; lr != UINT32_MAX;
                     lr = pair_next[lr]) {
                  emit_match(lr, r);
                }
              }
              return;
            }
            for (size_t r = 0; r < n; ++r) {
              const size_t base = r * right_width;
              if (pmins_out != nullptr) {
                for (size_t p = 0; p < parts.size(); ++p) {
                  const size_t ps = part_proj_slot[p];
                  if (nulls_ptr[base + ps] == 0) {
                    const int64_t bv = cells_ptr[base + ps];
                    if (bv < pmins_out[p]) pmins_out[p] = bv;
                    if (bv > pmaxs_out[p]) pmaxs_out[p] = bv;
                  }
                }
              }
              std::array<uint64_t, CompositeIntKey::kMaxParts> offs{};
              CompositeIntKey probe_key;
              probe_key.count = static_cast<uint8_t>(parts.size());
              bool key_ok = true;
              for (size_t idx = 0; idx < parts.size(); ++idx) {
                const size_t p = check_order[idx];
                const size_t ps = part_proj_slot[p];
                if (nulls_ptr[base + ps] != 0) {
                  key_ok = false;
                  break;
                }
                const int64_t value = cells_ptr[base + ps];
                const uint64_t off = static_cast<uint64_t>(value) -
                                     static_cast<uint64_t>(parts[p].lo);
                if (off > parts[p].span ||
                    (!parts[p].all_set &&
                     ((parts[p].words[off >> 6] >> (off & 63)) & 1ULL) == 0)) {
                  key_ok = false;
                  break;
                }
                offs[p] = off;
                probe_key.parts[p] = value;
              }
              if (!key_ok) {
                continue;
              }
              if (use_packed_key) {
                if (use_exact_pair_bits) {
                  const uint64_t opa = offs[pair_pa];
                  const uint64_t row_idx =
                      pair_outer_dense.empty() ? opa : pair_outer_dense[opa];
                  if (row_idx == UINT32_MAX) {
                    continue;
                  }
                  const uint64_t bit = (row_idx * pair_stride1) + offs[pair_pb];
                  if (((pair_bits[bit >> 6] >> (bit & 63)) & 1ULL) == 0) {
                    continue;
                  }
                }
                uint64_t packed_u64 = 0;
                for (size_t p = 0; p < parts.size(); ++p) {
                  packed_u64 |= offs[p] << parts[p].pack_shift;
                }
                if (!packed_bloom.empty()) {
                  const uint64_t kh = mix_hash64(packed_u64);
                  const size_t b0 = static_cast<size_t>(kh) & packed_bloom_mask;
                  const size_t b1 =
                      static_cast<size_t>(kh >> 24) & packed_bloom_mask;
                  if (((packed_bloom[b0 >> 6] >> (b0 & 63)) & 1ULL) == 0 ||
                      ((packed_bloom[b1 >> 6] >> (b1 & 63)) & 1ULL) == 0) {
                    continue;
                  }
                }
                const uint32_t* head =
                    pair_head.Find(static_cast<int64_t>(packed_u64));
                if (head == nullptr) {
                  continue;
                }
                for (uint32_t lr = *head; lr != UINT32_MAX;
                     lr = pair_next[lr]) {
                  emit_match(lr, r);
                }
              } else {
                const uint64_t kh =
                    mix_hash64(CompositeIntKeyHash{}(probe_key));
                const size_t b0 = static_cast<size_t>(kh) & bloom_mask;
                const size_t b1 = static_cast<size_t>(kh >> 24) & bloom_mask;
                if (((composite_bloom[b0 >> 6] >> (b0 & 63)) & 1ULL) == 0 ||
                    ((composite_bloom[b1 >> 6] >> (b1 & 63)) & 1ULL) == 0) {
                  continue;
                }
                const auto [first, last] =
                    buckets.equal_range_hashed(probe_key, kh);
                for (auto iter_b = first; iter_b != last; ++iter_b) {
                  emit_match(iter_b->second, r);
                }
              }
            }
          };
          if (build_all_zm) {
            Table::ScanMorsel single_page(1);
            const size_t total_pages = page_dir->page_ids.size();
            std::array<int64_t, CompositeIntKey::kMaxParts> pmins{};
            std::array<int64_t, CompositeIntKey::kMaxParts> pmaxs{};
            while (err.ok()) {
              const size_t p_start =
                  next_morsel.fetch_add(32, std::memory_order_relaxed);
              if (p_start >= total_pages) {
                break;
              }
              const size_t p_end = std::min(total_pages, p_start + 32);
              for (size_t pi = p_start; pi < p_end && err.ok(); ++pi) {
                single_page[0] = page_dir->page_ids[pi];
                for (size_t p = 0; p < parts.size(); ++p) {
                  pmins[p] = std::numeric_limits<int64_t>::max();
                  pmaxs[p] = std::numeric_limits<int64_t>::min();
                }
                Iterator iter = table.Value()->BeginMorselScan(
                    context.txn_, single_page, proj_opt, nullptr, std::nullopt,
                    nullptr, nullptr, ValueType::kNull);
                while (iter.IsValid()) {
                  batch.Reset();
                  const size_t n = iter.DecodeNextTypedBatch(&batch, 2048);
                  if (n == 0) {
                    break;
                  }
                  process_batch(n, pmins.data(), pmaxs.data());
                }
                const Status ist = iter.GetStatus();
                if (!ist.ok()) {
                  err = ist;
                  break;
                }
                for (size_t p = 0; p < parts.size(); ++p) {
                  (*building_all_bounds[p])[pi] = {pmins[p], pmaxs[p]};
                }
              }
            }
          } else {
            while (err.ok()) {
              const size_t m_start =
                  next_morsel.fetch_add(4, std::memory_order_relaxed);
              if (m_start >= morsels.size()) {
                break;
              }
              const size_t m_end = std::min(morsels.size(), m_start + 4);
              for (size_t mi = m_start; mi < m_end && err.ok(); ++mi) {
                Iterator iter = table.Value()->BeginMorselScan(
                    context.txn_, morsels[mi], proj_opt, nullptr, rf_full_col,
                    nullptr, rf_group.get(), rf_col_type);
                for (const StreamExtraRf& erf : extra_rfs) {
                  iter.AddExtraRuntimeFilter(erf.group.get(), erf.col,
                                             erf.type);
                }
                while (iter.IsValid()) {
                  batch.Reset();
                  const size_t n = iter.DecodeNextTypedBatch(&batch, 2048);
                  if (n == 0) {
                    break;
                  }
                  process_batch(n, nullptr, nullptr);
                }
                const Status ist = iter.GetStatus();
                if (!ist.ok()) {
                  err = ist;
                  break;
                }
              }
            }
          }
          flush_chunk();
        }));
        for (const Status& e : page_errors) {
          RETURN_IF_FAIL(e);
        }
        if (build_all_zm && page_dir != nullptr &&
            table.Value()->LayoutGeneration() == page_dir->layout_generation) {
          for (size_t p = 0; p < parts.size(); ++p) {
            page_dir->SetColIntBounds(parts[p].store_column,
                                      std::move(building_all_bounds[p]));
          }
        }
        Relation output(context.execution_runtime());
        output.schema = out_schema;
        auto out_col = std::make_shared<ColumnarTable>();
        for (auto& wc : worker_chunks) {
          for (ColumnarChunk& ch : wc) {
            out_col->AppendChunk(std::move(ch));
          }
        }
        if (out_col->total_rows > 0) {
          output.columnar = std::move(out_col);
        }
        if (::tinylamb::platform::GetEnv("TINYLAMB_GROUP_PHASES") != nullptr) {
          const auto t_m2 = std::chrono::steady_clock::now();
          const double bm_ms =
              std::chrono::duration<double, std::milli>(t_m1 - t_m0).count();
          const double scan_probe_ms =
              std::chrono::duration<double, std::milli>(t_m2 - t_m1).count();
          std::cerr << "[stream-morsel] table=" << probed.table
                    << " morsels=" << morsels.size() << " bm_ms=" << bm_ms
                    << " probe_ms=" << scan_probe_ms
                    << " left=" << total_left_rows
                    << " out=" << output.TotalRows() << "\n";
        }
        ++output.hash_joins;
        return std::optional<Relation>(std::move(output));
      }
      const Schema& table_schema = table.Value()->GetSchema();
      std::vector<slot_t> stage1_cols;
      std::vector<size_t> part_s1(parts.size(), 0);
      for (size_t p = 0; p < parts.size(); ++p) {
        auto it = std::ranges::find(stage1_cols, parts[p].store_column);
        if (it == stage1_cols.end()) {
          part_s1[p] = stage1_cols.size();
          stage1_cols.push_back(parts[p].store_column);
        } else {
          part_s1[p] = static_cast<size_t>(it - stage1_cols.begin());
        }
      }
      struct ColSrcLoc {
        bool in_stage1{false};
        size_t idx{0};
      };
      std::vector<slot_t> stage2_cols;
      auto locate_col = [&](slot_t sc) -> ColSrcLoc {
        auto it1 = std::ranges::find(stage1_cols, sc);
        if (it1 != stage1_cols.end()) {
          return ColSrcLoc{
              .in_stage1 = true,
              .idx = static_cast<size_t>(it1 - stage1_cols.begin())};
        }
        auto it2 = std::ranges::find(stage2_cols, sc);
        if (it2 == stage2_cols.end()) {
          const size_t idx = stage2_cols.size();
          stage2_cols.push_back(sc);
          return ColSrcLoc{.in_stage1 = false, .idx = idx};
        }
        return ColSrcLoc{.in_stage1 = false,
                         .idx = static_cast<size_t>(it2 - stage2_cols.begin())};
      };
      std::vector<ColSrcLoc> fast_res_locs(fast_residuals.size());
      for (size_t fp = 0; fp < fast_residuals.size(); ++fp) {
        fast_res_locs[fp] = locate_col(fast_residuals[fp].right_store_col);
      }
      std::vector<ColSrcLoc> proj_locs(right_width);
      for (size_t rc = 0; rc < right_width; ++rc) {
        proj_locs[rc] = locate_col(projections[index][rc]);
      }
      auto eval_fast_int_tlcs = eval_fast_int_stream;
      const uint32_t total_blocks = clean_tlcs->BlockCount();
      size_t tlcs_workers = std::min<size_t>(
          total_blocks,
          QueryParallelWorkerLimit(std::min<size_t>(8, total_blocks)));
      tlcs_workers = std::max<size_t>(1, tlcs_workers);
      std::atomic<uint32_t> next_block{0};
      std::vector<std::vector<ColumnarChunk>> worker_chunks(tlcs_workers);
      std::vector<Status> tlcs_errors(tlcs_workers, Status::kSuccess);
      std::vector<uint8_t> tlcs_bail(tlcs_workers, 0);
      RETURN_IF_FAIL(RunParallelTasks(tlcs_workers, [&](size_t w) {
        Status& err = tlcs_errors[w];
        uint8_t& bail = tlcs_bail[w];
        TlcsFile::DecodedBlock blk1;
        TlcsFile::DecodedBlock blk2;
        std::vector<uint8_t> io_scratch;
        struct CandPair {
          uint32_t left_row;
          uint32_t right_row;
        };
        std::vector<CandPair> block_cands;
        std::vector<uint32_t> cand_rows;
        std::vector<uint32_t> cur_left_rows;
        cur_left_rows.reserve(kColumnarChunkRows);
        ColumnarChunk cur_chunk;
        bool chunk_open = false;
        auto open_chunk = [&]() {
          cur_chunk = ColumnarChunk();
          cur_chunk.InitFromSchema(out_schema, kColumnarChunkRows);
          cur_chunk.EnsureArena();
          cur_left_rows.clear();
          chunk_open = true;
        };
        auto flush_chunk = [&]() {
          if (!chunk_open || cur_left_rows.empty()) {
            return;
          }
          const size_t n = cur_left_rows.size();
          cur_chunk.rows = n;
          auto left_gi = std::make_shared<ChunkGatherIndex>();
          left_gi->source_table = result.columnar;
          left_gi->src_chunks.resize(n);
          left_gi->src_rows.resize(n);
          for (size_t k = 0; k < n; ++k) {
            const LeftChunkLoc& loc = left_locs[cur_left_rows[k]];
            left_gi->src_chunks[k] = loc.chunk;
            left_gi->src_rows[k] = loc.row;
          }
          for (size_t c = 0; c < left_width; ++c) {
            if (left_col_uniform[c] != 0) {
              cur_chunk.columns[c].SetGathered(
                  left_gi, c, left_col_type[c], left_col_unsigned[c] != 0,
                  left_col_interval[c] != 0, true, left_col_has_null[c] != 0);
            } else {
              Value val;
              for (size_t k = 0; k < n; ++k) {
                const LeftChunkLoc& loc = left_locs[cur_left_rows[k]];
                lt.chunks[loc.chunk].columns[c].WriteValueInto(loc.row, &val);
                cur_chunk.columns[c].AppendValue(val, cur_chunk.EnsureArena(),
                                                 k);
              }
            }
          }
          worker_chunks[w].push_back(std::move(cur_chunk));
          cur_left_rows.clear();
          chunk_open = false;
        };
        Value cell;
        while (err.ok() && bail == 0) {
          const uint32_t b_start =
              next_block.fetch_add(16, std::memory_order_relaxed);
          if (b_start >= total_blocks) {
            break;
          }
          const uint32_t b_end = std::min(total_blocks, b_start + 16);
          for (uint32_t b = b_start; b < b_end && err.ok() && bail == 0; ++b) {
            bool block_miss = false;
            for (size_t idx = 0; idx < parts.size(); ++idx) {
              const size_t p = check_order[idx];
              int64_t bmin = 0;
              int64_t bmax = 0;
              if (!clean_tlcs->BlockIntBounds(b, parts[p].store_column, &bmin,
                                              &bmax)) {
                continue;
              }
              const int64_t lo = parts[p].lo;
              const uint64_t span = parts[p].span;
              if (bmax < lo || (bmin > lo && static_cast<uint64_t>(bmin) -
                                                     static_cast<uint64_t>(lo) >
                                                 span)) {
                block_miss = true;
                break;
              }
              const uint64_t off_lo =
                  bmin <= lo
                      ? 0ULL
                      : static_cast<uint64_t>(bmin) - static_cast<uint64_t>(lo);
              const uint64_t off_hi =
                  std::min<uint64_t>(span, static_cast<uint64_t>(bmax) -
                                               static_cast<uint64_t>(lo));
              if (off_hi - off_lo <= 65536ULL) {
                const auto w_lo = static_cast<size_t>(off_lo >> 6);
                const auto w_hi = static_cast<size_t>(off_hi >> 6);
                bool any_bit = false;
                if (w_lo == w_hi) {
                  uint64_t mask = (~uint64_t{0}) << (off_lo & 63);
                  mask &= (~uint64_t{0}) >>
                          (63U - static_cast<unsigned>(off_hi & 63));
                  any_bit = (parts[p].words[w_lo] & mask) != 0;
                } else {
                  const uint64_t head_mask = (~uint64_t{0}) << (off_lo & 63);
                  const uint64_t tail_mask =
                      (~uint64_t{0}) >>
                      (63U - static_cast<unsigned>(off_hi & 63));
                  if ((parts[p].words[w_lo] & head_mask) != 0 ||
                      (parts[p].words[w_hi] & tail_mask) != 0) {
                    any_bit = true;
                  } else {
                    for (size_t wi = w_lo + 1; wi < w_hi; ++wi) {
                      if (parts[p].words[wi] != 0) {
                        any_bit = true;
                        break;
                      }
                    }
                  }
                }
                if (!any_bit) {
                  block_miss = true;
                  break;
                }
              }
            }
            if (block_miss) {
              continue;
            }
            const Status st1 = clean_tlcs->DecodeBlockColumns(
                b, stage1_cols, table_schema, &blk1, true, &io_scratch);
            if (!st1.ok()) {
              err = st1;
              break;
            }
            const uint32_t nrows = blk1.row_count;
            if (nrows == 0) {
              continue;
            }
            std::array<const int64_t*, CompositeIntKey::kMaxParts> cells{};
            std::array<const TlcsFile::DecodedColumnChunk*,
                       CompositeIntKey::kMaxParts>
                key_cols{};
            for (size_t p = 0; p < parts.size(); ++p) {
              const auto& col = blk1.columns[part_s1[p]];
              if (col.int_cells.size() < nrows) {
                bail = 1;
                break;
              }
              key_cols[p] = &col;
              cells[p] = col.int_cells.data();
            }
            if (bail != 0) {
              break;
            }
            block_cands.clear();
            // Keep candidates in right-row order. Stage 2 gathers distinct
            // right rows in this order and maps candidates with one cursor.
            for (uint32_t r = 0; r < nrows; ++r) {
              std::array<uint64_t, CompositeIntKey::kMaxParts> offs{};
              CompositeIntKey probe_key;
              probe_key.count = static_cast<uint8_t>(parts.size());
              bool key_ok = true;
              for (size_t idx = 0; idx < parts.size(); ++idx) {
                const size_t p = check_order[idx];
                if (key_cols[p]->IsNull(r)) {
                  key_ok = false;
                  break;
                }
                const int64_t value = cells[p][r];
                const uint64_t off = static_cast<uint64_t>(value) -
                                     static_cast<uint64_t>(parts[p].lo);
                if (off > parts[p].span ||
                    (!parts[p].all_set &&
                     ((parts[p].words[off >> 6] >> (off & 63)) & 1ULL) == 0)) {
                  key_ok = false;
                  break;
                }
                offs[p] = off;
                probe_key.parts[p] = value;
              }
              if (!key_ok) {
                continue;
              }
              if (use_packed_key) {
                if (use_exact_pair_bits) {
                  const uint64_t opa = offs[pair_pa];
                  const uint64_t row_idx =
                      pair_outer_dense.empty() ? opa : pair_outer_dense[opa];
                  if (row_idx == UINT32_MAX) {
                    continue;
                  }
                  const uint64_t bit = (row_idx * pair_stride1) + offs[pair_pb];
                  if (((pair_bits[bit >> 6] >> (bit & 63)) & 1ULL) == 0) {
                    continue;
                  }
                }
                uint64_t packed_u64 = 0;
                for (size_t p = 0; p < parts.size(); ++p) {
                  packed_u64 |= offs[p] << parts[p].pack_shift;
                }
                if (!packed_bloom.empty()) {
                  const uint64_t kh = mix_hash64(packed_u64);
                  const size_t b0 = static_cast<size_t>(kh) & packed_bloom_mask;
                  const size_t b1 =
                      static_cast<size_t>(kh >> 24) & packed_bloom_mask;
                  if (((packed_bloom[b0 >> 6] >> (b0 & 63)) & 1ULL) == 0 ||
                      ((packed_bloom[b1 >> 6] >> (b1 & 63)) & 1ULL) == 0) {
                    continue;
                  }
                }
                const uint32_t* head =
                    pair_head.Find(static_cast<int64_t>(packed_u64));
                if (head == nullptr) {
                  continue;
                }
                for (uint32_t lr = *head; lr != UINT32_MAX;
                     lr = pair_next[lr]) {
                  block_cands.push_back(
                      CandPair{.left_row = lr, .right_row = r});
                }
              } else {
                const uint64_t kh =
                    mix_hash64(CompositeIntKeyHash{}(probe_key));
                const size_t b0 = static_cast<size_t>(kh) & bloom_mask;
                const size_t b1 = static_cast<size_t>(kh >> 24) & bloom_mask;
                if (((composite_bloom[b0 >> 6] >> (b0 & 63)) & 1ULL) == 0 ||
                    ((composite_bloom[b1 >> 6] >> (b1 & 63)) & 1ULL) == 0) {
                  continue;
                }
                const auto [first, last] =
                    buckets.equal_range_hashed(probe_key, kh);
                for (auto iter = first; iter != last; ++iter) {
                  block_cands.push_back(
                      CandPair{.left_row = iter->second, .right_row = r});
                }
              }
            }
            if (block_cands.empty()) {
              continue;
            }
            bool stage2_gathered = false;
            if (!stage2_cols.empty()) {
              cand_rows.clear();
              for (const CandPair& cp : block_cands) {
                if (cand_rows.empty() || cand_rows.back() != cp.right_row) {
                  cand_rows.push_back(cp.right_row);
                }
              }
              stage2_gathered = clean_tlcs->TryGatherMmapFixedColumns(
                  b, stage2_cols, cand_rows, table_schema, &blk2);
              if (!stage2_gathered) {
                const Status st2 = clean_tlcs->DecodeBlockColumns(
                    b, stage2_cols, table_schema, &blk2, true, &io_scratch);
                if (!st2.ok()) {
                  err = st2;
                  break;
                }
              }
            }
            size_t g_idx = 0;
            for (const CandPair& cp : block_cands) {
              if (stage2_gathered) {
                while (g_idx + 1 < cand_rows.size() &&
                       cand_rows[g_idx] < cp.right_row) {
                  ++g_idx;
                }
              }
              const uint32_t s2_row =
                  stage2_gathered ? static_cast<uint32_t>(g_idx) : cp.right_row;
              bool pass = true;
              for (size_t fp = 0; fp < fast_residuals.size(); ++fp) {
                if (left_fast_nulls[fp][cp.left_row] != 0) {
                  pass = false;
                  break;
                }
                const ColSrcLoc loc = fast_res_locs[fp];
                const auto& rcol = loc.in_stage1 ? blk1.columns[loc.idx]
                                                 : blk2.columns[loc.idx];
                const uint32_t rpos = loc.in_stage1 ? cp.right_row : s2_row;
                if (rcol.IsNull(rpos) || rpos >= rcol.int_cells.size()) {
                  pass = false;
                  break;
                }
                if (!eval_fast_int_tlcs(left_fast_vals[fp][cp.left_row],
                                        rcol.int_cells[rpos],
                                        fast_residuals[fp].op)) {
                  pass = false;
                  break;
                }
              }
              if (!pass) {
                continue;
              }
              if (!chunk_open) {
                open_chunk();
              }
              const size_t k = cur_left_rows.size();
              cur_left_rows.push_back(cp.left_row);
              for (size_t rc = 0; rc < right_width; ++rc) {
                const ColSrcLoc loc = proj_locs[rc];
                const auto& rcol = loc.in_stage1 ? blk1.columns[loc.idx]
                                                 : blk2.columns[loc.idx];
                const uint32_t rpos = loc.in_stage1 ? cp.right_row : s2_row;
                if (rcol.IsNull(rpos)) {
                  cell = Value();
                } else if (rcol.type == ValueType::kDouble) {
                  cell.SetDouble(std::bit_cast<double>(rcol.int_cells[rpos]));
                } else if (rcol.type == ValueType::kDate) {
                  cell = Value::DateFromDays(rcol.int_cells[rpos]);
                } else if (rcol.type == ValueType::kVarChar) {
                  cell.CopyFromStringView(rcol.VarcharAt(rpos));
                } else {
                  cell = Value(rcol.int_cells[rpos]);
                  if (rcol.is_unsigned) {
                    cell.SetUnsigned();
                  }
                }
                cur_chunk.columns[left_width + rc].AppendValue(
                    cell, cur_chunk.EnsureArena(), k);
              }
              if (cur_left_rows.size() == kColumnarChunkRows) {
                flush_chunk();
              }
            }
          }
        }
        flush_chunk();
      }));
      for (const Status& e : tlcs_errors) {
        RETURN_IF_FAIL(e);
      }
      if (std::ranges::any_of(tlcs_bail, [](uint8_t f) { return f != 0; })) {
        return std::optional<Relation>{};
      }
      Relation output(context.execution_runtime());
      output.schema = out_schema;
      auto out_col = std::make_shared<ColumnarTable>();
      for (auto& wc : worker_chunks) {
        for (ColumnarChunk& ch : wc) {
          out_col->AppendChunk(std::move(ch));
        }
      }
      if (out_col->total_rows > 0) {
        output.columnar = std::move(out_col);
      }
      if (::tinylamb::platform::GetEnv("TINYLAMB_GROUP_PHASES") != nullptr) {
        std::cerr << "[stream-tlcs] table=" << probed.table
                  << " left=" << total_left_rows
                  << " out=" << output.TotalRows() << "\n";
      }
      ++output.hash_joins;
      return std::optional<Relation>(std::move(output));
    }
    const CompiledScanFilter residual_filter =
        all_residuals_fast ? CompiledScanFilter{}
                           : CompileScanFilter(residual, out_schema);
    const CompiledScanFilter local_filter =
        CompileScanFilter(ordered_local[index], projected);
    const size_t nblocks = store->blocks.size();
    size_t workers = std::min<size_t>(
        nblocks, QueryParallelWorkerLimit(std::min<size_t>(8, nblocks)));
    workers = std::max<size_t>(1, workers);
    struct StreamMatch {
      uint32_t left_row;
      uint32_t block;
      uint32_t row;
    };
    std::vector<std::vector<StreamMatch>> worker_matches(emit_columnar ? workers
                                                                       : 0);
    std::vector<Relation> shards;
    if (!emit_columnar) {
      shards.reserve(workers);
      for (size_t w = 0; w < workers; ++w) {
        shards.emplace_back(context.execution_runtime());
        shards.back().schema = out_schema;
      }
    }
    std::vector<Status> worker_errors(workers, Status::kSuccess);
    // A stale/mixed block layout is a reason to abandon this opportunistic
    // stream probe and let LoadSource handle it, not a query-fatal error.
    std::vector<uint8_t> worker_layout_bail(workers, 0);
    const auto cell_value = [](const LSTableColumnBlock& column, uint32_t row) {
      if (column.has_null && column.IsNull(row)) {
        return Value();
      }
      if (column.type == ValueType::kDouble) {
        return Value(std::bit_cast<double>(column.int_cells[row]));
      }
      if (column.type == ValueType::kDate) {
        return Value::DateFromDays(column.int_cells[row]);
      }
      if (column.type == ValueType::kVarChar) {
        return Value(std::string(column.VarcharAt(row)));
      }
      Value v(column.int_cells[row]);
      if (column.is_unsigned) {
        v.SetUnsigned();
      }
      return v;
    };
    auto eval_fast_int_pred = [](int64_t l, int64_t r,
                                 BinaryOperation op) -> bool {
      switch (op) {
        case BinaryOperation::kEquals:
          return l == r;
        case BinaryOperation::kNotEquals:
          return l != r;
        case BinaryOperation::kLessThan:
          return l < r;
        case BinaryOperation::kLessThanEquals:
          return l <= r;
        case BinaryOperation::kGreaterThan:
          return l > r;
        case BinaryOperation::kGreaterThanEquals:
          return l >= r;
        default:
          return false;
      }
    };
    const auto block_col_has_any_null = [](const LSTableColumnBlock& col,
                                           uint32_t nrows) -> bool {
      if (!col.has_null) {
        return false;
      }
      const std::span<const uint8_t> ns =
          !col.nulls_view.empty() ? col.nulls_view
                                  : std::span<const uint8_t>(col.nulls);
      const size_t n = std::min<size_t>(nrows, ns.size());
      if (n == 0) {
        return false;
      }
      size_t i = 0;
      uint64_t acc = 0;
      for (; i + 8 <= n; i += 8) {
        uint64_t w = 0;
        std::memcpy(&w, ns.data() + i, 8);
        acc |= w;
      }
      for (; i < n; ++i) {
        acc |= ns[i];
      }
      return acc != 0;
    };
    std::vector<slot_t> part_store_cols;
    part_store_cols.reserve(parts.size());
    for (const auto& part : parts) {
      if (std::ranges::find(part_store_cols, part.store_column) ==
          part_store_cols.end()) {
        part_store_cols.push_back(part.store_column);
      }
    }
    std::vector<slot_t> payload_store_cols;
    payload_store_cols.reserve(fast_residuals.size() +
                               (emit_columnar ? 0 : right_width));
    for (const auto& fr : fast_residuals) {
      if (std::ranges::find(payload_store_cols, fr.right_store_col) ==
          payload_store_cols.end()) {
        payload_store_cols.push_back(fr.right_store_col);
      }
    }
    if (!emit_columnar) {
      for (size_t c = 0; c < right_width; ++c) {
        const slot_t sc = projections[index][c];
        if (std::ranges::find(payload_store_cols, sc) ==
            payload_store_cols.end()) {
          payload_store_cols.push_back(sc);
        }
      }
    }
    RETURN_IF_FAIL(RunParallelTasks(workers, [&](size_t worker) {
      const size_t begin = nblocks * worker / workers;
      const size_t end = nblocks * (worker + 1) / workers;
      Status& filter_error = worker_errors[worker];
      uint8_t& layout_bail = worker_layout_bail[worker];
      Row right_row;
      Row combined;
      if (!emit_columnar) {
        right_row.values_.resize(right_width);
        combined.values_.resize(left_width + right_width);
      }
      std::vector<const LSTableColumnBlock*> fast_rcols(fast_residuals.size(),
                                                        nullptr);
      std::vector<int64_t> fast_rvals(fast_residuals.size(), 0);
      LSTableStore::LazyBlockScratch key_scratch;
      LSTableStore::LazyBlockScratch payload_scratch;
      for (size_t b = begin; b < end && filter_error.ok() && !layout_bail;
           ++b) {
        const LSTablePageBlock& meta_block = store->blocks[b];
        const uint32_t nrows = meta_block.row_count;
        if (nrows == 0) {
          continue;
        }
        for (auto& part : parts) {
          if (part.store_column >= meta_block.columns.size()) {
            layout_bail = 1;
            break;
          }
        }
        for (size_t fp = 0; !layout_bail && fp < fast_residuals.size(); ++fp) {
          if (fast_residuals[fp].right_store_col >= meta_block.columns.size()) {
            layout_bail = 1;
            break;
          }
        }
        if (layout_bail != 0) {
          break;
        }
        bool block_miss = false;
        for (size_t idx = 0; idx < parts.size(); ++idx) {
          const size_t p = check_order[idx];
          const LSTableColumnBlock& column =
              meta_block.columns[parts[p].store_column];
          if (!column.has_min_max || column.max_int < column.min_int) {
            continue;
          }
          const int64_t lo = parts[p].lo;
          const uint64_t span = parts[p].span;
          if (column.max_int < lo ||
              (column.min_int > lo && static_cast<uint64_t>(column.min_int) -
                                              static_cast<uint64_t>(lo) >
                                          span)) {
            block_miss = true;
            break;
          }
          const uint64_t off_lo = column.min_int <= lo
                                      ? 0ULL
                                      : static_cast<uint64_t>(column.min_int) -
                                            static_cast<uint64_t>(lo);
          const uint64_t off_hi =
              std::min<uint64_t>(span, static_cast<uint64_t>(column.max_int) -
                                           static_cast<uint64_t>(lo));
          if (off_hi - off_lo <= 65536ULL) {
            const auto w_lo = static_cast<size_t>(off_lo >> 6);
            const auto w_hi = static_cast<size_t>(off_hi >> 6);
            bool any_bit = false;
            if (w_lo == w_hi) {
              uint64_t mask = (~uint64_t{0}) << (off_lo & 63);
              mask &=
                  (~uint64_t{0}) >> (63U - static_cast<unsigned>(off_hi & 63));
              any_bit = (parts[p].words[w_lo] & mask) != 0;
            } else {
              const uint64_t head_mask = (~uint64_t{0}) << (off_lo & 63);
              const uint64_t tail_mask =
                  (~uint64_t{0}) >> (63U - static_cast<unsigned>(off_hi & 63));
              if ((parts[p].words[w_lo] & head_mask) != 0 ||
                  (parts[p].words[w_hi] & tail_mask) != 0) {
                any_bit = true;
              } else {
                for (size_t wi = w_lo + 1; wi < w_hi; ++wi) {
                  if (parts[p].words[wi] != 0) {
                    any_bit = true;
                    break;
                  }
                }
              }
            }
            if (!any_bit) {
              block_miss = true;
              break;
            }
          }
        }
        if (block_miss) {
          continue;
        }
        key_scratch.transient.clear();
        const LSTablePageBlock* key_blk =
            store->MaterializeBlockView(b, &key_scratch, part_store_cols);
        if (key_blk == nullptr) {
          filter_error = Status::kIOError;
          break;
        }
        std::array<const int64_t*, CompositeIntKey::kMaxParts> cells{};
        std::array<const LSTableColumnBlock*, CompositeIntKey::kMaxParts>
            key_cols{};
        bool columns_ok = true;
        bool all_keys_nonnull = true;
        for (size_t p = 0; p < parts.size(); ++p) {
          key_cols[p] = &key_blk->columns[parts[p].store_column];
          cells[p] = key_cols[p]->int_cells.data();
          if (cells[p] == nullptr) {
            columns_ok = false;
            break;
          }
          if (block_col_has_any_null(*key_cols[p], nrows)) {
            all_keys_nonnull = false;
          }
        }
        const LSTablePageBlock* payload_blk =
            store->is_block_lazy ? nullptr : &meta_block;
        bool payload_ready =
            !store->is_block_lazy || payload_store_cols.empty();
        bool all_fast_nonnull = true;
        if (!store->is_block_lazy && columns_ok) {
          for (size_t fp = 0; fp < fast_residuals.size(); ++fp) {
            const slot_t sc = fast_residuals[fp].right_store_col;
            fast_rcols[fp] = &meta_block.columns[sc];
            if (fast_rcols[fp]->int_cells.empty()) {
              columns_ok = false;
              break;
            }
            if (block_col_has_any_null(*fast_rcols[fp], nrows)) {
              all_fast_nonnull = false;
            }
          }
        }
        if (!columns_ok) {
          layout_bail = 1;
          break;
        }
        auto emit_match_list = [&](uint32_t r, const auto& for_each_lidx) {
          if (!payload_ready) {
            payload_scratch.transient.clear();
            payload_blk = store->MaterializeBlockView(b, &payload_scratch,
                                                      payload_store_cols);
            if (payload_blk == nullptr) {
              filter_error = Status::kIOError;
              return;
            }
            all_fast_nonnull = true;
            for (size_t fp = 0; fp < fast_residuals.size(); ++fp) {
              fast_rcols[fp] =
                  &payload_blk->columns[fast_residuals[fp].right_store_col];
              if (fast_rcols[fp]->int_cells.empty()) {
                layout_bail = 1;
                return;
              }
              if (block_col_has_any_null(*fast_rcols[fp], nrows)) {
                all_fast_nonnull = false;
              }
            }
            payload_ready = true;
          }
          if (!all_fast_nonnull) {
            for (size_t fp = 0; fp < fast_residuals.size(); ++fp) {
              const LSTableColumnBlock* rcol = fast_rcols[fp];
              if (rcol->has_null && rcol->IsNull(r)) {
                return;
              }
              fast_rvals[fp] = rcol->int_cells[r];
            }
          } else {
            for (size_t fp = 0; fp < fast_residuals.size(); ++fp) {
              fast_rvals[fp] = fast_rcols[fp]->int_cells[r];
            }
          }
          if (emit_columnar) {
            auto& out_vec = worker_matches[worker];
            for_each_lidx([&](uint32_t lidx) {
              bool pair_ok = true;
              for (size_t fp = 0; fp < fast_residuals.size(); ++fp) {
                if (left_fast_nulls[fp][lidx] != 0 ||
                    !eval_fast_int_pred(left_fast_vals[fp][lidx],
                                        fast_rvals[fp],
                                        fast_residuals[fp].op)) {
                  pair_ok = false;
                  break;
                }
              }
              if (pair_ok) {
                out_vec.push_back(StreamMatch{
                    .left_row = lidx,
                    .block = static_cast<uint32_t>(b),
                    .row = r,
                });
              }
            });
            return;
          }
          bool right_built = false;
          auto ensure_right_built = [&]() -> bool {
            if (right_built) {
              return true;
            }
            for (size_t c = 0; c < right_width; ++c) {
              const slot_t store_column = projections[index][c];
              if (payload_blk == nullptr ||
                  store_column >= payload_blk->columns.size()) {
                layout_bail = 1;
                return false;
              }
              right_row.values_[c] =
                  cell_value(payload_blk->columns[store_column], r);
            }
            if (!ordered_local[index].empty() &&
                !MatchScanFilter(right_row, projected, local_filter, outer,
                                 context, ctes, &filter_error)) {
              return false;
            }
            right_built = true;
            return true;
          };
          if (fast_residuals.empty() && !ordered_local[index].empty()) {
            if (!ensure_right_built()) {
              return;
            }
          }
          for_each_lidx([&](uint32_t lidx) {
            if (!filter_error.ok()) {
              return;
            }
            const Row& left_r = left_rows[lidx];
            for (size_t fp = 0; fp < fast_residuals.size(); ++fp) {
              const Value& lv = left_r[fast_residuals[fp].left_col];
              if (lv.IsNull() ||
                  !eval_fast_int_pred(lv.value.int_value, fast_rvals[fp],
                                      fast_residuals[fp].op)) {
                return;
              }
            }
            if (!ensure_right_built()) {
              return;
            }
            if (!all_residuals_fast && !residual.empty()) {
              for (size_t c = 0; c < left_width; ++c) {
                combined.values_[c] = left_r.values_[c];
              }
              for (size_t c = 0; c < right_width; ++c) {
                combined.values_[left_width + c] = right_row.values_[c];
              }
              if (!MatchScanFilter(combined, out_schema, residual_filter, outer,
                                   context, ctes, &filter_error)) {
                return;
              }
            }
            Row out_row;
            out_row.values_.reserve(left_width + right_width);
            out_row.values_.insert(out_row.values_.end(),
                                   left_r.values_.begin(),
                                   left_r.values_.end());
            out_row.values_.insert(out_row.values_.end(),
                                   right_row.values_.begin(),
                                   right_row.values_.end());
            shards[worker].AddRow(std::move(out_row));
          });
        };
        auto probe_candidate_row = [&](uint32_t r,
                                       const CompositeIntKey& probe_key) {
          const uint64_t kh = mix_hash64(CompositeIntKeyHash{}(probe_key));
          const size_t b0 = static_cast<size_t>(kh) & bloom_mask;
          const size_t b1 = static_cast<size_t>(kh >> 24) & bloom_mask;
          if (((composite_bloom[b0 >> 6] >> (b0 & 63)) &
               (composite_bloom[b1 >> 6] >> (b1 & 63)) & 1ULL) == 0) {
            return;
          }
          const auto [match_begin, match_end] =
              buckets.equal_range_hashed(probe_key, kh);
          if (match_begin == match_end) {
            return;
          }
          emit_match_list(r, [&](auto&& fn) {
            for (auto it = match_begin; it != match_end; ++it) {
              fn(it->second);
            }
          });
        };
        auto probe_packed_row = [&](uint32_t r, uint64_t packed_u64) {
          if (!packed_bloom.empty()) {
            const uint64_t kh = mix_hash64(packed_u64);
            const size_t b0 = static_cast<size_t>(kh) & packed_bloom_mask;
            const size_t b1 = static_cast<size_t>(kh >> 24) & packed_bloom_mask;
            if (((packed_bloom[b0 >> 6] >> (b0 & 63)) & 1ULL) == 0 ||
                ((packed_bloom[b1 >> 6] >> (b1 & 63)) & 1ULL) == 0) {
              return;
            }
          }
          const uint32_t* head =
              pair_head.Find(static_cast<int64_t>(packed_u64));
          if (head == nullptr) {
            return;
          }
          emit_match_list(r, [&](auto&& fn) {
            for (uint32_t lidx = *head; lidx != UINT32_MAX;
                 lidx = pair_next[lidx]) {
              fn(lidx);
            }
          });
        };

        if (all_keys_nonnull && use_packed_key && parts.size() == 2) {
          const size_t p0 = check_order[0];
          const size_t p1 = check_order[1];
          const int64_t* __restrict__ c0 = cells[p0];
          const int64_t* __restrict__ c1 = cells[p1];
          const int64_t lo0 = parts[p0].lo;
          const uint64_t span0 = parts[p0].span;
          const bool all0 = parts[p0].all_set;
          const uint64_t* __restrict__ w0 = parts[p0].words.data();
          const unsigned sh0 = parts[p0].pack_shift;
          const int64_t lo1 = parts[p1].lo;
          const uint64_t span1 = parts[p1].span;
          const bool all1 = parts[p1].all_set;
          const uint64_t* __restrict__ w1 = parts[p1].words.data();
          const unsigned sh1 = parts[p1].pack_shift;
          const uint64_t* __restrict__ pb_ptr =
              use_exact_pair_bits ? pair_bits.data() : nullptr;
          const uint32_t* __restrict__ pb_dense_ptr =
              pair_outer_dense.empty() ? nullptr : pair_outer_dense.data();
          const uint64_t stride = pair_stride1;
          const bool pa_is_p0 = (pair_pa == p0);
          for (uint32_t r = 0; r < nrows && filter_error.ok() && !layout_bail;
               ++r) {
            const uint64_t o0 =
                static_cast<uint64_t>(c0[r]) - static_cast<uint64_t>(lo0);
            if (o0 > span0 ||
                (!all0 && ((w0[o0 >> 6] >> (o0 & 63)) & 1ULL) == 0)) {
              continue;
            }
            const uint64_t o1 =
                static_cast<uint64_t>(c1[r]) - static_cast<uint64_t>(lo1);
            if (o1 > span1 ||
                (!all1 && ((w1[o1 >> 6] >> (o1 & 63)) & 1ULL) == 0)) {
              continue;
            }
            if (pb_ptr != nullptr) {
              const uint64_t oa = pa_is_p0 ? o0 : o1;
              const uint64_t ob = pa_is_p0 ? o1 : o0;
              const uint64_t row_idx =
                  pb_dense_ptr != nullptr ? pb_dense_ptr[oa] : oa;
              if (row_idx == UINT32_MAX) {
                continue;
              }
              const uint64_t bit = (row_idx * stride) + ob;
              if (((pb_ptr[bit >> 6] >> (bit & 63)) & 1ULL) == 0) {
                continue;
              }
            }
            probe_packed_row(r, (o0 << sh0) | (o1 << sh1));
          }
        } else if (all_keys_nonnull && use_packed_key && parts.size() == 3) {
          const size_t p0 = check_order[0];
          const size_t p1 = check_order[1];
          const size_t p2 = check_order[2];
          const int64_t* __restrict__ c0 = cells[p0];
          const int64_t* __restrict__ c1 = cells[p1];
          const int64_t* __restrict__ c2 = cells[p2];
          const int64_t lo0 = parts[p0].lo;
          const uint64_t span0 = parts[p0].span;
          const bool all0 = parts[p0].all_set;
          const uint64_t* __restrict__ w0 = parts[p0].words.data();
          const unsigned sh0 = parts[p0].pack_shift;
          const int64_t lo1 = parts[p1].lo;
          const uint64_t span1 = parts[p1].span;
          const bool all1 = parts[p1].all_set;
          const uint64_t* __restrict__ w1 = parts[p1].words.data();
          const unsigned sh1 = parts[p1].pack_shift;
          const int64_t lo2 = parts[p2].lo;
          const uint64_t span2 = parts[p2].span;
          const bool all2 = parts[p2].all_set;
          const uint64_t* __restrict__ w2 = parts[p2].words.data();
          const unsigned sh2 = parts[p2].pack_shift;
          const uint64_t* __restrict__ pb_ptr =
              use_exact_pair_bits ? pair_bits.data() : nullptr;
          const uint32_t* __restrict__ pb_dense_ptr =
              pair_outer_dense.empty() ? nullptr : pair_outer_dense.data();
          const uint64_t stride = pair_stride1;
          const bool pair_is_01 =
              (pb_ptr != nullptr && ((pair_pa == p0 && pair_pb == p1) ||
                                     (pair_pa == p1 && pair_pb == p0)));
          const bool pa_is_p0 = (pair_pa == p0);
          const bool pa_is_p1 = (pair_pa == p1);
          const bool pb_is_p0 = (pair_pb == p0);
          const bool pb_is_p1 = (pair_pb == p1);
          for (uint32_t r = 0; r < nrows && filter_error.ok() && !layout_bail;
               ++r) {
            const uint64_t o0 =
                static_cast<uint64_t>(c0[r]) - static_cast<uint64_t>(lo0);
            if (o0 > span0 ||
                (!all0 && ((w0[o0 >> 6] >> (o0 & 63)) & 1ULL) == 0)) {
              continue;
            }
            const uint64_t o1 =
                static_cast<uint64_t>(c1[r]) - static_cast<uint64_t>(lo1);
            if (o1 > span1 ||
                (!all1 && ((w1[o1 >> 6] >> (o1 & 63)) & 1ULL) == 0)) {
              continue;
            }
            if (pair_is_01) {
              const uint64_t oa = pa_is_p0 ? o0 : o1;
              const uint64_t ob = pa_is_p0 ? o1 : o0;
              const uint64_t row_idx =
                  pb_dense_ptr != nullptr ? pb_dense_ptr[oa] : oa;
              if (row_idx == UINT32_MAX) {
                continue;
              }
              const uint64_t bit = (row_idx * stride) + ob;
              if (((pb_ptr[bit >> 6] >> (bit & 63)) & 1ULL) == 0) {
                continue;
              }
            }
            const uint64_t o2 =
                static_cast<uint64_t>(c2[r]) - static_cast<uint64_t>(lo2);
            if (o2 > span2 ||
                (!all2 && ((w2[o2 >> 6] >> (o2 & 63)) & 1ULL) == 0)) {
              continue;
            }
            if (pb_ptr != nullptr && !pair_is_01) {
              const uint64_t oa = pa_is_p0 ? o0 : (pa_is_p1 ? o1 : o2);
              const uint64_t ob = pb_is_p0 ? o0 : (pb_is_p1 ? o1 : o2);
              const uint64_t row_idx =
                  pb_dense_ptr != nullptr ? pb_dense_ptr[oa] : oa;
              if (row_idx == UINT32_MAX) {
                continue;
              }
              const uint64_t bit = (row_idx * stride) + ob;
              if (((pb_ptr[bit >> 6] >> (bit & 63)) & 1ULL) == 0) {
                continue;
              }
            }
            probe_packed_row(r, (o0 << sh0) | (o1 << sh1) | (o2 << sh2));
          }
        } else if (all_keys_nonnull && parts.size() == 1) {
          const int64_t* __restrict__ c0 = cells[0];
          const int64_t lo0 = parts[0].lo;
          const uint64_t span0 = parts[0].span;
          const uint64_t* __restrict__ w0 = parts[0].words.data();
          for (uint32_t r = 0; r < nrows && filter_error.ok() && !layout_bail;
               ++r) {
            const int64_t v0 = c0[r];
            const uint64_t o0 =
                static_cast<uint64_t>(v0) - static_cast<uint64_t>(lo0);
            if (o0 > span0 || ((w0[o0 >> 6] >> (o0 & 63)) & 1ULL) == 0) {
              continue;
            }
            CompositeIntKey probe_key;
            probe_key.count = 1;
            probe_key.parts[0] = v0;
            probe_candidate_row(r, probe_key);
          }
        } else {
          for (uint32_t r = 0; r < nrows && filter_error.ok() && !layout_bail;
               ++r) {
            std::array<uint64_t, CompositeIntKey::kMaxParts> offs{};
            CompositeIntKey probe_key;
            probe_key.count = static_cast<uint8_t>(parts.size());
            bool key_ok = true;
            for (size_t idx = 0; idx < parts.size(); ++idx) {
              const size_t p = check_order[idx];
              if (key_cols[p]->has_null && key_cols[p]->IsNull(r)) {
                key_ok = false;
                break;
              }
              const int64_t value = cells[p][r];
              const uint64_t off = static_cast<uint64_t>(value) -
                                   static_cast<uint64_t>(parts[p].lo);
              if (off > parts[p].span ||
                  (!parts[p].all_set &&
                   ((parts[p].words[off >> 6] >> (off & 63)) & 1ULL) == 0)) {
                key_ok = false;
                break;
              }
              offs[p] = off;
              probe_key.parts[p] = value;
            }
            if (key_ok) {
              if (use_packed_key) {
                if (use_exact_pair_bits) {
                  const uint64_t opa = offs[pair_pa];
                  const uint64_t row_idx =
                      pair_outer_dense.empty() ? opa : pair_outer_dense[opa];
                  if (row_idx == UINT32_MAX) {
                    continue;
                  }
                  const uint64_t bit = (row_idx * pair_stride1) + offs[pair_pb];
                  if (((pair_bits[bit >> 6] >> (bit & 63)) & 1ULL) == 0) {
                    continue;
                  }
                }
                uint64_t packed_u64 = 0;
                for (size_t p = 0; p < parts.size(); ++p) {
                  packed_u64 |= offs[p] << parts[p].pack_shift;
                }
                probe_packed_row(r, packed_u64);
              } else {
                probe_candidate_row(r, probe_key);
              }
            }
          }
        }
      }
    }));
    for (const Status& worker_error : worker_errors) {
      RETURN_IF_FAIL(worker_error);
    }
    if (std::ranges::any_of(worker_layout_bail,
                            [](uint8_t flag) { return flag != 0; })) {
      return std::optional<Relation>{};
    }
    Relation output(context.execution_runtime());
    output.schema = out_schema;
    if (emit_columnar) {
      size_t produced = 0;
      for (const auto& wm : worker_matches) {
        produced += wm.size();
      }
      if (produced == 0) {
        return std::optional<Relation>{};
      }
      const ColumnarTable& lt = *result.columnar;
      std::vector<ValueType> left_col_type(left_width);
      std::vector<uint8_t> left_col_unsigned(left_width, 0);
      std::vector<uint8_t> left_col_interval(left_width, 0);
      std::vector<uint8_t> left_col_uniform(left_width, 1);
      std::vector<uint8_t> left_col_has_null(left_width, 0);
      for (size_t c = 0; c < left_width; ++c) {
        const Column& sc = result.schema.GetColumn(c);
        left_col_type[c] = sc.Type();
        left_col_unsigned[c] = sc.IsUnsigned() ? 1 : 0;
        left_col_interval[c] = sc.IsInterval() ? 1 : 0;
        if (sc.Type() == ValueType::kNull) {
          left_col_uniform[c] = 0;
        }
        for (const ColumnarChunk& fc : lt.chunks) {
          if (fc.rows == 0) {
            continue;
          }
          const ColumnarColumn& lc = fc.columns[c];
          if (lc.has_null || lc.type == ValueType::kNull) {
            left_col_has_null[c] = 1;
          }
          if (!lc.is_uniform ||
              (lc.type != ValueType::kNull && lc.type != sc.Type())) {
            left_col_uniform[c] = 0;
          }
        }
      }
      std::atomic<bool> chunk_io_error{false};
      auto build_chunk_from_matches =
          [&](std::span<const StreamMatch> ms) -> ColumnarChunk {
        ColumnarChunk chunk;
        chunk.InitFromSchema(out_schema, ms.size());
        chunk.EnsureArena();
        chunk.rows = ms.size();
        auto left_gi = std::make_shared<ChunkGatherIndex>();
        left_gi->source_table = result.columnar;
        left_gi->src_chunks.resize(ms.size());
        left_gi->src_rows.resize(ms.size());
        for (size_t k = 0; k < ms.size(); ++k) {
          const LeftChunkLoc& loc = left_locs[ms[k].left_row];
          left_gi->src_chunks[k] = loc.chunk;
          left_gi->src_rows[k] = loc.row;
        }
        for (size_t c = 0; c < left_width; ++c) {
          if (left_col_uniform[c] != 0) {
            chunk.columns[c].SetGathered(
                left_gi, c, left_col_type[c], left_col_unsigned[c] != 0,
                left_col_interval[c] != 0, true, left_col_has_null[c] != 0);
          } else {
            Value val;
            for (size_t k = 0; k < ms.size(); ++k) {
              const LeftChunkLoc& loc = left_locs[ms[k].left_row];
              lt.chunks[loc.chunk].columns[c].WriteValueInto(loc.row, &val);
              chunk.columns[c].AppendValue(val, chunk.EnsureArena(), k);
            }
          }
        }
        for (size_t rc = 0; rc < right_width; ++rc) {
          const slot_t store_col = projections[index][rc];
          ColumnarColumn& dst = chunk.columns[left_width + rc];
          // T4178: reuse one scratch cell per output column. The old
          // shape built a fresh Value -- including an owned std::string
          // for every VARCHAR cell -- per matched row, only to copy it
          // into the arena and drop it; CopyFromStringView keeps the scratch
          // buffer warm across rows.
          std::vector<std::unique_ptr<LSTableLazyDecodedBlock>> mat_transient;
          uint32_t last_blk = UINT32_MAX;
          const LSTableColumnBlock* last_col = nullptr;
          Value cell;
          for (size_t k = 0; k < ms.size(); ++k) {
            const StreamMatch& match = ms[k];
            if (match.block != last_blk) {
              last_col = store->ResolveBlockColumn(match.block, store_col,
                                                   &mat_transient);
              if (last_col == nullptr) {
                chunk_io_error.store(true, std::memory_order_relaxed);
                break;
              }
              last_blk = match.block;
            }
            const LSTableColumnBlock& column = *last_col;
            if (column.has_null && column.IsNull(match.row)) {
              cell = Value();
            } else if (column.type == ValueType::kDouble) {
              cell.SetDouble(
                  std::bit_cast<double>(column.int_cells[match.row]));
            } else if (column.type == ValueType::kDate) {
              cell = Value::DateFromDays(column.int_cells[match.row]);
            } else if (column.type == ValueType::kVarChar) {
              cell.CopyFromStringView(column.VarcharAt(match.row));
            } else {
              cell = Value(column.int_cells[match.row]);
              if (column.is_unsigned) {
                cell.SetUnsigned();
              }
            }
            dst.AppendValue(cell, chunk.EnsureArena(), k);
          }
        }
        return chunk;
      };
      auto out_col = std::make_shared<ColumnarTable>();
      // P350: honor the columnar chunk invariant ("up to
      // kColumnarChunkRows rows", columnar_table.hpp) -- slice the matches
      // into 2048-row windows like the sibling generation paths
      // (scan_filter.cpp / build_input_joins.cpp) do.  The old
      // `produced <= 4096 || workers == 1` gate emitted one oversized
      // chunk, and even the parallel arm could exceed 2048 rows per worker.
      if (produced <= 4096 || workers == 1) {
        std::vector<StreamMatch> combined_matches;
        combined_matches.reserve(produced);
        for (const auto& wm : worker_matches) {
          combined_matches.insert(combined_matches.end(), wm.begin(), wm.end());
        }
        if (combined_matches.empty()) {
          out_col->AppendChunk(
              build_chunk_from_matches(std::span<const StreamMatch>{}));
        }
        for (size_t off = 0; off < combined_matches.size();
             off += kColumnarChunkRows) {
          out_col->AppendChunk(build_chunk_from_matches(
              std::span<const StreamMatch>(combined_matches)
                  .subspan(off, std::min(kColumnarChunkRows,
                                         combined_matches.size() - off))));
        }
      } else {
        // One window list per worker, flattened into global group indices
        // so chunks still land in (worker, window) order.
        std::vector<size_t> group_base(workers + 1, 0);
        for (size_t w = 0; w < workers; ++w) {
          group_base[w + 1] =
              group_base[w] +
              ((worker_matches[w].size() + kColumnarChunkRows - 1) /
               kColumnarChunkRows);
        }
        std::vector<ColumnarChunk> built_chunks(group_base[workers]);
        RETURN_IF_FAIL(RunParallelTasks(workers, [&](size_t w) {
          const std::vector<StreamMatch>& ms = worker_matches[w];
          for (size_t off = 0; off < ms.size(); off += kColumnarChunkRows) {
            built_chunks[group_base[w] + (off / kColumnarChunkRows)] =
                build_chunk_from_matches(
                    std::span<const StreamMatch>(ms).subspan(
                        off, std::min(kColumnarChunkRows, ms.size() - off)));
          }
        }));
        for (ColumnarChunk& ch : built_chunks) {
          if (ch.rows > 0) {
            out_col->AppendChunk(std::move(ch));
          }
        }
      }
      if (chunk_io_error.load(std::memory_order_relaxed)) {
        return Status::kIOError;
      }
      bool all_uniform = true;
      for (const ColumnarChunk& ch : out_col->chunks) {
        for (const ColumnarColumn& col : ch.columns) {
          if (!col.is_uniform) {
            all_uniform = false;
            break;
          }
        }
        if (!all_uniform) {
          break;
        }
      }
      output.SetColumnarTable(std::move(out_col), all_uniform);
      if (::tinylamb::platform::GetEnv("TINYLAMB_GROUP_PHASES") != nullptr) {
        std::cerr << "[stream] table=" << probed.table
                  << " left=" << total_left_rows << " out=" << produced
                  << " columnar=1\n";
      }
      ++output.hash_joins;
      return std::optional<Relation>(std::move(output));
    }
    size_t produced = 0;
    for (Relation& shard : shards) {
      produced += shard.TotalRows();
      output.MergeInMemoryShard(std::move(shard));
    }
    if (produced == 0) {
      return std::optional<Relation>{};
    }
    if (::tinylamb::platform::GetEnv("TINYLAMB_GROUP_PHASES") != nullptr) {
      std::cerr << "[stream] table=" << probed.table
                << " left=" << total_left_rows << " out=" << produced << "\n";
    }
    RETURN_IF_FAIL(output.FinishSpill());
    RETURN_IF_FAIL(output.GetStatus());
    ++output.hash_joins;
    return std::optional<Relation>(std::move(output));
  };
  for (const size_t i : visit_order) {
    available.insert(i);
    SelectSource source = statement.Sources()[i];
    if (lifted_inner_on[i]) {
      source.join_condition.reset();
    }
    // Outer-join ON conditions keep their syntactic predicate: folding a
    // WHERE conjunct into them would change matching into null padding.
    // Inner and cross joins admit it: sigma_p(A join_c B) equals
    // A join_(c AND p) B, and the join machinery turns the equality
    // conjuncts into hash keys while the residuals re-verify per pair.
    if (source.join_type == JoinType::kCross ||
        source.join_type == JoinType::kInner) {
      std::vector<Expression> applicable;
      applicable.reserve(ordered_predicates.size());
      for (size_t p = 0; p < ordered_predicates.size(); ++p) {
        const PredicateInfo& predicate = ordered_predicates[p];
        if (predicate_placed[p] || !placeable(predicate) ||
            predicate.sources.size() < 2 || !predicate.sources.contains(i) ||
            !IsSubset(predicate.sources, available)) {
          continue;
        }
        // Same rule as the single-source pre-filters above: a conjunct
        // over a source that a LATER RIGHT/FULL join NULL-supplies must
        // not filter below that join.  `a CROSS JOIN b RIGHT JOIN c ...
        // WHERE f(a, b) IS NULL` evaluated f before the RIGHT join, turned
        // c rows whose matches all failed f into NULL-padded rows, and the
        // padded rows then passed the IS NULL test.
        if (!std::ranges::all_of(predicate.sources, [&](size_t source_index) {
              return static_cast<bool>(filter_safe[source_index]);
            })) {
          continue;
        }
        applicable.push_back(predicate.expression);
        predicate_placed[p] = true;
      }
      if (!applicable.empty()) {
        if (source.join_condition) {
          applicable.push_back(source.join_condition);
        }
        source.join_condition = CombineConjuncts(applicable);
      }
    }
    // Final ordered-join step of a fusable grouped statement with an empty
    // WHERE (or, for a trailing LEFT JOIN, where every remaining unplaced
    // WHERE conjunct is a null-rejecting `right.col IS NULL` anti-join test —
    // Lean proof: lean/TinylambOpt/PredicateAndRelationalLowering.lean
    //   (fused_antijoin_groupby_sound))
    // Lean proof: lean/TinylambOpt/RollupAndJoinFilterRewrites.lean
    //   (eager_channel_preagg_before_left_join_sound,
    //    eager_channel_preagg_requires_unique_dimension):
    // the join feeds the aggregation directly and never materializes.
    bool can_fuse_anti_left = false;
    std::vector<size_t> anti_where_indices;
    anti_where_indices.reserve(ordered_predicates.size());
    if (fusion != nullptr && fusion->eligible &&
        available.size() == relations.size() && !all_where_placed() &&
        source.join_type == JoinType::kLeft && last_null_extension[i] == i &&
        source.using_columns.empty() && source.join_condition) {
      std::vector<slot_t> is_null_cols;
      is_null_cols.reserve(ordered_predicates.size());
      bool all_unplaced_are_right_is_null = true;
      for (size_t p = 0; p < ordered_predicates.size(); ++p) {
        if (predicate_placed[p]) {
          continue;
        }
        const PredicateInfo& predicate = ordered_predicates[p];
        if (p >= where_predicate_count || !placeable(predicate) ||
            predicate.sources.size() != 1 || !predicate.sources.contains(i) ||
            !predicate.expression ||
            predicate.expression->Type() != TypeTag::kUnaryExp) {
          all_unplaced_are_right_is_null = false;
          break;
        }
        const auto& unary = predicate.expression->AsUnaryExpression();
        if (unary.Op() != UnaryOperation::kIsNull || !unary.Child() ||
            unary.Child()->Type() != TypeTag::kColumnValue) {
          all_unplaced_are_right_is_null = false;
          break;
        }
        const auto col =
            LocalColumnOffset(relations[i].schema,
                              unary.Child()->AsColumnValue().GetColumnName());
        if (!col.has_value()) {
          all_unplaced_are_right_is_null = false;
          break;
        }
        is_null_cols.push_back(static_cast<slot_t>(*col));
        anti_where_indices.push_back(p);
      }
      if (all_unplaced_are_right_is_null && !is_null_cols.empty()) {
        const auto eq_keys =
            EqualityKeys(result.schema, relations[i].schema,
                         SplitConjuncts(source.join_condition));
        can_fuse_anti_left =
            std::ranges::any_of(eq_keys, [&](const EqualityKey& eq) {
              return !eq.null_safe &&
                     std::ranges::find(is_null_cols,
                                       static_cast<slot_t>(eq.right)) !=
                         is_null_cols.end();
            });
      }
    }
    // Lean proof: lean/TinylambOpt/PredicateAndRelationalLowering.lean
    // (unused_unique_left_join_elim_sound)
    if (can_prune_columns && !result.HasSpill() && !result.using_columns &&
        source.join_type == JoinType::kLeft && last_null_extension[i] == i &&
        !source.sample && source.using_columns.empty() &&
        source.join_condition != nullptr && loaded[i] &&
        !relations[i].HasSpill() && relations[i].TotalRows() <= 32768) {
      const auto on_conjuncts = SplitConjuncts(source.join_condition);
      const Schema lj_pair_schema = result.schema + relations[i].schema;
      const auto eq_keys =
          EqualityKeys(result.schema, relations[i].schema, on_conjuncts);
      const bool pure_int_equijoin =
          !on_conjuncts.empty() && eq_keys.size() == on_conjuncts.size() &&
          eq_keys.size() <= 2 &&
          std::ranges::all_of(on_conjuncts,
                              [&](const Expression& pred) {
                                return SafeToReduceEvaluationCountWithSchema(
                                    pred, lj_pair_schema);
                              }) &&
          std::ranges::all_of(eq_keys, [&](const EqualityKey& eq) {
            if (eq.null_safe || eq.left_offset != 0 || eq.right_offset != 0) {
              return false;
            }
            const Column& lc = result.schema.GetColumn(eq.left);
            const Column& rc = relations[i].schema.GetColumn(eq.right);
            return !lc.IsUnsigned() && !rc.IsUnsigned() &&
                   lc.Type() == rc.Type() &&
                   (lc.Type() == ValueType::kInt64 ||
                    lc.Type() == ValueType::kDate);
          });
      if (pure_int_equijoin) {
        SelectStatement future(statement);
        std::vector<SelectSource> remaining_sources;
        remaining_sources.reserve(statement.Sources().size());
        for (size_t r = 0; r < statement.Sources().size(); ++r) {
          if (!available.contains(r)) {
            auto rem = statement.Sources()[r];
            if (lifted_inner_on[r]) {
              rem.join_condition.reset();
            }
            remaining_sources.push_back(std::move(rem));
          }
        }
        future.SetSources(std::move(remaining_sources));
        std::vector<Expression> remaining_predicates;
        remaining_predicates.reserve(ordered_predicates.size());
        if (!all_where_placed() && statement.WhereClause()) {
          remaining_predicates.push_back(statement.WhereClause());
        } else {
          for (size_t q = 0; q < ordered_predicates.size(); ++q) {
            if (!predicate_placed[q]) {
              remaining_predicates.push_back(ordered_predicates[q].expression);
            }
          }
        }
        future.SetWhereClause(CombineConjuncts(remaining_predicates));
        const Schema joined_schema = result.schema + relations[i].schema;
        const auto needed = RequiredColumns(future, joined_schema);
        const bool right_unused =
            std::ranges::none_of(needed, [&](slot_t needed_col) {
              return needed_col >= result.schema.ColumnCount();
            });
        if (right_unused) {
          // Runtime-verify duplicate-freedom on non-NULL equijoin keys of
          // relations[i].
          bool unique_right = true;
          FlatIntSet seen_keys;
          seen_keys.Reserve(relations[i].TotalRows());
          const auto check_row_vals = [&](int64_t k0, int64_t k1) {
            int64_t packed = k0;
            if (eq_keys.size() == 2) {
              if (!std::in_range<int32_t>(k0) || !std::in_range<int32_t>(k1)) {
                unique_right = false;
                return;
              }
              packed = std::bit_cast<int64_t>(
                  (static_cast<uint64_t>(
                       static_cast<uint32_t>(static_cast<int32_t>(k0)))
                   << 32) |
                  static_cast<uint32_t>(static_cast<int32_t>(k1)));
            }
            if (seen_keys.Contains(packed)) {
              unique_right = false;
            } else {
              seen_keys.Insert(packed);
            }
          };
          if (relations[i].HasColumnarOnly()) {
            for (const ColumnarChunk& ch : relations[i].columnar->chunks) {
              const ColumnarColumn& c0 = ch.columns[eq_keys[0].right];
              const ColumnarColumn* c1 =
                  eq_keys.size() == 2 ? &ch.columns[eq_keys[1].right] : nullptr;
              for (size_t r = 0; r < ch.rows && unique_right; ++r) {
                int64_t k0 = 0;
                int64_t k1 = 0;
                if (!c0.TryReadNonNullInt(r, &k0) ||
                    (c1 != nullptr && !c1->TryReadNonNullInt(r, &k1))) {
                  continue;
                }
                check_row_vals(k0, k1);
              }
              if (!unique_right) {
                break;
              }
            }
          } else {
            for (const Row& r : relations[i].rows) {
              const Value& v0 = r[eq_keys[0].right];
              if (v0.IsNull()) {
                continue;
              }
              int64_t k1 = 0;
              if (eq_keys.size() == 2) {
                const Value& v1 = r[eq_keys[1].right];
                if (v1.IsNull()) {
                  continue;
                }
                k1 = v1.value.int_value;
              }
              check_row_vals(v0.value.int_value, k1);
              if (!unique_right) {
                break;
              }
            }
          }
          if (unique_right) {
            relations[i] = Relation(context.execution_runtime());
            if (fusion != nullptr && fusion->eligible &&
                available.size() == relations.size() && all_where_placed()) {
              Relation absorbed_dummy(context.execution_runtime());
              absorbed_dummy.rows.emplace_back();
              ASSIGN_OR_RETURN(
                  std::optional<Relation>, fused,
                  (TryFusedGroupedJoin(context, result, absorbed_dummy, {},
                                       JoinType::kInner, outer, ctes, fusion)));
              if (fused.has_value()) {
                result = std::move(*fused);
                fusion->fired = true;
                *where_fully_applied = true;
                return result;
              }
            }
            RETURN_IF_FAIL(prune_consumed_columns());
            continue;
          }
        }
      }
    }
    // Lean proof: lean/TinylambOpt/VectorizedJoinAndPruning.lean
    //   (left_join_lookup_restrict_right)
    // When loading a deferred right-hand source of a LEFT OUTER JOIN after
    // the left side (`result`) is already materialized and smaller than the
    // right table, restrict the right scan by a RuntimeFilter built from
    // `result`'s join keys. Unmatched left rows still emit NULL-padded right
    // columns; only right rows that could never match any left row are
    // pruned (e.g. TPC-DS Q40 `catalog_returns` 1.44M -> ~1.7K rows, Q78
    // `*_returns` 5M -> ~200 rows).
    auto load_deferred_source = [&](size_t src_idx) -> Status {
      auto binding = ordered_binding(src_idx);
      if (!binding && source.join_type == JoinType::kLeft &&
          source.join_condition != nullptr && !result.HasSpill() &&
          !statement.Sources()[src_idx].sample && result.TotalRows() > 0 &&
          result.TotalRows() <=
              std::max<size_t>(4096, deferred_order_rows[src_idx] / 2)) {
        const std::vector<EqualityKey> eq_keys =
            EqualityKeys(result.schema, relations[src_idx].schema,
                         SplitConjuncts(source.join_condition));
        std::vector<slot_t> bound_proj_cols;
        for (const EqualityKey& eq : eq_keys) {
          if (eq.null_safe || eq.left_offset != 0 || eq.right_offset != 0 ||
              eq.left >= result.schema.ColumnCount() ||
              eq.right >= relations[src_idx].schema.ColumnCount()) {
            continue;
          }
          const Column& lc = result.schema.GetColumn(eq.left);
          const Column& rc = relations[src_idx].schema.GetColumn(eq.right);
          if (lc.IsUnsigned() || rc.IsUnsigned() || lc.Type() != rc.Type() ||
              (lc.Type() != ValueType::kInt64 &&
               lc.Type() != ValueType::kDate)) {
            continue;
          }
          const auto proj_it =
              std::ranges::find(projections[src_idx], eq.right);
          if (proj_it == projections[src_idx].end()) {
            continue;
          }
          const auto proj_col = static_cast<slot_t>(
              std::distance(projections[src_idx].begin(), proj_it));
          if (std::ranges::find(bound_proj_cols, proj_col) !=
              bound_proj_cols.end()) {
            continue;
          }
          RuntimeFilterBuilder builder;
          builder.Reserve(result.TotalRows());
          if (result.HasColumnarOnly()) {
            for (const ColumnarChunk& chunk : result.columnar->chunks) {
              if (chunk.rows == 0) {
                continue;
              }
              const ColumnarColumn& cc = chunk.columns[eq.left];
              if (cc.is_uniform && !cc.is_unsigned &&
                  (cc.type == ValueType::kInt64 ||
                   cc.type == ValueType::kDate)) {
                for (size_t r = 0; r < chunk.rows; ++r) {
                  if (!cc.IsNull(r)) {
                    builder.Observe(cc.IntAt(r));
                  }
                }
              } else {
                Value val;
                for (size_t r = 0; r < chunk.rows; ++r) {
                  cc.WriteValueInto(r, &val);
                  if (!val.IsNull()) {
                    builder.Observe(val.value.int_value);
                  }
                }
              }
            }
          } else {
            for (const auto& row : result.rows) {
              const auto& key = row[eq.left];
              if (!key.IsNull()) {
                builder.Observe(key.value.int_value);
              }
            }
          }
          auto group = builder.Build(false);
          if (!group || group->empty()) {
            continue;
          }
          bound_proj_cols.push_back(proj_col);
          ScanRuntimeFilter filter{
              .group = std::move(group), .column = proj_col, .type = rc.Type()};
          if (binding) {
            binding->extra.push_back(std::move(filter));
          } else {
            binding = std::move(filter);
          }
        }
      }
      ASSIGN_OR_RETURN(
          Relation, loaded_rel,
          (LoadSource(context, statement.Sources()[src_idx], outer, ctes,
                      &projections[src_idx],
                      ordered_local[src_idx].empty() ? nullptr
                                                     : &ordered_local[src_idx],
                      binding ? &*binding : nullptr, nullptr, allow_ord_col)));
      relations[src_idx] = std::move(loaded_rel);
      loaded[src_idx] = true;
      return Status::kSuccess;
    };
    if (fusion != nullptr && fusion->eligible &&
        available.size() == relations.size() &&
        (all_where_placed() || can_fuse_anti_left) &&
        (source.join_type == JoinType::kLeft ||
         (source.join_type == JoinType::kFull &&
          source.using_columns.empty()) ||
         source.join_type == JoinType::kCross ||
         source.join_type == JoinType::kInner)) {
      if (!loaded[i] && deferred_order_rows[i] != 0) {
        RETURN_IF_FAIL(load_deferred_source(i));
      }
      std::vector<Expression> fusion_predicates;
      if (source.join_condition) {
        for (const Expression& conjunct :
             SplitConjuncts(source.join_condition)) {
          if (source.join_type == JoinType::kLeft &&
              IsNonThrowingScanFilter(conjunct, relations[i].schema)) {
            const std::vector<PredicateInfo> analyzed =
                AnalyzePredicates(conjunct, relations);
            if (analyzed.size() == 1 && analyzed[0].resolved &&
                !analyzed[0].contains_query &&
                analyzed[0].sources.size() == 1 &&
                analyzed[0].sources.contains(i)) {
              continue;
            }
          }
          fusion_predicates.push_back(conjunct);
        }
      }
      const JoinType fused_type = source.join_type == JoinType::kCross
                                      ? JoinType::kInner
                                      : source.join_type;
      fusion->anti_unmatched_only = can_fuse_anti_left;
      ASSIGN_OR_RETURN(
          std::optional<Relation>, fused,
          (TryFusedGroupedJoin(context, result, relations[i], fusion_predicates,
                               fused_type, outer, ctes, fusion)));
      if (fused.has_value()) {
        for (const size_t idx : anti_where_indices) {
          predicate_placed[idx] = true;
        }
        result = std::move(*fused);
        fusion->fired = true;
        *where_fully_applied = true;
        return result;
      }
      fusion->anti_unmatched_only = false;
    }
    std::optional<Relation> unmatched;
    if (source.join_type == JoinType::kLeft && last_null_extension[i] == i) {
      for (size_t p = 0; p < where_predicate_count; ++p) {
        const auto& predicate = ordered_predicates[p];
        if (predicate_placed[p] || !placeable(predicate) ||
            predicate.sources.size() != 1 || !predicate.sources.contains(i) ||
            predicate.expression->Type() != TypeTag::kUnaryExp) {
          continue;
        }
        const auto& unary = predicate.expression->AsUnaryExpression();
        if (unary.Op() != UnaryOperation::kIsNull ||
            unary.Child()->Type() != TypeTag::kColumnValue) {
          continue;
        }
        const auto column =
            LocalColumnOffset(relations[i].schema,
                              unary.Child()->AsColumnValue().GetColumnName());
        if (!column) {
          continue;
        }
        bool emit_right_columns = true;
        if (can_prune_columns && available.size() != relations.size() &&
            !result.HasSpill() && !result.using_columns) {
          SelectStatement future(statement);
          std::vector<SelectSource> remaining_sources;
          remaining_sources.reserve(statement.Sources().size());
          for (size_t r = 0; r < statement.Sources().size(); ++r) {
            if (!available.contains(r)) {
              auto remaining = statement.Sources()[r];
              if (lifted_inner_on[r]) {
                remaining.join_condition.reset();
              }
              remaining_sources.push_back(std::move(remaining));
            }
          }
          future.SetSources(std::move(remaining_sources));
          std::vector<Expression> remaining_predicates;
          remaining_predicates.reserve(ordered_predicates.size());
          if (!all_where_placed() && statement.WhereClause()) {
            remaining_predicates.push_back(statement.WhereClause());
          } else {
            for (size_t q = 0; q < ordered_predicates.size(); ++q) {
              if (!predicate_placed[q] && q != p) {
                remaining_predicates.push_back(
                    ordered_predicates[q].expression);
              }
            }
          }
          future.SetWhereClause(CombineConjuncts(remaining_predicates));
          const Schema joined = result.schema + relations[i].schema;
          const auto needed = RequiredColumns(future, joined);
          emit_right_columns =
              std::ranges::any_of(needed, [&](slot_t needed_col) {
                return needed_col >= result.schema.ColumnCount();
              });
        }
        if (!loaded[i] && deferred_order_rows[i] != 0) {
          RETURN_IF_FAIL(load_deferred_source(i));
        }
        ASSIGN_OR_RETURN(std::optional<Relation>, candidate,
                         TryUnmatchedLeftJoin(result, relations[i], source,
                                              static_cast<slot_t>(*column),
                                              emit_right_columns));
        if (candidate) {
          unmatched = std::move(candidate);
          predicate_placed[p] = true;
          break;
        }
      }
    }
    if (unmatched) {
      result = std::move(*unmatched);
      relations[i] = Relation(context.execution_runtime());
    } else if (!loaded[i] && deferred_order_rows[i] != 0) {
      ASSIGN_OR_RETURN(std::optional<Relation>, streamed,
                       try_stream_probe(i, source));
      if (streamed) {
        result = std::move(*streamed);
        loaded[i] = true;
      } else {
        RETURN_IF_FAIL(load_deferred_source(i));
        ASSIGN_OR_RETURN(Relation, result_r,
                         (Join(context, std::move(result),
                               std::move(relations[i]), source, outer, ctes)));
        result = std::move(result_r);
      }
    } else {
      ASSIGN_OR_RETURN(Relation, result_r,
                       (Join(context, std::move(result),
                             std::move(relations[i]), source, outer, ctes)));
      result = std::move(result_r);
    }
    // A nullable-side WHERE term can be evaluated after its outer join,
    // once no later RIGHT/FULL join can manufacture a new NULL for it.
    // This also allows subsequent inner joins to fuse their aggregation.
    std::vector<Expression> post_join;
    post_join.reserve(where_predicate_count);
    for (size_t p = 0; p < where_predicate_count; ++p) {
      const auto& predicate = ordered_predicates[p];
      if (predicate_placed[p] || !placeable(predicate) ||
          predicate.sources.empty() ||
          !IsSubset(predicate.sources, available) ||
          !std::ranges::all_of(predicate.sources, [&](size_t index) {
            return available.contains(last_null_extension[index]);
          })) {
        continue;
      }
      post_join.push_back(predicate.expression);
      predicate_placed[p] = true;
    }
    if (!post_join.empty()) {
      RETURN_IF_FAIL(FilterRelation(context, &result, post_join, outer, ctes));
    }
    RETURN_IF_FAIL(prune_consumed_columns());
  }
  *where_fully_applied = all_where_placed();
  if (!columnar_output && result.HasColumnarOnly()) {
    RETURN_IF_FAIL(result.EnsureRowsMaterialized());
  }
  return result;
}

}  // namespace tinylamb::relational_detail
