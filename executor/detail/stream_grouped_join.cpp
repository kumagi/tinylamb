/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/query_parallelism.hpp"
#include "common/status_or.hpp"
#include "common/string_util.hpp"
#include "database/transaction_context.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/detail/flat_int_key_map.hpp"
#include "executor/detail/fused_grouped_join.hpp"
#include "executor/detail/planning_heuristics.hpp"
#include "executor/detail/relation.hpp"
#include "executor/detail/scan_filter.hpp"
#include "executor/detail/subquery_runtime.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/binary_expression.hpp"
#include "expression/bytecode.hpp"
#include "expression/column_value.hpp"
#include "expression/expression.hpp"
#include "expression/rewrite.hpp"
#include "query/statement.hpp"
#include "table/lstable_bridge.hpp"
#include "table/table.hpp"
#include "table/table_statistics.hpp"
#include "type/column_name.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb::relational_detail {

// Stream a two-table integer equijoin directly into bounded group partials.
// Each iterator resolves this transaction's snapshot. A bounded exact integer
// projection may reuse key counts only after the table proves every page's
// physical visibility and unchanged LSN; dimension payloads are always fresh.
StatusOr<std::optional<Relation>> TryStreamGroupedBaseJoin(
    TransactionContext& context, const SelectStatement& statement,
    const Scope* outer, const CteMap& ctes,
    const std::vector<std::vector<slot_t>>& projections,
    const std::vector<Relation>& schemas, GroupedJoinFusionSpec* fusion) {
  const auto decline = [] { return std::optional<Relation>{}; };
  if (!fusion || !fusion->eligible || statement.Sources().size() != 2 ||
      outer || !ctes.empty() || fusion->group_keys.size() > 1 ||
      fusion->aggregates.empty() ||
      std::getenv("TINYLAMB_DISABLE_STREAM_GROUPED_JOIN"))
    return decline();
  for (size_t i = 0; i < 2; ++i) {
    const auto& source = statement.Sources()[i];
    if (source.query || source.unnest || source.sample || source.is_lateral ||
        source.from_nested_join || !source.using_columns.empty())
      return decline();
    if (i == 0 && source.join_type != JoinType::kInner &&
        source.join_type != JoinType::kCross)
      return decline();
    if (i == 1 && source.join_type != JoinType::kInner &&
        source.join_type != JoinType::kCross &&
        source.join_type != JoinType::kLeft)
      return decline();
  }
  const bool is_left_join = statement.Sources()[1].join_type == JoinType::kLeft;
  if (is_left_join &&
      (statement.WhereClause() || fusion->group_keys.size() != 1)) {
    return decline();
  }
  const auto qualifier = [](const SelectSource& source) -> const std::string& {
    return source.alias.empty() ? source.table : source.alias;
  };
  const std::string& first_qualifier = qualifier(statement.Sources()[0]);
  const std::string& second_qualifier = qualifier(statement.Sources()[1]);
  if (IdentifierEquals(first_qualifier, second_qualifier)) {
    return decline();
  }
  if (!fusion->group_keys.empty() &&
      fusion->group_keys[0]->Type() != TypeTag::kColumnValue)
    return decline();
  std::array<std::shared_ptr<Table>, 2> tables;
  std::array<size_t, 2> estimates{};
  for (size_t i = 0; i < 2; ++i) {
    ASSIGN_OR_RETURN(std::shared_ptr<Table>, table,
                     context.GetTable(statement.Sources()[i].table));
    tables[i] = std::move(table);
    ASSIGN_OR_RETURN(std::shared_ptr<TableStatistics>, stats,
                     context.GetStats(statement.Sources()[i].table));
    estimates[i] = stats->Rows();
  }
  if (is_left_join && estimates[0] > 500000) return decline();
  const size_t probe =
      is_left_join ? 1 : (estimates[0] >= estimates[1] ? 0 : 1);
  const size_t build = 1 - probe;
  const Schema probe_schema =
      ProjectSchema(schemas[probe].schema, projections[probe]);
  const Schema build_schema =
      ProjectSchema(schemas[build].schema, projections[build]);
  const Schema joined = probe_schema + build_schema;
  int group_column = -1;
  ValueType group_val_type = ValueType::kInt64;
  if (!fusion->group_keys.empty()) {
    group_column = build_schema.Offset(
        fusion->group_keys[0]->AsColumnValue().GetColumnName());
    if (group_column < 0) return decline();
    const auto& group_type =
        build_schema.GetColumn(static_cast<size_t>(group_column));
    if (group_type.IsUnsigned() || (group_type.Type() != ValueType::kInt64 &&
                                    group_type.Type() != ValueType::kDate))
      return decline();
    group_val_type = group_type.Type();
  }
  auto all_predicates = SplitConjuncts(statement.Sources()[1].join_condition);
  if (statement.WhereClause()) {
    if (is_left_join) return decline();
    for (Expression& wpred : SplitConjuncts(statement.WhereClause())) {
      all_predicates.push_back(std::move(wpred));
    }
  }
  if (all_predicates.empty()) return decline();
  std::vector<Expression> predicates;
  std::vector<Expression> probe_local_predicates;
  std::vector<Expression> build_local_predicates;
  for (const Expression& pred : all_predicates) {
    const std::vector<PredicateInfo> analyzed =
        AnalyzePredicates(pred, schemas);
    if (analyzed.size() == 1 && analyzed[0].resolved &&
        !analyzed[0].contains_query && analyzed[0].sources.size() == 1 &&
        analyzed[0].sources.contains(probe) &&
        IsNonThrowingScanComparison(pred, probe_schema)) {
      probe_local_predicates.push_back(pred);
    } else if (!is_left_join && analyzed.size() == 1 && analyzed[0].resolved &&
               !analyzed[0].contains_query && analyzed[0].sources.size() == 1 &&
               analyzed[0].sources.contains(build) &&
               IsNonThrowingScanComparison(pred, build_schema)) {
      build_local_predicates.push_back(pred);
    } else {
      predicates.push_back(pred);
    }
  }
  if (predicates.size() != 1) return decline();
  struct BuildIntFilter {
    slot_t build_col{0};
    BinaryOperation op{BinaryOperation::kEquals};
    int64_t constant{0};
  };
  std::vector<BuildIntFilter> build_int_filters;
  for (const Expression& pred : build_local_predicates) {
    std::optional<SimpleComparePredicate> cmp =
        TryCompileSimpleCompare(pred, build_schema);
    if (!cmp.has_value() || cmp->column_payload || !cmp->int_payload ||
        cmp->constant.IsUnsigned() ||
        cmp->column >= build_schema.ColumnCount()) {
      return decline();
    }
    const Column& bcol_meta = build_schema.GetColumn(cmp->column);
    if ((bcol_meta.Type() != ValueType::kInt64 &&
         bcol_meta.Type() != ValueType::kDate) ||
        bcol_meta.IsUnsigned()) {
      return decline();
    }
    build_int_filters.push_back(BuildIntFilter{
        .build_col = cmp->column,
        .op = cmp->op,
        .constant = cmp->int_constant,
    });
  }
  struct ProbeLikeFilter {
    slot_t probe_col{0};
    slot_t full_col{0};
    size_t varchar_slot{0};
    std::string pattern{};
    std::string sub0{};
    std::string sub1{};
    bool two_sub_wild{false};
    bool negated{false};
  };
  struct ProbeIntFilter {
    slot_t probe_col{0};
    slot_t full_col{0};
    BinaryOperation op{BinaryOperation::kEquals};
    int64_t constant{0};
  };
  std::vector<ProbeLikeFilter> probe_like_filters;
  std::vector<ProbeIntFilter> probe_int_filters;
  std::vector<IntegerPeekCompare> probe_int_peeks;
  for (const Expression& pred : probe_local_predicates) {
    if (std::optional<SimpleComparePredicate> cmp =
            TryCompileSimpleCompare(pred, probe_schema);
        cmp.has_value() && !cmp->column_payload && cmp->int_payload &&
        !cmp->constant.IsUnsigned() &&
        cmp->column < probe_schema.ColumnCount()) {
      const Column& pcol_meta = probe_schema.GetColumn(cmp->column);
      if ((pcol_meta.Type() == ValueType::kInt64 ||
           pcol_meta.Type() == ValueType::kDate) &&
          !pcol_meta.IsUnsigned()) {
        probe_int_filters.push_back(ProbeIntFilter{
            .probe_col = cmp->column,
            .full_col = projections[probe][cmp->column],
            .op = cmp->op,
            .constant = cmp->int_constant,
        });
        probe_int_peeks.push_back(IntegerPeekCompare{
            .column = projections[probe][cmp->column],
            .op = cmp->op,
            .constant = cmp->int_constant,
        });
        continue;
      }
    }
    std::optional<BytecodeProgram> prog = BytecodeCompiler::CompileRowCached(
        pred, probe_schema, BytecodeCompiler::Context::kFilter);
    if (!prog.has_value()) return decline();
    const ProgramShape shape = ClassifyProgram(*prog);
    if (shape.like_column < 0) return decline();
    const auto pcol = static_cast<slot_t>(shape.like_column);
    if (pcol >= probe_schema.ColumnCount() ||
        probe_schema.GetColumn(pcol).Type() != ValueType::kVarChar) {
      return decline();
    }
    const Value& pat_val = prog->Constants()[prog->Instructions()[1].operand];
    if (pat_val.IsCaseInsensitive()) return decline();
    size_t vc_slot = 0;
    for (size_t c = 0; c < pcol; ++c) {
      if (probe_schema.GetColumn(c).Type() == ValueType::kVarChar) {
        ++vc_slot;
      }
    }
    ProbeLikeFilter lf{
        .probe_col = pcol,
        .full_col = projections[probe][pcol],
        .varchar_slot = vc_slot,
        .pattern = std::string(shape.like_pattern),
        .negated = shape.like_negated,
    };
    const std::string_view pat = lf.pattern;
    if (pat.size() >= 3 && pat.front() == '%' && pat.back() == '%' &&
        pat.find('_') == std::string_view::npos) {
      const std::string_view inner = pat.substr(1, pat.size() - 2);
      const size_t mid = inner.find('%');
      if (mid != std::string_view::npos && mid > 0 && mid + 1 < inner.size() &&
          inner.find('%', mid + 1) == std::string_view::npos) {
        lf.sub0 = std::string(inner.substr(0, mid));
        lf.sub1 = std::string(inner.substr(mid + 1));
        lf.two_sub_wild = true;
      }
    }
    probe_like_filters.push_back(std::move(lf));
  }
  const auto match_probe_like = [](std::string_view sv,
                                   const ProbeLikeFilter& lf) noexcept -> bool {
    if (lf.two_sub_wild) {
      if (sv.size() < lf.sub0.size() + lf.sub1.size()) {
        return false;
      }
      const void* p0 =
          memmem(sv.data(), sv.size(), lf.sub0.data(), lf.sub0.size());
      if (p0 == nullptr) {
        return false;
      }
      const size_t after0 =
          static_cast<size_t>(static_cast<const char*>(p0) - sv.data()) +
          lf.sub0.size();
      const void* p1 = memmem(sv.data() + after0, sv.size() - after0,
                              lf.sub1.data(), lf.sub1.size());
      return p1 != nullptr;
    }
    return Like(sv, lf.pattern);
  };
  const auto keys = EqualityKeys(probe_schema, build_schema, predicates);
  if (keys.size() != 1 || keys[0].null_safe || keys[0].left_offset != 0 ||
      keys[0].right_offset != 0 || !IsColumnEqualityPredicate(predicates[0]))
    return decline();
  const auto& equality = predicates[0]->AsBinaryExpression();
  const auto unambiguous_col = [&](const ColumnName& col) {
    if (!col.schema.empty()) return true;
    return (probe_schema.Offset(col) >= 0) != (build_schema.Offset(col) >= 0);
  };
  if (!unambiguous_col(equality.Left()->AsColumnValue().GetColumnName()) ||
      !unambiguous_col(equality.Right()->AsColumnValue().GetColumnName()))
    return decline();
  const auto signed_int = [](const Column& column) {
    return column.Type() == ValueType::kInt64 && !column.IsUnsigned();
  };
  if (!signed_int(probe_schema.GetColumn(keys[0].left)) ||
      !signed_int(build_schema.GetColumn(keys[0].right)))
    return decline();
  // Fixed-width batches have a strict, reservable upper bound.
  for (size_t c = 0; c < probe_schema.ColumnCount(); ++c) {
    const auto type = probe_schema.GetColumn(c).Type();
    if (type != ValueType::kInt64 && type != ValueType::kDate &&
        type != ValueType::kDouble &&
        !(type == ValueType::kVarChar && !probe_like_filters.empty()))
      return decline();
  }
  std::vector<int> arguments;
  for (const auto* aggregate : fusion->aggregates) {
    const auto type = aggregate->GetType();
    if ((type != AggregationType::kCount && type != AggregationType::kSum) ||
        aggregate->Distinct() || aggregate->HasWhereFilter() ||
        aggregate->NeedsGroupContext())
      return decline();
    if (is_left_join && type != AggregationType::kCount) return decline();
    if (fusion->count_star[arguments.size()]) {
      if (is_left_join) return decline();
      arguments.push_back(-1);
      continue;
    }
    if (!aggregate->Child() ||
        aggregate->Child()->Type() != TypeTag::kColumnValue)
      return decline();
    const int offset =
        joined.Offset(aggregate->Child()->AsColumnValue().GetColumnName());
    if (offset < 0 ||
        !signed_int(joined.GetColumn(static_cast<size_t>(offset))))
      return decline();
    if (is_left_join &&
        static_cast<size_t>(offset) >= probe_schema.ColumnCount())
      return decline();
    arguments.push_back(offset);
  }
  // Outside an aggregate, only the grouping column may read a base cell.
  const auto safe_emit = [&](const auto& self,
                             const Expression& expression) -> bool {
    if (!expression) return false;
    if (expression->Type() == TypeTag::kAggregateExp ||
        expression->Type() == TypeTag::kConstantValue)
      return true;
    if (expression->Type() == TypeTag::kColumnValue) {
      return group_column >= 0 &&
             joined.Offset(expression->AsColumnValue().GetColumnName()) ==
                 static_cast<int>(probe_schema.ColumnCount()) + group_column;
    }
    if (expression->Type() != TypeTag::kBinaryExp &&
        expression->Type() != TypeTag::kUnaryExp)
      return false;
    return std::ranges::all_of(
        ExpressionChildren(expression),
        [&](const Expression& child) { return self(self, child); });
  };
  for (const auto& expression : fusion->select_items)
    if (!safe_emit(safe_emit, expression)) return decline();

  ASSIGN_OR_RETURN(TableLease, lease,
                   tables[probe]->AcquireLease(context.txn_.ID()));
  std::vector<Table::ScanMorsel> morsels;
  Relation dimension(context.execution_runtime());
  std::vector<int64_t> dimension_cells;
  std::vector<uint8_t> dimension_nulls;
  const size_t dimension_stride = build_schema.ColumnCount();
  bool typed_dimension =
      context.txn_.IsReadOnly() || context.txn_.WriteSetEmpty();
  for (size_t c = 0; c < dimension_stride; ++c) {
    const auto& column = build_schema.GetColumn(c);
    typed_dimension = typed_dimension && !column.IsUnsigned() &&
                      (column.Type() == ValueType::kInt64 ||
                       column.Type() == ValueType::kDate);
  }
  constexpr size_t kDimensionBytes = 8 * 1024 * 1024;
  auto dimension_charge =
      typed_dimension
          ? TryReserveQueryMemory(kDimensionBytes + 1024 * dimension_stride * 9)
          : decltype(TryReserveQueryMemory(0)){};
  typed_dimension = typed_dimension && dimension_charge;
  bool build_is_identity_1_to_n = false;
  size_t identity_dimension_rows = 0;
  if (typed_dimension) {
    const auto scan_begin = std::chrono::steady_clock::now();
    bool loaded_from_lstable = false;
    if (!context.txn_.HasAnyPageVersionChains()) {
      std::shared_ptr<const LSTableStore> bstore =
          GetOrBuildLSTableStore(context.txn_, *tables[build], true);
      if (bstore != nullptr && bstore->uncommitted_owner_txn == 0 &&
          bstore->layout_generation == tables[build]->LayoutGeneration() &&
          bstore->first_pid == tables[build]->FirstPageId()) {
        if (dimension_stride == 1 && build_int_filters.empty() &&
            projections[build][0] < bstore->column_is_identity_1_to_n.size() &&
            bstore->column_is_identity_1_to_n[projections[build][0]] != 0 &&
            bstore->total_rows > 0) {
          build_is_identity_1_to_n = true;
          identity_dimension_rows = static_cast<size_t>(bstore->total_rows);
          loaded_from_lstable = true;
        } else {
          const size_t total_cells = bstore->total_rows * dimension_stride;
          if (total_cells <= kDimensionBytes / 9) {
            dimension_cells.resize(total_cells);
            dimension_nulls.resize(total_cells, 0);
            size_t out_row = 0;
            for (const LSTablePageBlock& blk : bstore->blocks) {
              for (uint32_t r = 0; r < blk.row_count; ++r) {
                const size_t base = (out_row + r) * dimension_stride;
                for (size_t c = 0; c < dimension_stride; ++c) {
                  const LSTableColumnBlock& col =
                      blk.columns[projections[build][c]];
                  if (col.IsNull(r)) {
                    dimension_cells[base + c] = 0;
                    dimension_nulls[base + c] = 1;
                  } else {
                    dimension_cells[base + c] = col.int_cells[r];
                  }
                }
              }
              out_row += blk.row_count;
            }
            loaded_from_lstable = true;
          }
        }
      }
    }
    if (!loaded_from_lstable) {
      dimension_cells.reserve(kDimensionBytes / 9);
      dimension_nulls.reserve(kDimensionBytes / 9);
      Iterator iterator =
          tables[build]->BeginFullScan(context.txn_, projections[build]);
      TypedCellBatch batch;
      for (;;) {
        const size_t rows = iterator.DecodeNextTypedBatch(&batch, 1024);
        iterator.DropPageLatch();
        if (rows == 0) break;
        const size_t cells = rows * dimension_stride;
        if (dimension_cells.size() + cells > kDimensionBytes / 9) {
          typed_dimension = false;
          break;
        }
        dimension_cells.insert(dimension_cells.end(), batch.cells.begin(),
                               batch.cells.begin() + cells);
        dimension_nulls.insert(dimension_nulls.end(), batch.nulls.begin(),
                               batch.nulls.begin() + cells);
      }
      RETURN_IF_FAIL(iterator.GetStatus());
    }
    if (typed_dimension) {
      const size_t raw_rows = build_is_identity_1_to_n
                                  ? identity_dimension_rows
                                  : dimension_cells.size() / dimension_stride;
      if (!build_int_filters.empty() && !build_is_identity_1_to_n) {
        size_t out_r = 0;
        for (size_t r = 0; r < raw_rows; ++r) {
          const size_t base = r * dimension_stride;
          bool ok = true;
          for (const BuildIntFilter& bf : build_int_filters) {
            const size_t idx = base + bf.build_col;
            if (dimension_nulls[idx] != 0 ||
                !MatchIntegerCompare(dimension_cells[idx], bf.op,
                                     bf.constant)) {
              ok = false;
              break;
            }
          }
          if (!ok) continue;
          if (out_r != r) {
            const size_t dst = out_r * dimension_stride;
            for (size_t c = 0; c < dimension_stride; ++c) {
              dimension_cells[dst + c] = dimension_cells[base + c];
              dimension_nulls[dst + c] = dimension_nulls[base + c];
            }
          }
          ++out_r;
        }
        dimension_cells.resize(out_r * dimension_stride);
        dimension_nulls.resize(out_r * dimension_stride);
      }
      if (auto* runtime = context.execution_runtime()) {
        const size_t out_rows = build_is_identity_1_to_n
                                    ? identity_dimension_rows
                                    : dimension_cells.size() / dimension_stride;
        runtime->scan_rows += raw_rows;
        runtime->scan_output_rows += out_rows;
        runtime->scan_values_available +=
            raw_rows * tables[build]->GetSchema().ColumnCount();
        runtime->scan_values_decoded += raw_rows * dimension_stride;
        runtime->scan_ms += ElapsedMs(scan_begin);
      }
    } else {
      std::vector<int64_t>().swap(dimension_cells);
      std::vector<uint8_t>().swap(dimension_nulls);
      dimension_charge.reset();
    }
  }
  if (!typed_dimension) {
    ASSIGN_OR_RETURN(
        Relation, loaded,
        LoadSource(context, statement.Sources()[build], outer, ctes,
                   &projections[build],
                   build_local_predicates.empty() ? nullptr
                                                  : &build_local_predicates));
    dimension = std::move(loaded);
    RETURN_IF_FAIL(dimension.FinishSpill());
    RETURN_IF_FAIL(dimension.GetStatus());
    if (dimension.HasSpill()) return decline();
  }
  const size_t dimension_rows =
      build_is_identity_1_to_n
          ? identity_dimension_rows
          : (typed_dimension ? dimension_cells.size() / dimension_stride
                             : dimension.rows.size());
  struct IntegerCell {
    int64_t value;
    bool null;
  };
  const auto dimension_cell = [&](size_t row, size_t column) -> IntegerCell {
    if (build_is_identity_1_to_n) {
      return {static_cast<int64_t>(row + 1), false};
    }
    if (typed_dimension) {
      const size_t cell = row * dimension_stride + column;
      return {dimension_cells[cell], dimension_nulls[cell] != 0};
    }
    const Value& cell = dimension.rows[row][column];
    return {cell.IsNull() ? 0 : cell.value.int_value, cell.IsNull()};
  };
  size_t index_bytes;
  if (__builtin_mul_overflow(dimension_rows, size_t{128}, &index_bytes) ||
      __builtin_add_overflow(index_bytes, size_t{65536}, &index_bytes))
    return decline();
  auto index_charge = TryReserveQueryMemory(index_bytes);
  if (!index_charge) return decline();
  FlatJoinBuckets<int64_t> buckets;
  // Validate build-only nonnegative folds while forming groups and the
  // actual key domain; those three consumers need just one row pass.
  bool frequency_fold = true;
  std::optional<size_t> count_probe_col;
  std::vector<size_t> sum_columns;
  for (size_t a = 0; a < arguments.size(); ++a) {
    if (arguments[a] < 0) continue;
    if (static_cast<size_t>(arguments[a]) < probe_schema.ColumnCount()) {
      const size_t pcol = static_cast<size_t>(arguments[a]);
      if (is_left_join &&
          fusion->aggregates[a]->GetType() == AggregationType::kCount &&
          (!count_probe_col.has_value() || *count_probe_col == pcol)) {
        count_probe_col = pcol;
        continue;
      }
      frequency_fold = false;
      continue;
    }
    if (fusion->aggregates[a]->GetType() == AggregationType::kSum)
      sum_columns.push_back(static_cast<size_t>(arguments[a]) -
                            probe_schema.ColumnCount());
  }
  if ((is_left_join || !probe_like_filters.empty() ||
       !probe_int_filters.empty()) &&
      !frequency_fold) {
    return decline();
  }
  std::optional<int64_t> minimum, maximum;
  bool identity_pk_groups =
      group_column >= 0 && static_cast<size_t>(group_column) == keys[0].right &&
      (build_is_identity_1_to_n ||
       build_schema.GetColumn(keys[0].right).GetConstraint().ctype ==
           Constraint::kPrimaryKey);
  if (identity_pk_groups) {
    if (build_is_identity_1_to_n && sum_columns.empty() && dimension_rows > 0) {
      minimum = 1;
      maximum = static_cast<int64_t>(dimension_rows);
    } else {
      for (size_t i = 0; i < dimension_rows; ++i) {
        const auto join_key = dimension_cell(i, keys[0].right);
        if (join_key.null) {
          if (is_left_join) return decline();
          identity_pk_groups = false;
          minimum.reset();
          maximum.reset();
          break;
        }
        minimum = minimum ? std::min(*minimum, join_key.value) : join_key.value;
        maximum = maximum ? std::max(*maximum, join_key.value) : join_key.value;
        if (frequency_fold) {
          for (size_t column : sum_columns) {
            const auto cell = dimension_cell(i, column);
            if (!cell.null && cell.value < 0) {
              frequency_fold = false;
              break;
            }
          }
        }
      }
    }
  }
  FlatIntKeyMap<size_t> group_map;
  std::optional<size_t> null_group;
  std::vector<Value> groups;
  std::vector<size_t> row_groups;
  if (group_column < 0) {
    row_groups.assign(dimension_rows, 0);
    for (size_t i = 0; i < dimension_rows; ++i) {
      const auto join_key = dimension_cell(i, keys[0].right);
      if (join_key.null) continue;
      minimum = minimum ? std::min(*minimum, join_key.value) : join_key.value;
      maximum = maximum ? std::max(*maximum, join_key.value) : join_key.value;
      if (frequency_fold) {
        for (size_t column : sum_columns) {
          const auto cell = dimension_cell(i, column);
          if (!cell.null && cell.value < 0) {
            frequency_fold = false;
            break;
          }
        }
      }
    }
  } else if (!identity_pk_groups) {
    group_map.Reserve(dimension_rows);
    groups.reserve(dimension_rows);
    row_groups.resize(dimension_rows);
    for (size_t i = 0; i < dimension_rows; ++i) {
      const auto join_key = dimension_cell(i, keys[0].right);
      if (join_key.null) {
        if (is_left_join) return decline();
        continue;
      }
      minimum = minimum ? std::min(*minimum, join_key.value) : join_key.value;
      maximum = maximum ? std::max(*maximum, join_key.value) : join_key.value;
      if (frequency_fold)
        for (size_t column : sum_columns) {
          const auto cell = dimension_cell(i, column);
          if (!cell.null && cell.value < 0) {
            frequency_fold = false;
            break;
          }
        }
      const auto key = dimension_cell(i, static_cast<size_t>(group_column));
      if (key.null) {
        if (!null_group) {
          null_group = groups.size();
          groups.emplace_back();
        }
        row_groups[i] = *null_group;
      } else {
        bool inserted;
        size_t* entry = group_map.TryEmplace(key.value, &inserted);
        if (inserted) {
          *entry = groups.size();
          Value group(key.value);
          group.type = group_val_type;
          groups.push_back(std::move(group));
        }
        row_groups[i] = *entry;
      }
    }
  }
  const size_t total_groups =
      group_column < 0 ? 1
                       : (identity_pk_groups ? dimension_rows : groups.size());
  const auto group_key_value = [&](size_t g) -> Value {
    if (group_column < 0) {
      return Value();
    }
    if (!groups.empty()) {
      return groups[g];
    }
    const auto cell = dimension_cell(g, static_cast<size_t>(group_column));
    if (cell.null) {
      return Value();
    }
    Value v(cell.value);
    v.type = group_val_type;
    return v;
  };
  // A bounded dense, unique key domain needs no hash-bucket chain per
  // fact row. Duplicates and wide/sparse domains retain the general buckets.
  constexpr size_t kNoDenseRow = std::numeric_limits<size_t>::max();
  std::vector<size_t> dense_rows;
  int64_t dense_min = 0;
  size_t domain_span = 0;
  if (minimum) {
    const __int128_t span = static_cast<__int128_t>(*maximum) - *minimum + 1;
    const __int128_t max_span =
        (is_left_join || count_probe_col.has_value()) ? 500000 : 65536;
    if (span <= max_span &&
        (span <= 65536 ||
         span <= static_cast<__int128_t>(dimension_rows) * 4)) {
      dense_min = *minimum;
      domain_span = static_cast<size_t>(span);
    }
  }
  const bool allow_key_counts = frequency_fold && domain_span != 0 &&
                                !is_left_join && probe_like_filters.empty() &&
                                probe_int_filters.empty() &&
                                !count_probe_col.has_value();
  std::shared_ptr<const Table::IntegerKeyCounts> key_counts;
  auto counts_charge = allow_key_counts ? TryReserveQueryMemory(1024 * 1024)
                                        : decltype(TryReserveQueryMemory(0)){};
  if (counts_charge)
    key_counts = tables[probe]->FindCachedIntegerKeyCounts(
        context.txn_, projections[probe][keys[0].left], dense_min, domain_span);
  // An exact fact histogram can be folded once for each dimension row,
  // including duplicates. Only the ordinary scan needs a unique key->row map.
  const bool identity_dense_rows =
      build_is_identity_1_to_n && identity_pk_groups &&
      domain_span == dimension_rows && dense_min == 1;
  if (!key_counts && domain_span != 0 && !identity_dense_rows) {
    dense_rows.assign(domain_span, kNoDenseRow);
    for (size_t i = 0; i < dimension_rows; ++i) {
      const auto key = dimension_cell(i, keys[0].right);
      if (key.null) continue;
      const size_t slot =
          static_cast<uint64_t>(key.value) - static_cast<uint64_t>(dense_min);
      if (dense_rows[slot] != kNoDenseRow) {
        dense_rows.clear();
        break;
      }
      dense_rows[slot] = i;
    }
  }
  const bool has_dense_rows = identity_dense_rows || !dense_rows.empty();
  frequency_fold = frequency_fold && (key_counts || has_dense_rows);
  if ((is_left_join || !probe_like_filters.empty() ||
       !probe_int_filters.empty() || count_probe_col.has_value()) &&
      !frequency_fold) {
    return decline();
  }

  std::shared_ptr<const LSTableStore> probe_lstable_store;
  size_t lstable_blocks_per_morsel = 32;
  if ((context.txn_.IsReadOnly() || context.txn_.WriteSetEmpty()) &&
      !context.txn_.HasAnyPageVersionChains() &&
      (frequency_fold ||
       (probe_like_filters.empty() && probe_int_filters.empty())) &&
      !key_counts && has_dense_rows) {
    std::shared_ptr<const LSTableStore> store =
        GetOrBuildLSTableStore(context.txn_, *tables[probe], true);
    if (store != nullptr && store->uncommitted_owner_txn == 0 &&
        store->layout_generation == tables[probe]->LayoutGeneration() &&
        store->first_pid == tables[probe]->FirstPageId()) {
      probe_lstable_store = std::move(store);
      const size_t total_pages = probe_lstable_store->page_ids.size();
      lstable_blocks_per_morsel = std::clamp<size_t>(total_pages / 64, 1, 32);
      const size_t morsel_count =
          (total_pages + lstable_blocks_per_morsel - 1) /
          lstable_blocks_per_morsel;
      if (!context.txn_.HasAnyPageVersionChains() &&
          probe_lstable_store->blocks.size() == total_pages) {
        morsels.resize(morsel_count);
      } else {
        morsels.reserve(morsel_count);
        for (size_t i = 0; i < total_pages; i += lstable_blocks_per_morsel) {
          const size_t end =
              std::min(total_pages, i + lstable_blocks_per_morsel);
          morsels.emplace_back(probe_lstable_store->page_ids.begin() +
                                   static_cast<std::ptrdiff_t>(i),
                               probe_lstable_store->page_ids.begin() +
                                   static_cast<std::ptrdiff_t>(end));
        }
      }
    }
  }

  // The general signed/probe fold uses canonical Rows in physical order.
  // Box the columnar input only when it actually needs that fallback.
  if (typed_dimension && !frequency_fold && probe_lstable_store == nullptr) {
    for (size_t i = 0; i < dimension_rows; ++i) {
      Row row;
      row.values_.reserve(dimension_stride);
      for (size_t c = 0; c < dimension_stride; ++c) {
        const auto cell = dimension_cell(i, c);
        Value value = cell.null ? Value() : Value(cell.value);
        if (!cell.null) value.type = build_schema.GetColumn(c).Type();
        row.values_.push_back(std::move(value));
      }
      dimension.AddRow(std::move(row));
    }
    RETURN_IF_FAIL(dimension.FinishSpill());
    RETURN_IF_FAIL(dimension.GetStatus());
    if (dimension.HasSpill()) return decline();
  }
  if (!has_dense_rows && !key_counts) {
    buckets.reserve(dimension_rows);
    for (const Row& row : dimension.rows)
      if (!row[keys[0].right].IsNull())
        buckets.emplace(row[keys[0].right].value.int_value, &row);
  }
  std::vector<size_t> dense_groups;
  std::vector<uint8_t> dense_match_mask;
  if (!frequency_fold && !dense_rows.empty()) {
    dense_groups.resize(dense_rows.size(), kNoDenseRow);
    if (total_groups == 1) {
      dense_match_mask.assign(dense_rows.size(), 0);
    }
    for (size_t s = 0; s < dense_rows.size(); ++s) {
      const size_t r = dense_rows[s];
      if (r != kNoDenseRow) {
        dense_groups[s] = identity_pk_groups ? r : row_groups[r];
        if (total_groups == 1) {
          dense_match_mask[s] = 1;
        }
      }
    }
  }
  if (!key_counts && probe_lstable_store == nullptr) {
    morsels = tables[probe]->BuildScanMorsels(context.txn_, 8);
  }

  struct alignas(64) Partial {
    __int128_t sum{0}, low{0}, high{0};
    uint64_t count{0}, pairs{0};
  };
  const size_t width = arguments.size();
  const size_t workers = QueryParallelWorkerLimit(std::max<size_t>(
      1,
      std::min<size_t>({probe_lstable_store != nullptr ? size_t{16} : size_t{8},
                        morsels.size(), std::thread::hardware_concurrency()})));
  bool direct_emit = true;
  std::vector<int32_t> direct_agg_slot(fusion->select_items.size(), -1);
  for (size_t s = 0; s < fusion->select_items.size(); ++s) {
    const Expression& item = fusion->select_items[s];
    if (!item) {
      direct_emit = false;
      break;
    }
    if (group_column >= 0 && item->Type() == TypeTag::kColumnValue &&
        joined.Offset(item->AsColumnValue().GetColumnName()) ==
            static_cast<int>(probe_schema.ColumnCount()) + group_column) {
      continue;
    }
    if (item->Type() == TypeTag::kAggregateExp) {
      const AggregateExpression* agg = &item->AsAggregateExpression();
      int32_t found = -1;
      for (size_t a = 0; a < width; ++a) {
        if (fusion->aggregates[a] == agg) {
          found = static_cast<int32_t>(a);
          break;
        }
      }
      if (found >= 0) {
        direct_agg_slot[s] = found;
        continue;
      }
    }
    direct_emit = false;
    break;
  }
  const bool direct_freq_count_emit =
      identity_pk_groups && frequency_fold && direct_emit && width == 1 &&
      fusion->aggregates[0]->GetType() == AggregationType::kCount &&
      (arguments[0] < 0 ||
       static_cast<size_t>(arguments[0]) < probe_schema.ColumnCount());
  // Frequency folding is order-independent and accumulates into a shared
  // atomic frequency table, then folds once into a single partial table.
  // Only the general signed fold needs a table per ordered morsel.
  const size_t partial_slots = frequency_fold ? 1 : morsels.size();
  size_t per_morsel = 0, partial_count = 0, partial_bytes = 0;
  if (!direct_freq_count_emit) {
    if (__builtin_mul_overflow(total_groups, width, &per_morsel) ||
        __builtin_mul_overflow(partial_slots, per_morsel, &partial_count) ||
        __builtin_mul_overflow(partial_count, sizeof(Partial), &partial_bytes))
      return decline();
  }
  size_t scan_bytes, bytes;
  if (__builtin_mul_overflow(workers * 1024 * 9, probe_schema.ColumnCount(),
                             &scan_bytes) ||
      __builtin_add_overflow(partial_bytes, scan_bytes, &bytes))
    return decline();
  if (frequency_fold) {
    size_t frequency_bytes;
    const size_t freq_per_row =
        (probe_lstable_store != nullptr && dimension_rows <= 500000)
            ? (workers * sizeof(uint32_t))
            : sizeof(uint64_t);
    if (__builtin_mul_overflow(dimension_rows, freq_per_row,
                               &frequency_bytes) ||
        __builtin_add_overflow(bytes, frequency_bytes, &bytes))
      return decline();
  }
  auto partial_charge = TryReserveQueryMemory(bytes);
  if (!partial_charge) return decline();
  if (!key_counts && probe_lstable_store == nullptr && frequency_fold &&
      counts_charge && !morsels.empty())
    key_counts = tables[probe]->TryReadIntegerKeyCounts(
        context.txn_, morsels, projections[probe][keys[0].left], dense_min,
        domain_span);
  if (probe_lstable_store == nullptr &&
      (context.txn_.IsReadOnly() || context.txn_.WriteSetEmpty()) &&
      frequency_fold && !key_counts && has_dense_rows) {
    std::shared_ptr<const LSTableStore> store =
        GetOrBuildLSTableStore(context.txn_, *tables[probe], true);
    if (store != nullptr && store->uncommitted_owner_txn == 0 &&
        store->layout_generation == tables[probe]->LayoutGeneration() &&
        store->first_pid == tables[probe]->FirstPageId()) {
      bool valid = true;
      if (context.txn_.HasAnyPageVersionChains()) {
        for (const auto& morsel : morsels) {
          for (const page_id_t pid : morsel) {
            if (context.txn_.HasPageVersionChains(pid) ||
                store->FindBlock(pid) == nullptr) {
              valid = false;
              break;
            }
          }
          if (!valid) break;
        }
      }
      if (valid) {
        probe_lstable_store = std::move(store);
      }
    }
  }
  const bool use_worker_freqs = probe_lstable_store != nullptr &&
                                frequency_fold && !key_counts &&
                                dimension_rows <= 500000;
  const bool use_atomic_u32_freqs =
      probe_lstable_store != nullptr && frequency_fold && !key_counts &&
      !use_worker_freqs &&
      probe_lstable_store->total_rows < std::numeric_limits<uint32_t>::max();
  std::vector<Partial> partials(direct_freq_count_emit ? 0 : partial_count);
  std::vector<uint64_t> shared_frequencies(frequency_fold && !key_counts &&
                                                   !use_worker_freqs &&
                                                   !use_atomic_u32_freqs
                                               ? dimension_rows
                                               : 0,
                                           0);
  std::vector<uint32_t> shared_u32_frequencies(
      use_atomic_u32_freqs ? dimension_rows : 0, 0);
  std::vector<std::vector<uint32_t>> worker_frequencies(
      use_worker_freqs ? workers : 0);
  const slot_t full_probe_key_col = projections[probe][keys[0].left];
  const std::optional<slot_t> full_count_probe_col =
      count_probe_col.has_value()
          ? std::optional<slot_t>(projections[probe][*count_probe_col])
          : std::nullopt;
  std::atomic<size_t> next{0};
  std::atomic<bool> failed{false};
  std::mutex error_mutex;
  Status error = Status::kSuccess;
  const size_t scan_slots =
      std::max(morsels.size(), key_counts ? size_t{1} : size_t{0});
  std::vector<size_t> visible(scan_slots, 0), comparisons(scan_slots, 0);
  const auto fail = [&](Status status) {
    std::scoped_lock lock(error_mutex);
    if (!failed.load()) {
      error = std::move(status);
      failed.store(true);
    }
  };
  const auto run = [&](size_t worker) {
    try {
      uint32_t* local_freq = nullptr;
      if (use_worker_freqs) {
        worker_frequencies[worker].assign(dimension_rows, 0);
        local_freq = worker_frequencies[worker].data();
      }
      TypedCellBatch batch;
      if (key_counts) {
        visible[0] = key_counts->rows;
        next.store(morsels.size());
      }
      while (!failed.load()) {
        const size_t m = next.fetch_add(1);
        if (m >= morsels.size()) break;
        if (probe_lstable_store != nullptr) {
          const bool direct_block_index =
              !context.txn_.HasAnyPageVersionChains() &&
              probe_lstable_store->blocks.size() ==
                  probe_lstable_store->page_ids.size();
          const size_t block_count =
              direct_block_index
                  ? std::min<size_t>(lstable_blocks_per_morsel,
                                     probe_lstable_store->blocks.size() -
                                         m * lstable_blocks_per_morsel)
                  : morsels[m].size();
          for (size_t bi = 0; bi < block_count; ++bi) {
            const LSTablePageBlock* blk =
                direct_block_index
                    ? &probe_lstable_store
                           ->blocks[m * lstable_blocks_per_morsel + bi]
                    : probe_lstable_store->FindBlock(morsels[m][bi]);
            if (blk == nullptr || blk->row_count == 0) continue;
            visible[m] += blk->row_count;
            const LSTableColumnBlock& kcol = blk->columns[full_probe_key_col];
            if (!frequency_fold) {
              Partial* __restrict morsel_partials = &partials[m * per_morsel];
              if (!blk->has_any_null && total_groups == 1 &&
                  !dense_match_mask.empty() && width == 2 && arguments[0] < 0 &&
                  arguments[1] >= 0 &&
                  static_cast<size_t>(arguments[1]) <
                      probe_schema.ColumnCount() &&
                  fusion->aggregates[1]->GetType() == AggregationType::kSum) {
                const slot_t full_val_col =
                    projections[probe][static_cast<size_t>(arguments[1])];
                const LSTableColumnBlock& vcol = blk->columns[full_val_col];
                if (vcol.has_min_max && vcol.min_int >= 0) {
                  const int64_t* __restrict kcells = kcol.int_cells.data();
                  const int64_t* __restrict vcells = vcol.int_cells.data();
                  const uint8_t* __restrict dmask = dense_match_mask.data();
                  const size_t dsize = dense_match_mask.size();
                  const uint32_t nrows = blk->row_count;
                  uint64_t blk_matched = 0;
                  int64_t blk_sum = 0;
                  for (uint32_t r = 0; r < nrows; ++r) {
                    const uint64_t offset = static_cast<uint64_t>(kcells[r]) -
                                            static_cast<uint64_t>(dense_min);
                    if (offset < dsize && dmask[offset] != 0) {
                      ++blk_matched;
                      blk_sum += vcells[r];
                    }
                  }
                  morsel_partials[0].pairs += blk_matched;
                  morsel_partials[0].count += blk_matched;
                  morsel_partials[1].count += blk_matched;
                  morsel_partials[1].sum += blk_sum;
                  morsel_partials[1].high =
                      std::max(morsel_partials[1].high, morsel_partials[1].sum);
                  comparisons[m] += blk_matched;
                  continue;
                }
              }
              if (!blk->has_any_null && width == 1 && arguments[0] >= 0 &&
                  static_cast<size_t>(arguments[0]) <
                      probe_schema.ColumnCount() &&
                  fusion->aggregates[0]->GetType() == AggregationType::kSum) {
                const slot_t full_val_col =
                    projections[probe][static_cast<size_t>(arguments[0])];
                const LSTableColumnBlock& vcol = blk->columns[full_val_col];
                const int64_t* __restrict kcells = kcol.int_cells.data();
                const int64_t* __restrict vcells = vcol.int_cells.data();
                const uint32_t nrows = blk->row_count;
                size_t blk_matched = 0;
                if (vcol.has_min_max && vcol.min_int >= 0 &&
                    !dense_groups.empty()) {
                  const size_t* __restrict dgroups = dense_groups.data();
                  const size_t dsize = dense_groups.size();
                  for (uint32_t r = 0; r < nrows; ++r) {
                    const uint64_t offset = static_cast<uint64_t>(kcells[r]) -
                                            static_cast<uint64_t>(dense_min);
                    if (offset >= dsize) continue;
                    const size_t g = dgroups[offset];
                    if (g == kNoDenseRow) continue;
                    ++blk_matched;
                    Partial& partial = morsel_partials[g];
                    ++partial.pairs;
                    ++partial.count;
                    partial.sum += vcells[r];
                  }
                  for (size_t g = 0; g < total_groups; ++g) {
                    Partial& partial = morsel_partials[g];
                    partial.high = std::max(partial.high, partial.sum);
                  }
                  comparisons[m] += blk_matched;
                  continue;
                }
              }
              for (uint32_t r = 0; r < blk->row_count; ++r) {
                if (kcol.IsNull(r)) continue;
                const int64_t key = kcol.int_cells[r];
                const uint64_t offset = static_cast<uint64_t>(key) -
                                        static_cast<uint64_t>(dense_min);
                size_t target_row = 0;
                if (identity_dense_rows) {
                  if (offset >= dimension_rows) continue;
                  target_row = static_cast<size_t>(offset);
                } else {
                  if (offset >= dense_rows.size() ||
                      dense_rows[offset] == kNoDenseRow)
                    continue;
                  target_row = dense_rows[offset];
                }
                ++comparisons[m];
                const size_t g =
                    identity_pk_groups ? target_row : row_groups[target_row];
                ++morsel_partials[g * width].pairs;
                for (size_t a = 0; a < width; ++a) {
                  Partial& partial = morsel_partials[g * width + a];
                  if (arguments[a] < 0) {
                    ++partial.count;
                    continue;
                  }
                  const size_t column = static_cast<size_t>(arguments[a]);
                  int64_t value = 0;
                  if (column < probe_schema.ColumnCount()) {
                    const LSTableColumnBlock& acol =
                        blk->columns[projections[probe][column]];
                    if (acol.IsNull(r)) continue;
                    value = acol.int_cells[r];
                  } else {
                    const auto cell = dimension_cell(
                        target_row, column - probe_schema.ColumnCount());
                    if (cell.null) continue;
                    value = cell.value;
                  }
                  ++partial.count;
                  if (fusion->aggregates[a]->GetType() ==
                      AggregationType::kSum) {
                    partial.sum += value;
                    partial.low = std::min(partial.low, partial.sum);
                    partial.high = std::max(partial.high, partial.sum);
                  }
                }
              }
              continue;
            }
            if (!probe_int_peeks.empty() &&
                !blk->MatchesZoneMaps(&probe_int_peeks, nullptr,
                                      std::nullopt)) {
              continue;
            }
            if (!blk->has_any_null && probe_like_filters.empty() &&
                probe_int_filters.size() == 1 && local_freq != nullptr &&
                !identity_dense_rows) {
              const ProbeIntFilter& iflt = probe_int_filters[0];
              const int64_t* __restrict kcells = kcol.int_cells.data();
              const int64_t* __restrict fcells =
                  blk->columns[iflt.full_col].int_cells.data();
              const size_t* __restrict drows = dense_rows.data();
              const size_t dsize = dense_rows.size();
              const uint32_t nrows = blk->row_count;
              size_t blk_matched = 0;
              for (uint32_t r = 0; r < nrows; ++r) {
                if (!MatchIntegerCompare(fcells[r], iflt.op, iflt.constant)) {
                  continue;
                }
                const uint64_t offset = static_cast<uint64_t>(kcells[r]) -
                                        static_cast<uint64_t>(dense_min);
                if (offset >= dsize) continue;
                const size_t target_row = drows[offset];
                if (target_row == kNoDenseRow) continue;
                ++local_freq[target_row];
                ++blk_matched;
              }
              comparisons[m] += blk_matched;
              continue;
            }
            for (uint32_t r = 0; r < blk->row_count; ++r) {
              if (kcol.IsNull(r)) continue;
              const int64_t key = kcol.int_cells[r];
              const uint64_t offset =
                  static_cast<uint64_t>(key) - static_cast<uint64_t>(dense_min);
              size_t target_row = 0;
              if (identity_dense_rows) {
                if (offset >= dimension_rows) continue;
                target_row = static_cast<size_t>(offset);
              } else {
                if (offset >= dense_rows.size() ||
                    dense_rows[offset] == kNoDenseRow)
                  continue;
                target_row = dense_rows[offset];
              }
              if (full_count_probe_col.has_value() &&
                  blk->columns[*full_count_probe_col].IsNull(r))
                continue;
              if (!probe_int_filters.empty()) {
                bool filter_ok = true;
                for (const ProbeIntFilter& iflt : probe_int_filters) {
                  const LSTableColumnBlock& pcol = blk->columns[iflt.full_col];
                  if (pcol.IsNull(r) ||
                      !MatchIntegerCompare(pcol.int_cells[r], iflt.op,
                                           iflt.constant)) {
                    filter_ok = false;
                    break;
                  }
                }
                if (!filter_ok) continue;
              }
              if (!probe_like_filters.empty()) {
                bool filter_ok = true;
                for (const ProbeLikeFilter& lf : probe_like_filters) {
                  const LSTableColumnBlock& pcol = blk->columns[lf.full_col];
                  if (pcol.IsNull(r)) {
                    filter_ok = false;
                    break;
                  }
                  const bool matched = match_probe_like(pcol.VarcharAt(r), lf);
                  if ((lf.negated ? !matched : matched) == false) {
                    filter_ok = false;
                    break;
                  }
                }
                if (!filter_ok) continue;
              }
              if (local_freq != nullptr) {
                ++local_freq[target_row];
              } else if (use_atomic_u32_freqs) {
                std::atomic_ref<uint32_t>(shared_u32_frequencies[target_row])
                    .fetch_add(1U, std::memory_order_relaxed);
              } else {
                std::atomic_ref<uint64_t>(shared_frequencies[target_row])
                    .fetch_add(1ULL, std::memory_order_relaxed);
              }
              ++comparisons[m];
            }
          }
          continue;
        }
        Iterator iterator = tables[probe]->BeginMorselScan(
            context.txn_, morsels[m], projections[probe]);
        while (!failed.load()) {
          const size_t rows = iterator.DecodeNextTypedBatch(&batch, 1024);
          iterator.DropPageLatch();
          if (rows == 0) break;
          visible[m] += rows;
          for (size_t r = 0; r < rows; ++r) {
            const size_t base = r * batch.stride;
            if (batch.nulls[base + keys[0].left]) continue;
            if (count_probe_col.has_value() &&
                batch.nulls[base + *count_probe_col])
              continue;
            if (!probe_int_filters.empty()) {
              bool filter_ok = true;
              for (const ProbeIntFilter& iflt : probe_int_filters) {
                if (batch.nulls[base + iflt.probe_col] ||
                    !MatchIntegerCompare(batch.cells[base + iflt.probe_col],
                                         iflt.op, iflt.constant)) {
                  filter_ok = false;
                  break;
                }
              }
              if (!filter_ok) continue;
            }
            if (!probe_like_filters.empty()) {
              bool filter_ok = true;
              for (const ProbeLikeFilter& lf : probe_like_filters) {
                if (batch.nulls[base + lf.probe_col]) {
                  filter_ok = false;
                  break;
                }
                const bool matched =
                    match_probe_like(batch.Varchar(r, lf.varchar_slot), lf);
                if ((lf.negated ? !matched : matched) == false) {
                  filter_ok = false;
                  break;
                }
              }
              if (!filter_ok) continue;
            }
            const int64_t key = batch.cells[base + keys[0].left];
            const auto accumulate = [&](const Row& build_row) {
              ++comparisons[m];
              const size_t row_idx =
                  static_cast<size_t>(&build_row - dimension.rows.data());
              const size_t g =
                  identity_pk_groups ? row_idx : row_groups[row_idx];
              ++partials[m * per_morsel + g * width].pairs;
              for (size_t a = 0; a < width; ++a) {
                Partial& partial = partials[m * per_morsel + g * width + a];
                if (arguments[a] < 0) {
                  ++partial.count;
                  continue;
                }
                const size_t column = static_cast<size_t>(arguments[a]);
                int64_t value;
                if (column < batch.stride) {
                  if (batch.nulls[base + column]) continue;
                  value = batch.cells[base + column];
                } else {
                  const Value& cell = build_row[column - batch.stride];
                  if (cell.IsNull()) continue;
                  value = cell.value.int_value;
                }
                ++partial.count;
                if (fusion->aggregates[a]->GetType() == AggregationType::kSum) {
                  partial.sum += value;
                  partial.low = std::min(partial.low, partial.sum);
                  partial.high = std::max(partial.high, partial.sum);
                }
              }
            };
            if (!dense_rows.empty()) {
              const uint64_t offset =
                  static_cast<uint64_t>(key) - static_cast<uint64_t>(dense_min);
              if (offset >= dense_rows.size() ||
                  dense_rows[offset] == kNoDenseRow)
                continue;
              const size_t index = dense_rows[offset];
              if (frequency_fold) {
                std::atomic_ref<uint64_t>(shared_frequencies[index])
                    .fetch_add(1, std::memory_order_relaxed);
                ++comparisons[m];
              } else {
                accumulate(dimension.rows[index]);
              }
            } else {
              const auto [first, last] = buckets.equal_range(key);
              for (auto found = first; found != last; ++found)
                accumulate(*found->second);
            }
          }
        }
        if (!iterator.GetStatus().ok())
          fail(iterator.GetStatus());
        else if (iterator.IsValid() && !failed.load())
          fail(StatusError(StatusCode::kRuntimeError,
                           "typed join scan stopped before exhaustion"));
      }
    } catch (const std::exception& exception) {
      fail(StatusError(StatusCode::kRuntimeError, exception.what()));
    } catch (...) {
      fail(StatusError(StatusCode::kRuntimeError, "stream join worker failed"));
    }
  };
  const auto started = std::chrono::steady_clock::now();
  std::vector<std::jthread> threads;
  try {
    if (!key_counts)
      for (size_t w = 1; w < workers; ++w) threads.emplace_back(run, w);
    run(0);
  } catch (const std::exception& exception) {
    fail(StatusError(StatusCode::kRuntimeError, exception.what()));
  }
  threads.clear();
  RETURN_IF_FAIL(error);
  const auto total_freq_of = [&](size_t i) -> uint64_t {
    if (key_counts) {
      const auto key = dimension_cell(i, keys[0].right);
      if (key.null) return 0;
      const size_t offset =
          static_cast<uint64_t>(key.value) - static_cast<uint64_t>(dense_min);
      return key_counts->counts[offset];
    }
    if (use_worker_freqs) {
      uint64_t c = 0;
      for (size_t w = 0; w < workers; ++w) {
        if (!worker_frequencies[w].empty()) {
          c += worker_frequencies[w][i];
        }
      }
      return c;
    }
    if (use_atomic_u32_freqs) {
      return shared_u32_frequencies[i];
    }
    return shared_frequencies[i];
  };
  Relation result(context.execution_runtime());
  std::vector<Column> output_columns;
  for (const std::string& name : fusion->select_names)
    output_columns.emplace_back(name, ValueType::kNull);
  result.rows.reserve(total_groups);
  if (direct_freq_count_emit && fusion->rollup_count_distribution &&
      fusion->rollup_select_names.size() == 2) {
    FlatIntKeyMap<int64_t> dist_map;
    dist_map.Reserve(64);
    for (size_t i = 0; i < dimension_rows; ++i) {
      const uint64_t count = total_freq_of(i);
      if (key_counts) {
        comparisons[0] += count;
      }
      if (!is_left_join && count == 0) continue;
      if (count > static_cast<uint64_t>(std::numeric_limits<int64_t>::max()))
        return StatusError(StatusCode::kIsInfinity, "integer count overflow");
      bool inserted = false;
      int64_t* slot =
          dist_map.TryEmplace(static_cast<int64_t>(count), &inserted);
      if (inserted) {
        *slot = 1;
      } else {
        *slot += 1;
      }
    }
    output_columns.clear();
    for (const std::string& name : fusion->rollup_select_names) {
      output_columns.emplace_back(name, ValueType::kInt64);
    }
    result.rows.reserve(dist_map.Size());
    dist_map.ForEach([&](int64_t c_count, int64_t custdist) {
      Row output;
      output.values_.resize(2);
      if (fusion->rollup_count_col_first) {
        output.values_[0] = Value(c_count);
        output.values_[1] = Value(custdist);
      } else {
        output.values_[0] = Value(custdist);
        output.values_[1] = Value(c_count);
      }
      result.AddRow(std::move(output));
    });
    fusion->rollup_distribution_applied = true;
  } else if (direct_freq_count_emit) {
    const size_t n_select = fusion->select_items.size();
    for (size_t i = 0; i < dimension_rows; ++i) {
      const uint64_t count = total_freq_of(i);
      if (key_counts) {
        comparisons[0] += count;
      }
      if (!is_left_join && count == 0) continue;
      if (count > static_cast<uint64_t>(std::numeric_limits<int64_t>::max()))
        return StatusError(StatusCode::kIsInfinity, "integer count overflow");
      Row output;
      output.values_.resize(n_select);
      const Value cnt_val(static_cast<int64_t>(count));
      for (size_t s = 0; s < n_select; ++s) {
        output.values_[s] =
            direct_agg_slot[s] >= 0 ? cnt_val : group_key_value(i);
      }
      result.AddRow(std::move(output));
    }
  } else {
    if (frequency_fold) {
      for (size_t i = 0; i < dimension_rows; ++i) {
        const uint64_t count = total_freq_of(i);
        if (key_counts) {
          comparisons[0] += count;
        }
        const size_t g = identity_pk_groups ? i : row_groups[i];
        if (is_left_join) {
          partials[g * width].pairs = 1;
        }
        if (count == 0) continue;
        partials[g * width].pairs += count;
        for (size_t a = 0; a < width; ++a) {
          Partial& partial = partials[g * width + a];
          if (arguments[a] < 0 ||
              static_cast<size_t>(arguments[a]) < probe_schema.ColumnCount()) {
            partial.count += count;
            continue;
          }
          const auto cell =
              dimension_cell(i, static_cast<size_t>(arguments[a]) -
                                    probe_schema.ColumnCount());
          if (cell.null) continue;
          partial.count += count;
          if (fusion->aggregates[a]->GetType() == AggregationType::kSum) {
            partial.sum += static_cast<__int128_t>(cell.value) * count;
            partial.high = partial.sum;
          }
        }
      }
    }
    std::vector<Value> finished_aggs(width);
    for (size_t g = 0; g < total_groups; ++g) {
      bool has_pairs = is_left_join || group_column < 0;
      for (size_t m = 0; m < partial_slots && !has_pairs; ++m)
        has_pairs = partials[m * per_morsel + g * width].pairs != 0;
      if (!has_pairs) continue;
      for (size_t a = 0; a < width; ++a) {
        __int128_t sum = 0;
        uint64_t count = 0;
        for (size_t m = 0; m < partial_slots; ++m) {
          const Partial& partial = partials[m * per_morsel + g * width + a];
          if (sum + partial.low < std::numeric_limits<int64_t>::min() ||
              sum + partial.high > std::numeric_limits<int64_t>::max())
            return StatusError(StatusCode::kIsInfinity,
                               "integer overflow on '+'");
          sum += partial.sum;
          count += partial.count;
        }
        if (count > static_cast<uint64_t>(std::numeric_limits<int64_t>::max()))
          return StatusError(StatusCode::kIsInfinity, "integer count overflow");
        finished_aggs[a] =
            fusion->aggregates[a]->GetType() == AggregationType::kCount
                ? Value(static_cast<int64_t>(count))
            : count == 0 ? Value()
                         : Value(static_cast<int64_t>(sum));
      }
      if (direct_emit) {
        Row output;
        output.values_.resize(fusion->select_items.size());
        const Value gval = group_key_value(g);
        for (size_t s = 0; s < fusion->select_items.size(); ++s) {
          output.values_[s] =
              direct_agg_slot[s] >= 0
                  ? finished_aggs[static_cast<size_t>(direct_agg_slot[s])]
                  : gval;
        }
        result.AddRow(std::move(output));
        continue;
      }
      AggregateResultMap aggregate_results;
      for (size_t a = 0; a < width; ++a) {
        aggregate_results.emplace(fusion->aggregates[a], finished_aggs[a]);
      }
      Row representative(std::vector<Value>(joined.ColumnCount()));
      if (group_column >= 0) {
        representative.values_[probe_schema.ColumnCount() +
                               static_cast<size_t>(group_column)] =
            group_key_value(g);
      }
      Scope scope{.row = &representative, .schema = &joined, .outer = outer};
      Row output;
      for (const Expression& item : fusion->select_items) {
        ASSIGN_OR_RETURN(
            Value, value,
            TryEvaluate(item, scope, &aggregate_results, context, ctes));
        output.values_.push_back(std::move(value));
      }
      result.AddRow(std::move(output));
    }
  }
  if (!result.rows.empty())
    for (size_t i = 0; i < output_columns.size(); ++i) {
      output_columns[i] =
          Column(output_columns[i].Name(), ValueTypeOf(result.rows[0][i]));
    }
  result.schema = Schema("", std::move(output_columns));
  result.hash_joins = result.in_memory_hash_joins = 1;
  for (size_t count : comparisons) result.join_comparisons += count;
  if (auto* runtime = context.execution_runtime()) {
    runtime->join_ms += ElapsedMs(started);
    ++runtime->grouped_join_fusions;
    ++runtime->streamed_grouped_joins;
    for (size_t count : visible) runtime->scan_rows += count;
    runtime->scan_output_rows += result.join_comparisons;
  }
  fusion->fired = true;
  return std::optional<Relation>(std::move(result));
}

}  // namespace tinylamb::relational_detail
