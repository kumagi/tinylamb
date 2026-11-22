/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <iostream>
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
#include "common/parallel_work.hpp"
#include "common/platform/int128_conversion.hpp"
#include "common/platform/range_selection.hpp"
#include "common/query_parallelism.hpp"
#include "common/runtime_filter.hpp"
#include "common/status_or.hpp"
#include "common/string_util.hpp"
#include "database/transaction_context.hpp"
#include "executor/detail/columnar_table.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/detail/flat_int_key_map.hpp"
#include "executor/detail/fused_grouped_join.hpp"
#include "executor/detail/fused_grouped_join_internal.hpp"
#include "executor/detail/planning_heuristics.hpp"
#include "executor/detail/relation.hpp"
#include "executor/detail/relational_sort_limit.hpp"
#include "executor/detail/scan_filter.hpp"
#include "executor/detail/subquery_runtime.hpp"
#include "executor/detail/typed_stream_aggregate_internal.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/binary_expression.hpp"
#include "expression/bytecode.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/jit.hpp"
#include "expression/parameter_slot.hpp"
#include "expression/rewrite.hpp"
#include "query/statement.hpp"
#include "table/lstable_bridge.hpp"
#include "table/table.hpp"
#include "table/table_statistics.hpp"
#include "type/column_name.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace {

[[gnu::noinline]] size_t PrefilterDenseRfBlock(
    const int64_t* __restrict__ vals, uint32_t nrows, int64_t rf_lo,
    int64_t rf_hi, const uint64_t* __restrict__ bits, bool all_bits_set,
    const tinylamb::LSTableColumnBlock* rf_col, bool has_null,
    uint32_t* __restrict__ out_sel) noexcept {
  const auto lo_u = static_cast<uint64_t>(rf_lo);
  const uint64_t span_u = static_cast<uint64_t>(rf_hi) - lo_u;
  size_t n_sel = 0;
  if (all_bits_set) {
    uint32_t selected = tinylamb::platform::SelectInt64ClosedRange(
        vals, nrows, rf_lo, rf_hi, out_sel);
    if (has_null) {
      uint32_t nonnull = 0;
      for (uint32_t i = 0; i < selected; ++i) {
        if (!rf_col->IsNull(out_sel[i])) {
          out_sel[nonnull++] = out_sel[i];
        }
      }
      selected = nonnull;
    }
    return selected;
  }
  if (!has_null) {
    for (uint32_t r = 0; r < nrows; ++r) {
      const uint64_t off = static_cast<uint64_t>(vals[r]) - lo_u;
      if (off <= span_u && ((bits[off >> 6] >> (off & 63)) & 1ULL) != 0)
          [[unlikely]] {
        out_sel[n_sel++] = r;
      }
    }
  } else {
    for (uint32_t r = 0; r < nrows; ++r) {
      const uint64_t off = static_cast<uint64_t>(vals[r]) - lo_u;
      if (off <= span_u && ((bits[off >> 6] >> (off & 63)) & 1ULL) != 0)
          [[unlikely]] {
        if (!rf_col->IsNull(r)) {
          out_sel[n_sel++] = r;
        }
      }
    }
  }
  return n_sel;
}

}  // namespace

namespace tinylamb::relational_detail {
StatusOr<std::optional<Relation>> TryStreamGroupedBaseJoin(
    TransactionContext& context, const SelectStatement& statement,
    const Scope* outer, const CteMap& ctes,
    const std::vector<std::vector<slot_t>>& projections,
    const std::vector<Relation>& schemas, GroupedJoinFusionSpec* fusion) {
  const auto decline = [] { return std::optional<Relation>{}; };
  if ((fusion == nullptr) || !fusion->eligible || fusion->having ||
      statement.Sources().size() != 2 || (outer != nullptr) || !ctes.empty() ||
      fusion->group_keys.size() > 1 || fusion->aggregates.empty() ||
      (std::getenv("TINYLAMB_DISABLE_FUSED_STREAM_GROUPED_JOIN") != nullptr)) {
    return decline();
  }
  for (size_t i = 0; i < 2; ++i) {
    const auto& source = statement.Sources()[i];
    if (source.query || source.unnest || source.sample || source.is_lateral ||
        source.from_nested_join || !source.using_columns.empty()) {
      return decline();
    }
    if (i == 0 && source.join_type != JoinType::kInner &&
        source.join_type != JoinType::kCross) {
      return decline();
    }
    if (i == 1 && source.join_type != JoinType::kInner &&
        source.join_type != JoinType::kCross &&
        source.join_type != JoinType::kLeft) {
      return decline();
    }
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
      fusion->group_keys[0]->Type() != TypeTag::kColumnValue) {
    return decline();
  }
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
  if (is_left_join && estimates[0] > 500000) {
    return decline();
  }
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
    if (group_column < 0) {
      return decline();
    }
    const auto& group_type =
        build_schema.GetColumn(static_cast<size_t>(group_column));
    if (group_type.IsUnsigned() || (group_type.Type() != ValueType::kInt64 &&
                                    group_type.Type() != ValueType::kDate)) {
      return decline();
    }
    group_val_type = group_type.Type();
  }
  auto all_predicates = SplitConjuncts(statement.Sources()[1].join_condition);
  if (statement.WhereClause()) {
    if (is_left_join) {
      return decline();
    }
    for (Expression& wpred : SplitConjuncts(statement.WhereClause())) {
      all_predicates.push_back(std::move(wpred));
    }
  }
  if (all_predicates.empty()) {
    return decline();
  }
  std::vector<Expression> predicates;
  predicates.reserve(all_predicates.size());
  std::vector<Expression> probe_local_predicates;
  probe_local_predicates.reserve(all_predicates.size());
  std::vector<Expression> build_local_predicates;
  build_local_predicates.reserve(all_predicates.size());
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
  if (predicates.size() != 1) {
    return decline();
  }
  struct BuildIntFilter {
    slot_t build_col{0};
    BinaryOperation op{BinaryOperation::kEquals};
    int64_t constant{0};
  };
  std::vector<BuildIntFilter> build_int_filters;
  build_int_filters.reserve(build_local_predicates.size());
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
  probe_like_filters.reserve(probe_local_predicates.size());
  std::vector<ProbeIntFilter> probe_int_filters;
  probe_int_filters.reserve(probe_local_predicates.size());
  std::vector<IntegerPeekCompare> probe_int_peeks;
  probe_int_peeks.reserve(probe_local_predicates.size());
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
    if (!prog.has_value()) {
      return decline();
    }
    const ProgramShape shape = ClassifyProgram(*prog);
    if (shape.like_column < 0) {
      return decline();
    }
    const auto pcol = static_cast<slot_t>(shape.like_column);
    if (pcol >= probe_schema.ColumnCount() ||
        probe_schema.GetColumn(pcol).Type() != ValueType::kVarChar) {
      return decline();
    }
    const Value& pat_val = prog->Constants()[prog->Instructions()[1].operand];
    if (pat_val.IsCaseInsensitive()) {
      return decline();
    }
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
#ifdef _WIN32
      const size_t p0 = sv.find(lf.sub0);
      if (p0 == std::string_view::npos) {
        return false;
      }
      return sv.find(lf.sub1, p0 + lf.sub0.size()) != std::string_view::npos;
#else
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
#endif
    }
    return Like(sv, lf.pattern);
  };
  const auto keys = EqualityKeys(probe_schema, build_schema, predicates);
  if (keys.size() != 1 || keys[0].null_safe || keys[0].left_offset != 0 ||
      keys[0].right_offset != 0 || !IsColumnEqualityPredicate(predicates[0])) {
    return decline();
  }
  const auto& equality = predicates[0]->AsBinaryExpression();
  const auto unambiguous_col = [&](const ColumnName& col) {
    if (!col.schema.empty()) {
      return true;
    }
    return (probe_schema.Offset(col) >= 0) != (build_schema.Offset(col) >= 0);
  };
  if (!unambiguous_col(equality.Left()->AsColumnValue().GetColumnName()) ||
      !unambiguous_col(equality.Right()->AsColumnValue().GetColumnName())) {
    return decline();
  }
  const auto signed_int = [](const Column& column) {
    return column.Type() == ValueType::kInt64 && !column.IsUnsigned();
  };
  if (!signed_int(probe_schema.GetColumn(keys[0].left)) ||
      !signed_int(build_schema.GetColumn(keys[0].right))) {
    return decline();
  }
  // Fixed-width batches have a strict, reservable upper bound.
  for (size_t c = 0; c < probe_schema.ColumnCount(); ++c) {
    const auto type = probe_schema.GetColumn(c).Type();
    if (type != ValueType::kInt64 && type != ValueType::kDate &&
        type != ValueType::kDouble &&
        (type != ValueType::kVarChar || probe_like_filters.empty())) {
      return decline();
    }
  }
  std::vector<int> arguments;
  for (const auto* aggregate : fusion->aggregates) {
    const auto type = aggregate->GetType();
    if ((type != AggregationType::kCount && type != AggregationType::kSum) ||
        aggregate->Distinct() || aggregate->HasWhereFilter() ||
        aggregate->NeedsGroupContext()) {
      return decline();
    }
    if (is_left_join && type != AggregationType::kCount) {
      return decline();
    }
    if (fusion->count_star[arguments.size()]) {
      if (is_left_join) {
        return decline();
      }
      arguments.push_back(-1);
      continue;
    }
    if (!aggregate->Child() ||
        aggregate->Child()->Type() != TypeTag::kColumnValue) {
      return decline();
    }
    const int offset =
        joined.Offset(aggregate->Child()->AsColumnValue().GetColumnName());
    if (offset < 0 ||
        !signed_int(joined.GetColumn(static_cast<size_t>(offset)))) {
      return decline();
    }
    if (is_left_join &&
        std::cmp_greater_equal(offset, probe_schema.ColumnCount())) {
      return decline();
    }
    arguments.push_back(offset);
  }
  // Outside an aggregate, only the grouping column may read a base cell.
  const auto safe_emit = [&](const auto& self,
                             const Expression& expression) -> bool {
    if (!expression) {
      return false;
    }
    if (expression->Type() == TypeTag::kAggregateExp ||
        expression->Type() == TypeTag::kConstantValue) {
      return true;
    }
    if (expression->Type() == TypeTag::kColumnValue) {
      return group_column >= 0 &&
             joined.Offset(expression->AsColumnValue().GetColumnName()) ==
                 static_cast<int>(probe_schema.ColumnCount()) + group_column;
    }
    if (expression->Type() != TypeTag::kBinaryExp &&
        expression->Type() != TypeTag::kUnaryExp) {
      return false;
    }
    return std::ranges::all_of(
        ExpressionChildren(expression),
        [&](const Expression& child) { return self(self, child); });
  };
  for (const auto& expression : fusion->select_items) {
    if (!safe_emit(safe_emit, expression)) {
      return decline();
    }
  }

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
  constexpr size_t kDimensionBytes = size_t{8} * 1024 * 1024;
  auto dimension_charge =
      typed_dimension ? TryReserveQueryMemory(kDimensionBytes +
                                              (1024 * dimension_stride * 9))
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
        if (rows == 0) {
          break;
        }
        const size_t cells = rows * dimension_stride;
        if (dimension_cells.size() + cells > kDimensionBytes / 9) {
          typed_dimension = false;
          break;
        }
        dimension_cells.insert(
            dimension_cells.end(), batch.cells.begin(),
            batch.cells.begin() + static_cast<std::ptrdiff_t>(cells));
        dimension_nulls.insert(
            dimension_nulls.end(), batch.nulls.begin(),
            batch.nulls.begin() + static_cast<std::ptrdiff_t>(cells));
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
          if (!ok) {
            continue;
          }
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
    if (dimension.HasSpill()) {
      return decline();
    }
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
      return {.value = static_cast<int64_t>(row + 1), .null = false};
    }
    if (typed_dimension) {
      const size_t cell = (row * dimension_stride) + column;
      return {.value = dimension_cells[cell],
              .null = dimension_nulls[cell] != 0};
    }
    const Value& cell = dimension.rows[row][column];
    return {.value = cell.IsNull() ? 0 : cell.value.int_value,
            .null = cell.IsNull()};
  };
  size_t index_bytes = 0;
  if (__builtin_mul_overflow(dimension_rows, size_t{128}, &index_bytes) ||
      __builtin_add_overflow(index_bytes, size_t{65536}, &index_bytes)) {
    return decline();
  }
  auto index_charge = TryReserveQueryMemory(index_bytes);
  if (!index_charge) {
    return decline();
  }
  FlatJoinBuckets<int64_t> buckets;
  // Validate build-only nonnegative folds while forming groups and the
  // actual key domain; those three consumers need just one row pass.
  bool frequency_fold = true;
  std::optional<size_t> count_probe_col;
  std::vector<size_t> sum_columns;
  sum_columns.reserve(arguments.size());
  for (size_t a = 0; a < arguments.size(); ++a) {
    if (arguments[a] < 0) {
      continue;
    }
    if (std::cmp_less(arguments[a], probe_schema.ColumnCount())) {
      const auto pcol = static_cast<size_t>(arguments[a]);
      if (is_left_join &&
          fusion->aggregates[a]->GetType() == AggregationType::kCount &&
          (!count_probe_col.has_value() || *count_probe_col == pcol)) {
        count_probe_col = pcol;
        continue;
      }
      frequency_fold = false;
      continue;
    }
    if (fusion->aggregates[a]->GetType() == AggregationType::kSum) {
      sum_columns.push_back(static_cast<size_t>(arguments[a]) -
                            probe_schema.ColumnCount());
    }
  }
  if ((is_left_join || !probe_like_filters.empty() ||
       !probe_int_filters.empty()) &&
      !frequency_fold) {
    return decline();
  }
  std::optional<int64_t> minimum, maximum;
  bool identity_pk_groups =
      group_column >= 0 && std::cmp_equal(group_column, keys[0].right) &&
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
          if (is_left_join) {
            return decline();
          }
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
      if (join_key.null) {
        continue;
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
  } else if (!identity_pk_groups) {
    group_map.Reserve(dimension_rows);
    groups.reserve(dimension_rows);
    row_groups.resize(dimension_rows);
    for (size_t i = 0; i < dimension_rows; ++i) {
      const auto join_key = dimension_cell(i, keys[0].right);
      if (join_key.null) {
        if (is_left_join) {
          return decline();
        }
        continue;
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
      const auto key = dimension_cell(i, static_cast<size_t>(group_column));
      if (key.null) {
        if (!null_group) {
          null_group = groups.size();
          groups.emplace_back();
        }
        row_groups[i] = *null_group;
      } else {
        bool inserted = false;
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
      return {};
    }
    if (!groups.empty()) {
      return groups[g];
    }
    const auto cell = dimension_cell(g, static_cast<size_t>(group_column));
    if (cell.null) {
      return {};
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
    const platform::Int128 span =
        static_cast<platform::Int128>(*maximum) - *minimum + 1;
    const platform::Int128 max_span =
        (is_left_join || count_probe_col.has_value()) ? 500000 : 65536;
    if (span <= max_span &&
        (span <= 65536 ||
         span <= static_cast<platform::Int128>(dimension_rows) * 4)) {
      dense_min = *minimum;
      domain_span = static_cast<size_t>(span);
    }
  }
  const bool allow_key_counts = frequency_fold && domain_span != 0 &&
                                !is_left_join && probe_like_filters.empty() &&
                                probe_int_filters.empty() &&
                                !count_probe_col.has_value();
  std::shared_ptr<const Table::IntegerKeyCounts> key_counts;
  auto counts_charge = allow_key_counts
                           ? TryReserveQueryMemory(size_t{1024} * 1024)
                           : decltype(TryReserveQueryMemory(0)){};
  if (counts_charge) {
    key_counts = tables[probe]->FindCachedIntegerKeyCounts(
        context.txn_, projections[probe][keys[0].left], dense_min, domain_span);
  }
  // An exact fact histogram can be folded once for each dimension row,
  // including duplicates. Only the ordinary scan needs a unique key->row map.
  const bool identity_dense_rows =
      build_is_identity_1_to_n && identity_pk_groups &&
      domain_span == dimension_rows && dense_min == 1;
  if (!key_counts && domain_span != 0 && !identity_dense_rows) {
    dense_rows.assign(domain_span, kNoDenseRow);
    for (size_t i = 0; i < dimension_rows; ++i) {
      const auto key = dimension_cell(i, keys[0].right);
      if (key.null) {
        continue;
      }
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
  bool direct_block_index = false;
  if ((context.txn_.IsReadOnly() || context.txn_.WriteSetEmpty()) &&
      !context.txn_.HasAnyPageVersionChains() &&
      (frequency_fold ||
       (probe_like_filters.empty() && probe_int_filters.empty())) &&
      !key_counts && has_dense_rows) {
    std::shared_ptr<const LSTableStore> store = GetOrBuildLSTableStore(
        context.txn_, *tables[probe], true, &projections[probe]);
    if (store != nullptr && store->uncommitted_owner_txn == 0 &&
        !context.txn_.HasAnyPageVersionChains() &&
        store->layout_generation == tables[probe]->LayoutGeneration() &&
        store->first_pid == tables[probe]->FirstPageId()) {
      probe_lstable_store = std::move(store);
      const size_t total_pages = probe_lstable_store->page_ids.size();
      lstable_blocks_per_morsel = std::clamp<size_t>(total_pages / 64, 1, 32);
      const size_t morsel_count =
          (total_pages + lstable_blocks_per_morsel - 1) /
          lstable_blocks_per_morsel;
      direct_block_index = probe_lstable_store->blocks.size() == total_pages;
      if (direct_block_index) {
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
        if (!cell.null) {
          value.type = build_schema.GetColumn(c).Type();
        }
        row.values_.push_back(std::move(value));
      }
      dimension.AddRow(std::move(row));
    }
    RETURN_IF_FAIL(dimension.FinishSpill());
    RETURN_IF_FAIL(dimension.GetStatus());
    if (dimension.HasSpill()) {
      return decline();
    }
  }
  if (!has_dense_rows && !key_counts) {
    buckets.reserve(dimension_rows);
    for (const Row& row : dimension.rows) {
      if (!row[keys[0].right].IsNull()) {
        buckets.emplace(row[keys[0].right].value.int_value, &row);
      }
    }
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
    Status morsel_status = Status::kSuccess;
    morsels = tables[probe]->BuildScanMorsels(context.txn_, 8, &morsel_status);
    RETURN_IF_FAIL(morsel_status);
  }

  struct alignas(64) Partial {
    platform::Int128 sum{0}, low{0}, high{0};
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
       std::cmp_less(arguments[0], probe_schema.ColumnCount()));
  // Frequency folding is order-independent and accumulates into a shared
  // atomic frequency table, then folds once into a single partial table.
  // Only the general signed fold needs a table per ordered morsel.
  const size_t partial_slots = frequency_fold ? 1 : morsels.size();
  size_t per_morsel = 0, partial_count = 0, partial_bytes = 0;
  if (!direct_freq_count_emit) {
    if (__builtin_mul_overflow(total_groups, width, &per_morsel) ||
        __builtin_mul_overflow(partial_slots, per_morsel, &partial_count) ||
        __builtin_mul_overflow(partial_count, sizeof(Partial),
                               &partial_bytes)) {
      return decline();
    }
  }
  size_t scan_bytes = 0, bytes = 0;
  if (__builtin_mul_overflow(workers * 1024 * 9, probe_schema.ColumnCount(),
                             &scan_bytes) ||
      __builtin_add_overflow(partial_bytes, scan_bytes, &bytes)) {
    return decline();
  }
  if (frequency_fold) {
    size_t frequency_bytes = 0;
    const size_t freq_per_row =
        (probe_lstable_store != nullptr && dimension_rows <= 500000)
            ? (workers * sizeof(uint32_t))
            : sizeof(uint64_t);
    if (__builtin_mul_overflow(dimension_rows, freq_per_row,
                               &frequency_bytes) ||
        __builtin_add_overflow(bytes, frequency_bytes, &bytes)) {
      return decline();
    }
  }
  auto partial_charge = TryReserveQueryMemory(bytes);
  if (!partial_charge) {
    return decline();
  }
  if (!key_counts && probe_lstable_store == nullptr && frequency_fold &&
      counts_charge && !morsels.empty()) {
    key_counts = tables[probe]->TryReadIntegerKeyCounts(
        context.txn_, morsels, projections[probe][keys[0].left], dense_min,
        domain_span);
  }
  // The store and its scan morsels are selected together above. Once
  // page-scan morsels are built, keep them on the page-scan path.
  // The per-worker frequency counters are uint32_t: bound the total probe
  // rows so no single worker can wrap one entry (same sufficient condition
  // the shared atomic u32 form below uses).
  const bool use_worker_freqs =
      probe_lstable_store != nullptr && frequency_fold && !key_counts &&
      dimension_rows <= 500000 &&
      probe_lstable_store->total_rows < std::numeric_limits<uint32_t>::max();
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
      std::vector<uint32_t> blk_sel_rows;
      TypedCellBatch batch;
      if (key_counts) {
        visible[0] = key_counts->rows;
        next.store(morsels.size());
      }
      while (!failed.load()) {
        const size_t m = next.fetch_add(1);
        if (m >= morsels.size()) {
          break;
        }
        if (probe_lstable_store != nullptr) {
          const size_t block_begin = m * lstable_blocks_per_morsel;
          if (direct_block_index &&
              block_begin >= probe_lstable_store->blocks.size()) {
            fail(StatusError(StatusCode::kCorrupt,
                             "grouped join scan morsel has no storage block"));
            break;
          }
          const size_t block_count =
              direct_block_index
                  ? std::min(lstable_blocks_per_morsel,
                             probe_lstable_store->blocks.size() - block_begin)
                  : morsels[m].size();
          for (size_t bi = 0; bi < block_count; ++bi) {
            const LSTablePageBlock* blk =
                direct_block_index
                    ? &probe_lstable_store->blocks[block_begin + bi]
                    : probe_lstable_store->FindBlock(morsels[m][bi]);
            if (blk == nullptr || blk->row_count == 0) {
              continue;
            }
            visible[m] += blk->row_count;
            const LSTableColumnBlock& kcol = blk->columns[full_probe_key_col];
            if (!frequency_fold) {
              Partial* __restrict morsel_partials = &partials[m * per_morsel];
              if (!blk->has_any_null && total_groups == 1 &&
                  !dense_match_mask.empty() && width == 2 && arguments[0] < 0 &&
                  arguments[1] >= 0 &&
                  std::cmp_less(arguments[1], probe_schema.ColumnCount()) &&
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
                  platform::Int128 blk_sum = 0;
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
                  std::cmp_less(arguments[0], probe_schema.ColumnCount()) &&
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
                    if (r + 8 < nrows) {
                      const uint64_t pf_off =
                          static_cast<uint64_t>(kcells[r + 8]) -
                          static_cast<uint64_t>(dense_min);
                      if (pf_off < dsize) {
                        __builtin_prefetch(&dgroups[pf_off], 0, 1);
                      }
                    }
                    const uint64_t offset = static_cast<uint64_t>(kcells[r]) -
                                            static_cast<uint64_t>(dense_min);
                    if (offset >= dsize) {
                      continue;
                    }
                    const size_t g = dgroups[offset];
                    if (g == kNoDenseRow) {
                      continue;
                    }
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
                if (kcol.IsNull(r)) {
                  continue;
                }
                const int64_t key = kcol.int_cells[r];
                const uint64_t offset = static_cast<uint64_t>(key) -
                                        static_cast<uint64_t>(dense_min);
                size_t target_row = 0;
                if (identity_dense_rows) {
                  if (offset >= dimension_rows) {
                    continue;
                  }
                  target_row = static_cast<size_t>(offset);
                } else {
                  if (offset >= dense_rows.size() ||
                      dense_rows[offset] == kNoDenseRow) {
                    continue;
                  }
                  target_row = dense_rows[offset];
                }
                ++comparisons[m];
                const size_t g =
                    identity_pk_groups ? target_row : row_groups[target_row];
                ++morsel_partials[g * width].pairs;
                for (size_t a = 0; a < width; ++a) {
                  Partial& partial = morsel_partials[(g * width) + a];
                  if (arguments[a] < 0) {
                    ++partial.count;
                    continue;
                  }
                  const auto column = static_cast<size_t>(arguments[a]);
                  int64_t value = 0;
                  if (column < probe_schema.ColumnCount()) {
                    const LSTableColumnBlock& acol =
                        blk->columns[projections[probe][column]];
                    if (acol.IsNull(r)) {
                      continue;
                    }
                    value = acol.int_cells[r];
                  } else {
                    const auto cell = dimension_cell(
                        target_row, column - probe_schema.ColumnCount());
                    if (cell.null) {
                      continue;
                    }
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
              const LSTableColumnBlock& fcol = blk->columns[iflt.full_col];
              const int64_t* __restrict kcells = kcol.int_cells.data();
              const int64_t* __restrict fcells = fcol.int_cells.data();
              const size_t* __restrict drows = dense_rows.data();
              const size_t dsize = dense_rows.size();
              const uint32_t nrows = blk->row_count;
              bool peek_all_pass = false;
              if (fcol.has_min_max) {
                switch (iflt.op) {
                  case BinaryOperation::kEquals:
                    peek_all_pass = (fcol.min_int == iflt.constant &&
                                     fcol.max_int == iflt.constant);
                    break;
                  case BinaryOperation::kNotEquals:
                    peek_all_pass = (iflt.constant < fcol.min_int ||
                                     iflt.constant > fcol.max_int);
                    break;
                  case BinaryOperation::kLessThan:
                    peek_all_pass = (fcol.max_int < iflt.constant);
                    break;
                  case BinaryOperation::kLessThanEquals:
                    peek_all_pass = (fcol.max_int <= iflt.constant);
                    break;
                  case BinaryOperation::kGreaterThan:
                    peek_all_pass = (fcol.min_int > iflt.constant);
                    break;
                  case BinaryOperation::kGreaterThanEquals:
                    peek_all_pass = (fcol.min_int >= iflt.constant);
                    break;
                  default:
                    break;
                }
              }
              size_t blk_matched = 0;
              if (peek_all_pass) {
                // Pattern C: Dense All-Pass Streaming (s_block = 100%).
                for (uint32_t r = 0; r < nrows; ++r) {
                  if (r + 8 < nrows) {
                    const uint64_t pf_off =
                        static_cast<uint64_t>(kcells[r + 8]) -
                        static_cast<uint64_t>(dense_min);
                    if (pf_off < dsize) {
                      __builtin_prefetch(&drows[pf_off], 0, 1);
                    }
                  }
                  const uint64_t offset = static_cast<uint64_t>(kcells[r]) -
                                          static_cast<uint64_t>(dense_min);
                  if (offset >= dsize) {
                    continue;
                  }
                  const size_t target_row = drows[offset];
                  if (target_row == kNoDenseRow) {
                    continue;
                  }
                  ++local_freq[target_row];
                  ++blk_matched;
                }
              } else {
                // Pattern B / A: Build per-block Selection Vector first.
                if (blk_sel_rows.size() < nrows) {
                  blk_sel_rows.resize(nrows);
                }
                uint32_t* __restrict sel = blk_sel_rows.data();
                uint32_t blk_filtered = 0;
                for (uint32_t r = 0; r < nrows; ++r) {
                  sel[blk_filtered] = r;
                  blk_filtered += static_cast<uint32_t>(
                      MatchIntegerCompare(fcells[r], iflt.op, iflt.constant));
                }
                if (blk_filtered * 20U >= nrows) {
                  // Pattern B: Selection Vector + Software Prefetch (s >= 5%).
                  for (uint32_t si = 0; si < blk_filtered; ++si) {
                    if (si + 8 < blk_filtered) {
                      const uint64_t pf_off =
                          static_cast<uint64_t>(kcells[sel[si + 8]]) -
                          static_cast<uint64_t>(dense_min);
                      if (pf_off < dsize) {
                        __builtin_prefetch(&drows[pf_off], 0, 1);
                      }
                    }
                    const uint64_t offset =
                        static_cast<uint64_t>(kcells[sel[si]]) -
                        static_cast<uint64_t>(dense_min);
                    if (offset >= dsize) {
                      continue;
                    }
                    const size_t target_row = drows[offset];
                    if (target_row == kNoDenseRow) {
                      continue;
                    }
                    ++local_freq[target_row];
                    ++blk_matched;
                  }
                } else {
                  // Pattern A: Sparse Gather (s < 5%).
                  for (uint32_t si = 0; si < blk_filtered; ++si) {
                    const uint64_t offset =
                        static_cast<uint64_t>(kcells[sel[si]]) -
                        static_cast<uint64_t>(dense_min);
                    if (offset >= dsize) {
                      continue;
                    }
                    const size_t target_row = drows[offset];
                    if (target_row == kNoDenseRow) {
                      continue;
                    }
                    ++local_freq[target_row];
                    ++blk_matched;
                  }
                }
              }
              comparisons[m] += blk_matched;
              continue;
            }
            for (uint32_t r = 0; r < blk->row_count; ++r) {
              if (kcol.IsNull(r)) {
                continue;
              }
              const int64_t key = kcol.int_cells[r];
              const uint64_t offset =
                  static_cast<uint64_t>(key) - static_cast<uint64_t>(dense_min);
              size_t target_row = 0;
              if (identity_dense_rows) {
                if (offset >= dimension_rows) {
                  continue;
                }
                target_row = static_cast<size_t>(offset);
              } else {
                if (offset >= dense_rows.size() ||
                    dense_rows[offset] == kNoDenseRow) {
                  continue;
                }
                target_row = dense_rows[offset];
              }
              if (full_count_probe_col.has_value() &&
                  blk->columns[*full_count_probe_col].IsNull(r)) {
                continue;
              }
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
                if (!filter_ok) {
                  continue;
                }
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
                  if (lf.negated ? matched : !matched) {
                    filter_ok = false;
                    break;
                  }
                }
                if (!filter_ok) {
                  continue;
                }
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
          if (rows == 0) {
            break;
          }
          visible[m] += rows;
          for (size_t r = 0; r < rows; ++r) {
            const size_t base = r * batch.stride;
            if (batch.nulls[base + keys[0].left]) {
              continue;
            }
            if (count_probe_col.has_value() &&
                batch.nulls[base + *count_probe_col]) {
              continue;
            }
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
              if (!filter_ok) {
                continue;
              }
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
                if (lf.negated ? matched : !matched) {
                  filter_ok = false;
                  break;
                }
              }
              if (!filter_ok) {
                continue;
              }
            }
            const int64_t key = batch.cells[base + keys[0].left];
            const auto accumulate = [&](const Row& build_row) {
              ++comparisons[m];
              const auto row_idx =
                  static_cast<size_t>(&build_row - dimension.rows.data());
              const size_t g =
                  identity_pk_groups ? row_idx : row_groups[row_idx];
              ++partials[(m * per_morsel) + (g * width)].pairs;
              for (size_t a = 0; a < width; ++a) {
                Partial& partial = partials[(m * per_morsel) + (g * width) + a];
                if (arguments[a] < 0) {
                  ++partial.count;
                  continue;
                }
                const auto column = static_cast<size_t>(arguments[a]);
                int64_t value = 0;
                if (column < batch.stride) {
                  if (batch.nulls[base + column]) {
                    continue;
                  }
                  value = batch.cells[base + column];
                } else {
                  const Value& cell = build_row[column - batch.stride];
                  if (cell.IsNull()) {
                    continue;
                  }
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
                  dense_rows[offset] == kNoDenseRow) {
                continue;
              }
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
              for (auto found = first; found != last; ++found) {
                accumulate(*found->second);
              }
            }
          }
        }
        if (!iterator.GetStatus().ok()) {
          fail(iterator.GetStatus());
        } else if (iterator.IsValid() && !failed.load()) {
          fail(StatusError(StatusCode::kRuntimeError,
                           "typed join scan stopped before exhaustion"));
        }
      }
    } catch (const std::exception& exception) {
      fail(StatusError(StatusCode::kRuntimeError, exception.what()));
    } catch (...) {
      fail(StatusError(StatusCode::kRuntimeError, "stream join worker failed"));
    }
  };
  const auto started = std::chrono::steady_clock::now();
  std::vector<std::jthread> threads;
  threads.reserve(workers);
  try {
    if (!key_counts) {
      for (size_t w = 1; w < workers; ++w) {
        threads.emplace_back(run, w);
      }
    }
    run(0);
  } catch (const std::exception& exception) {
    fail(StatusError(StatusCode::kRuntimeError, exception.what()));
  }
  threads.clear();
  RETURN_IF_FAIL(error);
  const auto total_freq_of = [&](size_t i) -> uint64_t {
    if (key_counts) {
      const auto key = dimension_cell(i, keys[0].right);
      if (key.null) {
        return 0;
      }
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
  output_columns.reserve(fusion->select_names.size());
  for (const std::string& name : fusion->select_names) {
    output_columns.emplace_back(name, ValueType::kNull);
  }
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
      if (!is_left_join && count == 0) {
        continue;
      }
      if (count > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
        return StatusError(StatusCode::kIsInfinity, "integer count overflow");
      }
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
      if (!is_left_join && count == 0) {
        continue;
      }
      if (count > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
        return StatusError(StatusCode::kIsInfinity, "integer count overflow");
      }
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
        if (count == 0) {
          continue;
        }
        partials[g * width].pairs += count;
        for (size_t a = 0; a < width; ++a) {
          Partial& partial = partials[(g * width) + a];
          if (arguments[a] < 0 ||
              std::cmp_less(arguments[a], probe_schema.ColumnCount())) {
            partial.count += count;
            continue;
          }
          const auto cell =
              dimension_cell(i, static_cast<size_t>(arguments[a]) -
                                    probe_schema.ColumnCount());
          if (cell.null) {
            continue;
          }
          partial.count += count;
          if (fusion->aggregates[a]->GetType() == AggregationType::kSum) {
            partial.sum += static_cast<platform::Int128>(cell.value) * count;
            partial.high = partial.sum;
          }
        }
      }
    }
    std::vector<Value> finished_aggs(width);
    for (size_t g = 0; g < total_groups; ++g) {
      bool has_pairs = is_left_join || group_column < 0;
      for (size_t m = 0; m < partial_slots && !has_pairs; ++m) {
        has_pairs = partials[(m * per_morsel) + (g * width)].pairs != 0;
      }
      if (!has_pairs) {
        continue;
      }
      for (size_t a = 0; a < width; ++a) {
        platform::Int128 sum = 0;
        uint64_t count = 0;
        for (size_t m = 0; m < partial_slots; ++m) {
          const Partial& partial = partials[(m * per_morsel) + (g * width) + a];
          if (sum + partial.low < std::numeric_limits<int64_t>::min() ||
              sum + partial.high > std::numeric_limits<int64_t>::max()) {
            return StatusError(StatusCode::kIsInfinity,
                               "integer overflow on '+'");
          }
          sum += partial.sum;
          count += partial.count;
        }
        if (count >
            static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
          return StatusError(StatusCode::kIsInfinity, "integer count overflow");
        }
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
  if (result.rows.empty() && !fusion->rollup_distribution_applied) {
    for (size_t i = 0; i < output_columns.size(); ++i) {
      const Expression& item = fusion->select_items[i];
      if (!item) {
        continue;
      }
      const std::string name = fusion->select_names[i];
      Column declared = ProjectionExpressionColumn(name, item, joined);
      if (declared.Type() == ValueType::kNull) {
        declared = Column(
            name, ValueTypeFromTypeTag(item->ResultType(joined).GetType()));
      }
      output_columns[i] = std::move(declared);
    }
  } else if (!result.rows.empty()) {
    for (size_t i = 0; i < output_columns.size(); ++i) {
      output_columns[i] =
          Column(output_columns[i].Name(), ValueTypeOf(result.rows[0][i]));
    }
  }
  result.schema = Schema("", std::move(output_columns));
  result.hash_joins = result.in_memory_hash_joins = 1;
  for (size_t count : comparisons) {
    result.join_comparisons += count;
  }
  if (auto* runtime = context.execution_runtime()) {
    runtime->join_ms += ElapsedMs(started);
    ++runtime->grouped_join_fusions;
    ++runtime->streamed_grouped_joins;
    for (size_t count : visible) {
      runtime->scan_rows += count;
    }
    runtime->scan_output_rows += result.join_comparisons;
  }
  fusion->fired = true;
  return std::optional<Relation>(std::move(result));
}

StatusOr<DistinctLSTableProbeOutcome> TryDistinctLSTableProbe(
    const DistinctLSTableProbeContext& ctx) {
  TransactionContext& context = ctx.context;
  const Relation& left = ctx.left;
  const Relation& right = ctx.right;
  GroupedJoinFusionSpec* fusion = ctx.fusion;
  JoinType join_type = ctx.join_type;
  Relation& result = ctx.result;
  const bool use_dense_int_rows = ctx.use_dense_int_rows;
  const bool use_unique_int_table = ctx.use_unique_int_table;
  const auto& int_buckets = ctx.int_buckets;
  const bool singleton_right_relation = ctx.singleton_right_relation;
  const auto& equality_keys = ctx.equality_keys;
  const auto& residual_programs = ctx.residual_programs;
  const bool integer_key = ctx.integer_key;
  const bool packed_2x32_int_key = ctx.packed_2x32_int_key;
  const DeferredLSTableProbe* deferred_left_probe = ctx.deferred_left_probe;
  const bool prepared_pk_lookups_empty = ctx.prepared_pk_lookups_empty;
  const bool def_local_preds_ok = ctx.def_local_preds_ok;
  const auto& key_programs = ctx.key_programs;
  const auto& key_shapes = ctx.key_shapes;
  const size_t left_width = ctx.left_width;
  const auto& def_overwritten_by_pk = ctx.def_overwritten_by_pk;
  const auto& left_columns = ctx.left_columns;
  const auto& left_key_offsets = ctx.left_key_offsets;
  const auto join_begin = ctx.join_begin;
  const auto& dense_int_idxs = ctx.dense_int_idxs;
  const int64_t dense_int_min = ctx.dense_int_min;
  const auto& unique_int_table = ctx.unique_int_table;
  const size_t unique_int_mask = ctx.unique_int_mask;
  const bool distinct_fact_singleton = ctx.distinct_fact_singleton;
  const size_t scan_workers = ctx.worker_count;
  const bool debug_fusion = ctx.debug_fusion;

  const size_t total_left = left.rows.size();
  const size_t chunk_rows = 8192;
  const size_t chunk_count = (total_left + chunk_rows - 1) / chunk_rows;

  const auto not_fusable = [&](const char* why) {
    if (debug_fusion) {
      std::cerr << "[fusion] not fusable: " << why << '\n';
    }
    return DistinctLSTableProbeOutcome{.handled = true, .result = std::nullopt};
  };

  const auto hash_unique_int_key = HashUniqueIntKey;
  const bool dense_or_unique =
      use_dense_int_rows || use_unique_int_table || int_buckets.has_value();
  const bool singleton_probe =
      singleton_right_relation && equality_keys.empty();
  bool distinct_int_fast =
      fusion->aggregates.empty() && join_type == JoinType::kInner &&
      !fusion->having && !fusion->post_having_active &&
      fusion->topn_limit == 0 && !fusion->rollup_count_distribution &&
      fusion->output_order.empty() && result.using_columns == nullptr &&
      residual_programs.empty() &&
      (singleton_probe || (!singleton_right_relation && integer_key &&
                           !packed_2x32_int_key && dense_or_unique)) &&
      !left.HasSpill() && !key_programs.empty() && key_programs.size() <= 2 &&
      (deferred_left_probe == nullptr ||
       (prepared_pk_lookups_empty && def_local_preds_ok));
  std::array<slot_t, 2> gk_slots{0, 0};
  std::array<ValueType, 2> gk_types{ValueType::kNull, ValueType::kNull};
  if (distinct_int_fast) {
    for (size_t k = 0; k < key_programs.size(); ++k) {
      if (key_shapes[k].direct_column < 0 ||
          std::cmp_greater_equal(key_shapes[k].direct_column, left_width)) {
        distinct_int_fast = false;
        break;
      }
      const auto slot = static_cast<slot_t>(key_shapes[k].direct_column);
      const Column& col = left.schema.GetColumn(slot);
      if (col.IsUnsigned() ||
          (col.Type() != ValueType::kInt64 && col.Type() != ValueType::kDate)) {
        distinct_int_fast = false;
        break;
      }
      if (deferred_left_probe != nullptr &&
          (slot >= deferred_left_probe->projection.size() ||
           def_overwritten_by_pk[slot] != 0)) {
        distinct_int_fast = false;
        break;
      }
      gk_slots[k] = slot;
      gk_types[k] = col.Type();
    }
    if (!singleton_probe && deferred_left_probe != nullptr &&
        (left_columns[0] >= deferred_left_probe->projection.size() ||
         def_overwritten_by_pk[left_columns[0]] != 0)) {
      distinct_int_fast = false;
    }
  }
  struct DistinctDirectItem {
    enum class Kind : uint8_t { kConst, kKey0, kKey1 };
    Kind kind{Kind::kConst};
    Value constant{};
  };
  std::vector<DistinctDirectItem> dist_items;
  if (distinct_int_fast) {
    dist_items.reserve(fusion->select_items.size());
    for (const Expression& item : fusion->select_items) {
      if (!item) {
        distinct_int_fast = false;
        break;
      }
      if (item->Type() == TypeTag::kConstantValue &&
          !ContainsParameterSlot(item)) {
        dist_items.push_back({.kind = DistinctDirectItem::Kind::kConst,
                              .constant = item->AsConstantValue().GetValue()});
        continue;
      }
      if (item->Type() == TypeTag::kColumnValue) {
        const auto col_off = LocalColumnOffset(
            result.schema, item->AsColumnValue().GetColumnName());
        if (col_off.has_value() && *col_off == gk_slots[0]) {
          dist_items.push_back({.kind = DistinctDirectItem::Kind::kKey0});
          continue;
        }
        if (col_off.has_value() && key_programs.size() == 2 &&
            *col_off == gk_slots[1]) {
          dist_items.push_back({.kind = DistinctDirectItem::Kind::kKey1});
          continue;
        }
      }
      distinct_int_fast = false;
      break;
    }
  }
  if (distinct_fact_singleton && !distinct_int_fast) {
    return not_fusable("distinct fact singleton unsupported");
  }
  if (distinct_int_fast) {
    if (key_programs.size() == 1 && deferred_left_probe != nullptr &&
        deferred_left_probe->store != nullptr &&
        fusion->select_names.size() == dist_items.size() &&
        (singleton_probe || !left_columns.empty())) {
      const LSTableStore& store = *deferred_left_probe->store;
      const size_t total_blocks = store.blocks.size();
      const size_t base_proj_cols = deferred_left_probe->projection.size();
      const bool has_join_key = !singleton_probe;
      const slot_t jk0 = has_join_key ? left_columns[0] : 0;
      const int64_t off0 = has_join_key ? left_key_offsets[0] : 0;
      const slot_t gk0_slot = gk_slots[0];
      if (gk0_slot < base_proj_cols &&
          (!has_join_key || jk0 < base_proj_cols)) {
        const slot_t full_gk0 = deferred_left_probe->projection[gk0_slot];
        const slot_t full_jk0 =
            has_join_key ? deferred_left_probe->projection[jk0] : 0;
        const auto* peek_ptr = deferred_left_probe->integer_peeks.empty()
                                   ? nullptr
                                   : &deferred_left_probe->integer_peeks;
        const ScanRuntimeFilter& primary_rf =
            deferred_left_probe->runtime_filter;
        const RuntimeFilterGroup* zm_primary_group =
            primary_rf.enabled() ? primary_rf.group.get() : nullptr;
        const std::optional<slot_t> zm_primary_col =
            (zm_primary_group != nullptr && primary_rf.column < base_proj_cols)
                ? std::optional<slot_t>(
                      deferred_left_probe->projection[primary_rf.column])
                : std::nullopt;

        struct BsRfSpec {
          slot_t full_col{0};
          ValueType type{ValueType::kInt64};
          const RuntimeFilterGroup* group{nullptr};
        };
        std::vector<BsRfSpec> all_rfs;
        if (zm_primary_group != nullptr && zm_primary_col.has_value()) {
          all_rfs.push_back(BsRfSpec{
              .full_col = *zm_primary_col,
              .type = primary_rf.type,
              .group = zm_primary_group,
          });
        }
        for (const ScanRuntimeFilter& ext : primary_rf.extra) {
          if (!ext.enabled() || ext.column >= base_proj_cols) {
            continue;
          }
          all_rfs.push_back(BsRfSpec{
              .full_col = deferred_left_probe->projection[ext.column],
              .type = ext.type,
              .group = ext.group.get(),
          });
        }

        bool gk_type_uniform_match = true;
        std::vector<BsRfSpec> gk0_rfs_spec;
        gk0_rfs_spec.reserve(all_rfs.size());
        std::vector<BsRfSpec> other_rfs;
        other_rfs.reserve(all_rfs.size());
        for (const BsRfSpec& spec : all_rfs) {
          if (spec.full_col == full_gk0 &&
              (spec.type == gk_types[0] || spec.group->RejectsAll())) {
            gk0_rfs_spec.push_back(spec);
          } else {
            other_rfs.push_back(spec);
          }
        }

        std::optional<slot_t> full_rf_col;
        const RuntimeFilterGroup* active_rf_group = nullptr;
        ValueType active_rf_type = ValueType::kInt64;
        size_t picked_other_idx = other_rfs.size();
        for (size_t i = 0; i < other_rfs.size(); ++i) {
          if (other_rfs[i].group->has_dense_bits()) {
            picked_other_idx = i;
            break;
          }
        }
        if (picked_other_idx == other_rfs.size() && !other_rfs.empty()) {
          picked_other_idx = 0;
        }
        std::vector<BsRfSpec> extra_rf_specs;
        extra_rf_specs.reserve(other_rfs.size());
        if (picked_other_idx < other_rfs.size()) {
          full_rf_col = other_rfs[picked_other_idx].full_col;
          active_rf_group = other_rfs[picked_other_idx].group;
          active_rf_type = other_rfs[picked_other_idx].type;
        }
        for (size_t i = 0; i < other_rfs.size(); ++i) {
          if (i != picked_other_idx) {
            extra_rf_specs.push_back(other_rfs[i]);
          }
        }

        const auto check_dense_all_valid =
            [](const RuntimeFilterGroup* grp) -> bool {
          if (grp == nullptr || !grp->has_dense_bits()) {
            return false;
          }
          const int64_t lo = grp->Lo();
          const int64_t hi = grp->Hi();
          if (hi < lo ||
              static_cast<uint64_t>(hi) - static_cast<uint64_t>(lo) >=
                  65536ULL) {
            return false;
          }
          const uint64_t rspan =
              static_cast<uint64_t>(hi) - static_cast<uint64_t>(lo) + 1ULL;
          const auto full_words = static_cast<size_t>(rspan >> 6);
          const auto rem_bits = static_cast<uint32_t>(rspan & 63ULL);
          const auto need_words = static_cast<size_t>((rspan + 63ULL) >> 6);
          if (need_words > grp->dense_bits_word_count()) {
            return false;
          }
          const uint64_t* bits = grp->dense_bits_data();
          for (size_t wi = 0; wi < full_words; ++wi) {
            if (bits[wi] != ~uint64_t{0}) {
              return false;
            }
          }
          if (rem_bits != 0) {
            const uint64_t tail_mask = (uint64_t{1} << rem_bits) - 1ULL;
            if ((bits[full_words] & tail_mask) != tail_mask) {
              return false;
            }
          }
          return true;
        };

        const bool rf_has_dense =
            active_rf_group != nullptr && active_rf_group->has_dense_bits();
        const uint64_t* const rf_dense_bits =
            rf_has_dense ? active_rf_group->dense_bits_data() : nullptr;
        const int64_t rf_lo = rf_has_dense ? active_rf_group->Lo() : 0;
        const int64_t rf_hi = rf_has_dense ? active_rf_group->Hi() : 0;
        const bool rf_dense_all_valid = check_dense_all_valid(active_rf_group);

        bool dense_all_valid = false;
        if (has_join_key && use_dense_int_rows && !dense_int_idxs.empty()) {
          dense_all_valid = true;
          for (const uint32_t v : dense_int_idxs) {
            if (v == kInvalidRightIdx) {
              dense_all_valid = false;
              break;
            }
          }
        }
        const bool jk_is_monotonic =
            has_join_key && full_jk0 < store.column_is_monotonic.size() &&
            store.column_is_monotonic[full_jk0] != 0;
        const bool rf_is_monotonic =
            rf_has_dense && full_rf_col.has_value() &&
            *full_rf_col < store.column_is_monotonic.size() &&
            store.column_is_monotonic[*full_rf_col] != 0;

        bool gk_null_rejects = !gk0_rfs_spec.empty();
        if (!gk_null_rejects) {
          for (const NullPeekCompare& np : deferred_left_probe->null_peeks) {
            if (np.column == full_gk0 && !np.expect_null) {
              gk_null_rejects = true;
              break;
            }
          }
        }

        bool can_single_bitset = (extra_rf_specs.size() <= 4);
        int64_t raw_gk_min = std::numeric_limits<int64_t>::max();
        int64_t raw_gk_max = std::numeric_limits<int64_t>::min();
        uint64_t total_active_rows = 0;
        std::vector<uint32_t> active_blocks;
        active_blocks.reserve(total_blocks);

        bool any_rf_rejects_all = false;
        for (const BsRfSpec& spec : all_rfs) {
          if (spec.group->RejectsAll()) {
            any_rf_rejects_all = true;
            break;
          }
        }

        if (can_single_bitset && !any_rf_rejects_all) {
          for (size_t bi = 0; bi < total_blocks; ++bi) {
            const LSTablePageBlock& blk = store.blocks[bi];
            if (blk.row_count == 0 ||
                !blk.MatchesZoneMaps(peek_ptr, zm_primary_group,
                                     zm_primary_col)) {
              continue;
            }
            bool extra_zm_reject = false;
            for (const ScanRuntimeFilter& ext : primary_rf.extra) {
              if (!ext.enabled() || ext.column >= base_proj_cols) {
                continue;
              }
              if (ext.group->RejectsAll()) {
                extra_zm_reject = true;
                break;
              }
              const slot_t ext_full =
                  deferred_left_probe->projection[ext.column];
              if (ext_full < blk.columns.size()) {
                const LSTableColumnBlock& mcol = blk.columns[ext_full];
                if ((mcol.type == ValueType::kInt64 ||
                     mcol.type == ValueType::kDate) &&
                    mcol.type == ext.type &&
                    (!mcol.has_min_max ||
                     ext.group->RejectRange(mcol.min_int, mcol.max_int))) {
                  extra_zm_reject = true;
                  break;
                }
              }
            }
            if (extra_zm_reject) {
              continue;
            }
            if (has_join_key && use_dense_int_rows && integer_key &&
                !dense_int_idxs.empty() && off0 == 0 &&
                full_jk0 < blk.columns.size()) {
              const LSTableColumnBlock& jk = blk.columns[full_jk0];
              if (jk.has_min_max) {
                const uint64_t span = dense_int_idxs.size();
                const bool above =
                    jk.min_int >= dense_int_min &&
                    static_cast<uint64_t>(jk.min_int) -
                            static_cast<uint64_t>(dense_int_min) >=
                        span;
                if (jk.max_int < dense_int_min || above) {
                  continue;
                }
              }
            }
            bool null_peek_skip_blk = false;
            for (const NullPeekCompare& np : deferred_left_probe->null_peeks) {
              if (np.column >= blk.columns.size()) {
                can_single_bitset = false;
                break;
              }
              if (!blk.columns[np.column].has_null && np.expect_null) {
                null_peek_skip_blk = true;
                break;
              }
            }
            if (!can_single_bitset) {
              break;
            }
            if (null_peek_skip_blk) {
              continue;
            }
            if (full_gk0 >= blk.columns.size() ||
                (has_join_key && full_jk0 >= blk.columns.size())) {
              can_single_bitset = false;
              break;
            }
            const LSTableColumnBlock& gk_col = blk.columns[full_gk0];
            if (!gk_col.has_min_max ||
                gk_col.int_cells.size() < blk.row_count) {
              can_single_bitset = false;
              break;
            }
            if (gk_col.type != gk_types[0]) {
              gk_type_uniform_match = false;
            }
            if (has_join_key &&
                blk.columns[full_jk0].int_cells.size() < blk.row_count) {
              can_single_bitset = false;
              break;
            }
            for (const BsRfSpec& spec : other_rfs) {
              if (spec.full_col >= blk.columns.size() ||
                  blk.columns[spec.full_col].int_cells.size() < blk.row_count) {
                can_single_bitset = false;
                break;
              }
            }
            if (!can_single_bitset) {
              break;
            }
            if (peek_ptr != nullptr) {
              for (const IntegerPeekCompare& pred : *peek_ptr) {
                if (pred.column >= blk.columns.size() ||
                    blk.columns[pred.column].int_cells.size() < blk.row_count) {
                  can_single_bitset = false;
                  break;
                }
              }
              if (!can_single_bitset) {
                break;
              }
            }
            raw_gk_min = std::min(raw_gk_min, gk_col.min_int);
            raw_gk_max = std::max(raw_gk_max, gk_col.max_int);
            total_active_rows += blk.row_count;
            active_blocks.push_back(static_cast<uint32_t>(bi));
          }
        }

        if (can_single_bitset && !gk_type_uniform_match &&
            !gk0_rfs_spec.empty()) {
          can_single_bitset = false;
        }

        std::vector<const RuntimeFilterGroup*> gk0_rfs;
        gk0_rfs.reserve(gk0_rfs_spec.size());
        int64_t gk_min = raw_gk_min;
        int64_t gk_max = raw_gk_max;
        bool has_gk0_bound = false;
        if (can_single_bitset && !active_blocks.empty()) {
          for (const BsRfSpec& spec : gk0_rfs_spec) {
            gk0_rfs.push_back(spec.group);
            if (spec.group->has_bounds()) {
              has_gk0_bound = true;
              gk_min = std::max(gk_min, spec.group->Lo());
              gk_max = std::min(gk_max, spec.group->Hi());
            }
          }
          if (gk_min > gk_max) {
            active_blocks.clear();
          }
        }

        static constexpr uint64_t kMaxSingleBitsetSpan = 2097152ULL;
        uint64_t gk_span = 0;
        if (can_single_bitset && !active_blocks.empty()) {
          if (gk_max < gk_min ||
              static_cast<uint64_t>(gk_max) - static_cast<uint64_t>(gk_min) >=
                  kMaxSingleBitsetSpan) {
            can_single_bitset = false;
          } else {
            gk_span = static_cast<uint64_t>(gk_max) -
                      static_cast<uint64_t>(gk_min) + 1ULL;
          }
        }

        if (can_single_bitset) {
          const size_t n_words =
              active_blocks.empty()
                  ? 0
                  : static_cast<size_t>((gk_span + 63ULL) >> 6);
          const size_t use_workers =
              active_blocks.empty()
                  ? 1
                  : std::min<size_t>(
                        {scan_workers,

                         active_blocks.size(),
                         std::max<size_t>(1, total_active_rows / 4096)});
          std::optional<std::vector<std::vector<uint64_t>>> worker_bitsets;
          bool mem_ok = true;
          if (!active_blocks.empty()) {
            auto bitset_charge =
                TryReserveQueryMemory(use_workers * n_words * sizeof(uint64_t));
            if (!bitset_charge) {
              mem_ok = false;
            } else {
              worker_bitsets.emplace(use_workers,
                                     std::vector<uint64_t>(n_words, 0));
            }
          }

          if (mem_ok) {
            std::vector<uint8_t> worker_saw_null(use_workers, 0);
            if (!active_blocks.empty()) {
              const uint32_t* const dense_ptr = dense_int_idxs.data();
              const uint64_t dense_len = dense_int_idxs.size();
              const auto eval_bs_peek = [](BinaryOperation op, int64_t a,
                                           int64_t b) noexcept -> bool {
                switch (op) {
                  case BinaryOperation::kEquals:
                    return a == b;
                  case BinaryOperation::kNotEquals:
                    return a != b;
                  case BinaryOperation::kLessThan:
                    return a < b;
                  case BinaryOperation::kLessThanEquals:
                    return a <= b;
                  case BinaryOperation::kGreaterThan:
                    return a > b;
                  case BinaryOperation::kGreaterThanEquals:
                    return a >= b;
                  default:
                    return false;
                }
              };
              const auto check_right_bs = [&](int64_t jkey) noexcept -> bool {
                if (use_dense_int_rows) {
                  const uint64_t idx = static_cast<uint64_t>(jkey) -
                                       static_cast<uint64_t>(dense_int_min);
                  if (dense_all_valid) {
                    return idx < dense_len;
                  }
                  return idx < dense_len && dense_ptr[idx] != kInvalidRightIdx;
                }
                if (use_unique_int_table) {
                  size_t slot = hash_unique_int_key(jkey) & unique_int_mask;
                  while (unique_int_table[slot].r_idx !=
                         std::numeric_limits<uint32_t>::max()) {
                    if (unique_int_table[slot].key == jkey) [[likely]] {
                      return true;
                    }
                    slot = (slot + 1) & unique_int_mask;
                  }
                  return false;
                }
                const auto [first, last] = int_buckets->equal_range(jkey);
                return first != last;
              };

              const bool bitset_first_skip =
                  !has_join_key && (gk_span <= 524288ULL) &&
                  !full_rf_col.has_value() && extra_rf_specs.empty();
              const auto gk_base = static_cast<uint64_t>(gk_min);
              const auto rf_base = static_cast<uint64_t>(rf_lo);
              const uint64_t rf_span_m1 = rf_has_dense
                                              ? (static_cast<uint64_t>(rf_hi) -
                                                 static_cast<uint64_t>(rf_lo))
                                              : 0;
              const size_t n_active = active_blocks.size();
              std::atomic<size_t> next_ab{0};
              const auto run_bs_scan = [&](size_t w) {
                uint64_t* __restrict wbits = (*worker_bitsets)[w].data();
                uint8_t local_saw_null = 0;
                const uint64_t lgk_base = gk_base;
                const uint64_t lgk_span = gk_span;
                const uint64_t lrf_base = rf_base;
                const uint64_t lrf_span_m1 = rf_span_m1;
                const bool lgk_null_rejects = gk_null_rejects;
                const bool lbitset_first_skip = bitset_first_skip;

                const auto pack_64_flags_to_mask =
                    [](const uint8_t* __restrict flags) -> uint64_t {
                  uint64_t out = 0;
                  for (size_t k = 0; k < 8; ++k) {
                    uint64_t word = 0;
                    std::memcpy(&word, flags + (k * 8), sizeof(uint64_t));
                    const uint64_t bits = ((word & 0x0101010101010101ULL) *
                                           0x0102040810204080ULL) >>
                                          56;
                    out |= (bits << (k * 8));
                  }
                  return out;
                };
                const auto pack_64_nulls_valid_mask =
                    [](const uint8_t* __restrict nulls) -> uint64_t {
                  uint64_t out = 0;
                  for (size_t k = 0; k < 8; ++k) {
                    uint64_t nv = 0;
                    std::memcpy(&nv, nulls + (k * 8), sizeof(uint64_t));
                    const uint64_t nonnull = (((nv & 0x7F7F7F7F7F7F7F7FULL) +
                                               0x7F7F7F7F7F7F7F7FULL) |
                                              nv) &
                                             0x8080808080808080ULL;
                    const uint64_t zero_msb =
                        (~nonnull) & 0x8080808080808080ULL;
                    const uint64_t bits =
                        ((zero_msb >> 7) * 0x0102040810204080ULL) >> 56;
                    out |= (bits << (k * 8));
                  }
                  return out;
                };

                const auto run_simple_kernel =
                    [wbits_p = wbits, lgk_base, lgk_span, lrf_base, lrf_span_m1,
                     lgk_null_rejects, &local_saw_null, &pack_64_flags_to_mask,
                     &pack_64_nulls_valid_mask]<
                        bool HasRfNull, bool CheckRfRange, bool HasGkNull,
                        bool GkNeedBounds>(
                        uint32_t begin_r, uint32_t end_r,
                        const int64_t* __restrict rf_ints_p,
                        const uint8_t* __restrict rf_nulls_p,
                        const int64_t* __restrict gk_ints_p,
                        const uint8_t* __restrict gk_nulls_p) {
                      bool saw_null = false;
                      uint32_t r = begin_r;
                      for (; r + 64 <= end_r; r += 64) {
                        uint64_t rf_pass = ~uint64_t{0};
                        if constexpr (CheckRfRange) {
                          std::array<uint8_t, 64> rok{};
                          for (uint32_t i = 0; i < 64; ++i) {
                            rok[i] = static_cast<uint8_t>(
                                static_cast<uint64_t>(rf_ints_p[r + i]) -
                                    lrf_base <=
                                lrf_span_m1);
                          }
                          rf_pass = pack_64_flags_to_mask(rok.data());
                        }
                        if constexpr (HasRfNull) {
                          if (rf_pass != 0) {
                            rf_pass &= pack_64_nulls_valid_mask(rf_nulls_p + r);
                          }
                        }
                        uint64_t valid = rf_pass;
                        if constexpr (HasGkNull) {
                          if (valid != 0) {
                            const uint64_t gm =
                                pack_64_nulls_valid_mask(gk_nulls_p + r);
                            if (!lgk_null_rejects && (valid & ~gm) != 0) {
                              saw_null = true;
                            }
                            valid &= gm;
                          }
                        }
                        if (valid == 0) {
                          continue;
                        }
                        if constexpr (!GkNeedBounds) {
                          if (valid == ~uint64_t{0}) {
                            for (uint32_t i = 0; i < 64; ++i) {
                              const uint64_t off =
                                  static_cast<uint64_t>(gk_ints_p[r + i]) -
                                  lgk_base;
                              const uint64_t bit = uint64_t{1} << (off & 63);
                              uint64_t* const word = &wbits_p[off >> 6];
                              if ((*word & bit) == 0) {
                                *word |= bit;
                              }
                            }
                            continue;
                          }
                        }
                        if (std::popcount(valid) >= 24) {
                          for (uint32_t i = 0; i < 64; ++i) {
                            const uint64_t off =
                                static_cast<uint64_t>(gk_ints_p[r + i]) -
                                lgk_base;
                            const bool row_ok =
                                ((valid & (uint64_t{1} << i)) != 0) &&
                                (!GkNeedBounds || off < lgk_span);
                            const size_t idx =
                                row_ok ? static_cast<size_t>(off >> 6) : 0;
                            const uint64_t bit =
                                row_ok ? (uint64_t{1} << (off & 63)) : 0;
                            const uint64_t cur = wbits_p[idx];
                            if ((~cur & bit) != 0) {
                              wbits_p[idx] = cur | bit;
                            }
                          }
                          continue;
                        }
                        while (valid != 0) {
                          const auto b =
                              static_cast<uint32_t>(std::countr_zero(valid));
                          valid &= (valid - 1);
                          const uint64_t off =
                              static_cast<uint64_t>(gk_ints_p[r + b]) -
                              lgk_base;
                          if constexpr (GkNeedBounds) {
                            if (off >= lgk_span) [[unlikely]] {
                              continue;
                            }
                          }
                          const uint64_t bit = uint64_t{1} << (off & 63);
                          if ((wbits_p[off >> 6] & bit) == 0) {
                            wbits_p[off >> 6] |= bit;
                          }
                        }
                      }
                      for (; r < end_r; ++r) {
                        if constexpr (HasRfNull) {
                          if (rf_nulls_p[r] != 0) [[unlikely]] {
                            continue;
                          }
                        }
                        if constexpr (CheckRfRange) {
                          if (static_cast<uint64_t>(rf_ints_p[r]) - lrf_base >
                              lrf_span_m1) {
                            continue;
                          }
                        }
                        if constexpr (HasGkNull) {
                          if (gk_nulls_p[r] != 0) [[unlikely]] {
                            if (!lgk_null_rejects) {
                              saw_null = true;
                            }
                            continue;
                          }
                        }
                        const uint64_t off =
                            static_cast<uint64_t>(gk_ints_p[r]) - lgk_base;
                        if constexpr (GkNeedBounds) {
                          if (off >= lgk_span) [[unlikely]] {
                            continue;
                          }
                        }
                        const uint64_t bit = uint64_t{1} << (off & 63);
                        if ((wbits_p[off >> 6] & bit) == 0) {
                          wbits_p[off >> 6] |= bit;
                        }
                      }
                      if (saw_null) {
                        local_saw_null = 1;
                      }
                    };

                while (true) {
                  const size_t ai =
                      next_ab.fetch_add(1, std::memory_order_relaxed);
                  if (ai >= n_active) {
                    break;
                  }
                  const LSTablePageBlock& blk = store.blocks[active_blocks[ai]];
                  const LSTableColumnBlock& gk_col = blk.columns[full_gk0];
                  const LSTableColumnBlock* const jk_col =
                      has_join_key ? &blk.columns[full_jk0] : nullptr;
                  const int64_t* __restrict gk_ints = gk_col.int_cells.data();
                  const int64_t* __restrict jk_ints =
                      jk_col != nullptr ? jk_col->int_cells.data() : nullptr;
                  const bool gk_has_null = gk_col.has_null;
                  const std::span<const uint8_t> gk_nspan =
                      gk_has_null
                          ? (!gk_col.nulls_view.empty()
                                 ? gk_col.nulls_view
                                 : std::span<const uint8_t>(gk_col.nulls))
                          : std::span<const uint8_t>{};
                  const uint8_t* __restrict gk_nulls = gk_nspan.data();
                  const auto gk_null_len =
                      static_cast<uint32_t>(gk_nspan.size());

                  const bool jk_has_null =
                      jk_col != nullptr && jk_col->has_null;
                  const std::span<const uint8_t> jk_nspan =
                      jk_has_null
                          ? (!jk_col->nulls_view.empty()
                                 ? jk_col->nulls_view
                                 : std::span<const uint8_t>(jk_col->nulls))
                          : std::span<const uint8_t>{};
                  const uint8_t* __restrict jk_nulls = jk_nspan.data();
                  const auto jk_null_len =
                      static_cast<uint32_t>(jk_nspan.size());

                  const LSTableColumnBlock* const rf_col =
                      full_rf_col.has_value() ? &blk.columns[*full_rf_col]
                                              : nullptr;
                  const bool rf_type_ok =
                      rf_col == nullptr || rf_col->type == active_rf_type;
                  const bool rf_col_has_null =
                      rf_col != nullptr && rf_col->has_null;
                  const std::span<const uint8_t> rf_nspan =
                      rf_col_has_null
                          ? (!rf_col->nulls_view.empty()
                                 ? rf_col->nulls_view
                                 : std::span<const uint8_t>(rf_col->nulls))
                          : std::span<const uint8_t>{};
                  const uint8_t* __restrict rf_nulls = rf_nspan.data();
                  const auto rf_null_len =
                      static_cast<uint32_t>(rf_nspan.size());
                  const int64_t* __restrict rf_ints =
                      rf_col != nullptr ? rf_col->int_cells.data() : nullptr;

                  struct BsExtraRf {
                    const LSTableColumnBlock* col{nullptr};
                    const int64_t* ints{nullptr};
                    const uint64_t* dense_bits{nullptr};
                    int64_t lo{0};
                    int64_t hi{0};
                    bool has_null{false};
                    bool dense_all_valid{false};
                    bool type_ok{true};
                    const RuntimeFilterGroup* group{nullptr};
                  };
                  std::array<BsExtraRf, 4> extra_rfs{};
                  size_t extra_rf_count = 0;
                  for (const BsRfSpec& spec : extra_rf_specs) {
                    const LSTableColumnBlock* ecol =
                        &blk.columns[spec.full_col];
                    const bool hd = spec.group->has_dense_bits();
                    extra_rfs[extra_rf_count++] = BsExtraRf{
                        .col = ecol,
                        .ints = ecol->int_cells.data(),
                        .dense_bits =
                            hd ? spec.group->dense_bits_data() : nullptr,
                        .lo = hd ? spec.group->Lo() : 0,
                        .hi = hd ? spec.group->Hi() : 0,
                        .has_null = ecol->has_null,
                        .dense_all_valid = check_dense_all_valid(spec.group),
                        .type_ok = (ecol->type == spec.type),
                        .group = spec.group,
                    };
                  }

                  struct BlockNullPeek {
                    const uint8_t* ptr{nullptr};
                    uint32_t len{0};
                    bool expect_null{false};
                  };
                  std::array<BlockNullPeek, 8> row_null_peeks{};
                  size_t row_null_peek_count = 0;
                  bool overflow_null_peeks = false;
                  for (const NullPeekCompare& np :
                       deferred_left_probe->null_peeks) {
                    const LSTableColumnBlock& ncol = blk.columns[np.column];
                    if (!ncol.has_null) {
                      continue;
                    }
                    if (np.column == full_gk0 && !np.expect_null) {
                      continue;
                    }
                    if (has_join_key && np.column == full_jk0 &&
                        !np.expect_null) {
                      continue;
                    }
                    if (rf_col != nullptr && np.column == *full_rf_col &&
                        !np.expect_null) {
                      continue;
                    }
                    if (!np.expect_null) {
                      bool covered_by_extra = false;
                      for (const BsRfSpec& spec : extra_rf_specs) {
                        if (spec.full_col == np.column) {
                          covered_by_extra = true;
                          break;
                        }
                      }
                      if (covered_by_extra) {
                        continue;
                      }
                    }
                    if (row_null_peek_count >= row_null_peeks.size()) {
                      overflow_null_peeks = true;
                      break;
                    }
                    const std::span<const uint8_t> nsp =
                        !ncol.nulls_view.empty()
                            ? ncol.nulls_view
                            : std::span<const uint8_t>(ncol.nulls);
                    row_null_peeks[row_null_peek_count++] = BlockNullPeek{
                        .ptr = nsp.data(),
                        .len = static_cast<uint32_t>(nsp.size()),
                        .expect_null = np.expect_null,
                    };
                  }

                  bool peek_all_pass = true;
                  if (peek_ptr != nullptr) {
                    for (const IntegerPeekCompare& pred : *peek_ptr) {
                      const LSTableColumnBlock& pcol = blk.columns[pred.column];
                      if (pcol.has_null || !pcol.has_min_max) {
                        peek_all_pass = false;
                        break;
                      }
                      bool all_ok = false;
                      switch (pred.op) {
                        case BinaryOperation::kEquals:
                          all_ok = (pcol.min_int == pred.constant &&
                                    pcol.max_int == pred.constant);
                          break;
                        case BinaryOperation::kNotEquals:
                          all_ok = (pred.constant < pcol.min_int ||
                                    pred.constant > pcol.max_int);
                          break;
                        case BinaryOperation::kLessThan:
                          all_ok = (pcol.max_int < pred.constant);
                          break;
                        case BinaryOperation::kLessThanEquals:
                          all_ok = (pcol.max_int <= pred.constant);
                          break;
                        case BinaryOperation::kGreaterThan:
                          all_ok = (pcol.min_int > pred.constant);
                          break;
                        case BinaryOperation::kGreaterThanEquals:
                          all_ok = (pcol.min_int >= pred.constant);
                          break;
                        default:
                          break;
                      }
                      if (!all_ok) {
                        peek_all_pass = false;
                        break;
                      }
                    }
                  }

                  uint32_t r_begin = 0;
                  uint32_t r_end = blk.row_count;
                  bool blk_all_match_right = !has_join_key;
                  if (has_join_key && use_dense_int_rows && dense_all_valid &&
                      off0 == 0 && jk_col->has_min_max) {
                    if (jk_col->min_int >= dense_int_min &&
                        static_cast<uint64_t>(jk_col->max_int) -
                                static_cast<uint64_t>(dense_int_min) <
                            dense_len) {
                      blk_all_match_right = true;
                    } else if (jk_is_monotonic && !jk_has_null) {
                      const int64_t dense_hi =
                          dense_int_min + static_cast<int64_t>(dense_len - 1);
                      const int64_t* const lo_p = std::lower_bound(
                          jk_ints, jk_ints + r_end, dense_int_min);
                      const int64_t* const hi_p =
                          std::upper_bound(lo_p, jk_ints + r_end, dense_hi);
                      r_begin = static_cast<uint32_t>(lo_p - jk_ints);
                      r_end = static_cast<uint32_t>(hi_p - jk_ints);
                      blk_all_match_right = true;
                    }
                  }

                  bool blk_rf_range_all_pass = (rf_col == nullptr);
                  if (rf_col != nullptr && rf_dense_all_valid &&
                      rf_col->has_min_max) {
                    if (rf_col->min_int >= rf_lo && rf_col->max_int <= rf_hi) {
                      blk_rf_range_all_pass = true;
                    } else if (rf_is_monotonic && !rf_col_has_null &&
                               blk_all_match_right) {
                      const int64_t* const lo_p = std::lower_bound(
                          rf_ints + r_begin, rf_ints + r_end, rf_lo);
                      const int64_t* const hi_p =
                          std::upper_bound(lo_p, rf_ints + r_end, rf_hi);
                      r_begin = static_cast<uint32_t>(lo_p - rf_ints);
                      r_end = static_cast<uint32_t>(hi_p - rf_ints);
                      blk_rf_range_all_pass = true;
                    }
                  }

                  const bool gk_need_bounds =
                      has_gk0_bound &&
                      (gk_col.min_int < gk_min || gk_col.max_int > gk_max);
                  const bool rf_null_safe =
                      !rf_col_has_null || (rf_null_len >= r_end);
                  const bool gk_null_safe =
                      !gk_has_null || (gk_null_len >= r_end);

                  const bool block_simple_mode =
                      blk_all_match_right && !overflow_null_peeks &&
                      row_null_peek_count == 0 && peek_all_pass &&
                      extra_rf_count == 0 && !jk_has_null && rf_null_safe &&
                      gk_null_safe &&
                      (blk_rf_range_all_pass ||
                       (rf_col != nullptr && rf_type_ok && rf_dense_all_valid));
                  if (block_simple_mode) {
                    const auto launch_kernel = [&]<bool HasRfNull,
                                                   bool CheckRfRange,
                                                   bool HasGkNull>() {
                      if (!gk_need_bounds) {
                        // NOLINTBEGIN(readability-suspicious-call-argument)
                        run_simple_kernel.template
                        operator()<HasRfNull, CheckRfRange, HasGkNull, false>(
                            r_begin, r_end, rf_ints, rf_nulls, gk_ints,
                            gk_nulls);
                        // NOLINTEND(readability-suspicious-call-argument)
                      } else {
                        // NOLINTBEGIN(readability-suspicious-call-argument)
                        run_simple_kernel.template
                        operator()<HasRfNull, CheckRfRange, HasGkNull, true>(
                            r_begin, r_end, rf_ints, rf_nulls, gk_ints,
                            gk_nulls);
                        // NOLINTEND(readability-suspicious-call-argument)
                      }
                    };
                    const bool has_rfn = rf_col_has_null;
                    const bool chk_rfr = !blk_rf_range_all_pass && rf_type_ok;
                    const bool has_gkn = gk_has_null;
                    if (!has_rfn) {
                      if (!chk_rfr) {
                        if (!has_gkn) {
                          launch_kernel
                              .template operator()<false, false, false>();
                        } else {
                          launch_kernel
                              .template operator()<false, false, true>();
                        }
                      } else {
                        if (!has_gkn) {
                          launch_kernel
                              .template operator()<false, true, false>();
                        } else {
                          launch_kernel
                              .template operator()<false, true, true>();
                        }
                      }
                    } else {
                      if (!chk_rfr) {
                        if (!has_gkn) {
                          launch_kernel
                              .template operator()<true, false, false>();
                        } else {
                          launch_kernel
                              .template operator()<true, false, true>();
                        }
                      } else {
                        if (!has_gkn) {
                          launch_kernel
                              .template operator()<true, true, false>();
                        } else {
                          launch_kernel.template operator()<true, true, true>();
                        }
                      }
                    }
                    continue;
                  }

                  for (uint32_t r = r_begin; r < r_end; ++r) {
                    if (rf_col != nullptr) {
                      if (rf_col_has_null && r < rf_null_len &&
                          rf_nulls[r] != 0) [[unlikely]] {
                        continue;
                      }
                      if (!blk_rf_range_all_pass && rf_type_ok) {
                        const int64_t kv = rf_ints[r];
                        if (rf_dense_all_valid) {
                          if (static_cast<uint64_t>(kv) - lrf_base >
                              lrf_span_m1) {
                            continue;
                          }
                        } else if (rf_has_dense) {
                          const uint64_t rf_off =
                              static_cast<uint64_t>(kv) - lrf_base;
                          if (rf_off > lrf_span_m1 ||
                              (rf_dense_bits[rf_off >> 6] &
                               (uint64_t{1} << (rf_off & 63))) == 0) {
                            continue;
                          }
                        } else if (active_rf_group->Reject(kv)) {
                          continue;
                        }
                      }
                    }

                    if (extra_rf_count != 0) {
                      bool extra_ok = true;
                      for (size_t ei = 0; ei < extra_rf_count; ++ei) {
                        const BsExtraRf& erf = extra_rfs[ei];
                        if (erf.has_null && erf.col->IsNull(r)) {
                          extra_ok = false;
                          break;
                        }
                        if (!erf.type_ok) {
                          continue;
                        }
                        const int64_t kv = erf.ints[r];
                        if (erf.dense_all_valid) {
                          if (kv < erf.lo || kv > erf.hi) {
                            extra_ok = false;
                            break;
                          }
                        } else if (erf.dense_bits != nullptr) {
                          if (kv < erf.lo || kv > erf.hi) {
                            extra_ok = false;
                            break;
                          }
                          const uint64_t rf_off = static_cast<uint64_t>(kv) -
                                                  static_cast<uint64_t>(erf.lo);
                          if ((erf.dense_bits[rf_off >> 6] &
                               (uint64_t{1} << (rf_off & 63))) == 0) {
                            extra_ok = false;
                            break;
                          }
                        } else if (erf.group->Reject(kv)) {
                          extra_ok = false;
                          break;
                        }
                      }
                      if (!extra_ok) {
                        continue;
                      }
                    }

                    if (!peek_all_pass && peek_ptr != nullptr) {
                      bool peek_ok = true;
                      for (const IntegerPeekCompare& pred : *peek_ptr) {
                        const LSTableColumnBlock& pcol =
                            blk.columns[pred.column];
                        if ((pcol.has_null && pcol.IsNull(r)) ||
                            !eval_bs_peek(pred.op, pcol.int_cells[r],
                                          pred.constant)) {
                          peek_ok = false;
                          break;
                        }
                      }
                      if (!peek_ok) {
                        continue;
                      }
                    }

                    if (overflow_null_peeks) {
                      if (std::ranges::any_of(
                              deferred_left_probe->null_peeks,
                              [&](const NullPeekCompare& test) {
                                return blk.columns[test.column].IsNull(r) !=
                                       test.expect_null;
                              })) {
                        continue;
                      }
                    } else if (row_null_peek_count != 0) {
                      bool np_fail = false;
                      for (size_t ni = 0; ni < row_null_peek_count; ++ni) {
                        const BlockNullPeek& np = row_null_peeks[ni];
                        const bool is_n = r < np.len && np.ptr[r] != 0;
                        if (is_n != np.expect_null) {
                          np_fail = true;
                          break;
                        }
                      }
                      if (np_fail) {
                        continue;
                      }
                    }

                    if (jk_has_null && r < jk_null_len && jk_nulls[r] != 0)
                        [[unlikely]] {
                      continue;
                    }

                    const bool gk_null =
                        gk_has_null && r < gk_null_len && gk_nulls[r] != 0;
                    if (gk_null) [[unlikely]] {
                      if (lgk_null_rejects || local_saw_null != 0) {
                        continue;
                      }
                    } else {
                      const uint64_t off =
                          static_cast<uint64_t>(gk_ints[r]) - lgk_base;
                      if (gk_need_bounds && off >= lgk_span) [[unlikely]] {
                        continue;
                      }
                      if (lbitset_first_skip &&
                          (wbits[off >> 6] & (uint64_t{1} << (off & 63))) !=
                              0) {
                        continue;
                      }
                    }

                    if (!blk_all_match_right) {
                      const int64_t jkey = AddJoinKeyBias(jk_ints[r], off0);
                      if (!check_right_bs(jkey)) {
                        continue;
                      }
                    }

                    if (gk_null) [[unlikely]] {
                      local_saw_null = 1;
                    } else {
                      const uint64_t off =
                          static_cast<uint64_t>(gk_ints[r]) - lgk_base;
                      const uint64_t bit = uint64_t{1} << (off & 63);
                      if ((wbits[off >> 6] & bit) == 0) {
                        wbits[off >> 6] |= bit;
                      }
                    }
                  }
                }
                worker_saw_null[w] = local_saw_null;
              };

              if (use_workers == 1) {
                run_bs_scan(0);
              } else {
                RETURN_IF_FAIL(RunParallelTasks(use_workers, run_bs_scan));
              }
            }

            bool any_null = worker_saw_null[0] != 0;
            size_t total_unique = any_null ? 1 : 0;
            if (!active_blocks.empty()) {
              uint64_t* __restrict merged = (*worker_bitsets)[0].data();
              for (size_t w = 1; w < use_workers; ++w) {
                any_null = any_null || (worker_saw_null[w] != 0);
                const uint64_t* __restrict src = (*worker_bitsets)[w].data();
                for (size_t i = 0; i < n_words; ++i) {
                  merged[i] |= src[i];
                }
              }
              if (gk0_rfs.empty()) {
                for (size_t i = 0; i < n_words; ++i) {
                  total_unique += static_cast<size_t>(std::popcount(merged[i]));
                }
              } else {
                const auto gk_base = static_cast<uint64_t>(gk_min);
                const size_t filter_workers = n_words >= 512 ? use_workers : 1;
                std::vector<size_t> worker_pop(filter_workers, 0);
                const auto filter_words = [&](size_t fw) {
                  const size_t w_begin = n_words * fw / filter_workers;
                  const size_t w_end = n_words * (fw + 1) / filter_workers;
                  size_t pop = 0;
                  for (size_t wi = w_begin; wi < w_end; ++wi) {
                    uint64_t wv = merged[wi];
                    if (wv == 0) {
                      continue;
                    }
                    uint64_t keep = 0;
                    const uint64_t word_base =
                        gk_base + (static_cast<uint64_t>(wi) << 6);
                    while (wv != 0) {
                      const auto b =
                          static_cast<uint32_t>(std::countr_zero(wv));
                      wv &= (wv - 1);
                      const auto val = static_cast<int64_t>(word_base + b);
                      bool pass = true;
                      for (const RuntimeFilterGroup* g : gk0_rfs) {
                        if (g->Reject(val)) {
                          pass = false;
                          break;
                        }
                      }
                      if (pass) {
                        keep |= (uint64_t{1} << b);
                      }
                    }
                    merged[wi] = keep;
                    pop += static_cast<size_t>(std::popcount(keep));
                  }
                  worker_pop[fw] = pop;
                };
                if (filter_workers == 1) {
                  filter_words(0);
                } else {
                  RETURN_IF_FAIL(
                      RunParallelTasks(filter_workers, filter_words));
                }
                for (const size_t p : worker_pop) {
                  total_unique += p;
                }
              }
            }

            std::vector<Column> dist_output_columns;
            dist_output_columns.reserve(fusion->select_names.size());
            for (const std::string& name : fusion->select_names) {
              dist_output_columns.emplace_back(name, ValueType::kNull);
            }
            for (size_t c = 0; c < dist_items.size(); ++c) {
              if (dist_items[c].kind == DistinctDirectItem::Kind::kConst) {
                const Value& v = dist_items[c].constant;
                if (!v.IsNull()) {
                  Column col(dist_output_columns[c].Name(), ValueTypeOf(v));
                  col.SetUnsigned(v.IsUnsigned());
                  col.SetInterval(v.IsInterval());
                  dist_output_columns[c] = std::move(col);
                }
              } else {
                dist_output_columns[c] =
                    Column(dist_output_columns[c].Name(), gk_types[0]);
              }
            }

            const auto for_each_bitset_key = [&](auto&& emit_key) {
              if (any_null) {
                emit_key(true, 0);
              }
              if (active_blocks.empty()) {
                return;
              }
              const uint64_t* const bits = (*worker_bitsets)[0].data();
              for (size_t wi = 0; wi < n_words; ++wi) {
                uint64_t word = bits[wi];
                while (word != 0) {
                  const auto bit =
                      static_cast<uint32_t>(std::countr_zero(word));
                  word &= (word - 1);
                  const auto val = static_cast<int64_t>(
                      static_cast<uint64_t>(gk_min) +
                      (static_cast<uint64_t>(wi) << 6) + bit);
                  emit_key(false, val);
                }
              }
            };

            bool pushed_distinct = false;
            if (fusion->output_sink != nullptr) {
              auto batch_charge =
                  TryReserveQueryMemory(GroupedOutputSink::kBatchRows *
                                        dist_items.size() * sizeof(Value));
              bool accepted = false;
              if (batch_charge) {
                Status begin_status = Status::kSuccess;
                RETURN_IF_FAIL(RunParallelTasks(1, [&](size_t) {
                  auto begun = fusion->output_sink->begin(
                      Schema("", dist_output_columns), total_unique, 1);
                  begin_status = begun.GetStatus();
                  if (begun.HasValue()) {
                    accepted = begun.Value();
                  }
                }));
                RETURN_IF_FAIL(begin_status);
              }
              if (accepted) {
                fusion->output_sink->used = true;
                pushed_distinct = true;
                const size_t width = dist_items.size();
                std::vector<Value> batch(GroupedOutputSink::kBatchRows * width);
                size_t batch_rows = 0;
                size_t base_first = 0;
                Status sink_status = Status::kSuccess;
                for_each_bitset_key([&](bool is_null, int64_t val) {
                  if (!sink_status.ok()) {
                    return;
                  }
                  for (size_t c = 0; c < width; ++c) {
                    if (dist_items[c].kind ==
                        DistinctDirectItem::Kind::kConst) {
                      batch[(batch_rows * width) + c] = dist_items[c].constant;
                    } else if (is_null) {
                      batch[(batch_rows * width) + c] = Value();
                    } else {
                      batch[(batch_rows * width) + c] =
                          gk_types[0] == ValueType::kDate
                              ? Value::DateFromDays(val)
                              : Value(val);
                    }
                  }
                  ++batch_rows;
                  if (batch_rows == GroupedOutputSink::kBatchRows) {
                    sink_status = fusion->output_sink->push(
                        0, base_first,
                        std::span<const Value>(batch.data(),
                                               batch_rows * width));
                    base_first += batch_rows;
                    batch_rows = 0;
                  }
                });
                if (sink_status.ok() && batch_rows > 0) {
                  sink_status = fusion->output_sink->push(
                      0, base_first,
                      std::span<const Value>(batch.data(), batch_rows * width));
                }
                RETURN_IF_FAIL(sink_status);
                if (auto* runtime = context.execution_runtime()) {
                  runtime->pushed_group_rows += total_unique;
                }
              }
            }

            std::shared_ptr<ColumnarTable> out_distinct_columnar;
            bool distinct_uniform = false;
            if (!pushed_distinct && fusion->columnar_output) {
              out_distinct_columnar = std::make_shared<ColumnarTable>();
              if (total_unique > 0) {
                distinct_uniform = true;
                const size_t width = dist_items.size();
                std::vector<std::pair<bool, int64_t>> chunk_buf;
                chunk_buf.reserve(kColumnarChunkRows);
                const auto flush_chunk = [&]() {
                  const size_t count = chunk_buf.size();
                  if (count == 0) {
                    return;
                  }
                  ColumnarChunk chunk;
                  chunk.rows = count;
                  chunk.columns.resize(width);
                  ChunkStringArena* arena = nullptr;
                  for (size_t c = 0; c < width; ++c) {
                    const Column& proto = dist_output_columns[c];
                    if (dist_items[c].kind ==
                            DistinctDirectItem::Kind::kConst &&
                        TrySetConstantColumnarColumn(
                            &chunk.columns[c], proto.Type(), proto.IsUnsigned(),
                            proto.IsInterval(), dist_items[c].constant, count,
                            &chunk)) {
                      arena = chunk.string_arena.get();
                      continue;
                    }
                    chunk.columns[c].Init(proto.Type(), proto.IsUnsigned(),
                                          proto.IsInterval(), count);
                    if (dist_items[c].kind ==
                        DistinctDirectItem::Kind::kConst) {
                      for (size_t r = 0; r < count; ++r) {
                        if (dist_items[c].constant.type ==
                                ValueType::kVarChar &&
                            arena == nullptr) {
                          arena = chunk.EnsureArena();
                        }
                        chunk.columns[c].AppendValue(dist_items[c].constant,
                                                     arena, r);
                      }
                    } else {
                      for (size_t r = 0; r < count; ++r) {
                        if (chunk_buf[r].first) {
                          chunk.columns[c].AppendNull(r);
                        } else {
                          chunk.columns[c].AppendInt(chunk_buf[r].second);
                        }
                      }
                    }
                    if (!chunk.columns[c].is_uniform) {
                      distinct_uniform = false;
                    }
                  }
                  out_distinct_columnar->AppendChunk(std::move(chunk));
                  chunk_buf.clear();
                };
                for_each_bitset_key([&](bool is_null, int64_t val) {
                  chunk_buf.emplace_back(is_null, val);
                  if (chunk_buf.size() == kColumnarChunkRows) {
                    flush_chunk();
                  }
                });
                flush_chunk();
              }
            } else if (!pushed_distinct) {
              result.rows.reserve(total_unique);
              const size_t width = dist_items.size();
              for_each_bitset_key([&](bool is_null, int64_t val) {
                Row row;
                row.values_.reserve(width);
                for (size_t c = 0; c < width; ++c) {
                  if (dist_items[c].kind == DistinctDirectItem::Kind::kConst) {
                    row.values_.push_back(dist_items[c].constant);
                  } else if (is_null) {
                    row.values_.emplace_back();
                  } else {
                    row.values_.push_back(gk_types[0] == ValueType::kDate
                                              ? Value::DateFromDays(val)
                                              : Value(val));
                  }
                }
                result.rows.push_back(std::move(row));
              });
            }

            result.hash_joins = left.hash_joins + right.hash_joins + 1;
            result.in_memory_hash_joins =
                left.in_memory_hash_joins + right.in_memory_hash_joins + 1;
            result.nested_loop_joins =
                left.nested_loop_joins + right.nested_loop_joins;
            result.schema = Schema("", std::move(dist_output_columns));
            if (out_distinct_columnar != nullptr) {
              const bool non_empty = out_distinct_columnar->total_rows > 0;
              result.SetColumnarTable(std::move(out_distinct_columnar),
                                      distinct_uniform && non_empty);
            }
            result.peak_intermediate_rows =
                std::max(result.peak_intermediate_rows, result.TotalRows());
            if (auto* runtime = context.execution_runtime()) {
              runtime->join_ms += ElapsedMs(join_begin);
              ++runtime->grouped_join_fusions;
            }
            return DistinctLSTableProbeOutcome{.handled = true,
                                               .result = std::move(result)};
          }
        }
      }
    }
    struct DistinctIntEntry {
      int64_t k0{0};
      int64_t k1{0};
      uint8_t null_mask{0};
      bool operator==(const DistinctIntEntry& o) const noexcept {
        return k0 == o.k0 && k1 == o.k1 && null_mask == o.null_mask;
      }
    };
    const auto hash_entry = [](const DistinctIntEntry& e) noexcept -> uint64_t {
      uint64_t x = static_cast<uint64_t>(e.k0) * 0x9e3779b97f4a7c15ULL;
      x ^= static_cast<uint64_t>(e.k1) * 0xbf58476d1ce4e5b9ULL;
      x ^= static_cast<uint64_t>(e.null_mask) * 0x94d049bb133111ebULL;
      x ^= x >> 30;
      x *= 0xbf58476d1ce4e5b9ULL;
      x ^= x >> 27;
      x *= 0x94d049bb133111ebULL;
      return x ^ (x >> 31);
    };
    const size_t n_keys = key_programs.size();
    const size_t kParts = scan_workers > 1 ? 64 : 16;
    const size_t part_shift = scan_workers > 1 ? 58 : 60;
    const size_t part_mask = kParts - 1;
    bool use_jit_packed_dedup =
        deferred_left_probe != nullptr && n_keys == 2 &&
        std::getenv("TINYLAMB_DISABLE_JIT_PACKED_DEDUP") == nullptr;
    std::array<std::optional<JitPacked2x32RadixDedup::PackKernel>, 8>
        jit_pack_kernels;
    std::optional<JitPacked2x32RadixDedup::ScatterKernel> jit_scatter_kernel;
    std::optional<JitPacked2x32RadixDedup::DedupKernel> jit_dedup_kernel;
    if (use_jit_packed_dedup) {
      const auto pshift_u8 = static_cast<uint8_t>(part_shift);
      for (size_t mask = 0; mask < 8; ++mask) {
        const bool hn0 = (mask & 4U) != 0;
        const bool hn1 = (mask & 2U) != 0;
        const bool usel = (mask & 1U) != 0;
        jit_pack_kernels[mask] =
            JitPacked2x32RadixDedup::CompilePack(hn0, hn1, usel, pshift_u8);
        if (!jit_pack_kernels[mask].has_value()) {
          use_jit_packed_dedup = false;
          break;
        }
      }
      if (use_jit_packed_dedup) {
        jit_scatter_kernel = JitPacked2x32RadixDedup::CompileScatter(pshift_u8);
        jit_dedup_kernel = JitPacked2x32RadixDedup::CompileOpenAddrDedup();
        if (!jit_scatter_kernel.has_value() || !jit_dedup_kernel.has_value()) {
          use_jit_packed_dedup = false;
        }
      }
    }
    bool jit_rf_dense_all_set = false;
    bool jit_dense_join_all_valid = false;
    if (use_jit_packed_dedup && deferred_left_probe->runtime_filter.enabled()) {
      const RuntimeFilterGroup* rf_g =
          deferred_left_probe->runtime_filter.group.get();
      if (rf_g != nullptr && rf_g->has_dense_bits()) {
        const int64_t rlo = rf_g->Lo();
        const int64_t rhi = rf_g->Hi();
        if (rhi >= rlo &&
            static_cast<uint64_t>(rhi) - static_cast<uint64_t>(rlo) <
                65536ULL) {
          const uint64_t rspan =
              static_cast<uint64_t>(rhi) - static_cast<uint64_t>(rlo) + 1ULL;
          const auto full_words = static_cast<size_t>(rspan >> 6);
          const auto rem_bits = static_cast<uint32_t>(rspan & 63ULL);
          const auto need_words = static_cast<size_t>((rspan + 63ULL) >> 6);
          if (need_words <= rf_g->dense_bits_word_count()) {
            const uint64_t* bits = rf_g->dense_bits_data();
            bool all_set = true;
            for (size_t wi = 0; wi < full_words; ++wi) {
              if (bits[wi] != ~uint64_t{0}) {
                all_set = false;
                break;
              }
            }
            if (all_set && rem_bits != 0) {
              const uint64_t tail_mask = (uint64_t{1} << rem_bits) - 1ULL;
              all_set = (bits[full_words] & tail_mask) == tail_mask;
            }
            jit_rf_dense_all_set = all_set;
          }
        }
      }
    }
    if (use_jit_packed_dedup && !singleton_probe && use_dense_int_rows &&
        !dense_int_idxs.empty()) {
      jit_dense_join_all_valid = true;
      for (const uint32_t v : dense_int_idxs) {
        if (v == kInvalidRightIdx) {
          jit_dense_join_all_valid = false;
          break;
        }
      }
    }
    std::vector<std::vector<std::vector<DistinctIntEntry>>> worker_parts(
        scan_workers);
    for (auto& wp : worker_parts) {
      wp.resize(kParts);
    }
    std::vector<std::vector<std::vector<uint64_t>>> worker_packed_parts(
        use_jit_packed_dedup ? scan_workers : 0);
    for (auto& wpp : worker_packed_parts) {
      wpp.resize(kParts);
    }
    const auto match_right_key = [&](int64_t jkey) noexcept -> bool {
      if (singleton_probe) {
        return true;
      }
      if (use_dense_int_rows) {
        const uint64_t idx =
            static_cast<uint64_t>(jkey) - static_cast<uint64_t>(dense_int_min);
        return idx < dense_int_idxs.size() &&
               dense_int_idxs[idx] != kInvalidRightIdx;
      }
      if (use_unique_int_table) {
        size_t slot = hash_unique_int_key(jkey) & unique_int_mask;
        while (unique_int_table[slot].r_idx !=
               std::numeric_limits<uint32_t>::max()) {
          if (unique_int_table[slot].key == jkey) [[likely]] {
            return true;
          }
          slot = (slot + 1) & unique_int_mask;
        }
        return false;
      }
      const auto [first, last] = int_buckets->equal_range(jkey);
      return first != last;
    };
    const slot_t jk0 = singleton_probe ? 0 : left_columns[0];
    const int64_t off0 = singleton_probe ? 0 : left_key_offsets[0];
    const auto eval_int_peek = [](BinaryOperation op, int64_t a,
                                  int64_t b) noexcept -> bool {
      switch (op) {
        case BinaryOperation::kEquals:
          return a == b;
        case BinaryOperation::kNotEquals:
          return a != b;
        case BinaryOperation::kLessThan:
          return a < b;
        case BinaryOperation::kLessThanEquals:
          return a <= b;
        case BinaryOperation::kGreaterThan:
          return a > b;
        case BinaryOperation::kGreaterThanEquals:
          return a >= b;
        default:
          return false;
      }
    };
    std::atomic<size_t> dist_next_block{0};
    std::atomic<size_t> dist_next_chunk{0};
    const auto run_dist_scan = [&](size_t w) {
      auto& wp = worker_parts[w];
      constexpr size_t kL1Cap = 2048;
      std::vector<DistinctIntEntry> l1_entries(kL1Cap);
      std::vector<uint8_t> l1_valid(kL1Cap, 0);
      const auto record_entry = [&](int64_t v0, bool n0, int64_t v1, bool n1) {
        const auto nm =
            static_cast<uint8_t>((n0 ? 1 : 0) | ((n_keys == 2 && n1) ? 2 : 0));
        const DistinctIntEntry e{
            .k0 = n0 ? 0 : v0,
            .k1 = (n_keys == 2 && !n1) ? v1 : 0,
            .null_mask = nm,
        };
        const uint64_t h = hash_entry(e);
        const size_t l1_idx = static_cast<size_t>(h) & (kL1Cap - 1);
        if (l1_valid[l1_idx] != 0 && l1_entries[l1_idx] == e) {
          return;
        }
        l1_valid[l1_idx] = 1;
        l1_entries[l1_idx] = e;
        if (use_jit_packed_dedup && !n0 && !n1 &&
            v0 >= std::numeric_limits<int32_t>::min() &&
            v0 <= std::numeric_limits<int32_t>::max() &&
            v1 >= std::numeric_limits<int32_t>::min() &&
            v1 <= std::numeric_limits<int32_t>::max()) {
          const uint64_t pk =
              (static_cast<uint64_t>(
                   static_cast<uint32_t>(static_cast<int32_t>(v0)))
               << 32) |
              static_cast<uint64_t>(
                  static_cast<uint32_t>(static_cast<int32_t>(v1)));
          worker_packed_parts[w]
                             [(HashPacked2x32Key(pk) >> part_shift) & part_mask]
                                 .push_back(pk);
          return;
        }
        wp[(h >> part_shift) & part_mask].push_back(e);
      };
      if (deferred_left_probe != nullptr) {
        std::vector<uint32_t> sel_buf;
        std::vector<uint32_t> fb_rows;
        std::vector<uint64_t> packed_buf;
        std::array<uint32_t, 64> blk_hist{};
        std::array<uint64_t*, 64> dst_ptrs{};
        std::array<uint32_t, 64> dst_pos{};
        const LSTableStore& store = *deferred_left_probe->store;
        const size_t total_blocks = store.blocks.size();
        const size_t base_proj_cols = deferred_left_probe->projection.size();
        const auto* peek_ptr = deferred_left_probe->integer_peeks.empty()
                                   ? nullptr
                                   : &deferred_left_probe->integer_peeks;
        const ScanRuntimeFilter& primary_rf =
            deferred_left_probe->runtime_filter;
        const RuntimeFilterGroup* active_rf_group =
            primary_rf.enabled() ? primary_rf.group.get() : nullptr;
        const std::optional<slot_t> full_rf_col =
            (active_rf_group != nullptr && primary_rf.column < base_proj_cols)
                ? std::optional<slot_t>(
                      deferred_left_probe->projection[primary_rf.column])
                : std::nullopt;
        const bool rf_has_dense =
            active_rf_group != nullptr && active_rf_group->has_dense_bits();
        const uint64_t* const rf_dense_bits =
            rf_has_dense ? active_rf_group->dense_bits_data() : nullptr;
        const size_t rf_dense_words =
            rf_has_dense ? active_rf_group->dense_bits_word_count() : 0;
        const int64_t rf_lo = rf_has_dense ? active_rf_group->Lo() : 0;
        const int64_t rf_hi = rf_has_dense ? active_rf_group->Hi() : 0;
        const bool rf_col_monotonic =
            rf_has_dense && full_rf_col.has_value() &&
            *full_rf_col < store.column_is_monotonic.size() &&
            store.column_is_monotonic[*full_rf_col] != 0;
        std::array<const LSTableColumnBlock*, 16> proj_cols{};
        std::array<const int64_t*, 16> proj_int_ptrs{};
        std::array<uint8_t, 16> proj_has_null{};
        while (true) {
          const size_t bi =
              dist_next_block.fetch_add(1, std::memory_order_relaxed);
          if (bi >= total_blocks) {
            break;
          }
          const LSTablePageBlock& blk = store.blocks[bi];
          if (blk.row_count == 0) {
            continue;
          }
          if (!blk.MatchesZoneMaps(peek_ptr, active_rf_group, full_rf_col)) {
            continue;
          }
          bool extra_zm_reject = false;
          for (const ScanRuntimeFilter& ext : primary_rf.extra) {
            if (!ext.enabled() || ext.column >= base_proj_cols) {
              continue;
            }
            if (ext.group->RejectsAll()) {
              extra_zm_reject = true;
              break;
            }
            const slot_t ext_full = deferred_left_probe->projection[ext.column];
            if (ext_full < blk.columns.size()) {
              const LSTableColumnBlock& mcol = blk.columns[ext_full];
              // RejectRange speaks the int64 key domain; a block column of
              // any other type must not feed it (same guard as the sister
              // delayed-scan loop above and RuntimeFilterGroup::Reject).
              if ((mcol.type == ValueType::kInt64 ||
                   mcol.type == ValueType::kDate) &&
                  mcol.type == ext.type &&
                  (!mcol.has_min_max ||
                   ext.group->RejectRange(mcol.min_int, mcol.max_int))) {
                extra_zm_reject = true;
                break;
              }
            }
          }
          if (extra_zm_reject) {
            continue;
          }
          if (!singleton_probe && use_dense_int_rows &&
              !dense_int_idxs.empty() && off0 == 0) {
            const slot_t full_key = deferred_left_probe->projection[jk0];
            if (full_key < blk.columns.size()) {
              const LSTableColumnBlock& jk = blk.columns[full_key];
              if (jk.has_min_max) {
                const uint64_t span = dense_int_idxs.size();
                const bool above =
                    jk.min_int >= dense_int_min &&
                    static_cast<uint64_t>(jk.min_int) -
                            static_cast<uint64_t>(dense_int_min) >=
                        span;
                if (jk.max_int < dense_int_min || above) {
                  continue;
                }
              }
            }
          }
          bool peek_all_pass = true;
          if (peek_ptr != nullptr) {
            for (const IntegerPeekCompare& pred : *peek_ptr) {
              const LSTableColumnBlock& pcol = blk.columns[pred.column];
              if (pcol.has_null || !pcol.has_min_max ||
                  !eval_int_peek(pred.op, pcol.min_int, pred.constant) ||
                  !eval_int_peek(pred.op, pcol.max_int, pred.constant)) {
                peek_all_pass = false;
                break;
              }
            }
          }
          for (size_t c = 0; c < base_proj_cols; ++c) {
            const LSTableColumnBlock& cblk =
                blk.columns[deferred_left_probe->projection[c]];
            proj_cols[c] = &cblk;
            proj_int_ptrs[c] = cblk.int_cells.data();
            proj_has_null[c] = static_cast<uint8_t>(cblk.has_null);
          }
          const LSTableColumnBlock* const rf_col =
              full_rf_col.has_value() ? &blk.columns[*full_rf_col] : nullptr;
          const bool rf_col_has_null = rf_col != nullptr && rf_col->has_null;
          const bool blk_rf_monotonic = rf_col_monotonic && !rf_col_has_null;
          struct DefExtraRf {
            const LSTableColumnBlock* col{nullptr};
            const uint64_t* dense_bits{nullptr};
            int64_t lo{0};
            int64_t hi{0};
            bool has_null{false};
            const RuntimeFilterGroup* group{nullptr};
          };
          std::array<DefExtraRf, 4> extra_rfs{};
          size_t extra_rf_count = 0;
          for (const ScanRuntimeFilter& ext : primary_rf.extra) {
            if (!ext.enabled() || ext.column >= base_proj_cols ||
                extra_rf_count >= extra_rfs.size()) {
              continue;
            }
            const LSTableColumnBlock* ecol = proj_cols[ext.column];
            const bool hd = ext.group->has_dense_bits();
            extra_rfs[extra_rf_count++] = DefExtraRf{
                .col = ecol,
                .dense_bits = hd ? ext.group->dense_bits_data() : nullptr,
                .lo = hd ? ext.group->Lo() : 0,
                .hi = hd ? ext.group->Hi() : 0,
                .has_null = ecol->has_null,
                .group = ext.group.get(),
            };
          }
          const LSTableColumnBlock* const jk0_col =
              singleton_probe ? nullptr : proj_cols[jk0];
          const int64_t* const jk0_ints =
              singleton_probe ? nullptr : proj_int_ptrs[jk0];
          const bool jk_has_null = !singleton_probe && proj_has_null[jk0] != 0;
          const LSTableColumnBlock* const gk0_col = proj_cols[gk_slots[0]];
          const int64_t* const gk0_ints = proj_int_ptrs[gk_slots[0]];
          const bool gk0_has_null = proj_has_null[gk_slots[0]] != 0;
          const LSTableColumnBlock* const gk1_col =
              n_keys == 2 ? proj_cols[gk_slots[1]] : nullptr;
          const int64_t* const gk1_ints =
              n_keys == 2 ? proj_int_ptrs[gk_slots[1]] : nullptr;
          const bool gk1_has_null =
              n_keys == 2 && proj_has_null[gk_slots[1]] != 0;
          const bool blk_can_jit_pack =
              use_jit_packed_dedup && deferred_left_probe->null_peeks.empty() &&
              extra_rf_count == 0 && peek_all_pass &&
              (rf_col == nullptr ||
               (rf_has_dense && rf_hi >= rf_lo &&
                (static_cast<uint64_t>(rf_hi) - static_cast<uint64_t>(rf_lo)) >>
                    6 < rf_dense_words &&
                rf_col->type == primary_rf.type &&
                rf_col->type != ValueType::kNull &&
                rf_col->int_cells.size() >= blk.row_count &&
                (!rf_col_has_null ||
                 rf_col->Nulls().size() >= blk.row_count))) &&
              (singleton_probe ||
               (use_dense_int_rows && off0 == 0 && !dense_int_idxs.empty() &&
                jk0_col->type != ValueType::kNull &&
                jk0_col->int_cells.size() >= blk.row_count &&
                (!jk_has_null || jk0_col->Nulls().size() >= blk.row_count))) &&
              gk0_col->type != ValueType::kNull &&
              gk1_col->type != ValueType::kNull &&
              gk0_col->int_cells.size() >= blk.row_count &&
              gk1_col->int_cells.size() >= blk.row_count &&
              (!gk0_has_null || gk0_col->Nulls().size() >= blk.row_count) &&
              (!gk1_has_null || gk1_col->Nulls().size() >= blk.row_count);
          if (blk_can_jit_pack) {
            bool use_sel = false;
            uint32_t sel_cnt = blk.row_count;
            if (rf_col != nullptr) {
              const bool full_blk_rf =
                  jit_rf_dense_all_set && !rf_col_has_null &&
                  rf_col->has_min_max && rf_col->min_int >= rf_lo &&
                  rf_col->max_int <= rf_hi;
              if (!full_blk_rf) {
                if (sel_buf.size() < blk.row_count) {
                  sel_buf.resize(blk.row_count);
                }
                sel_cnt = static_cast<uint32_t>(PrefilterDenseRfBlock(
                    rf_col->int_cells.data(), blk.row_count, rf_lo, rf_hi,
                    rf_dense_bits, jit_rf_dense_all_set, rf_col,
                    rf_col_has_null, sel_buf.data()));
                if (sel_cnt == 0) {
                  continue;
                }
                use_sel = true;
              }
            }
            if (!singleton_probe) {
              const bool full_blk_join =
                  jit_dense_join_all_valid && !jk_has_null &&
                  jk0_col->has_min_max && jk0_col->min_int >= dense_int_min &&
                  static_cast<uint64_t>(jk0_col->max_int) -
                          static_cast<uint64_t>(dense_int_min) <
                      dense_int_idxs.size();
              if (!full_blk_join) {
                const uint8_t* const jk_null_ptr =
                    jk_has_null ? jk0_col->Nulls().data() : nullptr;
                const auto dmin_u = static_cast<uint64_t>(dense_int_min);
                const size_t dlen = dense_int_idxs.size();
                const uint32_t* const didxs = dense_int_idxs.data();
                uint32_t keep = 0;
                if (use_sel) {
                  for (uint32_t i = 0; i < sel_cnt; ++i) {
                    const uint32_t r = sel_buf[i];
                    if (jk_null_ptr != nullptr && jk_null_ptr[r] != 0) {
                      continue;
                    }
                    const uint64_t idx =
                        static_cast<uint64_t>(jk0_ints[r]) - dmin_u;
                    if (idx < dlen && didxs[idx] != kInvalidRightIdx) {
                      sel_buf[keep++] = r;
                    }
                  }
                } else {
                  if (sel_buf.size() < blk.row_count) {
                    sel_buf.resize(blk.row_count);
                  }
                  for (uint32_t r = 0; r < blk.row_count; ++r) {
                    if (jk_null_ptr != nullptr && jk_null_ptr[r] != 0) {
                      continue;
                    }
                    const uint64_t idx =
                        static_cast<uint64_t>(jk0_ints[r]) - dmin_u;
                    if (idx < dlen && didxs[idx] != kInvalidRightIdx) {
                      sel_buf[keep++] = r;
                    }
                  }
                  use_sel = true;
                }
                sel_cnt = keep;
                if (sel_cnt == 0) {
                  continue;
                }
              }
            }
            if (packed_buf.size() < sel_cnt) {
              packed_buf.resize(sel_cnt);
            }
            if (fb_rows.size() < sel_cnt) {
              fb_rows.resize(sel_cnt);
            }
            std::fill_n(blk_hist.data(), kParts, 0U);
            const uint8_t* const n0_ptr =
                (gk0_has_null && !gk0_col->Nulls().empty())
                    ? gk0_col->Nulls().data()
                    : nullptr;
            const uint8_t* const n1_ptr =
                (gk1_has_null && !gk1_col->Nulls().empty())
                    ? gk1_col->Nulls().data()
                    : nullptr;
            const size_t kern_idx = (n0_ptr != nullptr ? 4U : 0U) |
                                    (n1_ptr != nullptr ? 2U : 0U) |
                                    (use_sel ? 1U : 0U);
            uint32_t packed_cnt = 0;
            uint32_t fb_cnt = 0;
            jit_pack_kernels[kern_idx]->Run(
                sel_cnt, use_sel ? sel_buf.data() : nullptr, gk0_ints, n0_ptr,
                gk1_ints, n1_ptr, packed_buf.data(), blk_hist.data(),
                fb_rows.data(), &packed_cnt, &fb_cnt);
            for (uint32_t j = 0; j < fb_cnt; ++j) {
              const uint32_t r = fb_rows[j];
              const bool n0 = gk0_has_null && gk0_col->IsNull(r);
              const bool n1 = gk1_has_null && gk1_col->IsNull(r);
              record_entry(n0 ? 0 : gk0_ints[r], n0, !n1 ? gk1_ints[r] : 0, n1);
            }
            if (packed_cnt > 0) {
              auto& wpp = worker_packed_parts[w];
              for (size_t p = 0; p < kParts; ++p) {
                const uint32_t add_n = blk_hist[p];
                if (add_n != 0) {
                  const size_t old_sz = wpp[p].size();
                  wpp[p].resize(old_sz + add_n);
                  dst_ptrs[p] = wpp[p].data() + old_sz;
                } else {
                  dst_ptrs[p] = nullptr;
                }
                dst_pos[p] = 0;
              }
              jit_scatter_kernel->Run(packed_cnt, packed_buf.data(),
                                      dst_ptrs.data(), dst_pos.data());
            }
            continue;
          }
          for (uint32_t r = 0; r < blk.row_count; ++r) {
            if (!deferred_left_probe->null_peeks.empty() &&
                std::ranges::any_of(deferred_left_probe->null_peeks,
                                    [&](const NullPeekCompare& test) {
                                      return blk.columns[test.column].IsNull(
                                                 r) != test.expect_null;
                                    })) {
              continue;
            }
            if (rf_col != nullptr) {
              if (rf_col_has_null && rf_col->IsNull(r)) [[unlikely]] {
                continue;
              }
              const int64_t kv = rf_col->int_cells[r];
              if (rf_has_dense) {
                bool accepted = false;
                if (kv >= rf_lo && kv <= rf_hi) {
                  const uint64_t off =
                      static_cast<uint64_t>(kv) - static_cast<uint64_t>(rf_lo);
                  accepted = (rf_dense_bits[off >> 6] &
                              (uint64_t{1} << (off & 63))) != 0;
                }
                if (!accepted) {
                  if (blk_rf_monotonic) {
                    if (kv > rf_hi) {
                      break;
                    }
                    int64_t next_valid_key = rf_lo;
                    if (kv >= rf_lo) {
                      const uint64_t next_off = static_cast<uint64_t>(kv) -
                                                static_cast<uint64_t>(rf_lo) +
                                                1ULL;
                      auto w_idx = static_cast<size_t>(next_off >> 6);
                      if (w_idx >= rf_dense_words) {
                        break;
                      }
                      uint64_t word =
                          rf_dense_bits[w_idx] & (~0ULL << (next_off & 63));
                      while (word == 0) {
                        if (++w_idx >= rf_dense_words) {
                          break;
                        }
                        word = rf_dense_bits[w_idx];
                      }
                      if (w_idx >= rf_dense_words) {
                        break;
                      }
                      const uint64_t found_off =
                          (static_cast<uint64_t>(w_idx) << 6) +
                          static_cast<uint64_t>(std::countr_zero(word));
                      next_valid_key = rf_lo + static_cast<int64_t>(found_off);
                      if (next_valid_key > rf_hi) {
                        break;
                      }
                    }
                    if (rf_col->has_min_max &&
                        next_valid_key > rf_col->max_int) {
                      break;
                    }
                    const uint32_t row_count = blk.row_count;
                    const int64_t* const cells = rf_col->int_cells.data();
                    if (r + 1 < row_count && cells[r + 1] < next_valid_key) {
                      uint32_t lo = r + 1;
                      uint32_t step = 4;
                      while (lo + step < row_count &&
                             cells[lo + step] < next_valid_key) {
                        lo += step;
                        step <<= 1;
                      }
                      const uint32_t hi = std::min(row_count, lo + step + 1);
                      const int64_t* found = std::lower_bound(
                          cells + lo, cells + hi, next_valid_key);
                      r = static_cast<uint32_t>(found - cells) - 1;
                    }
                  }
                  continue;
                }
              } else if (active_rf_group->Reject(kv)) {
                continue;
              }
            }
            if (extra_rf_count != 0) {
              bool extra_ok = true;
              for (size_t ei = 0; ei < extra_rf_count; ++ei) {
                const DefExtraRf& erf = extra_rfs[ei];
                if (erf.has_null && erf.col->IsNull(r)) [[unlikely]] {
                  extra_ok = false;
                  break;
                }
                const int64_t kv = erf.col->int_cells[r];
                if (erf.dense_bits != nullptr) {
                  if (kv < erf.lo || kv > erf.hi) {
                    extra_ok = false;
                    break;
                  }
                  const uint64_t off =
                      static_cast<uint64_t>(kv) - static_cast<uint64_t>(erf.lo);
                  if ((erf.dense_bits[off >> 6] &
                       (uint64_t{1} << (off & 63))) == 0) {
                    extra_ok = false;
                    break;
                  }
                } else if (erf.group->Reject(kv)) {
                  extra_ok = false;
                  break;
                }
              }
              if (!extra_ok) {
                continue;
              }
            }
            if (!peek_all_pass && peek_ptr != nullptr) {
              bool peek_ok = true;
              for (const IntegerPeekCompare& pred : *peek_ptr) {
                const LSTableColumnBlock& pcol = blk.columns[pred.column];
                if ((pcol.has_null && pcol.IsNull(r)) ||
                    !eval_int_peek(pred.op, pcol.int_cells[r], pred.constant)) {
                  peek_ok = false;
                  break;
                }
              }
              if (!peek_ok) {
                continue;
              }
            }
            if (!singleton_probe) {
              if (jk_has_null && jk0_col->IsNull(r)) {
                continue;
              }
              const int64_t jkey = AddJoinKeyBias(jk0_ints[r], off0);
              if (!match_right_key(jkey)) {
                continue;
              }
            }
            const bool n0 = gk0_has_null && gk0_col->IsNull(r);
            const bool n1 = gk1_has_null && gk1_col->IsNull(r);
            record_entry(n0 ? 0 : gk0_ints[r], n0,
                         (n_keys == 2 && !n1) ? gk1_ints[r] : 0, n1);
          }
        }
        return;
      }
      if (left.HasColumnarOnly()) {
        const auto& chunks = left.columnar->chunks;
        const size_t n_chunks = chunks.size();
        while (true) {
          const size_t ci =
              dist_next_chunk.fetch_add(1, std::memory_order_relaxed);
          if (ci >= n_chunks) {
            break;
          }
          const ColumnarChunk& ch = chunks[ci];
          const ColumnarColumn* jk0_col =
              singleton_probe ? nullptr : &ch.columns[jk0];
          const ColumnarColumn& gk0_col = ch.columns[gk_slots[0]];
          const ColumnarColumn* gk1_col =
              n_keys == 2 ? &ch.columns[gk_slots[1]] : nullptr;
          const bool fast_types =
              (jk0_col == nullptr ||
               (jk0_col->is_uniform && !jk0_col->is_unsigned &&
                (jk0_col->type == ValueType::kInt64 ||
                 jk0_col->type == ValueType::kDate))) &&
              gk0_col.is_uniform && !gk0_col.is_unsigned &&
              (gk0_col.type == ValueType::kInt64 ||
               gk0_col.type == ValueType::kDate) &&
              (gk1_col == nullptr ||
               (gk1_col->is_uniform && !gk1_col->is_unsigned &&
                (gk1_col->type == ValueType::kInt64 ||
                 gk1_col->type == ValueType::kDate)));
          if (fast_types) {
            for (size_t r = 0; r < ch.rows; ++r) {
              if (!singleton_probe) {
                if (jk0_col->IsNull(r)) {
                  continue;
                }
                const int64_t jkey = AddJoinKeyBias(jk0_col->IntAt(r), off0);
                if (!match_right_key(jkey)) {
                  continue;
                }
              }
              const bool n0 = gk0_col.IsNull(r);
              const bool n1 = gk1_col != nullptr && gk1_col->IsNull(r);
              record_entry(n0 ? 0 : gk0_col.IntAt(r), n0,
                           (n_keys == 2 && gk1_col != nullptr && !n1)
                               ? gk1_col->IntAt(r)
                               : 0,
                           n1);
            }
          } else {
            Row tmp_row;
            for (size_t r = 0; r < ch.rows; ++r) {
              ch.MaterializeRowInto(r, &tmp_row);
              if (!singleton_probe) {
                const Value& jcell = tmp_row.values_[jk0];
                if (jcell.IsNull() || jcell.IsUnsigned()) {
                  continue;
                }
                const int64_t jkey =
                    AddJoinKeyBias(jcell.value.int_value, off0);
                if (!match_right_key(jkey)) {
                  continue;
                }
              }
              const Value& c0 = tmp_row.values_[gk_slots[0]];
              const bool n0 = c0.IsNull();
              const Value* c1 =
                  n_keys == 2 ? &tmp_row.values_[gk_slots[1]] : nullptr;
              const bool n1 = c1 != nullptr && c1->IsNull();
              record_entry(n0 ? 0 : c0.value.int_value, n0,
                           (c1 != nullptr && !n1) ? c1->value.int_value : 0,
                           n1);
            }
          }
        }
        return;
      }
      while (true) {
        const size_t chunk =
            dist_next_chunk.fetch_add(1, std::memory_order_relaxed);
        if (chunk >= chunk_count) {
          break;
        }
        const size_t begin = chunk * chunk_rows;
        const size_t end = std::min(total_left, begin + chunk_rows);
        for (size_t ri = begin; ri < end; ++ri) {
          const Row& lrow = left.rows[ri];
          if (!singleton_probe) {
            const Value& jcell = lrow.values_[jk0];
            if (jcell.IsNull() || jcell.IsUnsigned()) {
              continue;
            }
            const int64_t jkey = AddJoinKeyBias(jcell.value.int_value, off0);
            if (!match_right_key(jkey)) {
              continue;
            }
          }
          const Value& c0 = lrow.values_[gk_slots[0]];
          const bool n0 = c0.IsNull();
          const Value* c1 = n_keys == 2 ? &lrow.values_[gk_slots[1]] : nullptr;
          const bool n1 = c1 != nullptr && c1->IsNull();
          record_entry(n0 ? 0 : c0.value.int_value, n0,
                       (c1 != nullptr && !n1) ? c1->value.int_value : 0, n1);
        }
      }
    };
    if (scan_workers == 1) {
      run_dist_scan(0);
    } else {
      RETURN_IF_FAIL(RunParallelTasks(scan_workers, run_dist_scan));
    }
    std::vector<std::vector<DistinctIntEntry>> unique_parts(kParts);
    std::vector<std::vector<int64_t>> unique_k0(kParts);
    std::vector<std::vector<int64_t>> unique_k1(kParts);
    std::atomic<size_t> next_dedup_p{0};
    const auto run_dedup = [&](size_t) {
      std::vector<DistinctIntEntry> ht_entries;
      std::vector<uint8_t> ht_used;
      std::vector<uint64_t> p_ht_keys;
      std::vector<uint32_t> p_ht_gens;
      uint32_t p_cur_gen = 0;
      while (true) {
        const size_t p = next_dedup_p.fetch_add(1, std::memory_order_relaxed);
        if (p >= kParts) {
          break;
        }
        if (use_jit_packed_dedup) {
          size_t packed_n = 0;
          for (size_t w = 0; w < scan_workers; ++w) {
            packed_n += worker_packed_parts[w][p].size();
          }
          if (packed_n > 0) {
            const size_t est =
                std::min<size_t>(std::max<size_t>(packed_n, 32), 262144);
            const size_t init_cap =
                std::max<size_t>(64, std::bit_ceil(est * 2));
            if (p_ht_keys.size() < init_cap) {
              p_ht_keys.assign(init_cap, 0);
              p_ht_gens.assign(init_cap, 0);
              p_cur_gen = 1;
            } else {
              ++p_cur_gen;
              if (p_cur_gen == 0) {
                std::fill(p_ht_gens.begin(), p_ht_gens.end(), 0);
                p_cur_gen = 1;
              }
            }
            auto max_load = static_cast<uint32_t>((p_ht_keys.size() * 7) / 10);
            uint32_t n_occ = 0;
            auto& uk0 = unique_k0[p];
            auto& uk1 = unique_k1[p];
            uk0.resize(max_load);
            uk1.resize(max_load);
            for (size_t w = 0; w < scan_workers; ++w) {
              auto& bucket = worker_packed_parts[w][p];
              size_t off = 0;
              const size_t bsz = bucket.size();
              while (off < bsz) {
                const auto chunk = static_cast<uint32_t>(std::min<size_t>(
                    bsz - off, std::numeric_limits<uint32_t>::max()));
                const uint32_t done = jit_dedup_kernel->Run(
                    chunk, bucket.data() + off, p_ht_keys.data(),
                    p_ht_gens.data(), p_cur_gen,
                    static_cast<uint64_t>(p_ht_keys.size() - 1), max_load,
                    &n_occ, uk0.data(), uk1.data());
                off += done;
                if (off < bsz) {
                  const size_t new_cap = p_ht_keys.size() * 2;
                  p_ht_keys.assign(new_cap, 0);
                  p_ht_gens.assign(new_cap, 0);
                  p_cur_gen = 1;
                  const uint64_t nmask = static_cast<uint64_t>(new_cap - 1);
                  for (uint32_t j = 0; j < n_occ; ++j) {
                    const uint64_t pk =
                        (static_cast<uint64_t>(static_cast<uint32_t>(
                             static_cast<int32_t>(uk0[j])))
                         << 32) |
                        static_cast<uint64_t>(static_cast<uint32_t>(
                            static_cast<int32_t>(uk1[j])));
                    auto slot =
                        static_cast<size_t>(HashPacked2x32Key(pk) & nmask);
                    while (p_ht_gens[slot] == p_cur_gen) {
                      slot = (slot + 1) & static_cast<size_t>(nmask);
                    }
                    p_ht_keys[slot] = pk;
                    p_ht_gens[slot] = p_cur_gen;
                  }
                  max_load = static_cast<uint32_t>((new_cap * 7) / 10);
                  uk0.resize(max_load);
                  uk1.resize(max_load);
                }
              }
              std::vector<uint64_t>{}.swap(bucket);
            }
            uk0.resize(n_occ);
            uk1.resize(n_occ);
          }
        }
        size_t raw_n = 0;
        for (size_t w = 0; w < scan_workers; ++w) {
          raw_n += worker_parts[w][p].size();
        }
        if (raw_n == 0) {
          continue;
        }
        size_t cap = 16;
        const size_t target = raw_n * 2;
        while (cap < target) {
          cap <<= 1;
        }
        ht_entries.resize(cap);
        ht_used.assign(cap, 0);
        const size_t mask = cap - 1;
        auto& out = unique_parts[p];
        out.reserve(raw_n);
        for (size_t w = 0; w < scan_workers; ++w) {
          for (const DistinctIntEntry& e : worker_parts[w][p]) {
            size_t slot = static_cast<size_t>(hash_entry(e)) & mask;
            while (ht_used[slot] != 0) {
              if (ht_entries[slot] == e) {
                break;
              }
              slot = (slot + 1) & mask;
            }
            if (ht_used[slot] == 0) {
              ht_used[slot] = 1;
              ht_entries[slot] = e;
              out.push_back(e);
            }
          }
          std::vector<DistinctIntEntry>{}.swap(worker_parts[w][p]);
        }
      }
    };
    if (scan_workers == 1) {
      run_dedup(0);
    } else {
      RETURN_IF_FAIL(RunParallelTasks(scan_workers, run_dedup));
    }
    std::vector<std::vector<std::vector<uint64_t>>>().swap(worker_packed_parts);
    std::vector<size_t> part_offsets(kParts + 1, 0);
    for (size_t p = 0; p < kParts; ++p) {
      part_offsets[p + 1] =
          part_offsets[p] + unique_k0[p].size() + unique_parts[p].size();
    }
    const size_t total_unique = part_offsets[kParts];
    std::vector<Column> dist_output_columns;
    dist_output_columns.reserve(fusion->select_names.size());
    for (const std::string& name : fusion->select_names) {
      dist_output_columns.emplace_back(name, ValueType::kNull);
    }
    const auto emit_cell = [&](const DistinctIntEntry& e,
                               const DistinctDirectItem& d) -> Value {
      if (d.kind == DistinctDirectItem::Kind::kConst) {
        return d.constant;
      }
      if (d.kind == DistinctDirectItem::Kind::kKey0) {
        if ((e.null_mask & 1) != 0) {
          return {};
        }
        return gk_types[0] == ValueType::kDate ? Value::DateFromDays(e.k0)
                                               : Value(e.k0);
      }
      if ((e.null_mask & 2) != 0) {
        return {};
      }
      return gk_types[1] == ValueType::kDate ? Value::DateFromDays(e.k1)
                                             : Value(e.k1);
    };
    const auto emit_packed_cell = [&](int64_t k0v, int64_t k1v,
                                      const DistinctDirectItem& d) -> Value {
      if (d.kind == DistinctDirectItem::Kind::kConst) {
        return d.constant;
      }
      if (d.kind == DistinctDirectItem::Kind::kKey0) {
        return gk_types[0] == ValueType::kDate ? Value::DateFromDays(k0v)
                                               : Value(k0v);
      }
      return gk_types[1] == ValueType::kDate ? Value::DateFromDays(k1v)
                                             : Value(k1v);
    };
    for (size_t c = 0; c < dist_items.size(); ++c) {
      if (dist_items[c].kind == DistinctDirectItem::Kind::kConst) {
        const Value& v = dist_items[c].constant;
        dist_output_columns[c] =
            Column(dist_output_columns[c].Name(), ValueTypeOf(v));
        dist_output_columns[c].SetUnsigned(v.IsUnsigned());
        dist_output_columns[c].SetInterval(v.IsInterval());
      } else {
        const size_t key_slot =
            dist_items[c].kind == DistinctDirectItem::Kind::kKey0 ? 0 : 1;
        dist_output_columns[c] =
            Column(dist_output_columns[c].Name(), gk_types[key_slot]);
      }
    }
    bool pushed_distinct = false;
    if (fusion->output_sink != nullptr) {
      const size_t emit_workers = ParallelWorkerCount(total_unique / 8192);
      auto batch_charge =
          TryReserveQueryMemory(emit_workers * GroupedOutputSink::kBatchRows *
                                dist_items.size() * sizeof(Value));
      bool accepted = false;
      if (batch_charge) {
        Status begin_status = Status::kSuccess;
        RETURN_IF_FAIL(RunParallelTasks(1, [&](size_t) {
          auto begun = fusion->output_sink->begin(
              Schema("", dist_output_columns), total_unique, emit_workers);
          begin_status = begun.GetStatus();
          if (begun.HasValue()) accepted = begun.Value();
        }));
        RETURN_IF_FAIL(begin_status);
      }
      if (accepted) {
        fusion->output_sink->used = true;
        pushed_distinct = true;
        std::atomic<size_t> next_emit_p{0};
        std::vector<Status> emit_errors(emit_workers, Status::kSuccess);
        const size_t width = dist_items.size();
        const auto run_sink_emit = [&](size_t worker) {
          std::vector<Value> batch(GroupedOutputSink::kBatchRows * width);
          while (true) {
            const size_t p =
                next_emit_p.fetch_add(1, std::memory_order_relaxed);
            if (p >= kParts) {
              break;
            }
            const auto& uk0 = unique_k0[p];
            const auto& uk1 = unique_k1[p];
            const size_t base_first = part_offsets[p];
            for (size_t off = 0; off < uk0.size();
                 off += GroupedOutputSink::kBatchRows) {
              const size_t count =
                  std::min(GroupedOutputSink::kBatchRows, uk0.size() - off);
              for (size_t r = 0; r < count; ++r) {
                const int64_t k0v = uk0[off + r];
                const int64_t k1v = uk1[off + r];
                for (size_t c = 0; c < width; ++c) {
                  batch[(r * width) + c] =
                      emit_packed_cell(k0v, k1v, dist_items[c]);
                }
              }
              const Status st = fusion->output_sink->push(
                  worker, base_first + off,
                  std::span<const Value>(batch.data(), count * width));
              if (!st.ok()) {
                emit_errors[worker] = st;
                return;
              }
            }
            const auto& uvec = unique_parts[p];
            const size_t fb_base_first = base_first + uk0.size();
            for (size_t off = 0; off < uvec.size();
                 off += GroupedOutputSink::kBatchRows) {
              const size_t count =
                  std::min(GroupedOutputSink::kBatchRows, uvec.size() - off);
              for (size_t r = 0; r < count; ++r) {
                const DistinctIntEntry& e = uvec[off + r];
                for (size_t c = 0; c < width; ++c) {
                  batch[(r * width) + c] = emit_cell(e, dist_items[c]);
                }
              }
              const Status st = fusion->output_sink->push(
                  worker, fb_base_first + off,
                  std::span<const Value>(batch.data(), count * width));
              if (!st.ok()) {
                emit_errors[worker] = st;
                return;
              }
            }
          }
        };
        if (emit_workers == 1) {
          run_sink_emit(0);
        } else {
          RETURN_IF_FAIL(RunParallelTasks(emit_workers, run_sink_emit));
        }
        for (const Status& st : emit_errors) RETURN_IF_FAIL(st);
        if (auto* runtime = context.execution_runtime()) {
          runtime->pushed_group_rows += total_unique;
        }
      }
    }
    std::shared_ptr<ColumnarTable> out_distinct_columnar;
    bool distinct_uniform = false;
    if (!pushed_distinct && fusion->columnar_output) {
      out_distinct_columnar = std::make_shared<ColumnarTable>();
      if (total_unique > 0) {
        const size_t emit_workers = ParallelWorkerCount(total_unique / 8192);
        std::atomic<size_t> next_emit_p{0};
        const size_t width = dist_items.size();
        std::vector<std::vector<ColumnarChunk>> part_chunks(kParts);
        const auto run_col = [&](size_t) {
          while (true) {
            const size_t p =
                next_emit_p.fetch_add(1, std::memory_order_relaxed);
            if (p >= kParts) {
              break;
            }
            const auto& uk0 = unique_k0[p];
            const auto& uk1 = unique_k1[p];
            const auto& uvec = unique_parts[p];
            if (uk0.empty() && uvec.empty()) {
              continue;
            }
            auto& pchunks = part_chunks[p];
            pchunks.reserve(
                ((uk0.size() + kColumnarChunkRows - 1) / kColumnarChunkRows) +
                ((uvec.size() + kColumnarChunkRows - 1) / kColumnarChunkRows));
            for (size_t lo = 0; lo < uk0.size(); lo += kColumnarChunkRows) {
              const size_t count =
                  std::min(kColumnarChunkRows, uk0.size() - lo);
              ColumnarChunk chunk;
              chunk.rows = count;
              chunk.columns.resize(width);
              ChunkStringArena* arena = nullptr;
              for (size_t c = 0; c < width; ++c) {
                const Column& proto = dist_output_columns[c];
                if (dist_items[c].kind == DistinctDirectItem::Kind::kConst) {
                  if (TrySetConstantColumnarColumn(
                          &chunk.columns[c], proto.Type(), proto.IsUnsigned(),
                          proto.IsInterval(), dist_items[c].constant, count,
                          &chunk)) {
                    arena = chunk.string_arena.get();
                    continue;
                  }
                  chunk.columns[c].Init(proto.Type(), proto.IsUnsigned(),
                                        proto.IsInterval(), count);
                  for (size_t r = 0; r < count; ++r) {
                    if (dist_items[c].constant.type == ValueType::kVarChar &&
                        arena == nullptr) {
                      arena = chunk.EnsureArena();
                    }
                    chunk.columns[c].AppendValue(dist_items[c].constant, arena,
                                                 r);
                  }
                  continue;
                }
                const size_t key_slot =
                    dist_items[c].kind == DistinctDirectItem::Kind::kKey0 ? 0
                                                                          : 1;
                ColumnarColumn& col = chunk.columns[c];
                col.Init(gk_types[key_slot], false, false, 0);
                col.ints.resize(count);
                const int64_t* src =
                    key_slot == 0 ? (uk0.data() + lo) : (uk1.data() + lo);
                std::memcpy(col.ints.data(), src, count * sizeof(int64_t));
              }
              pchunks.push_back(std::move(chunk));
            }
            for (size_t lo = 0; lo < uvec.size(); lo += kColumnarChunkRows) {
              const size_t count =
                  std::min(kColumnarChunkRows, uvec.size() - lo);
              ColumnarChunk chunk;
              chunk.rows = count;
              chunk.columns.resize(width);
              ChunkStringArena* arena = nullptr;
              for (size_t c = 0; c < width; ++c) {
                const Column& proto = dist_output_columns[c];
                if (dist_items[c].kind == DistinctDirectItem::Kind::kConst &&
                    TrySetConstantColumnarColumn(
                        &chunk.columns[c], proto.Type(), proto.IsUnsigned(),
                        proto.IsInterval(), dist_items[c].constant, count,
                        &chunk)) {
                  arena = chunk.string_arena.get();
                  continue;
                }
                chunk.columns[c].Init(proto.Type(), proto.IsUnsigned(),
                                      proto.IsInterval(), count);
                for (size_t r = 0; r < count; ++r) {
                  Value cell = emit_cell(uvec[lo + r], dist_items[c]);
                  if (cell.type == ValueType::kVarChar && arena == nullptr) {
                    arena = chunk.EnsureArena();
                  }
                  chunk.columns[c].AppendValue(cell, arena, r);
                }
              }
              pchunks.push_back(std::move(chunk));
            }
          }
        };
        if (emit_workers == 1) {
          run_col(0);
        } else {
          RETURN_IF_FAIL(RunParallelTasks(emit_workers, run_col));
        }
        distinct_uniform = true;
        for (auto& pchunks : part_chunks) {
          for (ColumnarChunk& chunk : pchunks) {
            for (const ColumnarColumn& col : chunk.columns) {
              if (!col.is_uniform) {
                distinct_uniform = false;
              }
            }
            out_distinct_columnar->AppendChunk(std::move(chunk));
          }
        }
      }
    } else if (!pushed_distinct) {
      result.rows.resize(total_unique);
      const size_t emit_workers = ParallelWorkerCount(total_unique / 8192);
      std::atomic<size_t> next_emit_p{0};
      const size_t width = dist_items.size();
      const auto run_mat = [&](size_t) {
        while (true) {
          const size_t p = next_emit_p.fetch_add(1, std::memory_order_relaxed);
          if (p >= kParts) {
            break;
          }
          const auto& uk0 = unique_k0[p];
          const auto& uk1 = unique_k1[p];
          const size_t base = part_offsets[p];
          for (size_t r = 0; r < uk0.size(); ++r) {
            result.rows[base + r].values_.reserve(width);
            for (size_t c = 0; c < width; ++c) {
              result.rows[base + r].values_.push_back(
                  emit_packed_cell(uk0[r], uk1[r], dist_items[c]));
            }
          }
          const auto& uvec = unique_parts[p];
          const size_t fb_base = base + uk0.size();
          for (size_t r = 0; r < uvec.size(); ++r) {
            result.rows[fb_base + r].values_.reserve(width);
            for (size_t c = 0; c < width; ++c) {
              result.rows[fb_base + r].values_.push_back(
                  emit_cell(uvec[r], dist_items[c]));
            }
          }
        }
      };
      if (emit_workers == 1) {
        run_mat(0);
      } else {
        RETURN_IF_FAIL(RunParallelTasks(emit_workers, run_mat));
      }
    }
    result.hash_joins = left.hash_joins + right.hash_joins + 1;
    result.in_memory_hash_joins =
        left.in_memory_hash_joins + right.in_memory_hash_joins + 1;
    result.nested_loop_joins = left.nested_loop_joins + right.nested_loop_joins;
    result.schema = Schema("", std::move(dist_output_columns));
    if (out_distinct_columnar != nullptr) {
      const bool non_empty = out_distinct_columnar->total_rows > 0;
      result.SetColumnarTable(std::move(out_distinct_columnar),
                              distinct_uniform && non_empty);
    }
    result.peak_intermediate_rows =
        std::max(result.peak_intermediate_rows, result.TotalRows());
    if (auto* runtime = context.execution_runtime()) {
      runtime->join_ms += ElapsedMs(join_begin);
      ++runtime->grouped_join_fusions;
    }
    return DistinctLSTableProbeOutcome{.handled = true,
                                       .result = std::move(result)};
  }

  return DistinctLSTableProbeOutcome{.handled = false, .result = std::nullopt};
}

}  // namespace tinylamb::relational_detail
