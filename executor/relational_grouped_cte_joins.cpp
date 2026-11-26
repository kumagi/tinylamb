/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include <algorithm>
#include <array>
#include <atomic>
#include <cmath>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <iostream>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/parallel_work.hpp"
#include "common/platform/int128_conversion.hpp"
#include "common/platform/process.hpp"
#include "common/query_parallelism.hpp"
#include "common/runtime_filter.hpp"
#include "common/status_or.hpp"
#include "common/string_util.hpp"
#include "database/transaction_context.hpp"
#include "executor/aggregation.hpp"
#include "executor/detail/columnar_tlcs_fast_path.hpp"
#include "executor/detail/explain_format.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/detail/grouped_numeric_hash.hpp"
#include "executor/detail/planning_heuristics.hpp"
#include "executor/detail/relation.hpp"
#include "executor/detail/relational_fast_paths.hpp"
#include "executor/detail/relational_projection.hpp"
#include "executor/detail/relational_sort_limit.hpp"
#include "executor/detail/scan_filter.hpp"
#include "executor/detail/scan_tuple_filter.hpp"
#include "executor/detail/subquery_runtime.hpp"
#include "executor/detail/typed_stream_aggregate.hpp"
#include "executor/detail/window_eval.hpp"
#include "executor/relational.hpp"
#include "executor/table_sample.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/binary_expression.hpp"
#include "expression/bytecode.hpp"
#include "expression/case_expression.hpp"
#include "expression/cast_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/function_call_expression.hpp"
#include "expression/named_expression.hpp"
#include "expression/parameter_slot.hpp"
#include "expression/query_expression.hpp"
#include "expression/rewrite.hpp"
#include "expression/unary_expression.hpp"
#include "plan/plan.hpp"
#include "table/full_scan_iterator.hpp"
#include "table/iterator.hpp"
#include "table/lstable_bridge.hpp"
#include "table/table.hpp"
#include "table/table_statistics.hpp"
#include "table/tlcs_file.hpp"
#include "type/column.hpp"
#include "type/type.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb::relational_detail {
namespace {
// Tolerate the numeric/NULL cells accepted below. In particular, an
// independent rejecting conjunct must not hide overflow/division/cast errors
// elsewhere in the WHERE expression when skipping all-unmatched tuples.
bool NumericPushPredicate(const Expression& expression) {
  if (!expression) {
    return true;
  }
  switch (expression->Type()) {
    case TypeTag::kColumnValue:
      return true;
    case TypeTag::kConstantValue: {
      const auto& v = expression->AsConstantValue().GetValue();
      return v.IsNull() || v.type == ValueType::kInt64 ||
             v.type == ValueType::kDouble;
    }
    case TypeTag::kBinaryExp: {
      const auto op = expression->AsBinaryExpression().Op();
      if (op != BinaryOperation::kAnd && op != BinaryOperation::kOr &&
          op != BinaryOperation::kEquals && op != BinaryOperation::kNotEquals &&
          op != BinaryOperation::kLessThan &&
          op != BinaryOperation::kLessThanEquals &&
          op != BinaryOperation::kGreaterThan &&
          op != BinaryOperation::kGreaterThanEquals) {
        return false;
      }
      break;
    }
    case TypeTag::kUnaryExp: {
      const auto op = expression->AsUnaryExpression().Op();
      if (op != UnaryOperation::kIsNull && op != UnaryOperation::kIsNotNull &&
          op != UnaryOperation::kNot) {
        return false;
      }
      break;
    }
    case TypeTag::kFunctionCallExp:
      if (!IdentifierEquals(expression->AsFunctionCallExpression().FuncName(),
                            "COALESCE") ||
          ExpressionChildren(expression).empty()) {
        return false;
      }
      break;
    default:
      return false;
  }
  return std::ranges::all_of(ExpressionChildren(expression),
                             NumericPushPredicate);
}

// Transfer a necessary right-side match domain through GROUP BY only when it
// depends on preserved base group keys and every skipped computation is total.
// Catalog estimates are never used as correctness bounds.
struct GroupedTransferProof {
  std::string source;
  std::shared_ptr<const LSTableStore> store;
  std::unordered_map<size_t, slot_t> output_columns;
};

std::optional<GroupedTransferProof> ProveGroupedTransfer(
    TransactionContext& context, const SelectStatement& body) {
  if (::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_PREDICATE_TRANSFER") !=
          nullptr ||
      body.Sources().empty() || body.Sources().size() > 8 ||
      !context.txn_.WriteSetEmpty()) {
    return std::nullopt;
  }
  const auto spec = BuildGroupedJoinFusionSpec(body);
  if (!spec.eligible) {
    return std::nullopt;
  }
  std::vector<std::shared_ptr<const LSTableStore>> stores;
  std::vector<Schema> schemas;
  std::vector<std::pair<size_t, slot_t>> locations;
  Schema joined;
  std::unordered_set<std::string> aliases;
  uint64_t row_bound = 1;
  constexpr uint64_t kMaxCount = std::numeric_limits<int64_t>::max();
  std::vector<std::shared_ptr<Table>> tables;
  tables.reserve(body.Sources().size());
  for (size_t i = 0; i < body.Sources().size(); ++i) {
    const auto& source = body.Sources()[i];
    if (source.query || source.unnest || source.sample || source.is_lateral ||
        source.from_nested_join || !source.using_columns.empty() ||
        (source.join_type != JoinType::kInner &&
         source.join_type != JoinType::kCross &&
         source.join_type != JoinType::kLeft) ||
        !aliases.insert(TableKeyFilterKey(source)).second) {
      return std::nullopt;
    }
    auto table = context.GetTable(source.table);
    if (!table.HasValue()) {
      return std::nullopt;
    }
    Schema schema =
        QualifySchema(table.Value()->GetSchema(), TableKeyFilterKey(source));
    for (size_t c = 0; c < schema.ColumnCount(); ++c) {
      locations.emplace_back(i, static_cast<slot_t>(c));
    }
    joined = i == 0 ? schema : joined + schema;
    schemas.push_back(std::move(schema));
    tables.push_back(std::move(table.Value()));
  }
  std::vector<std::vector<slot_t>> needed_cols(body.Sources().size());
  const auto add_needed = [&](const ColumnName& name) {
    const auto off = LocalColumnOffset(joined, name);
    if (!off || *off >= locations.size()) {
      return;
    }
    const auto [src, col] = locations[*off];
    if (std::ranges::find(needed_cols[src], col) == needed_cols[src].end()) {
      needed_cols[src].push_back(col);
    }
  };
  const auto add_expr_cols = [&](const Expression& expr) {
    if (!expr) {
      return;
    }
    for (const ColumnName& name : expr->TouchedColumns()) {
      add_needed(name);
    }
  };
  add_expr_cols(body.WhereClause());
  for (const auto& source : body.Sources()) {
    add_expr_cols(source.join_condition);
  }
  for (const auto& key : body.GroupBy()) {
    if (key && key->Type() == TypeTag::kColumnValue) {
      add_needed(key->AsColumnValue().GetColumnName());
    }
  }
  for (size_t a = 0; a < spec.aggregates.size(); ++a) {
    if (!spec.count_star[a] && spec.aggregates[a]->Child() &&
        spec.aggregates[a]->Child()->Type() == TypeTag::kColumnValue) {
      add_needed(spec.aggregates[a]->Child()->AsColumnValue().GetColumnName());
    }
  }
  for (const auto& item : spec.select_items) {
    if (item && item->Type() == TypeTag::kColumnValue) {
      add_needed(item->AsColumnValue().GetColumnName());
    }
  }
  Schema joined_prefix;
  stores.reserve(body.Sources().size());
  for (size_t i = 0; i < body.Sources().size(); ++i) {
    const auto& source = body.Sources()[i];
    const auto& tbl = tables[i];
    const uint64_t gen = tbl->LayoutGeneration();
    const page_id_t first_pid = tbl->FirstPageId();
    auto store = GetOrBuildLSTableStore(
        context.txn_, *tbl, true,
        !needed_cols[i].empty() ? &needed_cols[i] : nullptr,
        /*allow_block_lazy=*/true);
    if (!store || store->uncommitted_owner_txn != 0 ||
        store->layout_generation != gen || store->first_pid != first_pid ||
        context.txn_.HasAnyPageVersionChains()) {
      return std::nullopt;
    }
    std::shared_ptr<const TlcsFile> backing_tlcs;
    const bool clean_txn =
        context.txn_.IsReadOnly() || context.txn_.WriteSetEmpty();
    if (gen != 0 && !tbl->RowsRewrittenInPlace() && clean_txn) {
      if (auto tlcs = tbl->GetBackingTlcsStore();
          tlcs != nullptr && tlcs->LayoutGeneration() == gen &&
          tlcs->FirstPageId() == first_pid && tlcs->HasColumnSynopses()) {
        backing_tlcs = std::move(tlcs);
      }
    }
    const Schema& schema = schemas[i];
    uint64_t multiplier = std::max<uint64_t>(1, store->total_rows);
    if (i != 0) {
      // A complete unique-key equijoin cannot multiply a preserved row.
      auto terms = SplitConjuncts(source.join_condition);
      if (source.join_type != JoinType::kLeft) {
        auto where = SplitConjuncts(body.WhereClause());
        terms.insert(terms.end(), where.begin(), where.end());
      }
      for (const auto& key : EqualityKeys(joined_prefix, schema, terms)) {
        const bool key_unique_nonnull =
            (key.right < store->column_is_unique_nonnull.size() &&
             store->column_is_unique_nonnull[key.right] != 0) ||
            (backing_tlcs != nullptr &&
             key.right < backing_tlcs->ColumnSynopses().size() &&
             backing_tlcs->ColumnSynopses()[key.right].is_unique_nonnull);
        if (!key.null_safe && key.left_offset == 0 && key.right_offset == 0 &&
            joined_prefix.GetColumn(key.left).Type() == ValueType::kInt64 &&
            schema.GetColumn(key.right).Type() == ValueType::kInt64 &&
            !joined_prefix.GetColumn(key.left).IsUnsigned() &&
            !schema.GetColumn(key.right).IsUnsigned() && key_unique_nonnull) {
          multiplier = 1;
          break;
        }
      }
    }
    if (multiplier > kMaxCount / row_bound) {
      return std::nullopt;
    }
    row_bound *= multiplier;
    joined_prefix = i == 0 ? schema : joined_prefix + schema;
    stores.push_back(std::move(store));
  }
  const auto resolve = [&](const ColumnName& name) {
    return LocalColumnOffset(joined, name);
  };
  const auto numeric_column = [&](size_t slot) {
    const auto [source, col] = locations[slot];
    const auto& declared = schemas[source].GetColumn(col);
    if (declared.IsUnsigned() ||
        (declared.Type() != ValueType::kInt64 &&
         declared.Type() != ValueType::kDouble) ||
        !stores[source]->HasColumn(col)) {
      return false;
    }
    return std::ranges::all_of(stores[source]->blocks, [&](const auto& block) {
      return col < block.columns.size() && !block.columns[col].is_unsigned &&
             block.columns[col].type == declared.Type();
    });
  };
  const auto total_predicate = [&](const Expression& predicate) {
    if (!NumericPushPredicate(predicate)) {
      return false;
    }
    if (!predicate) {
      return true;
    }
    for (const auto& name : predicate->TouchedColumns()) {
      auto slot = resolve(name);
      if (!slot || !numeric_column(*slot)) {
        return false;
      }
    }
    return true;
  };
  if (!total_predicate(body.WhereClause())) {
    return std::nullopt;
  }
  for (const auto& source : body.Sources()) {
    if (!total_predicate(source.join_condition)) {
      return std::nullopt;
    }
  }
  std::unordered_set<size_t> group_columns;
  for (const auto& key : body.GroupBy()) {
    if (!key || key->Type() != TypeTag::kColumnValue) {
      return std::nullopt;
    }
    const auto slot = resolve(key->AsColumnValue().GetColumnName());
    if (!slot || !numeric_column(*slot)) {
      return std::nullopt;
    }
    group_columns.insert(*slot);
  }
  for (size_t a = 0; a < spec.aggregates.size(); ++a) {
    const auto& agg = *spec.aggregates[a];
    if (agg.NeedsGroupContext() || agg.HasInnerGroupBy() ||
        agg.HasWhereFilter() || agg.Distinct()) {
      return std::nullopt;
    }
    if (spec.count_star[a]) {
      continue;
    }
    if (!agg.Child() || agg.Child()->Type() != TypeTag::kColumnValue) {
      return std::nullopt;
    }
    const auto slot = resolve(agg.Child()->AsColumnValue().GetColumnName());
    if (!slot || !numeric_column(*slot)) {
      return std::nullopt;
    }
    if (agg.GetType() == AggregationType::kCount ||
        agg.GetType() == AggregationType::kMin ||
        agg.GetType() == AggregationType::kMax) {
      continue;
    }
    if (agg.GetType() != AggregationType::kSum) {
      return std::nullopt;
    }
    const auto [source, col] = locations[*slot];
    const auto magnitude = [](int64_t v) {
      return v < 0 ? uint64_t{0} - static_cast<uint64_t>(v)
                   : static_cast<uint64_t>(v);
    };
    for (size_t bi = 0; bi < stores[source]->blocks.size(); ++bi) {
      const auto& block = stores[source]->blocks[bi];
      const auto& column = block.columns[col];
      auto all_rows_null = [&]() -> bool {
        std::vector<std::unique_ptr<LSTableLazyDecodedBlock>> transient;
        const LSTableColumnBlock* resolved =
            stores[source]->ColumnIsLazy(col)
                ? stores[source]->ResolveBlockColumn(bi, col, &transient)
                : &column;
        if (resolved == nullptr) {
          return false;
        }
        for (size_t r = 0; r < block.row_count; ++r) {
          if (!resolved->IsNull(r)) {
            return false;
          }
        }
        return true;
      };
      if (column.type == ValueType::kInt64) {
        if (!column.has_min_max) {
          // All-NULL blocks have no range. Do not infer this from a missing
          // synopsis.
          if (!all_rows_null()) {
            return std::nullopt;
          }
        } else if (std::max(magnitude(column.min_int),
                            magnitude(column.max_int)) >
                   kMaxCount / row_bound) {
          return std::nullopt;
        }
      } else {
        // n*u <= 1/8: twice the exact absolute-sum bound covers rounding in
        // any merge tree. Reject NaN/infinity, including a single input.
        if (row_bound > (uint64_t{1} << 50) || column.has_nan) {
          return std::nullopt;
        }
        if (!column.has_double_min_max) {
          if (!all_rows_null()) {
            return std::nullopt;
          }
        } else {
          const double maximum = std::max(std::abs(column.min_double),
                                          std::abs(column.max_double));
          if (!std::isfinite(maximum) ||
              maximum > std::numeric_limits<double>::max() / 2 /
                            static_cast<double>(row_bound)) {
            return std::nullopt;
          }
        }
      }
    }
  }
  GroupedTransferProof proof{.source = TableKeyFilterKey(body.Sources()[0]),
                             .store = stores[0],
                             .output_columns = {}};
  for (size_t output = 0; output < spec.select_items.size(); ++output) {
    const auto& item = spec.select_items[output];
    if (!item || item->Type() != TypeTag::kColumnValue) {
      continue;
    }
    const auto slot = resolve(item->AsColumnValue().GetColumnName());
    if (slot && group_columns.contains(*slot) && locations[*slot].first == 0 &&
        joined.GetColumn(*slot).Type() == ValueType::kInt64) {
      proof.output_columns.emplace(output, locations[*slot].second);
    }
  }
  return proof;
}

std::shared_ptr<const ScanTupleFilter> TryGroupedPredicateTransfer(
    TransactionContext& context, const SelectStatement& body,
    const std::vector<std::vector<size_t>>& probe_keys,
    const std::vector<std::unique_ptr<GroupedNumericHash>>& builds) {
  const auto proof = ProveGroupedTransfer(context, body);
  if (!proof) {
    return nullptr;
  }
  std::vector<size_t> selected;
  selected.reserve(probe_keys[1].size());
  std::vector<slot_t> columns;
  for (size_t output : probe_keys[1]) {
    const auto column = proof->output_columns.find(output);
    if (column == proof->output_columns.end()) {
      continue;
    }
    if (!std::ranges::all_of(
            std::span(probe_keys).subspan(1), [&](const auto& keys) {
              return std::ranges::find(keys, output) != keys.end();
            })) {
      continue;
    }
    if (std::ranges::find(selected, output) != selected.end()) {
      continue;
    }
    selected.push_back(output);
    columns.push_back(column->second);
  }
  if (columns.empty() || columns.size() > 4) {
    return nullptr;
  }
  size_t count = 0;
  for (size_t i = 1; i < builds.size(); ++i) {
    if (builds[i]->Size() > std::numeric_limits<size_t>::max() - count) {
      return nullptr;
    }
    count += builds[i]->Size();
  }
  if (count > std::numeric_limits<size_t>::max() / 10) {
    return nullptr;
  }
  const size_t bits = BloomRuntimeFilter::BitsForKeys(count);
  auto charge = TryReserveQueryMemory((bits / 8) + 1024);
  if (!charge) {
    return nullptr;
  }
  auto filter = std::make_shared<ScanTupleFilter>();
  filter->charge = std::move(charge);
  filter->source = proof->source;
  filter->store = proof->store;
  filter->columns = std::move(columns);
  std::vector<uint64_t> words(bits / 64, 0);
  for (size_t i = 1; i < builds.size(); ++i) {
    std::array<size_t, 4> positions{};
    for (size_t k = 0; k < selected.size(); ++k) {
      positions[k] =
          std::ranges::find(probe_keys[i], selected[k]) - probe_keys[i].begin();
    }
    builds[i]->ForEachKey([&](std::span<const int64_t> key) {
      std::array<int64_t, 4> projected{};
      for (size_t k = 0; k < selected.size(); ++k) {
        projected[k] = key[positions[k]];
      }
      const int64_t fingerprint = ScanTupleFilter::Fingerprint(
          std::span(projected).first(selected.size()));
      for (unsigned h = 0; h < BloomRuntimeFilter::kHashProbes; ++h) {
        const size_t bit =
            BloomRuntimeFilter::Hash(fingerprint, h) & (bits - 1);
        words[bit / 64] |= uint64_t{1} << (bit % 64);
      }
    });
  }
  filter->bloom =
      std::make_shared<BloomRuntimeFilter>(std::move(words), bits, count);
  return filter;
}

// Seed reverse transfer from only the preserved input's raw key columns.
// Its unfiltered domain is a superset of every grouped key, so this extra pass
// can prune right-hand inputs without first evaluating/materializing the left.
StatusOr<std::vector<std::shared_ptr<const ScanTupleFilter>>>
BuildReverseGroupedTransfers(
    TransactionContext& context,
    const std::vector<std::shared_ptr<SelectStatement>>& bodies,
    const std::vector<std::vector<size_t>>& probe_keys,
    const std::vector<std::unique_ptr<GroupedNumericHash>>& builds) {
  std::vector<std::shared_ptr<const ScanTupleFilter>> filters(bodies.size());
  if (::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_REVERSE_TRANSFER") !=
      nullptr) {
    return filters;
  }
  const auto left = ProveGroupedTransfer(context, *bodies[0]);
  if (!left) {
    return filters;
  }
  std::vector<size_t> selected;
  for (size_t output : probe_keys[1]) {
    if (left->output_columns.contains(output) &&
        std::ranges::find(selected, output) == selected.end() &&
        std::ranges::all_of(
            std::span(probe_keys).subspan(1), [&](const auto& keys) {
              return std::ranges::find(keys, output) != keys.end();
            })) {
      selected.push_back(output);
    }
  }
  if (selected.empty() || selected.size() > 4) {
    return filters;
  }
  std::vector<std::shared_ptr<ScanTupleFilter>> targets(bodies.size());
  bool any = false;
  uint64_t build_scan_rows = 0;
  for (size_t i = 1; i < bodies.size(); ++i) {
    const auto right = ProveGroupedTransfer(context, *bodies[i]);
    if (!right) {
      continue;
    }
    auto filter = std::make_shared<ScanTupleFilter>();
    for (size_t output : selected) {
      const size_t position =
          std::ranges::find(probe_keys[i], output) - probe_keys[i].begin();
      const auto column =
          right->output_columns.find(builds[i]->KeyColumns()[position]);
      if (column == right->output_columns.end()) {
        break;
      }
      filter->columns.push_back(column->second);
    }
    if (filter->columns.size() != selected.size()) {
      continue;
    }
    filter->source = right->source;
    filter->store = right->store;
    targets[i] = std::move(filter);
    build_scan_rows += std::min<uint64_t>(
        std::numeric_limits<uint64_t>::max() - build_scan_rows,
        right->store->total_rows);
    any = true;
  }
  // Avoid a full preserved-key pass when the build scans are already tiny.
  // This cost gate never participates in the correctness proof above.
  if (!any || build_scan_rows < left->store->total_rows / 8 ||
      left->store->total_rows > std::numeric_limits<size_t>::max() / 10) {
    return filters;
  }
  const size_t bits = BloomRuntimeFilter::BitsForKeys(left->store->total_rows);
  const size_t nblocks = left->store->blocks.size();
  const size_t workers =
      std::max<size_t>(1, std::min(nblocks, QueryParallelWorkerLimit(8)));
  auto charge = TryReserveQueryMemory(((bits / 8) * workers) + 4096);
  if (!charge) {
    return filters;
  }
  std::vector<std::vector<uint64_t>> words;
  words.reserve(workers);
  for (size_t w = 0; w < workers; ++w) {
    words.emplace_back(bits / 64, 0);
  }
  std::vector<slot_t> columns;
  columns.reserve(selected.size());
  for (size_t output : selected) {
    columns.push_back(left->output_columns.at(output));
  }
  RETURN_IF_FAIL(RunParallelTasks(workers, [&](size_t worker) {
    for (size_t b = nblocks * worker / workers;
         b < nblocks * (worker + 1) / workers; ++b) {
      const auto& block = left->store->blocks[b];
      for (size_t r = 0; r < block.row_count; ++r) {
        std::array<int64_t, 4> key{};
        bool valid = true;
        for (size_t k = 0; k < columns.size(); ++k) {
          const auto& column = block.columns[columns[k]];
          if (column.IsNull(r)) {
            valid = false;
            break;
          }
          key[k] = column.IntAt(r);
        }
        if (!valid) continue;
        const int64_t fingerprint =
            ScanTupleFilter::Fingerprint(std::span(key).first(columns.size()));
        for (unsigned h = 0; h < BloomRuntimeFilter::kHashProbes; ++h) {
          const size_t bit =
              BloomRuntimeFilter::Hash(fingerprint, h) & (bits - 1);
          words[worker][bit / 64] |= uint64_t{1} << (bit % 64);
        }
      }
    }
  }));
  if (workers > 1) {
    RETURN_IF_FAIL(RunParallelTasks(workers, [&](size_t worker) {
      const size_t count = bits / 64;
      for (size_t word = count * worker / workers;
           word < count * (worker + 1) / workers; ++word) {
        for (size_t w = 1; w < workers; ++w) {
          words[0][word] |= words[w][word];
        }
      }
    }));
  }
  const auto bloom = std::make_shared<BloomRuntimeFilter>(
      std::move(words[0]), bits, left->store->total_rows);
  for (size_t i = 1; i < targets.size(); ++i) {
    if (!targets[i]) {
      continue;
    }
    targets[i]->charge = charge;
    targets[i]->bloom = bloom;
    filters[i] = std::move(targets[i]);
  }
  if (auto* runtime = context.execution_runtime()) {
    runtime->transfer_seed_rows += left->store->total_rows;
  }
  return filters;
}

}  // namespace

StatusOr<std::optional<Relation>> TryPushGroupedCteJoins(
    TransactionContext& context, const SelectStatement& statement,
    const CteMap& inherited_ctes) {
  const auto decline = [&](int line) {
    if (::tinylamb::platform::GetEnv("TINYLAMB_FUSION_DEBUG") != nullptr &&
        statement.Sources().size() >= 2) {
      std::cerr << "[group-push] decline=" << line
                << " sources=" << statement.Sources().size()
                << " ctes=" << statement.WithQueries().size() << "\n";
    }
    return std::optional<Relation>{};
  };
  const auto& sources = statement.Sources();
  if (::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_GROUP_OUTPUT_PUSH") !=
          nullptr ||
      sources.size() < 2 || sources.size() > 4 ||
      (!statement.WithQueries().empty() &&
       statement.WithQueries().size() != sources.size()) ||
      (statement.WithQueries().empty() &&
       !std::ranges::all_of(sources,
                            [](const SelectSource& source) {
                              return source.query != nullptr;
                            })) ||
      !inherited_ctes.empty() || HasWindowFunctions(statement) ||
      statement.Distinct() || !statement.UnionAll().empty() ||
      statement.GetSetOperationTree() || statement.Qualify() ||
      statement.AsStruct() || statement.ForUpdate() ||
      StatementContainsVolatileFunction(statement) ||
      ContainsQuery(statement.WhereClause()) ||
      ContainsQuery(statement.Having()) ||
      !NumericPushPredicate(statement.WhereClause()) ||
      (statement.WhereClause() &&
       !SafeToReduceEvaluationCount(statement.WhereClause()))) {
    return decline(__LINE__);
  }
  for (const auto& item : statement.SelectList()) {
    if (ContainsQuery(item.expression)) {
      return decline(__LINE__);
    }
  }
  for (const auto& term : statement.GroupBy()) {
    if (ContainsQuery(term)) {
      return decline(__LINE__);
    }
  }
  for (const auto& term : statement.OrderBy()) {
    if (ContainsQuery(term.expression)) {
      return decline(__LINE__);
    }
  }
  const bool is_full_join =
      sources.size() == 2 && sources[1].join_type == JoinType::kFull;
  std::vector<Relation> shapes(sources.size());
  std::vector<std::shared_ptr<SelectStatement>> bodies(sources.size());
  std::unordered_set<std::string> seen;
  for (size_t i = 0; i < sources.size(); ++i) {
    const auto& source = sources[i];
    const auto found = statement.WithQueries().find(source.table);
    const auto qualifier = source.alias.empty() ? source.table : source.alias;
    const bool allowed_join =
        i == 0 || source.join_type == JoinType::kLeft ||
        (sources.size() == 2 && i == 1 && source.join_type == JoinType::kFull);
    if ((!source.query && found == statement.WithQueries().end()) ||
        (source.query && !statement.WithQueries().empty()) ||
        !seen.insert(qualifier).second ||
        statement.IsRecursiveWith(source.table) || source.unnest ||
        source.sample || source.is_lateral || source.from_nested_join ||
        !source.using_columns.empty() || !allowed_join) {
      return decline(__LINE__);
    }
    const auto definition = source.query ? source.query : found->second;
    const auto& body = *definition;
    if (body.GroupBy().empty() || body.Having() || body.Qualify() ||
        body.AsStruct() || body.ForUpdate() || body.HasLimit() ||
        body.Offset() != 0 || !body.OrderBy().empty() || body.Distinct() ||
        !body.UnionAll().empty() || body.GetSetOperationTree() ||
        !body.WithQueries().empty() || HasWindowFunctions(body)) {
      return decline(__LINE__);
    }
    std::unordered_set<std::string> refs;
    if (!source.query) {
      CollectCteBodyOuterRefs(statement, source.table, &refs);
    }
    for (const auto& name : refs) {
      if (statement.WithQueries().contains(name)) {
        return decline(__LINE__);
      }
    }
    auto spec = BuildGroupedJoinFusionSpec(body);
    if (!spec.eligible || spec.select_names.empty() ||
        spec.select_names.size() > 32) {
      return decline(__LINE__);
    }
    for (const auto& item : spec.select_items) {
      if (!item || (item->Type() != TypeTag::kColumnValue &&
                    item->Type() != TypeTag::kAggregateExp &&
                    item->Type() != TypeTag::kConstantValue)) {
        return decline(__LINE__);
      }
    }
    std::vector<Column> columns;
    columns.reserve(spec.select_names.size());
    for (const auto& name : spec.select_names) {
      columns.emplace_back(name, ValueType::kNull);
    }
    shapes[i].schema = QualifySchema(Schema("", std::move(columns)), qualifier);
    if (!source.query) {
      bodies[i] = PushSingleUseCtePredicates(context, statement, source.table);
    }
    if (!bodies[i]) {
      bodies[i] = definition;
    }
  }
  std::vector<std::vector<size_t>> probe_keys(sources.size());
  std::vector<std::unique_ptr<GroupedNumericHash>> builds(sources.size());
  for (size_t i = 1; i < sources.size(); ++i) {
    const auto terms = SplitConjuncts(sources[i].join_condition);
    const auto keys = EqualityKeys(shapes[0].schema, shapes[i].schema, terms);
    if (keys.empty() || keys.size() != terms.size() || keys.size() > 4) {
      return decline(__LINE__);
    }
    std::vector<size_t> build_keys;
    for (const auto& key : keys) {
      if (key.null_safe || key.left_offset != 0 || key.right_offset != 0) {
        return decline(__LINE__);
      }
      probe_keys[i].push_back(key.left);
      build_keys.push_back(key.right);
    }
    builds[i] = std::make_unique<GroupedNumericHash>(std::move(build_keys),
                                                     is_full_join);
  }
  // Skip a tuple absent from every nullable side only with a total WHERE
  // conjunct that rejects the all-NULL right sides independently of the left.
  // For FULL OUTER JOIN, also track whether unmatched right rows can ever pass.
  bool reject_unmatched = false;
  bool reject_unmatched_right = false;
  Schema shape = shapes[0].schema;
  for (size_t i = 1; i < shapes.size(); ++i) {
    shape = shape + shapes[i].schema;
  }
  const Row nulls(std::vector<Value>(shape.ColumnCount()));
  const Scope null_scope{.row = &nulls, .schema = &shape};
  const auto where_predicates =
      AnalyzePredicates(statement.WhereClause(), shapes);
  for (const auto& pred : where_predicates) {
    if (!pred.resolved) {
      return decline(__LINE__);
    }
  }
  for (const auto& pred : where_predicates) {
    if (pred.contains_query || !SafeToReduceEvaluationCount(pred.expression)) {
      continue;
    }
    if (!pred.sources.contains(0)) {
      auto value = TryEvaluate(pred.expression, null_scope, nullptr, context,
                               inherited_ctes);
      if (value.HasValue() && !Truthy(value.Value())) {
        reject_unmatched = true;
      }
    }
    if (is_full_join && !pred.sources.contains(1)) {
      auto value = TryEvaluate(pred.expression, null_scope, nullptr, context,
                               inherited_ctes);
      if (value.HasValue() && !Truthy(value.Value())) {
        reject_unmatched_right = true;
      }
    }
  }
  Relation result(context.execution_runtime());
  const auto add_stats = [&](const Relation& input) {
    result.hash_joins += input.hash_joins;
    result.hybrid_hash_joins += input.hybrid_hash_joins;
    result.in_memory_hash_joins += input.in_memory_hash_joins;
    result.nested_loop_joins += input.nested_loop_joins;
    result.join_comparisons += input.join_comparisons;
    result.peak_intermediate_rows =
        std::max(result.peak_intermediate_rows, input.peak_intermediate_rows);
  };
  std::vector<std::shared_ptr<const ScanTupleFilter>> reverse_filters(
      sources.size());
  if (is_full_join ? reject_unmatched_right : reject_unmatched) {
    try {
      ASSIGN_OR_RETURN(
          std::vector<std::shared_ptr<const ScanTupleFilter>>, filters,
          BuildReverseGroupedTransfers(context, bodies, probe_keys, builds));
      reverse_filters = std::move(filters);
    } catch (const std::bad_alloc&) {  // NOLINT(bugprone-empty-catch)
      // Nothing was published: memory pressure keeps the unpruned build.
    }
  }
  for (size_t i = 1; i < sources.size(); ++i) {
    auto sink = builds[i]->Sink();
    sink.input_filter = reverse_filters[i];
    ASSIGN_OR_RETURN(
        Relation, output,
        ExecuteQuery(context, *bodies[i], nullptr, inherited_ctes, &sink));
    RETURN_IF_FAIL(output.GetStatus());
    if (!sink.used || !builds[i]->Supported()) {
      return decline(__LINE__);
    }
    add_stats(output);
    const auto qualifier =
        sources[i].alias.empty() ? sources[i].table : sources[i].alias;
    shapes[i].schema = QualifySchema(builds[i]->OutputSchema(), qualifier);
  }
  struct DirectScalarTerm {
    AggregationType op{AggregationType::kSum};
    bool count_star{false};
    std::optional<BytecodeProgram> program;
  };
  struct DirectScalarSlot {
    int64_t int_sum{0};
    uint64_t uint_sum{0};
    double double_sum{0.0};
    bool has_int{false};
    bool has_uint{false};
    bool has_double{false};
  };
  const bool scalar_agg_candidate =
      StatementIsAggregated(statement) && statement.GroupBy().empty() &&
      !statement.Having() && !statement.Distinct() && !statement.Qualify() &&
      statement.OrderBy().empty() && statement.Offset() == 0 &&
      (!statement.HasLimit() || statement.Limit() >= 1) && BytecodeEnabled() &&
      !statement.SelectList().empty() &&
      ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_PUSHED_SCALAR_AGG") ==
          nullptr;
  bool use_direct_scalar = false;
  std::vector<DirectScalarTerm> scalar_terms;
  std::vector<std::vector<DirectScalarSlot>> worker_scalars;
  const auto accumulate_scalar = [&](const Row& row,
                                     std::vector<DirectScalarSlot>& slots,
                                     Status* err) {
    for (size_t ti = 0; ti < scalar_terms.size(); ++ti) {
      const DirectScalarTerm& term = scalar_terms[ti];
      DirectScalarSlot& slot = slots[ti];
      if (term.op == AggregationType::kCount && term.count_star) {
        ++slot.int_sum;
        continue;
      }
      StatusOr<Value> val = TryEvaluateRowAuto(*term.program, row);
      if (!val.HasValue()) {
        *err = val.GetStatus();
        return;
      }
      const Value& v = val.Value();
      if (v.IsNull()) {
        continue;
      }
      if (term.op == AggregationType::kCount) {
        ++slot.int_sum;
        continue;
      }
      if (v.type == ValueType::kInt64) {
        // Mirror AggregateAccumulator: one unsigned input (or a sum
        // already in the uint64 domain) moves the whole SUM to uint64,
        // with Int128 arithmetic rejecting anything outside
        // [0, UINT64_MAX]. Signed inputs stay exact in the int64 domain
        // until an unsigned one is seen, so all-signed SUMs keep their
        // signed result domain.
        const bool input_uint = v.IsUnsigned();
        if (slot.has_double) {
          slot.double_sum += input_uint
                                 ? static_cast<double>(
                                       static_cast<uint64_t>(v.value.int_value))
                                 : static_cast<double>(v.value.int_value);
        } else if (input_uint || slot.has_uint) {
          const platform::Int128 prior =
              slot.has_uint ? static_cast<platform::Int128>(slot.uint_sum)
                            : static_cast<platform::Int128>(slot.int_sum);
          const platform::Int128 input =
              input_uint ? static_cast<platform::Int128>(
                               static_cast<uint64_t>(v.value.int_value))
                         : static_cast<platform::Int128>(v.value.int_value);
          const platform::Int128 next = prior + input;
          if (next < 0 ||
              std::cmp_greater(next, std::numeric_limits<uint64_t>::max())) {
            *err =
                StatusError(StatusCode::kIsInfinity, "uint64 overflow in SUM");
            return;
          }
          slot.uint_sum = static_cast<uint64_t>(next);
          slot.has_uint = true;
          slot.has_int = false;
        } else if (slot.has_int) {
          if (__builtin_add_overflow(slot.int_sum, v.value.int_value,
                                     &slot.int_sum)) {
            *err =
                StatusError(StatusCode::kIsInfinity, "integer overflow on '+'");
            return;
          }
        } else {
          slot.int_sum = v.value.int_value;
          slot.has_int = true;
        }
      } else if (v.type == ValueType::kDouble) {
        if (!slot.has_double) {
          slot.double_sum = slot.has_uint  ? static_cast<double>(slot.uint_sum)
                            : slot.has_int ? static_cast<double>(slot.int_sum)
                                           : 0.0;
          slot.has_double = true;
          slot.has_int = false;
          slot.has_uint = false;
        }
        slot.double_sum += v.value.double_value;
      } else {
        *err = StatusError(StatusCode::kInvalidArgument,
                           "SUM requires signed INT64 or DOUBLE input");
        return;
      }
    }
  };
  struct NullSigBit {
    size_t local_col{0};
    uint8_t bit{0};
  };
  struct NullSigContrib {
    Status error{Status::kSuccess};
    bool is_null{true};
    int64_t int_val{0};
  };
  bool use_sig_scalar = false;
  size_t sig_slot_count = 0;
  std::vector<NullSigBit> left_sig_bits;
  std::vector<std::vector<NullSigBit>> build_sig_bits(sources.size());
  std::vector<bool> sig_mask_pass;
  std::vector<NullSigContrib> sig_contribs;
  std::vector<std::array<size_t, 64>> worker_sig_counts;
  std::vector<ExecutionRuntime> runtimes;
  std::vector<Relation> shards;
  std::vector<Row> scratch;
  std::vector<ColumnLookupCache> lookup;
  std::vector<size_t> offsets(sources.size());
  CompiledScanFilter filter;
  std::atomic<bool> supported{true};
  GroupedOutputSink probe_sink{
      .begin = [&](const Schema& schema, size_t,
                   size_t workers) -> StatusOr<bool> {
        if (schema.ColumnCount() != shapes[0].schema.ColumnCount()) {
          return false;
        }
        const auto qualifier =
            sources[0].alias.empty() ? sources[0].table : sources[0].alias;
        result.schema = QualifySchema(schema, qualifier);
        for (size_t i = 1; i < sources.size(); ++i) {
          offsets[i] = result.schema.ColumnCount();
          result.schema = result.schema + shapes[i].schema;
        }
        filter = CompileScanFilter(SplitConjuncts(statement.WhereClause()),
                                   result.schema);
        if (scalar_agg_candidate) {
          std::vector<DirectScalarTerm> compiled_terms;
          compiled_terms.reserve(statement.SelectList().size());
          bool all_scalar_ok = true;
          for (const NamedExpression& item : statement.SelectList()) {
            if (!item.expression ||
                item.expression->Type() != TypeTag::kAggregateExp) {
              all_scalar_ok = false;
              break;
            }
            const AggregateExpression& agg =
                item.expression->AsAggregateExpression();
            if (agg.Distinct() || agg.HasWhereFilter() ||
                agg.Having() != AggregateHavingModifier::kNone ||
                !agg.InnerOrderBy().empty() || agg.InnerLimit().has_value() ||
                agg.IgnoreNulls() ||
                (agg.GetType() != AggregationType::kSum &&
                 agg.GetType() != AggregationType::kCount)) {
              all_scalar_ok = false;
              break;
            }
            DirectScalarTerm term;
            term.op = agg.GetType();
            if (agg.GetType() == AggregationType::kCount && IsCountStar(agg)) {
              term.count_star = true;
            } else {
              if (!agg.Child() || ContainsParameterSlot(agg.Child()) ||
                  !SafeToReduceEvaluationCountWithSchema(agg.Child(),
                                                         result.schema)) {
                all_scalar_ok = false;
                break;
              }
              term.program = BytecodeCompiler::CompileRowCached(
                  agg.Child(), result.schema,
                  BytecodeCompiler::Context::kValue);
              if (!term.program.has_value()) {
                all_scalar_ok = false;
                break;
              }
              if (term.op == AggregationType::kSum) {
                const ValueType rt = term.program->ResultType();
                if (rt != ValueType::kInt64 && rt != ValueType::kDouble &&
                    rt != ValueType::kNull) {
                  all_scalar_ok = false;
                  break;
                }
              }
            }
            compiled_terms.push_back(std::move(term));
          }
          if (all_scalar_ok) {
            scalar_terms = std::move(compiled_terms);
            use_direct_scalar = true;
            std::vector<slot_t> sig_slots;
            const auto add_sig_col = [&](const ColumnName& name) -> bool {
              const std::optional<size_t> resolved =
                  LocalColumnOffset(result.schema, name);
              if (!resolved.has_value() ||
                  *resolved >= result.schema.ColumnCount()) {
                return false;
              }
              if (result.schema.GetColumn(*resolved).Type() ==
                  ValueType::kArray) {
                return false;
              }
              if (std::ranges::find(sig_slots, *resolved) == sig_slots.end()) {
                if (sig_slots.size() >= 6) {
                  return false;
                }
                sig_slots.push_back(*resolved);
              }
              return true;
            };
            const auto collect_bool = [&](const Expression& expr,
                                          auto&& self) -> bool {
              if (!expr) {
                return false;
              }
              if (expr->Type() == TypeTag::kUnaryExp) {
                const UnaryExpression& u = expr->AsUnaryExpression();
                if (u.Op() == UnaryOperation::kNot) {
                  return self(u.Child(), self);
                }
                if (u.Op() == UnaryOperation::kIsNull ||
                    u.Op() == UnaryOperation::kIsNotNull) {
                  if (!u.Child()) {
                    return false;
                  }
                  if (u.Child()->Type() == TypeTag::kColumnValue) {
                    return add_sig_col(
                        u.Child()->AsColumnValue().GetColumnName());
                  }
                  return self(u.Child(), self);
                }
                return false;
              }
              if (expr->Type() == TypeTag::kBinaryExp) {
                const BinaryExpression& b = expr->AsBinaryExpression();
                return (b.Op() == BinaryOperation::kAnd ||
                        b.Op() == BinaryOperation::kOr) &&
                       self(b.Left(), self) && self(b.Right(), self);
              }
              return false;
            };
            const auto collect_val = [&](const Expression& expr,
                                         auto&& self) -> bool {
              if (!expr) {
                return false;
              }
              switch (expr->Type()) {
                case TypeTag::kConstantValue:
                  return true;
                case TypeTag::kCastExp:
                  return self(expr->AsCastExpression().Child(), self);
                case TypeTag::kCaseExp: {
                  const CaseExpression& c = expr->AsCaseExpression();
                  for (const auto& [w, t] : c.when_clauses_) {
                    if (!collect_bool(w, collect_bool) || !self(t, self)) {
                      return false;
                    }
                  }
                  return !c.else_clause_ || self(c.else_clause_, self);
                }
                default:
                  return false;
              }
            };
            bool sig_ok = true;
            if (statement.WhereClause()) {
              if (ContainsParameterSlot(statement.WhereClause()) ||
                  !SafeToReduceEvaluationCountWithSchema(
                      statement.WhereClause(), result.schema)) {
                sig_ok = false;
              } else {
                for (const Expression& conj :
                     SplitConjuncts(statement.WhereClause())) {
                  if (!collect_bool(conj, collect_bool)) {
                    sig_ok = false;
                    break;
                  }
                }
              }
            }
            for (size_t ti = 0; ti < scalar_terms.size() && sig_ok; ++ti) {
              if (scalar_terms[ti].count_star) {
                continue;
              }
              const Expression& child = statement.SelectList()[ti]
                                            .expression->AsAggregateExpression()
                                            .Child();
              if (scalar_terms[ti].op == AggregationType::kCount &&
                  child->Type() == TypeTag::kColumnValue) {
                sig_ok = add_sig_col(child->AsColumnValue().GetColumnName());
              } else {
                sig_ok = collect_val(child, collect_val);
              }
            }
            if (sig_ok) {
              const size_t n_masks = size_t{1} << sig_slots.size();
              const size_t n_terms = scalar_terms.size();
              sig_mask_pass.assign(n_masks, true);
              sig_contribs.assign(n_masks * n_terms, NullSigContrib{});
              Row dummy_row(std::vector<Value>(result.schema.ColumnCount()));
              ColumnLookupCache dummy_lookup;
              for (size_t mask = 0; mask < n_masks && sig_ok; ++mask) {
                for (size_t b = 0; b < sig_slots.size(); ++b) {
                  const slot_t col = sig_slots[b];
                  if ((mask & (size_t{1} << b)) == 0) {
                    dummy_row.values_[col] = Value();
                    continue;
                  }
                  switch (result.schema.GetColumn(col).Type()) {
                    case ValueType::kDouble:
                      dummy_row.values_[col] = Value(1.0);
                      break;
                    case ValueType::kVarChar:
                      dummy_row.values_[col] = Value("a");
                      break;
                    case ValueType::kDate:
                      dummy_row.values_[col] = Value::DateFromDays(0);
                      break;
                    default:
                      dummy_row.values_[col] = Value(int64_t{1});
                      break;
                  }
                }
                if (statement.WhereClause()) {
                  Status dummy_err = Status::kSuccess;
                  sig_mask_pass[mask] = MatchScanFilter(
                      dummy_row, result.schema, filter, nullptr, context,
                      inherited_ctes, &dummy_err, &dummy_lookup);
                  if (!dummy_err.ok()) {
                    sig_ok = false;
                    break;
                  }
                }
                if (!sig_mask_pass[mask]) {
                  continue;
                }
                for (size_t ti = 0; ti < n_terms; ++ti) {
                  NullSigContrib& c = sig_contribs[(mask * n_terms) + ti];
                  if (scalar_terms[ti].count_star) {
                    c.is_null = false;
                    c.int_val = 1;
                    continue;
                  }
                  StatusOr<Value> val =
                      TryEvaluateRowAuto(*scalar_terms[ti].program, dummy_row);
                  if (!val.HasValue()) {
                    c.error = val.GetStatus();
                    continue;
                  }
                  const Value& v = val.Value();
                  if (v.IsNull()) {
                    c.is_null = true;
                  } else if (scalar_terms[ti].op == AggregationType::kCount) {
                    c.is_null = false;
                    c.int_val = 1;
                  } else if (v.type == ValueType::kInt64 && !v.IsUnsigned()) {
                    c.is_null = false;
                    c.int_val = v.value.int_value;
                  } else {
                    sig_ok = false;
                    break;
                  }
                }
              }
            }
            if (sig_ok) {
              sig_slot_count = sig_slots.size();
              left_sig_bits.clear();
              for (auto& bits : build_sig_bits) {
                bits.clear();
              }
              for (size_t b = 0; b < sig_slots.size(); ++b) {
                const slot_t col = sig_slots[b];
                if (col < shapes[0].schema.ColumnCount()) {
                  left_sig_bits.push_back(
                      {.local_col = col, .bit = static_cast<uint8_t>(b)});
                } else {
                  bool placed = false;
                  for (size_t s = 1; s < sources.size(); ++s) {
                    if (col >= offsets[s] &&
                        col < offsets[s] + shapes[s].schema.ColumnCount()) {
                      build_sig_bits[s].push_back(
                          {.local_col = col - offsets[s],
                           .bit = static_cast<uint8_t>(b)});
                      placed = true;
                      break;
                    }
                  }
                  if (!placed) {
                    sig_ok = false;
                  }
                }
              }
            }
            if (sig_ok) {
              worker_sig_counts.assign(workers, std::array<size_t, 64>{});
              use_sig_scalar = true;
            } else {
              worker_scalars.assign(
                  workers, std::vector<DirectScalarSlot>(scalar_terms.size()));
            }
          }
        }
        runtimes.resize(workers);
        if (!use_direct_scalar) {
          shards.resize(workers);
          for (size_t w = 0; w < workers; ++w) {
            shards[w].set_runtime(&runtimes[w]);
          }
        }
        scratch.resize(workers);
        lookup.resize(workers);
        for (size_t w = 0; w < workers; ++w) {
          scratch[w].values_.resize(result.schema.ColumnCount());
        }
        return true;
      },
      .push = [&](size_t worker, size_t,
                  std::span<const Value> cells) -> Status {
        const size_t width = shapes[0].schema.ColumnCount();
        Row& row = scratch[worker];
        Status error = Status::kSuccess;
        for (size_t r = 0; r < cells.size() / width; ++r) {
          const auto input = cells.subspan(r * width, width);
          bool valid = true;
          for (const Value& cell : input) {
            valid &= cell.IsNull() || cell.type == ValueType::kInt64 ||
                     cell.type == ValueType::kDouble;
          }
          for (size_t i = 1; i < sources.size(); ++i) {
            for (size_t k : probe_keys[i]) {
              valid &=
                  input[k].IsNull() || (input[k].type == ValueType::kInt64 &&
                                        !input[k].IsUnsigned());
            }
          }
          if (!valid) {
            supported.store(false, std::memory_order_relaxed);
            continue;
          }
          if (use_sig_scalar) {
            uint32_t base_mask = 0;
            for (const NullSigBit& sig : left_sig_bits) {
              if (!input[sig.local_col].IsNull()) {
                base_mask |= (uint32_t{1} << sig.bit);
              }
            }
            const auto probe_sig = [&](auto&& self, size_t index, uint32_t mask,
                                       bool any_match) -> void {
              if (index == sources.size()) {
                if (reject_unmatched && !any_match) {
                  return;
                }
                ++worker_sig_counts[worker][mask];
                return;
              }
              bool matched = false;
              const auto& bits = build_sig_bits[index];
              builds[index]->Matches(
                  input, probe_keys[index],
                  [&](std::span<const GroupedNumericHash::Cell> values) {
                    matched = true;
                    uint32_t next_mask = mask;
                    for (const NullSigBit& sig : bits) {
                      if (values[sig.local_col].type != ValueType::kNull) {
                        next_mask |= (uint32_t{1} << sig.bit);
                      }
                    }
                    self(self, index + 1, next_mask, true);
                  });
              if (!matched) {
                self(self, index + 1, mask, any_match);
              }
            };
            probe_sig(probe_sig, 1, base_mask, false);
            continue;
          }
          std::copy(input.begin(), input.end(), row.values_.begin());
          const auto probe = [&](auto&& self, size_t index,
                                 bool any_match) -> void {
            if (!error.ok()) {
              return;
            }
            if (index == sources.size()) {
              if (reject_unmatched && !any_match) {
                return;
              }
              if (MatchScanFilter(row, result.schema, filter, nullptr, context,
                                  inherited_ctes, &error, &lookup[worker])) {
                if (use_direct_scalar) {
                  accumulate_scalar(row, worker_scalars[worker], &error);
                } else {
                  shards[worker].AddRow(row);
                  if (!shards[worker].GetStatus().ok()) {
                    error = shards[worker].GetStatus();
                  }
                }
              }
              return;
            }
            bool matched = false;
            builds[index]->Matches(
                input, probe_keys[index],
                [&](std::span<const GroupedNumericHash::Cell> values) {
                  matched = true;
                  for (size_t c = 0; c < values.size(); ++c) {
                    row.values_[offsets[index] + c] = values[c].Get();
                  }
                  self(self, index + 1, true);
                });
            if (!matched) {
              for (size_t c = 0; c < builds[index]->Width(); ++c) {
                row.values_[offsets[index] + c] = Value();
              }
              self(self, index + 1, any_match);
            }
          };
          probe(probe, 1, false);
          RETURN_IF_FAIL(error);
        }
        return Status::kSuccess;
      }};
  if (reject_unmatched) {
    // Build completion precedes the probe scan. A memory/proof refusal keeps
    // the existing unpruned route; no partial filter is ever published.
    try {
      probe_sink.input_filter =
          TryGroupedPredicateTransfer(context, *bodies[0], probe_keys, builds);
    } catch (const std::bad_alloc&) {
      probe_sink.input_filter.reset();
    }
  }
  ASSIGN_OR_RETURN(
      Relation, output,
      ExecuteQuery(context, *bodies[0], nullptr, inherited_ctes, &probe_sink));
  RETURN_IF_FAIL(output.GetStatus());
  if (!probe_sink.used || !supported.load(std::memory_order_relaxed)) {
    return decline(__LINE__);
  }
  add_stats(output);
  if (is_full_join && !reject_unmatched_right && builds[1]->Size() > 0) {
    const size_t total_right = builds[1]->Size();
    const size_t right_workers =
        total_right >= 8192
            ? std::min<size_t>(ParallelWorkerCount(total_right / 8192),
                               total_right)
            : 1;
    std::vector<Row> right_scratch(right_workers);
    std::vector<ColumnLookupCache> right_lookup(right_workers);
    std::vector<Relation> right_shards;
    std::vector<ExecutionRuntime> right_runtimes(right_workers);
    std::vector<std::vector<DirectScalarSlot>> right_scalars;
    std::vector<std::array<size_t, 64>> right_sig_counts;
    if (use_sig_scalar) {
      right_sig_counts.assign(right_workers, std::array<size_t, 64>{});
    } else if (use_direct_scalar) {
      right_scalars.assign(right_workers,
                           std::vector<DirectScalarSlot>(scalar_terms.size()));
    } else {
      right_shards.resize(right_workers);
      for (size_t w = 0; w < right_workers; ++w) {
        right_shards[w].set_runtime(&right_runtimes[w]);
        right_shards[w].schema = result.schema;
      }
    }
    if (!use_sig_scalar) {
      for (size_t w = 0; w < right_workers; ++w) {
        right_scratch[w].values_.resize(result.schema.ColumnCount());
      }
    }
    std::vector<Status> right_errors(right_workers, Status::kSuccess);
    RETURN_IF_FAIL(RunParallelTasks(right_workers, [&](size_t w) {
      const size_t begin = (total_right * w) / right_workers;
      const size_t end = (total_right * (w + 1)) / right_workers;
      if (use_sig_scalar) {
        const auto& bits = build_sig_bits[1];
        auto& counts = right_sig_counts[w];
        for (size_t r = begin; r < end; ++r) {
          if (builds[1]->IsMatched(r)) continue;
          const auto cells = builds[1]->RowCells(r);
          uint32_t mask = 0;
          for (const NullSigBit& sig : bits) {
            if (cells[sig.local_col].type != ValueType::kNull) {
              mask |= (uint32_t{1} << sig.bit);
            }
          }
          ++counts[mask];
        }
        return;
      }
      Row& row = right_scratch[w];
      Status& err = right_errors[w];
      for (size_t r = begin; r < end && err.ok(); ++r) {
        if (builds[1]->IsMatched(r)) continue;
        const auto cells = builds[1]->RowCells(r);
        for (size_t c = 0; c < cells.size(); ++c) {
          row.values_[offsets[1] + c] = cells[c].Get();
        }
        if (MatchScanFilter(row, result.schema, filter, nullptr, context,
                            inherited_ctes, &err, &right_lookup[w])) {
          if (use_direct_scalar) {
            accumulate_scalar(row, right_scalars[w], &err);
          } else {
            right_shards[w].AddRow(row);
            if (!right_shards[w].GetStatus().ok()) {
              err = right_shards[w].GetStatus();
            }
          }
        }
      }
    }));
    for (const Status& st : right_errors) {
      RETURN_IF_FAIL(st);
    }
    if (use_sig_scalar) {
      for (const auto& rc : right_sig_counts) {
        worker_sig_counts.push_back(rc);
      }
    } else if (use_direct_scalar) {
      for (auto& ws : right_scalars) {
        worker_scalars.push_back(std::move(ws));
      }
    } else {
      for (size_t w = 0; w < right_workers; ++w) {
        shards.push_back(std::move(right_shards[w]));
        runtimes.push_back(std::move(right_runtimes[w]));
      }
    }
  }
  if (!sources.empty()) {
    result.hash_joins += sources.size() - 1;
    result.in_memory_hash_joins += sources.size() - 1;
  }
  if (auto* runtime = context.execution_runtime()) {
    ++runtime->pushed_group_joins;
  }
  if (use_direct_scalar) {
    if (use_sig_scalar) {
      const size_t n_masks = size_t{1} << sig_slot_count;
      const size_t n_terms = scalar_terms.size();
      std::array<size_t, 64> total_sig_counts{};
      for (const auto& wc : worker_sig_counts) {
        for (size_t m = 0; m < n_masks; ++m) {
          total_sig_counts[m] += wc[m];
        }
      }
      std::vector<DirectScalarSlot> merged(n_terms);
      for (size_t m = 0; m < n_masks; ++m) {
        const size_t count = total_sig_counts[m];
        if (count == 0 || !sig_mask_pass[m]) {
          continue;
        }
        for (size_t ti = 0; ti < n_terms; ++ti) {
          const NullSigContrib& c = sig_contribs[(m * n_terms) + ti];
          if (!c.error.ok()) {
            return c.error;
          }
          if (c.is_null) {
            continue;
          }
          DirectScalarSlot& slot = merged[ti];
          if (scalar_terms[ti].op == AggregationType::kCount) {
            if (count >
                    static_cast<size_t>(std::numeric_limits<int64_t>::max()) ||
                __builtin_add_overflow(
                    slot.int_sum, static_cast<int64_t>(count), &slot.int_sum)) {
              return StatusError(StatusCode::kIsInfinity,
                                 "integer overflow on '+'");
            }
          } else {
            int64_t term_delta = 0;
            if (count >
                    static_cast<size_t>(std::numeric_limits<int64_t>::max()) ||
                __builtin_mul_overflow(static_cast<int64_t>(count), c.int_val,
                                       &term_delta) ||
                __builtin_add_overflow(slot.int_sum, term_delta,
                                       &slot.int_sum)) {
              return StatusError(StatusCode::kIsInfinity,
                                 "integer overflow on '+'");
            }
            slot.has_int = true;
          }
        }
      }
      worker_scalars.assign(1, merged);
    }
    std::vector<Value> final_values;
    final_values.reserve(scalar_terms.size());
    for (size_t ti = 0; ti < scalar_terms.size(); ++ti) {
      if (scalar_terms[ti].op == AggregationType::kCount) {
        int64_t total_count = 0;
        for (const auto& ws : worker_scalars) {
          total_count += ws[ti].int_sum;
        }
        final_values.emplace_back(total_count);
      } else {
        bool any_int = false;
        bool any_double = false;
        int64_t int_sum = 0;
        double double_sum = 0.0;
        for (const auto& ws : worker_scalars) {
          const DirectScalarSlot& slot = ws[ti];
          if (slot.has_double) {
            if (!any_double) {
              double_sum = any_int ? static_cast<double>(int_sum) : 0.0;
              any_double = true;
              any_int = false;
            }
            double_sum += slot.double_sum;
          } else if (slot.has_int) {
            if (any_double) {
              double_sum += static_cast<double>(slot.int_sum);
            } else {
              if (__builtin_add_overflow(int_sum, slot.int_sum, &int_sum)) {
                return StatusError(StatusCode::kIsInfinity,
                                   "integer overflow on '+'");
              }
              any_int = true;
            }
          }
        }
        if (any_double) {
          final_values.emplace_back(double_sum);
        } else if (any_int) {
          final_values.emplace_back(int_sum);
        } else {
          final_values.emplace_back();
        }
      }
    }
    Relation finished(context.execution_runtime());
    CopyExecutionStats(&finished, result);
    std::vector<Column> output_columns;
    output_columns.reserve(statement.SelectList().size());
    for (size_t ti = 0; ti < statement.SelectList().size(); ++ti) {
      const NamedExpression& item = statement.SelectList()[ti];
      output_columns.push_back(ProjectionExpressionColumn(
          ProjectionName(item, ti), item.expression, result.schema));
    }
    finished.AddRow(Row(std::move(final_values)));
    RefineProjectionTypes(finished, output_columns);
    finished.schema = Schema("", std::move(output_columns));
    RETURN_IF_FAIL(finished.GetStatus());
    return std::optional<Relation>(std::move(finished));
  }
  for (size_t w = 0; w < shards.size(); ++w) {
    RETURN_IF_FAIL(shards[w].FinishSpill());
    RETURN_IF_FAIL(shards[w].GetStatus());
    if (shards[w].HasSpill()) {
      RETURN_IF_FAIL(
          shards[w].ForEachRow([&](const Row& row) { result.AddRow(row); }));
    } else {
      result.MergeInMemoryShard(std::move(shards[w]));
    }
    if (auto* runtime = context.execution_runtime()) {
      runtime->relation_spills += runtimes[w].relation_spills;
    }
  }
  ASSIGN_OR_RETURN(Relation, finished,
                   FinishQuery(context, statement, std::move(result), nullptr,
                               inherited_ctes, false));
  RETURN_IF_FAIL(finished.FinishSpill());
  RETURN_IF_FAIL(finished.GetStatus());
  return std::optional<Relation>(std::move(finished));
}

}  // namespace tinylamb::relational_detail
