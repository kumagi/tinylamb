/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/detail/analytics_factorized_join.hpp"
#include "executor/detail/typed_stream_aggregate_internal.hpp"

namespace tinylamb::relational_detail {

std::shared_ptr<const LSTableStore> SnapshotColumnStore(
    TransactionContext& context, const Table& table,
    const std::vector<slot_t>& projection, size_t column_count,
    bool allow_lazy_build) {
  std::vector<slot_t> needed = projection;
  if (projection.empty()) {
    needed.reserve(std::max<size_t>(1, column_count));
    for (size_t c = 0; c < column_count; ++c) {
      needed.push_back(static_cast<slot_t>(c));
    }
    if (needed.empty() && table.GetSchema().ColumnCount() > 0) {
      needed.push_back(0);
    }
  }
  std::shared_ptr<const LSTableStore> store =
      GetOrBuildLSTableStore(context.txn_, table, allow_lazy_build, &needed);
  if (store != nullptr &&
      store->layout_generation == table.LayoutGeneration() &&
      store->first_pid == table.FirstPageId()) {
    bool valid = true;
    if (store->uncommitted_owner_txn != 0) {
      valid = (store->uncommitted_owner_txn == context.txn_.ID());
    } else if (!context.txn_.IsReadOnly() && !context.txn_.WriteSetEmpty()) {
      valid = false;
    } else if (context.txn_.HasAnyPageVersionChains()) {
      for (const page_id_t pid : store->page_ids) {
        if (context.txn_.HasPageVersionChains(pid) ||
            store->FindBlock(pid) == nullptr) {
          valid = false;
          break;
        }
      }
    }
    if (valid) {
      return store;
    }
  }
  return nullptr;
}

// These dictionary / top-N shapes emit only bare keys, aggregates and
// constants. A composite item (ROUND(SUM(x), 2)) must take the general path,
// which evaluates its post-aggregate expression.
namespace {
bool HasPostAggItem(const TypedAggregationPlan& plan) {
  const auto composite = [](const TypedEmitItem& item) {
    return item.post_agg != nullptr;
  };
  return std::ranges::any_of(plan.emit_items, composite) ||
         std::ranges::any_of(plan.order_items, composite);
}
}  // namespace

// A proven unique grouping key makes every group a singleton. COUNT(*) is
// then constant, so ordering exclusively by it (and constants) cannot change
// the LIMIT tie boundary. Only direct, non-throwing inputs may skip unseen
// groups: computed keys, residual predicates and aggregate modifiers retain
// the ordinary path, including their errors outside the returned prefix.
StatusOr<std::optional<Relation>> TryUniqueGroupLimit(
    const LSTableStore& store, const TypedAggregationPlan& plan,
    const SelectStatement& statement, const std::vector<slot_t>& projection,
    const Schema& schema, ExecutionRuntime* runtime) try {
  if (HasPostAggItem(plan)) {
    return std::optional<Relation>{};
  }
  if (!statement.HasLimit() || statement.WithTies() || statement.Distinct() ||
      statement.Having() || !plan.dependent_key_programs.empty() ||
      !plan.filter_programs.empty() || !plan.filter_disjunctions.empty() ||
      !plan.null_filters.empty() || !plan.inlist_compares.empty() ||
      statement.Offset() > SIZE_MAX - statement.Limit()) {
    return std::optional<Relation>{};
  }
  const size_t capacity = statement.Offset() + statement.Limit();
  if (capacity > store.total_rows / 2) {
    return std::optional<Relation>{};
  }
  const auto column_at = [&](slot_t offset) {
    return projection.empty() ? offset : projection[offset];
  };
  if (std::ranges::none_of(plan.key_offsets, [&](slot_t offset) {
        const slot_t column = column_at(offset);
        return column < store.column_is_unique_nonnull.size() &&
               store.column_is_unique_nonnull[column] != 0;
      })) {
    return std::optional<Relation>{};
  }
  for (const auto& item : plan.order_items) {
    if (!item.constant &&
        (item.is_key || item.dependent_key ||
         !IsCountStar(*plan.aggregates[item.index].aggregate))) {
      return std::optional<Relation>{};
    }
  }
  for (const auto& input : plan.aggregates) {
    const auto& aggregate = *input.aggregate;
    if (aggregate.WhereFilter() || aggregate.NeedsGroupContext() ||
        aggregate.HasInnerGroupBy() ||
        aggregate.Having() != AggregateHavingModifier::kNone ||
        aggregate.HavingCondition() || !aggregate.InnerOrderBy().empty() ||
        aggregate.SecondaryArg() || !aggregate.ExtraArgs().empty() ||
        !aggregate.TrailingArgs().empty()) {
      return std::optional<Relation>{};
    }
    if (IsCountStar(aggregate)) {
      continue;
    }
    if (!input.column || input.child || input.conditional || input.int_expr ||
        input.double_expr) {
      return std::optional<Relation>{};
    }
    const auto& column = schema.GetColumn(*input.column);
    const auto type = aggregate.GetType();
    if (type != AggregationType::kCount &&
        (column.IsUnsigned() || column.IsInterval() ||
         (column.Type() != ValueType::kInt64 &&
          column.Type() != ValueType::kDouble) ||
         (type != AggregationType::kSum && type != AggregationType::kAvg &&
          type != AggregationType::kMin && type != AggregationType::kMax))) {
      return std::optional<Relation>{};
    }
  }
  // AnalyzeShape() derives string element types and buffer needs from the
  // aggregate + schema; they are identical for every row. Build each shape
  // once instead of re-parsing it (and re-allocating its std::string) for
  // every (row, aggregate) pair.
  std::vector<AggregateAccumulator::Shape> shapes;
  shapes.reserve(plan.aggregates.size());
  for (const auto& input : plan.aggregates) {
    shapes.push_back(
        AggregateAccumulator::AnalyzeShape(input.aggregate, &schema));
  }

  const auto started = std::chrono::steady_clock::now();
  Relation output(runtime);
  size_t visited = 0;
  std::vector<Column> columns;
  for (size_t i = 0; i < plan.emit_items.size(); ++i) {
    const auto& item = plan.emit_items[i];
    ValueType type = ValueType::kNull;
    bool is_unsigned = false;
    if (item.constant) {
      type = ValueTypeOf(*item.constant);
      is_unsigned = item.constant->IsUnsigned();
    } else if (item.is_key) {
      const auto& column = schema.GetColumn(plan.key_offsets[item.index]);
      type = column.Type();
      is_unsigned = column.IsUnsigned();
    } else {
      const auto& input = plan.aggregates[item.index];
      const auto kind = input.aggregate->GetType();
      type = kind == AggregationType::kCount ? ValueType::kInt64
             : kind == AggregationType::kAvg
                 ? ValueType::kDouble
                 : schema.GetColumn(*input.column).Type();
    }
    columns.emplace_back(ProjectionName(statement.SelectList()[i], i), type);
    columns.back().SetUnsigned(is_unsigned);
  }
  for (const auto& block : store.blocks) {
    for (size_t row = 0; row < block.row_count && output.TotalRows() < capacity;
         ++row) {
      ++visited;
      const auto value_at = [&](slot_t offset) {
        const auto& column = block.columns[column_at(offset)];
        if (column.IsNull(row)) {
          return Value();
        }
        if (column.type == ValueType::kVarChar) {
          return Value(std::string(column.VarcharAt(row)));
        }
        Value value;
        value.type = column.type;
        value.value.int_value = column.int_cells[row];
        if (column.is_unsigned) {
          value.SetUnsigned();
        }
        return value;
      };
      const bool keep = std::ranges::all_of(
          plan.filter_compares, [&](const TypedFilterCompare& compare) {
            const Value left = value_at(compare.left);
            const Value right =
                compare.constant_payload ? left : value_at(compare.right);
            return !left.IsNull() && !right.IsNull() &&
                   DecideTypedFilterCompare(left, right, compare);
          });
      if (!keep) {
        continue;
      }
      std::vector<Value> finished;
      finished.reserve(plan.aggregates.size());
      for (size_t a = 0; a < plan.aggregates.size(); ++a) {
        const auto& input = plan.aggregates[a];
        AggregateAccumulator state(input.aggregate, shapes[a]);
        RETURN_IF_FAIL(state.Add(IsCountStar(*input.aggregate)
                                     ? Value(1)
                                     : value_at(*input.column)));
        ASSIGN_OR_RETURN(Value, value, state.TryFinish());
        finished.push_back(std::move(value));
      }
      std::vector<Value> values;
      values.reserve(plan.emit_items.size());
      for (const auto& item : plan.emit_items) {
        values.push_back(item.constant ? *item.constant
                         : item.is_key ? value_at(plan.key_offsets[item.index])
                                       : finished[item.index]);
      }
      output.AddRow(Row(std::move(values)));
      RETURN_IF_FAIL(output.GetStatus());
    }
    if (output.TotalRows() == capacity) {
      break;
    }
  }
  if (runtime != nullptr) {
    runtime->scan_rows += visited;
    runtime->scan_output_rows += output.TotalRows();
    runtime->aggregate_input_rows += output.TotalRows();
    runtime->aggregate_groups += output.TotalRows();
    runtime->aggregate_updates += output.TotalRows() * plan.aggregates.size();
    runtime->aggregate_ms += ElapsedMs(started);
  }
  output.schema = Schema("", std::move(columns));
  RETURN_IF_FAIL(output.FinishSpill());
  return std::optional<Relation>(std::move(output));
} catch (const std::exception& error) {
  return StatusError(StatusCode::kRuntimeError, error.what());
} catch (...) {
  return StatusError(StatusCode::kRuntimeError, "singleton aggregate failed");
}

namespace {

// Packed dictionary pairs are unsigned integers. Stable byte passes avoid
// comparison sorting for large inputs; constant bytes need no pass.
void SortSparseDistinctPairs(std::vector<uint64_t>& pairs, unsigned key_bits) {
  if (pairs.size() < 2048) {
    std::ranges::sort(pairs);
    return;
  }
  const unsigned bytes = (key_bits + 7) / 8;
  std::array<std::array<size_t, 256>, 8> histograms{};
  uint64_t varying = 0;
  // All radix histograms share one input traversal. Scattering preserves the
  // value multiset, so later passes can reuse these exact byte frequencies.
  for (const uint64_t pair : pairs) {
    varying |= pair ^ pairs.front();
    for (unsigned byte = 0; byte < bytes; ++byte) {
      ++histograms[byte][(pair >> (byte * 8)) & 0xffU];
    }
  }
  std::vector<uint64_t> scratch(pairs.size());
  for (unsigned byte = 0; byte < bytes; ++byte) {
    const unsigned shift = byte * 8;
    if (((varying >> shift) & 0xffU) == 0) {
      continue;
    }
    auto& offsets = histograms[byte];
    size_t position = 0;
    for (size_t& offset : offsets) {
      const size_t count = offset;
      offset = position;
      position += count;
    }
    for (const uint64_t pair : pairs) {
      scratch[offsets[(pair >> shift) & 0xffU]++] = pair;
    }
    pairs.swap(scratch);
  }
}

// Exact whole-column bounds cover every filtered subset, group, prefix and
// worker subtotal. AVG also requires exact addition of promoted DOUBLE inputs.
// Keep NULLs, unsigned values and unproved arithmetic on the ordinary path.
const int64_t* BoundedIntegerAggregateColumn(
    const LSTableStore& store, const TypedAggregateInput& input,
    const std::vector<slot_t>& projection) {
  const auto type = input.aggregate->GetType();
  if (!input.column || input.aggregate->Distinct() ||
      (type != AggregationType::kSum && type != AggregationType::kAvg)) {
    return nullptr;
  }
  const slot_t column =
      projection.empty() ? *input.column : projection[*input.column];
  if (store.blocks.empty() || store.total_rows == 0 ||
      store.blocks.front().columns[column].type != ValueType::kInt64 ||
      store.blocks.front().columns[column].is_unsigned ||
      store.column_has_null[column] != 0 ||
      store.IntegerColumn(column).size() != store.total_rows ||
      store.column_min_int[column] > store.column_max_int[column]) {
    return nullptr;
  }
  const platform::Int128 lo = store.column_min_int[column];
  const platform::Int128 hi = store.column_max_int[column];
  const platform::Int128 maximum =
      std::max(lo < 0 ? -lo : lo, hi < 0 ? -hi : hi);
  const platform::Int128 limit =
      type == AggregationType::kAvg
          ? static_cast<platform::Int128>(int64_t{1} << 53)
          : static_cast<platform::Int128>(INT64_MAX);
  if (maximum > limit / store.total_rows) {
    return nullptr;
  }
  return store.IntegerColumn(column).data();
}

}  // namespace

// Give each fixed-width key exactly one partition owner. Workers aggregate
// original-order row IDs, then retain local TopN candidates. No global hash
// table or serial merge of millions of partial groups is needed.
StatusOr<std::optional<Relation>> TryPartitionedNumericTopN(
    const LSTableStore& store, const TypedAggregationPlan& plan,
    const SelectStatement& statement, const std::vector<slot_t>& projection,
    const Schema& schema, ExecutionRuntime* runtime) try {
  if (HasPostAggItem(plan)) {
    return std::optional<Relation>{};
  }
  if (std::getenv("TINYLAMB_DISABLE_PARTITIONED_NUMERIC_AGG") != nullptr ||
      plan.key_offsets.empty() || !plan.dependent_key_programs.empty() ||
      !plan.filter_programs.empty() || !plan.filter_disjunctions.empty() ||
      statement.Having() || statement.Distinct() || !statement.HasLimit() ||
      statement.WithTies() ||
      statement.Offset() > SIZE_MAX - statement.Limit() ||
      store.total_rows == 0 || store.total_rows > SIZE_MAX / 256) {
    return std::optional<Relation>{};
  }
  std::vector<const int64_t*> keys;
  keys.reserve(plan.key_offsets.size());
  for (const auto offset : plan.key_offsets) {
    const auto& column_schema = schema.GetColumn(offset);
    const slot_t column = projection.empty() ? offset : projection[offset];
    if ((column_schema.Type() != ValueType::kInt64 &&
         column_schema.Type() != ValueType::kDate) ||
        column_schema.IsUnsigned() || store.column_has_null[column] != 0U ||
        store.IntegerColumn(column).size() != store.total_rows) {
      return std::optional<Relation>{};
    }
    keys.push_back(store.IntegerColumn(column).data());
  }
  std::vector<const int64_t*> inputs;
  inputs.reserve(plan.aggregates.size());
  std::vector<size_t> slots(plan.aggregates.size(), SIZE_MAX);
  for (size_t a = 0; a < plan.aggregates.size(); ++a) {
    const auto& input = plan.aggregates[a];
    if (IsCountStar(*input.aggregate) && !input.aggregate->Distinct()) {
      continue;
    }
    const auto* column =
        BoundedIntegerAggregateColumn(store, input, projection);
    if (column == nullptr) {
      return std::optional<Relation>{};
    }
    slots[a] = inputs.size();
    inputs.push_back(column);
  }
  QueryMemoryCharge filter_charge;
  std::vector<EncodedDictionaryPredicate> filters;
  filters.reserve(plan.filter_compares.size());
  for (const auto& filter : plan.filter_compares) {
    auto compiled =
        CompileDictionaryPredicate(store, filter, projection, filter_charge);
    if (!compiled) {
      return std::optional<Relation>{};
    }
    filters.push_back(std::move(*compiled));
  }
  for (const auto& list : plan.inlist_compares) {
    auto compiled =
        CompileDictionaryInList(store, list, projection, filter_charge);
    if (!compiled) {
      return std::optional<Relation>{};
    }
    filters.push_back(std::move(*compiled));
  }
  OrderDictionaryPredicates(filters);
  const size_t rows = store.total_rows;
  const size_t workers = ParallelWorkerCount(std::max<size_t>(1, rows / 8192));
  const size_t partitions = workers == 1 ? 1 : std::bit_ceil(workers * 4);
  const auto partition_bits =
      static_cast<unsigned>(std::countr_zero(partitions));
  const size_t capacity =
      std::min(rows, statement.Offset() + statement.Limit());
  // Bounds include row/hash staging, rounded hash slots, every group state,
  // and worst-case retained payloads across workers. Skew can put all rows
  // in one partition; do not budget as though partitions were balanced.
  size_t state_bytes = 0;
  size_t bytes_per_row = 0;
  size_t bytes = 0;
  if (__builtin_mul_overflow(inputs.size(), 3 * sizeof(int64_t),
                             &state_bytes) ||
      __builtin_add_overflow(size_t{256}, state_bytes, &bytes_per_row) ||
      __builtin_mul_overflow(rows, bytes_per_row, &bytes) ||
      !QueryMemoryBudget::Global().CanReserve(bytes)) {
    return std::optional<Relation>{};
  }
  QueryMemoryCharge charge(bytes);
  const auto started = std::chrono::steady_clock::now();
  auto phase_start = started;
  const bool trace = std::getenv("TINYLAMB_GROUP_PHASES") != nullptr;
  const auto phase = [&](const char* name) {
    if (trace) {
      std::cerr << "[partitioned-numeric] " << name
                << " ms=" << ElapsedMs(phase_start) << " workers=" << workers
                << " partitions=" << partitions << '\n';
      phase_start = std::chrono::steady_clock::now();
    }
  };
  const auto mix = [](uint64_t value) {
    value ^= value >> 30;
    value *= 0xbf58476d1ce4e5b9ULL;
    value ^= value >> 27;
    value *= 0x94d049bb133111ebULL;
    return value ^ (value >> 31);
  };
  std::vector<uint64_t> hashes(rows);
  std::vector<uint8_t> passing(rows, 0);
  std::vector<size_t> positions(rows);
  std::vector<std::vector<size_t>> counts(workers,
                                          std::vector<size_t>(partitions, 0));
  RETURN_IF_FAIL(RunParallelTasks(workers, [&](size_t worker) {
    const size_t begin =
        ((rows / workers) * worker) + std::min(rows % workers, worker);
    const size_t end = ((rows / workers) * (worker + 1)) +
                       std::min(rows % workers, worker + 1);
    for (size_t row = begin; row < end; ++row) {
      if (!std::ranges::all_of(filters, [&](const auto& filter) {
            return filter.Matches(row);
          })) {
        continue;
      }
      passing[row] = 1;
      uint64_t hash = 0x9e3779b97f4a7c15ULL;
      for (const auto* key : keys) {
        hash = mix(hash ^ static_cast<uint64_t>(key[row]));
      }
      hashes[row] = hash;
      ++counts[worker][hash & (partitions - 1)];
    }
  }));
  std::vector<size_t> boundaries(partitions + 1, 0);
  for (size_t p = 0; p < partitions; ++p) {
    size_t next = boundaries[p];
    for (auto& count : counts) {
      const size_t size = count[p];
      count[p] = next;
      next += size;
    }
    boundaries[p + 1] = next;
  }
  RETURN_IF_FAIL(RunParallelTasks(workers, [&](size_t worker) {
    const size_t begin =
        ((rows / workers) * worker) + std::min(rows % workers, worker);
    const size_t end = ((rows / workers) * (worker + 1)) +
                       std::min(rows % workers, worker + 1);
    for (size_t row = begin; row < end; ++row) {
      if (passing[row]) {
        positions[counts[worker][hashes[row] & (partitions - 1)]++] = row;
      }
    }
  }));
  phase("partition");
  struct Group {
    size_t first;
    int64_t count;
  };
  struct Candidate {
    Group group;
    std::vector<int64_t> sums;
  };
  const auto precedes = [&](const Group& left, const int64_t* left_sums,
                            const Group& right, const int64_t* right_sums) {
    for (size_t i = 0; i < plan.order_items.size(); ++i) {
      const auto& item = plan.order_items[i];
      if (item.constant) {
        continue;
      }
      int comparison = 0;
      if (item.is_key) {
        const auto a = keys[item.index][left.first];
        const auto b = keys[item.index][right.first];
        comparison = (a > b) - (a < b);
      } else if (slots[item.index] == SIZE_MAX) {
        comparison = (left.count > right.count) - (left.count < right.count);
      } else {
        const auto a = left_sums[slots[item.index]];
        const auto b = right_sums[slots[item.index]];
        if (plan.aggregates[item.index].aggregate->GetType() ==
            AggregationType::kAvg) {
          const double x =
              static_cast<double>(a) / static_cast<double>(left.count);
          const double y =
              static_cast<double>(b) / static_cast<double>(right.count);
          comparison = (x > y) - (x < y);
        } else {
          comparison = (a > b) - (a < b);
        }
      }
      if (comparison != 0) {
        return plan.order_ascending[i] ? comparison < 0 : comparison > 0;
      }
    }
    return left.first < right.first;
  };
  const auto candidate_precedes = [&](const Candidate& a, const Candidate& b) {
    return precedes(a.group, a.sums.data(), b.group, b.sums.data());
  };
  std::vector<std::vector<Candidate>> winners(workers);
  std::vector<size_t> group_counts(workers, 0);
  RETURN_IF_FAIL(RunParallelTasks(workers, [&](size_t worker) {
    auto& heap = winners[worker];
    // Across partitions, a worker can encounter no more than all input rows.
    heap.reserve(std::min(capacity, rows / workers));
    for (size_t p = worker; p < partitions; p += workers) {
      const size_t size = boundaries[p + 1] - boundaries[p];
      if (size == 0) {
        continue;
      }
      const size_t hash_capacity = std::bit_ceil(size * 2);
      std::vector<size_t> index(hash_capacity, SIZE_MAX);
      std::vector<Group> groups;
      std::vector<int64_t> sums;
      groups.reserve(size);
      sums.reserve(size * inputs.size());
      for (size_t pos = boundaries[p]; pos < boundaries[p + 1]; ++pos) {
        const size_t row = positions[pos];
        size_t bucket = (hashes[row] >> partition_bits) & (hash_capacity - 1);
        while (index[bucket] != SIZE_MAX) {
          const size_t first = groups[index[bucket]].first;
          if (hashes[row] == hashes[first] &&
              std::ranges::all_of(keys, [&](const auto* key) {
                return key[row] == key[first];
              })) {
            break;
          }
          bucket = (bucket + 1) & (hash_capacity - 1);
        }
        if (index[bucket] == SIZE_MAX) {
          index[bucket] = groups.size();
          groups.push_back({row, 0});
          sums.resize(sums.size() + inputs.size(), 0);
        }
        const size_t group = index[bucket];
        ++groups[group].count;
        for (size_t a = 0; a < inputs.size(); ++a) {
          sums[(group * inputs.size()) + a] += inputs[a][row];
        }
      }
      group_counts[worker] += groups.size();
      for (size_t group = 0; group < groups.size(); ++group) {
        if (capacity == 0) {
          continue;
        }
        const auto* values =
            inputs.empty() ? nullptr : &sums[group * inputs.size()];
        if (heap.size() == capacity &&
            !precedes(groups[group], values, heap.front().group,
                      heap.front().sums.data())) {
          continue;
        }
        Candidate candidate{groups[group], {}};
        if (!inputs.empty()) {
          candidate.sums.assign(values, values + inputs.size());
        }
        if (heap.size() == capacity) {
          std::pop_heap(heap.begin(), heap.end(), candidate_precedes);
          heap.back() = std::move(candidate);
        } else {
          heap.push_back(std::move(candidate));
        }
        std::push_heap(heap.begin(), heap.end(), candidate_precedes);
      }
    }
  }));
  phase("aggregate-and-local-topn");
  std::vector<Candidate> retained;
  size_t candidates = 0;
  for (const auto& heap : winners) {
    candidates += heap.size();
  }
  retained.reserve(std::min(capacity, candidates));
  for (auto& heap : winners) {
    for (auto& candidate : heap) {
      if (retained.size() == capacity) {
        if (!candidate_precedes(candidate, retained.front())) {
          continue;
        }
        std::pop_heap(retained.begin(), retained.end(), candidate_precedes);
        retained.back() = std::move(candidate);
      } else {
        retained.push_back(std::move(candidate));
      }
      std::push_heap(retained.begin(), retained.end(), candidate_precedes);
    }
  }
  std::ranges::sort(retained, candidate_precedes);
  Relation output(runtime);
  std::vector<Column> columns;
  columns.reserve(statement.SelectList().size());
  for (size_t i = 0; i < statement.SelectList().size(); ++i) {
    columns.emplace_back(ProjectionName(statement.SelectList()[i], i),
                         ValueType::kNull);
  }
  for (const auto& candidate : retained) {
    std::vector<Value> values;
    values.reserve(plan.emit_items.size());
    for (const auto& item : plan.emit_items) {
      if (item.constant) {
        values.push_back(*item.constant);
      } else if (item.is_key) {
        const int64_t key = keys[item.index][candidate.group.first];
        values.push_back(
            schema.GetColumn(plan.key_offsets[item.index]).Type() ==
                    ValueType::kDate
                ? Value::DateFromDays(key)
                : Value(key));
      } else if (slots[item.index] == SIZE_MAX) {
        values.emplace_back(candidate.group.count);
      } else {
        const int64_t sum = candidate.sums[slots[item.index]];
        values.push_back(plan.aggregates[item.index].aggregate->GetType() ==
                                 AggregationType::kAvg
                             ? Value(static_cast<double>(sum) /
                                     static_cast<double>(candidate.group.count))
                             : Value(sum));
      }
    }
    if (output.TotalRows() == 0) {
      for (size_t i = 0; i < columns.size(); ++i) {
        columns[i] = Column(columns[i].Name(), ValueTypeOf(values[i]));
        columns[i].SetUnsigned(values[i].IsUnsigned());
      }
    }
    output.AddRow(Row(std::move(values)));
  }
  RETURN_IF_FAIL(output.GetStatus());
  output.schema = Schema("", std::move(columns));
  phase("finish");
  if (runtime != nullptr) {
    const size_t groups =
        std::accumulate(group_counts.begin(), group_counts.end(), size_t{0});
    runtime->scan_rows += rows;
    runtime->scan_output_rows += boundaries.back();
    ++runtime->typed_parallel_aggregations;
    ++runtime->partitioned_numeric_aggregations;
    runtime->aggregate_input_rows += boundaries.back();
    runtime->aggregate_updates += boundaries.back() * plan.aggregates.size();
    runtime->aggregate_groups += groups;
    ++runtime->topn_finish_uses;
    runtime->topn_finish_capacity = capacity;
    runtime->topn_finish_input_rows = groups;
    runtime->aggregate_ms += ElapsedMs(started);
  }
  return std::optional<Relation>(std::move(output));
} catch (const std::exception& error) {
  return StatusError(StatusCode::kRuntimeError, error.what());
} catch (...) {
  return StatusError(StatusCode::kRuntimeError,
                     "partitioned numeric aggregate failed");
}

namespace {

// Dense-indexed accumulator that allocates storage lazily in fixed pages on
// first touch. Workers only pay for the domain pages they actually observe:
// previously every worker `resize`d `domain`-sized counters and sums (and a
// whole `domain` bitmap per worker in some shapes), so with a million-entry
// dictionary and several workers the worker pass spent hundreds of MB of
// zero-fill and first-touch page faults even when each worker only saw a
// thin slice of the codes. One indirection through the page table replaces
// that bulk memset while keeping per-row access O(1).
template <typename T>
class PagedDomainAccumulator {
 public:
  static constexpr size_t kPageBits = 8;
  static constexpr size_t kPageSize = size_t{1} << kPageBits;
  static constexpr size_t kPageMask = kPageSize - 1;

  void Init(size_t cells) {
    const size_t pages = (cells + kPageMask) >> kPageBits;
    page_of_.assign(pages, 0);
    values_.clear();
  }
  T& At(size_t index) {
    const size_t page = index >> kPageBits;
    uint32_t& mapped = page_of_[page];
    if (mapped == 0) {
      mapped = static_cast<uint32_t>(values_.size() >> kPageBits) + 1;
      values_.resize(values_.size() + kPageSize);
    }
    return values_[(static_cast<size_t>(mapped - 1) << kPageBits) |
                   (index & kPageMask)];
  }
  // Dense page containing `index`, or nullptr when it was never touched.
  [[nodiscard]] const T* Page(size_t index) const {
    const uint32_t mapped = page_of_[index >> kPageBits];
    return mapped == 0
               ? nullptr
               : &values_[(static_cast<size_t>(mapped - 1) << kPageBits)];
  }

 private:
  std::vector<T> values_;
  std::vector<uint32_t> page_of_;
};

}  // namespace

// A shared dictionary with SQL-distinct entries is already a perfect group
// index. Count by code, merge counters, and decode only output groups.
// A LIMIT is optional: derived-table/CTE producers need all complete groups.
// The caller must first establish the store's snapshot and cache validity.
StatusOr<std::optional<Relation>> TryDictionaryAggregate(
    const LSTableStore& store, const TypedAggregationPlan& plan,
    const SelectStatement& statement, const std::vector<slot_t>& projection,
    const Schema& schema, ExecutionRuntime* runtime,
    const std::function<StatusOr<Value>(size_t)>* transform_key,
    const UtcTimestampTruncUnit* temporal_unit) try {
  if (HasPostAggItem(plan)) {
    return std::optional<Relation>{};
  }
  if ((!statement.HasLimit() &&
       std::getenv("TINYLAMB_DISABLE_UNBOUNDED_DICTIONARY_AGG") != nullptr) ||
      plan.key_offsets.size() != 1 || !plan.filter_programs.empty() ||
      !plan.null_filters.empty() || !plan.filter_disjunctions.empty() ||
      statement.Distinct() || statement.WithTies() ||
      (statement.HasLimit() &&
       statement.Offset() > SIZE_MAX - statement.Limit()) ||
      store.total_rows > static_cast<uint64_t>(INT64_MAX) ||
      !std::ranges::all_of(plan.filter_compares, [](const auto& compare) {
        return compare.constant_payload;
      })) {
    return std::optional<Relation>{};
  }
  const auto column_at = [&](slot_t offset) {
    return projection.empty() ? offset : projection[offset];
  };
  const slot_t key_column = column_at(plan.key_offsets.front());
  const auto& dictionary = store.dictionaries[key_column];
  const bool numeric_dictionary = dictionary.value_type == ValueType::kInt64 ||
                                  dictionary.value_type == ValueType::kDate;
  if (schema.GetColumn(plan.key_offsets.front()).IsUnsigned() ||
      (!numeric_dictionary && dictionary.value_type != ValueType::kVarChar) ||
      (!dictionary.active && dictionary.wide_code_bytes != 2 &&
       dictionary.wide_code_bytes != 4) ||
      dictionary.DictionaryCodeCount() != store.total_rows ||
      (!numeric_dictionary && transform_key == nullptr &&
       !dictionary.bytewise_equality)) {
    return std::optional<Relation>{};
  }
  // An aggregate of its own group key is constant within the group. Fold
  // frequencies instead of decoding its argument once for every input row.
  // SUM/AVG require a conservative bound before any output is published:
  // integer overflow and floating accumulation semantics must stay intact.
  std::vector<bool> key_arguments(plan.aggregates.size(), false);
  const LSTableColumnDictionary* distinct_dictionary = nullptr;
  std::vector<const int64_t*> numeric_inputs;
  numeric_inputs.reserve(plan.aggregates.size());
  std::vector<size_t> numeric_slots(plan.aggregates.size(), SIZE_MAX);
  for (size_t i = 0; i < plan.aggregates.size(); ++i) {
    const auto& input = plan.aggregates[i];
    if (IsCountStar(*input.aggregate) && !input.aggregate->Distinct()) {
      continue;
    }
    const auto type = input.aggregate->GetType();
    if (transform_key == nullptr && input.column &&
        *input.column == plan.key_offsets.front() && input.aggregate->Child() &&
        input.aggregate->Child()->Type() == TypeTag::kColumnValue &&
        !input.aggregate->Distinct() &&
        (type == AggregationType::kCount || type == AggregationType::kMin ||
         type == AggregationType::kMax || type == AggregationType::kSum ||
         type == AggregationType::kAvg)) {
      if (type == AggregationType::kSum || type == AggregationType::kAvg) {
        if (dictionary.value_type != ValueType::kInt64) {
          return std::optional<Relation>{};
        }
        const platform::Int128 bound = type == AggregationType::kAvg
                                           ? (platform::Int128{1} << 53)
                                           : platform::Int128{INT64_MAX};
        for (int64_t value : dictionary.IntEntries()) {
          const platform::Int128 magnitude =
              value < 0 ? -platform::Int128{value} : value;
          if (magnitude * store.total_rows > bound) {
            return std::optional<Relation>{};
          }
        }
      }
      key_arguments[i] = true;
      continue;
    }
    if (transform_key == nullptr && input.column &&
        !input.aggregate->Distinct() &&
        (type == AggregationType::kSum || type == AggregationType::kAvg)) {
      const int64_t* column =
          BoundedIntegerAggregateColumn(store, input, projection);
      if (column == nullptr) {
        return std::optional<Relation>{};
      }
      numeric_slots[i] = numeric_inputs.size();
      numeric_inputs.push_back(column);
      continue;
    }
    // One shared bitmap can serve repeated COUNT(DISTINCT column) terms.
    // Keep casts/expressions on the normal path: dictionary-code equality
    // need not equal equality after evaluating a cast.
    if (transform_key != nullptr || !input.column ||
        input.aggregate->GetType() != AggregationType::kCount ||
        !input.aggregate->Distinct() || !input.aggregate->Child() ||
        input.aggregate->Child()->Type() != TypeTag::kColumnValue) {
      return std::optional<Relation>{};
    }
    const auto& argument = store.dictionaries[column_at(*input.column)];
    if ((distinct_dictionary != nullptr && distinct_dictionary != &argument) ||
        (argument.value_type != ValueType::kInt64 &&
         argument.value_type != ValueType::kDate) ||
        (!argument.active && argument.wide_code_bytes != 2 &&
         argument.wide_code_bytes != 4) ||
        argument.DictionaryCodeCount() != store.total_rows) {
      return std::optional<Relation>{};
    }
    distinct_dictionary = &argument;
  }
  const size_t dependent_count = plan.dependent_key_programs.size();
  // The planner compiles dependent expressions against the plain signed
  // integer keys. Transformed/string/date dictionaries keep their old path.
  if (dependent_count != 0 && (dictionary.value_type != ValueType::kInt64 ||
                               transform_key != nullptr)) {
    return std::optional<Relation>{};
  }
  const size_t domain = (numeric_dictionary ? dictionary.IntEntryCount()
                                            : dictionary.StringEntryCount()) +
                        1;
  const size_t null_code = domain - 1;
  if (domain >= 32768 && transform_key == nullptr && dependent_count == 0 &&
      distinct_dictionary == nullptr && numeric_inputs.empty() &&
      !statement.Having() &&
      (!plan.filter_compares.empty() || !plan.inlist_compares.empty())) {
    ASSIGN_OR_RETURN(std::optional<Relation>, selective,
                     TryCompositeDictionaryAggregate(
                         store, plan, statement, projection, schema, runtime,
                         nullptr, nullptr, nullptr, true));
    if (selective) {
      return selective;
    }
  }
  const bool summarized =
      dictionary.code_counts.size() == domain &&
      dictionary.code_first_row.size() == domain &&
      distinct_dictionary == nullptr && numeric_inputs.empty() &&
      plan.inlist_compares.empty() &&
      std::ranges::all_of(plan.filter_compares, [&](const auto& compare) {
        return column_at(compare.left) == key_column &&
               (compare.string_payload ||
                (numeric_dictionary && !compare.is_double));
      });
  const size_t workers =
      summarized ? 1 : ParallelWorkerCount(store.blocks.size());
  // Two counters per worker/code, the result permutation and predicate mask.
  size_t bytes_per_code =
      (workers * 2 * sizeof(uint64_t)) + sizeof(size_t) + sizeof(uint8_t) +
      (transform_key == nullptr ? 0 : (sizeof(Value) * 2) + 128);
  size_t dependent_bytes_per_code = 0;
  size_t numeric_bytes_per_code = 0;
  if (__builtin_mul_overflow(dependent_count, sizeof(Value),
                             &dependent_bytes_per_code) ||
      __builtin_add_overflow(bytes_per_code, dependent_bytes_per_code,
                             &bytes_per_code) ||
      __builtin_mul_overflow(numeric_inputs.size(), workers * sizeof(int64_t),
                             &numeric_bytes_per_code) ||
      __builtin_add_overflow(bytes_per_code, numeric_bytes_per_code,
                             &bytes_per_code)) {
    return std::optional<Relation>{};
  }
  if (store.blocks.size() > SIZE_MAX / sizeof(uint64_t) - 1 ||
      domain > (SIZE_MAX - ((store.blocks.size() + 1) * sizeof(uint64_t))) /
                   bytes_per_code) {
    return std::optional<Relation>{};
  }
  size_t bytes = (domain * bytes_per_code) +
                 ((store.blocks.size() + 1) * sizeof(uint64_t));
  const size_t distinct_domain =
      distinct_dictionary == nullptr ? 0 : distinct_dictionary->IntEntryCount();
  const auto distinct_code_bits = static_cast<unsigned>(
      distinct_domain == 0 ? 0 : std::bit_width(distinct_domain - 1));
  // The product is an exact domain bound, not an NDV estimate. If the bitmap
  // would be too large, retain only observed (group, argument) code pairs.
  const size_t words_per_group =
      (distinct_domain / 64) + (distinct_domain % 64 != 0 ? 1 : 0);
  size_t bitmap_words = 0;
  size_t bitmap_bytes = 0;
  bool sparse_distinct = false;
  size_t pair_capacity = 0;
  if (__builtin_mul_overflow(domain, words_per_group, &bitmap_words) ||
      __builtin_mul_overflow(bitmap_words, workers * sizeof(uint64_t),
                             &bitmap_bytes) ||
      bitmap_bytes > size_t{64} * 1024 * 1024) {
    if (null_code > UINT32_MAX || distinct_domain > UINT32_MAX) {
      return std::optional<Relation>{};
    }
    // Bound resident pairs, not input cardinality. Full buffers become sorted
    // unique runs; binary external merges retain logarithmically many files.
    // Reserve input plus both merge buffers (three pairs/row) and fixed
    // merge I/O buffers. Smaller query budgets retain external union.
    const size_t io_bytes = workers * (size_t{128} << 10);
    const size_t available = std::min<size_t>(
        size_t{256} << 20, QueryMemoryBudget::Global().Remaining() / 2);
    if (available <= io_bytes + (workers * 2 * sizeof(uint64_t))) {
      return std::optional<Relation>{};
    }
    bitmap_bytes = available;
    pair_capacity = (available - io_bytes) / (workers * 3 * sizeof(uint64_t));
    bitmap_words = 0;
    sparse_distinct = true;
  }
  if (__builtin_add_overflow(bytes, bitmap_bytes, &bytes) ||
      (distinct_dictionary != nullptr &&
       (__builtin_mul_overflow(domain, sizeof(uint64_t), &bitmap_bytes) ||
        __builtin_add_overflow(bytes, bitmap_bytes, &bytes)))) {
    return std::optional<Relation>{};
  }
  if (!QueryMemoryBudget::Global().CanReserve(bytes)) {
    return std::optional<Relation>{};
  }
  QueryMemoryCharge charge(bytes);
  const auto started = std::chrono::steady_clock::now();
  const auto matches = [](auto left, auto right, BinaryOperation op) {
    switch (op) {
      case BinaryOperation::kEquals:
        return left == right;
      case BinaryOperation::kNotEquals:
        return left != right;
      case BinaryOperation::kLessThan:
        return left < right;
      case BinaryOperation::kLessThanEquals:
        return left <= right;
      case BinaryOperation::kGreaterThan:
        return left > right;
      default:
        return left >= right;
    }
  };
  std::vector<uint8_t> allowed(domain, 1);
  std::vector<TypedFilterCompare> filters;
  filters.reserve(plan.filter_compares.size());
  for (const auto& compare : plan.filter_compares) {
    if (compare.string_payload && column_at(compare.left) == key_column) {
      for (size_t code = 0; code < null_code; ++code) {
        allowed[code] &= static_cast<uint8_t>(
            MatchTypedStringCompare(dictionary.entries[code], compare));
      }
      allowed[null_code] = 0;
    } else if (numeric_dictionary && !compare.string_payload &&
               !compare.is_double && column_at(compare.left) == key_column) {
      const auto int_entries = dictionary.IntEntries();
      for (size_t code = 0; code < null_code; ++code) {
        allowed[code] &= static_cast<uint8_t>(
            matches(int_entries[code], compare.int_constant, compare.op));
      }
      allowed[null_code] = 0;
    } else {
      filters.push_back(compare);
      filters.back().left = column_at(compare.left);
    }
  }
  std::vector<EncodedDictionaryPredicate> encoded_filters;
  encoded_filters.reserve(filters.size());
  bool all_encoded = true;
  for (const auto& compare : filters) {
    auto encoded = CompileDictionaryPredicate(store, compare, {}, charge);
    if (!encoded) {
      all_encoded = false;
      break;
    }
    encoded_filters.push_back(std::move(*encoded));
  }
  for (const auto& list : plan.inlist_compares) {
    auto encoded = CompileDictionaryInList(store, list, projection, charge);
    if (!all_encoded || !encoded) {
      return std::optional<Relation>{};
    }
    encoded_filters.push_back(std::move(*encoded));
  }
  OrderDictionaryPredicates(encoded_filters);
  std::vector<uint64_t> row_base(store.blocks.size() + 1, 0);
  for (size_t block = 0; block < store.blocks.size(); ++block) {
    row_base[block + 1] = row_base[block] + store.blocks[block].row_count;
  }
  // count and first are read together for every eligible row. Keeping them
  // in one array halves the random accesses (one cache/TLB miss instead of
  // two) on large dictionary domains.
  struct GroupSlot {
    uint64_t count{0};
    uint64_t first{UINT64_MAX};
  };
  struct Counts {
    PagedDomainAccumulator<GroupSlot> slots;
    std::vector<uint64_t> distinct;
    std::vector<uint64_t> distinct_pairs;
    SortedDistinctRuns runs;
    Status error{Status::kSuccess};
    PagedDomainAccumulator<int64_t> sums;
  };
  std::vector<Counts> local(workers);
  const auto execute_worker = [&]<bool Sparse>(size_t worker) {
    auto& state = local[worker];
    // Paged accumulators only materialize the 4 KiB pages this worker
    // actually touches, instead of zero-filling the whole domain per worker.
    state.slots.Init(domain);

    state.distinct.resize(bitmap_words, 0);
    if constexpr (Sparse) {
      const size_t begin =
          all_encoded ? store.total_rows * worker / workers
                      : row_base[store.blocks.size() * worker / workers];
      const size_t end =
          all_encoded ? store.total_rows * (worker + 1) / workers
                      : row_base[store.blocks.size() * (worker + 1) / workers];
      state.distinct_pairs.reserve(std::min(end - begin, pair_capacity));
    }
    state.sums.Init(numeric_inputs.size() * domain);
    if (summarized) {
      for (size_t code = 0; code < domain; ++code) {
        if (allowed[code]) {
          GroupSlot& slot = state.slots.At(code);
          slot.count = dictionary.code_counts[code];
          slot.first = dictionary.code_first_row[code];
        }
      }
      return;
    }
    const auto observe_arguments = [&](size_t group, size_t row) {
      if (distinct_dictionary != nullptr) {
        const size_t code = ReadDictionaryCode(*distinct_dictionary, row);
        // NULL is excluded from COUNT(DISTINCT), but the group itself still
        // exists, even when all of its argument values are NULL.
        if (code < distinct_domain) {
          if constexpr (Sparse) {
            if (!state.error.ok()) {
              return;
            }
            const uint64_t pair =
                (static_cast<uint64_t>(group) << distinct_code_bits) |
                static_cast<uint64_t>(code);
            if (state.distinct_pairs.empty() ||
                state.distinct_pairs.back() != pair) {
              if (state.distinct_pairs.size() == pair_capacity) {
                SortSparseDistinctPairs(
                    state.distinct_pairs,
                    static_cast<unsigned>(std::bit_width(null_code)) +
                        distinct_code_bits);
                const auto duplicates =
                    std::ranges::unique(state.distinct_pairs);
                state.distinct_pairs.erase(duplicates.begin(),
                                           duplicates.end());
                state.error = state.runs.Add(state.distinct_pairs);
                state.distinct_pairs.clear();
                if (!state.error.ok()) {
                  return;
                }
              }
              state.distinct_pairs.push_back(pair);
            }
          } else {
            state.distinct[(group * words_per_group) + (code / 64)] |=
                uint64_t{1} << (code % 64);
          }
        }
      }
      for (size_t i = 0; i < numeric_inputs.size(); ++i) {
        state.sums.At((i * domain) + group) += numeric_inputs[i][row];
      }
    };
    if (all_encoded) {
      // Evaluate predicates by code and traverse contiguous compressed columns
      // without chasing the metadata of every row-storage page.
      const size_t rows = dictionary.DictionaryCodeCount();
      for (size_t row = rows * worker / workers;
           row < rows * (worker + 1) / workers; ++row) {
        if (!std::ranges::all_of(encoded_filters, [&](const auto& filter) {
              return filter.Matches(row);
            })) {
          continue;
        }
        const size_t code =
            std::min<size_t>(ReadDictionaryCode(dictionary, row), null_code);
        if (allowed[code] != 0) {
          GroupSlot& slot = state.slots.At(code);
          if (slot.count++ == 0) {
            slot.first = row;
          }
          observe_arguments(code, row);
        }
      }
      if constexpr (Sparse) {
        SortSparseDistinctPairs(
            state.distinct_pairs,
            static_cast<unsigned>(std::bit_width(null_code)) +
                distinct_code_bits);
        const auto duplicates = std::ranges::unique(state.distinct_pairs);
        state.distinct_pairs.erase(duplicates.begin(), duplicates.end());
      }
      return;
    }
    std::vector<const TypedFilterCompare*> remaining;
    remaining.reserve(filters.size());
    for (size_t b = store.blocks.size() * worker / workers;
         b < store.blocks.size() * (worker + 1) / workers; ++b) {
      const auto& block = store.blocks[b];
      const auto& key = block.columns[key_column];
      remaining.clear();
      for (const auto& filter : filters) {
        const auto& column = block.columns[filter.left];
        // Integer zone maps prove an entire block passes. For <> the two
        // endpoints alone do not exclude a matching value in the interior.
        bool all_pass = false;
        if (!filter.string_payload && !filter.is_double && !column.has_null &&
            column.has_min_max) {
          all_pass =
              filter.op == BinaryOperation::kNotEquals
                  ? (filter.int_constant < column.min_int ||
                     filter.int_constant > column.max_int)
                  : (matches(column.min_int, filter.int_constant, filter.op) &&
                     matches(column.max_int, filter.int_constant, filter.op));
        }
        if (!all_pass) {
          remaining.push_back(&filter);
        }
      }
      for (size_t r = 0; r < block.row_count; ++r) {
        const size_t code = key.IsNull(r) ? null_code : key.DictionaryCodeAt(r);
        if (allowed[code] == 0) {
          continue;
        }
        bool pass = true;
        for (const auto* filter : remaining) {
          const auto& column = block.columns[filter->left];
          if (column.IsNull(r)) {
            pass = false;
          } else if (filter->string_payload) {
            pass = MatchTypedStringCompare(column.VarcharAt(r), *filter);
          } else if (filter->is_double) {
            pass = matches(std::bit_cast<double>(column.int_cells[r]),
                           filter->double_constant, filter->op);
          } else {
            pass =
                matches(column.int_cells[r], filter->int_constant, filter->op);
          }
          if (!pass) {
            break;
          }
        }
        if (pass) {
          GroupSlot& slot = state.slots.At(code);
          if (slot.count++ == 0) {
            slot.first = row_base[b] + r;
          }
          observe_arguments(code, row_base[b] + r);
        }
      }
    }
    if constexpr (Sparse) {
      SortSparseDistinctPairs(state.distinct_pairs,
                              static_cast<unsigned>(std::bit_width(null_code)) +
                                  distinct_code_bits);
      const auto duplicates = std::ranges::unique(state.distinct_pairs);
      state.distinct_pairs.erase(duplicates.begin(), duplicates.end());
    }
  };
  RETURN_IF_FAIL(RunParallelTasks(workers, [&](size_t worker) {
    if (sparse_distinct) {
      execute_worker.template operator()<true>(worker);
    } else {
      execute_worker.template operator()<false>(worker);
    }
  }));
  for (const auto& state : local) {
    RETURN_IF_FAIL(state.error);
  }
  // One dense accumulator is enough for the merged state; the worker-local
  // page tables only exist to keep per-worker allocations sparse.
  struct Merged {
    std::vector<GroupSlot> slots;
    std::vector<uint64_t> distinct;
    std::vector<int64_t> sums;
  };
  Merged merged;
  merged.slots.resize(domain);
  merged.distinct.resize(bitmap_words, 0);
  merged.sums.resize(numeric_inputs.size() * domain, 0);
  const size_t merge_workers =
      ParallelWorkerCount(std::max<size_t>(1, domain / 8192));
  RETURN_IF_FAIL(RunParallelTasks(merge_workers, [&](size_t worker) {
    const size_t code_begin = domain * worker / merge_workers;
    const size_t code_end = domain * (worker + 1) / merge_workers;
    const size_t cell_begin = merged.sums.size() * worker / merge_workers;
    const size_t cell_end = merged.sums.size() * (worker + 1) / merge_workers;
    const size_t word_begin = bitmap_words * worker / merge_workers;
    const size_t word_end = bitmap_words * (worker + 1) / merge_workers;
    for (size_t w = 0; w < workers; ++w) {
      const Counts& source = local[w];
      // Each merge worker owns a contiguous range; probe only the pages a
      // source worker actually materialized instead of scanning `domain`
      // entries times `workers`.
      for (size_t page =
               code_begin >> PagedDomainAccumulator<GroupSlot>::kPageBits;
           code_begin < code_end &&
           (page << PagedDomainAccumulator<GroupSlot>::kPageBits) < code_end;
           ++page) {
        if (const GroupSlot* slots = source.slots.Page(
                page << PagedDomainAccumulator<GroupSlot>::kPageBits)) {
          const size_t from = std::max(
              code_begin, page << PagedDomainAccumulator<GroupSlot>::kPageBits);
          const size_t to = std::min(
              code_end, (page + 1)
                            << PagedDomainAccumulator<GroupSlot>::kPageBits);
          for (size_t code = from; code < to; ++code) {
            const GroupSlot& slot =
                slots[code & PagedDomainAccumulator<GroupSlot>::kPageMask];
            merged.slots[code].count += slot.count;
            merged.slots[code].first =
                std::min(merged.slots[code].first, slot.first);
          }
        }
      }
      for (size_t page =
               cell_begin >> PagedDomainAccumulator<int64_t>::kPageBits;
           cell_begin < cell_end &&
           (page << PagedDomainAccumulator<int64_t>::kPageBits) < cell_end;
           ++page) {
        if (const int64_t* sums = source.sums.Page(
                page << PagedDomainAccumulator<int64_t>::kPageBits)) {
          const size_t from = std::max(
              cell_begin, page << PagedDomainAccumulator<int64_t>::kPageBits);
          const size_t to = std::min(
              cell_end, (page + 1)
                            << PagedDomainAccumulator<int64_t>::kPageBits);
          for (size_t cell = from; cell < to; ++cell) {
            merged.sums[cell] +=
                sums[cell & PagedDomainAccumulator<int64_t>::kPageMask];
          }
        }
      }
      for (size_t word = word_begin; word < word_end; ++word) {
        merged.distinct[word] |= source.distinct[word];
      }
    }
  }));
  std::vector<uint64_t> distinct_counts;
  if (distinct_dictionary != nullptr) {
    distinct_counts.resize(domain, 0);
    if (sparse_distinct) {
      const bool spilled = std::ranges::any_of(
          local, [](const Counts& state) { return !state.runs.Empty(); });
      if (spilled) {
        SortedDistinctRuns runs;
        for (auto& state : local) {
          RETURN_IF_FAIL(state.runs.Add(state.distinct_pairs));
          std::vector<uint64_t>().swap(state.distinct_pairs);
          RETURN_IF_FAIL(runs.Absorb(std::move(state.runs)));
        }
        RETURN_IF_FAIL(runs.ForEach([&](uint64_t pair) {
          ++distinct_counts[pair >> distinct_code_bits];
        }));
      } else if (workers > 1 &&
                 domain <= QueryMemoryBudget::Global().Remaining() /
                               (workers * sizeof(uint64_t)) &&
                 QueryMemoryBudget::Global().CanReserve(
                     (workers * domain * sizeof(uint64_t)) +
                     (workers * 257 * sizeof(uint64_t)))) {
        // Disjoint sorted-value ranges own the final DISTINCT pairs. Every
        // equal pair takes the same range, so counts can be added without a
        // serial global union. Quantiles across all shards balance skew.
        QueryMemoryCharge merge_charge((workers * domain * sizeof(uint64_t)) +
                                       (workers * 257 * sizeof(uint64_t)));
        std::vector<uint64_t> samples;
        samples.reserve(workers * 256);
        for (const auto& state : local) {
          const auto& pairs = state.distinct_pairs;
          const size_t n = std::min<size_t>(256, pairs.size());
          for (size_t i = 0; i < n; ++i) {
            samples.push_back(pairs[pairs.size() * i / n]);
          }
        }
        std::ranges::sort(samples);
        std::vector<uint64_t> cuts;
        cuts.reserve(workers - 1);
        for (size_t i = 1; i < workers; ++i) {
          cuts.push_back(
              samples.empty() ? 0 : samples[samples.size() * i / workers]);
        }
        std::vector<std::vector<uint64_t>> partial(
            workers, std::vector<uint64_t>(domain, 0));
        RETURN_IF_FAIL(RunParallelTasks(workers, [&](size_t worker) {
          std::vector<uint64_t> merged_pairs;
          for (const auto& state : local) {
            const auto& pairs = state.distinct_pairs;
            const auto first =
                worker == 0 ? pairs.begin()
                            : std::lower_bound(pairs.begin(), pairs.end(),
                                               cuts[worker - 1]);
            const auto last =
                worker + 1 == workers
                    ? pairs.end()
                    : std::lower_bound(first, pairs.end(), cuts[worker]);
            if (first == last) continue;
            std::vector<uint64_t> next;
            next.reserve(merged_pairs.size() +
                         static_cast<size_t>(last - first));
            std::set_union(merged_pairs.begin(), merged_pairs.end(), first,
                           last, std::back_inserter(next));
            merged_pairs.swap(next);
          }
          for (const uint64_t pair : merged_pairs) {
            ++partial[worker][pair >> distinct_code_bits];
          }
        }));
        for (const auto& counts : partial) {
          for (size_t group = 0; group < domain; ++group) {
            distinct_counts[group] += counts[group];
          }
        }
      } else {
        auto pairs = std::move(local.front().distinct_pairs);
        for (size_t w = 1; w < workers; ++w) {
          const auto& right = local[w].distinct_pairs;
          std::vector<uint64_t> next;
          next.reserve(pairs.size() + right.size());
          std::set_union(pairs.begin(), pairs.end(), right.begin(), right.end(),
                         std::back_inserter(next));
          pairs.swap(next);
          // Release consumed inputs before merging the next worker. At peak,
          // the retained inputs and merge output hold at most two pairs/row.
          std::vector<uint64_t>().swap(local[w].distinct_pairs);
        }
        for (const uint64_t pair : pairs) {
          ++distinct_counts[pair >> distinct_code_bits];
        }
      }
    } else {
      for (size_t code = 0; code < domain; ++code) {
        for (size_t word = 0; word < words_per_group; ++word) {
          distinct_counts[code] += static_cast<uint64_t>(
              std::popcount(merged.distinct[(code * words_per_group) + word]));
        }
      }
    }
  }
  const auto aggregate_count = [&](size_t aggregate, size_t code) {
    return plan.aggregates[aggregate].aggregate->Distinct()
               ? distinct_counts[code]
               : merged.slots[code].count;
  };
  const auto aggregate_value = [&](size_t aggregate, size_t code) {
    if (key_arguments[aggregate]) {
      const auto type = plan.aggregates[aggregate].aggregate->GetType();
      if (type == AggregationType::kCount) {
        return Value(code == null_code
                         ? int64_t{0}
                         : static_cast<int64_t>(merged.slots[code].count));
      }
      if (code == null_code) {
        return Value();
      }
      if (type == AggregationType::kSum) {
        return Value(dictionary.IntEntryAt(code) *
                     static_cast<int64_t>(merged.slots[code].count));
      }
      if (type == AggregationType::kAvg) {
        return Value(static_cast<double>(
                         dictionary.IntEntryAt(code) *
                         static_cast<int64_t>(merged.slots[code].count)) /
                     static_cast<double>(merged.slots[code].count));
      }
      if (dictionary.value_type == ValueType::kInt64) {
        return Value(dictionary.IntEntryAt(code));
      }
      if (dictionary.value_type == ValueType::kDate) {
        return Value::DateFromDays(dictionary.IntEntryAt(code));
      }
      return Value(std::string(dictionary.StringEntryAt(code)));
    }
    const size_t slot = numeric_slots[aggregate];
    if (slot == SIZE_MAX) {
      return Value(static_cast<int64_t>(aggregate_count(aggregate, code)));
    }
    const int64_t sum = merged.sums[(slot * domain) + code];
    return plan.aggregates[aggregate].aggregate->GetType() ==
                   AggregationType::kAvg
               ? Value(static_cast<double>(sum) /
                       static_cast<double>(merged.slots[code].count))
               : Value(sum);
  };
  std::vector<Value> transformed_keys;
  size_t group_domain = domain;
  if (transform_key != nullptr) {
    std::unordered_map<Value, size_t, DistinctValueHash, DistinctValueEqual>
        ids;
    ids.reserve(domain);
    transformed_keys.reserve(domain);
    Merged transformed;
    transformed.slots.reserve(domain);
    for (size_t code = 0; code < domain; ++code) {
      if (merged.slots[code].count == 0) {
        continue;
      }
      // Only values observed after WHERE may raise an expression error.
      // Evaluate all such values before LIMIT can hide a failing group.
      Value value;
      if (temporal_unit != nullptr) {
        if (code != null_code) {
          const auto temporal =
              TryUtcTimestampTruncKey(dictionary.entries[code], *temporal_unit);
          if (!temporal) {
            // Do not mix packed keys with generic values. The ordinary
            // evaluator handles unsupported spellings, timezones and errors.
            return std::optional<Relation>{};
          }
          value = Value(*temporal);
        }
      } else {
        ASSIGN_OR_RETURN(Value, evaluated, (*transform_key)(code));
        value = std::move(evaluated);
      }
      Value canonical = CanonicalDistinctValue(value);
      const size_t value_bytes = EstimateValueBytes(canonical) * 2;
      if (!QueryMemoryBudget::Global().CanReserve(value_bytes)) {
        return std::optional<Relation>{};
      }
      const auto [entry, inserted] = ids.try_emplace(canonical, ids.size());
      if (inserted) {
        charge.Add(value_bytes);
        transformed_keys.push_back(std::move(canonical));
        transformed.slots.emplace_back();
      }
      const size_t id = entry->second;
      transformed.slots[id].count += merged.slots[code].count;
      transformed.slots[id].first =
          std::min(transformed.slots[id].first, merged.slots[code].first);
    }
    merged = std::move(transformed);
    group_domain = transformed_keys.size();
  }
  std::vector<size_t> groups;
  groups.reserve(group_domain);
  uint64_t input_rows = 0;
  for (size_t code = 0; code < group_domain; ++code) {
    if (merged.slots[code].count != 0) {
      groups.push_back(code);
      input_rows += merged.slots[code].count;
    }
  }
  // Dependent keys admit only checked signed-integer arithmetic. Keep their
  // non-NULL results in compact columns, and validate all observed groups in
  // parallel before LIMIT. NULL follows bytecode's evaluation/error order.
  std::vector<int64_t> dependent_values(domain * dependent_count);
  std::vector<Value> dependent_nulls(dependent_count);
  if (dependent_count != 0) {
    if (plan.dependent_key_inputs.size() != dependent_count) {
      return std::optional<Relation>{};
    }
    const size_t tasks =
        ParallelWorkerCount(std::max<size_t>(1, groups.size() / 8192));
    std::vector<Status> errors(tasks, Status::kSuccess);
    RETURN_IF_FAIL(RunParallelTasks(tasks, [&](size_t worker) {
      Row input(std::vector<Value>(1));
      for (size_t i = groups.size() * worker / tasks;
           i < groups.size() * (worker + 1) / tasks; ++i) {
        const size_t code = groups[i];
        for (size_t k = 0; k < dependent_count; ++k) {
          int64_t raw = 0;
          Status error = Status::kSuccess;
          const auto int_entries = dictionary.IntEntries();
          if (code != null_code &&
              EvaluateTypedIntExprCells(
                  plan.dependent_key_inputs[k], &int_entries[code],
                  [](slot_t) { return false; }, &raw, &error)) {
            dependent_values[(code * dependent_count) + k] = raw;
            continue;
          }
          input[0] = code == null_code ? Value() : Value(int_entries[code]);
          auto value =
              TryEvaluateRowAuto(plan.dependent_key_programs[k], input);
          if (!value.HasValue()) {
            errors[worker] = value.GetStatus();
            return;
          }
          if (code == null_code) {
            dependent_nulls[k] = std::move(value.Value());
          } else {
            dependent_values[(code * dependent_count) + k] =
                value.Value().value.int_value;
          }
        }
      }
    }));
    for (const auto& error : errors) {
      RETURN_IF_FAIL(error);
    }
  }
  const auto dependent_value = [&](size_t code, size_t key) {
    return code == null_code
               ? dependent_nulls[key]
               : Value(dependent_values[(code * dependent_count) + key]);
  };
  const auto precedes = [&](size_t left, size_t right) {
    for (size_t i = 0; i < plan.order_items.size(); ++i) {
      const auto& item = plan.order_items[i];
      if (item.constant) {
        continue;
      }
      int comparison = 0;
      if (item.dependent_key) {
        const Value a = dependent_value(left, *item.dependent_key);
        const Value b = dependent_value(right, *item.dependent_key);
        if (a.IsNull() || b.IsNull()) {
          if (a.IsNull() == b.IsNull()) {
            continue;
          }
          return a.IsNull() ? plan.order_nulls_first[i]
                            : !plan.order_nulls_first[i];
        }
        comparison = CompareForOrderBy(a, b);
      } else if (item.is_key) {
        const bool left_null = transform_key != nullptr
                                   ? transformed_keys[left].IsNull()
                                   : left == null_code;
        const bool right_null = transform_key != nullptr
                                    ? transformed_keys[right].IsNull()
                                    : right == null_code;
        if (left_null || right_null) {
          if (left_null == right_null) {
            continue;
          }
          return left_null ? plan.order_nulls_first[i]
                           : !plan.order_nulls_first[i];
        }
        if (transform_key != nullptr) {
          comparison = CompareForOrderBy(transformed_keys[left],
                                         transformed_keys[right]);
        } else if (numeric_dictionary) {
          comparison =
              (dictionary.IntEntryAt(left) > dictionary.IntEntryAt(right)) -
              (dictionary.IntEntryAt(left) < dictionary.IntEntryAt(right));
        } else {
          // Borrow dictionary text only for this comparison. Value ordering
          // also handles interval-shaped strings correctly.
          Value a;
          a.type = ValueType::kVarChar;
          a.value.varchar_value = dictionary.entries[left];
          Value b;
          b.type = ValueType::kVarChar;
          b.value.varchar_value = dictionary.entries[right];
          comparison = CompareForOrderBy(a, b);
        }
      } else {
        if (numeric_slots[item.index] == SIZE_MAX &&
            !key_arguments[item.index]) {
          const uint64_t a = aggregate_count(item.index, left);
          const uint64_t b = aggregate_count(item.index, right);
          comparison = (a > b) - (a < b);
        } else {
          const Value a = aggregate_value(item.index, left);
          const Value b = aggregate_value(item.index, right);
          if (a.IsNull() || b.IsNull()) {
            if (a.IsNull() == b.IsNull()) {
              continue;
            }
            return a.IsNull() ? plan.order_nulls_first[i]
                              : !plan.order_nulls_first[i];
          }
          comparison = CompareForOrderBy(a, b);
        }
      }
      if (comparison != 0) {
        return plan.order_ascending[i] ? comparison < 0 : comparison > 0;
      }
    }
    return merged.slots[left].first < merged.slots[right].first;
  };
  const size_t capacity = statement.HasLimit()
                              ? statement.Offset() + statement.Limit()
                              : groups.size();
  const size_t retained = std::min(capacity, groups.size());
  bool sorted_with_radix = false;
  if (retained < groups.size() && retained * 2 < groups.size()) {
    std::partial_sort(groups.begin(),
                      groups.begin() + static_cast<std::ptrdiff_t>(retained),
                      groups.end(), precedes);
    sorted_with_radix = true;
  } else if (std::getenv("TINYLAMB_DISABLE_RADIX_SORT") == nullptr &&
             groups.size() >= 1024 && plan.order_items.size() == 1) {
    const auto& item = plan.order_items[0];
    if (!item.constant && !item.is_key && !item.dependent_key &&
        !key_arguments[item.index]) {
      const bool asc = plan.order_ascending[0];
      const bool is_count = (numeric_slots[item.index] == SIZE_MAX);
      const bool is_int_sum =
          !is_count && plan.aggregates[item.index].aggregate->GetType() ==
                           AggregationType::kSum;
      if (is_count || is_int_sum) {
        // Lean proof:
        // lean/TinylambOpt/ResearchProofs/SortRadixKeyPartitionSoundness.lean
        // (sound)
        const size_t nslot = numeric_slots[item.index];
        RadixSortIndicesByUint64Key(
            groups,
            [&](size_t g) noexcept -> uint64_t {
              if (is_count) {
                const uint64_t c = aggregate_count(item.index, g);
                return asc ? c : ~c;
              }
              return EncodeSignedInt64ForRadix(
                  merged.sums[(nslot * domain) + g], asc);
            },
            [&](size_t a, size_t b) noexcept {
              return merged.slots[a].first < merged.slots[b].first;
            });
        sorted_with_radix = true;
      }
    }
  }
  if (!sorted_with_radix) {
    std::sort(groups.begin(), groups.end(), precedes);
  }
  if (runtime != nullptr) {
    runtime->scan_rows += store.total_rows;
    runtime->scan_output_rows += input_rows;
    ++runtime->typed_parallel_aggregations;
    runtime->aggregate_input_rows += input_rows;
    runtime->aggregate_updates += input_rows * plan.aggregates.size();
    runtime->aggregate_groups += groups.size();
    ++runtime->dictionary_aggregations;
    runtime->dictionary_summary_aggregations += summarized ? 1 : 0;
    runtime->dictionary_aggregate_rows += summarized ? 0 : store.total_rows;
    if (statement.HasLimit()) {
      ++runtime->topn_finish_uses;
      runtime->topn_finish_capacity = capacity;
      runtime->topn_finish_input_rows = groups.size();
    }
    runtime->aggregate_ms += ElapsedMs(started);
  }
  groups.resize(retained);
  Relation output(runtime);
  std::vector<Column> columns;
  columns.reserve(statement.SelectList().size());
  for (size_t i = 0; i < statement.SelectList().size(); ++i) {
    columns.emplace_back(ProjectionName(statement.SelectList()[i], i),
                         ValueType::kNull);
  }
  for (const size_t code : groups) {
    std::vector<Value> values;
    values.reserve(plan.emit_items.size());
    for (const auto& item : plan.emit_items) {
      if (item.dependent_key) {
        values.push_back(dependent_value(code, *item.dependent_key));
      } else if (item.constant) {
        values.push_back(*item.constant);
      } else if (item.is_key) {
        if (transform_key != nullptr) {
          if (temporal_unit != nullptr && !transformed_keys[code].IsNull()) {
            ASSIGN_OR_RETURN(Value, formatted,
                             FormatUtcTimestampTruncKey(
                                 transformed_keys[code].value.int_value));
            values.push_back(std::move(formatted));
          } else {
            values.push_back(transformed_keys[code]);
          }
        } else if (numeric_dictionary) {
          values.push_back(
              code == null_code
                  ? Value()
                  : (dictionary.value_type == ValueType::kDate
                         ? Value::DateFromDays(dictionary.IntEntryAt(code))
                         : Value(dictionary.IntEntryAt(code))));
        } else {
          values.push_back(code == null_code
                               ? Value()
                               : Value(std::string(dictionary.entries[code])));
        }
      } else {
        values.push_back(aggregate_value(item.index, code));
      }
    }
    if (output.TotalRows() == 0) {
      for (size_t i = 0; i < columns.size(); ++i) {
        columns[i] = Column(columns[i].Name(), ValueTypeOf(values[i]));
        columns[i].SetUnsigned(values[i].IsUnsigned());
      }
    }
    output.AddRow(Row(std::move(values)));
  }
  RETURN_IF_FAIL(output.GetStatus());
  output.schema = Schema("", std::move(columns));
  return std::optional<Relation>(std::move(output));
} catch (const std::exception& error) {
  return StatusError(StatusCode::kRuntimeError, error.what());
} catch (...) {
  return StatusError(StatusCode::kRuntimeError, "dictionary aggregate failed");
}

// Pack independent, SQL-distinct dictionary domains into one exact integer
// key. NULL occupies one extra code in each domain. This avoids hashing or
// copying variable-length values in a multi-column COUNT group. Key-only
// constant predicates are evaluated once over each dictionary domain.
StatusOr<std::optional<Relation>> TryCompositeDictionaryAggregate(
    const LSTableStore& store, const TypedAggregationPlan& plan,
    const SelectStatement& statement, const std::vector<slot_t>& projection,
    const Schema& schema, ExecutionRuntime* runtime,
    const std::vector<std::function<StatusOr<Value>(size_t)>>* transforms,
    const std::vector<std::optional<DictionaryCaseInput>>* cases,
    const std::vector<std::optional<UtcTimestampTruncUnit>>* extracts,
    bool require_selective) try {
  if (HasPostAggItem(plan)) {
    return std::optional<Relation>{};
  }
  if ((!statement.HasLimit() &&
       std::getenv("TINYLAMB_DISABLE_UNBOUNDED_DICTIONARY_AGG") != nullptr) ||
      plan.key_offsets.empty() ||
      (plan.key_offsets.size() < 2 && !require_selective) ||
      plan.key_offsets.size() > 8 || !plan.dependent_key_programs.empty() ||
      !plan.filter_programs.empty() || !plan.filter_disjunctions.empty() ||
      !plan.null_filters.empty() ||
      (transforms != nullptr &&
       (!plan.filter_compares.empty() || !plan.inlist_compares.empty())) ||
      statement.Distinct() || statement.WithTies() ||
      (statement.HasLimit() &&
       statement.Offset() > SIZE_MAX - statement.Limit()) ||
      store.total_rows > static_cast<uint64_t>(INT64_MAX)) {
    return std::optional<Relation>{};
  }
  std::vector<const int64_t*> numeric_inputs;
  numeric_inputs.reserve(plan.aggregates.size());
  std::vector<size_t> numeric_slots(plan.aggregates.size(), SIZE_MAX);
  for (size_t i = 0; i < plan.aggregates.size(); ++i) {
    const auto& input = plan.aggregates[i];
    if (IsCountStar(*input.aggregate) && !input.aggregate->Distinct()) {
      continue;
    }
    const int64_t* column =
        transforms == nullptr && cases == nullptr
            ? BoundedIntegerAggregateColumn(store, input, projection)
            : nullptr;
    if (column == nullptr) {
      return std::optional<Relation>{};
    }
    numeric_slots[i] = numeric_inputs.size();
    numeric_inputs.push_back(column);
  }
  const auto started = std::chrono::steady_clock::now();
  struct Key {
    const LSTableColumnDictionary* dictionary;
    uint32_t null_code;
    unsigned shift;
    uint64_t mask;
    std::vector<uint32_t> remap;
    std::vector<Value> values;
    std::optional<Value> extra;
    std::optional<uint32_t> otherwise_code;
    std::vector<EncodedDictionaryPredicate> conditions;
  };
  QueryMemoryCharge transform_charge;
  std::vector<Key> keys;
  keys.reserve(plan.key_offsets.size());
  unsigned shift = 0;
  for (const slot_t offset : plan.key_offsets) {
    // Small dictionaries can contain UINT64 bit patterns too. This kernel
    // emits signed Values; retain the ordinary typed path's unsigned tags.
    if (schema.GetColumn(offset).IsUnsigned()) {
      return std::optional<Relation>{};
    }
    const slot_t column = projection.empty() ? offset : projection[offset];
    const auto& dictionary = store.dictionaries[column];
    const bool numeric = dictionary.value_type == ValueType::kInt64 ||
                         dictionary.value_type == ValueType::kDate;
    const bool double_dictionary = dictionary.value_type == ValueType::kDouble;
    const bool transformed =
        transforms != nullptr && (*transforms)[keys.size()];
    // Transformed keys fold through `entries` (string payloads) only; keep
    // double keys on the plain code path.
    if (double_dictionary && transformed) {
      return std::optional<Relation>{};
    }
    if ((!dictionary.active && dictionary.wide_code_bytes != 2 &&
         dictionary.wide_code_bytes != 4) ||
        (!numeric && !transformed && !dictionary.bytewise_equality)) {
      return std::optional<Relation>{};
    }
    // DOUBLE dictionaries carry their domain in `double_entries` (built with
    // canonicalized ±0 and NaN excluded, so code equality is value
    // equality); counting `entries` instead made the domain 0 and folded
    // every row into the single NULL group (P680).
    const size_t domain =
        numeric ? dictionary.IntEntryCount()
                : (double_dictionary ? dictionary.DoubleEntryCount()
                                     : dictionary.StringEntryCount());
    if (domain > UINT32_MAX ||
        dictionary.DictionaryCodeCount() != store.total_rows) {
      return std::optional<Relation>{};
    }
    Key key{.dictionary = &dictionary,
            .null_code = static_cast<uint32_t>(domain),
            .shift = shift,
            .mask = 0,
            .remap = {},
            .values = {},
            .extra = std::nullopt,
            .otherwise_code = std::nullopt,
            .conditions = {}};
    size_t max_code = domain;
    if (transformed) {
      // The fold accepts only an unfiltered visible snapshot: every physical
      // dictionary entry occurs in its input. NULL is evaluated only if it
      // actually occurs. Scalar errors therefore cannot be hidden by TopN.
      const size_t entries = domain + 1;
      size_t transform_bytes = 0;
      if (__builtin_mul_overflow(entries,
                                 (sizeof(Value) * 2) + sizeof(uint32_t) + 96,
                                 &transform_bytes) ||
          !QueryMemoryBudget::Global().CanReserve(transform_bytes)) {
        return std::optional<Relation>{};
      }
      transform_charge.Add(transform_bytes);
      key.remap.resize(entries, 0);
      bool extracted = false;
      if (extracts != nullptr && (*extracts)[keys.size()]) {
        const auto unit = *(*extracts)[keys.size()];
        const size_t workers =
            ParallelWorkerCount(std::max<size_t>(1, domain / 8192));
        std::vector<uint8_t> valid(workers, 1);
        RETURN_IF_FAIL(RunParallelTasks(workers, [&](size_t worker) {
          for (size_t code = domain * worker / workers;
               code < domain * (worker + 1) / workers; ++code) {
            const auto value =
                TryUtcTimestampExtractKey(dictionary.entries[code], unit);
            if (!value) {
              valid[worker] = 0;
              break;
            }
            key.remap[code] = static_cast<uint32_t>(*value);
          }
        }));
        extracted =
            std::ranges::all_of(valid, [](uint8_t v) { return v != 0; });
        if (extracted) {
          const int64_t limit =
              unit == UtcTimestampTruncUnit::kMinute ? 60 : 24;
          for (int64_t value = 0; value < limit; ++value) {
            key.values.emplace_back(value);
          }
          key.remap[domain] = static_cast<uint32_t>(limit);
          key.values.emplace_back();
        }
      }
      if (!extracted) {
        key.values.reserve(entries);
        std::unordered_map<Value, uint32_t, DistinctValueHash,
                           DistinctValueEqual>
            ids;
        ids.reserve(entries);
        const size_t observed =
            domain + (store.column_has_null[column] != 0 ? 1 : 0);
        for (size_t code = 0; code < observed; ++code) {
          ASSIGN_OR_RETURN(Value, value, (*transforms)[keys.size()](code));
          Value canonical = CanonicalDistinctValue(value);
          const size_t value_bytes = EstimateValueBytes(canonical) * 2;
          if (!QueryMemoryBudget::Global().CanReserve(value_bytes)) {
            return std::optional<Relation>{};
          }
          const auto [entry, inserted] =
              ids.try_emplace(canonical, static_cast<uint32_t>(ids.size()));
          if (inserted) {
            transform_charge.Add(value_bytes);
            key.values.push_back(std::move(canonical));
          }
          key.remap[code] = entry->second;
        }
      }
      max_code = key.values.empty() ? 0 : key.values.size() - 1;
    }
    if (cases != nullptr && (*cases)[keys.size()]) {
      const auto& input = *(*cases)[keys.size()];
      if (transformed || numeric) {
        return std::optional<Relation>{};
      }
      key.otherwise_code = key.null_code;
      if (!input.otherwise.IsNull()) {
        const auto found = std::ranges::find(dictionary.entries,
                                             input.otherwise.AsStringView());
        if (found != dictionary.entries.end()) {
          key.otherwise_code =
              static_cast<uint32_t>(found - dictionary.entries.begin());
        } else {
          if (domain >= UINT32_MAX) {
            return std::optional<Relation>{};
          }
          const size_t extra_bytes = EstimateValueBytes(input.otherwise);
          if (!QueryMemoryBudget::Global().CanReserve(extra_bytes)) {
            return std::optional<Relation>{};
          }
          transform_charge.Add(extra_bytes);
          key.extra = input.otherwise;
          key.otherwise_code = static_cast<uint32_t>(domain + 1);
          max_code = domain + 1;
        }
      }
      for (const auto& condition : input.conditions) {
        auto compiled = CompileDictionaryPredicate(store, condition, projection,
                                                   transform_charge);
        if (!compiled) {
          return std::optional<Relation>{};
        }
        key.conditions.push_back(std::move(*compiled));
      }
    }
    const auto bits =
        static_cast<unsigned>(std::max(1, std::bit_width(max_code)));
    if (bits >= 64 || shift > 64 - bits) {
      return std::optional<Relation>{};
    }
    key.mask = (uint64_t{1} << bits) - 1;
    keys.push_back(std::move(key));
    shift += bits;
  }
  // With no LIMIT, every group needs materialization and encounter-order
  // sorting. For wide, almost-unique numeric tuples the ordinary typed hash
  // path is cheaper than code packing/scatter plus that full finish. Use the
  // exact domain product only as a cost bound, never to omit any input. Keep
  // string/transformed/selective keys on the encoded path: avoiding their
  // decoding or rejecting rows can still pay for partitioning.
  if (!statement.HasLimit() && keys.size() > 1 &&
      plan.filter_compares.empty() && plan.inlist_compares.empty() &&
      std::ranges::all_of(keys, [](const Key& key) {
        return key.remap.empty() && !key.otherwise_code &&
               (key.dictionary->value_type == ValueType::kInt64 ||
                key.dictionary->value_type == ValueType::kDate);
      })) {
    uint64_t domain_product = 1;
    for (const auto& key : keys) {
      const uint64_t domain = uint64_t{key.null_code} + 1;
      if (domain_product > store.total_rows / domain) {
        return std::optional<Relation>{};
      }
      domain_product *= domain;
    }
  }
  std::vector<EncodedDictionaryPredicate> filters;
  filters.reserve(plan.filter_compares.size());
  for (const auto& filter : plan.filter_compares) {
    auto compiled =
        CompileDictionaryPredicate(store, filter, projection, transform_charge);
    if (!compiled) {
      return std::optional<Relation>{};
    }
    filters.push_back(std::move(*compiled));
  }
  for (const auto& list : plan.inlist_compares) {
    auto compiled =
        CompileDictionaryInList(store, list, projection, transform_charge);
    if (!compiled) {
      return std::optional<Relation>{};
    }
    filters.push_back(std::move(*compiled));
  }
  OrderDictionaryPredicates(filters);
  const auto rows = static_cast<size_t>(store.total_rows);
  // Sampling chooses the aggregation layout only; it never proves absence
  // or drops input. Selective predicates otherwise allocate and zero a dense
  // dictionary-sized counter array per worker for mostly unobserved groups.
  if (require_selective) {
    if (filters.empty()) {
      return std::optional<Relation>{};
    }
    const size_t sample = std::min<size_t>(rows, 8192);
    size_t passing = 0;
    for (size_t i = 0; i < sample; ++i) {
      const size_t row = ((rows / sample) * i) + std::min(rows % sample, i);
      passing +=
          std::ranges::all_of(
              filters, [&](const auto& filter) { return filter.Matches(row); })
              ? 1U
              : 0U;
    }
    if (passing * 8 > sample) {
      return std::optional<Relation>{};
    }
  }
  const size_t prefix_capacity =
      statement.HasLimit() ? statement.Offset() + statement.Limit() : 0;
  // With no meaningful ordering/HAVING, the first K encountered COUNT groups
  // determine LIMIT. Their complete counts still require visiting every row,
  // but unrelated groups never need hash entries or materialized state.
  const bool prefix_only =
      numeric_inputs.empty() && !statement.Having() && prefix_capacity > 0 &&
      prefix_capacity <= 1024 &&
      std::ranges::all_of(
          plan.order_items,
          [](const auto& item) { return item.constant.has_value(); }) &&
      std::ranges::all_of(plan.aggregates, [](const auto& input) {
        const auto& agg = *input.aggregate;
        return IsCountStar(agg) && !agg.Distinct() && !agg.WhereFilter() &&
               !agg.NeedsGroupContext() && !agg.HasInnerGroupBy() &&
               agg.Having() == AggregateHavingModifier::kNone &&
               !agg.HavingCondition() && agg.InnerOrderBy().empty() &&
               !agg.SecondaryArg() && agg.ExtraArgs().empty() &&
               agg.TrailingArgs().empty();
      });
  // Worker/merge tables, their rounded capacities, TopN and (when needed)
  // selection vectors are bounded by input rows. A worker reserves its exact
  // passing-row bound once, so rejected rows never initialize hash buckets.
  size_t bytes_per_row = 160 + (filters.empty() ? 0 : sizeof(size_t));
  size_t numeric_bytes = 0;
  size_t bytes = 0;
  // Worker groups together and the merged table each hold at most rows
  // numeric tuples. Reserve their full bound before building either table.
  if (__builtin_mul_overflow(numeric_inputs.size(), 2 * sizeof(int64_t),
                             &numeric_bytes) ||
      __builtin_add_overflow(bytes_per_row, numeric_bytes, &bytes_per_row) ||
      __builtin_mul_overflow(prefix_only ? prefix_capacity * 18 : rows,
                             bytes_per_row, &bytes) ||
      !QueryMemoryBudget::Global().CanReserve(bytes)) {
    return std::optional<Relation>{};
  }
  QueryMemoryCharge charge(bytes);
  // Keep COUNT-only kernels free of numeric state updates and branches.
  const auto execute =
      [&]<bool Numeric>() -> StatusOr<std::optional<Relation>> {
    struct Group {
      uint64_t key;
      uint64_t count;
      uint64_t first;
    };
    struct Groups {
      FlatIntKeyMap<size_t> index;
      std::vector<Group> values;
      std::vector<int64_t> sums;
      size_t numeric_count = 0;
      void Reserve(size_t count, size_t width) {
        index.Reserve(count);
        values.reserve(count);
        numeric_count = width;
        sums.reserve(count * width);
      }
      size_t Add(uint64_t key, uint64_t count, uint64_t first) {
        bool inserted = false;
        size_t* slot = index.TryEmplace(std::bit_cast<int64_t>(key), &inserted);
        if (inserted) {
          *slot = values.size();
          values.push_back({key, count, first});
          if constexpr (Numeric) {
            sums.resize(sums.size() + numeric_count, 0);
          }
        } else {
          auto& group = values[*slot];
          group.count += count;
          group.first = std::min(group.first, first);
        }
        return *slot;
      }
    };
    const size_t workers = std::max<size_t>(
        1, std::min<size_t>(
               {QueryParallelWorkerLimit(std::thread::hardware_concurrency()),
                store.blocks.size(), 16}));
    const auto packed_key = [&](size_t row) {
      uint64_t packed = 0;
      for (const auto& key : keys) {
        uint64_t code = 0;
        if (key.otherwise_code &&
            !std::ranges::all_of(key.conditions, [&](const auto& condition) {
              return condition.Matches(row);
            })) {
          code = *key.otherwise_code;
        } else {
          const size_t input_code =
              std::min(ReadDictionaryCode(*key.dictionary, row), key.null_code);
          code = key.remap.empty() ? input_code : key.remap[input_code];
        }
        packed |= code << key.shift;
      }
      return packed;
    };
    const auto add_row = [&](Groups& groups, uint64_t packed, size_t row) {
      const size_t group = groups.Add(packed, 1, row);
      if constexpr (Numeric) {
        for (size_t a = 0; a < numeric_inputs.size(); ++a) {
          groups.sums[(group * numeric_inputs.size()) + a] +=
              numeric_inputs[a][row];
        }
      }
    };
    std::vector<Groups> local(workers);
    uint64_t input_rows = 0;
    if (prefix_only) {
      Groups prefix;
      prefix.Reserve(prefix_capacity, 0);
      for (size_t row = 0; row < rows && prefix.values.size() < prefix_capacity;
           ++row) {
        if (std::ranges::all_of(filters, [&](const auto& filter) {
              return filter.Matches(row);
            })) {
          prefix.Add(packed_key(row), 0, row);
        }
      }
      std::vector<uint64_t> passed(workers, 0);
      RETURN_IF_FAIL(RunParallelTasks(workers, [&](size_t worker) {
        auto& groups = local[worker];
        groups.values = prefix.values;
        uint64_t passed_count = 0;
        for (size_t row = rows * worker / workers;
             row < rows * (worker + 1) / workers; ++row) {
          if (!std::ranges::all_of(filters, [&](const auto& filter) {
                return filter.Matches(row);
              }))
            continue;
          ++passed_count;
          if (const auto* group =
                  prefix.index.Find(std::bit_cast<int64_t>(packed_key(row))))
            ++groups.values[*group].count;
        }
        passed[worker] = passed_count;
      }));
      for (const auto count : passed) {
        input_rows += count;
      }
    } else if (workers == 1) {
      local[0].Reserve(rows, numeric_inputs.size());
      for (size_t row = 0; row < rows; ++row) {
        if (std::ranges::all_of(filters, [&](const auto& filter) {
              return filter.Matches(row);
            })) {
          add_row(local[0], packed_key(row), row);
          ++input_rows;
        }
      }
    } else {
      // Dictionary codes prove key equality. Partition them before building
      // groups so the same key cannot enter two worker hash tables. This
      // removes the serial second hash build over all partial groups.
      // Rejected rows never read either buffer. Every accepted row gets its
      // packed key before scatter, and every position is written before use.
      // Avoid serial zero filling and first-touch pages in the worker pass.
      auto packed = std::make_unique_for_overwrite<
          uint64_t[]>(  // NOLINT(modernize-avoid-c-arrays)
          rows);
      auto positions = std::make_unique_for_overwrite<
          size_t[]>(  // NOLINT(modernize-avoid-c-arrays)
          rows);
      std::vector<uint8_t> owners(rows, UINT8_MAX);
      std::vector<std::vector<size_t>> counts(workers,
                                              std::vector<size_t>(workers, 0));
      RETURN_IF_FAIL(RunParallelTasks(workers, [&](size_t worker) {
        const size_t begin =
            ((rows / workers) * worker) + std::min(rows % workers, worker);
        const size_t end = ((rows / workers) * (worker + 1)) +
                           std::min(rows % workers, worker + 1);
        for (size_t row = begin; row < end; ++row) {
          if (!std::ranges::all_of(filters, [&](const auto& filter) {
                return filter.Matches(row);
              })) {
            continue;
          }
          packed[row] = packed_key(row);
          uint64_t hash = packed[row] + 0x9e3779b97f4a7c15ULL;
          hash = (hash ^ (hash >> 30)) * 0xbf58476d1ce4e5b9ULL;
          hash = (hash ^ (hash >> 27)) * 0x94d049bb133111ebULL;
          const size_t owner = (hash ^ (hash >> 31)) % workers;
          owners[row] = static_cast<uint8_t>(owner);
          ++counts[worker][owner];
        }
      }));
      std::vector<size_t> boundaries(workers + 1, 0);
      for (size_t owner = 0; owner < workers; ++owner) {
        size_t next = boundaries[owner];
        for (auto& count : counts) {
          const size_t size = count[owner];
          count[owner] = next;
          next += size;
        }
        boundaries[owner + 1] = next;
      }
      input_rows = boundaries.back();
      RETURN_IF_FAIL(RunParallelTasks(workers, [&](size_t worker) {
        const size_t begin =
            ((rows / workers) * worker) + std::min(rows % workers, worker);
        const size_t end = ((rows / workers) * (worker + 1)) +
                           std::min(rows % workers, worker + 1);
        for (size_t row = begin; row < end; ++row) {
          if (owners[row] != UINT8_MAX) {
            positions[counts[worker][owners[row]]++] = row;
          }
        }
      }));
      RETURN_IF_FAIL(RunParallelTasks(workers, [&](size_t worker) {
        const size_t begin = boundaries[worker];
        const size_t end = boundaries[worker + 1];
        auto& groups = local[worker];
        groups.Reserve(end - begin, numeric_inputs.size());
        for (size_t i = begin; i < end; ++i) {
          const size_t row = positions[i];
          add_row(groups, packed[row], row);
        }
      }));
    }
    size_t complete_groups = prefix_only ? local[0].values.size() : 0;
    if (!prefix_only) {
      for (const auto& groups : local) {
        complete_groups += groups.values.size();
      }
    }
    // Each complete key belongs to one owner. For COUNT ordering the global
    // top K is contained in the union of owner-local top K, including the
    // original encounter position as a deterministic tie breaker.
    const bool count_order =
        std::ranges::all_of(plan.order_items, [&](const auto& item) {
          return item.constant ||
                 (!item.is_key &&
                  IsCountStar(*plan.aggregates[item.index].aggregate));
        });
    if (statement.HasLimit() && !prefix_only && workers > 1 &&
        !statement.Having() && count_order) {
      RETURN_IF_FAIL(RunParallelTasks(workers, [&](size_t worker) {
        auto& groups = local[worker];
        const size_t keep = std::min(prefix_capacity, groups.values.size());
        if (keep == groups.values.size()) return;
        std::vector<size_t> order(groups.values.size());
        std::iota(order.begin(), order.end(), size_t{0});
        const auto precedes = [&](size_t a, size_t b) {
          const auto& left = groups.values[a];
          const auto& right = groups.values[b];
          for (size_t i = 0; i < plan.order_items.size(); ++i) {
            if (plan.order_items[i].constant) continue;
            if (left.count != right.count)
              return plan.order_ascending[i] ? left.count < right.count
                                             : left.count > right.count;
          }
          return left.first < right.first;
        };
        std::partial_sort(order.begin(),
                          order.begin() + static_cast<std::ptrdiff_t>(keep),
                          order.end(), precedes);
        Groups selected;
        selected.numeric_count = numeric_inputs.size();
        selected.values.reserve(keep);
        selected.sums.reserve(keep * numeric_inputs.size());
        for (size_t i = 0; i < keep; ++i) {
          selected.values.push_back(groups.values[order[i]]);
          if constexpr (Numeric)
            for (size_t a = 0; a < numeric_inputs.size(); ++a) {
              selected.sums.push_back(
                  groups.sums[(order[i] * numeric_inputs.size()) + a]);
            }
        }
        groups = std::move(selected);
      }));
    }
    Groups merged;
    if (prefix_only) {
      merged = std::move(local[0]);
      for (size_t worker = 1; worker < workers; ++worker) {
        for (size_t group = 0; group < merged.values.size(); ++group) {
          merged.values[group].count += local[worker].values[group].count;
        }
      }
    } else if (workers == 1) {
      merged = std::move(local[0]);
    } else {
      std::vector<size_t> offsets(workers + 1, 0);
      for (size_t worker = 0; worker < workers; ++worker) {
        offsets[worker + 1] = offsets[worker] + local[worker].values.size();
      }
      merged.values.resize(offsets.back());
      merged.sums.resize(offsets.back() * numeric_inputs.size());
      merged.numeric_count = numeric_inputs.size();
      RETURN_IF_FAIL(RunParallelTasks(workers, [&](size_t worker) {
        const auto& groups = local[worker];
        std::copy(groups.values.begin(), groups.values.end(),
                  merged.values.begin() +
                      static_cast<std::ptrdiff_t>(offsets[worker]));
        if constexpr (Numeric) {
          std::copy(groups.sums.begin(), groups.sums.end(),
                    merged.sums.begin() +
                        static_cast<std::ptrdiff_t>(offsets[worker] *
                                                    numeric_inputs.size()));
        }
      }));
    }
    if (runtime && !prefix_only) {
      ++runtime->partitioned_dictionary_aggregations;
    }
    const auto aggregate_value = [&](size_t aggregate, size_t group) {
      if constexpr (Numeric) {
        const size_t slot = numeric_slots[aggregate];
        if (slot != SIZE_MAX) {
          const int64_t sum =
              merged.sums[(group * numeric_inputs.size()) + slot];
          return plan.aggregates[aggregate].aggregate->GetType() ==
                         AggregationType::kAvg
                     ? Value(static_cast<double>(sum) /
                             static_cast<double>(merged.values[group].count))
                     : Value(sum);
        }
      }
      return Value(static_cast<int64_t>(merged.values[group].count));
    };
    const auto value_at = [&](uint64_t packed, size_t index, bool own) {
      const auto& key = keys[index];
      const size_t code = (packed >> key.shift) & key.mask;
      if (!key.remap.empty()) {
        return key.values[code];
      }
      if (key.extra && code == static_cast<size_t>(key.null_code) + 1) {
        return *key.extra;
      }
      if (code == key.null_code) {
        return Value();
      }
      const auto& dictionary = *key.dictionary;
      if (dictionary.value_type == ValueType::kInt64) {
        return Value(dictionary.IntEntryAt(code));
      }
      if (dictionary.value_type == ValueType::kDate) {
        return Value::DateFromDays(dictionary.IntEntryAt(code));
      }
      if (dictionary.value_type == ValueType::kDouble) {
        return Value(dictionary.DoubleEntries()[code]);
      }
      if (own) {
        return Value(std::string(dictionary.entries[code]));
      }
      Value borrowed;
      borrowed.type = ValueType::kVarChar;
      borrowed.value.varchar_value = dictionary.entries[code];
      return borrowed;
    };
    std::vector<size_t> order(merged.values.size());
    std::iota(order.begin(), order.end(), size_t{0});
    const auto precedes = [&](size_t a, size_t b) {
      const auto& left = merged.values[a];
      const auto& right = merged.values[b];
      for (size_t i = 0; i < plan.order_items.size(); ++i) {
        const auto& item = plan.order_items[i];
        if (item.constant) {
          continue;
        }
        int comparison = 0;
        if (item.is_key) {
          const Value l = value_at(left.key, item.index, false);
          const Value r = value_at(right.key, item.index, false);
          if (l.IsNull() || r.IsNull()) {
            if (l.IsNull() == r.IsNull()) {
              continue;
            }
            return l.IsNull() ? plan.order_nulls_first[i]
                              : !plan.order_nulls_first[i];
          }
          comparison = CompareForOrderBy(l, r);
        } else {
          if constexpr (Numeric) {
            comparison =
                numeric_slots[item.index] == SIZE_MAX
                    ? (left.count > right.count) - (left.count < right.count)
                    : CompareForOrderBy(aggregate_value(item.index, a),
                                        aggregate_value(item.index, b));
          } else {
            comparison =
                (left.count > right.count) - (left.count < right.count);
          }
        }
        if (comparison != 0) {
          return plan.order_ascending[i] ? comparison < 0 : comparison > 0;
        }
      }
      return left.first < right.first;
    };
    const size_t capacity = statement.HasLimit()
                                ? statement.Offset() + statement.Limit()
                                : order.size();
    const size_t retained = std::min(capacity, order.size());
    bool sorted_with_radix = false;
    if (retained < order.size() && retained * 2 < order.size()) {
      std::partial_sort(order.begin(),
                        order.begin() + static_cast<std::ptrdiff_t>(retained),
                        order.end(), precedes);
      sorted_with_radix = true;
    } else if (std::getenv("TINYLAMB_DISABLE_RADIX_SORT") == nullptr &&
               order.size() >= 1024 && plan.order_items.size() == 1) {
      const auto& item = plan.order_items[0];
      if (!item.constant && !item.is_key && !item.dependent_key) {
        const bool asc = plan.order_ascending[0];
        const bool is_count =
            (!Numeric || numeric_slots[item.index] == SIZE_MAX);
        const bool is_int_sum =
            Numeric && numeric_slots[item.index] != SIZE_MAX &&
            plan.aggregates[item.index].aggregate->GetType() ==
                AggregationType::kSum;
        if (is_count || is_int_sum) {
          // Lean proof:
          // lean/TinylambOpt/ResearchProofs/SortRadixKeyPartitionSoundness.lean
          // (sound)
          const size_t nslot = numeric_slots[item.index];
          RadixSortIndicesByUint64Key(
              order,
              [&](size_t g) noexcept -> uint64_t {
                if (is_count) {
                  const uint64_t c = merged.values[g].count;
                  return asc ? c : ~c;
                }
                if constexpr (Numeric) {
                  return EncodeSignedInt64ForRadix(
                      merged.sums[(g * numeric_inputs.size()) + nslot], asc);
                }
                return 0;
              },
              [&](size_t a, size_t b) noexcept {
                return merged.values[a].first < merged.values[b].first;
              });
          sorted_with_radix = true;
        }
      }
    }
    if (!sorted_with_radix) {
      std::sort(order.begin(), order.end(), precedes);
    }
    if (runtime != nullptr) {
      runtime->scan_rows += rows;
      runtime->scan_output_rows += input_rows;
      ++runtime->typed_parallel_aggregations;
      runtime->aggregate_input_rows += input_rows;
      runtime->aggregate_updates += input_rows * plan.aggregates.size();
      runtime->aggregate_groups += complete_groups;
      ++runtime->dictionary_aggregations;
      runtime->dictionary_aggregate_rows += rows;
      if (statement.HasLimit()) {
        ++runtime->topn_finish_uses;
        runtime->topn_finish_capacity = capacity;
        runtime->topn_finish_input_rows = complete_groups;
      }
      runtime->aggregate_ms += ElapsedMs(started);
    }
    order.resize(retained);
    Relation output(runtime);
    std::vector<Column> columns;
    columns.reserve(statement.SelectList().size());
    for (size_t i = 0; i < statement.SelectList().size(); ++i) {
      columns.emplace_back(ProjectionName(statement.SelectList()[i], i),
                           ValueType::kNull);
    }
    for (const size_t index : order) {
      const auto& group = merged.values[index];
      std::vector<Value> values;
      values.reserve(plan.emit_items.size());
      for (const auto& item : plan.emit_items) {
        if (item.constant) {
          values.push_back(*item.constant);
        } else if (item.is_key) {
          values.push_back(value_at(group.key, item.index, true));
        } else {
          values.push_back(aggregate_value(item.index, index));
        }
      }
      if (output.TotalRows() == 0) {
        for (size_t i = 0; i < columns.size(); ++i) {
          columns[i] = Column(columns[i].Name(), ValueTypeOf(values[i]));
          columns[i].SetUnsigned(values[i].IsUnsigned());
        }
      }
      output.AddRow(Row(std::move(values)));
    }
    RETURN_IF_FAIL(output.GetStatus());
    output.schema = Schema("", std::move(columns));
    return std::optional<Relation>(std::move(output));
  };
  if (numeric_inputs.empty()) {
    return execute.template operator()<false>();
  }
  return execute.template operator()<true>();
} catch (const std::exception& error) {
  return StatusError(StatusCode::kRuntimeError, error.what());
} catch (...) {
  return StatusError(StatusCode::kRuntimeError,
                     "composite dictionary aggregate failed");
}

namespace {

Expression DictionaryScalarColumn(const Expression& key) {
  if (!key || key->Type() != TypeTag::kFunctionCallExp ||
      ContainsParameterSlot(key)) {
    return {};
  }
  const auto& call = key->AsFunctionCallExpression();
  const auto& name = call.FuncName();
  if (name != "timestamp_trunc" && name != "datetime_trunc" &&
      name != "date_trunc" && name != "extract_minute" &&
      name != "extract_hour" && name != "byte_length" && name != "length" &&
      name != "octet_length" && name != "lower" && name != "upper" &&
      name != "regexp_replace") {
    return {};
  }
  if (call.Args().empty() || !call.Args().front() ||
      call.Args().front()->Type() != TypeTag::kColumnValue) {
    return {};
  }
  for (size_t i = 1; i < call.Args().size(); ++i) {
    const auto& argument = call.Args()[i];
    const bool unit_identifier =
        i == 1 &&
        (name == "timestamp_trunc" || name == "datetime_trunc" ||
         name == "date_trunc") &&
        argument && argument->Type() == TypeTag::kColumnValue;
    if (!argument ||
        (argument->Type() != TypeTag::kConstantValue && !unit_identifier)) {
      return {};
    }
  }
  return call.Args().front();
}

StatusOr<std::optional<Relation>> TryCompositeDictionaryExpressions(
    TransactionContext& context, const SelectStatement& statement,
    const Table& table, const std::vector<slot_t>& projection,
    const Schema& schema,
    const std::vector<const AggregateExpression*>& aggregates,
    const std::vector<Expression>& predicates, const CteMap& ctes) {
  std::vector<Expression> columns;
  columns.reserve(statement.GroupBy().size());
  std::vector<size_t> offsets;
  offsets.reserve(statement.GroupBy().size());
  std::vector<bool> computed;
  computed.reserve(statement.GroupBy().size());
  bool any_computed = false;
  bool any_scalar = false;
  bool any_case = false;
  std::vector<std::optional<DictionaryCaseInput>> cases;
  cases.reserve(statement.GroupBy().size());
  for (const auto& key : statement.GroupBy()) {
    const bool is_computed = key && key->Type() != TypeTag::kColumnValue;
    Expression column;
    std::optional<DictionaryCaseInput> conditional;
    if (key && key->Type() == TypeTag::kCaseExp &&
        !ContainsParameterSlot(key)) {
      const auto& expression = key->AsCaseExpression();
      if (expression.when_clauses_.size() != 1 ||
          !expression.when_clauses_[0].first || !expression.else_clause_ ||
          expression.else_clause_->Type() != TypeTag::kConstantValue) {
        return std::optional<Relation>{};
      }
      const auto& otherwise =
          expression.else_clause_->AsConstantValue().GetValue();
      if ((!otherwise.IsNull() &&
           (otherwise.type != ValueType::kVarChar ||
            !PlainStringComparable(otherwise.AsStringView()))) ||
          otherwise.IsCaseInsensitive()) {
        return std::optional<Relation>{};
      }
      column = expression.when_clauses_[0].second;
      if (!column || column->Type() != TypeTag::kColumnValue) {
        return std::optional<Relation>{};
      }
      conditional.emplace(
          DictionaryCaseInput{.conditions = {}, .otherwise = otherwise});
      for (const auto& conjunct :
           SplitConjuncts(expression.when_clauses_[0].first)) {
        auto condition = TryCompileTypedFilterCompare(conjunct, schema, true);
        if (!condition || !condition->constant_payload ||
            condition->is_double) {
          return std::optional<Relation>{};
        }
        conditional->conditions.push_back(std::move(*condition));
      }
      any_case = true;
    } else {
      column = is_computed ? DictionaryScalarColumn(key) : key;
      any_scalar |= is_computed;
    }
    cases.push_back(std::move(conditional));
    if (!column) {
      return std::optional<Relation>{};
    }
    const auto offset =
        LocalColumnOffset(schema, column->AsColumnValue().GetColumnName());
    if (!offset ||
        (is_computed &&
         schema.GetColumn(*offset).Type() != ValueType::kVarChar) ||
        std::ranges::find(offsets, *offset) != offsets.end()) {
      // Replacing two different expressions over the same column would make
      // positional classification ambiguous. Keep that shape on the ordinary
      // evaluator until it has explicit computed-column identities.
      return std::optional<Relation>{};
    }
    columns.push_back(std::move(column));
    offsets.push_back(*offset);
    computed.push_back(is_computed);
    any_computed |= is_computed;
  }
  if (!any_computed || (any_scalar && (!predicates.empty() || any_case))) {
    return std::optional<Relation>{};
  }
  SelectStatement raw(statement);
  raw.SetGroupBy(columns);
  const auto replace = [&](Expression expression) {
    if (expression) {
      for (size_t i = 0; i < columns.size(); ++i) {
        if (computed[i] &&
            expression->ToString() == statement.GroupBy()[i]->ToString()) {
          return columns[i];
        }
      }
    }
    return expression;
  };
  auto select = raw.SelectList();
  for (auto& item : select) {
    item.expression = replace(item.expression);
  }
  raw.SetSelectList(std::move(select));
  auto order = raw.OrderBy();
  for (auto& term : order) {
    term.expression = replace(term.expression);
  }
  raw.SetOrderBy(std::move(order));
  const auto plan =
      PlanTypedAggregation(raw, nullptr, schema, aggregates, predicates);
  if (!plan.eligible) {
    return std::optional<Relation>{};
  }
  const auto store =
      SnapshotColumnStore(context, table, projection, schema.ColumnCount());
  if (store == nullptr) {
    return std::optional<Relation>{};
  }
  Row representative(std::vector<Value>(schema.ColumnCount()));
  std::vector<std::function<StatusOr<Value>(size_t)>> transforms(
      columns.size());
  std::vector<std::optional<UtcTimestampTruncUnit>> extracts(columns.size());
  for (size_t i = 0; i < columns.size(); ++i) {
    if (!computed[i] || cases[i]) {
      continue;
    }
    const auto offset = offsets[i];
    const auto column = projection.empty() ? offset : projection[offset];
    const auto* dictionary = &store->dictionaries[column];
    const Expression key = statement.GroupBy()[i];
    const auto extract =
        schema.GetColumn(offset).IsInterval()
            ? std::nullopt
            : CompileUtcTimestampExtract(key->AsFunctionCallExpression());
    extracts[i] = extract;
    transforms[i] = [&, offset, dictionary, key,
                     extract](size_t code) -> StatusOr<Value> {
      if (extract) {
        if (code == dictionary->entries.size()) {
          return Value();
        }
        if (const auto value = TryUtcTimestampExtractKey(
                dictionary->entries[code], *extract)) {
          return Value(*value);
        }
      }
      // T4546: reuse the representative cell's owned string buffer across
      // dictionary codes (Value::CopyFromStringView assigns in place) instead
      // of allocating a fresh owned Value per code before re-parsing the key.
      if (code == dictionary->entries.size()) {
        representative[offset] = Value();
      } else {
        representative[offset].CopyFromStringView(dictionary->entries[code]);
      }
      const Scope scope{.row = &representative, .schema = &schema};
      return TryEvaluate(key, scope, nullptr, context, ctes);
    };
  }
  return TryCompositeDictionaryAggregate(
      *store, plan, statement, projection, schema, context.execution_runtime(),
      any_scalar ? &transforms : nullptr, any_case ? &cases : nullptr,
      &extracts);
}

}  // namespace

StatusOr<std::optional<Relation>> TryDictionaryExpressionAggregate(
    TransactionContext& context, const SelectStatement& statement,
    const Table& table, const std::vector<slot_t>& projection,
    const Schema& qualified_schema,
    const std::vector<const AggregateExpression*>& aggregates,
    const std::vector<Expression>& predicates, const CteMap& ctes) try {
  auto* runtime = context.execution_runtime();
  if (statement.GroupBy().empty() || statement.GroupBy().size() > 8 ||
      statement.Having() || statement.WithTies() || statement.Distinct() ||
      (runtime != nullptr && !runtime->table_key_filters.empty())) {
    return std::optional<Relation>{};
  }
  if (statement.GroupBy().size() != 1) {
    return TryCompositeDictionaryExpressions(context, statement, table,
                                             projection, qualified_schema,
                                             aggregates, predicates, ctes);
  }
  const Expression key = statement.GroupBy().front();
  const Expression column = DictionaryScalarColumn(key);
  if (!column) {
    return std::optional<Relation>{};
  }
  const auto offset = LocalColumnOffset(
      qualified_schema, column->AsColumnValue().GetColumnName());
  if (!offset ||
      qualified_schema.GetColumn(*offset).Type() != ValueType::kVarChar) {
    return std::optional<Relation>{};
  }
  // Classify only direct uses of this key, constants and COUNT(*). The
  // original key is evaluated after partial counts and before final grouping.
  SelectStatement raw = statement;
  raw.SetGroupBy({column});
  const std::string identity = key->ToString();
  auto select = raw.SelectList();
  for (auto& item : select) {
    if (item.expression && item.expression->ToString() == identity) {
      item.expression = column;
    }
  }
  raw.SetSelectList(std::move(select));
  auto order = raw.OrderBy();
  for (auto& term : order) {
    if (term.expression && term.expression->ToString() == identity) {
      term.expression = column;
    }
  }
  raw.SetOrderBy(std::move(order));
  const auto plan = PlanTypedAggregation(raw, nullptr, qualified_schema,
                                         aggregates, predicates);
  if (!plan.eligible) {
    return std::optional<Relation>{};
  }
  const auto store = SnapshotColumnStore(context, table, projection,
                                         qualified_schema.ColumnCount());
  if (store == nullptr) {
    return std::optional<Relation>{};
  }
  const slot_t full_column =
      projection.empty() ? static_cast<slot_t>(*offset) : projection[*offset];
  const auto& dictionary = store->dictionaries[full_column];
  const auto temporal_unit =
      CompileUtcTimestampTrunc(key->AsFunctionCallExpression());
  Row representative(std::vector<Value>(qualified_schema.ColumnCount()));
  const std::function<StatusOr<Value>(size_t)> transform = [&](size_t code) {
    representative[*offset] =
        code == dictionary.entries.size()
            ? Value()
            : Value(std::string(dictionary.entries[code]));
    const Scope scope{.row = &representative, .schema = &qualified_schema};
    return TryEvaluate(key, scope, nullptr, context, ctes);
  };
  return TryDictionaryAggregate(*store, plan, statement, projection,
                                qualified_schema, runtime, &transform,
                                temporal_unit ? &*temporal_unit : nullptr);
} catch (const std::exception& error) {
  return StatusError(StatusCode::kRuntimeError, error.what());
} catch (...) {
  return StatusError(StatusCode::kRuntimeError,
                     "dictionary expression aggregate failed");
}

StatusOr<bool> TryFoldDictionaryAggregateRows(
    TransactionContext& context, const SelectStatement& statement,
    const Table& table, const std::vector<slot_t>& projection,
    const Schema& schema,
    const std::vector<const AggregateExpression*>& aggregates,
    const std::vector<Expression>& predicates,
    const std::function<Status(const Row&, int64_t)>& consume,
    const std::function<Status(const Row&, int64_t, const Value&,
                               const std::vector<AggregateAccumulator>&)>&
        prepared_consume) try {
  auto* runtime = context.execution_runtime();
  if (schema.ColumnCount() != 1 ||
      schema.GetColumn(0).Type() != ValueType::kVarChar ||
      statement.GroupBy().size() != 1 ||
      (runtime != nullptr && !runtime->table_key_filters.empty())) {
    return false;
  }
  const auto is_input = [&](const Expression& expression) {
    return expression && expression->Type() == TypeTag::kColumnValue &&
           LocalColumnOffset(schema,
                             expression->AsColumnValue().GetColumnName()) == 0;
  };
  const auto& key = statement.GroupBy().front();
  if (!is_input(key) && !is_input(DictionaryScalarColumn(key))) {
    return false;
  }
  // Relational temporal calls may contain identifier-shaped date-part tokens
  // and SQL-specific temporal metadata. Those keep their original evaluator;
  // only these context-free keys can run on the pure AST worker path.
  bool pure_key = is_input(key);
  if (!pure_key) {
    const auto& name = key->AsFunctionCallExpression().FuncName();
    pure_key = name == "regexp_replace" || name == "lower" || name == "upper" ||
               name == "byte_length" || name == "length" ||
               name == "octet_length";
  }
  for (const auto& predicate : predicates) {
    if (!predicate || predicate->Type() != TypeTag::kBinaryExp) {
      return false;
    }
    const auto& binary = predicate->AsBinaryExpression();
    const bool left_input = is_input(binary.Left()) &&
                            binary.Right()->Type() == TypeTag::kConstantValue;
    const bool right_input = is_input(binary.Right()) &&
                             binary.Left()->Type() == TypeTag::kConstantValue;
    if (!left_input && !right_input) {
      return false;
    }
    const auto compare = TryCompileTypedFilterCompare(predicate, schema, true);
    if (!compare || !compare->string_payload || !compare->constant_payload ||
        compare->left != 0) {
      return false;
    }
  }
  for (const auto* aggregate : aggregates) {
    if (aggregate->Distinct() || aggregate->NeedsGroupContext() ||
        aggregate->WhereFilter() || aggregate->SecondaryArg() ||
        aggregate->HasInnerGroupBy() ||
        aggregate->Having() != AggregateHavingModifier::kNone ||
        aggregate->HavingCondition() || !aggregate->InnerOrderBy().empty() ||
        !aggregate->ExtraArgs().empty() || !aggregate->TrailingArgs().empty()) {
      return false;
    }
    const auto type = aggregate->GetType();
    if ((type == AggregationType::kCount && IsCountStar(*aggregate)) ||
        ((type == AggregationType::kCount || type == AggregationType::kMin ||
          type == AggregationType::kMax) &&
         is_input(aggregate->Child()))) {
      continue;
    }
    Expression child = aggregate->Child();
    if (child && child->Type() == TypeTag::kCastExp &&
        child->AsCastExpression().TargetValueType() == ValueType::kDouble) {
      child = child->AsCastExpression().Child();
    }
    if (type != AggregationType::kAvg || !child ||
        child->Type() != TypeTag::kFunctionCallExp) {
      return false;
    }
    const auto& call = child->AsFunctionCallExpression();
    if (call.FuncName() != "byte_length" || call.Args().size() != 1 ||
        !is_input(call.Args()[0])) {
      return false;
    }
  }
  const auto store =
      SnapshotColumnStore(context, table, projection, schema.ColumnCount());
  if (!store || store->total_rows > static_cast<uint64_t>(INT64_MAX)) {
    return false;
  }
  const slot_t column = projection.empty() ? 0 : projection[0];
  const auto& dictionary = store->dictionaries[column];
  const size_t domain = dictionary.entries.size();
  const bool encoded =
      (dictionary.wide_code_bytes == 2 || dictionary.wide_code_bytes == 4) &&
      dictionary.value_type == ValueType::kVarChar && domain <= UINT32_MAX &&
      dictionary.DictionaryCodeCount() == store->total_rows;
  if (!encoded && (!prepared_consume || !pure_key)) {
    return false;
  }
  // Every partial and final length sum stays exactly representable as a
  // double, irrespective of the order in which dictionary values are folded.
  constexpr uint64_t kExactDoubleInteger = uint64_t{1} << 53;
  if (encoded) {
    for (const auto& text : dictionary.entries) {
      if (store->total_rows != 0 &&
          text.size() > kExactDoubleInteger / store->total_rows) {
        return false;
      }
    }
  } else {
    for (const auto& block : store->blocks) {
      const auto& values = block.columns[column];
      for (size_t row = 0; row < block.row_count; ++row) {
        if (!values.IsNull(row) && store->total_rows != 0 &&
            values.VarcharAt(row).size() >
                kExactDoubleInteger / store->total_rows) {
          return false;
        }
      }
    }
  }
  const bool summarized = encoded &&
                          dictionary.code_counts.size() == domain + 1 &&
                          dictionary.code_first_row.size() == domain + 1;
  const size_t entries = encoded && !summarized ? domain + 1 : 0;
  size_t bytes = 0;
  if (__builtin_mul_overflow(entries, sizeof(int64_t) + sizeof(size_t),
                             &bytes) ||
      !QueryMemoryBudget::Global().CanReserve(bytes)) {
    return false;
  }
  const size_t block_bytes =
      encoded ? 0 : (store->blocks.size() + 1) * sizeof(size_t);
  if (__builtin_add_overflow(bytes, block_bytes, &bytes) ||
      !QueryMemoryBudget::Global().CanReserve(bytes)) {
    return false;
  }
  QueryMemoryCharge charge(bytes);
  std::vector<int64_t> counted(entries, 0);
  std::vector<uint32_t> observed;
  observed.reserve(entries);
  const auto read_code = [&](size_t row) {
    return std::min<size_t>(ReadDictionaryCode(dictionary, row), domain);
  };
  if (encoded && !summarized) {
    for (size_t row = 0; row < store->total_rows; ++row) {
      const size_t code = read_code(row);
      if (counted[code]++ == 0) {
        // The physical scan is already in encounter order. Record each code
        // once here instead of sorting the dictionary by saved row positions.
        observed.push_back(static_cast<uint32_t>(code));
      }
    }
  }
  const auto& counts = summarized ? dictionary.code_counts : counted;
  const auto& order = summarized ? dictionary.encounter_codes : observed;
  std::vector<size_t> block_offsets;
  if (!encoded) {
    block_offsets.reserve(store->blocks.size() + 1);
    block_offsets.push_back(0);
    for (const auto& block : store->blocks) {
      block_offsets.push_back(block_offsets.back() + block.row_count);
    }
  }
  const size_t input_count = encoded ? order.size() : store->total_rows;
  const auto input_row = [&](size_t index) {
    Row result(std::vector<Value>(1));
    // The immutable store stays alive through every callback and worker join.
    // Borrow the scan cell; copied-out rows/aggregate states own their bytes.
    auto& cell = result.values_[0];
    if (encoded) {
      const size_t code = order[index];
      if (code != domain) {
        cell.type = ValueType::kVarChar;
        cell.value.varchar_value = dictionary.entries[code];
      }
    } else {
      const size_t block = static_cast<size_t>(
          std::upper_bound(block_offsets.begin(), block_offsets.end(), index) -
          block_offsets.begin() - 1);
      const size_t row = index - block_offsets[block];
      const auto& values = store->blocks[block].columns[column];
      if (!values.IsNull(row)) {
        cell.type = ValueType::kVarChar;
        cell.value.varchar_value = values.VarcharAt(row);
      }
    }
    return result;
  };
  // Each transformed dictionary key belongs to exactly one owner. Finish
  // complete groups there, instead of copying/merging partial groups after
  // every small preparation wave. No expression results survive this call.
  const auto partitioned_fold = [&]() -> StatusOr<bool> {
    if (!encoded || !pure_key || !prepared_consume || input_count < 8192 ||
        std::getenv("TINYLAMB_DISABLE_PARTITIONED_DICTIONARY_FOLD")) {
      return false;
    }
    const auto* call = key->Type() == TypeTag::kFunctionCallExp
                           ? &key->AsFunctionCallExpression()
                           : nullptr;
    const bool regex_view =
        call && call->FuncName() == "regexp_replace" &&
        call->Args().size() == 3 &&
        call->Args()[1]->AsConstantValue().GetValue().type ==
            ValueType::kVarChar &&
        call->Args()[2]->AsConstantValue().GetValue().type ==
            ValueType::kVarChar;
    size_t base_bytes = 0;
    if (__builtin_mul_overflow(
            input_count,
            (regex_view ? sizeof(std::string_view) : sizeof(Value)) +
                size_t{192},
            &base_bytes)) {
      return false;
    }
    const size_t available = QueryMemoryBudget::Global().Remaining() / 2;
    if (base_bytes > available) {
      return false;
    }
    const size_t budget_bytes =
        base_bytes + std::min<size_t>(available - base_bytes, size_t{1} << 30);
    if (!QueryMemoryBudget::Global().CanReserve(budget_bytes)) {
      return false;
    }
    QueryMemoryCharge owned_charge(budget_bytes);
    const size_t workers = ParallelWorkerCount(input_count / 8192);
    std::vector<Value> keys(regex_view ? 0 : input_count);
    std::vector<std::string_view> text_keys(regex_view ? input_count : 0);
    std::vector<std::deque<std::string>> owned_text(workers);
    std::vector<uint64_t> hashes(input_count);
    std::vector<uint8_t> passing(input_count, 0);
    std::vector<size_t> positions(input_count);
    std::vector<std::vector<size_t>> sizes(workers,
                                           std::vector<size_t>(workers, 0));
    std::vector<Status> errors(workers, Status::kSuccess);
    std::atomic<size_t> retained{base_bytes};
    std::atomic<bool> too_large{false};
    std::vector<TypedFilterCompare> filters;
    filters.reserve(predicates.size());
    for (const auto& predicate : predicates) {
      filters.push_back(*TryCompileTypedFilterCompare(predicate, schema, true));
    }
    const auto cell = [&](size_t index) {
      Value value;
      const auto code = order[index];
      if (code != domain) {
        value.type = ValueType::kVarChar;
        value.value.varchar_value = dictionary.entries[code];
      }
      return value;
    };
    const auto mix = [](uint64_t x) {
      x ^= x >> 30;
      x *= 0xbf58476d1ce4e5b9ULL;
      x ^= x >> 27;
      x *= 0x94d049bb133111ebULL;
      return x ^ (x >> 31);
    };
    struct Group {
      size_t first;
      int64_t rows{0};
      int64_t nonnull{0};
      uint64_t length_sum{0};
      size_t min{SIZE_MAX};
      size_t max{SIZE_MAX};
    };
    const bool need_min = std::ranges::any_of(aggregates, [](const auto* a) {
      return a->GetType() == AggregationType::kMin;
    });
    const bool need_max = std::ranges::any_of(aggregates, [](const auto* a) {
      return a->GetType() == AggregationType::kMax;
    });
    const auto keys_equal = [&](size_t a, size_t b) {
      if (!regex_view) {
        return DistinctValueEqual{}(keys[a], keys[b]);
      }
      return passing[a] == passing[b] &&
             (passing[a] == 2 || text_keys[a] == text_keys[b]);
    };
    const auto key_value = [&](size_t index) {
      if (!regex_view) {
        return keys[index];
      }
      Value value;
      if (passing[index] != 2) {
        value.type = ValueType::kVarChar;
        value.value.varchar_value = text_keys[index];
      }
      return value;
    };
    std::vector<std::vector<Group>> partials(workers);
    std::vector<size_t> source_slots(input_count);
    const auto add_value = [&](Group& group, size_t i) {
      const int64_t repeats = counts[order[i]];
      group.rows += repeats;
      const Value value = cell(i);
      if (!value.IsNull()) {
        group.nonnull += repeats;
        group.length_sum +=
            value.AsStringView().size() * static_cast<uint64_t>(repeats);
        if (need_min &&
            (group.min == SIZE_MAX || (dictionary.codes_follow_string_order
                                           ? order[i] < order[group.min]
                                           : value < cell(group.min)))) {
          group.min = i;
        }
        if (need_max &&
            (group.max == SIZE_MAX || (dictionary.codes_follow_string_order
                                           ? order[group.max] < order[i]
                                           : cell(group.max) < value))) {
          group.max = i;
        }
      }
    };
    const auto start = std::chrono::steady_clock::now();
    auto phase_start = start;
    const auto phase = [&](const char* name) {
      if (std::getenv("TINYLAMB_GROUP_PHASES")) {
        std::cerr << "[dictionary-owned] " << name
                  << " ms=" << ElapsedMs(phase_start) << '\n';
        phase_start = std::chrono::steady_clock::now();
      }
    };
    RETURN_IF_FAIL(RunParallelTasks(workers, [&](size_t worker) {
      const size_t begin = ((input_count / workers) * worker) +
                           std::min(input_count % workers, worker);
      const size_t end = ((input_count / workers) * (worker + 1)) +
                         std::min(input_count % workers, worker + 1);
      std::vector<Value> arguments(call ? call->Args().size() : 0);
      if (call) {
        for (size_t a = 1; a < arguments.size(); ++a) {
          arguments[a] = call->Args()[a]->AsConstantValue().GetValue();
        }
      }
      std::string scratch;
      auto& prepared = partials[worker];
      // A full per-row reservation costs ~56 B/row (560 MB for 10 M rows)
      // even when the data has only a handful of distinct keys. Let the
      // vector grow geometrically to the number of real groups instead.
      prepared.reserve(std::min<size_t>(end - begin, 4096));
      // A direct-mapped cache evicts on every bucket collision, so with more
      // than a few hundred distinct keys phase 1 degenerated to roughly one
      // Group per row. Four ways per set keeps skewed key distributions
      // grouped while staying a fixed, small footprint.
      constexpr size_t kRecentSets = 64, kRecentWays = 4;
      std::array<std::array<size_t, kRecentWays>, kRecentSets> recent{};
      for (auto& set : recent) {
        set.fill(SIZE_MAX);
      }
      size_t next_way = 0;
      const auto observe = [&](size_t i) {
        auto& set = recent[hashes[i] & (kRecentSets - 1)];
        for (size_t way = 0; way < kRecentWays; ++way) {
          const size_t found = set[way];
          if (found != SIZE_MAX && hashes[i] == hashes[prepared[found].first] &&
              keys_equal(i, prepared[found].first)) {
            add_value(prepared[found], i);
            passing[i] = 0;
            return;
          }
        }
        const size_t slot = prepared.size();
        prepared.push_back({i});
        add_value(prepared.back(), i);
        set[next_way++ & (kRecentWays - 1)] = slot;
        source_slots[i] = (slot * workers) + worker;
        ++sizes[worker][hashes[i] % workers];
      };
      size_t credit = 0;
      const auto reserve = [&](size_t extra) {
        if (extra > credit) {
          const size_t request = std::max<size_t>(extra, 65536);
          size_t used = retained.load(std::memory_order_relaxed);
          for (;;) {
            if (request > budget_bytes - used) {
              too_large.store(true, std::memory_order_relaxed);
              return false;
            }
            if (retained.compare_exchange_weak(used, used + request,
                                               std::memory_order_relaxed))
              break;
          }
          credit += request;
        }
        credit -= extra;
        return true;
      };
      for (size_t i = begin; i < end; ++i) {
        const Value input = cell(i);
        if (!std::ranges::all_of(filters, [&](const auto& filter) {
              return !input.IsNull() &&
                     MatchTypedStringCompare(input.AsStringView(), filter);
            }))
          continue;
        // Dispatch the same AST builtin with borrowed input and constants
        // resolved once per worker; no row or ColumnValue string copies.
        // A capture-only replacement borrows immutable dictionary bytes.
        // Other replacements use the same regex implementation and own the
        // scratch result before the next row overwrites it.
        if (regex_view) {
          if (!input.IsNull()) {
            auto result = ReplaceCheckedRegexView(
                input.AsStringView(), arguments[1].AsStringView(),
                arguments[2].AsStringView(), &scratch);
            if (!result.HasValue()) {
              errors[worker] = result.GetStatus();
              return;
            }
            const auto view = result.Value();
            if (!view.empty() && view.data() == scratch.data()) {
              if (!reserve(scratch.capacity() + sizeof(std::string))) return;
              auto& owned = owned_text[worker].emplace_back(std::move(scratch));
              text_keys[i] = owned;
            } else {
              text_keys[i] = view.empty() ? std::string_view("") : view;
            }
            hashes[i] = mix(std::hash<std::string_view>{}(text_keys[i]));
            passing[i] = 1;
          } else {
            hashes[i] = mix(0);
            passing[i] = 2;
          }
          observe(i);
          continue;
        }
        auto evaluated = [&]() -> StatusOr<Value> {
          if (!call) return cell(i);
          arguments[0] = cell(i);
          return ExecuteFunction(call->FuncName(), arguments);
        }();
        if (!evaluated.HasValue()) {
          errors[worker] = evaluated.GetStatus();
          return;
        }
        Value canonical = std::move(evaluated.Value());
        if (CollationOf(canonical) == Collation::kCaseInsensitive ||
            canonical.type == ValueType::kDouble)
          canonical = CanonicalDistinctValue(canonical);
        if (!reserve(EstimateValueBytes(canonical))) return;
        keys[i] = std::move(canonical);
        hashes[i] = mix(std::hash<Value>{}(keys[i]));
        passing[i] = 1;
        observe(i);
      }
    }));
    for (const auto& error : errors) {
      RETURN_IF_FAIL(error);
    }
    if (too_large.load(std::memory_order_relaxed)) {
      return false;
    }
    std::vector<size_t> boundaries(workers + 1, 0);
    for (size_t owner = 0; owner < workers; ++owner) {
      size_t next = boundaries[owner];
      for (auto& size : sizes) {
        const size_t count = size[owner];
        size[owner] = next;
        next += count;
      }
      boundaries[owner + 1] = next;
    }
    RETURN_IF_FAIL(RunParallelTasks(workers, [&](size_t worker) {
      const size_t begin = ((input_count / workers) * worker) +
                           std::min(input_count % workers, worker);
      const size_t end = ((input_count / workers) * (worker + 1)) +
                         std::min(input_count % workers, worker + 1);
      for (size_t i = begin; i < end; ++i) {
        if (passing[i]) positions[sizes[worker][hashes[i] % workers]++] = i;
      }
    }));
    phase("evaluate-and-partition");
    std::vector<std::vector<Group>> groups(workers);
    RETURN_IF_FAIL(RunParallelTasks(workers, [&](size_t worker) {
      const size_t begin = boundaries[worker], end = boundaries[worker + 1];
      if (begin == end) return;
      std::vector<size_t> index(std::bit_ceil((end - begin) * 2), SIZE_MAX);
      auto& local = groups[worker];
      // As in phase 1, do not reserve one Group per row up front; grow to the
      // number of distinct keys actually present in this partition.
      local.reserve(std::min<size_t>(end - begin, 4096));
      for (size_t at = begin; at < end; ++at) {
        const size_t i = positions[at];
        size_t bucket = (hashes[i] / workers) & (index.size() - 1);
        while (index[bucket] != SIZE_MAX &&
               (hashes[i] != hashes[local[index[bucket]].first] ||
                !keys_equal(i, local[index[bucket]].first)))
          bucket = (bucket + 1) & (index.size() - 1);
        if (index[bucket] == SIZE_MAX) {
          index[bucket] = local.size();
          local.push_back({i});
        }
        auto& group = local[index[bucket]];
        const size_t source = source_slots[i];
        const auto& partial = partials[source % workers][source / workers];
        group.rows += partial.rows;
        group.nonnull += partial.nonnull;
        group.length_sum += partial.length_sum;
        if (partial.min != SIZE_MAX &&
            (group.min == SIZE_MAX ||
             (dictionary.codes_follow_string_order
                  ? order[partial.min] < order[group.min]
                  : cell(partial.min) < cell(group.min))))
          group.min = partial.min;
        if (partial.max != SIZE_MAX &&
            (group.max == SIZE_MAX ||
             (dictionary.codes_follow_string_order
                  ? order[group.max] < order[partial.max]
                  : cell(group.max) < cell(partial.max))))
          group.max = partial.max;
      }
    }));
    phase("aggregate");
    // Only COUNT predicates can be applied before the caller's generic
    // finalizer. Every admitted argument is non-throwing after key evaluation,
    // and the exact length bound above covers every complete AVG group.
    std::vector<TypedAggregateInput> inputs;
    inputs.reserve(aggregates.size());
    for (const auto* aggregate : aggregates) {
      TypedAggregateInput input;
      input.aggregate = aggregate;
      inputs.push_back(std::move(input));
    }
    auto having = TryCompilePositionalHaving(statement.Having(), inputs);
    if (having &&
        (having->rhs_is_agg ||
         aggregates[having->agg_index]->GetType() != AggregationType::kCount ||
         (having->has_constant && (having->constant.type != ValueType::kInt64 ||
                                   having->constant.IsUnsigned())))) {
      having.reset();
    }
    std::vector<const Group*> output;
    // The published statistics describe the fold itself, not the HAVING
    // filter: every group and every row that reached the aggregation counts,
    // whether or not HAVING kept the group (the sibling dictionary paths
    // publish the same totals).
    size_t folded_groups = 0;
    size_t folded_rows = 0;
    for (const auto& local : groups) {
      for (const auto& group : local) {
        ++folded_groups;
        folded_rows += static_cast<size_t>(group.rows);
        if (having) {
          const int64_t count = IsCountStar(*aggregates[having->agg_index])
                                    ? group.rows
                                    : group.nonnull;
          Value pass(count);
          if (having->has_constant) {
            ASSIGN_OR_RETURN(Value, compared,
                             TryBinary(having->op, pass, having->constant));
            pass = std::move(compared);
          }
          if (!Truthy(pass)) {
            continue;
          }
        }
        output.push_back(&group);
      }
    }
    std::ranges::sort(output, {}, [](const Group* g) { return g->first; });
    std::vector<AggregateAccumulator::Shape> shapes;
    shapes.reserve(aggregates.size());
    for (const auto* aggregate : aggregates) {
      shapes.push_back(AggregateAccumulator::AnalyzeShape(aggregate, &schema));
    }
    for (const Group* group : output) {
      std::vector<AggregateAccumulator> states;
      states.reserve(aggregates.size());
      for (size_t a = 0; a < aggregates.size(); ++a) {
        states.emplace_back(aggregates[a], shapes[a]);
        auto& state = states.back();
        switch (aggregates[a]->GetType()) {
          case AggregationType::kCount:
            state.count =
                IsCountStar(*aggregates[a]) ? group->rows : group->nonnull;
            break;
          case AggregationType::kAvg:
            state.count = group->nonnull;
            state.total = static_cast<double>(group->length_sum);
            break;
          case AggregationType::kMin:
            if (group->min != SIZE_MAX) {
              RETURN_IF_FAIL(state.Add(cell(group->min)));
            }
            break;
          case AggregationType::kMax:
            if (group->max != SIZE_MAX) {
              RETURN_IF_FAIL(state.Add(cell(group->max)));
            }
            break;
          default:
            return Status::kRuntimeError;
        }
      }
      RETURN_IF_FAIL(prepared_consume(input_row(group->first), group->rows,
                                      key_value(group->first), states));
    }
    phase("having-and-publish");
    if (runtime != nullptr) {
      runtime->scan_rows += store->total_rows;
      runtime->scan_values_available += store->total_rows;
      runtime->scan_values_decoded += input_count;
      runtime->scan_output_rows += folded_rows;
      runtime->aggregate_input_rows += folded_rows;
      runtime->aggregate_updates += folded_rows * aggregates.size();
      runtime->aggregate_groups += folded_groups;
      ++runtime->partitioned_dictionary_aggregations;
    }
    return true;
  };
  ASSIGN_OR_RETURN(bool, owned_folded, partitioned_fold());
  if (owned_folded) {
    return true;
  }
  if (runtime != nullptr) {
    runtime->scan_rows += store->total_rows;
    runtime->scan_values_available += store->total_rows;
    runtime->scan_values_decoded += input_count;
  }
  // Pure scalar evaluation and partial aggregation are independent per
  // observed dictionary entry (or source row without a shared dictionary).
  // Keep bounded waves in encounter order, but dynamically distribute small
  // morsels: long strings must not strand one statically assigned worker.
  // Workers never access the transaction or its runtime counters. A value
  // that does not fit the retained staging budget uses the serial callback.
  // NOLINTNEXTLINE(cppcoreguidelines-special-member-functions) guard type is
  // non-copyable by design
  struct PartialGroup {  // NOLINT(cppcoreguidelines-special-member-functions)
    Row row;
    Value key;
    std::vector<AggregateAccumulator> states;
    int64_t rows{0};
    // AggregateAccumulator is move-only (unique_ptr distinct state), so the
    // vector must never fall back to copying on reallocation.
    PartialGroup() = default;
    PartialGroup(PartialGroup&&) = default;
    PartialGroup& operator=(PartialGroup&&) noexcept = default;
  };
  struct LocalGroups {
    GroupKeyMap offsets;
    std::vector<PartialGroup> groups;
  };
  struct Prepared {
    Status status{Status::kSuccess};
    size_t worker{0};
    std::optional<size_t> group;
    bool evaluated{false};
  };
  std::vector<AggregateAccumulator::Shape> shapes;
  shapes.reserve(aggregates.size());
  for (const auto* aggregate : aggregates) {
    shapes.push_back(AggregateAccumulator::AnalyzeShape(aggregate, &schema));
  }
  constexpr size_t kWaveEntries = 4096;
  constexpr size_t kMorselEntries = 64;
  const size_t worker_limit =
      ParallelWorkerCount((input_count / kMorselEntries) +
                          (input_count % kMorselEntries != 0 ? 1 : 0));
  const size_t workspace = std::min<size_t>(
      size_t{64} << 20, QueryMemoryBudget::Global().Remaining() / 4);
  const bool prepare = pure_key && prepared_consume && worker_limit > 1 &&
                       workspace >= kWaveEntries * sizeof(Prepared) * 2 &&
                       QueryMemoryBudget::Global().CanReserve(workspace);
  QueryMemoryCharge staging_charge(prepare ? workspace : 0);
  for (size_t begin = 0; begin < input_count;) {
    const size_t count = std::min(kWaveEntries, input_count - begin);
    std::vector<Prepared> prepared(prepare ? count : 0);
    const size_t workers =
        prepare ? std::min(worker_limit,
                           (count + kMorselEntries - 1) / kMorselEntries)
                : 0;
    std::vector<LocalGroups> local(workers);
    if (prepare) {
      std::atomic<size_t> next{0};
      std::atomic<size_t> retained{count * sizeof(Prepared)};
      const auto evaluate = [&](size_t worker, size_t i) -> Status {
        Row row = input_row(begin + i);
        for (const auto& predicate : predicates) {
          ASSIGN_OR_RETURN(Value, passes, predicate->TryEvaluate(row, schema));
          if (!Truthy(passes)) {
            prepared[i].evaluated = true;
            return Status::kSuccess;
          }
        }
        ASSIGN_OR_RETURN(Value, value, key->TryEvaluate(row, schema));
        // Eligibility above limits arguments to COUNT(*), the input cell,
        // and BYTE_LENGTH(input) (optionally promoted for AVG). Re-entering
        // the AST for those arguments copied the same long string per
        // aggregate. Keep a pointer across the vector-owning Row move below.
        const Value* input = &row[0];
        size_t needed =
            EstimateRowBytes(row) + (2 * EstimateValueBytes(value)) +
            sizeof(PartialGroup) + 128 +
            (aggregates.size() *
             (sizeof(AggregateAccumulator) + EstimateValueBytes(*input)));
        size_t used = retained.load(std::memory_order_relaxed);
        do {
          if (needed > workspace || used > workspace - needed) {
            return Status::kSuccess;
          }
        } while (!retained.compare_exchange_weak(used, used + needed,
                                                 std::memory_order_relaxed));
        auto& state = local[worker];
        std::vector<Value> group_key{CanonicalDistinctValue(value)};
        size_t group = state.groups.size();
        if (auto found = state.offsets.find(group_key);
            found != state.offsets.end()) {
          group = found->second;
        } else {
          state.offsets.emplace(Row(std::move(group_key)), group);
          PartialGroup output;
          output.row = std::move(row);
          output.key = std::move(value);
          output.states.reserve(aggregates.size());
          for (size_t arg = 0; arg < aggregates.size(); ++arg) {
            output.states.emplace_back(aggregates[arg], shapes[arg]);
          }
          state.groups.push_back(std::move(output));
          prepared[i].worker = worker;
          prepared[i].group = group;
        }
        const int64_t repetitions = encoded ? counts[order[begin + i]] : 1;
        auto& output = state.groups[group];
        output.rows += repetitions;
        for (size_t arg = 0; arg < aggregates.size(); ++arg) {
          auto& accumulator = output.states[arg];
          switch (aggregates[arg]->GetType()) {
            case AggregationType::kCount:
              if (IsCountStar(*aggregates[arg]) || !input->IsNull()) {
                accumulator.count += repetitions;
              }
              break;
            case AggregationType::kAvg:
              if (!input->IsNull()) {
                accumulator.count += repetitions;
                accumulator.total +=
                    static_cast<double>(input->AsStringView().size()) *
                    static_cast<double>(repetitions);
              }
              break;
            default:
              RETURN_IF_FAIL(accumulator.Add(*input));
              break;
          }
        }
        prepared[i].evaluated = true;
        return Status::kSuccess;
      };
      RETURN_IF_FAIL(RunParallelTasks(workers, [&](size_t worker) {
        for (;;) {
          const size_t first =
              next.fetch_add(kMorselEntries, std::memory_order_relaxed);
          if (first >= count) {
            break;
          }
          for (size_t i = first; i < std::min(count, first + kMorselEntries);
               ++i) {
            prepared[i].status = evaluate(worker, i);
          }
        }
      }));
    }
    for (size_t i = 0; i < count; ++i) {
      const int64_t repetitions = encoded ? counts[order[begin + i]] : 1;
      if (prepare) {
        const auto& value = prepared[i];
        RETURN_IF_FAIL(value.status);
        if (value.evaluated) {
          if (value.group) {
            const auto& group = local[value.worker].groups[*value.group];
            RETURN_IF_FAIL(prepared_consume(group.row, group.rows, group.key,
                                            group.states));
          }
          continue;
        }
      }
      const Row row = input_row(begin + i);
      RETURN_IF_FAIL(consume(row, repetitions));
    }
    begin += count;
  }
  return true;
} catch (const std::exception& error) {
  return StatusError(StatusCode::kRuntimeError, error.what());
} catch (...) {
  return StatusError(StatusCode::kRuntimeError, "dictionary row fold failed");
}

}  // namespace tinylamb::relational_detail
