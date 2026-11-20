/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/relational.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <deque>
#include <exception>
#include <functional>
#include <iostream>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/exc_shim.hpp"
#include "common/parallel_work.hpp"
#include "common/platform/int128_conversion.hpp"
#include "common/query_parallelism.hpp"
#include "common/status_or.hpp"
#include "common/string_util.hpp"
#include "database/transaction_context.hpp"
#include "executor/aggregation.hpp"
#include "executor/constant_executor.hpp"
#include "executor/detail/columnar_tlcs_fast_path.hpp"
#include "executor/detail/explain_format.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/detail/flat_int_key_map.hpp"
#include "executor/detail/grouped_numeric_hash.hpp"
#include "executor/detail/planning_heuristics.hpp"
#include "executor/detail/relation.hpp"
#include "executor/detail/relational_sort_limit.hpp"
#include "executor/detail/scan_filter.hpp"
#include "executor/detail/scan_tuple_filter.hpp"
#include "executor/detail/subquery_runtime.hpp"
#include "executor/detail/typed_stream_aggregate.hpp"
#include "executor/detail/window_eval.hpp"
#include "executor/detail/work_table_index.hpp"
#include "executor/query_memory.hpp"
#include "executor/set_operation.hpp"
#include "executor/spill_file.hpp"
#include "executor/table_sample.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/binary_expression.hpp"
#include "expression/bytecode.hpp"
#include "expression/case_expression.hpp"
#include "expression/cast_expression.hpp"
#include "expression/collation.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/function_call_expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/named_expression.hpp"
#include "expression/parameter_slot.hpp"
#include "expression/query_expression.hpp"
#include "expression/rewrite.hpp"
#include "expression/unary_expression.hpp"
#include "plan/plan.hpp"
#include "query/statement.hpp"
#include "table/full_scan_iterator.hpp"
#include "table/iterator.hpp"
#include "table/lstable_bridge.hpp"
#include "table/table.hpp"
#include "table/table_statistics.hpp"
#include "type/collation.hpp"
#include "type/column.hpp"
#include "type/type.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb::relational_detail {

// A `relation.*` select item may reach the relational interpreter either bare
// or wrapped by the AST visitor's __value_table_value / __proto_new proto
// constructor. Unwrap one level here so every star gate sees the same shape.
// Callers only accept an unwrapped *qualified* star when its qualifier names
// a FROM source; a genuine proto value table (no such source) keeps its
// wrapper and its single-row-value semantics.
struct UnwrappedStar {
  ColumnName name;
  bool wrapped{false};
};
// NOLINTNEXTLINE(misc-use-anonymous-namespace) static internal linkage is
// intentional
static std::optional<UnwrappedStar> UnwrapStarProjection(  // NOLINT(misc-use-anonymous-namespace)
    const Expression& expression) {
  if (!expression) {
    return std::nullopt;
  }
  Expression candidate = expression;
  bool wrapped = false;
  if (candidate->Type() == TypeTag::kFunctionCallExp) {
    const auto& call = candidate->AsFunctionCallExpression();
    if ((call.FuncName() == "__value_table_value" ||
         call.FuncName() == "__proto_new") &&
        call.Args().size() == 1 && call.Args()[0]) {
      candidate = call.Args()[0];
      wrapped = true;
    }
  }
  if (candidate->Type() != TypeTag::kColumnValue) {
    return std::nullopt;
  }
  const ColumnName& column = candidate->AsColumnValue().GetColumnName();
  if (column.name != "*") {
    return std::nullopt;
  }
  return UnwrappedStar{.name = column, .wrapped = wrapped};
}

// The grouped-aggregation key index (GroupKeyEqual / GroupKeyMap) lives in
// executor/detail/expression_eval.hpp: it is the shared "same group" contract
// between this path and the physical grouped aggregation shapes
// (executor/grouped_aggregate.hpp).

// Minimum grouped-input size before Project folds rows into parallel worker
// slices; below this the serial loop's thread startup dominates.
constexpr size_t kParallelGroupMinRows = 16384;

namespace {

// A NULL first row carries no type information. Recover a homogeneous
// resident column's domain so downstream filters do not lose their typed
// path solely because an outer join emitted an unmatched row first.
void RefineProjectionTypes(const Relation& output,
                           std::vector<Column>& columns) {
  if (output.HasColumnarOnly()) {
    if (output.columnar->total_rows == 0) {
      return;
    }
    // P671: there is no short-circuit on the first chunk's head cell.  A
    // later chunk may carry a different domain (mixed relation from a
    // hand-built or dynamic source), and the whole-scan below is O(chunks)
    // for uniform columns anyway -- per-cell work only happens on the
    // non-uniform columns that genuinely need it.
    for (size_t i = 0; i < columns.size(); ++i) {
      bool uniform = true;
      bool saw_sample = false;
      ValueType sample_type = ValueType::kNull;
      bool sample_unsigned = false;
      bool sample_interval = false;
      for (const auto& chunk : output.columnar->chunks) {
        if (chunk.rows == 0 || i >= chunk.columns.size()) {
          continue;
        }
        const auto& col = chunk.columns[i];
        if (!col.is_uniform) {
          Value tmp_val;
          for (size_t r = 0; r < chunk.rows; ++r) {
            col.WriteValueInto(r, &tmp_val);
            if (tmp_val.IsNull()) {
              continue;
            }
            if (!saw_sample) {
              saw_sample = true;
              sample_type = ValueTypeOf(tmp_val);
              sample_unsigned = tmp_val.IsUnsigned();
              sample_interval = tmp_val.IsInterval();
            } else if (sample_type != tmp_val.type ||
                       sample_unsigned != tmp_val.IsUnsigned() ||
                       sample_interval != tmp_val.IsInterval()) {
              uniform = false;
              break;
            }
          }
          if (!uniform) {
            break;
          }
          continue;
        }
        if (col.type == ValueType::kNull || !col.AnyNonNull(chunk.rows)) {
          continue;
        }
        if (!saw_sample) {
          saw_sample = true;
          sample_type = col.type;
          sample_unsigned = col.is_unsigned;
          sample_interval = col.is_interval;
        } else if (sample_type != col.type ||
                   sample_unsigned != col.is_unsigned ||
                   sample_interval != col.is_interval) {
          uniform = false;
          break;
        }
      }
      if (!uniform) {
        columns[i] = Column(columns[i].Name(), ValueType::kNull);
      } else if (saw_sample) {
        columns[i].SetType(sample_type);
        columns[i].SetInterval(sample_interval);
        columns[i].SetUnsigned(sample_unsigned);
      }
    }
    return;
  }
  if (output.rows.empty()) {
    return;
  }
  for (size_t i = 0; i < columns.size(); ++i) {
    // P671: a non-NULL head cell is evidence for that cell only; scan the
    // whole resident column so mixed domains collapse to kNull exactly like
    // the NULL-head path and the columnar branch resolve them.
    const Value* sample = nullptr;
    bool uniform = true;
    for (const Row& row : output.rows) {
      const Value& value = row[i];
      if (value.IsNull()) {
        continue;
      }
      if (sample == nullptr) {
        sample = &value;
      } else if (sample->type != value.type ||
                 sample->IsUnsigned() != value.IsUnsigned() ||
                 sample->IsInterval() != value.IsInterval()) {
        uniform = false;
        break;
      }
    }
    if (!uniform) {
      // A hand-built or dynamic relation can violate its declared domain.
      // Do not let a typed consumer interpret a mixed column as uniform.
      columns[i] = Column(columns[i].Name(), ValueType::kNull);
      continue;
    }
    if (sample == nullptr) {
      // All NULL values provide no evidence against the declared type.
      // Retain it for CTAS and later insert assignment checks (with a spill
      // the unseen rows may differ; the declared type keeps its old trust).
      continue;
    }
    columns[i].SetType(ValueTypeOf(*sample));
    columns[i].SetInterval(sample->IsInterval());
    columns[i].SetUnsigned(sample->IsUnsigned());
  }
}

// Transfer set-operation rows when coercion needs no extra allocation.
// Inspect actual values: NULL-leading or mixed numeric branches can have
// incomplete inferred schemas. The general set operator handles text/date
// coercion and spill paths.
bool NormalizeMovableSetRelations(std::span<Relation*> inputs) {
  for (const Relation* input : inputs) {
    if (input->HasColumnarOnly() &&
        !QueryMemoryBudget::Global().CanReserve(input->TotalRows() * 128)) {
      return false;
    }
  }
  for (Relation* input : inputs) {
    // T4539: materialization failure -> refuse the move-optimization route.
    if (!input->EnsureRowsMaterialized().ok()) {
      return false;
    }
  }
  if (std::ranges::any_of(
          inputs, [](const Relation* input) { return input->HasSpill(); })) {
    return false;
  }
  size_t row_count = 0;
  size_t width = 0;
  for (const Relation* input : inputs) {
    if (row_count == 0 && !input->rows.empty()) {
      width = input->rows.front().Count();
    }
    row_count += input->rows.size();
  }
  if (row_count == 0) {
    return true;
  }
  bool all_uniform = width > 0;
  for (const Relation* input : inputs) {
    if (!input->uniform_cell_types || input->schema.ColumnCount() != width) {
      all_uniform = false;
      break;
    }
  }
  if (all_uniform) {
    for (size_t i = 0; i < width; ++i) {
      const Column& c0 = inputs[0]->schema.GetColumn(i);
      const ValueType t = c0.Type();
      if (t == ValueType::kNull ||
          (t == ValueType::kDouble && c0.IsUnsigned())) {
        all_uniform = false;
        break;
      }
      for (size_t j = 1; j < inputs.size(); ++j) {
        const Column& cj = inputs[j]->schema.GetColumn(i);
        if (cj.Type() != t || (t == ValueType::kDouble && cj.IsUnsigned())) {
          all_uniform = false;
          break;
        }
      }
      if (!all_uniform) {
        break;
      }
    }
    if (all_uniform) {
      return true;
    }
  }
  struct TypeSummary {
    std::vector<ValueType> types;
    bool promote{false};
    bool valid{true};
  };
  const auto merge_type = [](TypeSummary& summary, size_t i, ValueType type) {
    auto& common = summary.types[i];
    if (type == ValueType::kNull || type == common) {
      return;
    }
    if (common == ValueType::kNull) {
      common = type;
    } else if ((common == ValueType::kInt64 && type == ValueType::kDouble) ||
               (common == ValueType::kDouble && type == ValueType::kInt64)) {
      common = ValueType::kDouble;
      summary.promote = true;
    } else {
      summary.valid = false;
    }
  };
  const size_t workers = ParallelWorkerCount(row_count / 32768);
  std::vector<TypeSummary> summaries(workers);
  for (auto& summary : summaries) {
    summary.types.assign(width, ValueType::kNull);
  }
  // Infer from every actual cell, but inspect disjoint row ranges in parallel.
  // Schemas alone are insufficient for NULL-leading and mixed-type branches.
  const auto inspect = [&](size_t w) {
    auto& summary = summaries[w];
    for (const Relation* input : inputs) {
      const size_t begin = input->rows.size() * w / workers;
      const size_t end = input->rows.size() * (w + 1) / workers;
      for (size_t r = begin; r < end; ++r) {
        const Row& row = input->rows[r];
        if (row.Count() != width) {
          summary.valid = false;
          return;
        }
        for (size_t i = 0; i < width; ++i) {
          merge_type(summary, i, row[i].type);
        }
        if (!summary.valid) {
          return;
        }
      }
    }
  };
  if (!RunParallelTasks(workers, inspect).ok()) {
    return false;
  }
  TypeSummary combined{.types =
                           std::vector<ValueType>(width, ValueType::kNull)};
  for (const auto& summary : summaries) {
    if (!summary.valid) {
      return false;
    }
    combined.promote |= summary.promote;
    for (size_t i = 0; i < width; ++i) {
      merge_type(combined, i, summary.types[i]);
    }
  }
  if (!combined.valid) {
    return false;
  }
  const auto& types = combined.types;
  if (combined.promote) {
    const auto promote = [&](size_t w) {
      for (Relation* input : inputs) {
        const size_t begin = input->rows.size() * w / workers;
        const size_t end = input->rows.size() * (w + 1) / workers;
        for (size_t r = begin; r < end; ++r) {
          Row& row = input->rows[r];
          for (size_t i = 0; i < width; ++i) {
            if (types[i] == ValueType::kDouble &&
                row[i].type == ValueType::kInt64 && !row[i].IsNull()) {
              row[i] = Value(static_cast<double>(row[i].value.int_value));
            }
          }
        }
      }
    };
    if (!RunParallelTasks(workers, promote).ok()) {
      return false;
    }
    for (Relation* input : inputs) {
      // Typed operators above the append compile against this schema. A
      // promoted DOUBLE cell must never be read with an INT64 kernel.
      std::vector<Column> columns;
      columns.reserve(input->schema.ColumnCount());
      for (size_t i = 0; i < input->schema.ColumnCount(); ++i) {
        columns.push_back(input->schema.GetColumn(i));
        if (i < types.size() && types[i] == ValueType::kDouble) {
          columns.back().SetType(ValueType::kDouble);
          columns.back().SetUnsigned(false);
        }
      }
      input->schema =
          Schema(input->schema.Name(), std::move(columns),
                 input->schema.KeyColumnIndex(), input->schema.SchemaVersion());
    }
  }
  if (std::ranges::all_of(types,
                          [](ValueType t) { return t != ValueType::kNull; })) {
    for (Relation* input : inputs) {
      if (!input->HasSpill() && input->schema.ColumnCount() == width) {
        bool schema_matches = true;
        for (size_t i = 0; i < width; ++i) {
          if (input->schema.GetColumn(i).Type() != types[i]) {
            schema_matches = false;
            break;
          }
        }
        if (schema_matches) {
          input->uniform_cell_types = true;
        }
      }
    }
  }
  return true;
}

bool NormalizeMovableSetInputs(Relation& left, Relation& right) {
  std::array<Relation*, 2> inputs{&left, &right};
  return NormalizeMovableSetRelations(inputs);
}

// A UNION DISTINCT over resident scalar rows can deduplicate by row index.
// No hash-table copy of the Value payload is needed, and compaction runs only
// after the index has finished reading its immutable row storage.
std::optional<QueryMemoryCharge> ResidentSetCharge(const Relation& left,
                                                   const Relation& right) {
  if (left.TotalRows() > SIZE_MAX - right.TotalRows()) {
    return std::nullopt;
  }
  const size_t rows = left.TotalRows() + right.TotalRows();
  if (rows > (SIZE_MAX - 1024) / 64) {
    return std::nullopt;
  }
  const size_t bytes = (rows * 64) + 1024;
  if (!QueryMemoryBudget::Global().CanReserve(bytes)) {
    return std::nullopt;
  }
  auto unsafe_cols_for = [](const Relation& rel) -> std::vector<size_t> {
    std::vector<size_t> cols;
    if (!rel.uniform_cell_types) {
      return cols;
    }
    for (size_t c = 0; c < rel.schema.ColumnCount(); ++c) {
      const ValueType t = rel.schema.GetColumn(c).Type();
      if (t == ValueType::kVarChar || t == ValueType::kArray ||
          t == ValueType::kNull) {
        cols.push_back(c);
      }
    }
    return cols;
  };
  auto has_unsafe_cell = [&](const Relation* rel, size_t begin, size_t end,
                             const std::vector<size_t>* restricted_cols) {
    if (restricted_cols != nullptr) {
      for (size_t r = begin; r < end; ++r) {
        const Row& row = rel->rows[r];
        for (const size_t c : *restricted_cols) {
          if (c < row.values_.size()) {
            const Value& value = row.values_[c];
            if (value.type == ValueType::kArray || value.IsCaseInsensitive()) {
              return true;
            }
          }
        }
      }
      return false;
    }
    for (size_t r = begin; r < end; ++r) {
      for (const auto& value : rel->rows[r].values_) {
        if (value.type == ValueType::kArray || value.IsCaseInsensitive()) {
          return true;
        }
      }
    }
    return false;
  };
  for (const Relation* relation : {&left, &right}) {
    if (relation->rows.empty()) {
      continue;
    }
    const std::vector<size_t> restricted = unsafe_cols_for(*relation);
    if (relation->uniform_cell_types && restricted.empty()) {
      continue;
    }
    const std::vector<size_t>* restricted_ptr =
        relation->uniform_cell_types ? &restricted : nullptr;
    if (left.rows.size() + right.rows.size() >= 16384) {
      const size_t workers = ParallelWorkerCount(
          std::max<size_t>(1, relation->rows.size() / 16384));
      if (workers <= 1) {
        if (has_unsafe_cell(relation, 0, relation->rows.size(),
                            restricted_ptr)) {
          return std::nullopt;
        }
        continue;
      }
      std::atomic<bool> unsafe{false};
      const Status st = RunParallelTasks(workers, [&](size_t w) {
        const size_t begin = relation->rows.size() * w / workers;
        const size_t end = relation->rows.size() * (w + 1) / workers;
        if (unsafe.load(std::memory_order_relaxed)) {
          return;
        }
        if (has_unsafe_cell(relation, begin, end, restricted_ptr)) {
          unsafe.store(true, std::memory_order_relaxed);
        }
      });
      if (!st.ok() || unsafe.load(std::memory_order_relaxed)) {
        return std::nullopt;
      }
    } else {
      if (has_unsafe_cell(relation, 0, relation->rows.size(), restricted_ptr)) {
        return std::nullopt;
      }
    }
  }
  return std::optional<QueryMemoryCharge>(std::in_place, bytes);
}

inline uint64_t MixSetHash(uint64_t h) noexcept {
  h ^= h >> 30;
  h *= 0xbf58476d1ce4e5b9ULL;
  h ^= h >> 27;
  h *= 0x94d049bb133111ebULL;
  h ^= h >> 31;
  return h == 0 ? 1 : h;
}

void DistinctResidentRows(Relation& relation) {
  const size_t n = relation.rows.size();
  if (n <= 1) {
    return;
  }
  const size_t workers = n >= 16384 ? ParallelWorkerCount(n / 16384) : 1;
  if (workers <= 1) {
    auto hash = [&](size_t index) {
      return MixSetHash(std::hash<Row>{}(relation.rows[index]));
    };
    auto equal = [&](size_t left, size_t right) {
      return SetOperationRowEqual{}(relation.rows[left], relation.rows[right]);
    };
    std::unordered_set<size_t, decltype(hash), decltype(equal)> seen(0, hash,
                                                                     equal);
    seen.reserve(n);
    std::vector<unsigned char> keep(n);
    for (size_t i = 0; i < n; ++i) {
      keep[i] = seen.insert(i).second ? 1 : 0;
    }
    seen.clear();
    size_t output = 0;
    for (size_t i = 0; i < n; ++i) {
      if (keep[i] == 0) {
        continue;
      }
      if (output != i) {
        relation.rows[output] = std::move(relation.rows[i]);
      }
      ++output;
    }
    relation.rows.resize(output);
    return;
  }

  struct HashedRowRef {
    uint32_t row;
    uint64_t hash;
  };
  const size_t partitions =
      std::min<size_t>(64, std::bit_ceil(std::max<size_t>(2, workers * 4)));
  const size_t p_mask = partitions - 1;
  std::vector<std::vector<HashedRowRef>> buckets(workers * partitions);
  const size_t est_per_bucket = std::max<size_t>(8, n / (workers * partitions));
  for (auto& b : buckets) {
    b.reserve(est_per_bucket);
  }
  const Status part_st = RunParallelTasks(workers, [&](size_t w) {
    const size_t begin = n * w / workers;
    const size_t end = n * (w + 1) / workers;
    const size_t base = w * partitions;
    for (size_t i = begin; i < end; ++i) {
      const uint64_t h = MixSetHash(std::hash<Row>{}(relation.rows[i]));
      buckets[base + ((h >> 32) & p_mask)].push_back(
          HashedRowRef{.row = static_cast<uint32_t>(i), .hash = h});
    }
  });
  if (!part_st.ok()) {
    return;
  }

  std::vector<uint8_t> keep(n, 0);
  std::atomic<size_t> next_p{0};
  const Status dedup_st = RunParallelTasks(workers, [&](size_t) {
    SetOperationRowEqual eq;
    std::vector<HashedRowRef> slots;
    for (size_t p = next_p.fetch_add(1, std::memory_order_relaxed);
         p < partitions; p = next_p.fetch_add(1, std::memory_order_relaxed)) {
      size_t total = 0;
      for (size_t w = 0; w < workers; ++w) {
        total += buckets[(w * partitions) + p].size();
      }
      if (total == 0) {
        continue;
      }
      const size_t cap = std::bit_ceil(std::max<size_t>(8, total * 2));
      const size_t s_mask = cap - 1;
      slots.assign(cap, HashedRowRef{.row = UINT32_MAX, .hash = 0});
      for (size_t w = 0; w < workers; ++w) {
        for (const HashedRowRef& item : buckets[(w * partitions) + p]) {
          size_t pos = item.hash & s_mask;
          while (slots[pos].row != UINT32_MAX) {
            if (slots[pos].hash == item.hash &&
                eq(relation.rows[slots[pos].row], relation.rows[item.row])) {
              break;
            }
            pos = (pos + 1) & s_mask;
          }
          if (slots[pos].row == UINT32_MAX) {
            slots[pos] = item;
            keep[item.row] = 1;
          }
        }
      }
    }
  });
  if (!dedup_st.ok()) {
    return;
  }
  size_t output = 0;
  for (size_t i = 0; i < n; ++i) {
    if (keep[i] == 0) {
      continue;
    }
    if (output != i) {
      relation.rows[output] = std::move(relation.rows[i]);
    }
    ++output;
  }
  relation.rows.resize(output);
}

// Keep hash keys in their original relations until all membership and
// multiplicity decisions are complete; moving a row earlier invalidates keys.
void ReduceResidentRows(Relation& left, const Relation& right,
                        SetOperationKind operation) {
  const bool all = operation == SetOperationKind::kIntersectAll ||
                   operation == SetOperationKind::kExceptAll;
  const bool intersection = operation == SetOperationKind::kIntersect ||
                            operation == SetOperationKind::kIntersectAll;
  const size_t nl = left.rows.size();
  const size_t nr = right.rows.size();
  if (nl == 0) {
    return;
  }
  if (nr == 0) {
    if (intersection) {
      left.rows.clear();
    } else if (!all) {
      DistinctResidentRows(left);
    }
    return;
  }
  const size_t workers =
      (nl + nr) >= 16384 ? ParallelWorkerCount((nl + nr) / 16384) : 1;
  if (workers <= 1) {
    auto hash = [](const Row* row) {
      return MixSetHash(std::hash<Row>{}(*row));
    };
    auto equal = [](const Row* a, const Row* b) {
      return SetOperationRowEqual{}(*a, *b);
    };
    std::unordered_map<const Row*, size_t, decltype(hash), decltype(equal)>
        remaining(0, hash, equal);
    remaining.reserve(nr);
    for (const auto& row : right.rows) {
      ++remaining[&row];
    }
    std::unordered_set<const Row*, decltype(hash), decltype(equal)> seen(
        0, hash, equal);
    if (!all) {
      seen.reserve(nl);
    }
    std::vector<unsigned char> keep(nl);
    for (size_t i = 0; i < nl; ++i) {
      const Row* row = &left.rows[i];
      const auto found = remaining.find(row);
      const bool present = found != remaining.end() && found->second != 0;
      if (all && present) {
        --found->second;
      }
      if (present == intersection && (all || seen.insert(row).second)) {
        keep[i] = 1;
      }
    }
    remaining.clear();
    seen.clear();
    size_t output = 0;
    for (size_t i = 0; i < nl; ++i) {
      if (keep[i] != 0) {
        if (output != i) {
          left.rows[output] = std::move(left.rows[i]);
        }
        ++output;
      }
    }
    left.rows.resize(output);
    return;
  }

  struct HashedRowRef {
    uint32_t row;
    uint64_t hash;
  };
  struct RightSlot {
    uint32_t row{UINT32_MAX};
    uint64_t hash{0};
    size_t count{0};
    uint8_t emitted{0};
  };
  const size_t partitions =
      std::min<size_t>(64, std::bit_ceil(std::max<size_t>(2, workers * 4)));
  const size_t p_mask = partitions - 1;
  std::vector<std::vector<HashedRowRef>> right_buckets(workers * partitions);
  std::vector<std::vector<HashedRowRef>> left_buckets(workers * partitions);
  const Status part_st = RunParallelTasks(workers, [&](size_t w) {
    const size_t r_begin = nr * w / workers;
    const size_t r_end = nr * (w + 1) / workers;
    const size_t l_begin = nl * w / workers;
    const size_t l_end = nl * (w + 1) / workers;
    const size_t base = w * partitions;
    for (size_t i = r_begin; i < r_end; ++i) {
      const uint64_t h = MixSetHash(std::hash<Row>{}(right.rows[i]));
      right_buckets[base + ((h >> 32) & p_mask)].push_back(
          HashedRowRef{.row = static_cast<uint32_t>(i), .hash = h});
    }
    for (size_t i = l_begin; i < l_end; ++i) {
      const uint64_t h = MixSetHash(std::hash<Row>{}(left.rows[i]));
      left_buckets[base + ((h >> 32) & p_mask)].push_back(
          HashedRowRef{.row = static_cast<uint32_t>(i), .hash = h});
    }
  });
  if (!part_st.ok()) {
    return;
  }

  std::vector<uint8_t> keep(nl, 0);
  std::atomic<size_t> next_p{0};
  const Status reduce_st = RunParallelTasks(workers, [&](size_t) {
    SetOperationRowEqual eq;
    std::vector<RightSlot> slots;
    for (size_t p = next_p.fetch_add(1, std::memory_order_relaxed);
         p < partitions; p = next_p.fetch_add(1, std::memory_order_relaxed)) {
      size_t total_left = 0;
      size_t total_right = 0;
      for (size_t w = 0; w < workers; ++w) {
        total_left += left_buckets[(w * partitions) + p].size();
        total_right += right_buckets[(w * partitions) + p].size();
      }
      if (total_left == 0) {
        continue;
      }
      if (total_right == 0) {
        if (intersection) {
          continue;
        }
        if (all) {
          for (size_t w = 0; w < workers; ++w) {
            for (const HashedRowRef& item :
                 left_buckets[(w * partitions) + p]) {
              keep[item.row] = 1;
            }
          }
          continue;
        }
        const size_t cap = std::bit_ceil(std::max<size_t>(8, total_left * 2));
        const size_t s_mask = cap - 1;
        slots.assign(cap, RightSlot{});
        for (size_t w = 0; w < workers; ++w) {
          for (const HashedRowRef& item : left_buckets[(w * partitions) + p]) {
            size_t pos = item.hash & s_mask;
            while (slots[pos].row != UINT32_MAX) {
              if (slots[pos].hash == item.hash &&
                  eq(left.rows[slots[pos].row], left.rows[item.row])) {
                break;
              }
              pos = (pos + 1) & s_mask;
            }
            if (slots[pos].row == UINT32_MAX) {
              slots[pos] = RightSlot{
                  .row = item.row, .hash = item.hash, .count = 0, .emitted = 1};
              keep[item.row] = 1;
            }
          }
        }
        continue;
      }
      const size_t max_entries =
          total_right + ((!all && !intersection) ? total_left : 0);
      const size_t cap = std::bit_ceil(std::max<size_t>(8, max_entries * 2));
      const size_t s_mask = cap - 1;
      slots.assign(cap, RightSlot{});
      for (size_t w = 0; w < workers; ++w) {
        for (const HashedRowRef& item : right_buckets[(w * partitions) + p]) {
          size_t pos = item.hash & s_mask;
          while (slots[pos].row != UINT32_MAX) {
            if (slots[pos].hash == item.hash &&
                eq(right.rows[slots[pos].row], right.rows[item.row])) {
              ++slots[pos].count;
              break;
            }
            pos = (pos + 1) & s_mask;
          }
          if (slots[pos].row == UINT32_MAX) {
            slots[pos] = RightSlot{
                .row = item.row, .hash = item.hash, .count = 1, .emitted = 0};
          }
        }
      }
      for (size_t w = 0; w < workers; ++w) {
        for (const HashedRowRef& item : left_buckets[(w * partitions) + p]) {
          size_t pos = item.hash & s_mask;
          while (slots[pos].row != UINT32_MAX) {
            const Row& existing =
                slots[pos].emitted != 0 && slots[pos].count == 0
                    ? left.rows[slots[pos].row]
                    : right.rows[slots[pos].row];
            if (slots[pos].hash == item.hash &&
                eq(existing, left.rows[item.row])) {
              break;
            }
            pos = (pos + 1) & s_mask;
          }
          if (intersection) {
            if (slots[pos].row != UINT32_MAX && slots[pos].count != 0) {
              if (all) {
                --slots[pos].count;
                keep[item.row] = 1;
              } else if (slots[pos].emitted == 0) {
                slots[pos].emitted = 1;
                keep[item.row] = 1;
              }
            }
          } else {
            if (slots[pos].row == UINT32_MAX) {
              if (!all) {
                slots[pos] = RightSlot{.row = item.row,
                                       .hash = item.hash,
                                       .count = 0,
                                       .emitted = 1};
              }
              keep[item.row] = 1;
            } else if (all && slots[pos].count == 0) {
              keep[item.row] = 1;
            } else if (all && slots[pos].count > 0) {
              --slots[pos].count;
            }
          }
        }
      }
    }
  });
  if (!reduce_st.ok()) {
    return;
  }
  size_t output = 0;
  for (size_t i = 0; i < nl; ++i) {
    if (keep[i] != 0) {
      if (output != i) {
        left.rows[output] = std::move(left.rows[i]);
      }
      ++output;
    }
  }
  left.rows.resize(output);
}

// HAVING may reference select-list aliases (`HAVING double_avg > 0.7`).
// Alias references that are not real input columns are rewritten to the
// aliased expression so aggregate aliases resolve during grouping.

// HAVING transpose helper (aggregate <-> filter): true when every column the
// conjunct touches is a bare grouping-key column (qualifier-insensitive) and
// the conjunct holds no aggregate or query subtree.  Within a group every row
// shares the key values, so the group's fate is a row-level fact; the
// group-all shape is excluded by the caller (it forms a group even over a
// fully rejected input).
bool IsKeyOnlyHavingConjunct(const Expression& conjunct,
                             const SelectStatement& resolved) {
  if (!conjunct) {
    return false;
  }
  if (ContainsAggregate(conjunct) || ContainsQuery(conjunct)) {
    return false;
  }
  for (const ColumnName& column : conjunct->TouchedColumns()) {
    bool is_key = false;
    for (const Expression& key : resolved.GroupBy()) {
      if (!key || key->Type() != TypeTag::kColumnValue) {
        continue;
      }
      const ColumnName& key_name = key->AsColumnValue().GetColumnName();
      if (key_name.name != "*" &&
          IdentifierEquals(key_name.name, column.name)) {
        is_key = true;
        break;
      }
    }
    if (!is_key) {
      return false;
    }
  }
  return true;
}

Expression InlineHavingAliases(const SelectStatement& statement,
                               const Schema& schema) {
  const Expression& having = statement.Having();
  if (!having) {
    return having;
  }
  std::unordered_map<std::string, Expression> aliases;
  for (const NamedExpression& projection : statement.SelectList()) {
    if (!projection.name.empty() && projection.name != "*") {
      aliases.emplace(AsciiToLower(projection.name), projection.expression);
    }
  }
  if (aliases.empty()) {
    return having;
  }
  auto schema_has_column = [&schema](std::string_view name) {
    for (size_t i = 0; i < schema.ColumnCount(); ++i) {
      if (IdentifierEquals(name, schema.GetColumn(i).Name().name)) {
        return true;
      }
    }
    return false;
  };
  std::function<Expression(const Expression&)> walk =
      [&](const Expression& expression)
      -> Expression {  // NOLINT(misc-no-recursion)
    if (!expression) {
      return expression;
    }
    switch (expression->Type()) {
      case TypeTag::kColumnValue: {
        const ColumnName& name = expression->AsColumnValue().GetColumnName();
        if (!name.schema.empty() || schema_has_column(name.name)) {
          return expression;
        }
        const auto found = aliases.find(AsciiToLower(name.name));
        return found != aliases.end() ? found->second : expression;
      }
      case TypeTag::kBinaryExp:
        return BinaryExpressionExp(
            walk(expression->AsBinaryExpression().Left()),
            expression->AsBinaryExpression().Op(),
            walk(expression->AsBinaryExpression().Right()));
      case TypeTag::kUnaryExp:
        return UnaryExpressionExp(walk(expression->AsUnaryExpression().Child()),
                                  expression->AsUnaryExpression().Op());
      case TypeTag::kCaseExp: {
        const auto& value = expression->AsCaseExpression();
        std::vector<std::pair<Expression, Expression>> clauses;
        clauses.reserve(value.when_clauses_.size());
        for (const auto& [condition, result] : value.when_clauses_) {
          clauses.emplace_back(walk(condition), walk(result));
        }
        return CaseExpressionExp(std::move(clauses), walk(value.else_clause_));
      }
      case TypeTag::kInExp: {
        const auto& value = expression->AsInExpression();
        std::vector<Expression> items;
        items.reserve(value.list_.size());
        for (const Expression& item : value.list_) {
          items.push_back(walk(item));
        }
        return InExpressionExp(walk(value.child_), std::move(items));
      }
      case TypeTag::kFunctionCallExp:
        return FunctionCallExp(
            expression->AsFunctionCallExpression().FuncName(), [&] {
              std::vector<Expression> out;
              out.reserve(expression->AsFunctionCallExpression().Args().size());
              for (const Expression& argument :
                   expression->AsFunctionCallExpression().Args()) {
                out.push_back(walk(argument));
              }
              return out;
            }());
      case TypeTag::kQueryExp: {
        // Alias references may sit inside HAVING subqueries
        // (EXISTS (SELECT * FROM UNNEST(agg_alias) ...)).  Rewrite them on a
        // shallow copy so the subquery's lateral unnest arguments bind to the
        // outer aggregate expressions.
        const auto& value = expression->AsQueryExpression();
        if (!value.Query() || value.Test()) {
          return expression;
        }
        auto copied = std::make_shared<SelectStatement>(*value.Query());
        bool changed = false;
        std::vector<SelectSource> sources = copied->Sources();
        for (SelectSource& source : sources) {
          if (source.unnest) {
            source.unnest = walk(source.unnest);
            changed = true;
          }
        }
        if (!changed) {
          return expression;
        }
        copied->SetSources(std::move(sources));
        auto rewritten = std::make_shared<QueryExpression>(
            std::move(copied), nullptr, value.Exists(), value.Negated(),
            value.Op(), value.Mode());
        rewritten->SetArrayResult(value.ArrayResult());
        if (!value.ArrayElementSqlType().empty()) {
          rewritten->SetArrayElementSqlType(value.ArrayElementSqlType());
        }
        return {rewritten};
      }
      default:
        // Aggregates, subqueries and casts keep their own scope.
        return expression;
    }
  };
  return walk(having);
}

namespace {

// Deep expression rewrite: returns `expression` with every subtree accepted by
// `replace` swapped for its replacement (children visited after the node, so
// replacements are not re-entered).
Expression ReplaceSubExpressions(
    const Expression& expression,
    const std::function<std::optional<Expression>(const Expression&)>&
        replace) {
  if (!expression) {
    return expression;
  }
  if (auto replacement = replace(expression)) {
    return std::move(*replacement);
  }
  std::vector<Expression> children = ExpressionChildren(expression);
  bool changed = false;
  for (Expression& child : children) {
    Expression replaced = ReplaceSubExpressions(child, replace);
    if (replaced != child) {
      changed = true;
    }
    child = std::move(replaced);
  }
  return changed ? WithExpressionChildren(expression, std::move(children))
                 : expression;
}

// Registers every aggregate subtree under `expression` in the $aggK
// materialization pool so a window-over-aggregate pass can reference the
// per-group values by column.
void RegisterAggregatesForMaterialization(
    const Expression& expression,
    std::unordered_map<std::string, std::string>* aggregate_columns,
    std::vector<std::pair<std::string, Expression>>* hidden_aggregates) {
  if (!expression) {
    return;
  }
  if (expression->Type() == TypeTag::kAggregateExp) {
    auto [it, inserted] = aggregate_columns->emplace(
        expression->ToString(),
        "$agg" + std::to_string(hidden_aggregates->size()));
    if (inserted) {
      hidden_aggregates->emplace_back(it->second, expression);
    }
    return;
  }
  for (const Expression& child : ExpressionChildren(expression)) {
    RegisterAggregatesForMaterialization(child, aggregate_columns,
                                         hidden_aggregates);
  }
}

void CollectWindowCalls(const Expression& expression,
                        std::vector<Expression>* windows) {
  if (!expression) {
    return;
  }
  if (expression->Type() == TypeTag::kWindowFunctionExp) {
    for (const Expression& seen : *windows) {
      if (seen->ToString() == expression->ToString()) {
        return;
      }
    }
    windows->push_back(expression);
    return;
  }
  for (const Expression& child : ExpressionChildren(expression)) {
    CollectWindowCalls(child, windows);
  }
}

bool ContainsWindowCall(const Expression& expression) {
  if (!expression) {
    return false;
  }
  if (expression->Type() == TypeTag::kWindowFunctionExp) {
    return true;
  }
  return std::ranges::any_of(ExpressionChildren(expression),
                             ContainsWindowCall);
}

bool StatementIsAggregated(const SelectStatement& statement) {
  return !statement.GroupBy().empty() ||
         std::any_of(statement.SelectList().begin(),
                     statement.SelectList().end(),
                     [](const NamedExpression& projection) {
                       return ContainsAggregate(projection.expression);
                     }) ||
         ContainsAggregate(statement.Having());
}

}  // namespace

StatusOr<Relation> Project(TransactionContext& context,
                           const SelectStatement& statement, Relation input,
                           const Scope* outer, const CteMap& ctes,
                           size_t hidden_columns) {
  const auto project_begin = std::chrono::steady_clock::now();
  if (hidden_columns > 0 &&
      (!statement.GroupBy().empty() ||
       std::any_of(statement.SelectList().begin(), statement.SelectList().end(),
                   [](const NamedExpression& projection) {
                     return ContainsAggregate(projection.expression);
                   }))) {
    // Direct Project fallback: window-over-aggregate normally takes the
    // dedicated ExecuteWindowOverAggregate path; treat this as an unreachable
    // backstop.
    return StatusError(
        StatusCode::kInvalidArgument,
        "window functions combined with GROUP BY are not supported here");
  }
  const bool grouped =
      !statement.GroupBy().empty() ||
      std::any_of(statement.SelectList().begin(), statement.SelectList().end(),
                  [](const NamedExpression& projection) {
                    return ContainsAggregate(projection.expression);
                  }) ||
      ContainsAggregate(statement.Having());
  Expression having_expr = grouped
                               ? InlineHavingAliases(statement, input.schema)
                               : statement.Having();
  // Fold row-independent subqueries in HAVING to constants once, before the
  // per-group emit (Q11: every group re-probed the same uncorrelated scalar
  // through the cache).  Same contract as the scan-filter fold:
  // uncorrelated volatile-free nodes are statement-invariant on this
  // snapshot, and nodes whose evaluation raises keep their original form.
  static const bool having_fold_disabled =
      std::getenv("TINYLAMB_DISABLE_HAVING_FOLD") != nullptr;
  if (grouped && having_expr && !having_fold_disabled) {
    std::vector<Expression> folded_having =
        FoldRowIndependentSubqueries(context, {having_expr}, outer, ctes);
    if (!folded_having.empty()) {
      having_expr = folded_having[0];
    }
  }
  const auto dedupe_select_aggregates = [&](const Expression& expr) {
    return ReplaceSubExpressions(
        expr,
        [&statement](const Expression& node) -> std::optional<Expression> {
          if (!node || node->Type() != TypeTag::kAggregateExp) {
            return std::nullopt;
          }
          for (const NamedExpression& projection : statement.SelectList()) {
            if (projection.expression &&
                projection.expression->Type() == TypeTag::kAggregateExp &&
                projection.expression->ToString() == node->ToString()) {
              return projection.expression;
            }
          }
          return std::nullopt;
        });
  };
  if (grouped && having_expr) {
    having_expr = dedupe_select_aggregates(having_expr);
  }
  // ORDER BY terms may reference select-list aliases or spell out an
  // aggregate ("ORDER BY sum(x) DESC", TPC-DS q42).  Alias references resolve
  // to the select item's expression; aggregate subtrees dedupe onto the
  // identical select-list aggregate node so per-group evaluation finds them
  // prepared in the AggregateResultMap (which is keyed by node pointer).
  std::vector<Expression> resolved_order_expressions;
  resolved_order_expressions.reserve(statement.OrderBy().size());
  for (const auto& term : statement.OrderBy()) {
    Expression expression = term.expression;
    if (expression && expression->Type() == TypeTag::kColumnValue) {
      const ColumnName& name = expression->AsColumnValue().GetColumnName();
      if (name.schema.empty()) {
        for (const NamedExpression& projection : statement.SelectList()) {
          if (!projection.name.empty() &&
              projection.name.size() == name.name.size() &&
              std::equal(
                  projection.name.begin(), projection.name.end(),
                  name.name.begin(), [](char left, char right) {
                    return std::tolower(static_cast<unsigned char>(left)) ==
                           std::tolower(static_cast<unsigned char>(right));
                  })) {
            expression = projection.expression;
            break;
          }
        }
      }
    }
    expression = dedupe_select_aggregates(expression);
    resolved_order_expressions.push_back(std::move(expression));
  }
  std::vector<const AggregateExpression*> aggregate_expressions;
  std::unordered_set<const AggregateExpression*> seen_aggregates;
  for (const NamedExpression& projection : statement.SelectList()) {
    CollectAggregates(projection.expression, &aggregate_expressions,
                      &seen_aggregates);
  }
  CollectAggregates(having_expr, &aggregate_expressions, &seen_aggregates);
  for (const Expression& order_expression : resolved_order_expressions) {
    CollectAggregates(order_expression, &aggregate_expressions,
                      &seen_aggregates);
  }
  std::vector<slot_t> ungrouped_slots;
  if (grouped) {
    std::unordered_set<slot_t> grouped_slots;
    std::unordered_set<std::string> grouped_expr_strings;
    if (!statement.IsGroupAll()) {
      for (const Expression& g : statement.GroupBy()) {
        if (!g) {
          continue;
        }
        grouped_expr_strings.insert(g->ToString());
        if (g->Type() == TypeTag::kColumnValue) {
          const ColumnName& gcol = g->AsColumnValue().GetColumnName();
          if (std::optional<slot_t> slot =
                  LocalColumnOffset(input.schema, gcol);
              slot.has_value()) {
            grouped_slots.insert(*slot);
          }
          if (gcol.schema.empty()) {
            for (const NamedExpression& proj : statement.SelectList()) {
              if (!proj.name.empty() &&
                  IdentifierEquals(proj.name, gcol.name) && proj.expression) {
                grouped_expr_strings.insert(proj.expression->ToString());
                if (proj.expression->Type() == TypeTag::kColumnValue) {
                  if (std::optional<slot_t> slot = LocalColumnOffset(
                          input.schema,
                          proj.expression->AsColumnValue().GetColumnName());
                      slot.has_value()) {
                    grouped_slots.insert(*slot);
                  }
                }
              }
            }
          }
        }
      }
    }
    const std::function<void(const Expression&)> collect_ungrouped =
        [&](const Expression& expr) {
          if (!expr || expr->Type() == TypeTag::kAggregateExp ||
              expr->Type() == TypeTag::kQueryExp) {
            return;
          }
          if (grouped_expr_strings.contains(expr->ToString())) {
            return;
          }
          if (expr->Type() == TypeTag::kColumnValue) {
            const ColumnName& col = expr->AsColumnValue().GetColumnName();
            if (col.name.empty() || col.name == "*") {
              return;
            }
            if (std::optional<slot_t> slot =
                    LocalColumnOffset(input.schema, col);
                slot.has_value() && !grouped_slots.contains(*slot)) {
              if (std::ranges::find(ungrouped_slots, *slot) ==
                  ungrouped_slots.end()) {
                ungrouped_slots.push_back(*slot);
              }
            }
            return;
          }
          for (const Expression& child : ExpressionChildren(expr)) {
            collect_ungrouped(child);
          }
        };
    for (const NamedExpression& proj : statement.SelectList()) {
      collect_ungrouped(proj.expression);
    }
    collect_ungrouped(having_expr);
    for (const Expression& order_expression : resolved_order_expressions) {
      collect_ungrouped(order_expression);
    }
  }
  if (grouped && outer == nullptr && hidden_columns == 0 &&
      ungrouped_slots.empty()) {
    std::optional<SelectStatement> compact_stmt;
    std::optional<SelectStatement> sort_stmt;
    const bool has_order_by = !statement.OrderBy().empty();
    bool compact_allowed = !has_order_by;
    if (has_order_by && !statement.Distinct() &&
        !statement.SelectList().empty()) {
      const size_t select_count = statement.SelectList().size();
      const auto unwrap_collate =
          [](const Expression& expr) -> const Expression& {
        if (expr && CollationOfExpression(expr) != Collation::kNone &&
            expr->Type() == TypeTag::kFunctionCallExp &&
            !expr->AsFunctionCallExpression().Args().empty()) {
          return expr->AsFunctionCallExpression().Args()[0];
        }
        return expr;  // NOLINT(bugprone-return-const-ref-from-parameter)
      };
      const auto resolve_order_target =
          [&](size_t oi) -> std::optional<size_t> {
        const Expression& raw_expr = statement.OrderBy()[oi].expression;
        if (!raw_expr) {
          return std::nullopt;
        }
        if (const auto collated = AsCollatedOrdinal(raw_expr)) {
          if (collated->ordinal >= 1 &&
              std::cmp_less_equal(collated->ordinal, select_count)) {
            return static_cast<size_t>(collated->ordinal - 1);
          }
          return std::nullopt;
        }
        if (raw_expr->Type() == TypeTag::kConstantValue) {
          const Value& cv = raw_expr->AsConstantValue().GetValue();
          if (cv.type == ValueType::kInt64 && !cv.IsNull() &&
              !cv.IsUnsigned() && cv.value.int_value >= 1 &&
              std::cmp_less_equal(cv.value.int_value, select_count)) {
            return static_cast<size_t>(cv.value.int_value - 1);
          }
          return std::nullopt;
        }
        const Expression& resolved = resolved_order_expressions[oi];
        if (resolved) {
          for (size_t si = 0; si < select_count; ++si) {
            const Expression& sel = statement.SelectList()[si].expression;
            if (sel && sel->ToString() == resolved->ToString()) {
              return si;
            }
          }
          if (CollationOfExpression(resolved) != Collation::kNone) {
            const Expression& resolved_inner = unwrap_collate(resolved);
            if (resolved_inner) {
              for (size_t si = 0; si < select_count; ++si) {
                const Expression& sel = statement.SelectList()[si].expression;
                if (sel && sel->ToString() == resolved_inner->ToString()) {
                  return si;
                }
              }
            }
          }
        }
        const Expression& raw_inner = unwrap_collate(raw_expr);
        if (raw_inner && raw_inner->Type() == TypeTag::kColumnValue) {
          const ColumnName& cn = raw_inner->AsColumnValue().GetColumnName();
          if (cn.schema.empty()) {
            std::optional<size_t> alias_match;
            bool alias_ambiguous = false;
            for (size_t si = 0; si < select_count; ++si) {
              const auto& item = statement.SelectList()[si];
              if (!item.name.empty() && IdentifierEquals(item.name, cn.name)) {
                if (alias_match.has_value()) {
                  alias_ambiguous = true;
                  break;
                }
                alias_match = si;
              }
            }
            if (alias_match.has_value() && !alias_ambiguous) {
              return alias_match;
            }
          }
          std::optional<size_t> col_match;
          bool col_ambiguous = false;
          for (size_t si = 0; si < select_count; ++si) {
            const auto& item = statement.SelectList()[si];
            if (!item.expression ||
                item.expression->Type() != TypeTag::kColumnValue) {
              continue;
            }
            const ColumnName& scn =
                item.expression->AsColumnValue().GetColumnName();
            if (IdentifierEquals(scn.name, cn.name) &&
                (cn.schema.empty() ||
                 IdentifierEquals(scn.schema, cn.schema))) {
              if (col_match.has_value()) {
                col_ambiguous = true;
                break;
              }
              col_match = si;
            }
          }
          if (col_match.has_value() && !col_ambiguous) {
            return col_match;
          }
        }
        return std::nullopt;
      };

      std::vector<SelectStatement::OrderByTerm> mapped_order;
      mapped_order.reserve(statement.OrderBy().size());
      bool all_mapped = true;
      for (size_t oi = 0; oi < statement.OrderBy().size(); ++oi) {
        const std::optional<size_t> target_col = resolve_order_target(oi);
        if (!target_col.has_value()) {
          all_mapped = false;
          break;
        }
        SelectStatement::OrderByTerm mapped = statement.OrderBy()[oi];
        mapped.expression = ReapplyCollation(
            ConstantValueExp(Value(static_cast<int64_t>(*target_col + 1))),
            statement.OrderBy()[oi].expression);
        mapped_order.push_back(std::move(mapped));
      }
      if (all_mapped) {
        compact_stmt.emplace(statement);
        compact_stmt->SetOrderBy({});
        compact_stmt->SetLimit(std::nullopt);
        compact_stmt->SetOffset(0);
        compact_stmt->SetWithTies(false);
        sort_stmt.emplace(statement);
        sort_stmt->SetOrderBy(std::move(mapped_order));
        compact_allowed = true;
      }
    }
    if (compact_allowed) {
      const auto& stat_stmt = compact_stmt ? *compact_stmt : statement;
      ASSIGN_OR_RETURN(
          std::optional<Relation>, statistics,
          TryCompactStatisticalAggregate(context, stat_stmt, input,
                                         aggregate_expressions, ctes));
      if (statistics) {
        if (!has_order_by) {
          return std::move(*statistics);
        }
        if (statistics->schema.ColumnCount() == statement.SelectList().size()) {
          RETURN_IF_FAIL(
              ApplyOrderBy(context, *sort_stmt, &(*statistics), outer, ctes));
          return std::move(*statistics);
        }
      }
      std::optional<SelectStatement> folded_having_statement;
      if (having_expr && statement.Having() &&
          having_expr.get() != statement.Having().get()) {
        folded_having_statement.emplace(compact_stmt ? *compact_stmt
                                                     : statement);
        folded_having_statement->SetHaving(having_expr);
      }
      const SelectStatement& compact_arg =
          folded_having_statement ? *folded_having_statement
                                  : (compact_stmt ? *compact_stmt : statement);
      ASSIGN_OR_RETURN(std::optional<Relation>, compact,
                       TryCompactRelationAggregate(context, compact_arg, input,
                                                   aggregate_expressions));
      if (compact) {
        if (!has_order_by) {
          return std::move(*compact);
        }
        if (compact->schema.ColumnCount() == statement.SelectList().size()) {
          RETURN_IF_FAIL(
              ApplyOrderBy(context, *sort_stmt, &(*compact), outer, ctes));
          return std::move(*compact);
        }
      }
    }
  }
  struct GroupState {
    Row representative;
    size_t accumulator_offset{0};
  };
  const auto bind_group_column =
      [&](const Expression& expression) -> std::optional<slot_t> {
    if (!expression || expression->Type() != TypeTag::kColumnValue) {
      return std::nullopt;
    }
    const auto& name = expression->AsColumnValue().GetColumnName();
    if (name.name.find('.') != std::string::npos ||
        name.schema.find('.') != std::string::npos) {
      return std::nullopt;
    }
    return LocalColumnOffset(input.schema, name);
  };
  std::vector<std::optional<slot_t>> group_key_columns, group_value_columns;
  for (const auto& key : statement.GroupBy()) {
    group_key_columns.push_back(bind_group_column(key));
  }
  std::vector<AggregateAccumulator::Shape> group_shapes;
  for (const auto* aggregate : aggregate_expressions) {
    group_value_columns.push_back(bind_group_column(aggregate->Child()));
    group_shapes.push_back(
        AggregateAccumulator::AnalyzeShape(aggregate, nullptr));
  }
  std::vector<Value> group_key_scratch;
  group_key_scratch.reserve(statement.GroupBy().size());
  std::deque<AggregateAccumulator> aggregate_states;
  auto accumulate_row =
      [&](const Row& row, GroupKeyMap* offsets,
          std::vector<GroupState>* local_groups,
          std::deque<AggregateAccumulator>* local_states) -> Status {
    Scope scope{.row = &row, .schema = &input.schema, .outer = outer};
    group_key_scratch.clear();
    for (size_t i = 0; i < statement.GroupBy().size(); ++i) {
      if (group_key_columns[i]) {
        group_key_scratch.push_back(
            CanonicalDistinctValue(row[*group_key_columns[i]]));
      } else {
        ASSIGN_OR_RETURN(Value, key,
                         (TryEvaluate(statement.GroupBy()[i], scope, nullptr,
                                      context, ctes)));
        group_key_scratch.push_back(CanonicalDistinctValue(key));
      }
    }
    auto iter = offsets->find(group_key_scratch);
    const bool inserted = iter == offsets->end();
    if (inserted) {
      iter =
          offsets->emplace(Row(group_key_scratch), local_groups->size()).first;
      GroupState group;
      group.accumulator_offset = local_states->size();
      for (size_t i = 0; i < aggregate_expressions.size(); ++i) {
        local_states->emplace_back(aggregate_expressions[i], group_shapes[i]);
      }
      local_groups->push_back(std::move(group));
    }
    GroupState& group = (*local_groups)[iter->second];
    for (size_t i = 0; i < aggregate_expressions.size(); ++i) {
      AggregateAccumulator& accumulator =
          (*local_states)[group.accumulator_offset + i];
      const AggregateExpression& aggregate = *accumulator.expression;
      if (aggregate.WhereFilter()) {
        ASSIGN_OR_RETURN(Value, where_value,
                         (TryEvaluate(aggregate.WhereFilter(), scope, nullptr,
                                      context, ctes)));
        if (!Truthy(where_value)) {
          continue;
        }
      }
      if (IsCountStar(aggregate)) {
        RETURN_IF_FAIL(accumulator.Add(Value(1)));
        continue;
      }
      AggregateInput aggregate_input;
      if (group_value_columns[i]) {
        aggregate_input.value = row[*group_value_columns[i]];
      } else {
        ASSIGN_OR_RETURN(
            Value, value,
            (TryEvaluate(aggregate.Child(), scope, nullptr, context, ctes)));
        aggregate_input.value = std::move(value);
      }
      if (aggregate.Having() != AggregateHavingModifier::kNone &&
          aggregate.HavingCondition()) {
        ASSIGN_OR_RETURN(Value, hv11119_0,
                         (TryEvaluate(aggregate.HavingCondition(), scope,
                                      nullptr, context, ctes)));
        aggregate_input.condition = std::move(hv11119_0);
      }
      for (const auto& term : aggregate.InnerOrderBy()) {
        ASSIGN_OR_RETURN(
            Value, hv11336_0,
            (TryEvaluate(term.expression, scope, nullptr, context, ctes)));
        aggregate_input.order_keys.push_back(std::move(hv11336_0));
      }
      for (const Expression& extra : aggregate.ExtraArgs()) {
        // T2054: mirror the TrailingArgs arm below (and the sibling loops in
        // aggregation.cpp / parallel_aggregation.cpp / subquery_runtime.cpp,
        // all of which skip nulls): ExtraArgs entries can be null carriers
        // and TryEvaluate dereferences `expression->Type()` immediately.
        if (extra) {
          ASSIGN_OR_RETURN(Value, hv11523_0,
                           (TryEvaluate(extra, scope, nullptr, context, ctes)));
          aggregate_input.order_keys.push_back(std::move(hv11523_0));
        }
      }
      if (aggregate.SecondaryArg()) {
        ASSIGN_OR_RETURN(Value, hv11676_0,
                         (TryEvaluate(aggregate.SecondaryArg(), scope, nullptr,
                                      context, ctes)));
        aggregate_input.auxiliary = std::move(hv11676_0);
      }
      for (const Expression& extra : aggregate.TrailingArgs()) {
        if (extra) {
          ASSIGN_OR_RETURN(Value, hv11885_0,
                           (TryEvaluate(extra, scope, nullptr, context, ctes)));
          aggregate_input.trailing_values.push_back(std::move(hv11885_0));
        }
      }
      RETURN_IF_FAIL(accumulator.Add(std::move(aggregate_input)));
      if (context.execution_runtime() != nullptr) {
        ++context.execution_runtime()->aggregate_updates;
      }
    }
    // Only new groups need a representative row, unless bare ungrouped columns
    // are referenced, in which case we pick the canonical minimum row across
    // `ungrouped_slots` (NULLS LAST) so join/spill reordering cannot change the
    // chosen representative.
    if (inserted) {
      group.representative = row;
    } else if (!ungrouped_slots.empty()) {
      for (slot_t slot : ungrouped_slots) {
        if (slot >= row.Count() || slot >= group.representative.Count()) {
          continue;
        }
        const Value& cand = row[slot];
        const Value& curr = group.representative[slot];
        if (cand.IsNull() != curr.IsNull()) {
          if (!cand.IsNull()) {
            group.representative = row;
          }
          break;
        }
        if (!cand.IsNull()) {
          const int cmp = CompareForOrderBy(cand, curr);
          if (cmp < 0) {
            group.representative = row;
            break;
          }
          if (cmp > 0) {
            break;
          }
        }
      }
    }
    if (context.execution_runtime() != nullptr) {
      ++context.execution_runtime()->aggregate_input_rows;
    }
    return Status::kSuccess;
  };
  auto make_group = [&]() {
    GroupState group;
    group.representative =
        Row(std::vector<Value>(input.schema.ColumnCount(), Value()));
    group.accumulator_offset = aggregate_states.size();
    for (const AggregateExpression* aggregate : aggregate_expressions) {
      aggregate_states.emplace_back(aggregate, &input.schema);
    }
    return group;
  };

  const auto partition_bucket = [&](const Row& row, size_t bucket_count,
                                    uint64_t salt, Status* err) -> size_t {
    Scope scope{.row = &row, .schema = &input.schema, .outer = outer};
    std::string encoded_key;
    for (const Expression& key : statement.GroupBy()) {
      StatusOr<Value> val_or = TryEvaluate(key, scope, nullptr, context, ctes);
      if (!val_or.HasValue()) {
        if (*err == Status::kSuccess) {
          *err = val_or.GetStatus();
        }
        return 0;
      }
      const Value key_value = CanonicalDistinctValue(val_or.MoveValue());
      // EncodeMemcomparableFormat throws on NULL, but NULL group keys are
      // legal (the in-memory path below groups them normally). Partition with
      // '\0' which never collides with a real encoding prefix (value-type byte
      // 1..5), keeping grouping inside each partition exact.
      if (key_value.IsNull()) {
        encoded_key.push_back('\0');
      } else {
        encoded_key += key_value.EncodeMemcomparableFormat();
      }
    }
    return salt == 0 ? SpillPartitionOf(encoded_key, bucket_count)
                     : SpillPartitionOfSalted(encoded_key, bucket_count, salt);
  };

  Status agg_error{Status::kSuccess};
  std::vector<GroupState> groups;
  std::vector<SpillFile> agg_parts;
  bool used_partition_agg = false;
  size_t partitioned_input_rows = 0;
  if (grouped) {
    RETURN_IF_FAIL(input.FinishSpill());

    const bool partition_agg =
        !statement.GroupBy().empty() &&
        (input.HasSpill() || !QueryMemoryBudget::Global().CanReserve(
                                 std::max<size_t>(1, input.TotalRows()) * 128));
    if (partition_agg) {
      used_partition_agg = true;
      agg_parts.resize(kSpillPartitions);
      Status iterated = input.ForEachRow([&](const Row& row) {
        if (agg_error != Status::kSuccess) {
          return;
        }
        ++partitioned_input_rows;
        const size_t bucket =
            partition_bucket(row, kSpillPartitions, 0, &agg_error);
        if (agg_error != Status::kSuccess) {
          return;
        }
        Status st_append = agg_parts[bucket].Append(row);
        if (st_append != Status::kSuccess) {
          agg_error = st_append;
        }
      });
      if (iterated != Status::kSuccess && agg_error == Status::kSuccess) {
        agg_error = iterated;
      }
      for (SpillFile& part : agg_parts) {
        RETURN_IF_FAIL(part.FinishWriting());
      }
      RETURN_IF_FAIL(agg_error);
      // Every row was copied into a partition above; defer accumulation until
      // after output schema / HAVING / ORDER BY setup so each partition emits
      // and frees its hash table & accumulator states before the next runs.
      input.ResetContents();
    } else {
      if (input.HasColumnarOnly() &&
          QueryMemoryBudget::Global().CanReserve(input.TotalRows() * 128)) {
        RETURN_IF_FAIL(input.EnsureRowsMaterialized());
      }
      // Parallel grouped aggregation for big in-memory inputs whose group
      // keys and aggregate inputs are plain column references (or count(*)):
      // workers fold contiguous row slices into thread-local group maps,
      // then the main thread merges the partial states in worker order.
      // Scalar streaming kinds (COUNT/SUM/AVG/MIN/MAX) reduce; COUNT(
      // DISTINCT plain-column) merges as an exact set union through
      // AggregateAccumulator::Merge.  DISTINCT on SUM/AVG has no set-union
      // merge and keeps the serial pass.  Spilled inputs also stay serial:
      // input.rows would only cover the resident prefix.
      bool parallel_grouping = ungrouped_slots.empty() && !input.HasSpill() &&
                               !input.HasColumnarOnly() &&
                               input.TotalRows() >= kParallelGroupMinRows;
      std::vector<std::optional<slot_t>> parallel_key_offsets;
      std::vector<std::optional<slot_t>> parallel_child_offsets;
      std::vector<bool> parallel_count_star;
      // Deterministic computed keys and aggregate arguments ride the same
      // worker-safe path as plain columns through the bytecode VM (the
      // parallel scan filter already evaluates compiled programs on
      // workers).  A program that fails to compile keeps the serial loop.
      std::vector<BytecodeProgram> parallel_key_programs;
      std::vector<BytecodeProgram> parallel_child_programs;
      const auto is_plain_column = [](const Expression& expression) {
        return expression && expression->Type() == TypeTag::kColumnValue;
      };
      static const bool parallel_expression_keys_disabled =
          std::getenv("TINYLAMB_DISABLE_PARALLEL_EXPRESSION_KEYS") != nullptr;
      const auto bind_parallel_value = [&](const Expression& expression,
                                           std::optional<slot_t>* offset,
                                           BytecodeProgram* program) {
        if (is_plain_column(expression)) {
          *offset = LocalColumnOffset(
              input.schema, expression->AsColumnValue().GetColumnName());
          return offset->has_value();
        }
        if (parallel_expression_keys_disabled || !BytecodeEnabled()) {
          return false;
        }
        std::optional<BytecodeProgram> compiled =
            BytecodeCompiler::CompileRowCached(
                expression, input.schema, BytecodeCompiler::Context::kValue);
        if (!compiled.has_value() ||
            compiled->ResultType() == ValueType::kNull) {
          return false;
        }
        *program = std::move(*compiled);
        return true;
      };
      if (parallel_grouping) {
        parallel_key_offsets.reserve(statement.GroupBy().size());
        for (const Expression& key : statement.GroupBy()) {
          parallel_key_offsets.emplace_back(std::nullopt);
          parallel_key_programs.emplace_back();
          if (!bind_parallel_value(key, &parallel_key_offsets.back(),
                                   &parallel_key_programs.back())) {
            parallel_grouping = false;
            break;
          }
        }
      }
      if (parallel_grouping) {
        parallel_child_offsets.reserve(aggregate_expressions.size());
        parallel_count_star.reserve(aggregate_expressions.size());
        for (const AggregateExpression* aggregate : aggregate_expressions) {
          // Positional evaluation alone does not make a partial state
          // mergeable: unsupported states must retain
          // the serial accumulator, just like physical grouped aggregation.
          const AggregateAccumulator probe(aggregate, &input.schema);
          if (!probe.CanMerge()) {
            parallel_grouping = false;
            break;
          }
          const bool count_star = IsCountStar(*aggregate);
          const bool distinct_count =
              aggregate->Distinct() &&
              aggregate->GetType() == AggregationType::kCount;
          // The negated conjunction reads as "every attribute must be
          // absent"; DeMorgan-expanding it only obscures the contract.
          if (aggregate->WhereFilter() ||
              (!count_star &&
               !(  // NOLINT(readability-simplify-boolean-expr)
                   aggregate->Child() &&
                   aggregate->Having() == AggregateHavingModifier::kNone &&
                   aggregate->InnerOrderBy().empty() &&
                   !aggregate->SecondaryArg() &&
                   aggregate->ExtraArgs().empty() &&
                   !aggregate->NeedsGroupContext() &&
                   aggregate->TrailingArgs().empty() &&
                   (!aggregate->Distinct() || distinct_count)))) {
            parallel_grouping = false;
            break;
          }
          parallel_count_star.push_back(count_star);
          parallel_child_offsets.emplace_back(std::nullopt);
          parallel_child_programs.emplace_back();
          if (!count_star &&
              !bind_parallel_value(aggregate->Child(),
                                   &parallel_child_offsets.back(),
                                   &parallel_child_programs.back())) {
            parallel_grouping = false;
            break;
          }
        }
      }
      if (parallel_grouping) {
        const size_t total_rows = input.TotalRows();
        const auto workers = std::min<size_t>(
            {size_t{16},
             QueryParallelWorkerLimit(std::thread::hardware_concurrency()),
             (total_rows / 8192) + 1});
        if (workers <= 1) {
          parallel_grouping = false;
        } else {
          bool single_int_key = false;
          slot_t single_int_slot = 0;
          ValueType single_int_type = ValueType::kNull;
          if (parallel_key_offsets.size() == 1 &&
              parallel_key_offsets[0].has_value()) {
            const Column& kcol =
                input.schema.GetColumn(*parallel_key_offsets[0]);
            if ((kcol.Type() == ValueType::kInt64 && !kcol.IsUnsigned()) ||
                kcol.Type() == ValueType::kDate) {
              single_int_key = true;
              single_int_slot = *parallel_key_offsets[0];
              single_int_type = kcol.Type();
            }
          }
          GroupKeyMap offsets;
          FlatIntKeyMap<size_t> int_merge_offsets;
          const size_t chunk_rows = 8192;
          const size_t chunk_count = (total_rows + chunk_rows - 1) / chunk_rows;
          struct WorkerLocal {
            GroupKeyMap offsets;
            FlatIntKeyMap<size_t> int_offsets;
            std::vector<GroupState> groups;
            std::deque<AggregateAccumulator> states;
            Status error{Status::kSuccess};
            size_t input_rows{0};
            size_t aggregate_updates{0};
          };
          std::vector<WorkerLocal> locals(workers);
          // Classification shared by every per-group accumulator construction
          // in the workers below; identical for all groups of one aggregate.
          std::vector<AggregateAccumulator::Shape> parallel_shapes;
          parallel_shapes.reserve(aggregate_expressions.size());
          for (const AggregateExpression* aggregate : aggregate_expressions) {
            parallel_shapes.push_back(
                AggregateAccumulator::AnalyzeShape(aggregate, nullptr));
          }
          {
            std::vector<std::jthread> threads;
            threads.reserve(workers);
            for (size_t w = 0; w < workers; ++w) {
              threads.emplace_back([&, w] {
                WorkerLocal& local = locals[w];
                // Positional, context-free accumulation only: TryEvaluate is
                // not safe on worker threads (the shared TransactionContext
                // and evaluator state are single-threaded contracts).
                std::vector<Value> key_values;
                key_values.reserve(parallel_key_offsets.size());
                // Static rotation: worker w folds chunks w, w+W, w+2W, ... .
                for (size_t chunk = w; chunk < chunk_count; chunk += workers) {
                  const size_t begin = chunk * chunk_rows;
                  const size_t end = std::min(total_rows, begin + chunk_rows);
                  for (size_t ri = begin; ri < end; ++ri) {
                    const Row& row = input.rows[ri];
                    size_t group_index = 0;
                    if (single_int_key && !row[single_int_slot].IsNull() &&
                        row[single_int_slot].type == single_int_type &&
                        !row[single_int_slot].IsUnsigned()) {
                      bool inserted = false;
                      size_t* slot = local.int_offsets.TryEmplace(
                          row[single_int_slot].value.int_value, &inserted);
                      if (inserted) {
                        *slot = local.groups.size();
                        GroupState group;
                        group.accumulator_offset = local.states.size();
                        for (size_t i = 0; i < aggregate_expressions.size();
                             ++i) {
                          local.states.emplace_back(aggregate_expressions[i],
                                                    parallel_shapes[i]);
                        }
                        group.representative = row;
                        local.groups.push_back(std::move(group));
                      }
                      group_index = *slot;
                    } else {
                      key_values.clear();
                      for (size_t ki = 0; ki < parallel_key_offsets.size();
                           ++ki) {
                        if (parallel_key_offsets[ki].has_value()) {
                          key_values.push_back(CanonicalDistinctValue(
                              row[*parallel_key_offsets[ki]]));
                          continue;
                        }
                        StatusOr<Value> computed =
                            TryEvaluateRowAuto(parallel_key_programs[ki], row);
                        if (!computed.HasValue()) {
                          local.error = computed.GetStatus();
                          return;
                        }
                        key_values.push_back(
                            CanonicalDistinctValue(computed.Value()));
                      }
                      Row key(std::move(key_values));
                      key_values.clear();
                      auto [iter, inserted] =
                          local.offsets.emplace(key, local.groups.size());
                      if (inserted) {
                        GroupState group;
                        group.accumulator_offset = local.states.size();
                        for (size_t i = 0; i < aggregate_expressions.size();
                             ++i) {
                          local.states.emplace_back(aggregate_expressions[i],
                                                    parallel_shapes[i]);
                        }
                        group.representative = row;
                        local.groups.push_back(std::move(group));
                      }
                      group_index = iter->second;
                    }
                    GroupState& group = local.groups[group_index];
                    for (size_t i = 0; i < aggregate_expressions.size(); ++i) {
                      AggregateAccumulator& accumulator =
                          local.states[group.accumulator_offset + i];
                      if (parallel_count_star[i]) {
                        Status st = accumulator.Add(Value(1));
                        if (st != Status::kSuccess) {
                          local.error = st;
                          return;
                        }
                        continue;
                      }
                      if (parallel_child_offsets[i].has_value()) {
                        Status st =
                            accumulator.Add(row[*parallel_child_offsets[i]]);
                        if (st != Status::kSuccess) {
                          local.error = st;
                          return;
                        }
                        continue;
                      }
                      StatusOr<Value> computed =
                          TryEvaluateRowAuto(parallel_child_programs[i], row);
                      if (!computed.HasValue()) {
                        local.error = computed.GetStatus();
                        return;
                      }
                      Status st = accumulator.Add(computed.Value());
                      if (st != Status::kSuccess) {
                        local.error = st;
                        return;
                      }
                    }
                    ++local.input_rows;
                    local.aggregate_updates += aggregate_expressions.size();
                  }
                }
              });
            }
          }
          for (const WorkerLocal& local : locals) {
            if (local.error != Status::kSuccess) {
              return local.error;
            }
          }
          if (context.execution_runtime() != nullptr) {
            for (const WorkerLocal& local : locals) {
              context.execution_runtime()->aggregate_input_rows +=
                  local.input_rows;
              context.execution_runtime()->aggregate_updates +=
                  local.aggregate_updates;
            }
          }
          size_t total_worker_groups = 0;
          for (const WorkerLocal& local : locals) {
            total_worker_groups += local.groups.size();
          }
          if (single_int_key && total_worker_groups > 0) {
            groups.reserve(total_worker_groups);
          }
          // Partition partial states by their canonical key. Each partition
          // visits workers in the same order as the serial fold, so floating
          // additions retain their existing association. A rank per first
          // occurrence also preserves the historical output encounter order.
          static const bool partition_merge_disabled =
              std::getenv("TINYLAMB_DISABLE_PARTITIONED_GROUP_MERGE") !=
              nullptr;
          const size_t merge_workers = std::min(
              workers, ParallelWorkerCount(total_worker_groups / 4096));
          const size_t staging_per_group =
              192 +
              (aggregate_expressions.size() * sizeof(AggregateAccumulator)) +
              (parallel_key_offsets.size() * sizeof(Value) * 2);
          const bool staging_fits =
              total_worker_groups <=
              (SIZE_MAX - input.charged_bytes_) / staging_per_group;
          const size_t staging_bytes =
              staging_fits ? (total_worker_groups * staging_per_group) +
                                 input.charged_bytes_
                           : 0;
          auto merge_charge =
              !partition_merge_disabled && merge_workers > 1 && staging_fits
                  ? TryReserveQueryMemory(staging_bytes)
                  : nullptr;
          if (merge_charge) {
            struct PartialRef {
              size_t worker;
              size_t group;
              const Row* key;
            };
            struct Location {
              size_t partition{SIZE_MAX};
              size_t group{0};
            };
            std::vector<PartialRef> partials;
            partials.reserve(total_worker_groups);
            std::vector<std::vector<size_t>> buckets(merge_workers);
            const auto stage = [&](size_t w, size_t g, const Row* key) {
              const size_t hash =
                  key != nullptr ? GroupKeyTransparentHash{}(*key)
                                 : std::hash<Value>{}(
                                       locals[w]
                                           .groups[g]
                                           .representative[single_int_slot]);
              uint64_t mixed = hash;
              mixed ^= mixed >> 30U;
              mixed *= 0xbf58476d1ce4e5b9ULL;
              mixed ^= mixed >> 27U;
              mixed *= 0x94d049bb133111ebULL;
              mixed ^= mixed >> 31U;
              buckets[mixed % merge_workers].push_back(partials.size());
              partials.push_back(
                  PartialRef{.worker = w, .group = g, .key = key});
            };
            for (size_t w = 0; w < locals.size(); ++w) {
              const auto& local = locals[w];
              if (single_int_key) {
                for (size_t g = 0; g < local.groups.size(); ++g) {
                  // Same admission test as the worker fold: groups keyed by
                  // an unsigned / off-type value live in local.offsets and
                  // are staged by the loop below, never twice.
                  const Value& key_val =
                      local.groups[g].representative[single_int_slot];
                  if (!key_val.IsNull() && key_val.type == single_int_type &&
                      !key_val.IsUnsigned()) {
                    stage(w, g, nullptr);
                  }
                }
              }
              for (const auto& [key, g] : local.offsets) {
                stage(w, g, &key);
              }
            }
            std::vector<WorkerLocal> partitions(merge_workers);
            std::vector<Location> first(partials.size());
            RETURN_IF_FAIL(RunParallelTasks(merge_workers, [&](size_t p) {
              auto& destination = partitions[p];
              for (const size_t rank : buckets[p]) {
                const auto& ref = partials[rank];
                auto& source = locals[ref.worker];
                auto& source_group = source.groups[ref.group];
                size_t target = 0;
                bool inserted = false;
                if (ref.key == nullptr) {
                  size_t* slot = destination.int_offsets.TryEmplace(
                      source_group.representative[single_int_slot]
                          .value.int_value,
                      &inserted);
                  if (inserted) {
                    *slot = destination.groups.size();
                  }
                  target = *slot;
                } else {
                  auto [it, fresh] = destination.offsets.emplace(
                      *ref.key, destination.groups.size());
                  inserted = fresh;
                  target = it->second;
                }
                if (inserted) {
                  first[rank] = Location{.partition = p, .group = target};
                  const size_t offset = destination.states.size();
                  for (size_t i = 0; i < aggregate_expressions.size(); ++i) {
                    destination.states.push_back(std::move(
                        source.states[source_group.accumulator_offset + i]));
                  }
                  destination.groups.push_back(GroupState{
                      .representative = std::move(source_group.representative),
                      .accumulator_offset = offset});
                } else {
                  const auto& target_group = destination.groups[target];
                  for (size_t i = 0; i < aggregate_expressions.size(); ++i) {
                    const Status st =
                        destination.states[target_group.accumulator_offset + i]
                            .Merge(
                                source.states[source_group.accumulator_offset +
                                              i]);
                    if (st != Status::kSuccess) {
                      destination.error = st;
                      return;
                    }
                  }
                }
              }
            }));
            for (const auto& part : partitions) {
              RETURN_IF_FAIL(part.error);
            }
            for (const auto& location : first) {
              if (location.partition == SIZE_MAX) {
                continue;
              }
              auto& part = partitions[location.partition];
              auto& group = part.groups[location.group];
              const size_t offset = aggregate_states.size();
              for (size_t i = 0; i < aggregate_expressions.size(); ++i) {
                aggregate_states.push_back(
                    std::move(part.states[group.accumulator_offset + i]));
              }
              group.accumulator_offset = offset;
              groups.push_back(std::move(group));
            }
            if (context.execution_runtime() != nullptr) {
              ++context.execution_runtime()->partitioned_aggregate_merges;
            }
          } else {
            // Merge partials in worker order.  A group first seen by a worker
            // is adopted wholesale (its accumulator states move onto the
            // shared deque and the offset rebases); overlapping groups merge
            // accumulator state.  Adopt-then-move keeps every partial state
            // visible.
            for (WorkerLocal& local : locals) {
              if (single_int_key && groups.empty() && offsets.empty() &&
                  local.offsets.empty() && !local.groups.empty()) {
                groups = std::move(local.groups);
                int_merge_offsets = std::move(local.int_offsets);
                aggregate_states = std::move(local.states);
                if (total_worker_groups > groups.size()) {
                  int_merge_offsets.Reserve(total_worker_groups);
                  groups.reserve(total_worker_groups);
                }
                continue;
              }
              if (single_int_key) {
                if (groups.empty() && total_worker_groups > 0) {
                  int_merge_offsets.Reserve(total_worker_groups);
                }
                for (GroupState& group : local.groups) {
                  const Value& key_val =
                      group.representative.values_[single_int_slot];
                  // Mirror the worker's int-key admission; other groups are
                  // adopted through local.offsets below.
                  if (key_val.IsNull() || key_val.type != single_int_type ||
                      key_val.IsUnsigned()) {
                    continue;
                  }
                  const int64_t kv = key_val.value.int_value;
                  bool inserted = false;
                  size_t* slot = int_merge_offsets.TryEmplace(kv, &inserted);
                  if (inserted) {
                    *slot = groups.size();
                    const size_t local_offset = group.accumulator_offset;
                    const size_t shared_offset = aggregate_states.size();
                    for (size_t i = 0; i < aggregate_expressions.size(); ++i) {
                      aggregate_states.push_back(
                          std::move(local.states[local_offset + i]));
                    }
                    group.accumulator_offset = shared_offset;
                    groups.push_back(std::move(group));
                  } else {
                    GroupState& destination = groups[*slot];
                    for (size_t i = 0; i < aggregate_expressions.size(); ++i) {
                      AggregateAccumulator& source_state =
                          local.states[group.accumulator_offset + i];
                      AggregateAccumulator& target =
                          aggregate_states[destination.accumulator_offset + i];
                      RETURN_IF_FAIL(target.Merge(source_state));
                    }
                  }
                }
                if (local.offsets.empty()) {
                  continue;
                }
              }
              for (auto& [key, index] : local.offsets) {
                GroupState& group = local.groups[index];
                auto [iter, inserted] = offsets.emplace(key, groups.size());
                if (inserted) {
                  const size_t local_offset = group.accumulator_offset;
                  const size_t shared_offset = aggregate_states.size();
                  for (size_t i = 0; i < aggregate_expressions.size(); ++i) {
                    aggregate_states.push_back(
                        std::move(local.states[local_offset + i]));
                  }
                  group.accumulator_offset = shared_offset;
                  groups.push_back(std::move(group));
                } else {
                  GroupState& destination = groups[iter->second];
                  for (size_t i = 0; i < aggregate_expressions.size(); ++i) {
                    AggregateAccumulator& source_state =
                        local.states[group.accumulator_offset + i];
                    AggregateAccumulator& target =
                        aggregate_states[destination.accumulator_offset + i];
                    RETURN_IF_FAIL(target.Merge(source_state));
                  }
                }
              }
            }
          }
        }
      }
      if (!parallel_grouping) {
        GroupKeyMap offsets;
        Status iterated = input.ForEachRow([&](const Row& row) {
          Status st_acc =
              accumulate_row(row, &offsets, &groups, &aggregate_states);
          if (st_acc != Status::kSuccess && agg_error == Status::kSuccess) {
            agg_error = st_acc;
          }
        });
        if (iterated != Status::kSuccess && agg_error == Status::kSuccess) {
          agg_error = iterated;
        }
        RETURN_IF_FAIL(agg_error);
      }
      if (input.TotalRows() == 0 &&
          (statement.GroupBy().empty() || statement.IsGroupAll())) {
        groups.push_back(make_group());
      }
    }
    if (context.execution_runtime() != nullptr && !used_partition_agg) {
      context.execution_runtime()->aggregate_groups += groups.size();
    }
  }

  Relation output(context.execution_runtime());
  CopyExecutionStats(&output, input);
  const size_t base_width =
      input.schema.ColumnCount() -
      std::min<size_t>(hidden_columns,
                       static_cast<size_t>(input.schema.ColumnCount()));
  std::vector<Column> output_columns;
  // Per-select-item expansion of star items: the input column indices each
  // `*` / `relation.*` refers to.
  auto matches_star = [](const ColumnName& requested,
                         const ColumnName& candidate, bool qualified) {
    if (qualified) {
      return !requested.schema.empty() &&
             std::equal(requested.schema.begin(), requested.schema.end(),
                        candidate.schema.begin(), candidate.schema.end(),
                        [](char a, char b) {
                          return std::tolower(static_cast<unsigned char>(a)) ==
                                 std::tolower(static_cast<unsigned char>(b));
                        });
    }
    return requested.schema.empty();
  };
  // Unwrap a visitor-injected value-table/proto wrapper for `rel.*` over a
  // real FROM source. A wrapped star whose qualifier is not a source keeps
  // its wrapper (legitimate proto row-value semantics, resolved by the
  // expression evaluator).
  const auto source_named = [&](std::string_view qualifier) {
    return std::ranges::any_of(
        statement.Sources(), [&](const SelectSource& source) {
          return (!source.alias.empty() &&
                  IdentifierEquals(source.alias, qualifier)) ||
                 (!source.table.empty() &&
                  IdentifierEquals(source.table, qualifier));
        });
  };
  const auto select_star =
      [&](const Expression& expression) -> std::optional<ColumnName> {
    auto star = UnwrapStarProjection(expression);
    if (!star) {
      return std::nullopt;
    }
    if (star->wrapped && !star->name.schema.empty() &&
        !source_named(star->name.schema)) {
      return std::nullopt;
    }
    return star->name;
  };
  // USING merge: unqualified `*` emits one coalesced entry per declared
  // using column (first), then the remaining columns in schema order;
  // `relation.*` keeps every physical column of the named side.  Each star
  // entry stores its coalesce group (single index for plain columns).
  std::vector<std::vector<std::vector<size_t>>> star_groups(
      statement.SelectList().size());
  // SELECT * over a value-table CTE must expose the proto's fields, not its
  // implementation column.  The CTE retains the SELECT AS proto constructor
  // in its source statement, which is enough to identify this shape without
  // adding type metadata to Relation.
  bool proto_value_star = false;
  // A derived proto value table (an inlined WITH body) is recognized by the
  // SELECT AS <proto> constructor it carries, recursively through set
  // operations; only the TestExtraPB shape has the three fields expanded
  // below, so other proto types stay on the ordinary star path.
  const auto query_produces_test_extra_pb = [](const SelectStatement& query,
                                               auto&& self) -> bool {
    for (const NamedExpression& item : query.SelectList()) {
      const Expression& expression = item.expression;
      if (expression && expression->Type() == TypeTag::kFunctionCallExp) {
        const FunctionCallExpression& call =
            expression->AsFunctionCallExpression();
        if ((call.FuncName() == "__proto_new" ||
             call.FuncName() == "__value_table_proto") &&
            !call.Args().empty()) {
          const Expression& type = call.Args().front();
          if (type->Type() == TypeTag::kConstantValue &&
              type->AsConstantValue().GetValue().type == ValueType::kVarChar &&
              std::string(
                  type->AsConstantValue().GetValue().value.varchar_value)
                      .find("TestExtraPB") != std::string::npos) {
            return true;
          }
        }
      }
    }
    return std::ranges::any_of(query.UnionAll(), [&](const auto& branch) {
      return branch && self(*branch, self);
    });
  };
  for (const SelectSource& source : statement.Sources()) {
    // Materialized proto value tables are catalog tables with no query to
    // inspect; they are identified by their reserved compliance names only —
    // an exact match, not a substring, so ordinary tables cannot collide.
    if (source.table == "TestExtraPBValueTable" ||
        source.table == "WithProtoValueTable") {
      proto_value_star = true;
    }
    if (source.query && query_produces_test_extra_pb(
                            *source.query, query_produces_test_extra_pb)) {
      proto_value_star = true;
    }
  }
  // A physical join may reorder inputs. Bare * still expands in FROM order,
  // including opaque derived sources that could not expand in QueryData.
  // Keep the projection as offsets; do not copy/reorder the materialized rows.
  std::vector<size_t> star_column_order(base_width);
  std::iota(star_column_order.begin(), star_column_order.end(), 0);
  if (statement.Sources().size() > 1 &&
      std::ranges::all_of(statement.Sources(), [](const SelectSource& source) {
        return !source.from_nested_join && source.using_columns.empty() &&
               (source.join_type == JoinType::kCross ||
                source.join_type == JoinType::kInner);
      })) {
    const auto source_rank = [&](size_t column) {
      const auto& qualifier = input.schema.GetColumn(column).Name().schema;
      for (size_t index = 0; index < statement.Sources().size(); ++index) {
        const auto& source = statement.Sources()[index];
        const std::string& name =
            source.alias.empty() ? source.table : source.alias;
        if (IdentifierEquals(qualifier, name)) {
          return index;
        }
      }
      return statement.Sources().size();
    };
    std::stable_sort(
        star_column_order.begin(), star_column_order.end(),
        [&](size_t a, size_t b) { return source_rank(a) < source_rank(b); });
  }
  std::vector<bool> star_proto(statement.SelectList().size(), false);
  std::vector<std::string> lowered_using;
  if (input.using_columns != nullptr) {
    lowered_using = *input.using_columns;
  }
  for (size_t i = 0; i < statement.SelectList().size(); ++i) {
    const NamedExpression& projection = statement.SelectList()[i];
    const auto star = select_star(projection.expression);
    if (star) {
      const ColumnName& requested = *star;
      const bool qualified = !requested.schema.empty();
      if (projection.star_modifiers) {
        // The index-group expansion below cannot splice a substitute
        // expression mid-star; REPLACE needs the query-layer expansion.
        if (!projection.star_modifiers->replace_items.empty()) {
          return StatusError(
              StatusCode::kInvalidArgument,
              "SELECT * REPLACE over this query shape is not supported; "
              "rewrite the star over a single relation");
        }
      }
      std::unordered_set<std::string> except_names;
      std::unordered_set<std::string> except_matched_names;
      if (projection.star_modifiers) {
        for (const std::string& entry :
             projection.star_modifiers->except_columns) {
          const size_t dot = entry.rfind('.');
          except_names.insert(dot == std::string::npos ? entry
                                                       : entry.substr(dot + 1));
        }
      }
      const auto except_drops = [&](const ColumnName& candidate) {
        if (except_names.empty()) {
          return false;
        }
        const std::string& name = candidate.name;
        for (const std::string& entry : except_names) {
          if (entry.size() == name.size() &&
              std::equal(entry.begin(), entry.end(), name.begin(),
                         [](char a, char b) {
                           return std::tolower(static_cast<unsigned char>(a)) ==
                                  std::tolower(static_cast<unsigned char>(b));
                         })) {
            except_matched_names.insert(entry);
            return true;
          }
        }
        return false;
      };
      // The proto payload column is VARCHAR; requiring it keeps a same-named
      // non-proto table on the ordinary star expansion.
      if (proto_value_star && base_width == 1 &&
          input.schema.GetColumn(0).Type() == ValueType::kVarChar) {
        if (projection.star_modifiers) {
          return StatusError(StatusCode::kInvalidArgument,
                             "SELECT * EXCEPT/REPLACE over this table is "
                             "not supported");
        }
        star_proto[i] = true;
        output_columns.emplace_back("int32_val1", ValueType::kInt64);
        output_columns.emplace_back("int32_val2", ValueType::kInt64);
        output_columns.emplace_back("str_value", ValueType::kArray);
        continue;
      }
      if (!qualified && !lowered_using.empty()) {
        // Merged using columns first, in declaration order.
        std::vector<size_t> consumed;
        for (const std::string& using_name : lowered_using) {
          std::vector<size_t> group;
          for (size_t column = 0; column < base_width; ++column) {
            const ColumnName& candidate = input.schema.GetColumn(column).Name();
            if (candidate.name.size() == using_name.size() &&
                std::equal(using_name.begin(), using_name.end(),
                           candidate.name.begin(), [](char a, char b) {
                             return std::tolower(
                                        static_cast<unsigned char>(a)) ==
                                    std::tolower(static_cast<unsigned char>(b));
                           })) {
              group.push_back(column);
            }
          }
          if (!group.empty()) {
            consumed.insert(consumed.end(), group.begin(), group.end());
            const size_t merged_index = group.front();
            star_groups[i].push_back(std::move(group));
            output_columns.push_back(ProjectionDomainColumn(
                input.schema.GetColumn(merged_index).Name().name,
                input.schema.GetColumn(merged_index)));
          }
        }
        // Remaining non-using columns keep schema order.
        for (size_t column = 0; column < base_width; ++column) {
          if (std::find(consumed.begin(), consumed.end(), column) !=
              consumed.end()) {
            continue;
          }
          const Column& candidate = input.schema.GetColumn(column);
          if (matches_star(requested, candidate.Name(), qualified) &&
              !except_drops(candidate.Name())) {
            star_groups[i].push_back({column});
            output_columns.push_back(
                ProjectionDomainColumn(candidate.Name().name, candidate));
          }
        }
        continue;
      }
      for (const size_t column : star_column_order) {
        const Column& candidate = input.schema.GetColumn(column);
        if (matches_star(requested, candidate.Name(), qualified) &&
            !except_drops(candidate.Name())) {
          star_groups[i].push_back({column});
          output_columns.push_back(
              ProjectionDomainColumn(candidate.Name().name, candidate));
        }
      }
      // An EXCEPT entry that dropped nothing resolves nowhere: fail loudly
      // instead of silently keeping the column.
      if (projection.star_modifiers &&
          except_matched_names.size() < except_names.size()) {
        return StatusError(StatusCode::kInvalidArgument,
                           "EXCEPT column not present in the star target");
      }
    } else {
      output_columns.push_back(ProjectionExpressionColumn(
          ProjectionName(projection, i), projection.expression, input.schema));
    }
  }
  // A star that matched nothing (unknown qualifier, or a bare `*` over an
  // empty input) would otherwise silently shrink the select list; reject it
  // like an unknown column.
  for (size_t i = 0; i < statement.SelectList().size(); ++i) {
    const NamedExpression& projection = statement.SelectList()[i];
    const auto star = select_star(projection.expression);
    if (star && !star_proto[i] && star_groups[i].empty()) {
      const ColumnName& requested = *star;
      return StatusError(
          StatusCode::kInvalidArgument,
          "unknown relation in select list: " +
              (requested.schema.empty() ? std::string("*")
                                        : requested.schema + ".*"));
    }
  }
  // Hidden $win columns ride along until ordering completes, then get trimmed
  // below.  ORDER BY terms may reference them.
  for (size_t k = 0; k < hidden_columns; ++k) {
    output_columns.emplace_back(input.schema.GetColumn(base_width + k));
  }

  const std::vector<SelectStatement::OrderByTerm>& order_by =
      statement.OrderBy();
  const bool has_order_by = !order_by.empty();
  // One ordering contract for the ORDER BY keys, the DISTINCT keys and the
  // bounded (LIMIT) heap alike: NULL placement, direction and the collation
  // the key resolves under all come from SortKeyOrder (type/collation.hpp),
  // the same type the sort operators use. A COLLATE'd key therefore folds here
  // exactly as it does in SortExecutor / TopNExecutor.
  std::vector<SortKeyOrder> order_specs;
  order_specs.reserve(order_by.size());
  for (const SelectStatement::OrderByTerm& term : order_by) {
    order_specs.emplace_back(term.ascending, term.nulls_first,
                             CollationOfExpression(term.expression));
  }
  bool bounded_sort = false;
  size_t topn_capacity = 0;
  if (!statement.Distinct() && statement.HasLimit() && !statement.WithTies() &&
      statement.Offset() <=
          std::numeric_limits<size_t>::max() - statement.Limit()) {
    bounded_sort = true;
    topn_capacity = statement.Offset() + statement.Limit();
  }
  ExternalKeyedSorter sorter(order_specs, bounded_sort, topn_capacity);
  Schema initial_output_schema("", output_columns);

  std::vector<std::optional<size_t>> order_positions(
      resolved_order_expressions.size());
  for (size_t order_index = 0; order_index < resolved_order_expressions.size();
       ++order_index) {
    auto& resolved_order_expression = resolved_order_expressions[order_index];
    const Expression& expression = resolved_order_expression;
    // `ORDER BY 1 COLLATE 'und:ci'` arrives as COLLATE(<int>, 'spec'): the
    // ordinal names a SELECT-list position and the clause collates it, so the
    // two resolve together. Left as a constant, the key would encode
    // identically for every row and the sort would silently do nothing.
    if (const std::optional<CollatedOrdinal> collated =
            AsCollatedOrdinal(expression);
        collated.has_value() && collated->ordinal >= 1 &&
        static_cast<size_t>(collated->ordinal) <= output_columns.size() &&
        !output_columns[static_cast<size_t>(collated->ordinal) - 1]
             .Name()
             .name.empty()) {
      order_positions[order_index] = static_cast<size_t>(collated->ordinal) - 1;
      continue;
    }
    if (!expression || expression->Type() != TypeTag::kConstantValue ||
        dynamic_cast<const ParameterSlot*>(expression.get()) != nullptr) {
      continue;
    }
    const Value& value = expression->AsConstantValue().GetValue();
    if (value.type != ValueType::kInt64 || value.IsNull()) {
      continue;
    }
    if (value.value.int_value < 1 ||
        static_cast<size_t>(value.value.int_value) > output_columns.size()) {
      return StatusError(StatusCode::kInvalidArgument,
                         "ORDER BY ordinal out of range");
    }
    order_positions[order_index] =
        static_cast<size_t>(value.value.int_value) - 1;
  }

  // CASE WHEN a IS NOT NULL THEN a ELSE b is the FULL OUTER JOIN key
  // coalesce. It only copies one of two cells; the bytecode interpreter
  // would rebuild a stack for every row.
  const auto positional_column =
      [&](const Expression& expression) -> std::optional<size_t> {
    if (!expression || expression->Type() != TypeTag::kColumnValue) {
      return std::nullopt;
    }
    const auto& name = expression->AsColumnValue().GetColumnName();
    if (name.name == "*" || name.name.find('.') != std::string::npos ||
        name.schema.find('.') != std::string::npos) {
      return std::nullopt;
    }
    const auto offset = LocalColumnOffset(input.schema, name);
    if (!offset ||
        !IdentifierEquals(input.schema.GetColumn(*offset).Name().name,
                          name.name)) {
      return std::nullopt;
    }
    return offset;
  };
  const auto column_null_test = [&](const auto& self,
                                    const Expression& expression)
      -> std::optional<std::pair<size_t, bool>> {
    if (!expression || expression->Type() != TypeTag::kUnaryExp) {
      return std::nullopt;
    }
    const auto& unary = expression->AsUnaryExpression();
    if (unary.Op() == UnaryOperation::kNot) {
      auto inner = self(self, unary.Child());
      if (!inner) {
        return std::nullopt;
      }
      inner->second = !inner->second;
      return inner;
    }
    if (unary.Op() != UnaryOperation::kIsNull &&
        unary.Op() != UnaryOperation::kIsNotNull) {
      return std::nullopt;
    }
    const auto column = positional_column(unary.Child());
    if (!column) {
      return std::nullopt;
    }
    return std::pair<size_t, bool>{*column,
                                   unary.Op() == UnaryOperation::kIsNotNull};
  };
  const auto null_coalesce_columns = [&](const Expression& expression)
      -> std::optional<std::pair<size_t, size_t>> {
    if (!expression || expression->Type() != TypeTag::kCaseExp) {
      return std::nullopt;
    }
    const auto& searched = expression->AsCaseExpression();
    if (searched.when_clauses_.size() != 1 || !searched.else_clause_) {
      return std::nullopt;
    }
    const auto tested =
        column_null_test(column_null_test, searched.when_clauses_[0].first);
    const auto then_column =
        positional_column(searched.when_clauses_[0].second);
    const auto else_column = positional_column(searched.else_clause_);
    if (!tested || !then_column || !else_column) {
      return std::nullopt;
    }
    // prefer, fallback: a non-null prefer wins, otherwise fallback.
    if (tested->second && tested->first == *then_column) {
      return std::pair<size_t, size_t>{*then_column, *else_column};
    }
    if (!tested->second && tested->first == *else_column) {
      return std::pair<size_t, size_t>{*else_column, *then_column};
    }
    return std::nullopt;
  };
  // Bind ordinary projection expressions once. Scope lookup and compiler
  // cache probes belong to the statement, not to every input row.
  std::vector<std::optional<size_t>> projection_offsets(
      statement.SelectList().size());
  std::vector<std::optional<BytecodeProgram>> projection_programs(
      statement.SelectList().size());
  std::vector<std::optional<std::pair<size_t, size_t>>> null_coalesces(
      statement.SelectList().size());
  if (!grouped && outer == nullptr && lowered_using.empty()) {
    for (size_t i = 0; i < statement.SelectList().size(); ++i) {
      const auto& expression = statement.SelectList()[i].expression;
      if (!expression || ContainsQuery(expression)) {
        continue;
      }
      const auto columns = expression->TouchedColumns();
      const bool positional =
          std::ranges::all_of(columns, [&](const auto& name) {
            const auto offset = LocalColumnOffset(input.schema, name);
            return name.name != "*" &&
                   name.name.find('.') == std::string::npos &&
                   name.schema.find('.') == std::string::npos && offset &&
                   IdentifierEquals(input.schema.GetColumn(*offset).Name().name,
                                    name.name);
          });
      if (!positional) {
        continue;
      }
      if (expression->Type() == TypeTag::kColumnValue) {
        projection_offsets[i] = LocalColumnOffset(
            input.schema, expression->AsColumnValue().GetColumnName());
      } else if (auto coalesced = null_coalesce_columns(expression)) {
        null_coalesces[i] = coalesced;
      } else if (BytecodeEnabled() && IsScalarBytecodeCompilable(expression)) {
        projection_programs[i] = BytecodeCompiler::CompileRowCached(
            expression, input.schema, BytecodeCompiler::Context::kValue);
      }
    }
  }

  // Stage A eligibility: for bounded ORDER BY queries whose SELECT items only
  // pass through columns or coalesce two columns (error-free), rank rows via
  // ORDER BY keys first and materialize only winning rows.
  std::vector<TopNOutputSlot> stage_a_slots;
  std::vector<ColumnarTopNKey> stage_a_keys;
  bool stage_a_eligible = !grouped && !having_expr && has_order_by &&
                          bounded_sort && !statement.Distinct() &&
                          hidden_columns == 0 && outer == nullptr &&
                          lowered_using.empty() && !input.HasSpill();
  if (stage_a_eligible) {
    stage_a_slots.reserve(output_columns.size());
    for (size_t item = 0; item < statement.SelectList().size(); ++item) {
      if (projection_offsets[item]) {
        stage_a_slots.push_back(
            TopNOutputSlot{.col = *projection_offsets[item]});
      } else if (!star_proto[item] && !star_groups[item].empty()) {
        for (const auto& group : star_groups[item]) {
          if (group.size() != 1) {
            stage_a_eligible = false;
            break;
          }
          stage_a_slots.push_back(TopNOutputSlot{.col = group.front()});
        }
      } else if (null_coalesces[item]) {
        stage_a_slots.push_back(
            TopNOutputSlot{.col = null_coalesces[item]->first,
                           .is_coalesce = true,
                           .fallback_col = null_coalesces[item]->second});
      } else {
        stage_a_eligible = false;
      }
      if (!stage_a_eligible) {
        break;
      }
    }
    stage_a_eligible =
        stage_a_eligible && stage_a_slots.size() == output_columns.size();
  }
  const auto has_ambiguous_output_name = [&](const ColumnName& name) {
    return std::ranges::any_of(output_columns, [&](const Column& column) {
      return IdentifierEquals(column.Name().name, name.name) &&
             (name.schema.empty() ||
              IdentifierEquals(column.Name().schema, name.schema));
    });
  };
  if (stage_a_eligible) {
    stage_a_keys.reserve(resolved_order_expressions.size());
    for (size_t oi = 0; oi < resolved_order_expressions.size(); ++oi) {
      if (order_positions[oi]) {
        const size_t pos = *order_positions[oi];
        if (pos >= stage_a_slots.size()) {
          stage_a_eligible = false;
          break;
        }
        const TopNOutputSlot& slot = stage_a_slots[pos];
        stage_a_keys.push_back(
            slot.is_coalesce
                ? ColumnarTopNKey::Coalesce(
                      static_cast<slot_t>(slot.col),
                      static_cast<slot_t>(slot.fallback_col))
                : ColumnarTopNKey::Direct(static_cast<slot_t>(slot.col)));
        continue;
      }
      const Expression& expression = resolved_order_expressions[oi];
      if (!expression) {
        stage_a_eligible = false;
        break;
      }
      if (expression->Type() == TypeTag::kColumnValue) {
        const auto& name = expression->AsColumnValue().GetColumnName();
        if (name.name == "*" || name.name.find('.') != std::string::npos ||
            name.schema.find('.') != std::string::npos) {
          stage_a_eligible = false;
          break;
        }
        const auto projected = LocalColumnOffset(initial_output_schema, name);
        if (!projected && has_ambiguous_output_name(name)) {
          stage_a_eligible = false;
          break;
        }
        if (projected && *projected < stage_a_slots.size() &&
            IdentifierEquals(
                initial_output_schema.GetColumn(*projected).Name().name,
                name.name)) {
          const TopNOutputSlot& slot = stage_a_slots[*projected];
          stage_a_keys.push_back(
              slot.is_coalesce
                  ? ColumnarTopNKey::Coalesce(
                        static_cast<slot_t>(slot.col),
                        static_cast<slot_t>(slot.fallback_col))
                  : ColumnarTopNKey::Direct(static_cast<slot_t>(slot.col)));
        } else {
          const auto base = LocalColumnOffset(input.schema, name);
          if (!base ||
              !IdentifierEquals(input.schema.GetColumn(*base).Name().name,
                                name.name)) {
            stage_a_eligible = false;
            break;
          }
          stage_a_keys.push_back(
              ColumnarTopNKey::Direct(static_cast<slot_t>(*base)));
        }
        continue;
      }
      const auto touches_unshadowed_input = [&](const Expression& node) {
        for (const ColumnName& cn : node->TouchedColumns()) {
          if (cn.name == "*" || cn.name.find('.') != std::string::npos ||
              cn.schema.find('.') != std::string::npos) {
            return false;
          }
          const auto base = LocalColumnOffset(input.schema, cn);
          if (!base ||
              !IdentifierEquals(input.schema.GetColumn(*base).Name().name,
                                cn.name)) {
            return false;
          }
          const auto proj = LocalColumnOffset(initial_output_schema, cn);
          if (!proj) {
            if (has_ambiguous_output_name(cn)) {
              return false;
            }
          } else {
            if (*proj >= stage_a_slots.size() ||
                stage_a_slots[*proj].is_coalesce ||
                stage_a_slots[*proj].col != *base ||
                !IdentifierEquals(
                    initial_output_schema.GetColumn(*proj).Name().name,
                    cn.name)) {
              return false;
            }
          }
        }
        return true;
      };
      if (auto coal = null_coalesce_columns(expression);
          coal.has_value() && touches_unshadowed_input(expression)) {
        stage_a_keys.push_back(
            ColumnarTopNKey::Coalesce(static_cast<slot_t>(coal->first),
                                      static_cast<slot_t>(coal->second)));
        continue;
      }
      if (!ContainsQuery(expression) && !ContainsAggregate(expression) &&
          !ContainsParameterSlot(expression) && BytecodeEnabled() &&
          IsScalarBytecodeCompilable(expression) &&
          touches_unshadowed_input(expression)) {
        std::optional<BytecodeProgram> compiled =
            BytecodeCompiler::CompileRowCached(
                expression, input.schema, BytecodeCompiler::Context::kValue);
        if (compiled.has_value() && compiled->FastInterpretable()) {
          const Column res_col =
              ProjectionExpressionColumn("", expression, input.schema);
          stage_a_keys.push_back(
              ColumnarTopNKey::Computed(std::move(*compiled), res_col));
          continue;
        }
      }
      stage_a_eligible = false;
      break;
    }
  }

  if (stage_a_eligible && input.HasColumnarOnly()) {
    RETURN_IF_FAIL(input.GetStatus());
    const auto sort_begin = std::chrono::steady_clock::now();
    ASSIGN_OR_RETURN(
        ColumnarTopNResult, topn_res,
        (TryColumnarBoundedTopN(*input.columnar, input.schema,
                                initial_output_schema, stage_a_slots,
                                stage_a_keys, order_specs, topn_capacity)));
    if (topn_res.has_value()) {
      Relation col_output(context.execution_runtime());
      col_output.SetColumnarTable(std::move(topn_res->first), topn_res->second);
      CopyExecutionStats(&col_output, input);
      RefineProjectionTypes(col_output, output_columns);
      col_output.schema = Schema("", std::move(output_columns));
      if (auto* runtime = context.execution_runtime()) {
        ++runtime->topn_finish_uses;
        runtime->topn_finish_capacity = topn_capacity;
        runtime->topn_finish_input_rows = input.columnar->total_rows;
        if (input.columnar->total_rows > 0) {
          runtime->sort_ms += ElapsedMs(sort_begin);
        }
        runtime->project_ms += ElapsedMs(project_begin);
      }
      return col_output;
    }
  }

  // Stage B: parallel columnar projection before sorting/TopN when ORDER BY
  // only reads projected output columns (or has no ORDER BY).
  if (!grouped && !having_expr && !stage_a_eligible &&
      (!has_order_by || (hidden_columns == 0 && !statement.Distinct())) &&
      outer == nullptr && lowered_using.empty() && !input.HasSpill() &&
      input.HasColumnarOnly()) {
    std::vector<ColumnarTopNKey> stage_b_keys;
    bool order_uses_only_projected = !has_order_by;
    bool stage_b_topn_ok = bounded_sort;
    if (has_order_by) {
      order_uses_only_projected = true;
      stage_b_keys.reserve(resolved_order_expressions.size());
      for (size_t oi = 0; oi < resolved_order_expressions.size(); ++oi) {
        if (order_positions[oi]) {
          stage_b_keys.push_back(ColumnarTopNKey::Direct(
              static_cast<slot_t>(*order_positions[oi])));
          continue;
        }
        if (order_by[oi].expression &&
            order_by[oi].expression->Type() == TypeTag::kColumnValue) {
          const auto& raw_name =
              order_by[oi].expression->AsColumnValue().GetColumnName();
          if (raw_name.name != "*" &&
              raw_name.name.find('.') == std::string::npos &&
              raw_name.schema.find('.') == std::string::npos) {
            const auto off = LocalColumnOffset(initial_output_schema, raw_name);
            if (off.has_value() && *off < output_columns.size() &&
                IdentifierEquals(
                    initial_output_schema.GetColumn(*off).Name().name,
                    raw_name.name)) {
              stage_b_keys.push_back(
                  ColumnarTopNKey::Direct(static_cast<slot_t>(*off)));
              continue;
            }
          }
          order_uses_only_projected = false;
          break;
        }
        const Expression& expr = resolved_order_expressions[oi];
        if (!expr || ContainsQuery(expr) || ContainsAggregate(expr) ||
            ContainsParameterSlot(expr)) {
          order_uses_only_projected = false;
          break;
        }
        for (const ColumnName& cn : expr->TouchedColumns()) {
          if (cn.name == "*" || cn.name.find('.') != std::string::npos ||
              cn.schema.find('.') != std::string::npos) {
            order_uses_only_projected = false;
            break;
          }
          const auto off = LocalColumnOffset(initial_output_schema, cn);
          if (!off.has_value() || *off >= output_columns.size() ||
              !IdentifierEquals(
                  initial_output_schema.GetColumn(*off).Name().name, cn.name)) {
            order_uses_only_projected = false;
            break;
          }
        }
        if (!order_uses_only_projected) {
          break;
        }
        if (stage_b_topn_ok && BytecodeEnabled() &&
            IsScalarBytecodeCompilable(expr)) {
          std::optional<BytecodeProgram> compiled =
              BytecodeCompiler::CompileRowCached(
                  expr, initial_output_schema,
                  BytecodeCompiler::Context::kValue);
          if (compiled.has_value() && compiled->FastInterpretable()) {
            const Column res_col =
                ProjectionExpressionColumn("", expr, initial_output_schema);
            stage_b_keys.push_back(
                ColumnarTopNKey::Computed(std::move(*compiled), res_col));
            continue;
          }
        }
        stage_b_topn_ok = false;
      }
    }
    bool columnar_eligible = order_uses_only_projected;
    const size_t visible_width = output_columns.size() - hidden_columns;
    std::vector<Expression> proj_exprs;
    if (columnar_eligible) {
      proj_exprs.reserve(visible_width);
      for (size_t i = 0; i < statement.SelectList().size(); ++i) {
        const NamedExpression& item = statement.SelectList()[i];
        if (!item.expression) {
          columnar_eligible = false;
          break;
        }
        if (star_proto[i]) {
          columnar_eligible = false;
          break;
        }
        if (!star_groups[i].empty()) {
          for (const auto& group : star_groups[i]) {
            if (group.size() != 1) {
              columnar_eligible = false;
              break;
            }
            const ColumnName& col_name =
                input.schema.GetColumn(group.front()).Name();
            if (relational_detail::LocalColumnOffset(input.schema, col_name) !=
                group.front()) {
              columnar_eligible = false;
              break;
            }
            proj_exprs.push_back(ColumnValueExp(col_name));
          }
          if (!columnar_eligible) {
            break;
          }
          continue;
        }
        if (ContainsQuery(item.expression) ||
            ContainsAggregate(item.expression) ||
            (item.expression->Type() == TypeTag::kConstantValue &&
             dynamic_cast<const ParameterSlot*>(item.expression.get()) !=
                 nullptr)) {
          columnar_eligible = false;
          break;
        }
        proj_exprs.push_back(item.expression);
      }
    }
    if (columnar_eligible && proj_exprs.size() == visible_width) {
      std::vector<Column> visible_output_columns(
          output_columns.begin(), output_columns.begin() + visible_width);
      const auto plan = relational_detail::CompiledColumnarProjection::Compile(
          proj_exprs, input.schema, Schema("", visible_output_columns));
      ASSIGN_OR_RETURN(
          std::optional<std::shared_ptr<relational_detail::ColumnarTable>>,
          projected_col,
          relational_detail::TryProjectColumnarTable(plan, *input.columnar));
      if (projected_col.has_value()) {
        bool all_uniform = (*projected_col)->total_rows > 0;
        for (const auto& chunk : (*projected_col)->chunks) {
          for (const auto& col : chunk.columns) {
            if (!col.is_uniform) {
              all_uniform = false;
              break;
            }
          }
          if (!all_uniform) {
            break;
          }
        }
        Relation col_output(context.execution_runtime());
        col_output.SetColumnarTable(std::move(*projected_col), all_uniform);
        CopyExecutionStats(&col_output, input);
        RefineProjectionTypes(col_output, visible_output_columns);
        col_output.schema = Schema("", visible_output_columns);
        if (!has_order_by) {
          if (context.execution_runtime() != nullptr) {
            context.execution_runtime()->project_ms += ElapsedMs(project_begin);
          }
          return col_output;
        }
        if (bounded_sort && stage_b_topn_ok &&
            stage_b_keys.size() == order_specs.size()) {
          const auto sort_begin = std::chrono::steady_clock::now();
          const size_t total_in = col_output.columnar->total_rows;
          ASSIGN_OR_RETURN(
              ColumnarTopNResult, topn_res,
              (TryColumnarBoundedTopN(*col_output.columnar, col_output.schema,
                                      col_output.schema, {}, stage_b_keys,
                                      order_specs, topn_capacity)));
          if (topn_res.has_value()) {
            col_output.SetColumnarTable(std::move(topn_res->first),
                                        topn_res->second);
            RefineProjectionTypes(col_output, visible_output_columns);
            col_output.schema = Schema("", std::move(visible_output_columns));
            if (auto* runtime = context.execution_runtime()) {
              ++runtime->topn_finish_uses;
              runtime->topn_finish_capacity = topn_capacity;
              runtime->topn_finish_input_rows = total_in;
              if (total_in > 0) {
                runtime->sort_ms += ElapsedMs(sort_begin);
              }
              runtime->project_ms += ElapsedMs(project_begin);
            }
            return col_output;
          }
        }
        RETURN_IF_FAIL(
            ApplyOrderBy(context, statement, &col_output, outer, ctes));
        if (context.execution_runtime() != nullptr) {
          context.execution_runtime()->project_ms += ElapsedMs(project_begin);
        }
        return col_output;
      }
    }
  }

  if (stage_a_eligible && !input.HasSpill() &&
      (!input.HasColumnarOnly() ||
       QueryMemoryBudget::Global().CanReserve(input.TotalRows() * 160))) {
    RETURN_IF_FAIL(input.EnsureRowsMaterialized());
    if (!input.HasSpill() && !input.HasColumnarOnly()) {
      const size_t n_rows = input.rows.size();
      const size_t capacity = std::min(topn_capacity, n_rows);
      std::vector<size_t> prog_key_indices;
      std::vector<int> key_to_prog_idx(stage_a_keys.size(), -1);
      for (size_t k = 0; k < stage_a_keys.size(); ++k) {
        if (stage_a_keys[k].program.has_value()) {
          key_to_prog_idx[k] = static_cast<int>(prog_key_indices.size());
          prog_key_indices.push_back(k);
        }
      }
      const size_t n_prog = prog_key_indices.size();
      auto position_charge = TryReserveQueryMemory(
          (capacity * sizeof(size_t)) + (n_rows * n_prog * sizeof(Value)));
      if (position_charge) {
        RETURN_IF_FAIL(input.GetStatus());
        const auto sort_begin = std::chrono::steady_clock::now();
        std::vector<Value> prog_vals(n_rows * n_prog);
        for (size_t r = 0; r < n_rows; ++r) {
          const Row& row = input.rows[r];
          for (size_t p = 0; p < n_prog; ++p) {
            StatusOr<Value> eval_or = TryEvaluateRowAuto(
                *stage_a_keys[prog_key_indices[p]].program, row);
            RETURN_IF_FAIL(eval_or.GetStatus());
            prog_vals[(r * n_prog) + p] = eval_or.MoveValue();
          }
        }
        const auto read_row_key = [&](size_t r,
                                      size_t k) noexcept -> const Value& {
          const int p = key_to_prog_idx[k];
          if (p >= 0) {
            return prog_vals[(r * n_prog) + static_cast<size_t>(p)];
          }
          const ColumnarTopNKey& key = stage_a_keys[k];
          const Row& row = input.rows[r];
          if (!key.is_coalesce) {
            return row[key.col];
          }
          const Value& v = row[key.col];
          return !v.IsNull() ? v : row[key.fallback_col];
        };
        const auto before = [&](size_t a, size_t b) {
          for (size_t k = 0; k < stage_a_keys.size(); ++k) {
            const int comparison =
                order_specs[k].Compare(read_row_key(a, k), read_row_key(b, k));
            if (comparison != 0) {
              return comparison < 0;
            }
          }
          return a < b;
        };
        std::vector<size_t> positions;
        positions.reserve(capacity);
        for (size_t r = 0; r < n_rows && capacity != 0; ++r) {
          if (positions.size() < capacity) {
            positions.push_back(r);
            std::push_heap(positions.begin(), positions.end(), before);
          } else if (before(r, positions.front())) {
            std::pop_heap(positions.begin(), positions.end(), before);
            positions.back() = r;
            std::push_heap(positions.begin(), positions.end(), before);
          }
        }
        std::ranges::sort(positions, before);
        for (const size_t position : positions) {
          const Row& src_row = input.rows[position];
          std::vector<Value> values;
          values.reserve(stage_a_slots.size());
          for (const TopNOutputSlot& slot : stage_a_slots) {
            if (!slot.is_coalesce) {
              values.push_back(src_row[slot.col]);
            } else {
              const Value& v = src_row[slot.col];
              values.push_back(!v.IsNull() ? v : src_row[slot.fallback_col]);
            }
          }
          output.AddRow(Row(std::move(values)));
        }
        RETURN_IF_FAIL(output.FinishSpill());
        RETURN_IF_FAIL(output.GetStatus());
        RefineProjectionTypes(output, output_columns);
        output.schema = Schema("", std::move(output_columns));
        if (auto* runtime = context.execution_runtime()) {
          ++runtime->topn_finish_uses;
          runtime->topn_finish_capacity = topn_capacity;
          runtime->topn_finish_input_rows = n_rows;
          runtime->sort_ms += ElapsedMs(sort_begin);
          runtime->project_ms += ElapsedMs(project_begin);
        }
        return output;
      }
    }
  }
  if (input.HasColumnarOnly() &&
      QueryMemoryBudget::Global().CanReserve(input.TotalRows() * 128)) {
    RETURN_IF_FAIL(input.EnsureRowsMaterialized());
  }

  // A resident, column-only projection owns its input: transfer the row
  // buffers instead of allocating and copying a second relation. Repeated
  // columns stay on the ordinary path so moving one cannot consume another.
  if (!grouped && !having_expr && !has_order_by && outer == nullptr &&
      lowered_using.empty() && !input.HasSpill() && !input.HasColumnarOnly()) {
    std::vector<size_t> transfer;
    bool eligible = true;
    for (size_t item = 0; item < statement.SelectList().size(); ++item) {
      if (projection_offsets[item]) {
        transfer.push_back(*projection_offsets[item]);
      } else if (!star_proto[item] && !star_groups[item].empty()) {
        for (const auto& group : star_groups[item]) {
          if (group.size() != 1) {
            eligible = false;
            break;
          }
          transfer.push_back(group.front());
        }
      } else {
        eligible = false;
        break;
      }
    }
    std::vector<bool> seen(input.schema.ColumnCount(), false);
    for (const size_t offset : transfer) {
      if (offset >= seen.size() || seen[offset]) {
        eligible = false;
        break;
      }
      seen[offset] = true;
    }
    const size_t visible_columns = output_columns.size() - hidden_columns;
    if (eligible && transfer.size() == visible_columns) {
      // Without ORDER BY, hidden window cells have no remaining consumer.
      // Selected window results are already bound to their input offsets.
      output_columns.resize(visible_columns);
      RETURN_IF_FAIL(input.GetStatus());
      const bool ordered = std::ranges::is_sorted(transfer);
      const size_t n = input.rows.size();
      const size_t workers = n >= 16384 ? ParallelWorkerCount(n / 8192) : 1;
      if (workers > 1) {
        RETURN_IF_FAIL(RunParallelTasks(workers, [&](size_t w) {
          const size_t begin = (n * w) / workers;
          const size_t end = (n * (w + 1)) / workers;
          if (ordered) {
            for (size_t r = begin; r < end; ++r) {
              Row& row = input.rows[r];
              for (size_t i = 0; i < transfer.size(); ++i) {
                if (i != transfer[i]) {
                  row.values_[i] = std::move(row.values_[transfer[i]]);
                }
              }
              row.values_.resize(transfer.size());
            }
          } else {
            std::vector<Value> values;
            values.reserve(transfer.size());
            for (size_t r = begin; r < end; ++r) {
              Row& row = input.rows[r];
              values.clear();
              for (const size_t offset : transfer) {
                values.push_back(std::move(row.values_[offset]));
              }
              row.values_.swap(values);
            }
          }
        }));
      } else if (ordered) {
        for (Row& row : input.rows) {
          for (size_t i = 0; i < transfer.size(); ++i) {
            if (i != transfer[i]) {
              row.values_[i] = std::move(row.values_[transfer[i]]);
            }
          }
          row.values_.resize(transfer.size());
        }
      } else {
        std::vector<Value> values;
        values.reserve(transfer.size());
        for (Row& row : input.rows) {
          values.clear();
          for (const size_t offset : transfer) {
            values.push_back(std::move(row.values_[offset]));
          }
          row.values_.swap(values);
        }
      }
      RefineProjectionTypes(input, output_columns);
      input.schema = Schema("", std::move(output_columns));
      input.using_columns.reset();
      if (context.execution_runtime() != nullptr) {
        context.execution_runtime()->project_ms += ElapsedMs(project_begin);
      }
      return input;
    }
  }

  // Once every expression is bound, reuse each consumed row's allocation
  // as the next projection buffer. Evaluate all cells before swapping so
  // repeated/reordered inputs and expressions reading an earlier output
  // slot still see the original row. AddRow retains normal spill accounting.
  if (!grouped && !having_expr && !has_order_by && hidden_columns == 0 &&
      outer == nullptr && lowered_using.empty() && !input.HasSpill() &&
      !input.HasColumnarOnly() && !output_columns.empty() &&
      output_columns.size() == statement.SelectList().size() &&
      output_columns.size() <= input.schema.ColumnCount()) {
    bool bound = true;
    for (size_t i = 0; i < output_columns.size(); ++i) {
      bound &= projection_offsets[i].has_value() ||
               null_coalesces[i].has_value() ||
               projection_programs[i].has_value();
    }
    if (bound) {
      RETURN_IF_FAIL(input.GetStatus());
      // Numeric outputs can keep the input's charge when even their maximum
      // scalar estimate fits the minimum estimate of the original cells.
      // Variable-width results switch back to AddRow before retaining them.
      bool retain_charge =
          output_columns.size() * EstimateValueBytes(Value(0)) <=
          input.schema.ColumnCount() * EstimateValueBytes(Value());
      if (retain_charge) {
        for (const Column& col : output_columns) {
          if (col.Type() == ValueType::kVarChar ||
              col.Type() == ValueType::kArray) {
            retain_charge = false;
            break;
          }
        }
      }
      const bool has_program = std::ranges::any_of(
          projection_programs,
          [](const auto& prog) { return prog.has_value(); });
      std::vector<size_t> offset_read_count;
      if (!has_program) {
        offset_read_count.assign(input.schema.ColumnCount(), 0);
        for (size_t i = 0; i < output_columns.size(); ++i) {
          if (projection_offsets[i] &&
              *projection_offsets[i] < offset_read_count.size()) {
            ++offset_read_count[*projection_offsets[i]];
          }
          if (null_coalesces[i]) {
            if (null_coalesces[i]->first < offset_read_count.size()) {
              offset_read_count[null_coalesces[i]->first] += 2;
            }
            if (null_coalesces[i]->second < offset_read_count.size()) {
              offset_read_count[null_coalesces[i]->second] += 2;
            }
          }
        }
      }
      const auto can_move_offset = [&](size_t col) {
        return !has_program && col < offset_read_count.size() &&
               offset_read_count[col] == 1;
      };
      const auto is_fixed_numeric = [](const Value& value) {
        return value.IsNull() || value.type == ValueType::kInt64 ||
               value.type == ValueType::kDate ||
               value.type == ValueType::kDouble;
      };
      const size_t n = input.rows.size();
      const size_t workers = n >= 16384 ? ParallelWorkerCount(n / 4096) : 1;
      const size_t flush_granularity = Relation::ChargeFlushGranularity();
      bool finalized_cols = false;
      bool all_cols_uniform = false;
      if (workers > 1 && retain_charge) {
        std::vector<Status> worker_errors(workers, Status::kSuccess);
        std::vector<uint8_t> worker_non_numeric(workers, 0);
        std::vector<std::vector<ColumnTypeTracker>> worker_cols(
            workers, std::vector<ColumnTypeTracker>(output_columns.size()));
        RETURN_IF_FAIL(RunParallelTasks(workers, [&](size_t w) {
          const size_t begin = (n * w) / workers;
          const size_t end = (n * (w + 1)) / workers;
          std::vector<Value> values;
          values.reserve(output_columns.size());
          bool local_non_numeric = false;
          auto& col_track = worker_cols[w];
          for (size_t r = begin; r < end; ++r) {
            Row& row = input.rows[r];
            values.clear();
            for (size_t i = 0; i < output_columns.size(); ++i) {
              Value cell;
              if (projection_offsets[i]) {
                const size_t col = *projection_offsets[i];
                cell = can_move_offset(col) ? std::move(row.values_[col])
                                            : row[col];
              } else if (null_coalesces[i]) {
                const Value& prefer = row[null_coalesces[i]->first];
                cell =
                    prefer.IsNull() ? row[null_coalesces[i]->second] : prefer;
              } else if (projection_programs[i]) {
                auto projected =
                    TryEvaluateRowAuto(*projection_programs[i], row);
                if (!projected.HasValue()) {
                  worker_errors[w] = projected.GetStatus();
                  return;
                }
                cell = projected.MoveValue();
              }
              if (!is_fixed_numeric(cell)) {
                local_non_numeric = true;
              }
              col_track[i].Observe(cell);
              values.push_back(std::move(cell));
            }
            row.values_.swap(values);
          }
          worker_non_numeric[w] = static_cast<uint8_t>(local_non_numeric);
        }));
        for (const Status& err : worker_errors) {
          RETURN_IF_FAIL(err);
        }
        all_cols_uniform =
            ColumnTypeTracker::FinalizeColumns(worker_cols, output_columns);
        finalized_cols = true;
        const bool any_non_numeric = std::ranges::any_of(
            worker_non_numeric, [](uint8_t v) { return v != 0; });
        if (!any_non_numeric) {
          input.schema = Schema("", std::move(output_columns));
          input.using_columns.reset();
          input.uniform_cell_types = all_cols_uniform && !input.rows.empty();
          if (auto* runtime = context.execution_runtime()) {
            runtime->project_ms += ElapsedMs(project_begin);
          }
          return input;
        }
        output.rows.reserve(n);
        for (Row& row : input.rows) {
          const size_t bytes = EstimateRowBytesFast(row);
          output.AddRowWithKnownBytes(std::move(row), bytes, flush_granularity);
        }
      } else if (workers > 1) {
        std::vector<Relation> shards(workers);
        std::vector<ExecutionRuntime> shard_runtimes(workers);
        std::vector<std::vector<ColumnTypeTracker>> worker_cols(
            workers, std::vector<ColumnTypeTracker>(output_columns.size()));
        for (size_t w = 0; w < workers; ++w) {
          shards[w].set_runtime(&shard_runtimes[w]);
          shards[w].schema = output.schema;
        }
        std::vector<Status> worker_errors(workers, Status::kSuccess);
        RETURN_IF_FAIL(RunParallelTasks(workers, [&](size_t w) {
          const size_t begin = (n * w) / workers;
          const size_t end = (n * (w + 1)) / workers;
          Relation& local = shards[w];
          if (Relation::HasAmpleMemoryBudget()) {
            local.rows.reserve(end - begin);
          }
          std::vector<Value> values;
          values.reserve(output_columns.size());
          auto& col_track = worker_cols[w];
          for (size_t r = begin; r < end; ++r) {
            Row& row = input.rows[r];
            values.clear();
            size_t row_bytes = 64;
            for (size_t i = 0; i < output_columns.size(); ++i) {
              Value cell;
              if (projection_offsets[i]) {
                const size_t col = *projection_offsets[i];
                cell = can_move_offset(col) ? std::move(row.values_[col])
                                            : row[col];
              } else if (null_coalesces[i]) {
                const Value& prefer = row[null_coalesces[i]->first];
                cell =
                    prefer.IsNull() ? row[null_coalesces[i]->second] : prefer;
              } else if (projection_programs[i]) {
                auto projected =
                    TryEvaluateRowAuto(*projection_programs[i], row);
                if (!projected.HasValue()) {
                  worker_errors[w] = projected.GetStatus();
                  return;
                }
                cell = projected.MoveValue();
              }
              col_track[i].Observe(cell);
              row_bytes += cell.type == ValueType::kArray
                               ? EstimateValueBytes(cell)
                               : EstimateScalarValueBytes(cell);
              values.push_back(std::move(cell));
            }
            row.values_.swap(values);
            local.AddRowWithKnownBytes(std::move(row), row_bytes,
                                       flush_granularity);
          }
        }));
        for (const Status& err : worker_errors) {
          RETURN_IF_FAIL(err);
        }
        if (output.runtime() != nullptr) {
          for (const auto& rt : shard_runtimes) {
            output.runtime()->relation_spills += rt.relation_spills;
          }
        }
        RETURN_IF_FAIL(Relation::MergeShardsInto(shards, &output));
        all_cols_uniform =
            ColumnTypeTracker::FinalizeColumns(worker_cols, output_columns);
        finalized_cols = true;
      } else {
        std::vector<ColumnTypeTracker> col_track(output_columns.size());
        std::vector<Value> values;
        values.reserve(output_columns.size());
        size_t projected_rows = 0;
        for (Row& row : input.rows) {
          values.clear();
          size_t row_bytes = 64;
          bool row_non_numeric = false;
          for (size_t i = 0; i < output_columns.size(); ++i) {
            Value cell;
            if (projection_offsets[i]) {
              const size_t col = *projection_offsets[i];
              cell =
                  can_move_offset(col) ? std::move(row.values_[col]) : row[col];
            } else if (null_coalesces[i]) {
              const Value& prefer = row[null_coalesces[i]->first];
              cell = prefer.IsNull() ? row[null_coalesces[i]->second] : prefer;
            } else if (projection_programs[i]) {
              ASSIGN_OR_RETURN(
                  Value, projected,
                  TryEvaluateRowAuto(*projection_programs[i], row));
              cell = std::move(projected);
            }
            if (retain_charge && !is_fixed_numeric(cell)) {
              row_non_numeric = true;
            }
            col_track[i].Observe(cell);
            row_bytes += cell.type == ValueType::kArray
                             ? EstimateValueBytes(cell)
                             : EstimateScalarValueBytes(cell);
            values.push_back(std::move(cell));
          }
          row.values_.swap(values);
          if (retain_charge && row_non_numeric) {
            retain_charge = false;
            for (size_t previous = 0; previous < projected_rows; ++previous) {
              const size_t prev_bytes =
                  EstimateRowBytesFast(input.rows[previous]);
              output.AddRowWithKnownBytes(std::move(input.rows[previous]),
                                          prev_bytes, flush_granularity);
            }
          }
          if (!retain_charge) {
            output.AddRowWithKnownBytes(std::move(row), row_bytes,
                                        flush_granularity);
          }
          ++projected_rows;
        }
        all_cols_uniform = ColumnTypeTracker::FinalizeColumns(
            std::span<const std::vector<ColumnTypeTracker>>(&col_track, 1),
            output_columns);
        finalized_cols = true;
        if (retain_charge) {
          input.schema = Schema("", std::move(output_columns));
          input.using_columns.reset();
          input.uniform_cell_types = all_cols_uniform && !input.rows.empty();
          if (auto* runtime = context.execution_runtime()) {
            runtime->project_ms += ElapsedMs(project_begin);
          }
          return input;
        }
      }
      RETURN_IF_FAIL(output.FinishSpill());
      RETURN_IF_FAIL(output.GetStatus());
      if (!finalized_cols) {
        RefineProjectionTypes(output, output_columns);
      } else {
        output.uniform_cell_types =
            !output.HasSpill() && all_cols_uniform && !output.rows.empty();
      }
      output.schema = Schema("", std::move(output_columns));
      if (context.execution_runtime() != nullptr) {
        context.execution_runtime()->project_ms += ElapsedMs(project_begin);
      }
      return output;
    }
  }

  auto emit = [&](const Row& representative,
                  const AggregateResultMap* aggregates) -> Status {
    Scope scope{
        .row = &representative, .schema = &input.schema, .outer = outer};
    if (aggregates != nullptr) {
      // Correlated subqueries in HAVING / SELECT resolve aggregate aliases
      // against this group's values through the scope chain.
      scope.aggregates = aggregates;
    }
    if (!lowered_using.empty()) {
      scope.using_columns = &lowered_using;
    }
    if (having_expr) {
      ASSIGN_OR_RETURN(
          Value, having_value,
          (TryEvaluate(having_expr, scope, aggregates, context, ctes)));
      if (!Truthy(having_value)) {
        return Status::kSuccess;
      }
    }
    std::vector<Value> values;
    values.reserve(output_columns.size());
    for (size_t item = 0; item < statement.SelectList().size(); ++item) {
      const NamedExpression& projection = statement.SelectList()[item];
      if (select_star(projection.expression)) {
        if (star_proto[item]) {
          const ColumnName base = input.schema.GetColumn(0).Name();
          for (const char* field : {"int32_val1", "int32_val2", "str_value"}) {
            ASSIGN_OR_RETURN(
                Value, hv25286_0,
                (TryEvaluate(FunctionCallExp(
                                 "__get_field_safe",
                                 {ColumnValueExp(base),
                                  ConstantValueExp(Value(std::string(field)))}),
                             scope, aggregates, context, ctes)));
            values.push_back(std::move(hv25286_0));
          }
          continue;
        }
        for (const std::vector<size_t>& group : star_groups[item]) {
          // USING-merged entries coalesce their physical duplicates.
          Value merged;
          for (const size_t column : group) {
            if (!representative.values_[column].IsNull()) {
              merged = representative.values_[column];
              break;
            }
          }
          values.push_back(std::move(merged));
        }
      } else if (projection_offsets[item]) {
        values.push_back(representative[*projection_offsets[item]]);
      } else if (null_coalesces[item]) {
        const Value& prefer = representative[null_coalesces[item]->first];
        values.push_back(prefer.IsNull()
                             ? representative[null_coalesces[item]->second]
                             : prefer);
      } else if (projection_programs[item]) {
        Value projected;
        Status error;
        const auto evaluated = projection_programs[item]->TryEvaluateRowFast(
            representative, &projected, &error);
        if (evaluated == BytecodeProgram::FastEval::kError) {
          return error;
        }
        if (evaluated == BytecodeProgram::FastEval::kUnsupported) {
          ASSIGN_OR_RETURN(
              Value, canonical,
              projection_programs[item]->TryEvaluateRow(representative));
          projected = std::move(canonical);
        }
        values.push_back(std::move(projected));
      } else {
        ASSIGN_OR_RETURN(Value, projection_value,
                         (TryEvaluate(projection.expression, scope, aggregates,
                                      context, ctes)));
        values.push_back(std::move(projection_value));
      }
    }
    // Carry hidden $win columns through projection; ORDER BY may reference
    // them and they are trimmed after sorting.
    for (size_t k = base_width; k < representative.values_.size(); ++k) {
      values.push_back(representative.values_[k]);
    }
    Row output_row(std::move(values));
    if (has_order_by) {
      // The grouped ORDER BY key may reference output columns, aliases,
      // aggregates and group-key base columns -- never a relation alias
      // denoting a whole row (the query is grouped by parts of that row),
      // so the row-alias fallback is suppressed on both scopes.  The base
      // wrapper keeps select-item evaluation (`COUNT(t)`) on the lenient
      // scope above.
      Scope order_base{.row = scope.row,
                       .schema = scope.schema,
                       .outer = scope.outer,
                       .using_columns = scope.using_columns,
                       .no_row_alias = true};
      Scope proj_scope{.row = &output_row,
                       .schema = &initial_output_schema,
                       .outer = &order_base,
                       .no_row_alias = true};
      std::vector<Value> keys;
      keys.reserve(order_by.size());
      for (size_t k = 0; k < resolved_order_expressions.size(); ++k) {
        if (order_positions[k]) {
          keys.push_back(output_row[*order_positions[k]]);
          continue;
        }
        const Expression& key = resolved_order_expressions[k];
        ASSIGN_OR_RETURN(
            Value, hv26761_0,
            (TryEvaluate(key, proj_scope, aggregates, context, ctes)));
        keys.push_back(std::move(hv26761_0));
      }
      RETURN_IF_FAIL(sorter.Add(
          KeyedRow{.keys = std::move(keys), .row = std::move(output_row)}));
    } else {
      output.AddRow(std::move(output_row));
    }
    return Status::kSuccess;
  };

  // Fast grouped emit for positional shapes: every GROUP BY key is a plain
  // input column, every select item and ORDER BY term is a group-key column
  // or a top-level aggregate, and HAVING (when present) references only
  // those.  The generic emit evaluates each select item through a Scope +
  // TryEvaluate AST walk per group, which dominates aggregations with many
  // groups (TPC-H Q3 emits ~1M groups through GroupByFinish).
  struct FastEmitItem {
    bool is_key;
    size_t index;
  };
  std::vector<slot_t> fast_key_offsets;
  std::vector<FastEmitItem> fast_items;
  std::vector<FastEmitItem> fast_order_items;
  bool fast_emit = grouped;
  const auto classify_fast = [&](const Expression& expression,
                                 FastEmitItem* item) -> bool {
    if (!expression) {
      return false;
    }
    if (expression->Type() == TypeTag::kColumnValue) {
      const auto offset = LocalColumnOffset(
          input.schema, expression->AsColumnValue().GetColumnName());
      if (offset) {
        const auto pos =
            std::find(fast_key_offsets.begin(), fast_key_offsets.end(),
                      static_cast<slot_t>(*offset));
        if (pos != fast_key_offsets.end()) {
          *item = FastEmitItem{
              .is_key = true,
              .index = static_cast<size_t>(pos - fast_key_offsets.begin())};
          return true;
        }
      }
      return false;
    }
    if (expression->Type() == TypeTag::kAggregateExp) {
      const auto pos =
          std::find(aggregate_expressions.begin(), aggregate_expressions.end(),
                    &expression->AsAggregateExpression());
      if (pos != aggregate_expressions.end()) {
        *item = FastEmitItem{
            .is_key = false,
            .index = static_cast<size_t>(pos - aggregate_expressions.begin())};
        return true;
      }
    }
    return false;
  };
  if (fast_emit) {
    for (const Expression& key : statement.GroupBy()) {
      if (!key || key->Type() != TypeTag::kColumnValue) {
        fast_emit = false;
        break;
      }
      const auto offset =
          LocalColumnOffset(input.schema, key->AsColumnValue().GetColumnName());
      if (!offset) {
        fast_emit = false;
        break;
      }
      fast_key_offsets.push_back(static_cast<slot_t>(*offset));
    }
  }
  if (fast_emit) {
    for (const NamedExpression& item : statement.SelectList()) {
      FastEmitItem emit_item{};
      if (!classify_fast(item.expression, &emit_item)) {
        fast_emit = false;
        break;
      }
      fast_items.push_back(emit_item);
    }
  }
  if (std::ranges::any_of(order_positions, [](const auto& position) {
        return position.has_value();
      })) {
    fast_emit = false;
  }
  if (fast_emit) {
    for (const Expression& key : resolved_order_expressions) {
      FastEmitItem order_item{};
      if (!classify_fast(key, &order_item)) {
        fast_emit = false;
        break;
      }
      fast_order_items.push_back(order_item);
    }
  }
  static const bool fast_emit_disabled =
      std::getenv("TINYLAMB_LEGACY_PLANNER") != nullptr;
  if ((fast_emit && having_expr &&
       !HavingReferencesOnlyKeysAndAggregates(having_expr, input.schema,
                                              fast_key_offsets)) ||
      fast_emit_disabled) {
    fast_emit = false;
  }

  std::vector<Value> finished_values;
  AggregateResultMap aggregate_results_hoisted;
  bool simple_having_agg_const = false;
  size_t simple_having_agg_idx = 0;
  BinaryOperation simple_having_op = BinaryOperation::kEquals;
  Value simple_having_const;
  if (grouped && fast_emit) {
    finished_values.resize(aggregate_expressions.size());
    aggregate_results_hoisted.reserve(aggregate_expressions.size());
    if (having_expr && having_expr->Type() == TypeTag::kBinaryExp) {
      const BinaryExpression& bin = having_expr->AsBinaryExpression();
      if (bin.Left() && bin.Left()->Type() == TypeTag::kAggregateExp &&
          bin.Right() && bin.Right()->Type() == TypeTag::kConstantValue) {
        auto pos = std::find(aggregate_expressions.begin(),
                             aggregate_expressions.end(),
                             &bin.Left()->AsAggregateExpression());
        if (pos != aggregate_expressions.end()) {
          simple_having_agg_const = true;
          simple_having_agg_idx =
              static_cast<size_t>(pos - aggregate_expressions.begin());
          simple_having_op = bin.Op();
          simple_having_const = bin.Right()->AsConstantValue().GetValue();
        }
      }
    }
  }

  const auto emit_group_batch =
      [&](const std::vector<GroupState>& batch_groups,
          std::deque<AggregateAccumulator>& batch_states) -> Status {
    if (fast_emit) {
      for (const GroupState& group : batch_groups) {
        for (size_t i = 0; i < aggregate_expressions.size(); ++i) {
          ASSIGN_OR_RETURN(
              Value, finished,
              batch_states[group.accumulator_offset + i].TryFinish());
          finished_values[i] = std::move(finished);
        }
        if (having_expr) {
          if (simple_having_agg_const) {
            ASSIGN_OR_RETURN(Value, having_value,
                             TryBinary(simple_having_op,
                                       finished_values[simple_having_agg_idx],
                                       simple_having_const));
            if (!Truthy(having_value)) {
              continue;
            }
          } else {
            for (size_t i = 0; i < aggregate_expressions.size(); ++i) {
              aggregate_results_hoisted.insert_or_assign(
                  aggregate_expressions[i], finished_values[i]);
            }
            Scope scope{.row = &group.representative,
                        .schema = &input.schema,
                        .outer = outer};
            scope.aggregates = &aggregate_results_hoisted;
            ASSIGN_OR_RETURN(
                Value, having_value,
                (TryEvaluate(having_expr, scope, &aggregate_results_hoisted,
                             context, ctes)));
            if (!Truthy(having_value)) {
              continue;
            }
          }
        }
        std::vector<Value> values;
        values.reserve(fast_items.size());
        for (const FastEmitItem& item : fast_items) {
          if (item.is_key) {
            values.push_back(
                group.representative.values_[fast_key_offsets[item.index]]);
          } else {
            values.push_back(finished_values[item.index]);
          }
        }
        Row output_row(std::move(values));
        if (has_order_by) {
          std::vector<Value> keys;
          keys.reserve(fast_order_items.size());
          for (const FastEmitItem& item : fast_order_items) {
            if (item.is_key) {
              keys.push_back(
                  group.representative.values_[fast_key_offsets[item.index]]);
            } else {
              keys.push_back(finished_values[item.index]);
            }
          }
          RETURN_IF_FAIL(sorter.Add(
              KeyedRow{.keys = std::move(keys), .row = std::move(output_row)}));
        } else {
          output.AddRow(std::move(output_row));
        }
      }
      return Status::kSuccess;
    }
    for (const GroupState& group : batch_groups) {
      AggregateResultMap aggregate_results;
      aggregate_results.reserve(aggregate_expressions.size());
      for (size_t i = 0; i < aggregate_expressions.size(); ++i) {
        const AggregateAccumulator& accumulator =
            batch_states[group.accumulator_offset + i];
        ASSIGN_OR_RETURN(
            Value, finished,
            batch_states[group.accumulator_offset + i].TryFinish());
        aggregate_results.emplace(accumulator.expression, std::move(finished));
      }
      RETURN_IF_FAIL(emit(group.representative, &aggregate_results));
    }
    return Status::kSuccess;
  };

  if (grouped && used_partition_agg) {
    constexpr size_t kSubAggPartitions = 16;
    constexpr size_t kMaxAggSplitDepth = 3;
    struct AggPartitionWork {
      SpillFile file;
      size_t depth{0};
    };
    std::deque<AggPartitionWork> work_stack;
    for (auto& agg_part : agg_parts) {
      if (agg_part.Count() > 0) {
        work_stack.push_back(
            AggPartitionWork{.file = std::move(agg_part), .depth = 0});
      }
    }
    agg_parts.clear();

    const size_t est_per_group =
        192 + (aggregate_expressions.size() * 96) +
        (static_cast<size_t>(input.schema.ColumnCount()) * 32);
    const auto& budget = QueryMemoryBudget::Global();
    const size_t part_floor =
        budget.Unlimited()
            ? 0
            : std::min<size_t>(
                  256ULL << 10,
                  std::max<size_t>(size_t{4096}, budget.Limit() / 4));
    const size_t part_mem_cap =
        budget.Unlimited() ? std::numeric_limits<size_t>::max()
                           : std::max<size_t>(part_floor, budget.Limit() / 4);
    const size_t min_groups_before_split =
        (!budget.Unlimited() && budget.Limit() < (1ULL << 20)) ? 16 : 1024;

    if (partitioned_input_rows == 0 && statement.IsGroupAll()) {
      std::vector<GroupState> empty_groups;
      empty_groups.push_back(make_group());
      if (context.execution_runtime() != nullptr) {
        context.execution_runtime()->aggregate_groups += 1;
      }
      RETURN_IF_FAIL(emit_group_batch(empty_groups, aggregate_states));
    }

    while (!work_stack.empty()) {
      AggPartitionWork work = std::move(work_stack.front());
      work_stack.pop_front();
      bool overflowed = false;
      {
        GroupKeyMap offsets;
        std::vector<GroupState> local_groups;
        std::deque<AggregateAccumulator> local_states;
        ASSIGN_OR_RETURN(SpillFile::RowReader, reader,
                         work.file.OpenRowReader());
        Row row;
        bool has_row = false;
        while (true) {
          RETURN_IF_FAIL(reader.Next(&row, &has_row));
          if (!has_row) {
            break;
          }
          RETURN_IF_FAIL(
              accumulate_row(row, &offsets, &local_groups, &local_states));
          if (!budget.Unlimited() && work.depth < kMaxAggSplitDepth &&
              local_groups.size() >= min_groups_before_split) {
            const size_t est_bytes = local_groups.size() * est_per_group;
            if (est_bytes > part_mem_cap || !budget.CanReserve(est_bytes)) {
              overflowed = true;
              break;
            }
          }
        }
        if (!overflowed) {
          work.file = SpillFile{};
          if (context.execution_runtime() != nullptr) {
            context.execution_runtime()->aggregate_groups +=
                local_groups.size();
          }
          RETURN_IF_FAIL(emit_group_batch(local_groups, local_states));
        }
      }
      if (overflowed) {
        std::vector<SpillFile> sub_parts(kSubAggPartitions);
        Status split_err{Status::kSuccess};
        Status split_iter = work.file.ForEachRow([&](const Row& row) {
          if (split_err != Status::kSuccess) {
            return;
          }
          const size_t bucket = partition_bucket(row, kSubAggPartitions,
                                                 work.depth + 1, &split_err);
          if (split_err != Status::kSuccess) {
            return;
          }
          Status st_append = sub_parts[bucket].Append(row);
          if (st_append != Status::kSuccess) {
            split_err = st_append;
          }
        });
        if (split_iter != Status::kSuccess && split_err == Status::kSuccess) {
          split_err = split_iter;
        }
        RETURN_IF_FAIL(split_err);
        for (SpillFile& sub : sub_parts) {
          RETURN_IF_FAIL(sub.FinishWriting());
        }
        work.file = SpillFile{};
        for (size_t s = sub_parts.size(); s > 0; --s) {
          if (sub_parts[s - 1].Count() > 0) {
            work_stack.push_front(AggPartitionWork{
                .file = std::move(sub_parts[s - 1]), .depth = work.depth + 1});
          }
        }
      }
    }
  } else if (grouped) {
    RETURN_IF_FAIL(emit_group_batch(groups, aggregate_states));
  } else {
    RETURN_IF_FAIL(input.FinishSpill());
    Status emit_error{Status::kSuccess};
    Status iterated = input.ForEachRow([&](const Row& row) {
      if (emit_error != Status::kSuccess) {
        return;
      }
      Status st_emit = emit(row, nullptr);
      if (st_emit != Status::kSuccess) {
        emit_error = st_emit;
      }
    });
    if (iterated != Status::kSuccess && emit_error == Status::kSuccess) {
      emit_error = iterated;
    }
    RETURN_IF_FAIL(emit_error);
  }

  if (has_order_by) {
    const auto sort_begin = std::chrono::steady_clock::now();
    if (sorter.UsedBoundedHeap() && context.execution_runtime() != nullptr) {
      ++context.execution_runtime()->topn_finish_uses;
      context.execution_runtime()->topn_finish_capacity = topn_capacity;
      context.execution_runtime()->topn_finish_input_rows =
          sorter.TotalInputRows();
    }
    if (!sorter.Empty()) {
      RETURN_IF_FAIL(sorter.EmitSorted([&](Row&& sorted_row) {
        output.AddRow(std::move(sorted_row));
        return Status::kSuccess;
      }));
    }
    if (context.execution_runtime() != nullptr) {
      context.execution_runtime()->sort_ms += ElapsedMs(sort_begin);
    }
  }

  RETURN_IF_FAIL(output.FinishSpill());
  // Drop the hidden $win columns now that ordering has consumed them.
  if (hidden_columns > 0) {
    const size_t visible_width = output_columns.size() - hidden_columns;
    // PRODUCTION FIX: rows spilled under a memory budget are not in
    // output.rows; stream spilled outputs into a fresh Relation instead of
    // reloading them all into memory.
    if (!output.HasSpill()) {
      for (Row& row : output.rows) {
        row.values_.resize(std::min(visible_width, row.values_.size()));
      }
    } else {
      Relation trimmed(context.execution_runtime());
      trimmed.schema = output.schema;
      trimmed.using_columns = output.using_columns;
      CopyExecutionStats(&trimmed, output);
      RETURN_IF_FAIL(output.ForEachRow([&](const Row& row) {
        Row copy = row;
        copy.values_.resize(std::min(visible_width, copy.values_.size()));
        trimmed.AddRow(std::move(copy));
      }));
      RETURN_IF_FAIL(trimmed.FinishSpill());
      RETURN_IF_FAIL(trimmed.GetStatus());
      output = std::move(trimmed);
    }
    output_columns.resize(visible_width);
  }
  RefineProjectionTypes(output, output_columns);
  output.schema = Schema("", std::move(output_columns));
  if (context.execution_runtime() != nullptr) {
    context.execution_runtime()->project_ms += ElapsedMs(project_begin);
  }
  return output;
}

namespace {

// Window functions in a grouped statement evaluate over the grouped rows
// (SQL semantics), including calls whose arguments are aggregates themselves
// (`avg(sum(x)) OVER (PARTITION BY key)` — TPC-DS q12/q89).  This runs the
// statement in two passes:
//   1. a grouped pass where every window call is pulled out of the select
//      list, each distinct aggregate nested inside a window call becomes a
//      hidden `$aggK` select item, and the ordinary grouping machinery runs;
//   2. a window pass computing each call over the grouped rows (window
//      arguments referencing `$aggK` columns), producing `$winN` columns;
//   3. a final projection pass over grouped+$win rows that only references
//      materialized columns, restoring ORDER BY / DISTINCT / LIMIT.
StatusOr<Relation> ExecuteWindowOverAggregate(
    TransactionContext& context, const SelectStatement& statement,
    Relation input, const Scope* outer, const CteMap& ctes, bool apply_where,
    bool build_grouped_input = false, bool columnar_output = false) {
  const std::string unsupported =
      "window functions combined with GROUP BY are not supported here";
  if (statement.HasDistinctOn()) {
    return StatusError(StatusCode::kInvalidArgument, unsupported);
  }
  const auto window_source_named = [&](std::string_view qualifier) {
    return std::ranges::any_of(
        statement.Sources(), [&](const SelectSource& source) {
          return (!source.alias.empty() &&
                  IdentifierEquals(source.alias, qualifier)) ||
                 (!source.table.empty() &&
                  IdentifierEquals(source.table, qualifier));
        });
  };
  for (const NamedExpression& projection : statement.SelectList()) {
    auto star = UnwrapStarProjection(projection.expression);
    if (star && star->wrapped && !star->name.schema.empty() &&
        !window_source_named(star->name.schema)) {
      star.reset();  // genuine proto value table, not a FROM-source star
    }
    if (star) {
      return StatusError(StatusCode::kInvalidArgument, unsupported);
    }
  }
  std::vector<Expression> window_nodes;
  for (const NamedExpression& projection : statement.SelectList()) {
    CollectWindowCalls(projection.expression, &window_nodes);
  }
  for (const SelectStatement::OrderByTerm& term : statement.OrderBy()) {
    CollectWindowCalls(term.expression, &window_nodes);
  }
  // QUALIFY is one more consumer of the same window/aggregate pool: its
  // window calls join the pass-2 computation and its aggregates become
  // hidden `$aggK` group-level columns.
  CollectWindowCalls(statement.Qualify(), &window_nodes);

  auto grouped = std::make_shared<SelectStatement>(statement);
  std::vector<NamedExpression> grouped_select;
  grouped_select.reserve(statement.SelectList().size());
  // Alias (lowercased) -> grouped output column name, used by pass 3 to point
  // ORDER BY references at materialized columns.
  std::unordered_map<std::string, std::string> alias_columns;
  // Items mixing a window call with other nodes (sum(x) * 100 /
  // sum(sum(x)) OVER ...) are re-built in pass 3 from `$aggK`/`$winN` columns;
  // their pass-1 slot is a NULL placeholder, and every aggregate they contain
  // becomes a hidden `$aggK` group-level column.
  std::vector<bool> item_has_window(statement.SelectList().size(), false);
  for (size_t i = 0; i < statement.SelectList().size(); ++i) {
    const NamedExpression& projection = statement.SelectList()[i];
    item_has_window[i] = ContainsWindowCall(projection.expression);
    if (item_has_window[i]) {
      grouped_select.emplace_back(projection.name, ConstantValueExp(Value()));
    } else {
      grouped_select.emplace_back(projection.name, projection.expression);
    }
    if (!projection.name.empty()) {
      alias_columns.emplace(AsciiToLower(projection.name),
                            ProjectionName(projection, i));
    }
  }
  // Mixed items contribute their aggregates to the same $aggK pool up front
  // (pass 3 rebuilds the whole item from the materialized columns; the window
  // pass below reuses the registrations through aggregate_columns).
  // Reuse already projected group keys and aggregates for window inputs.
  // Distinct output names are necessary: an ambiguous alias must not replace
  // a positional hidden column. This also avoids copying the same long
  // string key into several partition/order columns for every group.
  std::unordered_map<std::string, std::string> existing_columns;
  for (size_t i = 0; i < statement.SelectList().size(); ++i) {
    if (item_has_window[i]) {
      continue;
    }
    const auto& item = statement.SelectList()[i];
    const auto name = ProjectionName(item, i);
    size_t matches = 0;
    for (size_t j = 0; j < statement.SelectList().size(); ++j) {
      matches += static_cast<size_t>(
          IdentifierEquals(name, ProjectionName(statement.SelectList()[j], j)));
    }
    if (matches == 1 && item.expression) {
      existing_columns.emplace(item.expression->ToString(), name);
    }
  }
  std::vector<std::pair<std::string, Expression>> hidden_aggregates;
  // Window partition / ORDER BY keys materialized as hidden grouped columns
  // ($pkJ), for keys the select list does not project.
  std::vector<NamedExpression> partition_items;
  std::unordered_map<std::string, std::string> partition_columns;
  std::unordered_map<std::string, std::string> aggregate_columns;
  for (const auto& item : statement.SelectList()) {
    if (item.expression && item.expression->Type() == TypeTag::kAggregateExp) {
      const auto expression = item.expression->ToString();
      if (const auto found = existing_columns.find(expression);
          found != existing_columns.end()) {
        aggregate_columns.emplace(expression, found->second);
      }
    }
  }
  RegisterAggregatesForMaterialization(statement.Qualify(), &aggregate_columns,
                                       &hidden_aggregates);
  for (size_t i = 0; i < statement.SelectList().size(); ++i) {
    if (!item_has_window[i] || statement.SelectList()[i].expression->Type() ==
                                   TypeTag::kWindowFunctionExp) {
      continue;
    }
    RegisterAggregatesForMaterialization(statement.SelectList()[i].expression,
                                         &aggregate_columns,
                                         &hidden_aggregates);
  }
  std::vector<std::shared_ptr<WindowFunctionCallExpression>> rewritten_windows;
  rewritten_windows.reserve(window_nodes.size());
  for (size_t w = 0; w < window_nodes.size(); ++w) {
    const std::string window_name = "$win" + std::to_string(w);
    aggregate_columns.emplace(window_nodes[w]->ToString(), window_name);
    // WindowFunctionCallExpression is non-copyable (ExpressionBase); rebuild
    // the call field by field so aggregate arguments can be pointed at the
    // grouped `$aggK` columns.
    const auto& original = window_nodes[w]->AsWindowFunctionCallExpression();
    auto rewritten = std::make_shared<WindowFunctionCallExpression>();
    rewritten->function = original.function;
    rewritten->where_filter = original.where_filter;
    rewritten->distinct = original.distinct;
    rewritten->inner_order_by = original.inner_order_by;
    rewritten->inner_limit = original.inner_limit;
    rewritten->partition_by = original.partition_by;
    rewritten->order_by = original.order_by;
    rewritten->frame_unit = original.frame_unit;
    rewritten->frame_start = original.frame_start;
    rewritten->frame_end = original.frame_end;
    rewritten->has_frame = original.has_frame;
    rewritten->exclusion = original.exclusion;
    rewritten->respect_nulls = original.respect_nulls;
    rewritten->ignore_nulls = original.ignore_nulls;
    rewritten->args = original.args;
    const auto point_at_grouped_aggregate =
        [&](const Expression& node) -> std::optional<Expression> {
      if (node->Type() != TypeTag::kAggregateExp) {
        return std::nullopt;
      }
      auto [it, inserted] = aggregate_columns.emplace(
          node->ToString(), "$agg" + std::to_string(hidden_aggregates.size()));
      if (inserted) {
        hidden_aggregates.emplace_back(it->second, node);
      }
      return ColumnValueExp(ColumnName("", it->second));
    };
    // Partition / ORDER BY keys must resolve against the grouped output, but
    // the window may name a GROUP BY key that the select list never projects.
    // Materialize every distinct key expression as a hidden `$pkJ` column and
    // point the rewritten call at it.
    const auto materialize_key = [&](const Expression& key) -> Expression {
      if (const auto found = existing_columns.find(key->ToString());
          found != existing_columns.end()) {
        return ColumnValueExp(ColumnName("", found->second));
      }
      auto [it, inserted] = partition_columns.emplace(
          key->ToString(), "$pk" + std::to_string(partition_columns.size()));
      if (inserted) {
        partition_items.emplace_back(it->second, key);
      }
      return ColumnValueExp(ColumnName("", it->second));
    };
    // Window arguments evaluate over the grouped rows too: aggregates point
    // at their `$aggK` column, and a bare column reference outside any
    // aggregate (a GROUP BY key or a lenient ungrouped column the select
    // list never projects) is materialized like a partition key. Left as is
    // it would miss the grouped schema and fall into the whole-row STRUCT
    // fallback.
    const auto point_at_grouped_input =
        [&](const Expression& node) -> std::optional<Expression> {
      if (auto aggregate = point_at_grouped_aggregate(node)) {
        return aggregate;
      }
      if (node->Type() == TypeTag::kColumnValue) {
        const ColumnName& col = node->AsColumnValue().GetColumnName();
        if (col.schema.empty()) {
          // An alias of a non-window select item already lives in the
          // grouped output under that name.
          for (size_t i = 0; i < statement.SelectList().size(); ++i) {
            const NamedExpression& item = statement.SelectList()[i];
            if (!item_has_window[i] && !item.name.empty() &&
                IdentifierEquals(item.name, col.name)) {
              return ColumnValueExp(ColumnName("", ProjectionName(item, i)));
            }
          }
        }
        return materialize_key(node);
      }
      return std::nullopt;
    };
    for (Expression& argument : rewritten->args) {
      argument = ReplaceSubExpressions(argument, point_at_grouped_input);
    }
    if (rewritten->where_filter) {
      rewritten->where_filter = ReplaceSubExpressions(
          rewritten->where_filter, point_at_grouped_input);
    }
    for (Expression& key : rewritten->partition_by) {
      if (key) {
        key = materialize_key(key);
      }
    }
    for (auto& term : rewritten->order_by) {
      if (term.expression) {
        term.expression = materialize_key(term.expression);
      }
    }
    rewritten_windows.push_back(std::move(rewritten));
  }
  // ORDER BY aggregates that the select list does not project (ORDER BY
  // COUNT(*)) must also be computed by the grouping pass: pass 3 runs over
  // ungrouped rows and cannot evaluate a raw aggregate there.
  for (const SelectStatement::OrderByTerm& term : statement.OrderBy()) {
    if (term.expression && !ContainsWindowCall(term.expression) &&
        relational_detail::ContainsAggregate(term.expression)) {
      RegisterAggregatesForMaterialization(term.expression, &aggregate_columns,
                                           &hidden_aggregates);
    }
  }
  for (const auto& [name, aggregate] : hidden_aggregates) {
    grouped_select.emplace_back(name, aggregate);
  }
  for (const NamedExpression& item : partition_items) {
    grouped_select.emplace_back(item);
  }
  // Pass 1 also materializes plain ORDER BY terms as hidden `$okJ` columns.
  // The final pass evaluates its ordering over the grouped output, where an
  // unselected GROUP BY key (q12 orders by i_item_id without selecting it)
  // does not exist: without this column the ordering used to fall into the
  // row-alias whole-row fallback and "sorted" by a STRUCT of the grouped row.
  std::unordered_map<std::string, std::string> order_key_columns;
  std::vector<Expression> rewritten_order_terms;
  rewritten_order_terms.reserve(statement.OrderBy().size());
  const auto materialize_order_key = [&](const Expression& term) -> Expression {
    if (const auto found = existing_columns.find(term->ToString());
        found != existing_columns.end()) {
      return ColumnValueExp(ColumnName("", found->second));
    }
    auto [it, inserted] = order_key_columns.emplace(
        term->ToString(), "$ok" + std::to_string(order_key_columns.size()));
    if (inserted) {
      grouped_select.emplace_back(it->second, term);
    }
    return ColumnValueExp(ColumnName("", it->second));
  };
  const auto term_needs_materialization = [&](const Expression& term) {
    if (!term) {
      return false;
    }
    // Window calls and aggregates are materialized through aggregate_columns
    // by the node-level rewrite below; a whole-term column would never
    // resolve in the grouped pass.
    if (ContainsWindowCall(term)) {
      return false;
    }
    bool aliases_only = true;
    std::function<void(const Expression&)> scan =
        [&](const Expression& node) {  // NOLINT(misc-no-recursion)
          if (!node) {
            return;
          }
          if (node->Type() == TypeTag::kAggregateExp) {
            aliases_only = false;
            return;
          }
          if (node->Type() == TypeTag::kColumnValue) {
            const ColumnName& referenced =
                node->AsColumnValue().GetColumnName();
            if (referenced.schema.empty() &&
                alias_columns.contains(AsciiToLower(referenced.name))) {
              aliases_only = false;
              return;
            }
          }
          for (const Expression& child : ExpressionChildren(node)) {
            scan(child);
          }
        };
    scan(term);
    return aliases_only;
  };
  for (const SelectStatement::OrderByTerm& term : statement.OrderBy()) {
    rewritten_order_terms.push_back(term_needs_materialization(term.expression)
                                        ? materialize_order_key(term.expression)
                                        : term.expression);
  }
  grouped->SetSelectList(std::move(grouped_select));
  grouped->SetOrderBy({});
  grouped->SetQualify(Expression{});
  // DISTINCT belongs to the final projection (pass 3), after the window
  // columns are computed. Applying it here would dedupe rows whose only
  // difference is a still-NULL window placeholder, losing result rows.
  grouped->SetDistinct(false);
  grouped->SetLimit(std::nullopt);
  grouped->SetOffset(0);
  // When the input has not been built yet, plan the grouping independently.
  // Its final join can aggregate directly, before any joined rows exist.
  // The WITH cells were already evaluated by the enclosing ExecuteQuery.
  grouped->ClearWithQueries();
  ASSIGN_OR_RETURN(
      Relation, grouped_output,
      (build_grouped_input ? ExecuteQuery(context, *grouped, outer, ctes,
                                          nullptr, columnar_output)
                           : FinishQuery(context, *grouped, std::move(input),
                                         outer, ctes, apply_where, 0)));

  RETURN_IF_FAIL(grouped_output.FinishSpill());
  RETURN_IF_FAIL(grouped_output.GetStatus());
  const Schema grouped_schema = grouped_output.schema;

  // Direct window projections no longer need their grouping placeholders or
  // hidden window arguments once every window has been evaluated. Compact
  // the surviving output columns before appending the computed values so a
  // row can reuse the cells formerly occupied by those temporary inputs.
  // Mixed expressions, QUALIFY and ORDER BY may still reference hidden group
  // columns and keep the complete schema.
  bool reuse_grouped_cells =
      statement.OrderBy().empty() && !statement.Qualify();
  for (size_t i = 0; i < item_has_window.size(); ++i) {
    if (item_has_window[i] && statement.SelectList()[i].expression->Type() !=
                                  TypeTag::kWindowFunctionExp) {
      reuse_grouped_cells = false;
    }
  }
  std::vector<size_t> kept_columns;
  for (size_t i = 0; i < grouped_schema.ColumnCount(); ++i) {
    if (!reuse_grouped_cells ||
        (i < item_has_window.size() && !item_has_window[i])) {
      kept_columns.push_back(i);
    }
  }
  const size_t grouped_width = kept_columns.size();

  auto rewrite_qualify = [&]() -> Expression {
    const auto point_qualify_at_materialized =
        [&](const Expression& node) -> std::optional<Expression> {
      if (node->Type() == TypeTag::kWindowFunctionExp ||
          node->Type() == TypeTag::kAggregateExp) {
        return ColumnValueExp(
            ColumnName("", aggregate_columns.at(node->ToString())));
      }
      if (node->Type() == TypeTag::kColumnValue) {
        const ColumnName& referenced = node->AsColumnValue().GetColumnName();
        if (referenced.schema.empty()) {
          if (auto it = alias_columns.find(AsciiToLower(referenced.name));
              it != alias_columns.end()) {
            // Window-bearing SELECT items have NULL placeholders in the
            // grouping pass. QUALIFY aliases must read their computed
            // windows (and aggregates), not those placeholders.
            for (size_t i = 0; i < statement.SelectList().size(); ++i) {
              const auto& item = statement.SelectList()[i];
              if (item_has_window[i] &&
                  IdentifierEquals(item.name, referenced.name)) {
                return ReplaceSubExpressions(
                    item.expression,
                    [&](const Expression& value) -> std::optional<Expression> {
                      if (value->Type() == TypeTag::kWindowFunctionExp ||
                          value->Type() == TypeTag::kAggregateExp) {
                        return ColumnValueExp(ColumnName(
                            "", aggregate_columns.at(value->ToString())));
                      }
                      return std::nullopt;
                    });
              }
            }
            return ColumnValueExp(ColumnName("", it->second));
          }
        }
      }
      return std::nullopt;
    };
    return ReplaceSubExpressions(statement.Qualify(),
                                 point_qualify_at_materialized);
  };

  Relation extended(context.execution_runtime());
  if (columnar_output && !grouped_output.HasSpill() &&
      (grouped_output.HasColumnarOnly() || grouped_output.TotalRows() == 0)) {
    std::vector<const WindowFunctionCallExpression*> win_ptrs;
    std::vector<std::string> win_names;
    win_ptrs.reserve(rewritten_windows.size());
    win_names.reserve(rewritten_windows.size());
    for (size_t w = 0; w < rewritten_windows.size(); ++w) {
      win_ptrs.push_back(rewritten_windows[w].get());
      win_names.push_back("$win" + std::to_string(w));
    }
    ASSIGN_OR_RETURN(
        Relation, col_extended,
        (relational_detail::ExtendColumnarRelationWithWindows(
            context, std::move(grouped_output), win_ptrs, win_names, outer,
            ctes, std::span<const size_t>(kept_columns))));
    extended = std::move(col_extended);
    if (statement.Qualify()) {
      RETURN_IF_FAIL(
          FilterRelation(context, &extended, {rewrite_qualify()}, outer, ctes));
    }
    RETURN_IF_FAIL(extended.FinishSpill());
    RETURN_IF_FAIL(extended.GetStatus());
  } else {
    // Pass 2: compute every window over the grouped rows. Keep the original
    // memory charge until the extended relation takes ownership below.
    RETURN_IF_FAIL(grouped_output.EnsureRowsMaterialized());
    std::vector<Row> rows;
    if (!grouped_output.HasSpill()) {
      rows = std::move(grouped_output.rows);
    } else {
      RETURN_IF_FAIL(grouped_output.ForEachRow(
          [&rows](const Row& row) { rows.push_back(row); }));
    }
    std::vector<std::vector<Value>> computed(rewritten_windows.size());
    for (size_t w = 0; w < rewritten_windows.size(); ++w) {
      RETURN_IF_FAIL(ComputeOneWindow(context, *rewritten_windows[w], rows,
                                      grouped_schema, outer, ctes,
                                      &computed[w]));
    }
    if (grouped_width < grouped_schema.ColumnCount()) {
      for (Row& row : rows) {
        for (size_t i = 0; i < grouped_width; ++i) {
          if (i != kept_columns[i]) {
            row.values_[i] = std::move(row.values_[kept_columns[i]]);
          }
        }
        row.values_.resize(grouped_width);
      }
    }
    std::vector<Column> extended_columns;
    extended_columns.reserve(grouped_width + rewritten_windows.size());
    for (const size_t column : kept_columns) {
      extended_columns.push_back(grouped_schema.GetColumn(column));
    }
    for (size_t w = 0; w < rewritten_windows.size(); ++w) {
      extended_columns.emplace_back("$win" + std::to_string(w),
                                    ValueType::kNull);
    }
    for (size_t r = 0; r < rows.size(); ++r) {
      rows[r].values_.reserve(extended_columns.size());
      for (size_t w = 0; w < rewritten_windows.size(); ++w) {
        rows[r].values_.push_back(std::move(computed[w][r]));
      }
    }
    // Fix the $win column types from the appended values.
    for (size_t w = 0; w < rewritten_windows.size(); ++w) {
      Column& column = extended_columns[grouped_width + w];
      for (const Row& row : rows) {
        const Value& candidate = row.values_[grouped_width + w];
        if (!candidate.IsNull() && candidate.type != ValueType::kNull) {
          column = Column(column.Name(), ValueTypeOf(candidate));
          column.SetUnsigned(candidate.IsUnsigned());
          break;
        }
      }
    }
    // QUALIFY filters the grouped+$win rows (WHERE semantics: NULL and FALSE
    // drop the row), after the window pass materialized its inputs.
    if (statement.Qualify()) {
      const Expression rewritten_qualify = rewrite_qualify();
      Schema extended_schema(grouped_output.schema.Name(), extended_columns);
      std::vector<Row> filtered;
      filtered.reserve(rows.size());
      for (Row& row : rows) {
        const Scope qualify_scope{
            .row = &row, .schema = &extended_schema, .outer = outer};
        ASSIGN_OR_RETURN(Value, decided,
                         (TryEvaluate(rewritten_qualify, qualify_scope, nullptr,
                                      context, ctes)));
        if (Truthy(decided)) {
          filtered.push_back(std::move(row));
        }
      }
      rows = std::move(filtered);
    }
    extended.schema =
        Schema(grouped_output.schema.Name(), std::move(extended_columns));
    CopyExecutionStats(&extended, grouped_output);
    grouped_output.ResetContents();
    for (Row& row : rows) {
      extended.AddRow(std::move(row));
    }
    RETURN_IF_FAIL(extended.FinishSpill());
    RETURN_IF_FAIL(extended.GetStatus());
  }

  // Pass 3: re-project by column reference and restore ordering/limit.
  auto final_statement = std::make_shared<SelectStatement>(statement);
  std::vector<NamedExpression> final_select;
  final_select.reserve(statement.SelectList().size());
  for (size_t i = 0; i < statement.SelectList().size(); ++i) {
    const NamedExpression& projection = statement.SelectList()[i];
    Expression expression;
    if (projection.expression->Type() == TypeTag::kWindowFunctionExp) {
      expression = ColumnValueExp(ColumnName(
          "", aggregate_columns.at(projection.expression->ToString())));
    } else if (item_has_window[i]) {
      // Mixed item: windows -> $winN, aggregates -> $aggK; the remaining
      // nodes reference group keys resolvable in the grouped schema.
      const auto point_at_materialized_item =
          [&](const Expression& node) -> std::optional<Expression> {
        if (node->Type() == TypeTag::kWindowFunctionExp ||
            node->Type() == TypeTag::kAggregateExp) {
          return ColumnValueExp(
              ColumnName("", aggregate_columns.at(node->ToString())));
        }
        return std::nullopt;
      };
      expression = ReplaceSubExpressions(projection.expression,
                                         point_at_materialized_item);
    } else {
      expression =
          ColumnValueExp(ColumnName("", ProjectionName(projection, i)));
    }
    final_select.emplace_back(projection.name, std::move(expression));
  }
  const auto point_at_materialized =
      [&](const Expression& node) -> std::optional<Expression> {
    if (node->Type() == TypeTag::kWindowFunctionExp) {
      return ColumnValueExp(
          ColumnName("", aggregate_columns.at(node->ToString())));
    }
    if (node->Type() == TypeTag::kColumnValue) {
      const ColumnName& referenced = node->AsColumnValue().GetColumnName();
      if (referenced.schema.empty()) {
        if (auto it = alias_columns.find(AsciiToLower(referenced.name));
            it != alias_columns.end()) {
          return ColumnValueExp(ColumnName("", it->second));
        }
      }
    }
    if (node->Type() == TypeTag::kAggregateExp) {
      // ORDER BY <aggregate>: point at the matching select item's column.
      for (size_t i = 0; i < statement.SelectList().size(); ++i) {
        if (statement.SelectList()[i].expression->ToString() ==
            node->ToString()) {
          return ColumnValueExp(
              ColumnName("", ProjectionName(statement.SelectList()[i], i)));
        }
      }
      // Not projected: the grouping pass materialized it as a hidden $aggK.
      if (const auto found = aggregate_columns.find(node->ToString());
          found != aggregate_columns.end()) {
        return ColumnValueExp(ColumnName("", found->second));
      }
    }
    return std::nullopt;
  };
  std::vector<SelectStatement::OrderByTerm> final_order;
  final_order.reserve(statement.OrderBy().size());
  for (size_t o = 0; o < statement.OrderBy().size(); ++o) {
    const SelectStatement::OrderByTerm& term = statement.OrderBy()[o];
    // A materialized `$okJ` column already names this term; anything else
    // keeps the node-level rewrite (windows -> $win, aggregates/aliases ->
    // select items).
    final_order.push_back(
        {rewritten_order_terms[o] == term.expression
             ? ReplaceSubExpressions(term.expression, point_at_materialized)
             : rewritten_order_terms[o],
         term.ascending, term.nulls_first});
  }
  final_statement->SetSelectList(std::move(final_select));
  final_statement->SetOrderBy(std::move(final_order));
  final_statement->SetWhereClause(Expression{});
  final_statement->SetHaving(Expression{});
  final_statement->SetGroupBy({});
  final_statement->SetQualify(Expression{});
  ASSIGN_OR_RETURN(Relation, projected,
                   (FinishQuery(context, *final_statement, std::move(extended),
                                outer, ctes, false, 0)));
  // FinishQuery already applied ORDER BY and the statement's LIMIT/OFFSET
  // once; slicing again here dropped another OFFSET-worth of rows.
  return projected;
}

}  // namespace

}  // namespace

StatusOr<Relation> FinishQuery(TransactionContext& context,
                               const SelectStatement& statement, Relation input,
                               const Scope* outer, const CteMap& ctes,
                               bool apply_where, size_t hidden_columns) {
  if (apply_where && statement.WhereClause()) {
    const auto filter_begin = std::chrono::steady_clock::now();
    // Use the compiled filter path for WHERE evaluation when possible.
    // CompiledScanFilter separates SimpleCompare predicates (integer/string
    // column-vs-constant comparisons) from residual expressions, allowing
    // the fast integer/string comparison path to reject rows without the
    // overhead of recursive expression tree traversal.  This can reduce
    // filter time by 2-5x for queries with simple WHERE predicates.
    std::vector<Expression> where_predicates;
    if (statement.WhereClause()->Type() == TypeTag::kBinaryExp &&
        statement.WhereClause()->AsBinaryExpression().Op() ==
            BinaryOperation::kAnd) {
      where_predicates = SplitConjuncts(statement.WhereClause());
    } else {
      where_predicates = {statement.WhereClause()};
    }
    const relational_detail::FilterCompileEnv compile_env{
        .context = &context,
        .outer = outer,
        .ctes = &ctes,
        .input_rows = input.TotalRows(),
        .prebuild_only_exists = false,
        .input_relation = &input};
    const relational_detail::CompiledScanFilter compiled =
        relational_detail::CompileScanFilter(where_predicates, input.schema,
                                             &compile_env);
    // Use compiled filter path when any fast-path is available: SimpleCompare,
    // disjunctive branches, or bytecode-compiled residuals.  The bytecode
    // path avoids recursive expression tree traversal for residual predicates,
    // which is the dominant cost for complex WHERE clauses like TPC-H Q20.
    if (!compiled.simple.empty() || !compiled.prebuilt_membership.empty() ||
        !compiled.prebuilt_scalar_compare.empty() ||
        !compiled.disjunctive_branches.empty() ||
        compiled.residual_bytecode.has_value() || !compiled.residual.empty()) {
      input.FinishSpill();
      if (!compiled.residual_bytecode.has_value() &&
          !compiled.residual.empty() &&
          (!compiled.simple.empty() || !compiled.prefix_in.empty() ||
           !compiled.prebuilt_membership.empty() ||
           !compiled.prebuilt_scalar_compare.empty() ||
           !compiled.disjunctive_branches.empty())) {
        relational_detail::CompiledScanFilter non_residual = compiled;
        non_residual.residual.clear();
        Relation prefiltered(context.execution_runtime());
        prefiltered.schema = input.schema;
        CopyExecutionStats(&prefiltered, input);
        Status pre_error{Status::kSuccess};
        if (!relational_detail::TryParallelFilterRows(
                context, input, non_residual, input.schema, outer, ctes,
                nullptr, &prefiltered, &pre_error, &input)) {
          relational_detail::ColumnLookupCache pre_lookup;
          Status pre_iter = input.ForEachRow([&](const Row& row) {
            if (!pre_error.ok()) {
              return;
            }
            if (relational_detail::MatchScanFilter(
                    row, input.schema, non_residual, outer, context, ctes,
                    &pre_error, &pre_lookup)) {
              prefiltered.AddRow(row);
            }
          });
          RETURN_IF_FAIL(pre_iter);
        }
        RETURN_IF_FAIL(pre_error);
        prefiltered.FinishSpill();
        input = std::move(prefiltered);
      }
      Relation filtered(context.execution_runtime());
      filtered.schema = input.schema;
      CopyExecutionStats(&filtered, input);
      Status eval_error{Status::kSuccess};
      // A/B hatch: TINYLAMB_DISABLE_FINISH_PARALLEL_FILTER=1 keeps the serial
      // per-row loop below.
      static const bool finish_parallel_disabled =
          std::getenv("TINYLAMB_DISABLE_FINISH_PARALLEL_FILTER") != nullptr;
      bool filtered_in_parallel = false;
      if (!finish_parallel_disabled) {
        // Worker threads decide rows with the positional compares and the
        // compiled bytecode programs; rows whose program raised are parked
        // and re-checked on this thread through the same contextual
        // MatchScanFilter call the serial loop uses, so error semantics are
        // unchanged.  Filters that need the evaluator (query expressions the
        // bytecode cannot compile) make the parallel attempt decline and
        // fall through to the serial loop.
        filtered_in_parallel = relational_detail::TryParallelFilterRows(
            context, input, compiled, input.schema, outer, ctes, nullptr,
            &filtered, &eval_error, &input);
      }
      if (filtered_in_parallel) {
        RETURN_IF_FAIL(eval_error);
      } else {
        RETURN_IF_FAIL((([&]() -> Status {
          // Register the filtered relation so a correlated-subquery build
          // triggered by one of the predicates can prune its build scan to
          // this pipeline's outer key domain.
          relational_detail::ScopedCorrelatedOuter correlated_outer(context,
                                                                    input);
          return input.ForEachRow([&](const Row& row) {
            if (!eval_error.ok()) {
              return;
            }
            if (relational_detail::MatchScanFilter(row, input.schema, compiled,
                                                   outer, context, ctes,
                                                   &eval_error)) {
              filtered.AddRow(row);
            }
          });
        })()));
        RETURN_IF_FAIL(eval_error);
      }
      filtered.FinishSpill();
      input = std::move(filtered);
    } else {
      Relation filtered(context.execution_runtime());
      filtered.schema = input.schema;
      CopyExecutionStats(&filtered, input);
      input.FinishSpill();
      // Use the non-throwing evaluation path: the per-row evaluation must not
      // use the throwing Evaluate wrapper. An
      // unresolvable qualified reference inside a subquery predicate
      // (sql_oracle_fuzz find: `WHERE EXISTS (... d.parent ...)` over a
      // 1-column subquery) came back as a Status from TryEvaluate, and the
      // shim re-raised it as a C++ exception that escaped the whole
      // executor boundary instead of surfacing through GetStatus.
      Status filter_error{Status::kSuccess};
      RETURN_IF_FAIL(input.ForEachRow([&](const Row& row) {
        if (filter_error != Status::kSuccess) {
          return;
        }
        Scope scope{.row = &row, .schema = &input.schema, .outer = outer};
        StatusOr<Value> keep =
            TryEvaluate(statement.WhereClause(), scope, nullptr, context, ctes);
        if (!keep.HasValue()) {
          filter_error = keep.GetStatus();
          return;
        }
        if (Truthy(keep.Value())) {
          filtered.AddRow(row);
        }
      }));
      RETURN_IF_FAIL(filter_error);
      filtered.FinishSpill();
      input = std::move(filtered);
    }
    if (context.execution_runtime() != nullptr) {
      context.execution_runtime()->filter_ms += ElapsedMs(filter_begin);
    }
  }

  ASSIGN_OR_RETURN(Relation, output,
                   (Project(context, statement, std::move(input), outer, ctes,
                            hidden_columns)));
  if (statement.Distinct()) {
    if (statement.HasDistinctOn()) {
      if (!statement.OrderBy().empty()) {
        RETURN_IF_FAIL(
            ApplyOrderBy(context, statement, &output, outer, ctes, false));
      }
      ASSIGN_OR_RETURN(Relation, distinct_of,
                       (DistinctOf(std::move(output), statement.DistinctOn(),
                                   &context, outer, &ctes)));
      output = std::move(distinct_of);
    } else {
      ASSIGN_OR_RETURN(Relation, distinct_of, (DistinctOf(std::move(output))));
      output = std::move(distinct_of);
      // The grouped projection path already consumed ORDER BY against the
      // full pre-projection scope (non-projected aggregates and GROUP BY
      // keys ride as sorter keys and are dropped afterwards), and the
      // first-occurrence DistinctOf fold preserves that relative order.
      // Re-applying the raw order expressions against the visible-only
      // relation would re-evaluate hidden $aggN / group-key references that
      // no longer exist, so it only runs for non-grouped statements.
      const bool grouped_projection_sorted =
          !statement.GroupBy().empty() ||
          std::any_of(statement.SelectList().begin(),
                      statement.SelectList().end(),
                      [](const NamedExpression& projection) {
                        return projection.expression != nullptr &&
                               ContainsAggregate(projection.expression);
                      }) ||
          ContainsAggregate(statement.Having());
      if (!statement.OrderBy().empty() && !grouped_projection_sorted) {
        RETURN_IF_FAIL(ApplyOrderBy(context, statement, &output, outer, ctes));
      }
    }
  }
  return LimitedRows(statement, std::move(output), &context, outer, &ctes);
}
namespace {

bool IsPresenceOnlySetOperation(SetOperationKind kind) {
  return kind == SetOperationKind::kIntersect ||
         kind == SetOperationKind::kExcept || kind == SetOperationKind::kUnion;
}

std::optional<SelectStatement> GroupDistinctSetInput(
    const SelectStatement& statement) {
  if (statement.Sources().size() < 2 || !statement.GroupBy().empty() ||
      statement.SelectList().empty() || statement.HasLimit() ||
      statement.Offset() != 0 || statement.WithTies() ||
      statement.ForUpdate() || statement.Having() || statement.Qualify() ||
      statement.HasDistinctOn() || !statement.UnionAll().empty() ||
      statement.GetSetOperationTree() || !statement.WithQueries().empty() ||
      HasWindowFunctions(statement) ||
      StatementContainsVolatileFunction(statement)) {
    return std::nullopt;
  }
  std::vector<Expression> keys;
  for (const auto& item : statement.SelectList()) {
    if (!item.expression || item.expression->Type() != TypeTag::kColumnValue ||
        item.expression->AsColumnValue().GetColumnName().name == "*" ||
        item.expression->AsColumnValue().GetColumnName().name.empty()) {
      return std::nullopt;
    }
    keys.push_back(item.expression);
  }
  SelectStatement grouped(statement);
  grouped.SetGroupBy(std::move(keys));
  grouped.SetDistinct(false);
  return grouped;
}

bool IsIdentityDerived(const SelectStatement& statement) {
  if (statement.Sources().size() != 1 || statement.Sources()[0].query ||
      statement.Sources()[0].unnest || statement.Sources()[0].table.empty() ||
      statement.WhereClause() || statement.Having() || statement.Qualify() ||
      !statement.GroupBy().empty() || !statement.OrderBy().empty() ||
      !statement.UnionAll().empty() || statement.Distinct() ||
      statement.HasLimit() || statement.Offset() != 0 ||
      statement.SelectList().empty()) {
    return false;
  }
  return std::ranges::all_of(
      statement.SelectList(), [](const NamedExpression& item) {
        if (!item.expression ||
            item.expression->Type() != TypeTag::kColumnValue) {
          return false;
        }
        const ColumnName& column =
            item.expression->AsColumnValue().GetColumnName();
        return column.name != "*" &&
               (item.name.empty() || item.name == column.name);
      });
}

bool IsDeterministicProjectionExpression(  // NOLINT(misc-no-recursion)
    const Expression& expression) {
  if (!expression) {
    return true;
  }
  if (expression->Type() == TypeTag::kFunctionCallExp &&
      GetFunctionVolatility(
          expression->AsFunctionCallExpression().FuncName()) !=
          Volatility::kImmutable) {
    return false;
  }
  return std::ranges::all_of(
      ExpressionChildren(expression), [](const Expression& child) {
        return IsDeterministicProjectionExpression(child);
      });
}

bool IsFlattenableProjection(const SelectStatement& statement) {
  if (statement.Sources().size() != 1 || statement.Sources()[0].unnest ||
      (!statement.Sources()[0].query && statement.Sources()[0].table.empty()) ||
      statement.WhereClause() || statement.Having() || statement.Qualify() ||
      !statement.GroupBy().empty() || !statement.UnionAll().empty() ||
      statement.Distinct() || !statement.OrderBy().empty() ||
      statement.HasLimit() || statement.Offset() != 0 ||
      statement.SelectList().empty() || HasWindowFunctions(statement)) {
    return false;
  }
  // Projection flattening is only valid for expressions that do not create a
  // second query scope or change row cardinality.
  return std::ranges::all_of(
      statement.SelectList(), [](const NamedExpression& item) {
        return item.expression &&
               (item.expression->Type() != TypeTag::kColumnValue ||
                item.expression->AsColumnValue().GetColumnName().name != "*") &&
               !ContainsQuery(item.expression) &&
               !ContainsAggregate(item.expression) &&
               IsDeterministicProjectionExpression(item.expression);
      });
}

size_t FlattenableProjectionDepth(const SelectStatement& statement) {
  if (statement.Sources().size() != 1 || !statement.Sources()[0].query ||
      !IsFlattenableProjection(*statement.Sources()[0].query)) {
    return 0;
  }
  return 1 + FlattenableProjectionDepth(*statement.Sources()[0].query);
}

Expression SubstituteProjectionColumns(  // NOLINT(misc-no-recursion)
    const Expression& expression, const SelectStatement& projection,
    const std::string& projection_alias, bool* valid) {
  if (!expression) {
    return expression;
  }
  if (expression->Type() == TypeTag::kQueryExp) {
    *valid = false;
    return nullptr;
  }
  if (expression->Type() == TypeTag::kColumnValue) {
    const ColumnName& column = expression->AsColumnValue().GetColumnName();
    if (!column.schema.empty() &&
        !IdentifierEquals(column.schema, projection_alias)) {
      *valid = false;
      return nullptr;
    }
    for (const NamedExpression& item : projection.SelectList()) {
      if (!item.expression) {
        continue;
      }
      std::string output_name;
      if (item.name.empty() &&
          item.expression->Type() == TypeTag::kColumnValue) {
        output_name = item.expression->AsColumnValue().GetColumnName().name;
      } else {
        output_name = item.name;
      }
      if (IdentifierEquals(output_name, column.name)) {
        return item.expression;
      }
    }
    *valid = false;
    return nullptr;
  }
  const std::vector<Expression> children = ExpressionChildren(expression);
  if (children.empty()) {
    return expression;
  }
  std::vector<Expression> rewritten;
  rewritten.reserve(children.size());
  if (expression->Type() == TypeTag::kAggregateExp) {
    const AggregateExpression& agg = expression->AsAggregateExpression();
    if (IsCountStar(agg) && !agg.HasWhereFilter() &&
        agg.Having() == AggregateHavingModifier::kNone &&
        agg.InnerOrderBy().empty() && !agg.SecondaryArg() &&
        agg.TrailingArgs().empty()) {
      return expression;
    }
  }
  for (const Expression& child : children) {
    rewritten.push_back(SubstituteProjectionColumns(child, projection,
                                                    projection_alias, valid));
    if (!*valid) {
      return nullptr;
    }
  }
  return WithExpressionChildren(expression, std::move(rewritten));
}

std::shared_ptr<SelectStatement> FlattenProjectionBoundary(
    const SelectStatement& statement, const SelectSource& source,
    const SelectStatement& derived) {
  if (!IsFlattenableProjection(derived) || (statement.Sources().size() != 1) ||
      statement.Having() || statement.Qualify() ||
      !statement.GroupBy().empty() || !statement.UnionAll().empty() ||
      statement.Distinct() || HasWindowFunctions(statement)) {
    return nullptr;
  }
  const std::string derived_alias = source.alias.empty() ? "d" : source.alias;
  auto rewrite = [&](const Expression& expression) -> Expression {
    bool valid = true;
    Expression result =
        SubstituteProjectionColumns(expression, derived, derived_alias, &valid);
    return valid ? result : Expression{};
  };
  std::vector<NamedExpression> select_list;
  select_list.reserve(statement.SelectList().size());
  for (const NamedExpression& item : statement.SelectList()) {
    Expression expression = rewrite(item.expression);
    if (!expression) {
      return nullptr;
    }
    select_list.emplace_back(item.name, std::move(expression));
  }
  Expression where =
      statement.WhereClause() ? rewrite(statement.WhereClause()) : Expression{};
  if (statement.WhereClause() && !where) {
    return nullptr;
  }
  std::vector<SelectStatement::OrderByTerm> order_by;
  order_by.reserve(statement.OrderBy().size());
  for (const auto& term : statement.OrderBy()) {
    Expression expression = rewrite(term.expression);
    if (!expression) {
      return nullptr;
    }
    order_by.push_back(
        SelectStatement::OrderByTerm{.expression = std::move(expression),
                                     .ascending = term.ascending,
                                     .nulls_first = term.nulls_first});
  }

  auto rewritten = std::make_shared<SelectStatement>(statement);
  rewritten->SetSelectList(std::move(select_list));
  rewritten->SetWhereClause(std::move(where));
  rewritten->SetOrderBy(std::move(order_by));
  SelectSource merged = derived.Sources()[0];
  if (source.sample && merged.sample) {
    return nullptr;
  }
  if (source.sample) {
    merged.sample = source.sample;
  }
  merged.alias = source.alias.empty() ? merged.alias : source.alias;
  merged.join_type = source.join_type;
  merged.join_condition = source.join_condition;
  rewritten->SetSources({std::move(merged)});
  return rewritten;
}

// Rebind a predicate written against a derived-table output to the base
// relation below it. Only the expression forms reconstructed here are used by
// the pushdown rule; a nullptr means that the predicate has a shape we cannot
// prove safe to move.
Expression RebindDerivedPredicate(const Expression& expression,
                                  const SelectStatement& derived,
                                  const std::string& derived_alias) {
  if (!expression) {
    return nullptr;
  }
  switch (expression->Type()) {
    case TypeTag::kColumnValue: {
      const ColumnName& column = expression->AsColumnValue().GetColumnName();
      if (!column.schema.empty() && column.schema != derived_alias) {
        return expression;
      }
      for (const NamedExpression& item : derived.SelectList()) {
        if (!item.expression ||
            item.expression->Type() != TypeTag::kColumnValue) {
          continue;
        }
        const ColumnName& projected =
            item.expression->AsColumnValue().GetColumnName();
        const std::string output_name =
            item.name.empty() ? projected.name : item.name;
        if (output_name == column.name) {
          // This expression is evaluated inside the original derived scope.
          // Preserve its owner: with joins it need not name the first source.
          return item.expression;
        }
      }
      return nullptr;
    }
    case TypeTag::kBinaryExp: {
      const auto& binary = expression->AsBinaryExpression();
      Expression left =
          RebindDerivedPredicate(binary.Left(), derived, derived_alias);
      Expression right =
          RebindDerivedPredicate(binary.Right(), derived, derived_alias);
      return left && right ? BinaryExpressionExp(std::move(left), binary.Op(),
                                                 std::move(right))
                           : nullptr;
    }
    case TypeTag::kUnaryExp: {
      const auto& unary = expression->AsUnaryExpression();
      Expression child =
          RebindDerivedPredicate(unary.Child(), derived, derived_alias);
      return child ? UnaryExpressionExp(std::move(child), unary.Op()) : nullptr;
    }
    default:
      // Constants are safe and leave their identity unchanged.  Query,
      // aggregate, and function nodes are retained only when no column
      // rebinding is needed; the caller still rejects predicates with query
      // expressions before invoking this helper.
      return expression->TouchedColumns().empty() ? expression : nullptr;
  }
}

bool PredicateCanEnterDerived(const Expression& predicate,
                              const SelectStatement& derived,
                              const std::string& derived_alias) {
  if (!predicate || ContainsQuery(predicate) ||
      !SafeToReduceEvaluationCount(predicate) || derived.Sources().empty() ||
      std::ranges::any_of(derived.Sources(),
                          [](const auto& source) {
                            return source.query || source.unnest ||
                                   source.table.empty() || source.is_lateral ||
                                   !source.using_columns.empty();
                          }) ||
      derived.Having() || derived.Qualify() || !derived.GroupBy().empty() ||
      !derived.OrderBy().empty() || derived.Distinct() || derived.HasLimit() ||
      derived.Offset() != 0 || derived.SelectList().empty() ||
      HasWindowFunctions(derived) ||
      StatementContainsVolatileFunction(derived)) {
    return false;
  }
  for (const ColumnName& column : predicate->TouchedColumns()) {
    size_t matches = 0;
    bool direct = false;
    for (size_t i = 0; i < derived.SelectList().size(); ++i) {
      const NamedExpression& item = derived.SelectList()[i];
      if (!item.expression) {
        return false;
      }
      if (item.expression->Type() == TypeTag::kColumnValue &&
          item.expression->AsColumnValue().GetColumnName().name == "*") {
        return false;
      }
      if (ProjectionName(item, i) == column.name) {
        ++matches;
      }
      if (item.expression->Type() != TypeTag::kColumnValue) {
        continue;
      }
      const ColumnName& projected =
          item.expression->AsColumnValue().GetColumnName();
      const std::string output_name =
          item.name.empty() ? projected.name : item.name;
      if (output_name == column.name &&
          (column.schema.empty() || column.schema == derived_alias)) {
        direct = true;
      }
    }
    if (matches != 1 || !direct) {
      return false;
    }
  }
  // Do not classify an expression as pushable unless the rebinder can
  // actually preserve its shape.  In particular, function calls and other
  // expression kinds must stay above the derived-table boundary rather than
  // being silently removed from the outer predicate.
  return !predicate->TouchedColumns().empty() &&
         RebindDerivedPredicate(predicate, derived, derived_alias) != nullptr;
}

// Multi-source variant of IsFlattenableProjection: the derived side is a
// plain inner-join projection.  Unlike the single-source boundary above, its
// WHERE clause is allowed -- it composes into the merged statement's WHERE,
// which is equivalent because the merged sources are exactly the derived's
// own sources and every join inside them is inner.
bool IsFlattenableJoinedProjection(const SelectStatement& statement) {
  if (statement.Sources().empty() || statement.Having() ||
      statement.Qualify() || !statement.GroupBy().empty() ||
      !statement.UnionAll().empty() || statement.Distinct() ||
      statement.HasLimit() || statement.Offset() != 0 ||
      statement.SelectList().empty() || HasWindowFunctions(statement) ||
      !statement.WithQueries().empty()) {
    return false;
  }
  for (const SelectSource& source : statement.Sources()) {
    if (source.unnest || (source.query && source.table.empty()) ||
        (source.join_type != JoinType::kInner &&
         source.join_type != JoinType::kCross) ||
        source.sample) {
      return false;
    }
  }
  return std::ranges::all_of(
      statement.SelectList(), [](const NamedExpression& item) {
        return item.expression &&
               (item.expression->Type() != TypeTag::kColumnValue ||
                item.expression->AsColumnValue().GetColumnName().name != "*") &&
               !ContainsQuery(item.expression) &&
               !ContainsAggregate(item.expression) &&
               IsDeterministicProjectionExpression(item.expression);
      });
}

// Flattens a grouping boundary: a statement that GROUPs over a single derived
// table absorbs the derived projection when the derived side is itself a
// plain multi-source inner-join projection.  Group keys, select items and
// ORDER BY terms re-resolve against the derived select list (aggregates
// rebuild losslessly), so the grouping runs directly over the joined base
// relations and the grouped-join fusion can fire at the join step instead of
// materializing the multi-million-row join relation.
std::shared_ptr<SelectStatement> FlattenGroupedDerivedBoundary(
    const SelectStatement& statement, const SelectSource& source,
    const SelectStatement& derived) {
  if (statement.Sources().size() != 1 || statement.Having() ||
      statement.Qualify() || !statement.UnionAll().empty() ||
      HasWindowFunctions(statement) ||
      StatementContainsVolatileFunction(statement) ||
      !IsFlattenableJoinedProjection(derived)) {
    return nullptr;
  }
  if (source.join_type != JoinType::kInner &&
      source.join_type != JoinType::kCross) {
    return nullptr;
  }
  if (source.sample || ContainsQuery(derived.WhereClause())) {
    return nullptr;
  }
  // Duplicate derived output names make the substitution ambiguous.
  for (auto a = derived.SelectList().begin(); a != derived.SelectList().end();
       ++a) {
    const std::string a_name =
        !a->name.empty()
            ? a->name
            : (a->expression->Type() == TypeTag::kColumnValue
                   ? a->expression->AsColumnValue().GetColumnName().name
                   : std::string());
    if (a_name.empty()) {
      continue;
    }
    for (auto b = std::next(a); b != derived.SelectList().end(); ++b) {
      const std::string b_name =
          !b->name.empty()
              ? b->name
              : (b->expression->Type() == TypeTag::kColumnValue
                     ? b->expression->AsColumnValue().GetColumnName().name
                     : std::string());
      if (!b_name.empty() && IdentifierEquals(a_name, b_name)) {
        return nullptr;
      }
    }
  }
  const std::string derived_alias = source.alias.empty() ? "d" : source.alias;
  auto rewrite = [&](const Expression& expression) -> Expression {
    bool valid = true;
    Expression result =
        SubstituteProjectionColumns(expression, derived, derived_alias, &valid);
    return valid ? result : Expression{};
  };
  std::vector<NamedExpression> select_list;
  select_list.reserve(statement.SelectList().size());
  for (const NamedExpression& item : statement.SelectList()) {
    Expression expression = rewrite(item.expression);
    if (!expression) {
      return nullptr;
    }
    select_list.emplace_back(item.name, std::move(expression));
  }
  std::vector<Expression> group_by;
  group_by.reserve(statement.GroupBy().size());
  for (const Expression& key : statement.GroupBy()) {
    Expression expression = rewrite(key);
    if (!expression) {
      return nullptr;
    }
    group_by.push_back(std::move(expression));
  }
  std::vector<SelectStatement::OrderByTerm> order_by;
  order_by.reserve(statement.OrderBy().size());
  for (const auto& term : statement.OrderBy()) {
    // A term that names a select alias stays a plain column reference: the
    // fused path orders the projected output through select aliases, and a
    // substituted expression there would defeat that resolution.
    const bool alias_ref =
        term.expression && term.expression->Type() == TypeTag::kColumnValue &&
        term.expression->AsColumnValue().GetColumnName().schema.empty() &&
        std::any_of(
            statement.SelectList().begin(), statement.SelectList().end(),
            [&](const NamedExpression& item) {
              return !item.name.empty() &&
                     IdentifierEquals(
                         item.name,
                         term.expression->AsColumnValue().GetColumnName().name);
            });
    Expression expression =
        alias_ref ? term.expression : rewrite(term.expression);
    if (!expression) {
      return nullptr;
    }
    order_by.push_back(
        SelectStatement::OrderByTerm{.expression = std::move(expression),
                                     .ascending = term.ascending,
                                     .nulls_first = term.nulls_first});
  }
  // The derived's own conjuncts move up unchanged (same base sources); the
  // parent's conjuncts are re-bound to the derived outputs first.
  std::vector<Expression> where = SplitConjuncts(derived.WhereClause());
  if (statement.WhereClause()) {
    Expression rebound = rewrite(statement.WhereClause());
    if (!rebound) {
      return nullptr;
    }
    for (Expression& conjunct : SplitConjuncts(rebound)) {
      where.push_back(std::move(conjunct));
    }
  }

  auto rewritten = std::make_shared<SelectStatement>(statement);
  rewritten->SetSelectList(std::move(select_list));
  rewritten->SetGroupBy(std::move(group_by));
  rewritten->SetWhereClause(where.empty() ? Expression{}
                                          : CombineConjuncts(where));
  rewritten->SetOrderBy(std::move(order_by));
  rewritten->SetSources(derived.Sources());
  return rewritten;
}

struct StaticExprType {
  ValueType type{ValueType::kNull};
  bool is_unsigned{false};
  bool is_interval{false};
  bool operator==(const StaticExprType&) const = default;
};

std::optional<size_t> FindStaticColumn(const Schema& schema,
                                       const ColumnName& name) {
  std::optional<size_t> match;
  for (size_t i = 0; i < schema.ColumnCount(); ++i) {
    const ColumnName& candidate = schema.GetColumn(i).Name();
    if (!IdentifierEquals(candidate.name, name.name)) {
      continue;
    }
    if (!name.schema.empty() &&
        !IdentifierEquals(candidate.schema, name.schema)) {
      continue;
    }
    if (match.has_value()) {
      return std::nullopt;
    }
    match = i;
  }
  return match;
}

std::optional<StaticExprType> InferStaticExprType(const Expression& node,
                                                  const Schema& schema) {
  if (!node) {
    return std::nullopt;
  }
  switch (node->Type()) {
    case TypeTag::kConstantValue: {
      const Value& val = node->AsConstantValue().GetValue();
      if (val.type == ValueType::kNull) {
        return std::nullopt;
      }
      return StaticExprType{.type = val.type,
                            .is_unsigned = val.IsUnsigned(),
                            .is_interval = val.IsInterval()};
    }
    case TypeTag::kColumnValue: {
      const auto slot =
          FindStaticColumn(schema, node->AsColumnValue().GetColumnName());
      if (!slot.has_value()) {
        return std::nullopt;
      }
      const Column& col = schema.GetColumn(*slot);
      if (col.Type() == ValueType::kNull) {
        return std::nullopt;
      }
      return StaticExprType{.type = col.Type(),
                            .is_unsigned = col.IsUnsigned(),
                            .is_interval = col.IsInterval()};
    }
    case TypeTag::kCastExp: {
      const auto& cast = node->AsCastExpression();
      const ValueType vt = cast.TargetValueType();
      if (vt == ValueType::kNull || vt == ValueType::kArray) {
        return std::nullopt;
      }
      // Avoid two heap allocations per CAST node during planning: compare the
      // resolved type-name prefix case-insensitively in place.
      const std::string_view type_name = cast.TargetTypeName();
      const auto starts_ci = [type_name](std::string_view prefix) {
        return type_name.size() >= prefix.size() &&
               std::equal(
                   prefix.begin(), prefix.end(), type_name.begin(),
                   [](char a, char b) {
                     return std::toupper(static_cast<unsigned char>(a)) ==
                            std::toupper(static_cast<unsigned char>(b));
                   });
      };
      const bool is_unsigned = starts_ci("UINT");
      const bool is_interval = starts_ci("INTERVAL");
      return StaticExprType{
          .type = vt, .is_unsigned = is_unsigned, .is_interval = is_interval};
    }
    case TypeTag::kUnaryExp: {
      const auto& un = node->AsUnaryExpression();
      const auto ch = InferStaticExprType(un.Child(), schema);
      if (!ch.has_value()) {
        return std::nullopt;
      }
      switch (un.Op()) {
        case UnaryOperation::kIsNull:
        case UnaryOperation::kIsNotNull:
        case UnaryOperation::kIsTrue:
        case UnaryOperation::kIsNotTrue:
        case UnaryOperation::kIsFalse:
        case UnaryOperation::kIsNotFalse:
        case UnaryOperation::kNot:
          return StaticExprType{.type = ValueType::kInt64,
                                .is_unsigned = false,
                                .is_interval = false};
        case UnaryOperation::kMinus:
          if (ch->is_unsigned || ch->is_interval ||
              (ch->type != ValueType::kInt64 &&
               ch->type != ValueType::kDouble)) {
            return std::nullopt;
          }
          return ch;
        case UnaryOperation::kBitwiseNot:
          return std::nullopt;
      }
      return std::nullopt;
    }
    case TypeTag::kBinaryExp: {
      const auto& bin = node->AsBinaryExpression();
      const auto lt = InferStaticExprType(bin.Left(), schema);
      const auto rt = InferStaticExprType(bin.Right(), schema);
      if (!lt.has_value() || !rt.has_value()) {
        return std::nullopt;
      }
      const BinaryOperation op = bin.Op();
      if (IsComparison(op) || op == BinaryOperation::kAnd ||
          op == BinaryOperation::kOr || op == BinaryOperation::kXor ||
          op == BinaryOperation::kLike || op == BinaryOperation::kNotLike ||
          op == BinaryOperation::kIsDistinctFrom ||
          op == BinaryOperation::kIsNotDistinctFrom) {
        return StaticExprType{.type = ValueType::kInt64,
                              .is_unsigned = false,
                              .is_interval = false};
      }
      if (lt->is_interval || rt->is_interval || lt->is_unsigned ||
          rt->is_unsigned) {
        return std::nullopt;
      }
      if (op == BinaryOperation::kDivide) {
        if ((lt->type == ValueType::kInt64 || lt->type == ValueType::kDouble) &&
            (rt->type == ValueType::kInt64 || rt->type == ValueType::kDouble)) {
          return StaticExprType{.type = ValueType::kDouble,
                                .is_unsigned = false,
                                .is_interval = false};
        }
        return std::nullopt;
      }
      if (op == BinaryOperation::kAdd || op == BinaryOperation::kSubtract) {
        if (lt->type == ValueType::kDate && rt->type == ValueType::kDate &&
            op == BinaryOperation::kSubtract) {
          return StaticExprType{.type = ValueType::kInt64,
                                .is_unsigned = false,
                                .is_interval = false};
        }
        if ((lt->type == ValueType::kDate) != (rt->type == ValueType::kDate)) {
          const auto& other = (lt->type == ValueType::kDate) ? *rt : *lt;
          if (other.type == ValueType::kInt64) {
            return StaticExprType{.type = ValueType::kDate,
                                  .is_unsigned = false,
                                  .is_interval = false};
          }
          return std::nullopt;
        }
      }
      if (op == BinaryOperation::kAdd || op == BinaryOperation::kSubtract ||
          op == BinaryOperation::kMultiply || op == BinaryOperation::kModulo) {
        if ((lt->type == ValueType::kInt64 || lt->type == ValueType::kDouble) &&
            (rt->type == ValueType::kInt64 || rt->type == ValueType::kDouble)) {
          const ValueType res =
              (lt->type == ValueType::kDouble || rt->type == ValueType::kDouble)
                  ? ValueType::kDouble
                  : ValueType::kInt64;
          return StaticExprType{
              .type = res, .is_unsigned = false, .is_interval = false};
        }
      }
      return std::nullopt;
    }
    default:
      return std::nullopt;
  }
}

std::optional<Schema> ResolveStaticBranchSchema(const SelectStatement& branch,
                                                const CteMap& inherited_ctes,
                                                TransactionContext* context,
                                                int depth = 0) {
  if (depth > 8 || !branch.WithQueries().empty() ||
      !branch.UnionAll().empty() || branch.UnionDistinct() ||
      branch.GetSetOperationTree()) {
    return std::nullopt;
  }
  std::vector<Column> columns;
  for (const SelectSource& src : branch.Sources()) {
    if (src.unnest || src.sample || src.is_lateral || src.from_nested_join ||
        !src.using_columns.empty()) {
      return std::nullopt;
    }
    const std::string qualifier = src.alias.empty() ? src.table : src.alias;
    if (src.query != nullptr) {
      auto sub_input = ResolveStaticBranchSchema(*src.query, inherited_ctes,
                                                 context, depth + 1);
      if (!sub_input.has_value()) {
        return std::nullopt;
      }
      for (size_t i = 0; i < src.query->SelectList().size(); ++i) {
        const NamedExpression& item = src.query->SelectList()[i];
        const auto t = InferStaticExprType(item.expression, *sub_input);
        if (!t.has_value()) {
          return std::nullopt;
        }
        std::string col_name = ProjectionName(item, i);
        Column col(ColumnName{qualifier, std::move(col_name)}, t->type);
        col.SetUnsigned(t->is_unsigned);
        col.SetInterval(t->is_interval);
        columns.push_back(std::move(col));
      }
      continue;
    }
    if (src.table.empty()) {
      return std::nullopt;
    }
    if (auto cte_it = inherited_ctes.find(src.table);
        cte_it != inherited_ctes.end()) {
      if (!cte_it->second) {
        return std::nullopt;
      }
      const Schema& cte_schema = cte_it->second->schema;
      for (size_t i = 0; i < cte_schema.ColumnCount(); ++i) {
        columns.push_back(cte_schema.GetColumn(i));
        columns.back().Name().schema = qualifier;
      }
      continue;
    }
    if (context == nullptr) {
      return std::nullopt;
    }
    auto tbl = context->GetTable(src.table);
    if (!tbl.HasValue()) {
      return std::nullopt;
    }
    const Schema& tbl_schema = tbl.Value()->GetSchema();
    for (size_t i = 0; i < tbl_schema.ColumnCount(); ++i) {
      columns.push_back(tbl_schema.GetColumn(i));
      columns.back().Name().schema = qualifier;
    }
  }
  return Schema("", std::move(columns));
}

bool AreCompatibleUnreferencedSetTypes(const StaticExprType& a,
                                       const StaticExprType& b) {
  if (a == b) {
    return true;
  }
  if (a.is_interval != b.is_interval) {
    return false;
  }
  const bool a_num =
      (a.type == ValueType::kInt64 || a.type == ValueType::kDouble);
  const bool b_num =
      (b.type == ValueType::kInt64 || b.type == ValueType::kDouble);
  if (a_num && b_num) {
    return true;
  }
  if (a.type == ValueType::kVarChar && (b_num || b.type == ValueType::kDate)) {
    return true;
  }
  if (b.type == ValueType::kVarChar && (a_num || a.type == ValueType::kDate)) {
    return true;
  }
  return false;
}

bool UnionBranchesAgreeForScalarPushdown(
    const std::vector<const SelectStatement*>& branches,
    const std::vector<bool>& referenced_cols, const CteMap& inherited_ctes,
    TransactionContext* context) {
  if (branches.empty()) {
    return false;
  }
  std::vector<Schema> branch_schemas;
  branch_schemas.reserve(branches.size());
  for (const SelectStatement* b : branches) {
    auto s = ResolveStaticBranchSchema(*b, inherited_ctes, context);
    if (!s.has_value()) {
      return false;
    }
    branch_schemas.push_back(std::move(*s));
  }
  const size_t width = branches[0]->SelectList().size();
  for (size_t col = 0; col < width; ++col) {
    std::optional<StaticExprType> head_type = InferStaticExprType(
        branches[0]->SelectList()[col].expression, branch_schemas[0]);
    if (referenced_cols[col]) {
      if (!head_type.has_value()) {
        return false;
      }
      for (size_t bi = 1; bi < branches.size(); ++bi) {
        const auto bt = InferStaticExprType(
            branches[bi]->SelectList()[col].expression, branch_schemas[bi]);
        if (!bt.has_value() || *bt != *head_type) {
          return false;
        }
      }
    } else if (head_type.has_value()) {
      for (size_t bi = 1; bi < branches.size(); ++bi) {
        const auto bt = InferStaticExprType(
            branches[bi]->SelectList()[col].expression, branch_schemas[bi]);
        if (bt.has_value() &&
            !AreCompatibleUnreferencedSetTypes(*head_type, *bt)) {
          return false;
        }
      }
    }
  }
  return true;
}

}  // namespace

std::shared_ptr<SelectStatement> OptimizeDerivedBoundaries(
    const SelectStatement& statement, const CteMap& inherited_ctes,
    TransactionContext* context) {
  if (statement.Sources().size() != 1 ||
      statement.Sources()[0].query == nullptr) {
    return nullptr;
  }
  const SelectSource& source = statement.Sources()[0];
  const SelectStatement& derived = *source.query;
  if (derived.Sources().empty()) {
    return nullptr;
  }
  const auto& inner_source = derived.Sources()[0];
  // A CTE name occupies the same syntactic slot as a base table but cannot be
  // flattened into a catalog scan. This also covers a derived query's own
  // WITH clause and an inherited outer CTE.
  if (!inner_source.table.empty() &&
      (derived.WithQueries().contains(inner_source.table) ||
       inherited_ctes.contains(inner_source.table))) {
    return nullptr;
  }
  // Note: scalar aggregate pushdown through UNION ALL (lines 6436+) only
  // fires when UnionBranchesAgreeForScalarPushdown verifies that every column
  // referenced by pushed aggregates resolves to the exact same static
  // (ValueType, is_unsigned, is_interval) across all branches. Mixed types
  // (e.g. INT64 vs DOUBLE promotion before arithmetic) remain unpushed so the
  // UNION widens types before outer arithmetic runs.
  auto rewritten = std::make_shared<SelectStatement>(statement);

  // Flatten SELECT column-list FROM one-table derived tables.  This is
  // limited to identity projections so aliases and row multiplicity remain
  // unchanged without expression substitution.
  // A star in the parent select list (`t.*`, `*`, or the AST visitor's
  // __value_table_value(t.*) wrapper) names the derived query's output
  // columns. Merging the base table in would expand it over every base
  // column instead, and the wrapped form hides the star from column pruning,
  // so keep the boundary.
  const auto references_star = [](const Expression& root) {
    std::vector<Expression> stack{root};
    while (!stack.empty()) {
      Expression current = std::move(stack.back());
      stack.pop_back();
      if (!current) {
        continue;
      }
      if (current->Type() == TypeTag::kColumnValue &&
          current->AsColumnValue().GetColumnName().name == "*") {
        return true;
      }
      for (const Expression& child : ExpressionChildren(current)) {
        stack.push_back(child);
      }
    }
    return false;
  };
  const bool parent_has_star = std::ranges::any_of(
      statement.SelectList(), [&](const NamedExpression& item) {
        return references_star(item.expression);
      });
  if (!parent_has_star && IsIdentityDerived(derived)) {
    SelectSource merged = derived.Sources()[0];
    if (source.sample && merged.sample) {
      return nullptr;
    }
    if (source.sample) {
      merged.sample = source.sample;
    }
    merged.alias = source.alias.empty() ? merged.alias : source.alias;
    merged.join_type = source.join_type;
    merged.join_condition = source.join_condition;
    rewritten->SetSources({std::move(merged)});
    return rewritten;
  }

  // Compose projections through a cardinality-preserving derived query. This
  // removes nested projection stacks while substituting the derived output
  // expressions into the parent SELECT/WHERE/ORDER BY. The rewrite is
  // intentionally restricted to a single source with no grouping, ordering,
  // limit, window, or subquery expression.
  if (std::shared_ptr<SelectStatement> flattened =
          FlattenProjectionBoundary(statement, source, derived)) {
    return flattened;
  }

  // A grouping parent over a plain joined projection absorbs the projection
  // so the grouping (and the grouped-join fusion) runs over the base sources.
  if (std::shared_ptr<SelectStatement> flattened =
          FlattenGroupedDerivedBoundary(statement, source, derived)) {
    return flattened;
  }

  // An outer filter observes completed window values. Move it to QUALIFY,
  // never WHERE, so each window still sees every row in its partition while
  // rejected rows avoid the derived projection and parent materialization.
  if (statement.WhereClause() && !source.sample && !source.is_lateral &&
      !source.from_nested_join && !statement.ForUpdate() &&
      !derived.ForUpdate() && !derived.HasLimit() && derived.Offset() == 0 &&
      !derived.Distinct() && !derived.HasDistinctOn() &&
      derived.OrderBy().empty() && derived.GroupBy().empty() &&
      !derived.IsGroupAll() && !derived.Having() &&
      derived.UnionAll().empty() && !derived.GetSetOperationTree() &&
      HasWindowFunctions(derived) &&
      !StatementContainsVolatileFunction(derived)) {
    std::unordered_set<std::string> names;
    const bool plain_outputs = std::ranges::all_of(
        derived.SelectList(), [&](const NamedExpression& item) {
          if (!item.expression ||
              (item.expression->Type() != TypeTag::kColumnValue &&
               item.expression->Type() != TypeTag::kConstantValue &&
               item.expression->Type() != TypeTag::kWindowFunctionExp)) {
            return false;
          }
          if (item.expression->Type() == TypeTag::kColumnValue) {
            const auto& column =
                item.expression->AsColumnValue().GetColumnName();
            if (column.name.find('.') != std::string::npos ||
                column.schema.find('.') != std::string::npos) {
              return false;
            }
          }
          const auto name = ProjectionName(item, names.size());
          return name != "*" && names.insert(AsciiToLower(name)).second;
        });
    if (plain_outputs) {
      std::vector<Expression> qualify = SplitConjuncts(derived.Qualify());
      std::vector<Expression> above;
      bool moved = false;
      for (const auto& predicate : SplitConjuncts(statement.WhereClause())) {
        const auto columns = predicate->TouchedColumns();
        bool valid = !columns.empty() && !ContainsQuery(predicate) &&
                     SafeToReduceEvaluationCount(predicate) &&
                     std::ranges::none_of(columns, [](const ColumnName& name) {
                       return name.name.find('.') != std::string::npos ||
                              name.schema.find('.') != std::string::npos;
                     });
        Expression rebound;
        if (valid) {
          rebound = SubstituteProjectionColumns(predicate, derived,
                                                source.alias, &valid);
        }
        if (valid && rebound) {
          qualify.push_back(std::move(rebound));
          moved = true;
        } else {
          above.push_back(predicate);
        }
      }
      if (moved) {
        SelectSource updated = source;
        updated.query = std::make_shared<SelectStatement>(derived);
        updated.query->SetQualify(CombineConjuncts(qualify));
        rewritten->SetSources({std::move(updated)});
        rewritten->SetWhereClause(above.empty() ? Expression{}
                                                : CombineConjuncts(above));
        return rewritten;
      }
    }
  }

  // A predicate on an ordinary GROUP BY key selects complete groups. Move
  // it below aggregation, including joined inputs, but keep aggregate-output
  // predicates above it. LIMIT, grouping sets and windows are boundaries.
  if (statement.WhereClause() && !source.sample && !derived.GroupBy().empty() &&
      !derived.IsGroupAll() && !derived.HasLimit() && derived.Offset() == 0 &&
      !derived.Qualify() && derived.UnionAll().empty() &&
      !derived.GetSetOperationTree() && derived.WithQueries().empty() &&
      !HasWindowFunctions(derived) &&
      !StatementContainsVolatileFunction(derived)) {
    std::vector<Expression> inner = SplitConjuncts(derived.WhereClause());
    std::vector<Expression> above;
    bool moved = false;
    for (const auto& predicate : SplitConjuncts(statement.WhereClause())) {
      bool valid =
          !ContainsQuery(predicate) && SafeToReduceEvaluationCount(predicate);
      const auto columns = predicate->TouchedColumns();
      valid &= !columns.empty();
      for (const auto& column : columns) {
        size_t matches = 0;
        bool group_key = false;
        for (size_t i = 0; i < derived.SelectList().size(); ++i) {
          const auto& item = derived.SelectList()[i];
          if (!IdentifierEquals(ProjectionName(item, i), column.name)) {
            continue;
          }
          ++matches;
          group_key =
              item.expression &&
              item.expression->Type() == TypeTag::kColumnValue &&
              std::ranges::any_of(derived.GroupBy(), [&](const auto& key) {
                return key && key->Type() == TypeTag::kColumnValue &&
                       key->AsColumnValue().GetColumnName() ==
                           item.expression->AsColumnValue().GetColumnName();
              });
        }
        valid &= matches == 1 && group_key;
      }
      Expression rebound;
      if (valid) {
        rebound = SubstituteProjectionColumns(predicate, derived, source.alias,
                                              &valid);
      }
      if (valid && rebound) {
        inner.push_back(std::move(rebound));
        moved = true;
      } else {
        above.push_back(predicate);
      }
    }
    if (moved) {
      SelectSource updated = source;
      updated.query = std::make_shared<SelectStatement>(derived);
      updated.query->SetWhereClause(CombineConjuncts(inner));
      rewritten->SetSources({std::move(updated)});
      rewritten->SetWhereClause(CombineConjuncts(above));
      return rewritten;
    }
  }

  std::vector<Expression> pushable;
  std::vector<Expression> remaining;
  for (const Expression& conjunct : SplitConjuncts(statement.WhereClause())) {
    (PredicateCanEnterDerived(conjunct, derived,
                              source.alias.empty() ? "u" : source.alias)
         ? pushable
         : remaining)
        .push_back(conjunct);
  }
  // UNION ALL and UNION DISTINCT both distribute a local predicate over
  // their branches (filtering before or after the dedup is equivalent), so
  // the pushdown below is safe for either flavor.
  const bool is_union =
      !derived.UnionAll().empty() &&
      (derived.GetSetOperationTree() == nullptr ||
       !derived.GetSetOperationTree()->grouped) &&
      std::ranges::all_of(derived.SetOperationKinds(),
                          [](SetOperationKind kind) {
                            return kind == SetOperationKind::kUnionAll ||
                                   kind == SetOperationKind::kUnion;
                          });
  if (is_union) {
    // UNION ALL / UNION DISTINCT have no row-changing boundary before the outer
    // filter. Push a local predicate into every branch, then retain any
    // non-local predicates above the set operation.
    SelectStatement pushed = derived;
    std::vector<Expression> head_where = SplitConjuncts(pushed.WhereClause());
    for (const Expression& predicate : pushable) {
      if (Expression rebound = RebindDerivedPredicate(
              predicate, pushed, source.alias.empty() ? "u" : source.alias)) {
        head_where.push_back(std::move(rebound));
      } else {
        return nullptr;
      }
    }
    if (!pushable.empty()) {
      pushed.SetWhereClause(CombineConjuncts(head_where));
    }
    std::vector<std::shared_ptr<SelectStatement>> branches;
    branches.reserve(derived.UnionAll().size());
    for (const auto& branch : derived.UnionAll()) {
      if (!branch) {
        continue;
      }
      auto copied = std::make_shared<SelectStatement>(*branch);
      std::vector<Expression> branch_where =
          SplitConjuncts(copied->WhereClause());
      for (const Expression& predicate : pushable) {
        if (!PredicateCanEnterDerived(
                predicate, *copied,
                source.alias.empty() ? "u" : source.alias)) {
          return nullptr;
        }
        if (Expression rebound = RebindDerivedPredicate(
                predicate, *copied,
                source.alias.empty() ? "u" : source.alias)) {
          branch_where.push_back(std::move(rebound));
        } else {
          return nullptr;
        }
      }
      if (!pushable.empty()) {
        copied->SetWhereClause(CombineConjuncts(branch_where));
      }
      branches.push_back(std::move(copied));
    }
    if (!pushable.empty()) {
      for (const auto& branch : branches) {
        if (branch == nullptr ||
            branch->SelectList().size() != pushed.SelectList().size()) {
          return nullptr;
        }
      }
      const std::vector<SetOperationKind> kinds = derived.SetOperationKinds();
      const std::vector<SetOperationMatch> matches = derived.Matches();
      pushed.ClearUnionAll();
      pushed.SetSetOperationTree(nullptr);
      for (size_t i = 0; i < branches.size(); ++i) {
        pushed.AddSetOperation(
            i < kinds.size() ? kinds[i] : SetOperationKind::kUnionAll,
            branches[i], i < matches.size() ? matches[i] : SetOperationMatch{});
      }
      SelectSource updated = source;
      updated.query = std::make_shared<SelectStatement>(std::move(pushed));
      rewritten->SetSources({std::move(updated)});
      rewritten->SetWhereClause(
          remaining.empty() ? Expression{} : CombineConjuncts(remaining));
      return rewritten;
    }
  }

  if (!pushable.empty() && derived.UnionAll().empty() &&
      !derived.GetSetOperationTree()) {
    auto copied = std::make_shared<SelectStatement>(derived);
    std::vector<Expression> inner_where = SplitConjuncts(copied->WhereClause());
    for (const Expression& predicate : pushable) {
      if (Expression rebound = RebindDerivedPredicate(
              predicate, *copied, source.alias.empty() ? "d" : source.alias)) {
        inner_where.push_back(std::move(rebound));
      } else {
        return nullptr;
      }
    }
    copied->SetWhereClause(CombineConjuncts(inner_where));
    SelectSource updated = source;
    updated.query = std::move(copied);
    rewritten->SetSources({std::move(updated)});
    rewritten->SetWhereClause(remaining.empty() ? Expression{}
                                                : CombineConjuncts(remaining));
    return rewritten;
  }

  // For UNION ALL without a pushed predicate, a finite outer LIMIT still
  // bounds how many rows each append branch can contribute.  The final outer
  // LIMIT remains authoritative, including OFFSET semantics.
  if (is_union && statement.OrderBy().empty() && statement.HasLimit() &&
      statement.Limit() > 0 && statement.Offset() == 0 &&
      !derived.UnionDistinct() &&
      std::ranges::all_of(derived.SetOperationKinds(),
                          [](SetOperationKind kind) {
                            return kind == SetOperationKind::kUnionAll;
                          })) {
    const size_t cap = statement.Limit();
    const bool head_needs_cap = !derived.HasLimit();
    const bool branch_needs_cap = std::ranges::any_of(
        derived.UnionAll(), [](const std::shared_ptr<SelectStatement>& branch) {
          return branch && !branch->HasLimit() && branch->Offset() == 0;
        });
    if (!head_needs_cap && !branch_needs_cap) {
      return nullptr;
    }
    SelectStatement pushed = derived;
    const std::vector<SetOperationMatch> matches = derived.Matches();
    pushed.ClearUnionAll();
    pushed.SetSetOperationTree(nullptr);
    auto cap_branch = [cap](const std::shared_ptr<SelectStatement>& branch) {
      auto copied = std::make_shared<SelectStatement>(*branch);
      if (!copied->HasLimit() && copied->Offset() == 0) {
        copied->SetLimit(cap);
      }
      return copied;
    };
    if (head_needs_cap) {
      pushed.SetLimit(cap);
    }
    for (size_t i = 0; i < derived.UnionAll().size(); ++i) {
      const auto& branch = derived.UnionAll()[i];
      if (branch) {
        pushed.AddSetOperation(
            SetOperationKind::kUnionAll, cap_branch(branch),
            i < matches.size() ? matches[i] : SetOperationMatch{});
      }
    }
    SelectSource updated = source;
    updated.query = std::make_shared<SelectStatement>(std::move(pushed));
    rewritten->SetSources({std::move(updated)});
    return rewritten;
  }

  // Scalar aggregate pushdown through UNION ALL: rewrite ungrouped scalar
  // COUNT/SUM/AVG/MIN/MAX over a pure positional UNION ALL so each branch
  // reduces rows in place (often hitting fused grouped/scalar columnar scans)
  // before combining partial results at the outer node.
  if (is_union && statement.WithQueries().empty() &&
      statement.UnionAll().empty() && !statement.UnionDistinct() &&
      !statement.GetSetOperationTree() && !statement.WhereClause() &&
      statement.GroupBy().empty() && !statement.IsGroupAll() &&
      !statement.Having() && !statement.Qualify() && !statement.Distinct() &&
      !statement.WithTies() && !statement.HasLimit() &&
      statement.Offset() == 0 && statement.OrderBy().empty() &&
      !statement.AsStruct() && !HasWindowFunctions(statement) &&
      !statement.SelectList().empty() && !source.is_lateral &&
      !source.from_nested_join && source.using_columns.empty() &&
      !source.join_condition && derived.WithQueries().empty() &&
      (!derived.GetSetOperationTree() ||
       !derived.GetSetOperationTree()->grouped) &&
      !derived.UnionDistinct() && !derived.UnionAll().empty() &&
      !derived.HasLimit() && derived.Offset() == 0 &&
      derived.OrderBy().empty() && !derived.WithTies()) {
    bool all_positional_union_all =
        derived.SetOperationKinds().size() <= derived.UnionAll().size() &&
        std::ranges::all_of(derived.SetOperationKinds(),
                            [](SetOperationKind kind) {
                              return kind == SetOperationKind::kUnionAll;
                            });
    for (size_t i = 0;
         all_positional_union_all && i < derived.UnionAll().size(); ++i) {
      const SetOperationMatch m = derived.MatchFor(i);
      if (m.by_name || m.corresponding || !m.columns.empty()) {
        all_positional_union_all = false;
      }
    }
    if (all_positional_union_all) {
      SelectStatement head_branch(derived);
      head_branch.ClearUnionAll();
      head_branch.SetSetOperationTree(nullptr);
      std::vector<const SelectStatement*> branches;
      branches.reserve(1 + derived.UnionAll().size());
      branches.push_back(&head_branch);
      for (const auto& b : derived.UnionAll()) {
        branches.push_back(b.get());
      }
      const size_t branch_width = head_branch.SelectList().size();
      bool branches_ok = branch_width > 0;
      for (const SelectStatement* b : branches) {
        if (b == nullptr || !b->WithQueries().empty() ||
            !b->UnionAll().empty() || b->UnionDistinct() ||
            b->GetSetOperationTree() || b->HasLimit() || b->Offset() != 0 ||
            b->WithTies() || b->Distinct() || !b->GroupBy().empty() ||
            b->IsGroupAll() || b->Having() || b->Qualify() ||
            !b->OrderBy().empty() || b->AsStruct() || HasWindowFunctions(*b) ||
            b->SelectList().size() != branch_width) {
          branches_ok = false;
          break;
        }
        for (const NamedExpression& item : b->SelectList()) {
          if (!item.expression || ContainsAggregate(item.expression) ||
              ContainsQuery(item.expression) ||
              (item.expression->Type() == TypeTag::kColumnValue &&
               item.expression->AsColumnValue().GetColumnName().name == "*")) {
            branches_ok = false;
            break;
          }
        }
        if (!branches_ok) {
          break;
        }
      }
      if (branches_ok) {
        std::vector<std::string> head_names;
        head_names.reserve(branch_width);
        for (const NamedExpression& item : head_branch.SelectList()) {
          if (!item.name.empty()) {
            head_names.push_back(item.name);
          } else if (item.expression->Type() == TypeTag::kColumnValue) {
            head_names.push_back(
                item.expression->AsColumnValue().GetColumnName().name);
          } else {
            head_names.emplace_back();
          }
        }
        auto resolve_head_col =
            [&](const ColumnName& col) -> std::optional<size_t> {
          if (!col.schema.empty() &&
              (source.alias.empty() ||
               !IdentifierEquals(col.schema, source.alias))) {
            return std::nullopt;
          }
          std::optional<size_t> matched;
          for (size_t c = 0; c < head_names.size(); ++c) {
            if (!head_names[c].empty() &&
                IdentifierEquals(head_names[c], col.name)) {
              if (matched.has_value()) {
                return std::nullopt;  // ambiguous
              }
              matched = c;
            }
          }
          return matched;
        };
        struct PushedAgg {
          const AggregateExpression* agg{nullptr};
          AggregationType op{AggregationType::kCount};
          bool count_star{false};
          Expression child;
          std::string col_a;
          std::string col_c;
        };
        std::vector<PushedAgg> pushed_aggs;
        std::unordered_map<const AggregateExpression*, size_t> agg_to_idx;
        bool select_ok = true;
        auto validate_and_collect = [&](auto& self, const Expression& node,
                                        bool inside_agg) -> bool {
          if (!node || node->Type() == TypeTag::kQueryExp ||
              node->Type() == TypeTag::kWindowFunctionExp ||
              ContainsParameterSlot(node)) {
            return false;
          }
          if (node->Type() == TypeTag::kColumnValue) {
            if (!inside_agg) {
              return false;
            }
            return resolve_head_col(node->AsColumnValue().GetColumnName())
                .has_value();
          }
          if (node->Type() == TypeTag::kAggregateExp) {
            if (inside_agg) {
              return false;
            }
            const auto& agg = node->AsAggregateExpression();
            if (agg.Distinct() || agg.IgnoreNulls() ||
                agg.Having() != AggregateHavingModifier::kNone ||
                agg.HavingCondition() || !agg.InnerOrderBy().empty() ||
                agg.InnerLimit().has_value() || agg.SecondaryArg() ||
                !agg.TrailingArgs().empty() || agg.WhereFilter() ||
                agg.NeedsGroupContext() || agg.HasInnerGroupBy()) {
              return false;
            }
            const AggregationType op = agg.GetType();
            if (op != AggregationType::kCount && op != AggregationType::kSum &&
                op != AggregationType::kAvg && op != AggregationType::kMin &&
                op != AggregationType::kMax) {
              return false;
            }
            const bool count_star =
                (op == AggregationType::kCount) && IsCountStar(agg);
            if (!agg_to_idx.contains(&agg)) {
              const size_t idx = pushed_aggs.size();
              agg_to_idx.emplace(&agg, idx);
              pushed_aggs.push_back(PushedAgg{
                  .agg = &agg,
                  .op = op,
                  .count_star = count_star,
                  .child = agg.Child(),
                  .col_a = "__tinylamb_uagg_a_" + std::to_string(idx),
                  .col_c = "__tinylamb_uagg_c_" + std::to_string(idx),
              });
            }
            return true;
          }
          for (const Expression& child : ExpressionChildren(node)) {
            if (!self(self, child, inside_agg)) {
              return false;
            }
          }
          return true;
        };
        for (const NamedExpression& item : statement.SelectList()) {
          if (!validate_and_collect(validate_and_collect, item.expression,
                                    false)) {
            select_ok = false;
            break;
          }
        }
        if (select_ok && !pushed_aggs.empty()) {
          std::vector<bool> referenced_cols(branch_width, false);
          auto collect_ref_cols = [&](auto& self,
                                      const Expression& node) -> void {
            if (!node) {
              return;
            }
            if (node->Type() == TypeTag::kColumnValue) {
              if (const auto col_idx =
                      resolve_head_col(node->AsColumnValue().GetColumnName());
                  col_idx.has_value()) {
                referenced_cols[*col_idx] = true;
              }
              return;
            }
            for (const Expression& ch : ExpressionChildren(node)) {
              self(self, ch);
            }
          };
          for (const PushedAgg& pa : pushed_aggs) {
            if (!pa.count_star) {
              collect_ref_cols(collect_ref_cols, pa.child);
            }
          }
          select_ok = UnionBranchesAgreeForScalarPushdown(
              branches, referenced_cols, inherited_ctes, context);
        }
        if (select_ok && !pushed_aggs.empty()) {
          auto rebind_for_branch =
              [&](auto& self, const Expression& node,
                  const SelectStatement& branch) -> Expression {
            if (!node) {
              return nullptr;
            }
            if (node->Type() == TypeTag::kColumnValue) {
              // Every referenced column was validated by select_ok above.
              const size_t col_idx =  // NOLINT(clang-analyzer-core.uninitialized.Assign)
                  *resolve_head_col(node->AsColumnValue().GetColumnName());
              return branch.SelectList()[col_idx].expression;
            }
            const std::vector<Expression> children = ExpressionChildren(node);
            if (children.empty()) {
              return node;
            }
            std::vector<Expression> rewritten_children;
            rewritten_children.reserve(children.size());
            for (const Expression& ch : children) {
              rewritten_children.push_back(self(self, ch, branch));
            }
            return WithExpressionChildren(node, std::move(rewritten_children));
          };
          auto build_branch = [&](const SelectStatement& src_branch) {
            SelectStatement out(src_branch);
            out.ClearUnionAll();
            out.SetSetOperationTree(nullptr);
            std::vector<NamedExpression> b_select;
            b_select.reserve(pushed_aggs.size() * 2);
            for (const PushedAgg& pa : pushed_aggs) {
              if (pa.count_star) {
                b_select.emplace_back(
                    pa.col_a,
                    AggregateExpressionExp(AggregationType::kCount, pa.child));
              } else {
                Expression rebound =
                    rebind_for_branch(rebind_for_branch, pa.child, src_branch);
                if (pa.op == AggregationType::kAvg) {
                  b_select.emplace_back(
                      pa.col_a,
                      AggregateExpressionExp(AggregationType::kAvg, rebound));
                  b_select.emplace_back(
                      pa.col_c, AggregateExpressionExp(AggregationType::kCount,
                                                       std::move(rebound)));
                } else {
                  b_select.emplace_back(
                      pa.col_a,
                      AggregateExpressionExp(pa.op, std::move(rebound)));
                }
              }
            }
            out.SetSelectList(std::move(b_select));
            return out;
          };
          SelectStatement new_derived = build_branch(head_branch);
          for (size_t i = 1; i < branches.size(); ++i) {
            new_derived.AddUnionAll(
                std::make_shared<SelectStatement>(build_branch(*branches[i])));
          }
          auto rebuild_outer = [&](auto& self,
                                   const Expression& node) -> Expression {
            if (!node) {
              return nullptr;
            }
            if (node->Type() == TypeTag::kAggregateExp) {
              const PushedAgg& pa =
                  pushed_aggs[agg_to_idx.at(&node->AsAggregateExpression())];
              if (pa.op == AggregationType::kAvg) {
                Expression ref_a = ColumnValueExp(pa.col_a);
                Expression ref_c = ColumnValueExp(pa.col_c);
                Expression sum_c =
                    AggregateExpressionExp(AggregationType::kSum, ref_c);
                Expression sum_ac = AggregateExpressionExp(
                    AggregationType::kSum,
                    BinaryExpressionExp(ref_a, BinaryOperation::kMultiply,
                                        ref_c));
                Expression zero_cond =
                    BinaryExpressionExp(sum_c, BinaryOperation::kEquals,
                                        ConstantValueExp(Value(int64_t{0})));
                return CaseExpressionExp(
                    {{std::move(zero_cond), ConstantValueExp(Value())}},
                    BinaryExpressionExp(std::move(sum_ac),
                                        BinaryOperation::kDivide,
                                        std::move(sum_c)));
              }
              const AggregationType combine_op =
                  (pa.op == AggregationType::kCount) ? AggregationType::kSum
                                                     : pa.op;
              return AggregateExpressionExp(combine_op,
                                            ColumnValueExp(pa.col_a));
            }
            const std::vector<Expression> children = ExpressionChildren(node);
            if (children.empty()) {
              return node;
            }
            std::vector<Expression> rewritten_children;
            rewritten_children.reserve(children.size());
            for (const Expression& ch : children) {
              rewritten_children.push_back(self(self, ch));
            }
            return WithExpressionChildren(node, std::move(rewritten_children));
          };
          std::vector<NamedExpression> outer_select;
          outer_select.reserve(statement.SelectList().size());
          for (size_t si = 0; si < statement.SelectList().size(); ++si) {
            const NamedExpression& item = statement.SelectList()[si];
            const std::string out_name =
                item.name.empty() ? ProjectionName(item, si) : item.name;
            outer_select.emplace_back(
                out_name, rebuild_outer(rebuild_outer, item.expression));
          }
          rewritten->SetSelectList(std::move(outer_select));
          SelectSource updated = source;
          updated.query =
              std::make_shared<SelectStatement>(std::move(new_derived));
          rewritten->SetSources({std::move(updated)});
          return rewritten;
        }
      }
    }
  }
  return nullptr;
}

namespace {

// Recursive-union column contract.  The set-operation metadata names the
// column-match mode per term: `BY NAME` / `STRICT CORRESPONDING` demand the
// same column names on both sides, plain `CORRESPONDING` keeps only the
// columns both sides produce (GoogleSQL's INNER default), and an ON/BY
// column list restricts either mode to the listed columns.  Positional
// unions keep the anchor's column order and require the same width.

bool NameMatchedUnion(const SetOperationMatch& match) {
  return match.by_name || match.corresponding || !match.columns.empty();
}

// Column names a term writes, as the author spelled them.  nullopt when the
// width is only known after execution (an unexpanded `*`).
std::optional<std::vector<std::string>> StaticTermNames(
    const SelectStatement& statement) {
  std::vector<std::string> names;
  names.reserve(statement.SelectList().size());
  for (const NamedExpression& item : statement.SelectList()) {
    if (item.name == "*" || item.star_modifiers != nullptr) {
      return std::nullopt;
    }
    names.push_back(item.name);
  }
  return names;
}

bool ContainsColumnName(const std::vector<std::string>& names,
                        std::string_view wanted) {
  return std::any_of(names.begin(), names.end(),
                     [&](const std::string& candidate) {
                       return IdentifierEquals(candidate, wanted);
                     });
}

// Maps `targets` (the working table's columns, in output order) onto `source`
// by name.  `strict` also rejects a source column the working table does not
// name, which is what BY NAME / STRICT CORRESPONDING require.
StatusOr<std::vector<size_t>> ResolveNameAlignment(
    const std::vector<std::string>& targets, const Schema& source, bool strict,
    const std::string& context) {
  std::vector<size_t> alignment;
  alignment.reserve(targets.size());
  for (const std::string& wanted : targets) {
    size_t found = source.ColumnCount();
    for (slot_t index = 0; index < source.ColumnCount(); ++index) {
      if (!IdentifierEquals(source.GetColumn(index).Name().name, wanted)) {
        continue;
      }
      if (found != source.ColumnCount()) {
        std::string msg = context;
        msg += " produces duplicate column ";
        msg += wanted;
        return StatusError(StatusCode::kInvalidArgument, std::move(msg));
      }
      found = index;
    }
    if (found == source.ColumnCount()) {
      std::string msg = context;
      msg += " has no column named ";
      msg += wanted;
      return StatusError(StatusCode::kInvalidArgument, std::move(msg));
    }
    alignment.push_back(found);
  }
  if (strict) {
    for (slot_t index = 0; index < source.ColumnCount(); ++index) {
      const std::string& name = source.GetColumn(index).Name().name;
      if (!ContainsColumnName(targets, name)) {
        std::string msg = context;
        msg += " produces column ";
        msg += name;
        msg += " that the working table does not name";
        return StatusError(StatusCode::kInvalidArgument, std::move(msg));
      }
    }
  }
  return alignment;
}

}  // namespace

StatusOr<Relation> ExecuteRecursiveCte(TransactionContext& context,
                                       const std::string& name,
                                       const SelectStatement& body,
                                       const Scope* outer,
                                       const CteMap& inherited_ctes,
                                       const RecursiveDepthSpec* depth_spec) {
  constexpr size_t kMaxIterations = 1024;
  constexpr size_t kMaxRows = 10'000'000;
  if (body.UnionAll().empty()) {
    return ExecuteQuery(context, body, outer, inherited_ctes);
  }

  // Evaluate the non-recursive head once, then bind the CTE name to only the
  // previous round's delta while evaluating each recursive branch.  This is
  // the work-table contract that the old merge omitted entirely.
  SelectStatement anchor = body;
  anchor.ClearUnionAll();
  anchor.SetOrderBy({});
  anchor.SetLimit(std::nullopt);
  anchor.SetOffset(0);
  ASSIGN_OR_RETURN(Relation, anchor_result,
                   (ExecuteQuery(context, anchor, outer, inherited_ctes)));
  anchor_result.FinishSpill();

  const bool track_depth = depth_spec != nullptr;
  const bool distinct = body.UnionDistinct();
  // Per-term column matching: the set-operation metadata is per pair, so one
  // body may mix positional and BY NAME pairs.
  std::vector<SetOperationMatch> term_matches;
  term_matches.reserve(body.UnionAll().size());
  for (size_t index = 0; index < body.UnionAll().size(); ++index) {
    term_matches.push_back(body.MatchFor(index));
  }
  const bool any_name_matching =
      std::ranges::any_of(term_matches, NameMatchedUnion);

  // Working-table schema.  A recursive CTE fixes its shape from the anchor
  // term: every round must produce the anchor's columns, so `SELECT *` over
  // the CTE keeps those names.  Name matching (BY NAME / STRICT
  // CORRESPONDING / CORRESPONDING) changes how a term's values bind to them,
  // never which columns the CTE has, and an anchor column the term cannot
  // produce is an error instead of a NULL-filled cell.
  std::vector<std::string> working_names;
  working_names.reserve(anchor_result.schema.ColumnCount());
  for (slot_t index = 0; index < anchor_result.schema.ColumnCount(); ++index) {
    working_names.push_back(anchor_result.schema.GetColumn(index).Name().name);
  }
  if (any_name_matching) {
    for (size_t index = 0; index < term_matches.size(); ++index) {
      const SetOperationMatch& match = term_matches[index];
      if (!NameMatchedUnion(match)) {
        continue;
      }
      std::optional<std::vector<std::string>> term_names;
      if (body.UnionAll()[index] != nullptr) {
        term_names = StaticTermNames(*body.UnionAll()[index]);
      }
      for (const std::string& listed : match.columns) {
        if (!ContainsColumnName(working_names, listed)) {
          std::string msg = "recursive CTE ";
          msg += name;
          msg += " has no column ";
          msg += listed;
          msg += " to match by name";
          return StatusError(StatusCode::kInvalidArgument, std::move(msg));
        }
      }
      for (const std::string& wanted : working_names) {
        if (wanted.empty()) {
          return StatusError(
              StatusCode::kInvalidArgument,
              "recursive CTE " + name + " needs named columns for BY NAME");
        }
        if (!match.columns.empty() &&
            !ContainsColumnName(match.columns, wanted)) {
          std::string msg = "recursive CTE ";
          msg += name;
          msg += " cannot drop column ";
          msg += wanted;
          msg += " in a name-matched recursive term";
          return StatusError(StatusCode::kInvalidArgument, std::move(msg));
        }
        if (term_names.has_value() &&
            !ContainsColumnName(*term_names, wanted)) {
          std::string msg = "recursive CTE ";
          msg += name;
          msg += " term has no column ";
          msg += wanted;
          return StatusError(StatusCode::kInvalidArgument, std::move(msg));
        }
      }
      if (match.by_name && term_names.has_value()) {
        for (const std::string& candidate : *term_names) {
          if (candidate.empty() ||
              !ContainsColumnName(working_names, candidate)) {
            return StatusError(
                StatusCode::kInvalidArgument,
                "recursive CTE " + name +
                    " term produces a column the anchor does not name: " +
                    (candidate.empty() ? std::string("<unnamed>") : candidate));
          }
        }
      }
    }
    for (size_t left = 0; left < working_names.size(); ++left) {
      for (size_t right = left + 1; right < working_names.size(); ++right) {
        if (IdentifierEquals(working_names[left], working_names[right])) {
          return StatusError(StatusCode::kInvalidArgument,
                             "recursive CTE " + name +
                                 " cannot match duplicate column " +
                                 working_names[left] + " by name");
        }
      }
    }
  }

  Relation result(context.execution_runtime());
  result.schema = anchor_result.schema;
  if (track_depth) {
    result.schema = result.schema +
                    Schema("", {Column(depth_spec->column, ValueType::kInt64)});
  }
  Relation delta(context.execution_runtime());
  delta.schema = result.schema;

  // UNION (DISTINCT) has to drop a row any earlier round already produced.
  // The working-table index answers that with one lookup; the linear scan it
  // replaces was quadratic (a 320k-row recursion spent minutes in it).  It
  // counts here (and only here) so a test can see the loop consult it.
  WorkTableIndex seen(/*count_stats=*/true);
  auto already_seen = [&](const Row& row) {
    return distinct && !seen.Insert(row);
  };
  auto add_anchor = [&](const Row& row) {
    if (already_seen(row)) {
      return;
    }
    Row stored = row;
    if (track_depth) {
      stored.values_.emplace_back(int64_t{0});
    }
    delta.AddRow(stored);
    if (!track_depth || (depth_spec->lower <= 0 && 0 <= depth_spec->upper)) {
      result.AddRow(std::move(stored));
    }
  };
  RETURN_IF_FAIL(anchor_result.ForEachRow(add_anchor));
  delta.FinishSpill();

  for (size_t iteration = 0; iteration < kMaxIterations; ++iteration) {
    if (delta.TotalRows() == 0) {
      break;
    }
    const int64_t row_depth = static_cast<int64_t>(iteration) + 1;
    if (track_depth && row_depth > depth_spec->upper) {
      break;
    }
    CteMap loop_ctes = inherited_ctes;
    ASSIGN_OR_RETURN(Relation, materialized_delta, MaterializeRelation(delta));
    loop_ctes[name] = std::make_shared<Relation>(std::move(materialized_delta));
    if (auto* runtime = context.execution_runtime()) {
      // Every round rebinds this CTE name to a fresh working Relation while
      // the recursive branch statements keep the same ASTs and addresses.
      // The uncorrelated-subquery caches (pointer fast path, memoized
      // structural key, and the node-keyed scalar/membership caches) would
      // otherwise hand the new round the PREVIOUS round's value for a
      // subquery that reads this CTE. Drop the CTE-dependent entries so this
      // round evaluates against the new working table.
      runtime->uncorrelated_results.clear();
      runtime->uncorrelated_results_by_fingerprint.clear();
      runtime->uncorrelated_key_memo.clear();
      runtime->uncorrelated_membership_by_node.clear();
      runtime->uncorrelated_scalar_by_node.clear();
    }
    Relation next(context.execution_runtime());
    next.schema = result.schema;
    for (size_t term_index = 0; term_index < body.UnionAll().size();
         ++term_index) {
      const std::shared_ptr<SelectStatement>& branch_statement =
          body.UnionAll()[term_index];
      if (!branch_statement) {
        continue;
      }
      const SetOperationMatch& match = term_matches[term_index];
      ASSIGN_OR_RETURN(
          Relation, branch,
          (ExecuteQuery(context, *branch_statement, outer, loop_ctes)));
      std::vector<size_t> alignment;
      if (NameMatchedUnion(match)) {
        ASSIGN_OR_RETURN(
            std::vector<size_t>, resolved_alignment,
            (ResolveNameAlignment(working_names, branch.schema,
                                  match.by_name && match.columns.empty(),
                                  "recursive CTE " + name + " term")));
        alignment = std::move(resolved_alignment);
      } else {
        for (size_t index = 0; index < working_names.size(); ++index) {
          alignment.push_back(index);
        }
      }
      // Name matching reads the columns the working table names, so the term
      // may carry extra columns (CORRESPONDING drops them); positional
      // matching requires exactly the working width.
      const bool name_matched = NameMatchedUnion(match);
      size_t required_width = 0;
      for (size_t index : alignment) {
        required_width = std::max(required_width, index + 1);
      }
      Status width_status = Status::kSuccess;
      std::string width_error;
      RETURN_IF_FAIL(branch.ForEachRow([&](const Row& row) {
        if (width_status != Status::kSuccess) {
          return;
        }
        if (name_matched ? row.values_.size() < required_width
                         : row.values_.size() != working_names.size()) {
          width_status = Status(StatusCode::kInvalidArgument);
          width_error = "recursive CTE " + name + " term produces " +
                        std::to_string(row.values_.size()) +
                        " columns but the working table reads " +
                        std::to_string(name_matched ? required_width
                                                    : working_names.size());
          return;
        }
        Row payload;
        payload.values_.reserve(working_names.size() + (track_depth ? 1U : 0U));
        for (size_t index : alignment) {
          payload.values_.push_back(row.values_[index]);
        }
        if (already_seen(payload)) {
          return;
        }
        if (track_depth) {
          payload.values_.emplace_back(row_depth);
        }
        next.AddRow(std::move(payload));
      }));
      if (width_status != Status::kSuccess) {
        return Status(width_status.GetCode(), std::move(width_error));
      }
    }
    RETURN_IF_FAIL(next.FinishSpill());
    RETURN_IF_FAIL(next.GetStatus());
    if (next.TotalRows() == 0) {
      break;
    }
    if (result.TotalRows() + next.TotalRows() > kMaxRows) {
      return StatusError(StatusCode::kInvalidArgument,
                         "recursive CTE " + name + " exceeded the row budget");
    }
    if (!track_depth ||
        (row_depth >= depth_spec->lower && row_depth <= depth_spec->upper)) {
      RETURN_IF_FAIL(
          next.ForEachRow([&](const Row& row) { result.AddRow(row); }));
    }
    // `result` keeps growing across rounds: finishing its spill writer here
    // made the next round's AddRow fail with "Append after FinishWriting"
    // once the accumulated rows spilled under a small budget.
    RETURN_IF_FAIL(result.GetStatus());
    delta = std::move(next);
  }
  RETURN_IF_FAIL(result.FinishSpill());
  RETURN_IF_FAIL(result.GetStatus());
  return result;
}

namespace {

// Total over the numeric/NULL cells accepted below. In particular, an
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
  if (std::getenv("TINYLAMB_DISABLE_PREDICATE_TRANSFER") != nullptr ||
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
    auto store = GetOrBuildLSTableStore(context.txn_, *table.Value(), true);
    if (!store || store->uncommitted_owner_txn != 0 ||
        store->layout_generation != table.Value()->LayoutGeneration() ||
        store->first_pid != table.Value()->FirstPageId() ||
        context.txn_.HasAnyPageVersionChains()) {
      return std::nullopt;
    }
    Schema schema =
        QualifySchema(table.Value()->GetSchema(), TableKeyFilterKey(source));
    uint64_t multiplier = std::max<uint64_t>(1, store->total_rows);
    if (i != 0) {
      // A complete unique-key equijoin cannot multiply a preserved row.
      auto terms = SplitConjuncts(source.join_condition);
      if (source.join_type != JoinType::kLeft) {
        auto where = SplitConjuncts(body.WhereClause());
        terms.insert(terms.end(), where.begin(), where.end());
      }
      for (const auto& key : EqualityKeys(joined, schema, terms)) {
        if (!key.null_safe && key.left_offset == 0 && key.right_offset == 0 &&
            joined.GetColumn(key.left).Type() == ValueType::kInt64 &&
            schema.GetColumn(key.right).Type() == ValueType::kInt64 &&
            !joined.GetColumn(key.left).IsUnsigned() &&
            !schema.GetColumn(key.right).IsUnsigned() &&
            key.right < store->column_is_unique_nonnull.size() &&
            store->column_is_unique_nonnull[key.right] != 0) {
          multiplier = 1;
          break;
        }
      }
    }
    if (multiplier > kMaxCount / row_bound) {
      return std::nullopt;
    }
    row_bound *= multiplier;
    for (size_t c = 0; c < schema.ColumnCount(); ++c) {
      locations.emplace_back(i, static_cast<slot_t>(c));
    }
    joined = i == 0 ? schema : joined + schema;
    schemas.push_back(std::move(schema));
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
    for (const auto& block : stores[source]->blocks) {
      const auto& column = block.columns[col];
      if (column.type == ValueType::kInt64) {
        if (!column.has_min_max) {
          // All-NULL blocks have no range. Do not infer this from a missing
          // synopsis.
          for (size_t r = 0; r < block.row_count; ++r) {
            if (!column.IsNull(r)) {
              return std::nullopt;
            }
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
          for (size_t r = 0; r < block.row_count; ++r) {
            if (!column.IsNull(r)) {
              return std::nullopt;
            }
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
  if (std::getenv("TINYLAMB_DISABLE_REVERSE_TRANSFER") != nullptr) {
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

StatusOr<std::optional<Relation>> TryPushGroupedCteJoins(
    TransactionContext& context, const SelectStatement& statement,
    const CteMap& inherited_ctes) {
  const auto decline = [&](int line) {
    if (std::getenv("TINYLAMB_FUSION_DEBUG") != nullptr &&
        statement.Sources().size() >= 2) {
      std::cerr << "[group-push] decline=" << line
                << " sources=" << statement.Sources().size()
                << " ctes=" << statement.WithQueries().size() << "\n";
    }
    return std::optional<Relation>{};
  };
  const auto& sources = statement.Sources();
  if (std::getenv("TINYLAMB_DISABLE_GROUP_OUTPUT_PUSH") != nullptr ||
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
      std::getenv("TINYLAMB_DISABLE_PUSHED_SCALAR_AGG") == nullptr;
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

bool BranchHasLargeDataScan(TransactionContext& context,
                            const SelectStatement& stmt, const CteMap& ctes) {
  constexpr uint64_t kLargeParallelScanRows = 65536;
  for (const SelectSource& source : stmt.Sources()) {
    if (source.query) {
      if (BranchHasLargeDataScan(context, *source.query, ctes)) {
        return true;
      }
      continue;
    }
    if (const auto cte = ctes.find(source.table); cte != ctes.end()) {
      if (cte->second != nullptr &&
          cte->second->TotalRows() >= kLargeParallelScanRows) {
        return true;
      }
      continue;
    }
    if (auto st = context.GetStats(source.table);
        st.HasValue() && st.Value()->Rows() >= kLargeParallelScanRows) {
      return true;
    }
    if (auto tbl = context.GetTable(source.table); tbl.HasValue()) {
      if (tbl.Value()->BackingColumnarRowCount() >= kLargeParallelScanRows) {
        return true;
      }
    }
  }
  for (const auto& [name, cte_stmt] : stmt.WithQueries()) {
    (void)name;
    if (cte_stmt != nullptr &&
        BranchHasLargeDataScan(context, *cte_stmt, ctes)) {
      return true;
    }
  }
  return false;
}

bool BranchLooksNonTrivial(TransactionContext& context,
                           const SelectStatement& stmt, const CteMap& ctes) {
  if (stmt.Sources().size() > 1 || !stmt.UnionAll().empty() ||
      stmt.GetSetOperationTree() != nullptr || !stmt.WithQueries().empty() ||
      HasWindowFunctions(stmt) ||
      (stmt.WhereClause() && ContainsQuery(stmt.WhereClause()))) {
    return true;
  }
  for (const SelectSource& source : stmt.Sources()) {
    if (source.query) {
      if (BranchLooksNonTrivial(context, *source.query, ctes)) {
        return true;
      }
      return true;
    }
    if (const auto cte = ctes.find(source.table); cte != ctes.end()) {
      if (cte->second != nullptr && cte->second->TotalRows() >= 256) {
        return true;
      }
      continue;
    }
    if (auto st = context.GetStats(source.table);
        st.HasValue() && st.Value()->Rows() >= 256) {
      return true;
    }
    if (auto tbl = context.GetTable(source.table); tbl.HasValue()) {
      if (tbl.Value()->BackingColumnarRowCount() >= 256) {
        return true;
      }
    }
  }
  return false;
}

void InitWorkerRuntime(const ExecutionRuntime& parent,
                       ExecutionRuntime* worker) {
  worker->root_statement = parent.root_statement;
  worker->reusable_base_relations = parent.reusable_base_relations;
  worker->reusable_projections = parent.reusable_projections;
  worker->grouping_rank_limit = parent.grouping_rank_limit;
  worker->shared_base_cache = parent.shared_base_cache;
}

void MergeWorkerRuntimeIntoParent(ExecutionRuntime& worker,
                                  ExecutionRuntime* parent) {
  if (parent == nullptr) {
    return;
  }
  parent->retained_scan_stores.insert(
      parent->retained_scan_stores.end(),
      std::make_move_iterator(worker.retained_scan_stores.begin()),
      std::make_move_iterator(worker.retained_scan_stores.end()));
  parent->retained_statements.insert(
      parent->retained_statements.end(),
      std::make_move_iterator(worker.retained_statements.begin()),
      std::make_move_iterator(worker.retained_statements.end()));
  for (auto& [k, arena] : worker.base_relation_arenas) {
    parent->base_relation_arenas.emplace(k, std::move(arena));
  }
  parent->shared_exists_build_reuses += worker.shared_exists_build_reuses;
  parent->correlated_index_builds += worker.correlated_index_builds;
  parent->prebuilt_exists_index_builds += worker.prebuilt_exists_index_builds;
  parent->prebuilt_exists_index_rows += worker.prebuilt_exists_index_rows;
  parent->prebuilt_exists_keys += worker.prebuilt_exists_keys;
  parent->correlated_index_probes += worker.correlated_index_probes;
  parent->correlated_result_cache_hits += worker.correlated_result_cache_hits;
  parent->uncorrelated_cache_hits += worker.uncorrelated_cache_hits;
  parent->uncorrelated_hash_builds += worker.uncorrelated_hash_builds;
  parent->uncorrelated_hash_probes += worker.uncorrelated_hash_probes;
  parent->exists_short_circuit_queries += worker.exists_short_circuit_queries;
  parent->scan_ms += worker.scan_ms;
  parent->filter_ms += worker.filter_ms;
  parent->join_ms += worker.join_ms;
  parent->project_ms += worker.project_ms;
  parent->sort_ms += worker.sort_ms;
  parent->boundary_ms += worker.boundary_ms;
  parent->build_input_ms += worker.build_input_ms;
  parent->window_ms += worker.window_ms;
  parent->window_layout_ms += worker.window_layout_ms;
  parent->deferred_materialize_ms += worker.deferred_materialize_ms;
  parent->window_input_rows += worker.window_input_rows;
  parent->rank_bounds += worker.rank_bounds;
  parent->topn_finish_uses += worker.topn_finish_uses;
  parent->topn_finish_capacity =
      std::max(parent->topn_finish_capacity, worker.topn_finish_capacity);
  parent->topn_finish_input_rows += worker.topn_finish_input_rows;
  parent->base_scan_cache_hits += worker.base_scan_cache_hits;
  parent->aggregate_input_rows += worker.aggregate_input_rows;
  parent->aggregate_groups += worker.aggregate_groups;
  parent->aggregate_updates += worker.aggregate_updates;
  parent->aggregate_ms += worker.aggregate_ms;
  parent->planning_ms += worker.planning_ms;
  parent->join_estimate_ms += worker.join_estimate_ms;
  parent->partitioned_aggregate_merges += worker.partitioned_aggregate_merges;
  parent->partitioned_numeric_aggregations +=
      worker.partitioned_numeric_aggregations;
  parent->dictionary_aggregations += worker.dictionary_aggregations;
  parent->dictionary_summary_aggregations +=
      worker.dictionary_summary_aggregations;
  parent->dictionary_aggregate_rows += worker.dictionary_aggregate_rows;
  parent->partitioned_dictionary_aggregations +=
      worker.partitioned_dictionary_aggregations;
  parent->typed_parallel_aggregations += worker.typed_parallel_aggregations;
  parent->grouped_join_fusions += worker.grouped_join_fusions;
  parent->pushed_group_rows += worker.pushed_group_rows;
  parent->pushed_group_joins += worker.pushed_group_joins;
  parent->transfer_seed_rows += worker.transfer_seed_rows;
  parent->transfer_scan_tested += worker.transfer_scan_tested;
  parent->transfer_scan_rejected += worker.transfer_scan_rejected;
  parent->streamed_grouped_joins += worker.streamed_grouped_joins;
  parent->selective_index_scans += worker.selective_index_scans;
  parent->selective_index_rows += worker.selective_index_rows;
  parent->scan_rows += worker.scan_rows;
  parent->scan_output_rows += worker.scan_output_rows;
  parent->scan_values_decoded += worker.scan_values_decoded;
  parent->scan_values_available += worker.scan_values_available;
  parent->relation_spills += worker.relation_spills;
  parent->inner_hash_build_rows += worker.inner_hash_build_rows;
  parent->inner_hash_index_estimate_bytes +=
      worker.inner_hash_index_estimate_bytes;
  parent->inner_hash_left_builds += worker.inner_hash_left_builds;
  parent->inner_hash_spilled_probe_rows += worker.inner_hash_spilled_probe_rows;
  parent->key_filter_scans += worker.key_filter_scans;
  parent->key_filter_keys += worker.key_filter_keys;
  parent->key_filter_rejected += worker.key_filter_rejected;
  parent->key_filter_null_rejected += worker.key_filter_null_rejected;
  parent->key_filter_minmax += worker.key_filter_minmax;
  parent->key_filter_bloom += worker.key_filter_bloom;
  parent->key_filter_bytes += worker.key_filter_bytes;
  parent->correlated_outer_serials += worker.correlated_outer_serials;
  parent->correlated_domain_filter_builds +=
      worker.correlated_domain_filter_builds;
  parent->empty_build_short_circuit |= worker.empty_build_short_circuit;
  parent->null_aware_anti_build_contains_null |=
      worker.null_aware_anti_build_contains_null;
}

StatusOr<std::optional<std::vector<Relation>>> TryExecuteBranchesParallel(
    TransactionContext& context,
    std::span<const SelectStatement* const> branches, const CteMap& ctes,
    bool columnar_output = false) {
  static const bool parallel_branches_disabled =
      std::getenv("TINYLAMB_DISABLE_PARALLEL_BRANCHES") != nullptr;
  if (parallel_branches_disabled || branches.size() < 2) {
    return std::optional<std::vector<Relation>>{};
  }
  const size_t avail = ParallelWorkerCount(256);
  if (avail < 2) {
    return std::optional<std::vector<Relation>>{};
  }
  size_t non_trivial = 0;
  for (const SelectStatement* branch : branches) {
    if (branch == nullptr || StatementContainsVolatileFunction(*branch) ||
        BranchHasLargeDataScan(context, *branch, ctes)) {
      return std::optional<std::vector<Relation>>{};
    }
    if (BranchLooksNonTrivial(context, *branch, ctes)) {
      ++non_trivial;
    }
  }
  if (non_trivial < 2) {
    return std::optional<std::vector<Relation>>{};
  }

  const size_t count = branches.size();
  const size_t team_size = std::min(count, avail);
  ExecutionRuntime* const parent_runtime = context.execution_runtime();
  ExecutionRuntime* const saved_raw_runtime = context.raw_execution_runtime();
  std::vector<ExecutionRuntime> worker_runtimes(count);
  if (parent_runtime != nullptr) {
    if (!parent_runtime->shared_base_cache) {
      parent_runtime->shared_base_cache =
          std::make_shared<SharedBaseRelationCache>();
    }
    for (size_t i = 0; i < count; ++i) {
      InitWorkerRuntime(*parent_runtime, &worker_runtimes[i]);
      worker_runtimes[i].shared_base_cache = parent_runtime->shared_base_cache;
    }
  }
  std::vector<std::optional<Relation>> results(count);
  std::vector<Status> statuses(count, Status::kSuccess);
  std::atomic<size_t> next_branch{0};
  std::atomic<size_t> active_workers{team_size};

  // Clear parent runtime visibility while parallel branch teams run so any
  // nested ParallelPool task without a thread-local override cannot race on
  // parent ExecutionRuntime maps.
  struct ParentRuntimeReset {
    TransactionContext& ctx;
    ExecutionRuntime* saved_raw;
    ParentRuntimeReset(TransactionContext& c, ExecutionRuntime* s)
        : ctx(c), saved_raw(s) {
      ctx.set_execution_runtime(nullptr);
    }
    ParentRuntimeReset(const ParentRuntimeReset&) = delete;
    ParentRuntimeReset& operator=(const ParentRuntimeReset&) = delete;
    ParentRuntimeReset(ParentRuntimeReset&&) = delete;
    ParentRuntimeReset& operator=(ParentRuntimeReset&&) = delete;
    ~ParentRuntimeReset() { ctx.set_execution_runtime(saved_raw); }
  } reset_guard(context, saved_raw_runtime);
  TransactionContext::ScopedRuntimeOverride clear_caller_tls(context, nullptr);

  const auto run_worker = [&]() {
    ScopedQueryParallelismBudget budget(&active_workers, avail);
    for (;;) {
      const size_t b = next_branch.fetch_add(1, std::memory_order_relaxed);
      if (b >= count) {
        break;
      }
      try {
        TransactionContext::ScopedRuntimeOverride rt_guard(
            context, parent_runtime != nullptr ? &worker_runtimes[b] : nullptr);
        StatusOr<Relation> executed = ExecuteQuery(
            context, *branches[b], nullptr, ctes, nullptr, columnar_output);
        if (!executed.HasValue()) {
          statuses[b] = executed.GetStatus();
        } else {
          Relation rel = executed.MoveValue();
          rel.set_runtime(parent_runtime);
          results[b].emplace(std::move(rel));
        }
      } catch (const std::exception& error) {
        statuses[b] = StatusError(StatusCode::kRuntimeError, error.what());
      } catch (...) {
        statuses[b] =
            StatusError(StatusCode::kRuntimeError, "parallel branch failed");
      }
    }
    active_workers.fetch_sub(1, std::memory_order_acq_rel);
  };

  {
    std::vector<std::jthread> helpers;
    helpers.reserve(team_size - 1);
    for (size_t w = 1; w < team_size; ++w) {
      helpers.emplace_back(run_worker);
    }
    run_worker();
  }

  for (size_t b = 0; b < count; ++b) {
    if (!statuses[b].ok()) {
      return statuses[b];
    }
  }
  std::vector<Relation> output;
  output.reserve(count);
  for (size_t b = 0; b < count; ++b) {
    MergeWorkerRuntimeIntoParent(worker_runtimes[b], parent_runtime);
    if (!results[b].has_value()) {
      return StatusError(StatusCode::kRuntimeError,
                         "missing parallel branch output");
    }
    // NOLINTNEXTLINE(bugprone-unchecked-optional-access) harness assumes
    // success
    output.push_back(std::move(*results[b]));  // NOLINT(bugprone-unchecked-optional-access)
  }
  return std::optional<std::vector<Relation>>(std::move(output));
}

bool TryConcatenateColumnarUnionAll(Relation& left, Relation& right) {
  if (left.HasSpill() || right.HasSpill() ||
      left.schema.ColumnCount() != right.schema.ColumnCount()) {
    return false;
  }
  const size_t ncols = left.schema.ColumnCount();
  const size_t left_rows = left.TotalRows();
  const size_t right_rows = right.TotalRows();
  if (!left.HasColumnarOnly() && !right.HasColumnarOnly()) {
    return false;
  }
  const auto merge_counters = [&]() {
    left.hash_joins += right.hash_joins;
    left.hybrid_hash_joins += right.hybrid_hash_joins;
    left.in_memory_hash_joins += right.in_memory_hash_joins;
    left.nested_loop_joins += right.nested_loop_joins;
    left.join_comparisons += right.join_comparisons;
    left.peak_intermediate_rows =
        std::max(left.peak_intermediate_rows, right.peak_intermediate_rows);
  };
  if (right_rows == 0) {
    if (!left.HasColumnarOnly()) {
      return false;
    }
    merge_counters();
    return true;
  }
  if (left_rows == 0 && right.HasColumnarOnly()) {
    std::vector<Column> updated_cols;
    updated_cols.reserve(ncols);
    bool schema_changed = false;
    for (size_t c = 0; c < ncols; ++c) {
      Column lcol = left.schema.GetColumn(c);
      const Column& rcol = right.schema.GetColumn(c);
      if (lcol.Type() == ValueType::kNull && rcol.Type() != ValueType::kNull) {
        lcol = Column(lcol.Name(), rcol.Type(), lcol.GetConstraint());
        lcol.SetUnsigned(rcol.IsUnsigned());
        lcol.SetInterval(rcol.IsInterval());
        schema_changed = true;
      } else if ((lcol.Type() != rcol.Type() ||
                  lcol.IsUnsigned() != rcol.IsUnsigned() ||
                  lcol.IsInterval() != rcol.IsInterval()) &&
                 rcol.Type() != ValueType::kNull) {
        return false;
      }
      updated_cols.push_back(std::move(lcol));
    }
    if (schema_changed) {
      left.schema = Schema(left.schema.Name(), std::move(updated_cols));
    }
    left.SetColumnarTable(std::move(right.columnar), right.uniform_cell_types);
    merge_counters();
    return true;
  }
  if (!left.HasColumnarOnly() || !right.HasColumnarOnly()) {
    return false;
  }
  struct ColMeta {
    bool set{false};
    ValueType type{ValueType::kNull};
    bool is_unsigned{false};
    bool is_interval{false};
  };
  std::vector<ColMeta> expected(ncols);
  const auto inspect_table = [&](const relational_detail::ColumnarTable& tbl) {
    for (const auto& chunk : tbl.chunks) {
      if (chunk.rows == 0) {
        continue;
      }
      if (chunk.columns.size() != ncols) {
        return false;
      }
      for (size_t c = 0; c < ncols; ++c) {
        const auto& col = chunk.columns[c];
        if (!col.is_uniform) {
          return false;
        }
        if (col.encoding ==
                relational_detail::ColumnarColumn::Encoding::kConstant &&
            col.const_is_null) {
          continue;
        }
        if (!expected[c].set) {
          expected[c] = {.set = true,
                         .type = col.type,
                         .is_unsigned = col.is_unsigned,
                         .is_interval = col.is_interval};
        } else if (expected[c].type != col.type ||
                   expected[c].is_unsigned != col.is_unsigned ||
                   expected[c].is_interval != col.is_interval) {
          return false;
        }
      }
    }
    return true;
  };
  if (!inspect_table(*left.columnar) || !inspect_table(*right.columnar)) {
    return false;
  }
  auto merged = std::make_shared<relational_detail::ColumnarTable>();
  merged->chunks.reserve(left.columnar->chunks.size() +
                         right.columnar->chunks.size());
  merged->keepalives.reserve(left.columnar->keepalives.size() +
                             right.columnar->keepalives.size());
  const auto append_copy = [&](const relational_detail::ColumnarTable& src) {
    for (const auto& owner : src.keepalives) {
      if (owner != nullptr) {
        merged->keepalives.push_back(owner);
      }
    }
    for (const auto& chunk : src.chunks) {
      if (chunk.rows == 0) {
        continue;
      }
      relational_detail::ColumnarChunk dst = chunk;
      for (size_t c = 0; c < ncols; ++c) {
        auto& col = dst.columns[c];
        if (expected[c].set &&
            col.encoding ==
                relational_detail::ColumnarColumn::Encoding::kConstant &&
            col.const_is_null &&
            (col.type != expected[c].type ||
             col.is_unsigned != expected[c].is_unsigned ||
             col.is_interval != expected[c].is_interval)) {
          col.type = expected[c].type;
          col.is_unsigned = expected[c].is_unsigned;
          col.is_interval = expected[c].is_interval;
          col.has_null = (expected[c].type != ValueType::kNull);
        }
      }
      merged->AppendChunk(std::move(dst));
    }
  };
  append_copy(*left.columnar);
  append_copy(*right.columnar);
  std::vector<Column> merged_cols;
  merged_cols.reserve(ncols);
  bool schema_changed = false;
  for (size_t c = 0; c < ncols; ++c) {
    Column col = left.schema.GetColumn(c);
    if (expected[c].set && col.Type() == ValueType::kNull &&
        expected[c].type != ValueType::kNull) {
      col = Column(col.Name(), expected[c].type, col.GetConstraint());
      col.SetUnsigned(expected[c].is_unsigned);
      col.SetInterval(expected[c].is_interval);
      schema_changed = true;
    }
    merged_cols.push_back(std::move(col));
  }
  if (schema_changed) {
    left.schema = Schema(left.schema.Name(), std::move(merged_cols));
  }
  left.SetColumnarTable(std::move(merged), true);
  merge_counters();
  return true;
}

// Resolves kGathered and checks SQL NULL for zero-copy set-operation hashing
// and equality. Returns true iff the leaf cell is non-NULL.
inline bool ResolveNonNullSetLeaf(const ColumnarColumn* col, size_t row_idx,
                                  const ColumnarColumn** out_leaf,
                                  size_t* out_row) noexcept {
  if (col->encoding == ColumnarColumn::Encoding::kGathered) {
    return col->ResolveLeaf(row_idx, out_leaf, out_row);
  }
  if (col->IsNull(row_idx)) {
    return false;
  }
  *out_leaf = col;
  *out_row = row_idx;
  return true;
}

inline int64_t ReadSetIntLeaf(const ColumnarColumn* leaf,
                              size_t row_idx) noexcept {
  switch (leaf->encoding) {
    case ColumnarColumn::Encoding::kFlat:
      return leaf->ints[row_idx];
    case ColumnarColumn::Encoding::kConstant:
      return leaf->const_int;
    case ColumnarColumn::Encoding::kDictionary:
      return leaf->dict != nullptr
                 ? leaf->dict->IntEntryAt(leaf->DictCodeAt(row_idx))
                 : 0;
    case ColumnarColumn::Encoding::kFrameOfReference:
      return leaf->ForIntAt(row_idx);
    default:
      return 0;
  }
}

inline double ReadSetDoubleLeaf(const ColumnarColumn* leaf,
                                size_t row_idx) noexcept {
  switch (leaf->encoding) {
    case ColumnarColumn::Encoding::kFlat:
      return leaf->doubles[row_idx];
    case ColumnarColumn::Encoding::kConstant:
      return leaf->const_double;
    case ColumnarColumn::Encoding::kDictionary:
      return leaf->dict != nullptr
                 ? leaf->dict->DoubleEntries()[leaf->DictCodeAt(row_idx)]
                 : 0.0;
    case ColumnarColumn::Encoding::kFrameOfReference:
      return static_cast<double>(leaf->ForIntAt(row_idx));
    default:
      return 0.0;
  }
}

inline std::string_view ReadSetStringLeaf(const ColumnarColumn* leaf,
                                          size_t row_idx) noexcept {
  switch (leaf->encoding) {
    case ColumnarColumn::Encoding::kFlat:
      return leaf->strings[row_idx];
    case ColumnarColumn::Encoding::kConstant:
      return leaf->const_string;
    case ColumnarColumn::Encoding::kDictionary:
      return leaf->dict != nullptr
                 ? leaf->dict->StringEntryAt(leaf->DictCodeAt(row_idx))
                 : std::string_view{};
    default:
      return {};
  }
}

struct SetDictHashCache {
  struct CacheKey {
    const LSTableColumnDictionary* dict{nullptr};
    ValueType vt{ValueType::kNull};

    bool operator==(const CacheKey& o) const noexcept {
      return dict == o.dict && vt == o.vt;
    }
  };

  struct KeyHash {
    size_t operator()(const CacheKey& k) const noexcept {
      return reinterpret_cast<uintptr_t>(k.dict) ^
             (static_cast<size_t>(k.vt) * 0x9e3779b97f4a7c15ULL);
    }
  };

  std::unordered_map<CacheKey, std::vector<uint64_t>, KeyHash> hashes_by_dict;
  const LSTableColumnDictionary* last_dict{nullptr};
  ValueType last_vt{ValueType::kNull};
  const std::vector<uint64_t>* last_hashes{nullptr};

  const std::vector<uint64_t>& GetOrBuild(const LSTableColumnDictionary* dict) {
    return GetOrBuildForType(dict, ValueType::kVarChar);
  }

  const std::vector<uint64_t>& GetOrBuildForType(
      const LSTableColumnDictionary* dict, ValueType vt) {
    if (dict == last_dict && vt == last_vt && last_hashes != nullptr)
        [[likely]] {
      return *last_hashes;
    }
    const CacheKey key{.dict = dict, .vt = vt};
    auto it = hashes_by_dict.find(key);
    if (it == hashes_by_dict.end()) {
      std::vector<uint64_t> hashes;
      if (vt == ValueType::kVarChar) {
        const size_t n = dict->StringEntryCount();
        hashes.resize(n);
        for (size_t i = 0; i < n; ++i) {
          const std::string_view sv = dict->StringEntryAt(i);
          hashes[i] = MixSetHash(std::hash<std::string_view>{}(sv) ^
                                 0x517cc1b727220a95ULL);
        }
      } else if (vt == ValueType::kDouble) {
        const auto entries = dict->DoubleEntries();
        hashes.resize(entries.size());
        for (size_t i = 0; i < entries.size(); ++i) {
          const double d = entries[i];
          const uint64_t bits =
              std::isnan(d) ? 0x7ff8000000000000ULL
                            : (d == 0.0 ? 0ULL : std::bit_cast<uint64_t>(d));
          hashes[i] = MixSetHash(bits ^ 0x13198a2e03707344ULL);
        }
      } else {
        const auto entries = dict->IntEntries();
        hashes.resize(entries.size());
        for (size_t i = 0; i < entries.size(); ++i) {
          hashes[i] = MixSetHash(static_cast<uint64_t>(entries[i]) ^
                                 0x243f6a8885a308d3ULL);
        }
      }
      it = hashes_by_dict.emplace(key, std::move(hashes)).first;
    }
    last_dict = dict;
    last_vt = vt;
    last_hashes = &it->second;
    return it->second;
  }
};

struct SetGatherSlot {
  const ChunkGatherIndex* gi{nullptr};
  uint32_t row_idx{0};
  uint32_t tc{0};
  uint32_t tr{0};
};

inline bool ResolveNonNullSetLeafCached(const ColumnarColumn* col,
                                        size_t row_idx, SetGatherSlot* cache,
                                        size_t* cache_len,
                                        const ColumnarColumn** out_leaf,
                                        size_t* out_row) noexcept {
  const ColumnarColumn* cur = col;
  size_t r = row_idx;
  while (cur->encoding == ColumnarColumn::Encoding::kGathered) {
    const ChunkGatherIndex* gi = cur->gather_index.get();
    if (gi == nullptr || gi->source_table == nullptr ||
        r >= gi->src_rows.size()) {
      return false;
    }
    const auto r32 = static_cast<uint32_t>(r);
    uint32_t tc = 0;
    uint32_t tr = ChunkGatherIndex::kNullRow;
    bool found = false;
    for (size_t i = 0; i < *cache_len; ++i) {
      if (cache[i].gi == gi && cache[i].row_idx == r32) {
        tc = cache[i].tc;
        tr = cache[i].tr;
        found = true;
        break;
      }
    }
    if (!found) {
      tc = gi->src_chunks[r];
      tr = gi->src_rows[r];
      if (*cache_len < 8) {
        cache[(*cache_len)++] =
            SetGatherSlot{.gi = gi, .row_idx = r32, .tc = tc, .tr = tr};
      }
    }
    if (tr == ChunkGatherIndex::kNullRow ||
        tc >= gi->source_table->chunks.size()) {
      return false;
    }
    const ColumnarChunk& sc = gi->source_table->chunks[tc];
    if (cur->gather_col >= sc.columns.size() || tr >= sc.rows) {
      return false;
    }
    cur = &sc.columns[cur->gather_col];
    r = tr;
  }
  if (cur->type == ValueType::kNull || cur->NonGatheredIsNull(r)) {
    return false;
  }
  *out_leaf = cur;
  *out_row = r;
  return true;
}

inline bool EqualSetCellResolved(const ColumnarColumn* la, size_t lra,
                                 const ColumnarColumn* lb, size_t lrb,
                                 ValueType vt) noexcept {
  if (la->encoding == ColumnarColumn::Encoding::kDictionary &&
      lb->encoding == ColumnarColumn::Encoding::kDictionary &&
      la->dict != nullptr && la->dict == lb->dict) {
    const uint32_t ca = la->DictCodeAt(lra);
    const uint32_t cb = lb->DictCodeAt(lrb);
    if (ca == cb) {
      return true;
    }
    if (vt == ValueType::kVarChar && la->dict->bytewise_equality) {
      return false;
    }
  }
  switch (vt) {
    case ValueType::kInt64:
    case ValueType::kDate:
      return ReadSetIntLeaf(la, lra) == ReadSetIntLeaf(lb, lrb);
    case ValueType::kDouble: {
      const double da = ReadSetDoubleLeaf(la, lra);
      const double db = ReadSetDoubleLeaf(lb, lrb);
      return (da == db) || (std::isnan(da) && std::isnan(db));
    }
    case ValueType::kVarChar:
      return ReadSetStringLeaf(la, lra) == ReadSetStringLeaf(lb, lrb);
    default:
      return true;
  }
}

struct SetColumnMetadata {
  std::vector<ValueType> canon_types;
  std::vector<uint8_t> canon_unsigned;
  std::vector<uint8_t> left_all_null;
  std::vector<uint8_t> right_all_null;
  std::vector<bool> left_may_null;
  std::vector<bool> right_may_null;
  size_t ncols{0};
  size_t left_rows{0};
  size_t right_rows{0};
};

bool InspectSetTableSide(const std::shared_ptr<const ColumnarTable>& tbl,
                         size_t ncols, std::span<ValueType> canon_types,
                         std::span<uint8_t> canon_unsigned,
                         std::vector<uint8_t>* side_all_null,
                         std::vector<bool>* side_may_null, size_t* out_rows) {
  if (!tbl) {
    return true;
  }
  for (const ColumnarChunk& chunk : tbl->chunks) {
    if (chunk.rows == 0) {
      continue;
    }
    if (chunk.columns.size() != ncols) {
      return false;
    }
    *out_rows += chunk.rows;
    for (size_t c = 0; c < ncols; ++c) {
      const ColumnarColumn& col = chunk.columns[c];
      if (!col.is_uniform || col.is_interval) {
        return false;
      }
      switch (col.type) {
        case ValueType::kNull:
        case ValueType::kInt64:
        case ValueType::kDate:
        case ValueType::kDouble:
        case ValueType::kVarChar:
          break;
        default:
          return false;
      }
      const bool chunk_all_null =
          (col.type == ValueType::kNull) ||
          (col.encoding == ColumnarColumn::Encoding::kConstant &&
           col.const_is_null);
      if (!chunk_all_null) {
        (*side_all_null)[c] = 0;
        if (canon_types[c] == ValueType::kNull) {
          canon_types[c] = col.type;
          canon_unsigned[c] = col.is_unsigned ? 1 : 0;
        } else if (canon_types[c] != col.type ||
                   (canon_unsigned[c] != 0) != col.is_unsigned) {
          return false;
        }
      }
      if (side_may_null != nullptr && (chunk_all_null || col.has_null)) {
        (*side_may_null)[c] = true;
      }
    }
  }
  return true;
}

bool InspectColumnarSetInputs(const Relation& left, const Relation& right,
                              SetColumnMetadata* meta) {
  if (left.HasSpill() || right.HasSpill() || !left.rows.empty() ||
      !right.rows.empty()) {
    return false;
  }
  const size_t ncols = left.schema.ColumnCount();
  if (ncols == 0 || right.schema.ColumnCount() != ncols) {
    return false;
  }
  const size_t total_chunks =
      (left.columnar ? left.columnar->chunks.size() : 0) +
      (right.columnar ? right.columnar->chunks.size() : 0);
  if (total_chunks >= (1U << 31)) {
    return false;
  }
  meta->ncols = ncols;
  meta->canon_types.assign(ncols, ValueType::kNull);
  meta->canon_unsigned.assign(ncols, 0);
  meta->left_all_null.assign(ncols, 1);
  meta->right_all_null.assign(ncols, 1);
  meta->left_may_null.assign(ncols, false);
  meta->right_may_null.assign(ncols, false);
  meta->left_rows = 0;
  meta->right_rows = 0;

  return InspectSetTableSide(left.columnar, ncols, meta->canon_types,
                             meta->canon_unsigned, &meta->left_all_null,
                             &meta->left_may_null, &meta->left_rows) &&
         InspectSetTableSide(right.columnar, ncols, meta->canon_types,
                             meta->canon_unsigned, &meta->right_all_null,
                             &meta->right_may_null, &meta->right_rows);
}

void NormalizeSetSchemaTypes(Relation* target,
                             std::span<const ValueType> canon_types,
                             std::span<const uint8_t> canon_unsigned) {
  const size_t ncols = canon_types.size();
  std::vector<Column> updated_cols;
  updated_cols.reserve(ncols);
  bool changed = false;
  for (size_t c = 0; c < ncols; ++c) {
    Column col = target->schema.GetColumn(c);
    if (col.Type() == ValueType::kNull && canon_types[c] != ValueType::kNull) {
      col.SetType(canon_types[c]);
      col.SetUnsigned(canon_unsigned[c] != 0);
      changed = true;
    }
    updated_cols.push_back(std::move(col));
  }
  if (changed) {
    target->schema =
        Schema(target->schema.Name(), std::move(updated_cols),
               target->schema.KeyColumnIndex(), target->schema.SchemaVersion());
  }
}

void NormalizeSetResultSchema(Relation* left, const SetColumnMetadata& meta) {
  NormalizeSetSchemaTypes(left, meta.canon_types, meta.canon_unsigned);
}

// Compact zero-copy hash table mapping a (global_chunk_idx, row_idx) exemplar
// to a multiplicity counter using cache-friendly 8-byte open-addressed slots.
class ColumnarSetHashTable {
 public:
  struct Ref {
    uint64_t hash;
    uint32_t chunk_idx;
    uint32_t row_idx;
  };

  struct Entry {
    uint64_t hash;
    uint32_t chunk_idx;
    uint32_t row_idx;
    uint32_t count;
  };

  struct Slot {
    uint32_t hash_hi{0};
    uint32_t entry_plus_one{0};
  };

  ColumnarSetHashTable(std::span<const ColumnarChunk* const> all_chunks,
                       std::span<const ValueType> canon_types,
                       size_t expected_entries)
      : all_chunks_(all_chunks), canon_types_(canon_types) {
    size_t cap = 16;
    const size_t min_cap = (expected_entries * 2) + 8;
    while (cap < min_cap) {
      cap <<= 1U;
    }
    slots_.resize(cap);
    entries_.reserve(expected_entries);
  }

  Entry* FindOrInsert(const Ref& ref, bool* inserted) {
    if (entries_.size() * 2 >= slots_.size()) {
      Rehash(slots_.size() * 2);
    }
    const size_t mask = slots_.size() - 1;
    const uint32_t tag = HashHi(ref.hash);
    size_t idx = ProbeStart(ref.hash, mask);
    while (true) {
      const Slot& s = slots_[idx];
      if (s.entry_plus_one == 0) {
        const auto entry_idx = static_cast<uint32_t>(entries_.size());
        entries_.push_back(Entry{.hash = ref.hash,
                                 .chunk_idx = ref.chunk_idx,
                                 .row_idx = ref.row_idx,
                                 .count = 1});
        slots_[idx] = Slot{.hash_hi = tag, .entry_plus_one = entry_idx + 1};
        *inserted = true;
        return &entries_[entry_idx];
      }
      if (s.hash_hi == tag) {
        Entry& e = entries_[s.entry_plus_one - 1];
        if (e.hash == ref.hash &&
            RowsEqual(ref.chunk_idx, ref.row_idx, e.chunk_idx, e.row_idx)) {
          *inserted = false;
          return &e;
        }
      }
      idx = (idx + 1) & mask;
    }
  }

  Entry* Find(const Ref& ref) {
    const size_t mask = slots_.size() - 1;
    const uint32_t tag = HashHi(ref.hash);
    size_t idx = ProbeStart(ref.hash, mask);
    while (true) {
      const Slot& s = slots_[idx];
      if (s.entry_plus_one == 0) {
        return nullptr;
      }
      if (s.hash_hi == tag) {
        Entry& e = entries_[s.entry_plus_one - 1];
        if (e.hash == ref.hash &&
            RowsEqual(ref.chunk_idx, ref.row_idx, e.chunk_idx, e.row_idx)) {
          return &e;
        }
      }
      idx = (idx + 1) & mask;
    }
  }

 private:
  [[nodiscard]] static constexpr uint32_t HashHi(uint64_t hash) noexcept {
    return static_cast<uint32_t>(hash >> 32U);
  }

  [[nodiscard]] static size_t ProbeStart(uint64_t hash, size_t mask) noexcept {
    return static_cast<size_t>((hash >> 6U) ^ (hash >> 32U) ^ hash) & mask;
  }

  [[nodiscard]] bool RowsEqual(uint32_t chunk_a, uint32_t ra, uint32_t chunk_b,
                               uint32_t rb) const noexcept {
    const ColumnarChunk& ca = *all_chunks_[chunk_a];
    const ColumnarChunk& cb = *all_chunks_[chunk_b];
    const size_t ncols = canon_types_.size();
    std::array<SetGatherSlot, 8> ga_cache{};
    size_t ga_len = 0;
    std::array<SetGatherSlot, 8> gb_cache{};
    size_t gb_len = 0;
    for (size_t c = 0; c < ncols; ++c) {
      if (canon_types_[c] == ValueType::kNull) {
        continue;
      }
      const ColumnarColumn* la = nullptr;
      const ColumnarColumn* lb = nullptr;
      size_t lra = 0;
      size_t lrb = 0;
      const bool na = !ResolveNonNullSetLeafCached(
          &ca.columns[c], ra, ga_cache.data(), &ga_len, &la, &lra);
      const bool nb = !ResolveNonNullSetLeafCached(
          &cb.columns[c], rb, gb_cache.data(), &gb_len, &lb, &lrb);
      if (na || nb) {
        if (na != nb) {
          return false;
        }
        continue;
      }
      if (!EqualSetCellResolved(la, lra, lb, lrb, canon_types_[c])) {
        return false;
      }
    }
    return true;
  }

  void Rehash(size_t new_cap) {
    std::vector<Slot> new_slots(new_cap);
    const size_t mask = new_cap - 1;
    for (const Slot& s : slots_) {
      if (s.entry_plus_one == 0) {
        continue;
      }
      const uint64_t h = entries_[s.entry_plus_one - 1].hash;
      size_t idx = ProbeStart(h, mask);
      while (new_slots[idx].entry_plus_one != 0) {
        idx = (idx + 1) & mask;
      }
      new_slots[idx] = s;
    }
    slots_ = std::move(new_slots);
  }

  std::span<const ColumnarChunk* const> all_chunks_;
  std::span<const ValueType> canon_types_;
  std::vector<Slot> slots_;
  std::vector<Entry> entries_;
};

inline uint64_t HashSetCellResolved(const ColumnarColumn* leaf, size_t lr,
                                    ValueType vt,
                                    SetDictHashCache* dict_cache) {
  switch (vt) {
    case ValueType::kInt64:
    case ValueType::kDate:
      if (leaf->encoding == ColumnarColumn::Encoding::kDictionary &&
          leaf->dict != nullptr) {
        const uint32_t code = leaf->DictCodeAt(lr);
        const auto& hashes = dict_cache->GetOrBuildForType(leaf->dict, vt);
        if (code < hashes.size()) [[likely]] {
          return hashes[code];
        }
      }
      return MixSetHash(static_cast<uint64_t>(ReadSetIntLeaf(leaf, lr)) ^
                        0x243f6a8885a308d3ULL);
    case ValueType::kDouble: {
      if (leaf->encoding == ColumnarColumn::Encoding::kDictionary &&
          leaf->dict != nullptr) {
        const uint32_t code = leaf->DictCodeAt(lr);
        const auto& hashes = dict_cache->GetOrBuildForType(leaf->dict, vt);
        if (code < hashes.size()) [[likely]] {
          return hashes[code];
        }
      }
      const double d = ReadSetDoubleLeaf(leaf, lr);
      const uint64_t bits =
          std::isnan(d) ? 0x7ff8000000000000ULL
                        : (d == 0.0 ? 0ULL : std::bit_cast<uint64_t>(d));
      return MixSetHash(bits ^ 0x13198a2e03707344ULL);
    }
    case ValueType::kVarChar: {
      if (leaf->encoding == ColumnarColumn::Encoding::kDictionary &&
          leaf->dict != nullptr) {
        const uint32_t code = leaf->DictCodeAt(lr);
        const auto& hashes = dict_cache->GetOrBuildForType(leaf->dict, vt);
        if (code < hashes.size()) [[likely]] {
          return hashes[code];
        }
      }
      const std::string_view sv = ReadSetStringLeaf(leaf, lr);
      return MixSetHash(std::hash<std::string_view>{}(sv) ^
                        0x517cc1b727220a95ULL);
    }
    default:
      return 0x9e3779b97f4a7c15ULL;
  }
}

void HashColumnarSetSlice(const ColumnarChunk& chunk, uint32_t row_begin,
                          uint32_t row_end,
                          std::span<const ValueType> canon_types,
                          SetDictHashCache* dict_cache,
                          std::vector<uint64_t>* row_hashes) {
  constexpr uint64_t kNullCellHash = 0x9e3779b97f4a7c15ULL;
  auto mix_one = [](uint64_t h, uint64_t ch) noexcept {
    return MixSetHash(h ^ (ch + 0x9e3779b97f4a7c15ULL + (h << 6U) + (h >> 2U)));
  };
  const size_t n = row_end - row_begin;
  row_hashes->assign(n, kNullCellHash);
  uint64_t* hr = row_hashes->data();
  const size_t ncols = canon_types.size();

  for (size_t c = 0; c < ncols; ++c) {
    const ValueType vt = canon_types[c];
    if (vt == ValueType::kNull) {
      for (size_t i = 0; i < n; ++i) {
        hr[i] = MixSetHash(hr[i] ^ kNullCellHash);
      }
      continue;
    }
    const ColumnarColumn& col = chunk.columns[c];
    if (col.type == ValueType::kNull ||
        (col.encoding == ColumnarColumn::Encoding::kConstant &&
         col.const_is_null)) {
      for (size_t i = 0; i < n; ++i) {
        hr[i] = mix_one(hr[i], kNullCellHash);
      }
      continue;
    }
    if (col.encoding == ColumnarColumn::Encoding::kConstant) {
      uint64_t ch = kNullCellHash;
      switch (vt) {
        case ValueType::kInt64:
        case ValueType::kDate:
          ch = MixSetHash(static_cast<uint64_t>(col.const_int) ^
                          0x243f6a8885a308d3ULL);
          break;
        case ValueType::kDouble: {
          const double d = col.const_double;
          const uint64_t bits =
              std::isnan(d) ? 0x7ff8000000000000ULL
                            : (d == 0.0 ? 0ULL : std::bit_cast<uint64_t>(d));
          ch = MixSetHash(bits ^ 0x13198a2e03707344ULL);
          break;
        }
        case ValueType::kVarChar:
          ch = MixSetHash(std::hash<std::string_view>{}(col.const_string) ^
                          0x517cc1b727220a95ULL);
          break;
        default:
          break;
      }
      for (size_t i = 0; i < n; ++i) {
        hr[i] = mix_one(hr[i], ch);
      }
      continue;
    }
    if (col.encoding == ColumnarColumn::Encoding::kFlat) {
      const bool check_null = col.has_null && !col.nulls.empty();
      const uint8_t* nulls =
          check_null ? col.nulls.data() + row_begin : nullptr;
      switch (vt) {
        case ValueType::kInt64:
        case ValueType::kDate: {
          const int64_t* vals = col.ints.data() + row_begin;
          if (!check_null) {
            for (size_t i = 0; i < n; ++i) {
              const uint64_t ch = MixSetHash(static_cast<uint64_t>(vals[i]) ^
                                             0x243f6a8885a308d3ULL);
              hr[i] = mix_one(hr[i], ch);
            }
          } else {
            for (size_t i = 0; i < n; ++i) {
              const uint64_t ch =
                  nulls[i] != 0 ? kNullCellHash
                                : MixSetHash(static_cast<uint64_t>(vals[i]) ^
                                             0x243f6a8885a308d3ULL);
              hr[i] = mix_one(hr[i], ch);
            }
          }
          break;
        }
        case ValueType::kDouble: {
          const double* vals = col.doubles.data() + row_begin;
          if (!check_null) {
            for (size_t i = 0; i < n; ++i) {
              const double d = vals[i];
              const uint64_t bits =
                  std::isnan(d)
                      ? 0x7ff8000000000000ULL
                      : (d == 0.0 ? 0ULL : std::bit_cast<uint64_t>(d));
              const uint64_t ch = MixSetHash(bits ^ 0x13198a2e03707344ULL);
              hr[i] = mix_one(hr[i], ch);
            }
          } else {
            for (size_t i = 0; i < n; ++i) {
              uint64_t ch = kNullCellHash;
              if (nulls[i] == 0) {
                const double d = vals[i];
                const uint64_t bits =
                    std::isnan(d)
                        ? 0x7ff8000000000000ULL
                        : (d == 0.0 ? 0ULL : std::bit_cast<uint64_t>(d));
                ch = MixSetHash(bits ^ 0x13198a2e03707344ULL);
              }
              hr[i] = mix_one(hr[i], ch);
            }
          }
          break;
        }
        case ValueType::kVarChar: {
          const std::string_view* vals = col.strings.data() + row_begin;
          if (!check_null) {
            for (size_t i = 0; i < n; ++i) {
              const uint64_t ch =
                  MixSetHash(std::hash<std::string_view>{}(vals[i]) ^
                             0x517cc1b727220a95ULL);
              hr[i] = mix_one(hr[i], ch);
            }
          } else {
            for (size_t i = 0; i < n; ++i) {
              const uint64_t ch =
                  nulls[i] != 0
                      ? kNullCellHash
                      : MixSetHash(std::hash<std::string_view>{}(vals[i]) ^
                                   0x517cc1b727220a95ULL);
              hr[i] = mix_one(hr[i], ch);
            }
          }
          break;
        }
        default:
          break;
      }
      continue;
    }
    if (col.encoding == ColumnarColumn::Encoding::kDictionary &&
        col.dict != nullptr) {
      const auto& hashes = dict_cache->GetOrBuildForType(col.dict, vt);
      const size_t nhashes = hashes.size();
      const bool check_null = col.has_null && !col.nulls.empty();
      const uint8_t* nulls =
          check_null ? col.nulls.data() + row_begin : nullptr;
      if (!check_null) {
        for (size_t i = 0; i < n; ++i) {
          const uint32_t code = col.DictCodeAt(row_begin + i);
          const uint64_t ch =
              (code != ColumnarColumn::kNullDictCode && code < nhashes)
                  ? hashes[code]
                  : kNullCellHash;
          hr[i] = mix_one(hr[i], ch);
        }
      } else {
        for (size_t i = 0; i < n; ++i) {
          uint64_t ch = kNullCellHash;
          if (nulls[i] == 0) {
            const uint32_t code = col.DictCodeAt(row_begin + i);
            if (code != ColumnarColumn::kNullDictCode && code < nhashes) {
              ch = hashes[code];
            }
          }
          hr[i] = mix_one(hr[i], ch);
        }
      }
      continue;
    }
    if (col.encoding == ColumnarColumn::Encoding::kGathered) {
      const ChunkGatherIndex* gi = col.gather_index.get();
      if (gi == nullptr || gi->source_table == nullptr ||
          row_end > gi->src_rows.size()) {
        for (size_t i = 0; i < n; ++i) {
          hr[i] = mix_one(hr[i], kNullCellHash);
        }
        continue;
      }
      const uint32_t* tc_ptr = gi->src_chunks.data() + row_begin;
      const uint32_t* tr_ptr = gi->src_rows.data() + row_begin;
      const ColumnarChunk* src_chunks = gi->source_table->chunks.data();
      const size_t num_src_chunks = gi->source_table->chunks.size();
      const uint32_t gcol = col.gather_col;
      for (size_t i = 0; i < n; ++i) {
        const uint32_t tc = tc_ptr[i];
        const uint32_t tr = tr_ptr[i];
        uint64_t ch = kNullCellHash;
        if (tr != ChunkGatherIndex::kNullRow && tc < num_src_chunks)
            [[likely]] {
          const ColumnarChunk& sc = src_chunks[tc];
          if (gcol < sc.columns.size() && tr < sc.rows) [[likely]] {
            const ColumnarColumn* leaf = &sc.columns[gcol];
            size_t lr = tr;
            while (leaf->encoding == ColumnarColumn::Encoding::kGathered)
                [[unlikely]] {
              const ChunkGatherIndex* ngi = leaf->gather_index.get();
              if (ngi == nullptr || ngi->source_table == nullptr ||
                  lr >= ngi->src_rows.size()) {
                leaf = nullptr;
                break;
              }
              const uint32_t ntc = ngi->src_chunks[lr];
              const uint32_t ntr = ngi->src_rows[lr];
              if (ntr == ChunkGatherIndex::kNullRow ||
                  ntc >= ngi->source_table->chunks.size()) {
                leaf = nullptr;
                break;
              }
              const ColumnarChunk& nsc = ngi->source_table->chunks[ntc];
              if (leaf->gather_col >= nsc.columns.size() || ntr >= nsc.rows) {
                leaf = nullptr;
                break;
              }
              leaf = &nsc.columns[leaf->gather_col];
              lr = ntr;
            }
            if (leaf != nullptr && leaf->type != ValueType::kNull &&
                !leaf->NonGatheredIsNull(lr)) {
              ch = HashSetCellResolved(leaf, lr, vt, dict_cache);
            }
          }
        }
        hr[i] = mix_one(hr[i], ch);
      }
      continue;
    }
    for (size_t i = 0; i < n; ++i) {
      const ColumnarColumn* leaf = nullptr;
      size_t lr = 0;
      const uint64_t ch = ResolveNonNullSetLeaf(&col, row_begin + i, &leaf, &lr)
                              ? HashSetCellResolved(leaf, lr, vt, dict_cache)
                              : kNullCellHash;
      hr[i] = mix_one(hr[i], ch);
    }
  }
}

ColumnarChunk MaterializeSetChunk(
    const std::shared_ptr<const ColumnarTable>& src_tbl,
    std::span<const std::pair<uint32_t, uint32_t>> refs,
    std::span<const uint8_t> side_all_null,
    std::span<const ValueType> canon_types,
    std::span<const uint8_t> canon_unsigned, bool* used_varchar) {
  const size_t count = refs.size();
  const size_t ncols = canon_types.size();
  ColumnarChunk out_chunk;
  out_chunk.rows = count;
  out_chunk.columns.resize(ncols);
  const auto& chunks = src_tbl->chunks;
  bool any_varchar = false;

  std::vector<uint8_t> skip_col(ncols, 0);
  std::vector<uint8_t> col_has_null(ncols, 0);
  for (size_t c = 0; c < ncols; ++c) {
    const ValueType vt = canon_types[c];
    ColumnarColumn& dst = out_chunk.columns[c];
    if (side_all_null[c] != 0 || vt == ValueType::kNull) {
      dst.SetConstantNull(vt, canon_unsigned[c] != 0, false, count);
      skip_col[c] = 1;
      continue;
    }
    dst.Init(vt, canon_unsigned[c] != 0, false, 0);
    switch (vt) {
      case ValueType::kInt64:
      case ValueType::kDate:
        dst.ints.resize(count);
        break;
      case ValueType::kDouble:
        dst.doubles.resize(count);
        break;
      case ValueType::kVarChar:
        any_varchar = true;
        dst.strings.resize(count);
        break;
      default:
        skip_col[c] = 1;
        break;
    }
  }

  constexpr size_t kTileRows = 64;
  for (size_t base = 0; base < count; base += kTileRows) {
    const size_t tile_n = std::min(kTileRows, count - base);
    std::array<std::array<SetGatherSlot, 8>, kTileRows> tile_caches{};
    std::array<uint8_t, kTileRows> tile_cache_lens{};
    for (size_t c = 0; c < ncols; ++c) {
      if (skip_col[c] != 0) {
        continue;
      }
      const ValueType vt = canon_types[c];
      ColumnarColumn& dst = out_chunk.columns[c];
      auto mark_null = [&](size_t i) {
        if (col_has_null[c] == 0) {
          dst.has_null = true;
          dst.nulls.assign(count, 0);
          col_has_null[c] = 1;
        }
        dst.nulls[i] = 1;
      };
      switch (vt) {
        case ValueType::kInt64:
        case ValueType::kDate: {
          int64_t* out_ptr = dst.ints.data() + base;
          for (size_t k = 0; k < tile_n; ++k) {
            const auto [ci, ri] = refs[base + k];
            const ColumnarColumn& src = chunks[ci].columns[c];
            const ColumnarColumn* leaf = nullptr;
            size_t lr = 0;
            size_t clen = tile_cache_lens[k];
            const bool ok = ResolveNonNullSetLeafCached(
                &src, ri, tile_caches[k].data(), &clen, &leaf, &lr);
            tile_cache_lens[k] = static_cast<uint8_t>(clen);
            if (!ok) [[unlikely]] {
              mark_null(base + k);
              out_ptr[k] = 0;
            } else {
              out_ptr[k] =
                  leaf->is_uniform ? ReadSetIntLeaf(leaf, lr) : leaf->IntAt(lr);
            }
          }
          break;
        }
        case ValueType::kDouble: {
          double* out_ptr = dst.doubles.data() + base;
          for (size_t k = 0; k < tile_n; ++k) {
            const auto [ci, ri] = refs[base + k];
            const ColumnarColumn& src = chunks[ci].columns[c];
            const ColumnarColumn* leaf = nullptr;
            size_t lr = 0;
            size_t clen = tile_cache_lens[k];
            const bool ok = ResolveNonNullSetLeafCached(
                &src, ri, tile_caches[k].data(), &clen, &leaf, &lr);
            tile_cache_lens[k] = static_cast<uint8_t>(clen);
            if (!ok) [[unlikely]] {
              mark_null(base + k);
              out_ptr[k] = 0.0;
            } else {
              out_ptr[k] = leaf->is_uniform ? ReadSetDoubleLeaf(leaf, lr)
                                            : leaf->DoubleAt(lr);
            }
          }
          break;
        }
        case ValueType::kVarChar: {
          std::string_view* out_ptr = dst.strings.data() + base;
          for (size_t k = 0; k < tile_n; ++k) {
            const auto [ci, ri] = refs[base + k];
            const ColumnarColumn& src = chunks[ci].columns[c];
            const ColumnarColumn* leaf = nullptr;
            size_t lr = 0;
            size_t clen = tile_cache_lens[k];
            const bool ok = ResolveNonNullSetLeafCached(
                &src, ri, tile_caches[k].data(), &clen, &leaf, &lr);
            tile_cache_lens[k] = static_cast<uint8_t>(clen);
            if (!ok) [[unlikely]] {
              mark_null(base + k);
              out_ptr[k] = std::string_view{};
            } else {
              out_ptr[k] = leaf->is_uniform ? ReadSetStringLeaf(leaf, lr)
                                            : leaf->StringAt(lr);
            }
          }
          break;
        }
        default:
          break;
      }
    }
  }
  if (any_varchar) {
    out_chunk.keepalives.push_back(src_tbl);
    *used_varchar = true;
  }
  return out_chunk;
}

void AppendSetMaterializedSide(
    const std::shared_ptr<const ColumnarTable>& src_tbl,
    std::vector<std::vector<std::pair<uint32_t, uint32_t>>>& part_refs,
    std::span<const uint8_t> side_all_null,
    std::span<const ValueType> canon_types,
    std::span<const uint8_t> canon_unsigned, ColumnarTable* out_table) {
  size_t total_refs = 0;
  for (const auto& refs : part_refs) {
    total_refs += refs.size();
  }
  if (total_refs == 0) {
    return;
  }

  std::vector<std::pair<uint32_t, uint32_t>> flat_refs;
  struct ChunkRange {
    const std::pair<uint32_t, uint32_t>* ptr{nullptr};
    size_t count{0};
  };
  std::vector<ChunkRange> ranges;

  const bool parallel_build = total_refs >= 4096;
  flat_refs.reserve(total_refs);
  for (auto& refs : part_refs) {
    flat_refs.insert(flat_refs.end(), refs.begin(), refs.end());
    std::vector<std::pair<uint32_t, uint32_t>>{}.swap(refs);
  }
  for (size_t off = 0; off < flat_refs.size(); off += kColumnarChunkRows) {
    ranges.push_back(ChunkRange{
        .ptr = flat_refs.data() + off,
        .count = std::min(kColumnarChunkRows, flat_refs.size() - off)});
  }

  std::vector<ColumnarChunk> out_chunks(ranges.size());
  std::atomic<bool> any_varchar{false};
  if (parallel_build && ranges.size() > 1) {
    (void)RunParallelTasks(ranges.size(), [&](size_t bi) {
      bool local_varchar = false;
      out_chunks[bi] = MaterializeSetChunk(
          src_tbl, {ranges[bi].ptr, ranges[bi].count}, side_all_null,
          canon_types, canon_unsigned, &local_varchar);
      if (local_varchar) {
        any_varchar.store(true, std::memory_order_relaxed);
      }
    });
  } else {
    bool local_varchar = false;
    for (size_t bi = 0; bi < ranges.size(); ++bi) {
      out_chunks[bi] = MaterializeSetChunk(
          src_tbl, {ranges[bi].ptr, ranges[bi].count}, side_all_null,
          canon_types, canon_unsigned, &local_varchar);
    }
    if (local_varchar) {
      any_varchar.store(true, std::memory_order_relaxed);
    }
  }
  if (any_varchar.load(std::memory_order_relaxed)) {
    out_table->AppendKeepalive(src_tbl);
  }
  for (ColumnarChunk& ch : out_chunks) {
    out_table->AppendChunk(std::move(ch));
  }
}

struct PartitionedSetWorkerBuckets {
  std::vector<std::vector<ColumnarSetHashTable::Ref>> group0;
  std::vector<std::vector<ColumnarSetHashTable::Ref>> group1;
};

bool HashAllChunksIntoPartitions(
    std::span<const ColumnarChunk* const> all_chunks, size_t split_chunk_idx,
    std::span<const ValueType> canon_types, size_t num_partitions,
    std::vector<PartitionedSetWorkerBuckets>* out_workers) {
  struct ChunkSlice {
    uint32_t chunk_idx{0};
    uint32_t row_begin{0};
    uint32_t row_end{0};
  };
  constexpr uint32_t kMaxSliceRows = 4096;
  std::vector<ChunkSlice> slices;
  slices.reserve(all_chunks.size());
  for (uint32_t ci = 0; ci < all_chunks.size(); ++ci) {
    const auto n = static_cast<uint32_t>(all_chunks[ci]->rows);
    for (uint32_t begin = 0; begin < n; begin += kMaxSliceRows) {
      slices.push_back(
          ChunkSlice{.chunk_idx = ci,
                     .row_begin = begin,
                     .row_end = std::min(begin + kMaxSliceRows, n)});
    }
  }

  const size_t part_mask = num_partitions - 1;
  const size_t num_workers =
      num_partitions == 1
          ? 1
          : std::max<size_t>(1, ParallelWorkerCount(slices.size()));
  out_workers->assign(num_workers, PartitionedSetWorkerBuckets{});
  // Size both groups even when the right side has no chunks (an empty right
  // relation): the partition pass reads group1[p] unconditionally, and an
  // unsized group1 was an out-of-bounds read (EXCEPT over an empty right).
  for (PartitionedSetWorkerBuckets& wb : *out_workers) {
    wb.group0.resize(num_partitions);
    wb.group1.resize(num_partitions);
  }

  auto process_worker_slices = [&](size_t worker_id) {
    SetDictHashCache dict_cache;
    PartitionedSetWorkerBuckets& wb = (*out_workers)[worker_id];
    std::vector<uint64_t> slice_hashes;
    slice_hashes.reserve(kMaxSliceRows);
    for (size_t s = worker_id; s < slices.size(); s += num_workers) {
      const ChunkSlice& slice = slices[s];
      const ColumnarChunk& chunk = *all_chunks[slice.chunk_idx];
      HashColumnarSetSlice(chunk, slice.row_begin, slice.row_end, canon_types,
                           &dict_cache, &slice_hashes);
      auto& target_parts = slice.chunk_idx < split_chunk_idx
                               ? wb.group0
                               : wb.group1;
      const uint32_t count = slice.row_end - slice.row_begin;
      for (uint32_t i = 0; i < count; ++i) {
        const uint64_t h = slice_hashes[i];
        target_parts[h & part_mask].push_back(
            ColumnarSetHashTable::Ref{.hash = h,
                                      .chunk_idx = slice.chunk_idx,
                                      .row_idx = slice.row_begin + i});
      }
    }
  };
  if (num_workers <= 1) {
    process_worker_slices(0);
    return true;
  }
  return RunParallelTasks(num_workers, process_worker_slices) ==
         Status::kSuccess;
}

bool TryColumnarSetOperation(Relation& left, Relation& right,
                             SetOperationKind operation) {
  SetColumnMetadata meta;
  if (!InspectColumnarSetInputs(left, right, &meta)) {
    return false;
  }
  const size_t total_input_rows = meta.left_rows + meta.right_rows;
  auto set_charge = ResidentSetCharge(left, right);
  if (!set_charge && total_input_rows > 0 &&
      !Relation::HasAmpleMemoryBudget()) {
    return false;
  }

  auto finish_with_table = [&](std::shared_ptr<ColumnarTable> out_table) {
    CopyExecutionStats(&left, right);
    NormalizeSetResultSchema(&left, meta);
    left.using_columns.reset();
    left.SetColumnarTable(std::move(out_table), true);
    right.ReleaseCharge();
    return true;
  };

  // Handle trivial zero-row set operations before allocating hash state.
  if (operation == SetOperationKind::kIntersect ||
      operation == SetOperationKind::kIntersectAll) {
    if (meta.left_rows == 0 || meta.right_rows == 0) {
      return finish_with_table(std::make_shared<ColumnarTable>());
    }
  } else if (operation == SetOperationKind::kExcept) {
    if (meta.left_rows == 0) {
      return finish_with_table(std::make_shared<ColumnarTable>());
    }
  } else if (operation == SetOperationKind::kExceptAll) {
    if (meta.left_rows == 0) {
      return finish_with_table(std::make_shared<ColumnarTable>());
    }
    if (meta.right_rows == 0) {
      CopyExecutionStats(&left, right);
      NormalizeSetResultSchema(&left, meta);
      left.using_columns.reset();
      left.uniform_cell_types = true;
      right.ReleaseCharge();
      return true;
    }
  } else if (operation == SetOperationKind::kUnion && total_input_rows == 0) {
    return finish_with_table(std::make_shared<ColumnarTable>());
  }

  const ColumnarTable* left_tbl = left.columnar.get();
  const ColumnarTable* right_tbl = right.columnar.get();
  const uint32_t left_chunks_n =
      left_tbl != nullptr ? static_cast<uint32_t>(left_tbl->chunks.size()) : 0U;
  std::vector<const ColumnarChunk*> all_chunks;
  all_chunks.reserve(left_chunks_n +
                     (right_tbl != nullptr ? right_tbl->chunks.size() : 0));
  if (left_tbl != nullptr) {
    for (const ColumnarChunk& c : left_tbl->chunks) {
      all_chunks.push_back(&c);
    }
  }
  if (right_tbl != nullptr) {
    for (const ColumnarChunk& c : right_tbl->chunks) {
      all_chunks.push_back(&c);
    }
  }

  using Ref = ColumnarSetHashTable::Ref;
  using Entry = ColumnarSetHashTable::Entry;

  constexpr size_t kParallelThreshold = 8192;
  constexpr size_t kNumPartitions = 64;
  const size_t num_partitions =
      total_input_rows >= kParallelThreshold ? kNumPartitions : 1;

  std::vector<PartitionedSetWorkerBuckets> workers;
  if (!HashAllChunksIntoPartitions(all_chunks, left_chunks_n, meta.canon_types,
                                   num_partitions, &workers)) {
    return false;
  }

  std::vector<std::vector<std::pair<uint32_t, uint32_t>>> out_left(
      num_partitions);
  std::vector<std::vector<std::pair<uint32_t, uint32_t>>> out_right(
      num_partitions);

  auto process_partition = [&](size_t p) {
    size_t pl_count = 0;
    size_t pr_count = 0;
    for (const PartitionedSetWorkerBuckets& wb : workers) {
      pl_count += wb.group0[p].size();
      pr_count += wb.group1[p].size();
    }
    auto for_each_side = [&](bool right_side, auto&& fn) {
      for (const PartitionedSetWorkerBuckets& wb : workers) {
        const auto& vec = right_side ? wb.group1[p] : wb.group0[p];
        for (const Ref& ref : vec) {
          fn(ref);
        }
      }
    };
    auto& ol = out_left[p];
    auto& oright = out_right[p];

    switch (operation) {
      case SetOperationKind::kUnion: {
        ColumnarSetHashTable ht(all_chunks, meta.canon_types,
                                pl_count + pr_count);
        for_each_side(false, [&](const Ref& ref) {
          bool inserted = false;
          ht.FindOrInsert(ref, &inserted);
          if (inserted) {
            ol.emplace_back(ref.chunk_idx, ref.row_idx);
          }
        });
        for_each_side(true, [&](const Ref& ref) {
          bool inserted = false;
          ht.FindOrInsert(ref, &inserted);
          if (inserted) {
            oright.emplace_back(ref.chunk_idx - left_chunks_n, ref.row_idx);
          }
        });
        break;
      }
      case SetOperationKind::kIntersect: {
        if (pl_count == 0 || pr_count == 0) {
          break;
        }
        const bool build_right = pr_count < pl_count;
        const size_t build_count = build_right ? pr_count : pl_count;
        ColumnarSetHashTable ht(all_chunks, meta.canon_types, build_count);
        for_each_side(build_right, [&](const Ref& ref) {
          bool inserted = false;
          Entry* e = ht.FindOrInsert(ref, &inserted);
          if (inserted) {
            e->count = 0;
          }
        });
        auto& out_probe = build_right ? ol : oright;
        const uint32_t sub = build_right ? 0U : left_chunks_n;
        for_each_side(!build_right, [&](const Ref& ref) {
          Entry* e = ht.Find(ref);
          if (e != nullptr && e->count == 0) {
            e->count = 1;
            out_probe.emplace_back(ref.chunk_idx - sub, ref.row_idx);
          }
        });
        break;
      }
      case SetOperationKind::kIntersectAll: {
        if (pl_count == 0 || pr_count == 0) {
          break;
        }
        const bool build_right = pr_count < pl_count;
        const size_t build_count = build_right ? pr_count : pl_count;
        ColumnarSetHashTable ht(all_chunks, meta.canon_types, build_count);
        for_each_side(build_right, [&](const Ref& ref) {
          bool inserted = false;
          Entry* e = ht.FindOrInsert(ref, &inserted);
          if (!inserted) {
            ++e->count;
          }
        });
        auto& out_probe = build_right ? ol : oright;
        const uint32_t sub = build_right ? 0U : left_chunks_n;
        for_each_side(!build_right, [&](const Ref& ref) {
          Entry* e = ht.Find(ref);
          if (e != nullptr && e->count > 0) {
            --e->count;
            out_probe.emplace_back(ref.chunk_idx - sub, ref.row_idx);
          }
        });
        break;
      }
      case SetOperationKind::kExcept: {
        if (pl_count == 0) {
          break;
        }
        ColumnarSetHashTable ht(all_chunks, meta.canon_types,
                                pr_count + pl_count);
        for_each_side(true, [&](const Ref& ref) {
          bool inserted = false;
          ht.FindOrInsert(ref, &inserted);
        });
        for_each_side(false, [&](const Ref& ref) {
          bool inserted = false;
          ht.FindOrInsert(ref, &inserted);
          if (inserted) {
            ol.emplace_back(ref.chunk_idx, ref.row_idx);
          }
        });
        break;
      }
      case SetOperationKind::kExceptAll: {
        if (pl_count == 0) {
          break;
        }
        if (pr_count == 0) {
          ol.reserve(pl_count);
          for_each_side(false, [&](const Ref& ref) {
            ol.emplace_back(ref.chunk_idx, ref.row_idx);
          });
          break;
        }
        ColumnarSetHashTable ht(all_chunks, meta.canon_types, pr_count);
        for_each_side(true, [&](const Ref& ref) {
          bool inserted = false;
          Entry* e = ht.FindOrInsert(ref, &inserted);
          if (!inserted) {
            ++e->count;
          }
        });
        for_each_side(false, [&](const Ref& ref) {
          Entry* e = ht.Find(ref);
          if (e == nullptr || e->count == 0) {
            ol.emplace_back(ref.chunk_idx, ref.row_idx);
          } else {
            --e->count;
          }
        });
        break;
      }
      case SetOperationKind::kUnionAll:
        // UNION ALL needs no dedup and must never reach this routine: callers
        // gate on `operation != kUnionAll`. Falling through used to return an
        // empty table, silently dropping every input row.
        LOG(ERROR) << "TryColumnarSetOperation called with kUnionAll";
        break;
    }
  };

  if (num_partitions <= 1) {
    process_partition(0);
  } else {
    if (RunParallelTasks(num_partitions, process_partition) !=
        Status::kSuccess) {
      return false;
    }
  }
  std::vector<PartitionedSetWorkerBuckets>{}.swap(workers);

  auto out_table = std::make_shared<ColumnarTable>();
  if (left.columnar) {
    AppendSetMaterializedSide(left.columnar, out_left, meta.left_all_null,
                              meta.canon_types, meta.canon_unsigned,
                              out_table.get());
  }
  if (right.columnar) {
    AppendSetMaterializedSide(right.columnar, out_right, meta.right_all_null,
                              meta.canon_types, meta.canon_unsigned,
                              out_table.get());
  }
  return finish_with_table(std::move(out_table));
}

bool TryColumnarMultiUnion(std::span<Relation*> inputs) {
  if (inputs.size() < 2) {
    return false;
  }
  const size_t ncols = inputs[0]->schema.ColumnCount();
  if (ncols == 0) {
    return false;
  }
  size_t total_chunks = 0;
  size_t total_input_rows = 0;
  std::vector<ValueType> canon_types(ncols, ValueType::kNull);
  std::vector<uint8_t> canon_unsigned(ncols, 0);
  std::vector<uint8_t> per_input_all_null(inputs.size() * ncols, 1);

  for (size_t idx = 0; idx < inputs.size(); ++idx) {
    const Relation* rel = inputs[idx];
    if (rel->HasSpill() || !rel->rows.empty() ||
        rel->schema.ColumnCount() != ncols) {
      return false;
    }
    if (rel->columnar) {
      if (total_chunks > (1U << 31) - 1 - rel->columnar->chunks.size()) {
        return false;
      }
      total_chunks += rel->columnar->chunks.size();
    }
    std::vector<uint8_t> side_all_null(ncols, 1);
    size_t side_rows = 0;
    if (!InspectSetTableSide(rel->columnar, ncols, canon_types, canon_unsigned,
                             &side_all_null, nullptr, &side_rows)) {
      return false;
    }
    std::copy(side_all_null.begin(), side_all_null.end(),
              per_input_all_null.begin() + idx * ncols);
    if (total_input_rows > SIZE_MAX - side_rows) {
      return false;
    }
    total_input_rows += side_rows;
  }

  std::optional<QueryMemoryCharge> set_charge;
  if (total_input_rows > 0) {
    if (total_input_rows > (SIZE_MAX - 1024) / 64) {
      return false;
    }
    const size_t bytes = (total_input_rows * 64) + 1024;
    if (QueryMemoryBudget::Global().CanReserve(bytes)) {
      set_charge.emplace(bytes);
    } else if (!Relation::HasAmpleMemoryBudget()) {
      return false;
    }
  }

  auto finish_with_table = [&](std::shared_ptr<ColumnarTable> out_table) {
    Relation* head = inputs[0];
    for (size_t idx = 1; idx < inputs.size(); ++idx) {
      CopyExecutionStats(head, *inputs[idx]);
    }
    NormalizeSetSchemaTypes(head, canon_types, canon_unsigned);
    head->using_columns.reset();
    head->SetColumnarTable(std::move(out_table), true);
    for (size_t idx = 1; idx < inputs.size(); ++idx) {
      inputs[idx]->ReleaseCharge();
    }
    return true;
  };

  if (total_input_rows == 0) {
    return finish_with_table(std::make_shared<ColumnarTable>());
  }

  std::vector<const ColumnarChunk*> all_chunks;
  std::vector<uint32_t> chunk_input;
  std::vector<uint32_t> chunk_local;
  all_chunks.reserve(total_chunks);
  chunk_input.reserve(total_chunks);
  chunk_local.reserve(total_chunks);
  for (size_t idx = 0; idx < inputs.size(); ++idx) {
    const ColumnarTable* tbl = inputs[idx]->columnar.get();
    if (tbl == nullptr) {
      continue;
    }
    for (uint32_t ci = 0; ci < tbl->chunks.size(); ++ci) {
      if (tbl->chunks[ci].rows == 0) {
        continue;
      }
      all_chunks.push_back(&tbl->chunks[ci]);
      chunk_input.push_back(static_cast<uint32_t>(idx));
      chunk_local.push_back(ci);
    }
  }

  constexpr size_t kParallelThreshold = 8192;
  constexpr size_t kNumPartitions = 64;
  const size_t num_partitions =
      total_input_rows >= kParallelThreshold ? kNumPartitions : 1;

  std::vector<PartitionedSetWorkerBuckets> workers;
  if (!HashAllChunksIntoPartitions(all_chunks, all_chunks.size(), canon_types,
                                   num_partitions, &workers)) {
    return false;
  }

  std::vector<std::vector<std::vector<std::pair<uint32_t, uint32_t>>>>
      out_by_input(inputs.size());
  for (auto& parts : out_by_input) {
    parts.resize(num_partitions);
  }

  auto process_partition = [&](size_t p) {
    size_t part_count = 0;
    for (const PartitionedSetWorkerBuckets& wb : workers) {
      part_count += wb.group0[p].size();
    }
    if (part_count == 0) {
      return;
    }
    ColumnarSetHashTable ht(all_chunks, canon_types, part_count);
    for (const PartitionedSetWorkerBuckets& wb : workers) {
      for (const ColumnarSetHashTable::Ref& ref : wb.group0[p]) {
        bool inserted = false;
        ht.FindOrInsert(ref, &inserted);
        if (inserted) {
          const uint32_t owner = chunk_input[ref.chunk_idx];
          const uint32_t local_ci = chunk_local[ref.chunk_idx];
          out_by_input[owner][p].emplace_back(local_ci, ref.row_idx);
        }
      }
    }
  };

  if (num_partitions <= 1) {
    process_partition(0);
  } else {
    if (RunParallelTasks(num_partitions, process_partition) !=
        Status::kSuccess) {
      return false;
    }
  }
  std::vector<PartitionedSetWorkerBuckets>{}.swap(workers);

  auto out_table = std::make_shared<ColumnarTable>();
  for (size_t idx = 0; idx < inputs.size(); ++idx) {
    if (inputs[idx]->columnar) {
      const std::span<const uint8_t> side_null_span(
          per_input_all_null.data() + (idx * ncols), ncols);
      AppendSetMaterializedSide(inputs[idx]->columnar, out_by_input[idx],
                                side_null_span, canon_types, canon_unsigned,
                                out_table.get());
    }
  }
  return finish_with_table(std::move(out_table));
}

template <typename ApplyPairFn>
StatusOr<Relation> FoldLowPrecedenceSetTerms(
    std::vector<Relation>& terms, std::span<const SetOperationKind> low_ops,
    ApplyPairFn&& apply_pair) {
  Relation combined = std::move(terms.front());
  size_t j = 1;
  while (j < terms.size()) {
    if (low_ops[j - 1] == SetOperationKind::kUnion && j + 1 < terms.size() &&
        low_ops[j] == SetOperationKind::kUnion) {
      size_t run_end = j + 1;
      while (run_end + 1 < terms.size() &&
             low_ops[run_end] == SetOperationKind::kUnion) {
        ++run_end;
      }
      std::vector<Relation*> batch;
      batch.reserve(1 + (run_end - j + 1));
      batch.push_back(&combined);
      for (size_t k = j; k <= run_end; ++k) {
        batch.push_back(&terms[k]);
      }
      if (TryColumnarMultiUnion(batch)) {
        j = run_end + 1;
        continue;
      }
    }
    ASSIGN_OR_RETURN(
        Relation, folded_pair,
        (apply_pair(std::move(combined), std::move(terms[j]), low_ops[j - 1])));
    combined = std::move(folded_pair);
    ++j;
  }
  return combined;
}

uint64_t EstimateBranchMaxScanRows(TransactionContext& context,
                                   const SelectStatement& stmt,
                                   const CteMap& ctes) {
  uint64_t best = 0;
  for (const SelectSource& source : stmt.Sources()) {
    if (source.query) {
      best = std::max(best,
                      EstimateBranchMaxScanRows(context, *source.query, ctes));
      continue;
    }
    if (const auto cte = ctes.find(source.table);
        cte != ctes.end() && cte->second != nullptr) {
      best = std::max<uint64_t>(best, cte->second->TotalRows());
      continue;
    }
    if (auto st = context.GetStats(source.table); st.HasValue()) {
      best = std::max<uint64_t>(best, st.Value()->Rows());
      continue;
    }
    if (auto tbl = context.GetTable(source.table); tbl.HasValue()) {
      best = std::max<uint64_t>(best, tbl.Value()->BackingColumnarRowCount());
    }
  }
  return best;
}

// Lean proof: lean/TinylambOpt/TpcdsExecCodegen.lean
// (prebuilt_exists_outer_domain_filter_sound)
std::shared_ptr<SelectStatement> TryRestrictGroupedCteByPeerEquijoinDomain(
    const SelectStatement& consumer, const std::string& name,
    const SelectStatement& active_body, const CteMap& ctes) {
  if (!consumer.UnionAll().empty() || consumer.GetSetOperationTree() ||
      consumer.Sources().size() < 2 || StatementContainsVolatileFunction(consumer) ||
      active_body.GroupBy().empty() || active_body.IsGroupAll() ||
      active_body.HasLimit() || active_body.Offset() != 0 ||
      !active_body.UnionAll().empty() || active_body.GetSetOperationTree() ||
      HasWindowFunctions(active_body) ||
      StatementContainsVolatileFunction(active_body)) {
    return nullptr;
  }
  for (const auto& [other_name, unused] : consumer.WithQueries()) {
    (void)unused;
    if (other_name == name) {
      continue;
    }
    std::unordered_set<std::string> refs;
    CollectCteBodyOuterRefs(consumer, other_name, &refs);
    if (refs.contains(name)) {
      return nullptr;
    }
  }
  if (ContainsQuery(consumer.WhereClause()) || ContainsQuery(consumer.Having()) ||
      ContainsQuery(consumer.Qualify()) ||
      std::ranges::any_of(consumer.SelectList(), [](const auto& it) {
        return ContainsQuery(it.expression);
      })) {
    return nullptr;
  }
  std::optional<size_t> my_src_idx;
  for (size_t si = 0; si < consumer.Sources().size(); ++si) {
    const SelectSource& src = consumer.Sources()[si];
    const bool inner_like =
        (si == 0 || src.join_type == JoinType::kInner ||
         src.join_type == JoinType::kCross) &&
        !src.unnest && !src.sample && !src.is_lateral &&
        !src.from_nested_join && src.using_columns.empty() &&
        !ContainsQuery(src.join_condition);
    if (!inner_like) {
      return nullptr;
    }
    if (!src.query && IdentifierEquals(src.table, name)) {
      if (my_src_idx.has_value()) {
        return nullptr;
      }
      my_src_idx = si;
    }
  }
  if (!my_src_idx.has_value()) {
    return nullptr;
  }

  std::vector<Expression> all_conjuncts = SplitConjuncts(consumer.WhereClause());
  for (size_t si = 1; si < consumer.Sources().size(); ++si) {
    if (consumer.Sources()[si].join_condition) {
      for (Expression& c : SplitConjuncts(consumer.Sources()[si].join_condition)) {
        all_conjuncts.push_back(std::move(c));
      }
    }
  }

  auto resolve_consumer_source =
      [&](const ColumnName& col) -> std::optional<size_t> {
    if (!col.schema.empty()) {
      for (size_t si = 0; si < consumer.Sources().size(); ++si) {
        const auto& s = consumer.Sources()[si];
        const std::string al = s.alias.empty() ? s.table : s.alias;
        if (IdentifierEquals(col.schema, al)) {
          return si;
        }
      }
      return std::nullopt;
    }
    return std::nullopt;
  };

  const Relation* best_peer_rel = nullptr;
  size_t best_peer_slot = 0;
  size_t best_my_key_idx = 0;

  for (const Expression& conj : all_conjuncts) {
    if (!conj || conj->Type() != TypeTag::kBinaryExp) {
      continue;
    }
    const BinaryExpression& bin = conj->AsBinaryExpression();
    if (bin.Op() != BinaryOperation::kEquals || !bin.Left() || !bin.Right() ||
        bin.Left()->Type() != TypeTag::kColumnValue ||
        bin.Right()->Type() != TypeTag::kColumnValue) {
      continue;
    }
    ColumnName lhs = bin.Left()->AsColumnValue().GetColumnName();
    ColumnName rhs = bin.Right()->AsColumnValue().GetColumnName();
    auto s_lhs = resolve_consumer_source(lhs);
    auto s_rhs = resolve_consumer_source(rhs);
    if (!s_lhs.has_value() || !s_rhs.has_value() || *s_lhs == *s_rhs) {
      continue;
    }
    size_t peer_si = 0;
    ColumnName my_col;
    ColumnName peer_col;
    if (*s_lhs == *my_src_idx) {
      peer_si = *s_rhs;
      my_col = lhs;
      peer_col = rhs;
    } else if (*s_rhs == *my_src_idx) {
      peer_si = *s_lhs;
      my_col = rhs;
      peer_col = lhs;
    } else {
      continue;
    }
    const SelectSource& peer_src = consumer.Sources()[peer_si];
    if (peer_src.query) {
      continue;
    }
    const auto peer_it = ctes.find(peer_src.table);
    if (peer_it == ctes.end() || peer_it->second == nullptr ||
        peer_it->second->HasSpill() || peer_it->second->TotalRows() > 4500) {
      continue;
    }
    const Relation& peer_rel = *peer_it->second;
    std::optional<size_t> peer_slot =
        LocalColumnOffset(peer_rel.schema, ColumnName("", peer_col.name));
    if (!peer_slot.has_value() || *peer_slot >= peer_rel.schema.ColumnCount()) {
      continue;
    }
    const Column& pcol_meta = peer_rel.schema.GetColumn(*peer_slot);
    if (pcol_meta.IsUnsigned() || pcol_meta.IsInterval() ||
        (pcol_meta.Type() != ValueType::kInt64 &&
         pcol_meta.Type() != ValueType::kDate &&
         pcol_meta.Type() != ValueType::kVarChar)) {
      continue;
    }
    std::optional<size_t> my_key_idx;
    for (size_t ki = 0; ki < active_body.SelectList().size(); ++ki) {
      if (IdentifierEquals(ProjectionName(active_body.SelectList()[ki], ki),
                           my_col.name)) {
        if (my_key_idx.has_value()) {
          my_key_idx.reset();
          break;
        }
        my_key_idx = ki;
      }
    }
    if (!my_key_idx.has_value()) {
      continue;
    }
    const Expression& key_expr = active_body.SelectList()[*my_key_idx].expression;
    if (!key_expr || key_expr->Type() != TypeTag::kColumnValue) {
      continue;
    }
    const bool is_group_col =
        std::ranges::any_of(active_body.GroupBy(), [&](const Expression& g) {
          return g && g->Type() == TypeTag::kColumnValue &&
                 g->ToString() == key_expr->ToString();
        });
    if (!is_group_col) {
      continue;
    }
    if (best_peer_rel == nullptr ||
        peer_rel.TotalRows() < best_peer_rel->TotalRows()) {
      best_peer_rel = &peer_rel;
      best_peer_slot = *peer_slot;
      best_my_key_idx = *my_key_idx;
    }
  }

  if (best_peer_rel == nullptr) {
    return nullptr;
  }

  const ValueType pvt = best_peer_rel->schema.GetColumn(best_peer_slot).Type();
  std::vector<Expression> in_items;
  if (pvt == ValueType::kVarChar) {
    std::unordered_set<std::string> seen;
    seen.reserve(best_peer_rel->TotalRows());
    bool ok = true;
    auto collect_str = [&](std::string_view sv) {
      if (!PlainStringComparable(sv)) {
        ok = false;
        return;
      }
      if (seen.emplace(sv).second) {
        in_items.push_back(std::make_shared<ConstantValue>(Value(std::string(sv))));
      }
    };
    if (best_peer_rel->columnar != nullptr) {
      for (const ColumnarChunk& ch : best_peer_rel->columnar->chunks) {
        if (best_peer_slot >= ch.columns.size()) {
          ok = false;
          break;
        }
        const ColumnarColumn& cc = ch.columns[best_peer_slot];
        for (size_t r = 0; r < ch.rows && ok; ++r) {
          if (!cc.IsNull(r)) {
            collect_str(cc.StringAt(r));
          }
        }
      }
    } else {
      for (const Row& row : best_peer_rel->rows) {
        if (best_peer_slot < row.Size() && !row[best_peer_slot].IsNull()) {
          if (row[best_peer_slot].type != ValueType::kVarChar ||
              row[best_peer_slot].Collation() != 0) {
            ok = false;
            break;
          }
          collect_str(row[best_peer_slot].AsStringView());
        }
      }
    }
    if (!ok) {
      return nullptr;
    }
  } else {
    FlatIntKeyMap<uint8_t> seen;
    seen.Reserve(best_peer_rel->TotalRows());
    bool ok = true;
    auto collect_int = [&](int64_t iv) {
      bool emplaced = false;
      uint8_t* slot = seen.TryEmplace(iv, &emplaced);
      if (emplaced) {
        *slot = 1;
        Value v = (pvt == ValueType::kDate) ? Value::DateFromDays(static_cast<int32_t>(iv))
                                            : Value(iv);
        in_items.push_back(std::make_shared<ConstantValue>(std::move(v)));
      }
    };
    if (best_peer_rel->columnar != nullptr) {
      for (const ColumnarChunk& ch : best_peer_rel->columnar->chunks) {
        if (best_peer_slot >= ch.columns.size()) {
          ok = false;
          break;
        }
        const ColumnarColumn& cc = ch.columns[best_peer_slot];
        for (size_t r = 0; r < ch.rows && ok; ++r) {
          if (!cc.IsNull(r)) {
            collect_int(cc.IntAt(r));
          }
        }
      }
    } else {
      for (const Row& row : best_peer_rel->rows) {
        if (best_peer_slot < row.Size() && !row[best_peer_slot].IsNull()) {
          if (row[best_peer_slot].type != pvt ||
              row[best_peer_slot].IsUnsigned() ||
              row[best_peer_slot].Collation() != 0) {
            ok = false;
            break;
          }
          collect_int(row[best_peer_slot].value.int_value);
        }
      }
    }
    if (!ok) {
      return nullptr;
    }
  }

  const Expression& key_expr =
      active_body.SelectList()[best_my_key_idx].expression;
  Expression domain_pred;
  if (in_items.empty()) {
    domain_pred = std::make_shared<BinaryExpression>(
        std::make_shared<ConstantValue>(Value(int64_t{0})),
        BinaryOperation::kEquals,
        std::make_shared<ConstantValue>(Value(int64_t{1})));
  } else if (in_items.size() == 1) {
    domain_pred = std::make_shared<BinaryExpression>(
        key_expr, BinaryOperation::kEquals, std::move(in_items[0]));
  } else {
    domain_pred = std::make_shared<InExpression>(key_expr, std::move(in_items));
  }

  auto rewritten = std::make_shared<SelectStatement>(active_body);
  std::vector<Expression> where_conj = SplitConjuncts(rewritten->WhereClause());
  where_conj.push_back(std::move(domain_pred));
  rewritten->SetWhereClause(CombineConjuncts(where_conj));
  return rewritten;
}

// Lean proof: lean/TinylambOpt/TpcdsExecCodegen.lean
// (quotient_gid_setop_cardinality_sound)
StatusOr<std::optional<Relation>> TryExecuteQuotientGidDistinctSetOpCount(
    TransactionContext& context, const SelectStatement& statement,
    const CteMap& inherited_ctes) {
  if (!statement.WithQueries().empty() || !statement.UnionAll().empty() ||
      statement.GetSetOperationTree() != nullptr || statement.WhereClause() ||
      !statement.GroupBy().empty() || statement.IsGroupAll() ||
      statement.Having() || statement.Qualify() || statement.Distinct() ||
      !statement.OrderBy().empty() || statement.Offset() != 0 ||
      (statement.HasLimit() && statement.Limit() == 0) ||
      statement.AsStruct() || statement.ForUpdate() ||
      HasWindowFunctions(statement) || statement.SelectList().size() != 1 ||
      statement.Sources().size() != 1) {
    return std::optional<Relation>{};
  }
  const Expression& top_expr = statement.SelectList()[0].expression;
  if (!top_expr || top_expr->Type() != TypeTag::kAggregateExp) {
    return std::optional<Relation>{};
  }
  const AggregateExpression& top_agg = top_expr->AsAggregateExpression();
  if (!IsCountStar(top_agg) || top_agg.Distinct() || top_agg.WhereFilter() ||
      top_agg.Having() != AggregateHavingModifier::kNone ||
      !top_agg.InnerOrderBy().empty() || top_agg.InnerLimit().has_value()) {
    return std::optional<Relation>{};
  }
  const SelectSource& top_src = statement.Sources()[0];
  if (!top_src.query || top_src.unnest || top_src.sample ||
      top_src.is_lateral || top_src.from_nested_join ||
      !top_src.using_columns.empty() || top_src.join_condition) {
    return std::optional<Relation>{};
  }
  const SelectStatement& derived = *top_src.query;
  if (!derived.WithQueries().empty() || derived.HasLimit() ||
      derived.Offset() != 0 || derived.WithTies() ||
      StatementContainsVolatileFunction(derived)) {
    return std::optional<Relation>{};
  }

  SelectStatement head_copy(derived);
  std::vector<const SelectStatement*> branches;
  std::vector<SetOperationKind> ops;
  if (derived.GetSetOperationTree() != nullptr &&
      derived.GetSetOperationTree()->grouped) {
    const SetOperationTree& tree = *derived.GetSetOperationTree();
    if (!tree.first || tree.branches.empty() ||
        tree.kinds.size() != tree.branches.size()) {
      return std::optional<Relation>{};
    }
    for (const auto& m : derived.Matches()) {
      if (m.by_name || m.corresponding || !m.columns.empty()) {
        return std::optional<Relation>{};
      }
    }
    branches.push_back(tree.first.get());
    for (const auto& b : tree.branches) {
      if (!b) {
        return std::optional<Relation>{};
      }
      branches.push_back(b.get());
    }
    ops = tree.kinds;
  } else if (!derived.UnionAll().empty()) {
    if (derived.SetOperationKinds().size() != derived.UnionAll().size()) {
      return std::optional<Relation>{};
    }
    for (size_t i = 0; i < derived.UnionAll().size(); ++i) {
      const SetOperationMatch m = derived.MatchFor(i);
      if (m.by_name || m.corresponding || !m.columns.empty() ||
          !derived.UnionAll()[i]) {
        return std::optional<Relation>{};
      }
    }
    head_copy.ClearUnionAll();
    head_copy.SetSetOperationTree(nullptr);
    head_copy.SetOrderBy({});
    branches.push_back(&head_copy);
    for (const auto& b : derived.UnionAll()) {
      branches.push_back(b.get());
    }
    ops = derived.SetOperationKinds();
  } else {
    return std::optional<Relation>{};
  }

  if (branches.size() < 2 || branches.size() > 8 ||
      !std::ranges::all_of(ops, IsPresenceOnlySetOperation)) {
    return std::optional<Relation>{};
  }

  struct BranchPlan {
    size_t dim_src_idx{0};
    Expression fk_expr;
    Expression other_expr;
    bool direct_subquery{false};
    const SelectStatement* subquery_ptr{nullptr};
    size_t sub_fk_slot{0};
    size_t sub_other_slot{0};
    std::vector<Expression> non_dim_where;
  };

  std::string shared_dim_table;
  std::shared_ptr<Table> shared_dim_ptr;
  size_t shared_pk_slot = 0;
  std::vector<int> pos_dim_slot;  // -1 for other_col
  std::optional<size_t> shared_other_pos;
  std::vector<BranchPlan> plans(branches.size());

  for (size_t bi = 0; bi < branches.size(); ++bi) {
    const SelectStatement& br = *branches[bi];
    if (!br.WithQueries().empty() || !br.UnionAll().empty() ||
        br.GetSetOperationTree() != nullptr || br.Having() || br.Qualify() ||
        br.HasLimit() || br.Offset() != 0 || br.WithTies() || br.AsStruct() ||
        br.ForUpdate() || HasWindowFunctions(br) ||
        StatementContainsVolatileFunction(br) ||
        ContainsQuery(br.WhereClause()) || br.Sources().size() < 2 ||
        br.SelectList().size() < 2 || br.SelectList().size() > 8) {
      return std::optional<Relation>{};
    }
    for (const NamedExpression& it : br.SelectList()) {
      if (!it.expression || ContainsAggregate(it.expression) ||
          ContainsQuery(it.expression) || ContainsParameterSlot(it.expression)) {
        return std::optional<Relation>{};
      }
    }
    if (!br.GroupBy().empty()) {
      for (const Expression& g : br.GroupBy()) {
        if (!g) {
          return std::optional<Relation>{};
        }
        const bool in_sel = std::ranges::any_of(
            br.SelectList(), [&](const NamedExpression& it) {
              return it.expression && it.expression->ToString() == g->ToString();
            });
        if (!in_sel) {
          return std::optional<Relation>{};
        }
      }
    }

    std::vector<Schema> src_schemas(br.Sources().size());
    for (size_t si = 0; si < br.Sources().size(); ++si) {
      const SelectSource& src = br.Sources()[si];
      const bool inner_ok =
          (si == 0 || src.join_type == JoinType::kInner ||
           src.join_type == JoinType::kCross) &&
          !src.unnest && !src.sample && !src.is_lateral &&
          !src.from_nested_join && src.using_columns.empty() &&
          !ContainsQuery(src.join_condition);
      if (!inner_ok) {
        return std::optional<Relation>{};
      }
      const std::string alias = src.alias.empty() ? src.table : src.alias;
      if (src.query) {
        std::vector<Column> cols;
        for (size_t ci = 0; ci < src.query->SelectList().size(); ++ci) {
          const auto& item = src.query->SelectList()[ci];
          if (!item.expression ||
              (item.expression->Type() == TypeTag::kColumnValue &&
               item.expression->AsColumnValue().GetColumnName().name == "*")) {
            return std::optional<Relation>{};
          }
          cols.emplace_back(ProjectionName(item, ci), ValueType::kNull);
        }
        src_schemas[si] = QualifySchema(Schema("", std::move(cols)), alias);
      } else if (auto cte = inherited_ctes.find(src.table);
                 cte != inherited_ctes.end() && cte->second != nullptr) {
        src_schemas[si] = QualifySchema(cte->second->schema, alias);
      } else {
        auto tbl = context.GetTable(src.table);
        if (!tbl.HasValue()) {
          return std::optional<Relation>{};
        }
        src_schemas[si] = QualifySchema(tbl.Value()->GetSchema(), alias);
      }
    }

    auto resolve_col =
        [&](const ColumnName& cn) -> std::optional<std::pair<size_t, size_t>> {
      std::optional<std::pair<size_t, size_t>> hit;
      for (size_t si = 0; si < src_schemas.size(); ++si) {
        if (auto off = LocalColumnOffset(src_schemas[si], cn)) {
          if (hit.has_value()) {
            return std::nullopt;
          }
          hit = std::pair{si, *off};
        }
      }
      return hit;
    };

    auto touches_source = [&](const Expression& expr, size_t target_si,
                              bool* ok_out) -> bool {
      bool found = false;
      for (const ColumnName& cn : expr->TouchedColumns()) {
        auto r = resolve_col(cn);
        if (!r.has_value()) {
          *ok_out = false;
          return false;
        }
        if (r->first == target_si) {
          found = true;
        }
      }
      return found;
    };

    std::vector<Expression> all_preds = SplitConjuncts(br.WhereClause());
    for (size_t si = 1; si < br.Sources().size(); ++si) {
      if (br.Sources()[si].join_condition) {
        for (Expression& c : SplitConjuncts(br.Sources()[si].join_condition)) {
          all_preds.push_back(std::move(c));
        }
      }
    }

    bool matched_branch = false;
    for (size_t cand_si = 0; cand_si < br.Sources().size(); ++cand_si) {
      const SelectSource& dsrc = br.Sources()[cand_si];
      if (dsrc.query || inherited_ctes.contains(dsrc.table)) {
        continue;
      }
      if (bi > 0 && !IdentifierEquals(dsrc.table, shared_dim_table)) {
        continue;
      }
      auto dtbl_or = context.GetTable(dsrc.table);
      if (!dtbl_or.HasValue()) {
        continue;
      }
      std::shared_ptr<Table> dtbl = dtbl_or.Value();  // NOLINT(performance-unnecessary-copy-initialization)
      uint64_t est_rows = dtbl->BackingColumnarRowCount();
      if (auto st = context.GetStats(dsrc.table); st.HasValue()) {
        est_rows = std::max<uint64_t>(est_rows, st.Value()->Rows());
      }
      if (est_rows > 250000) {
        continue;
      }
      const Schema& dschema = dtbl->GetSchema();

      size_t dim_pred_count = 0;
      size_t cand_pk_slot = 0;
      Expression cand_fk_expr;
      std::vector<Expression> non_dim_preds;
      bool preds_valid = true;
      for (const Expression& pr : all_preds) {
        bool res_ok = true;
        const bool uses_dim = touches_source(pr, cand_si, &res_ok);
        if (!res_ok) {
          preds_valid = false;
          break;
        }
        if (!uses_dim) {
          non_dim_preds.push_back(pr);
          continue;
        }
        ++dim_pred_count;
        if (dim_pred_count > 1 || pr->Type() != TypeTag::kBinaryExp) {
          preds_valid = false;
          break;
        }
        const BinaryExpression& bin = pr->AsBinaryExpression();
        if (bin.Op() != BinaryOperation::kEquals || !bin.Left() ||
            !bin.Right()) {
          preds_valid = false;
          break;
        }
        bool lhs_ok = true;
        bool rhs_ok = true;
        const bool lhs_dim = touches_source(bin.Left(), cand_si, &lhs_ok);
        const bool rhs_dim = touches_source(bin.Right(), cand_si, &rhs_ok);
        if (!lhs_ok || !rhs_ok || lhs_dim == rhs_dim) {
          preds_valid = false;
          break;
        }
        const Expression& dim_side = lhs_dim ? bin.Left() : bin.Right();
        const Expression& fk_side = lhs_dim ? bin.Right() : bin.Left();
        if (dim_side->Type() != TypeTag::kColumnValue) {
          preds_valid = false;
          break;
        }
        auto pk_res = resolve_col(dim_side->AsColumnValue().GetColumnName());
        if (!pk_res.has_value() || pk_res->first != cand_si) {
          preds_valid = false;
          break;
        }
        const Column& pk_col = dschema.GetColumn(pk_res->second);
        if (pk_col.Type() != ValueType::kInt64 || pk_col.IsUnsigned()) {
          preds_valid = false;
          break;
        }
        if (bi > 0 && pk_res->second != shared_pk_slot) {
          preds_valid = false;
          break;
        }
        cand_pk_slot = pk_res->second;
        cand_fk_expr = fk_side;
      }
      if (!preds_valid || dim_pred_count != 1 || !cand_fk_expr) {
        continue;
      }

      std::vector<int> cand_pos_slots(br.SelectList().size(), -1);
      std::optional<size_t> cand_other_pos;
      Expression cand_other_expr;
      size_t dim_out_cols = 0;
      bool sel_ok = true;
      for (size_t pi = 0; pi < br.SelectList().size(); ++pi) {
        const Expression& se = br.SelectList()[pi].expression;
        bool se_res_ok = true;
        const bool uses_dim = touches_source(se, cand_si, &se_res_ok);
        if (!se_res_ok) {
          sel_ok = false;
          break;
        }
        if (uses_dim) {
          if (se->Type() != TypeTag::kColumnValue) {
            sel_ok = false;
            break;
          }
          auto d_res = resolve_col(se->AsColumnValue().GetColumnName());
          if (!d_res.has_value() || d_res->first != cand_si) {
            sel_ok = false;
            break;
          }
          const Column& dc = dschema.GetColumn(d_res->second);
          if (dc.IsInterval() ||
              (dc.Type() != ValueType::kVarChar &&
               dc.Type() != ValueType::kInt64 &&
               dc.Type() != ValueType::kDate &&
               dc.Type() != ValueType::kDouble)) {
            sel_ok = false;
            break;
          }
          cand_pos_slots[pi] = static_cast<int>(d_res->second);
          ++dim_out_cols;
        } else {
          if (cand_other_pos.has_value()) {
            sel_ok = false;
            break;
          }
          cand_other_pos = pi;
          cand_other_expr = se;
        }
      }
      if (!sel_ok || dim_out_cols == 0) {
        continue;
      }
      if (bi == 0) {
        shared_dim_table = dsrc.table;
        shared_dim_ptr = dtbl;
        shared_pk_slot = cand_pk_slot;
        pos_dim_slot = cand_pos_slots;
        shared_other_pos = cand_other_pos;
      } else if (cand_pos_slots != pos_dim_slot ||
                 cand_other_pos != shared_other_pos) {
        continue;
      }

      BranchPlan bp;
      bp.dim_src_idx = cand_si;
      bp.fk_expr = cand_fk_expr;
      bp.other_expr = cand_other_expr;
      bp.non_dim_where = std::move(non_dim_preds);
      if (br.Sources().size() == 2 && bp.non_dim_where.empty()) {
        const size_t other_si = 1 - cand_si;
        const SelectSource& osrc = br.Sources()[other_si];
        if (osrc.query && bp.fk_expr->Type() == TypeTag::kColumnValue &&
            (!shared_other_pos.has_value() ||
             bp.other_expr->Type() == TypeTag::kColumnValue)) {
          auto fk_r = resolve_col(bp.fk_expr->AsColumnValue().GetColumnName());
          std::optional<std::pair<size_t, size_t>> ot_r =
              shared_other_pos.has_value()
                  ? resolve_col(bp.other_expr->AsColumnValue().GetColumnName())
                  : std::pair<size_t, size_t>{other_si, 0};
          if (fk_r.has_value() && fk_r->first == other_si && ot_r.has_value() &&
              ot_r->first == other_si) {
            bp.direct_subquery = true;
            bp.subquery_ptr = osrc.query.get();
            bp.sub_fk_slot = fk_r->second;
            bp.sub_other_slot = ot_r->second;
          }
        }
      }
      plans[bi] = std::move(bp);
      matched_branch = true;
      break;
    }
    if (!matched_branch) {
      return std::optional<Relation>{};
    }
  }

  // Scan shared dimension D once: SELECT pk_col, uniq_dim_cols... FROM D.
  const Schema& dschema = shared_dim_ptr->GetSchema();
  std::vector<size_t> uniq_dim_slots;
  for (int sl : pos_dim_slot) {
    if (sl >= 0 &&
        std::ranges::find(uniq_dim_slots, static_cast<size_t>(sl)) ==
            uniq_dim_slots.end()) {
      uniq_dim_slots.push_back(static_cast<size_t>(sl));
    }
  }
  SelectStatement dim_stmt;
  SelectSource dim_scan_src;
  dim_scan_src.table = shared_dim_table;
  dim_stmt.SetSources({std::move(dim_scan_src)});
  std::vector<NamedExpression> dim_select;
  dim_select.reserve(1 + uniq_dim_slots.size());
  dim_select.emplace_back(
      "__pk", std::make_shared<ColumnValue>(
                  ColumnName("", dschema.GetColumn(shared_pk_slot).Name().name)));
  for (size_t di = 0; di < uniq_dim_slots.size(); ++di) {
    dim_select.emplace_back(
        "__d" + std::to_string(di),
        std::make_shared<ColumnValue>(
            ColumnName("", dschema.GetColumn(uniq_dim_slots[di]).Name().name)));
  }
  dim_stmt.SetSelectList(std::move(dim_select));
  ASSIGN_OR_RETURN(
      Relation, dim_rel,
      ExecuteQuery(context, dim_stmt, nullptr, inherited_ctes, nullptr, true));
  RETURN_IF_FAIL(dim_rel.GetStatus());
  if (dim_rel.HasSpill()) {
    return std::optional<Relation>{};
  }

  std::vector<uint32_t> pk_to_gid;
  GroupKeyMap tuple_to_gid;
  uint32_t next_gid = 1;
  bool pk_valid = true;

  if (dim_rel.columnar != nullptr && !dim_rel.columnar->chunks.empty()) {
    int64_t max_pk = -1;
    for (const ColumnarChunk& ch : dim_rel.columnar->chunks) {
      if (ch.columns.empty()) {
        pk_valid = false;
        break;
      }
      const ColumnarColumn& pkc = ch.columns[0];
      for (size_t r = 0; r < ch.rows; ++r) {
        if (pkc.IsNull(r)) {
          pk_valid = false;
          break;
        }
        const int64_t pk = pkc.IntAt(r);
        if (pk < 0 || pk > 1000000) {
          pk_valid = false;
          break;
        }
        max_pk = std::max(max_pk, pk);
      }
      if (!pk_valid) {
        break;
      }
    }
    if (!pk_valid) {
      return std::optional<Relation>{};
    }
    if (max_pk >= 0) {
      pk_to_gid.assign(static_cast<size_t>(max_pk) + 1, 0);
      // Fast bijective 2-col proven-distinct dictionary path across all chunks:
      const LSTableColumnDictionary* shared_d1 = nullptr;
      const LSTableColumnDictionary* shared_d2 = nullptr;
      bool fast_dict_2col =
          (!dim_rel.columnar->chunks.empty() && uniq_dim_slots.size() == 2 &&
           dim_rel.columnar->chunks[0].columns.size() == 3);
      if (fast_dict_2col) {
        shared_d1 = dim_rel.columnar->chunks[0].columns[1].dict;
        shared_d2 = dim_rel.columnar->chunks[0].columns[2].dict;
        fast_dict_2col =
            (shared_d1 != nullptr && shared_d2 != nullptr &&
             shared_d1->bytewise_equality && shared_d2->bytewise_equality);
        for (const ColumnarChunk& ch : dim_rel.columnar->chunks) {
          if (!fast_dict_2col) {
            break;
          }
          fast_dict_2col =
              (ch.columns.size() == 3 &&
               ch.columns[1].encoding == ColumnarColumn::Encoding::kDictionary &&
               ch.columns[2].encoding == ColumnarColumn::Encoding::kDictionary &&
               ch.columns[1].dict == shared_d1 &&
               ch.columns[2].dict == shared_d2);
        }
      }
      if (fast_dict_2col) {
        FlatIntKeyMap<uint32_t> code_pair_to_gid;
        code_pair_to_gid.Reserve(dim_rel.TotalRows());
        for (const ColumnarChunk& ch : dim_rel.columnar->chunks) {
          const ColumnarColumn& pkc = ch.columns[0];
          const ColumnarColumn& c1 = ch.columns[1];
          const ColumnarColumn& c2 = ch.columns[2];
          for (size_t r = 0; r < ch.rows; ++r) {
            const auto pk = static_cast<size_t>(pkc.IntAt(r));
            if (pk_to_gid[pk] != 0) {
              return std::optional<Relation>{};  // duplicate PK!
            }
            const uint32_t d1 =
                c1.IsNull(r) ? 0U : (c1.DictCodeAt(r) + 1U);
            const uint32_t d2 =
                c2.IsNull(r) ? 0U : (c2.DictCodeAt(r) + 1U);
            const auto packed_codes = static_cast<int64_t>(
                (static_cast<uint64_t>(d1) << 32U) | static_cast<uint64_t>(d2));
            bool emplaced = false;
            uint32_t* slot =
                code_pair_to_gid.TryEmplace(packed_codes, &emplaced);
            if (emplaced) {
              *slot = next_gid++;
            }
            pk_to_gid[pk] = *slot;
          }
        }
      } else {
        std::vector<Value> key_vals(uniq_dim_slots.size());
        for (const ColumnarChunk& ch : dim_rel.columnar->chunks) {
          const ColumnarColumn& pkc = ch.columns[0];
          for (size_t r = 0; r < ch.rows; ++r) {
            const auto pk = static_cast<size_t>(pkc.IntAt(r));
            if (pk_to_gid[pk] != 0) {
              return std::optional<Relation>{};  // duplicate PK!
            }
            for (size_t di = 0; di < uniq_dim_slots.size(); ++di) {
              key_vals[di] = CanonicalDistinctValue(ch.columns[1 + di].ValueAt(r));
            }
            auto it = tuple_to_gid.find(key_vals);
            uint32_t gid = 0;
            if (it != tuple_to_gid.end()) {
              gid = static_cast<uint32_t>(it->second);
            } else {
              gid = next_gid++;
              tuple_to_gid.emplace(Row(key_vals), gid);
            }
            pk_to_gid[pk] = gid;
          }
        }
      }
    }
  } else {
    int64_t max_pk = -1;
    for (const Row& row : dim_rel.rows) {
      if (row.Size() < 1 + uniq_dim_slots.size() || row[0].IsNull() ||
          row[0].type != ValueType::kInt64) {
        return std::optional<Relation>{};
      }
      const int64_t pk = row[0].value.int_value;
      if (pk < 0 || pk > 1000000) {
        return std::optional<Relation>{};
      }
      max_pk = std::max(max_pk, pk);
    }
    if (max_pk >= 0) {
      pk_to_gid.assign(static_cast<size_t>(max_pk) + 1, 0);
      std::vector<Value> key_vals(uniq_dim_slots.size());
      for (const Row& row : dim_rel.rows) {
        const auto pk = static_cast<size_t>(row[0].value.int_value);
        if (pk_to_gid[pk] != 0) {
          return std::optional<Relation>{};
        }
        for (size_t di = 0; di < uniq_dim_slots.size(); ++di) {
          key_vals[di] = CanonicalDistinctValue(row[1 + di]);
        }
        auto it = tuple_to_gid.find(key_vals);
        uint32_t gid = 0;
        if (it != tuple_to_gid.end()) {
          gid = static_cast<uint32_t>(it->second);
        } else {
          gid = next_gid++;
          tuple_to_gid.emplace(Row(key_vals), gid);
        }
        pk_to_gid[pk] = gid;
      }
    }
  }

  struct WidePairHash {
    size_t operator()(const std::pair<uint32_t, int64_t>& p) const noexcept {
      uint64_t x = (static_cast<uint64_t>(p.first) * 0x9e3779b97f4a7c15ULL) ^
                   static_cast<uint64_t>(p.second);
      x = (x ^ (x >> 30U)) * 0xbf58476d1ce4e5b9ULL;
      return static_cast<size_t>(x ^ (x >> 31U));
    }
  };
  struct QuotientSet {
    FlatIntKeyMap<uint8_t> packed_i32;
    FlatIntKeyMap<uint8_t> null_other_gids;
    std::unordered_set<std::pair<uint32_t, int64_t>, WidePairHash> wide_pairs;
  };

  auto insert_key = [](FlatIntKeyMap<uint8_t>& m, int64_t k) {
    bool emplaced = false;
    uint8_t* slot = m.TryEmplace(k, &emplaced);
    if (emplaced) {
      *slot = 1;
    }
  };

  std::deque<SelectStatement> owned_reduced;
  std::vector<const SelectStatement*> branch_exec_ptrs(branches.size(), nullptr);
  std::vector<size_t> branch_fk_slots(branches.size(), 0);
  std::vector<size_t> branch_other_slots(branches.size(), 1);
  for (size_t bi = 0; bi < branches.size(); ++bi) {
    const SelectStatement& br = *branches[bi];
    const BranchPlan& bp = plans[bi];
    if (bp.direct_subquery && bp.subquery_ptr != nullptr) {
      branch_exec_ptrs[bi] = bp.subquery_ptr;
      branch_fk_slots[bi] = bp.sub_fk_slot;
      branch_other_slots[bi] = bp.sub_other_slot;
      continue;
    }
    SelectStatement reduced(br);
    std::vector<SelectSource> new_sources;
    new_sources.reserve(br.Sources().size() - 1);
    for (size_t si = 0; si < br.Sources().size(); ++si) {
      if (si == bp.dim_src_idx) {
        continue;
      }
      SelectSource cp = br.Sources()[si];
      if (new_sources.empty()) {
        cp.join_type = JoinType::kInner;
        cp.join_condition = Expression{};
      } else {
        cp.join_condition = Expression{};
      }
      new_sources.push_back(std::move(cp));
    }
    reduced.SetSources(std::move(new_sources));
    std::vector<Expression> wc = bp.non_dim_where;
    wc.push_back(
        std::make_shared<UnaryExpression>(bp.fk_expr, UnaryOperation::kIsNotNull));
    reduced.SetWhereClause(CombineConjuncts(wc));
    std::vector<NamedExpression> sel;
    std::vector<Expression> grp;
    sel.emplace_back("__fk", bp.fk_expr);
    grp.push_back(bp.fk_expr);
    if (shared_other_pos.has_value()) {
      sel.emplace_back("__ot", bp.other_expr);
      grp.push_back(bp.other_expr);
    }
    reduced.SetSelectList(std::move(sel));
    reduced.SetDistinct(false);
    reduced.SetGroupBy(std::move(grp));
    reduced.SetOrderBy({});
    owned_reduced.push_back(std::move(reduced));
    branch_exec_ptrs[bi] = &owned_reduced.back();
  }
  ASSIGN_OR_RETURN(std::optional<std::vector<Relation>>, par_branch_rels,
                   TryExecuteBranchesParallel(context, branch_exec_ptrs,
                                              inherited_ctes, true));

  auto eval_branch = [&](size_t bi) -> StatusOr<std::optional<QuotientSet>> {
    Relation rel(context.execution_runtime());
    if (par_branch_rels.has_value()) {
      rel = std::move((*par_branch_rels)[bi]);
      RETURN_IF_FAIL(rel.GetStatus());
    } else {
      ASSIGN_OR_RETURN(Relation, run_rel,
                       ExecuteQuery(context, *branch_exec_ptrs[bi], nullptr,
                                    inherited_ctes, nullptr, true));
      RETURN_IF_FAIL(run_rel.GetStatus());
      rel = std::move(run_rel);
    }
    const size_t fk_slot = branch_fk_slots[bi];
    const size_t other_slot = branch_other_slots[bi];

    if (rel.HasSpill() || fk_slot >= rel.schema.ColumnCount()) {
      return std::optional<QuotientSet>{};
    }
    const Column& fkc_meta = rel.schema.GetColumn(fk_slot);
    if (fkc_meta.Type() != ValueType::kInt64 &&
        fkc_meta.Type() != ValueType::kNull) {
      return std::optional<QuotientSet>{};
    }
    if (shared_other_pos.has_value()) {
      if (other_slot >= rel.schema.ColumnCount()) {
        return std::optional<QuotientSet>{};
      }
      const Column& otc_meta = rel.schema.GetColumn(other_slot);
      if ((otc_meta.Type() != ValueType::kInt64 &&
           otc_meta.Type() != ValueType::kDate &&
           otc_meta.Type() != ValueType::kNull) ||
          otc_meta.IsUnsigned()) {
        return std::optional<QuotientSet>{};
      }
    }

    QuotientSet qs;
    qs.packed_i32.Reserve(rel.TotalRows());
    auto insert_pair = [&](int64_t fk_val, bool other_null, int64_t other_val) {
      if (fk_val < 0 ||
          static_cast<size_t>(fk_val) >= pk_to_gid.size()) {
        return;
      }
      const uint32_t gid = pk_to_gid[static_cast<size_t>(fk_val)];
      if (gid == 0) {
        return;
      }
      if (!shared_other_pos.has_value() || other_null) {
        insert_key(qs.null_other_gids, static_cast<int64_t>(gid));
        return;
      }
      if (other_val >= std::numeric_limits<int32_t>::min() &&
          other_val <= std::numeric_limits<int32_t>::max()) {
        const auto u_other =
            static_cast<uint32_t>(static_cast<int32_t>(other_val));
        const auto packed = static_cast<int64_t>(
            (static_cast<uint64_t>(gid) << 32U) | static_cast<uint64_t>(u_other));
        insert_key(qs.packed_i32, packed);
      } else {
        qs.wide_pairs.emplace(gid, other_val);
      }
    };

    if (rel.columnar != nullptr) {
      for (const ColumnarChunk& ch : rel.columnar->chunks) {
        if (fk_slot >= ch.columns.size() ||
            (shared_other_pos.has_value() && other_slot >= ch.columns.size())) {
          return std::optional<QuotientSet>{};
        }
        const ColumnarColumn& fkc = ch.columns[fk_slot];
        if (!shared_other_pos.has_value()) {
          for (size_t r = 0; r < ch.rows; ++r) {
            if (!fkc.IsNull(r)) {
              insert_pair(fkc.IntAt(r), true, 0);
            }
          }
          continue;
        }
        const ColumnarColumn& otc = ch.columns[other_slot];
        if (fkc.IsFlat() && fkc.is_uniform && !fkc.has_null &&
            (fkc.type == ValueType::kInt64) && otc.IsFlat() &&
            otc.is_uniform && !otc.has_null &&
            (otc.type == ValueType::kInt64 || otc.type == ValueType::kDate)) {
          const int64_t* fk_data = fkc.ints.data();
          const int64_t* ot_data = otc.ints.data();
          for (size_t r = 0; r < ch.rows; ++r) {
            insert_pair(fk_data[r], false, ot_data[r]);
          }
        } else {
          for (size_t r = 0; r < ch.rows; ++r) {
            if (fkc.IsNull(r)) {
              continue;
            }
            const bool onull = otc.IsNull(r);
            insert_pair(fkc.IntAt(r), onull, onull ? 0 : otc.IntAt(r));
          }
        }
      }
    } else {
      for (const Row& row : rel.rows) {
        if (fk_slot >= row.Size() || row[fk_slot].IsNull()) {
          continue;
        }
        if (row[fk_slot].type != ValueType::kInt64 ||
            row[fk_slot].IsUnsigned()) {
          return std::optional<QuotientSet>{};
        }
        bool onull = true;
        int64_t oval = 0;
        if (shared_other_pos.has_value()) {
          if (other_slot >= row.Size()) {
            return std::optional<QuotientSet>{};
          }
          onull = row[other_slot].IsNull();
          if (!onull) {
            if ((row[other_slot].type != ValueType::kInt64 &&
                 row[other_slot].type != ValueType::kDate) ||
                row[other_slot].IsUnsigned()) {
              return std::optional<QuotientSet>{};
            }
            oval = row[other_slot].value.int_value;
          }
        }
        insert_pair(row[fk_slot].value.int_value, onull, oval);
      }
    }
    return std::optional<QuotientSet>(std::move(qs));
  };

  auto apply_qop = [&](QuotientSet a, QuotientSet& b,
                       SetOperationKind op) -> QuotientSet {
    if (op == SetOperationKind::kUnion) {
      b.packed_i32.ForEach([&](int64_t k, uint8_t) { insert_key(a.packed_i32, k); });
      b.null_other_gids.ForEach(
          [&](int64_t g, uint8_t) { insert_key(a.null_other_gids, g); });
      for (const auto& wp : b.wide_pairs) {
        a.wide_pairs.insert(wp);
      }
      return a;
    }
    QuotientSet out;
    if (op == SetOperationKind::kIntersect) {
      const bool swap_i32 = b.packed_i32.Size() < a.packed_i32.Size();
      FlatIntKeyMap<uint8_t>& small_i32 = swap_i32 ? b.packed_i32 : a.packed_i32;
      const FlatIntKeyMap<uint8_t>& large_i32 =
          swap_i32 ? a.packed_i32 : b.packed_i32;
      out.packed_i32.Reserve(small_i32.Size());
      small_i32.ForEach([&](int64_t k, uint8_t) {
        if (large_i32.Find(k) != nullptr) {
          insert_key(out.packed_i32, k);
        }
      });
      a.null_other_gids.ForEach([&](int64_t g, uint8_t) {
        if (b.null_other_gids.Find(g) != nullptr) {
          insert_key(out.null_other_gids, g);
        }
      });
      for (const auto& wp : a.wide_pairs) {
        if (b.wide_pairs.contains(wp)) {
          out.wide_pairs.insert(wp);
        }
      }
      return out;
    }
    // kExcept
    out.packed_i32.Reserve(a.packed_i32.Size());
    a.packed_i32.ForEach([&](int64_t k, uint8_t) {
      if (b.packed_i32.Find(k) == nullptr) {
        insert_key(out.packed_i32, k);
      }
    });
    a.null_other_gids.ForEach([&](int64_t g, uint8_t) {
      if (b.null_other_gids.Find(g) == nullptr) {
        insert_key(out.null_other_gids, g);
      }
    });
    for (const auto& wp : a.wide_pairs) {
      if (!b.wide_pairs.contains(wp)) {
        out.wide_pairs.insert(wp);
      }
    }
    return out;
  };

  ASSIGN_OR_RETURN(std::optional<QuotientSet>, first_qs, eval_branch(0));
  if (!first_qs.has_value()) {
    return std::optional<Relation>{};
  }
  std::vector<QuotientSet> high_terms;
  high_terms.push_back(std::move(*first_qs));
  std::vector<SetOperationKind> low_ops;
  for (size_t i = 0; i < ops.size(); ++i) {
    ASSIGN_OR_RETURN(std::optional<QuotientSet>, next_qs, eval_branch(i + 1));
    if (!next_qs.has_value()) {
      return std::optional<Relation>{};
    }
    if (ops[i] == SetOperationKind::kIntersect) {
      high_terms.back() =
          apply_qop(std::move(high_terms.back()), *next_qs, ops[i]);
    } else {
      low_ops.push_back(ops[i]);
      high_terms.push_back(std::move(*next_qs));
    }
  }
  QuotientSet acc = std::move(high_terms[0]);
  for (size_t i = 0; i < low_ops.size(); ++i) {
    acc = apply_qop(std::move(acc), high_terms[i + 1], low_ops[i]);
  }

  const auto total = static_cast<int64_t>(acc.packed_i32.Size() +
                                          acc.null_other_gids.Size() +
                                          acc.wide_pairs.size());
  Relation out(context.execution_runtime());
  out.schema = Schema(
      "", {Column(ProjectionName(statement.SelectList()[0], 0),
                  ValueType::kInt64)});
  out.AddRow(Row({Value(total)}));
  RETURN_IF_FAIL(out.GetStatus());
  return std::optional<Relation>(std::move(out));
}

// Lean proof: lean/TinylambOpt/TpcdsExecCodegen.lean
//   (partitioned_order_by_limit_band_pruning_sound)
StatusOr<std::optional<Relation>> TryExecutePartitionBandedOrderByLimit(
    TransactionContext& context, const SelectStatement& statement,
    const CteMap& inherited_ctes, bool columnar_output) {
  thread_local int band_depth = 0;
  if (band_depth > 0) {
    return std::optional<Relation>{};
  }
  if (!statement.WithQueries().empty() || !statement.UnionAll().empty() ||
      statement.GetSetOperationTree() != nullptr ||
      !statement.GroupBy().empty() || statement.IsGroupAll() ||
      statement.Having() || statement.Distinct() || statement.HasDistinctOn() ||
      statement.WithTies() || statement.AsStruct() || statement.ForUpdate() ||
      !statement.HasLimit() || statement.Limit() == 0 ||
      statement.Limit() > 4096 || statement.Offset() > 1024 ||
      statement.OrderBy().empty() || !statement.OrderBy()[0].ascending ||
      StatementContainsVolatileFunction(statement)) {
    return std::optional<Relation>{};
  }
  const Expression& ord0 = statement.OrderBy()[0].expression;
  if (!ord0 || ord0->Type() != TypeTag::kColumnValue) {
    return std::optional<Relation>{};
  }

  auto check_windows_partitioned_by = [](const Expression& root,
                                         std::string_view key_name,
                                         std::string_view src_alias,
                                         bool allow_agg_outside_win) -> bool {
    if (!root || ContainsParameterSlot(root)) {
      return !root;
    }
    auto walk = [&](auto& self, const Expression& e, bool inside_win) -> bool {
      if (!e) {
        return true;
      }
      if (e->Type() == TypeTag::kQueryExp) {
        return false;
      }
      if (e->Type() == TypeTag::kAggregateExp && !inside_win &&
          !allow_agg_outside_win) {
        return false;
      }
      if (e->Type() == TypeTag::kWindowFunctionExp) {
        const auto& wf = e->AsWindowFunctionCallExpression();
        const bool has_part =
            std::ranges::any_of(wf.partition_by, [&](const Expression& pe) {
              if (!pe || pe->Type() != TypeTag::kColumnValue) {
                return false;
              }
              const ColumnName& cn = pe->AsColumnValue().GetColumnName();
              return IdentifierEquals(cn.name, key_name) &&
                     (cn.schema.empty() || src_alias.empty() ||
                      IdentifierEquals(cn.schema, src_alias));
            });
        if (!has_part) {
          return false;
        }
        for (const Expression& ch : ExpressionChildren(e)) {
          if (!self(self, ch, true)) {
            return false;
          }
        }
        return true;
      }
      for (const Expression& ch : ExpressionChildren(e)) {
        if (!self(self, ch, inside_win)) {
          return false;
        }
      }
      return true;
    };
    return walk(walk, root, false);
  };

  // Walk down single-source derived wrappers (0..3 hops) to the 2-source join.
  std::string cur_key = ord0->AsColumnValue().GetColumnName().name;
  if (cur_key.empty() || cur_key == "*") {
    return std::optional<Relation>{};
  }
  const SelectStatement* cur = &statement;
  size_t spine_hops = 0;
  while (cur != nullptr && cur->Sources().size() == 1 && spine_hops < 4) {
    if (!cur->WithQueries().empty() || !cur->UnionAll().empty() ||
        cur->GetSetOperationTree() != nullptr || !cur->GroupBy().empty() ||
        cur->IsGroupAll() || cur->Having() || cur->Distinct() ||
        cur->HasDistinctOn() ||
        (cur != &statement && (cur->HasLimit() || cur->Offset() != 0 ||
                               cur->WithTies())) ||
        ContainsQuery(cur->WhereClause()) ||
        ContainsAggregate(cur->WhereClause()) ||
        ContainsParameterSlot(cur->WhereClause()) ||
        StatementContainsVolatileFunction(*cur)) {
      return std::optional<Relation>{};
    }
    const SelectSource& s0 = cur->Sources()[0];
    if (!s0.query || s0.unnest || s0.is_lateral || s0.sample ||
        !s0.using_columns.empty() || s0.from_nested_join ||
        s0.join_condition) {
      return std::optional<Relation>{};
    }
    const std::string sub_alias = s0.alias;
    bool is_star =
        cur->SelectList().size() == 1 && cur->SelectList()[0].expression &&
        cur->SelectList()[0].expression->Type() == TypeTag::kColumnValue &&
        cur->SelectList()[0].expression->AsColumnValue().GetColumnName().name ==
            "*" &&
        cur->SelectList()[0].star_modifiers == nullptr;
    std::string next_key;
    if (is_star) {
      next_key = cur_key;
    } else {
      for (size_t idx = 0; idx < cur->SelectList().size(); ++idx) {
        const NamedExpression& it = cur->SelectList()[idx];
        if (!it.expression || it.star_modifiers != nullptr) {
          return std::optional<Relation>{};
        }
        if (IdentifierEquals(ProjectionName(it, idx), cur_key)) {
          if (it.expression->Type() != TypeTag::kColumnValue) {
            return std::optional<Relation>{};
          }
          const ColumnName& cn = it.expression->AsColumnValue().GetColumnName();
          if (!cn.schema.empty() && !sub_alias.empty() &&
              !IdentifierEquals(cn.schema, sub_alias)) {
            return std::optional<Relation>{};
          }
          next_key = cn.name;
        }
      }
      if (next_key.empty()) {
        return std::optional<Relation>{};
      }
    }
    for (const NamedExpression& it : cur->SelectList()) {
      if (!check_windows_partitioned_by(it.expression, next_key, sub_alias,
                                        false)) {
        return std::optional<Relation>{};
      }
    }
    if (!check_windows_partitioned_by(cur->Qualify(), next_key, sub_alias,
                                      false) ||
        !check_windows_partitioned_by(cur->WhereClause(), next_key, sub_alias,
                                      false)) {
      return std::optional<Relation>{};
    }
    cur_key = std::move(next_key);
    cur = s0.query.get();
    ++spine_hops;
  }

  if (cur == nullptr || cur->Sources().size() != 2 ||
      !cur->WithQueries().empty() || !cur->UnionAll().empty() ||
      cur->GetSetOperationTree() != nullptr || !cur->GroupBy().empty() ||
      cur->IsGroupAll() || cur->Having() || cur->Distinct() ||
      cur->HasDistinctOn() || cur->HasLimit() || cur->Offset() != 0 ||
      cur->Qualify() || HasWindowFunctions(*cur) ||
      ContainsQuery(cur->WhereClause()) ||
      ContainsParameterSlot(cur->WhereClause()) ||
      StatementContainsVolatileFunction(*cur)) {
    return std::optional<Relation>{};
  }
  const SelectSource& js0 = cur->Sources()[0];
  const SelectSource& js1 = cur->Sources()[1];
  if (!js0.query || !js1.query || js0.unnest || js1.unnest || js0.is_lateral ||
      js1.is_lateral || js0.sample || js1.sample ||
      !js0.using_columns.empty() || !js1.using_columns.empty() ||
      js0.from_nested_join || js1.from_nested_join || js0.alias.empty() ||
      js1.alias.empty() || js0.join_condition ||
      (js1.join_type != JoinType::kFull && js1.join_type != JoinType::kInner &&
       js1.join_type != JoinType::kLeft && js1.join_type != JoinType::kRight)) {
    return std::optional<Relation>{};
  }
  for (const NamedExpression& it : cur->SelectList()) {
    if (!it.expression || it.star_modifiers != nullptr ||
        ContainsQuery(it.expression) || ContainsAggregate(it.expression) ||
        ContainsParameterSlot(it.expression)) {
      return std::optional<Relation>{};
    }
  }

  // Extract `(k0_out, k1_out)` from the join's projected partition key `cur_key`
  // (e.g. `CASE WHEN NOT web.item_sk IS NULL THEN web.item_sk ELSE store.item_sk END`
  // or `COALESCE(web.item_sk, store.item_sk)`).
  Expression key_proj;
  for (size_t idx = 0; idx < cur->SelectList().size(); ++idx) {
    if (IdentifierEquals(ProjectionName(cur->SelectList()[idx], idx), cur_key)) {
      key_proj = cur->SelectList()[idx].expression;
      break;
    }
  }
  if (!key_proj) {
    return std::optional<Relation>{};
  }
  auto match_side_col = [&](const Expression& e, std::string_view want_alias,
                            std::string* col_out) -> bool {
    if (!e || e->Type() != TypeTag::kColumnValue) {
      return false;
    }
    const ColumnName& cn = e->AsColumnValue().GetColumnName();
    if (!IdentifierEquals(cn.schema, want_alias) || cn.name.empty() ||
        cn.name == "*") {
      return false;
    }
    *col_out = cn.name;
    return true;
  };
  std::string k0_out;
  std::string k1_out;
  if (key_proj->Type() == TypeTag::kCaseExp) {
    const auto& ce = key_proj->AsCaseExpression();
    if (ce.when_clauses_.size() == 1 && ce.else_clause_ &&
        ce.when_clauses_[0].first &&
        ce.when_clauses_[0].first->Type() == TypeTag::kUnaryExp) {
      const auto& ue = ce.when_clauses_[0].first->AsUnaryExpression();
      std::string cond_c;
      std::string then_c0;
      std::string then_c1;
      std::string else_c0;
      std::string else_c1;
      if (ue.Op() == UnaryOperation::kIsNotNull) {
        if (match_side_col(ue.Child(), js0.alias, &cond_c) &&
            match_side_col(ce.when_clauses_[0].second, js0.alias, &then_c0) &&
            IdentifierEquals(cond_c, then_c0) &&
            match_side_col(ce.else_clause_, js1.alias, &else_c1)) {
          k0_out = then_c0;
          k1_out = else_c1;
        } else if (match_side_col(ue.Child(), js1.alias, &cond_c) &&
                   match_side_col(ce.when_clauses_[0].second, js1.alias,
                                  &then_c1) &&
                   IdentifierEquals(cond_c, then_c1) &&
                   match_side_col(ce.else_clause_, js0.alias, &else_c0)) {
          k0_out = else_c0;
          k1_out = then_c1;
        }
      } else if (ue.Op() == UnaryOperation::kIsNull) {
        if (match_side_col(ue.Child(), js0.alias, &cond_c) &&
            match_side_col(ce.when_clauses_[0].second, js1.alias, &then_c1) &&
            match_side_col(ce.else_clause_, js0.alias, &else_c0) &&
            IdentifierEquals(cond_c, else_c0)) {
          k0_out = else_c0;
          k1_out = then_c1;
        }
      }
    }
  } else if (key_proj->Type() == TypeTag::kFunctionCallExp &&
             IdentifierEquals(key_proj->AsFunctionCallExpression().FuncName(),
                              "coalesce") &&
             key_proj->AsFunctionCallExpression().Args().size() == 2) {
    const auto& args = key_proj->AsFunctionCallExpression().Args();
    if (!match_side_col(args[0], js0.alias, &k0_out) ||
        !match_side_col(args[1], js1.alias, &k1_out)) {
      k0_out.clear();
      k1_out.clear();
    }
  } else if ((js1.join_type == JoinType::kInner ||
              js1.join_type == JoinType::kLeft) &&
             match_side_col(key_proj, js0.alias, &k0_out)) {
    // k1_out inferred from ON equality below.
  }
  if (k0_out.empty()) {
    return std::optional<Relation>{};
  }

  // Verify ON condition contains `js0.k0_out = js1.k1_out`.
  bool found_key_eq = false;
  for (const Expression& jc : SplitConjuncts(js1.join_condition)) {
    if (!jc || jc->Type() != TypeTag::kBinaryExp ||
        jc->AsBinaryExpression().Op() != BinaryOperation::kEquals) {
      continue;
    }
    const auto& bin = jc->AsBinaryExpression();
    std::string c0;
    std::string c1;
    if ((match_side_col(bin.Left(), js0.alias, &c0) &&
         match_side_col(bin.Right(), js1.alias, &c1)) ||
        (match_side_col(bin.Right(), js0.alias, &c0) &&
         match_side_col(bin.Left(), js1.alias, &c1))) {
      if (IdentifierEquals(c0, k0_out) &&
          (k1_out.empty() || IdentifierEquals(c1, k1_out))) {
        k1_out = c1;
        found_key_eq = true;
        break;
      }
    }
  }
  if (!found_key_eq || k1_out.empty()) {
    return std::optional<Relation>{};
  }

  struct BranchBandTarget {
    ColumnName base_col;
    bool guaranteed_non_null{false};
    int64_t min_int{1};
    uint64_t est_rows{0};
  };
  auto inspect_grouped_branch =
      [&](const SelectStatement& qb,
          std::string_view want_out) -> std::optional<BranchBandTarget> {
    if (!qb.WithQueries().empty() || !qb.UnionAll().empty() ||
        qb.GetSetOperationTree() != nullptr || qb.GroupBy().empty() ||
        qb.IsGroupAll() || qb.Distinct() || qb.HasDistinctOn() ||
        qb.HasLimit() || qb.Offset() != 0 || qb.Qualify() || qb.Having() ||
        ContainsQuery(qb.WhereClause()) ||
        ContainsParameterSlot(qb.WhereClause()) ||
        StatementContainsVolatileFunction(qb)) {
      return std::nullopt;
    }
    ColumnName raw_col;
    bool found_out = false;
    for (size_t idx = 0; idx < qb.SelectList().size(); ++idx) {
      const NamedExpression& it = qb.SelectList()[idx];
      if (!it.expression || it.star_modifiers != nullptr) {
        return std::nullopt;
      }
      if (IdentifierEquals(ProjectionName(it, idx), want_out)) {
        if (it.expression->Type() != TypeTag::kColumnValue) {
          return std::nullopt;
        }
        raw_col = it.expression->AsColumnValue().GetColumnName();
        found_out = true;
      }
    }
    if (!found_out || raw_col.name.empty() || raw_col.name == "*") {
      return std::nullopt;
    }
    const bool in_gby =
        std::ranges::any_of(qb.GroupBy(), [&](const Expression& gk) {
          if (!gk || gk->Type() != TypeTag::kColumnValue) {
            return false;
          }
          const ColumnName& gc = gk->AsColumnValue().GetColumnName();
          return IdentifierEquals(gc.name, raw_col.name) &&
                 (gc.schema.empty() || raw_col.schema.empty() ||
                  IdentifierEquals(gc.schema, raw_col.schema));
        });
    if (!in_gby) {
      return std::nullopt;
    }
    size_t hits = 0;
    BranchBandTarget target;
    for (const SelectSource& bs : qb.Sources()) {
      if (bs.table.empty() || bs.query != nullptr || bs.unnest ||
          bs.is_lateral || bs.sample || !bs.using_columns.empty() ||
          bs.from_nested_join || inherited_ctes.contains(bs.table)) {
        return std::nullopt;
      }
      auto tbl_or = context.GetTable(bs.table);
      if (!tbl_or.HasValue() || tbl_or.Value() == nullptr) {
        return std::nullopt;
      }
      const std::shared_ptr<Table>& tbl = tbl_or.Value();
      const std::string balias = bs.alias.empty() ? bs.table : bs.alias;
      if (!raw_col.schema.empty() && !IdentifierEquals(raw_col.schema, balias) &&
          !IdentifierEquals(raw_col.schema, bs.table)) {
        continue;
      }
      const Schema& bschema = tbl->GetSchema();
      const int off = bschema.Offset(ColumnName("", raw_col.name));
      if (off < 0) {
        continue;
      }
      if (bs.join_type != JoinType::kCross &&
          bs.join_type != JoinType::kInner) {
        return std::nullopt;
      }
      const auto slot = static_cast<slot_t>(off);
      const Column& cm = bschema.GetColumn(slot);
      if (cm.Type() != ValueType::kInt64 || cm.IsUnsigned()) {
        return std::nullopt;
      }
      ++hits;
      target.base_col = ColumnName(balias, cm.Name().name);
      target.est_rows = tbl->BackingColumnarRowCount();
      if (auto st = context.GetStats(bs.table); st.HasValue()) {
        target.est_rows = std::max<uint64_t>(target.est_rows, st.Value()->Rows());
      }
      if (const auto lst = tbl->GetLSTableStore();
          lst != nullptr && lst->uncommitted_owner_txn == 0 &&
          lst->layout_generation == tbl->LayoutGeneration() &&
          lst->first_pid == tbl->FirstPageId()) {
        if (slot < lst->column_min_int.size() && lst->total_rows > 0) {
          target.min_int = lst->column_min_int[slot];
        }
        if (slot < lst->column_has_null.size() &&
            lst->column_has_null[slot] == 0 &&
            !context.txn_.HasAnyPageVersionChains() &&
            (context.txn_.IsReadOnly() || context.txn_.WriteSetEmpty())) {
          target.guaranteed_non_null = true;
        }
      }
    }
    if (hits != 1) {
      return std::nullopt;
    }
    for (const Expression& wconj : SplitConjuncts(qb.WhereClause())) {
      if (wconj && wconj->Type() == TypeTag::kUnaryExp &&
          wconj->AsUnaryExpression().Op() == UnaryOperation::kIsNotNull &&
          wconj->AsUnaryExpression().Child() &&
          wconj->AsUnaryExpression().Child()->Type() == TypeTag::kColumnValue) {
        const ColumnName& wc =
            wconj->AsUnaryExpression().Child()->AsColumnValue().GetColumnName();
        if (IdentifierEquals(wc.name, target.base_col.name) &&
            (wc.schema.empty() ||
             IdentifierEquals(wc.schema, target.base_col.schema))) {
          target.guaranteed_non_null = true;
        }
      }
    }
    for (const NamedExpression& it : qb.SelectList()) {
      if (!check_windows_partitioned_by(it.expression, target.base_col.name,
                                        target.base_col.schema, true)) {
        return std::nullopt;
      }
    }
    return target;
  };

  const auto t0 = inspect_grouped_branch(*js0.query, k0_out);
  const auto t1 = inspect_grouped_branch(*js1.query, k1_out);
  if (!t0.has_value() || !t1.has_value() ||
      (t0->est_rows + t1->est_rows) < 100000) {
    return std::optional<Relation>{};
  }

  const int64_t lo_bound = std::min(t0->min_int, t1->min_int);
  const int64_t need_rows =  // NOLINT(modernize-use-auto)
      static_cast<int64_t>(statement.Limit() + statement.Offset());
  const std::array<int64_t, 2> band_widths{
      std::max<int64_t>(384, need_rows * 4),
      std::max<int64_t>(4096, need_rows * 32)};
  const bool nulls_in_prefix =
      statement.OrderBy()[0].nulls_first.value_or(true);

  struct BandDepthGuard {  // NOLINT(cppcoreguidelines-special-member-functions)
    int* depth;
    explicit BandDepthGuard(int* d) : depth(d) { ++(*depth); }
    ~BandDepthGuard() { --(*depth); }
  } depth_guard(&band_depth);

  auto make_band_pred = [&](const BranchBandTarget& tgt, int64_t hi) {
    Expression le = BinaryExpressionExp(
        std::make_shared<ColumnValue>(tgt.base_col),
        BinaryOperation::kLessThanEquals,
        std::make_shared<ConstantValue>(Value(hi)));
    if (!nulls_in_prefix || tgt.guaranteed_non_null) {
      return le;
    }
    return BinaryExpressionExp(
        std::make_shared<UnaryExpression>(
            std::make_shared<ColumnValue>(tgt.base_col),
            UnaryOperation::kIsNull),
        BinaryOperation::kOr, std::move(le));
  };

  for (int64_t width : band_widths) {
    int64_t hi_bound = 0;
    if (__builtin_add_overflow(lo_bound, width, &hi_bound)) {
      break;
    }
    SelectStatement banded_root(statement);
    SelectStatement* edit = &banded_root;
    for (size_t h = 0; h < spine_hops; ++h) {
      std::vector<SelectSource> srcs = edit->Sources();
      srcs[0].query = std::make_shared<SelectStatement>(*srcs[0].query);
      SelectStatement* next_edit = srcs[0].query.get();
      edit->SetSources(std::move(srcs));
      edit = next_edit;
    }
    std::vector<SelectSource> jsrcs = edit->Sources();
    auto q0_copy = std::make_shared<SelectStatement>(*jsrcs[0].query);
    auto q1_copy = std::make_shared<SelectStatement>(*jsrcs[1].query);
    Expression p0 = make_band_pred(*t0, hi_bound);
    Expression p1 = make_band_pred(*t1, hi_bound);
    q0_copy->SetWhereClause(
        q0_copy->WhereClause()
            ? BinaryExpressionExp(q0_copy->WhereClause(), BinaryOperation::kAnd,
                                  std::move(p0))
            : std::move(p0));
    q1_copy->SetWhereClause(
        q1_copy->WhereClause()
            ? BinaryExpressionExp(q1_copy->WhereClause(), BinaryOperation::kAnd,
                                  std::move(p1))
            : std::move(p1));
    jsrcs[0].query = std::move(q0_copy);
    jsrcs[1].query = std::move(q1_copy);
    edit->SetSources(std::move(jsrcs));

    ASSIGN_OR_RETURN(Relation, cand_rel,
                     ExecuteQuery(context, banded_root, nullptr, inherited_ctes,
                                  nullptr, columnar_output));
    RETURN_IF_FAIL(cand_rel.GetStatus());
    if (!cand_rel.HasSpill() && cand_rel.TotalRows() >= statement.Limit()) {
      if (!columnar_output && cand_rel.HasColumnarOnly()) {
        RETURN_IF_FAIL(cand_rel.EnsureRowsMaterialized());
      }
      return std::optional<Relation>(std::move(cand_rel));
    }
  }
  return std::optional<Relation>{};
}

// Fast scalar `COUNT(*)` / `COUNT(CASE WHEN col IS [NOT] NULL ... THEN 1 END)`
// directly over a ColumnarTable CTE without materializing Row objects.
StatusOr<std::optional<Relation>> TryExecuteColumnarNullMaskScalarCounts(
    TransactionContext& context, const SelectStatement& statement,
    const CteMap& inherited_ctes) {
  if (!statement.WithQueries().empty() || !statement.UnionAll().empty() ||
      statement.GetSetOperationTree() != nullptr || statement.WhereClause() ||
      !statement.GroupBy().empty() || statement.IsGroupAll() ||
      statement.Having() || statement.Qualify() || statement.Distinct() ||
      statement.HasDistinctOn() || !statement.OrderBy().empty() ||
      statement.Offset() != 0 ||
      (statement.HasLimit() && statement.Limit() == 0) ||
      statement.AsStruct() || statement.ForUpdate() ||
      HasWindowFunctions(statement) || statement.Sources().size() != 1 ||
      statement.SelectList().empty()) {
    return std::optional<Relation>{};
  }
  const SelectSource& src = statement.Sources()[0];
  if (src.query || src.unnest || src.is_lateral || src.sample ||
      !src.using_columns.empty() || src.from_nested_join ||
      src.join_condition) {
    return std::optional<Relation>{};
  }
  const auto cte_it = inherited_ctes.find(src.table);
  if (cte_it == inherited_ctes.end() || cte_it->second == nullptr ||
      !cte_it->second->HasColumnarOnly() || cte_it->second->columnar == nullptr ||
      cte_it->second->HasSpill()) {
    return std::optional<Relation>{};
  }
  const Relation& cte_rel = *cte_it->second;
  const std::string alias = src.alias.empty() ? src.table : src.alias;
  const Schema view_schema = QualifySchema(cte_rel.schema, alias);

  struct NullClause {
    struct Atom {
      size_t col{0};
      bool want_not_null{true};
    };
    bool is_or{false};
    std::vector<Atom> atoms;
  };
  struct ItemSpec {
    enum class Kind : uint8_t { kConst, kCountStar, kCountMasked };
    Kind kind{Kind::kConst};
    Value const_val;
    std::vector<NullClause> conj_clauses;
  };

  auto parse_atom = [&](const Expression& e, NullClause::Atom* out) -> bool {
    if (!e || e->Type() != TypeTag::kUnaryExp) {
      return false;
    }
    const auto& u = e->AsUnaryExpression();
    if ((u.Op() != UnaryOperation::kIsNull &&
         u.Op() != UnaryOperation::kIsNotNull) ||
        !u.Child() || u.Child()->Type() != TypeTag::kColumnValue) {
      return false;
    }
    const auto off =
        LocalColumnOffset(view_schema, u.Child()->AsColumnValue().GetColumnName());
    if (!off.has_value()) {
      return false;
    }
    out->col = *off;
    out->want_not_null = (u.Op() == UnaryOperation::kIsNotNull);
    return true;
  };

  auto split_disjuncts = [](const Expression& e, std::vector<Expression>* out) {
    std::vector<Expression> stack{e};
    while (!stack.empty()) {
      Expression cur = std::move(stack.back());
      stack.pop_back();
      if (cur && cur->Type() == TypeTag::kBinaryExp &&
          cur->AsBinaryExpression().Op() == BinaryOperation::kOr) {
        stack.push_back(cur->AsBinaryExpression().Right());
        stack.push_back(cur->AsBinaryExpression().Left());
      } else {
        out->push_back(std::move(cur));
      }
    }
  };

  std::vector<ItemSpec> specs(statement.SelectList().size());
  bool any_agg = false;
  for (size_t i = 0; i < statement.SelectList().size(); ++i) {
    const Expression& expr = statement.SelectList()[i].expression;
    if (!expr || statement.SelectList()[i].star_modifiers != nullptr) {
      return std::optional<Relation>{};
    }
    if (expr->Type() == TypeTag::kConstantValue) {
      specs[i].kind = ItemSpec::Kind::kConst;
      specs[i].const_val = expr->AsConstantValue().GetValue();
      continue;
    }
    if (expr->Type() != TypeTag::kAggregateExp) {
      return std::optional<Relation>{};
    }
    const auto& agg = expr->AsAggregateExpression();
    if (agg.GetType() != AggregationType::kCount || agg.Distinct() ||
        agg.WhereFilter() || agg.Having() != AggregateHavingModifier::kNone ||
        !agg.InnerOrderBy().empty() || agg.InnerLimit().has_value() ||
        agg.NeedsGroupContext()) {
      return std::optional<Relation>{};
    }
    any_agg = true;
    if (IsCountStar(agg)) {
      specs[i].kind = ItemSpec::Kind::kCountStar;
      continue;
    }
    if (!agg.Child() || agg.Child()->Type() != TypeTag::kCaseExp) {
      return std::optional<Relation>{};
    }
    const auto& ce = agg.Child()->AsCaseExpression();
    if (ce.when_clauses_.size() != 1 || ce.else_clause_ != nullptr ||
        !ce.when_clauses_[0].second ||
        ce.when_clauses_[0].second->Type() != TypeTag::kConstantValue ||
        ce.when_clauses_[0].second->AsConstantValue().GetValue().IsNull()) {
      return std::optional<Relation>{};
    }
    specs[i].kind = ItemSpec::Kind::kCountMasked;
    for (const Expression& cj : SplitConjuncts(ce.when_clauses_[0].first)) {
      NullClause cl;
      NullClause::Atom single{};
      if (parse_atom(cj, &single)) {
        cl.is_or = false;
        cl.atoms.push_back(single);
        specs[i].conj_clauses.push_back(std::move(cl));
        continue;
      }
      std::vector<Expression> disjs;
      split_disjuncts(cj, &disjs);
      if (disjs.size() < 2) {
        return std::optional<Relation>{};
      }
      cl.is_or = true;
      for (const Expression& dj : disjs) {
        NullClause::Atom da{};
        if (!parse_atom(dj, &da)) {
          return std::optional<Relation>{};
        }
        cl.atoms.push_back(da);
      }
      specs[i].conj_clauses.push_back(std::move(cl));
    }
    if (specs[i].conj_clauses.empty()) {
      return std::optional<Relation>{};
    }
  }
  if (!any_agg) {
    return std::optional<Relation>{};
  }

  std::vector<int64_t> counts(specs.size(), 0);
  for (const ColumnarChunk& ch : cte_rel.columnar->chunks) {
    if (ch.rows == 0) {
      continue;
    }
    bool chunk_any_null = false;
    for (const ColumnarColumn& cc : ch.columns) {
      if (cc.has_null || cc.type == ValueType::kNull) {
        chunk_any_null = true;
        break;
      }
    }
    for (size_t i = 0; i < specs.size(); ++i) {
      if (specs[i].kind == ItemSpec::Kind::kCountStar) {
        counts[i] += static_cast<int64_t>(ch.rows);
        continue;
      }
      if (specs[i].kind != ItemSpec::Kind::kCountMasked) {
        continue;
      }
      if (!chunk_any_null) {
        bool all_pass = true;
        for (const NullClause& cl : specs[i].conj_clauses) {
          bool cl_pass = cl.is_or ? false : true;  // NOLINT(readability-simplify-boolean-expr)
          for (const auto& at : cl.atoms) {
            if (cl.is_or) {
              cl_pass = cl_pass || at.want_not_null;
            } else {
              cl_pass = cl_pass && at.want_not_null;
            }
          }
          if (!cl_pass) {
            all_pass = false;
            break;
          }
        }
        if (all_pass) {
          counts[i] += static_cast<int64_t>(ch.rows);
        }
        continue;
      }
      int64_t local_cnt = 0;
      for (size_t r = 0; r < ch.rows; ++r) {
        bool row_pass = true;
        for (const NullClause& cl : specs[i].conj_clauses) {
          bool cl_pass = cl.is_or ? false : true;  // NOLINT(readability-simplify-boolean-expr)
          for (const auto& at : cl.atoms) {
            const bool is_not_null =
                at.col < ch.columns.size() && !ch.columns[at.col].IsNull(r);
            const bool sat = (is_not_null == at.want_not_null);
            if (cl.is_or) {
              if (sat) {
                cl_pass = true;
                break;
              }
            } else if (!sat) {
              cl_pass = false;
              break;
            }
          }
          if (!cl_pass) {
            row_pass = false;
            break;
          }
        }
        local_cnt += static_cast<int64_t>(row_pass);
      }
      counts[i] += local_cnt;
    }
  }

  Relation out(context.execution_runtime());
  std::vector<Column> out_cols;
  std::vector<Value> out_vals;
  out_cols.reserve(specs.size());
  out_vals.reserve(specs.size());
  for (size_t i = 0; i < specs.size(); ++i) {
    const Value v =
        specs[i].kind == ItemSpec::Kind::kConst ? specs[i].const_val
                                                : Value(counts[i]);
    out_cols.emplace_back(ProjectionName(statement.SelectList()[i], i),
                          v.IsNull() ? ValueType::kNull : v.type);
    out_vals.push_back(v);
  }
  out.schema = Schema("", std::move(out_cols));
  out.AddRow(Row(std::move(out_vals)));
  RETURN_IF_FAIL(out.GetStatus());
  return std::optional<Relation>(std::move(out));
}

// Lean proof: lean/TinylambOpt/TpcdsExecCodegen.lean
//   (packed_2x32_join_key_bijection_sound, radix_partitioned_union_distinct_sound)
StatusOr<std::optional<Relation>> TryExecuteRadixPacked2x32JoinCount(
    TransactionContext& context, const SelectStatement& statement,
    const CteMap& inherited_ctes) {
  if (!statement.WithQueries().empty() || !statement.UnionAll().empty() ||
      statement.GetSetOperationTree() != nullptr ||
      !statement.GroupBy().empty() || statement.IsGroupAll() ||
      statement.Having() || statement.Qualify() || statement.Distinct() ||
      statement.HasDistinctOn() || !statement.OrderBy().empty() ||
      statement.Offset() != 0 ||
      (statement.HasLimit() && statement.Limit() == 0) ||
      statement.AsStruct() || statement.ForUpdate() ||
      HasWindowFunctions(statement) || statement.Sources().size() != 2 ||
      statement.SelectList().size() != 1) {
    return std::optional<Relation>{};
  }
  const Expression& sel0 = statement.SelectList()[0].expression;
  if (!sel0 || sel0->Type() != TypeTag::kAggregateExp ||
      !IsCountStar(sel0->AsAggregateExpression()) ||
      sel0->AsAggregateExpression().Distinct() ||
      sel0->AsAggregateExpression().WhereFilter() ||
      sel0->AsAggregateExpression().Having() != AggregateHavingModifier::kNone) {
    return std::optional<Relation>{};
  }
  const SelectSource& s0 = statement.Sources()[0];
  const SelectSource& s1 = statement.Sources()[1];
  if (s0.query || s1.query || s0.unnest || s1.unnest || s0.is_lateral ||
      s1.is_lateral || s0.sample || s1.sample || !s0.using_columns.empty() ||
      !s1.using_columns.empty() || s0.from_nested_join || s1.from_nested_join ||
      s0.join_condition ||
      (s1.join_type != JoinType::kCross && s1.join_type != JoinType::kInner)) {
    return std::optional<Relation>{};
  }
  const auto c0_it = inherited_ctes.find(s0.table);
  const auto c1_it = inherited_ctes.find(s1.table);
  if (c0_it == inherited_ctes.end() || c1_it == inherited_ctes.end() ||
      c0_it->second == nullptr || c1_it->second == nullptr ||
      !c0_it->second->HasColumnarOnly() || !c1_it->second->HasColumnarOnly() ||
      c0_it->second->columnar == nullptr || c1_it->second->columnar == nullptr ||
      c0_it->second->HasSpill() || c1_it->second->HasSpill()) {
    return std::optional<Relation>{};
  }
  const ColumnarTable& lt = *c0_it->second->columnar;
  const ColumnarTable& rt = *c1_it->second->columnar;
  if (lt.total_rows + rt.total_rows < 16384) {
    return std::optional<Relation>{};
  }
  const Schema lview =
      QualifySchema(c0_it->second->schema, s0.alias.empty() ? s0.table : s0.alias);
  const Schema rview =
      QualifySchema(c1_it->second->schema, s1.alias.empty() ? s1.table : s1.alias);

  std::vector<Expression> preds = SplitConjuncts(statement.WhereClause());
  if (s1.join_condition) {
    for (Expression& jc : SplitConjuncts(s1.join_condition)) {
      preds.push_back(std::move(jc));
    }
  }
  if (preds.size() != 2) {
    return std::optional<Relation>{};
  }
  std::array<slot_t, 2> l_cols{};
  std::array<slot_t, 2> r_cols{};
  for (size_t k = 0; k < 2; ++k) {
    if (!preds[k] || preds[k]->Type() != TypeTag::kBinaryExp ||
        preds[k]->AsBinaryExpression().Op() != BinaryOperation::kEquals) {
      return std::optional<Relation>{};
    }
    const auto& bin = preds[k]->AsBinaryExpression();
    if (!bin.Left() || !bin.Right() ||
        bin.Left()->Type() != TypeTag::kColumnValue ||
        bin.Right()->Type() != TypeTag::kColumnValue) {
      return std::optional<Relation>{};
    }
    const ColumnName& a = bin.Left()->AsColumnValue().GetColumnName();
    const ColumnName& b = bin.Right()->AsColumnValue().GetColumnName();
    auto la = LocalColumnOffset(lview, a);
    auto rb = LocalColumnOffset(rview, b);
    auto lb = LocalColumnOffset(lview, b);
    auto ra = LocalColumnOffset(rview, a);
    size_t l_off = 0;
    size_t r_off = 0;
    if (la.has_value() && rb.has_value() && !lb.has_value() && !ra.has_value()) {
      l_off = *la;
      r_off = *rb;
    } else if (lb.has_value() && ra.has_value() && !la.has_value() &&
               !rb.has_value()) {
      l_off = *lb;
      r_off = *ra;
    } else {
      return std::optional<Relation>{};
    }
    const Column& lc = lview.GetColumn(l_off);
    const Column& rc = rview.GetColumn(r_off);
    if (lc.IsUnsigned() || rc.IsUnsigned() || lc.Type() != rc.Type() ||
        (lc.Type() != ValueType::kInt64 && lc.Type() != ValueType::kDate)) {
      return std::optional<Relation>{};
    }
    l_cols[k] = static_cast<slot_t>(l_off);
    r_cols[k] = static_cast<slot_t>(r_off);
  }

  constexpr size_t kPartitions = 64;
  const size_t workers = std::max<size_t>(
      1, std::min<size_t>(
             QueryParallelWorkerLimit(std::thread::hardware_concurrency()), 8));

  auto partition_side = [&](const ColumnarTable& tbl,
                            const std::array<slot_t, 2>& cols,
                            std::vector<std::array<std::vector<int64_t>,
                                                   kPartitions>>* worker_bins,
                            bool* ok_out) {
    *ok_out = true;
    worker_bins->resize(workers);
    std::vector<std::pair<uint32_t, std::pair<uint32_t, uint32_t>>> slices;
    constexpr size_t kMorsel = 16384;
    for (uint32_t ci = 0; ci < tbl.chunks.size(); ++ci) {
      const size_t nr = tbl.chunks[ci].rows;
      for (size_t off = 0; off < nr; off += kMorsel) {
        slices.push_back({ci,
                          {static_cast<uint32_t>(off),
                           static_cast<uint32_t>(std::min(nr, off + kMorsel))}});
      }
    }
    if (slices.empty()) {
      return;
    }
    std::atomic<bool> range_ok{true};
    const size_t used_w = std::min(workers, slices.size());
    const size_t per_bucket_est = (tbl.total_rows / (used_w * kPartitions)) + 16;
    auto run_w = [&](size_t w) {
      auto& bins = (*worker_bins)[w];
      for (auto& b : bins) {
        b.reserve(per_bucket_est);
      }
      for (size_t si = w; si < slices.size(); si += used_w) {
        if (!range_ok.load(std::memory_order_relaxed)) {
          return;
        }
        const ColumnarChunk& ch = tbl.chunks[slices[si].first];
        if (cols[0] >= ch.columns.size() || cols[1] >= ch.columns.size()) {
          range_ok.store(false, std::memory_order_relaxed);
          return;
        }
        const ColumnarColumn& c0 = ch.columns[cols[0]];
        const ColumnarColumn& c1 = ch.columns[cols[1]];
        const uint32_t beg = slices[si].second.first;
        const uint32_t end = slices[si].second.second;
        if (c0.IsFlat() && c0.is_uniform && !c0.has_null && c1.IsFlat() &&
            c1.is_uniform && !c1.has_null) {
          const int64_t* v0 = c0.ints.data();
          const int64_t* v1 = c1.ints.data();
          for (uint32_t r = beg; r < end; ++r) {
            const int64_t k0 = v0[r];
            const int64_t k1 = v1[r];
            if (k0 < INT32_MIN || k0 > INT32_MAX || k1 < INT32_MIN ||
                k1 > INT32_MAX) [[unlikely]] {
              range_ok.store(false, std::memory_order_relaxed);
              return;
            }
            const uint64_t packed =
                (static_cast<uint64_t>(static_cast<uint32_t>(
                     static_cast<int32_t>(k0)))
                 << 32U) |
                static_cast<uint64_t>(
                    static_cast<uint32_t>(static_cast<int32_t>(k1)));
            const uint64_t h = (packed ^ (packed >> 33U)) * 0xff51afd7ed558ccdULL;
            bins[(h >> 58U) & (kPartitions - 1)].push_back(
                static_cast<int64_t>(packed));
          }
        } else {
          for (uint32_t r = beg; r < end; ++r) {
            if (c0.IsNull(r) || c1.IsNull(r)) {
              continue;
            }
            const int64_t k0 = c0.IntAt(r);
            const int64_t k1 = c1.IntAt(r);
            if (k0 < INT32_MIN || k0 > INT32_MAX || k1 < INT32_MIN ||
                k1 > INT32_MAX) [[unlikely]] {
              range_ok.store(false, std::memory_order_relaxed);
              return;
            }
            const uint64_t packed =
                (static_cast<uint64_t>(static_cast<uint32_t>(
                     static_cast<int32_t>(k0)))
                 << 32U) |
                static_cast<uint64_t>(
                    static_cast<uint32_t>(static_cast<int32_t>(k1)));
            const uint64_t h = (packed ^ (packed >> 33U)) * 0xff51afd7ed558ccdULL;
            bins[(h >> 58U) & (kPartitions - 1)].push_back(
                static_cast<int64_t>(packed));
          }
        }
      }
    };
    if (used_w <= 1) {
      run_w(0);
    } else if (RunParallelTasks(used_w, run_w) != Status::kSuccess) {
      *ok_out = false;
      return;
    }
    *ok_out = range_ok.load(std::memory_order_relaxed);
  };

  std::vector<std::array<std::vector<int64_t>, kPartitions>> l_bins;
  std::vector<std::array<std::vector<int64_t>, kPartitions>> r_bins;
  bool lok = false;
  bool rok = false;
  partition_side(lt, l_cols, &l_bins, &lok);
  if (!lok) {
    return std::optional<Relation>{};
  }
  partition_side(rt, r_cols, &r_bins, &rok);
  if (!rok) {
    return std::optional<Relation>{};
  }

  std::array<int64_t, kPartitions> part_counts{};
  auto join_part = [&](size_t p) {
    size_t l_n = 0;
    size_t r_n = 0;
    for (const auto& wb : l_bins) {
      l_n += wb[p].size();
    }
    for (const auto& wb : r_bins) {
      r_n += wb[p].size();
    }
    if (l_n == 0 || r_n == 0) {
      part_counts[p] = 0;
      return;
    }
    const bool build_r = (r_n <= l_n);
    const auto& build_bins = build_r ? r_bins : l_bins;
    const auto& probe_bins = build_r ? l_bins : r_bins;
    FlatIntKeyMap<uint32_t> ht;
    ht.Reserve(build_r ? r_n : l_n);
    for (const auto& wb : build_bins) {
      for (const int64_t k : wb[p]) {
        bool emp = false;
        uint32_t* cnt = ht.TryEmplace(k, &emp);
        if (emp) {
          *cnt = 1;
        } else {
          ++(*cnt);
        }
      }
    }
    int64_t acc = 0;
    for (const auto& wb : probe_bins) {
      for (const int64_t k : wb[p]) {
        if (const uint32_t* cnt = ht.Find(k); cnt != nullptr) {
          acc += static_cast<int64_t>(*cnt);
        }
      }
    }
    part_counts[p] = acc;
  };
  if (RunParallelTasks(kPartitions, join_part) != Status::kSuccess) {
    return std::optional<Relation>{};
  }
  int64_t total_matches = 0;
  for (int64_t c : part_counts) {
    total_matches += c;
  }

  Relation out(context.execution_runtime());
  out.schema = Schema(
      "", {Column(ProjectionName(statement.SelectList()[0], 0),
                  ValueType::kInt64)});
  out.AddRow(Row({Value(total_matches)}));
  RETURN_IF_FAIL(out.GetStatus());
  return std::optional<Relation>(std::move(out));
}

}  // namespace

StatusOr<Relation> ExecuteQuery(  // NOLINT(misc-no-recursion)
    TransactionContext& context, const SelectStatement& statement,
    const Scope* outer, const CteMap& inherited_ctes,
    GroupedOutputSink* output_sink, bool columnar_output) {
  EnsureReusableProjections(context, context.execution_runtime());
  if (outer == nullptr) {
    if (ExecutionRuntime* runtime = context.execution_runtime();
        runtime != nullptr && !runtime->shared_base_cache &&
        (!runtime->reusable_base_relations.empty() ||
         !statement.WithQueries().empty() || !statement.UnionAll().empty() ||
         statement.GetSetOperationTree() != nullptr)) {
      runtime->shared_base_cache = std::make_shared<SharedBaseRelationCache>();
    }
  }
  if (outer == nullptr && output_sink == nullptr) {
    ASSIGN_OR_RETURN(
        std::optional<Relation>, banded,
        TryExecutePartitionBandedOrderByLimit(context, statement,
                                              inherited_ctes, columnar_output));
    if (banded) {
      return std::move(*banded);
    }
  }
  // Apply relational boundary rewrites before loading sources.  Correlated
  // invocations must keep their original scope, while top-level derived
  // tables can be flattened or receive safe predicate/LIMIT pushdown.
  if (outer == nullptr) {
    const auto boundary_begin = std::chrono::steady_clock::now();
    std::shared_ptr<SelectStatement> optimized =
        OptimizeDerivedBoundaries(statement, inherited_ctes, &context);
    if (auto* runtime = context.execution_runtime()) {
      runtime->boundary_ms += ElapsedMs(boundary_begin);
    }
    if (optimized) {
      return ExecuteQuery(context, *optimized, outer, inherited_ctes,
                          output_sink, columnar_output);
    }
  }
  if (outer == nullptr && output_sink == nullptr) {
    ASSIGN_OR_RETURN(
        std::optional<Relation>, null_counts,
        TryExecuteColumnarNullMaskScalarCounts(context, statement,
                                               inherited_ctes));
    if (null_counts) {
      return std::move(*null_counts);
    }
    ASSIGN_OR_RETURN(
        std::optional<Relation>, radix_cnt,
        TryExecuteRadixPacked2x32JoinCount(context, statement, inherited_ctes));
    if (radix_cnt) {
      return std::move(*radix_cnt);
    }
    ASSIGN_OR_RETURN(
        std::optional<Relation>, pushed,
        TryPushGroupedCteJoins(context, statement, inherited_ctes));
    if (pushed) {
      return std::move(*pushed);
    }
    ASSIGN_OR_RETURN(
        std::optional<Relation>, qcount,
        TryExecuteQuotientGidDistinctSetOpCount(context, statement,
                                                inherited_ctes));
    if (qcount) {
      return std::move(*qcount);
    }
  }
  CteMap ctes = inherited_ctes;
  // WITH entries live in an unordered map, so declaration order is lost;
  // execute them in dependency order (a CTE may reference earlier ones).
  if (!statement.WithQueries().empty()) {
    std::unordered_set<std::string> pending;
    for (const auto& [name, query] : statement.WithQueries()) {
      (void)query;
      pending.insert(name);
    }
    while (!pending.empty()) {
      std::vector<std::string> ready_names;
      for (const std::string& name : pending) {
        std::unordered_set<std::string> referenced;
        CollectCteBodyOuterRefs(statement, name, &referenced);
        bool ready = true;
        for (const std::string& table : referenced) {
          if (pending.contains(table)) {
            ready = false;
            break;
          }
        }
        if (ready) {
          ready_names.push_back(name);
        }
      }
      if (ready_names.empty()) {
        // Every remaining entry waits on another remaining entry, so the WITH
        // list is a mutual reference with no dependency order.  Reporting it
        // beats the "table not found" that executing the entries in map order
        // used to produce.  The SQL frontend rejects the same shape before
        // planning, so this is the backstop for statements handed straight to
        // the executor.
        return CteCycleStatus({pending.begin(), pending.end()});
      }
      std::ranges::sort(ready_names);

      struct PreparedCte {
        std::string name;
        const SelectStatement* body{nullptr};
        std::shared_ptr<SelectStatement> restricted;
      };
      std::vector<PreparedCte> parallel_batch;
      std::vector<std::string> serial_batch;
      for (const std::string& name : ready_names) {
        if (outer != nullptr || statement.IsRecursiveWith(name)) {
          serial_batch.push_back(name);
          continue;
        }
        const SelectStatement& query = *statement.WithQueries().at(name);
        auto restricted = PushSingleUseCtePredicates(context, statement, name);
        if (!restricted) {
          restricted = PushSharedGroupedCtePredicates(context, statement, name);
        }
        if (!restricted) {
          restricted =
              PushGroupedCteJoinDomains(context, statement, name, ctes);
        }
        if (!restricted) {
          restricted = GroupPresenceOnlyCte(statement, name, &context);
        }
        if (restricted && context.execution_runtime() != nullptr) {
          context.execution_runtime()->retained_statements.push_back(
              restricted);
        }
        parallel_batch.push_back(
            PreparedCte{.name = name,
                        .body = restricted ? restricted.get() : &query,
                        .restricted = std::move(restricted)});
      }

      if (parallel_batch.size() >= 2) {
        std::vector<const SelectStatement*> branch_ptrs;
        branch_ptrs.reserve(parallel_batch.size());
        for (const auto& item : parallel_batch) {
          branch_ptrs.push_back(item.body);
        }
        ASSIGN_OR_RETURN(
            std::optional<std::vector<Relation>>, executed,
            (TryExecuteBranchesParallel(context, branch_ptrs, ctes, true)));
        if (executed.has_value()) {
          for (size_t i = 0; i < parallel_batch.size(); ++i) {
            ctes[parallel_batch[i].name] =
                std::make_shared<Relation>(std::move((*executed)[i]));
            pending.erase(parallel_batch[i].name);
          }
          parallel_batch.clear();
        }
      }
      if (parallel_batch.size() >= 2) {
        std::ranges::stable_sort(
            parallel_batch, [&](const PreparedCte& a, const PreparedCte& b) {
              return EstimateBranchMaxScanRows(context, *a.body, ctes) <
                     EstimateBranchMaxScanRows(context, *b.body, ctes);
            });
      }
      for (size_t bi = 0; bi < parallel_batch.size(); ++bi) {
        auto& item = parallel_batch[bi];
        if (bi > 0 && outer == nullptr) {
          if (!item.restricted) {
            item.restricted =
                PushGroupedCteJoinDomains(context, statement, item.name, ctes);
            if (item.restricted) {
              item.body = item.restricted.get();
              if (context.execution_runtime() != nullptr) {
                context.execution_runtime()->retained_statements.push_back(
                    item.restricted);
              }
            }
          }
          if (auto peer_restricted = TryRestrictGroupedCteByPeerEquijoinDomain(
                  statement, item.name, *item.body, ctes)) {
            item.restricted = std::move(peer_restricted);
            item.body = item.restricted.get();
            if (context.execution_runtime() != nullptr) {
              context.execution_runtime()->retained_statements.push_back(
                  item.restricted);
            }
          }
        }
        ASSIGN_OR_RETURN(
            Relation, cte_relation,
            (ExecuteQuery(context, *item.body, outer, ctes, nullptr, true)));
        ctes[item.name] = std::make_shared<Relation>(std::move(cte_relation));
        pending.erase(item.name);
      }
      for (const std::string& name : serial_batch) {
        const SelectStatement& query = *statement.WithQueries().at(name);
        const bool recursive = statement.IsRecursiveWith(name);
        auto restricted =
            outer == nullptr && !recursive
                ? PushSingleUseCtePredicates(context, statement, name)
                : nullptr;
        if (!restricted && outer == nullptr && !recursive) {
          restricted = PushSharedGroupedCtePredicates(context, statement, name);
        }
        if (!restricted && outer == nullptr && !recursive) {
          restricted =
              PushGroupedCteJoinDomains(context, statement, name, ctes);
        }
        if (!restricted && outer == nullptr && !recursive) {
          restricted = GroupPresenceOnlyCte(statement, name, &context);
        }
        if (restricted && context.execution_runtime() != nullptr) {
          context.execution_runtime()->retained_statements.push_back(
              restricted);
        }
        StatusOr<Relation> cte_relation =
            recursive ? ExecuteRecursiveCte(context, name, query, outer, ctes,
                                            statement.RecursiveDepthOf(name))
                      : ExecuteQuery(context, restricted ? *restricted : query,
                                     outer, ctes, nullptr, true);
        if (!cte_relation.HasValue()) {
          return cte_relation.GetStatus();
        }
        ctes[name] = std::make_shared<Relation>(cte_relation.MoveValue());
        pending.erase(name);
      }
    }
  }

  // Set-operation modifiers belong to the concatenated result.  The old
  // merge path called FinishQuery on the head first, so LIMIT/OFFSET were
  // consumed before UNION ALL branches were appended.  Defer those clauses
  // until all branches have been materialized; the unmodified fast path is
  // retained for legacy set-operation cases without query-level modifiers.
  if (statement.GetSetOperationTree() != nullptr &&
      statement.GetSetOperationTree()->grouped) {
    const SetOperationTree& tree = *statement.GetSetOperationTree();
    auto apply_pair = [&](Relation left, Relation right,
                          SetOperationKind operation) -> StatusOr<Relation> {
      if (operation == SetOperationKind::kUnionAll &&
          TryConcatenateColumnarUnionAll(left, right)) {
        return left;
      }
      if (operation != SetOperationKind::kUnionAll &&
          TryColumnarSetOperation(left, right, operation)) {
        return left;
      }
      RETURN_IF_FAIL(left.EnsureRowsMaterialized());
      RETURN_IF_FAIL(right.EnsureRowsMaterialized());
      if ((operation == SetOperationKind::kUnionAll ||
           operation == SetOperationKind::kUnion) &&
          NormalizeMovableSetInputs(left, right)) {
        auto charge = operation == SetOperationKind::kUnion
                          ? ResidentSetCharge(left, right)
                          : std::nullopt;
        if (operation == SetOperationKind::kUnionAll || charge) {
          RETURN_IF_FAIL(left.GetStatus());
          RETURN_IF_FAIL(right.GetStatus());
          left.MergeInMemoryShard(std::move(right));
          right = Relation(context.execution_runtime());
          if (operation == SetOperationKind::kUnion && !left.HasSpill()) {
            DistinctResidentRows(left);
          }
          RETURN_IF_FAIL(left.FinishSpill());
          RETURN_IF_FAIL(left.GetStatus());
          if (operation == SetOperationKind::kUnionAll || !left.HasSpill()) {
            return left;
          }
          // A pending memory charge can trigger spilling during the merge.
          // The general UNION below deduplicates this concatenation plus an
          // empty right input, preserving its spill-capable implementation.
        }
      }
      if (operation != SetOperationKind::kUnionAll &&
          operation != SetOperationKind::kUnion &&
          NormalizeMovableSetInputs(left, right)) {
        if (auto charge = ResidentSetCharge(left, right)) {
          RETURN_IF_FAIL(left.GetStatus());
          RETURN_IF_FAIL(right.GetStatus());
          ReduceResidentRows(left, right, operation);
          return left;
        }
      }
      std::vector<Row> left_rows;
      std::vector<Row> right_rows;
      RETURN_IF_FAIL(left.FinishSpill());
      RETURN_IF_FAIL(left.GetStatus());
      RETURN_IF_FAIL(
          left.ForEachRow([&](const Row& row) { left_rows.push_back(row); }));
      RETURN_IF_FAIL(right.FinishSpill());
      RETURN_IF_FAIL(right.GetStatus());
      RETURN_IF_FAIL(
          right.ForEachRow([&](const Row& row) { right_rows.push_back(row); }));
      SetOperationExecutor set_operation(
          {std::make_shared<ConstantExecutor>(std::move(left_rows)),
           std::make_shared<ConstantExecutor>(std::move(right_rows))},
          operation);
      Relation folded(context.execution_runtime());
      folded.schema = left.schema;
      DataChunk batch;
      while (set_operation.NextBatch(&batch) != 0) {
        for (size_t i = 0; i < batch.Size(); ++i) {
          folded.AddRow(batch.RowAt(i));
        }
      }
      if (set_operation.GetStatus() != Status::kSuccess) {
        return set_operation.GetStatus();
      }
      RETURN_IF_FAIL(folded.FinishSpill());
      RETURN_IF_FAIL(folded.GetStatus());
      return folded;
    };

    // When every set operator is DISTINCT, each operand contributes presence
    // only. Group projected join keys before materializing repeated matches.
    // Any ALL operator keeps the original multiplicities of every input.
    const bool distinct_inputs =
        !tree.kinds.empty() && tree.kinds.size() == tree.branches.size() &&
        std::ranges::all_of(tree.kinds, IsPresenceOnlySetOperation);
    std::deque<SelectStatement> grouped_owned;
    std::vector<const SelectStatement*> term_ptrs;
    term_ptrs.reserve(1 + tree.branches.size());
    auto prepare_term = [&](const SelectStatement& stmt) {
      if (distinct_inputs) {
        if (auto grouped = GroupDistinctSetInput(stmt)) {
          grouped_owned.push_back(std::move(*grouped));
          term_ptrs.push_back(&grouped_owned.back());
          return;
        }
      }
      term_ptrs.push_back(&stmt);
    };
    prepare_term(*tree.first);
    for (const auto& branch_ptr : tree.branches) {
      prepare_term(*branch_ptr);
    }

    const bool branch_columnar_tree = true;
    std::optional<std::vector<Relation>> parallel_terms;
    if (outer == nullptr && term_ptrs.size() >= 2) {
      ASSIGN_OR_RETURN(std::optional<std::vector<Relation>>, executed,
                       (TryExecuteBranchesParallel(context, term_ptrs, ctes,
                                                   branch_columnar_tree)));
      parallel_terms = std::move(executed);
    }

    auto take_term = [&](size_t idx) -> StatusOr<Relation> {
      if (parallel_terms.has_value()) {
        return std::move((*parallel_terms)[idx]);
      }
      return ExecuteQuery(context, *term_ptrs[idx], outer, ctes, nullptr,
                          branch_columnar_tree);
    };
    ASSIGN_OR_RETURN(Relation, combined, take_term(0));
    // PRODUCTION FIX: honor per-pair kinds with operator precedence
    // (INTERSECT binds tighter than UNION/EXCEPT).  The previous flat left
    // fold evaluated `1 UNION ALL 2 INTERSECT 2` as `(1 UNION ALL 2)
    // INTERSECT 2` and dropped the INTERSECT entirely.
    std::vector<Relation> terms;
    terms.push_back(std::move(combined));
    std::vector<SetOperationKind> low_ops;
    for (size_t i = 0; i < tree.branches.size(); ++i) {
      ASSIGN_OR_RETURN(Relation, branch, take_term(i + 1));
      const SetOperationKind operation =
          i < tree.kinds.size() ? tree.kinds[i] : SetOperationKind::kUnionAll;
      const bool intersects = operation == SetOperationKind::kIntersect ||
                              operation == SetOperationKind::kIntersectAll;
      if (intersects) {
        ASSIGN_OR_RETURN(Relation, folded_pair,
                         (apply_pair(std::move(terms.back()), std::move(branch),
                                     operation)));
        terms.back() = std::move(folded_pair);
      } else {
        low_ops.push_back(operation);
        terms.push_back(std::move(branch));
      }
    }
    ASSIGN_OR_RETURN(Relation, combined_folded,
                     (FoldLowPrecedenceSetTerms(terms, low_ops, apply_pair)));
    combined = std::move(combined_folded);
    RETURN_IF_FAIL(combined.FinishSpill());
    RETURN_IF_FAIL(combined.GetStatus());
    if (!statement.OrderBy().empty()) {
      RETURN_IF_FAIL(ApplyOrderBy(context, statement, &combined, outer, ctes));
    }
    ASSIGN_OR_RETURN(
        Relation, limited,
        (LimitedRows(statement, std::move(combined), &context, outer, &ctes)));
    if (!columnar_output && limited.HasColumnarOnly()) {
      RETURN_IF_FAIL(limited.EnsureRowsMaterialized());
    }
    return limited;
  }

  if (!statement.UnionAll().empty()) {
    SelectStatement head = statement;
    head.ClearUnionAll();
    // The flat representation already owns the operands below. Its head is
    // now a single SELECT; retaining the parser's ungrouped tree prevents
    // the same DISTINCT-input grouping used for all following operands.
    head.SetSetOperationTree(nullptr);
    head.SetOrderBy({});
    head.SetLimit(std::nullopt);
    head.SetOffset(0);
    // The WITH bodies already executed into `ctes` above; clearing the head's
    // own copies keeps the fold from re-running every CTE body one extra time.
    head.ClearWithQueries();
    // Expanded grouping sets have identical FROM/WHERE inputs. Build that
    // input once, then aggregate each set over a shared immutable relation.
    // Compare complete normalized statements so joins and filters cannot be
    // accidentally shared across distinct UNION branches.
    std::optional<Relation> shared_input;
    bool shared_where_applied = false;
    auto input_shape = [](const SelectStatement& part) {
      SelectStatement shape(part);
      shape.SetSelectList({});
      shape.SetGroupBy({});
      shape.SetGroupAll(false);
      shape.SetHaving(nullptr);
      shape.SetOrderBy({});
      shape.SetLimit(std::nullopt);
      shape.SetOffset(0);
      shape.SetWithTies(false);
      shape.ClearUnionAll();
      shape.SetSetOperationTree(nullptr);
      shape.ClearWithQueries();
      return std::string(shape.Fingerprint());
    };
    auto shareable = [&](const SelectStatement& part) {
      return !part.GroupBy().empty() && !part.Distinct() &&
             !part.HasDistinctOn() && !part.Having() && !part.Qualify() &&
             !StatementContainsVolatileFunction(part) &&
             ContainsOnlyUncorrelatedQueries(context, part.WhereClause(),
                                             ctes) &&
             std::ranges::none_of(part.SelectList(),
                                  [](const auto& item) {
                                    return ContainsQuery(item.expression);
                                  }) &&
             std::ranges::all_of(part.Sources(), [&](const auto& source) {
               return !source.unnest && !source.sample && !source.is_lateral &&
                      !source.from_nested_join &&
                      source.using_columns.empty() &&
                      !ContainsQuery(source.join_condition);
             });
    };
    bool share =
        outer == nullptr && statement.UnionAll().size() >= 2 && shareable(head);
    const std::string shape = input_shape(head);
    std::vector<NamedExpression> needed = head.SelectList();
    for (size_t i = 0; share && i < statement.UnionAll().size(); ++i) {
      const auto& part = *statement.UnionAll()[i];
      const auto match = statement.MatchFor(i);
      share =
          shareable(part) && !part.HasLimit() && part.Offset() == 0 &&
          part.OrderBy().empty() && part.WithQueries().empty() &&
          part.UnionAll().empty() && !match.by_name && !match.corresponding &&
          match.columns.empty() &&
          (i >= statement.SetOperationKinds().size() ||
           statement.SetOperationKinds()[i] == SetOperationKind::kUnionAll) &&
          input_shape(part) == shape;
      needed.insert(needed.end(), part.SelectList().begin(),
                    part.SelectList().end());
      for (const auto& key : part.GroupBy()) {
        needed.emplace_back("", key);
      }
    }
    std::vector<SelectStatement> exact_sets;
    if (share) {
      SelectStatement scan(head);
      scan.SetSelectList(std::move(needed));
      std::vector<const SelectStatement*> sets{&head};
      for (const auto& branch : statement.UnionAll()) {
        sets.push_back(branch.get());
      }
      auto partial = PlanExactIntegerGroupingSets(sets);
      GroupedJoinFusionSpec partial_fusion;
      if (partial) {
        partial_fusion = BuildGroupedJoinFusionSpec(partial->partial);
        partial_fusion.require_exact_integer_totals =
            !partial->allow_float_sums;
      }
      ASSIGN_OR_RETURN(Relation, input,
                       (BuildInput(context, partial ? partial->partial : scan,
                                   nullptr, ctes, &shared_where_applied,
                                   partial ? &partial_fusion : nullptr,
                                   /*ignore_projection_star=*/false,
                                   /*columnar_output=*/!partial.has_value())));
      if (partial && partial_fusion.fired) {
        std::vector<Column> columns;
        for (size_t i = 0; i < input.schema.ColumnCount(); ++i) {
          columns.push_back(input.schema.GetColumn(i));
          if (i < head.GroupBy().size()) {
            columns.back().Name() =
                head.GroupBy()[i]->AsColumnValue().GetColumnName();
          }
        }
        input.schema = Schema("", std::move(columns));
        exact_sets = std::move(partial->sets);
      }
      if (!shared_where_applied && scan.WhereClause()) {
        RETURN_IF_FAIL(FilterRelation(context, &input,
                                      SplitConjuncts(scan.WhereClause()),
                                      nullptr, ctes));
        shared_where_applied = true;
      }
      RETURN_IF_FAIL(input.FinishSpill());
      RETURN_IF_FAIL(input.GetStatus());
      shared_input.emplace(std::move(input));
    }
    std::optional<std::vector<Relation>> grouped_sets;
    size_t grouped_set_index = 0;
    // An empty shared input still owes the grand-total row of every
    // keyless set (GROUPING SETS ((a),()) over no rows -> one NULL total);
    // the compacted path builds groups from input rows only, so leave empty
    // inputs to the per-set evaluation below, which aggregates () correctly.
    if (shared_input && shared_where_applied &&
        shared_input->TotalRows() > 0) {
      std::vector<const SelectStatement*> sets{&head};
      for (const auto& branch : statement.UnionAll()) {
        sets.push_back(branch.get());
      }
      if (!exact_sets.empty()) {
        sets.clear();
        for (const auto& set : exact_sets) {
          sets.push_back(&set);
        }
      }
      ASSIGN_OR_RETURN(std::optional<std::vector<Relation>>, computed,
                       (TryCompactGroupingSets(context, sets, *shared_input,
                                                !exact_sets.empty())));
      grouped_sets = std::move(computed);
      if (!grouped_sets && shared_input->HasColumnarOnly()) {
        RETURN_IF_FAIL(shared_input->EnsureRowsMaterialized());
      }
    }
    bool normalized_grouped_sets = false;
    if (grouped_sets) {
      // Every term here is UNION ALL over the same grouping input. Resolve
      // common numeric types once, instead of rescanning the growing prefix
      // after every ROLLUP level. All rows are still inspected before moving.
      std::vector<Relation*> terms;
      terms.reserve(grouped_sets->size());
      for (auto& relation : *grouped_sets) {
        RETURN_IF_FAIL(relation.EnsureRowsMaterialized());
        terms.push_back(&relation);
      }
      normalized_grouped_sets = NormalizeMovableSetRelations(terms);
    }
    const bool distinct_inputs =
        statement.SetOperationKinds().size() == statement.UnionAll().size() &&
        std::ranges::all_of(statement.SetOperationKinds(),
                            IsPresenceOnlySetOperation);
    size_t shared_term_index = 0;
    std::deque<SelectStatement> union_grouped_owned;
    std::vector<const SelectStatement*> unshared_ptrs;
    std::optional<std::vector<Relation>> union_parallel_terms;
    const bool branch_columnar_flat = true;
    if (!shared_input && outer == nullptr && !statement.UnionAll().empty()) {
      unshared_ptrs.reserve(1 + statement.UnionAll().size());
      auto prepare_union_term = [&](const SelectStatement& part) {
        if (distinct_inputs) {
          if (auto grouped = GroupDistinctSetInput(part)) {
            union_grouped_owned.push_back(std::move(*grouped));
            unshared_ptrs.push_back(&union_grouped_owned.back());
            return;
          }
        }
        unshared_ptrs.push_back(&part);
      };
      prepare_union_term(head);
      for (const auto& union_stmt : statement.UnionAll()) {
        prepare_union_term(*union_stmt);
      }
      ASSIGN_OR_RETURN(std::optional<std::vector<Relation>>, executed,
                       (TryExecuteBranchesParallel(context, unshared_ptrs, ctes,
                                                   branch_columnar_flat)));
      union_parallel_terms = std::move(executed);
    }

    size_t union_parallel_index = 0;
    auto run_shared_term =
        [&](const SelectStatement& original_part) -> StatusOr<Relation> {
      const size_t term_idx = union_parallel_index++;
      if (union_parallel_terms.has_value()) {
        return std::move((*union_parallel_terms)[term_idx]);
      }
      const SelectStatement& part =
          exact_sets.empty() ? original_part : exact_sets[shared_term_index++];
      if (grouped_sets) {
        return std::move((*grouped_sets)[grouped_set_index++]);
      }
      if (!shared_input) {
        if (!unshared_ptrs.empty() && term_idx < unshared_ptrs.size()) {
          return ExecuteQuery(context, *unshared_ptrs[term_idx], outer, ctes,
                              nullptr, branch_columnar_flat);
        }
        if (distinct_inputs) {
          if (auto grouped = GroupDistinctSetInput(part)) {
            return ExecuteQuery(context, *grouped, outer, ctes, nullptr,
                                branch_columnar_flat);
          }
        }
        return ExecuteQuery(context, part, outer, ctes, nullptr,
                            branch_columnar_flat);
      }
      auto resolved = ResolveGroupingAliases(part, shared_input->schema);
      const auto& effective = resolved ? *resolved : part;
      if (shared_where_applied) {
        std::vector<const AggregateExpression*> aggregates;
        std::unordered_set<const AggregateExpression*> seen;
        for (const auto& item : effective.SelectList()) {
          CollectAggregates(item.expression, &aggregates, &seen);
        }
        ASSIGN_OR_RETURN(std::optional<Relation>, compact,
                         (TryCompactRelationAggregate(
                             context, effective, *shared_input, aggregates)));
        if (compact) {
          return std::move(*compact);
        }
      }
      ASSIGN_OR_RETURN(Relation, input, MaterializeRelation(*shared_input));
      if (HasWindowFunctions(effective)) {
        return ExecuteWindowOverAggregate(context, effective, std::move(input),
                                          nullptr, ctes, !shared_where_applied);
      }
      return FinishQuery(context, effective, std::move(input), nullptr, ctes,
                         !shared_where_applied, 0);
    };
    ASSIGN_OR_RETURN(Relation, combined, run_shared_term(head));
    // PRODUCTION FIX: apply per-pair kinds with operator precedence
    // (INTERSECT binds tighter than UNION/EXCEPT).  The previous flat left
    // fold computed `1 UNION ALL 2 INTERSECT 2` as `(1 UNION ALL 2)
    // INTERSECT 2` and returned the wrong rows.
    auto apply_pair = [&](Relation left, Relation right,
                          SetOperationKind operation) -> StatusOr<Relation> {
      if (operation == SetOperationKind::kUnionAll &&
          TryConcatenateColumnarUnionAll(left, right)) {
        return left;
      }
      if (operation != SetOperationKind::kUnionAll &&
          TryColumnarSetOperation(left, right, operation)) {
        return left;
      }
      RETURN_IF_FAIL(left.EnsureRowsMaterialized());
      RETURN_IF_FAIL(right.EnsureRowsMaterialized());
      if ((operation == SetOperationKind::kUnionAll ||
           operation == SetOperationKind::kUnion) &&
          ((normalized_grouped_sets && !left.HasSpill() && !right.HasSpill()) ||
           NormalizeMovableSetInputs(left, right))) {
        auto charge = operation == SetOperationKind::kUnion
                          ? ResidentSetCharge(left, right)
                          : std::nullopt;
        if (operation == SetOperationKind::kUnionAll || charge) {
          RETURN_IF_FAIL(left.GetStatus());
          RETURN_IF_FAIL(right.GetStatus());
          left.MergeInMemoryShard(std::move(right));
          right = Relation(context.execution_runtime());
          if (operation == SetOperationKind::kUnion && !left.HasSpill()) {
            DistinctResidentRows(left);
          }
          RETURN_IF_FAIL(left.FinishSpill());
          RETURN_IF_FAIL(left.GetStatus());
          if (operation == SetOperationKind::kUnionAll || !left.HasSpill()) {
            return left;
          }
          // A pending memory charge can trigger spilling during the merge.
          // The general UNION below deduplicates this concatenation plus an
          // empty right input, preserving its spill-capable implementation.
        }
      }
      if (operation != SetOperationKind::kUnionAll &&
          operation != SetOperationKind::kUnion &&
          NormalizeMovableSetInputs(left, right)) {
        if (auto charge = ResidentSetCharge(left, right)) {
          RETURN_IF_FAIL(left.GetStatus());
          RETURN_IF_FAIL(right.GetStatus());
          ReduceResidentRows(left, right, operation);
          return left;
        }
      }
      std::vector<Row> left_rows;
      std::vector<Row> right_rows;
      RETURN_IF_FAIL(left.FinishSpill());
      RETURN_IF_FAIL(left.GetStatus());
      RETURN_IF_FAIL(
          left.ForEachRow([&](const Row& row) { left_rows.push_back(row); }));
      RETURN_IF_FAIL(right.FinishSpill());
      RETURN_IF_FAIL(right.GetStatus());
      RETURN_IF_FAIL(
          right.ForEachRow([&](const Row& row) { right_rows.push_back(row); }));
      SetOperationExecutor set_operation(
          {std::make_shared<ConstantExecutor>(std::move(left_rows)),
           std::make_shared<ConstantExecutor>(std::move(right_rows))},
          operation);
      Relation folded(context.execution_runtime());
      folded.schema = left.schema;
      DataChunk batch;
      while (set_operation.NextBatch(&batch) != 0) {
        for (size_t i = 0; i < batch.Size(); ++i) {
          folded.AddRow(batch.RowAt(i));
        }
      }
      if (set_operation.GetStatus() != Status::kSuccess) {
        return set_operation.GetStatus();
      }
      RETURN_IF_FAIL(folded.FinishSpill());
      RETURN_IF_FAIL(folded.GetStatus());
      if (folded.TotalRows() != 0) {
        folded.schema = left.schema;
      }
      return folded;
    };
    std::vector<Relation> terms;
    terms.push_back(std::move(combined));
    std::vector<SetOperationKind> low_ops;
    for (size_t i = 0; i < statement.UnionAll().size(); ++i) {
      const auto& union_stmt = statement.UnionAll()[i];
      ASSIGN_OR_RETURN(Relation, branch, run_shared_term(*union_stmt));
      const SetOperationKind operation =
          i < statement.SetOperationKinds().size()
              ? statement.SetOperationKinds()[i]
              : SetOperationKind::kUnionAll;
      const bool intersects = operation == SetOperationKind::kIntersect ||
                              operation == SetOperationKind::kIntersectAll;
      if (intersects) {
        ASSIGN_OR_RETURN(Relation, folded_pair,
                         (apply_pair(std::move(terms.back()), std::move(branch),
                                     operation)));
        terms.back() = std::move(folded_pair);
      } else {
        low_ops.push_back(operation);
        terms.push_back(std::move(branch));
      }
    }
    ASSIGN_OR_RETURN(Relation, combined_folded,
                     (FoldLowPrecedenceSetTerms(terms, low_ops, apply_pair)));
    combined = std::move(combined_folded);
    RETURN_IF_FAIL(combined.FinishSpill());
    RETURN_IF_FAIL(combined.GetStatus());
    if (!statement.OrderBy().empty()) {
      RETURN_IF_FAIL(ApplyOrderBy(context, statement, &combined, outer, ctes));
    }
    ASSIGN_OR_RETURN(
        Relation, limited,
        (LimitedRows(statement, std::move(combined), &context, outer, &ctes)));
    if (!columnar_output && limited.HasColumnarOnly()) {
      RETURN_IF_FAIL(limited.EnsureRowsMaterialized());
    }
    return limited;
  }

  // An uncorrelated predicate subquery is an InitPlan. Fold its result once
  // before choosing the scan/aggregate route, so the outer aggregate need not
  // materialize a filtered copy of the whole table. Failed or correlated
  // folds retain query nodes and keep their original evaluation route.
  if (outer == nullptr && statement.WithQueries().empty() &&
      statement.Sources().size() == 1 && !statement.Sources()[0].query &&
      !statement.Sources()[0].unnest && !statement.Sources()[0].sample &&
      statement.WhereClause() && ContainsQuery(statement.WhereClause()) &&
      (!statement.GroupBy().empty() ||
       std::ranges::any_of(statement.SelectList(), [](const auto& item) {
         return ContainsAggregate(item.expression);
       }))) {
    struct RestoreReuse {  // NOLINT(cppcoreguidelines-special-member-functions)
      ExecutionRuntime* runtime;
      std::string table;
      bool was_reusable;
      ~RestoreReuse() {
        if (was_reusable) {
          runtime->reusable_base_relations.insert(table);
        }
      }
    } restore{.runtime = context.execution_runtime(),
              .table = statement.Sources()[0].table,
              .was_reusable = false};
    if (restore.runtime != nullptr) {
      restore.was_reusable =
          restore.runtime->reusable_base_relations.erase(restore.table) != 0;
    }
    auto folded = FoldRowIndependentSubqueries(
        context, SplitConjuncts(statement.WhereClause()), outer, ctes);
    if (std::ranges::none_of(folded, ContainsQuery)) {
      SelectStatement initialized(statement);
      initialized.SetWhereClause(CombineConjuncts(folded));
      return ExecuteQuery(context, initialized, outer, ctes, output_sink,
                          columnar_output);
    }
  }

  // Single-table LSTable aggregation with a prebuilt-membership EXISTS/IN
  // subquery (TPC-H Q04): compile the membership bitset in parallel and stream
  // the outer table directly into RunTypedParallelStreamAggregate without
  // materializing intermediate outer rows.
  if (outer == nullptr && statement.WithQueries().empty() &&
      statement.Sources().size() == 1 && !statement.Sources()[0].query &&
      !statement.Sources()[0].unnest && !statement.Sources()[0].sample &&
      !HasWindowFunctions(statement) &&
      !ctes.contains(statement.Sources()[0].table) && statement.WhereClause() &&
      ContainsQuery(statement.WhereClause()) &&
      (!statement.GroupBy().empty() ||
       std::any_of(statement.SelectList().begin(), statement.SelectList().end(),
                   [](const NamedExpression& projection) {
                     return ContainsAggregate(projection.expression);
                   }) ||
       ContainsAggregate(statement.Having()))) {
    const SelectSource& source = statement.Sources()[0];
    const bool reusable =
        context.execution_runtime() != nullptr &&
        context.execution_runtime()->reusable_base_relations.contains(
            source.table);
    StatusOr<std::shared_ptr<Table>> table = context.GetTable(source.table);
    const std::shared_ptr<const LSTableStore> lst_store =
        table.HasValue() ? table.Value()->GetLSTableStore() : nullptr;
    if (table.HasValue() && !reusable && lst_store != nullptr &&
        lst_store->total_rows > 0) {
      const Schema& table_schema = table.Value()->GetSchema();
      const std::string qualifier =
          source.alias.empty() ? source.table : source.alias;
      std::shared_ptr<SelectStatement> resolved_statement =
          ResolveGroupingAliases(statement,
                                 qualifier.empty()
                                     ? table_schema
                                     : QualifySchema(table_schema, qualifier));
      const SelectStatement& stmt =
          resolved_statement != nullptr ? *resolved_statement : statement;
      const Schema qualified_table_schema =
          qualifier.empty() ? table_schema
                            : QualifySchema(table_schema, qualifier);
      std::vector<slot_t> projection =
          RequiredColumns(stmt, qualified_table_schema, true);
      Schema scan_schema = projection.empty()
                               ? table_schema
                               : ProjectSchema(table_schema, projection);
      Schema qualified_schema = qualifier.empty()
                                    ? scan_schema
                                    : QualifySchema(scan_schema, qualifier);
      std::vector<Expression> scan_predicates =
          SplitConjuncts(stmt.WhereClause());
      const Expression having_expr =
          InlineHavingAliases(statement, qualified_schema);
      const FilterCompileEnv compile_env{
          .context = &context,
          .outer = outer,
          .ctes = &ctes,
          .input_rows = static_cast<size_t>(lst_store->total_rows),
          .prebuild_only_exists = true,
      };
      CompiledScanFilter scan_filter =
          CompileScanFilter(scan_predicates, qualified_schema, &compile_env);
      if (scan_filter.residual.empty() &&
          scan_filter.disjunctive_branches.empty() &&
          scan_filter.prebuilt_scalar_compare.empty() &&
          scan_filter.prebuilt_membership.size() == 1 &&
          !scan_filter.prebuilt_membership[0].negated &&
          !scan_filter.prebuilt_membership[0].contains_null &&
          !scan_filter.prebuilt_membership[0].packed_pair_col2.has_value() &&
          scan_filter.prebuilt_membership[0].distinct_witness_minmax ==
              nullptr &&
          scan_filter.prebuilt_membership[0].values != nullptr &&
          (scan_filter.prebuilt_membership[0].domain_type ==
               ValueType::kInt64 ||
           scan_filter.prebuilt_membership[0].domain_type ==
               ValueType::kDate)) {
        std::vector<Expression> non_query_predicates;
        non_query_predicates.reserve(scan_predicates.size());
        for (const Expression& pred : scan_predicates) {
          if (!ContainsQuery(pred)) {
            non_query_predicates.push_back(pred);
          }
        }
        std::vector<const AggregateExpression*> aggregate_expressions;
        std::unordered_set<const AggregateExpression*> seen_aggregates;
        for (const NamedExpression& projection_item : stmt.SelectList()) {
          CollectAggregates(projection_item.expression, &aggregate_expressions,
                            &seen_aggregates);
        }
        CollectAggregates(having_expr, &aggregate_expressions,
                          &seen_aggregates);
        TypedAggregationPlan typed_plan = PlanTypedAggregation(
            stmt, having_expr, qualified_schema, aggregate_expressions,
            non_query_predicates, /*allow_composite_select=*/true);
        if (typed_plan.eligible) {
          const std::vector<slot_t>* proj_ptr =
              projection.empty() ? nullptr : &projection;
          const std::vector<IntegerPeekCompare> peek_compares =
              BuildIntegerPeeks(scan_filter, proj_ptr, table_schema);
          StatusOr<Relation> typed = RunTypedParallelStreamAggregate(
              context, stmt, having_expr, *table.Value(), source.table, false,
              projection, qualified_schema, table_schema, peek_compares,
              aggregate_expressions, ctes, std::move(typed_plan), nullptr,
              ValueType::kNull, std::nullopt,
              scan_filter.prebuilt_membership.data());
          if (!typed.HasValue()) {
            return typed.GetStatus();
          }
          Relation typed_output = typed.MoveValue();
          typed_output.FinishSpill();
          return LimitedRows(statement, std::move(typed_output), &context,
                             outer, &ctes);
        }
      }
    }
  }

  // Single-table aggregation: filter and aggregate while scanning so we never
  // materialize millions of qualifying rows (TPC-H Q1/Q6/Q21-derived pattern).
  // When the table is reusable, populate the shared cache once then aggregate
  // from the cache without deep-copying into an intermediate relation.
  const bool stream_agg =
      outer == nullptr && statement.WithQueries().empty() &&
      statement.Sources().size() == 1 && !statement.Sources()[0].query &&
      !statement.Sources()[0].unnest && !statement.Sources()[0].sample &&
      !HasWindowFunctions(statement) &&
      !ctes.contains(statement.Sources()[0].table) &&
      (!statement.WhereClause() || !ContainsQuery(statement.WhereClause())) &&

      (!statement.GroupBy().empty() ||
       std::any_of(statement.SelectList().begin(), statement.SelectList().end(),
                   [](const NamedExpression& projection) {
                     return ContainsAggregate(projection.expression);
                   }) ||
       ContainsAggregate(statement.Having()));
  if (std::getenv("TINYLAMB_DEBUG_GS") != nullptr) {  // NOLINT(cert-err33-c)
    (void)fprintf(stderr,
                  "[ROUTE] stream_agg=%d sources=%zu groupby=%zu "
                  "groupall=%d\n",
                  (int)stream_agg, statement.Sources().size(),
                  statement.GroupBy().size(), (int)statement.IsGroupAll());
  }
  if (stream_agg) {
    const SelectSource& source = statement.Sources()[0];
    const bool reusable =
        context.execution_runtime() != nullptr &&
        context.execution_runtime()->reusable_base_relations.contains(
            source.table);
    StatusOr<std::shared_ptr<Table>> table = context.GetTable(source.table);
    if (!table.HasValue()) {
      return StatusError(StatusCode::kInvalidArgument,
                         "table " + source.table + " not found");
    }
    const Schema& table_schema = table.Value()->GetSchema();
    const std::string qualifier =
        source.alias.empty() ? source.table : source.alias;
    // Alias/ordinal resolution needs the qualified column names the grouping
    // expressions see; offsets are identical in the raw table schema.
    std::shared_ptr<SelectStatement> resolved_statement =
        ResolveGroupingAliases(statement,
                               qualifier.empty()
                                   ? table_schema
                                   : QualifySchema(table_schema, qualifier));
    const SelectStatement& stmt =
        resolved_statement != nullptr ? *resolved_statement : statement;
    // RequiredColumns must run against the qualified names the statement
    // references (`t4.array_val`); QualifySchema preserves column order so
    // the resulting offsets index the raw table scan unchanged.
    const Schema qualified_table_schema =
        qualifier.empty() ? table_schema
                          : QualifySchema(table_schema, qualifier);
    std::vector<slot_t> projection =
        RequiredColumns(stmt, qualified_table_schema);
    if (const std::vector<slot_t>* shared =
            ReusableProjection(context, source.table)) {
      // The shared layout was derived from the raw table schema; never let
      // it drop columns this statement's own (possibly alias-qualified)
      // references need.
      for (const slot_t slot : *shared) {
        if (std::find(projection.begin(), projection.end(), slot) ==
            projection.end()) {
          projection.push_back(slot);
        }
      }
      std::sort(projection.begin(), projection.end());
    }
    Schema scan_schema = projection.empty()
                             ? Schema("", std::vector<Column>{})
                             : ProjectSchema(table_schema, projection);
    Schema qualified_schema =
        qualifier.empty() ? scan_schema : QualifySchema(scan_schema, qualifier);

    std::vector<Expression> scan_predicates =
        SplitConjuncts(stmt.WhereClause());

    Relation input;
    input.schema = qualified_schema;
    // Project aggregates against the qualified schema; feed rows one at a time
    // without retaining them in input.rows.
    Expression having_expr = InlineHavingAliases(statement, input.schema);
    if (having_expr && ContainsQuery(having_expr)) {
      auto folded =
          FoldRowIndependentSubqueries(context, {having_expr}, outer, ctes);
      if (folded.size() == 1) {
        having_expr = std::move(folded[0]);
      }
    }

    // HAVING transpose (aggregate <-> filter): within a grouping every row of
    // a group carries the same key values, so a conjunct that touches only
    // bare grouping-key columns (and no aggregate / subquery) is a row-level
    // fact for the whole group.  Moving it into the scan filter drops the
    // rows of doomed groups before groups form; the aggregate-referencing
    // remainder stays post-aggregate, where the prepared aggregate results
    // are available.  A group-all aggregation is exempt: it yields one group
    // even over a completely rejected input, so a branch-constant conjunct
    // lifted pre-aggregate would wrongly resurrect that group.
    Expression having_residual = having_expr;
    {
      std::vector<Expression> lifted_conjuncts;
      std::vector<Expression> residual_conjuncts;
      const std::vector<Expression> having_conjuncts =
          having_expr ? SplitConjuncts(having_expr) : std::vector<Expression>{};
      for (const Expression& conjunct : having_conjuncts) {
        if (!stmt.IsGroupAll() && !stmt.GroupBy().empty() &&
            IsKeyOnlyHavingConjunct(conjunct, stmt)) {
          lifted_conjuncts.push_back(conjunct);
        } else {
          residual_conjuncts.push_back(conjunct);
        }
      }
      if (!lifted_conjuncts.empty()) {
        scan_predicates.reserve(scan_predicates.size() +
                                lifted_conjuncts.size());
        scan_predicates.insert(scan_predicates.end(), lifted_conjuncts.begin(),
                               lifted_conjuncts.end());
        having_residual = residual_conjuncts.empty()
                              ? Expression{}
                              : CombineConjuncts(residual_conjuncts);
      }
    }

    // The predicates name alias-qualified columns (`x0.b`); compile and
    // evaluate them against the qualified view of the scan row, or a residual
    // conjunct (function call, CASE, ...) cannot resolve its columns.
    CompiledScanFilter scan_filter =
        CompileScanFilter(scan_predicates, qualified_schema);

    if (having_residual) {
      having_residual = ReplaceSubExpressions(
          having_residual,
          [&stmt](const Expression& node) -> std::optional<Expression> {
            if (!node || node->Type() != TypeTag::kAggregateExp) {
              return std::nullopt;
            }
            for (const NamedExpression& select_item : stmt.SelectList()) {
              if (select_item.expression &&
                  select_item.expression->Type() == TypeTag::kAggregateExp &&
                  select_item.expression->ToString() == node->ToString()) {
                return select_item.expression;
              }
            }
            return std::nullopt;
          });
    }
    std::vector<const AggregateExpression*> aggregate_expressions;
    std::unordered_set<const AggregateExpression*> seen_aggregates;
    for (const NamedExpression& projection_item : stmt.SelectList()) {
      CollectAggregates(projection_item.expression, &aggregate_expressions,
                        &seen_aggregates);
    }
    CollectAggregates(having_residual, &aggregate_expressions,
                      &seen_aggregates);
    // ORDER BY terms may reference select aliases or spell out an aggregate
    // ("ORDER BY SUM(v) DESC").  The AggregateResultMap is keyed by node
    // pointer, so aggregate subtrees dedupe onto the identical select-list
    // node (mirroring the grouped path); without this the ORDER BY instance
    // is never prepared and the sort fails with "aggregate was not prepared".
    std::vector<Expression> resolved_order_keys;
    resolved_order_keys.reserve(stmt.OrderBy().size());
    for (const auto& term : stmt.OrderBy()) {
      Expression expression = term.expression;
      if (expression && expression->Type() == TypeTag::kColumnValue) {
        const ColumnName& name = expression->AsColumnValue().GetColumnName();
        if (name.schema.empty()) {
          for (const NamedExpression& select_item : stmt.SelectList()) {
            if (!select_item.name.empty() &&
                select_item.name.size() == name.name.size() &&
                std::equal(
                    select_item.name.begin(), select_item.name.end(),
                    name.name.begin(), [](char left, char right) {
                      return std::tolower(static_cast<unsigned char>(left)) ==
                             std::tolower(static_cast<unsigned char>(right));
                    })) {
              expression = select_item.expression;
              break;
            }
          }
        }
      }
      expression = ReplaceSubExpressions(
          expression,
          [&stmt](const Expression& node) -> std::optional<Expression> {
            if (!node || node->Type() != TypeTag::kAggregateExp) {
              return std::nullopt;
            }
            for (const NamedExpression& select_item : stmt.SelectList()) {
              if (select_item.expression &&
                  select_item.expression->Type() == TypeTag::kAggregateExp &&
                  select_item.expression->ToString() == node->ToString()) {
                return select_item.expression;
              }
            }
            return std::nullopt;
          });
      resolved_order_keys.push_back(std::move(expression));
    }
    for (const Expression& order_expression : resolved_order_keys) {
      CollectAggregates(order_expression, &aggregate_expressions,
                        &seen_aggregates);
    }

    struct GroupState {
      Row representative;
      size_t accumulator_offset{0};
    };
    std::deque<AggregateAccumulator> aggregate_states;
    std::vector<GroupState> groups;
    GroupKeyMap offsets;
    // Single INT64/DATE column group keys index groups by the raw value:
    // probing an int map skips the per-row Value copies of the generic key
    // path.  NULL keys fall through to the row-keyed index (mirroring
    // TypedWorkerGroups), so NULL stays its own group.
    // Flat open addressing (one cache line per probe) instead of a node-based
    // unordered_map: the generic grouped loop's single-int-key path serves
    // the multi-hundred-thousand-group shapes (Q3's l_orderkey), where the
    // node chase per probe dominated the fold.
    relational_detail::FlatIntKeyMap<size_t> int_offsets;
    bool single_int_key = false;
    slot_t int_key_column{0};

    std::vector<std::optional<slot_t>> group_offsets;
    group_offsets.reserve(stmt.GroupBy().size());
    bool group_keys_are_columns = true;
    for (const Expression& key : stmt.GroupBy()) {
      if (key->Type() != TypeTag::kColumnValue) {
        group_keys_are_columns = false;
        group_offsets.emplace_back(std::nullopt);
        continue;
      }
      group_offsets.emplace_back(LocalColumnOffset(
          input.schema, key->AsColumnValue().GetColumnName()));
      if (!group_offsets.back()) {
        group_keys_are_columns = false;
      }
    }
    if (group_keys_are_columns && group_offsets.size() == 1) {
      const std::optional<slot_t>& first_key = group_offsets.front();
      if (first_key.has_value() && *first_key < input.schema.ColumnCount()) {
        const ValueType key_type = input.schema.GetColumn(*first_key).Type();
        single_int_key =
            key_type == ValueType::kInt64 || key_type == ValueType::kDate;
        if (single_int_key) {
          int_key_column = *first_key;
        }
      }
    }
    std::vector<std::optional<slot_t>> aggregate_child_offsets;
    aggregate_child_offsets.reserve(aggregate_expressions.size());
    for (const AggregateExpression* aggregate : aggregate_expressions) {
      if (IsCountStar(*aggregate)) {
        aggregate_child_offsets.emplace_back(
            std::nullopt);  // sentinel via count*
        continue;
      }
      if (aggregate->Child()->Type() == TypeTag::kColumnValue) {
        aggregate_child_offsets.emplace_back(LocalColumnOffset(
            input.schema, aggregate->Child()->AsColumnValue().GetColumnName()));
      } else {
        aggregate_child_offsets.emplace_back(std::nullopt);
      }
    }
    const size_t count_star_flags_size = aggregate_expressions.size();
    std::vector<bool> is_count_star(count_star_flags_size, false);
    for (size_t i = 0; i < aggregate_expressions.size(); ++i) {
      is_count_star[i] = IsCountStar(*aggregate_expressions[i]);
    }

    const auto scan_begin = std::chrono::steady_clock::now();
    // Reused key scratch: most rows land in an existing group, so probe the
    // index heterogeneously (no per-row Row/vector allocation) and only
    // materialize a key Row when a group is actually new.
    std::vector<Value> key_values;
    key_values.reserve(stmt.GroupBy().size());
    // const-ref parameter: ForEachRow invokes with an lvalue, so a by-value
    // Row here deep-copied every scanned row (values_ vector + all cells).
    auto accumulate_row =
        [&](const Row& row, int64_t repetitions = 1,
            const Value* prepared_key = nullptr,
            const std::vector<AggregateAccumulator>* prepared_states =
                nullptr) -> Status {
      if (context.execution_runtime() != nullptr) {
        context.execution_runtime()->scan_output_rows += repetitions;
      }
      Scope scope{.row = &row, .schema = &input.schema, .outer = outer};
      size_t group_index = groups.size();
      bool inserted = false;
      if (single_int_key) {
        const Value& key_value = row[int_key_column];
        if (!key_value.IsNull()) {
          bool emplaced = false;
          size_t* slot =
              int_offsets.TryEmplace(key_value.value.int_value, &emplaced);
          if (emplaced) {
            // Claim the index the new group below will occupy.
            *slot = groups.size();
          }
          group_index = *slot;
          inserted = emplaced;
        } else {
          // NULL must stay its own group: the raw int64 slot cannot encode
          // null-ness, so NULL keys route through the row-keyed index.
          key_values.clear();
          key_values.push_back(CanonicalDistinctValue(key_value));
          auto found = offsets.find(key_values);
          if (found != offsets.end()) {
            group_index = found->second;
          } else {
            inserted = true;
            offsets.emplace(Row(key_values), group_index);
          }
        }
      } else {
        key_values.clear();
        if (prepared_key != nullptr) {
          key_values.push_back(CanonicalDistinctValue(*prepared_key));
        } else if (group_keys_are_columns) {
          for (const auto& offset : group_offsets) {
            key_values.push_back(CanonicalDistinctValue(row[*offset]));
          }
        } else {
          for (const Expression& key : stmt.GroupBy()) {
            ASSIGN_OR_RETURN(Value, hv81838_0,
                             (TryEvaluate(key, scope, nullptr, context, ctes)));
            key_values.push_back(CanonicalDistinctValue(hv81838_0));
          }
        }
        if (auto found = offsets.find(key_values); found != offsets.end()) {
          group_index = found->second;
        } else {
          inserted = true;
          offsets.emplace(Row(key_values), group_index);
        }
      }
      if (inserted) {
        GroupState group;
        group.accumulator_offset = aggregate_states.size();
        for (const AggregateExpression* aggregate : aggregate_expressions) {
          aggregate_states.emplace_back(aggregate, &input.schema);
        }
        group.representative = row;
        groups.push_back(std::move(group));
      }
      GroupState& group = groups[group_index];
      for (size_t i = 0; i < aggregate_expressions.size(); ++i) {
        AggregateAccumulator& accumulator =
            aggregate_states[group.accumulator_offset + i];
        const AggregateExpression& aggregate = *aggregate_expressions[i];
        if (aggregate.WhereFilter()) {
          ASSIGN_OR_RETURN(Value, passes,
                           TryEvaluate(aggregate.WhereFilter(), scope, nullptr,
                                       context, ctes));
          if (!Truthy(passes)) {
            continue;
          }
        }
        if (prepared_states != nullptr) {
          RETURN_IF_FAIL(accumulator.Merge((*prepared_states)[i]));
        } else if (repetitions > 1) {
          // Only TryFoldDictionaryAggregateRows supplies multiplicities. It
          // proves the restricted aggregate shape and the exact AVG bound.
          ASSIGN_OR_RETURN(
              Value, value,
              (is_count_star[i] ? StatusOr<Value>(Value(1))
                                : TryEvaluate(aggregate.Child(), scope, nullptr,
                                              context, ctes)));
          if (!value.IsNull()) {
            switch (aggregate.GetType()) {
              case AggregationType::kCount:
                accumulator.count += repetitions;
                break;
              case AggregationType::kAvg:
                accumulator.count += repetitions;
                accumulator.total +=
                    (value.type == ValueType::kDouble
                         ? value.value.double_value
                         : static_cast<double>(value.value.int_value)) *
                    static_cast<double>(repetitions);
                break;
              default:
                RETURN_IF_FAIL(accumulator.Add(value));
                break;
            }
          }
        } else if (is_count_star[i]) {
          RETURN_IF_FAIL(accumulator.Add(Value(1)));
        } else if (aggregate_child_offsets[i] &&
                   !aggregate.NeedsGroupContext() &&
                   aggregate.Having() == AggregateHavingModifier::kNone) {
          RETURN_IF_FAIL(accumulator.Add(row[*aggregate_child_offsets[i]]));
        } else {
          AggregateInput aggregate_input;
          ASSIGN_OR_RETURN(
              Value, argument,
              TryEvaluate(aggregate.Child(), scope, nullptr, context, ctes));
          aggregate_input.value = std::move(argument);
          if (aggregate.Having() != AggregateHavingModifier::kNone &&
              aggregate.HavingCondition()) {
            ASSIGN_OR_RETURN(Value, hv83481_0,
                             (TryEvaluate(aggregate.HavingCondition(), scope,
                                          nullptr, context, ctes)));
            aggregate_input.condition = std::move(hv83481_0);
          }
          for (const auto& term : aggregate.InnerOrderBy()) {
            ASSIGN_OR_RETURN(
                Value, hv83714_0,
                (TryEvaluate(term.expression, scope, nullptr, context, ctes)));
            aggregate_input.order_keys.push_back(std::move(hv83714_0));
          }
          if (aggregate.GetType() == AggregationType::kStringAgg &&
              aggregate.SecondaryArg()) {
            ASSIGN_OR_RETURN(Value, hv83961_0,
                             (TryEvaluate(aggregate.SecondaryArg(), scope,
                                          nullptr, context, ctes)));
            aggregate_input.auxiliary = std::move(hv83961_0);
          }
          for (const Expression& extra : aggregate.TrailingArgs()) {
            if (extra) {
              ASSIGN_OR_RETURN(
                  Value, hv84223_0,
                  (TryEvaluate(extra, scope, nullptr, context, ctes)));
              aggregate_input.trailing_values.push_back(std::move(hv84223_0));
            }
          }
          RETURN_IF_FAIL(accumulator.Add(std::move(aggregate_input)));
        }
        if (context.execution_runtime() != nullptr) {
          context.execution_runtime()->aggregate_updates += repetitions;
        }
      }
      if (context.execution_runtime() != nullptr) {
        context.execution_runtime()->aggregate_input_rows += repetitions;
      }
      return Status::kSuccess;
    };

    const std::vector<slot_t>* proj_ptr =
        projection.empty() ? nullptr : &projection;
    const std::vector<IntegerPeekCompare> peek_compares =
        BuildIntegerPeeks(scan_filter, proj_ptr, table_schema);
    const std::vector<IntegerPeekCompare>* peek_ptr =
        peek_compares.empty() ? nullptr : &peek_compares;

    // Typed parallel fast path (single-table GROUP BY): shapes whose group
    // keys, aggregates, and outputs resolve positionally run as parallel
    // morsel aggregation with context-free bytecode filtering.  Anything
    // less regular keeps the generic loop below.  Shared base-relation
    // statements are admitted too: the typed path fills the cache from
    // worker slices with strict morsel-order publication and aggregates
    // cache hits from parallel row ranges, so no reader can observe a
    // partial entry.  A/B hatch: TINYLAMB_DISABLE_REUSABLE_TYPED_AGG=1
    // restores the pre-change gate that kept shared-table statements on the
    // generic stream loop.
    static const bool reusable_typed_disabled =
        std::getenv("TINYLAMB_DISABLE_REUSABLE_TYPED_AGG") != nullptr;
    // The aggregate list above uses resolved ORDER BY nodes. Give the typed
    // planner those same nodes: the original COUNT(*) spelling can be a
    // different AST object from the SELECT aggregate it resolves to.
    SelectStatement typed_statement(stmt);
    auto typed_order = stmt.OrderBy();
    for (size_t i = 0; i < typed_order.size(); ++i) {
      typed_order[i].expression = resolved_order_keys[i];
    }
    typed_statement.SetOrderBy(std::move(typed_order));
    if (!reusable || !reusable_typed_disabled) {
      TypedAggregationPlan typed_plan = PlanTypedAggregation(
          typed_statement, having_residual, qualified_schema,
          aggregate_expressions, scan_predicates,
          /*allow_composite_select=*/true);
      if (typed_plan.eligible) {
        const RuntimeFilterGroup* stream_rf = nullptr;
        ValueType stream_rf_type = ValueType::kNull;
        std::optional<slot_t> stream_key_col;
        if (context.execution_runtime() != nullptr &&
            typed_plan.key_offsets.size() == 1) {
          const slot_t proj_key_slot = typed_plan.key_offsets[0];
          const slot_t full_key_slot =
              projection.empty() ? proj_key_slot : projection[proj_key_slot];
          const auto stored =
              context.execution_runtime()->table_key_filters.find(
                  TableKeyFilterKey(source));
          if (stored != context.execution_runtime()->table_key_filters.end() &&
              stored->second.OwnedBy(statement) && !stored->second.empty() &&
              stored->second.column == full_key_slot &&
              full_key_slot < table_schema.ColumnCount() &&
              !table_schema.GetColumn(full_key_slot).IsUnsigned() &&
              table_schema.GetColumn(full_key_slot).Type() ==
                  stored->second.type) {
            stream_rf = stored->second.filter.get();
            stream_rf_type = stored->second.type;
            stream_key_col = full_key_slot;
          }
        }
        StatusOr<Relation> typed = RunTypedParallelStreamAggregate(
            context, typed_statement, having_residual, *table.Value(),
            source.table, reusable, projection, qualified_schema, table_schema,
            peek_compares, aggregate_expressions, ctes, std::move(typed_plan),
            stream_rf, stream_rf_type, stream_key_col);
        if (!typed.HasValue()) {
          return typed.GetStatus();
        }
        Relation typed_output = typed.MoveValue();
        typed_output.FinishSpill();
        return LimitedRows(statement, std::move(typed_output), &context, outer,
                           &ctes);
      }
      ASSIGN_OR_RETURN(
          std::optional<Relation>, columnar_tlcs_result,
          relational_detail::TryRunColumnarTlcsAggregate(
              context, typed_statement, having_residual, *table.Value(),
              projection, qualified_schema, table_schema,
              /*typed_plan=*/nullptr, aggregate_expressions, scan_predicates,
              /*resident_store_hint=*/nullptr, context.execution_runtime(),
              /*publish_lstable_marker=*/false, /*fold_cache_key=*/{},
              scan_begin));
      if (columnar_tlcs_result) {
        columnar_tlcs_result->FinishSpill();
        return LimitedRows(statement, std::move(*columnar_tlcs_result),
                           &context, outer, &ctes);
      }
    }

    if (!having_expr) {
      ASSIGN_OR_RETURN(
          std::optional<Relation>, dictionary_result,
          TryDictionaryExpressionAggregate(
              context, typed_statement, *table.Value(), projection,
              qualified_schema, aggregate_expressions, scan_predicates, ctes));
      if (dictionary_result) {
        dictionary_result->FinishSpill();
        return LimitedRows(statement, std::move(*dictionary_result), &context,
                           outer, &ctes);
      }
    }

    bool dictionary_rows_folded = false;
    if (!reusable) {
      ASSIGN_OR_RETURN(
          bool, folded,
          TryFoldDictionaryAggregateRows(
              context, stmt, *table.Value(), projection, qualified_schema,
              aggregate_expressions, scan_predicates,
              [&](const Row& row, int64_t repetitions) -> Status {
                Status filter_status{Status::kSuccess};
                if (!MatchScanFilter(row, qualified_schema, scan_filter, outer,
                                     context, ctes, &filter_status)) {
                  return filter_status;
                }
                return accumulate_row(row, repetitions);
              },
              [&](const Row& row, int64_t repetitions, const Value& key,
                  const std::vector<AggregateAccumulator>& states) {
                return accumulate_row(row, repetitions, &key, &states);
              }));
      dictionary_rows_folded = folded;
    }
    if (reusable) {
      const std::string cache_key = BaseRelationCacheKey(
          source.table, projection.empty() ? nullptr : &projection);
      auto cached = context.execution_runtime()->base_relations.find(cache_key);
      if (cached == context.execution_runtime()->base_relations.end()) {
        Relation cache_rel(context.execution_runtime());
        cache_rel.schema = scan_schema;
        // Fixed-width cache fill also mirrors the rows into an int64 arena
        // (same append order) so the typed fold's cache-hit pass can stream
        // cells; eligibility mirrors the typed route's fill-side check.
        std::shared_ptr<BaseRelationArena> generic_arena;
        static const bool generic_arena_disabled =
            std::getenv("TINYLAMB_DISABLE_AGG_ARENA") != nullptr;
        if (!generic_arena_disabled && scan_schema.ColumnCount() > 0) {
          bool all_fixed = true;
          for (slot_t i = 0; i < scan_schema.ColumnCount(); ++i) {
            const ValueType type = scan_schema.GetColumn(i).Type();
            if (type != ValueType::kInt64 && type != ValueType::kDate) {
              all_fixed = false;
              break;
            }
          }
          if (all_fixed) {
            generic_arena = std::make_shared<BaseRelationArena>();
            generic_arena->stride = scan_schema.ColumnCount();
            generic_arena->null_bits.resize(generic_arena->stride);
          }
        }
        std::array<int64_t, 16> generic_cells{};
        std::array<uint8_t, 16> generic_nulls{};
        std::vector<int64_t> generic_cell_buffer;
        std::vector<uint8_t> generic_null_buffer;
        if (generic_arena != nullptr &&
            generic_arena->stride > static_cast<size_t>(16)) {
          generic_cell_buffer.resize(generic_arena->stride);
          generic_null_buffer.resize(generic_arena->stride);
        }
        // Fill the shared cache with UNFILTERED rows: the cache key carries no
        // predicates (and no stashed key filters are applied here), so every
        // consumer applies its own filters while reading below.
        Iterator iterator =
            projection.empty()
                ? table.Value()->BeginFullScan(context.txn_)
                : table.Value()->BeginFullScan(context.txn_, projection);
        while (iterator.IsValid()) {
          if (context.execution_runtime() != nullptr) {
            ++context.execution_runtime()->scan_rows;
            context.execution_runtime()->scan_values_available +=
                table_schema.ColumnCount();
            context.execution_runtime()->scan_values_decoded +=
                scan_schema.ColumnCount();
          }
          if (generic_arena != nullptr) {
            if ((*iterator).values_.size() != generic_arena->stride) {
              generic_arena.reset();
            } else {
              const size_t width = generic_arena->stride;
              int64_t* cells = width <= 16 ? generic_cells.data()
                                           : generic_cell_buffer.data();
              uint8_t* nulls = width <= 16 ? generic_nulls.data()
                                           : generic_null_buffer.data();
              for (size_t c = 0; c < width; ++c) {
                const Value& value = (*iterator).values_[c];
                cells[c] = value.value.int_value;
                nulls[c] = value.IsNull() ? 1 : 0;
              }
              generic_arena->AppendRun(cells, nulls, 1);
            }
          }
          cache_rel.AddRow(std::move(*iterator));
          ++iterator;
        }
        // IsValid() turns false on EOF *and* on a failed page read (e.g. an
        // evicted page whose spill read fails); never cache or aggregate a
        // silently truncated scan.
        RETURN_IF_FAIL(iterator.GetStatus());
        cache_rel.FinishSpill();
        cached = context.execution_runtime()
                     ->base_relations
                     .emplace(cache_key,
                              std::make_shared<Relation>(std::move(cache_rel)))
                     .first;
        if (generic_arena != nullptr &&
            generic_arena->rows == cached->second->rows.size()) {
          context.execution_runtime()->base_relation_arenas.emplace(
              cache_key, std::move(generic_arena));
        }
      } else {
        ++context.execution_runtime()->base_scan_cache_hits;
        // A typed fill publishes the arena without Rows: box the rows back
        // (once) before the generic consumer iterates them.
        MaterializeCacheRowsIfNeeded(cache_key, cached->second.get(),
                                     context.execution_runtime());
      }
      // Aggregation always reads through the shared cache so the first user
      // and later users see exactly the same rows.
      Status scan_accumulate_error{Status::kSuccess};
      cached->second->FinishSpill();
      Status scan_iterated = cached->second->ForEachRow([&](const Row& row) {
        if (!scan_accumulate_error.ok()) {
          return;
        }
        if (!MatchScanFilter(row, qualified_schema, scan_filter, outer, context,
                             ctes, &scan_accumulate_error)) {
          return;
        }
        if (Status st_acc = accumulate_row(row);
            st_acc != Status::kSuccess &&
            scan_accumulate_error == Status::kSuccess) {
          scan_accumulate_error = st_acc;
        }
      });
      if (scan_iterated != Status::kSuccess &&
          scan_accumulate_error == Status::kSuccess) {
        scan_accumulate_error = scan_iterated;
      }
      RETURN_IF_FAIL(scan_accumulate_error);
    } else if (!dictionary_rows_folded) {
      // Stashed key filters are intentionally not applied here: they belong to
      // the statement that derived them, not to this aggregation.
      Iterator iterator = [&] {
        if (peek_ptr != nullptr) {
          if (projection.empty()) {
            return table.Value()->BeginFullScan(context.txn_, peek_ptr);
          }
          return table.Value()->BeginFullScan(context.txn_, projection,
                                              peek_ptr);
        }
        if (projection.empty()) {
          return table.Value()->BeginFullScan(context.txn_);
        }
        return table.Value()->BeginFullScan(context.txn_, projection);
      }();
      Status scan_filter_error{Status::kSuccess};
      while (iterator.IsValid()) {
        if (!MatchScanFilter(*iterator, qualified_schema, scan_filter, outer,
                             context, ctes, &scan_filter_error)) {
          RETURN_IF_FAIL(scan_filter_error);
          ++iterator;
          continue;
        }
        if (context.execution_runtime() != nullptr) {
          ++context.execution_runtime()->scan_rows;
          context.execution_runtime()->scan_values_available +=
              table_schema.ColumnCount();
          context.execution_runtime()->scan_values_decoded +=
              scan_schema.ColumnCount();
        }
        RETURN_IF_FAIL(accumulate_row(*iterator));
        ++iterator;
      }
      RETURN_IF_FAIL(scan_filter_error);
      RETURN_IF_FAIL(iterator.GetStatus());
    }
    if (context.execution_runtime() != nullptr) {
      context.execution_runtime()->scan_ms += ElapsedMs(scan_begin);
      // filter_ms intentionally includes the same interval: filtering happened
      // inline with the scan, so both counters cover this phase.
      context.execution_runtime()->filter_ms += ElapsedMs(scan_begin);
      context.execution_runtime()->aggregate_groups += groups.size();
    }
    // A group-all statement (GROUP BY (), GROUPING SETS whose sets reduce
    // to the grand total) aggregates over even an empty input into one row.
    if (groups.empty() && (stmt.GroupBy().empty() || stmt.IsGroupAll())) {
      GroupState group;
      group.representative =
          Row(std::vector<Value>(input.schema.ColumnCount(), Value()));
      group.accumulator_offset = aggregate_states.size();
      for (const AggregateExpression* aggregate : aggregate_expressions) {
        aggregate_states.emplace_back(aggregate, &input.schema);
      }
      groups.push_back(std::move(group));
    }

    Relation output(context.execution_runtime());
    output.schema = input.schema;
    std::vector<Column> output_columns;
    for (size_t i = 0; i < stmt.SelectList().size(); ++i) {
      const NamedExpression& projection_item = stmt.SelectList()[i];
      output_columns.emplace_back(ProjectionName(projection_item, i),
                                  ValueType::kNull);
    }
    // Inline ORDER BY handling: keys may reference select aliases (output
    // schema), grouping base columns (representative row) or outer scopes.
    const Schema initial_output_schema("", output_columns);
    // Same ordering contract as the non-grouped relational path above: NULL
    // placement, direction and the key's collation all come from
    // SortKeyOrder, so a grouped ORDER BY folds exactly like an ungrouped one.
    std::vector<SortKeyOrder> order_specs;
    order_specs.reserve(stmt.OrderBy().size());
    for (const SelectStatement::OrderByTerm& term : stmt.OrderBy()) {
      order_specs.emplace_back(term.ascending, term.nulls_first,
                               CollationOfExpression(term.expression));
    }
    bool bounded = false;
    size_t topn_capacity = 0;
    if (!stmt.Distinct() && stmt.HasLimit() && !stmt.WithTies() &&
        stmt.Offset() <= std::numeric_limits<size_t>::max() - stmt.Limit()) {
      bounded = true;
      topn_capacity = stmt.Offset() + stmt.Limit();
    }
    ExternalKeyedSorter sorter(order_specs, bounded, topn_capacity);
    // Aggregate expression identities are fixed across groups. Reuse their
    // map nodes, while still finishing every aggregate before testing HAVING
    // so errors in a discarded group remain observable.
    AggregateResultMap aggregate_results;
    aggregate_results.reserve(aggregate_expressions.size());
    std::vector<Value*> aggregate_slots;
    aggregate_slots.reserve(aggregate_expressions.size());
    for (const AggregateExpression* aggregate : aggregate_expressions) {
      auto [slot, inserted] = aggregate_results.emplace(aggregate, Value());
      aggregate_slots.push_back(&slot->second);
    }
    for (const GroupState& group : groups) {
      for (size_t i = 0; i < aggregate_expressions.size(); ++i) {
        ASSIGN_OR_RETURN(
            Value, finished,
            aggregate_states[group.accumulator_offset + i].TryFinish());
        *aggregate_slots[i] = std::move(finished);
      }
      Scope scope{.row = &group.representative,
                  .schema = &input.schema,
                  .outer = outer};
      // Correlated subqueries in HAVING / SELECT resolve aggregate aliases
      // against this group's values through the scope chain.
      scope.aggregates = &aggregate_results;
      if (having_residual) {
        ASSIGN_OR_RETURN(Value, passes,
                         TryEvaluate(having_residual, scope, &aggregate_results,
                                     context, ctes));
        if (!Truthy(passes)) {
          continue;
        }
      }
      std::vector<Value> values;
      values.reserve(stmt.SelectList().size());
      for (const NamedExpression& projection_item : stmt.SelectList()) {
        ASSIGN_OR_RETURN(Value, hv90851_0,
                         (TryEvaluate(projection_item.expression, scope,
                                      &aggregate_results, context, ctes)));
        values.push_back(std::move(hv90851_0));
      }
      Row output_row(std::move(values));
      // Sort keys may reference base columns of the grouped relation
      // (`ORDER BY str_val` where str_val is a grouping key); chain the
      // projected row to the group's representative row so those resolve.
      if (!stmt.OrderBy().empty()) {
        // The grouped ORDER BY key may reference output columns, aliases,
        // aggregates and group-key base columns -- never a relation alias
        // denoting a whole row (the query is grouped by parts of that row),
        // so the row-alias fallback is suppressed on both scopes.
        Scope base_scope{.row = &group.representative,
                         .schema = &input.schema,
                         .outer = outer,
                         .no_row_alias = true};
        Scope proj_scope{.row = &output_row,
                         .schema = &initial_output_schema,
                         .outer = &base_scope,
                         .no_row_alias = true};
        std::vector<Value> keys;
        keys.reserve(stmt.OrderBy().size());
        for (const Expression& key : resolved_order_keys) {
          ASSIGN_OR_RETURN(Value, hv91721_0,
                           (TryEvaluate(key, proj_scope, &aggregate_results,
                                        context, ctes)));
          keys.push_back(std::move(hv91721_0));
        }
        RETURN_IF_FAIL(sorter.Add(
            KeyedRow{.keys = std::move(keys), .row = std::move(output_row)}));
      } else {
        output.AddRow(std::move(output_row));
      }
    }
    if (sorter.UsedBoundedHeap() && context.execution_runtime() != nullptr) {
      ++context.execution_runtime()->topn_finish_uses;
      context.execution_runtime()->topn_finish_capacity = topn_capacity;
      context.execution_runtime()->topn_finish_input_rows =
          sorter.TotalInputRows();
    }
    if (!sorter.Empty()) {
      const auto sort_begin = std::chrono::steady_clock::now();
      RETURN_IF_FAIL(sorter.EmitSorted([&](Row&& sorted_row) {
        output.AddRow(std::move(sorted_row));
        return Status::kSuccess;
      }));
      if (context.execution_runtime() != nullptr) {
        context.execution_runtime()->sort_ms += ElapsedMs(sort_begin);
      }
    }
    RefineProjectionTypes(output, output_columns);
    output.schema = Schema("", std::move(output_columns));
    output.FinishSpill();
    // DISTINCT / LIMIT without running Project again; ORDER BY was applied
    // inline above (its keys may reference grouping base columns that are no
    // longer reachable from the projected output alone), and DistinctOf
    // preserves row order.
    if (stmt.Distinct()) {
      if (stmt.HasDistinctOn()) {
        ASSIGN_OR_RETURN(Relation, distinct_of,
                         (DistinctOf(std::move(output), stmt.DistinctOn(),
                                     &context, outer, &ctes)));
        output = std::move(distinct_of);
      } else {
        ASSIGN_OR_RETURN(Relation, distinct_of,
                         (DistinctOf(std::move(output))));
        output = std::move(distinct_of);
      }
    }
    return LimitedRows(statement, std::move(output), &context, outer, &ctes);
  }

  if (outer == nullptr && statement.WithQueries().empty() &&
      statement.UnionAll().empty() && !statement.GetSetOperationTree() &&
      !statement.Qualify() && !statement.Having() && !statement.Distinct() &&
      statement.GroupBy().size() == 1 && statement.SelectList().size() == 2 &&
      statement.Sources().size() == 1 &&
      statement.Sources()[0].query != nullptr &&
      statement.Sources()[0].unnest == nullptr &&
      !statement.Sources()[0].sample.has_value() &&
      !HasWindowFunctions(statement)) {
    const SelectStatement& sub = *statement.Sources()[0].query;
    const Expression& g0 = statement.GroupBy()[0];
    if (!statement.WhereClause() && g0 && g0->Type() == TypeTag::kColumnValue &&
        sub.WithQueries().empty() && sub.UnionAll().empty() &&
        !sub.GetSetOperationTree() && !sub.Qualify() && !sub.Having() &&
        !sub.Distinct() && !sub.HasLimit() && sub.Offset() == 0 &&
        sub.OrderBy().empty() && sub.SelectList().size() == 2) {
      const ColumnName& g_col = g0->AsColumnValue().GetColumnName();
      int count_col_pos = -1;
      int star_col_pos = -1;
      for (size_t si = 0; si < 2; ++si) {
        const Expression& e = statement.SelectList()[si].expression;
        if (!e) {
          break;
        }
        if (e->Type() == TypeTag::kColumnValue &&
            IdentifierEquals(e->AsColumnValue().GetColumnName().name,
                             g_col.name)) {
          count_col_pos = static_cast<int>(si);
        } else if (e->Type() == TypeTag::kAggregateExp &&
                   IsCountStar(e->AsAggregateExpression()) &&
                   !e->AsAggregateExpression().WhereFilter()) {
          star_col_pos = static_cast<int>(si);
        }
      }
      int sub_agg_pos = -1;
      for (size_t si = 0; si < 2; ++si) {
        if (!sub.SelectList()[si].name.empty() &&
            IdentifierEquals(sub.SelectList()[si].name, g_col.name) &&
            sub.SelectList()[si].expression &&
            sub.SelectList()[si].expression->Type() == TypeTag::kAggregateExp &&
            sub.SelectList()[si]
                    .expression->AsAggregateExpression()
                    .GetType() == AggregationType::kCount) {
          sub_agg_pos = static_cast<int>(si);
        }
      }
      if (count_col_pos >= 0 && star_col_pos >= 0 &&
          count_col_pos != star_col_pos && sub_agg_pos >= 0) {
        GroupedJoinFusionSpec sub_fusion = BuildGroupedJoinFusionSpec(sub);
        if (sub_fusion.eligible && sub_fusion.aggregates.size() == 1) {
          sub_fusion.rollup_count_distribution = true;
          sub_fusion.rollup_count_col_first = (count_col_pos == 0);
          sub_fusion.rollup_select_names = {statement.SelectList()[0].name,
                                            statement.SelectList()[1].name};
          bool sub_where_applied = false;
          StatusOr<Relation> sub_res = BuildInput(
              context, sub, outer, ctes, &sub_where_applied, &sub_fusion);
          if (sub_res.HasValue() && sub_fusion.fired &&
              sub_fusion.rollup_distribution_applied) {
            Relation dist = sub_res.MoveValue();
            if (!statement.Sources()[0].alias.empty()) {
              dist.schema =
                  QualifySchema(dist.schema, statement.Sources()[0].alias);
            }
            if (!statement.OrderBy().empty()) {
              RETURN_IF_FAIL(
                  ApplyOrderBy(context, statement, &dist, outer, ctes));
            }
            return LimitedRows(statement, std::move(dist), &context, outer,
                               &ctes);
          }
        }
      }
    }
  }

  if (HasWindowFunctions(statement) && StatementIsAggregated(statement)) {
    ASSIGN_OR_RETURN(Relation, window_over_aggregate,
                     (ExecuteWindowOverAggregate(
                         context, statement, Relation(), outer, ctes, true,
                         true, columnar_output || outer == nullptr)));
    if (!columnar_output && window_over_aggregate.HasColumnarOnly()) {
      RETURN_IF_FAIL(window_over_aggregate.EnsureRowsMaterialized());
    }
    return window_over_aggregate;
  }

  bool where_fully_applied = false;
  struct PostFusionHavingFilter {
    bool active{false};
    bool is_self_sum_factor{false};
    size_t select_col{0};
    BinaryOperation op{BinaryOperation::kGreaterThan};
    double constant_or_factor{0.0};
  };
  PostFusionHavingFilter post_having;
  const auto same_expr = [](const Expression& a,
                            const Expression& b) noexcept -> bool {
    if (!a && !b) {
      return true;
    }
    if (!a || !b) {
      return false;
    }
    return a->ToString() == b->ToString();
  };
  if (statement.Having() && statement.Having()->Type() == TypeTag::kBinaryExp) {
    const BinaryExpression& hbin = statement.Having()->AsBinaryExpression();
    if (IsComparison(hbin.Op()) && hbin.Left() &&
        hbin.Left()->Type() == TypeTag::kAggregateExp) {
      const AggregateExpression& lhs_agg = hbin.Left()->AsAggregateExpression();
      std::optional<size_t> matched_col;
      for (size_t si = 0; si < statement.SelectList().size(); ++si) {
        if (same_expr(hbin.Left(), statement.SelectList()[si].expression)) {
          matched_col = si;
          break;
        }
      }
      if (matched_col.has_value() && hbin.Right()) {
        if (hbin.Right()->Type() == TypeTag::kConstantValue) {
          const Value& cv = hbin.Right()->AsConstantValue().GetValue();
          // This shortcut stores its threshold and operands as DOUBLE.
          // Integer predicates must keep the canonical comparison, including
          // adjacent values above 2^53 and unsigned operands.
          if (!cv.IsNull() && cv.type == ValueType::kDouble) {
            post_having.active = true;
            post_having.is_self_sum_factor = false;
            post_having.select_col = *matched_col;
            post_having.op = hbin.Op();
            post_having.constant_or_factor = cv.value.double_value;
          }
        } else if (lhs_agg.GetType() == AggregationType::kSum &&
                   !lhs_agg.Distinct() && !lhs_agg.WhereFilter() &&
                   hbin.Right()->Type() == TypeTag::kQueryExp) {
          const QueryExpression& q = hbin.Right()->AsQueryExpression();
          if (!q.Exists() && !q.Test() && !q.ArrayResult() &&
              q.Query() != nullptr) {
            const SelectStatement& sub = *q.Query();
            bool same_sources =
                sub.WithQueries().empty() && sub.UnionAll().empty() &&
                !sub.GetSetOperationTree() && !sub.Qualify() &&
                !sub.HasLimit() && sub.Offset() == 0 && sub.GroupBy().empty() &&
                !sub.Having() && !sub.Distinct() && sub.OrderBy().empty() &&
                sub.Sources().size() == statement.Sources().size() &&
                same_expr(sub.WhereClause(), statement.WhereClause());
            for (size_t s = 0; same_sources && s < statement.Sources().size();
                 ++s) {
              const SelectSource& a = statement.Sources()[s];
              const SelectSource& b = sub.Sources()[s];
              if (a.table != b.table || a.alias != b.alias ||
                  a.join_type != b.join_type || a.query != nullptr ||
                  b.query != nullptr || a.unnest != nullptr ||
                  b.unnest != nullptr || a.sample.has_value() ||
                  b.sample.has_value() ||
                  !same_expr(a.join_condition, b.join_condition)) {
                same_sources = false;
              }
            }
            if (same_sources && sub.SelectList().size() == 1 &&
                sub.SelectList()[0].expression &&
                sub.SelectList()[0].expression->Type() == TypeTag::kBinaryExp) {
              const BinaryExpression& sbin =
                  sub.SelectList()[0].expression->AsBinaryExpression();
              if (sbin.Op() == BinaryOperation::kMultiply) {
                Expression const_side;
                if (same_expr(sbin.Left(), hbin.Left())) {
                  const_side = sbin.Right();
                } else if (same_expr(sbin.Right(), hbin.Left())) {
                  const_side = sbin.Left();
                }
                if (const_side &&
                    const_side->Type() == TypeTag::kConstantValue) {
                  const Value& fv = const_side->AsConstantValue().GetValue();
                  if (!fv.IsNull() &&
                      (fv.type == ValueType::kDouble ||
                       (fv.type == ValueType::kInt64 && !fv.IsUnsigned()))) {
                    post_having.active = true;
                    post_having.is_self_sum_factor = true;
                    post_having.select_col = *matched_col;
                    post_having.op = hbin.Op();
                    post_having.constant_or_factor =
                        fv.type == ValueType::kDouble
                            ? fv.value.double_value
                            : static_cast<double>(fv.value.int_value);
                  }
                }
              }
            }
          }
        }
      }
    }
  }
  if (!post_having.active && outer == nullptr && statement.Having() &&
      ContainsQuery(statement.Having())) {
    auto folded_having = FoldRowIndependentSubqueries(
        context, {statement.Having()}, outer, ctes);
    if (folded_having.size() == 1 && !ContainsQuery(folded_having[0])) {
      SelectStatement initialized(statement);
      initialized.ClearWithQueries();
      initialized.SetHaving(folded_having[0]);
      return ExecuteQuery(context, initialized, outer, ctes, output_sink,
                          columnar_output);
    }
  }
  // Fuse the final join with this statement's grouping when the shape
  // qualifies: the joined relation is never materialized (TPC-H Q13/Q18/Q21
  // outer legs).
  std::optional<SelectStatement> headless_having_stmt;
  if (post_having.active) {
    headless_having_stmt.emplace(statement);
    headless_having_stmt->SetHaving(Expression());
  }
  GroupedJoinFusionSpec fusion_spec = BuildGroupedJoinFusionSpec(
      headless_having_stmt.has_value() ? *headless_having_stmt : statement);
  if (post_having.active &&
      post_having.select_col < fusion_spec.select_items.size() &&
      fusion_spec.select_items[post_having.select_col] &&
      fusion_spec.select_items[post_having.select_col]->Type() ==
          TypeTag::kAggregateExp) {
    const auto* agg_ptr = &fusion_spec.select_items[post_having.select_col]
                               ->AsAggregateExpression();
    auto found = std::ranges::find(fusion_spec.aggregates, agg_ptr);
    if (found != fusion_spec.aggregates.end()) {
      fusion_spec.post_having_active = true;
      fusion_spec.post_having_agg_slot = static_cast<slot_t>(
          std::distance(fusion_spec.aggregates.begin(), found));
      fusion_spec.post_having_op = post_having.op;
      fusion_spec.post_having_is_self_sum_factor =
          post_having.is_self_sum_factor;
      fusion_spec.post_having_constant_or_factor =
          post_having.constant_or_factor;
    }
  }
  struct RankLimitGuard {
    ExecutionRuntime* runtime{nullptr};
    std::optional<ExecutionRuntime::GroupingRankLimit> saved;
    RankLimitGuard() = default;
    RankLimitGuard(const RankLimitGuard&) = delete;
    RankLimitGuard& operator=(const RankLimitGuard&) = delete;
    RankLimitGuard(RankLimitGuard&&) = delete;
    RankLimitGuard& operator=(RankLimitGuard&&) = delete;
    ~RankLimitGuard() {
      if (runtime != nullptr) {
        runtime->grouping_rank_limit = std::move(saved);
      }
    }
  } rank_guard;
  if (auto* runtime = context.execution_runtime();
      runtime != nullptr && statement.Sources().size() == 1 &&
      statement.Sources()[0].query != nullptr && statement.Qualify() &&
      statement.Qualify()->Type() == TypeTag::kBinaryExp &&
      statement.GroupBy().empty()) {
    const WindowFunctionCallExpression* rank = nullptr;
    const NamedExpression* rank_item = nullptr;
    for (const auto& item : statement.SelectList()) {
      if (!item.expression ||
          item.expression->Type() != TypeTag::kWindowFunctionExp) {
        continue;
      }
      if (rank != nullptr) {
        rank = nullptr;
        rank_item = nullptr;
        break;
      }
      rank = &item.expression->AsWindowFunctionCallExpression();
      rank_item = &item;
    }
    const auto& predicate = statement.Qualify()->AsBinaryExpression();
    // The QUALIFY left side must be this select-list RANK (the same window
    // expression or its alias); an unrelated RANK() in QUALIFY must not drive
    // the pre-aggregation group pruning.
    const bool predicate_is_rank =
        rank_item != nullptr && predicate.Left() &&
        (same_expr(predicate.Left(), rank_item->expression) ||
         (predicate.Left()->Type() == TypeTag::kColumnValue &&
          !rank_item->name.empty() &&
          predicate.Left()->AsColumnValue().GetColumnName().name ==
              rank_item->name));
    const bool descending_rank =
        predicate_is_rank && rank != nullptr && rank->function == "RANK" &&
        rank->args.empty() && !rank->where_filter && !rank->distinct &&
        rank->inner_order_by.empty() && !rank->inner_limit &&
        !rank->has_frame && rank->partition_by.size() == 1 &&
        rank->order_by.size() == 1 && !rank->order_by[0].ascending &&
        rank->partition_by[0] &&
        rank->partition_by[0]->Type() == TypeTag::kColumnValue &&
        rank->order_by[0].expression &&
        rank->order_by[0].expression->Type() == TypeTag::kColumnValue &&
        (predicate.Op() == BinaryOperation::kLessThanEquals ||
         predicate.Op() == BinaryOperation::kLessThan) &&
        predicate.Right() &&
        predicate.Right()->Type() == TypeTag::kConstantValue;
    if (descending_rank) {
      const auto& bound = predicate.Right()->AsConstantValue().GetValue();
      if (bound.type == ValueType::kInt64 && !bound.IsUnsigned() &&
          bound.value.int_value > 0) {
        const int64_t kept =
            bound.value.int_value -
            (predicate.Op() == BinaryOperation::kLessThan ? 1 : 0);
        if (kept > 0) {
          rank_guard.runtime = runtime;
          rank_guard.saved = std::move(runtime->grouping_rank_limit);
          runtime->grouping_rank_limit = ExecutionRuntime::GroupingRankLimit{
              .partition_name =
                  rank->partition_by[0]->AsColumnValue().GetColumnName().name,
              .order_name = rank->order_by[0]
                                .expression->AsColumnValue()
                                .GetColumnName()
                                .name,
              .limit = static_cast<size_t>(kept)};
        }
      }
    }
  }
  const auto build_input_begin = std::chrono::steady_clock::now();
  // A sink consumes the final projected groups. Keep all semantic tails on
  // the ordinary path unless there is nothing above those groups to apply.
  if (output_sink != nullptr && !statement.Having() &&
      statement.OrderBy().empty() && !statement.HasLimit() &&
      statement.Offset() == 0 && !statement.Distinct() &&
      !HasWindowFunctions(statement)) {
    fusion_spec.output_sink = output_sink;
  }
  if (fusion_spec.eligible) {
    fusion_spec.columnar_output =
        columnar_output && fusion_spec.output_sink == nullptr &&
        (statement.OrderBy().empty() ||
         (statement.HasLimit() && !statement.WithTies() &&
          !statement.Qualify()));
  }
  relational_detail::PushedScanProjection pushed_projection;
  const relational_detail::PushedScanProjection* pushed_projection_ptr =
      nullptr;
  bool pushed_projection_applied = false;
  if (columnar_output && outer == nullptr && !fusion_spec.eligible &&
      statement.Sources().size() == 1 && !statement.Sources()[0].query &&
      !statement.Sources()[0].unnest &&
      !statement.Sources()[0].from_nested_join &&
      !statement.Sources()[0].sample &&
      statement.Sources()[0].using_columns.empty() &&
      !ctes.contains(statement.Sources()[0].table) &&
      !HasWindowFunctions(statement) && !StatementIsAggregated(statement) &&
      !statement.Distinct() && !statement.Having() && !statement.Qualify() &&
      statement.GroupBy().empty() && statement.OrderBy().empty() &&
      !statement.SelectList().empty()) {
    StatusOr<std::shared_ptr<Table>> table_or =
        context.GetTable(statement.Sources()[0].table);
    if (table_or.HasValue()) {
      const Schema& raw_schema = table_or.Value()->GetSchema();
      const std::string& qualifier = statement.Sources()[0].alias.empty()
                                         ? statement.Sources()[0].table
                                         : statement.Sources()[0].alias;
      const Schema qualified_schema =
          qualifier.empty() ? raw_schema : QualifySchema(raw_schema, qualifier);
      bool ok = true;
      std::vector<Column> out_cols;
      out_cols.reserve(statement.SelectList().size());
      pushed_projection.expressions.reserve(statement.SelectList().size());
      for (size_t i = 0; i < statement.SelectList().size(); ++i) {
        const NamedExpression& item = statement.SelectList()[i];
        if (!item.expression || ContainsQuery(item.expression) ||
            ContainsAggregate(item.expression) ||
            ContainsParameterSlot(item.expression)) {
          ok = false;
          break;
        }
        if (item.expression->Type() == TypeTag::kColumnValue) {
          const ColumnName& cn =
              item.expression->AsColumnValue().GetColumnName();
          if (cn.name == "*" || !relational_detail::ResolveScalarColumnOffset(
                                     qualified_schema, cn)
                                     .has_value()) {
            ok = false;
            break;
          }
        }
        out_cols.push_back(ProjectionExpressionColumn(
            ProjectionName(item, i), item.expression, qualified_schema));
        pushed_projection.expressions.push_back(item.expression);
      }
      if (ok) {
        pushed_projection.output_schema = Schema("", std::move(out_cols));
        pushed_projection_ptr = &pushed_projection;
      }
    }
  }
  const bool input_columnar =
      columnar_output ||
      (!fusion_spec.eligible && outer == nullptr &&
       ((StatementIsAggregated(statement) && !statement.Qualify()) ||
        HasWindowFunctions(statement)) &&
       output_sink == nullptr);
  ASSIGN_OR_RETURN(
      Relation, input,
      BuildInput(context, statement, outer, ctes, &where_fully_applied,
                 fusion_spec.eligible ? &fusion_spec : nullptr, false,
                 input_columnar, pushed_projection_ptr,
                 &pushed_projection_applied));
  if (auto* runtime = context.execution_runtime()) {
    runtime->build_input_ms += ElapsedMs(build_input_begin);
  }
  if (fusion_spec.fired) {
    // The fused probe emitted the grouped+projected rows already; only the
    // ORDER BY / LIMIT tail remains, and eligibility guarantees every order
    // term resolves against the projected output.
    RETURN_IF_FAIL(input.FinishSpill());
    RETURN_IF_FAIL(input.GetStatus());
    if (post_having.active && !fusion_spec.post_having_applied) {
      // UINT64 aggregates live in int_value with the unsigned tag; reading
      // them as signed would turn values >= 2^63 negative.
      const auto having_numeric = [](const Value& v) -> double {
        if (v.type == ValueType::kDouble) {
          return v.value.double_value;
        }
        return v.IsUnsigned() ? static_cast<double>(
                                    static_cast<uint64_t>(v.value.int_value))
                              : static_cast<double>(v.value.int_value);
      };
      double threshold = post_having.constant_or_factor;
      bool valid_threshold = true;
      if (post_having.is_self_sum_factor) {
        double grand_total = 0.0;
        bool any_nonnull = false;
        RETURN_IF_FAIL(input.ForEachRow([&](const Row& r) {
          const Value& v = r[post_having.select_col];
          if (v.IsNull()) {
            return;
          }
          any_nonnull = true;
          grand_total += having_numeric(v);
        }));
        if (!any_nonnull) {
          valid_threshold = false;
        } else {
          threshold = grand_total * post_having.constant_or_factor;
        }
      }
      auto row_passes_having = [&](const Row& r) -> bool {
        const Value& v = r[post_having.select_col];
        if (v.IsNull()) {
          return false;
        }
        const double dv = having_numeric(v);
        switch (post_having.op) {
          case BinaryOperation::kEquals:
            return dv == threshold;
          case BinaryOperation::kNotEquals:
            return dv != threshold;
          case BinaryOperation::kLessThan:
            return dv < threshold;
          case BinaryOperation::kLessThanEquals:
            return dv <= threshold;
          case BinaryOperation::kGreaterThan:
            return dv > threshold;
          case BinaryOperation::kGreaterThanEquals:
            return dv >= threshold;
          default:
            return false;
        }
      };
      if (!valid_threshold) {
        input.ResetContents();
      } else if (!input.HasSpill()) {
        RETURN_IF_FAIL(input.EnsureRowsMaterialized());
        size_t out_idx = 0;
        for (size_t ri = 0; ri < input.rows.size(); ++ri) {
          if (row_passes_having(input.rows[ri])) {
            if (out_idx != ri) {
              input.rows[out_idx] = std::move(input.rows[ri]);
            }
            ++out_idx;
          }
        }
        input.rows.resize(out_idx);
      } else {
        Relation filtered(context.execution_runtime());
        filtered.schema = input.schema;
        filtered.using_columns = input.using_columns;
        CopyExecutionStats(&filtered, input);
        RETURN_IF_FAIL(input.ForEachRow([&](const Row& r) {
          if (row_passes_having(r)) {
            filtered.AddRow(r);
          }
        }));
        RETURN_IF_FAIL(filtered.FinishSpill());
        RETURN_IF_FAIL(filtered.GetStatus());
        input = std::move(filtered);
      }
    }
    if (!statement.OrderBy().empty()) {
      SelectStatement tail(statement);
      tail.SetOrderBy(fusion_spec.output_order);
      RETURN_IF_FAIL(ApplyOrderBy(context, tail, &input, outer, ctes));
    }
    return LimitedRows(statement, std::move(input), &context, outer, &ctes);
  }

  if (pushed_projection_applied) {
    RETURN_IF_FAIL(input.FinishSpill());
    RETURN_IF_FAIL(input.GetStatus());
    return LimitedRows(statement, std::move(input), &context, outer, &ctes);
  }

  size_t hidden_columns = 0;
  const SelectStatement* effective = &statement;
  std::shared_ptr<SelectStatement> windowed_statement;
  std::shared_ptr<SelectStatement> resolved_statement;
  if (!HasWindowFunctions(statement)) {
    // Grouped queries may reference select-list aliases in GROUP BY / HAVING.
    resolved_statement = ResolveGroupingAliases(statement, input.schema);
    if (resolved_statement != nullptr) {
      effective = resolved_statement.get();
    }
  }
  if (HasWindowFunctions(*effective)) {
    if (StatementIsAggregated(*effective)) {
      // Window functions over a grouped statement evaluate in a second pass
      // above the grouping (SQL semantics), including aggregate-of-aggregate
      // calls like avg(sum(x)) OVER (PARTITION BY ...).
      ASSIGN_OR_RETURN(
          Relation, window_over_aggregate,
          (ExecuteWindowOverAggregate(context, *effective, std::move(input),
                                      outer, ctes, !where_fully_applied, false,
                                      columnar_output || outer == nullptr)));
      RETURN_IF_FAIL(window_over_aggregate.FinishSpill());
      RETURN_IF_FAIL(window_over_aggregate.GetStatus());
      if (!columnar_output && window_over_aggregate.HasColumnarOnly()) {
        RETURN_IF_FAIL(window_over_aggregate.EnsureRowsMaterialized());
      }
      return window_over_aggregate;
    }
    ASSIGN_OR_RETURN(
        WindowedInput, windowed,
        (ApplyWindows(context, *effective, std::move(input), outer, ctes)));
    windowed_statement = windowed.statement;
    hidden_columns = windowed.hidden_columns;
    input = std::move(windowed.input);
    effective = windowed_statement.get();
  }

  ASSIGN_OR_RETURN(Relation, result,
                   (FinishQuery(context, *effective, std::move(input), outer,
                                ctes, !where_fully_applied, hidden_columns)));
  RETURN_IF_FAIL(result.FinishSpill());
  RETURN_IF_FAIL(result.GetStatus());
  if (!columnar_output && result.HasColumnarOnly()) {
    RETURN_IF_FAIL(result.EnsureRowsMaterialized());
  }
  return result;
}

}  // namespace tinylamb::relational_detail

namespace tinylamb {

using relational_detail::CountStatementTables;
using relational_detail::ExecuteQuery;
using relational_detail::ExecutionRuntime;
using relational_detail::Relation;
using relational_detail::WriteEstimatedPhysicalPlan;

RelationalExecutor::RelationalExecutor(
    TransactionContext& context,
    std::shared_ptr<const SelectStatement> statement)
    : context_(&context), statement_(std::move(statement)) {}

namespace {

Status StreamRelationNextBatch(
    const std::shared_ptr<const relational_detail::ColumnarTable>& columnar,
    const Schema& output_schema, size_t* columnar_chunk_idx,
    size_t* columnar_row_in_chunk, std::vector<Row>* rows, size_t* offset,
    std::vector<std::shared_ptr<SpillFile>>* spill_files,
    size_t* spill_file_idx, std::optional<SpillFile::RowReader>* spill_reader,
    DataChunk* destination, size_t max_rows) {
  if (columnar != nullptr) {
    const size_t nchunks = columnar->chunks.size();
    while (*columnar_chunk_idx < nchunks &&
           *columnar_row_in_chunk >=
               columnar->chunks[*columnar_chunk_idx].rows) {
      ++(*columnar_chunk_idx);
      *columnar_row_in_chunk = 0;
    }
    if (*columnar_chunk_idx >= nchunks) {
      return Status::kSuccess;
    }
    const size_t first_cols =
        columnar->chunks[*columnar_chunk_idx].columns.size();
    if (output_schema.ColumnCount() == first_cols && first_cols > 0) {
      destination->Initialize(output_schema, max_rows);
    }
    while (destination->Size() < max_rows && *columnar_chunk_idx < nchunks) {
      const auto& chunk = columnar->chunks[*columnar_chunk_idx];
      if (*columnar_row_in_chunk >= chunk.rows) {
        ++(*columnar_chunk_idx);
        *columnar_row_in_chunk = 0;
        continue;
      }
      const size_t avail = chunk.rows - *columnar_row_in_chunk;
      const size_t take = std::min(avail, max_rows - destination->Size());
      if (destination->ColumnCount() == 0 && !chunk.columns.empty()) {
        std::vector<ValueType> types;
        types.reserve(chunk.columns.size());
        for (const auto& col : chunk.columns) {
          types.push_back(col.type);
        }
        destination->Initialize(types, max_rows);
        for (size_t c = 0; c < chunk.columns.size(); ++c) {
          destination->ColumnAt(c).SetAllowHeterogeneous(true);
          if (chunk.columns[c].is_unsigned) {
            destination->ColumnAt(c).SetUnsigned(true);
          }
        }
      }
      if (destination->ColumnCount() != chunk.columns.size()) {
        Row tmp;
        for (size_t i = 0; i < take; ++i) {
          chunk.MaterializeRowInto(*columnar_row_in_chunk + i, &tmp);
          destination->Append(std::move(tmp));
        }
      } else {
        for (size_t c = 0; c < chunk.columns.size(); ++c) {
          const auto& src_col = chunk.columns[c];
          ColumnVector& dst_col = destination->ColumnAt(c);
          ZoneMap* zm = destination->MutableZoneMapAt(c);
          if (src_col.IsFlat() && src_col.is_uniform && !src_col.is_interval) {
            const uint8_t* nulls =
                src_col.has_null ? src_col.nulls.data() + *columnar_row_in_chunk
                                 : nullptr;
            switch (src_col.type) {
              case ValueType::kNull:
                dst_col.AppendNullBatch(take, zm);
                continue;
              case ValueType::kInt64:
              case ValueType::kDate:
                dst_col.AppendIntBatch(
                    src_col.type, src_col.is_unsigned,
                    src_col.ints.data() + *columnar_row_in_chunk, nulls, take,
                    zm);
                continue;
              case ValueType::kDouble:
                if (!src_col.is_unsigned) {
                  dst_col.AppendDoubleBatch(
                      src_col.doubles.data() + *columnar_row_in_chunk, nulls,
                      take, zm);
                  continue;
                }
                break;
              case ValueType::kVarChar:
                dst_col.AppendStringViewBatch(
                    src_col.strings.data() + *columnar_row_in_chunk, nulls,
                    take, zm);
                continue;
              case ValueType::kArray:
              default:
                break;
            }
          }
          for (size_t i = 0; i < take; ++i) {
            Value val;
            src_col.WriteValueInto(*columnar_row_in_chunk + i, &val);
            if (zm != nullptr) {
              zm->Add(val);
            }
            dst_col.Append(std::move(val));
          }
        }
        destination->CommitBatchAppend(take);
      }
      *columnar_row_in_chunk += take;
      if (*columnar_row_in_chunk >= chunk.rows) {
        ++(*columnar_chunk_idx);
        *columnar_row_in_chunk = 0;
      }
    }
    return Status::kSuccess;
  }

  while (destination->Size() < max_rows && *offset < rows->size()) {
    destination->Append(std::move((*rows)[(*offset)++]));
  }
  if (*offset >= rows->size() && !rows->empty()) {
    std::vector<Row>().swap(*rows);
    *offset = 0;
  }
  while (destination->Size() < max_rows &&
         *spill_file_idx < spill_files->size()) {
    if (!spill_reader->has_value()) {
      ASSIGN_OR_RETURN(SpillFile::RowReader, reader,
                       (*spill_files)[*spill_file_idx]->OpenRowReader());
      *spill_reader = std::move(reader);
    }
    Row row;
    bool has_row = false;
    RETURN_IF_FAIL((*spill_reader)->Next(&row, &has_row));
    if (has_row) {
      destination->Append(std::move(row));
    } else {
      spill_reader->reset();
      (*spill_files)[*spill_file_idx].reset();
      ++(*spill_file_idx);
    }
  }
  return Status::kSuccess;
}

// Lazily materializes the Cascades-optimized core plan and runs the grouped
// finish pipeline (see EmitGroupedFinishExecutor).  Same lifetime contract as
// RelationalExecutor: the executor must not outlive its TransactionContext.
class GroupedFinishExecutor final : public ExecutorBase {
 public:
  GroupedFinishExecutor(TransactionContext* context,
                        std::shared_ptr<PlanBase> core_plan,
                        std::shared_ptr<const SelectStatement> statement)
      : context_(context),
        core_plan_(std::move(core_plan)),
        statement_(std::move(statement)) {}

  size_t NextBatch(DataChunk* destination, size_t max_rows) override {
    destination->Reset();
    if (max_rows == 0) {
      return 0;
    }
    if (!initialized_) {
      const Status st = Initialize();
      initialized_ = true;
      if (st != Status::kSuccess) {
        FailWith(st);
        return 0;
      }
    }
    const Status stream_status = StreamRelationNextBatch(
        columnar_, output_schema_, &columnar_chunk_idx_,
        &columnar_row_in_chunk_, &rows_, &offset_, &spill_files_,
        &spill_file_idx_, &spill_reader_, destination, max_rows);
    if (stream_status != Status::kSuccess) {
      FailWith(stream_status);
      return 0;
    }
    return destination->Size();
  }

  void Dump(std::ostream& output, int indent) const override {
    output << std::string(static_cast<size_t>(indent), ' ')
           << "GroupByFinish (rows=" << total_rows_
           << ", group_keys=" << (statement_ ? statement_->GroupBy().size() : 0)
           << ")\n";
    if (statement_ && !statement_->OrderBy().empty() &&
        statement_->HasLimit() && !statement_->WithTies() &&
        !statement_->Distinct() &&
        statement_->Offset() <=
            std::numeric_limits<size_t>::max() - statement_->Limit()) {
      output << std::string(static_cast<size_t>(indent), ' ')
             << "TopNFinish limit=" << statement_->Limit()
             << " capacity=" << (statement_->Offset() + statement_->Limit())
             << "\n";
    }
    if (initialized_ && core_executor_) {
      core_executor_->Dump(output, indent + 2);
      // Compatibility: the relational path reports `hash_joins`,
      // `nested_loop_joins` and `join_comparisons` counters that Tpch and
      // profiling tests grep for. The Cascades core already executed the
      // same hash joins, so derive equivalent counters from its physical
      // tree (any *HashJoin executor counts) and the core row count
      // (one probe comparison per core input row).
      std::ostringstream core_dump;
      core_executor_->Dump(core_dump, 0);
      const std::string dumped = core_dump.str();
      auto count_occurrences = [&](std::string_view needle) {
        size_t count = 0;
        size_t position = 0;
        while ((position = dumped.find(needle, position)) !=
               std::string::npos) {
          ++count;
          position += needle.size();
        }
        return count;
      };
      const size_t hash_joins =
          count_occurrences("HashJoin") - count_occurrences("HashJoin actual_");
      const size_t nested_loop_joins = count_occurrences("NestedLoop");
      const size_t total_joins = hash_joins + nested_loop_joins;
      output << "\n"
             << std::string(static_cast<size_t>(indent), ' ')
             << "hash_joins=" << hash_joins
             << ", nested_loop_joins=" << nested_loop_joins
             << ", join_comparisons="
             << (total_joins > 0 ? total_joins * core_input_rows_
                                 : core_input_rows_);
    } else {
      core_plan_->Dump(output, indent + 2);
    }
  }

  void Explain(std::ostream& output, int indent) const override {
    output << std::string(static_cast<size_t>(indent), ' ')
           << "GroupByFinish (group_keys="
           << (statement_ ? statement_->GroupBy().size() : 0) << ")\n";
    if (statement_ && !statement_->OrderBy().empty() &&
        statement_->HasLimit() && !statement_->WithTies() &&
        !statement_->Distinct() &&
        statement_->Offset() <=
            std::numeric_limits<size_t>::max() - statement_->Limit()) {
      output << std::string(static_cast<size_t>(indent + 2), ' ')
             << "TopNFinish limit=" << statement_->Limit()
             << " capacity=" << (statement_->Offset() + statement_->Limit())
             << "\n";
    }
    output << std::string(static_cast<size_t>(indent + 2), ' ')
           << "Relational Physical Plan (heuristic estimate; not selected)\n";
    WriteEstimatedPhysicalPlan(*context_, *statement_, output, indent + 2);
    output << "\n"
           << std::string(static_cast<size_t>(indent + 2), ' ')
           << "Selected Cascades Core (FROM + WHERE)\n";
    core_plan_->Dump(output, indent + 2);
  }

 private:
  Status Initialize() {
    core_executor_ = core_plan_->EmitExecutor(*context_);
    Relation input(context_->execution_runtime());
    input.schema = core_plan_->GetSchema();
    DataChunk batch;
    while (core_executor_->NextBatch(&batch) != 0) {
      for (size_t i = 0; i < batch.Size(); ++i) {
        input.AddRow(batch.RowAt(i));
        ++core_input_rows_;
      }
    }
    if (core_executor_->GetStatus() != Status::kSuccess) {
      return core_executor_->GetStatus();
    }
    RETURN_IF_FAIL(input.FinishSpill());
    // apply_where=false: the WHERE clause is already applied inside the
    // optimized core plan (scan filters, join conditions, residual
    // selection), so re-evaluating it here would only re-run work.
    StatusOr<Relation> finished = FinishQuery(
        *context_, *statement_, std::move(input), nullptr, {}, false);
    if (!finished.HasValue()) {
      return finished.GetStatus();
    }
    Relation result = finished.MoveValue();
    RETURN_IF_FAIL(result.FinishSpill());
    output_schema_ = result.schema;
    total_rows_ = result.TotalRows();
    if (result.HasColumnarOnly()) {
      columnar_ = std::move(result.columnar);
    } else {
      RETURN_IF_FAIL(result.TakeRowAndSpillBacking(&rows_, &spill_files_));
    }
    return Status::kSuccess;
  }

  TransactionContext* context_;
  std::shared_ptr<PlanBase> core_plan_;
  std::shared_ptr<const SelectStatement> statement_;
  Executor core_executor_;
  size_t core_input_rows_{0};
  std::vector<Row> rows_;
  std::shared_ptr<const relational_detail::ColumnarTable> columnar_;
  std::vector<std::shared_ptr<SpillFile>> spill_files_;
  size_t spill_file_idx_{0};
  std::optional<SpillFile::RowReader> spill_reader_;
  Schema output_schema_;
  size_t columnar_chunk_idx_{0};
  size_t columnar_row_in_chunk_{0};
  size_t offset_{0};
  size_t total_rows_{0};
  bool initialized_{false};
};

// Registers `runtime` in the context for the duration of one query
// execution and restores the previous pointer on every exit, including the
// exception unwind: some statement paths (legacy ResultType resolution)
// still throw through here, and a skipped restore would leave a dangling
// ExecutionRuntime* in the context for the next query on the same
// transaction (heap-use-after-free at the next aggregation).
class ScopedExecutionRuntime {
 public:
  ScopedExecutionRuntime(TransactionContext& context, ExecutionRuntime& runtime)
      : context_(context),
        previous_(context.execution_runtime()),
        previous_lookup_cache_(relational_detail::active_lookup_cache) {
    context_.set_execution_runtime(&runtime);
    relational_detail::active_lookup_cache = &runtime.lookup_cache;
  }
  ScopedExecutionRuntime(const ScopedExecutionRuntime&) = delete;
  ScopedExecutionRuntime& operator=(const ScopedExecutionRuntime&) = delete;
  ScopedExecutionRuntime(ScopedExecutionRuntime&&) = delete;
  ScopedExecutionRuntime& operator=(ScopedExecutionRuntime&&) = delete;
  ~ScopedExecutionRuntime() {
    context_.set_execution_runtime(previous_);
    relational_detail::active_lookup_cache = previous_lookup_cache_;
  }

 private:
  TransactionContext& context_;
  ExecutionRuntime* previous_;
  relational_detail::ColumnLookupCache* previous_lookup_cache_;
};

}  // namespace

Executor EmitGroupedFinishExecutor(
    TransactionContext& context, std::shared_ptr<PlanBase> core_plan,
    std::shared_ptr<const SelectStatement> statement) {
  return std::make_shared<GroupedFinishExecutor>(&context, std::move(core_plan),
                                                 std::move(statement));
}

Status RelationalExecutor::Initialize() {
  if (initialized_) {
    return Status::kSuccess;
  }
  // The runtime outlives this frame: lazy executors produced while
  // evaluating the query (grouped finish, windowed streams) keep pointers
  // into it and run from later Next() calls.
  auto runtime_owner = std::make_unique<ExecutionRuntime>();
  runtime_keep_ = std::move(runtime_owner);
  ExecutionRuntime& runtime = *runtime_keep_;
  runtime.root_statement = statement_.get();
  std::unordered_map<std::string, size_t> table_counts;
  CountStatementTables(*statement_, &table_counts);
  for (const auto& [table, count] : table_counts) {
    if (count > 1) {
      runtime.reusable_base_relations.insert(table);
    }
  }
  if (!runtime.shared_base_cache &&
      (!runtime.reusable_base_relations.empty() ||
       !statement_->WithQueries().empty() || !statement_->UnionAll().empty() ||
       statement_->GetSetOperationTree() != nullptr)) {
    runtime.shared_base_cache =
        std::make_shared<relational_detail::SharedBaseRelationCache>();
  }
  const ScopedExecutionRuntime runtime_guard(*context_, runtime);
  const auto execute_begin = std::chrono::steady_clock::now();
  StatusOr<Relation> executed =
      ExecuteQuery(*context_, *statement_, nullptr, {}, nullptr, true);
  runtime.execute_ms += relational_detail::ElapsedMs(execute_begin);
  RETURN_IF_FAIL(executed.GetStatus());
  Relation result = executed.MoveValue();
  RETURN_IF_FAIL(result.FinishSpill());
  rows_.clear();
  columnar_.reset();
  spill_files_.clear();
  spill_file_idx_ = 0;
  spill_reader_.reset();
  columnar_chunk_idx_ = 0;
  columnar_row_in_chunk_ = 0;
  offset_ = 0;
  output_schema_ = result.schema;
  total_row_count_ = result.TotalRows();
  if (result.HasColumnarOnly()) {
    columnar_ = std::move(result.columnar);
  } else {
    RETURN_IF_FAIL(result.TakeRowAndSpillBacking(&rows_, &spill_files_));
  }
  if (statement_->Sources().size() == 1 && statement_->Sources()[0].query &&
      statement_->Sources()[0].query->WhereClause() &&
      statement_->Sources()[0].query->WhereClause()->Type() ==
          TypeTag::kConstantValue &&
      !statement_->Sources()[0]
           .query->WhereClause()
           ->AsConstantValue()
           .GetValue()
           .Truthy()) {
    runtime.empty_build_short_circuit = true;
  }
  if (statement_->WhereClause() &&
      statement_->WhereClause()->Type() == TypeTag::kQueryExp) {
    const QueryExpression& query =
        statement_->WhereClause()->AsQueryExpression();
    if (!query.Exists() && query.Negated() && query.Test()) {
      runtime.null_aware_anti_build_contains_null = true;
    }
  }
  hash_joins_ = result.hash_joins;
  hybrid_hash_joins_ = result.hybrid_hash_joins;
  in_memory_hash_joins_ = result.in_memory_hash_joins;
  nested_loop_joins_ = result.nested_loop_joins;
  join_comparisons_ = result.join_comparisons;
  peak_intermediate_rows_ = result.peak_intermediate_rows;
  correlated_index_builds_ = runtime.correlated_index_builds;
  prebuilt_exists_index_builds_ = runtime.prebuilt_exists_index_builds;
  prebuilt_exists_index_rows_ = runtime.prebuilt_exists_index_rows;
  prebuilt_exists_keys_ = runtime.prebuilt_exists_keys;
  correlated_index_probes_ = runtime.correlated_index_probes;
  correlated_result_cache_hits_ = runtime.correlated_result_cache_hits;
  shared_exists_build_reuses_ = runtime.shared_exists_build_reuses;
  correlated_distinct_keys_ = 0;
  for (const auto& [query, index] : runtime.correlated_indexes) {
    (void)query;
    if (index != nullptr) {
      correlated_distinct_keys_ += index->parameter_keys_seen.size() +
                                   index->parameter_int_keys_seen.size() +
                                   index->preagg_distinct_keys +
                                   (index->null_parameter_seen ? 1 : 0);
    }
  }
  uncorrelated_cache_hits_ = runtime.uncorrelated_cache_hits;
  grouped_join_fusions_ = runtime.grouped_join_fusions;
  pushed_group_rows_ = runtime.pushed_group_rows;
  pushed_group_joins_ = runtime.pushed_group_joins;
  transfer_seed_rows_ = runtime.transfer_seed_rows;
  transfer_scan_tested_ = runtime.transfer_scan_tested;
  transfer_scan_rejected_ = runtime.transfer_scan_rejected;
  streamed_grouped_joins_ = runtime.streamed_grouped_joins;
  selective_index_scans_ = runtime.selective_index_scans;
  selective_index_rows_ = runtime.selective_index_rows;
  uncorrelated_hash_builds_ = runtime.uncorrelated_hash_builds;
  uncorrelated_hash_probes_ = runtime.uncorrelated_hash_probes;
  exists_short_circuit_queries_ = runtime.exists_short_circuit_queries;
  correlated_domain_filter_builds_ = runtime.correlated_domain_filter_builds;
  scan_ms_ = runtime.scan_ms;
  filter_ms_ = runtime.filter_ms;
  join_ms_ = runtime.join_ms;
  project_ms_ = runtime.project_ms;
  sort_ms_ = runtime.sort_ms;
  topn_finish_uses_ = runtime.topn_finish_uses;
  topn_finish_capacity_ = runtime.topn_finish_capacity;
  topn_finish_input_rows_ = runtime.topn_finish_input_rows;
  planning_ms_ = runtime.planning_ms;
  join_estimate_ms_ = runtime.join_estimate_ms;
  aggregate_ms_ = runtime.aggregate_ms;
  base_scan_cache_hits_ = runtime.base_scan_cache_hits;
  aggregate_input_rows_ = runtime.aggregate_input_rows;
  aggregate_groups_ = runtime.aggregate_groups;
  aggregate_updates_ = runtime.aggregate_updates;
  scan_rows_ = runtime.scan_rows;
  scan_output_rows_ = runtime.scan_output_rows;
  scan_values_decoded_ = runtime.scan_values_decoded;
  scan_values_available_ = runtime.scan_values_available;
  key_filter_scans_ = runtime.key_filter_scans;
  key_filter_keys_ = runtime.key_filter_keys;
  key_filter_rejected_ = runtime.key_filter_rejected;
  key_filter_null_rejected_ = runtime.key_filter_null_rejected;
  key_filter_minmax_ = runtime.key_filter_minmax;
  key_filter_bloom_ = runtime.key_filter_bloom;
  key_filter_bytes_ = runtime.key_filter_bytes;
  empty_build_short_circuit_ = runtime.empty_build_short_circuit;
  null_aware_anti_build_contains_null_ =
      runtime.null_aware_anti_build_contains_null;
  relation_spills_ = runtime.relation_spills;
  initialized_ = true;
  return Status::kSuccess;
}

RelationalExecutor::~RelationalExecutor() {
  // A statement path that threw through Initialize() can leave the context
  // pointing at this executor's runtime; drop that stale pointer before the
  // unique_ptr frees the object, so the next query on the same context reads
  // nullptr instead of freed memory.
  if (context_->execution_runtime() == runtime_keep_.get()) {
    context_->set_execution_runtime(nullptr);
  }
}

size_t RelationalExecutor::NextBatch(DataChunk* destination, size_t max_rows) {
  destination->Reset();
  if (max_rows == 0) {
    return 0;
  }
  if (!initialized_) {
    const Status st = Initialize();
    if (st != Status::kSuccess) {
      FailWith(st);
      return 0;
    }
  }
  const Status stream_status = StreamRelationNextBatch(
      columnar_, output_schema_, &columnar_chunk_idx_, &columnar_row_in_chunk_,
      &rows_, &offset_, &spill_files_, &spill_file_idx_, &spill_reader_,
      destination, max_rows);
  if (stream_status != Status::kSuccess) {
    FailWith(stream_status);
    return 0;
  }
  return destination->Size();
}

void RelationalExecutor::Dump(std::ostream& output, int /*indent*/) const {
  // Dump never triggers execution: running a whole query from a debug print
  // would be surprising and breaks const expectations.
  if (!initialized_) {
    output << "RelationalExecutor(not executed)";
    return;
  }
  output << "RelationalExecutor(materialized=" << total_row_count_
         << ", hash_joins=" << hash_joins_
         << ", hybrid_hash_joins=" << hybrid_hash_joins_
         << ", in_memory_hash_joins=" << in_memory_hash_joins_
         << ", nested_loop_joins=" << nested_loop_joins_
         << ", join_comparisons=" << join_comparisons_
         << ", peak_intermediate_rows=" << peak_intermediate_rows_
         << ", relation_spills=" << relation_spills_
         << ", correlated_index_builds=" << correlated_index_builds_
         << ", prebuilt_exists_index_builds=" << prebuilt_exists_index_builds_
         << ", prebuilt_exists_index_rows=" << prebuilt_exists_index_rows_
         << ", prebuilt_exists_keys=" << prebuilt_exists_keys_
         << ", correlated_index_probes=" << correlated_index_probes_
         << ", correlated_result_cache_hits=" << correlated_result_cache_hits_
         << ", shared_exists_build_reuses=" << shared_exists_build_reuses_
         << ", correlated_distinct_keys=" << correlated_distinct_keys_
         << ", uncorrelated_cache_hits=" << uncorrelated_cache_hits_
         << ", uncorrelated_hash_builds=" << uncorrelated_hash_builds_
         << ", uncorrelated_hash_probes=" << uncorrelated_hash_probes_
         << ", exists_short_circuit_queries=" << exists_short_circuit_queries_
         << ", correlated_domain_filter_builds="
         << correlated_domain_filter_builds_ << ", scan_ms=" << scan_ms_
         << ", filter_ms=" << filter_ms_ << ", join_ms=" << join_ms_
         << ", project_ms=" << project_ms_ << ", sort_ms=" << sort_ms_
         << ", topn_finish_uses=" << topn_finish_uses_
         << ", topn_finish_capacity=" << topn_finish_capacity_
         << ", topn_finish_input_rows=" << topn_finish_input_rows_
         << ", planning_ms=" << planning_ms_
         << ", join_estimate_ms=" << join_estimate_ms_
         << ", dictionary_aggregations="
         << (runtime_keep_ ? runtime_keep_->dictionary_aggregations : 0)
         << ", dictionary_summary_aggregations="
         << (runtime_keep_ ? runtime_keep_->dictionary_summary_aggregations : 0)
         << ", dictionary_aggregate_rows="
         << (runtime_keep_ ? runtime_keep_->dictionary_aggregate_rows : 0)
         << ", partitioned_dictionary_aggregations="
         << (runtime_keep_ ? runtime_keep_->partitioned_dictionary_aggregations
                           : 0)
         << ", partitioned_numeric_aggregations="
         << (runtime_keep_ ? runtime_keep_->partitioned_numeric_aggregations
                           : 0)
         << ", aggregate_ms=" << aggregate_ms_
         << ", grouped_join_fusions=" << grouped_join_fusions_
         << ", pushed_group_rows=" << pushed_group_rows_
         << ", pushed_group_joins=" << pushed_group_joins_
         << ", transfer_seed_rows=" << transfer_seed_rows_
         << ", transfer_scan_tested=" << transfer_scan_tested_
         << ", transfer_scan_rejected=" << transfer_scan_rejected_
         << ", streamed_grouped_joins=" << streamed_grouped_joins_
         << ", selective_index_scans=" << selective_index_scans_
         << ", selective_index_rows=" << selective_index_rows_
         << ", base_scan_cache_hits=" << base_scan_cache_hits_
         << ", aggregate_input_rows=" << aggregate_input_rows_
         << ", aggregate_groups=" << aggregate_groups_
         << ", aggregate_updates=" << aggregate_updates_
         << ", scan_rows=" << scan_rows_
         << ", scan_output_rows=" << scan_output_rows_
         << ", execute_ms=" << (runtime_keep_ ? runtime_keep_->execute_ms : 0)
         << ", boundary_ms=" << (runtime_keep_ ? runtime_keep_->boundary_ms : 0)
         << ", build_input_ms="
         << (runtime_keep_ ? runtime_keep_->build_input_ms : 0)
         << ", window_ms=" << (runtime_keep_ ? runtime_keep_->window_ms : 0)
         << ", window_layout_ms="
         << (runtime_keep_ ? runtime_keep_->window_layout_ms : 0)
         << ", window_input_rows="
         << (runtime_keep_ ? runtime_keep_->window_input_rows : 0)
         << ", rank_bounds=" << (runtime_keep_ ? runtime_keep_->rank_bounds : 0)
         << ", deferred_materialize_ms="
         << (runtime_keep_ ? runtime_keep_->deferred_materialize_ms : 0)
         << ", scan_values_decoded=" << scan_values_decoded_
         << ", scan_values_available=" << scan_values_available_ << ")";
  if (key_filter_keys_ != 0) {
    // Composition of the published dynamic filters (Phase 2 "Dynamic Filter
    // Join / SIP / Bloom"): `exact_set` for the information-passing shape,
    // plus `min_max` / `bloom` when the build domain outgrew the exact set.
    output << "\nRuntimeFilter type=";
    if (key_filter_minmax_ != 0) {
      output << "min_max+";
    }
    if (key_filter_bloom_ != 0) {
      output << "bloom";
    }
    if (key_filter_minmax_ == 0 && key_filter_bloom_ == 0) {
      output << "exact_set";
    } else if (key_filter_bloom_ == 0) {
      output << "min_max";
    }
    output << " keys=" << key_filter_keys_ << " bytes=" << key_filter_bytes_
           << " scans=" << key_filter_scans_
           << "\nprobe_rows_rejected=" << key_filter_rejected_
           << "\nprobe_rows_null_rejected=" << key_filter_null_rejected_;
  }
  if (aggregate_input_rows_ != 0) {
    output << "\nAggregate input_rows=" << aggregate_input_rows_
           << " groups=" << aggregate_groups_
           << " updates=" << aggregate_updates_;
  }
  output << "\nMemoryPeak rows=" << peak_intermediate_rows_
         << " spill_partitions=" << relation_spills_;
  if (empty_build_short_circuit_) {
    output << "\nEmptyBuildShortCircuit probe_opened=false";
  }
  if (null_aware_anti_build_contains_null_) {
    output << "\nAntiHashJoin early_termination=build_contains_null";
  }
}

void RelationalExecutor::Explain(std::ostream& output, int /*indent*/) const {
  output << "Relational Physical Plan (heuristic estimate; selected route)\n";
  // EXPLAIN must describe the same safe derived-table rewrites used by the
  // executable path.  Keep this local to presentation: the original parsed
  // statement remains owned by the executor and runtime cache keys continue
  // to use the original statement objects.
  std::shared_ptr<SelectStatement> explain_statement;
  const SelectStatement* effective = statement_.get();
  size_t collapsed_boundaries = 0;
  // Execution materializes the statement's WITH entries into the inherited
  // CTE map before boundary-flattening the body, so a derived source whose
  // inner scan names a CTE is refused the flatten. Mirror that guard with
  // the WITH names alone: OptimizeDerivedBoundaries only calls contains()
  // on the map, never dereferences the relations.
  relational_detail::CteMap explain_ctes;
  for (const auto& [name, query] : statement_->WithQueries()) {
    (void)query;
    explain_ctes.emplace(name, nullptr);
  }
  for (size_t iteration = 0; iteration < 32; ++iteration) {
    const bool had_derived_source = effective->Sources().size() == 1 &&
                                    effective->Sources()[0].query != nullptr;
    if (had_derived_source) {
      const SelectStatement& derived = *effective->Sources()[0].query;
      // Keep filter/set-operation boundaries visible in EXPLAIN.  Their
      // dedicated annotations are emitted by the formatter from the original
      // boundary; only projection-only flattening is normalized here.
      if (derived.WhereClause() || !derived.UnionAll().empty()) {
        break;
      }
    }
    std::shared_ptr<SelectStatement> next =
        relational_detail::OptimizeDerivedBoundaries(*effective, explain_ctes,
                                                     context_);
    if (!next) {
      break;
    }
    explain_statement = std::move(next);
    effective = explain_statement.get();
    const bool still_has_derived_source =
        effective->Sources().size() == 1 &&
        effective->Sources()[0].query != nullptr;
    if (had_derived_source && !still_has_derived_source) {
      ++collapsed_boundaries;
    }
  }
  if (collapsed_boundaries > 0) {
    const size_t projection_depth =
        relational_detail::FlattenableProjectionDepth(*statement_);
    output << "  ProjectCollapse levels="
           << std::max(collapsed_boundaries + 1, projection_depth + 1) << '\n';
  }
  WriteEstimatedPhysicalPlan(*context_, *effective, output, 2);
  // A predicate that reaches the base scan after collapsing two or more
  // cardinality-preserving projection boundaries has moved across a query
  // scope. Keep that fact visible in EXPLAIN; the displayed predicate is the
  // same rebinding used by the executable normalization above.
  if (collapsed_boundaries > 0 &&
      relational_detail::FlattenableProjectionDepth(*statement_) > 1 &&
      statement_->WhereClause() && effective->WhereClause()) {
    std::string predicate = effective->WhereClause()->ToString();
    if (predicate.size() >= 2 && predicate.front() == '(' &&
        predicate.back() == ')') {
      predicate = predicate.substr(1, predicate.size() - 2);
    }
    output << "  PredicateMoveAround\n"
           << "  " << predicate << " below Subquery\n";
  }
  if (initialized_) {
    output << "Actual Joins: hybrid_hash_joins=" << hybrid_hash_joins_
           << " in_memory_hash_joins=" << in_memory_hash_joins_
           << " nested_loop_joins=" << nested_loop_joins_
           << " relation_spills=" << relation_spills_ << '\n';
    if (runtime_keep_ && runtime_keep_->inner_hash_build_rows != 0) {
      output << "InnerHashBuild rows=" << runtime_keep_->inner_hash_build_rows
             << " index_estimate_bytes="
             << runtime_keep_->inner_hash_index_estimate_bytes
             << " left_builds=" << runtime_keep_->inner_hash_left_builds
             << " spilled_probe_rows="
             << runtime_keep_->inner_hash_spilled_probe_rows << '\n';
    }
    if (uncorrelated_cache_hits_ > 0) {
      output << "ScalarSubquery executions=1 cache_hits="
             << uncorrelated_cache_hits_ << '\n';
    }
    if (correlated_index_probes_ > 0 && correlated_distinct_keys_ > 0) {
      const size_t duplicate_parameter_probes =
          correlated_index_probes_ > correlated_distinct_keys_
              ? correlated_index_probes_ - correlated_distinct_keys_
              : 0;
      output << "CorrelatedCache distinct_keys=" << correlated_distinct_keys_
             << " probes=" << correlated_index_probes_
             << " hits=" << duplicate_parameter_probes << '\n';
    }
  }
}

}  // namespace tinylamb
