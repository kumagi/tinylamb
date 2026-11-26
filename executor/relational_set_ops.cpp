/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/detail/relational_set_ops.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <cmath>
#include <unordered_map>
#include <vector>

#include "common/parallel_work.hpp"
#include "executor/detail/columnar_table.hpp"
#include "executor/set_operation.hpp"

namespace tinylamb::relational_detail {

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
    cols.reserve(rel.schema.ColumnCount());
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

namespace {
inline uint64_t MixSetHash(uint64_t h) noexcept {
  h ^= h >> 30;
  h *= 0xbf58476d1ce4e5b9ULL;
  h ^= h >> 27;
  h *= 0x94d049bb133111ebULL;
  h ^= h >> 31;
  return h == 0 ? 1 : h;
}

}  // namespace

Status DistinctResidentRows(Relation& relation) {
  const size_t n = relation.rows.size();
  if (n <= 1) {
    return Status::kSuccess;
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
    return Status::kSuccess;
  }

  struct HashedRowRef {
    size_t row;
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
          HashedRowRef{.row = i, .hash = h});
    }
  });
  RETURN_IF_FAIL(part_st);

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
      slots.assign(cap, HashedRowRef{.row = SIZE_MAX, .hash = 0});
      for (size_t w = 0; w < workers; ++w) {
        for (const HashedRowRef& item : buckets[(w * partitions) + p]) {
          size_t pos = item.hash & s_mask;
          while (slots[pos].row != SIZE_MAX) {
            if (slots[pos].hash == item.hash &&
                eq(relation.rows[slots[pos].row], relation.rows[item.row])) {
              break;
            }
            pos = (pos + 1) & s_mask;
          }
          if (slots[pos].row == SIZE_MAX) {
            slots[pos] = item;
            keep[item.row] = 1;
          }
        }
      }
    }
  });
  RETURN_IF_FAIL(dedup_st);
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
  return Status::kSuccess;
}

// Keep hash keys in their original relations until all membership and
// multiplicity decisions are complete; moving a row earlier invalidates keys.
Status ReduceResidentRows(Relation& left, const Relation& right,
                          SetOperationKind operation) {
  const bool all = operation == SetOperationKind::kIntersectAll ||
                   operation == SetOperationKind::kExceptAll;
  const bool intersection = operation == SetOperationKind::kIntersect ||
                            operation == SetOperationKind::kIntersectAll;
  const size_t nl = left.rows.size();
  const size_t nr = right.rows.size();
  if (nl == 0) {
    return Status::kSuccess;
  }
  if (nr == 0) {
    if (intersection) {
      left.rows.clear();
    } else if (!all) {
      RETURN_IF_FAIL(DistinctResidentRows(left));
    }
    return Status::kSuccess;
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
    return Status::kSuccess;
  }

  struct HashedRowRef {
    size_t row;
    uint64_t hash;
  };
  struct RightSlot {
    size_t row{SIZE_MAX};
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
          HashedRowRef{.row = i, .hash = h});
    }
    for (size_t i = l_begin; i < l_end; ++i) {
      const uint64_t h = MixSetHash(std::hash<Row>{}(left.rows[i]));
      left_buckets[base + ((h >> 32) & p_mask)].push_back(
          HashedRowRef{.row = i, .hash = h});
    }
  });
  RETURN_IF_FAIL(part_st);

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
            while (slots[pos].row != SIZE_MAX) {
              if (slots[pos].hash == item.hash &&
                  eq(left.rows[slots[pos].row], left.rows[item.row])) {
                break;
              }
              pos = (pos + 1) & s_mask;
            }
            if (slots[pos].row == SIZE_MAX) {
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
          while (slots[pos].row != SIZE_MAX) {
            if (slots[pos].hash == item.hash &&
                eq(right.rows[slots[pos].row], right.rows[item.row])) {
              ++slots[pos].count;
              break;
            }
            pos = (pos + 1) & s_mask;
          }
          if (slots[pos].row == SIZE_MAX) {
            slots[pos] = RightSlot{
                .row = item.row, .hash = item.hash, .count = 1, .emitted = 0};
          }
        }
      }
      for (size_t w = 0; w < workers; ++w) {
        for (const HashedRowRef& item : left_buckets[(w * partitions) + p]) {
          size_t pos = item.hash & s_mask;
          while (slots[pos].row != SIZE_MAX) {
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
            if (slots[pos].row != SIZE_MAX && slots[pos].count != 0) {
              if (all) {
                --slots[pos].count;
                keep[item.row] = 1;
              } else if (slots[pos].emitted == 0) {
                slots[pos].emitted = 1;
                keep[item.row] = 1;
              }
            }
          } else {
            if (slots[pos].row == SIZE_MAX) {
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
  RETURN_IF_FAIL(reduce_st);
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
  return Status::kSuccess;
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

namespace {

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

// Precomputed per-column fast-gather mode for MaterializeSetChunk:
// 0 = general tile-cached path, 1 = flat non-null, 2 = flat nullable,
// 3 = 1-hop kGathered -> kFlat uniform.
std::vector<uint8_t> ClassifySetMaterializeColumns(
    const ColumnarTable& src_tbl, std::span<const uint8_t> side_all_null,
    std::span<const ValueType> canon_types) {
  const size_t ncols = canon_types.size();
  std::vector<uint8_t> modes(ncols, 0);
  const auto& chunks = src_tbl.chunks;
  if (chunks.empty()) {
    return modes;
  }
  for (size_t c = 0; c < ncols; ++c) {
    if (side_all_null[c] != 0) {
      continue;
    }
    const ValueType vt = canon_types[c];
    if (vt != ValueType::kInt64 && vt != ValueType::kDate &&
        vt != ValueType::kDouble) {
      continue;
    }
    bool all_flat = true;
    bool any_null = false;
    bool all_1hop = true;
    std::unordered_set<const ColumnarTable*> verified_leaf_tables;
    for (const ColumnarChunk& ch : chunks) {
      if (ch.rows == 0) {
        continue;
      }
      const ColumnarColumn& sc = ch.columns[c];
      if (all_flat) {
        if (!sc.IsFlat() || !sc.is_uniform || sc.type != vt) {
          all_flat = false;
        } else if (sc.has_null) {
          any_null = true;
        }
      }
      if (all_1hop) {
        if (sc.encoding != ColumnarColumn::Encoding::kGathered ||
            sc.gather_index == nullptr ||
            sc.gather_index->source_table == nullptr) {
          all_1hop = false;
        } else {
          const ColumnarTable* ltbl = sc.gather_index->source_table.get();
          if (verified_leaf_tables.insert(ltbl).second) {
            const size_t gcol = sc.gather_col;
            for (const ColumnarChunk& sch : ltbl->chunks) {
              if (sch.rows == 0) {
                continue;
              }
              if (gcol >= sch.columns.size()) {
                all_1hop = false;
                break;
              }
              const ColumnarColumn& leaf = sch.columns[gcol];
              if (!leaf.IsFlat() || !leaf.is_uniform || leaf.type != vt) {
                all_1hop = false;
                break;
              }
            }
          }
        }
      }
      if (!all_flat && !all_1hop) {
        break;
      }
    }
    if (all_flat) {
      modes[c] = any_null ? 2 : 1;
    } else if (all_1hop) {
      modes[c] = 3;
    }
  }
  return modes;
}

ColumnarChunk MaterializeSetChunk(
    const std::shared_ptr<const ColumnarTable>& src_tbl,
    std::span<const std::pair<uint32_t, uint32_t>> refs,
    std::span<const uint8_t> side_all_null,
    std::span<const ValueType> canon_types,
    std::span<const uint8_t> canon_unsigned, std::span<const uint8_t> col_modes,
    bool* used_varchar) {
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

  // Fast direct gather using precomputed per-column modes (avoids per-cell
  // ResolveNonNullSetLeafCached + tile-cache search in large UNION DISTINCT
  // materialization such as TPC-DS Q75).
  bool any_remaining_col = false;
  for (size_t c = 0; c < ncols; ++c) {
    if (skip_col[c] != 0) {
      continue;
    }
    const uint8_t mode = c < col_modes.size() ? col_modes[c] : uint8_t{0};
    if (mode == 0) {
      any_remaining_col = true;
      continue;
    }
    const ValueType vt = canon_types[c];
    ColumnarColumn& dst = out_chunk.columns[c];
    auto mark_null_fast = [&](size_t i) {
      if (col_has_null[c] == 0) {
        dst.has_null = true;
        dst.nulls.assign(count, 0);
        col_has_null[c] = 1;
      }
      dst.nulls[i] = 1;
    };
    if (mode == 1 || mode == 2) {
      if (vt == ValueType::kInt64 || vt == ValueType::kDate) {
        int64_t* __restrict__ out_ptr = dst.ints.data();
        if (mode == 1) {
          for (size_t i = 0; i < count; ++i) {
            const auto [ci, ri] = refs[i];
            out_ptr[i] = chunks[ci].columns[c].ints[ri];
          }
        } else {
          for (size_t i = 0; i < count; ++i) {
            const auto [ci, ri] = refs[i];
            const ColumnarColumn& sc = chunks[ci].columns[c];
            if (sc.has_null && sc.nulls[ri] != 0) [[unlikely]] {
              mark_null_fast(i);
              out_ptr[i] = 0;
            } else {
              out_ptr[i] = sc.ints[ri];
            }
          }
        }
      } else {
        double* __restrict__ out_ptr = dst.doubles.data();
        if (mode == 1) {
          for (size_t i = 0; i < count; ++i) {
            const auto [ci, ri] = refs[i];
            out_ptr[i] = chunks[ci].columns[c].doubles[ri];
          }
        } else {
          for (size_t i = 0; i < count; ++i) {
            const auto [ci, ri] = refs[i];
            const ColumnarColumn& sc = chunks[ci].columns[c];
            if (sc.has_null && sc.nulls[ri] != 0) [[unlikely]] {
              mark_null_fast(i);
              out_ptr[i] = 0.0;
            } else {
              out_ptr[i] = sc.doubles[ri];
            }
          }
        }
      }
    } else if (mode == 3) {
      if (vt == ValueType::kInt64 || vt == ValueType::kDate) {
        int64_t* __restrict__ out_ptr = dst.ints.data();
        for (size_t i = 0; i < count; ++i) {
          const auto [ci, ri] = refs[i];
          const ColumnarColumn& sc = chunks[ci].columns[c];
          const ChunkGatherIndex* gi = sc.gather_index.get();
          const uint32_t tr = gi->src_rows[ri];
          if (tr == ChunkGatherIndex::kNullRow) [[unlikely]] {
            mark_null_fast(i);
            out_ptr[i] = 0;
            continue;
          }
          const ColumnarColumn& leaf =
              gi->source_table->chunks[gi->src_chunks[ri]]
                  .columns[sc.gather_col];
          if (leaf.has_null && leaf.nulls[tr] != 0) [[unlikely]] {
            mark_null_fast(i);
            out_ptr[i] = 0;
          } else {
            out_ptr[i] = leaf.ints[tr];
          }
        }
      } else {
        double* __restrict__ out_ptr = dst.doubles.data();
        for (size_t i = 0; i < count; ++i) {
          const auto [ci, ri] = refs[i];
          const ColumnarColumn& sc = chunks[ci].columns[c];
          const ChunkGatherIndex* gi = sc.gather_index.get();
          const uint32_t tr = gi->src_rows[ri];
          if (tr == ChunkGatherIndex::kNullRow) [[unlikely]] {
            mark_null_fast(i);
            out_ptr[i] = 0.0;
            continue;
          }
          const ColumnarColumn& leaf =
              gi->source_table->chunks[gi->src_chunks[ri]]
                  .columns[sc.gather_col];
          if (leaf.has_null && leaf.nulls[tr] != 0) [[unlikely]] {
            mark_null_fast(i);
            out_ptr[i] = 0.0;
          } else {
            out_ptr[i] = leaf.doubles[tr];
          }
        }
      }
    }
    skip_col[c] = 1;
  }

  constexpr size_t kTileRows = 64;
  for (size_t base = 0; any_remaining_col && base < count; base += kTileRows) {
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

  const std::vector<uint8_t> col_modes =
      ClassifySetMaterializeColumns(*src_tbl, side_all_null, canon_types);
  std::vector<ColumnarChunk> out_chunks(ranges.size());
  std::atomic<bool> any_varchar{false};
  if (parallel_build && ranges.size() > 1) {
    const Status st = RunParallelTasks(ranges.size(), [&](size_t bi) {
      bool local_varchar = false;
      out_chunks[bi] = MaterializeSetChunk(
          src_tbl, {ranges[bi].ptr, ranges[bi].count}, side_all_null,
          canon_types, canon_unsigned, col_modes, &local_varchar);
      if (local_varchar) {
        any_varchar.store(true, std::memory_order_relaxed);
      }
    });
    if (!st.ok()) {
      // A worker that never started leaves out_chunks[bi] empty. Redo every
      // range serially rather than emitting partially materialized chunks or
      // silently dropping the thread-creation failure.
      any_varchar.store(false, std::memory_order_relaxed);
      for (size_t bi = 0; bi < ranges.size(); ++bi) {
        bool local_varchar = false;
        out_chunks[bi] = MaterializeSetChunk(
            src_tbl, {ranges[bi].ptr, ranges[bi].count}, side_all_null,
            canon_types, canon_unsigned, col_modes, &local_varchar);
        if (local_varchar) {
          any_varchar.store(true, std::memory_order_relaxed);
        }
      }
    }
  } else {
    bool local_varchar = false;
    for (size_t bi = 0; bi < ranges.size(); ++bi) {
      out_chunks[bi] = MaterializeSetChunk(
          src_tbl, {ranges[bi].ptr, ranges[bi].count}, side_all_null,
          canon_types, canon_unsigned, col_modes, &local_varchar);
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
      auto& target_parts =
          slice.chunk_idx < split_chunk_idx ? wb.group0 : wb.group1;
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

}  // namespace

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
    std::copy(
        side_all_null.begin(), side_all_null.end(),
        per_input_all_null.begin() + static_cast<std::ptrdiff_t>(idx * ncols));
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

}  // namespace tinylamb::relational_detail
