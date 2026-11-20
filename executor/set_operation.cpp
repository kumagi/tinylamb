/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "set_operation.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <ostream>
#include <stdexcept>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/exc_shim.hpp"
#include "common/parallel_work.hpp"
#include "common/query_parallelism.hpp"
#include "common/set_operation.hpp"
#include "executor/distinct.hpp"
#include "executor/executor_base.hpp"
#include "executor/query_memory.hpp"
#include "executor/spill_file.hpp"
#include "type/collation.hpp"
#include "type/row.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

namespace {

constexpr size_t kSetOperationPartitions = 32;

// Set operations use SQL equality, which folds case-insensitive VARCHAR text.
// The hash must fold the same text or CI peers land in different shards /
// spill partitions and are never compared with SetOperationRowEqual.
using SetOperationRowHash = DistinctRowHash;

StatusOr<ValueType> CommonSetValueType(ValueType left, ValueType right) {
  if (left == ValueType::kNull) {
    return right;
  }
  if (right == ValueType::kNull || left == right) {
    return left;
  }
  if ((left == ValueType::kInt64 && right == ValueType::kDouble) ||
      (left == ValueType::kDouble && right == ValueType::kInt64)) {
    return ValueType::kDouble;
  }
  // Textual types (STRING / NUMERIC / BIGNUMERIC literals) unify with
  // numerics by coercing the numeric side to text.
  const bool left_text = left == ValueType::kVarChar;
  const bool right_text = right == ValueType::kVarChar;
  // A typed DATE column followed by an untyped string literal keeps the
  // typed side's domain. Converting DATE back to STRING loses date
  // comparison semantics after CREATE TABLE AS SELECT.
  if ((left == ValueType::kDate && right_text) ||
      (right == ValueType::kDate && left_text)) {
    return ValueType::kDate;
  }
  // DATE was handled above; keep only the numeric coercion cases here.
  if ((left_text &&
       (right == ValueType::kInt64 || right == ValueType::kDouble)) ||
      (right_text &&
       (left == ValueType::kInt64 || left == ValueType::kDouble))) {
    return ValueType::kVarChar;
  }
  return StatusError(StatusCode::kInvalidArgument,
                     "set operation inputs have incompatible types");
}

}  // namespace

bool SetOperationRowEqual::operator()(const Row& left, const Row& right) const {
  if (left.values_.size() != right.values_.size()) {
    return false;
  }
  for (size_t i = 0; i < left.values_.size(); ++i) {
    const Value& a = left.values_[i];
    const Value& b = right.values_[i];
    if (a.type != b.type) {
      return false;
    }
    if (a.type == ValueType::kDouble || a.type == ValueType::kArray) {
      if (CompareForOrderBy(a, b) != 0) {
        return false;
      }
    } else if (a.type == ValueType::kVarChar) {
      // One COLLATE tag on either side resolves the comparison for both;
      // Value::operator== compares raw bytes and would keep 'A' and 'a' as
      // two distinct rows even though `=` folds them (set_operation.hpp's
      // "SQL equality" contract).
      if (!EqualUnderCollation(a, b)) {
        return false;
      }
    } else if (!(a == b)) {
      return false;
    }
  }
  return true;
}

Status SetOperationExecutor::MaterializeRows(
    std::vector<std::vector<Positioned>> rows) {
  if (rows.empty()) {
    return StatusError(StatusCode::kInvalidArgument,
                       "set operation needs at least one source");
  }
  size_t total_rows = 0;
  for (const auto& source : rows) {
    total_rows += source.size();
  }
  const size_t workers =
      total_rows < 16384
          ? 1
          : std::min<size_t>({8, total_rows / 4096,
                              QueryParallelWorkerLimit(
                                  std::thread::hardware_concurrency())});
  size_t width = 0;
  bool width_known = false;
  for (const auto& source : rows) {
    if (!source.empty()) {
      width = source.front().row.values_.size();
      width_known = true;
      break;
    }
  }
  if (width_known) {
    if (workers <= 1) {
      for (const auto& source : rows) {
        for (const Positioned& item : source) {
          if (item.row.values_.size() != width) {
            return StatusError(
                StatusCode::kInvalidArgument,
                "set operation inputs must have the same column count");
          }
        }
      }
      std::vector<ValueType> common_types(width, ValueType::kNull);
      for (const auto& source : rows) {
        for (const Positioned& item : source) {
          for (size_t column = 0; column < width; ++column) {
            ASSIGN_OR_RETURN(
                ValueType, common,
                (CommonSetValueType(common_types[column],
                                    item.row.values_[column].type)));
            common_types[column] = common;
          }
        }
      }
      for (auto& source : rows) {
        for (Positioned& item : source) {
          for (size_t column = 0; column < width; ++column) {
            Value& value = item.row.values_[column];
            if (value.IsNull() || value.type == common_types[column]) {
              continue;
            }
            if (common_types[column] == ValueType::kDouble &&
                value.type == ValueType::kInt64) {
              value = Value(value.IsUnsigned()
                                    ? static_cast<double>(static_cast<uint64_t>(
                                          value.value.int_value))
                                    : static_cast<double>(value.value.int_value));
              continue;
            }
            if (common_types[column] == ValueType::kDate &&
                value.type == ValueType::kVarChar) {
              ASSIGN_OR_RETURN(Value, date_value,
                               (Value::TryDate(value.value.varchar_value)));
              value = std::move(date_value);
              continue;
            }
            if (common_types[column] == ValueType::kVarChar) {
              value = Value(value.AsString());
              continue;
            }
            return StatusError(
                StatusCode::kInvalidArgument,
                "set operation value cannot be coerced to common type");
          }
        }
      }
    } else {
      std::vector<std::vector<ValueType>> worker_types(
          workers, std::vector<ValueType>(width, ValueType::kNull));
      std::vector<Status> worker_errors(workers, Status::kSuccess);
      std::vector<uint8_t> worker_mixed(workers, 0);
      RETURN_IF_FAIL(RunParallelTasks(workers, [&](size_t w) {
        auto& local_types = worker_types[w];
        for (const auto& source : rows) {
          const size_t lo = (source.size() * w) / workers;
          const size_t hi = (source.size() * (w + 1)) / workers;
          for (size_t i = lo; i < hi; ++i) {
            const Row& r = source[i].row;
            if (r.values_.size() != width) {
              worker_errors[w] = StatusError(
                  StatusCode::kInvalidArgument,
                  "set operation inputs must have the same column count");
              return;
            }
            for (size_t column = 0; column < width; ++column) {
              const ValueType vt = r.values_[column].type;
              if (vt == ValueType::kNull || vt == local_types[column]) {
                continue;
              }
              if (local_types[column] == ValueType::kNull) {
                local_types[column] = vt;
                continue;
              }
              StatusOr<ValueType> common =
                  CommonSetValueType(local_types[column], vt);
              if (!common.HasValue()) {
                worker_errors[w] = common.GetStatus();
                return;
              }
              local_types[column] = common.Value();
              worker_mixed[w] = 1;
            }
          }
        }
      }));
      for (const Status& st : worker_errors) {
        if (st != Status::kSuccess) {
          return st;
        }
      }
      std::vector<ValueType> common_types(width, ValueType::kNull);
      for (size_t w = 0; w < workers; ++w) {
        for (size_t column = 0; column < width; ++column) {
          ASSIGN_OR_RETURN(ValueType, common,
                           (CommonSetValueType(common_types[column],
                                               worker_types[w][column])));
          common_types[column] = common;
        }
      }
      bool any_coercion =
          std::ranges::any_of(worker_mixed, [](uint8_t m) { return m != 0; });
      for (size_t w = 0; !any_coercion && w < workers; ++w) {
        for (size_t column = 0; column < width; ++column) {
          if (worker_types[w][column] != ValueType::kNull &&
              worker_types[w][column] != common_types[column]) {
            any_coercion = true;
            break;
          }
        }
      }
      if (any_coercion) {
        std::fill(worker_errors.begin(), worker_errors.end(), Status::kSuccess);
        RETURN_IF_FAIL(RunParallelTasks(workers, [&](size_t w) {
          for (auto& source : rows) {
            const size_t lo = (source.size() * w) / workers;
            const size_t hi = (source.size() * (w + 1)) / workers;
            for (size_t i = lo; i < hi; ++i) {
              Row& r = source[i].row;
              for (size_t column = 0; column < width; ++column) {
                Value& value = r.values_[column];
                if (value.IsNull() || value.type == common_types[column]) {
                  continue;
                }
                if (common_types[column] == ValueType::kDouble &&
                    value.type == ValueType::kInt64) {
                  value = Value(value.IsUnsigned()
                                    ? static_cast<double>(static_cast<uint64_t>(
                                          value.value.int_value))
                                    : static_cast<double>(value.value.int_value));
                  continue;
                }
                if (common_types[column] == ValueType::kDate &&
                    value.type == ValueType::kVarChar) {
                  StatusOr<Value> date_value =
                      Value::TryDate(value.value.varchar_value);
                  if (!date_value.HasValue()) {
                    worker_errors[w] = date_value.GetStatus();
                    return;
                  }
                  value = date_value.MoveValue();
                  continue;
                }
                if (common_types[column] == ValueType::kVarChar) {
                  value = Value(value.AsString());
                  continue;
                }
                worker_errors[w] = StatusError(
                    StatusCode::kInvalidArgument,
                    "set operation value cannot be coerced to common type");
                return;
              }
            }
          }
        }));
        for (const Status& st : worker_errors) {
          if (st != Status::kSuccess) {
            return st;
          }
        }
      }
    }
  }
  output_.clear();
  output_offset_ = 0;
  if (operation_ != SetOperationKind::kUnionAll && workers > 1) {
    constexpr size_t kShards = 64;
    struct ShardItem {
      size_t row_idx{0};
      uint64_t hash{0};
    };
    const size_t num_sources = rows.size();
    std::vector<std::vector<std::array<std::vector<ShardItem>, kShards>>>
        local_shards(workers);
    for (auto& wv : local_shards) {
      wv.resize(num_sources);
    }
    RETURN_IF_FAIL(RunParallelTasks(workers, [&](size_t w) {
      const SetOperationRowHash hasher;
      for (size_t s = 0; s < num_sources; ++s) {
        const auto& src = rows[s];
        const size_t lo = (src.size() * w) / workers;
        const size_t hi = (src.size() * (w + 1)) / workers;
        auto& bins = local_shards[w][s];
        for (size_t i = lo; i < hi; ++i) {
          const uint64_t h = hasher(src[i].row);
          const size_t shard = (h ^ (h >> 29)) & (kShards - 1);
          bins[shard].push_back(
              ShardItem{.row_idx = i, .hash = h});
        }
      }
    }));

    std::array<std::vector<std::pair<size_t, size_t>>, kShards> shard_picks;
    const SetOperationKind op = operation_;
    RETURN_IF_FAIL(RunParallelTasks(workers, [&](size_t w) {
      for (size_t shard = w; shard < kShards; shard += workers) {
        auto& out_picks = shard_picks[shard];
        size_t s0_count = 0;
        for (size_t ww = 0; ww < workers; ++ww) {
          s0_count += local_shards[ww][0][shard].size();
        }
        if (op == SetOperationKind::kUnion) {
          size_t total_in_shard = s0_count;
          for (size_t s = 1; s < num_sources; ++s) {
            for (size_t ww = 0; ww < workers; ++ww) {
              total_in_shard += local_shards[ww][s][shard].size();
            }
          }
          if (total_in_shard == 0) {
            continue;
          }
          std::unordered_set<HashedRowRef, HashedRowRefHash, HashedRowRefEqual>
              seen;
          seen.reserve(total_in_shard);
          out_picks.reserve(total_in_shard);
          for (size_t s = 0; s < num_sources; ++s) {
            const auto& src = rows[s];
            const auto src_u32 = static_cast<uint32_t>(s);
            for (size_t ww = 0; ww < workers; ++ww) {
              for (const ShardItem& item : local_shards[ww][s][shard]) {
                if (seen.emplace(
                            HashedRowRef{&src[item.row_idx].row, item.hash})
                        .second) {
                  out_picks.emplace_back(src_u32, item.row_idx);
                }
              }
            }
          }
        } else if (op == SetOperationKind::kIntersect ||
                   op == SetOperationKind::kIntersectAll) {
          if (s0_count == 0) {
            continue;
          }
          const bool all = (op == SetOperationKind::kIntersectAll);
          std::unordered_map<HashedRowRef, size_t, HashedRowRefHash,
                             HashedRowRefEqual>
              counts;
          counts.reserve(s0_count);
          for (size_t ww = 0; ww < workers; ++ww) {
            for (const ShardItem& item : local_shards[ww][0][shard]) {
              ++counts[HashedRowRef{&rows[0][item.row_idx].row, item.hash}];
            }
          }
          for (size_t s = 1; s < num_sources && !counts.empty(); ++s) {
            size_t s_count = 0;
            for (size_t ww = 0; ww < workers; ++ww) {
              s_count += local_shards[ww][s][shard].size();
            }
            if (s_count == 0) {
              counts.clear();
              break;
            }
            std::unordered_map<HashedRowRef, size_t, HashedRowRefHash,
                               HashedRowRefEqual>
                current;
            current.reserve(std::min(s_count, counts.size()));
            for (size_t ww = 0; ww < workers; ++ww) {
              for (const ShardItem& item : local_shards[ww][s][shard]) {
                const HashedRowRef ref{&rows[s][item.row_idx].row, item.hash};
                if (counts.contains(ref)) {
                  ++current[ref];
                }
              }
            }
            for (auto iter = counts.begin(); iter != counts.end();) {
              const auto found = current.find(iter->first);
              if (found == current.end()) {
                iter = counts.erase(iter);
              } else {
                iter->second = std::min(iter->second, found->second);
                ++iter;
              }
            }
          }
          if (counts.empty()) {
            continue;
          }
          std::unordered_set<HashedRowRef, HashedRowRefHash, HashedRowRefEqual>
              emitted;
          if (!all) {
            emitted.reserve(counts.size());
          }
          out_picks.reserve(all ? s0_count : counts.size());
          for (size_t ww = 0; ww < workers; ++ww) {
            for (const ShardItem& item : local_shards[ww][0][shard]) {
              const HashedRowRef ref{&rows[0][item.row_idx].row, item.hash};
              const auto found = counts.find(ref);
              if (found == counts.end() || found->second == 0) {
                continue;
              }
              if (!all && !emitted.insert(ref).second) {
                continue;
              }
              out_picks.emplace_back(0U, item.row_idx);
              --found->second;
            }
          }
        } else {
          if (s0_count == 0) {
            continue;
          }
          const bool all = (op == SetOperationKind::kExceptAll);
          size_t rem_count = 0;
          for (size_t s = 1; s < num_sources; ++s) {
            for (size_t ww = 0; ww < workers; ++ww) {
              rem_count += local_shards[ww][s][shard].size();
            }
          }
          std::unordered_map<HashedRowRef, size_t, HashedRowRefHash,
                             HashedRowRefEqual>
              removed;
          if (rem_count > 0) {
            removed.reserve(rem_count);
            for (size_t s = 1; s < num_sources; ++s) {
              const auto& src = rows[s];
              for (size_t ww = 0; ww < workers; ++ww) {
                for (const ShardItem& item : local_shards[ww][s][shard]) {
                  ++removed[HashedRowRef{&src[item.row_idx].row, item.hash}];
                }
              }
            }
          }
          std::unordered_set<HashedRowRef, HashedRowRefHash, HashedRowRefEqual>
              emitted;
          if (!all) {
            emitted.reserve(s0_count);
          }
          out_picks.reserve(s0_count);
          for (size_t ww = 0; ww < workers; ++ww) {
            for (const ShardItem& item : local_shards[ww][0][shard]) {
              const HashedRowRef ref{&rows[0][item.row_idx].row, item.hash};
              if (!removed.empty()) {
                const auto found = removed.find(ref);
                if (found != removed.end() && found->second != 0) {
                  if (all) {
                    --found->second;
                  }
                  continue;
                }
              }
              if (!all && !emitted.insert(ref).second) {
                continue;
              }
              out_picks.emplace_back(0U, item.row_idx);
            }
          }
        }
      }
    }));

    std::array<size_t, kShards + 1> shard_offsets{};
    for (size_t shard = 0; shard < kShards; ++shard) {
      shard_offsets[shard + 1] =
          shard_offsets[shard] + shard_picks[shard].size();
    }
    const size_t total_out = shard_offsets[kShards];
    output_.resize(total_out);
    if (total_out >= 8192) {
      RETURN_IF_FAIL(RunParallelTasks(workers, [&](size_t w) {
        for (size_t shard = w; shard < kShards; shard += workers) {
          size_t dst = shard_offsets[shard];
          for (const auto& [src_idx, row_idx] : shard_picks[shard]) {
            output_[dst++] = std::move(rows[src_idx][row_idx]);
          }
        }
      }));
    } else if (total_out > 0) {
      size_t dst = 0;
      for (size_t shard = 0; shard < kShards; ++shard) {
        for (const auto& [src_idx, row_idx] : shard_picks[shard]) {
          output_[dst++] = std::move(rows[src_idx][row_idx]);
        }
      }
    }
    // Release dead rows and temporary shard index buffers in parallel.
    RETURN_IF_FAIL(RunParallelTasks(workers, [&](size_t w) {
      local_shards[w].clear();
      for (size_t shard = w; shard < kShards; shard += workers) {
        std::vector<std::pair<size_t, size_t>>().swap(shard_picks[shard]);
      }
      for (auto& src : rows) {
        const size_t lo = (src.size() * w) / workers;
        const size_t hi = (src.size() * (w + 1)) / workers;
        for (size_t i = lo; i < hi; ++i) {
          src[i].row.values_.clear();
          src[i].row.values_.shrink_to_fit();
        }
      }
    }));
    return Status::kSuccess;
  }
  switch (operation_) {
    case SetOperationKind::kUnionAll:
      for (const auto& source : rows) {
        AppendAll(source);
      }
      break;
    case SetOperationKind::kUnion: {
      SetOperationRowSet seen;
      for (const auto& source : rows) {
        AppendDistinct(source, &seen);
      }
      break;
    }
    case SetOperationKind::kIntersect:
      AppendIntersection(rows, false);
      break;
    case SetOperationKind::kIntersectAll:
      AppendIntersection(rows, true);
      break;
    case SetOperationKind::kExcept:
      AppendExcept(rows, false);
      break;
    case SetOperationKind::kExceptAll:
      AppendExcept(rows, true);
      break;
  }
  return Status::kSuccess;
}

Status SetOperationExecutor::MaterializePartitioned() {
  for (auto& source : spill_sources_) {
    for (SpillFile& partition : source) {
      RETURN_IF_FAIL(partition.FinishWriting());
    }
  }
  // A value's physical type participates in Row's hash. Repartition after
  // determining the global common type so INT64 1 and DOUBLE 1.0 still meet
  // in UNION DISTINCT / INTERSECT partitions.
  std::vector<std::vector<SpillFile>> normalized(spill_sources_.size());
  for (auto& source : normalized) {
    source.resize(kSetOperationPartitions);
  }
  for (size_t source = 0; source < spill_sources_.size(); ++source) {
    for (SpillFile& raw_partition : spill_sources_[source]) {
      Status row_error = Status::kSuccess;
      RETURN_IF_FAIL(raw_partition.ForEachRow([&](const Row& raw) {
        if (row_error != Status::kSuccess) {
          return;
        }
        Row row = raw;
        if (spill_width_.has_value() && row.values_.size() != *spill_width_) {
          row_error = StatusError(
              StatusCode::kInvalidArgument,
              "set operation inputs must have the same column count");
          return;
        }
        for (size_t column = 0; column < row.values_.size(); ++column) {
          Value& value = row.values_[column];
          const ValueType expected = spill_common_types_[column];
          if (value.IsNull() || value.type == expected) {
            continue;
          }
          if (expected == ValueType::kDouble &&
              value.type == ValueType::kInt64) {
            value = Value(value.IsUnsigned()
                                    ? static_cast<double>(static_cast<uint64_t>(
                                          value.value.int_value))
                                    : static_cast<double>(value.value.int_value));
            continue;
          }
          if (expected == ValueType::kDate &&
              value.type == ValueType::kVarChar) {
            StatusOr<Value> date_value =
                Value::TryDate(value.value.varchar_value);
            if (!date_value.HasValue()) {
              row_error = date_value.GetStatus();
              return;
            }
            value = date_value.MoveValue();
            continue;
          }
          if (expected == ValueType::kVarChar) {
            value = Value(value.AsString());
            continue;
          }
          row_error = StatusError(
              StatusCode::kInvalidArgument,
              "set operation value cannot be coerced to common type");
          return;
        }
        if (Status append = normalized[source][SetOperationRowHash{}(row) %
                                               kSetOperationPartitions]
                                .Append(row);
            append != Status::kSuccess && row_error == Status::kSuccess) {
          row_error = append;
        }
      }));
      // The callback latches the first coercion/append failure and skips the
      // remaining rows of this partition; propagate it like the in-memory
      // path instead of silently dropping the offending rows.
      RETURN_IF_FAIL(row_error);
    }
  }
  spill_sources_ = std::move(normalized);
  for (auto& source : spill_sources_) {
    for (SpillFile& partition : source) {
      RETURN_IF_FAIL(partition.FinishWriting());
    }
  }
  output_.clear();
  output_offset_ = 0;
  std::vector<Positioned> accumulated;
  for (size_t partition = 0; partition < kSetOperationPartitions; ++partition) {
    std::vector<std::vector<Positioned>> rows(spill_sources_.size());
    for (size_t source = 0; source < spill_sources_.size(); ++source) {
      ASSIGN_OR_RETURN(std::vector<Row>, spilled,
                       (spill_sources_[source][partition].ReadAllRows()));
      rows[source].reserve(spilled.size());
      for (Row row : spilled) {
        rows[source].push_back(
            Positioned{.row = std::move(row), .position = RowPosition()});
      }
    }
    RETURN_IF_FAIL(MaterializeRows(std::move(rows)));
    accumulated.insert(accumulated.end(),
                       std::make_move_iterator(output_.begin()),
                       std::make_move_iterator(output_.end()));
  }
  output_ = std::move(accumulated);
  output_offset_ = 0;
  return Status::kSuccess;
}

void SetOperationExecutor::AppendAll(const std::vector<Positioned>& source) {
  output_.insert(output_.end(), source.begin(), source.end());
}

void SetOperationExecutor::AppendDistinct(const std::vector<Positioned>& source,
                                          SetOperationRowSet* seen) {
  const SetOperationRowHash hasher;
  for (const Positioned& item : source) {
    if (seen->emplace(HashedRowRef{.row = &item.row, .hash = hasher(item.row)}).second) {
      output_.push_back(item);
    }
  }
}

void SetOperationExecutor::AppendIntersection(
    const std::vector<std::vector<Positioned>>& rows, bool all) {
  if (rows.empty()) {
    return;
  }
  const SetOperationRowHash hasher;
  SetOperationRowMap counts;
  for (const Positioned& item : rows[0]) {
    ++counts[HashedRowRef{.row = &item.row, .hash = hasher(item.row)}];
  }
  for (size_t source = 1; source < rows.size(); ++source) {
    SetOperationRowMap current;
    for (const Positioned& item : rows[source]) {
      ++current[HashedRowRef{.row = &item.row, .hash = hasher(item.row)}];
    }
    for (auto iter = counts.begin(); iter != counts.end();) {
      const auto found = current.find(iter->first);
      if (found == current.end()) {
        iter = counts.erase(iter);
      } else {
        iter->second = std::min(iter->second, found->second);
        ++iter;
      }
    }
  }
  SetOperationRowSet emitted;
  for (const Positioned& item : rows[0]) {
    const HashedRowRef ref{.row = &item.row, .hash = hasher(item.row)};
    const auto found = counts.find(ref);
    if (found == counts.end() || found->second == 0) {
      continue;
    }
    if (!all && !emitted.emplace(ref).second) {
      continue;
    }
    output_.push_back(item);
    if (--counts[ref] == 0) {
      counts.erase(ref);
    }
  }
}

void SetOperationExecutor::AppendExcept(
    const std::vector<std::vector<Positioned>>& rows, bool all) {
  if (rows.empty()) {
    return;
  }
  const SetOperationRowHash hasher;
  SetOperationRowMap removed;
  for (size_t source = 1; source < rows.size(); ++source) {
    for (const Positioned& item : rows[source]) {
      ++removed[HashedRowRef{.row = &item.row, .hash = hasher(item.row)}];
    }
  }
  SetOperationRowSet emitted;
  for (const Positioned& item : rows[0]) {
    const HashedRowRef ref{.row = &item.row, .hash = hasher(item.row)};
    const auto found = removed.find(ref);
    if (found != removed.end() && found->second != 0) {
      // EXCEPT DISTINCT removes the value as a set, regardless of how many
      // copies occur on the left. EXCEPT ALL consumes one right-hand copy per
      // left-hand occurrence.
      if (all) {
        if (--found->second == 0) {
          removed.erase(found);
        }
      } else {
        // Keep the key present for the remainder of the left input.  EXCEPT
        // DISTINCT removes every left-hand copy once the key occurs on the
        // right, whereas erasing it here would let later duplicates leak
        // back into the result.
      }
      continue;
    }
    if (!all && !emitted.emplace(ref).second) {
      continue;
    }
    output_.push_back(item);
  }
}

Status SetOperationExecutor::Materialize() {
  std::vector<std::vector<Positioned>> rows(sources_.size());
  QueryMemoryCharge charge;
  bool spilling = false;
  const auto observe_types = [&](const Row& row) -> Status {
    if (!spill_width_.has_value()) {
      spill_width_ = row.values_.size();
      spill_common_types_.assign(*spill_width_, ValueType::kNull);
    }
    if (row.values_.size() != *spill_width_) {
      return StatusError(
          StatusCode::kInvalidArgument,
          "set operation inputs must have the same column count");
    }
    for (size_t column = 0; column < row.values_.size(); ++column) {
      ASSIGN_OR_RETURN(ValueType, common,
                       (CommonSetValueType(spill_common_types_[column],
                                           row.values_[column].type)));
      spill_common_types_[column] = common;
    }
    return Status::kSuccess;
  };
  auto begin_spill = [&]() -> Status {
    spilling = true;
    spill_sources_.resize(sources_.size());
    for (auto& source : spill_sources_) {
      source.resize(kSetOperationPartitions);
    }
    for (size_t source = 0; source < rows.size(); ++source) {
      for (const Positioned& item : rows[source]) {
        RETURN_IF_FAIL(spill_sources_[source][SetOperationRowHash{}(item.row) %
                                              kSetOperationPartitions]
                           .Append(item.row));
      }
      rows[source].clear();
      rows[source].shrink_to_fit();
    }
    charge.ReleaseAll();
    return Status::kSuccess;
  };
  for (size_t source = 0; source < sources_.size(); ++source) {
    DataChunk batch;
    while (sources_[source]->NextBatch(&batch) != 0) {
      for (size_t i = 0; i < batch.Size(); ++i) {
        Row row = batch.RowAt(i);
        RowPosition position = batch.PositionAt(i);
        RETURN_IF_FAIL(observe_types(row));
        if (!spilling && operation_ != SetOperationKind::kUnionAll &&
            !QueryMemoryBudget::Global().CanReserve(EstimateRowBytes(row))) {
          RETURN_IF_FAIL(begin_spill());
        }
        if (spilling) {
          RETURN_IF_FAIL(spill_sources_[source][SetOperationRowHash{}(row) %
                                                kSetOperationPartitions]
                             .Append(row));
        } else {
          charge.Add(EstimateRowBytes(row));
          rows[source].push_back(
              Positioned{.row = std::move(row), .position = position});
        }
      }
    }
    if (auto st = sources_[source]->GetStatus(); st != Status::kSuccess) {
      return st;
    }
  }
  if (spilling) {
    RETURN_IF_FAIL(MaterializePartitioned());
  } else {
    RETURN_IF_FAIL(MaterializeRows(std::move(rows)));
  }
  materialized_ = true;
  return Status::kSuccess;
}

size_t SetOperationExecutor::NextBatch(DataChunk* destination,
                                       size_t max_rows) {
  destination->Reset();
  if (max_rows == 0) {
    return 0;
  }
  if (!materialized_) {
    Status st = Materialize();
    if (st != Status::kSuccess) {
      FailWith(st);
      return 0;
    }
  }
  while (destination->Size() < max_rows && output_offset_ < output_.size()) {
    Positioned& item = output_[output_offset_++];
    destination->Append(std::move(item.row), item.position);
  }
  return destination->Size();
}

void SetOperationExecutor::Dump(std::ostream& output, int indent) const {
  output << Indent(static_cast<size_t>(indent)) << "SetOperation\n";
  for (const Executor& source : sources_) {
    source->Dump(output, indent + 2);
    output << '\n';
  }
}

}  // namespace tinylamb
