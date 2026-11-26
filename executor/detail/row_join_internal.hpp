/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#pragma once
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
#include "common/platform/endian.hpp"
#include "common/platform/hash.hpp"
#include "common/platform/int128_conversion.hpp"
#include "common/platform/process.hpp"
#include "common/query_parallelism.hpp"
#include "common/status_or.hpp"
#include "common/string_util.hpp"
#include "database/transaction_context.hpp"
#include "executor/detail/columnar_join.hpp"
#include "executor/detail/columnar_table.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/detail/flat_int_key_map.hpp"
#include "executor/detail/fused_grouped_join.hpp"
#include "executor/detail/planning_columnar_keys.hpp"
#include "executor/detail/planning_heuristics.hpp"
#include "executor/detail/relation.hpp"
#include "executor/detail/scan_filter.hpp"
#include "executor/detail/subquery_runtime.hpp"
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

namespace tinylamb::relational_detail::row_join_detail {
// Parallel probe for large in-memory hash joins. Workers own statically
// interleaved chunks and accumulate private Relations; worker-order merging is
// deterministic for a fixed worker count, but differs from serial probe order.
// Each Relation observes the query memory budget and can spill. Build buckets
// are read-only; RIGHT/FULL match tracking uses private worker bitmaps merged
// after the probe. Residuals must be pure positional programs; callers exclude
// null-safe keys.
constexpr size_t kParallelProbeMinLeftRows = 16384;
constexpr size_t kParallelProbeChunk = 8192;

// A row-only residual can run on probe workers without entering the shared
// transaction/evaluator context. Preserve conjunct order and short circuiting.
inline std::vector<size_t> MatchingColumnOffsets(const Schema& schema,
                                                 const std::string& name) {
  std::vector<size_t> offsets;
  offsets.reserve(schema.ColumnCount());
  for (size_t i = 0; i < schema.ColumnCount(); ++i) {
    if (IdentifierEquals(schema.GetColumn(i).Name().name, name)) {
      offsets.push_back(i);
    }
  }
  return offsets;
}

inline bool IsDeclaredUsingEquality(
    const Expression& expression,
    const std::vector<std::string>& using_columns) {
  if (!expression || expression->Type() != TypeTag::kBinaryExp ||
      using_columns.empty()) {
    return false;
  }
  const auto& binary = expression->AsBinaryExpression();
  if (binary.Op() != BinaryOperation::kEquals ||
      binary.Left()->Type() != TypeTag::kColumnValue ||
      binary.Right()->Type() != TypeTag::kColumnValue) {
    return false;
  }
  const ColumnName& lhs = binary.Left()->AsColumnValue().GetColumnName();
  const ColumnName& rhs = binary.Right()->AsColumnValue().GetColumnName();
  return lhs.schema.empty() && rhs.schema.empty() &&
         IdentifierEquals(lhs.name, rhs.name) &&
         ContainsIdentifier(using_columns, lhs.name);
}

inline std::optional<std::vector<BytecodeProgram>> CompileProbeResiduals(
    const std::vector<Expression>& predicates, const Schema& schema) {
  static const bool disabled =
      ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_PARALLEL_JOIN_RESIDUAL") !=
      nullptr;
  if (predicates.empty() || disabled || !BytecodeEnabled()) {
    return std::nullopt;
  }
  std::vector<BytecodeProgram> programs;
  programs.reserve(predicates.size());
  for (const auto& predicate : predicates) {
    if (!predicate || ContainsQuery(predicate) ||
        ContainsParameterSlot(predicate) ||
        !SafeToReduceEvaluationCountWithSchema(predicate, schema)) {
      return std::nullopt;
    }
    auto program = BytecodeCompiler::CompileRowCached(
        predicate, schema, BytecodeCompiler::Context::kFilter);
    if (!program || program->ResultType() == ValueType::kNull) {
      return std::nullopt;
    }
    programs.push_back(std::move(*program));
  }
  return programs;
}

template <typename Buckets, typename KeyOfRow>
Status ParallelHashProbe(  // NOLINT(misc-use-anonymous-namespace)
    Buckets& buckets, Relation& left, const std::vector<slot_t>& left_columns,
    KeyOfRow key_of_row, bool left_outer_emit, size_t right_width,
    size_t worker_count, Relation* result, size_t* join_comparisons,
    bool stream_is_left = true, const Row* build_base = nullptr,
    std::vector<char>* build_matched = nullptr,
    const std::vector<BytecodeProgram>* residuals = nullptr,
    std::vector<uint64_t>* build_matched_bits = nullptr,
    size_t fixed_row_bytes = 0) {
  const size_t total = left.rows.size();
  static const bool release_disabled =
      ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_PARALLEL_PROBE_RELEASE") !=
      nullptr;
  const size_t num_chunks =
      (total + kParallelProbeChunk - 1) / kParallelProbeChunk;
  worker_count = std::min({worker_count, num_chunks, total});
  std::vector<Relation> shards(worker_count);
  std::vector<ExecutionRuntime> shard_runtimes(worker_count);
  for (size_t w = 0; w < worker_count; ++w) {
    shards[w].set_runtime(&shard_runtimes[w]);
    shards[w].schema = result->schema;
  }
  // Cache-line padded: every worker bumps its own counter once per emitted
  // match, so adjacent unpadded slots would false-share on the hot line.
  struct alignas(64) PaddedComparison {
    size_t value{0};
  };
  std::vector<PaddedComparison> comparisons(worker_count);
  const bool use_private_bits = build_matched != nullptr &&
                                build_base != nullptr && worker_count > 1 &&
                                worker_count <= 8;
  const size_t bitset_words =
      use_private_bits ? (build_matched->size() + 63) / 64 : 0;
  std::vector<std::vector<uint64_t>> worker_bits;
  if (use_private_bits && bitset_words > 0) {
    worker_bits.resize(worker_count);
    for (size_t w = 0; w < worker_count; ++w) {
      worker_bits[w].assign(bitset_words, 0);
    }
  }
  const size_t flush_granularity = Relation::ChargeFlushGranularity();
  const auto emit_local_row = [&](Relation& local, Row&& out_row) {
    if (fixed_row_bytes != 0) {
      local.AddRowWithKnownBytes(std::move(out_row), fixed_row_bytes,
                                 flush_granularity);
    } else {
      local.AddRow(std::move(out_row));
    }
  };
  if (left_outer_emit && Relation::HasAmpleMemoryBudget() && worker_count > 0) {
    const size_t est_per_shard =
        std::min<size_t>((total + worker_count - 1) / worker_count, 65536);
    for (Relation& shard : shards) {
      shard.rows.reserve(est_per_shard);
    }
  }
  std::mutex error_mutex;
  Status first_error{Status::kSuccess};

  constexpr bool kHasHashedLookup =
      requires(const Buckets& b, const decltype(key_of_row(left.rows[0]))& k) {
        b.HashKey(k);
        b.equal_range_hashed(k, uint64_t{0});
        b.PrefetchBucketHashed(uint64_t{0});
      };

  auto run_worker = [&](size_t worker_id) {
    Relation& local = shards[worker_id];
    uint64_t* local_bits =
        !worker_bits.empty() ? worker_bits[worker_id].data() : nullptr;
    const auto mark_build_match = [&](const Row* build_row) {
      if (build_matched == nullptr) {
        return;
      }
      const auto idx = static_cast<size_t>(build_row - build_base);
      if (local_bits != nullptr) {
        local_bits[idx >> 6] |= (uint64_t{1} << (idx & 63));
        return;
      }
      if (worker_count <= 1) {
        (*build_matched)[idx] = 1;
        return;
      }
      std::atomic_ref<char> flag((*build_matched)[idx]);
      if (flag.load(std::memory_order_relaxed) == 0) {
        flag.store(1, std::memory_order_relaxed);
      }
    };
    const size_t begin = (total * worker_id) / worker_count;
    const size_t end = (total * (worker_id + 1)) / worker_count;
    if (begin >= end) {
      return;
    }
    try {
      const auto emit_unmatched_probe = [&](Row& probe_row) {
        if (stream_is_left) {
          probe_row.values_.resize(probe_row.Count() + right_width);
          emit_local_row(local, std::move(probe_row));
        } else if (probe_row.values_.capacity() >=
                   right_width + probe_row.Count()) {
          const size_t probe_width = probe_row.Count();
          probe_row.values_.resize(right_width + probe_width);
          std::move_backward(probe_row.values_.begin(),
                             probe_row.values_.begin() +
                                 static_cast<std::ptrdiff_t>(probe_width),
                             probe_row.values_.end());
          std::fill_n(probe_row.values_.begin(), right_width, Value());
          emit_local_row(local, std::move(probe_row));
        } else {
          std::vector<Value> padded(right_width + probe_row.Count());
          std::move(probe_row.values_.begin(), probe_row.values_.end(),
                    padded.begin() + static_cast<std::ptrdiff_t>(right_width));
          std::vector<Value>().swap(probe_row.values_);
          emit_local_row(local, Row(std::move(padded)));
        }
      };
      constexpr size_t kBatch = 16;
      using KeyType = std::decay_t<decltype(key_of_row(left.rows[begin]))>;
      struct ProbeSlot {
        KeyType key{};
        uint64_t hash{0};
        bool has_null{false};
      };
      std::array<ProbeSlot, kBatch> batch{};
      for (size_t chunk = begin; chunk < end; chunk += kBatch) {
        const size_t count = std::min(kBatch, end - chunk);
        for (size_t j = 0; j < count; ++j) {
          Row& probe_row = left.rows[chunk + j];
          const bool null_key = HasNullKey(probe_row, left_columns);
          batch[j].has_null = null_key;
          if (!null_key) {
            batch[j].key = key_of_row(probe_row);
            if constexpr (kHasHashedLookup) {
              batch[j].hash = buckets.HashKey(batch[j].key);
              buckets.PrefetchBucketHashed(batch[j].hash);
            } else {
              buckets.PrefetchBucket(batch[j].key);
            }
          }
        }
        for (size_t j = 0; j < count; ++j) {
          Row& left_row = left.rows[chunk + j];
          if (batch[j].has_null) {
            if (left_outer_emit) {
              emit_unmatched_probe(left_row);
            }
            continue;
          }
          const auto [eq_begin, eq_end] = [&]() {
            if constexpr (kHasHashedLookup) {
              return buckets.equal_range_hashed(batch[j].key, batch[j].hash);
            } else {
              return buckets.equal_range(batch[j].key);
            }
          }();
          if (eq_begin == eq_end) {
            if (left_outer_emit) {
              emit_unmatched_probe(left_row);
            }
            continue;
          }
          if (residuals != nullptr) {
            bool matched = false;
            for (auto iter = eq_begin; iter != eq_end; ++iter) {
              ++comparisons[worker_id].value;
              const Row* build_row = iter->second;
              Row combined = stream_is_left ? ConcatRows(left_row, *build_row)
                                            : ConcatRows(*build_row, left_row);
              bool accepted = true;
              for (const auto& program : *residuals) {
                auto value = TryEvaluateRowAuto(program, combined);
                if (!value.HasValue()) {
                  std::scoped_lock error_lock(error_mutex);
                  if (first_error.ok()) {
                    first_error = value.GetStatus();
                  }
                  return;
                }
                if (!Truthy(value.Value())) {
                  accepted = false;
                  break;
                }
              }
              if (!accepted) {
                continue;
              }
              matched = true;
              mark_build_match(build_row);
              emit_local_row(local, std::move(combined));
            }
            if (!matched && left_outer_emit) {
              emit_unmatched_probe(left_row);
            }
            continue;
          }
          for (auto iter = eq_begin; iter != eq_end;) {
            const Row* build_row = iter->second;
            // Advance while a borrowed probe key still has its original row.
            // Only the last match may consume the input buffer.
            ++iter;
            ++comparisons[worker_id].value;
            mark_build_match(build_row);
            if (iter == eq_end) {
              auto& values = left_row.values_;
              if (stream_is_left) {
                values.reserve(values.size() + build_row->values_.size());
                values.insert(values.end(), build_row->values_.begin(),
                              build_row->values_.end());
                emit_local_row(local, std::move(left_row));
              } else if (values.capacity() >=
                         build_row->values_.size() + values.size()) {
                const size_t probe_width = values.size();
                const size_t build_w = build_row->values_.size();
                values.resize(build_w + probe_width);
                std::move_backward(
                    values.begin(),
                    values.begin() + static_cast<std::ptrdiff_t>(probe_width),
                    values.end());
                std::copy(build_row->values_.begin(), build_row->values_.end(),
                          values.begin());
                emit_local_row(local, std::move(left_row));
              } else {
                std::vector<Value> combined;
                combined.reserve(build_row->values_.size() + values.size());
                combined.insert(combined.end(), build_row->values_.begin(),
                                build_row->values_.end());
                combined.insert(combined.end(),
                                std::make_move_iterator(values.begin()),
                                std::make_move_iterator(values.end()));
                std::vector<Value>().swap(values);
                emit_local_row(local, Row(std::move(combined)));
              }
              break;
            }
            emit_local_row(local, stream_is_left
                                      ? ConcatRows(left_row, *build_row)
                                      : ConcatRows(*build_row, left_row));
          }
        }
      }
      if (!release_disabled) {
        // Every probe key in this chunk has been consumed. Accepted rows
        // own their cells; rejected/copy-only rows no longer feed a probe.
        // Release those buffers on their current workers while still hot,
        // avoiding a serial coordinator pass over the rejected payloads.
        for (size_t i = begin; i < end; ++i) {
          if (!left.rows[i].values_.empty()) {
            std::vector<Value>().swap(left.rows[i].values_);
          }
        }
      }
    } catch (const std::exception& error) {
      std::scoped_lock error_lock(error_mutex);
      if (first_error.ok()) {
        first_error = StatusError(StatusCode::kRuntimeError, error.what());
      }
      return;
    } catch (...) {
      std::scoped_lock error_lock(error_mutex);
      if (first_error.ok()) {
        first_error =
            StatusError(StatusCode::kRuntimeError, "parallel probe failed");
      }
      return;
    }
  };

  RETURN_IF_FAIL(RunParallelTasks(worker_count, run_worker));
  if (!worker_bits.empty() && bitset_words > 0) {
    if (build_matched_bits != nullptr) {
      build_matched_bits->assign(bitset_words, 0);
      for (size_t w = 0; w < worker_count; ++w) {
        const uint64_t* src = worker_bits[w].data();
        uint64_t* dst = build_matched_bits->data();
        for (size_t k = 0; k < bitset_words; ++k) {
          dst[k] |= src[k];
        }
      }
    } else {
      for (size_t k = 0; k < bitset_words; ++k) {
        uint64_t word = 0;
        for (size_t w = 0; w < worker_count; ++w) {
          word |= worker_bits[w][k];
        }
        if (word == 0) {
          continue;
        }
        const size_t base_idx = k * 64;
        while (word != 0) {
          const auto bit = static_cast<size_t>(std::countr_zero(word));
          if (base_idx + bit < build_matched->size()) {
            (*build_matched)[base_idx + bit] = 1;
          }
          word &= (word - 1);
        }
      }
    }
  }
  if (result->runtime() != nullptr) {
    for (const auto& runtime : shard_runtimes) {
      result->runtime()->relation_spills += runtime.relation_spills;
    }
  }
  const Status merge_status = Relation::MergeShardsInto(shards, result);
  if (first_error.ok() && merge_status != Status::kSuccess) {
    first_error = merge_status;
  }
  for (const PaddedComparison& count : comparisons) {
    *join_comparisons += count.value;
  }
  return first_error;
}

inline size_t FixedJoinRowBytes(const Relation& left, const Relation& right,
                                const Schema& result_schema) {
  if (!left.uniform_cell_types || !right.uniform_cell_types ||
      !Relation::HasAmpleMemoryBudget()) {
    return 0;
  }
  for (size_t col = 0; col < result_schema.ColumnCount(); ++col) {
    const ValueType vt = result_schema.GetColumn(col).Type();
    if (vt != ValueType::kInt64 && vt != ValueType::kDate &&
        vt != ValueType::kDouble && vt != ValueType::kNull) {
      return 0;
    }
  }
  return static_cast<size_t>(result_schema.ColumnCount()) * 40;
}

template <typename Key, typename Hash = std::hash<Key>, typename SkipBuild,
          typename BuildKey, typename SkipProbe, typename ProbeKey,
          typename MatchFn, typename MarkBuildFn,
          typename UnmatchedProbeFn>
Status ExecuteFlatBucketJoin(  // NOLINT(misc-use-anonymous-namespace)
    const Relation& build_side, Relation& probe_side,
    const std::vector<slot_t>& probe_columns, bool probe_outer_emit,
    bool stream_is_left, const Row* build_base,
    std::vector<char>* build_matched, std::vector<uint64_t>* build_matched_bits,
    size_t fixed_row_bytes, bool can_parallelize, size_t probe_workers,
    const std::vector<BytecodeProgram>* residuals, Relation* result,
    Status* residual_error, SkipBuild skip_build, BuildKey build_key,
    SkipProbe skip_probe, ProbeKey probe_key, MatchFn matches,
    MarkBuildFn mark_build, UnmatchedProbeFn emit_unmatched_probe) {
  const auto probe_with = [&](const auto& buckets) -> Status {
    if (can_parallelize) {
      RETURN_IF_FAIL(ParallelHashProbe(
          buckets, probe_side, probe_columns, probe_key, probe_outer_emit,
          build_side.schema.ColumnCount(), probe_workers, result,
          &result->join_comparisons, stream_is_left, build_base, build_matched,
          residuals, build_matched_bits, fixed_row_bytes));
      return *residual_error;
    }
    return probe_side.ForEachRow([&](const Row& probe_row) {
      bool matched = false;
      if (!skip_probe(probe_row)) {
        const auto key = probe_key(probe_row);
        const auto [begin, end] = buckets.equal_range(key);
        for (auto iter = begin; iter != end; ++iter) {
          ++result->join_comparisons;
          Row combined = stream_is_left ? ConcatRows(probe_row, *iter->second)
                                        : ConcatRows(*iter->second, probe_row);
          if (matches(combined)) {
            mark_build(*iter->second);
            result->AddRow(std::move(combined));
            matched = true;
          }
        }
      }
      if (!matched) {
        emit_unmatched_probe(probe_row);
      }
    });
  };
  const size_t build_workers =
      !build_side.HasSpill() && build_side.rows.size() >= 8192
          ? ParallelWorkerCount(build_side.rows.size() / 4096)
          : 1;
  if (build_workers > 1) {
    ShardedFlatJoinBuckets<Key, Hash> buckets;
    RETURN_IF_FAIL(buckets.BuildParallel(build_side.rows, build_key, skip_build,
                                         build_workers));
    return probe_with(buckets);
  }
  FlatJoinBuckets<Key, Hash> buckets;
  buckets.reserve(build_side.rows.size());
  for (const Row& row : build_side.rows) {
    if (!skip_build(row)) {
      buckets.emplace(build_key(row), &row);
    }
  }
  return probe_with(buckets);
}

// Bytes of inner rows one nested-loop chunk may hold: a bounded share of
// the query memory budget, mirroring partition_budget_bytes() above.
inline size_t NestedLoopInnerChunkBytes() {
  const QueryMemoryBudget& budget = QueryMemoryBudget::Global();
  if (budget.Unlimited()) {
    return size_t{1} << 30;
  }
  const size_t min_floor = std::min<size_t>(
      size_t{16} << 20, std::max<size_t>(size_t{4096}, budget.Limit() / 4));
  return std::max<size_t>(min_floor,
                          std::min(budget.Remaining() / 2, budget.Limit() / 4));
}

// Streams `rel` exactly once, invoking `fn(chunk, first_row_index)` per
// `chunk_bytes`-bounded run of rows.  A nested-loop join over a spilled
// inner uses it to re-scan the outer side per chunk instead of re-reading
// the inner's spill files once per outer row.
template <typename Fn>
Status ForEachRowChunk(Relation& rel, size_t chunk_bytes, Fn&& fn) {
  std::vector<Row> chunk;
  QueryMemoryCharge charge;
  size_t base = 0;
  Status first{Status::kSuccess};
  auto flush = [&] {
    if (chunk.empty() || first != Status::kSuccess) {
      return;
    }
    const Status st = fn(chunk, base);
    base += chunk.size();
    chunk.clear();
    charge.ReleaseAll();
    if (st != Status::kSuccess) {
      first = st;
    }
  };
  const Status stream = rel.ForEachRow([&](const Row& row) {
    if (first != Status::kSuccess) {
      return;
    }
    const size_t bytes = EstimateRowBytes(row) + 64;
    if (!chunk.empty() && charge.Bytes() + bytes > chunk_bytes) {
      flush();
    }
    charge.Add(bytes);
    chunk.push_back(row);
  });
  if (first == Status::kSuccess) {
    first = stream;
  }
  flush();
  return first;
}

}  // namespace tinylamb::relational_detail::row_join_detail
