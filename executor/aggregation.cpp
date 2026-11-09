/**
 * Copyright 2023 KUMAZAKI Hiroki
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "executor/aggregation.hpp"

#include <algorithm>
#include <cctype>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <ostream>
#include <stdexcept>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "executor/data_chunk.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/detail/subquery_runtime.hpp"
#include "executor/executor_base.hpp"
#include "executor/full_scan.hpp"
#include "executor/parallel_scan.hpp"
#include "executor/query_memory.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/bytecode.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/jit.hpp"
#include "expression/named_expression.hpp"
#include "page/row_position.hpp"
#include "table/lstable_bridge.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/type.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
namespace {
bool IsTypedAggregate(AggregationType type);
}  // namespace

AggregationExecutor::AggregationExecutor(
    std::shared_ptr<ExecutorBase> child, Schema input_schema,
    std::vector<NamedExpression> aggregates, size_t jit_threshold_rows)
    : child_(std::move(child)),
      input_schema_(std::move(input_schema)),
      aggregates_(std::move(aggregates)),
      jit_threshold_rows_(jit_threshold_rows) {
  if (aggregates_.size() == 1) {
    const auto& aggregate = aggregates_[0].expression->AsAggregateExpression();
    if (aggregate.GetType() == AggregationType::kSum && !aggregate.Distinct() &&
        !aggregate.HasWhereFilter() && !aggregate.NeedsGroupContext() &&
        aggregate.Child() &&
        aggregate.Child()->Type() == TypeTag::kColumnValue) {
      const int offset = input_schema_.Offset(
          aggregate.Child()->AsColumnValue().GetColumnName());
      if (offset >= 0 &&
          input_schema_.GetColumn(static_cast<size_t>(offset)).Type() ==
              ValueType::kInt64 &&
          !input_schema_.GetColumn(static_cast<size_t>(offset)).IsUnsigned() &&
          BytecodeEnabled() &&
          aggregate.Child()->AsColumnValue().GetColumnName().name.find(
              "uint64") == std::string::npos) {
        jit_sum_eligible_ = true;
        jit_sum_column_ = static_cast<uint16_t>(offset);
      }
    }
  }

  // Classify each aggregate once so batches can flow through typed
  // accumulators (reading ColumnVector storage directly) whenever every
  // aggregate argument is a numeric column reference, COUNT(*), or a
  // foldable constant.  Anything else keeps the generic per-row path.
  inputs_.reserve(aggregates_.size());
  all_typed_ = !aggregates_.empty();
  for (const NamedExpression& named : aggregates_) {
    AggregateInput input;
    const auto& agg = named.expression->AsAggregateExpression();
    if (IsTypedAggregate(agg.GetType()) && !agg.Distinct() &&
        !agg.WhereFilter() && !agg.NeedsGroupContext()) {
      if (IsCountStar(agg)) {
        input.kind = AggregateInputKind::kCountStar;
      } else if (agg.Child()->Type() == TypeTag::kColumnValue) {
        const int offset =
            input_schema_.Offset(agg.Child()->AsColumnValue().GetColumnName());
        if (offset >= 0) {
          const ValueType declared =
              input_schema_.GetColumn(static_cast<size_t>(offset)).Type();
          if (declared == ValueType::kInt64 || declared == ValueType::kDouble) {
            input.kind = AggregateInputKind::kTypedColumn;
            input.column = static_cast<size_t>(offset);
            input.type = declared;
          }
        }
      } else if (agg.Child()->Type() == TypeTag::kConstantValue) {
        Value constant = agg.Child()->AsConstantValue().GetValue();
        if (!constant.IsNull() && (constant.type == ValueType::kInt64 ||
                                   constant.type == ValueType::kDouble)) {
          input.kind = AggregateInputKind::kTypedConstant;
          input.constant = std::move(constant);
        }
      }
    }
    all_typed_ = all_typed_ && input.kind != AggregateInputKind::kGeneric;
    inputs_.push_back(std::move(input));
  }
}

bool AggregationExecutor::Execute(Row* dst) {
  if (executed_) {
    return false;
  }
  StatusOr<bool> folded = TryFoldScan(dst);
  if (!folded.HasValue()) {
    return FailWith(folded.GetStatus());
  }
  if (folded.Value()) {
    executed_ = true;
    return true;
  }
  if (jit_sum_eligible_) {
    int64_t total = 0;
    bool any = false;
    while (child_->NextBatch(&input_batch_) != 0) {
      rows_seen_ += input_batch_.Size();
      if (!jit_attempted_ && rows_seen_ >= jit_threshold_rows_) {
        jit_attempted_ = true;
        // Prefer the overflow-checked kernel so a single huge batch throws
        // exactly like the serial path; never fall back to the wrapping
        // kernel for correctness.
        jit_sum_ = JitInt64Kernels::CompileSumChecked();
      }
      const ColumnVector& column = input_batch_.ColumnAt(jit_sum_column_);
      if (jit_sum_ &&
          input_batch_.ZoneMapAt(jit_sum_column_).NullCount() == 0) {
        // The JIT kernel reads raw int64 storage; verify the child batch
        // really has the declared layout before trusting it.
        if (!input_batch_.HasLayout(input_schema_) ||
            column.Type() != ValueType::kInt64) {
          CHECK_MSG(false, "aggregation input layout mismatch");
        }
        bool batch_overflow = false;
        const int64_t partial = jit_sum_->SumChecked(
            column.IntegerData().data(), column.Size(), &batch_overflow);
        // The checked kernel reports intra-batch overflow exactly like the
        // AST evaluator; merging into the running total is checked too.
        if (batch_overflow) {
          return FailWith(StatusError(StatusCode::kIsInfinity,

                                      "integer overflow on '+'"));
        }
        if (__builtin_add_overflow(total, partial, &total)) {
          return FailWith(StatusError(StatusCode::kIsInfinity,

                                      "integer overflow on '+'"));
        }
        any = any || column.Size() != 0;
        ++jit_batches_;
      } else {
        for (size_t row = 0; row < column.Size(); ++row) {
          if (column.IsNull(row)) {
            continue;
          }
          if (__builtin_add_overflow(total, column.ValueAt(row).value.int_value,
                                     &total)) {
            return FailWith(StatusError(StatusCode::kIsInfinity,

                                        "integer overflow on '+'"));
          }
          any = true;
        }
      }
    }
    if (FailWithChildOf(*child_)) {
      return false;
    }
    *dst = Row({any ? Value(total) : Value()});
    executed_ = true;
    return true;
  }
  return NextGeneric(dst);
}

StatusOr<bool> AggregationExecutor::TryFoldScan(Row* dst) {
  auto scan = std::dynamic_pointer_cast<ParallelScan>(child_);
  auto serial = std::dynamic_pointer_cast<FullScan>(child_);
  if (scan && aggregates_.size() == 1) {
    const auto& agg = aggregates_[0].expression->AsAggregateExpression();
    if (agg.GetType() == AggregationType::kCount && agg.Distinct() &&
        !agg.WhereFilter() && !agg.NeedsGroupContext() && agg.Child() &&
        agg.Child()->Type() == TypeTag::kColumnValue) {
      const int offset =
          input_schema_.Offset(agg.Child()->AsColumnValue().GetColumnName());
      if (offset >= 0) {
        const auto& col_def =
            input_schema_.GetColumn(static_cast<size_t>(offset));
        if ((col_def.Type() == ValueType::kInt64 ||
             col_def.Type() == ValueType::kDate) &&
            !col_def.IsUnsigned() &&
            scan->HasSignedIntegerColumn(static_cast<size_t>(offset))) {
          const auto* store = scan->GetLSTableStore();
          if (store != nullptr && !store->blocks.empty()) {
            const slot_t full_col =
                scan->TableColumnSlot(static_cast<size_t>(offset));
            int64_t global_min = std::numeric_limits<int64_t>::max();
            int64_t global_max = std::numeric_limits<int64_t>::min();
            bool valid_bounds = true;
            bool any_non_null = false;
            for (const LSTablePageBlock& blk : store->blocks) {
              if (blk.row_count == 0) {
                continue;
              }
              if (full_col >= blk.columns.size()) {
                valid_bounds = false;
                break;
              }
              const LSTableColumnBlock& col = blk.columns[full_col];
              if (!col.has_min_max) {
                valid_bounds = false;
                break;
              }
              any_non_null = true;
              global_min = std::min(global_min, col.min_int);
              global_max = std::max(global_max, col.max_int);
            }
            if (valid_bounds && !any_non_null) {
              ASSIGN_OR_RETURN(
                  bool, empty_consumed,
                  scan->TryFoldLSTableBlocks(
                      1, [](size_t, const LSTablePageBlock&, const uint32_t*,
                            size_t) { return Status::kSuccess; }));
              if (empty_consumed) {
                *dst = Row({Value(int64_t{0})});
                return true;
              }
            } else if (valid_bounds && global_max >= global_min) {
              const __int128_t span128 = static_cast<__int128_t>(global_max) -
                                         static_cast<__int128_t>(global_min) +
                                         1;
              if (span128 > 0 && span128 <= 1048576) {
                const auto span = static_cast<size_t>(span128);
                const size_t workers = std::min<size_t>(8, scan->WorkerCount());
                std::vector<std::vector<uint8_t>> worker_seen(
                    workers, std::vector<uint8_t>(span, 0));
                ASSIGN_OR_RETURN(
                    bool, consumed_distinct,
                    scan->TryFoldLSTableBlocks(
                        workers,
                        [&](size_t w, const LSTablePageBlock& blk,
                            const uint32_t* rows, size_t count) -> Status {
                          const LSTableColumnBlock& col = blk.columns[full_col];
                          uint8_t* __restrict seen = worker_seen[w].data();
                          const int64_t* __restrict cells =
                              col.int_cells.data();
                          if (rows == nullptr && !col.has_null) {
                            for (size_t r = 0; r < count; ++r) {
                              seen[static_cast<size_t>(cells[r] - global_min)] =
                                  1;
                            }
                          } else if (rows == nullptr) {
                            for (size_t r = 0; r < count; ++r) {
                              if (!col.IsNull(r)) {
                                seen[static_cast<size_t>(cells[r] -
                                                         global_min)] = 1;
                              }
                            }
                          } else if (!col.has_null) {
                            for (size_t i = 0; i < count; ++i) {
                              seen[static_cast<size_t>(cells[rows[i]] -
                                                       global_min)] = 1;
                            }
                          } else {
                            for (size_t i = 0; i < count; ++i) {
                              const uint32_t r = rows[i];
                              if (!col.IsNull(r)) {
                                seen[static_cast<size_t>(cells[r] -
                                                         global_min)] = 1;
                              }
                            }
                          }
                          return Status::kSuccess;
                        }));
                if (consumed_distinct) {
                  uint8_t* __restrict merged = worker_seen[0].data();
                  for (size_t w = 1; w < workers; ++w) {
                    const uint8_t* __restrict src = worker_seen[w].data();
                    for (size_t k = 0; k < span; ++k) {
                      merged[k] |= src[k];
                    }
                  }
                  int64_t distinct_count = 0;
                  for (size_t k = 0; k < span; ++k) {
                    distinct_count += merged[k];
                  }
                  *dst = Row({Value(distinct_count)});
                  return true;
                }
              }
            }
          }
        }
      }
    }
  }
  if (!all_typed_ || (!scan && !serial)) {
    return false;
  }
  for (size_t i = 0; i < inputs_.size(); ++i) {
    const auto type =
        aggregates_[i].expression->AsAggregateExpression().GetType();
    if (inputs_[i].kind == AggregateInputKind::kCountStar) {
      continue;
    }
    if (inputs_[i].kind != AggregateInputKind::kTypedColumn ||
        inputs_[i].type != ValueType::kInt64 ||
        input_schema_.GetColumn(inputs_[i].column).IsUnsigned() ||
        (scan && !scan->HasSignedIntegerColumn(inputs_[i].column)) ||
        (serial &&
         (inputs_[i].column >= serial->OutputSchema().ColumnCount() ||
          serial->OutputSchema().GetColumn(inputs_[i].column).Type() !=
              ValueType::kInt64 ||
          serial->OutputSchema().GetColumn(inputs_[i].column).IsUnsigned())) ||
        (type != AggregationType::kCount && type != AggregationType::kSum &&
         type != AggregationType::kMin && type != AggregationType::kMax)) {
      return false;
    }
  }
  // Merge morsels in physical order. Prefix extrema preserve checked SUM's
  // *intermediate* overflow, including cancellation across morsel boundaries.
  struct alignas(64) Partial {
    __int128_t sum{0};
    __int128_t low{0};
    __int128_t high{0};
    uint64_t count{0};
    int64_t min{std::numeric_limits<int64_t>::max()};
    int64_t max{std::numeric_limits<int64_t>::min()};
  };
  const size_t width = inputs_.size();
  const size_t worker_count = scan ? scan->WorkerCount() : 1;
  const size_t morsel_count =
      scan ? std::max(scan->MorselCount(), worker_count) : 1;
  const size_t columns =
      scan ? scan->OutputColumnCount() : serial->OutputSchema().ColumnCount();
  const size_t maximum = std::numeric_limits<size_t>::max();
  if (width > maximum / sizeof(Partial)) {
    return false;
  }
  const size_t partial_bytes = width * sizeof(Partial);
  if (morsel_count > maximum / partial_bytes) {
    return false;
  }
  const size_t states_bytes = morsel_count * partial_bytes;
  if (columns > maximum / (kDefaultVectorSize * 9)) {
    return false;
  }
  const size_t batch_bytes = columns * kDefaultVectorSize * 9;
  if ((batch_bytes != 0U) &&
      worker_count > (maximum - states_bytes) / batch_bytes) {
    return false;
  }
  const size_t bytes = states_bytes + (worker_count * batch_bytes);
  if (!QueryMemoryBudget::Global().CanReserve(bytes)) {
    return false;
  }
  QueryMemoryCharge charge(bytes);
  std::vector<Partial> partials(morsel_count * width);
  const auto consume = [&](size_t morsel,
                           const TypedCellBatch& batch) -> Status {
    for (size_t i = 0; i < width; ++i) {
      const AggregateInput& input = inputs_[i];
      Partial& partial = partials[(morsel * width) + i];
      if (input.kind == AggregateInputKind::kCountStar) {
        partial.count += batch.rows;
        continue;
      }
      if (input.column >= batch.stride ||
          batch.slot_types[input.column] != ValueType::kInt64 ||
          batch.slot_unsigned[input.column]) {
        return StatusError(StatusCode::kRuntimeError,
                           "typed aggregate input layout mismatch");
      }
      const auto type =
          aggregates_[i].expression->AsAggregateExpression().GetType();
      for (size_t row = 0; row < batch.rows; ++row) {
        const size_t cell = (row * batch.stride) + input.column;
        if (batch.nulls[cell]) {
          continue;
        }
        const int64_t value = batch.cells[cell];
        ++partial.count;
        if (type == AggregationType::kSum) {
          partial.sum += value;
          partial.low = std::min(partial.low, partial.sum);
          partial.high = std::max(partial.high, partial.sum);
        } else if (type == AggregationType::kMin) {
          partial.min = std::min(partial.min, value);
        } else if (type == AggregationType::kMax) {
          partial.max = std::max(partial.max, value);
        }
      }
    }
    return Status::kSuccess;
  };
  bool consumed = false;
  if (scan) {
    const auto consume_lstable = [&](size_t worker, const LSTablePageBlock& blk,
                                     const uint32_t* rows,
                                     size_t count) -> Status {
      for (size_t i = 0; i < width; ++i) {
        const AggregateInput& input = inputs_[i];
        Partial& partial = partials[(worker * width) + i];
        if (input.kind == AggregateInputKind::kCountStar) {
          partial.count += count;
          continue;
        }
        const slot_t full_col = scan->TableColumnSlot(input.column);
        if (full_col >= blk.columns.size()) {
          return StatusError(StatusCode::kRuntimeError,
                             "lstable aggregate column out of range");
        }
        const LSTableColumnBlock& col = blk.columns[full_col];
        const auto type =
            aggregates_[i].expression->AsAggregateExpression().GetType();
        if (!col.has_null && type == AggregationType::kSum && col.has_min_max &&
            col.min_int >= 0 &&
            col.max_int <= (std::numeric_limits<int64_t>::max() >> 16)) {
          partial.count += count;
          const int64_t* __restrict cells = col.int_cells.data();
          int64_t blk_sum = 0;
          if (rows == nullptr) {
            for (size_t r = 0; r < count; ++r) {
              blk_sum += cells[r];
            }
          } else {
            for (size_t idx = 0; idx < count; ++idx) {
              blk_sum += cells[rows[idx]];
            }
          }
          partial.sum += blk_sum;
          partial.high = std::max(partial.high, partial.sum);
          continue;
        }
        if (rows == nullptr && !col.has_null) {
          partial.count += count;
          if (type == AggregationType::kCount) {
            continue;
          }
          if (type == AggregationType::kMin && col.has_min_max) {
            partial.min = std::min(partial.min, col.min_int);
            continue;
          }
          if (type == AggregationType::kMax && col.has_min_max) {
            partial.max = std::max(partial.max, col.max_int);
            continue;
          }
        }
        for (size_t idx = 0; idx < count; ++idx) {
          const uint32_t r = rows ? rows[idx] : static_cast<uint32_t>(idx);
          if (col.IsNull(r)) {
            continue;
          }
          const int64_t value = col.int_cells[r];
          ++partial.count;
          if (type == AggregationType::kSum) {
            partial.sum += value;
            partial.low = std::min(partial.low, partial.sum);
            partial.high = std::max(partial.high, partial.sum);
          } else if (type == AggregationType::kMin) {
            partial.min = std::min(partial.min, value);
          } else if (type == AggregationType::kMax) {
            partial.max = std::max(partial.max, value);
          }
        }
      }
      return Status::kSuccess;
    };
    ASSIGN_OR_RETURN(bool, lstable_consumed,
                     scan->TryFoldLSTableBlocks(worker_count, consume_lstable));
    consumed = lstable_consumed;
  }
  if (!consumed) {
    if (scan && scan->HasRowFilter()) {
      return false;
    }
    ASSIGN_OR_RETURN(bool, typed_consumed,
                     scan ? scan->TryFoldTypedMorsels(consume)
                          : serial->TryFoldTypedBatches(consume));
    consumed = typed_consumed;
  }
  if (!consumed) {
    return false;
  }
  std::vector<Value> result;
  result.reserve(width);
  for (size_t i = 0; i < width; ++i) {
    const auto type =
        aggregates_[i].expression->AsAggregateExpression().GetType();
    __int128_t total = 0;
    uint64_t count = 0;
    int64_t min = std::numeric_limits<int64_t>::max();
    int64_t max = std::numeric_limits<int64_t>::min();
    for (size_t m = 0; m < morsel_count; ++m) {
      const Partial& partial = partials[(m * width) + i];
      if (type == AggregationType::kSum &&
          (total + partial.low < std::numeric_limits<int64_t>::min() ||
           total + partial.high > std::numeric_limits<int64_t>::max())) {
        return StatusError(StatusCode::kIsInfinity, "integer overflow on '+'");
      }
      total += partial.sum;
      count += partial.count;
      min = std::min(min, partial.min);
      max = std::max(max, partial.max);
    }
    if (type == AggregationType::kCount) {
      if (count > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
        return StatusError(StatusCode::kIsInfinity, "integer count overflow");
      }
      result.emplace_back(static_cast<int64_t>(count));
    } else if (count == 0) {
      result.emplace_back();
    } else {
      result.emplace_back(type == AggregationType::kSum
                              ? static_cast<int64_t>(total)
                          : type == AggregationType::kMin ? min
                                                          : max);
    }
  }
  *dst = Row(std::move(result));
  return true;
}

namespace {

// int64 accumulation mirrors Value::operator+ overflow semantics.
StatusOr<int64_t> CheckedAdd(int64_t lhs, int64_t rhs) {
  int64_t result = 0;
  if (__builtin_add_overflow(lhs, rhs, &result)) {
    return StatusError(StatusCode::kIsInfinity, "integer overflow on '+'");
  }
  return result;
}

StatusOr<double> CheckedDoubleAdd(double lhs, double rhs) {
  const double result = lhs + rhs;
  if (std::isinf(result) && std::isfinite(lhs) && std::isfinite(rhs)) {
    return StatusError(StatusCode::kIsInfinity, "double overflow in SUM");
  }
  return result;
}

// The typed fast path only understands COUNT/SUM/AVG; every other kind
// (MIN/MAX with NaN poisoning, STRING_AGG, COUNTIF, ARRAY_AGG,
// LOGICAL_AND/OR, statistical and sketch aggregates) routes through the
// general accumulator, which owns NULL/DISTINCT/HAVING/ORDER-BY semantics.
bool IsTypedAggregate(AggregationType type) {
  switch (type) {
    case AggregationType::kCount:
    case AggregationType::kSum:
    case AggregationType::kAvg:
    case AggregationType::kMin:
    case AggregationType::kMax:
    case AggregationType::kBitAnd:
    case AggregationType::kBitOr:
    case AggregationType::kBitXor:
    case AggregationType::kLogicalAnd:
    case AggregationType::kLogicalOr:
      return true;
    default:
      return false;
  }
}

}  // namespace

bool AggregationExecutor::AccumulateTypedBatch(std::vector<Value>* results,
                                               std::vector<int64_t>* counts,
                                               const DataChunk& chunk) const {
  if (!all_typed_) {
    return false;
  }
  const size_t rows = chunk.Size();
  if (rows == 0) {
    return true;
  }
  // Bind every column reference first: a batch whose runtime storage does
  // not match the declared numeric type must fall back wholesale, before any
  // accumulator state has moved.
  std::vector<const ColumnVector*> columns(inputs_.size(), nullptr);
  for (size_t i = 0; i < inputs_.size(); ++i) {
    const AggregateInput& input = inputs_[i];
    if (input.kind != AggregateInputKind::kTypedColumn) {
      continue;
    }
    const ColumnVector& column = chunk.ColumnAt(input.column);
    const ValueType actual = column.Type();
    // An all-null kNull column contributes nothing, which matches the
    // generic path; anything else unexpected defers to the generic loop.
    if (actual != input.type && actual != ValueType::kNull) {
      return false;
    }
    columns[i] = &column;
  }
  for (size_t i = 0; i < inputs_.size(); ++i) {
    const AggregateInput& input = inputs_[i];
    const auto& agg = aggregates_[i].expression->AsAggregateExpression();
    if (input.kind == AggregateInputKind::kCountStar) {
      (*results)[i].value.int_value += static_cast<int64_t>(rows);
      continue;
    }
    if (input.kind == AggregateInputKind::kTypedConstant) {
      const Value& constant = input.constant;
      switch (agg.GetType()) {
        case AggregationType::kCount:
          (*results)[i].value.int_value += static_cast<int64_t>(rows);
          break;
        case AggregationType::kSum:
          if (constant.type == ValueType::kInt64) {
            if (constant.IsUnsigned()) {
              uint64_t scaled = 0;
              if (__builtin_mul_overflow(
                      static_cast<uint64_t>(rows),
                      static_cast<uint64_t>(constant.value.int_value),
                      &scaled)) {
                return FailWith(StatusError(StatusCode::kIsInfinity,
                                            "uint64 overflow in SUM"));
              }
              const uint64_t prior =
                  (*results)[i].IsNull()
                      ? 0
                      : static_cast<uint64_t>((*results)[i].value.int_value);
              uint64_t next = 0;
              if (__builtin_add_overflow(prior, scaled, &next)) {
                return FailWith(StatusError(StatusCode::kIsInfinity,
                                            "uint64 overflow in SUM"));
              }
              (*results)[i] = Value(static_cast<int64_t>(next)).WithUnsigned();
            } else {
              int64_t scaled = 0;
              if (__builtin_mul_overflow(static_cast<int64_t>(rows),
                                         constant.value.int_value, &scaled)) {
                return FailWith(StatusError(StatusCode::kIsInfinity,
                                            "integer overflow on '+'"));
              }
              StatusOr<int64_t> acc =
                  (*results)[i].IsNull()
                      ? CheckedAdd(0, scaled)
                      : CheckedAdd((*results)[i].value.int_value, scaled);
              if (!acc.HasValue()) {
                return FailWith(acc.GetStatus());
              }
              (*results)[i] = Value(acc.MoveValue());
            }
          } else {
            (*results)[i] = Value(
                ((*results)[i].IsNull() ? 0.0
                                        : (*results)[i].value.double_value) +
                (static_cast<double>(rows) * constant.value.double_value));
          }
          break;
        case AggregationType::kAvg:
          (*results)[i].value.double_value +=
              static_cast<double>(rows) *
              (constant.type == ValueType::kInt64
                   ? (constant.IsUnsigned()
                          ? static_cast<double>(
                                static_cast<uint64_t>(constant.value.int_value))
                          : static_cast<double>(constant.value.int_value))
                   : constant.value.double_value);
          (*counts)[i] += static_cast<int64_t>(rows);
          break;
        case AggregationType::kMin:
        case AggregationType::kMax:
          // Constant argument: the result is the constant itself.
          (*results)[i] = constant;
          break;
        default:
          // Non-typed aggregate kinds are handled by the generic path.
          break;
      }
      continue;
    }
    const ColumnVector& column = *columns[i];
    if (column.Type() == ValueType::kNull) {
      continue;  // all-null batch
    }
    const bool is_int = column.Type() == ValueType::kInt64;
    const std::vector<int64_t>& integers = column.IntegerData();
    const std::vector<double>& doubles = column.DoubleData();
    switch (agg.GetType()) {
      case AggregationType::kCount: {
        int64_t non_null = 0;
        for (size_t row = 0; row < rows; ++row) {
          if (!column.IsNull(row)) {
            ++non_null;
          }
        }
        (*results)[i].value.int_value += non_null;
        break;
      }
      case AggregationType::kSum: {
        if (is_int) {
          std::string column_name =
              agg.Child()->AsColumnValue().GetColumnName().name;
          std::ranges::transform(column_name, column_name.begin(),
                                 [](unsigned char c) {
                                   return static_cast<char>(std::tolower(c));
                                 });
          const bool is_uint64 =
              column.IsUnsigned() ||
              column_name.find("uint64") != std::string::npos;
          if (is_uint64) {
            uint64_t batch_sum = 0;
            bool any = false;
            for (size_t row = 0; row < rows; ++row) {
              if (column.IsNull(row)) {
                continue;
              }
              const auto value = static_cast<uint64_t>(integers[row]);
              if (batch_sum > std::numeric_limits<uint64_t>::max() - value) {
                return FailWith(StatusError(StatusCode::kIsInfinity,
                                            "uint64 overflow in SUM"));
              }
              batch_sum += value;
              any = true;
            }
            if (!any) {
              break;
            }
            Value& total = (*results)[i];
            const uint64_t prior =
                total.IsNull() ? 0
                               : static_cast<uint64_t>(total.value.int_value);
            if (prior > std::numeric_limits<uint64_t>::max() - batch_sum) {
              return FailWith(StatusError(StatusCode::kIsInfinity,
                                          "uint64 overflow in SUM"));
            }
            total = Value(static_cast<int64_t>(
                              static_cast<uint64_t>(prior + batch_sum)))
                        .WithUnsigned();
            break;
          }
          int64_t batch_sum = 0;
          bool any = false;
          for (size_t row = 0; row < rows; ++row) {
            if (column.IsNull(row)) {
              continue;
            }
            StatusOr<int64_t> next = CheckedAdd(batch_sum, integers[row]);
            if (!next.HasValue()) {
              return FailWith(next.GetStatus());
            }
            batch_sum = next.Value();
            any = true;
          }
          if (!any) {
            break;
          }
          Value& total = (*results)[i];
          if (total.IsNull()) {
            total = Value(batch_sum);
          } else {
            StatusOr<int64_t> next =
                CheckedAdd(total.value.int_value, batch_sum);
            if (!next.HasValue()) {
              return FailWith(next.GetStatus());
            }
            total = Value(next.Value());
          }
        } else {
          double batch_sum = 0.0;
          bool any = false;
          for (size_t row = 0; row < rows; ++row) {
            if (column.IsNull(row)) {
              continue;
            }
            StatusOr<double> next = CheckedDoubleAdd(batch_sum, doubles[row]);
            if (!next.HasValue()) {
              return FailWith(next.GetStatus());
            }
            batch_sum = next.Value();
            any = true;
          }
          if (!any) {
            break;
          }
          Value& total = (*results)[i];
          if (total.IsNull()) {
            total = Value(batch_sum);
          } else {
            StatusOr<double> next =
                CheckedDoubleAdd(total.value.double_value, batch_sum);
            if (!next.HasValue()) {
              return FailWith(next.GetStatus());
            }
            total = Value(next.Value());
          }
        }
        break;
      }
      case AggregationType::kAvg: {
        double& total = (*results)[i].value.double_value;
        for (size_t row = 0; row < rows; ++row) {
          if (column.IsNull(row)) {
            continue;
          }
          total += is_int ? (column.IsUnsigned()
                                 ? static_cast<double>(
                                       static_cast<uint64_t>(integers[row]))
                                 : static_cast<double>(integers[row]))
                          : doubles[row];
          ++(*counts)[i];
        }
        break;
      }
      case AggregationType::kMin: {
        Value& best = (*results)[i];
        for (size_t row = 0; row < rows; ++row) {
          if (column.IsNull(row)) {
            continue;
          }
          if (is_int) {
            const int64_t candidate = integers[row];
            if (column.IsUnsigned()) {
              if (best.IsNull() ||
                  static_cast<uint64_t>(candidate) <
                      static_cast<uint64_t>(best.value.int_value)) {
                best = Value(candidate).WithUnsigned();
              }
            } else {
              if (best.IsNull() || candidate < best.value.int_value) {
                best = Value(candidate);
              }
            }
          } else {
            const double candidate = doubles[row];
            // GoogleSQL: any NaN in the group makes MIN/MAX NaN.  NaN fails
            // every comparison, so seed the result explicitly (the ground
            // truth accumulator in expression_eval does the same).
            if (std::isnan(candidate)) {
              best = Value(std::numeric_limits<double>::quiet_NaN());
              continue;
            }
            if (best.IsNull() || candidate < best.value.double_value) {
              best = Value(candidate);
            }
          }
        }
        break;
      }
      case AggregationType::kMax: {
        Value& best = (*results)[i];
        for (size_t row = 0; row < rows; ++row) {
          if (column.IsNull(row)) {
            continue;
          }
          if (is_int) {
            const int64_t candidate = integers[row];
            if (column.IsUnsigned()) {
              if (best.IsNull() || static_cast<uint64_t>(best.value.int_value) <
                                       static_cast<uint64_t>(candidate)) {
                best = Value(candidate).WithUnsigned();
              }
            } else {
              if (best.IsNull() || best.value.int_value < candidate) {
                best = Value(candidate);
              }
            }
          } else {
            const double candidate = doubles[row];
            if (std::isnan(candidate)) {
              best = Value(std::numeric_limits<double>::quiet_NaN());
              continue;
            }
            if (best.IsNull() || best.value.double_value < candidate) {
              best = Value(candidate);
            }
          }
        }
        break;
      }
      case AggregationType::kBitAnd: {
        const Value batch_val = column.AggregateBitAnd();
        if (!batch_val.IsNull()) {
          if ((*results)[i].IsNull()) {
            (*results)[i] =
                column.IsUnsigned() ? batch_val.WithUnsigned() : batch_val;
          } else {
            Value res = Value(static_cast<int64_t>(
                (*results)[i].value.int_value & batch_val.value.int_value));
            if (column.IsUnsigned()) {
              res = res.WithUnsigned();
            }
            (*results)[i] = res;
          }
        }
        break;
      }
      case AggregationType::kBitOr: {
        const Value batch_val = column.AggregateBitOr();
        if (!batch_val.IsNull()) {
          if ((*results)[i].IsNull()) {
            (*results)[i] =
                column.IsUnsigned() ? batch_val.WithUnsigned() : batch_val;
          } else {
            Value res = Value(static_cast<int64_t>(
                (*results)[i].value.int_value | batch_val.value.int_value));
            if (column.IsUnsigned()) {
              res = res.WithUnsigned();
            }
            (*results)[i] = res;
          }
        }
        break;
      }
      case AggregationType::kBitXor: {
        const Value batch_val = column.AggregateBitXor();
        if (!batch_val.IsNull()) {
          if ((*results)[i].IsNull()) {
            (*results)[i] =
                column.IsUnsigned() ? batch_val.WithUnsigned() : batch_val;
          } else {
            Value res = Value(static_cast<int64_t>(
                (*results)[i].value.int_value ^ batch_val.value.int_value));
            if (column.IsUnsigned()) {
              res = res.WithUnsigned();
            }
            (*results)[i] = res;
          }
        }
        break;
      }
      case AggregationType::kLogicalAnd: {
        const Value batch_val = column.AggregateLogicalAnd();
        if (!batch_val.IsNull()) {
          if ((*results)[i].IsNull()) {
            (*results)[i] = batch_val;
          } else if (batch_val.value.int_value == 0) {
            (*results)[i] = Value(int64_t{0});
          }
        }
        break;
      }
      case AggregationType::kLogicalOr: {
        const Value batch_val = column.AggregateLogicalOr();
        if (!batch_val.IsNull()) {
          if ((*results)[i].IsNull()) {
            (*results)[i] = batch_val;
          } else if (batch_val.value.int_value != 0) {
            (*results)[i] = Value(int64_t{1});
          }
        }
        break;
      }
      default:
        // Only the typed aggregate kinds can reach this fast path.
        break;
    }
  }
  return true;
}

bool AggregationExecutor::NextGeneric(Row* dst) {
  std::vector<Value> results;
  results.resize(aggregates_.size());
  std::vector<int64_t> counts(aggregates_.size(), 0);
  std::vector<relational_detail::DistinctValueSet> distinct_values(
      aggregates_.size());
  // Every retained distinct value holds a forced global-budget reservation;
  // without releasing them when this aggregation ends, the process-wide
  // budget would ratchet up forever and eventually force everything to spill.
  size_t distinct_charged_bytes = 0;
  struct DistinctChargeRelease {
    size_t* bytes;
    explicit DistinctChargeRelease(size_t* charged_bytes)
        : bytes(charged_bytes) {}
    DistinctChargeRelease(const DistinctChargeRelease&) = delete;
    DistinctChargeRelease& operator=(const DistinctChargeRelease&) = delete;
    DistinctChargeRelease(DistinctChargeRelease&&) = delete;
    DistinctChargeRelease& operator=(DistinctChargeRelease&&) = delete;
    ~DistinctChargeRelease() {
      if (*bytes != 0) {
        QueryMemoryBudget::Global().Release(*bytes);
      }
    }
  } distinct_charge_release{&distinct_charged_bytes};
  // General accumulators for aggregate kinds the typed switches do not
  // model (kept in aggregate order).
  std::vector<std::unique_ptr<relational_detail::AggregateAccumulator>>
      accumulators(aggregates_.size());
  for (size_t i = 0; i < aggregates_.size(); ++i) {
    const auto& agg = aggregates_[i].expression->AsAggregateExpression();
    // If binding could not prove the input column type (CTAS and some
    // subquery schemas deliberately use kNull as an "unknown" type), keep
    // SUM/AVG in the generic accumulator.  Otherwise the inline typed switch
    // silently treats a UINT64 bit pattern as signed INT64 and can miss the
    // required overflow error.
    if (IsTypedAggregate(agg.GetType()) &&
        inputs_[i].kind != AggregateInputKind::kGeneric) {
      continue;
    }
    accumulators[i] = std::make_unique<relational_detail::AggregateAccumulator>(
        &aggregates_[i].expression->AsAggregateExpression(), &input_schema_);
  }
  for (size_t i = 0; i < aggregates_.size(); ++i) {
    const auto& agg = aggregates_[i].expression->AsAggregateExpression();
    switch (agg.GetType()) {
      case AggregationType::kCount:
        results[i] = Value(0);
        break;
      case AggregationType::kAvg:
        results[i] = Value(0.0);
        break;
      case AggregationType::kSum:
      case AggregationType::kMin:
      case AggregationType::kMax:
      case AggregationType::kBitAnd:
      case AggregationType::kBitOr:
      case AggregationType::kBitXor:
      case AggregationType::kLogicalAnd:
      case AggregationType::kLogicalOr:
        results[i] = Value();
        break;
      default:
        // Generic aggregates are initialized by their accumulator.
        break;
    }
  }
  while (child_->NextBatch(&input_batch_) != 0) {
    if (AccumulateTypedBatch(&results, &counts, input_batch_)) {
      continue;
    }
    bool all_done = true;
    for (size_t k = 0; k < aggregates_.size(); ++k) {
      if (accumulators[k] == nullptr || !accumulators[k]->IsDone()) {
        all_done = false;
        break;
      }
    }
    if (all_done && !aggregates_.empty()) {
      break;
    }
    Status agg_error{Status::kSuccess};
    auto eval_input = [&](const Expression& e,
                          const Row& row) -> std::optional<Value> {
      StatusOr<Value> v = e->TryEvaluate(row, input_schema_);
      if (!v.HasValue()) {
        agg_error = v.GetStatus();
        return std::nullopt;
      }
      return v.MoveValue();
    };
    for (size_t row_index = 0; row_index < input_batch_.Size(); ++row_index) {
      std::optional<Row> materialized;
      auto materialize = [&]() -> const Row& {
        if (!materialized) {
          materialized = input_batch_.RowAt(row_index);
        }
        return *materialized;
      };
      for (size_t i = 0; i < aggregates_.size(); ++i) {
        const auto& agg = aggregates_[i].expression->AsAggregateExpression();
        // Non-typed aggregates own their full semantics via the accumulator.
        if (accumulators[i]) {
          if (accumulators[i]->IsDone()) {
            continue;
          }
          if (agg.WhereFilter()) {
            std::optional<Value> keep =
                eval_input(agg.WhereFilter(), materialize());
            if (!keep.has_value()) {
              return FailWith(agg_error);
            }
            if (!relational_detail::Truthy(*keep)) {
              continue;
            }
          }
          relational_detail::AggregateInput input;
          if (!IsCountStar(agg)) {
            std::optional<Value> value = eval_input(agg.Child(), materialize());
            if (!value.has_value()) {
              return FailWith(agg_error);
            }
            input.value = std::move(*value);
          } else {
            input.value = Value(1);
          }
          if (agg.Having() != AggregateHavingModifier::kNone &&
              agg.HavingCondition()) {
            std::optional<Value> cond =
                eval_input(agg.HavingCondition(), materialize());
            if (!cond.has_value()) {
              return FailWith(agg_error);
            }
            input.condition = std::move(*cond);
          }
          for (const auto& term : agg.InnerOrderBy()) {
            std::optional<Value> key =
                eval_input(term.expression, materialize());
            if (!key.has_value()) {
              return FailWith(agg_error);
            }
            input.order_keys.push_back(std::move(*key));
          }
          if (agg.GetType() == AggregationType::kStringAgg &&
              agg.SecondaryArg()) {
            std::optional<Value> aux =
                eval_input(agg.SecondaryArg(), materialize());
            if (!aux.has_value()) {
              return FailWith(agg_error);
            }
            input.auxiliary = std::move(*aux);
          }
          for (const Expression& extra : agg.TrailingArgs()) {
            if (extra) {
              std::optional<Value> trail = eval_input(extra, materialize());
              if (!trail.has_value()) {
                return FailWith(agg_error);
              }
              input.trailing_values.push_back(std::move(*trail));
            }
          }
          if (Status st = accumulators[i]->Add(std::move(input));
              st != Status::kSuccess) {
            return FailWith(st);
          }
          continue;
        }
        if (agg.WhereFilter()) {
          std::optional<Value> keep =
              eval_input(agg.WhereFilter(), materialize());
          if (!keep.has_value()) {
            return FailWith(agg_error);
          }
          if (!relational_detail::Truthy(*keep)) {
            continue;
          }
        }
        Value val;
        if (IsCountStar(agg)) {
          val = Value(1);
        } else if (agg.Child()->Type() == TypeTag::kColumnValue) {
          const int offset = input_schema_.Offset(
              agg.Child()->AsColumnValue().GetColumnName());
          if (offset >= 0) {
            val = input_batch_.ColumnAt(static_cast<size_t>(offset))
                      .ValueAt(row_index);
          } else {
            std::optional<Value> value = eval_input(agg.Child(), materialize());
            if (!value.has_value()) {
              return FailWith(agg_error);
            }
            val = std::move(*value);
          }
        } else {
          std::optional<Value> value = eval_input(agg.Child(), materialize());
          if (!value.has_value()) {
            return FailWith(agg_error);
          }
          val = std::move(*value);
        }
        if (val.IsNull()) {
          continue;
        }
        if (agg.Distinct()) {
          const size_t bytes = EstimateValueBytes(val);
          QueryMemoryBudget::Global().ReserveForced(bytes);
          if (!distinct_values[i]
                   .insert(relational_detail::CanonicalDistinctValue(val))
                   .second) {
            QueryMemoryBudget::Global().Release(bytes);
            continue;
          }
          distinct_charged_bytes += bytes;
        }
        switch (agg.GetType()) {
          case AggregationType::kSum:
          case AggregationType::kAvg:
            // SUM/AVG are numeric aggregates; reading the union of a
            // non-numeric value would be garbage (SUM(varchar) must not
            // degrade to string concatenation).
            if (val.type != ValueType::kInt64 &&
                val.type != ValueType::kDouble) {
              return FailWith(StatusError(StatusCode::kInvalidArgument,

                                          "numeric value required"));
            }
            break;
          default:
            break;
        }
        switch (agg.GetType()) {
          case AggregationType::kSum:
            if (results[i].IsNull()) {
              results[i] = val;
            } else if (val.type == ValueType::kDouble &&
                       results[i].type == ValueType::kDouble) {
              StatusOr<double> next = CheckedDoubleAdd(
                  results[i].value.double_value, val.value.double_value);
              if (!next.HasValue()) {
                return FailWith(next.GetStatus());
              }
              results[i] = Value(next.Value());
            } else {
              results[i] = results[i] + val;
            }
            break;
          case AggregationType::kAvg:
            results[i].value.double_value +=
                val.type == ValueType::kDouble
                    ? val.value.double_value
                    : (val.IsUnsigned()
                           ? static_cast<double>(
                                 static_cast<uint64_t>(val.value.int_value))
                           : static_cast<double>(val.value.int_value));
            ++counts[i];
            break;
          case AggregationType::kMin:
            if (val.type == ValueType::kDouble &&
                std::isnan(val.value.double_value)) {
              // Any NaN makes the group MIN/MAX NaN (see ground truth).
              results[i] = Value(std::numeric_limits<double>::quiet_NaN());
              break;
            }
            if (results[i].type == ValueType::kDouble &&
                std::isnan(results[i].value.double_value)) {
              break;  // NaN best wins; comparisons would be unsound.
            }
            if (results[i].IsNull() || val < results[i]) {
              results[i] = val;
            }
            break;
          case AggregationType::kMax:
            if (val.type == ValueType::kDouble &&
                std::isnan(val.value.double_value)) {
              results[i] = Value(std::numeric_limits<double>::quiet_NaN());
              break;
            }
            if (results[i].type == ValueType::kDouble &&
                std::isnan(results[i].value.double_value)) {
              break;
            }
            if (results[i].IsNull() || results[i] < val) {
              results[i] = val;
            }
            break;
          case AggregationType::kCount:
            ++results[i].value.int_value;
            break;
          // PRODUCTION FIX: when a typed aggregate shares the row-wise loop
          // with a generic one (mixed SELECT list), the typed aggregate owns
          // no row accumulator; without these cases BIT_*/LOGICAL_* silently
          // produced NULL.
          case AggregationType::kBitAnd:
          case AggregationType::kBitOr:
          case AggregationType::kBitXor: {
            if (val.IsNull()) {
              break;
            }
            if (val.type != ValueType::kInt64) {
              return FailWith(
                  StatusError(StatusCode::kInvalidArgument,
                              "BIT aggregate requires an integer argument"));
            }
            const int64_t v = val.value.int_value;
            int64_t& cur = results[i].value.int_value;
            if (results[i].IsNull()) {
              results[i] =
                  val.IsUnsigned() ? Value(v).WithUnsigned() : Value(v);
            } else if (agg.GetType() == AggregationType::kBitAnd) {
              cur &= v;
              if (val.IsUnsigned()) {
                results[i] = results[i].WithUnsigned();
              }
            } else if (agg.GetType() == AggregationType::kBitOr) {
              cur |= v;
              if (val.IsUnsigned()) {
                results[i] = results[i].WithUnsigned();
              }
            } else {
              cur ^= v;
              if (val.IsUnsigned()) {
                results[i] = results[i].WithUnsigned();
              }
            }
            break;
          }
          case AggregationType::kLogicalAnd:
            if (!val.IsNull()) {
              if (results[i].IsNull()) {
                results[i] =
                    Value(val.value.int_value != 0 ? int64_t{1} : int64_t{0});
              } else if (val.value.int_value == 0) {
                results[i] = Value(int64_t{0});
              }
            }
            break;
          case AggregationType::kLogicalOr:
            if (!val.IsNull() && val.value.int_value != 0) {
              results[i] = Value(int64_t{1});
            }
            break;
          default:
            // Other aggregate kinds are owned by their accumulator.
            break;
        }
      }
    }
  }
  if (FailWithChildOf(*child_)) {
    return false;
  }
  for (size_t i = 0; i < aggregates_.size(); ++i) {
    const auto& agg = aggregates_[i].expression->AsAggregateExpression();
    switch (agg.GetType()) {
      case AggregationType::kAvg:
        if (counts[i] == 0) {
          results[i] = Value();
        } else {
          results[i].value.double_value /= static_cast<double>(counts[i]);
        }
        break;
      default:
        // NOP
        break;
    }
  }
  for (size_t i = 0; i < aggregates_.size(); ++i) {
    if (accumulators[i]) {
      StatusOr<Value> finished_value = accumulators[i]->TryFinish();
      if (!finished_value.HasValue()) {
        return FailWith(finished_value.GetStatus());
      }
      results[i] = finished_value.MoveValue();
    }
  }

  *dst = Row(results);
  executed_ = true;
  return true;
}

size_t AggregationExecutor::NextBatch(DataChunk* destination, size_t max_rows) {
  destination->Reset();
  if (max_rows == 0) {
    return 0;
  }
  Row row;
  if (!Execute(&row)) {
    return 0;
  }
  destination->Append(std::move(row));
  return 1;
}

void AggregationExecutor::Dump(std::ostream& o, int indent) const {
  // The Cascades path only creates scalar aggregates (no GROUP BY keys).
  o << "Aggregate strategy=scalar {";
  for (const auto& agg : aggregates_) {
    o << "\n"
      << Indent(static_cast<size_t>(indent) + 2) << agg.name << ": "
      << *agg.expression;
  }
  o << "\n" << Indent(static_cast<size_t>(indent)) << "}\n";
  child_->Dump(o, indent + 2);
}

}  // namespace tinylamb
