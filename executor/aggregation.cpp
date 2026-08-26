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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <ostream>
#include <stdexcept>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "executor/data_chunk.hpp"
#include "executor/executor_base.hpp"
#include "executor/query_memory.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/constant_value.hpp"
#include "expression/jit.hpp"
#include "expression/named_expression.hpp"
#include "page/row_position.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/type.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

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
        !aggregate.WhereFilter() &&
        aggregate.Child()->Type() == TypeTag::kColumnValue) {
      const int offset = input_schema_.Offset(
          aggregate.Child()->AsColumnValue().GetColumnName());
      if (offset >= 0 &&
          input_schema_.GetColumn(offset).Type() == ValueType::kInt64) {
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
    if (!agg.HasInnerGroupBy() && !agg.Distinct() && !agg.WhereFilter() &&
        IsCountStar(agg)) {
      input.kind = AggregateInputKind::kCountStar;
    } else if (!agg.HasInnerGroupBy() && !agg.Distinct() &&
               !agg.WhereFilter() &&
               agg.Child()->Type() == TypeTag::kColumnValue) {
      const int offset =
          input_schema_.Offset(agg.Child()->AsColumnValue().GetColumnName());
      if (offset >= 0) {
        const ValueType declared = input_schema_.GetColumn(offset).Type();
        if (declared == ValueType::kInt64 || declared == ValueType::kDouble) {
          input.kind = AggregateInputKind::kTypedColumn;
          input.column = static_cast<size_t>(offset);
          input.type = declared;
        }
      }
    } else if (!agg.HasInnerGroupBy() && !agg.Distinct() &&
               !agg.WhereFilter() &&
               agg.Child()->Type() == TypeTag::kConstantValue) {
      const Value constant = agg.Child()->AsConstantValue().GetValue();
      if (!constant.IsNull() && (constant.type == ValueType::kInt64 ||
                                 constant.type == ValueType::kDouble)) {
        input.kind = AggregateInputKind::kTypedConstant;
        input.constant = std::move(constant);
      }
    }
    all_typed_ = all_typed_ && input.kind != AggregateInputKind::kGeneric;
    inputs_.push_back(std::move(input));

    static const std::unordered_set<int> kNativeTypes = {
        static_cast<int>(AggregationType::kCount),
        static_cast<int>(AggregationType::kSum),
        static_cast<int>(AggregationType::kAvg),
        static_cast<int>(AggregationType::kMin),
        static_cast<int>(AggregationType::kMax)};
    const bool native =
        !agg.HasInnerGroupBy() && !agg.Distinct() && !agg.WhereFilter() &&
        kNativeTypes.count(static_cast<int>(agg.GetType())) != 0;
    delegates_to_accumulator_.push_back(!native);
    if (!native) {
      accumulators_.push_back(
          std::make_unique<relational_detail::AggregateAccumulator>(&agg));
    } else {
      accumulators_.emplace_back();
    }
  }
}

bool AggregationExecutor::Next(Row* dst, RowPosition* /*rp*/) {
  if (executed_) {
    return false;
  }
  if (jit_sum_eligible_) {
    int64_t total = 0;
    bool any = false;
    while (child_->NextBatch(&input_batch_) != 0) {
      rows_seen_ += input_batch_.Size();
      if (!jit_attempted_ && rows_seen_ >= jit_threshold_rows_) {
        jit_attempted_ = true;
        jit_sum_ = JitInt64Kernels::CompileSum();
      }
      const ColumnVector& column = input_batch_.ColumnAt(jit_sum_column_);
      if (jit_sum_ &&
          input_batch_.ZoneMapAt(jit_sum_column_).NullCount() == 0) {
        // The JIT kernel reads raw int64 storage; verify the child batch
        // really has the declared layout before trusting it.
        if (!input_batch_.HasLayout(input_schema_) ||
            column.Type() != ValueType::kInt64) {
          throw std::runtime_error("aggregation input layout mismatch");
        }
        total += jit_sum_->Sum(column.IntegerData().data(), column.Size());
        any = any || column.Size() != 0;
        ++jit_batches_;
      } else {
        for (size_t row = 0; row < column.Size(); ++row) {
          if (column.IsNull(row)) {
            continue;
          }
          total += column.ValueAt(row).value.int_value;
          any = true;
        }
      }
    }
    *dst = Row({any ? Value(total) : Value()});
    executed_ = true;
    return true;
  }
  return NextGeneric(dst);
}

namespace {

// int64 accumulation mirrors Value::operator+ overflow semantics.
int64_t CheckedAdd(int64_t lhs, int64_t rhs) {
  int64_t result = 0;
  if (__builtin_add_overflow(lhs, rhs, &result)) {
    throw std::runtime_error("integer overflow on '+'");
  }
  return result;
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
            int64_t scaled = 0;
            if (__builtin_mul_overflow(static_cast<int64_t>(rows),
                                       constant.value.int_value, &scaled)) {
              throw std::runtime_error("integer overflow on '+'");
            }
            (*results)[i] =
                (*results)[i].IsNull()
                    ? Value(CheckedAdd(0, scaled))
                    : Value(CheckedAdd((*results)[i].value.int_value, scaled));
          } else {
            (*results)[i] = Value(
                ((*results)[i].IsNull() ? 0.0
                                        : (*results)[i].value.double_value) +
                static_cast<double>(rows) * constant.value.double_value);
          }
          break;
        case AggregationType::kAvg:
          (*results)[i].value.double_value +=
              static_cast<double>(rows) *
              (constant.type == ValueType::kInt64
                   ? static_cast<double>(constant.value.int_value)
                   : constant.value.double_value);
          (*counts)[i] += static_cast<int64_t>(rows);
          break;
        case AggregationType::kMin:
        case AggregationType::kMax:
          // Constant argument: the result is the constant itself.
          (*results)[i] = constant;
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
          int64_t batch_sum = 0;
          bool any = false;
          for (size_t row = 0; row < rows; ++row) {
            if (column.IsNull(row)) {
              continue;
            }
            batch_sum = CheckedAdd(batch_sum, integers[row]);
            any = true;
          }
          if (!any) {
            break;
          }
          Value& total = (*results)[i];
          total = total.IsNull()
                      ? Value(batch_sum)
                      : Value(CheckedAdd(total.value.int_value, batch_sum));
        } else {
          double batch_sum = 0.0;
          bool any = false;
          for (size_t row = 0; row < rows; ++row) {
            if (column.IsNull(row)) {
              continue;
            }
            batch_sum += doubles[row];
            any = true;
          }
          if (!any) {
            break;
          }
          Value& total = (*results)[i];
          total = total.IsNull() ? Value(batch_sum)
                                 : Value(total.value.double_value + batch_sum);
        }
        break;
      }
      case AggregationType::kAvg: {
        double& total = (*results)[i].value.double_value;
        for (size_t row = 0; row < rows; ++row) {
          if (column.IsNull(row)) {
            continue;
          }
          total += is_int ? static_cast<double>(integers[row]) : doubles[row];
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
            if (best.IsNull() || candidate < best.value.int_value) {
              best = Value(candidate);
            }
          } else {
            const double candidate = doubles[row];
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
            if (best.IsNull() || best.value.int_value < candidate) {
              best = Value(candidate);
            }
          } else {
            const double candidate = doubles[row];
            if (best.IsNull() || best.value.double_value < candidate) {
              best = Value(candidate);
            }
          }
        }
        break;
      }
    }
  }
  return true;
}

bool AggregationExecutor::NextGeneric(Row* dst) {
  std::vector<Value> results;
  results.resize(aggregates_.size());
  std::vector<int64_t> counts(aggregates_.size(), 0);
  std::vector<std::unordered_set<Value>> distinct_values(aggregates_.size());
  for (size_t i = 0; i < aggregates_.size(); ++i) {
    if (delegates_to_accumulator_[i]) {
      continue;
    }
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
        results[i] = Value();
        break;
    }
  }
  while (child_->NextBatch(&input_batch_) != 0) {
    if (AccumulateTypedBatch(&results, &counts, input_batch_)) {
      continue;
    }
    for (size_t row_index = 0; row_index < input_batch_.Size(); ++row_index) {
      std::optional<Row> materialized;
      for (size_t i = 0; i < aggregates_.size(); ++i) {
        const auto& agg = aggregates_[i].expression->AsAggregateExpression();
        if (delegates_to_accumulator_[i]) {
          if (agg.HasInnerGroupBy()) {
            // Multi-level: accumulate every inner aggregate per inner group.
            if (!materialized) {
              materialized = input_batch_.RowAt(row_index);
            }
            accumulators_[i]->SetEvalSchema(&input_schema_);
            std::vector<const AggregateExpression*> inner_aggs;
            std::unordered_set<const AggregateExpression*> inner_seen;
            relational_detail::CollectAggregates(agg.Child(), &inner_aggs,
                                                 &inner_seen);
            for (const auto& term : agg.InnerOrderBy()) {
              relational_detail::CollectAggregates(term.expression,
                                                   &inner_aggs, &inner_seen);
            }
            std::vector<Value> inner_keys;
            inner_keys.reserve(agg.InnerGroupBy().size());
            for (const Expression& key : agg.InnerGroupBy()) {
              inner_keys.push_back(
                  key->Evaluate(*materialized, input_schema_));
            }
            Row inner_key(std::move(inner_keys));
            accumulators_[i]->RememberInnerRepresentative(inner_key,
                                                          *materialized);
            if (inner_aggs.empty() && IsCountStar(agg)) {
              // COUNT(* GROUP BY k): the implicit inner aggregation is the
              // per-group row count, accumulated row by row.
              relational_detail::AggregateAccumulator* inner_acc =
                  accumulators_[i]->InnerAccumulator(inner_key, &agg);
              inner_acc->Add(Value(1));
              continue;
            }
            if (inner_aggs.empty()) {
              accumulators_[i]->RememberInnerKey(inner_key);
              continue;
            }
            for (const AggregateExpression* inner : inner_aggs) {
              relational_detail::AggregateAccumulator* inner_acc =
                  accumulators_[i]->InnerAccumulator(inner_key, inner);
              const AggregateExpression* ml = inner;
              if (!inner->HasInnerGroupBy() && inner->Child() != nullptr &&
                  inner->Child()->Type() == TypeTag::kAggregateExp &&
                  inner->Child()->AsAggregateExpression()
                      .HasInnerGroupBy()) {
                ml = &inner->Child()->AsAggregateExpression();
              }
              if (inner->HasInnerGroupBy() || ml != inner) {
                // Recursive multi-level: partition one level deeper inside
                // the inner accumulator.
                inner_acc->SetEvalSchema(&input_schema_);
                std::vector<const AggregateExpression*> inner2_aggs;
                std::unordered_set<const AggregateExpression*> inner2_seen;
                relational_detail::CollectAggregates(inner->Child(),
                                                     &inner2_aggs,
                                                     &inner2_seen);
                for (const auto& term : inner->InnerOrderBy()) {
                  relational_detail::CollectAggregates(term.expression,
                                                       &inner2_aggs,
                                                       &inner2_seen);
                }
                const std::vector<Expression>& ml_keys = ml->InnerGroupBy();
                std::vector<Value> inner_keys2;
                inner_keys2.reserve(ml_keys.size());
                for (const Expression& key : ml_keys) {
                  inner_keys2.push_back(
                      key->Evaluate(*materialized, input_schema_));
                }
                Row inner_key2(std::move(inner_keys2));
                inner_acc->RememberInnerRepresentative(inner_key2,
                                                       *materialized);
                if (inner2_aggs.empty() && IsCountStar(*inner)) {
                  inner_acc->InnerAccumulator(inner_key2, inner)
                      ->Add(Value(1));
                } else if (inner2_aggs.empty()) {
                  inner_acc->RememberInnerKey(inner_key2);
                }
                for (const AggregateExpression* inner2 : inner2_aggs) {
                  if (inner2->WhereFilter() &&
                      !inner2->WhereFilter()
                           ->Evaluate(*materialized, input_schema_)
                           .Truthy()) {
                    continue;
                  }
                  Value x2;
                  if (IsCountStar(*inner2)) {
                    x2 = Value(1);
                  } else {
                    x2 =
                        inner2->Child()->Evaluate(*materialized, input_schema_);
                  }
                  inner_acc->InnerAccumulator(inner_key2, inner2)->Add(x2);
                }
                continue;
              }
              if (inner->WhereFilter() &&
                  !inner->WhereFilter()
                       ->Evaluate(*materialized, input_schema_)
                       .Truthy()) {
                continue;
              }
              Value x;
              if (IsCountStar(*inner)) {
                x = Value(1);
              } else {
                x = inner->Child()->Evaluate(*materialized, input_schema_);
              }
              inner_acc->Add(x);
            }
            continue;
          }
          relational_detail::AggregateInput input;
          if (agg.WhereFilter()) {
            if (!materialized) {
              materialized = input_batch_.RowAt(row_index);
            }
            if (!agg.WhereFilter()
                     ->Evaluate(*materialized, input_schema_)
                     .Truthy()) {
              continue;
            }
          }
          if (IsCountStar(agg)) {
            input.value = Value(1);
          } else if (agg.Child()->Type() == TypeTag::kColumnValue) {
            const int offset = input_schema_.Offset(
                agg.Child()->AsColumnValue().GetColumnName());
            if (offset >= 0) {
              input.value = input_batch_.ColumnAt(static_cast<size_t>(offset))
                                .ValueAt(row_index);
            } else {
              if (!materialized) {
                materialized = input_batch_.RowAt(row_index);
              }
              input.value = agg.Child()->Evaluate(*materialized, input_schema_);
            }
          } else {
            if (!materialized) {
              materialized = input_batch_.RowAt(row_index);
            }
            input.value = agg.Child()->Evaluate(*materialized, input_schema_);
          }
          if (agg.SecondaryArg()) {
            if (!materialized) {
              materialized = input_batch_.RowAt(row_index);
            }
            input.auxiliary =
                agg.SecondaryArg()->Evaluate(*materialized, input_schema_);
          }
          if (agg.Having() != AggregateHavingModifier::kNone &&
              agg.HavingCondition()) {
            if (!materialized) {
              materialized = input_batch_.RowAt(row_index);
            }
            input.condition =
                agg.HavingCondition()->Evaluate(*materialized, input_schema_);
          }
          accumulators_[i]->Add(std::move(input));
          continue;
        }
        if (agg.WhereFilter()) {
          if (!materialized) {
            materialized = input_batch_.RowAt(row_index);
          }
          if (!agg.WhereFilter()
                   ->Evaluate(*materialized, input_schema_)
                   .Truthy()) {
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
            if (!materialized) {
              materialized = input_batch_.RowAt(row_index);
            }
            val = agg.Child()->Evaluate(*materialized, input_schema_);
          }
        } else {
          if (!materialized) {
            materialized = input_batch_.RowAt(row_index);
          }
          val = agg.Child()->Evaluate(*materialized, input_schema_);
        }
        if (val.IsNull()) {
          continue;
        }
        if (agg.Distinct()) {
          const size_t bytes = EstimateValueBytes(val);
          QueryMemoryBudget::Global().ReserveForced(bytes);
          if (!distinct_values[i].insert(val).second) {
            QueryMemoryBudget::Global().Release(bytes);
            continue;
          }
        }
        switch (agg.GetType()) {
          case AggregationType::kSum:
          case AggregationType::kAvg:
            // SUM/AVG are numeric aggregates; reading the union of a
            // non-numeric value would be garbage (SUM(varchar) must not
            // degrade to string concatenation).
            if (val.type != ValueType::kInt64 &&
                val.type != ValueType::kDouble) {
              throw std::runtime_error("numeric value required");
            }
            break;
          default:
            break;
        }
        switch (agg.GetType()) {
          case AggregationType::kSum:
            results[i] = results[i].IsNull() ? val : results[i] + val;
            break;
          case AggregationType::kAvg:
            results[i].value.double_value +=
                val.type == ValueType::kDouble
                    ? val.value.double_value
                    : static_cast<double>(val.value.int_value);
            ++counts[i];
            break;
          case AggregationType::kMin:
            if (results[i].IsNull() || val < results[i]) {
              results[i] = val;
            }
            break;
          case AggregationType::kMax:
            if (results[i].IsNull() || results[i] < val) {
              results[i] = val;
            }
            break;
          case AggregationType::kCount:
            ++results[i].value.int_value;
            break;
        }
      }
    }
  }
  for (size_t i = 0; i < aggregates_.size(); ++i) {
    if (delegates_to_accumulator_[i]) {
      results[i] = accumulators_[i]->Finish();
      continue;
    }
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
  if (!Next(&row, nullptr)) {
    return 0;
  }
  destination->Append(std::move(row));
  return 1;
}

void AggregationExecutor::Dump(std::ostream& o, int indent) const {
  o << "AggregationExecutor {";
  for (const auto& agg : aggregates_) {
    o << "\n" << Indent(indent + 2) << agg.name << ": " << *agg.expression;
  }
  o << "\n" << Indent(indent) << "}";
}

}  // namespace tinylamb
