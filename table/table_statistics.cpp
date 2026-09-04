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

#include "table/table_statistics.hpp"

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <limits>
#include <optional>
#include <ostream>
#include <random>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/decoder.hpp"
#include "common/encoder.hpp"
#include "expression/binary_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/unary_expression.hpp"
#include "table/iterator.hpp"
#include "table/table.hpp"
#include "transaction/transaction.hpp"
#include "type/column_name.hpp"
#include "type/row.hpp"
#include "type/type.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
namespace {

constexpr uint64_t kStatisticsMagic = 0x544C535441545302ULL;  // TLSTATS + v2.
constexpr uint64_t kStatisticsVersion = 2;
constexpr size_t kStoredStringPrefixLength = 32;
// Reservoir size per column: a multiple of the histogram bucket count so
// every bucket rests on >=256 sampled values.  Populations up to this size
// are still collected exactly.
constexpr size_t kStatSampleSize = kHistogramBucketCount * 256;

Value CompactValue(const Value& value) {
  if (value.type != ValueType::kVarChar ||
      value.value.varchar_value.size() <= kStoredStringPrefixLength) {
    return value;
  }
  return Value(std::string(
      value.value.varchar_value.substr(0, kStoredStringPrefixLength)));
}

void CompactFrequencies(std::vector<ValueFrequency>* frequencies) {
  for (ValueFrequency& frequency : *frequencies) {
    frequency.value = CompactValue(frequency.value);
  }
}

struct ValueLess {
  bool operator()(const Value& left, const Value& right) const {
    return left < right;
  }
};

struct CollectedColumn {
  size_t non_null_count{0};
  size_t null_count{0};
  size_t distinct_count{0};
  std::vector<HistogramBucket> histogram;
  std::vector<ValueFrequency> lowest;
  std::vector<ValueFrequency> highest;
  std::vector<ValueFrequency> most_common;
};

bool SameValue(const Value& left, const Value& right) {
  return left.type == right.type && left == right;
}

size_t ScaleCount(size_t count, double multiplier) {
  if (count == 0 || multiplier <= 0) {
    return 0;
  }
  const long double scaled = static_cast<long double>(count) * multiplier;
  if (scaled >= std::numeric_limits<size_t>::max()) {
    return std::numeric_limits<size_t>::max();
  }
  return static_cast<size_t>(std::llround(scaled));
}

class ColumnCollector {
 public:
  explicit ColumnCollector(ValueType type) : type_(type) {}

  void Add(const Value& value) {
    if (value.IsNull()) {
      ++null_count_;
      return;
    }
    if (value.type != type_) {
      throw std::runtime_error("column statistics type mismatch");
    }
    ++non_null_count_;
    const Value compacted = CompactValue(value);
    TrackLowest(compacted);
    TrackHighest(compacted);
    Sample(compacted);
  }

  [[nodiscard]] CollectedColumn Finish() const {
    CollectedColumn result;
    result.null_count = null_count_;
    result.non_null_count = non_null_count_;

    // Aggregate the reservoir into sorted per-value runs.  When every value
    // fit into the sample the runs are the exact frequency table; otherwise
    // they are a uniform sample whose counts scale up to the population.
    std::vector<Value> sorted(sample_);
    std::ranges::sort(sorted, ValueLess{});
    std::vector<ValueFrequency> values;
    values.reserve(sorted.size());
    for (const Value& value : sorted) {
      if (!values.empty() && SameValue(values.back().value, value)) {
        ++values.back().count;
      } else {
        values.push_back(ValueFrequency{.value = value, .count = 1});
      }
    }

    result.distinct_count = values.size();
    double scale = 1.0;
    if (non_null_count_ > kStatSampleSize) {
      scale = static_cast<double>(non_null_count_) /
              static_cast<double>(sorted.size());
      result.distinct_count = EstimateDistinct(values, sorted.size());
      for (ValueFrequency& frequency : values) {
        frequency.count = ScaleCount(frequency.count, scale);
      }
    }

    // The trackers keep the true kBoundaryValueCount smallest/largest
    // distinct values with their exact occurrence counts, regardless of the
    // population size.
    result.lowest.assign(lowest_.begin(), lowest_.end());
    result.highest.assign(highest_.rbegin(), highest_.rend());

    result.most_common = values;
    std::ranges::sort(result.most_common, [](const ValueFrequency& left,
                                             const ValueFrequency& right) {
      if (left.count != right.count) {
        return left.count > right.count;
      }
      return left.value < right.value;
    });
    if (result.most_common.size() > kMostCommonValueCount) {
      result.most_common.resize(kMostCommonValueCount);
    }
    CompactFrequencies(&result.lowest);
    CompactFrequencies(&result.highest);
    CompactFrequencies(&result.most_common);

    if (values.empty()) {
      return result;
    }
    const size_t bucket_target = std::max<size_t>(
        1, (result.non_null_count + kHistogramBucketCount - 1) /
               kHistogramBucketCount);
    HistogramBucket bucket;
    for (const ValueFrequency& frequency : values) {
      if (bucket.distinct > 0 && bucket.count >= bucket_target &&
          result.histogram.size() + 1 < kHistogramBucketCount) {
        result.histogram.push_back(std::move(bucket));
        bucket = HistogramBucket{};
      }
      if (bucket.distinct == 0) {
        bucket.lower = frequency.value;
      }
      bucket.upper = frequency.value;
      bucket.count += frequency.count;
      ++bucket.distinct;
    }
    result.histogram.push_back(std::move(bucket));
    for (HistogramBucket& histogram_bucket : result.histogram) {
      histogram_bucket.lower = CompactValue(histogram_bucket.lower);
      histogram_bucket.upper = CompactValue(histogram_bucket.upper);
    }
    return result;
  }

 private:
  // Good-Turing style distinct estimate from the uniform sample: shrink by
  // the fraction of singleton observations that suggests unseen values.
  [[nodiscard]] size_t EstimateDistinct(const std::vector<ValueFrequency>& runs,
                                        size_t sampled) const {
    size_t singletons = 0;
    for (const ValueFrequency& frequency : runs) {
      if (frequency.count == 1) {
        ++singletons;
      }
    }
    auto estimate = static_cast<double>(runs.size());
    if (singletons > 0 && singletons < sampled) {
      const double coverage = 1.0 - (static_cast<double>(singletons) /
                                     static_cast<double>(sampled));
      if (coverage >= 0.05) {
        estimate = static_cast<double>(runs.size()) / coverage;
      } else {
        estimate = static_cast<double>(non_null_count_);
      }
    }
    const double capped = std::clamp(estimate, static_cast<double>(runs.size()),
                                     static_cast<double>(non_null_count_));
    return static_cast<size_t>(std::llround(capped));
  }

  void TrackLowest(const Value& value) {
    for (size_t i = 0; i < lowest_.size(); ++i) {
      if (SameValue(lowest_[i].value, value)) {
        ++lowest_[i].count;
        return;
      }
      if (value < lowest_[i].value) {
        lowest_.insert(lowest_.begin() + i,
                       ValueFrequency{.value = value, .count = 1});
        if (lowest_.size() > kBoundaryValueCount) {
          lowest_.pop_back();
        }
        return;
      }
    }
    if (lowest_.size() < kBoundaryValueCount) {
      lowest_.push_back(ValueFrequency{.value = value, .count = 1});
    }
  }

  void TrackHighest(const Value& value) {
    for (size_t i = 0; i < highest_.size(); ++i) {
      if (SameValue(highest_[i].value, value)) {
        ++highest_[i].count;
        return;
      }
      if (highest_[i].value < value) {
        highest_.insert(highest_.begin() + i,
                        ValueFrequency{.value = value, .count = 1});
        if (highest_.size() > kBoundaryValueCount) {
          highest_.pop_back();
        }
        return;
      }
    }
    if (highest_.size() < kBoundaryValueCount) {
      highest_.push_back(ValueFrequency{.value = value, .count = 1});
    }
  }

  // Reservoir sampling (Algorithm R): after the reservoir fills, the n-th
  // value replaces a uniformly chosen slot with probability S/n.
  void Sample(const Value& value) {
    if (sample_.size() < kStatSampleSize) {
      if (sample_.capacity() == 0) {
        sample_.reserve(kStatSampleSize);
      }
      sample_.push_back(value);
      return;
    }
    std::uniform_int_distribution<uint64_t> pick(0, non_null_count_ - 1);
    const uint64_t slot = pick(rng_);
    if (slot < kStatSampleSize) {
      sample_[slot] = value;
    }
  }

  ValueType type_;
  size_t null_count_{0};
  size_t non_null_count_{0};
  std::vector<Value> sample_;
  std::vector<ValueFrequency> lowest_;
  std::vector<ValueFrequency> highest_;
  // Deterministic on purpose: reproducible samples keep query plans and
  // statistics tests stable across runs. Not used for any security purpose.
  std::mt19937_64 rng_{
      kStatisticsMagic ^
      0x9E3779B97F4A7C15ULL};  // NOLINT(cert-msc32-c,cert-msc51-cpp)
};

long double Position(const Value& value) {
  switch (value.type) {
    case ValueType::kInt64:
    case ValueType::kDate:
      return static_cast<long double>(value.value.int_value);
    case ValueType::kDouble:
      return static_cast<long double>(value.value.double_value);
    case ValueType::kVarChar: {
      long double position = 0;
      for (size_t i = 0; i < 8; ++i) {
        position *= 257;
        if (i < value.value.varchar_value.size()) {
          position +=
              static_cast<unsigned char>(value.value.varchar_value[i]) + 1;
        }
      }
      return position;
    }
    case ValueType::kArray:
    case ValueType::kNull:
      return 0;
  }
  return 0;
}

std::optional<Value> CoerceValue(const Value& value, ValueType type) {
  if (value.IsNull()) {
    return std::nullopt;
  }
  if (value.type == type) {
    return value;
  }
  if (type == ValueType::kDouble && value.type == ValueType::kInt64) {
    return Value(static_cast<double>(value.value.int_value));
  }
  if (type == ValueType::kInt64 && value.type == ValueType::kDouble) {
    const double integer = std::trunc(value.value.double_value);
    if (integer == value.value.double_value &&
        // (double)INT64_MAX rounds up to 2^63, so the old `<= INT64_MAX`
        // upper bound accepted exactly 9223372036854775808.0 and the cast
        // was UB (typically wrapping to INT64_MIN).
        integer >= -9223372036854775808.0 && integer < 9223372036854775808.0) {
      return Value(static_cast<int64_t>(integer));
    }
  }
  return std::nullopt;
}

BinaryOperation ReverseComparison(BinaryOperation operation) {
  switch (operation) {
    case BinaryOperation::kLessThan:
      return BinaryOperation::kGreaterThan;
    case BinaryOperation::kLessThanEquals:
      return BinaryOperation::kGreaterThanEquals;
    case BinaryOperation::kGreaterThan:
      return BinaryOperation::kLessThan;
    case BinaryOperation::kGreaterThanEquals:
      return BinaryOperation::kLessThanEquals;
    default:
      return operation;
  }
}

int ResolveColumn(const Schema& schema, const ColumnName& column) {
  const int exact = schema.Offset(column);
  if (exact >= 0) {
    return exact;
  }
  for (size_t i = 0; i < schema.ColumnCount(); ++i) {
    const ColumnName& candidate = schema.GetColumn(i).Name();
    if (candidate.name == column.name &&
        (column.schema.empty() || candidate.schema == column.schema)) {
      return static_cast<int>(i);
    }
  }
  return -1;
}

double ClampProbability(double probability) {
  if (!std::isfinite(probability)) {
    return 0;
  }
  return std::clamp(probability, 0.0, 1.0);
}

double ColumnConstantSelectivity(const ColumnStats& stats, size_t rows,
                                 BinaryOperation operation,
                                 const Value& raw_value) {
  if (rows == 0 || raw_value.IsNull()) {
    return 0;
  }
  const std::optional<Value> value = CoerceValue(raw_value, stats.Type());
  if (!value) {
    return 0;
  }

  double count = 0;
  switch (operation) {
    case BinaryOperation::kEquals:
      count = stats.EstimateEqual(*value);
      break;
    case BinaryOperation::kNotEquals:
      count = static_cast<double>(stats.NonNullCount()) -
              stats.EstimateEqual(*value);
      break;
    case BinaryOperation::kLessThan:
      count = stats.EstimateRange(std::nullopt, false, value, false);
      break;
    case BinaryOperation::kLessThanEquals:
      count = stats.EstimateRange(std::nullopt, false, value, true);
      break;
    case BinaryOperation::kGreaterThan:
      count = stats.EstimateRange(value, false, std::nullopt, false);
      break;
    case BinaryOperation::kGreaterThanEquals:
      count = stats.EstimateRange(value, true, std::nullopt, false);
      break;
    case BinaryOperation::kLike:
    case BinaryOperation::kNotLike: {
      const double base =
          stats.Distinct() == 0 ? 0 : 1.0 / std::sqrt(stats.Distinct());
      const double matching = stats.NonNullCount() * std::min(0.25, base);
      count = operation == BinaryOperation::kLike
                  ? matching
                  : stats.NonNullCount() - matching;
      break;
    }
    default:
      return 1;
  }
  return ClampProbability(count / static_cast<double>(rows));
}

struct AtomicColumnPredicate {
  int column{-1};
  BinaryOperation operation{BinaryOperation::kEquals};
  Value value;
};

std::optional<AtomicColumnPredicate> ExtractAtomicPredicate(
    const Schema& schema, const Expression& expression) {
  if (!expression || expression->Type() != TypeTag::kBinaryExp) {
    return std::nullopt;
  }
  const auto& binary = expression->AsBinaryExpression();
  if (binary.Left()->Type() == TypeTag::kColumnValue &&
      binary.Right()->Type() == TypeTag::kConstantValue) {
    return AtomicColumnPredicate{
        .column = ResolveColumn(schema,
                                binary.Left()->AsColumnValue().GetColumnName()),
        .operation = binary.Op(),
        .value = binary.Right()->AsConstantValue().GetValue()};
  }
  if (binary.Left()->Type() == TypeTag::kConstantValue &&
      binary.Right()->Type() == TypeTag::kColumnValue) {
    return AtomicColumnPredicate{
        .column = ResolveColumn(
            schema, binary.Right()->AsColumnValue().GetColumnName()),
        .operation = ReverseComparison(binary.Op()),
        .value = binary.Left()->AsConstantValue().GetValue()};
  }
  return std::nullopt;
}

bool ValueSatisfies(const Value& value, BinaryOperation operation,
                    const Value& boundary) {
  if (value.IsNull() || boundary.IsNull()) {
    return false;
  }
  if (value.type != boundary.type) {
    const bool numeric_value =
        value.type == ValueType::kInt64 || value.type == ValueType::kDouble;
    const bool numeric_boundary = boundary.type == ValueType::kInt64 ||
                                  boundary.type == ValueType::kDouble;
    if (!numeric_value || !numeric_boundary) {
      return false;
    }
    const double left = value.type == ValueType::kDouble
                            ? value.value.double_value
                            : static_cast<double>(value.value.int_value);
    const double right = boundary.type == ValueType::kDouble
                             ? boundary.value.double_value
                             : static_cast<double>(boundary.value.int_value);
    switch (operation) {
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
      case BinaryOperation::kGreaterThanEquals:
        return left >= right;
      default:
        return false;
    }
  }
  switch (operation) {
    case BinaryOperation::kEquals:
      return value == boundary;
    case BinaryOperation::kNotEquals:
      return value != boundary;
    case BinaryOperation::kLessThan:
      return value < boundary;
    case BinaryOperation::kLessThanEquals:
      return value <= boundary;
    case BinaryOperation::kGreaterThan:
      return value > boundary;
    case BinaryOperation::kGreaterThanEquals:
      return value >= boundary;
    default:
      return false;
  }
}

// Recursion mirrors the expression tree structure by design: each node type
// combines the selectivity estimates of its children.
double EstimatePredicate(const TableStatistics& table,
                         const Schema& schema,  // NOLINT(misc-no-recursion)
                         const Expression& predicate) {
  if (!predicate) {
    return 1;
  }
  if (predicate->TouchedColumns().empty()) {
    try {
      const Value value = predicate->Evaluate(Row(), Schema());
      if (value.IsNull()) {
        return 0;
      }
      return value.Truthy() ? 1 : 0;
    } catch (const std::exception&) {
      return 1;
    }
  }

  if (predicate->Type() == TypeTag::kUnaryExp) {
    const auto& unary = predicate->AsUnaryExpression();
    if (unary.Op() == UnaryOperation::kNot) {
      return 1 - EstimatePredicate(table, schema, unary.Child());
    }
    if ((unary.Op() == UnaryOperation::kIsNull ||
         unary.Op() == UnaryOperation::kIsNotNull) &&
        unary.Child()->Type() == TypeTag::kColumnValue) {
      const int offset =
          ResolveColumn(schema, unary.Child()->AsColumnValue().GetColumnName());
      if (offset < 0 || table.Rows() == 0) {
        return 0;
      }
      const double null_fraction =
          table.Column(offset).NullCount() / static_cast<double>(table.Rows());
      return unary.Op() == UnaryOperation::kIsNull ? null_fraction
                                                   : 1 - null_fraction;
    }
  }

  if (predicate->Type() == TypeTag::kInExp) {
    const auto& in = predicate->AsInExpression();
    if (in.child_->Type() != TypeTag::kColumnValue) {
      return 0.25;
    }
    const int offset =
        ResolveColumn(schema, in.child_->AsColumnValue().GetColumnName());
    if (offset < 0) {
      return 0.25;
    }
    double selectivity = 0;
    for (const Expression& item : in.list_) {
      if (item->Type() != TypeTag::kConstantValue) {
        return 0.25;
      }
      selectivity += ColumnConstantSelectivity(
          table.Column(offset), table.Rows(), BinaryOperation::kEquals,
          item->AsConstantValue().GetValue());
    }
    return ClampProbability(selectivity);
  }

  if (predicate->Type() != TypeTag::kBinaryExp) {
    return 0.25;
  }
  const auto& binary = predicate->AsBinaryExpression();
  if (binary.Op() == BinaryOperation::kAnd) {
    if (binary.Left()->ToString() == binary.Right()->ToString()) {
      return EstimatePredicate(table, schema, binary.Left());
    }
    const double left = EstimatePredicate(table, schema, binary.Left());
    const double right = EstimatePredicate(table, schema, binary.Right());
    const auto left_atom = ExtractAtomicPredicate(schema, binary.Left());
    const auto right_atom = ExtractAtomicPredicate(schema, binary.Right());
    if (left_atom && right_atom && left_atom->column >= 0 &&
        left_atom->column == right_atom->column) {
      if (left_atom->operation == BinaryOperation::kEquals) {
        return ValueSatisfies(left_atom->value, right_atom->operation,
                              right_atom->value)
                   ? left
                   : 0;
      }
      if (right_atom->operation == BinaryOperation::kEquals) {
        return ValueSatisfies(right_atom->value, left_atom->operation,
                              left_atom->value)
                   ? right
                   : 0;
      }
      // Predicates over one ordered column are correlated. The tighter bound
      // is safer than pretending they are independent.
      return std::min(left, right);
    }
    return ClampProbability(left * right);
  }
  if (binary.Op() == BinaryOperation::kOr) {
    const double left = EstimatePredicate(table, schema, binary.Left());
    const double right = EstimatePredicate(table, schema, binary.Right());
    const auto left_atom = ExtractAtomicPredicate(schema, binary.Left());
    const auto right_atom = ExtractAtomicPredicate(schema, binary.Right());
    if (left_atom && right_atom && left_atom->column >= 0 &&
        left_atom->column == right_atom->column) {
      if (left_atom->operation == BinaryOperation::kEquals) {
        return ValueSatisfies(left_atom->value, right_atom->operation,
                              right_atom->value)
                   ? right
                   : ClampProbability(left + right);
      }
      if (right_atom->operation == BinaryOperation::kEquals) {
        return ValueSatisfies(right_atom->value, left_atom->operation,
                              left_atom->value)
                   ? left
                   : ClampProbability(left + right);
      }
      return std::max(left, right);
    }
    return ClampProbability(left + right - (left * right));
  }
  if (binary.Op() == BinaryOperation::kXor) {
    const double left = EstimatePredicate(table, schema, binary.Left());
    const double right = EstimatePredicate(table, schema, binary.Right());
    return ClampProbability((left * (1 - right)) + (right * (1 - left)));
  }

  const bool left_column = binary.Left()->Type() == TypeTag::kColumnValue;
  const bool right_column = binary.Right()->Type() == TypeTag::kColumnValue;
  const bool left_constant = binary.Left()->Type() == TypeTag::kConstantValue;
  const bool right_constant = binary.Right()->Type() == TypeTag::kConstantValue;
  if (left_column && right_constant) {
    const int offset =
        ResolveColumn(schema, binary.Left()->AsColumnValue().GetColumnName());
    if (offset < 0) {
      return 0.25;
    }
    return ColumnConstantSelectivity(
        table.Column(offset), table.Rows(), binary.Op(),
        binary.Right()->AsConstantValue().GetValue());
  }
  if (left_constant && right_column) {
    const int offset =
        ResolveColumn(schema, binary.Right()->AsColumnValue().GetColumnName());
    if (offset < 0) {
      return 0.25;
    }
    return ColumnConstantSelectivity(
        table.Column(offset), table.Rows(), ReverseComparison(binary.Op()),
        binary.Left()->AsConstantValue().GetValue());
  }
  if (left_column && right_column && binary.Op() == BinaryOperation::kEquals) {
    const int left_offset =
        ResolveColumn(schema, binary.Left()->AsColumnValue().GetColumnName());
    const int right_offset =
        ResolveColumn(schema, binary.Right()->AsColumnValue().GetColumnName());
    if (left_offset < 0 || right_offset < 0 || table.Rows() == 0) {
      return 0.1;
    }
    const ColumnStats& left = table.Column(left_offset);
    const ColumnStats& right = table.Column(right_offset);
    if (left_offset == right_offset) {
      return left.NonNullCount() / static_cast<double>(table.Rows());
    }
    const size_t max_distinct = std::max(left.Distinct(), right.Distinct());
    if (max_distinct == 0) {
      return 0;
    }
    const double both_non_null =
        (left.NonNullCount() / static_cast<double>(table.Rows())) *
        (right.NonNullCount() / static_cast<double>(table.Rows()));
    return ClampProbability(both_non_null / max_distinct);
  }
  return 0.25;
}

}  // namespace

double ColumnStats::EstimateEqual(const Value& raw_value) const {
  std::optional<Value> value = CoerceValue(raw_value, type_);
  if (!value || non_null_count_ == 0 || distinct_count_ == 0) {
    return 0;
  }
  *value = CompactValue(*value);
  for (const ValueFrequency& frequency : most_common_values_) {
    if (SameValue(frequency.value, *value)) {
      return frequency.count;
    }
  }
  for (const ValueFrequency& frequency : lowest_values_) {
    if (SameValue(frequency.value, *value)) {
      return frequency.count;
    }
  }
  for (const ValueFrequency& frequency : highest_values_) {
    if (SameValue(frequency.value, *value)) {
      return frequency.count;
    }
  }
  for (const HistogramBucket& bucket : histogram_) {
    if (*value < bucket.lower || bucket.upper < *value) {
      continue;
    }
    if (bucket.distinct == 0) {
      return 0;
    }
    return static_cast<double>(bucket.count) / bucket.distinct;
  }
  // Legacy varchar statistics do not have persisted boundaries.
  if (histogram_.empty()) {
    return static_cast<double>(non_null_count_) / distinct_count_;
  }
  return 0;
}

double ColumnStats::EstimateLessThan(const Value& raw_value) const {
  std::optional<Value> value = CoerceValue(raw_value, type_);
  if (!value || non_null_count_ == 0) {
    return 0;
  }
  *value = CompactValue(*value);
  if (histogram_.empty()) {
    return non_null_count_ * 0.5;
  }

  double result = 0;
  for (const HistogramBucket& bucket : histogram_) {
    if (*value <= bucket.lower) {
      break;
    }
    if (bucket.upper < *value) {
      result += bucket.count;
      continue;
    }
    if (SameValue(*value, bucket.upper)) {
      result += std::max(
          0.0, static_cast<double>(bucket.count) - EstimateEqual(*value));
      break;
    }
    const long double lower = Position(bucket.lower);
    const long double upper = Position(bucket.upper);
    const long double target = Position(*value);
    const double fraction =
        upper <= lower
            ? 0
            : std::clamp(
                  static_cast<double>((target - lower) / (upper - lower)), 0.0,
                  1.0);
    result += bucket.count * fraction;
    break;
  }
  return std::clamp(result, 0.0, static_cast<double>(non_null_count_));
}

double ColumnStats::EstimateRange(const std::optional<Value>& raw_lower,
                                  bool lower_inclusive,
                                  const std::optional<Value>& raw_upper,
                                  bool upper_inclusive) const {
  std::optional<Value> lower;
  std::optional<Value> upper;
  if (raw_lower) {
    lower = CoerceValue(*raw_lower, type_);
  }
  if (raw_upper) {
    upper = CoerceValue(*raw_upper, type_);
  }
  if ((raw_lower && !lower) || (raw_upper && !upper)) {
    return 0;
  }
  if (lower) {
    *lower = CompactValue(*lower);
  }
  if (upper) {
    *upper = CompactValue(*upper);
  }
  if (lower && upper && *upper < *lower) {
    return 0;
  }
  if (lower && upper && SameValue(*lower, *upper)) {
    return lower_inclusive && upper_inclusive ? EstimateEqual(*lower) : 0;
  }

  auto before_upper = static_cast<double>(non_null_count_);
  if (upper) {
    before_upper = EstimateLessThan(*upper);
    if (upper_inclusive) {
      before_upper += EstimateEqual(*upper);
    }
  }
  double before_lower = 0;
  if (lower) {
    before_lower = EstimateLessThan(*lower);
    if (!lower_inclusive) {
      before_lower += EstimateEqual(*lower);
    }
  }
  return std::clamp(before_upper - before_lower, 0.0,
                    static_cast<double>(non_null_count_));
}

ColumnStats& ColumnStats::operator*=(double multiplier) {
  non_null_count_ = ScaleCount(non_null_count_, multiplier);
  null_count_ = ScaleCount(null_count_, multiplier);
  const double distinct_multiplier =
      multiplier < 1 ? std::sqrt(std::max(0.0, multiplier)) : 1;
  distinct_count_ = std::min(non_null_count_,
                             ScaleCount(distinct_count_, distinct_multiplier));
  for (HistogramBucket& bucket : histogram_) {
    bucket.count = ScaleCount(bucket.count, multiplier);
    bucket.distinct = std::min(
        bucket.count, ScaleCount(bucket.distinct, distinct_multiplier));
  }
  const auto scale_frequencies = [&](std::vector<ValueFrequency>& values) {
    for (ValueFrequency& value : values) {
      value.count = ScaleCount(value.count, multiplier);
    }
  };
  scale_frequencies(lowest_values_);
  scale_frequencies(highest_values_);
  scale_frequencies(most_common_values_);
  return *this;
}

void ColumnStats::Duplicate(size_t multiplier) {
  non_null_count_ = ScaleCount(non_null_count_, multiplier);
  null_count_ = ScaleCount(null_count_, multiplier);
  for (HistogramBucket& bucket : histogram_) {
    bucket.count = ScaleCount(bucket.count, multiplier);
  }
  const auto multiply_frequencies = [&](std::vector<ValueFrequency>& values) {
    for (ValueFrequency& value : values) {
      value.count = ScaleCount(value.count, multiplier);
    }
  };
  multiply_frequencies(lowest_values_);
  multiply_frequencies(highest_values_);
  multiply_frequencies(most_common_values_);
}

TableStatistics::TableStatistics(const Schema& schema) {
  stats_.reserve(schema.ColumnCount());
  for (size_t i = 0; i < schema.ColumnCount(); ++i) {
    stats_.emplace_back(schema.GetColumn(i).Type());
  }
}

Status TableStatistics::Update(Transaction& txn, const Table& target) {
  const Schema& schema = target.GetSchema();
  std::vector<ColumnCollector> collectors;
  collectors.reserve(schema.ColumnCount());
  for (size_t i = 0; i < schema.ColumnCount(); ++i) {
    collectors.emplace_back(schema.GetColumn(i).Type());
  }

  // Collect into locals first: if a row trips a type-mismatch throw, the
  // previous statistics stay intact instead of being half-cleared.
  size_t scanned_rows = 0;
  for (Iterator iterator = target.BeginFullScan(txn); iterator.IsValid();
       ++iterator) {
    const Row& row = *iterator;
    ++scanned_rows;
    for (size_t i = 0; i < collectors.size(); ++i) {
      collectors[i].Add(row[i]);
    }
  }

  std::vector<ColumnStats> rebuilt;
  rebuilt.reserve(collectors.size());
  for (size_t i = 0; i < collectors.size(); ++i) {
    CollectedColumn collected = collectors[i].Finish();
    ColumnStats stats(schema.GetColumn(i).Type());
    stats.non_null_count_ = collected.non_null_count;
    stats.null_count_ = collected.null_count;
    stats.distinct_count_ = collected.distinct_count;
    stats.histogram_ = std::move(collected.histogram);
    stats.lowest_values_ = std::move(collected.lowest);
    stats.highest_values_ = std::move(collected.highest);
    stats.most_common_values_ = std::move(collected.most_common);
    rebuilt.push_back(std::move(stats));
  }
  row_count_ = scanned_rows;
  stats_ = std::move(rebuilt);
  return Status::kSuccess;
}

double TableStatistics::EstimateSelectivity(const Schema& schema,
                                            const Expression& predicate) const {
  if (row_count_ == 0) {
    return 0;
  }
  return ClampProbability(EstimatePredicate(*this, schema, predicate));
}

double TableStatistics::ReductionFactor(const Schema& schema,
                                        const Expression& predicate) const {
  const double selectivity = EstimateSelectivity(schema, predicate);
  if (selectivity <= 0) {
    return std::numeric_limits<double>::max();
  }
  return std::max(1.0, 1.0 / selectivity);
}

double TableStatistics::EstimateCount(int column_index, const Value& from,
                                      const Value& to) const {
  if (column_index < 0 || static_cast<size_t>(column_index) >= stats_.size()) {
    throw std::out_of_range("statistics column index");
  }
  std::optional<Value> lower = CoerceValue(from, stats_[column_index].Type());
  std::optional<Value> upper = CoerceValue(to, stats_[column_index].Type());
  if (!lower || !upper) {
    return 0;
  }
  if (*upper < *lower) {
    std::swap(lower, upper);
  }
  return stats_[column_index].EstimateRange(lower, true, upper, true);
}

TableStatistics TableStatistics::TransformBy(int column_index,
                                             const Value& from,
                                             const Value& to) const {
  TableStatistics result(*this);
  const double estimated = EstimateCount(column_index, from, to);
  const double multiplier = row_count_ == 0 ? 0 : estimated / row_count_;
  result.row_count_ = ScaleCount(row_count_, multiplier);
  for (ColumnStats& stats : result.stats_) {
    stats *= multiplier;
  }
  return result;
}

TableStatistics TableStatistics::Filter(const Schema& schema,
                                        const Expression& predicate) const {
  TableStatistics result(*this);
  const double multiplier = EstimateSelectivity(schema, predicate);
  result.row_count_ = ScaleCount(row_count_, multiplier);
  for (ColumnStats& stats : result.stats_) {
    stats *= multiplier;
  }
  return result;
}

TableStatistics TableStatistics::ScaleToRows(size_t rows) const {
  TableStatistics result(*this);
  const double multiplier =
      row_count_ == 0 ? 0 : rows / static_cast<double>(row_count_);
  result.row_count_ = rows;
  for (ColumnStats& stats : result.stats_) {
    stats *= multiplier;
  }
  return result;
}

void TableStatistics::Concat(const TableStatistics& rhs) {
  if (stats_.empty()) {
    row_count_ = rhs.row_count_;
  }
  stats_.insert(stats_.end(), rhs.stats_.begin(), rhs.stats_.end());
}

void TableStatistics::Assign(size_t rows, std::vector<ColumnStats> columns) {
  row_count_ = rows;
  stats_ = std::move(columns);
}

TableStatistics TableStatistics::operator*(size_t multiplier) const {
  TableStatistics result(*this);
  result.row_count_ = ScaleCount(result.row_count_, multiplier);
  for (ColumnStats& stats : result.stats_) {
    stats.Duplicate(multiplier);
  }
  return result;
}

Encoder& operator<<(Encoder& encoder, const ValueFrequency& frequency) {
  encoder << frequency.value << static_cast<uint64_t>(frequency.count);
  return encoder;
}

Decoder& operator>>(Decoder& decoder, ValueFrequency& frequency) {
  uint64_t count = 0;
  decoder >> frequency.value >> count;
  frequency.count = count;
  return decoder;
}

Encoder& operator<<(Encoder& encoder, const HistogramBucket& bucket) {
  encoder << bucket.lower << bucket.upper << static_cast<uint64_t>(bucket.count)
          << static_cast<uint64_t>(bucket.distinct);
  return encoder;
}

Decoder& operator>>(Decoder& decoder, HistogramBucket& bucket) {
  uint64_t count = 0;
  uint64_t distinct = 0;
  decoder >> bucket.lower >> bucket.upper >> count >> distinct;
  bucket.count = count;
  bucket.distinct = distinct;
  return decoder;
}

Encoder& operator<<(Encoder& encoder, const ColumnStats& stats) {
  encoder << stats.type_ << static_cast<uint64_t>(stats.non_null_count_)
          << static_cast<uint64_t>(stats.null_count_)
          << static_cast<uint64_t>(stats.distinct_count_) << stats.histogram_
          << stats.lowest_values_ << stats.highest_values_
          << stats.most_common_values_;
  return encoder;
}

Decoder& operator>>(Decoder& decoder, ColumnStats& stats) {
  uint64_t non_null = 0;
  uint64_t nulls = 0;
  uint64_t distinct = 0;
  decoder >> stats.type_ >> non_null >> nulls >> distinct >> stats.histogram_ >>
      stats.lowest_values_ >> stats.highest_values_ >>
      stats.most_common_values_;
  stats.non_null_count_ = non_null;
  stats.null_count_ = nulls;
  stats.distinct_count_ = distinct;
  return decoder;
}

Encoder& operator<<(Encoder& encoder, const TableStatistics& stats) {
  encoder << kStatisticsMagic << kStatisticsVersion
          << static_cast<uint64_t>(stats.row_count_) << stats.stats_;
  return encoder;
}

Decoder& operator>>(Decoder& decoder, TableStatistics& stats) {
  uint64_t marker = 0;
  decoder >> marker;
  if (marker == kStatisticsMagic) {
    uint64_t version = 0;
    uint64_t rows = 0;
    decoder >> version;
    if (version != kStatisticsVersion) {
      throw std::runtime_error("unsupported table statistics version");
    }
    decoder >> rows >> stats.stats_;
    stats.row_count_ = rows;
    return decoder;
  }

  // Legacy format started directly with vector<ColumnStats>::size().
  constexpr uint64_t kMaxLegacyColumnCount = 4096;
  if (marker > kMaxLegacyColumnCount) {
    // Corrupt/fuzzed input must not turn the marker into an allocation bomb.
    throw std::runtime_error("corrupt legacy table statistics header");
  }
  stats.stats_.clear();
  stats.stats_.resize(marker);
  stats.row_count_ = 0;
  for (ColumnStats& column : stats.stats_) {
    decoder >> column.type_;
    uint64_t count = 0;
    uint64_t distinct = 0;
    switch (column.type_) {
      case ValueType::kInt64: {
        int64_t maximum = 0;
        int64_t minimum = 0;
        decoder >> maximum >> minimum >> count >> distinct;
        if (count > 0) {
          column.histogram_.push_back(HistogramBucket{.lower = Value(minimum),
                                                      .upper = Value(maximum),
                                                      .count = count,
                                                      .distinct = distinct});
        }
        break;
      }
      case ValueType::kDate: {
        int64_t maximum = 0;
        int64_t minimum = 0;
        decoder >> maximum >> minimum >> count >> distinct;
        if (count > 0) {
          column.histogram_.push_back(
              HistogramBucket{.lower = Value::DateFromDays(minimum),
                              .upper = Value::DateFromDays(maximum),
                              .count = count,
                              .distinct = distinct});
        }
        break;
      }
      case ValueType::kDouble: {
        double maximum = 0;
        double minimum = 0;
        decoder >> maximum >> minimum >> count >> distinct;
        if (count > 0) {
          column.histogram_.push_back(HistogramBucket{.lower = Value(minimum),
                                                      .upper = Value(maximum),
                                                      .count = count,
                                                      .distinct = distinct});
        }
        break;
      }
      case ValueType::kVarChar:
      case ValueType::kArray:
        decoder >> count >> distinct;
        break;
      case ValueType::kNull:
        throw std::runtime_error("invalid legacy column statistics");
    }
    column.non_null_count_ = count;
    column.distinct_count_ = distinct;
    stats.row_count_ = std::max(stats.row_count_, column.non_null_count_);
  }
  return decoder;
}

std::ostream& operator<<(std::ostream& out, const ColumnStats& stats) {
  out << "Type: " << static_cast<int>(stats.type_)
      << " NonNull: " << stats.non_null_count_ << " Null: " << stats.null_count_
      << " Distinct: " << stats.distinct_count_ << " Histogram: [";
  for (size_t i = 0; i < stats.histogram_.size(); ++i) {
    if (i > 0) {
      out << ", ";
    }
    const HistogramBucket& bucket = stats.histogram_[i];
    out << bucket.lower << ".." << bucket.upper << ":" << bucket.count << "/"
        << bucket.distinct;
  }
  out << "]";
  return out;
}

std::ostream& operator<<(std::ostream& out, const TableStatistics& stats) {
  out << "Rows: " << stats.row_count_ << "\n";
  for (const ColumnStats& column : stats.stats_) {
    out << column << "\n";
  }
  return out;
}

}  // namespace tinylamb
