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
#include "common/platform/random.hpp"
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

// TLSTATS + v3. v3 appends the table's row-layout generation (the clustering
// proof stamp); v2 payloads are still readable and simply carry no proof.
constexpr uint64_t kStatisticsMagic = 0x544C535441545302ULL;
constexpr uint64_t kStatisticsVersion = 3;
constexpr uint64_t kStatisticsVersionWithoutProof = 2;
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
  // Fraction of adjacent non-NULL pairs in scan (physical) order that are
  // non-decreasing. 1.0 for sorted input, ~0.5 for random input.
  double correlation{1.0};
};

bool SameValue(const Value& left, const Value& right) {
  return left.type == right.type && left == right;
}

size_t ScaleCount(size_t count, double multiplier) {
  if (count == 0 || multiplier <= 0) {
    return 0;
  }
  const long double scaled = static_cast<long double>(count) * multiplier;
  if (scaled >= static_cast<long double>(std::numeric_limits<size_t>::max())) {
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
    CHECK_MSG(value.type == type_, "column statistics type mismatch");
    ++non_null_count_;
    // Only oversized VARCHAR cells need the truncating copy; everything
    // else is tracked by reference, saving one full Value copy (and a
    // string materialization for VARCHAR) per column per scanned row.
    Value compacted;
    const bool oversized =
        value.type == ValueType::kVarChar &&
        value.value.varchar_value.size() > kStoredStringPrefixLength;
    if (oversized) {
      compacted = CompactValue(value);
    }
    const Value& tracked = oversized ? compacted : value;
    TrackOrder(tracked);
    TrackLowest(tracked);
    TrackHighest(tracked);
    Sample(tracked);
  }

  [[nodiscard]] CollectedColumn Finish() const {
    CollectedColumn result;
    result.null_count = null_count_;
    result.non_null_count = non_null_count_;
    if (total_pairs_ > 0) {
      result.correlation = std::clamp(static_cast<double>(ordered_pairs_) /
                                          static_cast<double>(total_pairs_),
                                      0.0, 1.0);
    }

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
    size_t doubletons = 0;
    for (const ValueFrequency& frequency : runs) {
      if (frequency.count == 1) {
        ++singletons;
      } else if (frequency.count == 2) {
        ++doubletons;
      }
    }
    auto estimate = static_cast<double>(runs.size());
    if (singletons > 0 && singletons < sampled) {
      const double coverage = 1.0 - (static_cast<double>(singletons) /
                                     static_cast<double>(sampled));
      if (coverage >= 0.05) {
        estimate = static_cast<double>(runs.size()) / coverage;
      } else {
        // High-distinctness columns leave the sample nearly all singletons
        // (a 4096-row sample over 200k distinct values sees ~98% of them
        // once), so the coverage ratio degenerates.  Claiming the full
        // population here fabricated NDV = row count for exactly the FK
        // columns join costing multiplies together, collapsing composite-key
        // estimates by orders of magnitude.  Chao1 (S_obs + f1^2 / 2·f2,
        // bias-corrected without doubletons) tracks those populations from
        // the singleton/doubleton shape; cap at the observed count so a
        // degenerate sample can never exceed the population.
        const auto observed = static_cast<double>(runs.size());
        const auto f1 = static_cast<double>(singletons);
        estimate = doubletons > 0
                       ? observed + ((f1 * f1) /
                                     (2.0 * static_cast<double>(doubletons)))
                       : observed + ((f1 * (f1 - 1.0)) / 2.0);
      }
    }
    const double capped = std::clamp(estimate, static_cast<double>(runs.size()),
                                     static_cast<double>(non_null_count_));
    return static_cast<size_t>(std::llround(capped));
  }

  void TrackOrder(const Value& value) {
    if (has_prev_) {
      ++total_pairs_;
      if (!(value < prev_)) {
        ++ordered_pairs_;
      }
    }
    prev_ = value;
    has_prev_ = true;
  }

  void TrackLowest(const Value& value) {
    // Fast reject for orderable scalars: with a full sorted window, a value
    // above the largest of the k lowest can neither displace them nor match
    // one (equality follows the same order).  Interval-shaped varchars can
    // compare unequal byte-wise yet be equal as intervals, so they keep the
    // linear scan.
    if (lowest_.size() == kBoundaryValueCount && lowest_.back().value < value &&
        (value.type == ValueType::kInt64 || value.type == ValueType::kDouble ||
         value.type == ValueType::kDate)) {
      return;
    }
    for (size_t i = 0; i < lowest_.size(); ++i) {
      if (SameValue(lowest_[i].value, value)) {
        ++lowest_[i].count;
        return;
      }
      if (value < lowest_[i].value) {
        lowest_.insert(lowest_.begin() + static_cast<std::ptrdiff_t>(i),
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
    if (highest_.size() == kBoundaryValueCount &&
        value < highest_.back().value &&
        (value.type == ValueType::kInt64 || value.type == ValueType::kDouble ||
         value.type == ValueType::kDate)) {
      return;
    }
    for (size_t i = 0; i < highest_.size(); ++i) {
      if (SameValue(highest_[i].value, value)) {
        ++highest_[i].count;
        return;
      }
      if (highest_[i].value < value) {
        highest_.insert(highest_.begin() + static_cast<std::ptrdiff_t>(i),
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
    // Portable draw: statistics (and thus plans) must not depend on the
    // standard library's distribution algorithm.
    const auto slot =
        platform::UniformInt<uint64_t>(rng_, 0, non_null_count_ - 1);
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
  // Physical-order adjacency tracking for the clustering correlation
  // (TODO.md item 1a). NULLs neither start nor break a run: they are skipped
  // so the fraction still reflects the order of stored values.
  Value prev_;
  bool has_prev_{false};
  size_t ordered_pairs_{0};
  size_t total_pairs_{0};
  // Deterministic on purpose: reproducible samples keep query plans and
  // statistics tests stable across runs. Not used for any security purpose.
  std::mt19937_64 rng_{// NOLINT(cert-msc32-c,cert-msc51-cpp)
                       kStatisticsMagic ^ 0x9E3779B97F4A7C15ULL};
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

// ResolveColumn for a statistics lookup: an offset past the statistics'
// column vector (a derived plan whose stats are narrower than its schema)
// is treated as unresolved instead of indexing out of range.
int ResolveStatsColumn(const TableStatistics& table, const Schema& schema,
                       const ColumnName& column) {
  const int offset = ResolveColumn(schema, column);
  if (offset < 0 || static_cast<size_t>(offset) >= table.Columns()) {
    return -1;
  }
  return offset;
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

  // An unmeasured column knows nothing: not that the value is absent, not
  // that half the column matches a range. Neutral defaults keep the plan's
  // row estimates alive instead of collapsing them to a proven-empty zero.
  if (stats.Unmeasured()) {
    switch (operation) {
      case BinaryOperation::kEquals:
      case BinaryOperation::kNotEquals: {
        const double equal =
            std::min(0.25, 1.0 / std::sqrt(static_cast<double>(rows)));
        return operation == BinaryOperation::kEquals ? equal : 1 - equal;
      }
      case BinaryOperation::kLessThan:
      case BinaryOperation::kLessThanEquals:
      case BinaryOperation::kGreaterThan:
      case BinaryOperation::kGreaterThanEquals:
        return 1.0 / 3.0;
      case BinaryOperation::kLike:
      case BinaryOperation::kNotLike: {
        const double matching =
            std::min(0.25, 1.0 / std::sqrt(static_cast<double>(rows)));
        return operation == BinaryOperation::kLike ? matching : 1 - matching;
      }
      default:
        return 1;
    }
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
          stats.Distinct() == 0
              ? 0
              : 1.0 / std::sqrt(static_cast<double>(stats.Distinct()));
      const double matching =
          static_cast<double>(stats.NonNullCount()) * std::min(0.25, base);
      count = operation == BinaryOperation::kLike
                  ? matching
                  : static_cast<double>(stats.NonNullCount()) - matching;
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
    StatusOr<Value> value = predicate->TryEvaluate(Row(), Schema());
    if (!value.HasValue()) {
      return 1;
    }
    if (value.Value().IsNull()) {
      return 0;
    }
    return value.Value().Truthy() ? 1 : 0;
  }

  if (predicate->Type() == TypeTag::kUnaryExp) {
    const auto& unary = predicate->AsUnaryExpression();
    if (unary.Op() == UnaryOperation::kNot) {
      return 1 - EstimatePredicate(table, schema, unary.Child());
    }
    if ((unary.Op() == UnaryOperation::kIsNull ||
         unary.Op() == UnaryOperation::kIsNotNull) &&
        unary.Child()->Type() == TypeTag::kColumnValue) {
      const int offset = ResolveStatsColumn(
          table, schema, unary.Child()->AsColumnValue().GetColumnName());
      if (offset < 0 || table.Rows() == 0) {
        return 0;
      }
      const ColumnStats& stats = table.Column(static_cast<size_t>(offset));
      // An unmeasured column has no null mass to report either way; the
      // neutral 0.1 keeps a NULL-filtering join rewriter from "proving" the
      // predicate eliminates everything.
      if (stats.Unmeasured()) {
        return unary.Op() == UnaryOperation::kIsNull ? 0.1 : 0.9;
      }
      const double null_fraction = static_cast<double>(stats.NullCount()) /
                                   static_cast<double>(table.Rows());
      return unary.Op() == UnaryOperation::kIsNull ? null_fraction
                                                   : 1 - null_fraction;
    }
  }

  if (predicate->Type() == TypeTag::kInExp) {
    const auto& in = predicate->AsInExpression();
    if (in.child_->Type() != TypeTag::kColumnValue) {
      return 0.25;
    }
    const int offset = ResolveStatsColumn(
        table, schema, in.child_->AsColumnValue().GetColumnName());
    if (offset < 0) {
      return 0.25;
    }
    double selectivity = 0;
    for (const Expression& item : in.list_) {
      if (item->Type() != TypeTag::kConstantValue) {
        return 0.25;
      }
      selectivity += ColumnConstantSelectivity(
          table.Column(static_cast<size_t>(offset)), table.Rows(),
          BinaryOperation::kEquals, item->AsConstantValue().GetValue());
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
    const int offset = ResolveStatsColumn(
        table, schema, binary.Left()->AsColumnValue().GetColumnName());
    if (offset < 0) {
      return 0.25;
    }
    return ColumnConstantSelectivity(
        table.Column(static_cast<size_t>(offset)), table.Rows(), binary.Op(),
        binary.Right()->AsConstantValue().GetValue());
  }
  if (left_constant && right_column) {
    const int offset = ResolveStatsColumn(
        table, schema, binary.Right()->AsColumnValue().GetColumnName());
    if (offset < 0) {
      return 0.25;
    }
    return ColumnConstantSelectivity(
        table.Column(static_cast<size_t>(offset)), table.Rows(),
        ReverseComparison(binary.Op()),
        binary.Left()->AsConstantValue().GetValue());
  }
  if (left_column && right_column && binary.Op() == BinaryOperation::kEquals) {
    const int left_offset = ResolveStatsColumn(
        table, schema, binary.Left()->AsColumnValue().GetColumnName());
    const int right_offset = ResolveStatsColumn(
        table, schema, binary.Right()->AsColumnValue().GetColumnName());
    if (left_offset < 0 || right_offset < 0 || table.Rows() == 0) {
      return 0.1;
    }
    const ColumnStats& left = table.Column(static_cast<size_t>(left_offset));
    const ColumnStats& right = table.Column(static_cast<size_t>(right_offset));
    if (left_offset == right_offset) {
      return static_cast<double>(left.NonNullCount()) /
             static_cast<double>(table.Rows());
    }
    const size_t max_distinct = std::max(left.Distinct(), right.Distinct());
    if (max_distinct == 0) {
      // Unmeasured columns (a computed projection column, a stats-less
      // schema): unknown distribution, not a proven non-match.
      if (left.Unmeasured() || right.Unmeasured()) {
        return 0.1;
      }
      return 0;
    }
    const double both_non_null = (static_cast<double>(left.NonNullCount()) /
                                  static_cast<double>(table.Rows())) *
                                 (static_cast<double>(right.NonNullCount()) /
                                  static_cast<double>(table.Rows()));
    return ClampProbability(both_non_null / static_cast<double>(max_distinct));
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
      return static_cast<double>(frequency.count);
    }
  }
  for (const ValueFrequency& frequency : lowest_values_) {
    if (SameValue(frequency.value, *value)) {
      return static_cast<double>(frequency.count);
    }
  }
  for (const ValueFrequency& frequency : highest_values_) {
    if (SameValue(frequency.value, *value)) {
      return static_cast<double>(frequency.count);
    }
  }
  for (const HistogramBucket& bucket : histogram_) {
    if (*value < bucket.lower || bucket.upper < *value) {
      continue;
    }
    if (bucket.distinct == 0) {
      return 0;
    }
    return static_cast<double>(bucket.count) /
           static_cast<double>(bucket.distinct);
  }
  // Legacy varchar statistics do not have persisted boundaries.
  if (histogram_.empty()) {
    return static_cast<double>(non_null_count_) /
           static_cast<double>(distinct_count_);
  }
  return 0;
}

double ColumnStats::ClusteringStrength() const {
  return std::clamp(std::abs((2.0 * correlation_) - 1.0), 0.0, 1.0);
}

ColumnStats ColumnStats::UntransformedPrior() const {
  ColumnStats prior(*this);
  prior.histogram_.clear();
  prior.lowest_values_.clear();
  prior.highest_values_.clear();
  prior.most_common_values_.clear();
  return prior;
}

double ColumnStats::EstimateJoinMatches(const ColumnStats& other,
                                        bool null_safe) const {
  const double cross = static_cast<double>(non_null_count_) *
                       static_cast<double>(other.non_null_count_);
  const double null_pairs = null_safe
                                ? static_cast<double>(null_count_) *
                                      static_cast<double>(other.null_count_)
                                : 0.0;
  if (cross == 0.0) {
    return null_pairs;
  }
  // Different domains need coercion-aware costing; retain the NDV fallback.
  if (type_ != other.type_) {
    const auto widest = static_cast<double>(
        std::max<size_t>({1, distinct_count_, other.distinct_count_}));
    return (cross / widest) + null_pairs;
  }
  if (!histogram_.empty() && !other.histogram_.empty() &&
      (histogram_.back().upper < other.histogram_.front().lower ||
       other.histogram_.back().upper < histogram_.front().lower)) {
    return null_pairs;
  }
  std::vector<Value> tracked;
  const auto collect = [&](const std::vector<ValueFrequency>& values) {
    for (const auto& entry : values) {
      if (std::ranges::none_of(tracked, [&](const Value& value) {
            return SameValue(value, entry.value);
          })) {
        tracked.push_back(entry.value);
      }
    }
  };
  collect(most_common_values_);
  collect(other.most_common_values_);
  auto left_mass = static_cast<double>(non_null_count_);
  auto right_mass = static_cast<double>(other.non_null_count_);
  auto left_ndv = static_cast<double>(distinct_count_);
  auto right_ndv = static_cast<double>(other.distinct_count_);
  double pairs = 0.0;
  for (const Value& value : tracked) {
    const double l = std::min(left_mass, EstimateEqual(value));
    const double r = std::min(right_mass, other.EstimateEqual(value));
    pairs += l * r;
    left_mass -= l;
    right_mass -= r;
    left_ndv -= l > 0.0 ? 1.0 : 0.0;
    right_ndv -= r > 0.0 ? 1.0 : 0.0;
  }
  pairs += left_mass * right_mass / std::max({1.0, left_ndv, right_ndv});
  return std::clamp(pairs, 0.0, cross) + null_pairs;
}

double TableStatistics::EstimateDistinctRows(
    const std::vector<size_t>& columns) const {
  if (row_count_ == 0) {
    return 0.0;
  }
  if (columns.empty()) {
    return 1.0;
  }
  std::vector<size_t> seen;
  std::vector<double> counts;
  const auto rows = static_cast<double>(row_count_);
  for (size_t column : columns) {
    if (column >= stats_.size()) {
      return rows;
    }
    if (std::ranges::find(seen, column) != seen.end()) {
      continue;
    }
    seen.push_back(column);
    const auto& stats = stats_[column];
    const double ndv = static_cast<double>(stats.Distinct()) +
                       (stats.NullCount() > 0 ? 1.0 : 0.0);
    if (ndv == 0.0) {
      return rows;
    }
    counts.push_back(std::clamp(ndv, 1.0, rows));
  }
  std::ranges::sort(counts, std::greater<>());
  double estimate = 1.0, weight = 1.0;
  for (double ndv : counts) {
    estimate *= std::pow(ndv, weight);
    if (estimate >= rows) {
      return rows;
    }
    weight *= 0.5;
  }
  return estimate;
}

double ColumnStats::EstimateLessThan(const Value& raw_value) const {
  std::optional<Value> value = CoerceValue(raw_value, type_);
  if (!value || non_null_count_ == 0) {
    return 0;
  }
  *value = CompactValue(*value);
  // When the tracked value lists account for every non-NULL row (the
  // small-relation case: ANALYZE sampled the whole table, or a low-cardinality
  // column whose MCV list is uncapped), their exact counts answer range
  // questions precisely; interpolating a histogram bucket instead can
  // over- or under-estimate by whole rows and flip an index-vs-full-scan
  // decision. Partial coverage (large relations) falls through to the
  // histogram below. MCV, lowest and highest overlap on small tables, so
  // the union is deduplicated before the totals are compared.
  std::vector<const ValueFrequency*> tracked;
  tracked.reserve(most_common_values_.size() + lowest_values_.size() +
                  highest_values_.size());
  auto track = [&](const std::vector<ValueFrequency>& list) {
    for (const ValueFrequency& frequency : list) {
      bool seen = false;
      for (const ValueFrequency* kept : tracked) {
        if (SameValue(kept->value, frequency.value)) {
          seen = true;
          break;
        }
      }
      if (!seen) {
        tracked.push_back(&frequency);
      }
    }
  };
  track(most_common_values_);
  track(lowest_values_);
  track(highest_values_);
  double tracked_total = 0;
  for (const ValueFrequency* frequency : tracked) {
    tracked_total += static_cast<double>(frequency->count);
  }
  if (!tracked.empty() &&
      tracked_total >= static_cast<double>(non_null_count_)) {
    double exact = 0;
    for (const ValueFrequency* frequency : tracked) {
      if (frequency->value < *value) {
        exact += static_cast<double>(frequency->count);
      }
    }
    return std::clamp(exact, 0.0, static_cast<double>(non_null_count_));
  }
  if (histogram_.empty()) {
    return static_cast<double>(non_null_count_) * 0.5;
  }

  double result = 0;
  for (const HistogramBucket& bucket : histogram_) {
    if (*value <= bucket.lower) {
      break;
    }
    if (bucket.upper < *value) {
      result += static_cast<double>(bucket.count);
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
    result += static_cast<double>(bucket.count) * fraction;
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
  non_null_count_ =
      ScaleCount(non_null_count_, static_cast<double>(multiplier));
  null_count_ = ScaleCount(null_count_, static_cast<double>(multiplier));
  for (HistogramBucket& bucket : histogram_) {
    bucket.count = ScaleCount(bucket.count, static_cast<double>(multiplier));
  }
  const auto multiply_frequencies = [&](std::vector<ValueFrequency>& values) {
    for (ValueFrequency& value : values) {
      value.count = ScaleCount(value.count, static_cast<double>(multiplier));
    }
  };
  multiply_frequencies(lowest_values_);
  multiply_frequencies(highest_values_);
  multiply_frequencies(most_common_values_);
}

TableStatistics::TableStatistics(const Schema& schema) : row_count_(0) {
  stats_.reserve(schema.ColumnCount());
  for (size_t i = 0; i < schema.ColumnCount(); ++i) {
    stats_.emplace_back(schema.GetColumn(i).Type());
  }
}

Status TableStatistics::Update(Transaction& txn, const Table& target) {
  const Schema& schema = target.GetSchema();
  // The generation window brackets the whole scan: a writer that enters or
  // leaves Table::Insert/Update/Delete while this loop runs moves the counter
  // away from `generation`, and a row that committed after the scan's snapshot
  // would be visible to a later reader without having been measured here.
  const uint64_t generation = target.LayoutGeneration();
  std::vector<ColumnCollector> collectors;
  collectors.reserve(schema.ColumnCount());
  for (size_t i = 0; i < schema.ColumnCount(); ++i) {
    collectors.emplace_back(schema.GetColumn(i).Type());
  }

  // Collect into locals first: a row that trips the type-mismatch CHECK
  // aborts the process anyway, but any other error keeps the previous
  // statistics intact instead of half-clearing them.
  size_t scanned_rows = 0;
  Iterator iterator = target.BeginFullScan(txn);
  for (; iterator.IsValid(); ++iterator) {
    const Row& row = *iterator;
    ++scanned_rows;
    for (size_t i = 0; i < collectors.size(); ++i) {
      collectors[i].Add(row[i]);
    }
  }
  // A scan that stopped on an error would otherwise persist partial counts
  // as if they were complete.
  RETURN_IF_FAIL(iterator.GetStatus());
  const bool stable =
      generation != 0 && target.LayoutGeneration() == generation;

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
    stats.correlation_ = collected.correlation;
    rebuilt.push_back(std::move(stats));
  }
  row_count_ = scanned_rows;
  stats_ = std::move(rebuilt);
  layout_generation_ = stable ? generation : 0;
  if (stable && !target.RowsRewrittenInPlace()) {
    std::ignore = target.RebuildLSTableStore(txn);
  }
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
  const bool index_valid =
      column_index >= 0 && static_cast<size_t>(column_index) < stats_.size();
  CHECK_MSG(index_valid, "statistics column index");
  if (!index_valid) {
    return 0;
  }
  const ColumnStats& column_stats = stats_[static_cast<size_t>(column_index)];
  std::optional<Value> lower;
  std::optional<Value> upper;
  if (!from.IsNull()) {
    lower = CoerceValue(from, column_stats.Type());
    if (!lower) {
      return 0;
    }
  }
  if (!to.IsNull()) {
    upper = CoerceValue(to, column_stats.Type());
    if (!upper) {
      return 0;
    }
  }
  if (!lower && !upper) {
    return static_cast<double>(row_count_);
  }
  if (column_stats.Unmeasured()) {
    // No measured distribution: a neutral third of the table instead of a
    // "proven empty" zero that would make every range access look free.
    return static_cast<double>(row_count_) / 3.0;
  }
  if (lower && upper && *upper < *lower) {
    // An inverted range (e.g. BETWEEN 9 AND 3) is legitimately empty: report
    // zero instead of swapping, matching ColumnStats::EstimateRange and the
    // scan iterators (which yield no rows for such bounds).
    return 0;
  }
  return column_stats.EstimateRange(lower, true, upper, true);
}

TableStatistics TableStatistics::TransformBy(int column_index,
                                             const Value& from,
                                             const Value& to) const {
  TableStatistics result(*this);
  const double estimated = EstimateCount(column_index, from, to);
  const double multiplier =
      row_count_ == 0 ? 0 : estimated / static_cast<double>(row_count_);
  result.row_count_ = ScaleCount(row_count_, multiplier);
  for (ColumnStats& stats : result.stats_) {
    stats *= multiplier;
  }
  // A rewritten column no longer describes the stored values, so the physical
  // clustering proof cannot travel with it.
  result.layout_generation_ = 0;
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
  // The estimate is a projection of this table, not the table itself: a
  // clustering claim must be re-derived from a fresh measurement.
  result.layout_generation_ = 0;
  return result;
}

TableStatistics TableStatistics::ScaleToRows(size_t rows) const {
  TableStatistics result(*this);
  const double multiplier =
      row_count_ == 0
          ? 0
          : static_cast<double>(rows) / static_cast<double>(row_count_);
  result.row_count_ = rows;
  for (ColumnStats& stats : result.stats_) {
    stats *= multiplier;
  }
  result.layout_generation_ = 0;
  return result;
}

TableStatistics TableStatistics::ScaleCardinality(size_t rows) const {
  TableStatistics result(Schema("", {}));
  result.row_count_ = rows;
  const double multiplier =
      row_count_ == 0
          ? 0
          : static_cast<double>(rows) / static_cast<double>(row_count_);
  const double distinct_multiplier =
      multiplier < 1 ? std::sqrt(std::max(0.0, multiplier)) : 1;
  result.stats_.reserve(stats_.size());
  for (const ColumnStats& stats : stats_) {
    ColumnStats scaled;
    scaled.type_ = stats.type_;
    scaled.non_null_count_ = ScaleCount(stats.non_null_count_, multiplier);
    scaled.null_count_ = ScaleCount(stats.null_count_, multiplier);
    scaled.distinct_count_ =
        std::min(scaled.non_null_count_,
                 ScaleCount(stats.distinct_count_, distinct_multiplier));
    scaled.correlation_ = stats.correlation_;
    result.stats_.push_back(std::move(scaled));
  }
  return result;
}

void TableStatistics::Concat(const TableStatistics& rhs) {
  if (stats_.empty()) {
    row_count_ = rhs.row_count_;
  }
  stats_.insert(stats_.end(), rhs.stats_.begin(), rhs.stats_.end());
  // Two row sources are not one table's chain: no clustering proof survives.
  layout_generation_ = 0;
}

void TableStatistics::Assign(size_t rows, std::vector<ColumnStats> columns,
                             uint64_t layout_generation) {
  row_count_ = rows;
  stats_ = std::move(columns);
  layout_generation_ = layout_generation;
}

TableStatistics TableStatistics::operator*(size_t multiplier) const {
  TableStatistics result(*this);
  result.row_count_ =
      ScaleCount(result.row_count_, static_cast<double>(multiplier));
  for (ColumnStats& stats : result.stats_) {
    stats.Duplicate(multiplier);
  }
  result.layout_generation_ = 0;
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
          << stats.most_common_values_ << stats.correlation_;
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
  // Payloads written before the correlation field carry no trailing bytes;
  // keep them readable with the neutral (fully clustered) default.
  stats.correlation_ = 1.0;
  if (!decoder.AtEnd()) {
    double correlation = 1.0;
    decoder >> correlation;
    if (!decoder.Failed()) {
      stats.correlation_ = std::clamp(correlation, 0.0, 1.0);
    }
  }
  return decoder;
}

Encoder& operator<<(Encoder& encoder, const TableStatistics& stats) {
  encoder << kStatisticsMagic << kStatisticsVersion
          << static_cast<uint64_t>(stats.row_count_) << stats.stats_
          << stats.layout_generation_;
  return encoder;
}

Decoder& operator>>(Decoder& decoder, TableStatistics& stats) {
  uint64_t marker = 0;
  decoder >> marker;
  if (marker == kStatisticsMagic) {
    uint64_t version = 0;
    uint64_t rows = 0;
    decoder >> version;
    if (version != kStatisticsVersion &&
        version != kStatisticsVersionWithoutProof) {
      decoder.Fail();
      return decoder;
    }
    decoder >> rows >> stats.stats_;
    stats.row_count_ = rows;
    // A v2 record predates the proof stamp; it describes whatever the chain
    // looked like in an earlier process, so it certifies nothing.
    stats.layout_generation_ = 0;
    if (version == kStatisticsVersion) {
      uint64_t generation = 0;
      decoder >> generation;
      if (!decoder.Failed()) {
        stats.layout_generation_ = generation;
      }
    }
    return decoder;
  }

  // Legacy format started directly with vector<ColumnStats>::size().
  constexpr uint64_t kMaxLegacyColumnCount = 4096;
  if (marker > kMaxLegacyColumnCount) {
    // Corrupt/fuzzed input must not turn the marker into an allocation bomb.
    decoder.Fail();
    return decoder;
  }
  stats.stats_.clear();
  stats.stats_.resize(marker);
  stats.row_count_ = 0;
  stats.layout_generation_ = 0;
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
          column.histogram_.push_back(
              HistogramBucket{.lower = Value(minimum),
                              .upper = Value(maximum),
                              .count = static_cast<size_t>(count),
                              .distinct = static_cast<size_t>(distinct)});
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
                              .count = static_cast<size_t>(count),
                              .distinct = static_cast<size_t>(distinct)});
        }
        break;
      }
      case ValueType::kDouble: {
        double maximum = 0;
        double minimum = 0;
        decoder >> maximum >> minimum >> count >> distinct;
        if (count > 0) {
          column.histogram_.push_back(
              HistogramBucket{.lower = Value(minimum),
                              .upper = Value(maximum),
                              .count = static_cast<size_t>(count),
                              .distinct = static_cast<size_t>(distinct)});
        }
        break;
      }
      case ValueType::kVarChar:
      case ValueType::kArray:
        decoder >> count >> distinct;
        break;
      case ValueType::kNull:
        decoder.Fail();
        return decoder;
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
