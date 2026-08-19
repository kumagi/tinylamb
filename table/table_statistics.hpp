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

#ifndef TINYLAMB_TABLE_STATISTICS_HPP
#define TINYLAMB_TABLE_STATISTICS_HPP

#include <cstddef>
#include <iosfwd>
#include <optional>
#include <vector>

#include "expression/expression.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {
class Table;
class Transaction;
class Encoder;
class Decoder;
class TableStatistics;

inline constexpr size_t kHistogramBucketCount = 16;
inline constexpr size_t kBoundaryValueCount = 5;
inline constexpr size_t kMostCommonValueCount = 5;

struct ValueFrequency {
  Value value;
  size_t count{0};

  bool operator==(const ValueFrequency&) const = default;
  friend Encoder& operator<<(Encoder& encoder, const ValueFrequency& value);
  friend Decoder& operator>>(Decoder& decoder, ValueFrequency& value);
};

// Equi-depth histogram bucket. Values are inclusive at both boundaries.
struct HistogramBucket {
  Value lower;
  Value upper;
  size_t count{0};
  size_t distinct{0};

  bool operator==(const HistogramBucket&) const = default;
  friend Encoder& operator<<(Encoder& encoder, const HistogramBucket& bucket);
  friend Decoder& operator>>(Decoder& decoder, HistogramBucket& bucket);
};

class ColumnStats {
 public:
  ColumnStats() = default;
  explicit ColumnStats(ValueType type) : type_(type) {}

  [[nodiscard]] ValueType Type() const { return type_; }
  [[nodiscard]] size_t Count() const { return non_null_count_; }
  [[nodiscard]] size_t NonNullCount() const { return non_null_count_; }
  [[nodiscard]] size_t NullCount() const { return null_count_; }
  [[nodiscard]] size_t Distinct() const { return distinct_count_; }
  [[nodiscard]] const std::vector<HistogramBucket>& Histogram() const {
    return histogram_;
  }
  [[nodiscard]] const std::vector<ValueFrequency>& LowestValues() const {
    return lowest_values_;
  }
  [[nodiscard]] const std::vector<ValueFrequency>& HighestValues() const {
    return highest_values_;
  }
  [[nodiscard]] const std::vector<ValueFrequency>& MostCommonValues() const {
    return most_common_values_;
  }

  [[nodiscard]] double EstimateEqual(const Value& value) const;
  [[nodiscard]] double EstimateRange(const std::optional<Value>& lower,
                                     bool lower_inclusive,
                                     const std::optional<Value>& upper,
                                     bool upper_inclusive) const;

  ColumnStats& operator*=(double multiplier);
  bool operator==(const ColumnStats&) const = default;

  friend Encoder& operator<<(Encoder& encoder, const ColumnStats& stats);
  friend Decoder& operator>>(Decoder& decoder, ColumnStats& stats);
  friend Decoder& operator>>(Decoder& decoder, TableStatistics& stats);
  friend std::ostream& operator<<(std::ostream& out, const ColumnStats& stats);

 private:
  friend class TableStatistics;

  [[nodiscard]] double EstimateLessThan(const Value& value) const;
  void Duplicate(size_t multiplier);

  ValueType type_{ValueType::kNull};
  size_t non_null_count_{0};
  size_t null_count_{0};
  size_t distinct_count_{0};
  std::vector<HistogramBucket> histogram_;
  std::vector<ValueFrequency> lowest_values_;
  std::vector<ValueFrequency> highest_values_;
  std::vector<ValueFrequency> most_common_values_;
};

class TableStatistics {
 public:
  explicit TableStatistics(const Schema& schema);
  TableStatistics(const TableStatistics&) = default;
  TableStatistics(TableStatistics&&) = default;
  TableStatistics& operator=(const TableStatistics&) = default;
  TableStatistics& operator=(TableStatistics&&) = default;
  bool operator==(const TableStatistics&) const = default;
  ~TableStatistics() = default;

  Status Update(Transaction& txn, const Table& target);

  // Inverse selectivity kept for the existing Plan API. A return value of 10
  // means that approximately one tenth of the input rows survive.
  [[nodiscard]] double ReductionFactor(const Schema& schema,
                                       const Expression& predicate) const;
  [[nodiscard]] double EstimateSelectivity(const Schema& schema,
                                           const Expression& predicate) const;

  [[nodiscard]] size_t Rows() const { return row_count_; }
  [[nodiscard]] size_t Columns() const { return stats_.size(); }
  [[nodiscard]] const ColumnStats& Column(size_t index) const {
    return stats_.at(index);
  }

  [[nodiscard]] double EstimateCount(int column_index, const Value& from,
                                     const Value& to) const;
  [[nodiscard]] TableStatistics TransformBy(int column_index, const Value& from,
                                            const Value& to) const;
  [[nodiscard]] TableStatistics Filter(const Schema& schema,
                                       const Expression& predicate) const;
  [[nodiscard]] TableStatistics ScaleToRows(size_t rows) const;

  void Concat(const TableStatistics& rhs);
  TableStatistics operator*(size_t multiplier) const;

  friend Encoder& operator<<(Encoder& encoder, const TableStatistics& stats);
  friend Decoder& operator>>(Decoder& decoder, TableStatistics& stats);
  friend std::ostream& operator<<(std::ostream& out,
                                  const TableStatistics& stats);

 private:
  size_t row_count_{0};
  std::vector<ColumnStats> stats_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_TABLE_STATISTICS_HPP
