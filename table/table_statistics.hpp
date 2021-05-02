//
// Created by kumagi on 2022/03/06.
//

#ifndef TINYLAMB_TABLE_STATISTICS_HPP
#define TINYLAMB_TABLE_STATISTICS_HPP

#include <cmath>
#include <vector>

#include "expression/expression.hpp"
#include "type/schema.hpp"

namespace tinylamb {
class Table;
class Transaction;
class Encoder;
class ExpressionBase;

struct IntegerColumnStats {
  int64_t max;
  int64_t min;
  size_t count;
  size_t distinct;
  void Init() {
    max = std::numeric_limits<typeof(max)>::min();
    min = std::numeric_limits<typeof(max)>::max();
    count = 0;
    distinct = 0;
  }
  void Check(const Value& sample) {
    max = std::max(max, sample.value.int_value);
    min = std::min(min, sample.value.int_value);
    ++count;
  }
  IntegerColumnStats& operator*=(double multiplier) {
    count = std::floor(count * multiplier);
    distinct = std::floor(distinct * multiplier);
    return *this;
  }
  bool operator==(const IntegerColumnStats&) const = default;
  [[nodiscard]] double EstimateCount(int64_t from, int64_t to) const;
  friend Encoder& operator<<(Encoder& a, const IntegerColumnStats& sc);
  friend Decoder& operator>>(Decoder& a, IntegerColumnStats& sc);
  friend std::ostream& operator<<(std::ostream& o, const IntegerColumnStats& t);
};

struct VarcharColumnStats {
  char max[8];
  char min[8];
  size_t count;
  size_t distinct;
  void Check(const Value& sample) {
    char cmp[0] = {};
    size_t len = std::min(8ul, sample.value.varchar_value.length());
    memcpy(cmp, sample.value.varchar_value.data(), len);
    if (memcmp(max, cmp, len) < 0) {
      memcpy(max, cmp, len);
    }
    if (memcmp(min, cmp, len) > 0) {
      memcpy(min, cmp, len);
    }
    ++count;
  }
  VarcharColumnStats& operator*=(double multiplier) {
    count = std::floor(count * multiplier);
    distinct = std::floor(distinct * multiplier);
    return *this;
  }
  bool operator==(const VarcharColumnStats&) const = default;
  [[nodiscard]] double EstimateCount(std::string_view from,
                                     std::string_view to) const;
  friend Encoder& operator<<(Encoder& a, const VarcharColumnStats& sc);
  friend Decoder& operator>>(Decoder& a, VarcharColumnStats& sc);
  friend std::ostream& operator<<(std::ostream& o, const VarcharColumnStats& t);
};

struct DoubleColumnStats {
  double max;
  double min;
  size_t count;
  size_t distinct;
  void Check(const Value& sample) {
    max = std::max(max, sample.value.double_value);
    min = std::min(min, sample.value.double_value);
    ++count;
  }
  DoubleColumnStats& operator*=(double multiplier) {
    count = std::floor(count * multiplier);
    distinct = std::floor(distinct * multiplier);
    return *this;
  }
  bool operator==(const DoubleColumnStats&) const = default;
  [[nodiscard]] double EstimateCount(double from, double to) const;
  friend Encoder& operator<<(Encoder& a, const DoubleColumnStats& sc);
  friend Decoder& operator>>(Decoder& a, DoubleColumnStats& sc);
  friend std::ostream& operator<<(std::ostream& o, const DoubleColumnStats& t);
};

struct ColumnStats {
  ColumnStats() : type(ValueType::kNull) {}
  ColumnStats(const ColumnStats&) = default;
  ColumnStats(ColumnStats&&) = default;
  ColumnStats& operator=(const ColumnStats&) = default;
  ColumnStats& operator=(ColumnStats&&) = default;
  ~ColumnStats() = default;
  bool operator==(const ColumnStats& o) const {
    if (type != o.type) {
      return false;
    }
    switch (type) {
      case ValueType::kNull:
        return true;
      case ValueType::kInt64:
        return stat.int_stats == o.stat.int_stats;
      case ValueType::kVarChar:
        return stat.varchar_stats == o.stat.varchar_stats;
      case ValueType::kDouble:
        return stat.double_stats == o.stat.double_stats;
    }
  }

  explicit ColumnStats(ValueType t) : type(t) {}
  union {
    IntegerColumnStats int_stats;
    VarcharColumnStats varchar_stats;
    DoubleColumnStats double_stats;
  } stat{};

  [[nodiscard]] size_t Count() const {
    switch (type) {
      case ValueType::kNull:
        assert(!"never reach here");
      case ValueType::kInt64:
        return stat.int_stats.count;
      case ValueType::kVarChar:
        return stat.varchar_stats.count;
      case ValueType::kDouble:
        return stat.double_stats.count;
    }
  }
  [[nodiscard]] size_t Distinct() const {
    switch (type) {
      case ValueType::kNull:
        assert(!"never reach here");
      case ValueType::kInt64:
        return stat.int_stats.distinct;
      case ValueType::kVarChar:
        return stat.varchar_stats.distinct;
      case ValueType::kDouble:
        return stat.double_stats.distinct;
    }
    abort();
    return 0;
  }
  [[nodiscard]] double EstimateCount(int64_t from, int64_t to) const {
    switch (type) {
      case ValueType::kNull:
        assert(!"never reach here");
      case ValueType::kInt64:
        return stat.int_stats.EstimateCount(from, to);
      case ValueType::kVarChar:
        assert(!"never reach here");
      case ValueType::kDouble:
        assert(!"never reach here");
    }
    abort();
    return 0.0;
  }
  [[nodiscard]] double EstimateCount(double from, double to) const {
    switch (type) {
      case ValueType::kNull:
        assert(!"never reach here");
      case ValueType::kInt64:
        assert(!"never reach here");
      case ValueType::kVarChar:
        assert(!"never reach here");
      case ValueType::kDouble:
        return stat.double_stats.EstimateCount(from, to);
    }
    abort();
    return 0.0;
  }
  [[nodiscard]] double EstimateCount(std::string_view from,
                                     std::string_view to) const {
    switch (type) {
      case ValueType::kNull:
        assert(!"never reach here");
      case ValueType::kInt64:
        assert(!"never reach here");
      case ValueType::kVarChar:
        return stat.varchar_stats.EstimateCount(from, to);
      case ValueType::kDouble:
        assert(!"never reach here");
    }
    abort();
    return 0.0;
  }

  ColumnStats& operator*=(double multiplier) {
    switch (type) {
      case ValueType::kNull:
        break;
      case ValueType::kInt64:
        stat.int_stats *= multiplier;
        break;
      case ValueType::kVarChar:
        stat.varchar_stats *= multiplier;
        break;
      case ValueType::kDouble:
        stat.double_stats *= multiplier;
        break;
    }
    return *this;
  }

  friend Encoder& operator<<(Encoder& a, const ColumnStats& sc);
  friend Decoder& operator>>(Decoder& a, ColumnStats& sc);
  friend std::ostream& operator<<(std::ostream& o, const ColumnStats& t);
  ValueType type;
};

class TableStatistics {
 public:
  explicit TableStatistics(const Schema& sc);
  TableStatistics(const TableStatistics&) = default;
  TableStatistics(TableStatistics&&) = default;
  TableStatistics& operator=(const TableStatistics&) = default;
  TableStatistics& operator=(TableStatistics&&) = default;
  bool operator==(const TableStatistics&) const = default;
  ~TableStatistics() = default;

  Status Update(Transaction& txn, const Table& target);
  [[nodiscard]] double ReductionFactor(const Schema& sc,
                                       const Expression& predicate) const;

  friend Encoder& operator<<(Encoder& e, const TableStatistics& t);
  friend Decoder& operator>>(Decoder& d, TableStatistics& t);
  friend std::ostream& operator<<(std::ostream& o, const TableStatistics& t);

  [[nodiscard]] size_t Rows() const {
    size_t ans = 0;
    for (const auto& st : stats_) {
      ans = std::max(ans, st.Count());
    }
    return ans;
  }

  [[nodiscard]] size_t Columns() const { return stats_.size(); }
  [[nodiscard]] double EstimateCount(int col_idx, const Value& from,
                                     const Value& to) const {
    assert(from.type == to.type);
    switch (from.type) {
      case ValueType::kNull:
        LOG(FATAL) << "null value estimation is not implemented yet";
        break;
      case ValueType::kInt64:
        return stats_[col_idx].EstimateCount(from.value.int_value,
                                             to.value.int_value);
      case ValueType::kVarChar:
        return stats_[col_idx].EstimateCount(from.value.varchar_value,
                                             to.value.varchar_value);
      case ValueType::kDouble:
        return stats_[col_idx].EstimateCount(from.value.double_value,
                                             to.value.double_value);
    }
    abort();
  }

  [[nodiscard]] TableStatistics TransformBy(int col_idx, const Value& from,
                                            const Value& to) const {
    TableStatistics ret(*this);
    double multiplier = EstimateCount(col_idx, from, to);
    for (auto& st : ret.stats_) {
      st *= multiplier / st.Count();
    }
    return ret;
  }

  void Concat(const TableStatistics& rhs) {
    stats_.reserve(stats_.size() + rhs.stats_.size());
    for (const auto& s : rhs.stats_) {
      stats_.emplace_back(s);
    }
  }

  TableStatistics operator*(size_t multiplier) const {
    TableStatistics ans(*this);
    for (auto& st : ans.stats_) {
      st *= multiplier;
    }
    return ans;
  }

 private:
  std::vector<ColumnStats> stats_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_TABLE_STATISTICS_HPP
