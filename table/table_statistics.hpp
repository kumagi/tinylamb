//
// Created by kumagi on 2022/03/06.
//

#ifndef TINYLAMB_TABLE_STATISTICS_HPP
#define TINYLAMB_TABLE_STATISTICS_HPP

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
  friend Encoder& operator<<(Encoder& a, const DoubleColumnStats& sc);
  friend Decoder& operator>>(Decoder& a, DoubleColumnStats& sc);
  friend std::ostream& operator<<(std::ostream& o, const DoubleColumnStats& t);
};

struct ColumnStats {
  ColumnStats() : type(ValueType::kUnknown) {}
  explicit ColumnStats(ValueType t) : type(t) {}
  union {
    IntegerColumnStats int_stats;
    VarcharColumnStats varchar_stats;
    DoubleColumnStats double_stats;
  } stat{};

  [[nodiscard]] size_t Count() const {
    switch (type) {
      case ValueType::kUnknown:
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
      case ValueType::kUnknown:
        assert(!"never reach here");
      case ValueType::kInt64:
        return stat.int_stats.distinct;
      case ValueType::kVarChar:
        return stat.varchar_stats.distinct;
      case ValueType::kDouble:
        return stat.double_stats.distinct;
    }
  }
  friend Encoder& operator<<(Encoder& a, const ColumnStats& sc);
  friend Decoder& operator>>(Decoder& a, ColumnStats& sc);
  friend std::ostream& operator<<(std::ostream& o, const ColumnStats& t);
  ValueType type;
};

class TableStatistics {
 public:
  explicit TableStatistics(const Schema& sc);
  Status Update(Transaction& txn, const Table& target);
  double ReductionFactor(const Schema& sc, const Expression& predicate) const;

  friend Encoder& operator<<(Encoder& e, const TableStatistics& t);
  friend Decoder& operator>>(Decoder& d, TableStatistics& t);
  friend std::ostream& operator<<(std::ostream& o, const TableStatistics& t);

  size_t row_count_;
  std::vector<ColumnStats> stats_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_TABLE_STATISTICS_HPP
