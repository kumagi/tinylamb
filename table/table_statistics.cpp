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

#include "table_statistics.hpp"

#include <cassert>
#include <cstdint>
#include <memory>
#include <string_view>
#include <unordered_set>

#include "common/encoder.hpp"
#include "expression/binary_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "table/table.hpp"

namespace tinylamb {
namespace {
class DistinctCounterBase {
 public:
  virtual ~DistinctCounterBase() = default;

  virtual void Add(int64_t v) = 0;

  virtual void Add(std::string_view v) = 0;

  virtual void Add(double v) = 0;

  virtual void Output(IntegerColumnStats& o) = 0;

  virtual void Output(VarcharColumnStats& o) = 0;

  virtual void Output(DoubleColumnStats& o) = 0;
};

class Int64DistinctCounter : public DistinctCounterBase {
 public:
  ~Int64DistinctCounter() override = default;

  Int64DistinctCounter() {
    max = std::numeric_limits<int64_t>::min();
    min = std::numeric_limits<int64_t>::max();
    count = 0;
    counter_.clear();
  }

  void Add(int64_t v) override {
    max = max < v ? v : max;
    min = v < min ? v : min;
    counter_.insert(v);
    ++count;
  }

  void Add(std::string_view /*v*/) override {
    assert(!"called Add(varchar) to int counter");
  }

  void Add(double /*v*/) override {
    assert(!"called Add(double) to integer counter");
  }

  void Output(IntegerColumnStats& o) override {
    o.max = max;
    o.min = min;
    o.count = count;
    o.distinct = counter_.size();
  }

  void Output(VarcharColumnStats& /*o*/) override {
    assert(!"called Output(varchar) to int counter");
  }

  void Output(DoubleColumnStats& /*o*/) override {
    assert(!"called Output(double) to int counter");
  }

  int64_t max{};
  int64_t min{};
  int64_t count{};
  std::unordered_set<int64_t> counter_;
};

class VarcharDistinctCounter : public DistinctCounterBase {
 public:
  ~VarcharDistinctCounter() override = default;

  VarcharDistinctCounter() {
    max = "";
    min = std::string(256, '\xff');
    count = 0;
    counter_.clear();
  }

  void Add(int64_t /*v*/) override {
    assert(!"called Add(int64) to varchar counter");
  }

  void Add(std::string_view v) override {
    std::string str(v);
    max = max < str ? str : max;
    min = str < min ? str : min;
    count++;
    counter_.insert(std::string(v));
  }

  void Add(double /*v*/) override {
    assert(!"called Add(double) to varchar counter");
  }

  void Output(IntegerColumnStats& /*o*/) override {
    assert(!"called Output(int64) to varchar counter");
  }

  void Output(VarcharColumnStats& o) override {
    ::memcpy(o.max, max.data(), std::min(8LU, max.size()));
    ::memcpy(o.min, min.data(), std::min(8LU, min.size()));
    o.count = count;
    o.distinct = counter_.size();
  }

  void Output(DoubleColumnStats& /*o*/) override {
    assert(!"called Output(double) to varchar counter");
  }

  std::string max;
  std::string min;
  int64_t count{0};
  std::unordered_set<std::string> counter_;
};

class DoubleDistinctCounter : public DistinctCounterBase {
 public:
  ~DoubleDistinctCounter() override = default;

  DoubleDistinctCounter() {
    max = std::numeric_limits<double>::min();
    min = std::numeric_limits<double>::max();
    count = 0;
    counter_.clear();
  }

  void Add(int64_t /*v*/) override {
    assert(!"called Add(int64) to double counter");
  }

  void Add(std::string_view /*v*/) override {
    assert(!"called Add(varchar) to double counter");
  }

  void Add(double v) override {
    max = max < v ? v : max;
    max = v < min ? v : min;
    count++;
    counter_.insert(v);
  }

  void Output(IntegerColumnStats& /*o*/) override {
    assert(!"called Output(int64) to double counter");
  }

  void Output(VarcharColumnStats& /*o*/) override {
    assert(!"called Output(varchar) to double counter");
  }

  void Output(DoubleColumnStats& o) override {
    o.max = max;
    o.min = min;
    o.count = count;
    o.distinct = counter_.size();
  }

  double max{};
  double min{};
  int64_t count{};
  int64_t distinct{};
  std::unordered_set<double> counter_;
};

class DistinctCounter {
 public:
  explicit DistinctCounter(ValueType type) {
    switch (type) {
      case ValueType::kNull:
        assert(!"Never reach here");
      case ValueType::kInt64:
        counter_ = std::make_unique<Int64DistinctCounter>();
        break;
      case ValueType::kVarChar:
        counter_ = std::make_unique<VarcharDistinctCounter>();
        break;
      case ValueType::kDouble:
        counter_ = std::make_unique<DoubleDistinctCounter>();
        break;
    }
  }

  void Add(const Value& v) const {
    switch (v.type) {
      case ValueType::kNull:
        assert(!"Never reach here");
      case ValueType::kInt64:
        counter_->Add(v.value.int_value);
        return;
      case ValueType::kVarChar:
        counter_->Add(v.value.varchar_value);
        break;
      case ValueType::kDouble:
        counter_->Add(v.value.double_value);
        break;
    }
  }

  void Output(IntegerColumnStats& o) const { counter_->Output(o); }
  void Output(VarcharColumnStats& o) const { counter_->Output(o); }
  void Output(DoubleColumnStats& o) const { counter_->Output(o); }

  std::unique_ptr<DistinctCounterBase> counter_;
};
}  // namespace

TableStatistics::TableStatistics(const Schema& sc) {
  stats_.reserve(sc.ColumnCount());
  for (size_t i = 0; i < sc.ColumnCount(); ++i) {
    stats_.emplace_back(sc.GetColumn(i).Type());
  }
}

Status TableStatistics::Update(Transaction& txn, const Table& target) {
  int rows = 0;
  const Schema& schema = target.GetSchema();
  Iterator it = target.BeginFullScan(txn);
  std::vector<std::unique_ptr<DistinctCounter> > dist_counters;
  dist_counters.reserve(schema.ColumnCount());
  for (size_t i = 0; i < schema.ColumnCount(); ++i) {
    const Column& col = schema.GetColumn(i);
    dist_counters.emplace_back(new DistinctCounter(col.Type()));
  }
  while (it.IsValid()) {
    const Row& row = *it;
    for (size_t i = 0; i < dist_counters.size(); ++i) {
      dist_counters[i]->Add(row[i]);
    }
    ++rows;
    ++it;
  }
  for (size_t i = 0; i < stats_.size(); ++i) {
    ColumnStats& cs = stats_[i];
    switch (cs.type) {
      case ValueType::kNull:
        assert(!"never reach here");
      case ValueType::kInt64:
        dist_counters[i]->Output(cs.stat.int_stats);
        break;
      case ValueType::kVarChar:
        dist_counters[i]->Output(cs.stat.varchar_stats);
        break;
      case ValueType::kDouble:
        dist_counters[i]->Output(cs.stat.double_stats);
        break;
    }
  }
  return Status::kSuccess;
}

// Returns estimated inverted selection ratio if the `sc` is selected by
// `predicate`. If the predicate selects rows to 1 / x, returns x.
// Returning 1 means no selection (pass through).
double TableStatistics::ReductionFactor(const Schema& sc,
                                        const Expression& predicate) const {
  assert(0 < sc.ColumnCount());
  if (predicate->Type() == TypeTag::kBinaryExp) {
    const auto* bo = reinterpret_cast<const BinaryExpression*>(predicate.get());
    std::unordered_set<ColumnName> columns = sc.ColumnSet();
    if (bo->Op() == BinaryOperation::kEquals) {
      if (bo->Left()->Type() == TypeTag::kColumnValue &&
          bo->Right()->Type() == TypeTag::kColumnValue) {
        const auto* lcv =
            reinterpret_cast<const ColumnValue*>(bo->Left().get());
        const auto* rcv =
            reinterpret_cast<const ColumnValue*>(bo->Right().get());
        if (columns.find(lcv->GetColumnName()) != columns.end() &&
            columns.find(rcv->GetColumnName()) != columns.end()) {
          int offset_left = sc.Offset(lcv->GetColumnName());
          assert(0 <= offset_left && offset_left < (int)stats_.size());
          int offset_right = sc.Offset(rcv->GetColumnName());
          assert(0 <= offset_right && offset_right < (int)stats_.size());
          return std::min(static_cast<double>(stats_[offset_left].Distinct()),
                          static_cast<double>(stats_[offset_right].Distinct()));
        }
      }
      if (bo->Left()->Type() == TypeTag::kColumnValue) {
        const auto* lcv =
            reinterpret_cast<const ColumnValue*>(bo->Left().get());
        int offset_left = sc.Offset(lcv->GetColumnName());
        assert(0 <= offset_left && offset_left < (int)stats_.size());
        return static_cast<double>(stats_[offset_left].Distinct());
      }
      if (bo->Right()->Type() == TypeTag::kColumnValue) {
        const auto* rcv =
            reinterpret_cast<const ColumnValue*>(bo->Left().get());
        int offset_right = sc.Offset(rcv->GetColumnName());
        return static_cast<double>(stats_[offset_right].Distinct());
      }
      if (bo->Left()->Type() == TypeTag::kConstantValue &&
          bo->Right()->Type() == TypeTag::kConstantValue) {
        Value left = reinterpret_cast<const ConstantValue*>(bo->Left().get())
                         ->GetValue();
        Value right = reinterpret_cast<const ConstantValue*>(bo->Right().get())
                          ->GetValue();
        if (left == right) {
          return 1;
        }
        return std::numeric_limits<double>::max();
      }
    }
    if (bo->Op() == BinaryOperation::kAnd) {
      return ReductionFactor(sc, bo->Left()) * ReductionFactor(sc, bo->Right());
    }
    if (bo->Op() == BinaryOperation::kOr) {
      // FIXME: what should I return?
      return ReductionFactor(sc, bo->Left()) + ReductionFactor(sc, bo->Right());
    }
    // TODO: kGreaterThan, kGreaterEqual, kLessThan, kLessEqual, kNotEqual, kXor
  }
  return 1;
}

double IntegerColumnStats::EstimateCount(int64_t from, int64_t to) const {
  if (to <= from) {
    std::swap(from, to);
  }
  assert(from <= to);
  from = std::max(min, from);
  to = std::min(max, to);
  return (from - to) * static_cast<double>(count) / distinct;
}

Encoder& operator<<(Encoder& e, const IntegerColumnStats& s) {
  e << s.max << s.min << s.count << s.distinct;
  return e;
}

double VarcharColumnStats::EstimateCount(std::string_view from,
                                         std::string_view to) const {
  if (to <= from) {
    std::swap(from, to);
  }
  if (to <= min || max <= from) {
    return 1;
  }
  return 2;  // FIXME: there must be a better estimation!
}

Encoder& operator<<(Encoder& e, const VarcharColumnStats& s) {
  e << s.count << s.distinct;
  return e;
}

double DoubleColumnStats::EstimateCount(double from, double to) const {
  if (to <= from) {
    std::swap(from, to);
  }
  assert(from <= to);
  from = std::max(min, from);
  to = std::min(max, to);
  return (from - to) * count / distinct;
}

Encoder& operator<<(Encoder& e, const DoubleColumnStats& s) {
  e << s.max << s.min << s.count << s.distinct;
  return e;
}

Decoder& operator>>(Decoder& d, IntegerColumnStats& s) {
  d >> s.max >> s.min >> s.count >> s.distinct;
  return d;
}

Decoder& operator>>(Decoder& d, VarcharColumnStats& s) {
  d >> s.count >> s.distinct;
  return d;
}

Decoder& operator>>(Decoder& d, DoubleColumnStats& s) {
  d >> s.max >> s.min >> s.count >> s.distinct;
  return d;
}

std::ostream& operator<<(std::ostream& o, const IntegerColumnStats& t) {
  o << "Max: " << t.max << " Min: " << t.min << " Rows:" << t.count
    << " Distinct: " << t.distinct;
  return o;
}

std::ostream& operator<<(std::ostream& o, const VarcharColumnStats& t) {
  o << "Max: " << t.max << " Min: " << t.min << " Rows:" << t.count
    << " Distinct: " << t.distinct;
  return o;
}

std::ostream& operator<<(std::ostream& o, const DoubleColumnStats& t) {
  o << "Max: " << t.max << " Min: " << t.min << " Rows:" << t.count
    << " Distinct: " << t.distinct;
  return o;
}

Encoder& operator<<(Encoder& a, const ColumnStats& sc) {
  a << sc.type;
  switch (sc.type) {
    case ValueType::kNull:
      assert(!"never reach here");
    case ValueType::kInt64:
      a << sc.stat.int_stats;
      break;
    case ValueType::kVarChar:
      a << sc.stat.varchar_stats;
      break;
    case ValueType::kDouble:
      a << sc.stat.double_stats;
      break;
  }
  return a;
}

Decoder& operator>>(Decoder& d, ColumnStats& sc) {
  d >> sc.type;
  switch (sc.type) {
    case ValueType::kNull:
      assert(!"never reach here");
    case ValueType::kInt64:
      d >> sc.stat.int_stats;
      break;
    case ValueType::kVarChar:
      d >> sc.stat.varchar_stats;
      break;
    case ValueType::kDouble:
      d >> sc.stat.double_stats;
      break;
  }
  return d;
}

std::ostream& operator<<(std::ostream& o, const ColumnStats& t) {
  switch (t.type) {
    case ValueType::kNull:
      assert(!"never reach here");
    case ValueType::kInt64:
      o << t.stat.int_stats;
      break;
    case ValueType::kVarChar:
      o << t.stat.varchar_stats;
      break;
    case ValueType::kDouble:
      o << t.stat.double_stats;
      break;
  }
  return o;
}

Encoder& operator<<(Encoder& e, const TableStatistics& s) {
  e << s.stats_;
  return e;
}

Decoder& operator>>(Decoder& d, TableStatistics& s) {
  d >> s.stats_;
  return d;
}

std::ostream& operator<<(std::ostream& o, const TableStatistics& t) {
  o << "Rows: " << t.Rows() << "\n";
  for (const auto& stat : t.stats_) {
    o << stat << "\n";
  }
  return o;
}
}  // namespace tinylamb
