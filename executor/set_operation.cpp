/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "set_operation.hpp"

#include <algorithm>
#include <cstddef>
#include <iterator>
#include <stdexcept>
#include <string>
#include <unordered_set>
#include <utility>

#include "common/constants.hpp"
#include "executor/query_memory.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

namespace {

constexpr size_t kSetOperationPartitions = 32;

ValueType CommonSetValueType(ValueType left, ValueType right) {
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
  if ((left_text &&
       (right == ValueType::kInt64 || right == ValueType::kDouble ||
        right == ValueType::kDate)) ||
      (right_text && (left == ValueType::kInt64 || left == ValueType::kDouble ||
                      left == ValueType::kDate))) {
    return ValueType::kVarChar;
  }
  throw std::invalid_argument("set operation inputs have incompatible types");
}

}  // namespace

void SetOperationExecutor::MaterializeRows(
    std::vector<std::vector<Positioned>> rows) {
  if (rows.empty()) {
    throw std::invalid_argument("set operation needs at least one source");
  }
  size_t width = 0;
  bool width_known = false;
  for (const auto& source : rows) {
    for (const Positioned& item : source) {
      if (!width_known) {
        width = item.row.values_.size();
        width_known = true;
      } else if (item.row.values_.size() != width) {
        throw std::invalid_argument(
            "set operation inputs must have the same column count");
      }
    }
  }
  if (width_known) {
    std::vector<ValueType> common_types(width, ValueType::kNull);
    for (const auto& source : rows) {
      for (const Positioned& item : source) {
        for (size_t column = 0; column < width; ++column) {
          common_types[column] = CommonSetValueType(
              common_types[column], item.row.values_[column].type);
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
            value = Value(static_cast<double>(value.value.int_value));
            continue;
          }
          if (common_types[column] == ValueType::kVarChar) {
            value = Value(value.AsString());
            continue;
          }
          throw std::invalid_argument(
              "set operation value cannot be coerced to common type");
        }
      }
    }
  }
  output_.clear();
  output_offset_ = 0;
  switch (operation_) {
    case SetOperationKind::kUnionAll:
      for (const auto& source : rows) {
        AppendAll(source);
      }
      break;
    case SetOperationKind::kUnion: {
      std::unordered_set<Row> seen;
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
}

void SetOperationExecutor::MaterializePartitioned() {
  for (auto& source : spill_sources_) {
    for (SpillFile& partition : source) {
      partition.FinishWriting();
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
      raw_partition.ForEachRow([&](const Row& raw) {
        Row row = raw;
        if (spill_width_.has_value() && row.values_.size() != *spill_width_) {
          throw std::invalid_argument(
              "set operation inputs must have the same column count");
        }
        for (size_t column = 0; column < row.values_.size(); ++column) {
          Value& value = row.values_[column];
          const ValueType expected = spill_common_types_[column];
          if (value.IsNull() || value.type == expected) {
            continue;
          }
          if (expected == ValueType::kDouble &&
              value.type == ValueType::kInt64) {
            value = Value(static_cast<double>(value.value.int_value));
            continue;
          }
          if (expected == ValueType::kVarChar) {
            value = Value(value.AsString());
            continue;
          }
          throw std::invalid_argument(
              "set operation value cannot be coerced to common type");
        }
        normalized[source][std::hash<Row>{}(row) % kSetOperationPartitions]
            .Append(row);
      });
    }
  }
  spill_sources_ = std::move(normalized);
  for (auto& source : spill_sources_) {
    for (SpillFile& partition : source) {
      partition.FinishWriting();
    }
  }
  output_.clear();
  output_offset_ = 0;
  std::vector<Positioned> accumulated;
  for (size_t partition = 0; partition < kSetOperationPartitions; ++partition) {
    std::vector<std::vector<Positioned>> rows(spill_sources_.size());
    for (size_t source = 0; source < spill_sources_.size(); ++source) {
      const std::vector<Row> spilled =
          spill_sources_[source][partition].ReadAllRows();
      rows[source].reserve(spilled.size());
      for (Row row : spilled) {
        rows[source].push_back(Positioned{std::move(row), RowPosition()});
      }
    }
    MaterializeRows(std::move(rows));
    accumulated.insert(accumulated.end(),
                       std::make_move_iterator(output_.begin()),
                       std::make_move_iterator(output_.end()));
  }
  output_ = std::move(accumulated);
  output_offset_ = 0;
}

void SetOperationExecutor::AppendAll(const std::vector<Positioned>& source) {
  output_.insert(output_.end(), source.begin(), source.end());
}

void SetOperationExecutor::AppendDistinct(const std::vector<Positioned>& source,
                                          std::unordered_set<Row>* seen) {
  for (const Positioned& item : source) {
    if (seen->insert(item.row).second) {
      output_.push_back(item);
    }
  }
}

void SetOperationExecutor::AppendIntersection(
    const std::vector<std::vector<Positioned>>& rows, bool all) {
  if (rows.empty()) {
    return;
  }
  std::unordered_map<Row, size_t> counts;
  for (const Positioned& item : rows[0]) {
    ++counts[item.row];
  }
  for (size_t source = 1; source < rows.size(); ++source) {
    std::unordered_map<Row, size_t> current;
    for (const Positioned& item : rows[source]) {
      ++current[item.row];
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
  std::unordered_set<Row> emitted;
  for (const Positioned& item : rows[0]) {
    const auto found = counts.find(item.row);
    if (found == counts.end() || found->second == 0) {
      continue;
    }
    if (!all && !emitted.insert(item.row).second) {
      continue;
    }
    output_.push_back(item);
    if (--counts[item.row] == 0) {
      counts.erase(item.row);
    }
  }
}

void SetOperationExecutor::AppendExcept(
    const std::vector<std::vector<Positioned>>& rows, bool all) {
  if (rows.empty()) {
    return;
  }
  std::unordered_map<Row, size_t> removed;
  for (size_t source = 1; source < rows.size(); ++source) {
    for (const Positioned& item : rows[source]) {
      ++removed[item.row];
    }
  }
  std::unordered_set<Row> emitted;
  for (const Positioned& item : rows[0]) {
    const auto found = removed.find(item.row);
    if (found != removed.end() && found->second != 0) {
      if (--found->second == 0) {
        removed.erase(found);
      }
      continue;
    }
    if (!all && !emitted.insert(item.row).second) {
      continue;
    }
    output_.push_back(item);
  }
}

void SetOperationExecutor::Materialize() {
  std::vector<std::vector<Positioned>> rows(sources_.size());
  QueryMemoryCharge charge;
  bool spilling = false;
  const auto observe_types = [&](const Row& row) {
    if (!spill_width_.has_value()) {
      spill_width_ = row.values_.size();
      spill_common_types_.assign(*spill_width_, ValueType::kNull);
    }
    if (row.values_.size() != *spill_width_) {
      throw std::invalid_argument(
          "set operation inputs must have the same column count");
    }
    for (size_t column = 0; column < row.values_.size(); ++column) {
      spill_common_types_[column] = CommonSetValueType(
          spill_common_types_[column], row.values_[column].type);
    }
  };
  auto begin_spill = [&]() {
    spilling = true;
    spill_sources_.resize(sources_.size());
    for (auto& source : spill_sources_) {
      source.resize(kSetOperationPartitions);
    }
    for (size_t source = 0; source < rows.size(); ++source) {
      for (const Positioned& item : rows[source]) {
        spill_sources_[source]
                      [std::hash<Row>{}(item.row) % kSetOperationPartitions]
                          .Append(item.row);
      }
      rows[source].clear();
      rows[source].shrink_to_fit();
    }
    charge.ReleaseAll();
  };
  for (size_t source = 0; source < sources_.size(); ++source) {
    Row row;
    RowPosition position;
    while (sources_[source]->Next(&row, &position)) {
      observe_types(row);
      if (!spilling && operation_ != SetOperationKind::kUnionAll &&
          !QueryMemoryBudget::Global().CanReserve(EstimateRowBytes(row))) {
        begin_spill();
      }
      if (spilling) {
        spill_sources_[source][std::hash<Row>{}(row) % kSetOperationPartitions]
            .Append(row);
      } else {
        charge.Add(EstimateRowBytes(row));
        rows[source].push_back(Positioned{std::move(row), position});
      }
    }
  }
  if (spilling) {
    MaterializePartitioned();
  } else {
    MaterializeRows(std::move(rows));
  }
  materialized_ = true;
}

bool SetOperationExecutor::Next(Row* destination, RowPosition* position) {
  if (!materialized_) {
    Materialize();
  }
  if (output_offset_ == output_.size()) {
    return false;
  }
  const Positioned& item = output_[output_offset_++];
  *destination = item.row;
  if (position != nullptr) {
    *position = item.position;
  }
  return true;
}

void SetOperationExecutor::Dump(std::ostream& output, int indent) const {
  output << Indent(indent) << "SetOperation\n";
  for (const Executor& source : sources_) {
    source->Dump(output, indent + 2);
    output << '\n';
  }
}

}  // namespace tinylamb
