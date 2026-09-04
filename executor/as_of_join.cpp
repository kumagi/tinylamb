/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/as_of_join.hpp"

#include <algorithm>
#include <cstddef>
#include <memory>
#include <optional>
#include <ostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/executor_base.hpp"
#include "page/row_position.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {

namespace {

struct RowKeyHash {
  size_t operator()(const Row& row) const {
    size_t seed = 0;
    relational_detail::DistinctValueHash hasher;
    for (const Value& v : row.values_) {
      seed ^= hasher(v) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
    }
    return seed;
  }
};

struct RowKeyEqual {
  bool operator()(const Row& a, const Row& b) const {
    if (a.values_.size() != b.values_.size()) {
      return false;
    }
    relational_detail::DistinctValueEqual eq;
    for (size_t i = 0; i < a.values_.size(); ++i) {
      if (!eq(a[i], b[i])) {
        return false;
      }
    }
    return true;
  }
};

Schema MakeCombinedSchema(const Schema& left, const Schema& right) {
  std::vector<Column> cols;
  cols.reserve(left.ColumnCount() + right.ColumnCount());
  for (size_t i = 0; i < left.ColumnCount(); ++i) {
    cols.push_back(left.GetColumn(i));
  }
  for (size_t i = 0; i < right.ColumnCount(); ++i) {
    cols.push_back(right.GetColumn(i));
  }
  return Schema(std::string(left.Name()) + "_asof_" + std::string(right.Name()),
                std::move(cols));
}

}  // namespace

AsOfJoin::AsOfJoin(Executor left, Schema left_schema, Executor right,
                   Schema right_schema,
                   std::vector<std::pair<slot_t, slot_t>> equi_keys,
                   slot_t left_as_of_col, slot_t right_as_of_col,
                   AsOfComparison comp, bool is_left_outer,
                   std::optional<Value> max_lookback)
    : left_(std::move(left)),
      left_schema_(std::move(left_schema)),
      right_(std::move(right)),
      right_schema_(std::move(right_schema)),
      equi_keys_(std::move(equi_keys)),
      left_as_of_col_(left_as_of_col),
      right_as_of_col_(right_as_of_col),
      comparison_(comp),
      is_left_outer_(is_left_outer),
      max_lookback_(std::move(max_lookback)),
      output_schema_(MakeCombinedSchema(left_schema_, right_schema_)) {}

void AsOfJoin::Materialize() {
  if (materialized_) {
    return;
  }
  materialized_ = true;
  output_rows_.clear();
  cursor_ = 0;

  // Materialize and index right rows by equi keys
  std::unordered_map<Row, std::vector<Row>, RowKeyHash, RowKeyEqual> right_map;
  std::vector<Row> right_all_rows;

  Row r_row;
  RowPosition r_pos;
  while (right_->Next(&r_row, &r_pos)) {
    if (equi_keys_.empty()) {
      right_all_rows.push_back(r_row);
    } else {
      std::vector<Value> key_vals;
      key_vals.reserve(equi_keys_.size());
      bool has_null = false;
      for (const auto& [l_col, r_col] : equi_keys_) {
        key_vals.push_back(
            relational_detail::CanonicalDistinctValue(r_row[r_col]));
        // SQL equality never matches on NULL equi keys.
        has_null = has_null || r_row[r_col].IsNull();
      }
      if (has_null) {
        continue;
      }
      right_map[Row(std::move(key_vals))].push_back(r_row);
    }
  }

  // Iterate over left rows
  Row l_row;
  RowPosition l_pos;
  while (left_->Next(&l_row, &l_pos)) {
    const Value& l_as_of = l_row[left_as_of_col_];
    if (l_as_of.IsNull()) {
      if (is_left_outer_) {
        std::vector<Value> out_vals = l_row.values_;
        for (size_t i = 0; i < right_schema_.ColumnCount(); ++i) {
          out_vals.emplace_back();
        }
        output_rows_.emplace_back(std::move(out_vals));
      }
      continue;
    }

    const std::vector<Row>* candidates = nullptr;
    if (equi_keys_.empty()) {
      candidates = &right_all_rows;
    } else {
      bool l_key_null = false;
      std::vector<Value> key_vals;
      key_vals.reserve(equi_keys_.size());
      for (const auto& [l_col, r_col] : equi_keys_) {
        key_vals.push_back(
            relational_detail::CanonicalDistinctValue(l_row[l_col]));
        l_key_null = l_key_null || l_row[l_col].IsNull();
      }
      if (l_key_null) {
        // A NULL equi key matches nothing; the outer padding below still
        // emits the row for a left-outer AS OF join.
        candidates = nullptr;
      } else {
        auto it = right_map.find(Row(std::move(key_vals)));
        if (it != right_map.end()) {
          candidates = &it->second;
        }
      }
    }

    const Row* best_match = nullptr;
    Value best_val;

    if (candidates != nullptr) {
      for (const Row& candidate : *candidates) {
        const Value& r_as_of = candidate[right_as_of_col_];
        if (r_as_of.IsNull()) {
          continue;
        }

        if (comparison_ == AsOfComparison::kLessEqual) {
          if (r_as_of <= l_as_of) {
            if (max_lookback_.has_value()) {
              Value diff = l_as_of - r_as_of;
              if (diff > *max_lookback_) {
                continue;
              }
            }
            if (best_match == nullptr || best_val < r_as_of) {
              best_match = &candidate;
              best_val = r_as_of;
            }
          }
        } else {  // kGreaterEqual
          if (r_as_of >= l_as_of) {
            if (max_lookback_.has_value()) {
              Value diff = r_as_of - l_as_of;
              if (diff > *max_lookback_) {
                continue;
              }
            }
            if (best_match == nullptr || r_as_of < best_val) {
              best_match = &candidate;
              best_val = r_as_of;
            }
          }
        }
      }
    }

    if (best_match != nullptr) {
      std::vector<Value> out_vals = l_row.values_;
      out_vals.insert(out_vals.end(), best_match->values_.begin(),
                      best_match->values_.end());
      output_rows_.emplace_back(std::move(out_vals));
    } else if (is_left_outer_) {
      std::vector<Value> out_vals = l_row.values_;
      for (size_t i = 0; i < right_schema_.ColumnCount(); ++i) {
        out_vals.emplace_back();
      }
      output_rows_.emplace_back(std::move(out_vals));
    }
  }
}

bool AsOfJoin::Next(Row* dst, RowPosition* rp) {
  if (!materialized_) {
    Materialize();
  }
  if (cursor_ >= output_rows_.size()) {
    return false;
  }
  *dst = output_rows_[cursor_++];
  if (rp != nullptr) {
    *rp = RowPosition();
  }
  return true;
}

size_t AsOfJoin::NextBatch(DataChunk* destination, size_t max_rows) {
  destination->Reset(output_schema_, max_rows);
  if (max_rows == 0) {
    return 0;
  }
  Row row;
  RowPosition pos;
  size_t count = 0;
  while (count < max_rows && Next(&row, &pos)) {
    destination->Append(row, pos);
    ++count;
  }
  return count;
}

void AsOfJoin::Dump(std::ostream& o, int indent) const {
  o << "AsOfJoin: \n" << Indent(indent + 2);
  left_->Dump(o, indent + 2);
  o << "\n" << Indent(indent + 2);
  right_->Dump(o, indent + 2);
}

}  // namespace tinylamb
