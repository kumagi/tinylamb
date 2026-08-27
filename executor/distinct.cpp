/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/distinct.hpp"

#include <cstddef>
#include <ostream>
#include <vector>

#include "common/constants.hpp"
#include "page/row_position.hpp"
#include "type/row.hpp"
#include "type/value.hpp"

namespace tinylamb {
namespace {

// DISTINCT collapses rows that are equal under SQL ordering semantics: all
// NaNs compare equal (GoogleSQL groups every NaN into a single result row),
// -0.0 equals +0.0, and comparison of floating point values is exact rather
// than within an epsilon.  Other types keep their ordinary equality so
// interval-string and NULL handling are unchanged.
bool DistinctEquals(const Row& a, const Row& b) {
  if (a.values_.size() != b.values_.size()) { return false; }
  for (size_t i = 0; i < a.values_.size(); ++i) {
    const Value& x = a[i];
    const Value& y = b[i];
    const bool equal =
        (x.type == ValueType::kDouble && y.type == ValueType::kDouble)
            ? (CompareForOrderBy(x, y) == 0)
            : (x == y);
    if (!equal) { return false; }
  }
  return true;
}

}  // namespace

bool DistinctExecutor::Next(Row* dst, RowPosition* rp) {
  Row row;
  RowPosition position;
  while (source_->Next(&row, &position)) {
    if (distinct_on_.empty()) {
      bool duplicate = false;
      for (const Row& seen : seen_) {
        if (DistinctEquals(seen, row)) {
          duplicate = true;
          break;
        }
      }
      if (!duplicate) {
        seen_.push_back(row);
        *dst = seen_.back();
        if (rp != nullptr) { *rp = position; }
        return true;
      }
    } else {
      std::vector<Value> current_keys;
      current_keys.reserve(distinct_on_.size());
      for (const auto& expr : distinct_on_) {
        current_keys.push_back(expr->Evaluate(row, schema_));
      }
      bool duplicate = false;
      for (const auto& seen_k : seen_keys_) {
        if (seen_k.size() == current_keys.size()) {
          bool match = true;
          for (size_t i = 0; i < seen_k.size(); ++i) {
            const Value& x = seen_k[i];
            const Value& y = current_keys[i];
            const bool equal =
                (x.type == ValueType::kDouble && y.type == ValueType::kDouble)
                    ? (CompareForOrderBy(x, y) == 0)
                    : (x == y);
            if (!equal) {
              match = false;
              break;
            }
          }
          if (match) {
            duplicate = true;
            break;
          }
        }
      }
      if (!duplicate) {
        seen_keys_.push_back(std::move(current_keys));
        *dst = std::move(row);
        if (rp != nullptr) { *rp = position; }
        return true;
      }
    }
  }
  return false;
}
void DistinctExecutor::Dump(std::ostream& output, int indent) const {
  output << "Distinct\n" << Indent(static_cast<size_t>(indent) + 2);
  source_->Dump(output, indent + 2);
}

bool SortDistinctExecutor::Next(Row* dst, RowPosition* rp) {
  Row row;
  RowPosition position;
  while (source_->Next(&row, &position)) {
    if (have_previous_ && DistinctEquals(previous_, row)) { continue; }
    previous_ = row;
    have_previous_ = true;
    *dst = std::move(row);
    if (rp != nullptr) { *rp = position; }
    return true;
  }
  return false;
}

void SortDistinctExecutor::Dump(std::ostream& output, int indent) const {
  output << "SortDistinct\n" << Indent(static_cast<size_t>(indent) + 2);
  source_->Dump(output, indent + 2);
}
}  // namespace tinylamb
