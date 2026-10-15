/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/distinct.hpp"

#include <cstddef>
#include <ostream>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/status_or.hpp"
#include "page/row_position.hpp"
#include "type/collation.hpp"
#include "type/row.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
namespace {

// DISTINCT collapses rows that are equal under SQL ordering semantics: all
// NaNs compare equal (GoogleSQL groups every NaN into a single result row),
// -0.0 equals +0.0, and comparison of floating point values is exact rather
// than within an epsilon.  Other types keep their ordinary equality so
// interval-string and NULL handling are unchanged.
bool DistinctEquals(const Row& a, const Row& b) {
  if (a.values_.size() != b.values_.size()) {
    return false;
  }
  for (size_t i = 0; i < a.values_.size(); ++i) {
    // One peer rule for DISTINCT and ORDER BY: NaN collapses, -0.0 equals
    // +0.0, interval-equivalent text matches, and a case-insensitive VARCHAR
    // pair is equal once folded (type/collation.hpp).
    if (!EqualUnderCollation(a[i], b[i])) {
      return false;
    }
  }
  return true;
}

}  // namespace

bool DistinctRowEqual::operator()(const Row& left, const Row& right) const {
  return DistinctEquals(left, right);
}

bool DistinctExecutor::Next(Row* dst, RowPosition* rp) {
  Row row;
  RowPosition position;
  while (source_->Next(&row, &position)) {
    if (distinct_on_.empty()) {
      const auto [it, inserted] = seen_.insert(std::move(row));
      if (inserted) {
        *dst = *it;
        if (rp != nullptr) {
          *rp = position;
        }
        return true;
      }
    } else {
      Row current_keys;
      current_keys.values_.reserve(distinct_on_.size());
      for (const auto& expr : distinct_on_) {
        StatusOr<Value> key = expr->TryEvaluate(row, schema_);
        if (!key.HasValue()) {
          return FailWith(key.GetStatus());
        }
        current_keys.values_.push_back(key.MoveValue());
      }
      if (seen_keys_.insert(std::move(current_keys)).second) {
        *dst = std::move(row);
        if (rp != nullptr) {
          *rp = position;
        }
        return true;
      }
    }
  }
  FailWithChildOf(*source_);
  return false;
}
void DistinctExecutor::Dump(std::ostream& output, int indent) const {
  output << "HashDistinct\n" << Indent(static_cast<size_t>(indent) + 2);
  source_->Dump(output, indent + 2);
}

bool SortDistinctExecutor::Next(Row* dst, RowPosition* rp) {
  Row row;
  RowPosition position;
  while (source_->Next(&row, &position)) {
    if (have_previous_ && DistinctEquals(previous_, row)) {
      continue;
    }
    previous_ = row;
    have_previous_ = true;
    *dst = std::move(row);
    if (rp != nullptr) {
      *rp = position;
    }
    return true;
  }
  FailWithChildOf(*source_);
  return false;
}

void SortDistinctExecutor::Dump(std::ostream& output, int indent) const {
  output << "SortDistinct\n" << Indent(static_cast<size_t>(indent) + 2);
  source_->Dump(output, indent + 2);
}
}  // namespace tinylamb
