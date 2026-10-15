/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/pdqsort.hpp"

#include <algorithm>
#include <utility>
#include <vector>

#include "executor/sort.hpp"
#include "expression/expression.hpp"
#include "page/row_position.hpp"
#include "type/collation.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {

namespace {

int CompareRowKeys(const Row& lhs, const Row& rhs, const Schema& schema,
                   const std::vector<SortExecutor::Key>& keys, Status* error) {
  for (const SortExecutor::Key& key : keys) {
    StatusOr<Value> lv_or = key.expression->TryEvaluate(lhs, schema);
    StatusOr<Value> rv_or = key.expression->TryEvaluate(rhs, schema);
    if (!lv_or.HasValue() || !rv_or.HasValue()) {
      // Comparators cannot abort mid-sort: record the first failure and
      // treat the pair as equal.
      if (error != nullptr && *error == Status::kSuccess) {
        *error = lv_or.HasValue() ? rv_or.GetStatus() : lv_or.GetStatus();
      }
      return 0;
    }
    Value lv = lv_or.MoveValue();
    Value rv = rv_or.MoveValue();
    if (lv.IsNull() && rv.IsNull()) {
      continue;
    }
    // CompareForOrderBy canonicalizes NaN (equal to itself, above +inf)
    // like SortExecutor/TopN.  Raw `lv < rv` / `rv < lv` makes NaN compare
    // "equal" to every value while 5 < 7 -- not a strict weak ordering, so
    // std::sort below is UB (unguarded partitioning can read out of
    // bounds) and NaN rows land in inconsistent positions.  SortKeyOrder adds
    // the NULL placement, the direction and the key's collation on top.
    const int cmp = MakeSortKeyOrder(key).Compare(lv, rv);
    if (cmp != 0) {
      return cmp;
    }
  }
  return 0;
}

}  // namespace

void PdqSort::Sort(std::vector<std::pair<Row, RowPosition>>& rows,
                   const Schema& schema,
                   const std::vector<SortExecutor::Key>& keys, Status* error) {
  std::sort(rows.begin(), rows.end(),
            [&](const std::pair<Row, RowPosition>& a,
                const std::pair<Row, RowPosition>& b) {
              return CompareRowKeys(a.first, b.first, schema, keys, error) < 0;
            });
}

void PdqSort::Sort(std::vector<Row>& rows, const Schema& schema,
                   const std::vector<SortExecutor::Key>& keys, Status* error) {
  std::sort(rows.begin(), rows.end(), [&](const Row& a, const Row& b) {
    return CompareRowKeys(a, b, schema, keys, error) < 0;
  });
}

}  // namespace tinylamb
