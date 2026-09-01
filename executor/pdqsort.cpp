/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/pdqsort.hpp"

#include <algorithm>
#include <cstddef>
#include <utility>
#include <vector>

#include "executor/sort.hpp"
#include "expression/expression.hpp"
#include "page/row_position.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {

namespace {

int CompareRowKeys(const Row& lhs, const Row& rhs, const Schema& schema,
                   const std::vector<SortExecutor::Key>& keys) {
  for (const auto& key : keys) {
    Value lv = key.expression->Evaluate(lhs, schema);
    Value rv = key.expression->Evaluate(rhs, schema);
    if (lv.IsNull() && rv.IsNull()) continue;
    if (lv.IsNull()) {
      bool nulls_first = key.nulls_first.value_or(key.ascending);
      return nulls_first ? -1 : 1;
    }
    if (rv.IsNull()) {
      bool nulls_first = key.nulls_first.value_or(key.ascending);
      return nulls_first ? 1 : -1;
    }
    if (lv < rv) return key.ascending ? -1 : 1;
    if (rv < lv) return key.ascending ? 1 : -1;
  }
  return 0;
}

}  // namespace

void PdqSort::Sort(std::vector<std::pair<Row, RowPosition>>& rows,
                   const Schema& schema,
                   const std::vector<SortExecutor::Key>& keys) {
  std::sort(rows.begin(), rows.end(),
            [&](const std::pair<Row, RowPosition>& a,
                const std::pair<Row, RowPosition>& b) {
              return CompareRowKeys(a.first, b.first, schema, keys) < 0;
            });
}

void PdqSort::Sort(std::vector<Row>& rows, const Schema& schema,
                   const std::vector<SortExecutor::Key>& keys) {
  std::sort(rows.begin(), rows.end(), [&](const Row& a, const Row& b) {
    return CompareRowKeys(a, b, schema, keys) < 0;
  });
}

}  // namespace tinylamb
