/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_PDQSORT_HPP
#define TINYLAMB_EXECUTOR_PDQSORT_HPP

#include <algorithm>
#include <cstddef>
#include <utility>
#include <vector>

#include "common/status_or.hpp"
#include "executor/sort.hpp"
#include "page/row_position.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {

// Historical name for the materialized-key sorting adapter. The current
// implementation delegates ordering to std::sort; it does not promise a
// particular standard-library sorting algorithm.
class PdqSort {
 public:
  // `error` receives the first key-evaluation failure (comparators cannot
  // abort mid-sort; callers must check it afterwards).
  static void Sort(std::vector<std::pair<Row, RowPosition>>& rows,
                   const Schema& schema,
                   const std::vector<SortExecutor::Key>& keys,
                   Status* error = nullptr);

  static void Sort(std::vector<Row>& rows, const Schema& schema,
                   const std::vector<SortExecutor::Key>& keys,
                   Status* error = nullptr);
};

}  // namespace tinylamb

#endif  // TINYLAMB_EXECUTOR_PDQSORT_HPP
