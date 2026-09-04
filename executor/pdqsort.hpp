/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_PDQSORT_HPP
#define TINYLAMB_EXECUTOR_PDQSORT_HPP

#include <algorithm>
#include <cstddef>
#include <utility>
#include <vector>

#include "executor/sort.hpp"
#include "page/row_position.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {

// In-memory fast path pattern-defeating quicksort for in-memory row
// collections.
class PdqSort {
 public:
  static void Sort(std::vector<std::pair<Row, RowPosition>>& rows,
                   const Schema& schema,
                   const std::vector<SortExecutor::Key>& keys);

  static void Sort(std::vector<Row>& rows, const Schema& schema,
                   const std::vector<SortExecutor::Key>& keys);
};

}  // namespace tinylamb

#endif  // TINYLAMB_EXECUTOR_PDQSORT_HPP
