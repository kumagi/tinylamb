/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_DETAIL_GROUPED_OUTPUT_SINK_HPP
#define TINYLAMB_EXECUTOR_DETAIL_GROUPED_OUTPUT_SINK_HPP

#include <cstddef>
#include <functional>
#include <memory>
#include <span>

#include "common/status_or.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb::relational_detail {

struct ScanTupleFilter;

// A synchronous grouped-output boundary. Begin may decline before consuming
// anything (e.g. insufficient retained-state budget). Push borrows row-major
// cells for this call only, never owns Rows, and may run on different workers
// concurrently. Each worker receives a contiguous range in encounter order;
// `first` is the global group offset. All pushes finish before ExecuteQuery
// returns. Consumers must retain/copy cells they need beyond Push.
struct GroupedOutputSink {
  static constexpr size_t kBatchRows = 1024;
  std::function<StatusOr<bool>(const Schema&, size_t rows, size_t workers)>
      begin;
  std::function<Status(size_t worker, size_t first, std::span<const Value>)>
      push;
  bool used{false};
  std::shared_ptr<const ScanTupleFilter> input_filter{};
};

}  // namespace tinylamb::relational_detail
#endif
