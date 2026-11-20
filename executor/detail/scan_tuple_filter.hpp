/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_DETAIL_SCAN_TUPLE_FILTER_HPP
#define TINYLAMB_EXECUTOR_DETAIL_SCAN_TUPLE_FILTER_HPP

#include <array>
#include <bit>
#include <memory>
#include <span>
#include <string>
#include <vector>

#include "common/runtime_filter.hpp"
#include "executor/detail/relation.hpp"
#include "table/lstable_bridge.hpp"

namespace tinylamb::relational_detail {

// A necessary match domain, never a substitute for the join. Bound to the
// immutable scan image used to prove that pruning whole groups cannot hide
// evaluation errors. A changed/unavailable image must ignore this filter.
struct ScanTupleFilter {
  std::shared_ptr<QueryMemoryCharge> charge;
  std::string source;
  std::shared_ptr<const LSTableStore> store;
  std::vector<slot_t> columns;
  std::shared_ptr<const BloomRuntimeFilter> bloom;

  static int64_t Fingerprint(std::span<const int64_t> values) {
    uint64_t hash = 0x9e3779b97f4a7c15ULL;
    for (int64_t value : values) {
      hash ^= BloomRuntimeFilter::Hash(value, 0);
      hash *= 0x100000001b3ULL;
    }
    return std::bit_cast<int64_t>(hash);
  }

  [[nodiscard]] bool Reject(const LSTablePageBlock& block, size_t row) const {
    if (!bloom || columns.empty() || columns.size() > 4) {
      return false;
    }
    std::array<int64_t, 4> key{};
    // Validate the complete domain before rejecting even a NULL key.
    for (slot_t column : columns) {
      if (column >= block.columns.size() ||
          block.columns[column].type != ValueType::kInt64 ||
          block.columns[column].is_unsigned) {
        return false;
      }
    }
    for (size_t i = 0; i < columns.size(); ++i) {
      const auto& column = block.columns[columns[i]];
      if (column.IsNull(row)) {
        // NULL cannot match an equality key but may be an outer-join
        // unmatched row; keep it and let the join operator decide.
        return false;
      }
      key[i] = column.IntAt(row);
    }
    return bloom->Reject(Fingerprint(std::span(key).first(columns.size())));
  }
};

}  // namespace tinylamb::relational_detail
#endif
