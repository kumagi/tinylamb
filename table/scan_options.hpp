/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_TABLE_SCAN_OPTIONS_HPP
#define TINYLAMB_TABLE_SCAN_OPTIONS_HPP

#include <optional>
#include <unordered_set>
#include <vector>

#include "common/constants.hpp"
#include "table/full_scan_iterator.hpp"

namespace tinylamb {

struct TableScanOptions {
  std::optional<std::vector<slot_t>> projection;
  const std::unordered_set<int64_t>* key_filter = nullptr;
  std::optional<slot_t> key_column = std::nullopt;
  const std::vector<IntegerPeekCompare>* peek_compares = nullptr;
  // DML source scans set lock_rows: every occupied slot is write-intent
  // locked BEFORE its row is resolved, so the yielded image is the head
  // version (newest committed) rather than the transaction's snapshot
  // version.  wait_for_write_intent picks AddWriteSet (wait) over
  // TryAddWriteSet (fail fast on a foreign pending intent).
  bool lock_rows = false;
  bool wait_for_write_intent = true;
};

}  // namespace tinylamb

#endif  // TINYLAMB_TABLE_SCAN_OPTIONS_HPP
