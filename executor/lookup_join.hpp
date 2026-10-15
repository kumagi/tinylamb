/**
 * Copyright 2023 KUMAZAKI Hiroki
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//
// Created by kumagi on 22/07/14.
//

#ifndef TINYLAMB_LOOKUP_JOIN_HPP
#define TINYLAMB_LOOKUP_JOIN_HPP

#include <optional>
#include <vector>

#include "common/constants.hpp"
#include "executor/executor_base.hpp"
#include "index/index.hpp"
#include "index/index_scan_iterator.hpp"

namespace tinylamb {
class Table;
class Transaction;

// Index nested-loop join (lookup join).
//
// For every left row the executor builds a seek key from `left_cols` and
// probes the right table's B+Tree through `index`, emitting
// `left + right` for each candidate that satisfies the join keys.  The two
// column vectors are aligned with the index's leading key columns -- NOT with
// the order the equi-join happens to list its keys in -- because the seek key
// must be a prefix of the stored key image:
//
//   * positions [0, range_position) are equality-pinned: they become the seek
//     key prefix and are re-checked with `kEquals` on every candidate.
//   * when `range_position` is set, that position carries the range bound.
//     The probe opens the half-open index range
//     `prefix <= key < prefix + bound` (or `prefix + bound <= key < prefix`
//     for the mirrored operators) and re-checks the comparison
//     `right_cols[range_position] <range_operation> left_cols[range_position]`
//     per candidate, so inclusive/exclusive boundary rows are always
//     resolved by the predicate, never by the seek.
//
// `range_position` == the equality-prefix length, and the whole probe degrades
// to a prefix point lookup when it is std::nullopt.  Anything the seek cannot
// decide -- a NULL, NaN or cross-typed bound, a UINT64-tagged INT64 that
// encodes into a different key space, a value whose type is outside the range
// whitelist -- falls back per row to the wider prefix scan; the re-check then
// rejects it, so the fallback only costs a wider scan, never a wrong row.
class LookupJoin : public ExecutorBase {
 public:
  LookupJoin(Transaction& txn, Executor left, std::vector<slot_t> left_cols,
             const Table& tbl, const Index& idx, std::vector<slot_t> right_cols,
             std::optional<size_t> range_position = std::nullopt,
             BinaryOperation range_operation = BinaryOperation::kLessThan,
             TransactionManager::IndexReadGuard index_read_guard = {});
  LookupJoin(const LookupJoin&) = delete;
  LookupJoin(LookupJoin&&) = delete;
  LookupJoin& operator=(const LookupJoin&) = delete;
  LookupJoin& operator=(LookupJoin&&) = delete;
  ~LookupJoin() override = default;

  bool Next(Row* dst, RowPosition* /* rp */) override;
  void Dump(std::ostream& o, int indent) const override;

 private:
  // Positions the next left row into a new index probe. Returns false at
  // end-of-input or on failure (with the sticky status recorded).
  bool Load();
  // Builds the half-open [begin, end) seek range for `left_key`.
  void BuildSeekRange(const Row& left_key);
  // Re-checks one candidate row against the join keys.
  [[nodiscard]] bool Matches(const Row& left_key, const Row& right_key) const;

  Transaction& txn_;
  Executor left_;
  std::vector<slot_t> left_cols_;
  const Table& right_;
  const Index& right_idx_;
  Row hold_left_;
  TransactionManager::IndexReadGuard index_read_guard_;
  std::unique_ptr<IndexScanIterator> right_it_;
  std::vector<slot_t> right_cols_;
  std::optional<size_t> range_position_;
  BinaryOperation range_operation_{BinaryOperation::kLessThan};
  // Declared type of the index key column at `range_position_`; kNull when
  // there is no range column.
  ValueType range_key_type_{ValueType::kNull};
  std::vector<Value> begin_key_;
  std::vector<Value> end_key_;
  bool at_most_one_{false};
};
}  // namespace tinylamb

#endif  // TINYLAMB_LOOKUP_JOIN_HPP
