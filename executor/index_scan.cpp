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
// Created by kumagi on 22/06/19.
//

#include "index_scan.hpp"

#include <ostream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "expression/expression.hpp"
#include "index/index.hpp"
#include "index/index_scan_iterator.hpp"
#include "page/row_position.hpp"
#include "table/table.hpp"
#include "transaction/transaction.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {

IndexScan::IndexScan(Transaction& txn, const Table& table, const Index& index,
                     const Value& begin, const Value& end, bool ascending,
                     Expression where, const Schema& sc, bool lock_rows,
                     bool wait_for_write_intent)
    : IndexScan(txn, table, index,
                begin.IsNull() ? std::vector<Value>{}
                               : std::vector<Value>{begin},
                end.IsNull() ? std::vector<Value>{} : std::vector<Value>{end},
                ascending, std::move(where), sc, lock_rows,
                wait_for_write_intent) {}

IndexScan::IndexScan(Transaction& txn, const Table& table, const Index& index,
                     const std::vector<Value>& begin_key,
                     const std::vector<Value>& end_key,
                     bool ascending, Expression where, Schema sc,
                     bool lock_rows, bool wait_for_write_intent)
    : txn_(txn),
      table_(table),
      index_(index),
      ascending_(ascending),
      lock_rows_(lock_rows),
      wait_for_write_intent_(wait_for_write_intent),
      iter_(new IndexScanIterator(table, index, txn, begin_key, end_key,
                                  ascending)),
      cond_(std::move(where)),
      schema_(std::move(sc)) {}

IndexScan::IndexScan(
    Transaction& txn, const Table& table, const Index& index,
    std::vector<std::pair<std::vector<Value>, std::vector<Value>>> ranges,
    bool ascending, Expression where, Schema sc, bool lock_rows,
    bool wait_for_write_intent)
    : txn_(txn),
      table_(table),
      index_(index),
      ascending_(ascending),
      lock_rows_(lock_rows),
      wait_for_write_intent_(wait_for_write_intent),
      // Contract: at least one range. Callers build one entry per distinct
      // IN value.
      iter_(new IndexScanIterator(
          table, index, txn,
          ranges.empty() ? std::vector<Value>{} : ranges.front().first,
          ranges.empty() ? std::vector<Value>{} : ranges.front().second,
          ascending)),
      cond_(std::move(where)),
      schema_(std::move(sc)) {
  range_count_ = ranges.size();
  if (!ranges.empty()) {
    pending_.assign(std::make_move_iterator(ranges.begin()) + 1,
                    std::make_move_iterator(ranges.end()));
  }
}

void IndexScan::OpenRange(const std::vector<Value>& begin_key,
                          const std::vector<Value>& end_key) {
  iter_ = Iterator(new IndexScanIterator(table_, index_, txn_, begin_key,
                                         end_key, ascending_));
}

bool IndexScan::Next(Row* dst, RowPosition* rp) {
  for (;;) {
    while (!iter_.IsValid()) {
      if (pending_offset_ == pending_.size()) { return false;
}
      auto range = std::move(pending_[pending_offset_++]);
      OpenRange(range.first, range.second);
    }
    const RowPosition pointed_row = iter_.Position();
    const bool locked = !lock_rows_ ||
                        (wait_for_write_intent_
                             ? txn_.AddWriteSet(pointed_row)
                             : txn_.TryAddWriteSet(pointed_row));
    if (!locked) {
      throw std::runtime_error("write intent wait timed out on table " +
                               std::string(table_.GetSchema().Name()));
    }
    *dst = *iter_;
    ++iter_;
    if (!dst->IsValid()) { continue;
}
    if (rp != nullptr) { *rp = pointed_row;
}
    if (cond_ && !cond_->Evaluate(*dst, schema_).Truthy()) { continue;
}
    return true;
  }
}

void IndexScan::Dump(std::ostream& o, int /*indent*/) const {
  if (range_count_ > 1) {
    o << "IndexScan: " << table_.GetSchema().Name() << " x" << range_count_
      << " points";
  } else {
    o << "IndexScan: " << iter_;
  }
  if (!ascending_) { o << " reverse"; }
  if (cond_) {
    o << " WHERE " << *cond_;
  }
}

}  // namespace tinylamb
