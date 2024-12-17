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
#include <utility>

#include "expression/expression.hpp"
#include "index/index.hpp"
#include "index/index_scan_iterator.hpp"
#include "page/row_position.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {

IndexScan::IndexScan(Transaction& txn, const Table& table, const Index& index,
                     const Value& begin, const Value& end, bool ascending,
                     Expression where, const Schema& sc)
    : iter_(new IndexScanIterator(table, index, txn, begin, end, ascending)),
      cond_(std::move(where)),
      schema_(sc) {}

bool IndexScan::Next(Row* dst, RowPosition* rp) {
  do {
    if (!iter_.IsValid()) {
      return false;
    }
    RowPosition pointed_row = iter_.Position();
    if (rp != nullptr) {
      *rp = pointed_row;
    }
    *dst = *iter_;
    ++iter_;
  } while (!cond_->Evaluate(*dst, schema_).Truthy());
  return true;
}

void IndexScan::Dump(std::ostream& o, int /*indent*/) const {
  o << "IndexScan: " << iter_ << " WHERE " << *cond_;
}

}  // namespace tinylamb