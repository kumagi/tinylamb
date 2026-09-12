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
// Created by kumagi on 22/06/30.
//

#include "index_only_scan.hpp"

#include <ostream>
#include <utility>
#include <vector>

#include "common/status_or.hpp"
#include "expression/expression.hpp"
#include "index/index.hpp"
#include "index/index_scan_iterator.hpp"
#include "index/index_schema.hpp"
#include "page/row_position.hpp"
#include "type/column.hpp"

namespace tinylamb {

IndexOnlyScan::IndexOnlyScan(Transaction& txn, const Table& table,
                             const Index& index, const Value& begin,
                             const Value& end, bool ascending, Expression where,
                             const Schema& sc)
    : IndexOnlyScan(
          txn, table, index,
          begin.IsNull() ? std::vector<Value>{} : std::vector<Value>{begin},
          end.IsNull() ? std::vector<Value>{} : std::vector<Value>{end},
          ascending, std::move(where), sc) {}

IndexOnlyScan::IndexOnlyScan(Transaction& txn, const Table& table,
                             const Index& index,
                             const std::vector<Value>& begin_key,
                             const std::vector<Value>& end_key, bool ascending,
                             Expression where, const Schema& sc)
    : iter_(table, index, txn, begin_key, end_key, ascending),
      cond_(std::move(where)),
      key_schema_(KeySchema(index, sc)),
      value_schema_(ValueSchema(index, sc)),
      output_schema_(OutputSchema(index, sc)),
      include_offsets_(index.sc_.include_),
      ascending_(ascending) {}

Schema IndexOnlyScan::KeySchema(const Index& idx, const Schema& input_schema) {
  const IndexSchema& is = idx.sc_;
  std::vector<Column> cols;
  cols.reserve(is.key_.size());
  for (const auto& key : is.key_) {
    cols.push_back(input_schema.GetColumn(key));
  }
  return {"", cols};
}

Schema IndexOnlyScan::ValueSchema(const Index& idx,
                                  const Schema& input_schema) {
  const IndexSchema& is = idx.sc_;
  std::vector<Column> cols;
  cols.reserve(is.include_.size());
  for (const auto& key : is.include_) {
    cols.push_back(input_schema.GetColumn(key));
  }
  return {"", cols};
}

Schema IndexOnlyScan::OutputSchema(const Index& idx,
                                   const Schema& input_schema) {
  const IndexSchema& is = idx.sc_;
  std::vector<Column> cols;
  cols.reserve(is.key_.size() + is.include_.size());
  for (const auto& key : is.key_) {
    cols.push_back(input_schema.GetColumn(key));
  }
  for (const auto& included : is.include_) {
    cols.push_back(input_schema.GetColumn(included));
  }
  return {"", cols};
}

bool IndexOnlyScan::Next(Row* dst, RowPosition* /*rp*/) {
  while (iter_.IsValid()) {
    // Heap visibility: uncommitted or snapshot-invisible index entries must
    // not leak through INCLUDE columns that are not versioned.
    const Row& heap_row = *iter_;
    if (!heap_row.IsValid()) {
      ++iter_;
      continue;
    }
    if (include_offsets_.empty()) {
      *dst = iter_.GetKey();
    } else {
      Row inc;
      inc.values_.reserve(include_offsets_.size());
      for (slot_t offset : include_offsets_) {
        inc.values_.push_back(heap_row[offset]);
      }
      *dst = iter_.GetKey() + std::move(inc);
    }
    ++iter_;
    if (!dst->IsValid()) {
      continue;
    }
    if (cond_) {
      StatusOr<Value> res = cond_->TryEvaluate(*dst, output_schema_);
      if (!res.HasValue()) {
        return FailWith(res.GetStatus());
      }
      if (!res.Value().Truthy()) {
        continue;
      }
    }
    return true;
  }
  return false;
}

void IndexOnlyScan::Dump(std::ostream& o, int /*indent*/) const {
  o << "IndexOnlyScan: " << iter_;
  if (!ascending_) {
    o << " reverse";
  }
  if (cond_) {
    o << " WHERE " << *cond_;
  }
}

}  // namespace tinylamb
