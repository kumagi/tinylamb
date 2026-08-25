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

//  Created by kumagi on 2022/02/21.
//

#include "executor/full_scan.hpp"

#include <algorithm>
#include <cstddef>
#include <ostream>

#include "executor/data_chunk.hpp"
#include "table/table.hpp"

namespace tinylamb {
FullScan::FullScan(Transaction& txn, const Table& table, size_t max_rows)
    : table_(&table), iter_(table_->BeginFullScan(txn)), max_rows_(max_rows) {}

bool FullScan::Next(Row* dst, RowPosition* rp) {
  if (!iter_.IsValid() || emitted_ >= max_rows_) {
    return false;
  }
  *dst = *iter_;
  if (rp != nullptr) {
    *rp = iter_.Position();
  }
  ++iter_;
  ++emitted_;
  return true;
}

size_t FullScan::NextBatch(DataChunk* destination, size_t max_rows) {
  const size_t remaining = max_rows_ - std::min(emitted_, max_rows_);
  const size_t batch_limit = std::min(max_rows, remaining);
  destination->Reset(table_->GetSchema(), batch_limit);
  while (iter_.IsValid() && emitted_ < max_rows_ &&
         destination->Size() < batch_limit) {
    destination->Append(*iter_, iter_.Position());
    ++iter_;
    ++emitted_;
  }
  return destination->Size();
}

void FullScan::Dump(std::ostream& o, int /*indent*/) const {
  o << "FullScan: " << table_->GetSchema().Name();
  if (max_rows_ != std::numeric_limits<size_t>::max()) {
    o << " (max rows: " << max_rows_ << ")";
  }
}
}  // namespace tinylamb
