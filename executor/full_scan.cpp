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

#include <cstddef>
#include <ostream>

#include "executor/data_chunk.hpp"
#include "table/table.hpp"

namespace tinylamb {
FullScan::FullScan(Transaction& txn, const Table& table)
    : table_(&table), iter_(table_->BeginFullScan(txn)) {}

bool FullScan::Next(Row* dst, RowPosition* rp) {
  if (!iter_.IsValid()) {
    return false;
  }
  *dst = *iter_;
  if (rp != nullptr) {
    *rp = iter_.Position();
  }
  ++iter_;
  return true;
}

size_t FullScan::NextBatch(DataChunk* destination, size_t max_rows) {
  destination->Reset(table_->GetSchema(), max_rows);
  while (iter_.IsValid() && destination->Size() < max_rows) {
    destination->Append(*iter_, iter_.Position());
    ++iter_;
  }
  return destination->Size();
}

void FullScan::Dump(std::ostream& o, int /*indent*/) const {
  o << "FullScan: " << table_->GetSchema().Name();
}
}  // namespace tinylamb
