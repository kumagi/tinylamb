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
// Created by kumagi on 22/05/04.
//

#include "page_storage.hpp"

#include <cstddef>
#include <string_view>

#include "transaction/transaction.hpp"

namespace tinylamb {

PageStorage::PageStorage(std::string_view dbname)
    : dbname_(dbname),
      logger_(LogName(), static_cast<size_t>(8 * 1024 * 1024), 1),
      pm_(DBName(), 1024),
      rm_(LogName(), pm_.GetPool()),
      tm_(&lm_, &pm_, &logger_, &rm_),
      cm_(MasterRecordName(), &tm_, pm_.GetPool()) {
  rm_.RecoverFrom(0, &tm_);
}

void PageStorage::DiscardAllUpdates() { pm_.GetPool()->DropAllPages(); }

Transaction PageStorage::Begin() { return tm_.Begin(); }

std::string PageStorage::DBName() const { return dbname_ + ".db"; }
std::string PageStorage::LogName() const { return dbname_ + ".log"; }
std::string PageStorage::MasterRecordName() const {
  return dbname_ + ".last_checkpoint";
}

}  // namespace tinylamb