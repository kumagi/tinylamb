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

#ifndef TINYLAMB_PAGE_STORAGE_HPP
#define TINYLAMB_PAGE_STORAGE_HPP

#include "database/transaction_context.hpp"
#include "page/page_manager.hpp"
#include "recovery/checkpoint_manager.hpp"
#include "recovery/logger.hpp"
#include "transaction/lock_manager.hpp"
#include "transaction/transaction_manager.hpp"

namespace tinylamb {

class PageStorage {
 public:
  explicit PageStorage(std::string_view dbname);

  Transaction Begin();
  std::string DBName() const;
  std::string LogName() const;
  std::string MasterRecordName() const;

  void DiscardAllUpdates();

 private:
  friend class Database;
  friend class Database;

  std::string dbname_;
  LockManager lm_;
  Logger logger_;
  PageManager pm_;
  RecoveryManager rm_;
  TransactionManager tm_;
  CheckpointManager cm_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_PAGE_STORAGE_HPP
