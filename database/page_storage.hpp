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
#include "transaction/transaction_manager.hpp"

namespace tinylamb {

// NOLINTNEXTLINE(clang-analyzer-optin.performance.Padding): ownership
// order (logger, pool, catalog managers) documents the teardown order.
class PageStorage {
 public:
  // Opens the WAL + page files and runs recovery. Construction failures that
  // previously escaped as exceptions surface as Status.
  static StatusOr<std::unique_ptr<PageStorage>> Create(std::string_view dbname,
                                                       size_t wal_sync_ms = 1);
  PageStorage(const PageStorage&) = delete;
  PageStorage& operator=(const PageStorage&) = delete;

  Transaction Begin();
  Transaction BeginReadOnly();
  [[nodiscard]] std::string DBName() const;
  [[nodiscard]] std::string LogName() const;
  [[nodiscard]] std::string MasterRecordName() const;

  void DiscardAllUpdates();

  friend std::ostream& operator<<(std::ostream& o, const PageStorage& ps);

 private:
  friend class Database;

  // Construction follows declaration order; destruction reverses it. Both
  // directions are LOAD-BEARING -- do not reorder these members:
  //  * logger_ is constructed before rm_: RecoveryManager replays the WAL
  //    file, so Logger must have created/opened it first (page_storage.cpp
  //    relies on this at startup).
  //  * tm_/cm_ hold raw pointers into pm_(pool)/logger_/rm_.
  //  * cm_ is declared LAST on purpose: ~CheckpointManager() joins its
  //    background worker, which keeps dereferencing tm_/pp_/logger_ until
  //    that join completes. Any other order risks use-after-free at
  //    shutdown or a failed startup.
  PageStorage(std::string_view dbname, std::unique_ptr<Logger> logger,
              std::unique_ptr<PageManager> pm,
              std::unique_ptr<RecoveryManager> rm,
              std::unique_ptr<TransactionManager> tm,
              std::unique_ptr<CheckpointManager> cm);

  std::string dbname_;
  std::unique_ptr<Logger> logger_;
  std::unique_ptr<PageManager> pm_;
  // rm_/tm_/cm_ keep raw pointers into logger_/pm_, so they must be
  // constructed after them and destroyed before them (declaration order).
  std::unique_ptr<RecoveryManager> rm_;
  std::unique_ptr<TransactionManager> tm_;
  std::unique_ptr<CheckpointManager> cm_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_PAGE_STORAGE_HPP
