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

#ifndef TINYLAMB_RECOVERY_MANAGER_HPP
#define TINYLAMB_RECOVERY_MANAGER_HPP

#include <fstream>
#include <string>
#include <string_view>

#include "common/constants.hpp"

namespace tinylamb {

class Page;
class PageManager;
class PagePool;
class PageRef;
class Transaction;
class TransactionManager;
struct LogRecord;

class RecoveryManager {
 public:
  RecoveryManager(std::string_view log_path, PagePool* pp);

  void SinglePageRecovery(PageRef&& page, TransactionManager* tm);

  void RecoverFrom(lsn_t checkpoint_lsn, TransactionManager* tm);

  bool ReadLog(lsn_t lsn, LogRecord* dst) const;

  void LogUndoWithPage(lsn_t lsn, const LogRecord& log, TransactionManager* tm);

 private:
  std::string log_name_;
  std::ifstream log_file_;
  PagePool* pool_{nullptr};
};

}  // namespace tinylamb

#endif  // TINYLAMB_RECOVERY_MANAGER_HPP
