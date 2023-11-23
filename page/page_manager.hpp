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

#ifndef TINYLAMB_PAGE_MANAGER_HPP
#define TINYLAMB_PAGE_MANAGER_HPP

#include <string_view>

#include "common/log_message.hpp"
#include "page/page.hpp"
#include "page/page_pool.hpp"
#include "recovery/recovery_manager.hpp"

namespace tinylamb {

class Logger;
class RecoveryManager;
class MetaPage;
class Transaction;
class TransactionManager;

class PageManager {
  static constexpr page_id_t kMetaPageId = 0;

 public:
  PageManager(std::string_view db_name, size_t capacity);

  PageRef GetPage(page_id_t page_id);

  PageRef AllocateNewPage(Transaction& txn, PageType new_page_type);

  // Logically delete the page.
  void DestroyPage(Transaction& txn, Page* target);

  PagePool* GetPool() { return &pool_; }

 private:
  PageRef GetMetaPage();

  friend class RecoveryManager;
  PagePool pool_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_PAGE_MANAGER_HPP
