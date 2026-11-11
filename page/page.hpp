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

#ifndef TINYLAMB_PAGE_HPP
#define TINYLAMB_PAGE_HPP

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <ostream>
#include <shared_mutex>
#include <string_view>
#include <thread>

#include "common/constants.hpp"
#include "common/status_or.hpp"
#include "page/page_type.hpp"
#include "page/row_page.hpp"

namespace tinylamb {

class Transaction;

// Logical row block container (unbounded in-memory/LSM block).
class Page {
 public:
  Page(page_id_t pid, PageType type) { PageInit(pid, type); }
  Page() = default;

  void PageInit(page_id_t pid, PageType type) {
    type_ = type;
    page_id_ = pid;
    page_lsn_ = 0;
    recovery_lsn_ = 0;
    body.row_page.Initialize();
  }

  StatusOr<std::string_view> Read(Transaction& txn, slot_t slot,
                                  bool resolve_head = true) const {
    return body.row_page.Read(page_id_, txn, slot, resolve_head);
  }
  StatusOr<slot_t> Insert(Transaction& txn, std::string_view record) {
    return body.row_page.Insert(page_id_, txn, record);
  }
  Status Update(Transaction& txn, slot_t slot, std::string_view record) {
    return body.row_page.Update(page_id_, txn, slot, record);
  }
  Status Delete(Transaction& txn, slot_t slot) {
    return body.row_page.Delete(page_id_, txn, slot);
  }

  [[nodiscard]] slot_t RowCount() const { return body.row_page.RowCount(); }

  Status SetNextPageId(Transaction& txn, page_id_t next) {
    return body.row_page.SetNextPage(page_id_, txn, next);
  }
  Status SetPrevPageId(Transaction& txn, page_id_t prev) {
    return body.row_page.SetPrevPage(page_id_, txn, prev);
  }
  Status SetNextPage(Transaction& txn, page_id_t next) {
    return SetNextPageId(txn, next);
  }
  Status SetPrevPage(Transaction& txn, page_id_t prev) {
    return SetPrevPageId(txn, prev);
  }

  [[nodiscard]] PageType Type() const { return type_; }
  [[nodiscard]] page_id_t PageID() const { return page_id_; }
  [[nodiscard]] lsn_t PageLSN() const { return page_lsn_; }
  [[nodiscard]] lsn_t RecoveryLSN() const { return recovery_lsn_; }
  void SetPageLSN(lsn_t new_lsn) { page_lsn_ = new_lsn; }
  void SetRecLSN(lsn_t new_lsn) {
    if (recovery_lsn_ == 0) {
      recovery_lsn_ = new_lsn;
    } else {
      recovery_lsn_ = std::min(recovery_lsn_, new_lsn);
    }
  }
  void ResetRecLSN(lsn_t new_lsn) { recovery_lsn_ = new_lsn; }
  void SetChecksum() {}
  [[nodiscard]] bool IsValid() const { return type_ == PageType::kRowPage; }
  [[nodiscard]] std::shared_mutex* Latch() const { return &latch_; }
  void SetExclusiveOwner(std::thread::id id) const {
    exclusive_owner_.store(id, std::memory_order_relaxed);
  }
  void ClearExclusiveOwner() const {
    exclusive_owner_.store(std::thread::id{}, std::memory_order_relaxed);
  }
  [[nodiscard]] bool IsExclusivelyOwnedByCurrentThread() const {
    return exclusive_owner_.load(std::memory_order_relaxed) ==
           std::this_thread::get_id();
  }

  void Dump(std::ostream& o, int indent = 0) const;
  friend std::ostream& operator<<(std::ostream& o, const Page& p) {
    p.Dump(o, 0);
    return o;
  }

  PageType type_{PageType::kUnknown};
  page_id_t page_id_{0};
  lsn_t page_lsn_{0};
  lsn_t recovery_lsn_{0};

  struct Body {
    RowPage row_page;
  } body;

 private:
  mutable std::shared_mutex latch_;
  mutable std::atomic<std::thread::id> exclusive_owner_{};
};

}  // namespace tinylamb

#endif  // TINYLAMB_PAGE_HPP
