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
    evicted_.store(false, std::memory_order_release);
    referenced_.store(false, std::memory_order_relaxed);
    // A reused Page object must not inherit the previous generation's
    // eviction/accounting state, nor a stale exclusive latch owner. Recovery
    // and abort paths call PageInit without a PageRef, so a leftover thread
    // id would make IsExclusivelyOwnedByCurrentThread() spuriously true and
    // cause lock_page_if_needed to skip the content latch (data race).
    exclusive_owner_.store(std::thread::id{}, std::memory_order_relaxed);
    in_eviction_queue_ = false;
    accounted_bytes_ = 0;
    accounted_emergency_bytes_ = 0;
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

  Status SetNextPage(Transaction& txn, page_id_t next) {
    return body.row_page.SetNextPage(page_id_, txn, next);
  }
  Status SetPrevPage(Transaction& txn, page_id_t prev) {
    return body.row_page.SetPrevPage(page_id_, txn, prev);
  }

  [[nodiscard]] PageType Type() const { return type_; }
  [[nodiscard]] page_id_t PageID() const { return page_id_; }
  [[nodiscard]] lsn_t PageLSN() const { return page_lsn_; }
  // LSN fields are read by checkpoint liveness snapshots (max_dirty_lsn).
  // Callers must hold the owning PageRef's latch while using these setters;
  // the fields are deliberately plain (not atomic) and are not
  // self-synchronized.
  void SetPageLSN(lsn_t new_lsn) { page_lsn_ = new_lsn; }
  void SetRecLSN(lsn_t new_lsn) {
    if (recovery_lsn_ == 0) {
      recovery_lsn_ = new_lsn;
    } else {
      recovery_lsn_ = std::min(recovery_lsn_, new_lsn);
    }
  }
  // recLSN is monotonic (SetRecLSN keeps the minimum); no reset accessor.
  // Note: integrity CRCs live on the serialized block (page_manager.cpp), not
  // on the in-memory Page; there is no page-level SetChecksum API.
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

  [[nodiscard]] bool IsEvicted() const noexcept {
    return evicted_.load(std::memory_order_acquire);
  }
  void SetEvicted(bool evicted) noexcept {
    evicted_.store(evicted, std::memory_order_release);
  }
  void MarkReferenced() noexcept {
    referenced_.store(true, std::memory_order_relaxed);
  }
  bool TakeReferenced() noexcept {
    return referenced_.exchange(false, std::memory_order_relaxed);
  }

  PageType type_{PageType::kUnknown};
  page_id_t page_id_{0};
  lsn_t page_lsn_{0};
  lsn_t recovery_lsn_{0};
  size_t accounted_bytes_{0};
  size_t accounted_emergency_bytes_{0};
  bool in_eviction_queue_{false};

  struct Body {
    RowPage row_page;
  } body;

 private:
  mutable std::shared_mutex latch_;
  mutable std::atomic<std::thread::id> exclusive_owner_{};
  std::atomic<bool> evicted_{false};
  std::atomic<bool> referenced_{false};
};

}  // namespace tinylamb

#endif  // TINYLAMB_PAGE_HPP
