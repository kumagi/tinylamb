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

#include "page_manager.hpp"

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <mutex>
#include <stdexcept>
#include <string>
#include <string_view>

#include "common/constants.hpp"
#include "page/meta_page.hpp"
#include "page/page_ref.hpp"
#include "page_type.hpp"
#include "recovery/log_record.hpp"
#include "recovery/recovery_manager.hpp"
#include "transaction/transaction.hpp"
#include "transaction/transaction_manager.hpp"

namespace tinylamb {

StatusOr<std::unique_ptr<PageManager>> PageManager::Create(
    std::string_view db_name, size_t capacity, bool initialize_meta) {
  ASSIGN_OR_RETURN(std::unique_ptr<PagePool>, pool,
                   PagePool::Create(db_name, capacity));
  // NOLINTNEXTLINE(clang-analyzer-cplusplus.NewDeleteLeaks)
  auto manager = std::unique_ptr<PageManager>(
      new PageManager(db_name, capacity, std::move(pool)));  // NOLINT
  if (initialize_meta) {
    RETURN_IF_FAIL(manager->GetMetaPage().GetStatus());
  }
  return manager;
}

PageManager::PageManager(std::string_view /*db_name*/, size_t /*capacity*/,
                         std::unique_ptr<PagePool> pool)
    : pool_(std::move(pool)) {}

Status PageManager::InitializeMetaPage() {
  RETURN_IF_FAIL(GetMetaPage().GetStatus());
  return Status::kSuccess;
}

StatusOr<PageRef> PageManager::GetPage(uint64_t page_id, bool shared) {
  bool cache_hit = false;
  ASSIGN_OR_RETURN(PageRef, ref, pool_->GetPage(page_id, &cache_hit, shared));
  if (!cache_hit && !ref->IsValid()) {
    // Found a broken or new page: hand back the empty PageRef the existing
    // callers (BPlusTree bootstrap) test with IsValid().
    if (page_id > 3) {
      LOG(WARN) << "PMDIAG empty ref pid=" << page_id
                << " type=" << static_cast<int>(ref->Type())
                << " file_bytes=" << pool_->DebugDataFileSizeForDiag();
    }
    return PageRef{};
  }
  return ref;
}

page_id_t PageManager::GetTableTail(page_id_t first_page,
                                    page_id_t catalog_hint) {
  std::scoped_lock lock(table_tails_mu_);
  return table_tails_.try_emplace(first_page, TableTailState{catalog_hint})
      .first->second.published_tail;
}

page_id_t PageManager::GetTableTailFor(txn_id_t viewer, page_id_t first_page,
                                       page_id_t catalog_hint) {
  std::scoped_lock lock(table_tails_mu_);
  auto& state =
      table_tails_.try_emplace(first_page, TableTailState{catalog_hint})
          .first->second;
  if (state.pending.has_value() && state.pending->owner == viewer) {
    return state.pending->owner_tail;
  }
  return state.published_tail;
}

Status PageManager::TryReserveTableTail(txn_id_t owner, page_id_t first_page,
                                        page_id_t expected) {
  std::scoped_lock lock(table_tails_mu_);
  auto& state = table_tails_.try_emplace(first_page, TableTailState{expected})
                    .first->second;
  if (state.poisoned) {
    return StatusError(StatusCode::kIOError,
                       "table tail is poisoned by a failed transaction");
  }
  if (state.pending.has_value()) {
    if (state.pending->owner != owner ||
        state.pending->owner_tail != expected) {
      return Status::kConflicts;
    }
    return Status::kSuccess;
  }
  if (state.published_tail != expected) {
    return Status::kConflicts;
  }
  table_tail_reservations_[owner].insert(first_page);
  PendingTableTail pending;
  pending.owner = owner;
  pending.owner_tail = expected;
  state.pending = std::move(pending);
  return Status::kSuccess;
}

Status PageManager::ReserveTableTail(txn_id_t owner, page_id_t first_page,
                                     page_id_t expected) {
  return ReserveTableTailImpl(owner, first_page, expected, nullptr);
}

Status PageManager::ReserveTableTail(Transaction& txn, page_id_t first_page,
                                     page_id_t expected) {
  return ReserveTableTailImpl(txn.ID(), first_page, expected, &txn);
}

Status PageManager::WaitForTableTailPage(txn_id_t viewer, page_id_t first_page,
                                         page_id_t page_id) {
  return WaitForTableTailPageImpl(viewer, first_page, page_id, nullptr);
}

Status PageManager::WaitForTableTailPage(Transaction& txn, page_id_t first_page,
                                         page_id_t page_id) {
  return WaitForTableTailPageImpl(txn.ID(), first_page, page_id, &txn);
}

bool PageManager::OwnerHoldsForeignTailReservation(txn_id_t owner,
                                                   page_id_t first_page) const {
  const auto reservations_it = table_tail_reservations_.find(owner);
  if (reservations_it == table_tail_reservations_.end()) {
    return false;
  }
  return std::ranges::any_of(
      reservations_it->second,
      [first_page](page_id_t held) { return held != first_page; });
}

Status PageManager::ReserveTableTailImpl(txn_id_t owner, page_id_t first_page,
                                         page_id_t expected,
                                         Transaction* watcher) {
  constexpr auto kWaitLimit = std::chrono::seconds(5);
  const auto deadline = std::chrono::steady_clock::now() + kWaitLimit;
  // Sampled accounting, same discipline as WaitForTableTailPageImpl.
  const auto wait_started = std::chrono::steady_clock::now();
  const auto record_wait = [&] {
    const auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                        std::chrono::steady_clock::now() - wait_started)
                        .count();
    if (ns > 1'000'000) {
      tail_wait_count_.fetch_add(1, std::memory_order_relaxed);
      tail_wait_ns_.fetch_add(static_cast<uint64_t>(ns),
                              std::memory_order_relaxed);
    }
  };
  std::unique_lock lock(table_tails_mu_);
  for (;;) {
    const auto state_it = table_tails_.find(first_page);
    if (state_it == table_tails_.end()) {
      record_wait();
      return Status::kNotExists;
    }
    TableTailState& state = state_it->second;
    if (state.poisoned) {
      record_wait();
      return StatusError(StatusCode::kIOError,
                         "table tail is poisoned by a failed transaction");
    }
    if (state.pending.has_value() && state.pending->owner == owner) {
      if (state.pending->owner_tail == expected) {
        lock.unlock();
        const Status reserved =
            TryReserveTableTail(owner, first_page, expected);
        record_wait();
        return reserved;
      }
      // A stale self-reservation from an earlier walk of the same insert
      // (this owner has since followed the chain to a different
      // predecessor) can never be consumed by a link: retire it, as
      // CancelTableTailReservation would, so the fresh claim below proceeds
      // and waiters are not wedged behind a reservation nobody will link.
      state.pending.reset();
      const auto reservations_it = table_tail_reservations_.find(owner);
      if (reservations_it != table_tail_reservations_.end()) {
        reservations_it->second.erase(first_page);
        if (reservations_it->second.empty()) {
          table_tail_reservations_.erase(reservations_it);
        }
      }
      table_tails_cv_.notify_all();
    }
    if (!state.pending.has_value()) {
      if (state.published_tail != expected) {
        record_wait();
        return Status::kConflicts;
      }
      lock.unlock();
      const Status claimed = TryReserveTableTail(owner, first_page, expected);
      if (claimed != Status::kConflicts) {
        record_wait();
        return claimed;
      }
      lock.lock();
      continue;
    }
    // Reservation-cycle prevention: this caller already grows another
    // table. Waiting here risks the two-grower cycle (each holds the table
    // the other waits for); the wait-for graph cannot see it, so it would
    // burn the full wait limit on BOTH sides and then fail. Fail this
    // growth attempt immediately instead -- the statement unwinds, the
    // transaction releases its reservations, and the other grower proceeds.
    if (watcher != nullptr &&
        OwnerHoldsForeignTailReservation(owner, first_page)) {
      record_wait();
      return StatusError(
          StatusCode::kConflicts,
          "table tail reservation would cycle with a held reservation");
    }
    if (std::chrono::steady_clock::now() >= deadline) {
      record_wait();
      return StatusError(StatusCode::kConflicts,
                         "timed out waiting for table tail reservation");
    }
    // Sliced wait: a wounded transaction (deadlock-detector victim) must
    // abandon the growth wait so its abort -- and every row intent it
    // holds -- proceeds without riding the full limit.
    if (watcher != nullptr) {
      watcher->DebugSetPhase("tail-reserve-wait");
    }
    const auto wake_by = std::min(deadline, std::chrono::steady_clock::now() +
                                                std::chrono::milliseconds(25));
    table_tails_cv_.wait_until(lock, wake_by, [&] {
      const auto it = table_tails_.find(first_page);
      return it == table_tails_.end() || it->second.poisoned ||
             !it->second.pending.has_value() ||
             it->second.pending->owner == owner;
    });
    if (watcher != nullptr) {
      watcher->DebugSetPhase(nullptr);
      if (watcher->IsWounded()) {
        record_wait();
        return StatusError(StatusCode::kConflicts,
                           "transaction wounded while waiting for table tail");
      }
    }
  }
}

Status PageManager::LinkReservedTableTail(txn_id_t owner, page_id_t first_page,
                                          page_id_t predecessor,
                                          page_id_t successor) {
  std::scoped_lock lock(table_tails_mu_);
  const auto state_it = table_tails_.find(first_page);
  if (state_it == table_tails_.end()) {
    return Status::kNotExists;
  }
  auto& state = state_it->second;
  if (state.poisoned || !state.pending.has_value() ||
      state.pending->owner != owner ||
      state.pending->owner_tail != predecessor) {
    return Status::kConflicts;
  }
  state.pending->unpublished_pages.insert(successor);
  state.pending->owner_tail = successor;
  return Status::kSuccess;
}

Status PageManager::CheckTableTailPage(txn_id_t viewer, page_id_t first_page,
                                       page_id_t page_id) {
  std::scoped_lock lock(table_tails_mu_);
  const auto state_it = table_tails_.find(first_page);
  if (state_it == table_tails_.end()) {
    return Status::kSuccess;
  }
  const auto& state = state_it->second;
  if (state.poisoned) {
    return StatusError(StatusCode::kIOError,
                       "table tail is poisoned by a failed transaction");
  }
  if (!state.pending.has_value() || state.pending->owner == viewer) {
    return Status::kSuccess;
  }
  if (state.pending->unpublished_pages.contains(page_id)) {
    return Status::kConflicts;
  }
  return Status::kSuccess;
}

Status PageManager::WaitForTableTailPageImpl(txn_id_t viewer,
                                             page_id_t first_page,
                                             page_id_t page_id,
                                             Transaction* watcher) {
  constexpr auto kWaitLimit = std::chrono::seconds(5);
  const auto deadline = std::chrono::steady_clock::now() + kWaitLimit;
  // Sampled accounting: waits under 1ms are the uncontended majority and
  // record nothing (same discipline as the index gate).
  const auto wait_started = std::chrono::steady_clock::now();
  const auto record_wait = [&] {
    const auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                        std::chrono::steady_clock::now() - wait_started)
                        .count();
    if (ns > 1'000'000) {
      tail_wait_count_.fetch_add(1, std::memory_order_relaxed);
      tail_wait_ns_.fetch_add(static_cast<uint64_t>(ns),
                              std::memory_order_relaxed);
    }
  };
  std::unique_lock lock(table_tails_mu_);
  for (;;) {
    const auto state_it = table_tails_.find(first_page);
    if (state_it == table_tails_.end()) {
      record_wait();
      return Status::kNotExists;
    }
    const auto& state = state_it->second;
    if (state.poisoned) {
      record_wait();
      return StatusError(StatusCode::kIOError,
                         "table tail is poisoned by a failed transaction");
    }
    if (!state.pending.has_value() || state.pending->owner == viewer ||
        !state.pending->unpublished_pages.contains(page_id)) {
      record_wait();
      return Status::kSuccess;
    }
    if (std::chrono::steady_clock::now() >= deadline) {
      record_wait();
      return StatusError(StatusCode::kConflicts,
                         "timed out waiting for unpublished table tail");
    }
    // Sliced wait so a wounded transaction abandons promptly (see
    // ReserveTableTailImpl).
    if (watcher != nullptr) {
      watcher->DebugSetPhase("tail-publish-wait");
    }
    const auto wake_by = std::min(deadline, std::chrono::steady_clock::now() +
                                                std::chrono::milliseconds(25));
    table_tails_cv_.wait_until(lock, wake_by, [&] {
      const auto it = table_tails_.find(first_page);
      return it == table_tails_.end() || it->second.poisoned ||
             !it->second.pending.has_value() ||
             it->second.pending->owner == viewer ||
             !it->second.pending->unpublished_pages.contains(page_id);
    });
    if (watcher != nullptr) {
      watcher->DebugSetPhase(nullptr);
      if (watcher->IsWounded()) {
        record_wait();
        return StatusError(StatusCode::kConflicts,
                           "transaction wounded while waiting for table tail");
      }
    }
  }
}

Status PageManager::WaitForTableTailQuiescence(page_id_t first_page,
                                               txn_id_t requester) {
  constexpr auto kWaitLimit = std::chrono::seconds(5);
  const auto deadline = std::chrono::steady_clock::now() + kWaitLimit;
  std::unique_lock lock(table_tails_mu_);
  for (;;) {
    const auto state_it = table_tails_.find(first_page);
    if (state_it == table_tails_.end()) {
      return Status::kSuccess;
    }
    const auto& state = state_it->second;
    if (state.poisoned) {
      return StatusError(StatusCode::kIOError,
                         "table tail is poisoned by a failed transaction");
    }
    if (!state.pending.has_value()) {
      return Status::kSuccess;
    }
    if (state.pending->owner == requester) {
      return Status::kConflicts;
    }
    if (std::chrono::steady_clock::now() >= deadline) {
      return StatusError(StatusCode::kConflicts,
                         "timed out waiting for table tail owner");
    }
    table_tails_cv_.wait_until(lock, deadline, [&] {
      const auto it = table_tails_.find(first_page);
      return it == table_tails_.end() || it->second.poisoned ||
             !it->second.pending.has_value() ||
             it->second.pending->owner == requester;
    });
  }
}

void PageManager::ResolveTableTailReservations(txn_id_t owner,
                                               TableTailResolution resolution) {
  std::scoped_lock lock(table_tails_mu_);
  const auto reservations_it = table_tail_reservations_.find(owner);
  if (reservations_it == table_tail_reservations_.end()) {
    return;
  }
  for (page_id_t first_page : reservations_it->second) {
    const auto state_it = table_tails_.find(first_page);
    if (state_it == table_tails_.end()) {
      continue;
    }
    auto& state = state_it->second;
    if (!state.pending.has_value() || state.pending->owner != owner) {
      continue;
    }
    if (resolution == TableTailResolution::kPublish) {
      state.published_tail = state.pending->owner_tail;
      state.pending.reset();
    } else if (resolution == TableTailResolution::kDiscard) {
      state.pending.reset();
    } else {
      state.pending.reset();
      state.poisoned = true;
    }
  }
  table_tail_reservations_.erase(reservations_it);
  table_tails_cv_.notify_all();
}

void PageManager::CancelTableTailReservation(txn_id_t owner,
                                             page_id_t first_page) {
  std::scoped_lock lock(table_tails_mu_);
  const auto state_it = table_tails_.find(first_page);
  if (state_it == table_tails_.end() || !state_it->second.pending.has_value() ||
      state_it->second.pending->owner != owner ||
      !state_it->second.pending->unpublished_pages.empty()) {
    return;
  }
  state_it->second.pending.reset();
  const auto reservations_it = table_tail_reservations_.find(owner);
  if (reservations_it != table_tail_reservations_.end()) {
    reservations_it->second.erase(first_page);
    if (reservations_it->second.empty()) {
      table_tail_reservations_.erase(reservations_it);
    }
  }
  table_tails_cv_.notify_all();
}

void PageManager::AdvanceTableTail(page_id_t first_page, page_id_t expected,
                                   page_id_t next) {
  std::scoped_lock lock(table_tails_mu_);
  auto [it, inserted] =
      table_tails_.try_emplace(first_page, TableTailState{next});
  if (!inserted && !it->second.pending.has_value() &&
      it->second.published_tail == expected) {
    it->second.published_tail = next;
  }
}

void PageManager::ForgetTableTail(page_id_t first_page) {
  std::scoped_lock lock(table_tails_mu_);
  const auto it = table_tails_.find(first_page);
  if (it != table_tails_.end() && !it->second.pending.has_value()) {
    table_tails_.erase(it);
    table_tails_cv_.notify_all();
  }
}

// Logically delete the page.
Status PageManager::DestroyPage(Transaction& system_txn, Page* target) {
  ASSIGN_OR_RETURN(PageRef, meta, GetMetaPage());
  return meta->DestroyPage(system_txn, target);
}

// D3 (docs/design.md): undo restored the destroyed page; drop it from the
// allocator free stack so the next AllocateNewPage cannot re-issue it.
// Reports the meta-page failure instead of aborting: callers run on the
// recovery/abort path, where MoveValue()'s LOG(FATAL)+abort would turn a
// distressed meta page into a whole-process crash.
//
// The destroyed page is usually the free-list head (destroys push on top of
// the chain and undo unwinds in reverse LSN order), but a destroy that is
// still uncommitted can sit above it in the chain.  In that case walk the
// chain and relink the predecessor instead of the head.  Hops latch one page
// at a time and never hold the meta latch across GetPage (the allocator's
// candidate latch is held while it waits for the meta latch -- see
// AllocateNewPage), so a page that an allocator consumed and re-initialized
// mid-walk simply ends the walk; AllocateNewPage's stale-head escape hatch
// then keeps the allocator live and the startup free-list rebuild heals the
// leftover link.
Status PageManager::PopFreePageHead(page_id_t pid, page_id_t next) {
  page_id_t head = 0;
  page_id_t max_page = 0;
  {
    ASSIGN_OR_RETURN(PageRef, meta, GetMetaPage());
    if (meta->body.meta_page.FirstFreePage() == pid) {
      meta->body.meta_page.PopFreePageHead(pid, next);
      return Status::kSuccess;
    }
    head = meta->body.meta_page.FirstFreePage();
    max_page = meta->body.meta_page.MaxPageCount();
  }
  // A corrupt chain must not spin: bound the walk by the page high-water
  // mark, and retry from a freshly read head in case the list moved under
  // us (an allocator consuming pages above pid makes pid the new head).
  for (int attempt = 0; attempt < 3; ++attempt) {
    page_id_t cursor = head;
    for (page_id_t hops = 0; cursor != 0 && hops <= max_page; ++hops) {
      ASSIGN_OR_RETURN(PageRef, page, pool_->GetPage(cursor, nullptr));
      if (page->Type() != PageType::kFreePage) {
        break;
      }
      if (page->body.free_page.NextFreePage() == pid) {
        page->body.free_page.SetNextFreePage(next);
        return Status::kSuccess;
      }
      cursor = page->body.free_page.NextFreePage();
    }
    if (attempt == 2) {
      break;
    }
    ASSIGN_OR_RETURN(PageRef, meta, GetMetaPage());
    if (meta->body.meta_page.FirstFreePage() == pid) {
      meta->body.meta_page.PopFreePageHead(pid, next);
      return Status::kSuccess;
    }
    head = meta->body.meta_page.FirstFreePage();
  }
  return Status::kSuccess;
}

StatusOr<PageRef> PageManager::AllocateNewPage(Transaction& system_txn,
                                               PageType new_page_type) {
  // Two-phase allocation: pick the candidate under the meta latch, but
  // release that latch before blocking on the candidate page's latch.  The
  // runtime undo of an aborted destroy latches the candidate page first and
  // takes the meta latch second (LogUndoWithPage -> PopFreePageHead), so
  // holding meta across GetPage here would give the two paths opposite
  // orders and let an allocator deadlock against an aborting destructor.
  // Re-validation under the re-taken latch keeps the pop correct when
  // another allocator moved the head in between; the loser just retries.
  // |force_fresh| skips the free list entirely: set when the free-list
  // head's destroy is still owned by an active transaction, in which
  // case the id cannot be safely reissued (the destroyer's abort would
  // resurrect the old page image and catalog entry over the new owner's
  // content).  The skipped entry becomes allocatable once its destroyer
  // finishes -- a commit leaves the page free with a settled destroyer,
  // while an abort restores it to a non-free type the candidate check
  // rejects (and PopFreePageHead unlinks).
  bool force_fresh = false;
  for (;;) {
    page_id_t candidate = 0;
    {
      ASSIGN_OR_RETURN(PageRef, meta, GetMetaPage());
      candidate = force_fresh ? meta->body.meta_page.MaxPageCount() + 1
                              : meta->body.meta_page.PeekAllocationCandidate();
    }
    ASSIGN_OR_RETURN(PageRef, page, pool_->GetPage(candidate, nullptr));
    ASSIGN_OR_RETURN(PageRef, meta, GetMetaPage());
    // Snapshot the allocator before mutating it: the WAL append below can
    // fail (dead logger), and without a rollback the id leaks (max-path ids
    // are skipped forever, free-path ids orphan a free-list entry).  This
    // mirrors MetaPage::DestroyPage's rollback on log-append failure.
    const page_id_t saved_first_free_page =
        meta->body.meta_page.FirstFreePage();
    const page_id_t saved_max_page_count = meta->body.meta_page.MaxPageCount();
    const bool recycled = !force_fresh && saved_first_free_page == candidate;
    if (recycled && page->Type() == PageType::kFreePage) {
      const txn_id_t destroyer = page->body.free_page.Destroyer();
      // A page whose destroyer is still active must not be reissued — not
      // even to the destroying transaction itself.  An abort restores the
      // pre-destroy page image, and the restored rows stay visible only
      // because their version chains survive; reusing the id first would
      // have erased them (see InvalidatePageVersions) and let the restore
      // resurrect invisible rows / clobber the interim owner's content.
      // Once the destroyer settles, reuse is safe: a commit makes the
      // destroy permanent, and an abort unlinks the entry entirely.
      if (destroyer != 0 && system_txn.IsTransactionActive(destroyer)) {
        force_fresh = true;
        continue;
      }
    }
    const PageAllocationKind allocation_kind =
        recycled ? PageAllocationKind::kRecycled : PageAllocationKind::kFresh;
    const PageType old_target_type =
        recycled ? page->Type() : PageType::kFreePage;
    std::string old_target_payload;
    if (recycled && old_target_type == PageType::kFreePage) {
      old_target_payload =
          EncodeAllocationFreePayload(page->body.free_page.NextFreePage(),
                                      page->body.free_page.Destroyer());
    }
    const std::string old_meta_payload = EncodeAllocationMetaPayload(
        saved_first_free_page, saved_max_page_count);
    const bool allocated =
        force_fresh
            ? meta->body.meta_page.AllocateFreshId(candidate)
            : meta->body.meta_page.AllocateCandidate(candidate, page.get());
    if (!allocated) {
      // The candidate moved while the meta latch was released; the pinned
      // page was never mutated, so dropping it and recomputing is safe.
      // A head that still names this candidate while the page is no longer
      // free is a stale link that PopFreePageHead could not relink (its
      // comment covers when that happens); retrying would re-peek the same
      // dead head forever.  Abandon the chain -- the ids return at the
      // startup free-list rebuild -- instead of hanging every allocation.
      if (!force_fresh && meta->body.meta_page.FirstFreePage() == candidate &&
          page->Type() != PageType::kFreePage) {
        meta->body.meta_page.ResetFreeList();
      }
      continue;
    }
    const Status allocate_log =
        system_txn
            .AllocatePageLog(candidate, new_page_type, allocation_kind,
                             old_target_type, old_target_payload,
                             old_meta_payload)
            .GetStatus();
    if (allocate_log != Status::kSuccess) {
      meta->body.meta_page.RollbackAllocation(saved_first_free_page,
                                              saved_max_page_count);
      return allocate_log;
    }
    meta->SetPageLSN(system_txn.PrevRecordEndLSN());
    meta->SetRecLSN(system_txn.PrevLSN());
    page->PageInit(candidate, new_page_type);
    page->SetPageLSN(system_txn.PrevRecordEndLSN());
    page->SetRecLSN(system_txn.PrevLSN());
    if (recycled) {
      system_txn.RecordRecycledPageAllocation(candidate);
    }
    return page;
  }
}

Status PageManager::ApplyAllocationState(PageRef& target, lsn_t rec_lsn,
                                         lsn_t page_lsn, const LogRecord& log,
                                         bool redo_target) {
  if (log.wire_version < kAllocationPayloadWalVersion ||
      (log.type != LogType::kSystemAllocPage &&
       log.type != LogType::kCompensateAllocPage) ||
      log.pid == 0 || log.redo_data.size() != kAllocationMetaPayloadSize ||
      static_cast<uint8_t>(log.allocation_kind) >
          static_cast<uint8_t>(PageAllocationKind::kInPlace) ||
      log.allocated_page_type == PageType::kUnknown ||
      log.allocated_page_type == PageType::kMetaPage) {
    return StatusError(StatusCode::kCorrupt,
                       "invalid allocation compensation header");
  }

  page_id_t old_first_free = 0;
  page_id_t old_max_page_count = 0;
  if (!DecodeAllocationMetaPayload(log.redo_data, &old_first_free,
                                   &old_max_page_count)) {
    return StatusError(StatusCode::kCorrupt, "invalid allocation meta payload");
  }

  const PageAllocationKind kind = log.allocation_kind;
  page_id_t recycled_next = 0;
  txn_id_t recycled_destroyer = 0;
  if (kind == PageAllocationKind::kFresh) {
    if (log.previous_page_type != PageType::kFreePage ||
        !log.undo_data.empty() ||
        old_max_page_count == std::numeric_limits<page_id_t>::max() ||
        old_max_page_count + 1 != log.pid) {
      return StatusError(StatusCode::kCorrupt,
                         "invalid fresh allocation payload");
    }
  } else if (kind == PageAllocationKind::kRecycled) {
    if (log.previous_page_type != PageType::kFreePage ||
        !DecodeAllocationFreePayload(log.undo_data, &recycled_next,
                                     &recycled_destroyer) ||
        old_first_free != log.pid ||
        (recycled_next != 0 &&
         (recycled_next == log.pid || (old_first_free <= old_max_page_count &&
                                       recycled_next > old_max_page_count)))) {
      // Name every decoded field: this validation is the authority on
      // allocation-record self-consistency, and a bare "invalid" forced
      // postmortems to re-derive the payload by hand.
      return StatusError(
          StatusCode::kCorrupt,
          "invalid recycled allocation payload (pid=" +
              std::to_string(log.pid) +
              ", old_first_free=" + std::to_string(old_first_free) +
              ", old_max_page_count=" + std::to_string(old_max_page_count) +
              ", recycled_next=" + std::to_string(recycled_next) +
              ", recycled_destroyer=" + std::to_string(recycled_destroyer) +
              ", undo_bytes=" + std::to_string(log.undo_data.size()) + ")");
    }
  } else {
    if (log.previous_page_type == PageType::kMetaPage ||
        old_first_free > old_max_page_count ||
        log.undo_data.size() != kPageBodySize) {
      return StatusError(StatusCode::kCorrupt,
                         "invalid in-place allocation payload");
    }
  }

  if (kind == PageAllocationKind::kInPlace) {
    if (redo_target) {
      if (log.type == LogType::kSystemAllocPage) {
        target->PageInit(log.pid, log.allocated_page_type);
      } else {
        target->PageInit(log.pid, log.previous_page_type);
        std::memcpy(&target->body, log.undo_data.data(), kPageBodySize);
      }
      target->SetRecLSN(rec_lsn);
      target->SetPageLSN(page_lsn);
    }
    return Status::kSuccess;
  }

  const bool fresh_allocation = kind == PageAllocationKind::kFresh;
  const page_id_t expected_first_free =
      fresh_allocation ? old_first_free : recycled_next;
  const page_id_t expected_max_page_count =
      fresh_allocation ? log.pid : std::max(old_max_page_count, log.pid);

  ASSIGN_OR_RETURN(PageRef, meta, GetMetaPage());
  const bool redo_meta = meta->PageLSN() < page_lsn;
  const bool restore_old = log.type == LogType::kCompensateAllocPage;
  const bool exact_old_meta =
      restore_old &&
      meta->body.meta_page.FirstFreePage() == expected_first_free &&
      meta->body.meta_page.MaxPageCount() == expected_max_page_count;
  const page_id_t merge_next = meta->body.meta_page.FirstFreePage();

  if (redo_target) {
    if (!restore_old) {
      target->PageInit(log.pid, log.allocated_page_type);
    } else {
      target->PageInit(log.pid, PageType::kFreePage);
      if (kind == PageAllocationKind::kRecycled) {
        target->body.free_page.SetNextFreePage(recycled_next);
        target->body.free_page.SetDestroyer(recycled_destroyer);
      }
    }
    if (restore_old && redo_meta && !exact_old_meta) {
      target->body.free_page.SetNextFreePage(merge_next);
    }
    target->SetRecLSN(rec_lsn);
    target->SetPageLSN(page_lsn);
  }

  if (!redo_meta) {
    return Status::kSuccess;
  }

  page_id_t restored_first_free = old_first_free;
  page_id_t restored_max_page_count = old_max_page_count;
  if (!restore_old) {
    restored_first_free = expected_first_free;
    restored_max_page_count = expected_max_page_count;
  } else if (!exact_old_meta) {
    restored_first_free = log.pid;
    restored_max_page_count = std::max(
        {meta->body.meta_page.MaxPageCount(), old_max_page_count, log.pid});
  }
  meta->PageInit(kMetaPageId, PageType::kMetaPage);
  meta->body.meta_page.RollbackAllocation(restored_first_free,
                                          restored_max_page_count);
  meta->SetRecLSN(rec_lsn);
  meta->SetPageLSN(page_lsn);
  return Status::kSuccess;
}

Status PageManager::ReplayAllocation(PageRef& target, lsn_t lsn,
                                     const LogRecord& log, bool redo_target) {
  return ApplyAllocationState(target, lsn, lsn + log.Size(), log, redo_target);
}

StatusOr<lsn_t> PageManager::CompensateAllocation(PageRef& target, lsn_t lsn,
                                                  const LogRecord& log,
                                                  TransactionManager* tm) {
  if (log.wire_version < kAllocationPayloadWalVersion) {
    return lsn_t{0};
  }
  if (tm == nullptr) {
    return StatusError(StatusCode::kRuntimeError,
                       "allocation compensation has no transaction manager");
  }
  ASSIGN_OR_RETURN(lsn_t, clr_end,
                   tm->CompensateAllocatePageLog(log, log.prev_lsn));
  const LogRecord clr =
      LogRecord::CompensateAllocatePageLogRecord(log, log.prev_lsn);
  RETURN_IF_FAIL(ApplyAllocationState(target, lsn, clr_end, clr, true));
  return clr_end;
}

StatusOr<std::string> PageManager::GetAllocationMetaPayload() {
  ASSIGN_OR_RETURN(PageRef, meta, GetMetaPage());
  return EncodeAllocationMetaPayload(meta->body.meta_page.FirstFreePage(),
                                     meta->body.meta_page.MaxPageCount());
}

StatusOr<PageRef> PageManager::GetMetaPage() {
  ASSIGN_OR_RETURN(PageRef, meta_page, pool_->GetPage(kMetaPageId, nullptr));
  if (meta_page.IsNull()) {
    return StatusError(StatusCode::kIOError, "failed to get meta page");
  }
  if (meta_page->Type() != PageType::kMetaPage) {
    if (meta_page->Type() != PageType::kFreePage || meta_page->IsValid() ||
        meta_page->checksum != 0) {
      return StatusError(StatusCode::kCorrupt, "page 0 is not a meta page");
    }
    meta_page->PageInit(kMetaPageId, PageType::kMetaPage);
  }
  return meta_page;
}

}  // namespace tinylamb
