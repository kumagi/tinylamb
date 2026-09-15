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

#include "page/meta_page.hpp"

#include <cassert>
#include <cstdint>
#include <functional>
#include <ostream>
#include <string>
#include <utility>

#include "common/constants.hpp"
#include "page/branch_page.hpp"
#include "page/free_page.hpp"
#include "page/leaf_page.hpp"
#include "page/page_pool.hpp"
#include "page/page_type.hpp"
#include "page/row_page.hpp"
#include "transaction/transaction.hpp"

namespace tinylamb {
page_id_t MetaPage::PeekAllocationCandidate() const {
  return first_free_page == 0 ? max_page_count + 1 : first_free_page;
}

bool MetaPage::AllocateCandidate(page_id_t candidate, Page* candidate_page) {
  // Revalidates the choice made by PeekAllocationCandidate: another
  // allocator may have moved the head or advanced max_page_count while the
  // meta latch was released between the two phases (see
  // PageManager::AllocateNewPage for why the latch must not span the
  // candidate GetPage).
  if (first_free_page == 0) {
    if (max_page_count + 1 != candidate) {
      return false;
    }
    max_page_count = candidate;
    return true;
  }
  if (first_free_page != candidate) {
    return false;
  }
  first_free_page = candidate_page->body.free_page.next_free_page;
  return true;
}

// Precondition: latch of page is taken by txn.
Status MetaPage::DestroyPage(Transaction& txn, Page* target) {
  page_id_t free_page_id = target->PageID();
  // D3 (docs/design.md): capture the destroyed page's type and -- when it
  // still holds rows -- its body image so undo of an aborted destroy can
  // restore the page exactly (e.g. a rolled-back DROP TABLE keeps its rows).
  // A provably empty page is reconstructible from the type alone.
  const PageType old_type = target->Type();
  bool has_content = true;
  switch (old_type) {
    case PageType::kRowPage:
      has_content = target->body.row_page.RowCount() != 0;
      break;
    case PageType::kLeafPage:
      has_content = target->body.leaf_page.RowCount() != 0;
      break;
    case PageType::kBranchPage:
      has_content = target->body.branch_page.RowCount() != 0;
      break;
    case PageType::kPaxPage:
      // PaxPage exposes no cheap emptiness probe; conservatively capture the
      // body so undo of an aborted destroy restores every stored column.
      // Destroying a PAX page is rare (DROP TABLE), so the copy cost is fine.
      has_content = true;
      break;
    default:
      has_content = false;
      break;
  }
  std::string old_body;
  if (has_content) {
    old_body.assign(reinterpret_cast<const char*>(&target->body),
                    kPageBodySize);
  }
  const lsn_t old_page_lsn = target->PageLSN();
  const lsn_t old_rec_lsn = target->RecoveryLSN();
  target->PageInit(free_page_id, PageType::kFreePage);
  assert(target->PageID() == free_page_id);
  FreePage& free_page = target->body.free_page;
  // Add the free page to the free page chain.
  const page_id_t old_first_free_page = first_free_page;
  free_page.next_free_page = first_free_page;
  first_free_page = free_page_id;
  Status append =
      txn.DestroyPageLog(free_page_id, old_type, old_body).GetStatus();
  if (append != Status::kSuccess) {
    // WAL append failed: no CLR will compensate, so undo the in-memory
    // destroy (free-list head, body image, stamps) instead of leaving an
    // unlogged free page the allocator could re-issue after the abort.
    first_free_page = old_first_free_page;
    target->PageInit(free_page_id, old_type);
    if (has_content) {
      std::memcpy(
          &target->body, old_body.data(),
          std::min(old_body.size(), static_cast<size_t>(kPageBodySize)));
    }
    target->SetPageLSN(old_page_lsn);
    target->recovery_lsn = old_rec_lsn;
    return append;
  }
  // Stamp the freed image with the destroy record's end LSN.  Without this
  // the PageInit'd page keeps page_lsn == 0, the write-back durability gate
  // (WaitForDurable(0)) is a no-op, and the free-page image can reach disk
  // before kSystemDestroyPage is durable: lose the record to a torn tail and
  // the startup free-list rebuild links a page the catalog still references
  // (double allocation).
  target->SetPageLSN(txn.PrevRecordEndLSN());
  target->SetRecLSN(txn.PrevRecordEndLSN());
  return Status::kSuccess;
}

void MetaPage::Dump(std::ostream& o, int /*unused*/) const {
  o << "[FirstFree: " << first_free_page << "]";
}
}  // namespace tinylamb

uint64_t std::hash<tinylamb::MetaPage>::operator()(
    const tinylamb::MetaPage& m) {
  const uint64_t kChecksumSalt = 0xbe1a0a4;
  return kChecksumSalt + std::hash<uint64_t>()(m.max_page_count) +
         std::hash<uint64_t>()(m.first_free_page);
}
