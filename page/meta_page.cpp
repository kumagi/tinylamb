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

#include "common/constants.hpp"
#include "page/branch_page.hpp"
#include "page/free_page.hpp"
#include "page/leaf_page.hpp"
#include "page/page_pool.hpp"
#include "page/page_type.hpp"
#include "page/row_page.hpp"
#include "transaction/transaction.hpp"

namespace tinylamb {
PageRef MetaPage::AllocateNewPage(Transaction& txn, PagePool& pool,
                                  PageType new_page_type) {
  page_id_t new_page_id = 0;
  PageRef ret = [&]() {
    if (first_free_page == 0) {
      // Reserve nothing before the load succeeds: a throwing GetPage must not
      // permanently advance max_page_count (that would strand a page-id hole).
      const page_id_t candidate = max_page_count + 1;
      PageRef page = pool.GetPage(candidate, nullptr);
      new_page_id = candidate;
      max_page_count = candidate;
      return page;
    }
    new_page_id = first_free_page;
    PageRef page = pool.GetPage(new_page_id, nullptr);
    first_free_page = page.GetFreePage().next_free_page;
    return page;
  }();
  ret->PageInit(new_page_id, new_page_type);
  txn.AllocatePageLog(new_page_id, new_page_type);

  return ret;
}

// Precondition: latch of page is taken by txn.
void MetaPage::DestroyPage(Transaction& txn, Page* target) {
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
  target->PageInit(free_page_id, PageType::kFreePage);
  assert(target->PageID() == free_page_id);
  FreePage& free_page = target->body.free_page;
  // Add the free page to the free page chain.
  free_page.next_free_page = first_free_page;
  first_free_page = free_page_id;
  txn.DestroyPageLog(free_page_id, old_type, std::move(old_body));
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
