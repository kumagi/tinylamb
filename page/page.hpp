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

#include <unistd.h>

#include <array>
#include <cstdint>
#include <iostream>
#include <limits>
#include <type_traits>

#include "common/constants.hpp"
#include "common/log_message.hpp"
#include "common/status_or.hpp"
#include "page/branch_page.hpp"
#include "page/free_page.hpp"
#include "page/leaf_page.hpp"
#include "page/meta_page.hpp"
#include "page/pax_page.hpp"
#include "page/row_page.hpp"

namespace tinylamb {
class Page {
 public:
  Page(page_id_t pid, PageType type);

  void PageInit(page_id_t pid, PageType type);

  [[nodiscard]] page_id_t PageID() const { return page_id; }
  [[nodiscard]] PageType Type() const { return type; }
  [[nodiscard]] lsn_t PageLSN() const { return page_lsn; }
  [[nodiscard]] lsn_t RecoveryLSN() const { return recovery_lsn; }
  void SetPageLSN(lsn_t lsn) { page_lsn = lsn; }
  void SetRecLSN(lsn_t lsn) { recovery_lsn = std::min(lsn, recovery_lsn); }
  // Marks the on-disk image as current through page_lsn: after a successful
  // write-back the file reflects every record up to PageLSN(), so redo never
  // needs anything at or below it. Without this the recLSN of a hot page
  // would ratchet down forever and pin old WAL segments against recycling.
  // Call only while no mutator can touch the page (flush paths).
  void ResetRecLSN() { recovery_lsn = std::numeric_limits<lsn_t>::max(); }

  // Meta page manipulations.
  Status DestroyPage(Transaction& txn, Page* target);

  size_t RowCount(Transaction& txn) const;

  // Row page manipulations.
  // resolve_head applies to row pages only: false is a pure snapshot read
  // (see RowPage::Read / Transaction::ReadVersion).
  StatusOr<std::string_view> Read(Transaction& txn, slot_t slot,
                                  bool resolve_head = true) const;

  StatusOr<slot_t> Insert(Transaction& txn, std::string_view record);

  Status Update(Transaction& txn, slot_t slot, std::string_view row);

  Status Delete(Transaction& txn, slot_t pos);

  Status SetNextPage(Transaction& txn, page_id_t next);
  Status SetPrevPage(Transaction& txn, page_id_t prev);
  void SetNextPageImpl(page_id_t next);
  void SetPrevPageImpl(page_id_t prev);

  [[nodiscard]] slot_t RowCount() const;

  StatusOr<std::string_view> ReadKey(Transaction& txn, slot_t slot) const;

  std::string_view GetKey(slot_t slot) const;

  page_id_t GetPage(slot_t slot) const;

  // Leaf & Branch common manipulation.
  Status SetLowFence(Transaction& txn, const IndexKey& key);

  Status SetHighFence(Transaction& txn, const IndexKey& key);

  [[nodiscard]] IndexKey GetLowFence(Transaction& txn) const;

  [[nodiscard]] IndexKey GetHighFence(Transaction& txn) const;

  [[nodiscard]] Status SetFoster(Transaction& txn, const FosterPair& foster);

  [[nodiscard]] StatusOr<FosterPair> GetFoster(Transaction& txn) const;

  Status MoveRightToFoster(Transaction& txn, Page& foster);

  Status MoveLeftFromFoster(Transaction& txn, Page& foster);

  void SetLowFenceImpl(const IndexKey& key);

  void SetHighFenceImpl(const IndexKey& key);

  Status SetFosterImpl(const FosterPair& foster);

  // Leaf page manipulations.
  Status InsertLeaf(Transaction& txn, std::string_view key,
                    std::string_view value);

  Status Update(Transaction& txn, std::string_view key, std::string_view value);

  Status Delete(Transaction& txn, std::string_view key);

  StatusOr<std::string_view> Read(Transaction& txn, std::string_view key) const;

  StatusOr<std::string_view> LowestKey(Transaction& txn) const;

  StatusOr<std::string_view> HighestKey(Transaction& txn) const;

  // Branch page manipulations.
  Status InsertBranch(Transaction& txn, std::string_view key, page_id_t pid);

  Status UpdateBranch(Transaction& txn, std::string_view key, page_id_t pid);

  StatusOr<page_id_t> GetPageForKey(Transaction& txn, std::string_view key,
                                    bool less_than) const;

  Status SetLowestValue(Transaction& txn, page_id_t v);

  Status SplitInto(Transaction& txn, std::string_view new_key, Page* right,
                   std::string* middle);

  Status PageTypeChange(Transaction& txn, PageType new_type);

  // Internal methods exposed for recovery.
  Status InsertImpl(slot_t slot, std::string_view redo);

  void UpdateImpl(slot_t slot, std::string_view redo);

  void DeleteImpl(slot_t slot);

  Status InsertImpl(std::string_view key, std::string_view value);

  void UpdateImpl(std::string_view key, std::string_view value);

  void DeleteImpl(std::string_view key);

  Status InsertBranchImpl(std::string_view key, page_id_t pid);

  void UpdateBranchImpl(std::string_view key, page_id_t pid);

  void DeleteBranchImpl(std::string_view key);

  void SetLowestValueBranchImpl(page_id_t lowest_value);

  void PageTypeChangeImpl(PageType new_type);

  void SetChecksum() const;

  // Page has no spare bytes (static_assert sizeof(Page) == kPageSize), and the
  // stored CRC is 32 bits wide, so the "repaired in memory" claim rides in the
  // unused high half of `checksum` instead of a new member.
  static constexpr uint64_t kRepairedInMemoryBit = uint64_t{1} << 63;
  static constexpr uint64_t kChecksumDiskMask = ~kRepairedInMemoryBit;

  // Marks the page as repaired in memory (recovery redo/undo, the free-list
  // relink). The stored CRC then still describes the PRE-repair disk image, so
  // IsValid() would report a page recovery just fixed as torn -- and a false
  // "torn" verdict is expensive: PageManager::GetPage hands back an empty ref
  // and BPlusTree::Open silently bootstraps a fresh, empty tree, losing the
  // committed catalog. In-memory only (EncodeDisk masks it away);
  // SetChecksum() clears it, because the on-disk image then matches the body
  // again.
  void MarkRepairedInMemory() const { checksum |= kRepairedInMemoryBit; }

  [[nodiscard]] bool IsValid() const;

  // True when the cached checksum matches the current page image, i.e. the
  // in-memory content equals what the last WriteBack persisted. This is the
  // DIRTY predicate: a repaired page deliberately still reports false so the
  // repair is written back instead of being lost with the buffer pool.
  [[nodiscard]] bool ChecksumMatches() const;

  // Version-1 disk codec. The in-memory Page remains a directly addressable
  // object, while its fixed header is emitted/accepted in big-endian order.
  void EncodeDisk(char* destination) const;
  Status DecodeDisk(const char* source);
  // Single-pread read path: the page image already sits in this object's own
  // bytes (the body was never copied out), so the big-endian header scalars
  // are fixed up in place.  Same verdicts and resulting member states as
  // DecodeDisk over that image.
  Status DecodeDiskInPlace();

  void* operator new(size_t /*byte size, always kPageSize*/);

  void operator delete(void* page) noexcept;

  void Dump(std::ostream& o, int indent) const;

  friend std::ostream& operator<<(std::ostream& o, const Page& p) {
    p.Dump(o, 0);
    return o;
  }

  uint32_t format_magic = 0;
  uint32_t format_version = 0;

  // The ID for this page. This ID is also an offset of this page in file.
  page_id_t page_id = 0;

  // End LSN of the latest WAL record whose effect is reflected on this page.
  lsn_t page_lsn = 0;

  // Start LSN of the earliest WAL record that made this page dirty.
  uint64_t recovery_lsn = 0;
  enum PageType type = PageType::kUnknown;
  // Low 32 bits: the CRC32C of the image as loaded/written. High bits: only
  // kRepairedInMemoryBit is used, and only in memory (see
  // MarkRepairedInMemory); the disk header always carries the bare CRC, so the
  // page format is unaffected.
  mutable uint64_t checksum = 0;

  // RowPage contains an intentional zero-sized tail array. Keep this union's
  // layout identical to the 32 KiB page format.
#if defined(__GNUC__) || defined(__clang__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"
#endif
  union PageBody {
    std::array<char, kPageBodySize> dummy_;
    MetaPage meta_page;
    FreePage free_page;
    RowPage row_page;
    LeafPage leaf_page;
    BranchPage branch_page;
    PaxPage pax_page;

    PageBody() : dummy_() {}
  };
  PageBody body;
#if defined(__GNUC__) || defined(__clang__)
#pragma GCC diagnostic pop
#endif
};

static_assert(std::is_trivially_destructible<Page>::value,
              "Page must be trivially destructible");
static_assert(sizeof(Page) == kPageSize,
              "Page size must be equal to kPageSize");
static_assert(std::is_standard_layout_v<Page>,
              "Page codec depends on a standard in-memory layout");
}  // namespace tinylamb

template <>
class std::hash<tinylamb::Page> {
 public:
  uint64_t operator()(const tinylamb::Page& p) const;
};

#endif  // TINYLAMB_PAGE_HPP
