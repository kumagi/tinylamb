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

#ifndef TINYLAMB_ROW_PAGE_HPP
#define TINYLAMB_ROW_PAGE_HPP

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <ostream>
#include <string>
#include <string_view>
#include <vector>

#include "common/constants.hpp"
#include "common/status_or.hpp"

namespace tinylamb {

class Transaction;

// Contiguous packed storage for variable-length row images within a RowPage.
// Uses a single flat byte buffer + 4-byte end offsets per slot instead of
// per-row heap-allocated std::string objects. Trade-off: a same-length update
// is O(1) memcpy, but changing a row's length shifts all later bytes and
// rewrites all later end offsets (O(page rows)); prefer same-size in-place
// updates on hot rows.
struct PackedSlots {
  std::string blob;
  std::vector<uint32_t> ends;

  [[nodiscard]] size_t size() const noexcept { return ends.size(); }
  [[nodiscard]] bool empty() const noexcept { return ends.empty(); }

  [[nodiscard]] std::string_view View(size_t slot) const noexcept {
    if (slot >= ends.size()) {
      return {};
    }
    const uint32_t start = slot == 0 ? 0 : ends[slot - 1];
    const uint32_t end = ends[slot];
    if (end > blob.size() || start > blob.size() || end <= start) {
      return {};
    }
    return std::string_view(
        blob.data() + start,
        end - start);  // NOLINT(modernize-return-braced-init-list)
  }

  [[nodiscard]] bool EmptySlot(size_t slot) const noexcept {
    if (slot >= ends.size()) {
      return true;
    }
    const uint32_t start = slot == 0 ? 0 : ends[slot - 1];
    return ends[slot] == start || ends[slot] > blob.size() ||
           start > blob.size();
  }

  [[nodiscard]] size_t UsedRowBytes() const noexcept { return blob.size(); }

  [[nodiscard]] size_t HeapBytes() const noexcept {
    return blob.capacity() + (ends.capacity() * sizeof(uint32_t));
  }

  void Clear(bool release = false) {
    if (release) {
      std::string().swap(blob);
      std::vector<uint32_t>().swap(ends);
    } else {
      blob.clear();
      ends.clear();
    }
  }

  void Reserve(size_t nslots, size_t total_bytes) {
    ends.reserve(nslots);
    blob.reserve(total_bytes);
  }

  void ResizeSlots(size_t nslots) {
    if (nslots > ends.size()) {
      ends.resize(nslots, static_cast<uint32_t>(blob.size()));
    }
  }

  void Set(size_t slot, std::string_view data) {
    if (slot >= ends.size()) {
      const auto cur_end = static_cast<uint32_t>(blob.size());
      if (slot > ends.size()) {
        ends.resize(slot, cur_end);
      }
      blob.append(data);
      ends.push_back(static_cast<uint32_t>(blob.size()));
      return;
    }
    const uint32_t start = slot == 0 ? 0 : ends[slot - 1];
    const uint32_t old_end = ends[slot];
    // ends[] is populated from page images without a monotonicity guarantee
    // at this layer; a corrupt image must not wrap old_len toward 4 GiB or
    // push replace()/memcpy() past blob.size() (LATENT: writers keep ends
    // monotone, DecodeUncommittedOverlay now validates them too).
    const uint32_t old_len = old_end >= start ? old_end - start : 0;
    const size_t pos =
        start <= blob.size() ? static_cast<size_t>(start) : blob.size();
    const size_t new_len = data.size();
    if (new_len == old_len && pos + new_len <= blob.size()) {
      if (new_len > 0) {
        std::memcpy(blob.data() + pos, data.data(), new_len);
      }
      return;
    }
    blob.replace(pos, old_len, data);
    const int64_t delta =
        static_cast<int64_t>(new_len) - static_cast<int64_t>(old_len);
    for (size_t i = slot; i < ends.size(); ++i) {
      ends[i] = static_cast<uint32_t>(static_cast<int64_t>(ends[i]) + delta);
    }
  }

  bool operator==(const PackedSlots& o) const noexcept {
    return ends == o.ends && blob == o.blob;
  }
};

// Unbounded in-memory/LSM row block.
// Each block holds up to kMaxSlotsPerBlock logical row slots before rolling
// over to a new block (for parallel morsel scan partitioning), while each slot
// stores packed row images with copy-on-write clean snapshots and disk
// eviction support.
class RowPage {
 public:
  static constexpr slot_t kMaxSlotsPerBlock = 4096;
  static constexpr uint32_t kMaxColumnarRows = 32768;
  static constexpr size_t kSlotEntryBytes = 4;
  // Reserve 24 bytes at the end of the page body for the packed-slot
  // directory header (slot count + end offsets) used by PackedSlots.
  static constexpr size_t kBlockBudgetBytes = kPageBodySize - 24;
  static constexpr uint64_t kInvalidDiskOffset =
      std::numeric_limits<uint64_t>::max();

  void Initialize() {
    prev_page_id_ = 0;
    next_page_id_ = 0;
    row_count_ = 0;
    columnar_row_count_ = 0;
    live_packed_.Clear(/*release=*/true);
    live_clean_ref_.reset();
    std::vector<size_t>().swap(hole_sizes_);
    committed_allocated_ = false;
    committed_prev_page_id_ = 0;
    committed_next_page_id_ = 0;
    committed_link_lsn_ = 0;
    committed_columnar_row_count_ = 0;
    committed_packed_.Clear(/*release=*/true);
    committed_clean_ref_.reset();
    std::vector<lsn_t>().swap(committed_lsns_);
    committed_max_lsn_ = 0;
    ++committed_seq_;
    evicted_ = false;
    disk_dirty_ = false;
    disk_offset_ = kInvalidDiskOffset;
    disk_length_ = 0;
  }

  void ClearLiveState() {
    prev_page_id_ = 0;
    next_page_id_ = 0;
    row_count_ = 0;
    columnar_row_count_ = 0;
    live_packed_.Clear(/*release=*/true);
    live_clean_ref_.reset();
    std::vector<size_t>().swap(hole_sizes_);
    ++live_seq_;
  }

  [[nodiscard]] const PackedSlots& LivePacked() const noexcept {
    return live_clean_ref_ != nullptr ? *live_clean_ref_ : live_packed_;
  }
  [[nodiscard]] const PackedSlots& CommittedPacked() const noexcept {
    return committed_clean_ref_ != nullptr ? *committed_clean_ref_
                                           : committed_packed_;
  }

  void EnsureLiveMutable() {
    if (live_clean_ref_ != nullptr) {
      live_packed_ = *live_clean_ref_;
      live_clean_ref_.reset();
    }
    ++live_seq_;
  }

  void EnsureCommittedMutable() {
    if (committed_clean_ref_ != nullptr) {
      committed_packed_ = *committed_clean_ref_;
      committed_clean_ref_.reset();
    }
  }

  bool ApplyCommittedSlot(slot_t slot, std::string_view image, lsn_t lsn) {
    committed_allocated_ = true;
    EnsureCommittedMutable();
    if (slot >= committed_packed_.size()) {
      committed_packed_.ResizeSlots(static_cast<size_t>(slot) + 1);
      committed_lsns_.resize(static_cast<size_t>(slot) + 1, 0);
    }
    if (lsn >= committed_lsns_[slot]) {
      committed_packed_.Set(slot, image);
      committed_lsns_[slot] = lsn;
      committed_max_lsn_ = std::max(committed_max_lsn_, lsn);
      disk_dirty_ = true;
      ++committed_seq_;
      return true;
    }
    return false;
  }

  void ClearCommittedState(lsn_t rec_lsn) {
    committed_allocated_ = false;
    committed_columnar_row_count_ = 0;
    committed_packed_.Clear(/*release=*/true);
    committed_clean_ref_.reset();
    std::vector<lsn_t>().swap(committed_lsns_);
    committed_prev_page_id_ = 0;
    committed_next_page_id_ = 0;
    committed_link_lsn_ = rec_lsn;
    committed_max_lsn_ = std::max(committed_max_lsn_, rec_lsn);
    disk_dirty_ = false;
    disk_offset_ = kInvalidDiskOffset;
    disk_length_ = 0;
    evicted_ = false;
    ++committed_seq_;
  }

  void ResetLiveToCommitted() {
    prev_page_id_ = committed_prev_page_id_;
    next_page_id_ = committed_next_page_id_;
    columnar_row_count_ = committed_columnar_row_count_;
    std::vector<size_t>().swap(hole_sizes_);
    if (committed_clean_ref_ != nullptr) {
      live_clean_ref_ = committed_clean_ref_;
      live_packed_.Clear(/*release=*/true);
    } else {
      live_clean_ref_.reset();
      live_packed_ = committed_packed_;
    }
    const PackedSlots& p = LivePacked();
    row_count_ = 0;
    for (size_t s = 0; s < p.size(); ++s) {
      if (!p.EmptySlot(s)) {
        ++row_count_;
      }
    }
    ++live_seq_;
  }

  void SetSharedCleanSnapshot(std::shared_ptr<const PackedSlots> snap,
                              slot_t non_empty_count) {
    live_clean_ref_ = snap;
    committed_clean_ref_ = std::move(snap);
    live_packed_.Clear(/*release=*/true);
    committed_packed_.Clear(/*release=*/true);
    std::vector<size_t>().swap(hole_sizes_);
    row_count_ = non_empty_count;
  }

  [[nodiscard]] bool LiveEqualsCommittedLocked() const noexcept {
    if (evicted_ || !committed_allocated_ || IsColumnarStub()) {
      return false;
    }
    if (prev_page_id_ != committed_prev_page_id_ ||
        next_page_id_ != committed_next_page_id_ ||
        columnar_row_count_ != committed_columnar_row_count_) {
      return false;
    }
    if (live_clean_ref_ != nullptr &&
        live_clean_ref_.get() == committed_clean_ref_.get()) {
      return true;
    }
    return LivePacked() == CommittedPacked();
  }

  void EvictPayloadsLocked() {
    live_packed_.Clear(/*release=*/true);
    live_clean_ref_.reset();
    std::vector<size_t>().swap(hole_sizes_);
    committed_packed_.Clear(/*release=*/true);
    committed_clean_ref_.reset();
    std::vector<lsn_t>().swap(committed_lsns_);
    evicted_ = true;
  }

  [[nodiscard]] size_t ApproxHeapBytes() const noexcept {
    size_t bytes = live_packed_.HeapBytes() +
                   (hole_sizes_.capacity() * sizeof(size_t)) +
                   committed_packed_.HeapBytes() +
                   (committed_lsns_.capacity() * sizeof(lsn_t));
    if (live_clean_ref_ != nullptr) {
      bytes += live_clean_ref_->HeapBytes();
    }
    if (committed_clean_ref_ != nullptr &&
        committed_clean_ref_.get() != live_clean_ref_.get()) {
      bytes += committed_clean_ref_->HeapBytes();
    }
    return bytes;
  }

  [[nodiscard]] bool IsColumnarStub() const noexcept {
    return columnar_row_count_ != 0 && LivePacked().empty();
  }

  [[nodiscard]] uint32_t ColumnarRowCount() const noexcept {
    return columnar_row_count_;
  }

  [[nodiscard]] size_t UsedBytes() const {
    if (IsColumnarStub()) {
      return kBlockBudgetBytes;
    }
    const PackedSlots& live = LivePacked();
    return (live.size() * kSlotEntryBytes) + live.UsedRowBytes();
  }

  [[nodiscard]] bin_size_t FreeSize() const {
    if (IsColumnarStub()) {
      return 0;
    }
    const size_t used = UsedBytes();
    return used < kBlockBudgetBytes
               ? static_cast<bin_size_t>(kBlockBudgetBytes - used)
               : 0;
  }

  [[nodiscard]] bin_size_t FreeSizeForTest() const { return FreeSize(); }

  // Test-only: the undo reservation recorded for a slot after a delete or a
  // shrink. An aborted INSERT must leave this at 0 (it had no previous image).
  [[nodiscard]] size_t HoleReservationForTest(slot_t slot) const {
    return slot < hole_sizes_.size() ? hole_sizes_[slot] : 0;
  }

  // #1110 structural contract: hole reuse must attempt the write-intent
  // acquire at most once per Insert (first-fit), scanning the remaining
  // holes with read-only probes only.  Counts TryAddWriteSet calls made by
  // Insert's hole loop since the last reset.
  static void ResetHoleAcquireProbesForTest();
  [[nodiscard]] static uint64_t HoleAcquireProbesForTest();

  StatusOr<std::string_view> Read(page_id_t page_id, Transaction& txn,
                                  slot_t slot, bool resolve_head = true) const;

  StatusOr<slot_t> Insert(page_id_t page_id, Transaction& txn,
                          std::string_view record);

  Status Update(page_id_t page_id, Transaction& txn, slot_t slot,
                std::string_view record);

  StatusOr<lsn_t> RestorePhysicalUpdate(page_id_t page_id, Transaction& txn,
                                        slot_t slot, std::string_view record);

  Status Delete(page_id_t page_id, Transaction& txn, slot_t slot);

  Status SetNextPage(page_id_t page_id, Transaction& txn, page_id_t next);
  Status SetPrevPage(page_id_t page_id, Transaction& txn, page_id_t prev);
  void SetNextPageImpl(page_id_t next) { next_page_id_ = next; }
  void SetPrevPageImpl(page_id_t prev) { prev_page_id_ = prev; }

  [[nodiscard]] slot_t RowCount() const {
    return IsColumnarStub()
               ? static_cast<slot_t>(std::min<uint32_t>(
                     columnar_row_count_, std::numeric_limits<slot_t>::max()))
               : row_count_;
  }
  [[nodiscard]] slot_t RowMax() const {
    return IsColumnarStub()
               ? static_cast<slot_t>(std::min<uint32_t>(
                     columnar_row_count_, std::numeric_limits<slot_t>::max()))
               : static_cast<slot_t>(LivePacked().size());
  }
  [[nodiscard]] bool IsSlotOccupied(slot_t slot) const {
    if (IsColumnarStub()) {
      return slot < columnar_row_count_;
    }
    return !LivePacked().EmptySlot(slot);
  }
  [[nodiscard]] bool ShouldPreallocateNextBlock() const {
    // Keep an 8-slot headroom (~0.2% of kMaxSlotsPerBlock) so the next block
    // is ready before a full scan rolls over to a new page.
    return (IsColumnarStub() || FreeSize() < 1024 ||
            LivePacked().size() + 8 >= kMaxSlotsPerBlock) &&
           next_page_id_ == 0;
  }

  [[nodiscard]] std::string_view GetRow(slot_t slot) const {
    return LivePacked().View(slot);
  }

  StatusOr<slot_t> InsertRow(std::string_view new_row);
  StatusOr<slot_t> InsertRowAt(slot_t slot, std::string_view new_row);
  Status UpdateRow(slot_t slot, std::string_view record);
  void DeleteRow(slot_t slot);
  void RestoreLiveSlot(slot_t slot, std::string_view image);

  void Dump(std::ostream& o, int indent) const;
  friend std::ostream& operator<<(std::ostream& o, const RowPage& r) {
    r.Dump(o, 0);
    return o;
  }

  page_id_t prev_page_id_ = 0;
  page_id_t next_page_id_ = 0;
  slot_t row_count_ = 0;
  uint32_t columnar_row_count_ = 0;
  PackedSlots live_packed_;
  std::shared_ptr<const PackedSlots> live_clean_ref_;
  std::vector<size_t> hole_sizes_;

  // Committed baseline state used by fuzzy checkpoint, eviction, and recovery.
  bool committed_allocated_ = false;
  page_id_t committed_prev_page_id_ = 0;
  page_id_t committed_next_page_id_ = 0;
  lsn_t committed_link_lsn_ = 0;
  uint32_t committed_columnar_row_count_ = 0;
  PackedSlots committed_packed_;
  std::shared_ptr<const PackedSlots> committed_clean_ref_;
  std::vector<lsn_t> committed_lsns_;
  lsn_t committed_max_lsn_ = 0;
  uint64_t committed_seq_ = 0;
  uint64_t live_seq_ = 0;

  // Out-of-core block backing state in <db>.blocks.
  bool evicted_ = false;
  bool disk_dirty_ = false;
  uint64_t disk_offset_ = kInvalidDiskOffset;
  uint32_t disk_length_ = 0;
  uint32_t disk_file_gen_ = 0;
};

}  // namespace tinylamb

#endif  // TINYLAMB_ROW_PAGE_HPP
