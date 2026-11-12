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

#include "page/page_manager.hpp"

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <exception>
#include <filesystem>
#include <fstream>
#include <map>
#include <memory>
#include <mutex>
#include <new>
#include <ranges>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/crc32c.hpp"
#include "common/decoder.hpp"
#include "common/encoder.hpp"
#include "common/log_message.hpp"
#include "common/platform/file_io.hpp"
#include "common/serdes.hpp"
#include "common/status_or.hpp"
#include "page/page.hpp"
#include "page/page_ref.hpp"
#include "page/page_type.hpp"
#include "recovery/log_record.hpp"
#include "transaction/transaction.hpp"
#include "type/row.hpp"

namespace tinylamb {

namespace {
constexpr std::array<char, 8> kFuzzyCheckpointMagic = {'T', 'L', 'S', 'M',
                                                       'C', 'K', 'P', '1'};
constexpr uint32_t kFuzzyCheckpointVersion = 2;
constexpr auto kTableTailWaitTimeout = std::chrono::milliseconds(5000);
constexpr auto kTableTailWaitSlice = std::chrono::milliseconds(5);

void AppendU32(std::string* dst, uint32_t v) {
  std::array<char, sizeof(uint32_t)> buf{};
  std::memcpy(buf.data(), &v, sizeof(uint32_t));
  dst->append(buf.data(), sizeof(uint32_t));
}
void AppendU64(std::string* dst, uint64_t v) {
  std::array<char, sizeof(uint64_t)> buf{};
  std::memcpy(buf.data(), &v, sizeof(uint64_t));
  dst->append(buf.data(), sizeof(uint64_t));
}
void AppendString(std::string* dst, std::string_view sv) {
  AppendU32(dst, static_cast<uint32_t>(sv.size()));
  dst->append(sv.data(), sv.size());
}
bool ConsumeU32(const char* data, size_t size, size_t* off, uint32_t* out) {
  if (*off + sizeof(uint32_t) > size) {
    return false;
  }
  std::memcpy(out, data + *off, sizeof(uint32_t));
  *off += sizeof(uint32_t);
  return true;
}
bool ConsumeU64(const char* data, size_t size, size_t* off, uint64_t* out) {
  if (*off + sizeof(uint64_t) > size) {
    return false;
  }
  std::memcpy(out, data + *off, sizeof(uint64_t));
  *off += sizeof(uint64_t);
  return true;
}
bool ConsumeString(const char* data, size_t size, size_t* off,
                   std::string* out) {
  uint32_t len = 0;
  if (!ConsumeU32(data, size, off, &len) || *off + len > size) {
    return false;
  }
  out->assign(data + *off, len);
  *off += len;
  return true;
}
// Restoring a previously admitted image is not a new space reservation.
// Another transaction may have consumed bytes released by a shrink/delete.
// The dynamically sized row block can exceed its admission budget during
// rollback, just as replay can; dropping the old image would lose data.
void RestoreAbortedRow(RowPage& page, slot_t slot, const std::string& image) {
  if (slot >= page.slots_.size()) {
    page.slots_.resize(static_cast<size_t>(slot) + 1);
    page.hole_sizes_.resize(static_cast<size_t>(slot) + 1, 0);
  }
  const bool occupied = !page.slots_[slot].empty();
  page.slots_[slot] = image;
  if (!occupied && !image.empty()) {
    ++page.row_count_;
  } else if (occupied && image.empty()) {
    --page.row_count_;
  }
  page.hole_sizes_[slot] = 0;
}

struct LsmIndexEntry {
  RowPosition pos;
  Row include;
  friend Encoder& operator<<(Encoder& e, const LsmIndexEntry& v) {
    e << v.pos << v.include;
    return e;
  }
  friend Decoder& operator>>(Decoder& d, LsmIndexEntry& t) {
    d >> t.pos >> t.include;
    return d;
  }
};

bool TryDecodeIndexEntryList(std::string_view encoded, bool deleted,
                             std::vector<LsmIndexEntry>* out) {
  if (deleted) {
    out->clear();
    return encoded.empty();
  }
  constexpr size_t kPosOnlySize = RowPosition::Size() + sizeof(uint64_t);
  if (encoded.size() < sizeof(uint64_t) + kPosOnlySize) {
    return false;
  }
  uint64_t count = 0;
  DeserializeU64(
      encoded.data(),  // NOLINT(bugprone-suspicious-stringview-data-usage)
      &count);
  if (count == 0 || count > (1U << 20U) ||
      count > (encoded.size() - sizeof(uint64_t)) / kPosOnlySize) {
    return false;
  }
  const std::string_view entries = encoded.substr(sizeof(uint64_t));
  if (count * kPosOnlySize == entries.size()) {
    std::vector<LsmIndexEntry> parsed(static_cast<size_t>(count));
    bool all_pos_only = true;
    for (size_t i = 0; i < parsed.size(); ++i) {
      const std::string_view chunk =
          entries.substr(i * kPosOnlySize, kPosOnlySize);
      uint64_t cols = 0;
      DeserializeU64(chunk.data() + RowPosition::Size(), &cols);
      if (cols != 0) {
        all_pos_only = false;
        break;
      }
      parsed[i].pos.Deserialize(
          chunk.data());  // NOLINT(bugprone-suspicious-stringview-data-usage)
      if (!parsed[i].pos.IsValid()) {
        all_pos_only = false;
        break;
      }
    }
    if (all_pos_only) {
      *out = std::move(parsed);
      return true;
    }
  }
  std::string buffer(encoded);
  std::stringstream ss(buffer);
  Decoder dec(ss);
  std::vector<LsmIndexEntry> parsed;
  dec >> parsed;
  if (!ss || !dec.AtEnd() || parsed.empty()) {
    return false;
  }
  for (const LsmIndexEntry& e : parsed) {
    if (!e.pos.IsValid() || e.include.Size() > 1024) {
      return false;
    }
  }
  *out = std::move(parsed);
  return true;
}

bool TryApplyIndexListDelta(std::string_view before_bytes, bool before_deleted,
                            std::string_view after_bytes, bool after_deleted,
                            std::string* target_value, bool* target_deleted) {
  if (before_deleted && after_deleted) {
    return false;
  }
  std::vector<LsmIndexEntry> before;
  std::vector<LsmIndexEntry> after;
  std::vector<LsmIndexEntry> target;
  if (!TryDecodeIndexEntryList(before_bytes, before_deleted, &before) ||
      !TryDecodeIndexEntryList(after_bytes, after_deleted, &after) ||
      !TryDecodeIndexEntryList(*target_value, *target_deleted, &target)) {
    return false;
  }
  for (const LsmIndexEntry& old_entry : before) {
    const bool still_present =
        std::ranges::any_of(after, [&](const LsmIndexEntry& new_entry) {
          return new_entry.pos == old_entry.pos;
        });
    if (!still_present) {
      std::erase_if(target, [&](const LsmIndexEntry& cur) {
        return cur.pos == old_entry.pos;
      });
    }
  }
  for (const LsmIndexEntry& new_entry : after) {
    const auto old_it =
        std::ranges::find_if(before, [&](const LsmIndexEntry& old_entry) {
          return old_entry.pos == new_entry.pos;
        });
    if (old_it != before.end() && old_it->include == new_entry.include) {
      continue;
    }
    const auto target_it = std::ranges::find_if(
        target,
        [&](const LsmIndexEntry& cur) { return cur.pos == new_entry.pos; });
    if (target_it != target.end()) {
      target_it->include = new_entry.include;
    } else {
      target.push_back(new_entry);
    }
  }
  if (target.empty()) {
    target_value->clear();
    *target_deleted = true;
  } else {
    *target_value = Encode(target);
    *target_deleted = false;
  }
  return true;
}

void RemoveOneOwnerFromBack(std::vector<txn_id_t>* owners, txn_id_t txn_id,
                            txn_id_t* latest_owner) {
  for (auto it = owners->rbegin(); it != owners->rend(); ++it) {
    if (*it == txn_id) {
      owners->erase(std::next(it).base());
      break;
    }
  }
  *latest_owner = owners->empty() ? 0 : owners->back();
}

void RemoveOneOwnerFromFront(std::vector<txn_id_t>* owners, txn_id_t txn_id,
                             txn_id_t* latest_owner) {
  for (auto it = owners->begin(); it != owners->end(); ++it) {
    if (*it == txn_id) {
      owners->erase(it);
      break;
    }
  }
  *latest_owner = owners->empty() ? 0 : owners->back();
}

}  // namespace

const LsmSortedRun::Entry* LsmSortedRun::Find(std::string_view key) const {
  auto it = std::lower_bound(
      entries.begin(), entries.end(), key,
      [](const Entry& e, std::string_view k) { return e.key < k; });
  if (it != entries.end() && it->key == key) {
    return &*it;
  }
  return nullptr;
}

bool LsmTreeState::LookupLiveLocked(std::string_view key,
                                    std::string* value_out) const {
  if (destroyed) {
    return false;
  }
  auto mem_it = memtable.find(key);
  if (mem_it != memtable.end()) {
    if (mem_it->second.deleted) {
      return false;
    }
    if (value_out != nullptr) {
      *value_out = mem_it->second.value;
    }
    return true;
  }
  if (auto cit = committed_mem.find(key); cit != committed_mem.end()) {
    if (cit->second.deleted) {
      return false;
    }
    if (value_out != nullptr) {
      *value_out = cit->second.value;
    }
    return true;
  }
  for (const auto& run : std::ranges::reverse_view(runs)) {
    if (const auto* entry = run.Find(key)) {
      if (entry->deleted) {
        return false;
      }
      if (value_out != nullptr) {
        *value_out = entry->value;
      }
      return true;
    }
  }
  return false;
}

bool LsmTreeState::LookupCommittedLocked(std::string_view key,
                                         std::string* value_out,
                                         lsn_t* lsn_out) const {
  if (committed_destroyed) {
    if (lsn_out != nullptr) {
      *lsn_out = committed_destroy_lsn;
    }
    return false;
  }
  if (auto cit = committed_mem.find(key); cit != committed_mem.end()) {
    if (lsn_out != nullptr) {
      *lsn_out = cit->second.committed_lsn;
    }
    if (cit->second.deleted) {
      return false;
    }
    if (value_out != nullptr) {
      *value_out = cit->second.value;
    }
    return true;
  }
  for (const auto& run : std::ranges::reverse_view(runs)) {
    if (const auto* entry = run.Find(key)) {
      if (lsn_out != nullptr) {
        *lsn_out = entry->committed_lsn;
      }
      if (entry->deleted) {
        return false;
      }
      if (value_out != nullptr) {
        *value_out = entry->value;
      }
      return true;
    }
  }
  if (lsn_out != nullptr) {
    *lsn_out = 0;
  }
  return false;
}

LsmSortedRun LsmTreeState::BuildCompactedCommittedRunLocked() const {
  if (runs.empty() && committed_mem.empty()) {
    LsmSortedRun compacted;
    compacted.generation = next_generation;
    return compacted;
  }
  if (committed_mem.empty() && runs.size() == 1 &&
      std::ranges::none_of(runs[0].entries, &LsmSortedRun::Entry::deleted)) {
    LsmSortedRun compacted = runs[0];
    compacted.generation = next_generation;
    return compacted;
  }
  if (runs.size() <= 1) {
    LsmSortedRun compacted;
    compacted.generation = next_generation;
    const std::vector<LsmSortedRun::Entry>* base =
        runs.empty() ? nullptr : &runs[0].entries;
    const size_t base_n = base != nullptr ? base->size() : 0;
    compacted.entries.reserve(base_n + committed_mem.size());
    size_t i = 0;
    auto it = committed_mem.begin();
    while (i < base_n && it != committed_mem.end()) {
      const int cmp = (*base)[i].key.compare(it->first);
      if (cmp < 0) {
        if (!(*base)[i].deleted) {
          compacted.entries.push_back((*base)[i]);
        }
        ++i;
      } else {
        if (cmp == 0) {
          ++i;
        }
        if (!it->second.deleted) {
          compacted.entries.push_back(LsmSortedRun::Entry{
              .key = it->first,
              .value = it->second.value,
              .deleted = false,
              .committed_lsn = it->second.committed_lsn,
          });
        }
        ++it;
      }
    }
    while (i < base_n) {
      if (!(*base)[i].deleted) {
        compacted.entries.push_back((*base)[i]);
      }
      ++i;
    }
    for (; it != committed_mem.end(); ++it) {
      if (!it->second.deleted) {
        compacted.entries.push_back(LsmSortedRun::Entry{
            .key = it->first,
            .value = it->second.value,
            .deleted = false,
            .committed_lsn = it->second.committed_lsn,
        });
      }
    }
    return compacted;
  }
  std::map<std::string, LsmSortedRun::Entry, std::less<>> merged;
  for (const LsmSortedRun& run : runs) {
    for (const LsmSortedRun::Entry& e : run.entries) {
      merged[e.key] = e;
    }
  }
  for (const auto& [k, c] : committed_mem) {
    merged[k] = LsmSortedRun::Entry{
        .key = k,
        .value = c.value,
        .deleted = c.deleted,
        .committed_lsn = c.committed_lsn,
    };
  }
  LsmSortedRun compacted;
  compacted.generation = next_generation;
  compacted.entries.reserve(merged.size());
  for (auto& [k, e] : merged) {
    if (!e.deleted) {
      compacted.entries.push_back(std::move(e));
    }
  }
  return compacted;
}

bool LsmTreeState::FlushAndCompactIfIdleLocked() {
  if (active_uncommitted_ops != 0 || committed_mem.size() < kFlushThreshold) {
    return false;
  }
  LsmSortedRun flushed;
  flushed.generation = next_generation++;
  flushed.entries.reserve(committed_mem.size());
  for (auto& [k, c] : committed_mem) {
    flushed.entries.push_back(LsmSortedRun::Entry{
        .key = k,
        .value = std::move(c.value),
        .deleted = c.deleted,
        .committed_lsn = c.committed_lsn,
    });
  }
  committed_mem.clear();
  memtable.clear();
  runs.push_back(std::move(flushed));
  if (runs.size() >= kCompactionThreshold) {
    LsmSortedRun compacted = BuildCompactedCommittedRunLocked();
    runs.clear();
    if (!compacted.entries.empty()) {
      runs.push_back(std::move(compacted));
    }
    return true;
  }
  return false;
}

StatusOr<std::unique_ptr<PageManager>> PageManager::Create(
    std::string_view db_name, size_t /*capacity*/, bool /*initialize_meta*/) {
  const std::string path(db_name);
  const int fd = platform::OpenFile(
      path, {.mode = platform::OpenMode::kReadWrite, .create = true}, 0644);
  if (fd < 0) {
    return StatusError(
        StatusCode::kIOError,
        "failed to open database file " + path + ": " + std::strerror(errno));
  }
  if (platform::TryLockExclusive(fd) != 0) {
    const int err = errno;
    platform::CloseFile(fd);
    if (err == EWOULDBLOCK || err == EAGAIN) {
      return StatusError(StatusCode::kConflicts,
                         "database file is already open: " + path);
    }
    return StatusError(
        StatusCode::kIOError,
        "failed to lock database file " + path + ": " + std::strerror(err));
  }
  return std::unique_ptr<PageManager>(new PageManager(path, fd));
}

PageManager::PageManager(std::string db_name, int lock_fd)
    : db_name_(std::move(db_name)), lock_fd_(lock_fd) {}

PageManager::~PageManager() { CloseDatabaseFile(); }

void PageManager::CloseDatabaseFile() {
  std::scoped_lock io_lk(checkpoint_io_mu_);
  if (lock_fd_ >= 0) {
    (void)platform::UnlockFile(lock_fd_);
    platform::CloseFile(lock_fd_);
    lock_fd_ = -1;
  }
}

void PageManager::SetCompactionCallback(std::function<void()> callback) {
  std::scoped_lock lk(compaction_cb_mu_);
  compaction_cb_ = std::move(callback);
}

void PageManager::DropAllState() {
  max_committed_txn_id_.store(0, std::memory_order_release);
  unpersisted_committed_ops_.store(0, std::memory_order_relaxed);
  {
    std::scoped_lock lk(blocks_mu_);
    blocks_.clear();
    free_pages_.clear();
    pending_destroyed_pages_.clear();
  }
  {
    std::scoped_lock lk(trees_mu_);
    lsm_trees_.clear();
  }
  {
    std::scoped_lock lk(table_tails_mu_);
    table_tails_.clear();
    tail_reservations_.clear();
    unpublished_tail_pages_.clear();
  }
}

StatusOr<PageRef> PageManager::GetPage(page_id_t page_id, bool shared) {
  if (page_id == 0) {
    return Status::kNotExists;
  }
  Page* page = nullptr;
  {
    std::scoped_lock lk(blocks_mu_);
    auto it = blocks_.find(page_id);
    if (it == blocks_.end() || it->second == nullptr ||
        it->second->Type() != PageType::kRowPage) {
      cache_misses_.fetch_add(1, std::memory_order_relaxed);
      return Status::kNotExists;
    }
    cache_hits_.fetch_add(1, std::memory_order_relaxed);
    page = it->second.get();
  }
  return PageRef(page, page->Latch(), shared);
}

StatusOr<PageRef> PageManager::AllocateNewPage(Transaction& txn,
                                               PageType new_page_type) {
  Page* page = nullptr;
  page_id_t pid = 0;
  {
    std::scoped_lock lk(blocks_mu_);
    if (!free_pages_.empty()) {
      pid = free_pages_.back();
      free_pages_.pop_back();
    } else {
      pid = ++max_page_id_;
    }
    auto& slot = blocks_[pid];
    if (slot == nullptr) {
      // Publish only an unavailable placeholder until initialization owns
      // the content latch and the allocation WAL record has been queued.
      slot = std::make_unique<Page>(pid, PageType::kUnknown);
    }
    page = slot.get();
  }
  PageRef initialized(page, page->Latch(), false);
  txn.InvalidatePageVersions(pid);
  ASSIGN_OR_RETURN(lsn_t, lsn, txn.AllocatePageLog(pid, new_page_type));
  {
    // GetPage checks type under blocks_mu_. Keep that publication atomic
    // with respect to lookups while the content latch protects the reset.
    // Never wait for the page latch while holding blocks_mu_.
    std::scoped_lock lk(blocks_mu_);
    initialized->PageInit(pid, new_page_type);
    initialized->SetPageLSN(lsn);
  }
  return initialized;
}

Status PageManager::DestroyPage(Transaction& txn, Page* target) {
  if (target == nullptr) {
    return Status::kInvalidArgument;
  }
  const page_id_t pid = target->PageID();
  ASSIGN_OR_RETURN(lsn_t, lsn, txn.DestroyPageLog(pid));
  target->SetPageLSN(lsn);
  target->type_ = PageType::kUnknown;
  target->body.row_page.ClearLiveState();
  {
    std::scoped_lock lk(blocks_mu_);
    pending_destroyed_pages_[txn.ID()].push_back(pid);
  }
  return Status::kSuccess;
}

StatusOr<std::vector<page_id_t>> PageManager::InstallColumnarStubChain(
    page_id_t first_pid, const std::vector<uint32_t>& block_row_counts,
    lsn_t commit_lsn) {
  if (first_pid == 0) {
    return Status::kInvalidArgument;
  }
  if (block_row_counts.empty()) {
    return std::vector<page_id_t>{first_pid};
  }
  for (const uint32_t cnt : block_row_counts) {
    if (cnt == 0 || cnt > RowPage::kMaxColumnarRows) {
      return Status::kInvalidArgument;
    }
  }
  std::vector<page_id_t> pids;
  pids.reserve(block_row_counts.size());
  {
    std::scoped_lock lk(blocks_mu_);
    auto first_it = blocks_.find(first_pid);
    if (first_it == blocks_.end() || first_it->second == nullptr ||
        first_it->second->Type() != PageType::kRowPage) {
      return Status::kNotExists;
    }
    const RowPage& first_rp = first_it->second->body.row_page;
    if (!first_rp.slots_.empty() || first_rp.columnar_row_count_ != 0) {
      return Status::kConflicts;
    }
    pids.push_back(first_pid);
    for (size_t i = 1; i < block_row_counts.size(); ++i) {
      page_id_t pid = 0;
      if (!free_pages_.empty()) {
        pid = free_pages_.back();
        free_pages_.pop_back();
      } else {
        pid = ++max_page_id_;
      }
      pids.push_back(pid);
    }
    committed_max_page_id_ = std::max(committed_max_page_id_, max_page_id_);
    for (size_t i = 0; i < pids.size(); ++i) {
      const page_id_t pid = pids[i];
      const page_id_t prev_pid = i == 0 ? 0 : pids[i - 1];
      const page_id_t next_pid = (i + 1 < pids.size()) ? pids[i + 1] : 0;
      auto& slot = blocks_[pid];
      if (slot == nullptr) {
        slot = std::make_unique<Page>(pid, PageType::kRowPage);
      } else {
        slot->PageInit(pid, PageType::kRowPage);
      }
      RowPage& rp = slot->body.row_page;
      rp.Initialize();
      rp.prev_page_id_ = prev_pid;
      rp.next_page_id_ = next_pid;
      rp.columnar_row_count_ = block_row_counts[i];
      rp.committed_allocated_ = true;
      rp.committed_prev_page_id_ = prev_pid;
      rp.committed_next_page_id_ = next_pid;
      rp.committed_link_lsn_ = commit_lsn;
      rp.committed_columnar_row_count_ = block_row_counts[i];
      slot->SetPageLSN(commit_lsn);
    }
  }
  {
    std::scoped_lock lk(table_tails_mu_);
    table_tails_[first_pid] = pids.back();
  }
  return pids;
}

Status PageManager::HydrateColumnarStubPage(
    page_id_t page_id, std::vector<std::string>&& encoded_rows) {
  Page* page = nullptr;
  {
    std::scoped_lock lk(blocks_mu_);
    auto it = blocks_.find(page_id);
    if (it == blocks_.end() || it->second == nullptr ||
        it->second->Type() != PageType::kRowPage) {
      return Status::kNotExists;
    }
    page = it->second.get();
  }
  bool hydrated_live = false;
  std::vector<std::string> committed_copy;
  {
    PageRef ref(page, page->Latch(), /*shared=*/false);
    RowPage& rp = ref->body.row_page;
    if (rp.IsColumnarStub()) {
      committed_copy = encoded_rows;
      slot_t non_empty = 0;
      for (const std::string& s : encoded_rows) {
        if (!s.empty()) {
          ++non_empty;
        }
      }
      rp.slots_ = std::move(encoded_rows);
      rp.hole_sizes_.assign(rp.slots_.size(), 0);
      rp.row_count_ = non_empty;
      rp.columnar_row_count_ = 0;
      hydrated_live = true;
    }
  }
  if (hydrated_live) {
    std::scoped_lock lk(blocks_mu_);
    RowPage& rp = page->body.row_page;
    if (rp.committed_columnar_row_count_ != 0 && rp.committed_slots_.empty()) {
      rp.committed_slots_ = std::move(committed_copy);
      rp.committed_lsns_.assign(rp.committed_slots_.size(),
                                rp.committed_link_lsn_);
      rp.committed_columnar_row_count_ = 0;
    }
  }
  return Status::kSuccess;
}

LsmTreeState* PageManager::GetOrCreateTreeLocked(page_id_t tree_id) const {
  auto& ptr = lsm_trees_[tree_id];
  if (ptr == nullptr) {
    ptr = std::make_unique<LsmTreeState>();
  }
  return ptr.get();
}

LsmTreeState* PageManager::FindTreeLocked(page_id_t tree_id) const {
  auto it = lsm_trees_.find(tree_id);
  return it != lsm_trees_.end() ? it->second.get() : nullptr;
}

Status PageManager::LsmInsert(Transaction& txn, page_id_t tree_id,
                              std::string_view key, std::string_view value) {
  LsmTreeState* tree = nullptr;
  {
    std::scoped_lock lk(trees_mu_);
    tree = GetOrCreateTreeLocked(tree_id);
  }
  std::unique_lock tree_lk(tree->mutex);
  if (tree->destroyed) {
    if (tree->destroyed_by_txn != 0 && tree->destroyed_by_txn != txn.ID()) {
      return Status::kConflicts;
    }
    tree->destroyed = false;
    tree->destroyed_by_txn = 0;
  }
  std::string existing;
  if (tree->LookupLiveLocked(key, &existing)) {
    return Status::kDuplicates;
  }
  ASSIGN_OR_RETURN(lsn_t, lsn,
                   txn.LsmPutLog(tree_id, key, value, {}, /*had_prev=*/false));
  (void)lsn;
  auto& entry = tree->memtable[std::string(key)];
  entry.value.assign(value.data(), value.size());
  entry.deleted = false;
  entry.generation = tree->next_generation++;
  entry.uncommitted_txn = txn.ID();
  entry.uncommitted_owners.push_back(txn.ID());
  ++tree->active_uncommitted_ops;
  return Status::kSuccess;
}

Status PageManager::LsmUpdate(Transaction& txn, page_id_t tree_id,
                              std::string_view key, std::string_view value) {
  LsmTreeState* tree = nullptr;
  {
    std::scoped_lock lk(trees_mu_);
    tree = GetOrCreateTreeLocked(tree_id);
  }
  std::unique_lock tree_lk(tree->mutex);
  std::string existing;
  if (!tree->LookupLiveLocked(key, &existing)) {
    return Status::kNotExists;
  }
  ASSIGN_OR_RETURN(
      lsn_t, lsn,
      txn.LsmPutLog(tree_id, key, value, existing, /*had_prev=*/true));
  (void)lsn;
  auto& entry = tree->memtable[std::string(key)];
  entry.value.assign(value.data(), value.size());
  entry.deleted = false;
  entry.generation = tree->next_generation++;
  entry.uncommitted_txn = txn.ID();
  entry.uncommitted_owners.push_back(txn.ID());
  ++tree->active_uncommitted_ops;
  return Status::kSuccess;
}

Status PageManager::LsmDelete(Transaction& txn, page_id_t tree_id,
                              std::string_view key) {
  LsmTreeState* tree = nullptr;
  {
    std::scoped_lock lk(trees_mu_);
    tree = GetOrCreateTreeLocked(tree_id);
  }
  std::unique_lock tree_lk(tree->mutex);
  std::string existing;
  if (!tree->LookupLiveLocked(key, &existing)) {
    return Status::kNotExists;
  }
  ASSIGN_OR_RETURN(lsn_t, lsn, txn.LsmDeleteLog(tree_id, key, existing));
  (void)lsn;
  auto& entry = tree->memtable[std::string(key)];
  entry.value.clear();
  entry.deleted = true;
  entry.generation = tree->next_generation++;
  entry.uncommitted_txn = txn.ID();
  entry.uncommitted_owners.push_back(txn.ID());
  ++tree->active_uncommitted_ops;
  return Status::kSuccess;
}

StatusOr<std::string> PageManager::LsmRead(page_id_t tree_id,
                                           std::string_view key) const {
  cache_hits_.fetch_add(1, std::memory_order_relaxed);
  LsmTreeState* tree = nullptr;
  {
    std::scoped_lock lk(trees_mu_);
    tree = FindTreeLocked(tree_id);
  }
  if (tree == nullptr) {
    return Status::kNotExists;
  }
  std::shared_lock tree_lk(tree->mutex);
  std::string value;
  if (!tree->LookupLiveLocked(key, &value)) {
    return Status::kNotExists;
  }
  return value;
}

std::vector<std::pair<std::string, std::string>> PageManager::LsmScanRange(
    page_id_t tree_id, std::string_view begin, std::string_view end,
    bool ascending, const Transaction* reader) const {
  const txn_id_t reader_txn_id = reader != nullptr ? reader->ID() : 0;
  cache_hits_.fetch_add(2, std::memory_order_relaxed);
  if (!begin.empty() && !end.empty() && end < begin) {
    return {};
  }
  LsmTreeState* tree = nullptr;
  {
    std::scoped_lock lk(trees_mu_);
    tree = FindTreeLocked(tree_id);
  }
  if (tree == nullptr) {
    return {};
  }
  std::shared_lock tree_lk(tree->mutex);
  if (tree->destroyed) {
    return {};
  }

  auto in_bounds = [&](std::string_view k) -> bool {
    if (!begin.empty() && k < begin) {
      return false;
    }
    if (!end.empty()) {
      if (ascending ? (end < k) : (end <= k)) {
        return false;
      }
    }
    return true;
  };

  std::map<std::string, std::pair<std::string, bool>, std::less<>> merged;
  for (const LsmSortedRun& run : tree->runs) {
    auto it = begin.empty() ? run.entries.begin()
                            : std::lower_bound(
                                  run.entries.begin(), run.entries.end(), begin,
                                  [](const LsmSortedRun::Entry& e,
                                     std::string_view b) { return e.key < b; });
    for (; it != run.entries.end(); ++it) {
      if (!end.empty() && (ascending ? (end < it->key) : (end <= it->key))) {
        break;
      }
      if (in_bounds(it->key)) {
        merged[it->key] = {it->value, it->deleted};
      }
    }
  }
  auto cmem_it = begin.empty() ? tree->committed_mem.begin()
                               : tree->committed_mem.lower_bound(begin);
  for (; cmem_it != tree->committed_mem.end(); ++cmem_it) {
    if (!end.empty() &&
        (ascending ? (end < cmem_it->first) : (end <= cmem_it->first))) {
      break;
    }
    if (in_bounds(cmem_it->first)) {
      merged[cmem_it->first] = {cmem_it->second.value, cmem_it->second.deleted};
    }
  }
  auto mem_it = begin.empty() ? tree->memtable.begin()
                              : tree->memtable.lower_bound(begin);
  for (; mem_it != tree->memtable.end(); ++mem_it) {
    if (!end.empty() &&
        (ascending ? (end < mem_it->first) : (end <= mem_it->first))) {
      break;
    }
    if (!in_bounds(mem_it->first)) {
      continue;
    }
    const auto& owners = mem_it->second.uncommitted_owners;
    const bool has_foreign_uncommitted =
        reader_txn_id != 0 && std::ranges::any_of(owners, [&](txn_id_t owner) {
          return owner != reader_txn_id;
        });
    if (has_foreign_uncommitted) {
      // A non-unique index bucket can be updated by several transactions.
      // Start from the committed baseline and apply only this reader's own
      // staged logical WAL mutations in chronological order.
      std::string cur_val;
      bool cur_del = true;
      if (const auto base_it = merged.find(mem_it->first);
          base_it != merged.end()) {
        cur_val = base_it->second.first;
        cur_del = base_it->second.second;
      }
      bool has_own = false;
      reader->ForEachPendingLsmWrite(
          tree_id, mem_it->first, [&](const LogRecord& r) {
            has_own = true;
            const bool is_del = r.type == LogType::kLsmDelete;
            const bool before_del = !is_del && !r.lsm_had_prev;
            const std::string_view before_bytes =
                before_del ? std::string_view{} : std::string_view(r.undo_data);
            const std::string_view after_bytes =
                is_del ? std::string_view{} : std::string_view(r.redo_data);
            if (!TryApplyIndexListDelta(before_bytes, before_del, after_bytes,
                                        is_del, &cur_val, &cur_del)) {
              cur_del = is_del;
              cur_val = is_del ? std::string() : r.redo_data;
            }
          });
      if (has_own) {
        merged[mem_it->first] = {std::move(cur_val), cur_del};
      }
      continue;
    }
    merged[mem_it->first] = {mem_it->second.value, mem_it->second.deleted};
  }

  std::vector<std::pair<std::string, std::string>> result;
  result.reserve(merged.size());
  if (ascending) {
    for (auto& [k, val_del] : merged) {
      if (!val_del.second) {
        result.emplace_back(k, std::move(val_del.first));
      }
    }
  } else {
    for (auto& it : std::ranges::reverse_view(merged)) {
      if (!it.second.second) {
        result.emplace_back(it.first, std::move(it.second.first));
      }
    }
  }
  return result;
}

Status PageManager::LsmDestroy(Transaction& txn, page_id_t tree_id) {
  LsmTreeState* tree = nullptr;
  {
    std::scoped_lock lk(trees_mu_);
    tree = FindTreeLocked(tree_id);
  }
  if (tree == nullptr) {
    return Status::kSuccess;
  }
  ASSIGN_OR_RETURN(lsn_t, lsn, txn.LsmDestroyLog(tree_id));
  (void)lsn;
  std::unique_lock tree_lk(tree->mutex);
  tree->destroyed = true;
  tree->destroyed_by_txn = txn.ID();
  ++tree->active_uncommitted_ops;
  return Status::kSuccess;
}

void PageManager::ApplyTransactionCommit(
    txn_id_t txn_id, lsn_t commit_lsn, const std::vector<LogRecord>& records) {
  txn_id_t cur_max = max_committed_txn_id_.load(std::memory_order_relaxed);
  while (txn_id > cur_max && !max_committed_txn_id_.compare_exchange_weak(
                                 cur_max, txn_id, std::memory_order_release,
                                 std::memory_order_relaxed)) {
  }
  for (const LogRecord& r : records) {
    const lsn_t rec_lsn = r.record_lsn != 0 ? r.record_lsn : commit_lsn;
    switch (r.type) {
      case LogType::kSystemAllocPage: {
        std::scoped_lock lk(blocks_mu_);
        committed_max_page_id_ = std::max(committed_max_page_id_, r.pid);
        auto it = blocks_.find(r.pid);
        if (it != blocks_.end() && it->second != nullptr) {
          it->second->body.row_page.committed_allocated_ = true;
          it->second->SetPageLSN(std::max(it->second->PageLSN(), rec_lsn));
        }
        break;
      }
      case LogType::kSystemDestroyPage: {
        std::scoped_lock lk(blocks_mu_);
        auto it = blocks_.find(r.pid);
        if (it != blocks_.end() && it->second != nullptr) {
          RowPage& rp = it->second->body.row_page;
          rp.committed_allocated_ = false;
          rp.committed_columnar_row_count_ = 0;
          rp.committed_slots_.clear();
          rp.committed_lsns_.clear();
          rp.committed_prev_page_id_ = 0;
          rp.committed_next_page_id_ = 0;
          rp.committed_link_lsn_ = rec_lsn;
          it->second->SetPageLSN(std::max(it->second->PageLSN(), rec_lsn));
        }
        break;
      }
      case LogType::kInsertRow:
      case LogType::kUpdateRow:
      case LogType::kCompensateUpdate: {
        std::scoped_lock lk(blocks_mu_);
        committed_max_page_id_ = std::max(committed_max_page_id_, r.pid);
        auto it = blocks_.find(r.pid);
        if (it != blocks_.end() && it->second != nullptr) {
          Page* page = it->second.get();
          RowPage& rp = page->body.row_page;
          rp.committed_allocated_ = true;
          if (r.slot >= rp.committed_slots_.size()) {
            rp.committed_slots_.resize(static_cast<size_t>(r.slot) + 1);
            rp.committed_lsns_.resize(static_cast<size_t>(r.slot) + 1, 0);
          }
          if (rec_lsn >= rp.committed_lsns_[r.slot]) {
            rp.committed_slots_[r.slot] = r.redo_data;
            rp.committed_lsns_[r.slot] = rec_lsn;
          }
          page->SetPageLSN(std::max(page->PageLSN(), rec_lsn));
        }
        break;
      }
      case LogType::kDeleteRow: {
        std::scoped_lock lk(blocks_mu_);
        auto it = blocks_.find(r.pid);
        if (it != blocks_.end() && it->second != nullptr) {
          Page* page = it->second.get();
          RowPage& rp = page->body.row_page;
          if (r.slot >= rp.committed_slots_.size()) {
            rp.committed_slots_.resize(static_cast<size_t>(r.slot) + 1);
            rp.committed_lsns_.resize(static_cast<size_t>(r.slot) + 1, 0);
          }
          if (rec_lsn >= rp.committed_lsns_[r.slot]) {
            rp.committed_slots_[r.slot].clear();
            rp.committed_lsns_[r.slot] = rec_lsn;
          }
          page->SetPageLSN(std::max(page->PageLSN(), rec_lsn));
        }
        break;
      }
      case LogType::kSetRowPageNext: {
        std::scoped_lock lk(blocks_mu_);
        auto it = blocks_.find(r.pid);
        if (it != blocks_.end() && it->second != nullptr) {
          Page* page = it->second.get();
          if (rec_lsn >= page->body.row_page.committed_link_lsn_) {
            page->body.row_page.committed_next_page_id_ = r.redo_pid;
            page->body.row_page.committed_link_lsn_ = rec_lsn;
          }
          page->SetPageLSN(std::max(page->PageLSN(), rec_lsn));
        }
        break;
      }
      case LogType::kSetRowPagePrev: {
        std::scoped_lock lk(blocks_mu_);
        auto it = blocks_.find(r.pid);
        if (it != blocks_.end() && it->second != nullptr) {
          Page* page = it->second.get();
          if (rec_lsn >= page->body.row_page.committed_link_lsn_) {
            page->body.row_page.committed_prev_page_id_ = r.redo_pid;
            page->body.row_page.committed_link_lsn_ = rec_lsn;
          }
          page->SetPageLSN(std::max(page->PageLSN(), rec_lsn));
        }
        break;
      }
      case LogType::kLsmPut:
      case LogType::kLsmDelete: {
        const bool is_delete = r.type == LogType::kLsmDelete;
        LsmTreeState* tree = nullptr;
        {
          std::scoped_lock lk(trees_mu_);
          tree = GetOrCreateTreeLocked(r.pid);
        }
        std::unique_lock tree_lk(tree->mutex);
        if (!is_delete) {
          tree->committed_destroyed = false;
        }
        lsn_t existing_lsn = 0;
        std::string committed_val;
        bool committed_del =
            !tree->LookupCommittedLocked(r.key, &committed_val, &existing_lsn);
        const bool before_del = !is_delete && !r.lsm_had_prev;
        const std::string_view before_bytes =
            before_del ? std::string_view{} : std::string_view(r.undo_data);
        const std::string_view after_bytes =
            is_delete ? std::string_view{} : std::string_view(r.redo_data);
        if (TryApplyIndexListDelta(before_bytes, before_del, after_bytes,
                                   is_delete, &committed_val, &committed_del)) {
          tree->committed_mem[r.key] = LsmTreeState::CommittedEntry{
              .value = std::move(committed_val),
              .deleted = committed_del,
              .committed_lsn = std::max(rec_lsn, existing_lsn),
          };
        } else if (rec_lsn >= existing_lsn) {
          tree->committed_mem[r.key] = LsmTreeState::CommittedEntry{
              .value = is_delete ? std::string() : r.redo_data,
              .deleted = is_delete,
              .committed_lsn = rec_lsn,
          };
        }
        if (auto mit = tree->memtable.find(r.key);
            mit != tree->memtable.end()) {
          RemoveOneOwnerFromFront(&mit->second.uncommitted_owners, txn_id,
                                  &mit->second.uncommitted_txn);
          if (mit->second.uncommitted_owners.empty()) {
            std::string synced_val;
            const bool synced_live =
                tree->LookupCommittedLocked(r.key, &synced_val);
            mit->second.value =
                synced_live ? std::move(synced_val) : std::string();
            mit->second.deleted = !synced_live;
            mit->second.generation = tree->next_generation++;
          }
        }
        if (tree->active_uncommitted_ops > 0) {
          --tree->active_uncommitted_ops;
        }
        std::ignore = tree->FlushAndCompactIfIdleLocked();
        break;
      }
      case LogType::kLsmDestroy: {
        LsmTreeState* tree = nullptr;
        {
          std::scoped_lock lk(trees_mu_);
          tree = FindTreeLocked(r.pid);
        }
        if (tree != nullptr) {
          std::unique_lock tree_lk(tree->mutex);
          tree->committed_destroyed = true;
          tree->destroyed_by_txn = 0;
          tree->committed_destroy_lsn = rec_lsn;
          tree->memtable.clear();
          tree->committed_mem.clear();
          tree->runs.clear();
          if (tree->active_uncommitted_ops > 0) {
            --tree->active_uncommitted_ops;
          }
        }
        break;
      }
      default:
        break;
    }
  }
  {
    std::scoped_lock lk(blocks_mu_);
    auto it = pending_destroyed_pages_.find(txn_id);
    if (it != pending_destroyed_pages_.end()) {
      for (const page_id_t pid : it->second) {
        free_pages_.push_back(pid);
      }
      pending_destroyed_pages_.erase(it);
    }
  }
  if (!records.empty()) {
    const size_t ops = unpersisted_committed_ops_.fetch_add(
                           records.size(), std::memory_order_relaxed) +
                       records.size();
    if (ops >= kCompactionPersistOpsThreshold) {
      unpersisted_committed_ops_.store(0, std::memory_order_relaxed);
      std::function<void()> cb;
      {
        std::scoped_lock lk(compaction_cb_mu_);
        cb = compaction_cb_;
      }
      if (cb) {
        cb();
      }
    }
  }
}

void PageManager::ApplyTransactionAbort(txn_id_t txn_id,
                                        const std::vector<LogRecord>& records) {
  txn_id_t cur_max = max_committed_txn_id_.load(std::memory_order_relaxed);
  while (txn_id > cur_max && !max_committed_txn_id_.compare_exchange_weak(
                                 cur_max, txn_id, std::memory_order_release,
                                 std::memory_order_relaxed)) {
  }
  for (const auto& r : std::ranges::reverse_view(records)) {
    auto lock_page_if_needed = [](Page* p) {
      return p->IsExclusivelyOwnedByCurrentThread()
                 ? std::unique_lock<std::shared_mutex>()
                 : std::unique_lock<std::shared_mutex>(*p->Latch());
    };
    switch (r.type) {
      case LogType::kInsertRow: {
        Page* page = nullptr;
        {
          std::scoped_lock lk(blocks_mu_);
          auto bit = blocks_.find(r.pid);
          if (bit != blocks_.end()) {
            page = bit->second.get();
          }
        }
        if (page != nullptr) {
          auto page_lk = lock_page_if_needed(page);
          page->body.row_page.DeleteRow(r.slot);
        }
        break;
      }
      case LogType::kUpdateRow: {
        Page* page = nullptr;
        {
          std::scoped_lock lk(blocks_mu_);
          auto bit = blocks_.find(r.pid);
          if (bit != blocks_.end()) {
            page = bit->second.get();
          }
        }
        if (page != nullptr) {
          auto page_lk = lock_page_if_needed(page);
          RestoreAbortedRow(page->body.row_page, r.slot, r.undo_data);
        }
        break;
      }
      case LogType::kDeleteRow: {
        Page* page = nullptr;
        {
          std::scoped_lock lk(blocks_mu_);
          auto bit = blocks_.find(r.pid);
          if (bit != blocks_.end()) {
            page = bit->second.get();
          }
        }
        if (page != nullptr) {
          auto page_lk = lock_page_if_needed(page);
          RestoreAbortedRow(page->body.row_page, r.slot, r.undo_data);
        }
        break;
      }
      case LogType::kSetRowPageNext: {
        Page* page = nullptr;
        {
          std::scoped_lock lk(blocks_mu_);
          auto bit = blocks_.find(r.pid);
          if (bit != blocks_.end()) {
            page = bit->second.get();
          }
        }
        if (page != nullptr) {
          auto page_lk = lock_page_if_needed(page);
          page->body.row_page.SetNextPageImpl(r.undo_pid);
        }
        break;
      }
      case LogType::kSetRowPagePrev: {
        Page* page = nullptr;
        {
          std::scoped_lock lk(blocks_mu_);
          auto bit = blocks_.find(r.pid);
          if (bit != blocks_.end()) {
            page = bit->second.get();
          }
        }
        if (page != nullptr) {
          auto page_lk = lock_page_if_needed(page);
          page->body.row_page.SetPrevPageImpl(r.undo_pid);
        }
        break;
      }
      case LogType::kSystemAllocPage: {
        std::scoped_lock lk(blocks_mu_);
        auto bit = blocks_.find(r.pid);
        if (bit != blocks_.end() && bit->second != nullptr) {
          bit->second->type_ = PageType::kUnknown;
          bit->second->body.row_page.Initialize();
        }
        free_pages_.push_back(r.pid);
        break;
      }
      case LogType::kSystemDestroyPage: {
        std::scoped_lock lk(blocks_mu_);
        auto bit = blocks_.find(r.pid);
        if (bit != blocks_.end() && bit->second != nullptr) {
          Page* p = bit->second.get();
          p->type_ = PageType::kRowPage;
          RowPage& rp = p->body.row_page;
          rp.prev_page_id_ = rp.committed_prev_page_id_;
          rp.next_page_id_ = rp.committed_next_page_id_;
          rp.columnar_row_count_ = rp.committed_columnar_row_count_;
          rp.slots_ = rp.committed_slots_;
          rp.hole_sizes_.assign(rp.slots_.size(), 0);
          rp.row_count_ = 0;
          for (const std::string& s : rp.slots_) {
            if (!s.empty()) {
              ++rp.row_count_;
            }
          }
        }
        break;
      }
      case LogType::kLsmPut:
      case LogType::kLsmDelete: {
        const bool is_delete = r.type == LogType::kLsmDelete;
        LsmTreeState* tree = nullptr;
        {
          std::scoped_lock lk(trees_mu_);
          tree = FindTreeLocked(r.pid);
        }
        if (tree != nullptr) {
          std::unique_lock tree_lk(tree->mutex);
          auto mit = tree->memtable.find(r.key);
          if (mit != tree->memtable.end()) {
            RemoveOneOwnerFromBack(&mit->second.uncommitted_owners, txn_id,
                                   &mit->second.uncommitted_txn);
            if (mit->second.uncommitted_owners.empty()) {
              std::string synced_val;
              const bool synced_live =
                  tree->LookupCommittedLocked(r.key, &synced_val);
              mit->second.value =
                  synced_live ? std::move(synced_val) : std::string();
              mit->second.deleted = !synced_live;
              mit->second.generation = tree->next_generation++;
            } else {
              const bool undo_del = !is_delete && !r.lsm_had_prev;
              const std::string_view redo_bytes =
                  is_delete ? std::string_view{}
                            : std::string_view(r.redo_data);
              const std::string_view undo_bytes =
                  undo_del ? std::string_view{} : std::string_view(r.undo_data);
              if (TryApplyIndexListDelta(redo_bytes, is_delete, undo_bytes,
                                         undo_del, &mit->second.value,
                                         &mit->second.deleted)) {
                mit->second.generation = tree->next_generation++;
              } else if ((is_delete && mit->second.deleted) ||
                         (!is_delete && !mit->second.deleted &&
                          mit->second.value == r.redo_data)) {
                mit->second.value = undo_del ? std::string() : r.undo_data;
                mit->second.deleted = undo_del;
                mit->second.generation = tree->next_generation++;
              }
            }
          } else {
            const bool undo_del = !is_delete && !r.lsm_had_prev;
            auto& restored = tree->memtable[r.key];
            restored.value = undo_del ? std::string() : r.undo_data;
            restored.deleted = undo_del;
            restored.generation = tree->next_generation++;
            restored.uncommitted_txn = 0;
            restored.uncommitted_owners.clear();
          }
          if (tree->active_uncommitted_ops > 0) {
            --tree->active_uncommitted_ops;
          }
          tree->FlushAndCompactIfIdleLocked();
        }
        break;
      }
      case LogType::kLsmDestroy: {
        LsmTreeState* tree = nullptr;
        {
          std::scoped_lock lk(trees_mu_);
          tree = FindTreeLocked(r.pid);
        }
        if (tree != nullptr) {
          std::unique_lock tree_lk(tree->mutex);
          tree->destroyed = false;
          tree->destroyed_by_txn = 0;
          if (tree->active_uncommitted_ops > 0) {
            --tree->active_uncommitted_ops;
          }
        }
        break;
      }
      default:
        break;
    }
  }
  {
    std::scoped_lock lk(blocks_mu_);
    pending_destroyed_pages_.erase(txn_id);
  }
}

Status PageManager::WriteFuzzyCheckpoint(lsn_t replay_start_lsn) {
  unpersisted_committed_ops_.store(0, std::memory_order_relaxed);
  std::string buf;
  buf.reserve(4096);
  buf.append(kFuzzyCheckpointMagic.data(), kFuzzyCheckpointMagic.size());
  AppendU32(&buf, kFuzzyCheckpointVersion);
  AppendU64(&buf, replay_start_lsn);
  AppendU64(&buf, max_committed_txn_id_.load(std::memory_order_acquire));

  struct BlockSnapshot {
    page_id_t pid{0};
    page_id_t prev_pid{0};
    page_id_t next_pid{0};
    lsn_t link_lsn{0};
    uint32_t columnar_rows{0};
    std::vector<std::string> slots;
    std::vector<lsn_t> lsns;
  };
  std::vector<BlockSnapshot> block_snaps;
  std::vector<page_id_t> free_snap;
  page_id_t max_pid_snap = 10;
  {
    std::scoped_lock lk(blocks_mu_);
    max_pid_snap = std::max(max_page_id_, committed_max_page_id_);
    free_snap = free_pages_;
    block_snaps.reserve(blocks_.size());
    for (const auto& [pid, p] : blocks_) {
      if (p == nullptr) {
        continue;
      }
      const RowPage& rp = p->body.row_page;
      if (!rp.committed_allocated_) {
        continue;
      }
      block_snaps.push_back(BlockSnapshot{
          .pid = p->PageID(),
          .prev_pid = rp.committed_prev_page_id_,
          .next_pid = rp.committed_next_page_id_,
          .link_lsn = rp.committed_link_lsn_,
          .columnar_rows = rp.committed_columnar_row_count_,
          .slots = rp.committed_slots_,
          .lsns = rp.committed_lsns_,
      });
    }
  }

  AppendU64(&buf, max_pid_snap);
  AppendU32(&buf, static_cast<uint32_t>(free_snap.size()));
  for (page_id_t fpid : free_snap) {
    AppendU64(&buf, fpid);
  }
  AppendU32(&buf, static_cast<uint32_t>(block_snaps.size()));
  for (const BlockSnapshot& b : block_snaps) {
    AppendU64(&buf, b.pid);
    AppendU64(&buf, b.prev_pid);
    AppendU64(&buf, b.next_pid);
    AppendU64(&buf, b.link_lsn);
    if (b.columnar_rows != 0 && b.slots.empty()) {
      AppendU32(&buf, 0x80000000U | b.columnar_rows);
      continue;
    }
    const auto nslots = static_cast<uint32_t>(b.slots.size());
    AppendU32(&buf, nslots);
    for (uint32_t i = 0; i < nslots; ++i) {
      const lsn_t slsn = i < b.lsns.size() ? b.lsns[i] : 0;
      AppendU64(&buf, slsn);
      AppendString(&buf, b.slots[i]);
    }
  }

  struct TreeSnapshot {
    page_id_t tree_id{0};
    LsmSortedRun compacted;
  };
  std::vector<TreeSnapshot> tree_snaps;
  {
    std::vector<std::pair<page_id_t, LsmTreeState*>> trees;
    {
      std::scoped_lock lk(trees_mu_);
      trees.reserve(lsm_trees_.size());
      for (const auto& [tid, t] : lsm_trees_) {
        if (t != nullptr) {
          trees.emplace_back(tid, t.get());
        }
      }
    }
    for (auto [tid, t] : trees) {
      std::shared_lock tree_lk(t->mutex);
      if (t->committed_destroyed && t->committed_mem.empty() &&
          t->runs.empty()) {
        continue;
      }
      LsmSortedRun compacted = t->BuildCompactedCommittedRunLocked();
      tree_snaps.push_back(TreeSnapshot{
          .tree_id = tid,
          .compacted = std::move(compacted),
      });
    }
  }

  AppendU32(&buf, static_cast<uint32_t>(tree_snaps.size()));
  for (const TreeSnapshot& ts : tree_snaps) {
    AppendU64(&buf, ts.tree_id);
    AppendU32(&buf, static_cast<uint32_t>(ts.compacted.entries.size()));
    for (const LsmSortedRun::Entry& e : ts.compacted.entries) {
      AppendU64(&buf, e.committed_lsn);
      AppendString(&buf, e.key);
      AppendString(&buf, e.value);
    }
  }

  const uint32_t crc = Crc32C(buf.data(), buf.size());
  AppendU32(&buf, crc);

  // Atomically replace db_name_ via a temp file + fdatasync + rename while
  // transferring the exclusive advisory lock to the new inode before rename so
  // no crash can leave db_name_ truncated/half-written and no lock gap exists.
  std::scoped_lock io_lk(checkpoint_io_mu_);
  if (lock_fd_ < 0) {
    return Status::kIOError;
  }
  const std::string tmp_path = db_name_ + ".tmp";
  const int tmp_fd = platform::OpenFile(tmp_path,
                                        {.mode = platform::OpenMode::kReadWrite,
                                         .create = true,
                                         .truncate = true},
                                        0644);
  if (tmp_fd < 0) {
    return StatusError(
        StatusCode::kIOError,
        "open failed on checkpoint temp: " + std::string(std::strerror(errno)));
  }
  auto cleanup_tmp = [&](int fd_to_close) {
    if (fd_to_close >= 0) {
      platform::CloseFile(fd_to_close);
    }
    std::error_code ec;
    std::filesystem::remove(tmp_path, ec);
  };
  size_t written = 0;
  while (written < buf.size()) {
    const ssize_t n =
        platform::Pwrite(tmp_fd, buf.data() + written, buf.size() - written,
                         static_cast<int64_t>(written));
    if (n <= 0) {
      if (errno == EINTR) {
        continue;
      }
      const int err = errno;
      cleanup_tmp(tmp_fd);
      return StatusError(
          StatusCode::kIOError,
          "pwrite failed on checkpoint: " + std::string(std::strerror(err)));
    }
    written += static_cast<size_t>(n);
  }
  if (platform::DataSync(tmp_fd) != 0) {
    const int err = errno;
    cleanup_tmp(tmp_fd);
    return StatusError(
        StatusCode::kIOError,
        "fdatasync failed on checkpoint: " + std::string(std::strerror(err)));
  }
  if (platform::TryLockExclusive(tmp_fd) != 0) {
    const int err = errno;
    cleanup_tmp(tmp_fd);
    return StatusError(
        StatusCode::kIOError,
        "lock failed on checkpoint temp: " + std::string(std::strerror(err)));
  }
  std::error_code rename_ec;
  std::filesystem::rename(tmp_path, db_name_, rename_ec);
  if (rename_ec) {
    (void)platform::UnlockFile(tmp_fd);
    cleanup_tmp(tmp_fd);
    return StatusError(StatusCode::kIOError,
                       "rename failed on checkpoint: " + rename_ec.message());
  }
  const std::filesystem::path parent_dir =
      std::filesystem::path(db_name_).parent_path();
  (void)platform::SyncDirectory(parent_dir.empty() ? std::filesystem::path(".")
                                                   : parent_dir);
  (void)platform::UnlockFile(lock_fd_);
  platform::CloseFile(lock_fd_);
  lock_fd_ = tmp_fd;
  return Status::kSuccess;
}

Status PageManager::LoadFuzzyCheckpoint(lsn_t* replay_start_lsn_out,
                                        bool* loaded_out) try {
  if (replay_start_lsn_out != nullptr) {
    *replay_start_lsn_out = 0;
  }
  if (loaded_out != nullptr) {
    *loaded_out = false;
  }
  std::error_code ec;
  const auto fsize = std::filesystem::file_size(db_name_, ec);
  if (ec || fsize == 0) {
    return Status::kSuccess;
  }
  std::ifstream in(db_name_, std::ios::binary);
  if (!in) {
    return Status::kIOError;
  }
  std::string buf(static_cast<size_t>(fsize), '\0');
  in.read(buf.data(), static_cast<std::streamsize>(buf.size()));
  if (static_cast<size_t>(in.gcount()) != buf.size()) {
    return Status::kCorrupt;
  }
  constexpr size_t kMinSize = kFuzzyCheckpointMagic.size() +
                              (sizeof(uint32_t) * 4) + (sizeof(uint64_t) * 2);
  if (buf.size() < kMinSize ||
      std::memcmp(buf.data(), kFuzzyCheckpointMagic.data(),
                  kFuzzyCheckpointMagic.size()) != 0) {
    return Status::kCorrupt;
  }
  const size_t payload_size = buf.size() - sizeof(uint32_t);
  uint32_t stored_crc = 0;
  std::memcpy(&stored_crc, buf.data() + payload_size, sizeof(uint32_t));
  if (stored_crc != Crc32C(buf.data(), payload_size)) {
    return Status::kCorrupt;
  }

  size_t off = kFuzzyCheckpointMagic.size();
  uint32_t version = 0;
  lsn_t replay_lsn = 0;
  txn_id_t persisted_max_txn = 0;
  page_id_t max_pid = 10;
  uint32_t free_count = 0;
  if (!ConsumeU32(buf.data(), payload_size, &off, &version) ||
      (version != 1 && version != kFuzzyCheckpointVersion) ||
      !ConsumeU64(buf.data(), payload_size, &off, &replay_lsn) ||
      (version >= 2 &&
       !ConsumeU64(buf.data(), payload_size, &off, &persisted_max_txn)) ||
      !ConsumeU64(buf.data(), payload_size, &off, &max_pid) ||
      !ConsumeU32(buf.data(), payload_size, &off, &free_count) ||
      free_count > (payload_size - off) / sizeof(uint64_t)) {
    return Status::kCorrupt;
  }

  std::scoped_lock lk(blocks_mu_, trees_mu_);
  blocks_.clear();
  lsm_trees_.clear();
  free_pages_.clear();
  max_committed_txn_id_.store(persisted_max_txn, std::memory_order_release);
  unpersisted_committed_ops_.store(0, std::memory_order_relaxed);
  max_page_id_ = std::max<page_id_t>(10, max_pid);
  committed_max_page_id_ = max_page_id_;

  for (uint32_t i = 0; i < free_count; ++i) {
    page_id_t fpid = 0;
    if (!ConsumeU64(buf.data(), payload_size, &off, &fpid)) {
      return Status::kCorrupt;
    }
    free_pages_.push_back(fpid);
  }

  uint32_t num_blocks = 0;
  if (!ConsumeU32(buf.data(), payload_size, &off, &num_blocks) ||
      num_blocks >
          (payload_size - off) / (4 * sizeof(uint64_t) + sizeof(uint32_t))) {
    return Status::kCorrupt;
  }
  for (uint32_t i = 0; i < num_blocks; ++i) {
    page_id_t pid = 0;
    page_id_t prev_pid = 0;
    page_id_t next_pid = 0;
    lsn_t link_lsn = 0;
    uint32_t raw_nslots = 0;
    if (!ConsumeU64(buf.data(), payload_size, &off, &pid) ||
        !ConsumeU64(buf.data(), payload_size, &off, &prev_pid) ||
        !ConsumeU64(buf.data(), payload_size, &off, &next_pid) ||
        !ConsumeU64(buf.data(), payload_size, &off, &link_lsn) ||
        !ConsumeU32(buf.data(), payload_size, &off, &raw_nslots)) {
      return Status::kCorrupt;
    }
    auto page = std::make_unique<Page>(pid, PageType::kRowPage);
    RowPage& rp = page->body.row_page;
    rp.committed_allocated_ = true;
    rp.prev_page_id_ = prev_pid;
    rp.next_page_id_ = next_pid;
    rp.committed_prev_page_id_ = prev_pid;
    rp.committed_next_page_id_ = next_pid;
    rp.committed_link_lsn_ = link_lsn;
    if ((raw_nslots & 0x80000000U) != 0) {
      const uint32_t col_rows = raw_nslots & 0x7FFFFFFFU;
      if (col_rows == 0 || col_rows > RowPage::kMaxColumnarRows) {
        return Status::kCorrupt;
      }
      rp.columnar_row_count_ = col_rows;
      rp.committed_columnar_row_count_ = col_rows;
      page->SetPageLSN(link_lsn);
      max_page_id_ = std::max(max_page_id_, pid);
      committed_max_page_id_ = max_page_id_;
      blocks_[pid] = std::move(page);
      continue;
    }
    const uint32_t nslots = raw_nslots;
    if (nslots > RowPage::kMaxColumnarRows ||
        nslots > (payload_size - off) / (sizeof(uint64_t) + sizeof(uint32_t))) {
      return Status::kCorrupt;
    }
    rp.slots_.resize(nslots);
    rp.hole_sizes_.assign(nslots, 0);
    rp.committed_slots_.resize(nslots);
    rp.committed_lsns_.resize(nslots, 0);
    lsn_t max_slot_lsn = link_lsn;
    for (uint32_t s = 0; s < nslots; ++s) {
      lsn_t slsn = 0;
      std::string val;
      if (!ConsumeU64(buf.data(), payload_size, &off, &slsn) ||
          !ConsumeString(buf.data(), payload_size, &off, &val)) {
        return Status::kCorrupt;
      }
      if (!val.empty()) {
        ++rp.row_count_;
      }
      rp.slots_[s] = val;
      rp.committed_slots_[s] = std::move(val);
      rp.committed_lsns_[s] = slsn;
      max_slot_lsn = std::max(max_slot_lsn, slsn);
    }
    page->SetPageLSN(max_slot_lsn);
    max_page_id_ = std::max(max_page_id_, pid);
    committed_max_page_id_ = max_page_id_;
    blocks_[pid] = std::move(page);
  }

  uint32_t num_trees = 0;
  if (!ConsumeU32(buf.data(), payload_size, &off, &num_trees) ||
      num_trees >
          (payload_size - off) / (sizeof(uint64_t) + sizeof(uint32_t))) {
    return Status::kCorrupt;
  }
  for (uint32_t i = 0; i < num_trees; ++i) {
    page_id_t tid = 0;
    uint32_t nentries = 0;
    if (!ConsumeU64(buf.data(), payload_size, &off, &tid) ||
        !ConsumeU32(buf.data(), payload_size, &off, &nentries) ||
        nentries >
            (payload_size - off) / (sizeof(uint64_t) + 2 * sizeof(uint32_t))) {
      return Status::kCorrupt;
    }
    auto tree = std::make_unique<LsmTreeState>();
    LsmSortedRun run;
    run.generation = tree->next_generation++;
    run.entries.reserve(nentries);
    for (uint32_t e = 0; e < nentries; ++e) {
      lsn_t elsn = 0;
      std::string key;
      std::string val;
      if (!ConsumeU64(buf.data(), payload_size, &off, &elsn) ||
          !ConsumeString(buf.data(), payload_size, &off, &key) ||
          !ConsumeString(buf.data(), payload_size, &off, &val)) {
        return Status::kCorrupt;
      }
      run.entries.push_back(LsmSortedRun::Entry{
          .key = std::move(key),
          .value = std::move(val),
          .deleted = false,
          .committed_lsn = elsn,
      });
    }
    if (!run.entries.empty()) {
      tree->runs.push_back(std::move(run));
    }
    lsm_trees_[tid] = std::move(tree);
  }

  if (off != payload_size) {
    return Status::kCorrupt;
  }
  if (replay_start_lsn_out != nullptr) {
    *replay_start_lsn_out = replay_lsn;
  }
  if (loaded_out != nullptr) {
    *loaded_out = true;
  }
  return Status::kSuccess;
} catch (const std::bad_alloc&) {
  return Status::kNoSpace;
} catch (const std::exception&) {
  return Status::kCorrupt;
}

Status PageManager::ReplayCommittedRecord(lsn_t lsn, const LogRecord& r) {
  txn_id_t cur_max = max_committed_txn_id_.load(std::memory_order_relaxed);
  while (r.owner > cur_max && !max_committed_txn_id_.compare_exchange_weak(
                                  cur_max, r.owner, std::memory_order_release,
                                  std::memory_order_relaxed)) {
  }
  switch (r.type) {
    case LogType::kSystemAllocPage: {
      std::scoped_lock lk(blocks_mu_);
      max_page_id_ = std::max(max_page_id_, r.pid);
      committed_max_page_id_ = std::max(committed_max_page_id_, r.pid);
      std::erase(free_pages_, r.pid);
      auto& slot = blocks_[r.pid];
      if (slot == nullptr) {
        slot = std::make_unique<Page>(r.pid, PageType::kRowPage);
        slot->body.row_page.committed_allocated_ = true;
        slot->SetPageLSN(lsn);
      } else if (slot->Type() != PageType::kRowPage && lsn >= slot->PageLSN()) {
        slot->PageInit(r.pid, PageType::kRowPage);
        slot->body.row_page.committed_allocated_ = true;
        slot->SetPageLSN(lsn);
      } else {
        slot->body.row_page.committed_allocated_ = true;
      }
      break;
    }
    case LogType::kSystemDestroyPage: {
      std::scoped_lock lk(blocks_mu_);
      max_page_id_ = std::max(max_page_id_, r.pid);
      committed_max_page_id_ = std::max(committed_max_page_id_, r.pid);
      auto it = blocks_.find(r.pid);
      if (it != blocks_.end() && it->second != nullptr &&
          lsn >= it->second->PageLSN()) {
        it->second->type_ = PageType::kUnknown;
        it->second->body.row_page.Initialize();
        it->second->SetPageLSN(lsn);
      }
      if (std::ranges::find(free_pages_, r.pid) == free_pages_.end()) {
        free_pages_.push_back(r.pid);
      }
      break;
    }
    case LogType::kInsertRow:
    case LogType::kUpdateRow:
    case LogType::kCompensateUpdate: {
      std::scoped_lock lk(blocks_mu_);
      max_page_id_ = std::max(max_page_id_, r.pid);
      committed_max_page_id_ = std::max(committed_max_page_id_, r.pid);
      std::erase(free_pages_, r.pid);
      auto& page = blocks_[r.pid];
      if (page == nullptr) {
        page = std::make_unique<Page>(r.pid, PageType::kRowPage);
      }
      page->type_ = PageType::kRowPage;
      RowPage& rp = page->body.row_page;
      rp.committed_allocated_ = true;
      if (r.slot >= rp.committed_slots_.size()) {
        rp.slots_.resize(static_cast<size_t>(r.slot) + 1);
        rp.hole_sizes_.resize(static_cast<size_t>(r.slot) + 1, 0);
        rp.committed_slots_.resize(static_cast<size_t>(r.slot) + 1);
        rp.committed_lsns_.resize(static_cast<size_t>(r.slot) + 1, 0);
      }
      if (lsn >= rp.committed_lsns_[r.slot]) {
        if (rp.slots_[r.slot].empty() && !r.redo_data.empty()) {
          ++rp.row_count_;
        }
        rp.slots_[r.slot] = r.redo_data;
        rp.committed_slots_[r.slot] = r.redo_data;
        rp.committed_lsns_[r.slot] = lsn;
        page->SetPageLSN(std::max(page->PageLSN(), lsn));
      }
      break;
    }
    case LogType::kDeleteRow: {
      std::scoped_lock lk(blocks_mu_);
      auto it = blocks_.find(r.pid);
      if (it == blocks_.end() || it->second == nullptr) {
        break;
      }
      Page* page = it->second.get();
      RowPage& rp = page->body.row_page;
      if (r.slot >= rp.committed_slots_.size()) {
        rp.slots_.resize(static_cast<size_t>(r.slot) + 1);
        rp.committed_slots_.resize(static_cast<size_t>(r.slot) + 1);
        rp.committed_lsns_.resize(static_cast<size_t>(r.slot) + 1, 0);
      }
      if (lsn >= rp.committed_lsns_[r.slot]) {
        if (!rp.slots_[r.slot].empty()) {
          --rp.row_count_;
        }
        rp.slots_[r.slot].clear();
        rp.committed_slots_[r.slot].clear();
        rp.committed_lsns_[r.slot] = lsn;
        page->SetPageLSN(std::max(page->PageLSN(), lsn));
      }
      break;
    }
    case LogType::kSetRowPageNext: {
      std::scoped_lock lk(blocks_mu_);
      max_page_id_ = std::max(max_page_id_, r.pid);
      auto& page = blocks_[r.pid];
      if (page == nullptr) {
        page = std::make_unique<Page>(r.pid, PageType::kRowPage);
      }
      RowPage& rp = page->body.row_page;
      if (lsn >= rp.committed_link_lsn_) {
        rp.next_page_id_ = r.redo_pid;
        rp.committed_next_page_id_ = r.redo_pid;
        rp.committed_link_lsn_ = lsn;
        page->SetPageLSN(std::max(page->PageLSN(), lsn));
      }
      break;
    }
    case LogType::kSetRowPagePrev: {
      std::scoped_lock lk(blocks_mu_);
      max_page_id_ = std::max(max_page_id_, r.pid);
      auto& page = blocks_[r.pid];
      if (page == nullptr) {
        page = std::make_unique<Page>(r.pid, PageType::kRowPage);
      }
      RowPage& rp = page->body.row_page;
      if (lsn >= rp.committed_link_lsn_) {
        rp.prev_page_id_ = r.redo_pid;
        rp.committed_prev_page_id_ = r.redo_pid;
        rp.committed_link_lsn_ = lsn;
        page->SetPageLSN(std::max(page->PageLSN(), lsn));
      }
      break;
    }
    case LogType::kLsmPut:
    case LogType::kLsmDelete: {
      const bool is_delete = r.type == LogType::kLsmDelete;
      std::scoped_lock lk(trees_mu_);
      if (!is_delete) {
        max_page_id_ = std::max(max_page_id_, r.pid);
        committed_max_page_id_ = std::max(committed_max_page_id_, r.pid);
      }
      LsmTreeState* tree = GetOrCreateTreeLocked(r.pid);
      if (lsn >= tree->committed_destroy_lsn) {
        if (!is_delete) {
          tree->destroyed = false;
          tree->committed_destroyed = false;
        }
        lsn_t existing_lsn = 0;
        std::string committed_val;
        bool committed_del =
            !tree->LookupCommittedLocked(r.key, &committed_val, &existing_lsn);
        const bool before_del = !is_delete && !r.lsm_had_prev;
        const std::string_view before_bytes =
            before_del ? std::string_view{} : std::string_view(r.undo_data);
        const std::string_view after_bytes =
            is_delete ? std::string_view{} : std::string_view(r.redo_data);
        if (TryApplyIndexListDelta(before_bytes, before_del, after_bytes,
                                   is_delete, &committed_val, &committed_del)) {
          auto& mem = tree->memtable[r.key];
          mem.value = committed_val;
          mem.deleted = committed_del;
          mem.generation = tree->next_generation++;
          mem.uncommitted_txn = 0;
          mem.uncommitted_owners.clear();
          tree->committed_mem[r.key] = LsmTreeState::CommittedEntry{
              .value = std::move(committed_val),
              .deleted = committed_del,
              .committed_lsn = std::max(lsn, existing_lsn),
          };
        } else if (lsn >= existing_lsn) {
          auto& mem = tree->memtable[r.key];
          mem.value = is_delete ? std::string() : r.redo_data;
          mem.deleted = is_delete;
          mem.generation = tree->next_generation++;
          mem.uncommitted_txn = 0;
          mem.uncommitted_owners.clear();
          tree->committed_mem[r.key] = LsmTreeState::CommittedEntry{
              .value = is_delete ? std::string() : r.redo_data,
              .deleted = is_delete,
              .committed_lsn = lsn,
          };
        }
      }
      break;
    }
    case LogType::kLsmDestroy: {
      std::scoped_lock lk(trees_mu_);
      LsmTreeState* tree = GetOrCreateTreeLocked(r.pid);
      if (lsn >= tree->committed_destroy_lsn) {
        tree->destroyed = true;
        tree->committed_destroyed = true;
        tree->committed_destroy_lsn = lsn;
        tree->memtable.clear();
        tree->committed_mem.clear();
        tree->runs.clear();
      }
      break;
    }
    default:
      break;
  }
  return Status::kSuccess;
}

page_id_t PageManager::GetTableTail(page_id_t first_page,
                                    page_id_t catalog_hint) {
  std::scoped_lock lock(table_tails_mu_);
  auto [it, inserted] = table_tails_.try_emplace(
      first_page, catalog_hint != 0 ? catalog_hint : first_page);
  if (!inserted && it->second == 0) {
    it->second = catalog_hint != 0 ? catalog_hint : first_page;
  }
  return it->second;
}

page_id_t PageManager::GetTableTailFor(txn_id_t viewer, page_id_t first_page,
                                       page_id_t catalog_hint) {
  std::scoped_lock lock(table_tails_mu_);
  const auto res = tail_reservations_.find(first_page);
  if (res != tail_reservations_.end() && res->second.owner == viewer &&
      res->second.new_tail != 0) {
    return res->second.new_tail;
  }
  auto [it, inserted] = table_tails_.try_emplace(
      first_page, catalog_hint != 0 ? catalog_hint : first_page);
  if (!inserted && it->second == 0) {
    it->second = catalog_hint != 0 ? catalog_hint : first_page;
  }
  return it->second;
}

Status PageManager::TryReserveTableTail(txn_id_t owner, page_id_t first_page,
                                        page_id_t expected) {
  std::scoped_lock lock(table_tails_mu_);
  const auto res = tail_reservations_.find(first_page);
  if (res != tail_reservations_.end()) {
    if (res->second.owner == owner) {
      const page_id_t visible_tail = res->second.new_tail != 0
                                         ? res->second.new_tail
                                         : res->second.predecessor;
      return visible_tail == expected ? Status::kSuccess : Status::kConflicts;
    }
    return Status::kConflicts;
  }
  const auto tail = table_tails_.find(first_page);
  if (tail != table_tails_.end() && tail->second != expected) {
    return Status::kConflicts;
  }
  table_tails_[first_page] = expected;
  tail_reservations_[first_page] =
      TailReservation{.owner = owner, .predecessor = expected, .new_tail = 0};
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

Status PageManager::ReserveTableTailImpl(txn_id_t owner, page_id_t first_page,
                                         page_id_t expected,
                                         const Transaction* watcher) {
  std::unique_lock lock(table_tails_mu_);
  for (const auto& [reserved_table, reservation] : tail_reservations_) {
    if (reservation.owner == owner && reserved_table != first_page) {
      return StatusError(StatusCode::kConflicts,
                         "transaction already holds a table tail reservation");
    }
  }
  txn_id_t recorded_holder = 0;
  struct WaitCleanup {
    WaitCleanup(const Transaction* transaction, txn_id_t* holder)
        : watcher(transaction), recorded_holder(holder) {}
    WaitCleanup(const WaitCleanup&) = delete;
    WaitCleanup& operator=(const WaitCleanup&) = delete;
    WaitCleanup(WaitCleanup&&) = delete;
    WaitCleanup& operator=(WaitCleanup&&) = delete;
    const Transaction* watcher;
    txn_id_t* recorded_holder;
    ~WaitCleanup() {
      if (watcher != nullptr && *recorded_holder != 0) {
        watcher->ClearWaitForHolder();
        watcher->DebugSetPhase(nullptr);
      }
    }
  } cleanup{watcher, &recorded_holder};
  const auto start = std::chrono::steady_clock::now();
  const auto deadline = start + kTableTailWaitTimeout;
  bool waited = false;
  for (;;) {
    if (watcher != nullptr && watcher->IsWounded()) {
      return StatusError(StatusCode::kConflicts,
                         "wounded while waiting for table tail reservation");
    }
    const auto res = tail_reservations_.find(first_page);
    if (res == tail_reservations_.end()) {
      break;
    }
    if (res->second.owner == owner) {
      const page_id_t visible_tail = res->second.new_tail != 0
                                         ? res->second.new_tail
                                         : res->second.predecessor;
      return visible_tail == expected ? Status::kSuccess : Status::kConflicts;
    }
    if (watcher != nullptr && res->second.owner != recorded_holder) {
      recorded_holder = res->second.owner;
      watcher->DebugSetPhase("tail-reservation-wait");
      watcher->NoteWaitForHolder(recorded_holder, RowPosition(first_page, 0));
    }
    waited = true;
    const auto now = std::chrono::steady_clock::now();
    if (now >= deadline) {
      return StatusError(StatusCode::kConflicts,
                         "table tail reservation wait timed out");
    }
    table_tails_cv_.wait_for(
        lock, std::min(kTableTailWaitSlice,
                       std::chrono::duration_cast<std::chrono::milliseconds>(
                           deadline - now)));
  }
  if (watcher != nullptr && recorded_holder != 0) {
    watcher->ClearWaitForHolder();
    watcher->DebugSetPhase(nullptr);
    recorded_holder = 0;
  }
  if (waited) {
    const auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                        std::chrono::steady_clock::now() - start)
                        .count();
    if (ns >= 1'000'000) {
      tail_wait_count_.fetch_add(1, std::memory_order_relaxed);
      tail_wait_ns_.fetch_add(static_cast<uint64_t>(ns),
                              std::memory_order_relaxed);
    }
  }
  const auto tail = table_tails_.find(first_page);
  if (tail != table_tails_.end() && tail->second != expected) {
    return Status::kConflicts;
  }
  table_tails_[first_page] = expected;
  tail_reservations_[first_page] =
      TailReservation{.owner = owner, .predecessor = expected, .new_tail = 0};
  return Status::kSuccess;
}

Status PageManager::LinkReservedTableTail(txn_id_t owner, page_id_t first_page,
                                          page_id_t predecessor,
                                          page_id_t successor) {
  std::scoped_lock lock(table_tails_mu_);
  const auto res = tail_reservations_.find(first_page);
  if (res == tail_reservations_.end() || res->second.owner != owner) {
    return Status::kConflicts;
  }
  const page_id_t current_tail = res->second.new_tail != 0
                                     ? res->second.new_tail
                                     : res->second.predecessor;
  if (current_tail != predecessor) {
    return Status::kConflicts;
  }
  res->second.new_tail = successor;
  unpublished_tail_pages_[successor] = PendingTailLink{
      .owner = owner,
      .first_page = first_page,
      .predecessor = predecessor,
      .poisoned = false,
  };
  return Status::kSuccess;
}

Status PageManager::CheckTableTailPage(txn_id_t viewer, page_id_t first_page,
                                       page_id_t page_id) {
  std::scoped_lock lock(table_tails_mu_);
  const auto it = unpublished_tail_pages_.find(page_id);
  if (it == unpublished_tail_pages_.end()) {
    return Status::kSuccess;
  }
  if (it->second.first_page != first_page || it->second.poisoned) {
    return Status::kCorrupt;
  }
  return it->second.owner == viewer ? Status::kSuccess : Status::kConflicts;
}

Status PageManager::WaitForTableTailPage(txn_id_t viewer, page_id_t first_page,
                                         page_id_t page_id) {
  return WaitForTableTailPageImpl(viewer, first_page, page_id, nullptr);
}

Status PageManager::WaitForTableTailPage(Transaction& txn, page_id_t first_page,
                                         page_id_t page_id) {
  return WaitForTableTailPageImpl(txn.ID(), first_page, page_id, &txn);
}

Status PageManager::WaitForTableTailPageImpl(txn_id_t viewer,
                                             page_id_t first_page,
                                             page_id_t page_id,
                                             const Transaction* watcher) {
  std::unique_lock lock(table_tails_mu_);
  txn_id_t recorded_holder = 0;
  struct WaitCleanup {
    WaitCleanup(const Transaction* transaction, txn_id_t* holder)
        : watcher(transaction), recorded_holder(holder) {}
    WaitCleanup(const WaitCleanup&) = delete;
    WaitCleanup& operator=(const WaitCleanup&) = delete;
    WaitCleanup(WaitCleanup&&) = delete;
    WaitCleanup& operator=(WaitCleanup&&) = delete;
    const Transaction* watcher;
    txn_id_t* recorded_holder;
    ~WaitCleanup() {
      if (watcher != nullptr && *recorded_holder != 0) {
        watcher->ClearWaitForHolder();
        watcher->DebugSetPhase(nullptr);
      }
    }
  } cleanup{watcher, &recorded_holder};
  const auto deadline =
      std::chrono::steady_clock::now() + kTableTailWaitTimeout;
  for (;;) {
    const auto it = unpublished_tail_pages_.find(page_id);
    if (it == unpublished_tail_pages_.end()) {
      return Status::kSuccess;
    }
    if (it->second.first_page != first_page || it->second.poisoned) {
      return Status::kCorrupt;
    }
    if (it->second.owner == viewer) {
      return Status::kSuccess;
    }
    if (watcher != nullptr && watcher->IsWounded()) {
      return StatusError(StatusCode::kConflicts,
                         "wounded while waiting for unpublished table tail");
    }
    if (watcher != nullptr && it->second.owner != recorded_holder) {
      recorded_holder = it->second.owner;
      watcher->DebugSetPhase("tail-page-wait");
      watcher->NoteWaitForHolder(recorded_holder,
                                 RowPosition(first_page, page_id));
    }
    const auto now = std::chrono::steady_clock::now();
    if (now >= deadline) {
      return StatusError(StatusCode::kConflicts,
                         "unpublished table tail wait timed out");
    }
    table_tails_cv_.wait_for(
        lock, std::min(kTableTailWaitSlice,
                       std::chrono::duration_cast<std::chrono::milliseconds>(
                           deadline - now)));
  }
}

Status PageManager::WaitForTableTailQuiescence(page_id_t first_page,
                                               txn_id_t requester) {
  std::unique_lock lock(table_tails_mu_);
  const auto has_foreign_tail_work = [&] {
    const auto res = tail_reservations_.find(first_page);
    if (res != tail_reservations_.end() && res->second.owner != requester) {
      return true;
    }
    return std::ranges::any_of(unpublished_tail_pages_, [&](const auto& entry) {
      const auto& pending = entry.second;
      return pending.first_page == first_page && pending.owner != requester;
    });
  };
  if (!table_tails_cv_.wait_for(lock, kTableTailWaitTimeout,
                                [&] { return !has_foreign_tail_work(); })) {
    return StatusError(StatusCode::kConflicts,
                       "table tail quiescence wait timed out");
  }
  return Status::kSuccess;
}

void PageManager::ResolveTableTailReservations(txn_id_t owner,
                                               TableTailResolution resolution) {
  std::scoped_lock lock(table_tails_mu_);
  for (auto it = tail_reservations_.begin(); it != tail_reservations_.end();) {
    if (it->second.owner != owner) {
      ++it;
      continue;
    }
    if (resolution == TableTailResolution::kPublish &&
        it->second.new_tail != 0) {
      table_tails_[it->first] = it->second.new_tail;
    } else if (resolution == TableTailResolution::kDiscard) {
      table_tails_[it->first] = it->second.predecessor;
    }
    it = tail_reservations_.erase(it);
  }
  for (auto it = unpublished_tail_pages_.begin();
       it != unpublished_tail_pages_.end();) {
    if (it->second.owner != owner) {
      ++it;
      continue;
    }
    if (resolution == TableTailResolution::kPoison) {
      it->second.poisoned = true;
      ++it;
      continue;
    }
    it = unpublished_tail_pages_.erase(it);
  }
  table_tails_cv_.notify_all();
}

void PageManager::CancelTableTailReservation(txn_id_t owner,
                                             page_id_t first_page) {
  std::scoped_lock lock(table_tails_mu_);
  const auto res = tail_reservations_.find(first_page);
  if (res != tail_reservations_.end() && res->second.owner == owner &&
      res->second.new_tail == 0) {
    tail_reservations_.erase(res);
    table_tails_cv_.notify_all();
  }
}

void PageManager::AdvanceTableTail(page_id_t first_page, page_id_t expected,
                                   page_id_t next) {
  std::scoped_lock lock(table_tails_mu_);
  auto [it, inserted] = table_tails_.try_emplace(first_page, next);
  if (!inserted && (it->second == 0 || it->second == expected)) {
    it->second = next;
  }
}

void PageManager::ForgetTableTail(page_id_t first_page) {
  std::scoped_lock lock(table_tails_mu_);
  table_tails_.erase(first_page);
  tail_reservations_.erase(first_page);
  for (auto it = unpublished_tail_pages_.begin();
       it != unpublished_tail_pages_.end();) {
    if (it->second.first_page == first_page) {
      it = unpublished_tail_pages_.erase(it);
    } else {
      ++it;
    }
  }
  table_tails_cv_.notify_all();
}

void PageManager::NotifyTableTailWaiters() {
  std::scoped_lock lock(table_tails_mu_);
  table_tails_cv_.notify_all();
}

}  // namespace tinylamb
