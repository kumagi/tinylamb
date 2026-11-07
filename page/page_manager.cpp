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

#include <fcntl.h>
#include <sys/file.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <map>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/crc32c.hpp"
#include "common/log_message.hpp"
#include "common/serdes.hpp"
#include "common/status_or.hpp"
#include "page/page.hpp"
#include "page/page_ref.hpp"
#include "page/page_type.hpp"
#include "recovery/log_record.hpp"
#include "transaction/transaction.hpp"

namespace tinylamb {

namespace {
constexpr char kFuzzyCheckpointMagic[8] = {'T', 'L', 'S', 'M',
                                           'C', 'K', 'P', '1'};
constexpr uint32_t kFuzzyCheckpointVersion = 1;
constexpr auto kTableTailWaitTimeout = std::chrono::milliseconds(5000);
constexpr auto kTableTailWaitSlice = std::chrono::milliseconds(5);

void AppendU32(std::string* dst, uint32_t v) {
  char buf[sizeof(uint32_t)];
  std::memcpy(buf, &v, sizeof(uint32_t));
  dst->append(buf, sizeof(uint32_t));
}
void AppendU64(std::string* dst, uint64_t v) {
  char buf[sizeof(uint64_t)];
  std::memcpy(buf, &v, sizeof(uint64_t));
  dst->append(buf, sizeof(uint64_t));
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
  for (auto it = runs.rbegin(); it != runs.rend(); ++it) {
    if (const auto* entry = it->Find(key)) {
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

LsmSortedRun LsmTreeState::BuildCompactedCommittedRunLocked() const {
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

void LsmTreeState::FlushAndCompactIfIdleLocked() {
  if (active_uncommitted_ops != 0 || committed_mem.size() < kFlushThreshold) {
    return;
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
  }
}

StatusOr<std::unique_ptr<PageManager>> PageManager::Create(
    std::string_view db_name, size_t /*capacity*/, bool /*initialize_meta*/) {
  const std::string path(db_name);
  const int fd = ::open(path.c_str(), O_CREAT | O_RDWR | O_CLOEXEC, 0644);
  if (fd < 0) {
    return StatusError(
        StatusCode::kIOError,
        "failed to open database file " + path + ": " + std::strerror(errno));
  }
  if (::flock(fd, LOCK_EX | LOCK_NB) != 0) {
    const int err = errno;
    ::close(fd);
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
  if (lock_fd_ >= 0) {
    (void)::flock(lock_fd_, LOCK_UN);
    ::close(lock_fd_);
    lock_fd_ = -1;
  }
}

void PageManager::DropAllState() {
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
      slot = std::make_unique<Page>(pid, new_page_type);
    } else {
      slot->PageInit(pid, new_page_type);
    }
    page = slot.get();
  }
  txn.InvalidatePageVersions(pid);
  ASSIGN_OR_RETURN(lsn_t, lsn, txn.AllocatePageLog(pid, new_page_type));
  page->SetPageLSN(lsn);
  return PageRef(page, page->Latch(), false);
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
    tree->destroyed = false;
  }
  std::string existing;
  if (tree->LookupLiveLocked(key, &existing)) {
    return Status::kDuplicates;
  }
  ASSIGN_OR_RETURN(lsn_t, lsn,
                   txn.LsmPutLog(tree_id, key, value, {}, /*had_prev=*/false));
  (void)lsn;
  tree->memtable[std::string(key)] = LsmTreeState::MemEntry{
      .value = std::string(value),
      .deleted = false,
      .generation = tree->next_generation++,
  };
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
  tree->memtable[std::string(key)] = LsmTreeState::MemEntry{
      .value = std::string(value),
      .deleted = false,
      .generation = tree->next_generation++,
  };
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
  tree->memtable[std::string(key)] = LsmTreeState::MemEntry{
      .value = {},
      .deleted = true,
      .generation = tree->next_generation++,
  };
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
    bool ascending) const {
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
  auto mem_it = begin.empty() ? tree->memtable.begin()
                              : tree->memtable.lower_bound(begin);
  for (; mem_it != tree->memtable.end(); ++mem_it) {
    if (!end.empty() &&
        (ascending ? (end < mem_it->first) : (end <= mem_it->first))) {
      break;
    }
    if (in_bounds(mem_it->first)) {
      merged[mem_it->first] = {mem_it->second.value, mem_it->second.deleted};
    }
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
    for (auto it = merged.rbegin(); it != merged.rend(); ++it) {
      if (!it->second.second) {
        result.emplace_back(it->first, std::move(it->second.first));
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
  ++tree->active_uncommitted_ops;
  return Status::kSuccess;
}

void PageManager::ApplyTransactionCommit(
    txn_id_t txn_id, lsn_t commit_lsn, const std::vector<LogRecord>& records) {
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
      case LogType::kLsmPut: {
        LsmTreeState* tree = nullptr;
        {
          std::scoped_lock lk(trees_mu_);
          tree = GetOrCreateTreeLocked(r.pid);
        }
        std::unique_lock tree_lk(tree->mutex);
        tree->committed_destroyed = false;
        lsn_t existing_lsn = 0;
        if (auto cit = tree->committed_mem.find(r.key);
            cit != tree->committed_mem.end()) {
          existing_lsn = cit->second.committed_lsn;
        } else {
          for (auto rit = tree->runs.rbegin(); rit != tree->runs.rend();
               ++rit) {
            if (const auto* e = rit->Find(r.key); e != nullptr) {
              existing_lsn = e->committed_lsn;
              break;
            }
          }
        }
        if (rec_lsn >= existing_lsn) {
          tree->committed_mem[r.key] = LsmTreeState::CommittedEntry{
              .value = r.redo_data,
              .deleted = false,
              .committed_lsn = rec_lsn,
          };
        }
        if (tree->active_uncommitted_ops > 0) {
          --tree->active_uncommitted_ops;
        }
        tree->FlushAndCompactIfIdleLocked();
        break;
      }
      case LogType::kLsmDelete: {
        LsmTreeState* tree = nullptr;
        {
          std::scoped_lock lk(trees_mu_);
          tree = GetOrCreateTreeLocked(r.pid);
        }
        std::unique_lock tree_lk(tree->mutex);
        lsn_t existing_lsn = 0;
        if (auto cit = tree->committed_mem.find(r.key);
            cit != tree->committed_mem.end()) {
          existing_lsn = cit->second.committed_lsn;
        } else {
          for (auto rit = tree->runs.rbegin(); rit != tree->runs.rend();
               ++rit) {
            if (const auto* e = rit->Find(r.key); e != nullptr) {
              existing_lsn = e->committed_lsn;
              break;
            }
          }
        }
        if (rec_lsn >= existing_lsn) {
          tree->committed_mem[r.key] = LsmTreeState::CommittedEntry{
              .value = {},
              .deleted = true,
              .committed_lsn = rec_lsn,
          };
        }
        if (tree->active_uncommitted_ops > 0) {
          --tree->active_uncommitted_ops;
        }
        tree->FlushAndCompactIfIdleLocked();
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
}

void PageManager::ApplyTransactionAbort(txn_id_t txn_id,
                                        const std::vector<LogRecord>& records) {
  for (auto it = records.rbegin(); it != records.rend(); ++it) {
    const LogRecord& r = *it;
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
          (void)page->body.row_page.UpdateRow(r.slot, r.undo_data);
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
          (void)page->body.row_page.InsertRowAt(r.slot, r.undo_data);
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
      case LogType::kLsmPut: {
        LsmTreeState* tree = nullptr;
        {
          std::scoped_lock lk(trees_mu_);
          tree = FindTreeLocked(r.pid);
        }
        if (tree != nullptr) {
          std::unique_lock tree_lk(tree->mutex);
          const auto mit = tree->memtable.find(r.key);
          if (mit == tree->memtable.end() ||
              (!mit->second.deleted && mit->second.value == r.redo_data)) {
            if (r.lsm_had_prev) {
              tree->memtable[r.key] = LsmTreeState::MemEntry{
                  .value = r.undo_data,
                  .deleted = false,
                  .generation = tree->next_generation++,
              };
            } else {
              tree->memtable[r.key] = LsmTreeState::MemEntry{
                  .value = {},
                  .deleted = true,
                  .generation = tree->next_generation++,
              };
            }
          }
          if (tree->active_uncommitted_ops > 0) {
            --tree->active_uncommitted_ops;
          }
          tree->FlushAndCompactIfIdleLocked();
        }
        break;
      }
      case LogType::kLsmDelete: {
        LsmTreeState* tree = nullptr;
        {
          std::scoped_lock lk(trees_mu_);
          tree = FindTreeLocked(r.pid);
        }
        if (tree != nullptr) {
          std::unique_lock tree_lk(tree->mutex);
          const auto mit = tree->memtable.find(r.key);
          if (mit == tree->memtable.end() || mit->second.deleted) {
            tree->memtable[r.key] = LsmTreeState::MemEntry{
                .value = r.undo_data,
                .deleted = false,
                .generation = tree->next_generation++,
            };
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
  std::string buf;
  buf.reserve(4096);
  buf.append(kFuzzyCheckpointMagic, sizeof(kFuzzyCheckpointMagic));
  AppendU32(&buf, kFuzzyCheckpointVersion);
  AppendU64(&buf, replay_start_lsn);

  struct BlockSnapshot {
    page_id_t pid{0};
    page_id_t prev_pid{0};
    page_id_t next_pid{0};
    lsn_t link_lsn{0};
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
      std::unique_lock tree_lk(t->mutex);
      if (t->committed_destroyed && t->committed_mem.empty() &&
          t->runs.empty()) {
        continue;
      }
      LsmSortedRun compacted = t->BuildCompactedCommittedRunLocked();
      if (t->active_uncommitted_ops == 0) {
        t->committed_mem.clear();
        t->memtable.clear();
        t->runs.clear();
        if (!compacted.entries.empty()) {
          t->runs.push_back(compacted);
        }
      }
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

  // Write in-place to lock_fd_ so the flock held on db_name_ stays attached to
  // the exact inode on disk!
  if (lock_fd_ < 0) {
    return Status::kIOError;
  }
  if (::ftruncate(lock_fd_, 0) != 0) {
    return StatusError(
        StatusCode::kIOError,
        "ftruncate failed on checkpoint: " + std::string(std::strerror(errno)));
  }
  size_t written = 0;
  while (written < buf.size()) {
    const ssize_t n =
        ::pwrite(lock_fd_, buf.data() + written, buf.size() - written,
                 static_cast<off_t>(written));
    if (n <= 0) {
      if (errno == EINTR) {
        continue;
      }
      return StatusError(
          StatusCode::kIOError,
          "pwrite failed on checkpoint: " + std::string(std::strerror(errno)));
    }
    written += static_cast<size_t>(n);
  }
  if (::fdatasync(lock_fd_) != 0) {
    return StatusError(
        StatusCode::kIOError,
        "fdatasync failed on checkpoint: " + std::string(std::strerror(errno)));
  }
  return Status::kSuccess;
}

Status PageManager::LoadFuzzyCheckpoint(lsn_t* replay_start_lsn_out,
                                        bool* loaded_out) {
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
  constexpr size_t kMinSize = sizeof(kFuzzyCheckpointMagic) +
                              sizeof(uint32_t) * 4 + sizeof(uint64_t) * 2;
  if (buf.size() < kMinSize ||
      std::memcmp(buf.data(), kFuzzyCheckpointMagic,
                  sizeof(kFuzzyCheckpointMagic)) != 0) {
    return Status::kCorrupt;
  }
  const size_t payload_size = buf.size() - sizeof(uint32_t);
  uint32_t stored_crc = 0;
  std::memcpy(&stored_crc, buf.data() + payload_size, sizeof(uint32_t));
  if (stored_crc != Crc32C(buf.data(), payload_size)) {
    return Status::kCorrupt;
  }

  size_t off = sizeof(kFuzzyCheckpointMagic);
  uint32_t version = 0;
  lsn_t replay_lsn = 0;
  page_id_t max_pid = 10;
  uint32_t free_count = 0;
  if (!ConsumeU32(buf.data(), payload_size, &off, &version) ||
      version != kFuzzyCheckpointVersion ||
      !ConsumeU64(buf.data(), payload_size, &off, &replay_lsn) ||
      !ConsumeU64(buf.data(), payload_size, &off, &max_pid) ||
      !ConsumeU32(buf.data(), payload_size, &off, &free_count)) {
    return Status::kCorrupt;
  }

  std::scoped_lock lk(blocks_mu_, trees_mu_);
  blocks_.clear();
  lsm_trees_.clear();
  free_pages_.clear();
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
  if (!ConsumeU32(buf.data(), payload_size, &off, &num_blocks)) {
    return Status::kCorrupt;
  }
  for (uint32_t i = 0; i < num_blocks; ++i) {
    page_id_t pid = 0;
    page_id_t prev_pid = 0;
    page_id_t next_pid = 0;
    lsn_t link_lsn = 0;
    uint32_t nslots = 0;
    if (!ConsumeU64(buf.data(), payload_size, &off, &pid) ||
        !ConsumeU64(buf.data(), payload_size, &off, &prev_pid) ||
        !ConsumeU64(buf.data(), payload_size, &off, &next_pid) ||
        !ConsumeU64(buf.data(), payload_size, &off, &link_lsn) ||
        !ConsumeU32(buf.data(), payload_size, &off, &nslots)) {
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
  if (!ConsumeU32(buf.data(), payload_size, &off, &num_trees)) {
    return Status::kCorrupt;
  }
  for (uint32_t i = 0; i < num_trees; ++i) {
    page_id_t tid = 0;
    uint32_t nentries = 0;
    if (!ConsumeU64(buf.data(), payload_size, &off, &tid) ||
        !ConsumeU32(buf.data(), payload_size, &off, &nentries)) {
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
}

Status PageManager::ReplayCommittedRecord(lsn_t lsn, const LogRecord& r) {
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
    case LogType::kLsmPut: {
      std::scoped_lock lk(trees_mu_);
      max_page_id_ = std::max(max_page_id_, r.pid);
      committed_max_page_id_ = std::max(committed_max_page_id_, r.pid);
      LsmTreeState* tree = GetOrCreateTreeLocked(r.pid);
      if (lsn >= tree->committed_destroy_lsn) {
        tree->destroyed = false;
        tree->committed_destroyed = false;
        lsn_t existing_lsn = 0;
        if (auto cit = tree->committed_mem.find(r.key);
            cit != tree->committed_mem.end()) {
          existing_lsn = cit->second.committed_lsn;
        } else {
          for (auto rit = tree->runs.rbegin(); rit != tree->runs.rend();
               ++rit) {
            if (const auto* e = rit->Find(r.key); e != nullptr) {
              existing_lsn = e->committed_lsn;
              break;
            }
          }
        }
        if (lsn >= existing_lsn) {
          tree->memtable[r.key] = LsmTreeState::MemEntry{
              .value = r.redo_data,
              .deleted = false,
              .generation = tree->next_generation++,
          };
          tree->committed_mem[r.key] = LsmTreeState::CommittedEntry{
              .value = r.redo_data,
              .deleted = false,
              .committed_lsn = lsn,
          };
        }
      }
      break;
    }
    case LogType::kLsmDelete: {
      std::scoped_lock lk(trees_mu_);
      LsmTreeState* tree = GetOrCreateTreeLocked(r.pid);
      if (lsn >= tree->committed_destroy_lsn) {
        lsn_t existing_lsn = 0;
        if (auto cit = tree->committed_mem.find(r.key);
            cit != tree->committed_mem.end()) {
          existing_lsn = cit->second.committed_lsn;
        } else {
          for (auto rit = tree->runs.rbegin(); rit != tree->runs.rend();
               ++rit) {
            if (const auto* e = rit->Find(r.key); e != nullptr) {
              existing_lsn = e->committed_lsn;
              break;
            }
          }
        }
        if (lsn >= existing_lsn) {
          tree->memtable[r.key] = LsmTreeState::MemEntry{
              .value = {},
              .deleted = true,
              .generation = tree->next_generation++,
          };
          tree->committed_mem[r.key] = LsmTreeState::CommittedEntry{
              .value = {},
              .deleted = true,
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
    for (const auto& [page_id, pending] : unpublished_tail_pages_) {
      (void)page_id;
      if (pending.first_page == first_page && pending.owner != requester) {
        return true;
      }
    }
    return false;
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

}  // namespace tinylamb
