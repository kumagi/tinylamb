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

#include "page_pool.hpp"

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <ios>
#include <iterator>
#include <limits>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

#include "common/constants.hpp"
#include "common/log_message.hpp"
#include "meta_page.hpp"
#include "page/page_ref.hpp"
#include "page_type.hpp"
#include "recovery/recovery_manager.hpp"

namespace tinylamb {

PagePool::PagePool(std::string_view file_name, size_t capacity)
    : file_name_(file_name),
      src_(file_name_.c_str(), std::ios_base::in | std::ios_base::out |
                                   std::ios_base::ate | std::ios_base::binary),
      capacity_(capacity) {
  if (src_.fail()) {
    src_.open(file_name_, std::ios_base::in | std::ios_base::out |
                              std::ios_base::ate | std::ios_base::binary |
                              std::ios_base::trunc);
    if (src_.fail()) {
      throw std::runtime_error("failed to open file: " + file_name_);
    }
  }
}

PageRef PagePool::GetPage(page_id_t page_id, bool* cache_hit) {
  std::unique_lock latch(pool_latch);
  auto entry = pool_.find(page_id);
  if (entry != pool_.end()) {
    entry->second->pin_count++;
    Touch(entry->second);
    if (cache_hit != nullptr) {
      *cache_hit = true;
    }
    latch.unlock();
    return {this, entry->second->page.get(), entry->second->page_latch.get()};
  }

  if (pool_lru_.size() == capacity_) {
    EvictOnePage();
  }
  if (cache_hit != nullptr) {
    *cache_hit = false;
  }
  return AllocNewPage(page_id, std::move(latch));
}

void PagePool::DropAllPages() {
  std::scoped_lock latch(pool_latch);
  pool_.clear();
  pool_lru_.clear();
}

void PagePool::FlushPageForTest(page_id_t page_id) {
  std::scoped_lock latch(pool_latch);
  const auto it = pool_.find(page_id);
  if (it == pool_.end()) {
    return;  // Already evicted.
  }
  WriteBack(it->second->page.get());
}

void PagePool::Unpin(page_id_t page_id) {
  std::scoped_lock latch(pool_latch);
  auto page_entry = pool_.find(page_id);
  assert(page_entry != pool_.end());
  page_entry->second->pin_count--;
}

// Precondition: pool_latch is locked.
bool PagePool::EvictPage(LruType::iterator target) {
  assert(!pool_latch.try_lock());
  assert(pool_.find(target->page->PageID()) != pool_.end());
  if (0 < target->pin_count) {
    return false;
  }
  WriteBack(target->page.get());
  uint64_t page_id = target->page->PageID();
  pool_lru_.erase(target);
  pool_.erase(page_id);
  return true;
}

// Precondition: pool_latch is locked.
bool PagePool::EvictOnePage() {
  // assert(!pool_latch.try_lock());
  auto target = pool_lru_.begin();
  while (target != pool_lru_.end() && !EvictPage(target)) {
    target++;
  }
  return target != pool_lru_.end();
}

// Precondition: pool_latch is locked.
PageRef PagePool::AllocNewPage(page_id_t pid,
                               std::unique_lock<std::mutex> lock) {
  std::unique_ptr<Page> new_page(new Page(pid, PageType::kUnknown));
  ReadFrom(new_page.get(), pid);
  pool_lru_.emplace_back(new_page.release());
  pool_.emplace(pid, std::prev(pool_lru_.end()));
  lock.unlock();
  return {this, pool_lru_.back().page.get(), pool_lru_.back().page_latch.get()};
}

// Precondition: pool_latch is locked.
void PagePool::Touch(LruType::iterator it) {
  assert(!pool_latch.try_lock());
  Entry tmp(std::move(*it));
  const page_id_t page_id = tmp.page->PageID();
  pool_lru_.erase(it);
  pool_lru_.push_back(std::move(tmp));
  pool_[page_id] = std::prev(pool_lru_.end());
}

PagePool::~PagePool() {
  const std::scoped_lock latch(pool_latch);
  for (auto& it : pool_lru_) {
    if (0 < it.pin_count) {
      LOG(ERROR) << "caution: pinned page(" << it.page->PageID()
                 << ") is to be deleted at pin count " << it.pin_count;
    }
    WriteBack(it.page.get());
  }
  src_.flush();
  src_.close();
}

void PagePool::WriteBack(const Page* target) {
  target->SetChecksum();
  // LOG(WARN) << "write back: " << *target;
  src_.seekp(target->PageID() * kPageSize, std::ios_base::beg);
  if (src_.fail()) {
    src_.clear();
  }
  src_.write(reinterpret_cast<const char*>(target), kPageSize);
  if (src_.fail()) {
    throw std::runtime_error("cannot write back page: " +
                             std::to_string(static_cast<int>(src_.bad())));
  }
}

// Precondition: target has allocated memory at kPageSize.
void PagePool::ReadFrom(Page* target, page_id_t pid) {
  src_.seekg(pid * kPageSize, std::ios_base::beg);
  if (src_.fail()) {
    return;
  }
  src_.read(reinterpret_cast<char*>(target), kPageSize);
  if (src_.fail()) {
    target->PageInit(pid, PageType::kFreePage);
    src_.clear();
  }

  // RecLSN = MAX means a clean page.
  target->recovery_lsn = std::numeric_limits<lsn_t>::max();
}

}  // namespace tinylamb