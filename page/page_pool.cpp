#include "page_pool.hpp"

#include <recovery/recovery.hpp>

#include "meta_page.hpp"
#include "page/page_ref.hpp"
#include "row_page.hpp"

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
  std::scoped_lock latch(pool_latch);
  auto entry = pool_.find(page_id);
  if (entry != pool_.end()) {
    entry->second->pin_count++;
    Touch(entry->second);
    if (cache_hit != nullptr) {
      *cache_hit = true;
    }

    std::scoped_lock page_lock(*entry->second->page_latch);
    return {this, entry->second->page.get()};
  } else {
    if (pool_lru_.size() == capacity_) {
      EvictOnePage();
    }
    if (cache_hit != nullptr) {
      *cache_hit = false;
    }
    return AllocNewPage(page_id);
  }
}

void PagePool::LostAllPageForTest() {
  std::scoped_lock latch(pool_latch);
  pool_.clear();
  pool_lru_.clear();
}

void PagePool::FlushPageForTest(page_id_t page_id) {
  std::scoped_lock latch(pool_latch);
  const auto it = pool_.find(page_id);
  assert(it != pool_.end());
  WriteBack(it->second->page.get());
}

bool PagePool::Unpin(page_id_t page_id) {
  std::scoped_lock latch(pool_latch);
  auto page_entry = pool_.find(page_id);
  if (page_entry != pool_.end()) {
    page_entry->second->pin_count--;
    return true;
  }
  return false;
}

void PagePool::PageLock(uint64_t page_id) {
  std::scoped_lock latch(pool_latch);
  auto entry = pool_.find(page_id);
  if (entry == pool_.end()) {
    AllocNewPage(page_id);
    entry = pool_.find(page_id);
  }
  entry->second->page_latch->lock();
}

void PagePool::PageUnlock(uint64_t page_id) {
  std::scoped_lock latch(pool_latch);
  auto entry = pool_.find(page_id);
  assert(entry != pool_.end());
  entry->second->page_latch->unlock();
}

bool PagePool::EvictPage(LruType::iterator target) {
  assert(pool_.find(target->page->PageId()) != pool_.end());
  if (0 < target->pin_count) {
    return false;
  }
  WriteBack(target->page.get());
  uint64_t page_id = target->page->PageId();
  pool_lru_.erase(target);
  pool_.erase(page_id);
  return true;
}

// Precondition: pool_latch is locked.
bool PagePool::EvictOnePage() {
  assert(!pool_latch.try_lock());
  auto target = pool_lru_.begin();
  while (target != pool_lru_.end() && !EvictPage(target)) {
    target++;
  }
  return target != pool_lru_.end();
}

// Precondition: pool_latch is locked.
PageRef PagePool::AllocNewPage(page_id_t pid) {
  assert(!pool_latch.try_lock());
  std::unique_ptr<Page> new_page(new Page(pid, PageType::kMetaPage));
  ReadFrom(new_page.get(), pid);
  Entry new_entry{1, std::move(new_page), std::make_unique<std::mutex>()};
  pool_lru_.push_back(std::move(new_entry));
  pool_.emplace(pid, std::prev(pool_lru_.end()));
  return {this, pool_lru_.back().page.get()};
}

void PagePool::Touch(LruType::iterator it) {
  Entry tmp(std::move(*it));
  page_id_t page_id = tmp.page->PageId();
  pool_lru_.erase(it);
  pool_lru_.push_back(std::move(tmp));
  pool_[page_id] = std::prev(pool_lru_.end());
}

PagePool::~PagePool() {
  std::scoped_lock latch(pool_latch);
  for (auto& it : pool_lru_) {
    if (0 < it.pin_count) {
      LOG(ERROR) << "caution: pinned page(" << it.page->PageId()
                 << ") is to be deleted at count " << it.pin_count;
    }
    WriteBack(it.page.get());
  }
  src_.flush();
  src_.close();
}

void ZeroFillUntil(std::fstream& of, size_t expected) {
  if (expected == 0) {
    return;
  }
  of.clear();
  of.seekp(0, std::ios_base::beg);
  const std::ofstream::pos_type start = of.tellp();
  of.seekp(0, std::ios_base::end);
  const std::ofstream::pos_type finish = of.tellp();
  const size_t needs = expected - (finish - start);
  assert(needs < kPageSize * 10);
  for (size_t i = 0; i < needs; ++i) {
    of.write("\0", 1);
  }
}

void PagePool::WriteBack(const Page* target) {
  target->SetChecksum();
  src_.seekp(target->PageId() * kPageSize, std::ios_base::beg);
  if (src_.fail()) {
    ZeroFillUntil(src_, target->PageId() * kPageSize);
    src_.clear();
  }
  src_.write(reinterpret_cast<const char*>(target), kPageSize);
  if (src_.fail()) {
    throw std::runtime_error("cannot write back page: " +
                             std::to_string(src_.bad()));
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