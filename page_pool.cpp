#include "page_pool.hpp"

namespace pedasus {

PagePool::PagePool(std::string_view file_name, size_t capacity)
    : file_name_(file_name), src_(
    file_name_.c_str(),
    std::ios_base::in | std::ios_base::out | std::ios_base::ate |
    std::ios_base::binary),
      capacity_(capacity) {
  if (src_.fail()) {
    src_.open(file_name_.c_str(),
              std::ios_base::in | std::ios_base::out | std::ios_base::ate |
              std::ios_base::binary | std::ios_base::trunc);
    if (src_.fail()) {
      throw std::runtime_error("failed to open file: " + file_name_);
    }
  }
}

Page* PagePool::GetPage(int64_t page_id) {
  auto entry = pool_.find(page_id);
  if (entry != pool_.end()) {
    Touch(entry->second);
    entry->second->pinned = true;
    return entry->second->page.get();
  } else {
    if (IsCapacityFull()) {
      EvictOnePage();
    }
    return AllocNewPage(page_id);
  }
}

PagePool::Entry* PagePool::Unpin(size_t page_id) {
  auto page_entry = pool_.find(page_id);
  if (page_entry != pool_.end()) {
    page_entry->second->pinned = false;
    return &*page_entry->second;
  } else {
    // A page not loaded memory cannot be unpinned.
    assert(!"Don't unpin not loaded page.");
  }
}

bool PagePool::EvictPage(LruType::iterator target) {
  assert(pool_.find(target->page_id) != pool_.end());
  uint64_t target_page_id = target->page_id;
  if (target->pinned) {
    return false;
  }
  target->page->WriteBack(src_);
  pool_lru_.erase(target);
  pool_.erase(target_page_id);
  return true;
}

bool PagePool::EvictOnePage() {
  auto target = pool_lru_.begin();
  while (target != pool_lru_.end() &&
         !EvictPage(target)) {
    target++;
  }
  return target != pool_lru_.end();
}

Page* PagePool::AllocNewPage(size_t pid) {
  std::unique_ptr<Page> new_page(new Page(src_, pid));
  Entry new_entry;
  new_entry.pinned = true;
  new_entry.page_id = pid;
  new_entry.page = std::move(new_page);
  pool_lru_.push_back(std::move(new_entry));
  pool_.emplace(pid, std::prev(pool_lru_.end()));
  return pool_lru_.back().page.get();
}

void PagePool::Touch(LruType::iterator it) {
  Entry tmp(std::move(*it));
  pool_lru_.erase(it);
  pool_lru_.push_back(std::move(tmp));
}


PagePool::~PagePool() {
  for (auto& it : pool_lru_) {
    if (it.pinned) {
      std::cout << "caution: pinned page is to be deleted\n";
    }
    it.page->WriteBack(src_);
  }
  src_.flush();
  src_.close();
}

}  // namespace pedasus