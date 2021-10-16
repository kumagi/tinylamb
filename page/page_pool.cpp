#include "page_pool.hpp"

#include "catalog_page.hpp"
#include "meta_page.hpp"
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

Page* PagePool::GetPage(uint64_t page_id) {
  std::scoped_lock latch(pool_latch);
  auto entry = pool_.find(page_id);
  if (entry != pool_.end()) {
    Page* result = entry->second->page.get();
    assert(page_id == result->PageId());
    entry->second->pin_count++;
    Touch(entry->second);
    assert(entry->second->page->PageId() == page_id);
    return result;
  } else {
    if (pool_lru_.size() == capacity_) {
      EvictOnePage();
    }
    return AllocNewPage(page_id);
  }
}

bool PagePool::Unpin(uint64_t page_id) {
  std::scoped_lock latch(pool_latch);
  auto page_entry = pool_.find(page_id);
  if (page_entry != pool_.end()) {
    page_entry->second->pin_count--;
    return true;
  }
  return false;
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

bool PagePool::EvictOnePage() {
  auto target = pool_lru_.begin();
  while (target != pool_lru_.end() && !EvictPage(target)) {
    target++;
  }
  return target != pool_lru_.end();
}

Page* PagePool::AllocNewPage(size_t pid) {
  std::unique_ptr<Page> new_page(new Page(pid, PageType::kMetaPage));
  ReadFrom(new_page.get(), pid);
  assert(new_page->PageId() == pid);
  Entry new_entry;
  new_entry.pin_count++;
  new_entry.page = std::move(new_page);
  pool_lru_.push_back(std::move(new_entry));
  pool_.emplace(pid, std::prev(pool_lru_.end()));
  return pool_lru_.back().page.get();
}

void PagePool::Touch(LruType::iterator it) {
  Entry tmp(std::move(*it));
  uint64_t page_id = tmp.page->PageId();
  pool_lru_.erase(it);
  pool_lru_.push_back(std::move(tmp));
  pool_[page_id] = std::prev(pool_lru_.end());
}

PagePool::~PagePool() {
  std::scoped_lock latch(pool_latch);
  for (auto& it : pool_lru_) {
    if (0 < it.pin_count) {
      LOG(ERROR) << "caution: pinned page(" << it.page->PageId()
                 << ") is to be deleted\n";
    }
    WriteBack(it.page.get());
  }
  src_.flush();
  src_.close();
}

void ZeroFillUntil(std::fstream& of, size_t expected) {
  of.clear();
  of.seekp(0, std::ios_base::beg);
  const std::ofstream::pos_type start = of.tellp();
  of.seekp(0, std::ios_base::end);
  const std::ofstream::pos_type finish = of.tellp();
  const size_t needs = expected - (finish - start);
  LOG(TRACE) << "write " << needs << " bytes";
  for (size_t i = 0; i < needs; ++i) {
    of.write("\0", 1);
  }
}

void PagePool::WriteBack(const Page* target) {
  target->SetChecksum();
  src_.seekp(target->PageId() * kPageSize, std::ios_base::beg);
  if (src_.fail()) {
    ZeroFillUntil(src_, target->PageId() * kPageSize);
  }
  src_.write(reinterpret_cast<const char*>(target), kPageSize);
  if (src_.fail()) {
    throw std::runtime_error("cannot write back page: " +
                             std::to_string(src_.bad()));
  }
}

void PagePool::ReadFrom(Page* target, uint64_t pid) {
  src_.seekg(pid * kPageSize, std::ios_base::beg);
  if (src_.fail()) {
    return;
  }
  src_.read(reinterpret_cast<char*>(target), kPageSize);
  if (src_.fail()) {
    LOG(INFO) << "Page " << pid << " is empty, newly allocate one";
    target->PageInit(pid, PageType::kFreePage);
    src_.clear();
    return;
  }
  bool needs_recover = false;
  switch (target->Type()) {
    case PageType::kUnknown:
      LOG(ERROR) << "Reading page unknown type page";
      return;
    case PageType::kMetaPage: {
      auto* meta_page = reinterpret_cast<MetaPage*>(target);
      needs_recover = !meta_page->IsValid();
      break;
    }
    case PageType::kCatalogPage: {
      auto* catalog_page = reinterpret_cast<CatalogPage*>(target);
      needs_recover = !catalog_page->IsValid();
      break;
    }
    case PageType::kRowPage: {
      auto* row_page = reinterpret_cast<RowPage*>(target);
      needs_recover = !row_page->IsValid();
      break;
    }
    case PageType::kFreePage:
      break;
  }
  if (needs_recover) {
    LOG(ERROR) << "Page " << target->PageId() << " checksum matched";
  } else {
    LOG(INFO) << "Page " << target->PageId() << " checksum matched";
  }
  if (!needs_recover) return;

  LOG(ERROR) << "page checksum unmatched on " << pid;
}

}  // namespace tinylamb