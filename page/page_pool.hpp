#ifndef TINYLAMB_PAGEPOOL_HPP
#define TINYLAMB_PAGEPOOL_HPP

#include <cassert>
#include <fstream>
#include <list>
#include <mutex>
#include <unordered_map>
#include <utility>
#include <memory>

#include "../constants.hpp"
#include "page.hpp"

namespace tinylamb {

class PagePool {
 private:
  struct Entry {
    // If pinned, this page will never been evicted.
    uint32_t pin_count = 0;

    // An pointer to physical page in memory.
    std::unique_ptr<Page> page = nullptr;
  };
  typedef std::list<Entry> LruType;

 public:
  PagePool(std::string_view file_name, size_t capacity);

  Page* GetPage(uint64_t page_id);

  bool Unpin(size_t page_id);

  uint64_t Size() const {
    std::scoped_lock latch(pool_latch);
    return pool_lru_.size();
  }

  ~PagePool();

 private:

  bool EvictPage(LruType::iterator target);

  // Scan first unpinned page and evict it.
  // Return false if all pages are pinned.
  bool EvictOnePage();

  Page* AllocNewPage(size_t pid);

private:
  void Touch(LruType::iterator it);

  void WriteBack(const Page* target);

  void ReadFrom(Page* target, uint64_t pid);

  std::string file_name_;

  std::fstream src_;

  // Count of allowed max pages entry in memory.
  size_t capacity_;

  // A list to detect least recently used page.
  LruType pool_lru_;

  // A map to find PageID -> page*.
  std::unordered_map<uint64_t, LruType::iterator> pool_;

  mutable std::mutex pool_latch;
};

}  // namespace tinylamb

#endif // TINYLAMB_PAGEPOOL_HPP
