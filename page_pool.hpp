#ifndef PEDASOS_PAGEPOOL_HPP
#define PEDASOS_PAGEPOOL_HPP

#include <cassert>
#include <fstream>
#include <list>
#include <unordered_map>
#include <utility>

#include "constants.hpp"
#include "page.hpp"

namespace pedasus {

class PagePool {
 private:
  struct Entry {
    // If pinned, this page will never been evicted.
    bool pinned = true;

    // ID for the page, this is also be an offset of the file.
    uint64_t page_id = 0;

    // An pointer to physical page in memory.
    std::unique_ptr<Page> page = nullptr;
  };
  typedef std::list<Entry> LruType;

 public:
  Page* GetPage(int64_t page_id);

  Entry* Unpin(size_t page_id);

  uint64_t Size() const {
    return pool_lru_.size();
  }

  PagePool(std::string_view file_name, size_t capacity);

  ~PagePool();

 private:

  bool EvictPage(LruType::iterator target);

  // Scan first unpinned page and evict it.
  // Return false if all pages are pinned.
  bool EvictOnePage();

  Page* AllocNewPage(size_t pid);

  void Touch(LruType::iterator it);

  bool IsCapacityFull() const {
    return pool_lru_.size() == capacity_;
  }


private:
  std::string file_name_;

  std::fstream src_;

  // Count of allowed max pages entry in memory.
  size_t capacity_;

  // A list to detect least recentry used page.
  LruType pool_lru_;

  // A map to find PageID -> page*.
  std::unordered_map<int64_t, LruType::iterator> pool_;
};

}  // namespace pedasus
#endif // PEDASOS_PAGEPOOL_HPP
