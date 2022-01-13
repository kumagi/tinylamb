#ifndef TINYLAMB_PAGE_POOL_HPP
#define TINYLAMB_PAGE_POOL_HPP

#include <atomic>
#include <cassert>
#include <fstream>
#include <list>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <utility>

#include "constants.hpp"
#include "page/page.hpp"

namespace tinylamb {

class CheckpointManager;
class PageRef;
class RecoveryManager;

class PagePool {
 private:
  struct Entry {
    explicit Entry(Page* p)
        : pin_count(1), page(p), page_latch(new std::mutex()) {}

    // If pinned, this page will never been evicted.
    uint32_t pin_count = 0;

    // A pointer to physical page in memory.
    std::unique_ptr<Page> page = nullptr;

    // An exclusive latch for this page.
    std::unique_ptr<std::mutex> page_latch;

    Entry(const Entry&) = delete;
    Entry& operator=(const Entry&) = delete;
    Entry(Entry&&) = default;
    Entry& operator=(Entry&&) = default;
  };
  typedef std::list<Entry> LruType;

 public:
  PagePool(std::string_view file_name, size_t capacity);
  ~PagePool();

  PageRef GetPage(page_id_t page_id, bool* cache_hit);

  page_id_t Size() const {
    std::scoped_lock latch(pool_latch);
    return pool_lru_.size();
  }

  // Flush all page buffer without write back.
  void LostAllPageForTest();

  void FlushPageForTest(page_id_t page_id);

 private:
  friend class PageRef;
  friend class CheckpointManager;
  friend class RecoveryManager;

  bool Unpin(size_t page_id);

  bool EvictPage(LruType::iterator target);

  // Scan first unpinned page and evict it.
  // Return false if all pages are pinned.
  bool EvictOnePage();

  PageRef AllocNewPage(size_t pid);

 private:
  void Touch(LruType::iterator it);

  void WriteBack(const Page* target);

  void ReadFrom(Page* target, page_id_t pid);

  std::string file_name_;

  std::fstream src_;

  // Count of allowed max pages entry in memory.
  size_t capacity_;

  // A list to detect least recently used page.
  LruType pool_lru_;

  // A map to find PageID -> page*.
  std::unordered_map<page_id_t, LruType::iterator> pool_;

  mutable std::mutex pool_latch;
};

}  // namespace tinylamb

#endif  // TINYLAMB_PAGE_POOL_HPP
