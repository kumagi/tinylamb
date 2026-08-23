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

#ifndef TINYLAMB_PAGE_POOL_HPP
#define TINYLAMB_PAGE_POOL_HPP

#include <atomic>
#include <cassert>
#include <functional>
#include <list>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "common/constants.hpp"
#include "page/page.hpp"

namespace tinylamb {

class CheckpointManager;
class PageRef;
class RecoveryManager;

class PagePool {
 private:
  struct Entry {
    explicit Entry(Page* p)
        : pin_count(1), page(p), page_latch(new std::shared_mutex()) {}

    // If pinned, this page will never been evicted.
    std::atomic<uint32_t> pin_count{0};

    // A pointer to physical page in memory.
    std::unique_ptr<Page> page = nullptr;

    // A physical page latch. Readers share it while writers remain exclusive.
    std::unique_ptr<std::shared_mutex> page_latch;

    Entry(const Entry&) = delete;
    Entry& operator=(const Entry&) = delete;
    Entry(Entry&& other) noexcept
        : pin_count(other.pin_count.load()),
          page(std::move(other.page)),
          page_latch(std::move(other.page_latch)) {}
    Entry& operator=(Entry&& other) noexcept {
      if (this != &other) {
        pin_count.store(other.pin_count.load());
        page = std::move(other.page);
        page_latch = std::move(other.page_latch);
      }
      return *this;
    }
  };
  typedef std::list<Entry> LruType;

 public:
  PagePool(std::string_view file_name, size_t capacity);
  ~PagePool();

  PageRef GetPage(page_id_t page_id, bool* cache_hit = nullptr,
                  bool shared = false);

  // Like GetPage, but a corrupt on-disk image is returned verbatim instead of
  // rejected. RecoveryManager needs the raw bytes to run Single Page
  // Recovery; every other caller should use GetPage.
  PageRef GetPageForRecovery(page_id_t page_id, bool* cache_hit = nullptr);

  page_id_t Size() const {
    std::shared_lock latch(pool_latch);
    return pool_lru_.size();
  }

  friend std::ostream& operator<<(std::ostream& o, const PagePool& pp) {
    std::shared_lock latch(pp.pool_latch);
    o << "PagePool(file=" << pp.file_name_ << ", capacity=" << pp.capacity_
      << ", pages=" << pp.pool_lru_.size() << ")";
    return o;
  }

  // Flush all page buffer without write back.
  void DropAllPages();

  void FlushPageForTest(page_id_t page_id);

  // Installs a WAL durability hook: before a dirty page image is pwritten,
  // the pool invokes gate(page->PageLSN()) so the caller can guarantee every
  // log record up to that LSN is already durable (WAL write-ahead rule).
  // The gate runs while the pool latch is NOT held; it may block on fsync.
  // Must be wired before the pool is used concurrently. An empty gate keeps
  // the previous behavior of writing back without any durability check.
  void SetDurabilityGate(std::function<void(lsn_t)> gate);

 private:
  friend class PageRef;
  friend class CheckpointManager;
  friend class RecoveryManager;

  void Unpin(page_id_t page_id);

  // Detach the LRU unpinned page from the pool maps. Caller writes it back
  // after releasing pool_latch so file I/O does not serialize GetPage hits.
  bool DetachVictim(std::unique_ptr<Page>* victim);

  // Refresh the specified entry in LRU.
  void Touch(LruType::iterator it);

  // Write `target` page into the file. Caller must hold file_latch_ but NOT
  // pool_latch, so the durability gate may block without stalling the pool.
  void WriteBack(const Page* target);

  // Read page at `pid` from the file to `target`. Caller must hold
  // file_latch_. With validate, a non-zero but corrupt checksum throws;
  // without it, broken images are handed back for Single Page Recovery.
  void ReadFrom(Page* target, page_id_t pid, bool validate) const;

  PageRef GetPageImpl(page_id_t page_id, bool* cache_hit, bool shared,
                      bool validate);

  std::string file_name_;

  int fd_{-1};

  // Rows of allowed max pages entry in memory.
  size_t capacity_;

  // A list to detect least recently used page.
  LruType pool_lru_;

  // A map to find PageID -> page*.
  std::unordered_map<page_id_t, LruType::iterator> pool_;

  // Page ids detached from the pool whose dirty images have not reached the
  // file yet. Guarded by pool_latch; misses on these ids must wait so a stale
  // on-disk image is never read and installed.
  std::unordered_set<page_id_t> flushing_;

  // Entries stranded by DropAllPages while still pinned by live PageRefs.
  // They are kept alive here (without write back) until the pool dies so a
  // late Unpin never touches freed entries.
  LruType retired_;

  // WAL durability hook; see SetDurabilityGate. Read without extra locking,
  // so it must be installed before the pool is used concurrently.
  std::function<void(lsn_t)> durability_gate_;

  mutable std::shared_mutex pool_latch;

  // Serializes pread/pwrite; held independently of pool_latch so concurrent
  // table loads can keep resolving cached pages while one miss I/Os.
  mutable std::mutex file_latch_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_PAGE_POOL_HPP
