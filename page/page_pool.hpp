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

#include <array>
#include <atomic>
#include <cassert>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <limits>
#include <list>
#include <memory>
#include <mutex>
#include <set>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "common/constants.hpp"
#include "common/status_or.hpp"
#include "page/page.hpp"

namespace tinylamb {

class CheckpointManager;
class PageRef;
class RecoveryManager;

// NOLINTNEXTLINE(clang-analyzer-optin.performance.Padding)
class PagePool {
 public:
  // Non-copyable: the pool is referenced by raw pointers pool-wide.
  PagePool(const PagePool&) = delete;
  PagePool& operator=(const PagePool&) = delete;
  PagePool(PagePool&&) = delete;
  PagePool& operator=(PagePool&&) = delete;

 private:
  struct Entry {
    // Initializer order follows declaration order (pin_count, page,
    // page_latch, page_id) -- reordering the list trips -Wreorder.
    Entry(Page* p, uint64_t id)
        : pin_count(1),
          page(p),
          page_latch(new std::shared_mutex()),
          page_id(id) {}
    ~Entry() = default;

    // If pinned, this page will never been evicted.
    std::atomic<uint32_t> pin_count{0};

    // A pointer to physical page in memory.
    std::unique_ptr<Page> page = nullptr;

    // A physical page latch. Readers share it while writers remain exclusive.
    std::unique_ptr<std::shared_mutex> page_latch;

    // The id this entry was installed under, immutable for the entry's
    // lifetime. Pool-latch scans must read THIS copy, never
    // page->PageID(): a fast-path holder can be re-initializing the Page
    // object (SPR/PageInit memset) under its page latch while the scan
    // holds only pool_latch, and the two locks never order against each
    // other. The Page's own field stays the recovery-time authority
    // (GetPageForRecovery re-stamps recycled ids).
    const uint64_t page_id;

    // Highest page_lsn whose image a completed (or in-flight, io-latch
    // serialized) pwrite has put on disk for this entry.  A staged
    // write-back whose snapshot is OLDER skips its pwrite instead of
    // overwriting a newer completed image with an older one: the page latch
    // no longer spans the whole write once the middle phase runs latch-free,
    // so the disk-serial order is enforced by this counter under the IO
    // latch instead.
    std::atomic<lsn_t> written_page_lsn{0};

    Entry(const Entry&) = delete;
    Entry& operator=(const Entry&) = delete;
    // Never value-move a live Entry: the stripe maps publish Entry* and a
    // move would leave them dangling while copying pin_count without
    // synchronization. Touch/DetachVictim only splice list nodes, which keeps
    // addresses stable.
    Entry(Entry&& other) = delete;
    Entry& operator=(Entry&& other) = delete;
  };
  typedef std::list<Entry> LruType;

  // Number of independent lock stripes. Both the fast-path map and the file
  // I/O latches are addressed by page_id % kPoolShards so operations on
  // distinct pages rarely share a lock.
  static constexpr size_t kPoolShards = 64;

  static constexpr size_t ShardIndex(page_id_t page_id) {
    return static_cast<size_t>(page_id % kPoolShards);
  }

 public:
  // Opens (creating when needed) the backing file. A failed open is a
  // Status, not an exception.
  static StatusOr<std::unique_ptr<PagePool>> Create(std::string_view file_name,
                                                    size_t capacity);
  ~PagePool();

  StatusOr<PageRef> GetPage(page_id_t page_id, bool* cache_hit = nullptr,
                            bool shared = false);

  // Like GetPage, but a corrupt on-disk image is returned verbatim instead of
  // rejected. RecoveryManager needs the raw bytes to run Single Page
  // Recovery; a valid image with a different page id is converted to an
  // invalid recovery sentinel. Every other caller should use GetPage.
  StatusOr<PageRef> GetPageForRecovery(page_id_t page_id,
                                       bool* cache_hit = nullptr);

  page_id_t Size() const {
    std::shared_lock latch(pool_latch);
    return pool_lru_.size();
  }

  [[nodiscard]] uint64_t CacheHits() const noexcept {
    return cache_hits_.load(std::memory_order_relaxed);
  }

  [[nodiscard]] uint64_t CacheMisses() const noexcept {
    return cache_misses_.load(std::memory_order_relaxed);
  }

  [[nodiscard]] size_t PinnedPageCount() const noexcept;

  friend std::ostream& operator<<(std::ostream& o, const PagePool& pp) {
    std::shared_lock latch(pp.pool_latch);
    o << "PagePool(file=" << pp.file_name_ << ", capacity=" << pp.capacity_
      << ", pages=" << pp.pool_lru_.size() << ")";
    return o;
  }

  // Flush all page buffer without write back.
  void DropAllPages();

  Status FlushPage(page_id_t page_id);

  // Checkpoint-flush write-back of one resident page.  Unlike FlushPage,
  // which holds the page's exclusive latch across the durability gate and
  // the pwrite, this variant takes the latch only to snapshot the content
  // (FPW emit + image encode), runs the gate and the pwrite WITHOUT the page
  // latch, and re-latches at the end to seal the page only when the staged
  // image is still its newest content.  A page re-dirtied in between keeps
  // its dirty accounting; its writer's own write-back covers the newer
  // image, and a crash replays forward from the older on-disk image (the
  // durability gate ordered WAL >= the staged page_lsn before the pwrite).
  // The latch-free middle phase is what keeps a checkpoint's flush pass from
  // stalling every transaction that needs one of the pages being flushed.
  Status WriteBackStaged(page_id_t pid);

  // Test-only synchronous write-back of one resident page.  Errors are
  // reported to the caller: silently dropping a durability-gate or I/O
  // failure here would let tests pass while persistence is broken.  Callers
  // that cannot fail (fixture Flush() helpers) may discard the result.
  Status FlushPageForTest(page_id_t page_id);

  // Installs a WAL durability hook: before a dirty page image is pwritten,
  // the pool invokes gate(page->PageLSN()) so the caller can guarantee every
  // log record up to that LSN is already durable (WAL write-ahead rule).
  // The gate runs while the pool latch is NOT held; it may block on fsync.
  // Must be wired before the pool is used concurrently. An empty gate keeps
  // the previous behavior of writing back without any durability check.
  void SetDurabilityGate(std::function<Status(lsn_t)> gate);

  // Installs the full-page-image (FPW) sink. Before a dirty page image whose
  // last WAL snapshot is no longer retained reaches the data file, the pool
  // emits sink(pid, type, body) -- a kFullPageImage WAL record -- and waits
  // for its durability through the gate. This is what keeps torn data-file
  // pages rebuildable after CheckpointManager recycles the WAL prefix
  // (docs/wal_format.md). Must be wired before the pool is used
  // concurrently. An empty sink disables FPW emission.
  void SetWalSnapshotSink(
      std::function<StatusOr<lsn_t>(page_id_t, PageType, std::string_view)>
          sink);

  // Raises the LSN below which WAL-resident full-page images have been (or
  // may have been) recycled; a page snapshot older than the floor no longer
  // protects its page, so the next flush re-emits it. Monotonic; called by
  // the checkpoint manager after recycling and once at startup with the
  // master-record checkpoint.
  void SetSnapshotRetentionFloor(lsn_t floor);

  // fdatasync the data file. Every pwrite that completed before this call is
  // durable when it returns, so the full-page images protecting those writes
  // are no longer needed: the unsynced-snapshot floor advances past them even
  // when OTHER pages are still being written back (a checkpoint under load
  // must never stall WAL recycling just because some pwrite is in flight).
  // Writes still in flight when the sync starts keep their pages pending; the
  // next successful sync retires them. `watermark` is the WAL LSN captured
  // before the sync; page snapshots below it are considered stale for
  // emission purposes so a page re-flushed after the checkpoint always logs
  // a fresh full image. Returns false when the sync failed (data file IO
  // error); the checkpoint must then not recycle anything.
  bool SyncDataFile(lsn_t watermark);

  void SetSyncDataFileFaultForTest(bool fail) {
    sync_data_file_fault_for_test_.store(fail, std::memory_order_release);
  }

  void SetPWriteFaultForTest(bool fail) {
    pwrite_fault_for_test_.store(fail, std::memory_order_release);
  }

  // LSN at or below which some page's current image may not be durable yet,
  // together with the full-page image that would rebuild it. kMaxLsn when
  // every completed pwrite is known durable (i.e. covered by SyncDataFile).
  // The checkpoint folds this into its WAL-recycling floor.
  // Diagnostic helper (PMDIAG): current on-disk size of the data file.
  [[nodiscard]] uint64_t DebugDataFileSizeForDiag() const;

  // True when the page id currently has a resident entry. Unlike GetPage this
  // never pins, materializes, or touches the disk; recovery's undo walks use
  // it to tell "this page exists in this database" apart from the fresh
  // past-EOF image a subsequent GetPageForRecovery would materialize for a
  // never-issued id.
  [[nodiscard]] bool ContainsResident(page_id_t page_id) const;

  [[nodiscard]] lsn_t UnsyncedSnapshotFloor() const {
    return unsynced_snapshot_floor_.load(std::memory_order_acquire);
  }

 private:
  friend class PageRef;
  friend class CheckpointManager;
  friend class RecoveryManager;

  // Cache-line padded so neighboring stripes never share a coherence line.
  struct alignas(64) PoolShard {
    std::mutex mu;
    // Fast lookup: page id -> resident entry. Guarded by mu. Entries live in
    // pool_lru_ and are spliced (never value-moved) by Touch, so the stored
    // pointers stay stable until DetachVictim removes them under both this
    // mutex and pool_latch.
    std::unordered_map<page_id_t, Entry*> map;
  };

  struct alignas(64) IoLatch {
    std::mutex mu;
  };

  // Decrement pin_count once; log instead of wrapping on underflow.
  static void ReleasePin(Entry& entry, page_id_t page_id);

  // Latch an already-pinned page and wrap it in a PageRef. PageRef's
  // constructor takes a std::shared_mutex lock and is not noexcept: a
  // re-entrant fetch of a page this thread already holds exclusively fails
  // with std::system_error (EDEADLK on libstdc++). Call sites take the pin
  // before latching, so the latch failure has to undo that pin and surface as
  // a Status -- letting the exception escape would leak the pin and break the
  // StatusOr contract.
  static StatusOr<PageRef> LatchPinned(PagePool* pool, Page* page,
                                       std::shared_mutex* page_latch,
                                       bool shared,
                                       std::atomic<uint32_t>* pin_count);

  // Detach the LRU unpinned page from the pool maps. Caller writes it back
  // after releasing pool_latch so file I/O does not serialize GetPage hits.
  bool DetachVictim(std::unique_ptr<Page>* victim);

  // Refresh the specified entry in LRU.
  void Touch(LruType::iterator it);

  // Write `target` page into the file. Caller must hold the IO latch of the
  // target's page id (io_latches_[ShardIndex(...)].mu) but NOT pool_latch,
  // so the durability gate may block without stalling the pool.  |entry|
  // (the resident entry backing |target|, when one exists) records the
  // completed image's page_lsn so a concurrently staged write-back cannot
  // overwrite the newer on-disk image with an older snapshot.
  Status WriteBack(Page* target, Entry* entry = nullptr);

  struct PendingFullPageImage {
    lsn_t snapshot_end = 0;
    uint64_t completed_pwrite_serial = 0;
    bool pwrite_in_flight = false;
    bool emitting_snapshot = false;
  };

  void RefreshUnsyncedSnapshotFloorLocked();

  // Incremental floor bookkeeping over the two PendingFullPageImage fields
  // that feed RefreshUnsyncedSnapshotFloorLocked (snapshot_end and
  // emitting_snapshot).  Call pair-removed-then-pair-inserted around any
  // transition of those fields so the floor never has to rescan
  // unsynced_pages_ (O(|unsynced_pages_|) per emit event was measurable on
  // write-heavy phases).  Caller must hold sync_state_mu_.
  void FloorTrackInsertLocked(lsn_t snapshot_end, bool emitting_snapshot);
  void FloorTrackRemoveLocked(lsn_t snapshot_end, bool emitting_snapshot);
  void VerifyFloorTrackingLocked() const;

  // Emits a kFullPageImage WAL record for `target` when no retained snapshot
  // covers the page, returning its END LSN (0 when none was emitted). The
  // record is durable before the pwrite runs (via the durability gate).
  StatusOr<lsn_t> EmitSnapshotIfNeeded(const Page* target);

  // Read page at `pid` from the file to `target`. Caller must hold the IO
  // latch of pid and must have observed flushing_ without pid under
  // pool_latch inside that same IO latch scope. With validate, a non-zero
  // but corrupt checksum is reported as kCorrupt; without it, broken images
  // are handed back for Single Page Recovery.
  Status ReadFrom(Page* target, page_id_t pid, bool validate) const;

  StatusOr<PageRef> GetPageImpl(page_id_t page_id, bool* cache_hit, bool shared,
                                bool validate);

  PagePool(std::string_view file_name, size_t capacity, int fd);

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
  std::condition_variable_any writeback_cv_;
  std::mutex writeback_wait_mu_;
  std::atomic<uint32_t> active_writebacks_{0};
  std::atomic<uint32_t> active_evictions_{0};
  std::atomic<uint32_t> active_loads_{0};
  std::atomic<bool> dropping_pages_{false};

  // Entries stranded by DropAllPages (which retires every buffered page
  // without write back). They are kept alive here until the pool dies so a
  // live PageRef or an in-flight stripe-map hit never touches freed memory.
  LruType retired_;

  // WAL durability hook; see SetDurabilityGate. Read without extra locking,
  // so it must be installed before the pool is used concurrently.
  std::function<Status(lsn_t)> durability_gate_;

  // Full-page-image (FPW) sink; see SetWalSnapshotSink. Same wiring rule.
  std::function<StatusOr<lsn_t>(page_id_t, PageType, std::string_view)>
      wal_snapshot_sink_;

  // Last WAL-resident full-page image per flushed page id -> record END
  // LSN. Guarded by snapshot_mu_ (flush paths only; the checkpoint reads
  // only the atomic floors below). Entries at or below
  // snapshot_retention_floor_ are stale and re-emitted on the next flush.
  std::mutex snapshot_mu_;
  std::unordered_map<page_id_t, lsn_t> wal_snapshot_lsns_;
  std::atomic<lsn_t> snapshot_retention_floor_{0};
  // WAL LSN at the last data-file fsync: snapshots below it are stale (their
  // images are durable), so the next flush of such a page re-emits one.
  std::atomic<lsn_t> sync_watermark_{0};
  // Per-page FPW tracking, guarded by sync_state_mu_. SyncDataFile retires
  // only successful pwrites completed before its fdatasync began; emitting,
  // in-flight, and failed writes retain their full-page images.
  std::unordered_map<page_id_t, PendingFullPageImage> unsynced_pages_;
  // Incremental mirror of the floor inputs below: the non-zero snapshot_end
  // values of every non-emitting entry, plus how many entries are currently
  // emitting a snapshot (each of those floors the LSN at 0).
  std::multiset<lsn_t> unsynced_snapshot_ends_;
  size_t emitting_fpw_count_{0};
  uint64_t next_pwrite_serial_ = 1;
  // Min value in unsynced_pages_; kMaxLsn when every completed pwrite is
  // covered by SyncDataFile. Lowered at emit time, recomputed by
  // SyncDataFile. The checkpoint folds this into its WAL-recycling floor.
  std::atomic<lsn_t> unsynced_snapshot_floor_{
      std::numeric_limits<lsn_t>::max()};
  std::mutex sync_state_mu_;
  std::atomic<bool> sync_data_file_fault_for_test_{false};
  std::atomic<bool> pwrite_fault_for_test_{false};
  std::atomic<uint64_t> pwrite_count_{0};

  mutable std::shared_mutex pool_latch;

  // Per-stripe fast maps for the GetPage/Unpin hot path; see PoolShard.
  // Mutations are nested under pool_latch (lock order pool_latch -> shard),
  // while hits and unpins take only their single stripe mutex.
  std::array<PoolShard, kPoolShards> shards_{};

  // Per-page-id file I/O latches. Held across pread/pwrite so two threads
  // never interleave accesses to the SAME page image, while distinct page
  // ids keep streaming in parallel. A caller may hold a content latch while
  // requesting another page, so writeback takes the target content latch
  // before this IO latch. A miss then takes pool_latch (shared, briefly)
  // inside the IO latch to check flushing_; no path reverses that order.
  std::array<IoLatch, kPoolShards> io_latches_{};

  mutable std::atomic<uint64_t> cache_hits_{0};
  mutable std::atomic<uint64_t> cache_misses_{0};
};

}  // namespace tinylamb

#endif  // TINYLAMB_PAGE_POOL_HPP
