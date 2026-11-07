/**
 * Copyright 2026 KUMAZAKI Hiroki
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

#ifndef TINYLAMB_MEMTABLE_HPP
#define TINYLAMB_MEMTABLE_HPP

#include <atomic>
#include <limits>
#include <mutex>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/status_or.hpp"
#include "table/memtable/row_entry.hpp"
#include "table/memtable/skiplist.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"

namespace tinylamb {

// Per-shard write lock: uncontended acquire is one RMW and the contention
// path spins with a pause instead of taking pthread's futex detour -- shard
// hold windows are single skiplist operations (nanoseconds), so a futex
// round-trip on contention costs more than the critical section itself.
// Fairness is weaker than std::mutex; drains are still serialized globally
// by flush_mutex_ (a plain std::mutex), which bounds any writer's wait.
class ShardLock {
 public:
  ShardLock() = default;
  ShardLock(const ShardLock&) = delete;
  ShardLock& operator=(const ShardLock&) = delete;
  void lock() {
    while (flag_.test_and_set(std::memory_order_acquire)) {
#if defined(__x86_64__)
      __builtin_ia32_pause();
#endif
    }
  }
  void unlock() { flag_.clear(std::memory_order_release); }

 private:
  // C++20: atomic_flag's default constructor guarantees the cleared
  // (unlocked) state, so no explicit initializer is needed.
  std::atomic_flag flag_;
};

class SortedRun;

class MemTable final {
 public:
  explicit MemTable(const Schema& sc);
  MemTable(const MemTable&) = delete;
  MemTable(MemTable&&) = delete;
  MemTable& operator=(const MemTable&) = delete;
  MemTable& operator=(MemTable&&) = delete;
  ~MemTable() = default;

  [[nodiscard]] StatusOr<Row> Get(std::string_view key) const;
  // Three-state point lookup: kSuccess (row_out filled), kDeleted (tombstone
  // here; stop searching older layers), kNotExists (absent; keep searching).
  [[nodiscard]] Status Lookup(std::string_view key, Row* row_out) const;
  [[nodiscard]] bool Contains(std::string_view key) const;
  Status Put(const Row& r, bool sync = false);
  Status Delete(std::string_view key, bool sync = false);
  // On kNoSpace the output vectors are cleared (they hold only a drain
  // prefix) and *generation_out is left untouched: never persist them.
  // The sealed data stays pending for the next Sync (head-of-drain order).
  Status Sync(std::vector<char>* blob_out, std::vector<uint32_t>* key_run_out,
              std::vector<uint32_t>* row_run_out, size_t* generation_out,
              std::vector<uint8_t>* deleted_out = nullptr);

  // Crash-recovery semantics: Sync() serializes pending entries into the
  // output vectors. Durability is the CALLER's responsibility -- write the
  // returned blob to stable storage and fsync(2) it before acknowledging.
  // After Sync returns, the memtable is empty; the drained data lives only
  // in the returned bytes until the caller persists them.
  //
  // deleted_out (optional) receives one byte per entry, parallel to the
  // key/row offset runs: 1 = tombstone, 0 = live row. Without it callers
  // must re-derive the flag from the row_off == key_off sentinel, which
  // works but silently depends on an internal encoding detail.
  //
  // SetDurabilityCallback installs an optional hook invoked at the end of
  // each successful Sync with the serialized blob. Use it to append to a
  // WAL or write a run file without polling Sync outputs manually.
  using DurabilityFn = void (*)(const char* data, size_t len, void* ctx);
  void SetDurabilityCallback(DurabilityFn fn, void* ctx) {
    // Publish (fn, ctx) as a pair under flush_mutex_. Two separate atomics
    // cannot do this: with two racing reconfigurations, a Sync that acquires
    // fn from publication #1 could pair it with ctx from publication #2
    // (each atomic's release/acquire only covers its own word). Sync already
    // holds flush_mutex_ across the drain and its callback invocation, so
    // guarding the store with the same mutex makes publish/consume
    // mutually exclusive and the pair always consistent.
    std::scoped_lock lk(flush_mutex_);
    durability_fn_ = fn;
    durability_ctx_ = ctx;
  }

  // MVCC read-version alignment (see common/version.hpp). When enabled, every
  // Put/Delete stamps the RowEntry with a process-wide monotonically
  // increasing version, and each Sync assigns the flush generation from the
  // same counter. Generations then become COMPARABLE ACROSS all memtables
  // (partitions/tables), so an executor can pin a read version V and read each
  // partition at V. Off by default to preserve per-instance generation_ values
  // that existing tests rely on.
  void SetGlobalVersionMode(bool on) {
    // Atomic: toggling while writers are calling Put/Delete on other threads
    // was a plain-bool data race (TSan).
    global_version_mode_.store(on, std::memory_order_relaxed);
  }
  [[nodiscard]] bool GlobalVersionMode() const {
    return global_version_mode_.load(std::memory_order_relaxed);
  }

  // Drain byte budget for Sync()'s serialized blob (offsets are uint32, so
  // the production ceiling is UINT32_MAX; Sync clamps any larger value to
  // that ceiling so the run can never wrap its offsets). Exposed for tests:
  // the kNoSpace recovery path (leftover sealed data must stay at the HEAD
  // of the next drain, newest-wins intact) is otherwise only reachable with
  // a ~4 GiB blob. Atomic so a test thread may adjust it without racing a
  // concurrent Sync's budget read.
  void SetDrainByteBudget(size_t budget) {
    drain_budget_.store(budget, std::memory_order_relaxed);
  }

 private:
  // (Sync() is the single drain entry point; the former private Drain()
  // wrapper was dead code whose "callers hold flush_mutex_" contract would
  // have self-deadlocked -- Sync() takes that mutex itself.)

 public:
  // Copy-on-query snapshot of pending entries in ascending memcomparable key
  // order, tombstones included. Merges every shard's sealed + active lists.
  // Note: shards are captured sequentially, so with concurrent writers the
  // result matches some point per shard rather than one global instant --
  // the standard trade-off of sharded memtables. Read-time LSM scans use it
  // so iteration never races writers.
  //
  // MVCC: `max_visible_version` bounds the collapse. Entries with a version
  // greater than the bound are skipped DURING the per-key newest-wins merge,
  // so an older entry (version <= bound) survives for its key instead of
  // vanishing behind a newer invisible write. version 0 means "unversioned /
  // always visible" (the non-global-mode default) and always passes. The
  // parameterless form keeps the pre-bound newest for every key: pinning a
  // read version V must use Snapshot(V), otherwise the collapse drops the
  // visible history before the view can filter it.
  [[nodiscard]] std::vector<std::pair<std::string, RowEntry>> Snapshot(
      uint64_t max_visible_version = UINT64_MAX) const;
  // Entries not yet flushed to a sorted run. Counts physical entries across
  // active + sealed (a key present in both lists counts twice); use
  // Snapshot().size() for the deduplicated logical key count.
  [[nodiscard]] size_t Size() const;
  [[nodiscard]] const Schema& SchemaRef() const { return schema_; }

 private:
  static constexpr int kNumShards = 16;

  // One shard is exactly the FASTER-style two-list scheme: writes land in
  // `active`; Sync swaps it into `sealed` in O(1) under the shard lock, then
  // serializes `sealed` with the lock released while writers continue into a
  // fresh `active`. Point reads consult active first (newest wins).
  struct Shard {
    mutable ShardLock lock;
    SkipList active;
    SkipList sealed;
    bool draining{false};  // sealed holds data being serialized
  };
  Shard& ShardFor(std::string_view key) {
    return shards_[static_cast<size_t>(ShardIndex(key))];
  }
  const Shard& ShardFor(std::string_view key) const {
    return shards_[static_cast<size_t>(ShardIndex(key))];
  }
  static int ShardIndex(std::string_view key) {
    // FNV-1a: cheap, well-spread on memcomparable key bytes.
    uint64_t h = 14695981039346656037ULL;
    for (const char c : key) {
      h ^= static_cast<uint8_t>(c);
      h *= 1099511628211ULL;
    }
    return static_cast<int>(h % static_cast<uint64_t>(kNumShards));
  }

  // Snapshot of one shard's pending entries (caller holds shard lock).
  // Entries with version > max_visible_version (and version != 0) are
  // skipped, letting an older visible version win the per-key collapse.
  static std::vector<std::pair<std::string, RowEntry>> SnapshotShard(
      const Shard& s, uint64_t max_visible_version);

  // Stored by VALUE (same contract as FullScanIterator / LsmView): callers
  // routinely pass a temporary or a Schema resolved from a SchemaRegistry
  // that is only valid for a scope; a reference member would dangle
  // silently on the first Put(). Schema is cheap to copy.
  const Schema schema_;
  // Writers contend only within their own shard; disjoint-key workloads hit
  // independent locks and scale near-linearly up to kNumShards.
  std::vector<Shard> shards_;
  std::mutex flush_mutex_;  // at most one drain across all shards at a time
  // Fixed-width key fast path (see Put): a key column of kInt64/kDouble
  // always encodes to 9 memcomparable bytes, resolved once from the schema
  // so the ingest loop never pays the encode call chain per row.
  bool fixed_key_{false};
  ValueType key_type_{ValueType::kNull};
  slot_t key_idx_{0};
  size_t generation_{0};
  // Sync()'s serialized-blob byte budget. Production ceiling: the run
  // offsets are uint32, so anything past UINT32_MAX cannot be addressed.
  std::atomic<size_t> drain_budget_{std::numeric_limits<uint32_t>::max()};
  std::atomic<bool> global_version_mode_{false};
  // Sync() runs on one thread while SetDurabilityCallback() may be called
  // from another; the raw pointer/function pair was a data race (TSan
  // flags it). Both members are guarded by flush_mutex_: Sync reads them
  // under the mutex it already holds and SetDurabilityCallback stores them
  // under it, so a drain always sees a consistent (fn, ctx) pair.
  DurabilityFn durability_fn_{nullptr};
  void* durability_ctx_{nullptr};
};

}  // namespace tinylamb

#endif  // TINYLAMB_MEMTABLE_HPP
