/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_PARALLEL_HASH_JOIN_HPP
#define TINYLAMB_EXECUTOR_PARALLEL_HASH_JOIN_HPP

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <iosfwd>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/status_or.hpp"
#include "executor/data_chunk.hpp"
#include "executor/executor_base.hpp"
#include "executor/join_kind.hpp"
#include "executor/pipeline_breaker.hpp"
#include "executor/query_memory.hpp"
#include "page/row_position.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"

namespace tinylamb {

// Synchronized concurrent hash table with striped partition mutexes,
// enabling multiple build workers to insert tuples concurrently into
// a single shared hash table.
class ConcurrentJoinHashTable {
 public:
  static constexpr size_t kShardCount = 64;
  static constexpr size_t kNil = ~size_t{0};
  static constexpr size_t kMaxIntKeyParts = 4;

  struct Entry {
    Row row;
    RowPosition position;
    uint64_t hash{0};
    std::string key_bytes;  // only filled in byte-key mode
    size_t next{kNil};
  };

  explicit ConcurrentJoinHashTable(size_t expected_entries = 1024);

  // Switch the table to fixed-width integer keys (uniform signed-INT64 or
  // DATE key columns only).  Must be called before the first insert; inserts
  // then carry raw key parts instead of allocating a memcomparable string
  // per row.
  void EnableIntKeys(size_t key_parts);
  [[nodiscard]] bool IntKeyMode() const { return int_key_parts_ != 0; }
  [[nodiscard]] size_t IntKeyParts() const { return int_key_parts_; }

  void Insert(uint64_t hash, std::string key_bytes, Row row,
              RowPosition position);
  void InsertInt(uint64_t hash, const int64_t* key_parts, Row row,
                 RowPosition position);

  // Probe the table.  Both run after the build barrier, so the shards are
  // immutable and no lock is taken; fn is invoked once per matching entry.
  template <typename Fn>
  void FindMatches(uint64_t hash, std::string_view key_bytes, Fn&& fn) const {
    const Shard& shard = shards_[hash % kShardCount];
    const size_t slot = (hash >> 8) & shard.mask;
    for (size_t curr = shard.head_slots[slot]; curr != kNil;) {
      const Entry& entry = shard.entries[curr];
      if (entry.hash == hash && entry.key_bytes == key_bytes) {
        fn(entry.row, entry.position);
      }
      curr = entry.next;
    }
  }

  template <typename Fn>
  void FindIntMatches(uint64_t hash, const int64_t* key_parts, Fn&& fn) const {
    const Shard& shard = shards_[hash % kShardCount];
    const size_t slot = (hash >> 8) & shard.mask;
    for (size_t curr = shard.head_slots[slot]; curr != kNil;) {
      const Entry& entry = shard.entries[curr];
      if (entry.hash == hash && IntKeyEquals(shard, curr, key_parts)) {
        fn(entry.row, entry.position);
      }
      curr = entry.next;
    }
  }

  [[nodiscard]] size_t Size() const;
  [[nodiscard]] size_t EstimatedBytes() const;

 private:
  struct Shard {
    mutable std::mutex mutex;
    std::vector<Entry> entries;
    // Raw key parts, parallel to entries; only used in int-key mode.
    std::vector<std::array<int64_t, kMaxIntKeyParts>> int_keys;
    std::vector<size_t> head_slots;  // head index into entries
    size_t mask{0};
  };

  [[nodiscard]] bool IntKeyEquals(const Shard& shard, size_t entry_index,
                                  const int64_t* key_parts) const;

  std::array<Shard, kShardCount> shards_;
  size_t int_key_parts_{0};  // 0 = byte-key mode
};

// Multi-threaded parallel hash join where worker threads share a synchronized
// concurrent hash table during build, synchronize at a barrier, and
// concurrently probe to emit joined rows.
class SharedBuildParallelHashJoin : public ExecutorBase,
                                    public PipelineBreaker {
 public:
  // Non-copyable by design: instances are owned by unique_ptr and
  // referenced by raw pointers throughout the executor graph.
  SharedBuildParallelHashJoin(const SharedBuildParallelHashJoin&) = delete;
  SharedBuildParallelHashJoin& operator=(const SharedBuildParallelHashJoin&) =
      delete;
  SharedBuildParallelHashJoin(SharedBuildParallelHashJoin&&) = delete;
  SharedBuildParallelHashJoin& operator=(SharedBuildParallelHashJoin&&) =
      delete;
  SharedBuildParallelHashJoin(
      Executor left, std::vector<slot_t> left_cols, Executor right,
      std::vector<slot_t> right_cols,
      size_t worker_count = std::thread::hardware_concurrency(),
      JoinKind kind = JoinKind::kInner, size_t right_width = 0);

  ~SharedBuildParallelHashJoin() override = default;

  size_t NextBatch(DataChunk* destination,
                   size_t max_rows = kDefaultVectorSize) override;
  void Dump(std::ostream& o, int indent) const override;
  void Explain(std::ostream& o, int indent) const override;

  // PipelineBreaker interface
  [[nodiscard]] bool IsMaterialized() const override { return materialized_; }
  void MaterializePipeline() override;
  [[nodiscard]] size_t MaterializedRowCount() const override;
  [[nodiscard]] size_t MaterializedBytes() const override;

  [[nodiscard]] size_t WorkerCount() const { return worker_count_; }
  [[nodiscard]] JoinKind Kind() const { return kind_; }

 private:
  Status EnsureMaterialized();
  Status BuildSharedHashTable();
  Status ParallelProbe();
  [[nodiscard]] static std::string MakeKey(const Row& row,
                                           const std::vector<slot_t>& cols);
  [[nodiscard]] static bool KeyHasNull(const Row& row,
                                       const std::vector<slot_t>& cols);
  [[nodiscard]] static bool KeyHasNaN(const Row& row,
                                      const std::vector<slot_t>& cols);
  [[nodiscard]] static uint64_t HashKey(std::string_view key);

  Executor left_;
  std::vector<slot_t> left_cols_;
  Executor right_;
  std::vector<slot_t> right_cols_;
  size_t worker_count_{1};
  JoinKind kind_{JoinKind::kInner};
  // Inner joins pick the smaller drained side as the shared hash build at
  // materialization time (same contract as the sequential HashJoin): the
  // planner's orientation is a guess, and materializing a multi-million-row
  // build side because of a bad estimate is the single most expensive
  // mistake a join can make. Non-inner kinds keep the right-build contract
  // their emit paths assume. Output stays left-then-right either way.
  bool build_left_{false};
  bool build_empty_{true};
  // The non-build side, drained alongside the build side in
  // BuildSharedHashTable and consumed by ParallelProbe.
  std::vector<std::pair<Row, RowPosition>> probe_tuples_;
  // Full right row width for left-outer NULL padding; inferred from the
  // first build row when the build side is non-empty.
  size_t right_width_{0};
  // Seen while building: a NULL key anywhere in the build set makes every
  // NOT IN comparison UNKNOWN (three-valued kNullAwareAnti emits nothing).
  // Atomic: written concurrently by build workers; the build-side join()
  // barrier makes the final value visible to probes.
  std::atomic<bool> build_has_null_key_{false};
  // 0 = byte-key mode; 1 = uniform signed INT64 keys; 2 = uniform DATE keys.
  // Decided once in BuildSharedHashTable before any insert; the probe phase
  // reads it after the build barrier.
  int int_key_type_{0};

  ConcurrentJoinHashTable shared_hash_table_;
  std::vector<std::pair<Row, RowPosition>> output_;
  size_t output_offset_{0};
  bool materialized_{false};
  // Latched by EnsureMaterialized when materialization throws, so a retried
  // Next() rethrows the same failure instead of reporting the half-built
  // output_ as a complete result.
  Status materialization_failure_{Status::kSuccess};
  QueryMemoryCharge charge_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_EXECUTOR_PARALLEL_HASH_JOIN_HPP
