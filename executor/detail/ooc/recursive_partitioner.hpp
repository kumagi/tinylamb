/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_DETAIL_OOC_RECURSIVE_PARTITIONER_HPP
#define TINYLAMB_EXECUTOR_DETAIL_OOC_RECURSIVE_PARTITIONER_HPP

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "common/query_grant.hpp"
#include "common/spill/spill_store.hpp"
#include "common/status_or.hpp"
#include "executor/data_chunk.hpp"
#include "type/row.hpp"

namespace tinylamb {

struct PartitionLeaf {
  bool in_memory{false};
  std::vector<Row> memory_rows;
  SpillSegmentId segment_id{kInvalidSpillSegmentId};
  uint64_t row_count{0};
  uint64_t estimated_bytes{0};
  uint32_t depth{0};
  bool skew_fallback{false};
};

// RAII owner for a collection of PartitionLeaf segments produced by
// RecursivePartitioner so aborted/early-exiting callers never leak spill
// blocks.
class PartitionLeafSet {
 public:
  explicit PartitionLeafSet(SpillStore* store = nullptr)
      : store_(store),
        store_alive_(store != nullptr ? store->LifetimeToken()
                                      : std::weak_ptr<const bool>{}) {}
  ~PartitionLeafSet() { DropAll(); }

  PartitionLeafSet(const PartitionLeafSet&) = delete;
  PartitionLeafSet& operator=(const PartitionLeafSet&) = delete;
  PartitionLeafSet(PartitionLeafSet&& other) noexcept;
  PartitionLeafSet& operator=(PartitionLeafSet&& other) noexcept;

  void DropLeaf(size_t index) noexcept;
  void DropAll() noexcept;

  std::vector<PartitionLeaf> leaves;

 private:
  friend class RecursivePartitioner;
  SpillStore* store_{nullptr};
  std::weak_ptr<const bool> store_alive_;
};

// Grace-style recursive radix hash partitioner (`ooca-v2.md` §5.2, ADR 0005 /
// ADR 0006):
//
// - Pure in-memory fast path when input fits within `memory_budget_bytes`.
// - Spills directly across `F` radix buckets (`4 <= F <= 64`, dynamically
//   bounded so `F * write_buf <= B_op / 2`).
// - Uses MSB hash bits at depth `d`, recursing only on oversized buckets while
//   immediately dropping consumed parent segments so blocks are recycled.
// - Detects skewed buckets (< 20% shrink: `child_bytes > 0.80 * parent_bytes`)
//   or `depth >= max_depth` and marks them `skew_fallback = true`.
class RecursivePartitioner {
 public:
  struct Options {
    SpillStore* spill_store{nullptr};
    size_t memory_budget_bytes{16ULL << 20};
    QueryMemoryGrant* grant{nullptr};
    uint32_t max_depth{8};
    bool force_spill{false};
  };

  using RowHashFn = std::function<uint64_t(const Row&)>;

  RecursivePartitioner(Options options, RowHashFn hash_fn);
  ~RecursivePartitioner();

  RecursivePartitioner(const RecursivePartitioner&) = delete;
  RecursivePartitioner& operator=(const RecursivePartitioner&) = delete;
  RecursivePartitioner(RecursivePartitioner&&) = delete;
  RecursivePartitioner& operator=(RecursivePartitioner&&) = delete;

  // Chooses fanout bits in [2..6] (F = 4..64) and per-writer buffer size so
  // `F * writer_buf_bytes <= budget / 2`.
  [[nodiscard]] static uint32_t ComputeFanoutBits(
      size_t budget_bytes, size_t* writer_buf_bytes_out) noexcept;

  // Extracts bucket index using MSB radix bits at recursion depth `depth` or
  // explicit cumulative bit offset `bit_offset`.
  [[nodiscard]] static uint32_t RadixBucket(uint64_t hash, uint32_t depth,
                                            uint32_t fanout_bits) noexcept;
  [[nodiscard]] static uint32_t RadixBucketAtOffset(
      uint64_t hash, uint64_t bit_offset, uint32_t fanout_bits) noexcept;

  [[nodiscard]] Status AddRow(const Row& row);
  [[nodiscard]] Status AddRow(Row&& row);
  [[nodiscard]] Status AddChunk(const DataChunk& chunk);

  // Completes partitioning and recursively splits any bucket larger than
  // `memory_budget_bytes` (unless skew fallback applies). An error leaves
  // result_out unchanged and allows retrying Finalize or calling Reset.
  // Input is closed once finalization starts.
  [[nodiscard]] Status Finalize(PartitionLeafSet* result_out);

  // Reads all rows from a single leaf (either in-memory or spilled segment)
  // via a single bounded block reader.
  [[nodiscard]] static Status ReadLeafRows(
      SpillStore* store, const PartitionLeaf& leaf,
      const std::function<Status(Row&&)>& on_row,
      size_t reader_buf_bytes = SpillStore::kBlockSize);

  void Reset() noexcept;

  [[nodiscard]] bool Spilled() const noexcept { return spilled_; }
  [[nodiscard]] uint32_t MaxObservedDepth() const noexcept {
    return max_observed_depth_;
  }
  [[nodiscard]] uint32_t SkewFallbackCount() const noexcept {
    return skew_fallback_count_;
  }
  [[nodiscard]] uint64_t TotalRows() const noexcept { return total_rows_; }
  [[nodiscard]] uint64_t PeakMemoryBytes() const noexcept {
    return peak_mem_bytes_;
  }

 private:
  struct BucketWriter {
    std::unique_ptr<SpillSegmentWriter> writer;
    std::vector<Row> batch;
    size_t batch_bytes{0};
    uint64_t row_count{0};
    uint64_t estimated_bytes{0};
    std::string scratch;
  };

  struct PendingPartition {
    SpillSegmentId segment_id{kInvalidSpillSegmentId};
    uint64_t row_count{0};
    uint64_t estimated_bytes{0};
    uint64_t parent_bytes{0};
    uint32_t depth{0};
    uint32_t bit_offset{0};
  };

  [[nodiscard]] size_t EffectiveBudgetBytes() const noexcept;
  [[nodiscard]] static Status WriteRowToBucket(BucketWriter* bucket, Row&& row,
                                               size_t est_bytes);
  [[nodiscard]] static Status FlushBucket(BucketWriter* bucket);
  [[nodiscard]] Status TransitionToSpill();
  // hash_fn_ is a caller-supplied std::function and may throw; the Status
  // boundary requires translating any exception instead of letting it escape.
  [[nodiscard]] StatusOr<uint64_t> HashRow(const Row& row);
  [[nodiscard]] Status SplitOversizedPartition(
      const PendingPartition& part, uint32_t fanout_bits, size_t writer_buf,
      std::vector<PendingPartition>* children_out);
  void CleanupInternal() noexcept;

  Options options_;
  std::weak_ptr<const bool> store_alive_;
  RowHashFn hash_fn_;
  std::optional<QueryMemoryGrant::LocalBatchCache> grant_cache_;

  uint32_t root_fanout_bits_{4};

  std::vector<Row> mem_rows_;
  size_t mem_bytes_{0};
  size_t peak_mem_bytes_{0};
  uint64_t total_rows_{0};

  std::vector<BucketWriter> root_writers_;
  std::vector<PendingPartition> pending_partitions_;
  PartitionLeafSet completed_leaves_;
  uint32_t max_observed_depth_{0};
  uint32_t skew_fallback_count_{0};
  bool spilled_{false};
  bool finalization_started_{false};
  bool finalized_{false};
};

}  // namespace tinylamb

#endif  // TINYLAMB_EXECUTOR_DETAIL_OOC_RECURSIVE_PARTITIONER_HPP
