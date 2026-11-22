/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_DETAIL_OOC_SPILLABLE_STORE_HPP
#define TINYLAMB_EXECUTOR_DETAIL_OOC_SPILLABLE_STORE_HPP

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "common/query_grant.hpp"
#include "common/spill/spill_store.hpp"
#include "common/status_or.hpp"
#include "executor/data_chunk.hpp"
#include "page/row_position.hpp"
#include "type/row.hpp"

namespace tinylamb {

// Budget-bounded batch container that stays purely in RAM (0 SpillStore FDs)
// until resident memory exceeds its budget or forced spill triggers, then
// streams remaining batches into a single `SpillStore` segment. Reads use a
// single sequential cursor (reader_ / mem_cursor_idx_) that ResetCursor()
// rewinds; concurrent multiple cursors are not supported.
class SpillableStore {
 public:
  struct Options {
    SpillStore* spill_store{nullptr};
    size_t memory_budget_bytes{16ULL << 20};
    QueryMemoryGrant* grant{nullptr};
    size_t io_buffer_bytes{SpillStore::kBlockSize};
    bool force_spill{false};
  };

  explicit SpillableStore(Options options);
  ~SpillableStore();

  SpillableStore(const SpillableStore&) = delete;
  SpillableStore& operator=(const SpillableStore&) = delete;
  SpillableStore(SpillableStore&&) = delete;
  SpillableStore& operator=(SpillableStore&&) = delete;

  [[nodiscard]] Status AppendChunk(const DataChunk& chunk);
  [[nodiscard]] Status AppendChunk(DataChunk&& chunk);
  [[nodiscard]] Status AppendRow(const Row& row,
                                 RowPosition pos = RowPosition());

  // Finalizes appends (flushes staging buffers and seals any spill segment).
  [[nodiscard]] Status Seal();

  // Rewinds sequential cursor back to row 0 (sealing first if needed).
  // Supports repeated sequential scans over both in-memory and spilled runs.
  [[nodiscard]] Status ResetCursor();

  // Streams the next DataChunk. Returns `false` at EOF.
  [[nodiscard]] StatusOr<bool> NextChunk(DataChunk* chunk_out);

  // Convenience row-at-a-time cursor layered over buffered chunks.
  [[nodiscard]] StatusOr<bool> NextRow(Row* row_out,
                                       RowPosition* pos_out = nullptr);

  // Drops any spilled segment, frees memory, and resets state for reuse.
  void Reset() noexcept;

  [[nodiscard]] bool Spilled() const noexcept { return spilled_; }
  [[nodiscard]] bool Empty() const noexcept { return total_rows_ == 0; }
  [[nodiscard]] uint64_t RowCount() const noexcept { return total_rows_; }
  [[nodiscard]] uint64_t InMemoryBytes() const noexcept { return mem_bytes_; }
  [[nodiscard]] uint64_t PeakMemoryBytes() const noexcept {
    return peak_mem_bytes_;
  }
  [[nodiscard]] uint64_t SpilledBytes() const noexcept {
    return spilled_bytes_;
  }
  [[nodiscard]] SpillSegmentId SegmentId() const noexcept {
    return segment_id_;
  }

 private:
  template <typename ChunkRef>
  [[nodiscard]] Status AppendChunkImpl(ChunkRef&& chunk);
  [[nodiscard]] size_t EffectiveBudgetBytes() const noexcept;
  [[nodiscard]] size_t EffectiveIoBufferBytes() const noexcept;
  // Row-batch flush cap: staging flushes and spill frames stay below this
  // many bytes (mirrored in Options-derived budgets), so serialized frames
  // are never a full-chunk-scale duplicate of the in-memory chunk.
  [[nodiscard]] size_t StagingFlushBytes() const noexcept;
  [[nodiscard]] Status FlushStagingChunk();
  [[nodiscard]] Status TransitionToSpill();
  [[nodiscard]] Status SpillChunk(const DataChunk& chunk);
  // Release a scratch buffer grown by an oversized single-row frame so its
  // high-water capacity does not stay charged against the operator budget.
  void TrimCodecScratch();

  Options options_;
  std::weak_ptr<const bool> spill_store_alive_;
  std::optional<QueryMemoryGrant::ActiveConsumerGuard> consumer_guard_;
  std::optional<QueryMemoryGrant::LocalBatchCache> grant_cache_;

  std::vector<DataChunk> mem_chunks_;
  DataChunk staging_chunk_;
  size_t staging_bytes_{0};
  size_t staging_reserved_bytes_{0};
  size_t mem_bytes_{0};
  size_t peak_mem_bytes_{0};
  uint64_t spilled_bytes_{0};
  uint64_t total_rows_{0};

  bool spilled_{false};
  bool sealed_{false};
  std::unique_ptr<SpillSegmentWriter> writer_;
  SpillSegmentId segment_id_{kInvalidSpillSegmentId};
  std::string scratch_codec_;

  // Sequential read cursor state:
  std::unique_ptr<SpillSegmentReader> reader_;
  size_t mem_cursor_idx_{0};
  DataChunk active_read_chunk_;
  size_t active_read_row_{0};
};

}  // namespace tinylamb

#endif  // TINYLAMB_EXECUTOR_DETAIL_OOC_SPILLABLE_STORE_HPP
