/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_DETAIL_OOC_EXTERNAL_SORTER_HPP
#define TINYLAMB_EXECUTOR_DETAIL_OOC_EXTERNAL_SORTER_HPP

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <vector>

#include "common/query_grant.hpp"
#include "common/spill/spill_store.hpp"
#include "common/status_or.hpp"
#include "executor/data_chunk.hpp"
#include "type/collation.hpp"
#include "type/row.hpp"

namespace tinylamb {

struct OocSortKey {
  slot_t column_index{0};
  bool ascending{true};
  std::optional<bool> nulls_first{std::nullopt};
  Collation collation{Collation::kNone};
};

// Bounded-memory external K-way merge sorter and Top-N operator over `Row`s
// (`ooca-v2.md` §5.2, ADR 0005 / ADR 0006).
//
// - Keeps purely in-memory sort / Top-N max-heap when inputs fit within
//   `memory_budget_bytes` (0 SpillStore FDs, zero spill overhead).
// - Spills sorted runs framed via `WriteRowBlock` when exceeding budget, then
//   performs multi-pass K-way merges keeping active reader/writer buffers
//   inside budget and immediately dropping consumed source runs for block
//   reuse.
class ExternalSorter {
 public:
  struct Options {
    SpillStore* spill_store{nullptr};
    size_t memory_budget_bytes{16ULL << 20};
    QueryMemoryGrant* grant{nullptr};
    std::vector<OocSortKey> keys;
    std::optional<size_t> limit;
    bool force_spill{false};
  };

  explicit ExternalSorter(Options options);
  ~ExternalSorter();

  ExternalSorter(const ExternalSorter&) = delete;
  ExternalSorter& operator=(const ExternalSorter&) = delete;
  ExternalSorter(ExternalSorter&&) = delete;
  ExternalSorter& operator=(ExternalSorter&&) = delete;

  [[nodiscard]] Status AddRow(const Row& row);
  [[nodiscard]] Status AddRow(Row&& row);
  [[nodiscard]] Status AddChunk(const DataChunk& chunk);
  // Pre-checks whether row `row_idx` in `chunk` cannot enter the active
  // Top-K heap (`!RowPrecedes(row, heap_root)`), letting callers skip
  // materializing `chunk.RowAt(row_idx)` when all SQL keys are direct slots
  // followed by an ascending sequence tie-breaker.
  [[nodiscard]] bool ShouldSkipTopKChunkRow(
      const DataChunk& chunk, size_t row_idx,
      size_t sql_key_count) const noexcept;
  void RecordSkippedRow() noexcept { ++total_rows_; }

  // Completes input phase, flushes any remaining run, and reduces spilled runs
  // via intermediate merge passes until a single streaming K-way pass can
  // emit directly into `NextRow`/`NextChunk`.
  [[nodiscard]] Status Finish();

  // Streams sorted output rows. Must be called after `Finish()`.
  [[nodiscard]] StatusOr<bool> NextRow(Row* row_out);
  [[nodiscard]] StatusOr<bool> NextChunk(DataChunk* chunk_out,
                                         size_t max_rows = kDefaultVectorSize);

  // Clears accumulated rows/runs and statistics so the sorter can be reused
  // for a new input. Construction-time `Options` are intentionally retained:
  // in particular a `limit` cannot be cleared here, so a sorter built with a
  // Top-N limit keeps applying it after Reset(). Rebuild the sorter for a
  // limit-less pass.
  void Reset() noexcept;

  [[nodiscard]] bool Spilled() const noexcept { return spilled_; }
  [[nodiscard]] uint64_t TotalRows() const noexcept { return total_rows_; }
  [[nodiscard]] uint64_t PeakMemoryBytes() const noexcept {
    return peak_mem_bytes_;
  }
  [[nodiscard]] uint64_t SpilledBytes() const noexcept {
    return spilled_bytes_;
  }
  [[nodiscard]] uint32_t RunCount() const noexcept { return run_count_; }
  [[nodiscard]] uint32_t MergePasses() const noexcept { return merge_passes_; }

 private:
  struct MergeStream {
    std::unique_ptr<SpillSegmentReader> reader;
    std::vector<Row> block_rows;
    size_t row_idx{0};
    std::string scratch;
    bool eof{false};
    // P34/61: owned copy of the current row's key columns (deep Value
    // copies; a copied VarChar materializes its data, so scratch reuse
    // cannot dangle).  The K-way merge compares these compact, contiguous
    // cells instead of chasing Row -> values_ -> Value per comparison.
    std::vector<Value> key_cells;
    std::vector<uint8_t> key_present;  // 1 = cells[i] valid for this row
  };

  // P34/61: loser tree over the merge streams (memory.md §3.4).  Each pop
  // costs exactly ceil(log2 k) key comparisons (the binary heap did ~2 log2 k)
  // and compares cached key cells; equal keys break by stream index.
  struct LoserMerge;

  [[nodiscard]] size_t EffectiveBudgetBytes() const noexcept;
  [[nodiscard]] int CompareRows(const Row& lhs, const Row& rhs) const noexcept;
  [[nodiscard]] bool RowPrecedes(const Row& lhs,
                                 const Row& rhs) const noexcept {
    return CompareRows(lhs, rhs) < 0;
  }
  [[nodiscard]] bool TrySortRowsPackedTwoKey(std::vector<Row>* rows) const;

  [[nodiscard]] Status FlushCurrentRun();
  [[nodiscard]] int CompareKeyCells(const MergeStream& lhs,
                                    const MergeStream& rhs) const noexcept;
  [[nodiscard]] Status RefreshStreamKey(MergeStream* stream) noexcept;
  [[nodiscard]] Status WriteSortedRun(std::vector<Row>* rows,
                                      SpillSegmentId* seg_out);
  [[nodiscard]] Status MergeRunGroup(std::span<const SpillSegmentId> inputs,
                                     size_t reader_buf_bytes,
                                     size_t writer_buf_bytes,
                                     SpillSegmentId* merged_out);
  [[nodiscard]] static Status AdvanceMergeStream(MergeStream* stream);
  [[nodiscard]] Status OpenFinalMergeStreams(size_t reader_buf_bytes);
  void ComputeMergeGeometry(size_t* merge_fan_in_out, size_t* reader_buf_out,
                            size_t* writer_buf_out) const noexcept;
  void ObservePeakMemory(size_t bytes) noexcept;
  void DropRunSegments() noexcept;

  Options options_;
  std::weak_ptr<const bool> spill_store_alive_;
  std::vector<SortKeyOrder> key_orders_;
  // Count this sorter as a live memory consumer for the query's grant, like
  // SpillableStore does, so ComputeEffectiveDop sees the whole query (#12699).
  std::optional<QueryMemoryGrant::ActiveConsumerGuard> consumer_guard_;
  std::optional<QueryMemoryGrant::LocalBatchCache> grant_cache_;

  std::vector<Row> run_rows_;
  size_t run_bytes_{0};
  bool in_limit_heap_{false};

  std::vector<SpillSegmentId> run_segments_;
  std::vector<MergeStream> final_streams_;
  std::unique_ptr<LoserMerge> final_merge_;  // stream-idx loser tree

  size_t in_mem_cursor_{0};
  uint64_t total_rows_{0};
  uint64_t emitted_rows_{0};
  uint64_t spilled_bytes_{0};
  size_t peak_mem_bytes_{0};
  uint32_t run_count_{0};
  uint32_t merge_passes_{0};
  bool spilled_{false};
  bool finished_{false};
};

}  // namespace tinylamb

#endif  // TINYLAMB_EXECUTOR_DETAIL_OOC_EXTERNAL_SORTER_HPP
