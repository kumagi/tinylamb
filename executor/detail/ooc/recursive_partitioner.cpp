/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/detail/ooc/recursive_partitioner.hpp"

#include <algorithm>
#include <ranges>
#include <utility>

#include "common/memory_budget.hpp"
#include "type/row_block_codec.hpp"

namespace tinylamb {
namespace {

constexpr size_t kPartitionWriteBatchRows = 128;
constexpr size_t kPartitionWriteBatchBytes = 16ULL << 10;

uint64_t MixHashWithDepth(uint64_t hash, uint32_t depth) noexcept {
  uint64_t x = hash ^ (static_cast<uint64_t>(depth) * 0x9e3779b97f4a7c15ULL);
  x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
  x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
  return x ^ (x >> 31);
}

}  // namespace

PartitionLeafSet::PartitionLeafSet(PartitionLeafSet&& other) noexcept
    : leaves(std::move(other.leaves)),
      store_(other.store_),
      store_alive_(std::move(other.store_alive_)) {
  other.store_ = nullptr;
}

PartitionLeafSet& PartitionLeafSet::operator=(
    PartitionLeafSet&& other) noexcept {
  if (this != &other) {
    DropAll();
    leaves = std::move(other.leaves);
    store_ = other.store_;
    store_alive_ = std::move(other.store_alive_);
    other.store_ = nullptr;
  }
  return *this;
}

void PartitionLeafSet::DropLeaf(size_t index) noexcept {
  if (index >= leaves.size()) {
    return;
  }
  PartitionLeaf& leaf = leaves[index];
  SpillStore::DropSegmentIfAlive(store_, store_alive_, leaf.segment_id);
  leaf.segment_id = kInvalidSpillSegmentId;
  leaf.memory_rows.clear();
}

void PartitionLeafSet::DropAll() noexcept {
  for (PartitionLeaf& leaf : leaves) {
    SpillStore::DropSegmentIfAlive(store_, store_alive_, leaf.segment_id);
    leaf.segment_id = kInvalidSpillSegmentId;
  }
  leaves.clear();
}

uint32_t RecursivePartitioner::ComputeFanoutBits(
    size_t budget_bytes, size_t* writer_buf_bytes_out) noexcept {
  if (budget_bytes == 0 || budget_bytes >= (8ULL << 20)) {
    *writer_buf_bytes_out = 32ULL << 10;
    return 6;  // 64 * 32 KiB = 2 MiB <= 4 MiB
  }
  if (budget_bytes >= (2ULL << 20)) {
    *writer_buf_bytes_out = 32ULL << 10;
    return 5;  // 32 * 32 KiB = 1 MiB
  }
  if (budget_bytes >= (512ULL << 10)) {
    *writer_buf_bytes_out = 16ULL << 10;
    return 4;  // 16 * 16 KiB = 256 KiB <= 256 KiB..512 KiB
  }
  *writer_buf_bytes_out =
      std::min<size_t>(16384, std::max<size_t>(4096, budget_bytes / 8));
  return 2;  // 4-way minimal fanout
}

uint32_t RecursivePartitioner::RadixBucketAtOffset(
    uint64_t hash, uint32_t bit_offset, uint32_t fanout_bits) noexcept {
  const uint32_t bits = std::clamp<uint32_t>(fanout_bits, 1U, 8U);
  const uint32_t mask = (uint32_t{1} << bits) - 1U;
  const uint32_t bit_end = bit_offset + bits;
  if (bit_end <= 64U) {
    return static_cast<uint32_t>((hash >> (64U - bit_end)) & mask);
  }
  // Beyond 64 MSB bits, derive fresh well-mixed bits keyed by cumulative offset
  // while staying purely deterministic for identical hashes.
  const uint64_t mixed = MixHashWithDepth(hash, bit_offset);
  return static_cast<uint32_t>((mixed >> (64U - bits)) & mask);
}

uint32_t RecursivePartitioner::RadixBucket(uint64_t hash, uint32_t depth,
                                           uint32_t fanout_bits) noexcept {
  const uint32_t bits = std::clamp<uint32_t>(fanout_bits, 1U, 8U);
  return RadixBucketAtOffset(hash, depth * bits, bits);
}

RecursivePartitioner::RecursivePartitioner(Options options, RowHashFn hash_fn)
    : options_(options), hash_fn_(std::move(hash_fn)) {
  root_fanout_bits_ =
      ComputeFanoutBits(EffectiveBudgetBytes(), &root_writer_buf_);
  if (options_.grant != nullptr) {
    const size_t refill =
        options_.memory_budget_bytes > 0
            ? std::min<size_t>(
                  QueryMemoryGrant::LocalBatchCache::kDefaultRefillBytes,
                  std::max<size_t>(65536, options_.memory_budget_bytes / 4))
            : QueryMemoryGrant::LocalBatchCache::kDefaultRefillBytes;
    grant_cache_.emplace(options_.grant, refill);
  }
}

RecursivePartitioner::~RecursivePartitioner() { CleanupInternal(); }

size_t RecursivePartitioner::EffectiveBudgetBytes() const noexcept {
  size_t b = options_.memory_budget_bytes;
  if (options_.grant != nullptr) {
    const size_t grant_op = options_.grant->OperatorBudgetBytes();
    if (b == 0 || grant_op < b) {
      b = grant_op;
    }
  }
  return b;
}

void RecursivePartitioner::CleanupInternal() noexcept {
  for (BucketWriter& bw : root_writers_) {
    if (bw.writer != nullptr) {
      bw.writer->Abort();
    }
  }
  root_writers_.clear();
  mem_rows_.clear();
  mem_rows_.shrink_to_fit();
  mem_bytes_ = 0;
  if (grant_cache_.has_value()) {
    grant_cache_->ReleaseAll();
  }
}

void RecursivePartitioner::Reset() noexcept {
  CleanupInternal();
  peak_mem_bytes_ = 0;
  total_rows_ = 0;
  max_observed_depth_ = 0;
  skew_fallback_count_ = 0;
  spilled_ = false;
  finalized_ = false;
}

Status RecursivePartitioner::FlushBucket(BucketWriter* bucket) {
  if (bucket->batch.empty()) {
    return Status::kSuccess;
  }
  RETURN_IF_FAIL(
      WriteRowBlock(bucket->writer.get(), bucket->batch, &bucket->scratch));
  bucket->batch.clear();
  bucket->batch_bytes = 0;
  return Status::kSuccess;
}

Status RecursivePartitioner::WriteRowToBucket(BucketWriter* bucket, Row&& row,
                                              size_t est_bytes) {
  bucket->estimated_bytes += est_bytes;
  ++bucket->row_count;
  bucket->batch_bytes += est_bytes;
  bucket->batch.push_back(std::move(row));
  if (bucket->batch.size() >= kPartitionWriteBatchRows ||
      bucket->batch_bytes >= kPartitionWriteBatchBytes) {
    RETURN_IF_FAIL(FlushBucket(bucket));
  }
  return Status::kSuccess;
}

Status RecursivePartitioner::InitRootSpillWriters() {
  if (options_.spill_store == nullptr) {
    return StatusError(
        StatusCode::kMemoryLimitExceeded,
        "RecursivePartitioner exceeded budget with no SpillStore configured");
  }
  root_fanout_bits_ =
      ComputeFanoutBits(EffectiveBudgetBytes(), &root_writer_buf_);
  const size_t fanout = size_t{1} << root_fanout_bits_;
  root_writers_.clear();
  root_writers_.resize(fanout);
  for (size_t i = 0; i < fanout; ++i) {
    root_writers_[i].writer = std::make_unique<SpillSegmentWriter>(
        options_.spill_store, root_writer_buf_);
  }
  return Status::kSuccess;
}

Status RecursivePartitioner::TransitionToSpill() {
  if (spilled_) {
    return Status::kSuccess;
  }
  RETURN_IF_FAIL(InitRootSpillWriters());
  for (Row& r : mem_rows_) {
    const uint64_t h = hash_fn_(r);
    const uint32_t bucket = RadixBucket(h, /*depth=*/0, root_fanout_bits_);
    const size_t bytes = EstimateRowMemoryBytes(r);
    RETURN_IF_FAIL(
        WriteRowToBucket(&root_writers_[bucket], std::move(r), bytes));
  }
  mem_rows_.clear();
  mem_rows_.shrink_to_fit();
  mem_bytes_ = 0;
  if (grant_cache_.has_value()) {
    grant_cache_->ReleaseAll();
  }
  spilled_ = true;
  return Status::kSuccess;
}

Status RecursivePartitioner::AddRow(const Row& row) {
  Row copy = row;
  return AddRow(std::move(copy));
}

Status RecursivePartitioner::AddRow(Row&& row) {
  if (finalized_) {
    return StatusError(StatusCode::kInvalidArgument,
                       "cannot AddRow to finalized RecursivePartitioner");
  }
  ++total_rows_;
  const size_t row_bytes = EstimateRowMemoryBytes(row);
  const size_t budget = EffectiveBudgetBytes();
  const bool always_spill = options_.force_spill ||
                            ProcessMemoryBudget::Global().AlwaysSpillEnabled();

  if (spilled_) {
    const uint64_t h = hash_fn_(row);
    const uint32_t bucket = RadixBucket(h, /*depth=*/0, root_fanout_bits_);
    return WriteRowToBucket(&root_writers_[bucket], std::move(row), row_bytes);
  }

  bool need_spill = always_spill;
  if (!need_spill && budget > 0 && !mem_rows_.empty() &&
      mem_bytes_ + row_bytes > budget) {
    need_spill = true;
  }
  if (!need_spill && budget > 0 && row_bytes > budget) {
    need_spill = true;
  }
  if (!need_spill && grant_cache_.has_value() &&
      !grant_cache_->Reserve(row_bytes)) {
    need_spill = true;
  }

  if (need_spill) {
    RETURN_IF_FAIL(TransitionToSpill());
    const uint64_t h = hash_fn_(row);
    const uint32_t bucket = RadixBucket(h, /*depth=*/0, root_fanout_bits_);
    return WriteRowToBucket(&root_writers_[bucket], std::move(row), row_bytes);
  }

  mem_rows_.push_back(std::move(row));
  mem_bytes_ += row_bytes;
  if (mem_bytes_ > peak_mem_bytes_) {
    peak_mem_bytes_ = mem_bytes_;
    ProcessMemoryBudget::Global().RecordOperatorPeakBytes(peak_mem_bytes_);
  }
  return Status::kSuccess;
}

Status RecursivePartitioner::AddChunk(const DataChunk& chunk) {
  for (size_t r = 0; r < chunk.Size(); ++r) {
    RETURN_IF_FAIL(AddRow(chunk.RowAt(r)));
  }
  return Status::kSuccess;
}

Status RecursivePartitioner::SplitOversizedPartition(
    const PendingPartition& part, uint32_t fanout_bits, size_t writer_buf,
    std::vector<PendingPartition>* children_out) {
  const size_t fanout = size_t{1} << fanout_bits;
  std::vector<BucketWriter> writers(fanout);
  for (size_t i = 0; i < fanout; ++i) {
    writers[i].writer =
        std::make_unique<SpillSegmentWriter>(options_.spill_store, writer_buf);
  }

  SpillSegmentReader reader(options_.spill_store, part.segment_id, writer_buf);
  std::vector<Row> block_rows;
  std::string read_scratch;
  const uint32_t next_depth = part.depth + 1;

  while (true) {
    block_rows.clear();
    ASSIGN_OR_RETURN(bool, has_more,
                     ReadRowBlock(&reader, &block_rows, &read_scratch));
    if (!has_more) {
      break;
    }
    for (Row& row : block_rows) {
      const uint64_t h = hash_fn_(row);
      const uint32_t bucket =
          RadixBucketAtOffset(h, part.bit_offset, fanout_bits);
      const size_t est = EstimateRowMemoryBytes(row);
      RETURN_IF_FAIL(WriteRowToBucket(&writers[bucket], std::move(row), est));
    }
  }

  for (BucketWriter& bw : writers) {
    if (bw.row_count == 0) {
      continue;
    }
    RETURN_IF_FAIL(FlushBucket(&bw));
    ASSIGN_OR_RETURN(SpillSegmentId, id, bw.writer->Finish(false));
    bw.scratch.clear();
    bw.scratch.shrink_to_fit();
    children_out->push_back(PendingPartition{
        .segment_id = id,
        .row_count = bw.row_count,
        .estimated_bytes = bw.estimated_bytes,
        .parent_bytes = part.estimated_bytes,
        .depth = next_depth,
        .bit_offset = part.bit_offset + fanout_bits,
    });
  }
  return Status::kSuccess;
}

Status RecursivePartitioner::Finalize(PartitionLeafSet* result_out) {
  if (result_out == nullptr) {
    return StatusError(StatusCode::kInvalidArgument,
                       "null result_out in RecursivePartitioner::Finalize");
  }
  if (finalized_) {
    return StatusError(StatusCode::kInvalidArgument,
                       "RecursivePartitioner already finalized");
  }
  result_out->DropAll();
  result_out->store_ = options_.spill_store;
  result_out->store_alive_ = options_.spill_store != nullptr
                                 ? options_.spill_store->LifetimeToken()
                                 : std::weak_ptr<const bool>{};
  finalized_ = true;

  if (!spilled_) {
    if (!mem_rows_.empty()) {
      PartitionLeaf leaf;
      leaf.in_memory = true;
      leaf.row_count = mem_rows_.size();
      leaf.estimated_bytes = mem_bytes_;
      leaf.memory_rows = std::move(mem_rows_);
      leaf.depth = 0;
      result_out->leaves.push_back(std::move(leaf));
    }
    mem_rows_.clear();
    mem_bytes_ = 0;
    if (grant_cache_.has_value()) {
      grant_cache_->ReleaseAll();
    }
    return Status::kSuccess;
  }

  // Seal root buckets first so all staging buffers are released before
  // recursive child passes allocate their own writers.
  uint64_t total_estimated_bytes = 0;
  for (const BucketWriter& bw : root_writers_) {
    total_estimated_bytes += bw.estimated_bytes;
  }

  // Own pending segments in a local RAII guard so any error returns clean up
  // all remaining segments without leaks.
  struct PendingGuard {
    explicit PendingGuard(SpillStore* s)
        : store(s),
          store_alive(s != nullptr ? s->LifetimeToken()
                                   : std::weak_ptr<const bool>{}) {}
    PendingGuard(const PendingGuard&) = delete;
    PendingGuard& operator=(const PendingGuard&) = delete;
    PendingGuard(PendingGuard&&) = delete;
    PendingGuard& operator=(PendingGuard&&) = delete;
    ~PendingGuard() {
      for (const PendingPartition& p : items) {
        SpillStore::DropSegmentIfAlive(store, store_alive, p.segment_id);
      }
    }
    SpillStore* store;
    std::weak_ptr<const bool> store_alive;
    std::vector<PendingPartition> items;
  } pending_guard(options_.spill_store);

  for (BucketWriter& bw : root_writers_) {
    if (bw.row_count == 0) {
      bw.writer->Abort();
      continue;
    }
    Status flush_st = FlushBucket(&bw);
    if (flush_st != Status::kSuccess) {
      CleanupInternal();
      return flush_st;
    }
    auto fin = bw.writer->Finish(false);
    if (!fin.HasValue()) {
      CleanupInternal();
      return fin.GetStatus();
    }
    pending_guard.items.push_back(PendingPartition{
        .segment_id = fin.Value(),
        .row_count = bw.row_count,
        .estimated_bytes = bw.estimated_bytes,
        .parent_bytes = total_estimated_bytes,
        .depth = 0,
        .bit_offset = root_fanout_bits_,
    });
  }
  root_writers_.clear();

  const size_t budget = EffectiveBudgetBytes();
  // The fanout is fixed at tree creation: recomputing it from the CURRENT
  // budget would let an active-consumer change shrink or grow the split
  // factor mid-tree, so children would no longer be refinements of the
  // parent split and the root's leaf-size guarantee would erode (skew
  // fallbacks spike). A budget change only re-sizes the writer buffers.
  size_t child_writer_buf = 16384;
  (void)ComputeFanoutBits(budget, &child_writer_buf);
  const uint32_t child_fanout_bits = root_fanout_bits_;

  while (!pending_guard.items.empty()) {
    PendingPartition part = pending_guard.items.back();
    pending_guard.items.pop_back();
    max_observed_depth_ = std::max(max_observed_depth_, part.depth);
    ProcessMemoryBudget::Global().RecordPartitionDepth(part.depth);

    const bool fits_budget = (budget == 0) || (part.estimated_bytes <= budget);
    if (fits_budget) {
      PartitionLeaf leaf;
      leaf.in_memory = false;
      leaf.segment_id = part.segment_id;
      leaf.row_count = part.row_count;
      leaf.estimated_bytes = part.estimated_bytes;
      leaf.depth = part.depth;
      leaf.skew_fallback = false;
      part.segment_id = kInvalidSpillSegmentId;  // ownership moved
      result_out->leaves.push_back(std::move(leaf));
      continue;
    }

    // Skew / depth guard: < 20% shrink vs parent (`bytes > 0.80 * parent`),
    // or recursion hit `max_depth`.
    const bool insufficient_shrink =
        part.parent_bytes > 0 &&
        part.estimated_bytes >
            static_cast<uint64_t>(static_cast<double>(part.parent_bytes) *
                                  0.80);
    if (part.depth >= options_.max_depth || insufficient_shrink) {
      ++skew_fallback_count_;
      ProcessMemoryBudget::Global().RecordSkewFallback();
      PartitionLeaf leaf;
      leaf.in_memory = false;
      leaf.segment_id = part.segment_id;
      leaf.row_count = part.row_count;
      leaf.estimated_bytes = part.estimated_bytes;
      leaf.depth = part.depth;
      leaf.skew_fallback = true;
      part.segment_id = kInvalidSpillSegmentId;  // ownership moved
      result_out->leaves.push_back(std::move(leaf));
      continue;
    }

    std::vector<PendingPartition> children;
    const Status split_st = SplitOversizedPartition(
        part, child_fanout_bits, child_writer_buf, &children);
    // Drop consumed parent segment immediately (blocks return to free_blocks_).
    options_.spill_store->DropSegment(part.segment_id);
    part.segment_id = kInvalidSpillSegmentId;
    if (split_st != Status::kSuccess) {
      for (const PendingPartition& c : children) {
        options_.spill_store->DropSegment(c.segment_id);
      }
      return split_st;
    }
    for (const PendingPartition& child : std::views::reverse(children)) {
      pending_guard.items.push_back(child);
    }
  }

  return Status::kSuccess;
}

Status RecursivePartitioner::ReadLeafRows(
    SpillStore* store, const PartitionLeaf& leaf,
    const std::function<Status(Row&&)>& on_row, size_t reader_buf_bytes) {
  if (leaf.in_memory) {
    for (const Row& r : leaf.memory_rows) {
      Row copy = r;
      RETURN_IF_FAIL(on_row(std::move(copy)));
    }
    return Status::kSuccess;
  }
  if (store == nullptr || leaf.segment_id == kInvalidSpillSegmentId) {
    return Status::kSuccess;
  }
  SpillSegmentReader reader(store, leaf.segment_id, reader_buf_bytes);
  std::vector<Row> block;
  std::string scratch;
  while (true) {
    block.clear();
    ASSIGN_OR_RETURN(bool, has_more, ReadRowBlock(&reader, &block, &scratch));
    if (!has_more) {
      break;
    }
    for (Row& r : block) {
      RETURN_IF_FAIL(on_row(std::move(r)));
    }
  }
  return Status::kSuccess;
}

}  // namespace tinylamb
