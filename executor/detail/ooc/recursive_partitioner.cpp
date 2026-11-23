/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/detail/ooc/recursive_partitioner.hpp"

#include <algorithm>
#include <new>
#include <ranges>
#include <stdexcept>
#include <utility>

#include "common/exception_status.hpp"
#include "common/memory_budget.hpp"
#include "type/row_block_codec.hpp"

namespace tinylamb {
namespace {

constexpr size_t kPartitionWriteBatchRows = 128;
constexpr size_t kPartitionWriteBatchBytes = 16ULL << 10;

uint64_t MixHashWithDepth(uint64_t hash, uint64_t depth) noexcept {
  uint64_t x = hash ^ (depth * 0x9e3779b97f4a7c15ULL);
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
  *writer_buf_bytes_out = std::min<size_t>(16384, budget_bytes / 8);
  return 2;  // 4-way minimal fanout
}

uint32_t RecursivePartitioner::RadixBucketAtOffset(
    uint64_t hash, uint64_t bit_offset, uint32_t fanout_bits) noexcept {
  const uint32_t bits = std::clamp<uint32_t>(fanout_bits, 1U, 8U);
  const uint32_t mask = (uint32_t{1} << bits) - 1U;
  if (bit_offset <= 64U - bits) {
    return static_cast<uint32_t>((hash >> (64U - bits - bit_offset)) & mask);
  }
  // Beyond 64 MSB bits, derive fresh well-mixed bits keyed by cumulative offset
  // while staying purely deterministic for identical hashes.
  const uint64_t mixed = MixHashWithDepth(hash, bit_offset);
  return static_cast<uint32_t>((mixed >> (64U - bits)) & mask);
}

uint32_t RecursivePartitioner::RadixBucket(uint64_t hash, uint32_t depth,
                                           uint32_t fanout_bits) noexcept {
  const uint32_t bits = std::clamp<uint32_t>(fanout_bits, 1U, 8U);
  return RadixBucketAtOffset(hash, static_cast<uint64_t>(depth) * bits, bits);
}

RecursivePartitioner::RecursivePartitioner(Options options, RowHashFn hash_fn)
    : options_(options),
      store_alive_((options.spill_store != nullptr) ? options.spill_store->LifetimeToken()
                                       : std::weak_ptr<const bool>{}),
      hash_fn_(std::move(hash_fn)) {
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
  for (const PendingPartition& part : pending_partitions_) {
    SpillStore::DropSegmentIfAlive(options_.spill_store, store_alive_,
                                   part.segment_id);
  }
  pending_partitions_.clear();
  completed_leaves_.DropAll();
  std::vector<Row>().swap(mem_rows_);
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
  finalization_started_ = false;
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

StatusOr<uint64_t> RecursivePartitioner::HashRow(const Row& row) {
  try {
    return hash_fn_(row);
  } catch (...) {
    return StatusFromCurrentException("partition hash function threw");
  }
}

Status RecursivePartitioner::TransitionToSpill() try {
  if (spilled_) {
    return Status::kSuccess;
  }
  if (options_.spill_store == nullptr) {
    return StatusError(
        StatusCode::kMemoryLimitExceeded,
        "RecursivePartitioner exceeded budget with no SpillStore configured");
  }
  size_t writer_buffer = 0;
  const uint32_t fanout_bits =
      ComputeFanoutBits(EffectiveBudgetBytes(), &writer_buffer);
  std::vector<BucketWriter> pending(size_t{1} << fanout_bits);
  for (BucketWriter& bucket : pending) {
    bucket.writer = std::make_unique<SpillSegmentWriter>(options_.spill_store,
                                                         writer_buffer);
  }
  // Keep the original rows until every pending writer has flushed. A failed
  // transition destroys its private segments and leaves the rows retryable.
  for (const Row& row : mem_rows_) {
    ASSIGN_OR_RETURN(uint64_t, row_hash, HashRow(row));
    const uint32_t bucket = RadixBucket(row_hash, 0, fanout_bits);
    BucketWriter& writer = pending[bucket];
    const size_t row_bytes = EstimateRowMemoryBytes(row);
    if (row_bytes >= kPartitionWriteBatchBytes) {
      RETURN_IF_FAIL(FlushBucket(&writer));
      RETURN_IF_FAIL(WriteRowBlock(writer.writer.get(),
                                  std::span<const Row>(&row, 1),
                                  &writer.scratch));
      writer.estimated_bytes += row_bytes;
      ++writer.row_count;
    } else {
      RETURN_IF_FAIL(WriteRowToBucket(&writer, Row(row), row_bytes));
    }
  }
  for (BucketWriter& bucket : pending) {
    RETURN_IF_FAIL(FlushBucket(&bucket));
    RETURN_IF_FAIL(bucket.writer->Flush());
  }

  root_writers_ = std::move(pending);
  root_fanout_bits_ = fanout_bits;
  std::vector<Row>().swap(mem_rows_);
  mem_bytes_ = 0;
  if (grant_cache_.has_value()) {
    grant_cache_->ReleaseAll();
  }
  spilled_ = true;
  return Status::kSuccess;
} catch (const std::length_error&) {
  return Status::kTooBigData;
} catch (const std::bad_alloc&) {
  return Status::kMemoryLimitExceeded;
}

Status RecursivePartitioner::AddRow(const Row& row) try {
  Row copy = row;
  return AddRow(std::move(copy));
} catch (const std::length_error&) {
  return Status::kTooBigData;
} catch (const std::bad_alloc&) {
  return Status::kMemoryLimitExceeded;
}

Status RecursivePartitioner::AddRow(Row&& row) {
  if (options_.spill_store != nullptr && store_alive_.expired()) {
    return StatusError(StatusCode::kInvalidArgument,
                       "spill store is no longer alive");
  }
  if (finalization_started_) {
    return StatusError(StatusCode::kInvalidArgument,
                       "cannot AddRow after partition finalization starts");
  }
  const size_t row_bytes = EstimateRowMemoryBytes(row);
  if (!spilled_) {
    const size_t budget = EffectiveBudgetBytes();
    const bool need_spill =
        options_.force_spill ||
        ProcessMemoryBudget::Global().AlwaysSpillEnabled() ||
        (budget > 0 &&
         (row_bytes > budget || mem_bytes_ > budget - row_bytes)) ||
        (grant_cache_.has_value() && !grant_cache_->Reserve(row_bytes));
    if (!need_spill) {
      try {
        mem_rows_.push_back(std::move(row));
      } catch (const std::length_error&) {
        if (grant_cache_) {
          grant_cache_->Release(row_bytes);
        }
        return Status::kTooBigData;
      } catch (const std::bad_alloc&) {
        if (grant_cache_) {
          grant_cache_->Release(row_bytes);
        }
        return Status::kMemoryLimitExceeded;
      }
      ++total_rows_;
      mem_bytes_ += row_bytes;
      if (mem_bytes_ > peak_mem_bytes_) {
        peak_mem_bytes_ = mem_bytes_;
        ProcessMemoryBudget::Global().RecordOperatorPeakBytes(peak_mem_bytes_);
      }
      return Status::kSuccess;
    }
    RETURN_IF_FAIL(TransitionToSpill());
  }

  ASSIGN_OR_RETURN(uint64_t, row_hash, HashRow(row));
  const uint32_t bucket = RadixBucket(row_hash, 0, root_fanout_bits_);
  RETURN_IF_FAIL(
      WriteRowToBucket(&root_writers_[bucket], std::move(row), row_bytes));
  ++total_rows_;
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
    std::vector<PendingPartition>* children_out) try {
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
      ASSIGN_OR_RETURN(uint64_t, h, HashRow(row));
      const uint32_t bucket =
          RadixBucketAtOffset(h, part.bit_offset, fanout_bits);
      const size_t est = EstimateRowMemoryBytes(row);
      RETURN_IF_FAIL(WriteRowToBucket(&writers[bucket], std::move(row), est));
    }
  }

  // Reserve up front: a push_back reallocation throwing bad_alloc after a
  // writer's Finish() would orphan its segment (written but never registered).
  children_out->reserve(children_out->size() + writers.size());
  for (BucketWriter& bw : writers) {
    if (bw.row_count == 0) {
      continue;
    }
    RETURN_IF_FAIL(FlushBucket(&bw));
    ASSIGN_OR_RETURN(SpillSegmentId, id, bw.writer->Finish(false));
    bw.scratch.clear();
    // T3462: shrink_to_fit can throw bad_alloc across the Status boundary;
    // swap-with-empty frees the capacity without allocating (T2060 idiom).
    std::string().swap(bw.scratch);
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
} catch (const std::length_error&) {
  return Status::kTooBigData;
} catch (const std::bad_alloc&) {
  return Status::kMemoryLimitExceeded;
}

Status RecursivePartitioner::Finalize(PartitionLeafSet* result_out) try {
  if (result_out == nullptr) {
    return StatusError(StatusCode::kInvalidArgument,
                       "null result_out in RecursivePartitioner::Finalize");
  }
  if (finalized_) {
    return StatusError(StatusCode::kInvalidArgument,
                       "RecursivePartitioner already finalized");
  }
  if (options_.spill_store != nullptr && store_alive_.expired()) {
    return StatusError(StatusCode::kInvalidArgument,
                       "spill store is no longer alive");
  }
  if (!finalization_started_) {
    completed_leaves_.store_ = options_.spill_store;
    completed_leaves_.store_alive_ = store_alive_;
    finalization_started_ = true;
  }

  if (!spilled_) {
    if (!mem_rows_.empty()) {
      PartitionLeaf& leaf = completed_leaves_.leaves.emplace_back();
      leaf.in_memory = true;
      leaf.row_count = mem_rows_.size();
      leaf.estimated_bytes = mem_bytes_;
      leaf.memory_rows = std::move(mem_rows_);
      leaf.depth = 0;
    }
    mem_rows_.clear();
    mem_bytes_ = 0;
    if (grant_cache_.has_value()) {
      grant_cache_->ReleaseAll();
    }
    *result_out = std::move(completed_leaves_);
    finalized_ = true;
    return Status::kSuccess;
  }

  // Seal root buckets first so all staging buffers are released before
  // recursive child passes allocate their own writers.
  uint64_t total_estimated_bytes = 0;
  for (const BucketWriter& bw : root_writers_) {
    total_estimated_bytes += bw.estimated_bytes;
  }

  pending_partitions_.reserve(root_writers_.size());
  for (BucketWriter& bw : root_writers_) {
    if (bw.writer == nullptr) {
      continue;  // Already transferred by an earlier attempt.
    }
    if (bw.row_count == 0) {
      bw.writer.reset();
      continue;
    }
    // Start with an empty writer buffer so writing the next encoded batch
    // either buffers it in full or appends it to the segment in one call.
    // A failed append can then leave the original batch ready to retry.
    RETURN_IF_FAIL(bw.writer->Flush());
    RETURN_IF_FAIL(FlushBucket(&bw));
    ASSIGN_OR_RETURN(SpillSegmentId, id, bw.writer->Finish(false));
    pending_partitions_.push_back(PendingPartition{
        .segment_id = id,
        .row_count = bw.row_count,
        .estimated_bytes = bw.estimated_bytes,
        .parent_bytes = total_estimated_bytes,
        .depth = 0,
        .bit_offset = root_fanout_bits_,
    });
    bw.writer.reset();
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
  if (budget != 0) {
    child_writer_buf =
        std::min(child_writer_buf, budget >> (child_fanout_bits + 1));
  }

  while (!pending_partitions_.empty()) {
    const PendingPartition part = pending_partitions_.back();
    max_observed_depth_ = std::max(max_observed_depth_, part.depth);
    ProcessMemoryBudget::Global().RecordPartitionDepth(part.depth);

    const bool fits_budget = (budget == 0) || (part.estimated_bytes <= budget);
    // Skew / depth guard: < 20% shrink vs parent (`bytes > 0.80 * parent`),
    // or recursion hit `max_depth`.
    const bool insufficient_shrink =
        part.parent_bytes > 0 &&
        part.estimated_bytes >
            static_cast<uint64_t>(static_cast<double>(part.parent_bytes) *
                                  0.80);
    const bool skew_fallback =
        !fits_budget && (part.depth >= options_.max_depth || insufficient_shrink);
    if (fits_budget || skew_fallback) {
      completed_leaves_.leaves.push_back(PartitionLeaf{
          .memory_rows = {},
          .segment_id = part.segment_id,
          .row_count = part.row_count,
          .estimated_bytes = part.estimated_bytes,
          .depth = part.depth,
          .skew_fallback = skew_fallback,
      });
      pending_partitions_.pop_back();
      if (skew_fallback) {
        ++skew_fallback_count_;
        ProcessMemoryBudget::Global().RecordSkewFallback();
      }
      continue;
    }

    const size_t child_fanout = size_t{1} << child_fanout_bits;
    pending_partitions_.reserve(pending_partitions_.size() - 1 + child_fanout);
    std::vector<PendingPartition> children;
    children.reserve(child_fanout);
    const Status split_st = SplitOversizedPartition(
        part, child_fanout_bits, child_writer_buf, &children);
    if (split_st != Status::kSuccess) {
      for (const PendingPartition& c : children) {
        SpillStore::DropSegmentIfAlive(options_.spill_store, store_alive_,
                                       c.segment_id);
      }
      return split_st;
    }
    // Keep ownership of the parent until all children have sealed.
    pending_partitions_.pop_back();
    SpillStore::DropSegmentIfAlive(options_.spill_store, store_alive_,
                                   part.segment_id);
    for (const PendingPartition& child : std::views::reverse(children)) {
      pending_partitions_.push_back(child);
    }
  }

  *result_out = std::move(completed_leaves_);
  finalized_ = true;
  return Status::kSuccess;
} catch (const std::length_error&) {
  return Status::kTooBigData;
} catch (const std::bad_alloc&) {
  return Status::kMemoryLimitExceeded;
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
