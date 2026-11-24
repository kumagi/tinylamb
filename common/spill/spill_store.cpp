/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "common/spill/spill_store.hpp"

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <new>
#include <string>
#include <utility>

#include "common/memory_budget.hpp"
#include "common/platform/file_io.hpp"
#include "common/platform/process.hpp"

namespace tinylamb {
namespace {

Status ErrnoToSpillStatus(int err, const char* context) {
  if (err == ENOSPC || err == EDQUOT) {
    return StatusError(StatusCode::kNoSpace,
                       std::string(context) + ": no temporary space available");
  }
  return StatusError(StatusCode::kIOError,
                     std::string(context) + ": errno=" + std::to_string(err));
}

Status PwriteAll(int fd, const uint8_t* data, size_t len, int64_t offset,
                 size_t& done) {
  while (done < len) {
    const ssize_t rc = platform::Pwrite(fd, data + done, len - done,
                                        offset + static_cast<int64_t>(done));
    if (rc < 0) {
      if (errno == EINTR) {
        continue;
      }
      return ErrnoToSpillStatus(errno, "spill write failed");
    }
    if (rc == 0) {
      return StatusError(StatusCode::kIOError, "spill write returned 0 bytes");
    }
    done += static_cast<size_t>(rc);
  }
  return Status::kSuccess;
}

Status PreadAll(int fd, uint8_t* dst, size_t len, int64_t offset) {
  size_t done = 0;
  while (done < len) {
    const ssize_t rc = platform::Pread(fd, dst + done, len - done,
                                       offset + static_cast<int64_t>(done));
    if (rc < 0) {
      if (errno == EINTR) {
        continue;
      }
      return ErrnoToSpillStatus(errno, "spill read failed");
    }
    if (rc == 0) {
      return StatusError(StatusCode::kCorrupt,
                         "unexpected EOF while reading spill segment");
    }
    done += static_cast<size_t>(rc);
  }
  return Status::kSuccess;
}

std::filesystem::path ResolveTempDir(const SpillStoreOptions& options) {
  // T2066: an explicitly provided temp_dir is honored verbatim, with ""
  // resolving to the current working directory; only an option the caller
  // never set falls through to TINYLAMB_TEMP_DIR / the OS temp directory.
  if (options.temp_dir_set) {
    return options.temp_dir.empty() ? std::filesystem::path(".")
                                    : options.temp_dir;
  }
  if (!options.temp_dir.empty()) {
    return options.temp_dir;
  }
  const char* env_dir = ::tinylamb::platform::GetEnv("TINYLAMB_TEMP_DIR");
  if (env_dir != nullptr && env_dir[0] != '\0') {
    return {env_dir};
  }
  return {};
}

}  // namespace

SpillStore::SpillStore(SpillStoreOptions options)
    : options_(std::move(options)) {}

SpillStore::~SpillStore() {
  alive_token_.reset();
  std::unique_lock lk(mu_);
  // A Read() that dropped mu_ for PreadAll still needs mu_, segments_ and fd_
  // to finish. Wait for those readers before destroying any of them; the
  // lifetime token above stops new helper-driven drops but not Read() itself.
  reads_done_cv_.wait(lk, [this] { return active_reads_total_ == 0; });
  segments_.clear();
  free_blocks_.clear();
  active_blocks_ = 0;
  current_active_bytes_ = 0;
  if (fd_ >= 0) {
    // A failed close on an unlinked temp file can leak the descriptor (notably
    // Windows' _close() may leave the underlying OS handle open on failure).
    // Use the platform's best-effort release helper so the handle is freed even
    // on a non-EINTR failure; a destructor has no caller to report an error to.
    platform::ReleaseFile(fd_);
    fd_ = -1;
  }
}

size_t SpillStore::EffectiveTempLimitBytes() const noexcept {
  if (options_.temp_limit_bytes > 0) {
    return options_.temp_limit_bytes;
  }
  if (options_.use_global_temp_limit) {
    return ProcessMemoryBudget::Global().TempLimitBytes();
  }
  return 0;
}

Status SpillStore::EnsureFileOpenLocked() {
  if (fd_ >= 0) {
    return Status::kSuccess;
  }
  const int opened = platform::OpenUnlinkedTempFile(ResolveTempDir(options_));
  if (opened < 0) {
    return ErrnoToSpillStatus(errno, "failed to open spill temp file");
  }
  fd_ = opened;
  return Status::kSuccess;
}

void SpillStore::ReturnBlocksLocked(
    const std::vector<uint32_t>& blocks) noexcept {
  if (blocks.empty()) {
    return;
  }
  for (const uint32_t blk : blocks) {
    // Callers (DropSegment/Clear and Append's rollback) are noexcept cleanup
    // paths: an escaping std::bad_alloc would terminate the process.  If the
    // free list cannot grow, leak the remaining physical blocks from the reuse
    // pool instead; the accounting below still drops every returned block so
    // the temp-space limit stays honest.
    try {
      free_blocks_.push_back(blk);
    } catch (const std::bad_alloc&) {
      break;
    }
  }
  active_blocks_ =
      active_blocks_ > blocks.size() ? active_blocks_ - blocks.size() : 0;
  current_active_bytes_ = active_blocks_ * kBlockSize;
}

void SpillStore::MaybeTruncateWhenEmptyLocked() noexcept {
  if (active_blocks_ != 0 || fd_ < 0) {
    return;
  }
  if (platform::Truncate(fd_, 0) != 0) {
    return;  // keep the stale byte accounting; the file is still non-empty
  }
  free_blocks_.clear();
  next_block_index_ = 0;
  physical_file_bytes_ = 0;
}

StatusOr<SpillSegmentId> SpillStore::AllocateSegment() {
  std::scoped_lock lk(mu_);
  const SpillSegmentId id = next_segment_id_++;
  const auto [it, inserted] = segments_.emplace(id, SegmentState{});
  if (!inserted) {
    // The id is still live; never hand an existing segment to a new owner.
    return StatusError(StatusCode::kCorrupt, "spill segment id already in use");
  }
  ++segments_created_;
  return id;
}

void SpillStore::DropSegment(SpillSegmentId id) noexcept {
  if (id == kInvalidSpillSegmentId) {
    return;
  }
  std::scoped_lock lk(mu_);
  auto it = segments_.find(id);
  if (it == segments_.end() || it->second.pending_drop) {
    return;
  }
  ++segments_dropped_;
  if (it->second.write_in_progress || it->second.active_reads > 0) {
    // An Append() PwriteAll or Read() PreadAll is running unlocked; defer
    // returning existing blocks and erasing this entry until it finishes so
    // those blocks cannot be reused or truncated underneath the in-flight
    // I/O.
    it->second.pending_drop = true;
    return;
  }
  ReturnBlocksLocked(it->second.blocks);
  segments_.erase(it);
  MaybeTruncateWhenEmptyLocked();
}

Status SpillStore::Append(SpillSegmentId id, const void* data, size_t len) {
  if (id == kInvalidSpillSegmentId) {
    return StatusError(StatusCode::kInvalidArgument,
                       "invalid spill segment id");
  }
  if (len == 0) {
    std::scoped_lock lk(mu_);
    auto it = segments_.find(id);
    // Match the zero-length Read path: a segment scheduled for drop is no
    // longer writable, even for a zero-byte append.
    if (it == segments_.end() || it->second.pending_drop) {
      return StatusError(StatusCode::kNotExists, "spill segment not found");
    }
    return Status::kSuccess;
  }
  if (data == nullptr) {
    return StatusError(StatusCode::kInvalidArgument,
                       "null buffer in SpillStore::Append");
  }
  // T37: the injected-failure counter is consumed at each actual Pwrite
  // attempt below, NOT up front: a rejected Append (unknown / pending_drop
  // segment, temp-space limit, block-index limit) never issues I/O, so it
  // must not burn a scheduled injection slot or inflate the OOC fuzz
  // sweep's fired-count with injections that were never observable.

  struct WriteSlice {
    uint32_t block_idx{0};
    size_t offset_in_block{0};
    size_t chunk_len{0};
    size_t src_offset{0};
    bool completes_block{false};
  };

  int fd = -1;
  std::vector<uint32_t> newly_reserved;
  std::vector<WriteSlice> plan;

  {
    std::scoped_lock lk(mu_);
    auto it = segments_.find(id);
    if (it == segments_.end() || it->second.pending_drop) {
      return StatusError(StatusCode::kNotExists, "spill segment not found");
    }
    SegmentState& seg = it->second;
    if (seg.write_in_progress) {
      return StatusError(StatusCode::kConflicts,
                         "concurrent Append on the same spill segment");
    }

    const auto intra = static_cast<size_t>(seg.logical_bytes % kBlockSize);
    const size_t tail_free =
        (seg.logical_bytes ==
         static_cast<uint64_t>(seg.blocks.size()) * kBlockSize)
            ? 0
            : (kBlockSize - intra);
    const size_t remaining_after_tail = len > tail_free ? (len - tail_free) : 0;
    const size_t needed_blocks =
        (remaining_after_tail / kBlockSize) +
        static_cast<size_t>(remaining_after_tail % kBlockSize != 0);

    const size_t limit_bytes = EffectiveTempLimitBytes();
    if (limit_bytes > 0) {
      const uint64_t limit_blocks = limit_bytes / kBlockSize;
      if (active_blocks_ > limit_blocks ||
          needed_blocks > limit_blocks - active_blocks_) {
        return StatusError(StatusCode::kNoSpace,
                           "spill store temporary space limit exceeded");
      }
    }

    // Only blocks that cannot be reused from free_blocks_ advance
    // next_block_index_; an Append served entirely from the free list must
    // succeed even when the high-water mark is near UINT32_MAX.
    const size_t fresh_blocks =
        needed_blocks - std::min(needed_blocks, free_blocks_.size());
    if (fresh_blocks >
        std::numeric_limits<uint32_t>::max() - next_block_index_) {
      return StatusError(StatusCode::kNoSpace,
                         "spill store maximum addressable blocks reached");
    }

    if (needed_blocks >= plan.max_size() ||
        needed_blocks > seg.blocks.max_size() - seg.blocks.size()) {
      return StatusError(StatusCode::kTooBigData,
                         "spill store append plan is too large");
    }

    RETURN_IF_FAIL(EnsureFileOpenLocked());
    fd = fd_;

    // Pre-allocate every vector this Append grows BEFORE mutating the block
    // ledger: a push_back/insert reallocation throwing mid-section would
    // leave reserved blocks unowned and write_in_progress stuck true,
    // permanently leaking the segment (DropSegment/Clear only defer while a
    // write is in progress). With capacity secured, the reservation, plan
    // build and final commit cannot throw; an allocation failure here is
    // translated to a Status while no state has changed yet.
    try {
      newly_reserved.reserve(needed_blocks);
      plan.reserve(needed_blocks + 1);
      seg.blocks.reserve(seg.blocks.size() + needed_blocks);
    } catch (const std::bad_alloc&) {
      return StatusError(StatusCode::kMemoryLimitExceeded,
                         "spill store append plan allocation failed");
    }
    for (size_t i = 0; i < needed_blocks; ++i) {
      if (!free_blocks_.empty()) {
        newly_reserved.push_back(free_blocks_.back());
        free_blocks_.pop_back();
        ++blocks_reused_;
      } else {
        newly_reserved.push_back(next_block_index_++);
      }
    }
    active_blocks_ += needed_blocks;
    current_active_bytes_ = active_blocks_ * kBlockSize;
    peak_active_bytes_ = std::max(peak_active_bytes_, current_active_bytes_);
    seg.write_in_progress = true;

    size_t src_off = 0;
    if (tail_free > 0 && !seg.blocks.empty()) {
      const size_t take = std::min(len, tail_free);
      plan.push_back(WriteSlice{
          .block_idx = seg.blocks.back(),
          .offset_in_block = intra,
          .chunk_len = take,
          .src_offset = 0,
          .completes_block = (intra + take == kBlockSize),
      });
      src_off += take;
    }
    for (const uint32_t blk : newly_reserved) {
      const size_t take = std::min(kBlockSize, len - src_off);
      plan.push_back(WriteSlice{
          .block_idx = blk,
          .offset_in_block = 0,
          .chunk_len = take,
          .src_offset = src_off,
          .completes_block = (take == kBlockSize),
      });
      src_off += take;
    }
  }

  // Execute Pwrite I/O outside `mu_` so independent segments/readers proceed
  // without lock contention.
  Status io_status = Status::kSuccess;
  uint64_t written_extent = 0;
  const auto* bytes = static_cast<const uint8_t*>(data);
  for (const auto& s : plan) {  // NOLINT(modernize-loop-convert)
    // T37: consume once per physical Pwrite so "fail the N-th spill I/O
    // call" (docs/ooc_fuzzing.md) counts the same events the loop emits;
    // previously only the SECOND+ block of one Append consulted the
    // counter while a single up-front consume covered (or wasted) the lot.
    if (ProcessMemoryBudget::Global().ConsumeInjectedSpillIoFailure()) {
      io_status = StatusError(StatusCode::kIOError,
                              "injected spill I/O failure during write");
      break;
    }
    const auto file_off = static_cast<int64_t>(
        (static_cast<uint64_t>(s.block_idx) * kBlockSize) + s.offset_in_block);
    size_t written = 0;
    io_status =
        PwriteAll(fd, bytes + s.src_offset, s.chunk_len, file_off, written);
    if (written != 0) {
      written_extent =
          std::max(written_extent, static_cast<uint64_t>(file_off) + written);
    }
    if (io_status != Status::kSuccess) {
      break;
    }
    if (s.completes_block && platform::kSupportsAdviseDontNeed) {
      platform::AdviseDontNeed(
          fd,
          static_cast<int64_t>(static_cast<uint64_t>(s.block_idx) * kBlockSize),
          static_cast<int64_t>(kBlockSize));
      // T2978: the header promises "Total blocks handed to
      // platform::AdviseDontNeed since construction", but only the read
      // path (ReleaseSegmentPageCache) counted; write-path advisories were
      // invisible, so the #1094 regression guard under-counted exactly the
      // write-dominated run-generation workloads.
      advise_calls_.fetch_add(1, std::memory_order_relaxed);
    }
  }

  {
    std::scoped_lock lk(mu_);
    physical_file_bytes_ = std::max(physical_file_bytes_, written_extent);
    peak_physical_file_bytes_ =
        std::max(peak_physical_file_bytes_, physical_file_bytes_);
    auto it = segments_.find(id);
    if (io_status != Status::kSuccess || it == segments_.end() ||
        it->second.pending_drop) {
      ReturnBlocksLocked(newly_reserved);
      if (it != segments_.end()) {
        it->second.write_in_progress = false;
        if (it->second.pending_drop && it->second.active_reads == 0) {
          ReturnBlocksLocked(it->second.blocks);
          segments_.erase(it);
        }
      }
      MaybeTruncateWhenEmptyLocked();
      return io_status == Status::kSuccess
                 ? StatusError(StatusCode::kNotExists,
                               "spill segment dropped concurrently")
                 : io_status;
    }
    SegmentState& seg = it->second;
    try {
      seg.blocks.reserve(seg.blocks.size() + newly_reserved.size());
      seg.blocks.insert(seg.blocks.end(), newly_reserved.begin(),
                        newly_reserved.end());
    } catch (const std::bad_alloc&) {
      // Data was written but cannot be tracked; release the reservation and
      // clear the in-progress flag so the segment does not wedge forever.
      ReturnBlocksLocked(newly_reserved);
      seg.write_in_progress = false;
      return StatusError(StatusCode::kMemoryLimitExceeded,
                         "out of memory recording spill blocks");
    }
    seg.logical_bytes += len;
    seg.write_in_progress = false;
  }

  bytes_written_.fetch_add(len, std::memory_order_relaxed);
  ProcessMemoryBudget::Global().RecordSpillBytes(len);
  return Status::kSuccess;
}

Status SpillStore::Read(SpillSegmentId id, uint64_t offset, void* dst,
                        size_t len) const {
  if (id == kInvalidSpillSegmentId) {
    return StatusError(StatusCode::kInvalidArgument,
                       "invalid spill segment id");
  }
  if (len == 0) {
    std::scoped_lock lk(mu_);
    auto it = segments_.find(id);
    if (it == segments_.end() || it->second.pending_drop) {
      return StatusError(StatusCode::kNotExists, "spill segment not found");
    }
    if (offset > it->second.logical_bytes) {
      return StatusError(StatusCode::kInvalidArgument,
                         "spill read offset past EOF");
    }
    return Status::kSuccess;
  }
  if (dst == nullptr) {
    return StatusError(StatusCode::kInvalidArgument,
                       "null destination in SpillStore::Read");
  }

  struct ReadSlice {
    uint32_t block_idx{0};
    size_t offset_in_block{0};
    size_t chunk_len{0};
    size_t dst_offset{0};
  };

  int fd = -1;
  std::vector<ReadSlice> plan;
  {
    std::scoped_lock lk(mu_);
    auto& self = const_cast<SpillStore&>(*this);
    auto it = segments_.find(id);
    if (it == segments_.end() || it->second.pending_drop) {
      return StatusError(StatusCode::kNotExists, "spill segment not found");
    }
    const SegmentState& seg = it->second;
    if (offset > seg.logical_bytes || len > seg.logical_bytes - offset) {
      return StatusError(StatusCode::kInvalidArgument,
                         "spill read range exceeds segment size");
    }
    fd = fd_;
    if (fd < 0) {
      return StatusError(StatusCode::kCorrupt,
                         "non-empty spill segment has no backing fd");
    }
    uint64_t pos = offset;
    size_t dst_off = 0;
    size_t rem = len;
    while (rem > 0) {
      const auto seg_blk_idx = static_cast<size_t>(pos / kBlockSize);
      const auto intra = static_cast<size_t>(pos % kBlockSize);
      if (seg_blk_idx >= seg.blocks.size()) {
        return StatusError(StatusCode::kCorrupt,
                           "spill segment block index out of range");
      }
      const size_t take = std::min(rem, kBlockSize - intra);
      plan.push_back(ReadSlice{
          .block_idx = seg.blocks[seg_blk_idx],
          .offset_in_block = intra,
          .chunk_len = take,
          .dst_offset = dst_off,
      });
      pos += take;
      dst_off += take;
      rem -= take;
    }
    // Pin the segment's blocks against DropSegment/Clear block reuse and
    // file truncation while PreadAll runs outside mu_. The store-wide count
    // also lets ~SpillStore wait for this read before destroying mu_/fd_.
    ++seg.active_reads;
    ++self.active_reads_total_;
  }

  auto* out = static_cast<uint8_t*>(dst);
  Status io_status = Status::kSuccess;
  try {
    for (const ReadSlice& s : plan) {
      // T37: consume once per physical Pread, mirroring Append's per-Pwrite
      // consumption, so "fail the N-th spill I/O" counts the same events the
      // loop emits. All validation already passed above, so a rejected Read
      // never burns a slot; the injection now fires outside mu_.
      if (ProcessMemoryBudget::Global().ConsumeInjectedSpillIoFailure()) {
        io_status = StatusError(StatusCode::kIOError,
                                "injected spill I/O failure during read");
        break;
      }
      const auto file_off = static_cast<int64_t>(
          (static_cast<uint64_t>(s.block_idx) * kBlockSize) +
          s.offset_in_block);
      io_status = PreadAll(fd, out + s.dst_offset, s.chunk_len, file_off);
      if (io_status != Status::kSuccess) {
        break;
      }
    }
  } catch (...) {
    // An exception escaping the unlocked section (e.g. a StatusError
    // allocation failure) must still release the active-read pins, or
    // ~SpillStore would wait forever on reads_done_cv_.
    std::scoped_lock lk(mu_);
    auto& self = const_cast<SpillStore&>(*this);
    auto it = self.segments_.find(id);
    if (it != self.segments_.end()) {
      SegmentState& seg = it->second;
      --seg.active_reads;
      if (seg.pending_drop && !seg.write_in_progress && seg.active_reads == 0) {
        self.ReturnBlocksLocked(seg.blocks);
        self.segments_.erase(it);
        self.MaybeTruncateWhenEmptyLocked();
      }
    }
    if (--self.active_reads_total_ == 0) {
      self.reads_done_cv_.notify_all();
    }
    throw;
  }

  {
    std::scoped_lock lk(mu_);
    // Read() is const for callers, but deferred bookkeeping (active-read
    // unpin, finishing a pending drop, freeing blocks, truncating the file)
    // is a real internal mutation; it is serialized under mu_ like every
    // other segment-state write.
    auto& self = const_cast<SpillStore&>(*this);
    auto it = self.segments_.find(id);
    if (it != self.segments_.end()) {
      SegmentState& seg = it->second;
      --seg.active_reads;
      // Finish a drop deferred by DropSegment/Clear/Append once the last
      // in-flight I/O on this segment is done.
      if (seg.pending_drop && !seg.write_in_progress && seg.active_reads == 0) {
        self.ReturnBlocksLocked(seg.blocks);
        self.segments_.erase(it);
        self.MaybeTruncateWhenEmptyLocked();
      }
    }
    if (--self.active_reads_total_ == 0) {
      self.reads_done_cv_.notify_all();
    }
  }
  RETURN_IF_FAIL(io_status);

  bytes_read_.fetch_add(len, std::memory_order_relaxed);
  return Status::kSuccess;
}

void SpillStore::ReleaseSegmentPageCache(SpillSegmentId id,
                                         uint64_t up_to_offset) const {
  if (!platform::kSupportsAdviseDontNeed || id == kInvalidSpillSegmentId ||
      up_to_offset < kBlockSize) {
    return;
  }
  int fd = -1;
  auto& self = const_cast<SpillStore&>(*this);
  // The newly completed blocks are almost always a single block (one 64 KiB
  // boundary per refill); keep that case allocation- and lock-time free.
  std::array<uint32_t, 64> stack_blocks{};
  std::vector<uint32_t> heap_blocks;
  size_t count = 0;
  const uint32_t* pending_blocks = nullptr;
  {
    std::scoped_lock lk(mu_);
    if (fd_ < 0) {
      return;
    }
    auto it = segments_.find(id);
    if (it == segments_.end() || it->second.pending_drop) {
      return;
    }
    const SegmentState& seg = it->second;
    const size_t completed_blocks = static_cast<size_t>(
        std::min<uint64_t>(up_to_offset / kBlockSize, seg.blocks.size()));
    if (completed_blocks <= seg.advised_blocks) {
      return;
    }
    fd = fd_;
    const size_t first = seg.advised_blocks;
    count = completed_blocks - first;
    if (count <= stack_blocks.size()) {
      std::copy_n(seg.blocks.begin() + static_cast<ptrdiff_t>(first), count,
                  stack_blocks.begin());
      pending_blocks = stack_blocks.data();
    } else {
      heap_blocks.assign(
          seg.blocks.begin() + static_cast<ptrdiff_t>(first),
          seg.blocks.begin() + static_cast<ptrdiff_t>(completed_blocks));
      pending_blocks = heap_blocks.data();
    }
    // Reserve the whole range under mu_ BEFORE issuing any advice outside the
    // lock: a concurrent call with a larger up_to_offset then starts at this
    // completed_blocks and only advises the newer blocks, so each block is
    // handed to AdviseDontNeed at most once per segment lifetime.
    seg.advised_blocks = completed_blocks;
    // Pin the block numbers just like Read() does. Otherwise Clear()/
    // DropSegment() could return these physical blocks to the free list and a
    // new segment could reuse them while the advisory syscalls below are
    // outside mu_, aiming MADV_DONTNEED at another segment's live pages.
    ++seg.active_reads;
    ++self.active_reads_total_;
  }
  // Always release the active_reads pins, even if an advisory syscall starts
  // throwing: ~SpillStore waits for active_reads_total_ == 0, so an exception
  // escaping this loop would hang destruction.
  const auto release_pins = [&]() {
    std::scoped_lock lk(mu_);
    auto it = self.segments_.find(id);
    if (it != self.segments_.end()) {
      SegmentState& seg = it->second;
      --seg.active_reads;
      if (seg.pending_drop && !seg.write_in_progress && seg.active_reads == 0) {
        self.ReturnBlocksLocked(seg.blocks);
        self.segments_.erase(it);
        self.MaybeTruncateWhenEmptyLocked();
      }
    }
    if (--self.active_reads_total_ == 0) {
      self.reads_done_cv_.notify_all();
    }
  };
  try {
    for (size_t i = 0; i < count; ++i) {
      advise_calls_.fetch_add(1, std::memory_order_relaxed);
      platform::AdviseDontNeed(
          fd,
          static_cast<int64_t>(static_cast<uint64_t>(pending_blocks[i]) *
                               kBlockSize),
          static_cast<int64_t>(kBlockSize));
    }
  } catch (...) {
    release_pins();
    throw;
  }
  release_pins();
}

StatusOr<uint64_t> SpillStore::SegmentSize(SpillSegmentId id) const {
  if (id == kInvalidSpillSegmentId) {
    return StatusError(StatusCode::kInvalidArgument,
                       "invalid spill segment id");
  }
  std::scoped_lock lk(mu_);
  auto it = segments_.find(id);
  if (it == segments_.end() || it->second.pending_drop) {
    return StatusError(StatusCode::kNotExists, "spill segment not found");
  }
  return it->second.logical_bytes;
}

void SpillStore::Clear() noexcept {
  std::scoped_lock lk(mu_);
  for (auto it = segments_.begin(); it != segments_.end();) {
    if (!it->second.pending_drop) {
      ++segments_dropped_;
      it->second.pending_drop = true;
    }
    if (it->second.write_in_progress || it->second.active_reads > 0) {
      ++it;
      continue;
    }
    ReturnBlocksLocked(it->second.blocks);
    it = segments_.erase(it);
  }
  MaybeTruncateWhenEmptyLocked();
}

SpillStoreStats SpillStore::Stats() const {
  std::scoped_lock lk(mu_);
  size_t live = 0;
  for (const auto& [seg_id, seg] : segments_) {
    (void)seg_id;
    if (!seg.pending_drop) {
      ++live;
    }
  }
  return SpillStoreStats{
      .bytes_written = bytes_written_.load(std::memory_order_relaxed),
      .bytes_read = bytes_read_.load(std::memory_order_relaxed),
      .current_active_bytes = current_active_bytes_,
      .peak_active_bytes = peak_active_bytes_,
      .physical_file_bytes = physical_file_bytes_,
      .peak_physical_file_bytes = peak_physical_file_bytes_,
      .segments_created = segments_created_,
      .segments_dropped = segments_dropped_,
      .live_segments = live,
      .blocks_reused = blocks_reused_,
      .open_fds = (fd_ >= 0) ? 1 : 0,
  };
}

// --- SpillSegmentWriter ---

SpillSegmentWriter::SpillSegmentWriter(SpillStore* store, size_t buffer_bytes)
    : store_(store),
      store_alive_(store != nullptr ? store->LifetimeToken()
                                    : std::weak_ptr<const bool>{}),
      buffer_capacity_(buffer_bytes) {}

SpillSegmentWriter::~SpillSegmentWriter() { Abort(); }

SpillSegmentWriter::SpillSegmentWriter(SpillSegmentWriter&& other) noexcept
    : store_(other.store_),
      store_alive_(std::move(other.store_alive_)),
      buffer_capacity_(other.buffer_capacity_),
      id_(other.id_),
      buffer_(std::move(other.buffer_)),
      bytes_written_(other.bytes_written_),
      finished_(other.finished_) {
  other.id_ = kInvalidSpillSegmentId;
  other.bytes_written_ = 0;
  other.finished_ = true;
}

SpillSegmentWriter& SpillSegmentWriter::operator=(
    SpillSegmentWriter&& other) noexcept {
  if (this != &other) {
    Abort();
    store_ = other.store_;
    store_alive_ = std::move(other.store_alive_);
    buffer_capacity_ = other.buffer_capacity_;
    id_ = other.id_;
    buffer_ = std::move(other.buffer_);
    bytes_written_ = other.bytes_written_;
    finished_ = other.finished_;
    other.id_ = kInvalidSpillSegmentId;
    other.bytes_written_ = 0;
    other.finished_ = true;
  }
  return *this;
}

Status SpillSegmentWriter::EnsureOpen() {
  if (id_ != kInvalidSpillSegmentId) {
    return Status::kSuccess;
  }
  if (store_ == nullptr) {
    return StatusError(StatusCode::kInvalidArgument,
                       "SpillSegmentWriter has null SpillStore");
  }
  ASSIGN_OR_RETURN(SpillSegmentId, allocated, store_->AllocateSegment());
  id_ = allocated;
  finished_ = false;
  return Status::kSuccess;
}

Status SpillSegmentWriter::Write(const void* data, size_t len) {
  if (finished_) {
    return StatusError(StatusCode::kInvalidArgument,
                       "write to finished SpillSegmentWriter");
  }
  if (data == nullptr && len != 0) {
    return StatusError(StatusCode::kInvalidArgument, "null spill write buffer");
  }
  RETURN_IF_FAIL(EnsureOpen());
  if (len == 0) {
    return Status::kSuccess;
  }

  if (buffer_capacity_ == 0) {
    RETURN_IF_FAIL(store_->Append(id_, data, len));
    bytes_written_ += len;
    return Status::kSuccess;
  }

  const auto* src = static_cast<const uint8_t*>(data);
  size_t rem = len;
  while (rem > 0) {
    if (buffer_.empty() && rem >= buffer_capacity_) {
      // Bypass intermediate buffer when caller already writes >=
      // buffer_capacity_.
      RETURN_IF_FAIL(store_->Append(id_, src, rem));
      bytes_written_ += rem;
      return Status::kSuccess;
    }
    const size_t space = buffer_capacity_ - buffer_.size();
    const size_t take = std::min(rem, space);
    try {
      buffer_.insert(buffer_.end(), src, src + take);
    } catch (const std::bad_alloc&) {
      // Write returns Status: a reallocation failure must not escape into the
      // OOC caller's Status boundary.
      return StatusError(StatusCode::kMemoryLimitExceeded,
                         "spill write buffer allocation failed");
    }
    src += take;
    rem -= take;
    if (buffer_.size() == buffer_capacity_) {
      // Flush first: on failure the buffered bytes were not persisted and
      // must not be counted as written. `bytes_written_` tracks persisted
      // bytes only; BytesWritten() adds the still-buffered tail back so the
      // returned logical offset stays stable across a failed flush + retry.
      RETURN_IF_FAIL(store_->Append(id_, buffer_.data(), buffer_.size()));
      bytes_written_ += buffer_.size();
      buffer_.clear();
    }
  }
  return Status::kSuccess;
}

Status SpillSegmentWriter::Flush() {
  if (buffer_.empty() || store_ == nullptr || id_ == kInvalidSpillSegmentId) {
    return Status::kSuccess;
  }
  RETURN_IF_FAIL(store_->Append(id_, buffer_.data(), buffer_.size()));
  bytes_written_ += buffer_.size();
  buffer_.clear();
  return Status::kSuccess;
}

StatusOr<SpillSegmentId> SpillSegmentWriter::Finish(bool keep_empty) {
  if (finished_) {
    return id_;
  }
  if (id_ == kInvalidSpillSegmentId && keep_empty) {
    RETURN_IF_FAIL(EnsureOpen());
  }
  if (id_ != kInvalidSpillSegmentId) {
    RETURN_IF_FAIL(Flush());
  }
  buffer_.clear();
  // T3462: shrink_to_fit reallocates and may throw bad_alloc straight out
  // of this Status-returning path (T2060 only covered Abort's noexcept
  // route).  Swapping with an empty vector releases the capacity without
  // any allocation.
  std::vector<uint8_t>().swap(buffer_);
  finished_ = true;
  return id_;
}

void SpillSegmentWriter::Abort() noexcept {
  buffer_.clear();
  // shrink_to_fit() may allocate and throw bad_alloc; this is noexcept and
  // is called from the destructor. Swap with an empty vector instead: the
  // old buffer is freed by the temporary's destructor without reallocating.
  std::vector<uint8_t>().swap(buffer_);
  if (!finished_ && store_ != nullptr && id_ != kInvalidSpillSegmentId) {
    SpillStore::DropSegmentIfAlive(store_, store_alive_, id_);
  }
  id_ = kInvalidSpillSegmentId;
  finished_ = true;
}

// --- SpillSegmentReader ---

SpillSegmentReader::SpillSegmentReader(const SpillStore* store,
                                       SpillSegmentId id, size_t buffer_bytes)
    : store_(store),
      store_alive_(store != nullptr ? store->LifetimeToken()
                                    : std::weak_ptr<const bool>{}),
      id_(id),
      buffer_capacity_(buffer_bytes) {}

Status SpillSegmentReader::Open() {
  if (opened_) {
    return Status::kSuccess;
  }
  if (store_ == nullptr || store_alive_.expired() ||
      id_ == kInvalidSpillSegmentId) {
    return StatusError(StatusCode::kInvalidArgument,
                       "invalid or expired SpillSegmentReader arguments");
  }
  ASSIGN_OR_RETURN(uint64_t, size, store_->SegmentSize(id_));
  total_size_ = size;
  cursor_ = 0;
  buf_start_ = 0;
  buf_pos_ = 0;
  buf_len_ = 0;
  last_advised_offset_ = 0;
  opened_ = true;
  return Status::kSuccess;
}

Status SpillSegmentReader::Seek(uint64_t offset) {
  RETURN_IF_FAIL(Open());
  if (offset > total_size_) {
    return StatusError(StatusCode::kInvalidArgument,
                       "SpillSegmentReader::Seek past EOF");
  }
  cursor_ = offset;
  // The exclusive end of the buffered range is not a buffer hit: treating it
  // as one skips the last_advised_offset_ rewind on a backward seek.
  if (offset >= buf_start_ && offset < buf_start_ + buf_len_) {
    buf_pos_ = static_cast<size_t>(offset - buf_start_);
  } else {
    buf_pos_ = 0;
    buf_len_ = 0;
    buf_start_ = offset;
    const uint64_t block_boundary =
        (offset / SpillStore::kBlockSize) * SpillStore::kBlockSize;
    last_advised_offset_ = std::min(block_boundary, last_advised_offset_);
  }
  return Status::kSuccess;
}

Status SpillSegmentReader::Refill() {
  if (cursor_ >= total_size_) {
    buf_pos_ = 0;
    buf_len_ = 0;
    return Status::kSuccess;
  }
  if (buffer_capacity_ == 0) {
    return Status::kSuccess;
  }
  const size_t to_read = static_cast<size_t>(
      std::min<uint64_t>(buffer_capacity_, total_size_ - cursor_));
  try {
    buffer_.resize(to_read);
  } catch (const std::bad_alloc&) {
    // Refill returns Status: a read-ahead buffer reallocation failure must not
    // escape into the OOC caller's Status boundary (the write path already
    // guards its buffer_.insert the same way).
    return StatusError(StatusCode::kMemoryLimitExceeded,
                       "spill read buffer allocation failed");
  }
  RETURN_IF_FAIL(store_->Read(id_, cursor_, buffer_.data(), to_read));
  // Advise the OS we no longer need completed blocks prior to `cursor_`.
  const uint64_t completed_boundary =
      (cursor_ / SpillStore::kBlockSize) * SpillStore::kBlockSize;
  if (completed_boundary > last_advised_offset_) {
    store_->ReleaseSegmentPageCache(id_, completed_boundary);
    last_advised_offset_ = completed_boundary;
  }
  buf_start_ = cursor_;
  buf_pos_ = 0;
  buf_len_ = to_read;
  return Status::kSuccess;
}

Status SpillSegmentReader::ReadExact(void* dst, size_t len) {
  RETURN_IF_FAIL(Open());
  if (len == 0) {
    return Status::kSuccess;
  }
  if (len > total_size_ - cursor_) {
    return StatusError(StatusCode::kInvalidArgument,
                       "SpillSegmentReader::ReadExact past EOF");
  }
  auto* out = static_cast<uint8_t*>(dst);
  size_t rem = len;
  while (rem > 0) {
    if (buf_pos_ < buf_len_) {
      const size_t avail = buf_len_ - buf_pos_;
      const size_t take = std::min(rem, avail);
      std::memcpy(out, buffer_.data() + buf_pos_, take);
      out += take;
      buf_pos_ += take;
      cursor_ += take;
      rem -= take;
      continue;
    }
    if (buffer_capacity_ == 0 || rem >= buffer_capacity_) {
      RETURN_IF_FAIL(store_->Read(id_, cursor_, out, rem));
      cursor_ += rem;
      const uint64_t completed_boundary =
          (cursor_ / SpillStore::kBlockSize) * SpillStore::kBlockSize;
      if (completed_boundary > last_advised_offset_) {
        store_->ReleaseSegmentPageCache(id_, completed_boundary);
        last_advised_offset_ = completed_boundary;
      }
      break;
    }
    RETURN_IF_FAIL(Refill());
    if (buf_len_ == 0) {
      return StatusError(StatusCode::kCorrupt,
                         "unexpected zero-length spill refill");
    }
  }
  return Status::kSuccess;
}

StatusOr<size_t> SpillSegmentReader::ReadUpTo(void* dst, size_t len) {
  RETURN_IF_FAIL(Open());
  const size_t to_read =
      static_cast<size_t>(std::min<uint64_t>(len, total_size_ - cursor_));
  if (to_read == 0) {
    return size_t{0};
  }
  RETURN_IF_FAIL(ReadExact(dst, to_read));
  return to_read;
}

}  // namespace tinylamb
