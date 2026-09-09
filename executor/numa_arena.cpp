/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/numa_arena.hpp"

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

namespace tinylamb {

NumaArenaPartition::NumaArenaPartition(size_t block_size)
    : default_block_size_(std::max<size_t>(4096, block_size)) {
  AddBlock(default_block_size_);
}

NumaArenaPartition::~NumaArenaPartition() = default;

NumaArenaPartition::NumaArenaPartition(NumaArenaPartition&& other) noexcept
    : default_block_size_(other.default_block_size_),
      blocks_(std::move(other.blocks_)),
      current_block_idx_(other.current_block_idx_),
      current_offset_(other.current_offset_),
      allocated_(other.allocated_),
      capacity_(other.capacity_) {
  other.allocated_ = 0;
  other.capacity_ = 0;
  other.current_block_idx_ = 0;
  other.current_offset_ = 0;
}

NumaArenaPartition& NumaArenaPartition::operator=(
    NumaArenaPartition&& other) noexcept {
  if (this != &other) {
    default_block_size_ = other.default_block_size_;
    blocks_ = std::move(other.blocks_);
    current_block_idx_ = other.current_block_idx_;
    current_offset_ = other.current_offset_;
    allocated_ = other.allocated_;
    capacity_ = other.capacity_;
    other.allocated_ = 0;
    other.capacity_ = 0;
    other.current_block_idx_ = 0;
    other.current_offset_ = 0;
  }
  return *this;
}

void NumaArenaPartition::AddBlock(size_t min_size) {
  const size_t sz = std::max(default_block_size_, min_size);
  // Runtime-sized buffer; Block::data owns unique_ptr<uint8_t[]> by design.
  blocks_.push_back(Block{
      .data =
          std::make_unique<uint8_t[]>(sz),  // NOLINT(modernize-avoid-c-arrays)
      .size = sz,
  });
  capacity_ += sz;
}

void* NumaArenaPartition::Allocate(size_t bytes, size_t alignment) {
  if (bytes == 0) {
    return nullptr;
  }

  // First block-relative offset >= `from` whose ABSOLUTE address (base +
  // offset) honors the alignment.  operator new[] only guarantees
  // max_align_t for a block base, so requests above that (SIMD 32/64B)
  // must compensate for the base's own misalignment or the returned
  // pointer violates the contract depending on the heap state.
  auto aligned_from = [](const uint8_t* base, size_t from, size_t align) {
    const uintptr_t addr = reinterpret_cast<uintptr_t>(base) + from;
    if (const auto misalign = addr % align; misalign != 0) {
      from += align - misalign;
    }
    return from;
  };

  if (current_block_idx_ < blocks_.size()) {
    const Block& block = blocks_[current_block_idx_];
    const auto offset =
        aligned_from(block.data.get(), current_offset_, alignment);
    if (offset + bytes <= block.size) {
      void* result = block.data.get() + offset;
      current_offset_ = offset + bytes;
      allocated_ += bytes;
      return result;
    }
  }

  // Next block or allocate new large block
  if (current_block_idx_ + 1 < blocks_.size()) {
    ++current_block_idx_;
    const Block& block = blocks_[current_block_idx_];
    const auto offset = aligned_from(block.data.get(), 0, alignment);
    if (offset + bytes <= block.size) {
      current_offset_ = offset + bytes;
      allocated_ += bytes;
      return block.data.get() + offset;
    }
  }

  AddBlock(bytes + alignment);
  current_block_idx_ = blocks_.size() - 1;
  const Block& block = blocks_[current_block_idx_];
  const auto offset = aligned_from(block.data.get(), 0, alignment);
  current_offset_ = offset + bytes;
  allocated_ += bytes;
  return block.data.get() + offset;
}

void NumaArenaPartition::Reset() {
  current_block_idx_ = 0;
  current_offset_ = 0;
  allocated_ = 0;
}

NumaArena::NumaArena(size_t partition_count, size_t default_block_size) {
  const size_t count = std::max<size_t>(1, partition_count);
  partitions_.reserve(count);
  for (size_t i = 0; i < count; ++i) {
    partitions_.push_back(
        std::make_unique<NumaArenaPartition>(default_block_size));
  }
}

NumaArenaPartition& NumaArena::Partition(size_t index) {
  assert(index < partitions_.size());
  return *partitions_[index];
}

const NumaArenaPartition& NumaArena::Partition(size_t index) const {
  assert(index < partitions_.size());
  return *partitions_[index];
}

size_t NumaArena::TotalAllocatedBytes() const {
  size_t total = 0;
  for (const auto& p : partitions_) {
    total += p->AllocatedBytes();
  }
  return total;
}

size_t NumaArena::TotalCapacityBytes() const {
  size_t total = 0;
  for (const auto& p : partitions_) {
    total += p->CapacityBytes();
  }
  return total;
}

void NumaArena::Reset() {
  for (auto& p : partitions_) {
    p->Reset();
  }
}

}  // namespace tinylamb
