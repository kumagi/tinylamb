/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_NUMA_ARENA_HPP
#define TINYLAMB_EXECUTOR_NUMA_ARENA_HPP

#include <cstddef>
#include <cstdint>
#include <memory>
#include <thread>
#include <vector>

namespace tinylamb {

// Partitioned bump memory arena designed for NUMA-aware / thread-local
// parallel query execution without cross-thread lock contention.
class NumaArenaPartition {
 public:
  explicit NumaArenaPartition(size_t block_size = 64 * 1024);
  ~NumaArenaPartition();

  NumaArenaPartition(const NumaArenaPartition&) = delete;
  NumaArenaPartition& operator=(const NumaArenaPartition&) = delete;
  NumaArenaPartition(NumaArenaPartition&& other) noexcept;
  NumaArenaPartition& operator=(NumaArenaPartition&& other) noexcept;

  void* Allocate(size_t bytes, size_t alignment = alignof(std::max_align_t));

  template <typename T, typename... Args>
  T* New(Args&&... args) {
    void* ptr = Allocate(sizeof(T), alignof(T));
    return new (ptr) T(std::forward<Args>(args)...);
  }

  void Reset();

  [[nodiscard]] size_t AllocatedBytes() const { return allocated_; }
  [[nodiscard]] size_t CapacityBytes() const { return capacity_; }

 private:
  struct Block {
    std::unique_ptr<uint8_t[]> data;
    size_t size{0};
  };

  void AddBlock(size_t min_size);

  size_t default_block_size_{64 * 1024};
  std::vector<Block> blocks_;
  size_t current_block_idx_{0};
  size_t current_offset_{0};
  size_t allocated_{0};
  size_t capacity_{0};
};

class NumaArena {
 public:
  explicit NumaArena(
      size_t partition_count = std::thread::hardware_concurrency(),
      size_t default_block_size = 64 * 1024);

  NumaArenaPartition& Partition(size_t index);
  const NumaArenaPartition& Partition(size_t index) const;

  [[nodiscard]] size_t PartitionCount() const { return partitions_.size(); }
  [[nodiscard]] size_t TotalAllocatedBytes() const;
  [[nodiscard]] size_t TotalCapacityBytes() const;

  void Reset();

 private:
  std::vector<std::unique_ptr<NumaArenaPartition>> partitions_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_EXECUTOR_NUMA_ARENA_HPP
