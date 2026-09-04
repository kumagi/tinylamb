/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_PARALLEL_HASH_JOIN_HPP
#define TINYLAMB_EXECUTOR_PARALLEL_HASH_JOIN_HPP

#include <array>
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

  struct Entry {
    Row row;
    RowPosition position;
    uint64_t hash{0};
    std::string key_bytes;
    size_t next{kNil};
  };

  explicit ConcurrentJoinHashTable(size_t expected_entries = 1024);

  void Insert(uint64_t hash, std::string key_bytes, Row row,
              RowPosition position);

  [[nodiscard]] std::vector<std::pair<Row, RowPosition>> FindMatches(
      uint64_t hash, std::string_view key_bytes) const;

  [[nodiscard]] size_t Size() const;
  [[nodiscard]] size_t EstimatedBytes() const;

 private:
  struct Shard {
    mutable std::mutex mutex;
    std::vector<Entry> entries;
    std::vector<size_t> head_slots;  // head index into entries
    size_t mask{0};
  };

  std::array<Shard, kShardCount> shards_;
};

// Multi-threaded parallel hash join where worker threads share a synchronized
// concurrent hash table during build, synchronize at a barrier, and
// concurrently probe to emit joined rows.
class SharedBuildParallelHashJoin : public ExecutorBase,
                                    public PipelineBreaker {
 public:
  SharedBuildParallelHashJoin(
      Executor left, std::vector<slot_t> left_cols, Executor right,
      std::vector<slot_t> right_cols,
      size_t worker_count = std::thread::hardware_concurrency(),
      JoinKind kind = JoinKind::kInner);

  ~SharedBuildParallelHashJoin() override = default;

  bool Next(Row* dst, RowPosition* rp) override;
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
  void EnsureMaterialized();
  void BuildSharedHashTable();
  void ParallelProbe();
  [[nodiscard]] static std::string MakeKey(const Row& row,
                                           const std::vector<slot_t>& cols);
  [[nodiscard]] static bool KeyHasNull(const Row& row,
                                       const std::vector<slot_t>& cols);
  [[nodiscard]] static uint64_t HashKey(std::string_view key);

  Executor left_;
  std::vector<slot_t> left_cols_;
  Executor right_;
  std::vector<slot_t> right_cols_;
  size_t worker_count_{1};
  JoinKind kind_{JoinKind::kInner};

  ConcurrentJoinHashTable shared_hash_table_;
  std::vector<std::pair<Row, RowPosition>> output_;
  size_t output_offset_{0};
  bool materialized_{false};
  QueryMemoryCharge charge_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_EXECUTOR_PARALLEL_HASH_JOIN_HPP
