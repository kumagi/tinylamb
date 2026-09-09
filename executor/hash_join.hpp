/**
 * Copyright 2023 KUMAZAKI Hiroki
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef TINYLAMB_HASH_JOIN_HPP
#define TINYLAMB_HASH_JOIN_HPP

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "executor/executor_base.hpp"
#include "executor/hash_join_mode.hpp"
#include "executor/join_kind.hpp"
#include "executor/pipeline_breaker.hpp"
#include "executor/query_memory.hpp"
#include "expression/expression.hpp"
#include "page/row_position.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"

namespace tinylamb {

// Open-addressing join index (multiplicative hash + linear probing, load
// factor <= 0.7) with a per-key entry chain for duplicate matches.  Defined
// in hash_join.cpp.
class JoinHashIndex {
 public:
  static constexpr size_t kNil = ~size_t{0};
  enum class KeyMode : uint8_t { kInt64, kBytes };

  void Init(KeyMode mode, size_t expected_entries);
  void Insert(uint64_t hash, int64_t int_key, std::string_view byte_key,
              size_t row_index);
  [[nodiscard]] size_t Find(uint64_t hash, int64_t int_key,
                            std::string_view byte_key) const;
  [[nodiscard]] size_t ChainNext(size_t entry) const;
  [[nodiscard]] size_t RowIndex(size_t entry) const;

  [[nodiscard]] static uint64_t HashInt64(int64_t key);
  [[nodiscard]] static uint64_t HashBytes(std::string_view key);

 private:
  struct Entry {
    size_t row;
    size_t next;
  };

  void StoreSlotKey(size_t slot, int64_t int_key, std::string_view byte_key);
  [[nodiscard]] bool SlotKeyEquals(size_t slot, int64_t int_key,
                                   std::string_view byte_key) const;
  void Grow();

  KeyMode mode_{KeyMode::kBytes};
  std::vector<size_t> slots_;
  size_t mask_{0};
  size_t occupied_slots_{0};
  std::vector<Entry> entries_;
  std::vector<int64_t> slot_int_keys_;
  std::string arena_;
  std::vector<std::pair<uint32_t, uint32_t>> slot_byte_keys_;
};

class HashJoin : public ExecutorBase, public PipelineBreaker {
 public:
  HashJoin(Executor left, std::vector<slot_t> left_cols, Executor right,
           std::vector<slot_t> right_cols,
           size_t worker_count = std::thread::hardware_concurrency());
  HashJoin(Executor left, std::vector<slot_t> left_cols, Executor right,
           std::vector<slot_t> right_cols, HashJoinMode mode,
           size_t worker_count = std::thread::hardware_concurrency());
  // Semi/anti variants (decorrelated IN / EXISTS / NOT EXISTS). The probe
  // side is always the left child: matching emits that row once (semi) or
  // not at all (anti), and its row position survives for UPDATE/DELETE.
  HashJoin(Executor left, std::vector<slot_t> left_cols, Executor right,
           std::vector<slot_t> right_cols, HashJoinMode mode, JoinKind kind,
           size_t worker_count = std::thread::hardware_concurrency(),
           size_t right_width = 0, size_t left_width = 0);
  HashJoin(const HashJoin&) = delete;
  HashJoin(HashJoin&&) = delete;
  HashJoin& operator=(const HashJoin&) = delete;
  HashJoin& operator=(HashJoin&&) = delete;
  ~HashJoin() override;

  bool Next(Row* dst, RowPosition* rp) override;
  size_t NextBatch(DataChunk* destination,
                   size_t max_rows = kDefaultVectorSize) override;
  void Dump(std::ostream& o, int indent) const override;

  // PipelineBreaker interface
  [[nodiscard]] bool IsMaterialized() const override { return materialized_; }
  void MaterializePipeline() override { Materialize(); }
  [[nodiscard]] size_t MaterializedRowCount() const override;
  [[nodiscard]] size_t MaterializedBytes() const override;

  [[nodiscard]] size_t WorkerCount() const { return worker_count_; }
  [[nodiscard]] HashJoinMode Mode() const { return mode_; }
  [[nodiscard]] JoinKind Kind() const {
    return kind_;
  }  // Non-null only when a join key is null-safe; parallels left_cols_/
  // right_cols_. Passed to the key encoders so a NULL component hashes and
  // compares equal to another NULL (IS NOT DISTINCT FROM semantics) instead of
  // rejecting the row.
  [[nodiscard]] const std::vector<bool>* NullSafeArg() const {
    return any_null_safe_ ? &key_null_safe_ : nullptr;
  }

  // EXPLAIN annotations from the physical plan: per-key null-safety (an IS
  // NOT DISTINCT FROM equijoin matches NULL to NULL) and the residual
  // predicate filtered above the join.  When any key is null-safe the key
  // encoder folds NULL into a comparable sentinel so `NULL IS NOT DISTINCT
  // FROM NULL` joins correctly.
  void SetJoinAnnotations(std::vector<bool> key_null_safe,
                          Expression residual_note) {
    key_null_safe_ = std::move(key_null_safe);
    residual_note_ = std::move(residual_note);
    any_null_safe_ =
        std::ranges::any_of(key_null_safe_, [](bool safe) { return safe; });
  }

 private:
  struct JoinState;

  Status Materialize();
  Status MaterializeInMemory();
  Status MaterializeHybrid();
  bool MaterializeOrThrow();
  // Dedicated semi/anti pipeline: materialize both sides (reactive spill
  // aware), index the right side, and stream left rows through an existence
  // check. Output rows are the untouched probe rows, so row positions are
  // preserved for UPDATE/DELETE consumers.
  Status MaterializeSemiAnti();
  Status MaterializeOuter();
  Status MaterializeMarkJoin();
  Status MaterializeSingle();

  Status IntakeBothSides();
  void BuildShards();
  [[nodiscard]] uint32_t ShardOf(uint64_t hash) const;
  bool FetchNextProbe();
  void SetupInMemoryJoin();
  void SetupOneSideSpilled();
  Status SetupBothSpilled();
  template <typename RightCont>
  void JoinPartitionPair(
      const std::vector<std::pair<Row, RowPosition>>& left_part,
      const RightCont& right_part,
      std::vector<std::pair<Row, RowPosition>>* out);
  void RunStripedProbe();
  bool EmitNextMatch(Row* dst, RowPosition* rp);

  Executor left_;
  std::vector<slot_t> left_cols_;
  Executor right_;
  std::vector<slot_t> right_cols_;

  HashJoinMode mode_{HashJoinMode::kInMemory};
  JoinKind kind_{JoinKind::kInner};
  size_t right_width_{0};
  size_t left_width_{0};
  size_t worker_count_;
  bool materialized_{false};
  bool materialize_failed_{false};
  Status materialize_error_{Status::kSuccess};
  bool pipelined_{false};
  bool build_left_side_{false};
  size_t actual_build_rows_{0};
  size_t actual_probe_rows_{0};
  size_t join_matches_{0};
  // EXPLAIN-only join annotations (see SetJoinAnnotations).
  std::vector<bool> key_null_safe_;
  bool any_null_safe_{false};
  Expression residual_note_;
  // Adaptive small-build execution: the join starts as a nested loop over the
  // tiny build side (and installs its key set as an exact runtime filter),
  // then builds the hash index once the outer cardinality exceeds the
  // nested-loop threshold. Rejected probe rows are counted by the filter.
  static constexpr size_t kSmallBuildThreshold = 64;
  static constexpr size_t kNestedLoopProbeLimit = 4;
  bool runtime_filter_active_{false};
  size_t runtime_filter_keys_{0};
  std::atomic<size_t> probe_rows_rejected_{0};
  std::atomic<size_t> probe_rows_null_rejected_{0};
  bool adaptive_switched_{false};
  bool adaptive_stayed_nested_loop_{false};
  size_t memory_peak_bytes_{0};
  size_t spill_partition_count_{0};
  bool reopt_recorded_{false};
  // The hybrid path keeps its fully materialized output (frozen spill spec).
  std::vector<std::pair<Row, RowPosition>> output_;
  size_t output_offset_{0};
  QueryMemoryCharge output_charge_;

  std::unique_ptr<JoinState> state_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_HASH_JOIN_HPP
