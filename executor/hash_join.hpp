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
#include <optional>
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
  // Outer/single joins require the null-supplying input's width when that
  // input is empty; omitted widths are inferred only from actual input rows.
  HashJoin(Executor left, std::vector<slot_t> left_cols, Executor right,
           std::vector<slot_t> right_cols, HashJoinMode mode, JoinKind kind,
           size_t worker_count = std::thread::hardware_concurrency(),
           size_t right_width = 0, size_t left_width = 0);
  HashJoin(const HashJoin&) = delete;
  HashJoin(HashJoin&&) = delete;
  HashJoin& operator=(const HashJoin&) = delete;
  HashJoin& operator=(HashJoin&&) = delete;
  ~HashJoin() override;

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
  // predicate retained by the plan.  When any key is null-safe the key
  // encoder folds NULL into a comparable sentinel so `NULL IS NOT DISTINCT
  // FROM NULL` joins correctly.
  void SetJoinAnnotations(std::vector<bool> key_null_safe,
                          Expression residual_note) {
    key_null_safe_ = std::move(key_null_safe);
    residual_note_ = std::move(residual_note);
    any_null_safe_ =
        std::ranges::any_of(key_null_safe_, [](bool safe) { return safe; });
  }

  void SetJoinResidual(Expression residual, Schema schema) {
    residual_predicate_ = std::move(residual);
    residual_schema_ = std::move(schema);
  }

  // Dynamic ("runtime") join filter pushdown (Phase 2 "Dynamic Filter Join /
  // SIP / Bloom").  With `build_left` the executor materializes the LEFT side
  // first, publishes a sound filter over its join keys to the probe scan
  // (children[1]) through ExecutorBase::AttachRuntimeFilter, and only then
  // consumes the probe side.  A probe row whose key has no build counterpart
  // cannot join, so the scan drops it before row decode.
  //
  // Gated at run time: an inner, null-safe-free, single INT64/DATE equi-join
  // whose probe child accepted the filter and whose build side did not spill
  // (a spilled build side is an incomplete key domain).  Anything else falls
  // back to the unchanged both-sides-first path.
  void EnableDynamicFilter(bool build_left) {
    dynamic_filter_ = true;
    dynamic_filter_build_left_ = build_left;
  }

  // Hash semi reduction (Phase 2 "Hash Semi Reduction").  A semi / anti /
  // null-aware anti join never emits a build column, so its hash build is a
  // pure membership test over the join key.  That admits two reductions which
  // the plain pipeline cannot do:
  //
  //  * probe key domain: the probe side's distinct join keys are materialized
  //    FIRST, so a build row whose key the probe side never carries is dropped
  //    while the build is still being consumed.  A huge fact on the build side
  //    shrinks to the keys that can still join instead of being hashed in full.
  //  * build key dedup: existence only needs one row per distinct key, so the
  //    duplicate chains the shared index would otherwise carry are never built.
  //
  // Purely an optimization: the emitted rows and their order are exactly the
  // plain pipeline's.  The gate is on kind, so inner / outer / mark / single
  // joins ignore it, and a run-time guard (the domain cap) falls back to the
  // unrestricted intake without changing any result either.
  void EnableSemiReduction() { semi_reduction_ = true; }
  [[nodiscard]] bool SemiReductionEnabled() const { return semi_reduction_; }
  // True when the reduced intake actually ran (an inner/outer kind, or a
  // disabled reduction, leaves this false even when it was requested).
  [[nodiscard]] bool SemiReductionActive() const {
    return semi_reduction_active_;
  }
  // Distinct probe join keys the domain holds. 0 on the fallback path.
  [[nodiscard]] size_t SemiReductionProbeKeys() const {
    return semi_reduction_probe_keys_;
  }
  // Build rows the child produced, the ones kept, and the split of the dropped
  // ones into "no probe row carries this key" and "duplicate of a kept key".
  [[nodiscard]] size_t SemiReductionBuildRows() const {
    return semi_reduction_build_rows_;
  }
  [[nodiscard]] size_t SemiReductionBuildRowsKept() const {
    return semi_reduction_build_rows_kept_;
  }
  [[nodiscard]] size_t SemiReductionBuildRowsPruned() const {
    return semi_reduction_build_rows_ - semi_reduction_build_rows_kept_;
  }
  [[nodiscard]] size_t SemiReductionDuplicateKeysPruned() const {
    return semi_reduction_duplicate_keys_;
  }
  // "probe_key_domain", "empty_probe_domain", "fallback=domain_cap" or
  // "disabled" -- why the run-time intake took the path it did.
  [[nodiscard]] const std::string& SemiReductionDescription() const {
    return semi_reduction_description_;
  }
  // Test hook: the domain cap is a run-time guard, so a test lowers it to
  // exercise the fallback without materializing a million distinct keys.
  static void SetSemiReductionMaxDomainKeysForTest(size_t keys);
  [[nodiscard]] bool DynamicFilterEnabled() const { return dynamic_filter_; }
  [[nodiscard]] bool DynamicFilterPublished() const {
    return dynamic_filter_published_;
  }
  [[nodiscard]] size_t DynamicFilterKeys() const {
    return dynamic_filter_keys_;
  }
  [[nodiscard]] size_t DynamicFilterProbeRows() const {
    return dynamic_filter_probe_rows_;
  }
  [[nodiscard]] size_t DynamicFilterRejected() const {
    return dynamic_filter_rejected_;
  }
  [[nodiscard]] const std::string& DynamicFilterDescription() const {
    return dynamic_filter_description_;
  }

 private:
  struct JoinState;

  Status Materialize();
  Status MaterializeInMemory();
  Status MaterializeInMemorySetup();
  // Build side first, publish the filter, then probe side.
  Status MaterializeDynamicFilter();
  [[nodiscard]] bool DynamicFilterEligible() const;
  Status MaterializeInMemoryTail();
  Status MaterializeHybrid();
  bool MaterializeOrThrow();
  // Dedicated semi/anti pipeline: materialize both sides (reactive spill
  // aware), index the right side, and stream left rows through an existence
  // check. Output rows are the untouched probe rows, so row positions are
  // preserved for UPDATE/DELETE consumers.
  Status MaterializeSemiAnti();
  // Existence-only join with a reduced hash build: the probe key domain is
  // built first and the build side is then consumed through it, deduplicated
  // on the join key. Emits through the same phase as MaterializeSemiAnti.
  Status MaterializeSemiAntiReduced();
  // Shared emission phase of the semi / anti / null-aware anti pipeline. The
  // caller has already decided which build rows exist; `build` describes the
  // build side as its CHILD produced it, which is what the three-valued
  // `NOT IN` rule and the "empty build side" rule read.
  struct SemiAntiBuild {
    size_t total_rows{0};
    bool has_null_key{false};
  };
  Status EmitSemiAnti(std::optional<ValueType> int_mode,
                      const SemiAntiBuild& build);
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
  Expression residual_predicate_;
  Schema residual_schema_;
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
  // Dynamic join filter state (see EnableDynamicFilter).
  bool dynamic_filter_{false};
  bool dynamic_filter_build_left_{false};
  bool dynamic_filter_published_{false};
  size_t dynamic_filter_keys_{0};
  // Rows the probe side produced, and how many the filter dropped afterwards.
  // When the probe scan applied the filter itself the pruning happened before
  // row decode and is only visible as the difference between these two.
  size_t dynamic_filter_probe_rows_{0};
  size_t dynamic_filter_rejected_{0};
  size_t dynamic_filter_null_rejected_{0};
  std::string dynamic_filter_description_;
  // Forces SetupInMemoryJoin to keep the orientation the dynamic filter chose
  // instead of re-deciding it from the two sides' byte sizes.
  bool dynamic_filter_forced_build_left_{false};
  bool adaptive_switched_{false};
  bool adaptive_stayed_nested_loop_{false};
  // Hash semi reduction state (see EnableSemiReduction). A distinct probe-key
  // domain above this cap would cost more memory than the build rows it could
  // prune, so the run-time intake gives up on the domain and keeps only the
  // (always sound) build key dedup.
  static std::atomic<size_t> semi_reduction_max_domain_keys_;
  bool semi_reduction_{false};
  bool semi_reduction_active_{false};
  size_t semi_reduction_probe_keys_{0};
  size_t semi_reduction_build_rows_{0};
  size_t semi_reduction_build_rows_kept_{0};
  size_t semi_reduction_duplicate_keys_{0};
  std::string semi_reduction_description_{"disabled"};
  size_t memory_peak_bytes_{0};
  size_t spill_partition_count_{0};
  // The hybrid path keeps its fully materialized output (frozen spill spec).
  std::vector<std::pair<Row, RowPosition>> output_;
  size_t output_offset_{0};
  QueryMemoryCharge output_charge_;

  std::unique_ptr<JoinState> state_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_HASH_JOIN_HPP
