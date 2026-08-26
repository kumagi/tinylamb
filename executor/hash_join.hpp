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

class HashJoin : public ExecutorBase {
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

  [[nodiscard]] size_t WorkerCount() const { return worker_count_; }
  [[nodiscard]] HashJoinMode Mode() const { return mode_; }
  [[nodiscard]] JoinKind Kind() const { return kind_; }

 private:
  struct JoinState;

  void Materialize();
  void MaterializeInMemory();
  void MaterializeHybrid();
  void MaterializeOrThrow();
  // Dedicated semi/anti pipeline: materialize both sides (reactive spill
  // aware), index the right side, and stream left rows through an existence
  // check. Output rows are the untouched probe rows, so row positions are
  // preserved for UPDATE/DELETE consumers.
  void MaterializeSemiAnti();
  void MaterializeOuter();

  void IntakeBothSides();
  void BuildShards();
  uint32_t ShardOf(uint64_t hash) const;
  bool FetchNextProbe();
  void SetupInMemoryJoin();
  void SetupOneSideSpilled();
  void SetupBothSpilled();
  template <typename RightCont>
  void JoinPartitionPair(const std::vector<std::pair<Row, RowPosition>>& left_part,
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
  bool pipelined_{false};
  bool build_left_side_{false};
  // The hybrid path keeps its fully materialized output (frozen spill spec).
  std::vector<std::pair<Row, RowPosition>> output_;
  size_t output_offset_{0};
  QueryMemoryCharge output_charge_;

  std::unique_ptr<JoinState> state_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_HASH_JOIN_HPP
