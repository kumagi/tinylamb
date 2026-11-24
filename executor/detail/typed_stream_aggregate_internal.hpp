/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#pragma once

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <exception>
#include <functional>
#include <iostream>
#include <iterator>
#include <limits>
#include <memory>
#include <mutex>
#include <numeric>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/exc_shim.hpp"
#include "common/exception_status.hpp"
#include "common/parallel_work.hpp"
#include "common/platform/extended_float.hpp"
#include "common/platform/int128_conversion.hpp"
#include "common/query_parallelism.hpp"
#include "common/regex_guard.hpp"
#include "common/sorted_distinct_runs.hpp"
#include "common/status_or.hpp"
#include "common/string_util.hpp"
#include "database/transaction_context.hpp"
#include "executor/aggregation.hpp"
#include "executor/detail/columnar_table.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/detail/flat_int_key_map.hpp"
#include "executor/detail/planning_heuristics.hpp"
#include "executor/detail/relation.hpp"
#include "executor/detail/scan_filter.hpp"
#include "executor/detail/subquery_runtime.hpp"
#include "executor/detail/timestamp_trunc_kernel.hpp"
#include "executor/detail/typed_stream_aggregate.hpp"
#include "executor/detail/window_eval.hpp"
#include "executor/query_memory.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/binary_expression.hpp"
#include "expression/bytecode.hpp"
#include "expression/case_expression.hpp"
#include "expression/cast_expression.hpp"
#include "expression/collation.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/function_call_expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/named_expression.hpp"
#include "expression/parameter_slot.hpp"
#include "expression/query_expression.hpp"
#include "expression/rewrite.hpp"
#include "expression/unary_expression.hpp"
#include "query/statement.hpp"
#include "table/full_scan_iterator.hpp"
#include "table/iterator.hpp"
#include "table/lstable_bridge.hpp"
#include "table/table.hpp"
#include "type/collation.hpp"
#include "type/column.hpp"
#include "type/type.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb::relational_detail {

// Chunked storage for the flat per-group accumulator states.  A plain
// std::vector<AggregateAccumulator> reallocates on geometric growth, and the
// realloc transient (old buffer + new buffer live together) peaks at ~3x the
// final state size — with 16 workers each folding 1.4M groups x ~300 bytes
// that was a ~20 GB transient spike whose page faults and cache thrash
// dominated the very scans the flat layout was meant to speed up.  Fixed
// chunks never move, and the chunk-pointer vector's growth moves 16-byte
// handles.
class AccumulatorChunks {
 public:
  static constexpr size_t kSlotsPerChunk = 512;
  // Backing store inside a chunk is committed lazily in this-many-slot
  // segments: the chunk keeps its uniform logical slot geometry (slot i
  // always lives in segment i / kSegmentSlots) while the tail segments of a
  // mostly-empty chunk are never allocated at all — a small group count no
  // longer commits the full ~440KB chunk slab.
  static constexpr size_t kSegmentSlots = 64;
  static constexpr size_t kSegmentBytes =
      kSegmentSlots * sizeof(AggregateAccumulator);

  void Configure(size_t stride) { stride_ = stride; }

  // Wholesale adoption when a merge target takes over a worker's whole state
  // (same stride and chunk geometry, so slot positions carry over as-is).
  void AdoptChunksFrom(AccumulatorChunks&& other) {
    chunks_ = std::move(other.chunks_);
    stride_ = other.stride_;
  }

  [[nodiscard]] AggregateAccumulator& State(size_t group, size_t i) {
    const size_t slot = (group * stride_) + i;
    return chunks_[slot / kSlotsPerChunk]->At(slot % kSlotsPerChunk);
  }
  [[nodiscard]] const AggregateAccumulator& State(size_t group,
                                                  size_t i) const {
    const size_t slot = (group * stride_) + i;
    return chunks_[slot / kSlotsPerChunk]->At(slot % kSlotsPerChunk);
  }

  void AppendGroup(const std::vector<const AggregateExpression*>& aggregates,
                   const std::vector<AggregateAccumulator::Shape>* shapes) {
    for (size_t i = 0; i < aggregates.size(); ++i) {
      if (chunks_.empty() || chunks_.back()->constructed() == kSlotsPerChunk) {
        chunks_.push_back(std::make_unique<Chunk>(kSlotsPerChunk));
      }
      if (shapes != nullptr) {
        chunks_.back()->ConstructAt((*aggregates[i]), (*shapes)[i]);
      } else {
        chunks_.back()->ConstructAt((*aggregates[i]));
      }
    }
  }

  // Backing store committed across all chunks so far — segment-granular, so
  // a small group count commits far less than the full
  // kSlotsPerChunk * sizeof(AggregateAccumulator) (~440KB) a single eager
  // slab used to reserve regardless of use.
  [[nodiscard]] size_t CommittedBytes() const {
    size_t total = 0;
    for (const auto& chunk : chunks_) {
      total += chunk->committed_bytes();
    }
    return total;
  }

 private:
  class Chunk {
   public:
    explicit Chunk(size_t capacity)
        : segments_((capacity + kSegmentSlots - 1) / kSegmentSlots) {}
    Chunk(const Chunk&) = delete;
    Chunk& operator=(const Chunk&) = delete;
    Chunk(Chunk&&) = delete;
    Chunk& operator=(Chunk&&) = delete;
    ~Chunk() {
      for (size_t i = 0; i < constructed_; ++i) {
        At(i).~AggregateAccumulator();
      }
    }
    AggregateAccumulator& At(size_t index) {
      return *std::launder(reinterpret_cast<AggregateAccumulator*>(
          segments_[index / kSegmentSlots].get() +
          ((index % kSegmentSlots) * sizeof(AggregateAccumulator))));
    }
    AggregateAccumulator& ConstructAt(const AggregateExpression& aggregate) {
      auto* placed =  // NOLINT(cppcoreguidelines-owning-memory)
          new (EnsureSlot(constructed_)) AggregateAccumulator(&aggregate);
      ++constructed_;
      return *placed;
    }
    AggregateAccumulator& ConstructAt(
        const AggregateExpression& aggregate,
        const AggregateAccumulator::Shape& shape) {
      auto* placed =  // NOLINT(cppcoreguidelines-owning-memory)
          new (EnsureSlot(constructed_))
              AggregateAccumulator(&aggregate, shape);
      ++constructed_;
      return *placed;
    }
    [[nodiscard]] size_t constructed() const { return constructed_; }
    [[nodiscard]] size_t committed_bytes() const {
      return committed_segments_ * kSegmentBytes;
    }

   private:
    std::byte* EnsureSlot(size_t index) {
      // NOLINTNEXTLINE(modernize-avoid-c-arrays) runtime-sized byte segment
      std::unique_ptr<std::byte[]>& segment = segments_[index / kSegmentSlots];
      if (segment == nullptr) {
        // NOLINTNEXTLINE(modernize-avoid-c-arrays)
        segment = std::make_unique<std::byte[]>(kSegmentBytes);
        ++committed_segments_;
      }
      return segment.get() +
             ((index % kSegmentSlots) * sizeof(AggregateAccumulator));
    }

    // NOLINTNEXTLINE(modernize-avoid-c-arrays) runtime-sized byte segments
    std::vector<std::unique_ptr<std::byte[]>> segments_;
    size_t committed_segments_{0};
    size_t constructed_{0};
  };

  std::vector<std::unique_ptr<Chunk>> chunks_;
  size_t stride_{1};
};

struct TypedWorkerGroups {
  bool int_key{false};
  bool packed_varchar_key{false};
  // Open-addressing: a fresh multi-million-group build (Q18/Q21 shapes) costs
  // no per-group node allocation, unlike the node-based row_index below.
  FlatIntKeyMap<size_t> int_index;
  // The group payload already owns canonical keys. Index their group IDs,
  // avoiding a second owned Row and a node allocation for every group.
  struct KeySlot {
    size_t hash{0};
    size_t group{SIZE_MAX};
  };
  std::vector<KeySlot> key_index;
  size_t indexed_groups{0};

  void ReserveKeyIndex(size_t capacity) {
    std::vector<KeySlot> replacement(capacity);
    for (const auto& old : key_index) {
      if (old.group == SIZE_MAX) {
        continue;
      }
      size_t slot = old.hash & (capacity - 1);
      while (replacement[slot].group != SIZE_MAX) {
        slot = (slot + 1) & (capacity - 1);
      }
      replacement[slot] = old;
    }
    key_index = std::move(replacement);
  }
  // A/B hatch: TINYLAMB_DISABLE_FLAT_GROUP_FOLD=1 restores the per-group
  // vector-of-vector layout.  The flat layout below strides two contiguous
  // vectors instead: group g's canonical keys live at
  // flat_group_keys[g * key_stride + k] and its accumulators at
  // flat_group_states[g * agg_stride + i].  The previous layout cost one heap
  // allocation per group per container — across Q21's two 1.4M-group builds
  // and Q18's 1.5M-group build that is ~5.7M allocations the flat layout
  // never makes, with the group states gaining contiguous locality in the
  // bargain.
  bool flat_layout{true};
  // Slim SoA mode (plan.slim): no AggregateAccumulator storage at all —
  // per (group, aggregate) slots live in three parallel vectors indexed by
  // group * agg_stride + i.  Element width is 8 bytes, so geometric growth
  // moves are cheap (the chunked layout above exists for the ~300-byte
  // accumulators; these need no chunking).
  bool slim{false};
  const std::vector<SlimAggKind>* slim_kinds{nullptr};
  // Resident grouping sets can prove a dynamic expression only produces
  // signed integers/doubles. Keep their sums separate, as the AST accumulator
  // does, and remember whether each group observed a DOUBLE (flag bit 1).
  std::vector<bool> mixed_numeric_sums;
  std::vector<int64_t> slim_counts;
  std::vector<int64_t> slim_int_totals;
  std::vector<double> slim_double_totals;
  std::vector<uint8_t> slim_flags;  // bit0: saw an infinite input (SUM double)
  // kCountDistinctInt64 state: kSlimDistinctSlots inline int64 slots per
  // (group, aggregate), the inline occupancy count, and a lazily materialized
  // overflow set.  Slot stride is kSlimDistinctSlots * agg_stride.
  std::vector<int64_t> slim_distinct;
  std::vector<uint8_t> slim_distinct_n;
  std::vector<std::unique_ptr<FlatIntKeyMap<uint8_t>>> slim_distinct_overflow;
  // Legacy layout (hatch only).
  std::vector<std::vector<Value>> group_keys;  // canonical key values
  std::vector<std::vector<AggregateAccumulator>> group_states;
  // Flat layout: canonical keys in one contiguous vector, accumulators in
  // fixed-size chunks (never moved, so no realloc transient).
  std::vector<Value> flat_group_keys;
  // Compact grouping sets can borrow canonical keys from their immutable
  // input until all sets have emitted their owned result rows.
  const std::vector<slot_t>* borrowed_key_offsets{nullptr};
  std::vector<const Row*> borrowed_key_rows;
  const ColumnarTable* borrowed_columnar{nullptr};
  const std::vector<uint32_t>* borrowed_chunk_prefix{nullptr};
  std::vector<uint32_t> borrowed_key_row_ids;
  AccumulatorChunks chunked_states;
  size_t key_stride{0};
  size_t agg_stride{0};
  size_t group_count{0};
  int64_t min_int_key{std::numeric_limits<int64_t>::max()};
  int64_t max_int_key{std::numeric_limits<int64_t>::min()};
  bool has_non_int_key{false};
  static constexpr size_t kMruSlots = 8;
  std::array<int64_t, kMruSlots> mru_keys{};
  std::array<size_t, kMruSlots> mru_groups{SIZE_MAX, SIZE_MAX, SIZE_MAX,
                                           SIZE_MAX, SIZE_MAX, SIZE_MAX,
                                           SIZE_MAX, SIZE_MAX};
  static size_t MruSlot(int64_t raw_key) {
    const auto u = static_cast<uint64_t>(raw_key);
    return static_cast<size_t>(((u ^ (u >> 33)) * 0x9E3779B97F4A7C15ULL) >> 61);
  }
  // Per-aggregate classification shared by every FindOrInsert of this worker;
  // null falls back to per-construction analysis.
  const std::vector<AggregateAccumulator::Shape>* shapes{nullptr};

  void Configure(bool flat, size_t keys_per_group,
                 size_t aggregates_per_group) {
    flat_layout = flat;
    key_stride = keys_per_group;
    agg_stride = aggregates_per_group;
    chunked_states.Configure(std::max<size_t>(aggregates_per_group, 1));
  }

  void ConfigureSlim(const TypedAggregationPlan& plan) {
    slim = plan.slim;
    slim_kinds = &plan.slim_kinds;
  }

  // Slim state accessors for group g, aggregate i.
  [[nodiscard]] int64_t& SlimCount(size_t g, size_t i) {
    return slim_counts[(g * agg_stride) + i];
  }
  [[nodiscard]] int64_t SlimCount(size_t g, size_t i) const {
    return slim_counts[(g * agg_stride) + i];
  }
  [[nodiscard]] int64_t& SlimIntTotal(size_t g, size_t i) {
    return slim_int_totals[(g * agg_stride) + i];
  }
  [[nodiscard]] int64_t SlimIntTotal(size_t g, size_t i) const {
    return slim_int_totals[(g * agg_stride) + i];
  }
  [[nodiscard]] double& SlimDoubleTotal(size_t g, size_t i) {
    return slim_double_totals[(g * agg_stride) + i];
  }
  [[nodiscard]] double SlimDoubleTotal(size_t g, size_t i) const {
    return slim_double_totals[(g * agg_stride) + i];
  }
  [[nodiscard]] uint8_t& SlimFlags(size_t g, size_t i) {
    return slim_flags[(g * agg_stride) + i];
  }
  [[nodiscard]] uint8_t SlimFlags(size_t g, size_t i) const {
    return slim_flags[(g * agg_stride) + i];
  }

  // Inserts one int64/date distinct value for kCountDistinctInt64; false when
  // it was already present.  Mirrors AggregateAccumulator::InsertDistinctInt.
  bool SlimInsertDistinct(size_t g, size_t i, int64_t raw) {
    const size_t state = (g * agg_stride) + i;
    int64_t* slots = &slim_distinct[state * kSlimDistinctSlots];
    const uint8_t n = slim_distinct_n[state];
    for (size_t s = 0; s < n; ++s) {
      if (slots[s] == raw) {
        return false;
      }
    }
    if (n < kSlimDistinctSlots) {
      slots[n] = raw;
      slim_distinct_n[state] = static_cast<uint8_t>(n + 1);
      return true;
    }
    if (state >= slim_distinct_overflow.size()) {
      slim_distinct_overflow.resize(slim_distinct_n.size());
    }
    auto& overflow = slim_distinct_overflow[state];
    if (!overflow) {
      overflow = std::make_unique<FlatIntKeyMap<uint8_t>>();
      overflow->Reserve(16);
    }
    bool inserted = false;
    uint8_t* slot = overflow->TryEmplace(raw, &inserted);
    if (inserted) {
      *slot = 1;
    }
    return inserted;
  }

  // Distinct values seen so far for kCountDistinctInt64 (the value COUNT
  // (DISTINCT) finishes to).
  [[nodiscard]] size_t SlimDistinctTotal(size_t g, size_t i) const {
    const size_t state = (g * agg_stride) + i;
    size_t total = slim_distinct_n[state];
    if (state < slim_distinct_overflow.size() &&
        slim_distinct_overflow[state]) {
      total += slim_distinct_overflow[state]->Size();
    }
    return total;
  }

  [[nodiscard]] size_t GroupCount() const {
    return flat_layout ? group_count : group_states.size();
  }
  static std::pair<size_t, size_t> LocateColumnarChunkRow(
      const std::vector<uint32_t>& prefix, uint32_t row_id,
      size_t* hint_ci) noexcept {
    size_t ci = *hint_ci;
    const size_t nchunks = prefix.size() - 1;
    if (ci >= nchunks || row_id < prefix[ci] || row_id >= prefix[ci + 1]) {
      if (ci + 1 < nchunks && row_id >= prefix[ci + 1] &&
          row_id < prefix[ci + 2]) {
        ++ci;
      } else {
        const auto it =
            std::upper_bound(prefix.begin() + 1, prefix.end(), row_id);
        ci = static_cast<size_t>((it - prefix.begin()) - 1);
      }
      *hint_ci = ci;
    }
    return {ci, static_cast<size_t>(row_id - prefix[ci])};
  }

  [[nodiscard]] const Value& GroupKey(size_t group, size_t k) const {
    if (borrowed_key_offsets != nullptr) {
      return (*borrowed_key_rows[group])[(*borrowed_key_offsets)[k]];
    }
    if (flat_layout) {
      return flat_group_keys[(group * key_stride) + k];
    }
    return group_keys[group][k];
  }

  void ReadGroupKey(size_t group, size_t k, Value* dst,
                    size_t* chunk_hint = nullptr) const {
    if (borrowed_columnar != nullptr) {
      size_t local_hint = chunk_hint != nullptr ? *chunk_hint : 0;
      const auto [ci, lr] = LocateColumnarChunkRow(
          *borrowed_chunk_prefix, borrowed_key_row_ids[group], &local_hint);
      if (chunk_hint != nullptr) {
        *chunk_hint = local_hint;
      }
      borrowed_columnar->chunks[ci]
          .columns[(*borrowed_key_offsets)[k]]
          .WriteValueInto(lr, dst);
      return;
    }
    *dst = GroupKey(group, k);
  }

  void ReadGroupKeyOwned(size_t group, size_t k, Value* dst,
                         size_t* chunk_hint = nullptr) const {
    ReadGroupKey(group, k, dst, chunk_hint);
    if (borrowed_columnar != nullptr && !dst->IsNull() &&
        dst->type == ValueType::kVarChar && dst->owned_data.empty()) {
      dst->owned_data.assign(dst->value.varchar_value);
      dst->value.varchar_value = dst->owned_data;
    }
  }
  [[nodiscard]] const AggregateAccumulator& State(size_t group,
                                                  size_t i) const {
    if (flat_layout) {
      return chunked_states.State(group, i);
    }
    return group_states[group][i];
  }
  AggregateAccumulator& State(size_t group, size_t i) {
    if (flat_layout) {
      return chunked_states.State(group, i);
    }
    return group_states[group][i];
  }

  void ReserveExpectedKeys(size_t expected_groups) {
    if (int_key || expected_groups == 0) {
      return;
    }
    size_t cap = key_index.empty() ? 16 : key_index.size();
    while (expected_groups <= (std::numeric_limits<size_t>::max() - 1) / 10 &&
           (expected_groups + 1) * 10 > cap * 7) {
      cap *= 2;
    }
    if (cap > key_index.size()) {
      ReserveKeyIndex(cap);
    }
  }

  void ReserveGroups(size_t target_groups) {
    if (target_groups == 0) {
      return;
    }
    if (int_key) {
      int_index.Reserve(target_groups);
    }
    if (flat_layout) {
      if (borrowed_columnar != nullptr) {
        borrowed_key_row_ids.reserve(target_groups);
      } else if (borrowed_key_offsets != nullptr) {
        borrowed_key_rows.reserve(target_groups);
      } else {
        flat_group_keys.reserve(target_groups * key_stride);
      }
      if (slim) {
        const size_t stride = agg_stride;
        const size_t have_groups = slim_counts.size() / stride;
        if (target_groups > have_groups) {
          const size_t cells = target_groups * stride;
          slim_counts.resize(cells, 0);
          slim_int_totals.resize(cells, 0);
          slim_double_totals.resize(cells, 0.0);
          slim_flags.resize(cells, 0);
          if (std::ranges::any_of(*slim_kinds, [](SlimAggKind kind) {
                return IsSlimDistinctKind(kind);
              })) {
            slim_distinct.resize(cells * kSlimDistinctSlots, 0);
            slim_distinct_n.resize(cells, 0);
          }
        }
      }
    }
  }

  void AppendBorrowedSlimGroup(const Row* row) {
    borrowed_key_rows.push_back(row);
    ReserveSlimBlock();
    ++group_count;
  }

  void AppendBorrowedSlimGroupRowId(uint32_t row_id) {
    borrowed_key_row_ids.push_back(row_id);
    ReserveSlimBlock();
    ++group_count;
  }

  void AppendSlimOnlyGroup() {
    ReserveSlimBlock();
    ++group_count;
  }

  void AppendNonSlimGroup(
      const std::vector<Value>& canonical_keys, size_t aggregate_count,
      const std::vector<const AggregateExpression*>& aggregates) {
    AppendGroup(canonical_keys, aggregate_count, aggregates);
  }

 private:
  // Slim group state grows in blocks of groups: one zeroing resize per block
  // instead of one tail-insert per group.  Multi-million-group folds paid a
  // vector::_M_fill_insert call (plus its capacity dance) per new group per
  // array before, which callgrind put at ~7% of the fold.  Capacity is
  // derived from the arrays themselves (not a separate counter), so the
  // merge path's std::move adoption carries the padding along for free.
  static constexpr size_t kSlimGroupBlock = 4096;
  void ReserveSlimBlock() {
    const size_t stride = agg_stride;
    const size_t have_groups = slim_counts.size() / stride;
    if (group_count < have_groups) {
      return;
    }
    const size_t groups = have_groups + kSlimGroupBlock;
    const size_t cells = groups * stride;
    slim_counts.resize(cells, 0);
    slim_int_totals.resize(cells, 0);
    slim_double_totals.resize(cells, 0.0);
    slim_flags.resize(cells, 0);
    if (std::ranges::any_of(*slim_kinds, [](SlimAggKind kind) {
          return IsSlimDistinctKind(kind);
        })) {
      slim_distinct.resize(cells * kSlimDistinctSlots, 0);
      slim_distinct_n.resize(cells, 0);
    }
  }

  void AppendGroup(const std::vector<Value>& canonical_keys,
                   size_t aggregate_count,
                   const std::vector<const AggregateExpression*>& aggregates) {
    if (int_key && canonical_keys.size() == 1 && !canonical_keys[0].IsNull()) {
      const int64_t k = canonical_keys[0].value.int_value;
      min_int_key = std::min(min_int_key, k);
      max_int_key = std::max(max_int_key, k);
    } else {
      has_non_int_key = true;
    }
    if (flat_layout) {
      flat_group_keys.insert(flat_group_keys.end(), canonical_keys.begin(),
                             canonical_keys.end());
      if (!slim && aggregate_count != 0) {
        chunked_states.AppendGroup(aggregates, shapes);
      }
    } else {
      group_keys.push_back(canonical_keys);
      group_states.emplace_back();
      auto& states = group_states.back();
      states.reserve(aggregate_count);
      for (size_t i = 0; i < aggregate_count; ++i) {
        if (shapes != nullptr) {
          states.emplace_back(aggregates[i], (*shapes)[i]);
        } else {
          states.emplace_back(aggregates[i]);
        }
      }
    }
    if (slim) {
      ReserveSlimBlock();
    }
    ++group_count;
  }

 public:
  bool TryPackCanonicalKeys(const std::vector<Value>& canonical_keys,
                            int64_t* raw_key) const {
    if (int_key) {
      if (canonical_keys.empty() || canonical_keys[0].IsNull()) {
        return false;
      }
      *raw_key = canonical_keys[0].value.int_value;
      return true;
    }
    if (!packed_varchar_key || canonical_keys.empty() ||
        canonical_keys.size() > 2) {
      return false;
    }
    uint64_t packed = 0;
    for (size_t k = 0; k < canonical_keys.size(); ++k) {
      const Value& key = canonical_keys[k];
      if (key.IsNull() || key.type != ValueType::kVarChar ||
          key.IsCaseInsensitive()) {
        return false;
      }
      uint32_t part = 0;
      if (!TryPackShortVarcharSpan(key.value.varchar_value, &part)) {
        return false;
      }
      packed |= static_cast<uint64_t>(part) << (k * 32);
    }
    *raw_key = static_cast<int64_t>(packed);
    return true;
  }

  size_t FindOrInsert(const std::vector<Value>& canonical_keys, int64_t raw_key,
                      size_t aggregate_count,
                      const std::vector<const AggregateExpression*>& aggregates,
                      bool* inserted) {
    // A NULL key must stay its own group: the raw int64 slot cannot encode
    // null-ness (NULL's bit pattern collides with 0), so NULL keys always go
    // through the canonical row index even on the int_key fast path.
    int64_t packed_key = raw_key;
    const bool use_int_key =
        (int_key && !canonical_keys.empty() && !canonical_keys[0].IsNull()) ||
        (packed_varchar_key &&
         TryPackCanonicalKeys(canonical_keys, &packed_key));
    if (use_int_key) {
      bool emplaced = false;
      size_t* slot = int_index.TryEmplace(packed_key, &emplaced);
      *inserted = emplaced;
      if (emplaced) {
        *slot = GroupCount();
        AppendGroup(canonical_keys, aggregate_count, aggregates);
      }
      return *slot;
    }
    if ((indexed_groups + 1) * 10 > key_index.size() * 7) {
      ReserveKeyIndex(key_index.empty() ? 16 : key_index.size() * 2);
    }
    uint64_t mixed = GroupKeyTransparentHash{}(canonical_keys);
    mixed = (mixed ^ (mixed >> 30)) * 0xbf58476d1ce4e5b9ULL;
    mixed = (mixed ^ (mixed >> 27)) * 0x94d049bb133111ebULL;
    const auto hash = static_cast<size_t>(mixed ^ (mixed >> 31));
    const size_t mask = key_index.size() - 1;
    size_t slot = hash & mask;
    while (key_index[slot].group != SIZE_MAX) {
      const auto& candidate = key_index[slot];
      if (candidate.hash == hash) {
        bool equal = true;
        for (size_t k = 0; k < canonical_keys.size(); ++k) {
          if (!DistinctValueEqual{}(canonical_keys[k],
                                    GroupKey(candidate.group, k))) {
            equal = false;
            break;
          }
        }
        if (equal) {
          *inserted = false;
          return candidate.group;
        }
      }
      slot = (slot + 1) & mask;
    }
    const size_t group = GroupCount();
    AppendGroup(canonical_keys, aggregate_count, aggregates);
    key_index[slot] = KeySlot{.hash = hash, .group = group};
    ++indexed_groups;
    *inserted = true;
    return group;
  }

  static size_t HashRowKey(const Row& row, const std::vector<slot_t>& offsets) {
    uint64_t mixed = 0x9e3779b97f4a7c15ULL;
    for (const slot_t slot : offsets) {
      mixed = (mixed << 7) | (mixed >> 57);
      mixed ^= std::hash<Value>{}(row[slot]);
      mixed *= 0x100000001b3ULL;
    }
    mixed = (mixed ^ (mixed >> 30)) * 0xbf58476d1ce4e5b9ULL;
    mixed = (mixed ^ (mixed >> 27)) * 0x94d049bb133111ebULL;
    return static_cast<size_t>(mixed ^ (mixed >> 31));
  }

  // Resident inputs remain alive throughout the fold. Probe their key cells
  // directly and only own/canonicalize a tuple when it creates a group.
  size_t FindOrInsertRow(
      const Row& row, const std::vector<slot_t>& offsets,
      const std::vector<const AggregateExpression*>& aggregates,
      std::vector<Value>* scratch, bool* inserted,
      std::optional<size_t> precomputed_hash = std::nullopt) {
    const auto append_row_group = [&] {
      borrowed_key_rows.push_back(&row);
      if (slim) {
        ReserveSlimBlock();
      } else if (!aggregates.empty()) {
        chunked_states.AppendGroup(aggregates, shapes);
      }
      ++group_count;
    };
    const auto copy_keys = [&] {
      scratch->clear();
      for (const slot_t slot : offsets) {
        scratch->push_back(CanonicalDistinctValue(row[slot]));
      }
    };
    if (borrowed_key_offsets != nullptr && int_key &&
        !row[offsets[0]].IsNull()) {
      size_t* slot =
          int_index.TryEmplace(row[offsets[0]].value.int_value, inserted);
      if (*inserted) {
        *slot = GroupCount();
        append_row_group();
      }
      return *slot;
    }
    if (borrowed_key_offsets == nullptr &&
        (int_key || std::ranges::any_of(offsets, [&](slot_t slot) {
           return row[slot].IsCaseInsensitive();
         }))) {
      copy_keys();
      return FindOrInsert(*scratch,
                          int_key && !scratch->front().IsNull()
                              ? scratch->front().value.int_value
                              : 0,
                          aggregates.size(), aggregates, inserted);
    }
    if ((indexed_groups + 1) * 10 > key_index.size() * 7) {
      ReserveKeyIndex(key_index.empty() ? 16 : key_index.size() * 2);
    }
    const size_t hash =
        precomputed_hash ? *precomputed_hash : HashRowKey(row, offsets);
    const size_t mask = key_index.size() - 1;
    size_t slot = hash & mask;
    while (key_index[slot].group != SIZE_MAX) {
      const auto& candidate = key_index[slot];
      bool equal = candidate.hash == hash;
      for (size_t k = 0; equal && k < offsets.size(); ++k) {
        equal =
            DistinctValueEqual{}(row[offsets[k]], GroupKey(candidate.group, k));
      }
      if (equal) {
        *inserted = false;
        return candidate.group;
      }
      slot = (slot + 1) & mask;
    }
    const size_t group = GroupCount();
    if (borrowed_key_offsets != nullptr) {
      append_row_group();
    } else {
      copy_keys();
      AppendGroup(*scratch, aggregates.size(), aggregates);
    }
    key_index[slot] = KeySlot{.hash = hash, .group = group};
    ++indexed_groups;
    *inserted = true;
    return group;
  }

  // Raw-slot access for the arena fold fast path: the caller holds the raw
  // int64 key, probes it directly, and builds canonical keys only on first
  // insert.  Semantics identical to FindOrInsert's int-key branch (NULL keys
  // never reach here; callers fall back to the Row path for those).
  size_t* RawIntSlot(int64_t raw_key, bool* emplaced) {
    return int_index.TryEmplace(raw_key, emplaced);
  }
  // Warms the group slot for a key that FindOrInsert/RawIntSlot will probe a
  // few rows later; the intervening filter work hides the line fill.
  void PrefetchGroupSlot(int64_t raw_key) const { int_index.Prefetch(raw_key); }
  size_t ClaimSlotForNewGroup(
      size_t* slot, const std::vector<Value>& canonical_keys,
      size_t aggregate_count,
      const std::vector<const AggregateExpression*>& aggregates) {
    *slot = GroupCount();
    AppendGroup(canonical_keys, aggregate_count, aggregates);
    return *slot;
  }

  size_t AppendMonotonicIntGroup(int64_t raw_key, ValueType key_type,
                                 bool is_unsigned) {
    min_int_key = std::min(min_int_key, raw_key);
    max_int_key = std::max(max_int_key, raw_key);
    Value kv;
    kv.type = key_type;
    kv.value.int_value = raw_key;
    if (key_type == ValueType::kInt64 && is_unsigned) {
      kv.SetUnsigned();
    }
    flat_group_keys.push_back(kv);
    ReserveSlimBlock();
    return group_count++;
  }

  void ResetMonotonicIntGroup(size_t g, int64_t new_raw_key, ValueType key_type,
                              bool is_unsigned) {
    min_int_key = std::min(min_int_key, new_raw_key);
    max_int_key = std::max(max_int_key, new_raw_key);
    Value kv;
    kv.type = key_type;
    kv.value.int_value = new_raw_key;
    if (key_type == ValueType::kInt64 && is_unsigned) {
      kv.SetUnsigned();
    }
    flat_group_keys[g * key_stride] = kv;
    const size_t base = g * agg_stride;
    for (size_t i = 0; i < agg_stride; ++i) {
      const size_t st = base + i;
      slim_counts[st] = 0;
      slim_int_totals[st] = 0;
      slim_double_totals[st] = 0.0;
      slim_flags[st] = 0;
      if (st < slim_distinct_n.size()) {
        slim_distinct_n[st] = 0;
      }
      if (st < slim_distinct_overflow.size() && slim_distinct_overflow[st]) {
        slim_distinct_overflow[st]->Clear();
      }
    }
  }
};

// SQL DISTINCT puts all NaNs in one peer class and folds signed zeros.
// Other DOUBLE values can use their exact representation in the integer set.
inline int64_t CanonicalDistinctDoubleBits(double value) {
  if (value == 0.0) {
    return 0;
  }
  if (std::isnan(value)) {
    // Fixed canonical NaN peer key: must match CanonicalDistinctIntKey /
    // CanonicalDoubleBits in typed_stream_aggregate_partitioned.cpp so the
    // slim and partitioned COUNT(DISTINCT double) paths share one key space
    // on every platform (quiet_NaN()'s bit pattern is implementation-defined).
    return int64_t{0x7ff8000000000000LL};
  }
  return std::bit_cast<int64_t>(value);
}

// One (group, aggregate) slim-state update for an already-resolved input
// value.  Shared by the Row-based accumulate loop and the cached-arena fold
// fast path so the two can never drift; false (with *error set) on a raise.
// A null `value` is the COUNT(*) sentinel and always counts.
inline bool ApplySlimValue(TypedWorkerGroups* local, size_t group_index,
                           size_t i, SlimAggKind kind, const Value* value,
                           Status* error) {
  if (kind == SlimAggKind::kCount) {
    // COUNT(*): value == nullptr counts every surviving row; COUNT(expr)
    // skips NULLs exactly like the streaming accumulator.
    if (value == nullptr || !value->IsNull()) {
      ++local->SlimCount(group_index, i);
    }
    return true;
  }
  if (value == nullptr || value->IsNull()) {
    return true;
  }
  if (kind == SlimAggKind::kCountDistinctDouble) {
    if (value->type != ValueType::kDouble) {
      *error = StatusError(StatusCode::kInvalidArgument,
                           "slim COUNT(DISTINCT) input type mismatch");
      return false;
    }
    static_cast<void>(local->SlimInsertDistinct(
        group_index, i,
        CanonicalDistinctDoubleBits(value->value.double_value)));
    return true;
  }
  if (kind == SlimAggKind::kCountDistinctInt64) {
    // NULL inputs never enter the set (mirrored by the skip above); the
    // running count stays 0 because the finish derives the cardinality from
    // the inline occupancy and the overflow set.
    if (value->type == ValueType::kInt64 || value->type == ValueType::kDate) {
      static_cast<void>(
          local->SlimInsertDistinct(group_index, i, value->value.int_value));
      return true;
    }
    *error = StatusError(StatusCode::kInvalidArgument,
                         "slim COUNT(DISTINCT) input type mismatch");
    return false;
  }
  switch (kind) {
    case SlimAggKind::kSumInt64: {
      if (value->type != ValueType::kInt64 || value->IsUnsigned()) {
        *error = StatusError(StatusCode::kInvalidArgument,
                             "slim SUM input type mismatch");
        return false;
      }
      int64_t updated = 0;
      if (__builtin_add_overflow(local->SlimIntTotal(group_index, i),
                                 value->value.int_value, &updated)) {
        *error =
            StatusError(StatusCode::kIsInfinity, "integer overflow in SUM");
        return false;
      }
      local->SlimIntTotal(group_index, i) = updated;
      ++local->SlimCount(group_index, i);
      return true;
    }
    case SlimAggKind::kSumDouble: {
      if (!local->mixed_numeric_sums.empty() && local->mixed_numeric_sums[i]) {
        if (value->type == ValueType::kInt64 && !value->IsUnsigned()) {
          int64_t next = 0;
          if (__builtin_add_overflow(local->SlimIntTotal(group_index, i),
                                     value->value.int_value, &next)) {
            *error =
                StatusError(StatusCode::kIsInfinity, "integer overflow in SUM");
            return false;
          }
          local->SlimIntTotal(group_index, i) = next;
          ++local->SlimCount(group_index, i);
          return true;
        }
        local->SlimFlags(group_index, i) |= 2;
      }
      // A DOUBLE-declared expression can carry INT64 runtime values (for
      // example CASE WHEN p THEN 1 ELSE 1.5 END stored in a columnar
      // fallback). The generic accumulator promotes int to double for SUM and
      // kAvgDouble already does the same, so accept them here instead of
      // failing the whole query with an input type mismatch.
      double addend = 0.0;
      if (value->type == ValueType::kDouble) {
        addend = value->value.double_value;
      } else if (value->type == ValueType::kInt64) {
        addend = value->IsUnsigned()
                     ? static_cast<double>(
                           static_cast<uint64_t>(value->value.int_value))
                     : static_cast<double>(value->value.int_value);
      } else {
        *error = StatusError(StatusCode::kInvalidArgument,
                             "slim SUM input type mismatch");
        return false;
      }
      if (std::isinf(addend)) {
        local->SlimFlags(group_index, i) |= 1;
      }
      local->SlimDoubleTotal(group_index, i) += addend;
      ++local->SlimCount(group_index, i);
      return true;
    }
    case SlimAggKind::kAvgDouble: {
      // AVG(INT64) sums through a double intermediate, so INT64
      // overflow never applies here (mirrors TryApplyCore).
      double addend = 0.0;
      if (value->type == ValueType::kDouble) {
        addend = value->value.double_value;
      } else if (value->type == ValueType::kInt64) {
        addend = value->IsUnsigned()
                     ? static_cast<double>(
                           static_cast<uint64_t>(value->value.int_value))
                     : static_cast<double>(value->value.int_value);
      } else {
        *error = StatusError(StatusCode::kInvalidArgument,
                             "slim AVG input type mismatch");
        return false;
      }
      local->SlimDoubleTotal(group_index, i) += addend;
      ++local->SlimCount(group_index, i);
      return true;
    }
    case SlimAggKind::kMinInt64:
    case SlimAggKind::kMaxInt64: {
      if (value->type != ValueType::kInt64 || value->IsUnsigned()) {
        *error = StatusError(StatusCode::kInvalidArgument,
                             "slim MIN/MAX input type mismatch");
        return false;
      }
      const int64_t v = value->value.int_value;
      int64_t& cur = local->SlimIntTotal(group_index, i);
      int64_t& cnt = local->SlimCount(group_index, i);
      if (cnt == 0 || (kind == SlimAggKind::kMinInt64 ? v < cur : cur < v)) {
        cur = v;
      }
      ++cnt;
      return true;
    }
    case SlimAggKind::kMinDate:
    case SlimAggKind::kMaxDate: {
      if (value->type != ValueType::kDate) {
        *error = StatusError(StatusCode::kInvalidArgument,
                             "slim DATE MIN/MAX input type mismatch");
        return false;
      }
      const int64_t v = value->value.int_value;
      int64_t& cur = local->SlimIntTotal(group_index, i);
      int64_t& cnt = local->SlimCount(group_index, i);
      if (cnt == 0 || (kind == SlimAggKind::kMinDate ? v < cur : cur < v)) {
        cur = v;
      }
      ++cnt;
      return true;
    }
    case SlimAggKind::kMinDouble:
    case SlimAggKind::kMaxDouble: {
      if (value->type != ValueType::kDouble) {
        *error = StatusError(StatusCode::kInvalidArgument,
                             "slim DOUBLE MIN/MAX input type mismatch");
        return false;
      }
      const double v = value->value.double_value;
      if (std::isnan(v)) {
        local->SlimFlags(group_index, i) |= 1;
      }
      double& cur = local->SlimDoubleTotal(group_index, i);
      int64_t& cnt = local->SlimCount(group_index, i);
      if (cnt == 0 || (kind == SlimAggKind::kMinDouble ? v < cur : cur < v)) {
        cur = v;
      }
      ++cnt;
      return true;
    }
    default:
      *error = StatusError(StatusCode::kInvalidArgument,
                           "aggregate has no slim state");
      return false;
  }
}

inline bool ApplySlimCell(TypedWorkerGroups* local, size_t group_index,
                          size_t i, SlimAggKind kind, ValueType cell_type,
                          int64_t cell, bool cell_unsigned, bool* handled,
                          Status* error) {
  *handled = true;
  switch (kind) {
    case SlimAggKind::kCount:
      ++local->SlimCount(group_index, i);
      return true;
    case SlimAggKind::kSumInt64:
      if (cell_type == ValueType::kInt64 && !cell_unsigned) {
        int64_t updated = 0;
        if (__builtin_add_overflow(local->SlimIntTotal(group_index, i), cell,
                                   &updated)) {
          *error =
              StatusError(StatusCode::kIsInfinity, "integer overflow in SUM");
          return false;
        }
        local->SlimIntTotal(group_index, i) = updated;
        ++local->SlimCount(group_index, i);
      } else {
        *handled = false;
      }
      return true;
    case SlimAggKind::kSumDouble:
      if (cell_type == ValueType::kDouble) {
        const auto addend = std::bit_cast<double>(cell);
        if (std::isinf(addend)) {
          local->SlimFlags(group_index, i) |= 1;
        }
        local->SlimDoubleTotal(group_index, i) += addend;
        ++local->SlimCount(group_index, i);
      } else {
        *handled = false;
      }
      return true;
    case SlimAggKind::kAvgDouble:
      if (cell_type == ValueType::kDouble) {
        local->SlimDoubleTotal(group_index, i) += std::bit_cast<double>(cell);
        ++local->SlimCount(group_index, i);
      } else if (cell_type == ValueType::kInt64) {
        local->SlimDoubleTotal(group_index, i) +=
            cell_unsigned ? static_cast<double>(static_cast<uint64_t>(cell))
                          : static_cast<double>(cell);
        ++local->SlimCount(group_index, i);
      } else {
        *handled = false;
      }
      return true;
    case SlimAggKind::kCountDistinctDouble:
      if (cell_type == ValueType::kDouble) {
        static_cast<void>(local->SlimInsertDistinct(
            group_index, i,
            CanonicalDistinctDoubleBits(std::bit_cast<double>(cell))));
      } else {
        *handled = false;
      }
      return true;
    case SlimAggKind::kCountDistinctInt64:
      if ((cell_type == ValueType::kInt64 || cell_type == ValueType::kDate) &&
          !cell_unsigned) {
        static_cast<void>(local->SlimInsertDistinct(group_index, i, cell));
      } else {
        // The raw-bit distinct set cannot tell signed -1 from UINT64_MAX.
        *handled = false;
      }
      return true;
    case SlimAggKind::kMinInt64:
    case SlimAggKind::kMaxInt64:
      if (cell_type == ValueType::kInt64 && !cell_unsigned) {
        int64_t& cur = local->SlimIntTotal(group_index, i);
        int64_t& cnt = local->SlimCount(group_index, i);
        if (cnt == 0 ||
            (kind == SlimAggKind::kMinInt64 ? cell < cur : cur < cell)) {
          cur = cell;
        }
        ++cnt;
      } else {
        *handled = false;
      }
      return true;
    case SlimAggKind::kMinDate:
    case SlimAggKind::kMaxDate:
      if (cell_type == ValueType::kDate) {
        int64_t& cur = local->SlimIntTotal(group_index, i);
        int64_t& cnt = local->SlimCount(group_index, i);
        if (cnt == 0 ||
            (kind == SlimAggKind::kMinDate ? cell < cur : cur < cell)) {
          cur = cell;
        }
        ++cnt;
      } else {
        *handled = false;
      }
      return true;
    case SlimAggKind::kMinDouble:
    case SlimAggKind::kMaxDouble:
      if (cell_type == ValueType::kDouble) {
        const auto dv = std::bit_cast<double>(cell);
        if (std::isnan(dv)) {
          local->SlimFlags(group_index, i) |= 1;
        }
        double& cur = local->SlimDoubleTotal(group_index, i);
        int64_t& cnt = local->SlimCount(group_index, i);
        if (cnt == 0 ||
            (kind == SlimAggKind::kMinDouble ? dv < cur : cur < dv)) {
          cur = dv;
        }
        ++cnt;
      } else {
        *handled = false;
      }
      return true;
    default:
      *handled = false;
      return true;
  }
}

inline bool MergeSlimCell(TypedWorkerGroups* dst, size_t dst_group,
                          const TypedWorkerGroups& src, size_t src_group,
                          size_t i, SlimAggKind kind, Status* error) {
  const int64_t sc = src.SlimCount(src_group, i);
  if (sc == 0) {
    return true;
  }
  switch (kind) {
    case SlimAggKind::kCount:
      dst->SlimCount(dst_group, i) += sc;
      return true;
    case SlimAggKind::kSumInt64: {
      int64_t updated = 0;
      if (__builtin_add_overflow(dst->SlimIntTotal(dst_group, i),
                                 src.SlimIntTotal(src_group, i), &updated)) {
        *error =
            StatusError(StatusCode::kIsInfinity, "integer overflow in SUM");
        return false;
      }
      dst->SlimIntTotal(dst_group, i) = updated;
      dst->SlimCount(dst_group, i) += sc;
      return true;
    }
    case SlimAggKind::kSumDouble: {
      if (!dst->mixed_numeric_sums.empty() && dst->mixed_numeric_sums[i]) {
        int64_t updated = 0;
        if (__builtin_add_overflow(dst->SlimIntTotal(dst_group, i),
                                   src.SlimIntTotal(src_group, i), &updated)) {
          *error =
              StatusError(StatusCode::kIsInfinity, "integer overflow in SUM");
          return false;
        }
        dst->SlimIntTotal(dst_group, i) = updated;
      }
      const double sv = src.SlimDoubleTotal(src_group, i);
      double& dv = dst->SlimDoubleTotal(dst_group, i);
      dv += sv;
      // Partial sums can reach +/-inf from finite inputs; adding opposite
      // signs then yields NaN. Mark it as an overflow so the finalizer raises
      // like the row-order canonical path instead of returning NaN.
      if (std::isnan(dv) && !std::isnan(sv)) {
        dv = std::numeric_limits<double>::infinity();
        dst->SlimFlags(dst_group, i) |= 1;
      }
      dst->SlimFlags(dst_group, i) |= src.SlimFlags(src_group, i);
      dst->SlimCount(dst_group, i) += sc;
      return true;
    }
    case SlimAggKind::kAvgDouble:
      dst->SlimDoubleTotal(dst_group, i) += src.SlimDoubleTotal(src_group, i);
      dst->SlimCount(dst_group, i) += sc;
      return true;
    case SlimAggKind::kMinInt64:
    case SlimAggKind::kMinDate: {
      const int64_t sv = src.SlimIntTotal(src_group, i);
      int64_t& dc = dst->SlimCount(dst_group, i);
      if (dc == 0 || sv < dst->SlimIntTotal(dst_group, i)) {
        dst->SlimIntTotal(dst_group, i) = sv;
      }
      dc += sc;
      return true;
    }
    case SlimAggKind::kMaxInt64:
    case SlimAggKind::kMaxDate: {
      const int64_t sv = src.SlimIntTotal(src_group, i);
      int64_t& dc = dst->SlimCount(dst_group, i);
      if (dc == 0 || dst->SlimIntTotal(dst_group, i) < sv) {
        dst->SlimIntTotal(dst_group, i) = sv;
      }
      dc += sc;
      return true;
    }
    case SlimAggKind::kMinDouble: {
      const double sv = src.SlimDoubleTotal(src_group, i);
      dst->SlimFlags(dst_group, i) |= src.SlimFlags(src_group, i);
      int64_t& dc = dst->SlimCount(dst_group, i);
      if (dc == 0 || sv < dst->SlimDoubleTotal(dst_group, i)) {
        dst->SlimDoubleTotal(dst_group, i) = sv;
      }
      dc += sc;
      return true;
    }
    case SlimAggKind::kMaxDouble: {
      const double sv = src.SlimDoubleTotal(src_group, i);
      dst->SlimFlags(dst_group, i) |= src.SlimFlags(src_group, i);
      int64_t& dc = dst->SlimCount(dst_group, i);
      if (dc == 0 || dst->SlimDoubleTotal(dst_group, i) < sv) {
        dst->SlimDoubleTotal(dst_group, i) = sv;
      }
      dc += sc;
      return true;
    }
    default:
      *error = StatusError(StatusCode::kInvalidArgument,
                           "aggregate has no slim state");
      return false;
  }
}

inline bool FinishSlimCell(const TypedWorkerGroups& groups, size_t group,
                           size_t i, SlimAggKind kind, Value* out,
                           Status* error) {
  const int64_t count = groups.SlimCount(group, i);
  switch (kind) {
    case SlimAggKind::kCount:
      *out = Value(count);
      return true;
    case SlimAggKind::kSumInt64:
      *out = count == 0 ? Value() : Value(groups.SlimIntTotal(group, i));
      return true;
    case SlimAggKind::kSumDouble: {
      if (count == 0) {
        *out = Value();
        return true;
      }
      const bool mixed =
          !groups.mixed_numeric_sums.empty() && groups.mixed_numeric_sums[i];
      if (mixed && (groups.SlimFlags(group, i) & 2) == 0) {
        // T4903 (reviewed): per-group dynamic tagging is the CANONICAL
        // semantics here, not a slim-path artifact: the reference
        // AggregateAccumulator::Finalize (aggregate_accumulator.cpp, kSum
        // arm "if (!total_is_double && total == 0.0)") also returns kInt64
        // for a group that never saw a DOUBLE input. SlimFlags bit2 mirrors
        // total_is_double exactly, and the split is pinned by
        // RelationalRegressionTest.GroupingSetsPreserveDynamicSumTypesAndOverflow.
        *out = Value(groups.SlimIntTotal(group, i));
        return true;
      }
      const double total =
          groups.SlimDoubleTotal(group, i) +
          (mixed ? static_cast<double>(groups.SlimIntTotal(group, i)) : 0.0);
      if ((groups.SlimFlags(group, i) & 1) == 0 && std::isinf(total)) {
        *error = StatusError(StatusCode::kIsInfinity,
                             "SUM overflow: result is infinite");
        return false;
      }
      *out = Value(total);
      return true;
    }
    case SlimAggKind::kAvgDouble:
      *out = count == 0 ? Value()
                        : Value(groups.SlimDoubleTotal(group, i) /
                                static_cast<double>(count));
      return true;
    case SlimAggKind::kCountDistinctDouble:
    case SlimAggKind::kCountDistinctInt64: {
      const size_t total = groups.SlimDistinctTotal(group, i);
      if (total > static_cast<size_t>(std::numeric_limits<int64_t>::max())) {
        *error = StatusError(StatusCode::kIsInfinity,
                             "COUNT(DISTINCT) overflow: result exceeds INT64");
        return false;
      }
      *out = Value(static_cast<int64_t>(total));
      return true;
    }
    case SlimAggKind::kMinInt64:
    case SlimAggKind::kMaxInt64:
      *out = count == 0 ? Value() : Value(groups.SlimIntTotal(group, i));
      return true;
    case SlimAggKind::kMinDate:
    case SlimAggKind::kMaxDate:
      if (count == 0) {
        *out = Value();
      } else {
        Value dv(groups.SlimIntTotal(group, i));
        dv.type = ValueType::kDate;
        *out = dv;
      }
      return true;
    case SlimAggKind::kMinDouble:
    case SlimAggKind::kMaxDouble:
      if (count == 0) {
        *out = Value();
      } else if ((groups.SlimFlags(group, i) & 1) != 0) {
        *out = Value(std::numeric_limits<double>::quiet_NaN());
      } else {
        *out = Value(groups.SlimDoubleTotal(group, i));
      }
      return true;
    default:
      *error = StatusError(StatusCode::kInvalidArgument,
                           "aggregate has no slim state");
      return false;
  }
}

template <typename T>
inline bool ComparePrimitiveHaving(BinaryOperation op, T a, T b) {
  switch (op) {
    case BinaryOperation::kEquals:
      return a == b;
    case BinaryOperation::kNotEquals:
      return a != b;
    case BinaryOperation::kLessThan:
      return a < b;
    case BinaryOperation::kLessThanEquals:
      return a <= b;
    case BinaryOperation::kGreaterThan:
      return a > b;
    case BinaryOperation::kGreaterThanEquals:
      return a >= b;
    default:
      return false;
  }
}

inline bool EvalPositionalHavingValues(const PositionalHaving& clause,
                                       const Value& left,
                                       const Value* right_agg, Status* error) {
  if (!clause.has_constant && !clause.rhs_is_agg) {
    return Truthy(left);
  }
  const Value& right = clause.rhs_is_agg ? *right_agg : clause.constant;
  if (left.IsNull() || right.IsNull()) {
    return false;
  }
  if ((left.type == ValueType::kInt64 || left.type == ValueType::kDate) &&
      left.type == right.type && !left.IsUnsigned() && !right.IsUnsigned() &&
      (clause.op == BinaryOperation::kEquals ||
       clause.op == BinaryOperation::kNotEquals ||
       clause.op == BinaryOperation::kLessThan ||
       clause.op == BinaryOperation::kLessThanEquals ||
       clause.op == BinaryOperation::kGreaterThan ||
       clause.op == BinaryOperation::kGreaterThanEquals)) {
    return ComparePrimitiveHaving(clause.op, left.value.int_value,
                                  right.value.int_value);
  }
  if (left.type == ValueType::kDouble && right.type == ValueType::kDouble &&
      (clause.op == BinaryOperation::kEquals ||
       clause.op == BinaryOperation::kNotEquals ||
       clause.op == BinaryOperation::kLessThan ||
       clause.op == BinaryOperation::kLessThanEquals ||
       clause.op == BinaryOperation::kGreaterThan ||
       clause.op == BinaryOperation::kGreaterThanEquals)) {
    return ComparePrimitiveHaving(clause.op, left.value.double_value,
                                  right.value.double_value);
  }
  StatusOr<Value> compared = TryBinary(clause.op, left, right);
  if (!compared.HasValue()) {
    *error = compared.GetStatus();
    return false;
  }
  return Truthy(compared.MoveValue());
}

// Per-morsel cache-fill buffer owned by a scan worker.  Rows land in the
// worker's vector (published in morsel order later); fixed-width fills also
// append row-major int64 cells + null bytes so the published cache gets its
// int64 arena mirror without per-row work under the publish lock.  The typed
// fill instead decodes straight into `mirror` (same arena layout, no Rows).
struct MorselFillBuffer {
  std::vector<Row> rows;
  size_t arena_stride{0};  // 0 = no arena mirror for this morsel.
  std::vector<int64_t> arena_cells;
  std::vector<uint8_t> arena_nulls;
  // Cells actually written (arena_cells/arena_nulls grow geometrically and
  // are cut to this length at publish, keeping the rows*stride == cells.size
  // invariant while skipping the per-row zeroing resize).
  size_t arena_used{0};
  // Typed-fill mirror (rows without Row objects); published through
  // BaseRelationArena::AppendTyped.
  BaseRelationArena mirror;
};

// Appends a decoded TypedCellBatch range (all columns, kept or filtered rows —
// cache fills are unfiltered) into an arena run: null bytes fold into the
// per-column bitmaps, cells copy verbatim, varchar bytes extend the run's
// byte arenas with offset-adjusted end marks.
inline void AppendBatchToArena(const TypedCellBatch& batch, size_t first_row,
                               size_t run_rows, BaseRelationArena* arena) {
  if (run_rows == 0) {
    return;
  }
  const size_t stride = arena->stride;
  const size_t first = arena->rows;
  arena->cells.insert(
      arena->cells.end(),
      batch.cells.begin() + static_cast<std::ptrdiff_t>(first_row * stride),
      batch.cells.begin() +
          static_cast<std::ptrdiff_t>((first_row + run_rows) * stride));
  for (size_t c = 0; c < stride; ++c) {
    auto& bits = arena->null_bits[c];
    bits.resize(((first + run_rows - 1) >> 6) + 1, 0);
    for (size_t r = 0; r < run_rows; ++r) {
      if (batch.nulls[((first_row + r) * stride) + c] != 0) {
        const size_t row = first + r;
        bits[row >> 6] |= uint64_t{1} << (row & 63);
      }
    }
  }
  size_t varchar_slot = 0;
  for (size_t c = 0; c < stride; ++c) {
    if (arena->slot_types[c] != ValueType::kVarChar) {
      continue;
    }
    BaseRelationArena::VarcharRun& run = arena->varchars[varchar_slot];
    const std::vector<uint32_t>& batch_ends = batch.varchar_ends[varchar_slot];
    const size_t run_bytes = run.bytes.size();
    const uint32_t batch_prefix =
        first_row == 0 ? 0U : batch_ends[first_row - 1];
    run.bytes.insert(run.bytes.end(),
                     batch.varchar_bytes[varchar_slot].begin() + batch_prefix,
                     batch.varchar_bytes[varchar_slot].begin() +
                         batch_ends[first_row + run_rows - 1]);
    run.ends.reserve(run.ends.size() + run_rows);
    for (size_t r = 0; r < run_rows; ++r) {
      run.AppendEnd(run_bytes, batch_ends[first_row + r] - batch_prefix);
    }
    ++varchar_slot;
  }
  arena->rows += run_rows;
}

// Typed cell value for the arena fold: the payload layout follows the slot
// type (DOUBLE bit-casts through the int64 cell; VARCHAR slots never reach
// this helper on the raw-cell paths because their folds need byte strings).
inline Value ArenaCellValue(const BaseRelationArena& arena,
                            const int64_t* cells, size_t column,
                            const Schema& schema) {
  Value value;
  const ValueType type = arena.slot_types.size() == arena.stride
                             ? arena.slot_types[column]
                             : schema.GetColumn(column).Type();
  value.type = type;
  if (type == ValueType::kDouble) {
    value.value.double_value = std::bit_cast<double>(cells[column]);
  } else {
    value.value.int_value = cells[column];
  }
  if (type == ValueType::kInt64 && schema.GetColumn(column).IsUnsigned()) {
    value.SetUnsigned();
  }
  return value;
}

// MaterializeArenaRange and MaterializeCacheRowsIfNeeded live in
// subquery_runtime.cpp next to the arena they serve.

// Appends one decoded row to the morsel fill buffer (Row + optional arena
// mirror).  `row` must not be touched afterwards when !fill_copy (the row
// moves).
template <typename RowLike>
void AppendFillRow(RowLike&& row, MorselFillBuffer* fill, bool fill_copy) {
  if (fill->arena_stride != 0) {
    const size_t width = fill->arena_stride;
    const size_t base = fill->arena_used;
    if (base + width > fill->arena_cells.size()) {
      const size_t grown = std::max(base + width, fill->arena_cells.size() * 2);
      fill->arena_cells.resize(grown, 0);
      fill->arena_nulls.resize(grown, 0);
    }
    for (size_t c = 0; c < width && c < row.values_.size(); ++c) {
      const Value& value = row.values_[c];
      fill->arena_cells[base + c] =
          value.IsNull()
              ? 0
              : (value.type == ValueType::kDouble
                     ? std::bit_cast<int64_t>(value.value.double_value)
                     : value.value.int_value);
      fill->arena_nulls[base + c] = value.IsNull() ? 1 : 0;
    }
    fill->arena_used = base + width;
  }
  if (fill_copy) {
    fill->rows.emplace_back(row);
  } else {
    fill->rows.push_back(std::forward<RowLike>(row));
  }
}

inline StatusOr<std::vector<Value>> EvaluateDependentKeys(
    const TypedAggregationPlan& plan, const TypedWorkerGroups& groups,
    size_t group, size_t* chunk_hint = nullptr) {
  std::vector<Value> results;
  if (plan.dependent_key_programs.empty()) {
    return results;
  }
  std::vector<Value> keys;
  keys.reserve(plan.key_offsets.size());
  size_t local_hint = chunk_hint != nullptr ? *chunk_hint : 0;
  for (size_t k = 0; k < plan.key_offsets.size(); ++k) {
    groups.ReadGroupKeyOwned(group, k, &keys.emplace_back(), &local_hint);
  }
  if (chunk_hint != nullptr) {
    *chunk_hint = local_hint;
  }
  const Row row(std::move(keys));
  results.reserve(plan.dependent_key_programs.size());
  for (const auto& program : plan.dependent_key_programs) {
    ASSIGN_OR_RETURN(Value, value, TryEvaluateRowAuto(program, row));
    results.push_back(std::move(value));
  }
  return results;
}

struct DictionaryCaseInput {
  std::vector<TypedFilterCompare> conditions;
  Value otherwise;
};

std::shared_ptr<const LSTableStore> SnapshotColumnStore(
    TransactionContext& context, const Table& table,
    const std::vector<slot_t>& projection, size_t column_count,
    bool allow_lazy_build = true);

StatusOr<std::optional<Relation>> TryUniqueGroupLimit(
    const LSTableStore& store, const TypedAggregationPlan& plan,
    const SelectStatement& statement, const std::vector<slot_t>& projection,
    const Schema& schema, ExecutionRuntime* runtime);

StatusOr<std::optional<Relation>> TryPartitionedNumericTopN(
    const LSTableStore& store, const TypedAggregationPlan& plan,
    const SelectStatement& statement, const std::vector<slot_t>& projection,
    const Schema& schema, ExecutionRuntime* runtime);

StatusOr<std::optional<Relation>> TryDictionaryAggregate(
    const LSTableStore& store, const TypedAggregationPlan& plan,
    const SelectStatement& statement, const std::vector<slot_t>& projection,
    const Schema& schema, ExecutionRuntime* runtime,
    const std::function<StatusOr<Value>(size_t)>* transform_key = nullptr,
    const UtcTimestampTruncUnit* temporal_unit = nullptr);

StatusOr<std::optional<Relation>> TryCompositeDictionaryAggregate(
    const LSTableStore& store, const TypedAggregationPlan& plan,
    const SelectStatement& statement, const std::vector<slot_t>& projection,
    const Schema& schema, ExecutionRuntime* runtime,
    const std::vector<std::function<StatusOr<Value>(size_t)>>* transforms =
        nullptr,
    const std::vector<std::optional<DictionaryCaseInput>>* cases = nullptr,
    const std::vector<std::optional<UtcTimestampTruncUnit>>* extracts = nullptr,
    bool require_selective = false);

inline uint32_t ReadDictionaryCode(const LSTableColumnDictionary& dictionary,
                                   size_t row) noexcept {
  // Route through DictionaryCodeAt so the active/packed/wide streams all
  // honor the zero-copy *_codes_view accessors and the row bound check;
  // reading the owned vectors directly would bypass mmap-backed stores.
  return dictionary.DictionaryCodeAt(row);
}

struct EncodedDictionaryPredicate {
  const LSTableColumnDictionary* dictionary;
  size_t null_code;
  std::vector<uint8_t> allowed;
  std::optional<bool> constant;
  double domain_fraction;

  [[nodiscard]] bool Matches(size_t row) const noexcept {
    return constant
               ? *constant
               : allowed[std::min<size_t>(ReadDictionaryCode(*dictionary, row),
                                          null_code)] != 0;
  }
};

// These masks contain only non-throwing comparisons. Remove proven true
// predicates and try the narrowest accepted dictionary domain first. This is
// a selectivity estimate, never a proof that a particular row passes.
inline void OrderDictionaryPredicates(
    std::vector<EncodedDictionaryPredicate>& filters) {
  std::erase_if(filters, [](const auto& filter) {
    return filter.constant.value_or(false);
  });
  std::stable_sort(filters.begin(), filters.end(),
                   [](const auto& a, const auto& b) {
                     return a.domain_fraction < b.domain_fraction;
                   });
}

template <typename Predicate>
std::optional<EncodedDictionaryPredicate> CompileDictionaryMask(
    const LSTableStore& store, slot_t column, bool string_payload,
    Predicate&& predicate, QueryMemoryCharge& charge) {
  const auto& dictionary = store.dictionaries[column];
  if ((!dictionary.active && dictionary.wide_code_bytes != 2 &&
       dictionary.wide_code_bytes != 4) ||
      (string_payload ? dictionary.value_type != ValueType::kVarChar
                      : (dictionary.value_type != ValueType::kInt64 &&
                         dictionary.value_type != ValueType::kDate)) ||
      dictionary.DictionaryCodeCount() != store.total_rows) {
    return std::nullopt;
  }
  const size_t domain = string_payload ? dictionary.StringEntryCount()
                                       : dictionary.IntEntryCount();
  // domain is a std::vector size and cannot be SIZE_MAX; only the
  // reservation check is meaningful here.
  if (!QueryMemoryBudget::Global().CanReserve(domain + 1)) {
    return std::nullopt;
  }
  charge.Add(domain + 1);
  EncodedDictionaryPredicate result{
      .dictionary = &dictionary,
      .null_code = domain,
      .allowed = std::vector<uint8_t>(domain + 1, 0),
      .constant = std::nullopt,
      .domain_fraction = 0};
  // Mask predicates only read immutable dictionary entries and constants.
  // Large dictionaries otherwise serialize LIKE preparation before the
  // parallel scan even starts. Each worker owns a disjoint code range.
  const size_t workers =
      ParallelWorkerCount(std::max<size_t>(1, domain / 8192));
  std::vector<size_t> accepted_by_worker(workers, 0);
  const Status status = RunParallelTasks(workers, [&](size_t worker) {
    const size_t begin =
        ((domain / workers) * worker) + std::min(domain % workers, worker);
    const size_t end = ((domain / workers) * (worker + 1)) +
                       std::min(domain % workers, worker + 1);
    size_t accepted = 0;
    for (size_t code = begin; code < end; ++code) {
      const bool pass = predicate(dictionary, code);
      result.allowed[code] = static_cast<uint8_t>(pass);
      accepted += pass ? 1 : 0;
    }
    accepted_by_worker[worker] = accepted;
  });
  if (status != Status::kSuccess) {
    return std::nullopt;
  }
  const size_t accepted = std::accumulate(accepted_by_worker.begin(),
                                          accepted_by_worker.end(), size_t{0});
  result.domain_fraction =
      static_cast<double>(accepted) / static_cast<double>(domain + 1);
  if (accepted == 0) {
    result.constant = false;
  } else if (accepted == domain && store.column_has_null[column] == 0) {
    result.constant = true;
  }
  return result;
}

inline std::optional<EncodedDictionaryPredicate> CompileDictionaryPredicate(
    const LSTableStore& store, const TypedFilterCompare& filter,
    const std::vector<slot_t>& projection, QueryMemoryCharge& charge) {
  if (!filter.constant_payload || filter.is_double) {
    return std::nullopt;
  }
  const slot_t column =
      projection.empty() ? filter.left : projection[filter.left];
  const auto matches = [](auto a, auto b, BinaryOperation op) {
    switch (op) {
      case BinaryOperation::kEquals:
        return a == b;
      case BinaryOperation::kNotEquals:
        return a != b;
      case BinaryOperation::kLessThan:
        return a < b;
      case BinaryOperation::kLessThanEquals:
        return a <= b;
      case BinaryOperation::kGreaterThan:
        return a > b;
      default:
        return a >= b;
    }
  };
  return CompileDictionaryMask(
      store, column, filter.string_payload,
      [&](const LSTableColumnDictionary& dictionary, size_t code) {
        return filter.string_payload
                   ? MatchTypedStringCompare(dictionary.StringEntryAt(code),
                                             filter)
                   : matches(dictionary.IntEntryAt(code), filter.int_constant,
                             filter.op);
      },
      charge);
}

inline std::optional<EncodedDictionaryPredicate> CompileDictionaryInList(
    const LSTableStore& store, const TypedInListCompare& filter,
    const std::vector<slot_t>& projection, QueryMemoryCharge& charge) {
  if (filter.is_double || filter.prefix_len != 0) {
    return std::nullopt;
  }
  const slot_t column =
      projection.empty() ? filter.column : projection[filter.column];
  // Sort the IN list once so each dictionary code costs O(log |IN|) instead of
  // O(|IN|); the mask is built before the scan starts, so a linear probe per
  // code made this O(domain x |IN|).  The sorted copies are read-only and safe
  // to share across the mask workers.
  std::vector<std::string_view> sorted_strings;
  std::vector<int64_t> sorted_ints;
  if (filter.is_string) {
    sorted_strings.assign(filter.strings.begin(), filter.strings.end());
    std::sort(sorted_strings.begin(), sorted_strings.end());
  } else {
    sorted_ints.assign(filter.int_values.begin(), filter.int_values.end());
    std::sort(sorted_ints.begin(), sorted_ints.end());
  }
  return CompileDictionaryMask(
      store, column, filter.is_string,
      [&](const LSTableColumnDictionary& dictionary, size_t code) {
        return filter.is_string
                   ? std::binary_search(sorted_strings.begin(),
                                        sorted_strings.end(),
                                        dictionary.StringEntryAt(code))
                   : std::binary_search(sorted_ints.begin(), sorted_ints.end(),
                                        dictionary.IntEntryAt(code));
      },
      charge);
}

}  // namespace tinylamb::relational_detail
