/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/detail/typed_stream_aggregate.hpp"

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
#include <limits>
#include <memory>
#include <mutex>
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
#include "common/query_parallelism.hpp"
#include "common/status_or.hpp"
#include "common/string_util.hpp"
#include "database/transaction_context.hpp"
#include "executor/aggregation.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/detail/flat_int_key_map.hpp"
#include "executor/detail/planning_heuristics.hpp"
#include "executor/detail/relation.hpp"
#include "executor/detail/scan_filter.hpp"
#include "executor/detail/subquery_runtime.hpp"
#include "executor/detail/window_eval.hpp"
#include "executor/query_memory.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/binary_expression.hpp"
#include "expression/bytecode.hpp"
#include "expression/case_expression.hpp"
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

// Group states local to one worker (or to the merged result on the main
// thread).  Single INT64/DATE key statements index groups by the raw int
// value; everything else keys a canonical Value row.
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

 private:
  class Chunk {
   public:
    explicit Chunk(size_t capacity)
        : storage_(new std::byte[capacity * sizeof(AggregateAccumulator)]) {}
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
          storage_.get() + (index * sizeof(AggregateAccumulator))));
    }
    AggregateAccumulator& ConstructAt(const AggregateExpression& aggregate) {
      auto* placed =  // NOLINT(cppcoreguidelines-owning-memory)
          new (storage_.get() + (constructed_ * sizeof(AggregateAccumulator)))
              AggregateAccumulator(&aggregate);
      ++constructed_;
      return *placed;
    }
    AggregateAccumulator& ConstructAt(
        const AggregateExpression& aggregate,
        const AggregateAccumulator::Shape& shape) {
      auto* placed =  // NOLINT(cppcoreguidelines-owning-memory)
          new (storage_.get() + (constructed_ * sizeof(AggregateAccumulator)))
              AggregateAccumulator(&aggregate, shape);
      ++constructed_;
      return *placed;
    }
    [[nodiscard]] size_t constructed() const { return constructed_; }

   private:
    // NOLINTNEXTLINE(modernize-avoid-c-arrays)
    std::unique_ptr<std::byte[]> storage_;
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
  std::unordered_map<Row, size_t, GroupKeyTransparentHash,
                     GroupKeyTransparentEqual>
      row_index;
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
  std::vector<int64_t> slim_counts;
  std::vector<int64_t> slim_int_totals;
  std::vector<double> slim_double_totals;
  std::vector<uint8_t> slim_flags;  // bit0: saw an infinite input (SUM double)
  // kCountDistinctInt64 state: kSlimDistinctSlots inline int64 slots per
  // (group, aggregate), the inline occupancy count, and a lazily materialized
  // overflow set.  Slot stride is kSlimDistinctSlots * agg_stride.
  std::vector<int64_t> slim_distinct;
  std::vector<uint8_t> slim_distinct_n;
  std::vector<std::unique_ptr<std::unordered_set<int64_t>>>
      slim_distinct_overflow;
  // Legacy layout (hatch only).
  std::vector<std::vector<Value>> group_keys;  // canonical key values
  std::vector<std::vector<AggregateAccumulator>> group_states;
  // Flat layout: canonical keys in one contiguous vector, accumulators in
  // fixed-size chunks (never moved, so no realloc transient).
  std::vector<Value> flat_group_keys;
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
  [[nodiscard]] int64_t& SlimIntTotal(size_t g, size_t i) {
    return slim_int_totals[(g * agg_stride) + i];
  }
  [[nodiscard]] double& SlimDoubleTotal(size_t g, size_t i) {
    return slim_double_totals[(g * agg_stride) + i];
  }
  [[nodiscard]] uint8_t& SlimFlags(size_t g, size_t i) {
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
      overflow = std::make_unique<std::unordered_set<int64_t>>();
    }
    return overflow->insert(raw).second;
  }

  // Distinct values seen so far for kCountDistinctInt64 (the value COUNT
  // (DISTINCT) finishes to).
  [[nodiscard]] size_t SlimDistinctTotal(size_t g, size_t i) const {
    const size_t state = (g * agg_stride) + i;
    size_t total = slim_distinct_n[state];
    if (state < slim_distinct_overflow.size() &&
        slim_distinct_overflow[state]) {
      total += slim_distinct_overflow[state]->size();
    }
    return total;
  }

  [[nodiscard]] size_t GroupCount() const {
    return flat_layout ? group_count : group_states.size();
  }
  [[nodiscard]] const Value& GroupKey(size_t group, size_t k) const {
    if (flat_layout) {
      return flat_group_keys[(group * key_stride) + k];
    }
    return group_keys[group][k];
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

  void ReserveGroups(size_t target_groups) {
    if (target_groups == 0) {
      return;
    }
    if (int_key) {
      int_index.Reserve(target_groups);
    }
    if (flat_layout) {
      flat_group_keys.reserve(target_groups * key_stride);
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
                return kind == SlimAggKind::kCountDistinctInt64;
              })) {
            slim_distinct.resize(cells * kSlimDistinctSlots, 0);
            slim_distinct_n.resize(cells, 0);
          }
        }
      }
    }
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
          return kind == SlimAggKind::kCountDistinctInt64;
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
      if (!slim) {
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
    // Heterogeneous lookup: most rows land in an existing group, so probe
    // with the scratch key vector directly instead of paying a Row
    // construction (vector alloc + Value copies) per probe.  The transparent
    // hash reproduces std::hash<Row> exactly, so both paths agree.
    if (auto found = row_index.find(canonical_keys); found != row_index.end()) {
      *inserted = false;
      return found->second;
    }
    Row key(canonical_keys);
    auto [iter, emplaced] = row_index.try_emplace(std::move(key), GroupCount());
    *inserted = emplaced;
    if (emplaced) {
      AppendGroup(canonical_keys, aggregate_count, aggregates);
    }
    return iter->second;
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
        slim_distinct_overflow[st]->clear();
      }
    }
  }
};

namespace {

// One (group, aggregate) slim-state update for an already-resolved input
// value.  Shared by the Row-based accumulate loop and the cached-arena fold
// fast path so the two can never drift; false (with *error set) on a raise.
// A null `value` is the COUNT(*) sentinel and always counts.
bool ApplySlimValue(TypedWorkerGroups* local, size_t group_index, size_t i,
                    SlimAggKind kind, const Value* value, Status* error) {
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
      if (value->type != ValueType::kDouble) {
        *error = StatusError(StatusCode::kInvalidArgument,
                             "slim SUM input type mismatch");
        return false;
      }
      if (std::isinf(value->value.double_value)) {
        local->SlimFlags(group_index, i) |= 1;
      }
      local->SlimDoubleTotal(group_index, i) += value->value.double_value;
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
    default:
      *error = StatusError(StatusCode::kInvalidArgument,
                           "aggregate has no slim state");
      return false;
  }
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
void AppendBatchToArena(const TypedCellBatch& batch, size_t first_row,
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
    const auto run_bytes = static_cast<uint32_t>(run.bytes.size());
    const uint32_t batch_prefix =
        first_row == 0 ? 0U : batch_ends[first_row - 1];
    run.bytes.insert(run.bytes.end(),
                     batch.varchar_bytes[varchar_slot].begin() + batch_prefix,
                     batch.varchar_bytes[varchar_slot].begin() +
                         batch_ends[first_row + run_rows - 1]);
    run.ends.reserve(run.ends.size() + run_rows);
    for (size_t r = 0; r < run_rows; ++r) {
      run.ends.push_back(run_bytes +
                         (batch_ends[first_row + r] - batch_prefix));
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
      fill->arena_cells[base + c] = value.value.int_value;
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

}  // namespace

StatusOr<Relation> RunTypedParallelStreamAggregate(
    TransactionContext& context, const SelectStatement& stmt,
    const Expression& having_expr, const Table& table,
    const std::string& table_name, bool reusable,
    const std::vector<slot_t>& projection, const Schema& qualified_schema,
    const Schema& table_schema,
    const std::vector<IntegerPeekCompare>& peek_compares,
    const std::vector<const AggregateExpression*>& aggregate_expressions,
    const CteMap& ctes, TypedAggregationPlan plan,
    const RuntimeFilterGroup* stream_runtime_filter,
    ValueType stream_runtime_filter_type,
    std::optional<slot_t> stream_key_column,
    const PrebuiltMembershipPredicate* stream_prebuilt_membership) {
  ExecutionRuntime* runtime = context.execution_runtime();
  const auto scan_begin = std::chrono::steady_clock::now();
  std::shared_ptr<const LSTableStore> direct_lstable_store;
  {
    std::shared_ptr<const LSTableStore> store =
        GetOrBuildLSTableStore(context.txn_, table, true);
    if (store != nullptr &&
        store->layout_generation == table.LayoutGeneration() &&
        store->first_pid == table.FirstPageId()) {
      bool valid = true;
      if (store->uncommitted_owner_txn != 0) {
        valid = (store->uncommitted_owner_txn == context.txn_.ID());
      } else if (!context.txn_.IsReadOnly() && !context.txn_.WriteSetEmpty()) {
        valid = false;
      } else if (context.txn_.HasAnyPageVersionChains()) {
        for (const page_id_t pid : store->page_ids) {
          if (context.txn_.HasPageVersionChains(pid) ||
              store->FindBlock(pid) == nullptr) {
            valid = false;
            break;
          }
        }
      }
      if (valid) {
        direct_lstable_store = std::move(store);
      }
    }
  }
  const bool int_key =
      plan.key_offsets.size() == 1 &&
      (qualified_schema.GetColumn(plan.key_offsets[0]).Type() ==
           ValueType::kInt64 ||
       qualified_schema.GetColumn(plan.key_offsets[0]).Type() ==
           ValueType::kDate);
  const bool packed_varchar_key =
      !int_key && !plan.key_offsets.empty() && plan.key_offsets.size() <= 2 &&
      std::ranges::all_of(plan.key_offsets, [&](slot_t offset) {
        return qualified_schema.GetColumn(offset).Type() == ValueType::kVarChar;
      });
  // Slot -> VARCHAR-ordinal map for the cell fold (SIZE_MAX = fixed-width
  // slot); filled by the arena setup below.
  std::vector<size_t> varchar_ordinal;
  // Peek compares pre-reject rows inside the scan; they must be disabled
  // while filling the shared cache, whose contract is unfiltered rows.
  const std::optional<std::vector<slot_t>> projection_opt =
      projection.empty() ? std::nullopt
                         : std::optional<std::vector<slot_t>>(projection);

  // Shared base-relation cache: fill on miss (unfiltered rows, per the
  // BaseRelationCacheKey contract), aggregate from it on hit.
  std::shared_ptr<Relation> cached_relation;
  std::shared_ptr<const BaseRelationArena> fold_arena;
  std::string fold_cache_key;
  const bool lstable_skip_cache_copy =
      (direct_lstable_store != nullptr &&
       direct_lstable_store->total_rows > 16384) ||
      (table.GetLSTableStore() != nullptr &&
       table.GetLSTableStore()->total_rows > 16384);
  if (reusable && runtime != nullptr && stream_runtime_filter == nullptr) {
    fold_cache_key = BaseRelationCacheKey(
        table_name, projection.empty() ? nullptr : &projection);
    auto found = runtime->base_relations.find(fold_cache_key);
    if (found != runtime->base_relations.end()) {
      cached_relation = found->second;
      ++runtime->base_scan_cache_hits;
      if (lstable_skip_cache_copy && cached_relation != nullptr &&
          cached_relation->rows.empty() && !cached_relation->HasSpill() &&
          !runtime->base_relation_arenas.contains(fold_cache_key)) {
        cached_relation.reset();
      }
    } else {
      cached_relation = nullptr;
    }
  } else if (reusable && runtime != nullptr &&
             stream_runtime_filter != nullptr) {
    fold_cache_key = BaseRelationCacheKey(
        table_name, projection.empty() ? nullptr : &projection);
    if (runtime->base_relations.contains(fold_cache_key)) {
      ++runtime->base_scan_cache_hits;
    }
  }
  const bool raw_fill_cache =
      cached_relation == nullptr && reusable && runtime != nullptr;
  const bool publish_lstable_marker =
      raw_fill_cache && lstable_skip_cache_copy &&
      !runtime->base_relations.contains(fold_cache_key);
  const bool fill_cache = raw_fill_cache && !lstable_skip_cache_copy &&
                          stream_runtime_filter == nullptr &&
                          stream_prebuilt_membership == nullptr;
  const bool use_direct_lstable =
      direct_lstable_store != nullptr && cached_relation == nullptr &&
      !fill_cache && plan.filter_programs.empty() &&
      std::ranges::none_of(plan.aggregates, [](const auto& input) {
        return input.child.has_value();
      });
  const std::vector<Table::ScanMorsel> morsels =
      (use_direct_lstable || cached_relation != nullptr)
          ? std::vector<Table::ScanMorsel>{}
          : table.BuildScanMorsels(context.txn_, 8);
  const std::vector<IntegerPeekCompare>* peek_ptr =
      fill_cache || peek_compares.empty() ? nullptr : &peek_compares;
  const bool bulk_count =
      !fill_cache && stream_prebuilt_membership == nullptr && plan.slim &&
      plan.key_offsets.empty() && plan.filter_programs.empty() &&
      plan.inlist_compares.empty() &&
      std::ranges::all_of(plan.aggregates,
                          [](const auto& input) {
                            return IsCountStar(*input.aggregate) &&
                                   !input.aggregate->Distinct();
                          }) &&
      std::ranges::all_of(plan.filter_compares, [&](const auto& compare) {
        if (!compare.constant_payload || compare.is_double ||
            compare.string_payload ||
            compare.left >= qualified_schema.ColumnCount()) {
          return false;
        }
        const slot_t column =
            projection.empty() ? compare.left : projection[compare.left];
        return std::ranges::any_of(peek_compares, [&](const auto& peek) {
          return peek.column == column && peek.op == compare.op &&
                 peek.constant == compare.int_constant;
        });
      });
  // A/B hatch: TINYLAMB_DISABLE_FILL_COPY=1 restores move-append into the
  // cache-fill buffer.  Copying FIXED-WIDTH fills keeps the scan iterator's
  // decode buffer alive (no per-row vector reallocation for 6M+ rows), the
  // same trade ParallelScan makes for its chunk appends; VARCHAR/ARRAY cells
  // keep the move so long text never deep-copies.
  static const bool fill_copy_disabled =
      std::getenv("TINYLAMB_DISABLE_FILL_COPY") != nullptr;
  bool fill_copy = !fill_copy_disabled;
  for (slot_t i = 0; i < qualified_schema.ColumnCount(); ++i) {
    const ValueType type = qualified_schema.GetColumn(i).Type();
    if (type == ValueType::kVarChar || type == ValueType::kArray) {
      fill_copy = false;
      break;
    }
  }

  const auto accumulate_row = [&](auto&& row, TypedWorkerGroups* local,
                                  Status* error, size_t* rows_output,
                                  size_t* updates, MorselFillBuffer* fill,
                                  std::vector<Value>* scratch) -> void {
    // Cache fills must capture UNFILTERED rows (BaseRelationCacheKey
    // contract): every consumer, including this statement, applies its own
    // predicates while reading or aggregating.  The fill below MOVES the row
    // instead of copying it (the copy dominated cache-fill time for the
    // multi-million-row base relations behind Q18/Q21); every read of `row`
    // therefore has to happen before the move.  A raise that skips the fill
    // is unobservable: the cache is only published after a fully successful
    // run.
    // Warms the int-key group slot while the filter work below runs; the key
    // cell was just decoded, so reading it here is free and the slot line is
    // resident by the time FindOrInsert probes.
    if (int_key && !plan.key_offsets.empty() && local != nullptr) {
      const Value& key_cell = row[plan.key_offsets[0]];
      if (!key_cell.IsNull()) {
        local->PrefetchGroupSlot(key_cell.value.int_value);
      }
    }
    bool keep = true;
    for (const TypedFilterCompare& compare : plan.filter_compares) {
      const Value& left = row[compare.left];
      if (compare.constant_payload) {
        if (left.IsNull() || !DecideTypedFilterCompare(left, left, compare)) {
          keep = false;
          break;
        }
        continue;
      }
      const Value& right = row[compare.right];
      if (left.IsNull() || right.IsNull() ||
          !DecideTypedFilterCompare(left, right, compare)) {
        keep = false;
        break;
      }
    }
    if (keep) {
      for (const TypedInListCompare& list : plan.inlist_compares) {
        const Value& cell = row[list.column];
        if (cell.IsNull()) {
          keep = false;
          break;
        }
        bool matched = false;
        if (list.is_string) {
          matched = MatchTypedStringInList(cell.value.varchar_value, list);
        } else if (list.is_double) {
          for (const double item : list.double_values) {
            if (cell.value.double_value == item) {
              matched = true;
              break;
            }
          }
        } else {
          for (const int64_t item : list.int_values) {
            if (cell.value.int_value == item) {
              matched = true;
              break;
            }
          }
        }
        if (!matched) {
          keep = false;
          break;
        }
      }
    }
    for (const BytecodeProgram& program : plan.filter_programs) {
      StatusOr<Value> decided = TryEvaluateRowAuto(program, row);
      if (!decided.HasValue()) {
        *error = decided.GetStatus();
        return;
      }
      if (!Truthy(decided.Value())) {
        keep = false;
        break;
      }
    }
    if (!keep) {
      if (fill != nullptr) {
        AppendFillRow(row, fill, fill_copy);
      }
      return;
    }
    ++*rows_output;
    bool inserted = false;
    size_t group_index = 0;
    // Int-key fast path: probe the raw slot with the cell value directly and
    // build canonical keys only when a new group actually opens.  The hit
    // path (the overwhelmingly common case on multi-million-row folds) then
    // constructs no Value at all for the group key.
    if (int_key && !plan.key_offsets.empty() &&
        !row[plan.key_offsets[0]].IsNull()) {
      const Value& key_cell = row[plan.key_offsets[0]];
      size_t* slot = local->RawIntSlot(key_cell.value.int_value, &inserted);
      if (inserted) {
        scratch->clear();
        scratch->push_back(CanonicalDistinctValue(key_cell));
        group_index = local->ClaimSlotForNewGroup(
            slot, *scratch, plan.aggregates.size(), aggregate_expressions);
      } else {
        group_index = *slot;
      }
    } else if (packed_varchar_key) {
      uint64_t packed = 0;
      bool can_pack = true;
      for (size_t k = 0; k < plan.key_offsets.size(); ++k) {
        const Value& key_cell = row[plan.key_offsets[k]];
        uint32_t part = 0;
        if (key_cell.IsNull() || key_cell.type != ValueType::kVarChar ||
            key_cell.IsCaseInsensitive() ||
            !TryPackShortVarcharSpan(key_cell.value.varchar_value, &part)) {
          can_pack = false;
          break;
        }
        packed |= static_cast<uint64_t>(part) << (k * 32);
      }
      if (can_pack) {
        size_t* slot =
            local->RawIntSlot(static_cast<int64_t>(packed), &inserted);
        if (inserted) {
          scratch->clear();
          for (const slot_t offset : plan.key_offsets) {
            scratch->push_back(CanonicalDistinctValue(row[offset]));
          }
          group_index = local->ClaimSlotForNewGroup(
              slot, *scratch, plan.aggregates.size(), aggregate_expressions);
        } else {
          group_index = *slot;
        }
      } else {
        scratch->clear();
        for (const slot_t offset : plan.key_offsets) {
          scratch->push_back(CanonicalDistinctValue(row[offset]));
        }
        group_index = local->FindOrInsert(*scratch, 0, plan.aggregates.size(),
                                          aggregate_expressions, &inserted);
      }
    } else {
      scratch->clear();
      int64_t raw_key = 0;
      for (const slot_t offset : plan.key_offsets) {
        scratch->push_back(CanonicalDistinctValue(row[offset]));
        if (scratch->size() == 1 && int_key) {
          raw_key = row[offset].value.int_value;
        }
      }
      group_index =
          local->FindOrInsert(*scratch, raw_key, plan.aggregates.size(),
                              aggregate_expressions, &inserted);
    }
    // Counted per worker and folded into the shared runtime stat after the
    // join: multiple workers racing on the plain counter is a data race.
    ++*updates;
    if (local->slim) {
      for (size_t i = 0; i < plan.aggregates.size(); ++i) {
        const TypedAggregateInput& input = plan.aggregates[i];
        const Value* value = nullptr;
        Value evaluated;
        if (input.column.has_value()) {
          value = &row[*input.column];
        } else if (input.conditional.has_value()) {
          evaluated = EvaluateTypedConditionalOnRow(*input.conditional, row);
          value = &evaluated;
        } else if (input.double_expr.has_value()) {
          evaluated = EvaluateTypedDoubleExprOnRow(*input.double_expr, row);
          value = &evaluated;
        } else if (input.int_expr.has_value()) {
          StatusOr<Value> iv = EvaluateTypedIntExprOnRow(*input.int_expr, row);
          if (!iv.HasValue()) {
            *error = iv.GetStatus();
            return;
          }
          evaluated = iv.MoveValue();
          value = &evaluated;
        } else if (input.child.has_value()) {
          StatusOr<Value> child_value = TryEvaluateRowAuto(*input.child, row);
          if (!child_value.HasValue()) {
            *error = child_value.GetStatus();
            return;
          }
          evaluated = child_value.MoveValue();
          value = &evaluated;
        }
        if (!ApplySlimValue(local, group_index, i, (*local->slim_kinds)[i],
                            value, error)) {
          return;
        }
      }
      if (fill != nullptr) {
        AppendFillRow(row, fill, fill_copy);
      }
      return;
    }
    for (size_t i = 0; i < plan.aggregates.size(); ++i) {
      const TypedAggregateInput& input = plan.aggregates[i];
      AggregateAccumulator& accumulator = local->State(group_index, i);
      if (input.column.has_value()) {
        if (Status st = accumulator.Add(row[*input.column]);
            st != Status::kSuccess) {
          *error = st;
          return;
        }
        continue;
      }
      if (input.conditional.has_value()) {
        if (Status st = accumulator.Add(
                EvaluateTypedConditionalOnRow(*input.conditional, row));
            st != Status::kSuccess) {
          *error = st;
          return;
        }
        continue;
      }
      if (input.double_expr.has_value()) {
        if (Status st = accumulator.Add(
                EvaluateTypedDoubleExprOnRow(*input.double_expr, row));
            st != Status::kSuccess) {
          *error = st;
          return;
        }
        continue;
      }
      if (input.int_expr.has_value()) {
        StatusOr<Value> iv = EvaluateTypedIntExprOnRow(*input.int_expr, row);
        if (!iv.HasValue()) {
          *error = iv.GetStatus();
          return;
        }
        if (Status st = accumulator.Add(iv.MoveValue());
            st != Status::kSuccess) {
          *error = st;
          return;
        }
        continue;
      }
      if (!input.child.has_value()) {
        // COUNT(*).
        if (Status st = accumulator.Add(Value(1)); st != Status::kSuccess) {
          *error = st;
          return;
        }
        continue;
      }
      StatusOr<Value> value = TryEvaluateRowAuto(*input.child, row);
      if (!value.HasValue()) {
        *error = value.GetStatus();
        return;
      }
      if (Status st = accumulator.Add(value.Value()); st != Status::kSuccess) {
        *error = st;
        return;
      }
    }
    if (fill != nullptr) {
      AppendFillRow(row, fill, fill_copy);
    }
  };

  // Typed-fill fold: one decoded cell row of a TypedCellBatch through the
  // same filter/key/slim ladder as accumulate_row above, never constructing a
  // Row.  `mirror` (per-morsel arena run) receives every row, filtered or
  // not — cache fills capture UNFILTERED rows (BaseRelationCacheKey
  // contract).  Declined shapes keep the Row loop above; the value contracts
  // (canonical keys, NULL handling, slim kinds) are shared helpers, so the
  // two paths cannot drift semantically.
  const auto fold_cell_row = [&](const TypedCellBatch& batch, size_t r,
                                 TypedWorkerGroups* local, Status* error,
                                 size_t* rows_output, size_t* updates,
                                 std::vector<Value>* scratch,
                                 std::array<Value, 8>* row_values) -> void {
    const size_t base = r * batch.stride;
    const auto make_value = [&](slot_t c) {
      Value value;
      switch (batch.slot_types[c]) {
        case ValueType::kDouble:
          value.type = ValueType::kDouble;
          value.value.double_value =
              std::bit_cast<double>(batch.cells[base + c]);
          break;
        case ValueType::kVarChar:
          // Probe-only view into the batch; copies become owned (Value copy
          // ctor) the moment a key or accumulator retains one.
          value.type = ValueType::kVarChar;
          value.value.varchar_value = batch.Varchar(r, varchar_ordinal[c]);
          break;
        default:
          value.type = batch.slot_types[c];
          value.value.int_value = batch.cells[base + c];
          break;
      }
      if (value.type == ValueType::kInt64 && batch.slot_unsigned[c]) {
        value.SetUnsigned();
      }
      return value;
    };
    const auto is_null = [&](slot_t c) { return batch.nulls[base + c] != 0; };
    const int64_t* row_cells = batch.cells.data() + base;
    const bool filter_compares_done_in_scan =
        !fill_cache && peek_ptr != nullptr &&
        peek_compares.size() == plan.filter_compares.size();
    bool keep = true;
    if (!filter_compares_done_in_scan) {
      for (const TypedFilterCompare& compare : plan.filter_compares) {
        if (!DecideTypedFilterCompareCells(compare, row_cells, is_null,
                                           make_value)) {
          keep = false;
          break;
        }
      }
    }
    if (keep) {
      for (const TypedInListCompare& list : plan.inlist_compares) {
        if (is_null(list.column)) {
          keep = false;
          break;
        }
        bool matched = false;
        if (list.is_string) {
          matched = MatchTypedStringInList(
              make_value(list.column).value.varchar_value, list);
        } else if (list.is_double) {
          const double v = make_value(list.column).value.double_value;
          for (const double item : list.double_values) {
            if (v == item) {
              matched = true;
              break;
            }
          }
        } else {
          const int64_t v = batch.cells[base + list.column];
          for (const int64_t item : list.int_values) {
            if (v == item) {
              matched = true;
              break;
            }
          }
        }
        if (!matched) {
          keep = false;
          break;
        }
      }
    }
    if (!keep) {
      return;
    }
    ++*rows_output;
    bool inserted = false;
    size_t group_index = 0;
    if (int_key && !plan.key_offsets.empty() && !is_null(plan.key_offsets[0])) {
      const int64_t raw_key = batch.cells[base + plan.key_offsets[0]];
      const size_t mru_idx = TypedWorkerGroups::MruSlot(raw_key);
      if (local->mru_groups[mru_idx] != SIZE_MAX &&
          local->mru_keys[mru_idx] == raw_key) {
        group_index = local->mru_groups[mru_idx];
      } else {
        size_t* slot = local->RawIntSlot(raw_key, &inserted);
        if (inserted) {
          scratch->clear();
          for (const slot_t offset : plan.key_offsets) {
            scratch->push_back(CanonicalDistinctValue(make_value(offset)));
          }
          group_index = local->ClaimSlotForNewGroup(
              slot, *scratch, plan.aggregates.size(), aggregate_expressions);
        } else {
          group_index = *slot;
        }
        local->mru_keys[mru_idx] = raw_key;
        local->mru_groups[mru_idx] = group_index;
      }
    } else if (packed_varchar_key) {
      uint64_t packed = 0;
      bool can_pack = true;
      for (size_t k = 0; k < plan.key_offsets.size(); ++k) {
        const slot_t offset = plan.key_offsets[k];
        uint32_t part = 0;
        if (is_null(offset) ||
            !TryPackShortVarcharSpan(batch.Varchar(r, varchar_ordinal[offset]),
                                     &part)) {
          can_pack = false;
          break;
        }
        packed |= static_cast<uint64_t>(part) << (k * 32);
      }
      if (can_pack) {
        const auto raw_packed = static_cast<int64_t>(packed);
        const size_t mru_idx = TypedWorkerGroups::MruSlot(raw_packed);
        if (local->mru_groups[mru_idx] != SIZE_MAX &&
            local->mru_keys[mru_idx] == raw_packed) {
          group_index = local->mru_groups[mru_idx];
        } else {
          size_t* slot = local->RawIntSlot(raw_packed, &inserted);
          if (inserted) {
            scratch->clear();
            for (const slot_t offset : plan.key_offsets) {
              scratch->push_back(CanonicalDistinctValue(make_value(offset)));
            }
            group_index = local->ClaimSlotForNewGroup(
                slot, *scratch, plan.aggregates.size(), aggregate_expressions);
          } else {
            group_index = *slot;
          }
          local->mru_keys[mru_idx] = raw_packed;
          local->mru_groups[mru_idx] = group_index;
        }
      } else {
        scratch->clear();
        for (const slot_t offset : plan.key_offsets) {
          if (is_null(offset)) {
            scratch->emplace_back();
          } else {
            scratch->push_back(CanonicalDistinctValue(make_value(offset)));
          }
        }
        group_index = local->FindOrInsert(*scratch, 0, plan.aggregates.size(),
                                          aggregate_expressions, &inserted);
      }
    } else {
      scratch->clear();
      int64_t raw_key = 0;
      for (const slot_t offset : plan.key_offsets) {
        if (is_null(offset)) {
          scratch->emplace_back();
        } else {
          scratch->push_back(CanonicalDistinctValue(make_value(offset)));
        }
        if (scratch->size() == 1 && int_key) {
          raw_key = scratch->back().value.int_value;
        }
      }
      group_index =
          local->FindOrInsert(*scratch, raw_key, plan.aggregates.size(),
                              aggregate_expressions, &inserted);
    }
    ++*updates;
    if (local->slim) {
      // Raw-cell fast path: every slim kind reads at most one column cell,
      // so the common payload kinds (COUNT / SUM / AVG / COUNT(DISTINCT)
      // over fixed-width cells) fold straight from batch.cells without
      // boxing a Value per input.  NULL inputs skip exactly like the generic
      // path (ApplySlimValue returns early on IsNull for every kind); a kind
      // or cell type the raw switch does not cover falls through to the
      // generic boxed call, which raises the same type-mismatch errors.
      const std::vector<SlimAggKind>& kinds = *local->slim_kinds;
      std::array<Value, 8>& stack_values = *row_values;
      for (size_t i = 0; i < plan.aggregates.size(); ++i) {
        const TypedAggregateInput& input = plan.aggregates[i];
        const SlimAggKind kind = kinds[i];
        if (input.double_expr.has_value()) {
          double addend = 0.0;
          if (!EvaluateTypedDoubleExprCells(*input.double_expr, row_cells,
                                            is_null, &addend)) {
            continue;
          }
          if (kind == SlimAggKind::kSumDouble) {
            if (std::isinf(addend)) {
              local->SlimFlags(group_index, i) |= 1;
            }
            local->SlimDoubleTotal(group_index, i) += addend;
            ++local->SlimCount(group_index, i);
            continue;
          }
          if (kind == SlimAggKind::kAvgDouble) {
            local->SlimDoubleTotal(group_index, i) += addend;
            ++local->SlimCount(group_index, i);
            continue;
          }
          stack_values[i] = Value(addend);
          if (!ApplySlimValue(local, group_index, i, kind, &stack_values[i],
                              error)) {
            return;
          }
          continue;
        }
        if (input.int_expr.has_value()) {
          int64_t addend = 0;
          if (!EvaluateTypedIntExprCells(*input.int_expr, row_cells, is_null,
                                         &addend, error)) {
            if (*error != Status::kSuccess) {
              return;
            }
            continue;
          }
          stack_values[i] = Value(addend);
          if (!ApplySlimValue(local, group_index, i, kind, &stack_values[i],
                              error)) {
            return;
          }
          continue;
        }
        ValueType cell_type = ValueType::kNull;
        int64_t cell = 0;
        bool cell_unsigned = false;
        std::optional<slot_t> fallback_col;
        if (input.conditional.has_value()) {
          if (!DecideTypedFilterCompareCells(input.conditional->condition,
                                             row_cells, is_null, make_value)) {
            continue;
          }
          if (input.conditional->then_column.has_value()) {
            const slot_t col = *input.conditional->then_column;
            if (is_null(col)) {
              continue;
            }
            cell_type = batch.slot_types[col];
            cell = row_cells[col];
            cell_unsigned = batch.slot_unsigned[col];
            fallback_col = col;
          } else {
            cell_type = input.conditional->result_type;
            cell = input.conditional->then_int_constant;
          }
        } else if (input.column.has_value()) {
          const slot_t column = *input.column;
          if (is_null(column)) {
            continue;
          }
          cell_type = batch.slot_types[column];
          cell = row_cells[column];
          cell_unsigned = batch.slot_unsigned[column];
          fallback_col = column;
        } else {
          // COUNT(*) (the only no-column slim input) counts unconditionally;
          // any other shape keeps the generic nullptr sentinel path.
          if (kind == SlimAggKind::kCount) {
            ++local->SlimCount(group_index, i);
            continue;
          }
          if (!ApplySlimValue(local, group_index, i, kind, nullptr, error)) {
            return;
          }
          continue;
        }
        bool handled = true;
        switch (kind) {
          case SlimAggKind::kCount:
            ++local->SlimCount(group_index, i);
            break;
          case SlimAggKind::kSumInt64:
            if (cell_type == ValueType::kInt64 && !cell_unsigned) {
              int64_t updated = 0;
              if (__builtin_add_overflow(local->SlimIntTotal(group_index, i),
                                         cell, &updated)) {
                *error = StatusError(StatusCode::kIsInfinity,
                                     "integer overflow in SUM");
                return;
              }
              local->SlimIntTotal(group_index, i) = updated;
              ++local->SlimCount(group_index, i);
            } else {
              handled = false;
            }
            break;
          case SlimAggKind::kSumDouble:
            if (cell_type == ValueType::kDouble) {
              const auto addend = std::bit_cast<double>(cell);
              if (std::isinf(addend)) {
                local->SlimFlags(group_index, i) |= 1;
              }
              local->SlimDoubleTotal(group_index, i) += addend;
              ++local->SlimCount(group_index, i);
            } else {
              handled = false;
            }
            break;
          case SlimAggKind::kAvgDouble:
            if (cell_type == ValueType::kDouble) {
              local->SlimDoubleTotal(group_index, i) +=
                  std::bit_cast<double>(cell);
              ++local->SlimCount(group_index, i);
            } else if (cell_type == ValueType::kInt64) {
              local->SlimDoubleTotal(group_index, i) +=
                  cell_unsigned
                      ? static_cast<double>(static_cast<uint64_t>(cell))
                      : static_cast<double>(cell);
              ++local->SlimCount(group_index, i);
            } else {
              handled = false;
            }
            break;
          case SlimAggKind::kCountDistinctInt64:
            if (cell_type == ValueType::kInt64 ||
                cell_type == ValueType::kDate) {
              static_cast<void>(
                  local->SlimInsertDistinct(group_index, i, cell));
            } else {
              handled = false;
            }
            break;
          default:
            handled = false;
            break;
        }
        if (handled) {
          continue;
        }
        if (fallback_col.has_value()) {
          stack_values[i] = make_value(*fallback_col);
        } else {
          stack_values[i] = Value(cell);
          stack_values[i].type = cell_type;
        }
        if (!ApplySlimValue(local, group_index, i, kind, &stack_values[i],
                            error)) {
          return;
        }
      }
      return;
    }
    for (size_t i = 0; i < plan.aggregates.size(); ++i) {
      const TypedAggregateInput& input = plan.aggregates[i];
      AggregateAccumulator& accumulator = local->State(group_index, i);
      if (input.column.has_value()) {
        const Value value =
            is_null(*input.column) ? Value() : make_value(*input.column);
        if (Status st = accumulator.Add(value); st != Status::kSuccess) {
          *error = st;
          return;
        }
        continue;
      }
      if (input.conditional.has_value()) {
        Value value;
        if (DecideTypedFilterCompareCells(input.conditional->condition,
                                          row_cells, is_null, make_value)) {
          if (input.conditional->then_column.has_value()) {
            const slot_t col = *input.conditional->then_column;
            if (!is_null(col)) {
              value = make_value(col);
            }
          } else {
            value = Value(input.conditional->then_int_constant);
            value.type = input.conditional->result_type;
          }
        }
        if (Status st = accumulator.Add(value); st != Status::kSuccess) {
          *error = st;
          return;
        }
        continue;
      }
      if (input.double_expr.has_value()) {
        double dbl = 0.0;
        const Value value = EvaluateTypedDoubleExprCells(
                                *input.double_expr, row_cells, is_null, &dbl)
                                ? Value(dbl)
                                : Value();
        if (Status st = accumulator.Add(value); st != Status::kSuccess) {
          *error = st;
          return;
        }
        continue;
      }
      if (input.int_expr.has_value()) {
        int64_t iv = 0;
        const bool ok = EvaluateTypedIntExprCells(*input.int_expr, row_cells,
                                                  is_null, &iv, error);
        if (*error != Status::kSuccess) {
          return;
        }
        const Value value = ok ? Value(iv) : Value();
        if (Status st = accumulator.Add(value); st != Status::kSuccess) {
          *error = st;
          return;
        }
        continue;
      }
      // COUNT(*): the typed fill declines expression aggregates, so a
      // column-less input here is exactly COUNT(*).
      if (Status st = accumulator.Add(Value(1)); st != Status::kSuccess) {
        *error = st;
        return;
      }
    }
  };

  // Per-morsel fill buffers are moved into the published cache relation as
  // soon as their predecessors have landed, which keeps the cache row order
  // identical to a page-ordered serial scan while holding at most one buffer
  // per worker in flight (never the whole table twice).
  std::mutex publish_mutex;
  size_t next_to_publish = 0;
  std::map<size_t, MorselFillBuffer> pending_publish;
  // Rows landed in the published cache (Row-fill appends plus typed-fill
  // mirrors) — the arena registration guard compares against this, because a
  // typed fill publishes no Rows at all.
  size_t published_rows = 0;
  // Owned unconditionally while filling so an error return frees the rows.
  std::shared_ptr<Relation> cache_owner(fill_cache ? new Relation(runtime)
                                                   : nullptr);
  Relation* cache_target = cache_owner.get();
  if (cache_target != nullptr) {
    cache_target->schema = qualified_schema;
  }
  // A/B hatch: TINYLAMB_DISABLE_AGG_ARENA=1 keeps the cache Row-only.  The
  // arena mirrors the rows with schema-typed cells (INT64/DATE/DOUBLE as
  // 8-byte payloads, VARCHAR bytes in per-slot arenas) so the typed fold's
  // cache-hit pass can stream cells without visiting per-Row heap blocks
  // (Q18/Q21's multi-million-row folds).
  static const bool arena_disabled =
      std::getenv("TINYLAMB_DISABLE_AGG_ARENA") != nullptr;
  std::shared_ptr<BaseRelationArena> fill_arena;
  std::vector<int64_t> arena_cell_buffer;
  std::vector<char> arena_null_buffer;
  // Typed fill (TINYLAMB_TYPED_FILL=1): the scan decodes schema-typed cell
  // batches instead of Rows, the fold consumes raw cells, and the cache is
  // published with the arena only (rows are materialized from it on demand by
  // consumers that cannot take the arena path). Non-reusable scans fold the
  // same decoded batches without mirroring them. Programs needing a Row use
  // owned scratch cells and the existing filter/accumulator evaluator.
  static const bool typed_fill_enabled =
      std::getenv("TINYLAMB_DISABLE_TYPED_FILL") == nullptr;
  bool typed_fill = false;
  if (!arena_disabled) {
    bool all_typed = qualified_schema.ColumnCount() > 0;
    for (slot_t i = 0; i < qualified_schema.ColumnCount(); ++i) {
      const ValueType type = qualified_schema.GetColumn(i).Type();
      if (type != ValueType::kInt64 && type != ValueType::kDate &&
          type != ValueType::kDouble && type != ValueType::kVarChar) {
        all_typed = false;
        break;
      }
    }
    if (all_typed) {
      fill_arena = std::make_shared<BaseRelationArena>();
      fill_arena->stride = qualified_schema.ColumnCount();
      fill_arena->slot_types.reserve(fill_arena->stride);
      size_t varchar_count = 0;
      for (slot_t i = 0; i < qualified_schema.ColumnCount(); ++i) {
        const ValueType type = qualified_schema.GetColumn(i).Type();
        fill_arena->slot_types.push_back(type);
        if (type == ValueType::kVarChar) {
          ++varchar_count;
        }
      }
      fill_arena->null_bits.resize(fill_arena->stride);
      fill_arena->varchars.resize(varchar_count);
      arena_cell_buffer.resize(fill_arena->stride);
      arena_null_buffer.resize(fill_arena->stride);
      varchar_ordinal.assign(fill_arena->stride, SIZE_MAX);
      size_t varchar_ordinal_next = 0;
      for (size_t i = 0; i < fill_arena->stride; ++i) {
        if (fill_arena->slot_types[i] == ValueType::kVarChar) {
          varchar_ordinal[i] = varchar_ordinal_next++;
        }
      }
      typed_fill = typed_fill_enabled;
    }
  }
  const auto publish_morsel = [&](size_t morsel, MorselFillBuffer* buffer) {
    std::scoped_lock lock(publish_mutex);
    // Cut the geometric arena buffers to the written length so the
    // rows*stride == cells.size invariant below holds exactly.
    buffer->arena_cells.resize(buffer->arena_used);
    buffer->arena_nulls.resize(buffer->arena_used);
    pending_publish.emplace(morsel, std::move(*buffer));
    buffer->rows.clear();
    buffer->rows.shrink_to_fit();
    buffer->arena_cells.clear();
    buffer->arena_cells.shrink_to_fit();
    buffer->arena_nulls.clear();
    buffer->arena_nulls.shrink_to_fit();
    while (pending_publish.contains(next_to_publish)) {
      auto node = pending_publish.extract(next_to_publish);
      // Plain push_back: repeated exact reserves would move every existing
      // row per morsel (quadratic).  Geometric growth moves are cheap.
      for (Row& row : node.mapped().rows) {
        cache_target->rows.push_back(std::move(row));
      }
      published_rows += node.mapped().rows.size();
      // Splice the morsel's arena mirror (bulk memcpy, still row order); a
      // layout mismatch drops the mirror — the registration guard keeps an
      // unusable partial out of the runtime map.
      if (fill_arena != nullptr) {
        if (node.mapped().mirror.rows > 0) {
          if (node.mapped().mirror.stride == fill_arena->stride &&
              node.mapped().mirror.slot_types == fill_arena->slot_types) {
            fill_arena->AppendTyped(node.mapped().mirror);
            published_rows += node.mapped().mirror.rows;
          } else {
            fill_arena.reset();
          }
        } else if (node.mapped().arena_stride == fill_arena->stride &&
                   node.mapped().rows.size() * fill_arena->stride ==
                       node.mapped().arena_cells.size()) {
          fill_arena->AppendRun(node.mapped().arena_cells.data(),
                                node.mapped().arena_nulls.data(),
                                node.mapped().rows.size());
        } else if (node.mapped().arena_stride != 0) {
          fill_arena.reset();
        }
      }
      ++next_to_publish;
    }
  };

  const size_t effective_morsels =
      use_direct_lstable
          ? std::max<size_t>(1, direct_lstable_store->blocks.size())
          : morsels.size();
  auto worker_count = std::min<size_t>(
      {std::thread::hardware_concurrency(),
       cached_relation != nullptr ? size_t{16} : effective_morsels,
       size_t{16}});
  if (worker_count < 2) {
    worker_count = 1;
  }
  bool bounded = false;
  size_t topn_capacity = 0;
  if (!stmt.Distinct() && stmt.HasLimit() && !stmt.WithTies() &&
      stmt.Offset() <= std::numeric_limits<size_t>::max() - stmt.Limit()) {
    bounded = true;
    topn_capacity = stmt.Offset() + stmt.Limit();
  }
  // A/B hatch: TINYLAMB_DISABLE_HAVING_FAST=1 restores the per-group
  // AggregateResultMap path for every HAVING shape.
  static const bool having_fast_disabled =
      std::getenv("TINYLAMB_DISABLE_HAVING_FAST") != nullptr;
  std::vector<PositionalHaving> positional_having;
  if (having_expr != nullptr && !having_fast_disabled) {
    if (!TryCompilePositionalHavingConjuncts(having_expr, plan.aggregates,
                                             &positional_having)) {
      positional_having.clear();
    }
  }
  // The per-aggregate classification (array element type, SUM column-name
  // scan) is identical for every group of every worker; derive it once.
  std::vector<AggregateAccumulator::Shape> aggregate_shapes;
  aggregate_shapes.reserve(aggregate_expressions.size());
  for (const AggregateExpression* aggregate : aggregate_expressions) {
    aggregate_shapes.push_back(
        AggregateAccumulator::AnalyzeShape(aggregate, nullptr));
  }
  // A/B hatch: TINYLAMB_DISABLE_FLAT_GROUP_FOLD=1 restores the per-group
  // vector-of-vector group storage.
  static const bool flat_fold_disabled =
      std::getenv("TINYLAMB_DISABLE_FLAT_GROUP_FOLD") != nullptr;
  std::vector<TypedWorkerGroups> workers(worker_count);
  for (TypedWorkerGroups& local : workers) {
    local.int_key = int_key;
    local.packed_varchar_key = packed_varchar_key;
    local.shapes = &aggregate_shapes;
    local.Configure(!flat_fold_disabled, plan.key_offsets.size(),
                    plan.aggregates.size());
    local.ConfigureSlim(plan);
  }
  // A typed fill publishes the arena without Rows.  A hit whose fold shape
  // CAN stream cells goes straight to the arena fold; anything else boxes the
  // rows back here, before the workers spawn (MaterializeCacheRowsParallel
  // latches rows_boxed, so the resized-but-unboxed state is never observable).
  // Mirror of the worker's arena_ok gate: when the fold CANNOT stream cells,
  // box the arena's rows back (parallel) before spawning workers.
  if (cached_relation != nullptr && !arena_disabled && runtime != nullptr &&
      !workers.empty()) {
    // Same formula as the worker's arena_ok gate: when this fold cannot
    // stream cells (non-slim shapes included), the arena-only cache must be
    // boxed back into rows before the workers spawn.
    bool fold_can_stream_arena = workers[0].slim && int_key &&
                                 plan.filter_programs.empty() &&
                                 plan.aggregates.size() <= 8;
    if (fold_can_stream_arena) {
      for (const TypedAggregateInput& input : plan.aggregates) {
        const auto aggregate_index =
            static_cast<size_t>(&input - plan.aggregates.data());
        if (input.child.has_value() ||
            (!input.column.has_value() && !input.conditional.has_value() &&
             !input.double_expr.has_value() && !input.int_expr.has_value() &&
             (*workers[0].slim_kinds)[aggregate_index] !=
                 SlimAggKind::kCount)) {
          fold_can_stream_arena = false;
          break;
        }
      }
    }
    if (!fold_can_stream_arena && cached_relation->rows.empty()) {
      MaterializeCacheRowsParallel(fold_cache_key, cached_relation.get(),
                                   runtime);
    }
  }
  std::vector<Status> worker_errors(worker_count, Status::kSuccess);
  std::vector<size_t> scanned(worker_count, 0);
  std::vector<size_t> emitted(worker_count, 0);
  std::vector<size_t> aggregate_updates(worker_count, 0);
  std::vector<size_t> reclaimed_groups(worker_count, 0);
  std::exception_ptr worker_exception;

  std::vector<slot_t> proj_to_full(qualified_schema.ColumnCount());
  std::vector<ValueType> proj_types(qualified_schema.ColumnCount());
  std::vector<uint8_t> proj_unsigned(qualified_schema.ColumnCount(), 0);
  for (slot_t c = 0; c < qualified_schema.ColumnCount(); ++c) {
    proj_to_full[c] = projection.empty() ? c : projection[c];
    proj_types[c] = qualified_schema.GetColumn(c).Type();
    proj_unsigned[c] = qualified_schema.GetColumn(c).IsUnsigned() ? 1 : 0;
  }

  const bool monotonic_int_key =
      use_direct_lstable && direct_lstable_store->total_rows >= 65536 &&
      stream_prebuilt_membership == nullptr && int_key &&
      plan.key_offsets.size() == 1 && !flat_fold_disabled && plan.slim &&
      plan.order_items.empty() && !bounded &&
      (!having_expr || !positional_having.empty()) &&
      proj_to_full[plan.key_offsets[0]] <
          direct_lstable_store->column_is_monotonic.size() &&
      direct_lstable_store
              ->column_is_monotonic[proj_to_full[plan.key_offsets[0]]] != 0 &&
      direct_lstable_store
              ->column_has_null[proj_to_full[plan.key_offsets[0]]] == 0;

  bool dict_group_key = false;
  size_t dict_group_stride1 = 1;
  size_t dict_group_total_codes = 0;
  if (use_direct_lstable && !int_key && !plan.key_offsets.empty() &&
      plan.key_offsets.size() <= 2) {
    const slot_t full0 = proj_to_full[plan.key_offsets[0]];
    if (full0 < direct_lstable_store->dictionaries.size() &&
        direct_lstable_store->dictionaries[full0].active &&
        direct_lstable_store->dictionaries[full0].value_type ==
            ValueType::kVarChar &&
        direct_lstable_store->column_has_null[full0] == 0) {
      const size_t n0 =
          direct_lstable_store->dictionaries[full0].entries.size();
      if (plan.key_offsets.size() == 1 && n0 > 0 && n0 <= 256) {
        dict_group_key = true;
        dict_group_total_codes = n0;
      } else if (plan.key_offsets.size() == 2) {
        const slot_t full1 = proj_to_full[plan.key_offsets[1]];
        if (full1 < direct_lstable_store->dictionaries.size() &&
            direct_lstable_store->dictionaries[full1].active &&
            direct_lstable_store->dictionaries[full1].value_type ==
                ValueType::kVarChar &&
            direct_lstable_store->column_has_null[full1] == 0) {
          const size_t n1 =
              direct_lstable_store->dictionaries[full1].entries.size();
          if (n0 > 0 && n1 > 0 && n0 * n1 <= 256) {
            dict_group_key = true;
            dict_group_stride1 = n1;
            dict_group_total_codes = n0 * n1;
          }
        }
      }
    }
  }
  bool bounded_int_group_key = false;
  int64_t bounded_int_min = 0;
  size_t bounded_int_span = 0;
  if (use_direct_lstable && int_key && !monotonic_int_key &&
      plan.key_offsets.size() == 1 && plan.slim && !plan.aggregates.empty() &&
      plan.aggregates.size() <= 4 &&
      std::ranges::all_of(plan.slim_kinds, [](SlimAggKind k) {
        return k == SlimAggKind::kCount || k == SlimAggKind::kSumInt64 ||
               k == SlimAggKind::kSumDouble || k == SlimAggKind::kAvgDouble;
      })) {
    const slot_t full0 = proj_to_full[plan.key_offsets[0]];
    if (full0 < direct_lstable_store->column_has_null.size() &&
        direct_lstable_store->column_has_null[full0] == 0 &&
        full0 < direct_lstable_store->column_min_int.size() &&
        direct_lstable_store->column_min_int[full0] <=
            direct_lstable_store->column_max_int[full0]) {
      const int64_t lo = direct_lstable_store->column_min_int[full0];
      const int64_t hi = direct_lstable_store->column_max_int[full0];
      const uint64_t span =
          static_cast<uint64_t>(hi) - static_cast<uint64_t>(lo) + 1ULL;
      if (span > 0 && span <= 65536ULL) {
        bounded_int_group_key = true;
        bounded_int_min = lo;
        bounded_int_span = static_cast<size_t>(span);
      }
    }
  }

  struct LSTableAggDictFilter {
    slot_t full_column{0};
    std::array<uint64_t, 4> pass_mask{};
    std::array<uint8_t, 256> pass{};
  };
  std::vector<LSTableAggDictFilter> lstable_agg_dict_filters;
  std::vector<uint8_t> filter_compare_covered;
  std::vector<uint8_t> inlist_compare_covered;
  if (use_direct_lstable) {
    filter_compare_covered.assign(plan.filter_compares.size(), 0);
    inlist_compare_covered.assign(plan.inlist_compares.size(), 0);
    std::vector<int> col_to_df(table_schema.ColumnCount(), -1);
    auto get_or_init_df = [&](slot_t full_col) -> LSTableAggDictFilter& {
      if (col_to_df[full_col] < 0) {
        col_to_df[full_col] = static_cast<int>(lstable_agg_dict_filters.size());
        LSTableAggDictFilter df;
        df.full_column = full_col;
        const auto& dict = direct_lstable_store->dictionaries[full_col];
        const size_t n_entries = dict.value_type == ValueType::kVarChar
                                     ? dict.entries.size()
                                     : (dict.value_type == ValueType::kDouble
                                            ? dict.double_entries.size()
                                            : dict.int_entries.size());
        for (size_t code = 0; code < n_entries; ++code) {
          df.pass[code] = 1;
          df.pass_mask[code >> 6] |= uint64_t{1} << (code & 63);
        }
        lstable_agg_dict_filters.push_back(df);
      }
      return lstable_agg_dict_filters[static_cast<size_t>(col_to_df[full_col])];
    };
    for (size_t fi = 0; fi < plan.filter_compares.size(); ++fi) {
      const TypedFilterCompare& cmp = plan.filter_compares[fi];
      if (!cmp.constant_payload) {
        continue;
      }
      const slot_t full_col = proj_to_full[cmp.left];
      if (full_col >= direct_lstable_store->dictionaries.size() ||
          !direct_lstable_store->dictionaries[full_col].active) {
        continue;
      }
      const auto& dict = direct_lstable_store->dictionaries[full_col];
      if (cmp.string_payload && dict.value_type == ValueType::kVarChar) {
        LSTableAggDictFilter& df = get_or_init_df(full_col);
        df.pass_mask = {0ULL, 0ULL, 0ULL, 0ULL};
        for (size_t code = 0; code < dict.entries.size(); ++code) {
          if (df.pass[code] == 0) {
            continue;
          }
          const bool ok = (cmp.op == BinaryOperation::kEquals)
                              ? (dict.entries[code] == cmp.string_constant)
                              : (dict.entries[code] != cmp.string_constant);
          if (!ok) {
            df.pass[code] = 0;
          } else {
            df.pass_mask[code >> 6] |= uint64_t{1} << (code & 63);
          }
        }
        filter_compare_covered[fi] = 1;
      } else if (cmp.is_double && dict.value_type == ValueType::kDouble) {
        LSTableAggDictFilter& df = get_or_init_df(full_col);
        df.pass_mask = {0ULL, 0ULL, 0ULL, 0ULL};
        for (size_t code = 0; code < dict.double_entries.size(); ++code) {
          if (df.pass[code] == 0) {
            continue;
          }
          const double a = dict.double_entries[code];
          bool ok = false;
          switch (cmp.op) {
            case BinaryOperation::kEquals:
              ok = (a == cmp.double_constant);
              break;
            case BinaryOperation::kNotEquals:
              ok = (a != cmp.double_constant);
              break;
            case BinaryOperation::kLessThan:
              ok = (a < cmp.double_constant);
              break;
            case BinaryOperation::kLessThanEquals:
              ok = (a <= cmp.double_constant);
              break;
            case BinaryOperation::kGreaterThan:
              ok = (a > cmp.double_constant);
              break;
            default:
              ok = (a >= cmp.double_constant);
              break;
          }
          if (!ok) {
            df.pass[code] = 0;
          } else {
            df.pass_mask[code >> 6] |= uint64_t{1} << (code & 63);
          }
        }
        filter_compare_covered[fi] = 1;
      }
    }
    for (size_t li = 0; li < plan.inlist_compares.size(); ++li) {
      const TypedInListCompare& list = plan.inlist_compares[li];
      const slot_t full_col = proj_to_full[list.column];
      if (full_col >= direct_lstable_store->dictionaries.size() ||
          !direct_lstable_store->dictionaries[full_col].active) {
        continue;
      }
      const auto& dict = direct_lstable_store->dictionaries[full_col];
      if (list.is_string && dict.value_type == ValueType::kVarChar) {
        LSTableAggDictFilter& df = get_or_init_df(full_col);
        df.pass_mask = {0ULL, 0ULL, 0ULL, 0ULL};
        for (size_t code = 0; code < dict.entries.size(); ++code) {
          if (df.pass[code] == 0) {
            continue;
          }
          const std::string_view sv = dict.entries[code];
          const bool matched = MatchTypedStringInList(sv, list);
          if (!matched) {
            df.pass[code] = 0;
          } else {
            df.pass_mask[code >> 6] |= uint64_t{1} << (code & 63);
          }
        }
        inlist_compare_covered[li] = 1;
      } else if (list.is_double && dict.value_type == ValueType::kDouble) {
        LSTableAggDictFilter& df = get_or_init_df(full_col);
        df.pass_mask = {0ULL, 0ULL, 0ULL, 0ULL};
        for (size_t code = 0; code < dict.double_entries.size(); ++code) {
          if (df.pass[code] == 0) {
            continue;
          }
          const double dv = dict.double_entries[code];
          bool matched = false;
          for (const double item : list.double_values) {
            if (dv == item) {
              matched = true;
              break;
            }
          }
          if (!matched) {
            df.pass[code] = 0;
          } else {
            df.pass_mask[code >> 6] |= uint64_t{1} << (code & 63);
          }
        }
        inlist_compare_covered[li] = 1;
      } else if (!list.is_string && !list.is_double &&
                 dict.value_type == ValueType::kInt64) {
        LSTableAggDictFilter& df = get_or_init_df(full_col);
        df.pass_mask = {0ULL, 0ULL, 0ULL, 0ULL};
        for (size_t code = 0; code < dict.int_entries.size(); ++code) {
          if (df.pass[code] == 0) {
            continue;
          }
          const int64_t iv = dict.int_entries[code];
          bool matched = false;
          for (const int64_t item : list.int_values) {
            if (iv == item) {
              matched = true;
              break;
            }
          }
          if (!matched) {
            df.pass[code] = 0;
          } else {
            df.pass_mask[code >> 6] |= uint64_t{1} << (code & 63);
          }
        }
        inlist_compare_covered[li] = 1;
      }
    }
  }

  struct FastDictDoubleTerm {
    TypedDoubleExprInput::Shape shape{TypedDoubleExprInput::Shape::kGeneric};
    TypedIntExprInput::Shape int_shape{TypedIntExprInput::Shape::kGeneric};
    bool is_int_term{false};
    slot_t full_col0{0};
    slot_t full_col1{0};
    slot_t full_col2{0};
    double k0{0.0};
    double k1{0.0};
    size_t primary_agg_idx{0};
    bool reuse_prev_mul_const_minus{false};
  };
  bool fast_dict_double_agg = false;
  std::vector<FastDictDoubleTerm> fast_dict_terms;
  std::vector<std::pair<size_t, size_t>> fast_dict_dup_aggs;
  bool all_fast_dict_filters_covered = use_direct_lstable;
  if (use_direct_lstable) {
    for (size_t fi = 0; fi < plan.filter_compares.size(); ++fi) {
      if (filter_compare_covered[fi] != 0) {
        continue;
      }
      const TypedFilterCompare& cmp = plan.filter_compares[fi];
      if (!cmp.constant_payload || cmp.is_double || cmp.string_payload) {
        all_fast_dict_filters_covered = false;
        break;
      }
      const slot_t full_col = proj_to_full[cmp.left];
      const bool in_peek = std::ranges::any_of(
          peek_compares, [&](const IntegerPeekCompare& peek) {
            return peek.column == full_col && peek.op == cmp.op &&
                   peek.constant == cmp.int_constant;
          });
      if (!in_peek) {
        all_fast_dict_filters_covered = false;
        break;
      }
    }
    for (size_t li = 0;
         all_fast_dict_filters_covered && li < plan.inlist_compares.size();
         ++li) {
      if (inlist_compare_covered[li] == 0) {
        all_fast_dict_filters_covered = false;
        break;
      }
    }
  }
  if (use_direct_lstable &&
      (dict_group_key || bounded_int_group_key || plan.key_offsets.empty()) &&
      plan.slim && plan.filter_programs.empty() &&
      all_fast_dict_filters_covered && plan.aggregates.size() <= 8) {
    bool eligible = true;
    const auto col_no_null = [&](slot_t full_col) noexcept -> bool {
      return full_col < direct_lstable_store->column_has_null.size() &&
             direct_lstable_store->column_has_null[full_col] == 0;
    };
    const auto safe_int_col_bounds = [&](slot_t full_col, __int128_t* lo_out,
                                         __int128_t* hi_out) noexcept -> bool {
      if (!col_no_null(full_col) ||
          full_col >= direct_lstable_store->column_min_int.size() ||
          direct_lstable_store->column_min_int[full_col] >
              direct_lstable_store->column_max_int[full_col]) {
        return false;
      }
      *lo_out = direct_lstable_store->column_min_int[full_col];
      *hi_out = direct_lstable_store->column_max_int[full_col];
      return true;
    };
    const auto safe_int_sum_bound = [&](__int128_t lo, __int128_t hi,
                                        __int128_t limit) noexcept -> bool {
      if (lo < std::numeric_limits<int64_t>::min() ||
          hi > std::numeric_limits<int64_t>::max()) {
        return false;
      }
      const __int128_t max_abs = std::max(lo < 0 ? -lo : lo, hi < 0 ? -hi : hi);
      const __int128_t nrows =
          std::max<__int128_t>(direct_lstable_store->total_rows, 1);
      return max_abs <= limit / nrows;
    };
    for (const IntegerPeekCompare& pred : peek_compares) {
      if (!col_no_null(pred.column)) {
        eligible = false;
        break;
      }
    }
    for (const LSTableAggDictFilter& df : lstable_agg_dict_filters) {
      if (!col_no_null(df.full_column)) {
        eligible = false;
        break;
      }
    }
    if (eligible && stream_prebuilt_membership != nullptr &&
        !col_no_null(proj_to_full[stream_prebuilt_membership->column])) {
      eligible = false;
    }
    for (size_t i = 0; eligible && i < plan.aggregates.size(); ++i) {
      const TypedAggregateInput& input = plan.aggregates[i];
      const SlimAggKind kind = plan.slim_kinds[i];
      if (input.conditional.has_value() || input.child.has_value()) {
        eligible = false;
        break;
      }
      if (kind == SlimAggKind::kCount) {
        if (input.column.has_value() &&
            !col_no_null(proj_to_full[*input.column])) {
          eligible = false;
          break;
        }
        if (input.double_expr.has_value() || input.int_expr.has_value()) {
          eligible = false;
          break;
        }
        continue;
      }
      if (kind != SlimAggKind::kSumDouble && kind != SlimAggKind::kAvgDouble &&
          kind != SlimAggKind::kSumInt64) {
        eligible = false;
        break;
      }
      FastDictDoubleTerm term;
      if (input.column.has_value() &&
          proj_types[*input.column] == ValueType::kDouble &&
          (kind == SlimAggKind::kSumDouble ||
           kind == SlimAggKind::kAvgDouble)) {
        term.shape = TypedDoubleExprInput::Shape::kSingleDoubleCol;
        term.full_col0 = proj_to_full[*input.column];
        if (!col_no_null(term.full_col0)) {
          eligible = false;
          break;
        }
      } else if (input.double_expr.has_value() &&
                 input.double_expr->shape !=
                     TypedDoubleExprInput::Shape::kGeneric &&
                 (kind == SlimAggKind::kSumDouble ||
                  kind == SlimAggKind::kAvgDouble)) {
        term.shape = input.double_expr->shape;
        term.full_col0 = proj_to_full[input.double_expr->col0];
        term.full_col1 = proj_to_full[input.double_expr->col1];
        term.full_col2 = proj_to_full[input.double_expr->col2];
        term.k0 = input.double_expr->k0;
        term.k1 = input.double_expr->k1;
        if (!col_no_null(term.full_col0) ||
            (term.shape != TypedDoubleExprInput::Shape::kSingleDoubleCol &&
             !col_no_null(term.full_col1)) ||
            (term.shape == TypedDoubleExprInput::Shape::
                               kMulConstMinusColMulConstPlusCol &&
             !col_no_null(term.full_col2))) {
          eligible = false;
          break;
        }
      } else if (!dict_group_key &&
                 (kind == SlimAggKind::kSumInt64 ||
                  kind == SlimAggKind::kAvgDouble) &&
                 input.column.has_value() &&
                 proj_types[*input.column] == ValueType::kInt64 &&
                 proj_unsigned[*input.column] == 0) {
        const slot_t full0 = proj_to_full[*input.column];
        __int128_t lo0 = 0;
        __int128_t hi0 = 0;
        const __int128_t limit =
            kind == SlimAggKind::kAvgDouble
                ? static_cast<__int128_t>(int64_t{1} << 53)
                : static_cast<__int128_t>(std::numeric_limits<int64_t>::max());
        if (!safe_int_col_bounds(full0, &lo0, &hi0) ||
            !safe_int_sum_bound(lo0, hi0, limit)) {
          eligible = false;
          break;
        }
        term.is_int_term = true;
        term.int_shape = TypedIntExprInput::Shape::kSingleIntCol;
        term.full_col0 = full0;
      } else if (!dict_group_key &&
                 (kind == SlimAggKind::kSumInt64 ||
                  kind == SlimAggKind::kAvgDouble) &&
                 input.int_expr.has_value() &&
                 input.int_expr->shape != TypedIntExprInput::Shape::kGeneric) {
        const TypedIntExprInput& ie = *input.int_expr;
        const slot_t full0 = proj_to_full[ie.col0];
        const slot_t full1 = proj_to_full[ie.col1];
        const slot_t full2 = proj_to_full[ie.col2];
        __int128_t lo0 = 0, hi0 = 0;
        if (!safe_int_col_bounds(full0, &lo0, &hi0)) {
          eligible = false;
          break;
        }
        __int128_t lo = lo0, hi = hi0;
        if (ie.shape != TypedIntExprInput::Shape::kSingleIntCol) {
          __int128_t lo1 = 0, hi1 = 0;
          if (!safe_int_col_bounds(full1, &lo1, &hi1)) {
            eligible = false;
            break;
          }
          const __int128_t p0 = lo0 * lo1;
          const __int128_t p1 = lo0 * hi1;
          const __int128_t p2 = hi0 * lo1;
          const __int128_t p3 = hi0 * hi1;
          lo = std::min({p0, p1, p2, p3});
          hi = std::max({p0, p1, p2, p3});
          if (lo < std::numeric_limits<int64_t>::min() ||
              hi > std::numeric_limits<int64_t>::max()) {
            eligible = false;
            break;
          }
          if (ie.shape == TypedIntExprInput::Shape::kMulTwoIntColsPlusCol) {
            __int128_t lo2 = 0, hi2 = 0;
            if (!safe_int_col_bounds(full2, &lo2, &hi2)) {
              eligible = false;
              break;
            }
            lo += lo2;
            hi += hi2;
          }
        }
        const __int128_t limit =
            kind == SlimAggKind::kAvgDouble
                ? static_cast<__int128_t>(int64_t{1} << 53)
                : static_cast<__int128_t>(std::numeric_limits<int64_t>::max());
        if (!safe_int_sum_bound(lo, hi, limit)) {
          eligible = false;
          break;
        }
        term.is_int_term = true;
        term.int_shape = ie.shape;
        term.full_col0 = full0;
        term.full_col1 = full1;
        term.full_col2 = full2;
      } else {
        eligible = false;
        break;
      }
      bool found_dup = false;
      for (const FastDictDoubleTerm& existing : fast_dict_terms) {
        if (existing.is_int_term == term.is_int_term &&
            existing.shape == term.shape &&
            existing.int_shape == term.int_shape &&
            existing.full_col0 == term.full_col0 &&
            existing.full_col1 == term.full_col1 &&
            existing.full_col2 == term.full_col2 && existing.k0 == term.k0 &&
            existing.k1 == term.k1) {
          fast_dict_dup_aggs.emplace_back(i, existing.primary_agg_idx);
          found_dup = true;
          break;
        }
      }
      if (!found_dup) {
        term.primary_agg_idx = i;
        if (!term.is_int_term && !fast_dict_terms.empty() &&
            term.shape ==
                TypedDoubleExprInput::Shape::kMulConstMinusColMulConstPlusCol) {
          const FastDictDoubleTerm& prev = fast_dict_terms.back();
          if (!prev.is_int_term &&
              prev.shape == TypedDoubleExprInput::Shape::kMulConstMinusCol &&
              prev.full_col0 == term.full_col0 &&
              prev.full_col1 == term.full_col1 && prev.k0 == term.k0) {
            term.reuse_prev_mul_const_minus = true;
          }
        }
        fast_dict_terms.push_back(term);
      }
    }
    if (bounded_int_group_key && fast_dict_terms.size() > 1) {
      eligible = false;
    }
    fast_dict_double_agg = eligible;
  }
  const bool bounded_has_double_term =
      fast_dict_double_agg && bounded_int_group_key &&
      !fast_dict_terms.empty() && !fast_dict_terms[0].is_int_term;
  const bool bounded_has_int_term =
      fast_dict_double_agg && bounded_int_group_key &&
      !fast_dict_terms.empty() && fast_dict_terms[0].is_int_term;
  std::vector<std::vector<double>> bounded_worker_sums(
      bounded_has_double_term ? worker_count : 0);
  std::vector<std::vector<int64_t>> bounded_worker_int_sums(
      bounded_has_int_term ? worker_count : 0);
  std::vector<std::vector<uint32_t>> bounded_worker_counts(
      (fast_dict_double_agg && bounded_int_group_key) ? worker_count : 0);

  const auto check_slim_group_having = [&](TypedWorkerGroups& local,
                                           size_t group, Status* err) -> bool {
    for (const PositionalHaving& clause : positional_having) {
      const size_t i = clause.agg_index;
      const int64_t count = local.SlimCount(group, i);
      Value finished;
      switch ((*local.slim_kinds)[i]) {
        case SlimAggKind::kCount:
          finished = Value(count);
          break;
        case SlimAggKind::kSumInt64:
          finished = count == 0 ? Value() : Value(local.SlimIntTotal(group, i));
          break;
        case SlimAggKind::kSumDouble: {
          if (count == 0) {
            break;
          }
          const double sum_result = local.SlimDoubleTotal(group, i);
          if ((local.SlimFlags(group, i) & 1) == 0 && std::isinf(sum_result)) {
            *err = StatusError(StatusCode::kIsInfinity,
                               "SUM overflow: result is infinite");
            return false;
          }
          finished = Value(sum_result);
          break;
        }
        case SlimAggKind::kAvgDouble:
          finished = count == 0 ? Value()
                                : Value(local.SlimDoubleTotal(group, i) /
                                        static_cast<double>(count));
          break;
        case SlimAggKind::kCountDistinctInt64:
          finished =
              Value(static_cast<int64_t>(local.SlimDistinctTotal(group, i)));
          break;
        default:
          *err = StatusError(StatusCode::kInvalidArgument,
                             "aggregate has no slim state");
          return false;
      }
      if (!clause.has_constant) {
        if (!Truthy(finished)) {
          return false;
        }
        continue;
      }
      if (finished.IsNull() || clause.constant.IsNull()) {
        return false;
      }
      if (finished.type == ValueType::kInt64 &&
          clause.constant.type == ValueType::kInt64 && !finished.IsUnsigned() &&
          !clause.constant.IsUnsigned()) {
        const int64_t a = finished.value.int_value;
        const int64_t b = clause.constant.value.int_value;
        bool ok = false;
        switch (clause.op) {
          case BinaryOperation::kEquals:
            ok = (a == b);
            break;
          case BinaryOperation::kNotEquals:
            ok = (a != b);
            break;
          case BinaryOperation::kLessThan:
            ok = (a < b);
            break;
          case BinaryOperation::kLessThanEquals:
            ok = (a <= b);
            break;
          case BinaryOperation::kGreaterThan:
            ok = (a > b);
            break;
          default:
            ok = (a >= b);
            break;
        }
        if (!ok) {
          return false;
        }
        continue;
      }
      if ((finished.type == ValueType::kDouble ||
           finished.type == ValueType::kInt64) &&
          (clause.constant.type == ValueType::kDouble ||
           clause.constant.type == ValueType::kInt64) &&
          !finished.IsUnsigned() && !clause.constant.IsUnsigned()) {
        const double a = finished.type == ValueType::kDouble
                             ? finished.value.double_value
                             : static_cast<double>(finished.value.int_value);
        const double b =
            clause.constant.type == ValueType::kDouble
                ? clause.constant.value.double_value
                : static_cast<double>(clause.constant.value.int_value);
        bool ok = false;
        switch (clause.op) {
          case BinaryOperation::kEquals:
            ok = (a == b);
            break;
          case BinaryOperation::kNotEquals:
            ok = (a != b);
            break;
          case BinaryOperation::kLessThan:
            ok = (a < b);
            break;
          case BinaryOperation::kLessThanEquals:
            ok = (a <= b);
            break;
          case BinaryOperation::kGreaterThan:
            ok = (a > b);
            break;
          default:
            ok = (a >= b);
            break;
        }
        if (!ok) {
          return false;
        }
        continue;
      }
      StatusOr<Value> compared =
          TryBinary(clause.op, finished, clause.constant);
      if (!compared.HasValue()) {
        *err = compared.GetStatus();
        return false;
      }
      if (!Truthy(compared.Value())) {
        return false;
      }
    }
    return true;
  };
  const auto run_worker = [&](size_t worker) {
    try {
      std::vector<Value> scratch;
      scratch.reserve(plan.key_offsets.size());
      if (cached_relation != nullptr && !cached_relation->HasSpill()) {
        // Cache hit: partition the resident rows into contiguous ranges.  A
        // typed fill publishes the arena WITHOUT Rows (resized-but-unboxed
        // once a presize ran); the arena lookup therefore cannot key on
        // rows.empty().
        const std::vector<Row>& rows = cached_relation->rows;
        std::shared_ptr<const BaseRelationArena> hit_arena;
        if (runtime != nullptr) {
          auto found_arena = runtime->base_relation_arenas.find(fold_cache_key);
          if (found_arena != runtime->base_relation_arenas.end()) {
            hit_arena = found_arena->second;
          }
        }
        const size_t total =
            !rows.empty() ? rows.size() : (hit_arena ? hit_arena->rows : 0);
        const size_t begin = total * worker / worker_count;
        const size_t end = total * (worker + 1) / worker_count;
        // Scratch arrays for the arena fold below; every slot is written
        // before it is read within a row, so they carry no cross-row state.
        std::array<Value, 8> cache_hit_values{};
        std::array<const Value*, 8> cache_hit_value_ptrs{};
        // Arena fast path: an int64 mirror of the cache lets the fold stream
        // raw cells instead of visiting per-Row heap blocks.  Requires a
        // single int64/date group key, no bytecode filter residual, slim
        // states, and column/conditional/double_expr (or COUNT(*)) aggregate
        // inputs; anything else keeps the Row loop below.
        std::shared_ptr<const BaseRelationArena> arena;
        bool arena_ok = !arena_disabled && workers[worker].slim && int_key &&
                        plan.filter_programs.empty() &&
                        plan.aggregates.size() <= 8;
        if (arena_ok) {
          for (const TypedAggregateInput& input : plan.aggregates) {
            const auto aggregate_index =
                static_cast<size_t>(&input - plan.aggregates.data());
            if (input.child.has_value() ||
                (!input.column.has_value() && !input.conditional.has_value() &&
                 !input.double_expr.has_value() &&
                 !input.int_expr.has_value() &&
                 (*workers[worker].slim_kinds)[aggregate_index] !=
                     SlimAggKind::kCount)) {
              arena_ok = false;
              break;
            }
          }
        }
        if (arena_ok && hit_arena != nullptr && hit_arena->rows == total) {
          arena = hit_arena;
        }
        if (arena != nullptr) {
          const size_t stride = arena->stride;
          std::vector<Value> fallback_row_values;
          std::vector<int64_t> fallback_cells(stride);
          constexpr size_t kArenaPrefetchDistance = 8;
          for (size_t index = begin; index < end; ++index) {
            if (worker_errors[worker] != Status::kSuccess) {
              return;
            }
            ++scanned[worker];
            const int64_t* cells = arena->cells.data() + (index * stride);
            const auto arena_is_null = [&](slot_t c) {
              return arena->IsNull(index, c);
            };
            const auto arena_make_value = [&](slot_t c) {
              if (arena->slot_types.size() == stride &&
                  arena->slot_types[c] == ValueType::kVarChar) {
                Value v;
                v.type = ValueType::kVarChar;
                v.value.varchar_value =
                    arena->Varchar(index, varchar_ordinal[c]);
                return v;
              }
              return ArenaCellValue(*arena, cells, c, qualified_schema);
            };
            // The cells and null bitmap stream linearly (hardware prefetch
            // covers them); the group-slot probe is the random access, so
            // warm it one window ahead of the row that needs it.
            if (index + kArenaPrefetchDistance < end) {
              const int64_t* ahead = cells + (kArenaPrefetchDistance * stride);
              __builtin_prefetch(ahead);
              __builtin_prefetch(ahead + plan.key_offsets[0]);
              if (!arena->IsNull(index + kArenaPrefetchDistance,
                                 plan.key_offsets[0])) {
                workers[worker].PrefetchGroupSlot(ahead[plan.key_offsets[0]]);
              }
            }
            if (arena->IsNull(index, plan.key_offsets[0])) {
              // NULL keys keep their own group through the canonical row
              // path; materialize the row lazily for the rare case.
              fallback_row_values.clear();
              for (size_t c = 0; c < stride; ++c) {
                if (arena->IsNull(index, c)) {
                  fallback_row_values.emplace_back();
                } else {
                  fallback_row_values.push_back(arena_make_value(c));
                }
              }
              Row fallback_row(std::move(fallback_row_values));
              accumulate_row(fallback_row, &workers[worker],
                             &worker_errors[worker], &emitted[worker],
                             &aggregate_updates[worker], nullptr, &scratch);
              fallback_row_values = std::move(fallback_row.values_);
              continue;
            }
            bool keep = true;
            for (const TypedFilterCompare& compare : plan.filter_compares) {
              if (!DecideTypedFilterCompareCells(compare, cells, arena_is_null,
                                                 arena_make_value)) {
                keep = false;
                break;
              }
            }
            if (keep) {
              for (const TypedInListCompare& list : plan.inlist_compares) {
                if (arena->IsNull(index, list.column)) {
                  keep = false;
                  break;
                }
                bool matched = false;
                if (list.is_string) {
                  matched = MatchTypedStringInList(
                      arena->Varchar(index, varchar_ordinal[list.column]),
                      list);
                } else if (list.is_double) {
                  const auto v = std::bit_cast<double>(cells[list.column]);
                  for (const double item : list.double_values) {
                    if (v == item) {
                      matched = true;
                      break;
                    }
                  }
                } else {
                  const int64_t cell = cells[list.column];
                  for (const int64_t item : list.int_values) {
                    if (cell == item) {
                      matched = true;
                      break;
                    }
                  }
                }
                if (!matched) {
                  keep = false;
                  break;
                }
              }
            }
            if (!keep) {
              continue;
            }
            ++emitted[worker];
            bool inserted = false;
            const int64_t raw_key = cells[plan.key_offsets[0]];
            size_t* slot = workers[worker].RawIntSlot(raw_key, &inserted);
            size_t group_index = 0;
            if (inserted) {
              scratch.clear();
              for (const slot_t offset : plan.key_offsets) {
                Value key_value;
                key_value.type = qualified_schema.GetColumn(offset).Type();
                key_value.value.int_value = cells[offset];
                scratch.push_back(CanonicalDistinctValue(key_value));
              }
              group_index = workers[worker].ClaimSlotForNewGroup(
                  slot, scratch, plan.aggregates.size(), aggregate_expressions);
            } else {
              group_index = *slot;
            }
            ++aggregate_updates[worker];
            std::array<Value, 8>& stack_values = cache_hit_values;
            std::array<const Value*, 8>& value_ptrs = cache_hit_value_ptrs;
            for (size_t i = 0; i < plan.aggregates.size(); ++i) {
              const TypedAggregateInput& input = plan.aggregates[i];
              if (input.column.has_value()) {
                const slot_t column = *input.column;
                if (arena->IsNull(index, column)) {
                  stack_values[i] = Value();
                } else {
                  stack_values[i] = arena_make_value(column);
                }
                value_ptrs[i] = &stack_values[i];
                continue;
              }
              if (input.conditional.has_value()) {
                stack_values[i] = Value();
                if (DecideTypedFilterCompareCells(input.conditional->condition,
                                                  cells, arena_is_null,
                                                  arena_make_value)) {
                  if (input.conditional->then_column.has_value()) {
                    const slot_t col = *input.conditional->then_column;
                    if (!arena->IsNull(index, col)) {
                      stack_values[i] = arena_make_value(col);
                    }
                  } else {
                    stack_values[i] =
                        Value(input.conditional->then_int_constant);
                    stack_values[i].type = input.conditional->result_type;
                  }
                }
                value_ptrs[i] = &stack_values[i];
                continue;
              }
              if (input.double_expr.has_value()) {
                double dbl = 0.0;
                stack_values[i] =
                    EvaluateTypedDoubleExprCells(*input.double_expr, cells,
                                                 arena_is_null, &dbl)
                        ? Value(dbl)
                        : Value();
                value_ptrs[i] = &stack_values[i];
                continue;
              }
              if (input.int_expr.has_value()) {
                int64_t iv = 0;
                const bool ok = EvaluateTypedIntExprCells(
                    *input.int_expr, cells, arena_is_null, &iv,
                    &worker_errors[worker]);
                if (worker_errors[worker] != Status::kSuccess) {
                  return;
                }
                stack_values[i] = ok ? Value(iv) : Value();
                value_ptrs[i] = &stack_values[i];
                continue;
              }
              value_ptrs[i] = nullptr;  // COUNT(*) sentinel.
            }
            for (size_t i = 0; i < plan.aggregates.size(); ++i) {
              if (!ApplySlimValue(&workers[worker], group_index, i,
                                  (*workers[worker].slim_kinds)[i],
                                  value_ptrs[i], &worker_errors[worker])) {
                return;
              }
            }
          }
          return;
        }
        // Rows own their cells in a separate heap block, so stream the Value
        // arrays into cache ahead of the consume point; the row headers
        // alone prefetch well in hardware.
        constexpr size_t kPrefetchDistance = 8;
        for (size_t index = begin; index < end; ++index) {
          if (index + kPrefetchDistance < end) {
            const Row& ahead = rows[index + kPrefetchDistance];
            __builtin_prefetch(&ahead, /*write=*/0, /*locality=*/1);
            __builtin_prefetch(ahead.values_.data(), /*write=*/0,
                               /*locality=*/1);
          }
          if (worker_errors[worker] != Status::kSuccess) {
            return;
          }
          ++scanned[worker];
          accumulate_row(rows[index], &workers[worker], &worker_errors[worker],
                         &emitted[worker], &aggregate_updates[worker], nullptr,
                         &scratch);
        }
        return;
      }
      if (cached_relation != nullptr) {
        // Spilled cache: worker 0 streams it serially.
        if (worker != 0) {
          return;
        }
        Status error = Status::kSuccess;
        size_t rows_output = 0;
        size_t updates = 0;
        cached_relation->FinishSpill();
        cached_relation->ForEachRow([&](const Row& row) {
          if (error != Status::kSuccess) {
            return;
          }
          ++scanned[worker];
          accumulate_row(row, &workers[worker], &error, &rows_output, &updates,
                         nullptr, &scratch);
        });
        worker_errors[worker] = error;
        emitted[worker] += rows_output;
        aggregate_updates[worker] += updates;
        return;
      }
      // Static rotation, same contract as the grouped-chunk fold above:
      // worker w scans morsels w, w+W, ... so the per-worker partial sums and
      // the worker-order merge are deterministic (rank boundaries in the
      // ROLLUP queries read these doubles).
      // Per-worker scratch arrays for the fold; every slot is written before
      // it is read within a row, so they carry no cross-row state.
      std::array<Value, 8> fold_row_values{};
      const size_t morsel_begin =
          fill_cache ? worker : (morsels.size() * worker) / worker_count;
      const size_t morsel_end =
          fill_cache ? morsels.size()
                     : (morsels.size() * (worker + 1)) / worker_count;
      const size_t morsel_step = fill_cache ? worker_count : 1;
      const RuntimeFilterGroup* active_rf =
          fill_cache ? nullptr : stream_runtime_filter;
      const std::optional<slot_t> active_key_col =
          active_rf != nullptr ? stream_key_column : std::nullopt;
      const ValueType active_rf_type =
          active_rf != nullptr ? stream_runtime_filter_type : ValueType::kNull;

      if (use_direct_lstable) {
        const size_t total_blocks = direct_lstable_store->blocks.size();
        const size_t blk_begin = (total_blocks * worker) / worker_count;
        const size_t blk_end = (total_blocks * (worker + 1)) / worker_count;
        TypedWorkerGroups& local = workers[worker];
        Status& err = worker_errors[worker];

        const RuntimeFilterGroup* lstable_rf = nullptr;
        if (active_rf != nullptr && !active_rf->empty() &&
            active_key_col.has_value() &&
            *active_key_col < table_schema.ColumnCount()) {
          const Column& kcol = table_schema.GetColumn(*active_key_col);
          if ((kcol.Type() == ValueType::kInt64 ||
               kcol.Type() == ValueType::kDate) &&
              !kcol.IsUnsigned() &&
              (active_rf_type == ValueType::kNull ||
               active_rf_type == kcol.Type())) {
            lstable_rf = active_rf;
          }
        }

        if (bulk_count && lstable_rf == nullptr &&
            lstable_agg_dict_filters.empty()) {
          size_t matching_rows = 0;
          for (size_t bi = blk_begin; bi < blk_end; ++bi) {
            const LSTablePageBlock& blk = direct_lstable_store->blocks[bi];
            if (blk.row_count == 0 ||
                !blk.MatchesZoneMaps(peek_ptr, nullptr, std::nullopt)) {
              continue;
            }
            if (peek_ptr == nullptr || peek_ptr->empty()) {
              scanned[worker] += blk.row_count;
              matching_rows += blk.row_count;
              continue;
            }
            if (peek_ptr->size() == 1 &&
                !blk.columns[(*peek_ptr)[0].column].has_null) {
              const IntegerPeekCompare& pred = (*peek_ptr)[0];
              const int64_t* __restrict pcells =
                  blk.columns[pred.column].int_cells.data();
              const int64_t k = pred.constant;
              const uint32_t nrows = blk.row_count;
              size_t blk_cnt = 0;
              switch (pred.op) {
                case BinaryOperation::kGreaterThan:
                  for (uint32_t r = 0; r < nrows; ++r) {
                    blk_cnt += static_cast<size_t>(pcells[r] > k);
                  }
                  break;
                case BinaryOperation::kGreaterThanEquals:
                  for (uint32_t r = 0; r < nrows; ++r) {
                    blk_cnt += static_cast<size_t>(pcells[r] >= k);
                  }
                  break;
                case BinaryOperation::kLessThan:
                  for (uint32_t r = 0; r < nrows; ++r) {
                    blk_cnt += static_cast<size_t>(pcells[r] < k);
                  }
                  break;
                case BinaryOperation::kLessThanEquals:
                  for (uint32_t r = 0; r < nrows; ++r) {
                    blk_cnt += static_cast<size_t>(pcells[r] <= k);
                  }
                  break;
                case BinaryOperation::kEquals:
                  for (uint32_t r = 0; r < nrows; ++r) {
                    blk_cnt += static_cast<size_t>(pcells[r] == k);
                  }
                  break;
                case BinaryOperation::kNotEquals:
                  for (uint32_t r = 0; r < nrows; ++r) {
                    blk_cnt += static_cast<size_t>(pcells[r] != k);
                  }
                  break;
                default:
                  break;
              }
              scanned[worker] += blk_cnt;
              matching_rows += blk_cnt;
              continue;
            }
            for (uint32_t r = 0; r < blk.row_count; ++r) {
              bool ok = true;
              for (const IntegerPeekCompare& pred : *peek_ptr) {
                const LSTableColumnBlock& pcol = blk.columns[pred.column];
                if (pcol.IsNull(r) ||
                    !MatchIntegerCompare(pcol.int_cells[r], pred.op,
                                         pred.constant)) {
                  ok = false;
                  break;
                }
              }
              if (ok) {
                ++scanned[worker];
                ++matching_rows;
              }
            }
          }
          if (matching_rows > 0) {
            bool inserted = false;
            const size_t group =
                local.FindOrInsert({}, 0, plan.aggregates.size(),
                                   aggregate_expressions, &inserted);
            for (size_t agg = 0; agg < plan.aggregates.size(); ++agg) {
              auto& count = local.SlimCount(group, agg);
              if (std::cmp_greater(
                      matching_rows,
                      std::numeric_limits<int64_t>::max() - count)) {
                err = StatusError(StatusCode::kIsInfinity,
                                  "integer overflow in COUNT");
                break;
              }
              count += static_cast<int64_t>(matching_rows);
            }
            emitted[worker] += matching_rows;
            aggregate_updates[worker] += matching_rows;
          }
          return;
        }

        std::array<uint16_t, 256> dict_pair_to_group{};
        dict_pair_to_group.fill(0xFFFFU);
        if (fast_dict_double_agg && bounded_int_group_key) {
          if (bounded_has_double_term) {
            bounded_worker_sums[worker].assign(bounded_int_span, 0.0);
          }
          if (bounded_has_int_term) {
            bounded_worker_int_sums[worker].assign(bounded_int_span, 0);
          }
          bounded_worker_counts[worker].assign(bounded_int_span, 0U);
        }
        bool has_ungrouped = false;
        size_t ungrouped_idx = 0;
        bool has_mono_key = false;
        int64_t cur_mono_key = 0;
        size_t cur_mono_group = 0;
        bool have_rf_cache = false;
        int64_t last_rf_key = 0;
        bool last_rf_rejected = false;
        const slot_t mono_key_full_col =
            monotonic_int_key ? proj_to_full[plan.key_offsets[0]] : 0;
        const ValueType mono_key_type = monotonic_int_key
                                            ? proj_types[plan.key_offsets[0]]
                                            : ValueType::kNull;
        const bool mono_key_unsigned =
            monotonic_int_key && proj_unsigned[plan.key_offsets[0]] != 0;

        for (size_t bi = blk_begin; bi < blk_end; ++bi) {
          if (err != Status::kSuccess) {
            break;
          }
          const LSTablePageBlock& blk = direct_lstable_store->blocks[bi];
          if (blk.row_count == 0) {
            continue;
          }
          if (!blk.MatchesZoneMaps(peek_ptr, lstable_rf, active_key_col)) {
            continue;
          }
          bool zone_rejected = false;
          for (const LSTableAggDictFilter& df : lstable_agg_dict_filters) {
            const LSTableColumnBlock& dcol = blk.columns[df.full_column];
            if (((dcol.dict_mask[0] & df.pass_mask[0]) |
                 (dcol.dict_mask[1] & df.pass_mask[1]) |
                 (dcol.dict_mask[2] & df.pass_mask[2]) |
                 (dcol.dict_mask[3] & df.pass_mask[3])) == 0) {
              zone_rejected = true;
              break;
            }
          }
          if (zone_rejected) {
            continue;
          }
          for (const TypedFilterCompare& cmp : plan.filter_compares) {
            if (!cmp.constant_payload || !cmp.is_double) {
              continue;
            }
            const LSTableColumnBlock& dcol =
                blk.columns[proj_to_full[cmp.left]];
            if (!dcol.has_double_min_max) {
              zone_rejected = true;
              break;
            }
            switch (cmp.op) {
              case BinaryOperation::kEquals:
                if (cmp.double_constant < dcol.min_double ||
                    cmp.double_constant > dcol.max_double) {
                  zone_rejected = true;
                }
                break;
              case BinaryOperation::kNotEquals:
                if (dcol.min_double == cmp.double_constant &&
                    dcol.max_double == cmp.double_constant) {
                  zone_rejected = true;
                }
                break;
              case BinaryOperation::kLessThan:
                if (dcol.min_double >= cmp.double_constant) {
                  zone_rejected = true;
                }
                break;
              case BinaryOperation::kLessThanEquals:
                if (dcol.min_double > cmp.double_constant) {
                  zone_rejected = true;
                }
                break;
              case BinaryOperation::kGreaterThan:
                if (dcol.max_double <= cmp.double_constant) {
                  zone_rejected = true;
                }
                break;
              case BinaryOperation::kGreaterThanEquals:
                if (dcol.max_double < cmp.double_constant) {
                  zone_rejected = true;
                }
                break;
              default:
                break;
            }
            if (zone_rejected) {
              break;
            }
          }
          if (zone_rejected) {
            continue;
          }

          bool dict_all_pass = !blk.has_any_null;
          if (dict_all_pass) {
            for (const LSTableAggDictFilter& df : lstable_agg_dict_filters) {
              const LSTableColumnBlock& dcol = blk.columns[df.full_column];
              if (((dcol.dict_mask[0] & ~df.pass_mask[0]) |
                   (dcol.dict_mask[1] & ~df.pass_mask[1]) |
                   (dcol.dict_mask[2] & ~df.pass_mask[2]) |
                   (dcol.dict_mask[3] & ~df.pass_mask[3])) != 0) {
                dict_all_pass = false;
                break;
              }
            }
          }
          bool peek_all_pass = !blk.has_any_null && lstable_rf == nullptr &&
                               active_rf == nullptr;
          if (peek_all_pass) {
            for (size_t fi = 0; fi < plan.filter_compares.size(); ++fi) {
              if (filter_compare_covered[fi] != 0) {
                continue;
              }
              const TypedFilterCompare& cmp = plan.filter_compares[fi];
              if (!cmp.constant_payload || cmp.string_payload) {
                peek_all_pass = false;
                break;
              }
              const LSTableColumnBlock& col =
                  blk.columns[proj_to_full[cmp.left]];
              if (cmp.is_double) {
                if (!col.has_double_min_max) {
                  peek_all_pass = false;
                  break;
                }
                switch (cmp.op) {
                  case BinaryOperation::kEquals:
                    if (col.min_double != cmp.double_constant ||
                        col.max_double != cmp.double_constant) {
                      peek_all_pass = false;
                    }
                    break;
                  case BinaryOperation::kNotEquals:
                    if (cmp.double_constant >= col.min_double &&
                        cmp.double_constant <= col.max_double) {
                      peek_all_pass = false;
                    }
                    break;
                  case BinaryOperation::kLessThan:
                    if (col.max_double >= cmp.double_constant) {
                      peek_all_pass = false;
                    }
                    break;
                  case BinaryOperation::kLessThanEquals:
                    if (col.max_double > cmp.double_constant) {
                      peek_all_pass = false;
                    }
                    break;
                  case BinaryOperation::kGreaterThan:
                    if (col.min_double <= cmp.double_constant) {
                      peek_all_pass = false;
                    }
                    break;
                  default:
                    if (col.min_double < cmp.double_constant) {
                      peek_all_pass = false;
                    }
                    break;
                }
              } else {
                if (!col.has_min_max) {
                  peek_all_pass = false;
                  break;
                }
                switch (cmp.op) {
                  case BinaryOperation::kEquals:
                    if (col.min_int != cmp.int_constant ||
                        col.max_int != cmp.int_constant) {
                      peek_all_pass = false;
                    }
                    break;
                  case BinaryOperation::kNotEquals:
                    if (cmp.int_constant >= col.min_int &&
                        cmp.int_constant <= col.max_int) {
                      peek_all_pass = false;
                    }
                    break;
                  case BinaryOperation::kLessThan:
                    if (col.max_int >= cmp.int_constant) {
                      peek_all_pass = false;
                    }
                    break;
                  case BinaryOperation::kLessThanEquals:
                    if (col.max_int > cmp.int_constant) {
                      peek_all_pass = false;
                    }
                    break;
                  case BinaryOperation::kGreaterThan:
                    if (col.min_int <= cmp.int_constant) {
                      peek_all_pass = false;
                    }
                    break;
                  default:
                    if (col.min_int < cmp.int_constant) {
                      peek_all_pass = false;
                    }
                    break;
                }
              }
              if (!peek_all_pass) {
                break;
              }
            }
          }
          if (peek_all_pass) {
            for (size_t li = 0; li < plan.inlist_compares.size(); ++li) {
              if (inlist_compare_covered[li] == 0) {
                peek_all_pass = false;
                break;
              }
            }
          }
          const bool block_all_pass = peek_all_pass && dict_all_pass &&
                                      stream_prebuilt_membership == nullptr;

          if (block_all_pass && lstable_rf == nullptr &&
              (peek_ptr == nullptr || peek_ptr->empty()) && !blk.has_any_null &&
              monotonic_int_key && plan.aggregates.size() == 1 && local.slim &&
              (*local.slim_kinds)[0] == SlimAggKind::kSumDouble &&
              plan.aggregates[0].column.has_value() &&
              proj_types[*plan.aggregates[0].column] == ValueType::kDouble) {
            const int64_t* key_cells =
                blk.columns[mono_key_full_col].int_cells.data();
            const slot_t val_full_col =
                proj_to_full[*plan.aggregates[0].column];
            const LSTableColumnBlock& val_col = blk.columns[val_full_col];
            const int64_t* val_cells = val_col.int_cells.data();
            const bool use_val_dict =
                val_full_col < direct_lstable_store->dictionaries.size() &&
                direct_lstable_store->dictionaries[val_full_col].active &&
                direct_lstable_store->dictionaries[val_full_col].value_type ==
                    ValueType::kDouble &&
                !val_col.dict_codes.empty();
            const uint8_t* val_codes =
                use_val_dict ? val_col.dict_codes.data() : nullptr;
            const double* val_dict =
                use_val_dict ? direct_lstable_store->dictionaries[val_full_col]
                                   .double_entries.data()
                             : nullptr;
            const uint32_t nrows = blk.row_count;
            scanned[worker] += nrows;
            emitted[worker] += nrows;
            aggregate_updates[worker] += nrows;
            const bool can_fast_having =
                having_expr != nullptr && positional_having.size() == 1 &&
                positional_having[0].agg_index == 0 &&
                positional_having[0].has_constant &&
                !positional_having[0].constant.IsNull() &&
                !positional_having[0].constant.IsUnsigned() &&
                (positional_having[0].constant.type == ValueType::kDouble ||
                 positional_having[0].constant.type == ValueType::kInt64);
            const BinaryOperation fast_having_op =
                can_fast_having ? positional_having[0].op
                                : BinaryOperation::kEquals;
            const double fast_having_const =
                can_fast_having
                    ? (positional_having[0].constant.type == ValueType::kDouble
                           ? positional_having[0].constant.value.double_value
                           : static_cast<double>(
                                 positional_having[0].constant.value.int_value))
                    : 0.0;
            uint32_t r = 0;
            while (r < nrows) {
              const int64_t raw_key = key_cells[r];
              uint32_t r_end = r + 1;
              while (r_end < nrows && key_cells[r_end] == raw_key) {
                ++r_end;
              }
              double run_sum = 0.0;
              uint8_t run_flags = 0;
              if (val_codes != nullptr) {
                for (uint32_t k = r; k < r_end; ++k) {
                  run_sum += val_dict[val_codes[k]];
                }
              } else {
                for (uint32_t k = r; k < r_end; ++k) {
                  const auto val = std::bit_cast<double>(val_cells[k]);
                  if (std::isinf(val)) [[unlikely]] {
                    run_flags |= 1;
                  }
                  run_sum += val;
                }
              }
              const auto run_cnt = static_cast<int64_t>(r_end - r);
              r = r_end;
              if (!has_mono_key) [[unlikely]] {
                has_mono_key = true;
                cur_mono_key = raw_key;
                cur_mono_group = local.AppendMonotonicIntGroup(
                    raw_key, mono_key_type, mono_key_unsigned);
                bool emp = false;
                *local.int_index.TryEmplace(raw_key, &emp) = cur_mono_group;
              } else if (raw_key != cur_mono_key) {
                if (can_fast_having && r_end < nrows && run_flags == 0 &&
                    !std::isinf(run_sum)) {
                  bool keep_run = false;
                  switch (fast_having_op) {
                    case BinaryOperation::kGreaterThan:
                      keep_run = (run_sum > fast_having_const);
                      break;
                    case BinaryOperation::kGreaterThanEquals:
                      keep_run = (run_sum >= fast_having_const);
                      break;
                    case BinaryOperation::kLessThan:
                      keep_run = (run_sum < fast_having_const);
                      break;
                    case BinaryOperation::kLessThanEquals:
                      keep_run = (run_sum <= fast_having_const);
                      break;
                    case BinaryOperation::kEquals:
                      keep_run = (run_sum == fast_having_const);
                      break;
                    case BinaryOperation::kNotEquals:
                      keep_run = (run_sum != fast_having_const);
                      break;
                    default:
                      keep_run = true;
                      break;
                  }
                  if (!keep_run) {
                    ++reclaimed_groups[worker];
                    continue;
                  }
                }
                if (cur_mono_group > 0 && having_expr != nullptr &&
                    !positional_having.empty()) {
                  const bool passed =
                      check_slim_group_having(local, cur_mono_group, &err);
                  if (err != Status::kSuccess) {
                    return;
                  }
                  if (!passed) {
                    local.ResetMonotonicIntGroup(cur_mono_group, raw_key,
                                                 mono_key_type,
                                                 mono_key_unsigned);
                    cur_mono_key = raw_key;
                    ++reclaimed_groups[worker];
                  } else {
                    cur_mono_group = local.AppendMonotonicIntGroup(
                        raw_key, mono_key_type, mono_key_unsigned);
                    cur_mono_key = raw_key;
                  }
                } else {
                  cur_mono_group = local.AppendMonotonicIntGroup(
                      raw_key, mono_key_type, mono_key_unsigned);
                  cur_mono_key = raw_key;
                }
              }
              local.SlimFlags(cur_mono_group, 0) |= run_flags;
              local.SlimDoubleTotal(cur_mono_group, 0) += run_sum;
              local.SlimCount(cur_mono_group, 0) += run_cnt;
            }
            continue;
          }

          if (fast_dict_double_agg && lstable_rf == nullptr &&
              !blk.has_any_null) {
            struct BlockTermPtrs {
              const int64_t* col0{nullptr};
              const int64_t* col1{nullptr};
              const int64_t* col2{nullptr};
              const uint8_t* codes0{nullptr};
              const uint8_t* codes1{nullptr};
              const uint8_t* codes2{nullptr};
              const double* dict0{nullptr};
              const double* dict1{nullptr};
              const double* dict1_omd{nullptr};
              const double* dict2{nullptr};
              const double* dict2_opt{nullptr};
            };
            std::array<BlockTermPtrs, 8> term_ptrs{};
            const size_t nterms = fast_dict_terms.size();
            const auto bind_double_dict =
                [&](slot_t full_col, const uint8_t** codes_out,
                    const double** dict_out, const double** omd_out,
                    const double** opt_out, double k_omd, double k_opt) {
                  if (full_col < direct_lstable_store->dictionaries.size() &&
                      direct_lstable_store->dictionaries[full_col].active &&
                      direct_lstable_store->dictionaries[full_col].value_type ==
                          ValueType::kDouble &&
                      !blk.columns[full_col].dict_codes.empty()) {
                    const auto& d =
                        direct_lstable_store->dictionaries[full_col];
                    *codes_out = blk.columns[full_col].dict_codes.data();
                    *dict_out = d.double_entries.data();
                    if (omd_out != nullptr && k_omd == 1.0 &&
                        !d.one_minus_double_entries.empty()) {
                      *omd_out = d.one_minus_double_entries.data();
                    }
                    if (opt_out != nullptr && k_opt == 1.0 &&
                        !d.one_plus_double_entries.empty()) {
                      *opt_out = d.one_plus_double_entries.data();
                    }
                  }
                };
            for (size_t ti = 0; ti < nterms; ++ti) {
              const FastDictDoubleTerm& term = fast_dict_terms[ti];
              term_ptrs[ti].col0 = blk.columns[term.full_col0].int_cells.data();
              if (term.is_int_term) {
                if (term.int_shape != TypedIntExprInput::Shape::kSingleIntCol) {
                  term_ptrs[ti].col1 =
                      blk.columns[term.full_col1].int_cells.data();
                  if (term.int_shape ==
                      TypedIntExprInput::Shape::kMulTwoIntColsPlusCol) {
                    term_ptrs[ti].col2 =
                        blk.columns[term.full_col2].int_cells.data();
                  }
                }
              } else {
                bind_double_dict(term.full_col0, &term_ptrs[ti].codes0,
                                 &term_ptrs[ti].dict0, nullptr, nullptr, 0.0,
                                 0.0);
                if (term.shape !=
                    TypedDoubleExprInput::Shape::kSingleDoubleCol) {
                  term_ptrs[ti].col1 =
                      blk.columns[term.full_col1].int_cells.data();
                  bind_double_dict(term.full_col1, &term_ptrs[ti].codes1,
                                   &term_ptrs[ti].dict1,
                                   &term_ptrs[ti].dict1_omd, nullptr, term.k0,
                                   0.0);
                  if (term.shape == TypedDoubleExprInput::Shape::
                                        kMulConstMinusColMulConstPlusCol) {
                    term_ptrs[ti].col2 =
                        blk.columns[term.full_col2].int_cells.data();
                    bind_double_dict(term.full_col2, &term_ptrs[ti].codes2,
                                     &term_ptrs[ti].dict2, nullptr,
                                     &term_ptrs[ti].dict2_opt, 0.0, term.k1);
                  }
                }
              }
            }
            std::array<uint32_t, 256> blk_group_counts{};
            const uint32_t nrows = blk.row_count;
            uint32_t blk_matched = 0;
            const int64_t* mship_cells =
                stream_prebuilt_membership != nullptr
                    ? blk.columns
                          [proj_to_full[stream_prebuilt_membership->column]]
                              .int_cells.data()
                    : nullptr;

            // Specialized unrolled fast path for Q01's 5-term grouped double
            // aggregation when all rows in the block pass filters.
            const bool is_q1_5term_fast =
                block_all_pass && dict_group_key &&
                dict_group_total_codes > 0 && dict_group_total_codes <= 8 &&
                nterms == 5 &&
                fast_dict_terms[0].shape ==
                    TypedDoubleExprInput::Shape::kSingleDoubleCol &&
                fast_dict_terms[1].shape ==
                    TypedDoubleExprInput::Shape::kSingleDoubleCol &&
                fast_dict_terms[2].shape ==
                    TypedDoubleExprInput::Shape::kMulConstMinusCol &&
                fast_dict_terms[3].shape ==
                    TypedDoubleExprInput::Shape::
                        kMulConstMinusColMulConstPlusCol &&
                fast_dict_terms[3].reuse_prev_mul_const_minus &&
                fast_dict_terms[4].shape ==
                    TypedDoubleExprInput::Shape::kSingleDoubleCol &&
                fast_dict_terms[2].full_col0 == fast_dict_terms[1].full_col0 &&
                fast_dict_terms[4].full_col0 == fast_dict_terms[2].full_col1 &&
                term_ptrs[0].codes0 != nullptr &&
                term_ptrs[2].dict1_omd != nullptr &&
                term_ptrs[3].dict2_opt != nullptr;
            if (is_q1_5term_fast) {
              const slot_t key_full0 = proj_to_full[plan.key_offsets[0]];
              const uint8_t* __restrict codes0 =
                  blk.columns[key_full0].dict_codes.data();
              const uint8_t* __restrict codes1 =
                  plan.key_offsets.size() == 2
                      ? blk.columns[proj_to_full[plan.key_offsets[1]]]
                            .dict_codes.data()
                      : nullptr;
              const uint8_t* __restrict q_codes = term_ptrs[0].codes0;
              const double* __restrict q_dict = term_ptrs[0].dict0;
              const int64_t* __restrict p_cells = term_ptrs[1].col0;
              const uint8_t* __restrict d_codes = term_ptrs[2].codes1;
              const double* __restrict d_dict = term_ptrs[2].dict1;
              const double* __restrict omd_dict = term_ptrs[2].dict1_omd;
              const uint8_t* __restrict t_codes = term_ptrs[3].codes2;
              const double* __restrict opt_dict = term_ptrs[3].dict2_opt;
              std::array<std::array<double, 5>, 8> blk_sums{};
              for (uint32_t r = 0; r < nrows; ++r) {
                const size_t pair_code = codes1 == nullptr
                                             ? static_cast<size_t>(codes0[r])
                                             : (static_cast<size_t>(codes0[r]) *
                                                dict_group_stride1) +
                                                   codes1[r];
                uint16_t g = dict_pair_to_group[pair_code];
                if (g == 0xFFFFU) [[unlikely]] {
                  scratch.clear();
                  for (const slot_t offset : plan.key_offsets) {
                    const LSTableColumnBlock& kcol =
                        blk.columns[proj_to_full[offset]];
                    Value kv;
                    kv.type = ValueType::kVarChar;
                    kv.value.varchar_value = kcol.VarcharAt(r);
                    scratch.push_back(CanonicalDistinctValue(kv));
                  }
                  bool inserted = false;
                  const size_t new_g =
                      local.FindOrInsert(scratch, 0, plan.aggregates.size(),
                                         aggregate_expressions, &inserted);
                  g = static_cast<uint16_t>(new_g);
                  dict_pair_to_group[pair_code] = g;
                }
                ++blk_group_counts[g];
                const uint8_t dc = d_codes[r];
                const double q = q_dict[q_codes[r]];
                const auto p = std::bit_cast<double>(p_cells[r]);
                const double disc_p = p * omd_dict[dc];
                const double charge = disc_p * opt_dict[t_codes[r]];
                const double d = d_dict[dc];
                double* __restrict gs = blk_sums[g].data();
                gs[0] += q;
                gs[1] += p;
                gs[2] += disc_p;
                gs[3] += charge;
                gs[4] += d;
              }
              blk_matched = nrows;
              for (size_t g = 0; g < local.GroupCount() && g < 8; ++g) {
                if (blk_group_counts[g] == 0) {
                  continue;
                }
                for (size_t ti = 0; ti < 5; ++ti) {
                  local.SlimDoubleTotal(g,
                                        fast_dict_terms[ti].primary_agg_idx) +=
                      blk_sums[g][ti];
                }
              }
            } else if (plan.key_offsets.empty()) {
              std::array<double, 8> blk_ungrouped_sums{};
              std::array<int64_t, 8> blk_ungrouped_int_sums{};
              const bool is_q6_fast =
                  !dict_all_pass && lstable_agg_dict_filters.size() == 2 &&
                  peek_ptr != nullptr && peek_ptr->size() == 2 &&
                  (*peek_ptr)[0].column == (*peek_ptr)[1].column &&
                  (*peek_ptr)[0].op == BinaryOperation::kGreaterThanEquals &&
                  (*peek_ptr)[1].op == BinaryOperation::kLessThan &&
                  (*peek_ptr)[1].constant >= (*peek_ptr)[0].constant &&
                  mship_cells == nullptr && nterms == 1 &&
                  !fast_dict_terms[0].is_int_term &&
                  fast_dict_terms[0].shape ==
                      TypedDoubleExprInput::Shape::kMulTwoDoubleCols &&
                  !blk.columns[lstable_agg_dict_filters[0].full_column]
                       .dict_codes.empty() &&
                  !blk.columns[lstable_agg_dict_filters[1].full_column]
                       .dict_codes.empty();
              const bool is_all_pass_single_int =
                  block_all_pass && nterms == 1 &&
                  fast_dict_terms[0].is_int_term;
              const bool is_single_peek_single_int =
                  !block_all_pass && dict_all_pass && mship_cells == nullptr &&
                  peek_ptr != nullptr && peek_ptr->size() == 1 && nterms == 1 &&
                  fast_dict_terms[0].is_int_term;
              if (is_all_pass_single_int) {
                const FastDictDoubleTerm& term = fast_dict_terms[0];
                const int64_t* __restrict c0 = term_ptrs[0].col0;
                int64_t isum = 0;
                if (term.int_shape == TypedIntExprInput::Shape::kSingleIntCol) {
                  for (uint32_t r = 0; r < nrows; ++r) {
                    isum += c0[r];
                  }
                } else if (term.int_shape ==
                           TypedIntExprInput::Shape::kMulTwoIntCols) {
                  const int64_t* __restrict c1 = term_ptrs[0].col1;
                  for (uint32_t r = 0; r < nrows; ++r) {
                    isum += c0[r] * c1[r];
                  }
                } else {
                  const int64_t* __restrict c1 = term_ptrs[0].col1;
                  const int64_t* __restrict c2 = term_ptrs[0].col2;
                  for (uint32_t r = 0; r < nrows; ++r) {
                    isum += (c0[r] * c1[r]) + c2[r];
                  }
                }
                blk_matched = nrows;
                blk_ungrouped_int_sums[0] = isum;
              } else if (is_single_peek_single_int) {
                const FastDictDoubleTerm& term = fast_dict_terms[0];
                const IntegerPeekCompare& pred = (*peek_ptr)[0];
                const int64_t* __restrict pcells =
                    blk.columns[pred.column].int_cells.data();
                const int64_t pconst = pred.constant;
                const BinaryOperation pop = pred.op;
                const int64_t* __restrict c0 = term_ptrs[0].col0;
                int64_t isum = 0;
                uint32_t matched = 0;
                if (term.int_shape == TypedIntExprInput::Shape::kSingleIntCol) {
                  for (uint32_t r = 0; r < nrows; ++r) {
                    const bool ok = MatchIntegerCompare(pcells[r], pop, pconst);
                    matched += ok ? 1U : 0U;
                    isum += ok ? c0[r] : 0;
                  }
                } else if (term.int_shape ==
                           TypedIntExprInput::Shape::kMulTwoIntCols) {
                  const int64_t* __restrict c1 = term_ptrs[0].col1;
                  for (uint32_t r = 0; r < nrows; ++r) {
                    const bool ok = MatchIntegerCompare(pcells[r], pop, pconst);
                    matched += ok ? 1U : 0U;
                    isum += ok ? (c0[r] * c1[r]) : 0;
                  }
                } else {
                  const int64_t* __restrict c1 = term_ptrs[0].col1;
                  const int64_t* __restrict c2 = term_ptrs[0].col2;
                  for (uint32_t r = 0; r < nrows; ++r) {
                    const bool ok = MatchIntegerCompare(pcells[r], pop, pconst);
                    matched += ok ? 1U : 0U;
                    isum += ok ? ((c0[r] * c1[r]) + c2[r]) : 0;
                  }
                }
                blk_matched = matched;
                blk_ungrouped_int_sums[0] = isum;
              } else if (is_q6_fast) {
                const uint8_t* __restrict dc0 =
                    blk.columns[lstable_agg_dict_filters[0].full_column]
                        .dict_codes.data();
                const uint8_t* __restrict dc1 =
                    blk.columns[lstable_agg_dict_filters[1].full_column]
                        .dict_codes.data();
                const uint8_t* __restrict pass0 =
                    lstable_agg_dict_filters[0].pass.data();
                const uint8_t* __restrict pass1 =
                    lstable_agg_dict_filters[1].pass.data();
                const int64_t* __restrict pcells =
                    blk.columns[(*peek_ptr)[0].column].int_cells.data();
                const int64_t plo = (*peek_ptr)[0].constant;
                const auto pspan = static_cast<uint64_t>(
                    (*peek_ptr)[1].constant - (*peek_ptr)[0].constant);
                const BlockTermPtrs& ptrs = term_ptrs[0];
                double sum0 = 0.0;
                if (peek_all_pass) {
                  for (uint32_t r = 0; r < nrows; ++r) {
                    if ((pass0[dc0[r]] & pass1[dc1[r]]) == 0) {
                      continue;
                    }
                    ++blk_matched;
                    const double v0 = ptrs.codes0 != nullptr
                                          ? ptrs.dict0[ptrs.codes0[r]]
                                          : std::bit_cast<double>(ptrs.col0[r]);
                    const double v1 = ptrs.codes1 != nullptr
                                          ? ptrs.dict1[ptrs.codes1[r]]
                                          : std::bit_cast<double>(ptrs.col1[r]);
                    sum0 += v0 * v1;
                  }
                } else {
                  for (uint32_t r = 0; r < nrows; ++r) {
                    if ((pass0[dc0[r]] & pass1[dc1[r]]) == 0 ||
                        std::cmp_greater_equal(pcells[r] - plo, pspan)) {
                      continue;
                    }
                    ++blk_matched;
                    const double v0 = ptrs.codes0 != nullptr
                                          ? ptrs.dict0[ptrs.codes0[r]]
                                          : std::bit_cast<double>(ptrs.col0[r]);
                    const double v1 = ptrs.codes1 != nullptr
                                          ? ptrs.dict1[ptrs.codes1[r]]
                                          : std::bit_cast<double>(ptrs.col1[r]);
                    sum0 += v0 * v1;
                  }
                }
                blk_ungrouped_sums[0] = sum0;
              } else {
                for (uint32_t r = 0; r < nrows; ++r) {
                  if (!dict_all_pass) {
                    bool dict_ok = true;
                    for (const LSTableAggDictFilter& df :
                         lstable_agg_dict_filters) {
                      const LSTableColumnBlock& dcol =
                          blk.columns[df.full_column];
                      if (dcol.dict_codes.empty() ||
                          df.pass[dcol.dict_codes[r]] == 0) {
                        dict_ok = false;
                        break;
                      }
                    }
                    if (!dict_ok) {
                      continue;
                    }
                  }
                  if (!peek_all_pass && peek_ptr != nullptr &&
                      !peek_ptr->empty()) {
                    bool peek_ok = true;
                    for (const IntegerPeekCompare& pred : *peek_ptr) {
                      if (!MatchIntegerCompare(
                              blk.columns[pred.column].int_cells[r], pred.op,
                              pred.constant)) {
                        peek_ok = false;
                        break;
                      }
                    }
                    if (!peek_ok) {
                      continue;
                    }
                  }
                  if (mship_cells != nullptr &&
                      !stream_prebuilt_membership->values->Contains(
                          mship_cells[r])) {
                    continue;
                  }
                  ++blk_matched;
                  double prev_val = 0.0;
                  for (size_t ti = 0; ti < nterms; ++ti) {
                    const FastDictDoubleTerm& term = fast_dict_terms[ti];
                    const BlockTermPtrs& ptrs = term_ptrs[ti];
                    if (term.is_int_term) {
                      int64_t iv = 0;
                      switch (term.int_shape) {
                        case TypedIntExprInput::Shape::kSingleIntCol:
                          iv = ptrs.col0[r];
                          break;
                        case TypedIntExprInput::Shape::kMulTwoIntCols:
                          iv = ptrs.col0[r] * ptrs.col1[r];
                          break;
                        case TypedIntExprInput::Shape::kMulTwoIntColsPlusCol:
                          iv = (ptrs.col0[r] * ptrs.col1[r]) + ptrs.col2[r];
                          break;
                        default:
                          break;
                      }
                      blk_ungrouped_int_sums[ti] += iv;
                      continue;
                    }
                    const double v0 = ptrs.codes0 != nullptr
                                          ? ptrs.dict0[ptrs.codes0[r]]
                                          : std::bit_cast<double>(ptrs.col0[r]);
                    double val = 0.0;
                    switch (term.shape) {
                      case TypedDoubleExprInput::Shape::kSingleDoubleCol:
                        val = v0;
                        break;
                      case TypedDoubleExprInput::Shape::kMulTwoDoubleCols: {
                        const double v1 =
                            ptrs.codes1 != nullptr
                                ? ptrs.dict1[ptrs.codes1[r]]
                                : std::bit_cast<double>(ptrs.col1[r]);
                        val = v0 * v1;
                        break;
                      }
                      case TypedDoubleExprInput::Shape::kMulConstMinusCol: {
                        const double f1 =
                            ptrs.dict1_omd != nullptr
                                ? ptrs.dict1_omd[ptrs.codes1[r]]
                                : (term.k0 -
                                   (ptrs.codes1 != nullptr
                                        ? ptrs.dict1[ptrs.codes1[r]]
                                        : std::bit_cast<double>(ptrs.col1[r])));
                        val = v0 * f1;
                        break;
                      }
                      case TypedDoubleExprInput::Shape::
                          kMulConstMinusColMulConstPlusCol: {
                        const double f2 =
                            ptrs.dict2_opt != nullptr
                                ? ptrs.dict2_opt[ptrs.codes2[r]]
                                : (term.k1 +
                                   (ptrs.codes2 != nullptr
                                        ? ptrs.dict2[ptrs.codes2[r]]
                                        : std::bit_cast<double>(ptrs.col2[r])));
                        if (term.reuse_prev_mul_const_minus) {
                          val = prev_val * f2;
                        } else {
                          const double f1 =
                              ptrs.dict1_omd != nullptr
                                  ? ptrs.dict1_omd[ptrs.codes1[r]]
                                  : (term.k0 - (ptrs.codes1 != nullptr
                                                    ? ptrs.dict1[ptrs.codes1[r]]
                                                    : std::bit_cast<double>(
                                                          ptrs.col1[r])));
                          val = v0 * f1 * f2;
                        }
                        break;
                      }
                      default:
                        break;
                    }
                    prev_val = val;
                    blk_ungrouped_sums[ti] += val;
                  }
                }
              }
              if (blk_matched > 0) {
                if (!has_ungrouped) {
                  bool inserted = false;
                  ungrouped_idx =
                      local.FindOrInsert({}, 0, plan.aggregates.size(),
                                         aggregate_expressions, &inserted);
                  has_ungrouped = true;
                }
                blk_group_counts[ungrouped_idx] = blk_matched;
                for (size_t ti = 0; ti < nterms; ++ti) {
                  const size_t pidx = fast_dict_terms[ti].primary_agg_idx;
                  if (fast_dict_terms[ti].is_int_term) {
                    if ((*local.slim_kinds)[pidx] == SlimAggKind::kAvgDouble) {
                      local.SlimDoubleTotal(ungrouped_idx, pidx) +=
                          static_cast<double>(blk_ungrouped_int_sums[ti]);
                    } else {
                      local.SlimIntTotal(ungrouped_idx, pidx) +=
                          blk_ungrouped_int_sums[ti];
                    }
                  } else {
                    local.SlimDoubleTotal(ungrouped_idx, pidx) +=
                        blk_ungrouped_sums[ti];
                  }
                }
              }
            } else if (bounded_int_group_key) {
              const slot_t key_full0 = proj_to_full[plan.key_offsets[0]];
              const int64_t* __restrict key_cells =
                  blk.columns[key_full0].int_cells.data();
              uint32_t* __restrict w_cnts =
                  bounded_worker_counts[worker].data();
              if (nterms == 0) {
                if (block_all_pass) {
                  for (uint32_t r = 0; r < nrows; ++r) {
                    const auto idx =
                        static_cast<size_t>(key_cells[r] - bounded_int_min);
                    ++w_cnts[idx];
                  }
                  blk_matched = nrows;
                } else {
                  for (uint32_t r = 0; r < nrows; ++r) {
                    if (!dict_all_pass) {
                      bool dict_ok = true;
                      for (const LSTableAggDictFilter& df :
                           lstable_agg_dict_filters) {
                        const LSTableColumnBlock& dcol =
                            blk.columns[df.full_column];
                        if (dcol.dict_codes.empty() ||
                            df.pass[dcol.dict_codes[r]] == 0) {
                          dict_ok = false;
                          break;
                        }
                      }
                      if (!dict_ok) {
                        continue;
                      }
                    }
                    if (!peek_all_pass && peek_ptr != nullptr &&
                        !peek_ptr->empty()) {
                      bool peek_ok = true;
                      for (const IntegerPeekCompare& pred : *peek_ptr) {
                        if (!MatchIntegerCompare(
                                blk.columns[pred.column].int_cells[r], pred.op,
                                pred.constant)) {
                          peek_ok = false;
                          break;
                        }
                      }
                      if (!peek_ok) {
                        continue;
                      }
                    }
                    if (mship_cells != nullptr &&
                        !stream_prebuilt_membership->values->Contains(
                            mship_cells[r])) {
                      continue;
                    }
                    ++blk_matched;
                    const auto idx =
                        static_cast<size_t>(key_cells[r] - bounded_int_min);
                    ++w_cnts[idx];
                  }
                }
              } else if (bounded_has_int_term) {
                int64_t* __restrict w_isums =
                    bounded_worker_int_sums[worker].data();
                const FastDictDoubleTerm& term = fast_dict_terms[0];
                const BlockTermPtrs& ptrs = term_ptrs[0];
                if (block_all_pass &&
                    term.int_shape == TypedIntExprInput::Shape::kSingleIntCol) {
                  const int64_t* __restrict c0 = ptrs.col0;
                  for (uint32_t r = 0; r < nrows; ++r) {
                    const auto idx =
                        static_cast<size_t>(key_cells[r] - bounded_int_min);
                    w_isums[idx] += c0[r];
                    ++w_cnts[idx];
                  }
                  blk_matched = nrows;
                } else {
                  for (uint32_t r = 0; r < nrows; ++r) {
                    if (!dict_all_pass) {
                      bool dict_ok = true;
                      for (const LSTableAggDictFilter& df :
                           lstable_agg_dict_filters) {
                        const LSTableColumnBlock& dcol =
                            blk.columns[df.full_column];
                        if (dcol.dict_codes.empty() ||
                            df.pass[dcol.dict_codes[r]] == 0) {
                          dict_ok = false;
                          break;
                        }
                      }
                      if (!dict_ok) {
                        continue;
                      }
                    }
                    if (!peek_all_pass && peek_ptr != nullptr &&
                        !peek_ptr->empty()) {
                      bool peek_ok = true;
                      for (const IntegerPeekCompare& pred : *peek_ptr) {
                        if (!MatchIntegerCompare(
                                blk.columns[pred.column].int_cells[r], pred.op,
                                pred.constant)) {
                          peek_ok = false;
                          break;
                        }
                      }
                      if (!peek_ok) {
                        continue;
                      }
                    }
                    if (mship_cells != nullptr &&
                        !stream_prebuilt_membership->values->Contains(
                            mship_cells[r])) {
                      continue;
                    }
                    ++blk_matched;
                    int64_t iv = 0;
                    switch (term.int_shape) {
                      case TypedIntExprInput::Shape::kSingleIntCol:
                        iv = ptrs.col0[r];
                        break;
                      case TypedIntExprInput::Shape::kMulTwoIntCols:
                        iv = ptrs.col0[r] * ptrs.col1[r];
                        break;
                      case TypedIntExprInput::Shape::kMulTwoIntColsPlusCol:
                        iv = (ptrs.col0[r] * ptrs.col1[r]) + ptrs.col2[r];
                        break;
                      default:
                        break;
                    }
                    const auto idx =
                        static_cast<size_t>(key_cells[r] - bounded_int_min);
                    w_isums[idx] += iv;
                    ++w_cnts[idx];
                  }
                }
              } else {
                double* __restrict w_sums = bounded_worker_sums[worker].data();
                const FastDictDoubleTerm& term = fast_dict_terms[0];
                const BlockTermPtrs& ptrs = term_ptrs[0];
                for (uint32_t r = 0; r < nrows; ++r) {
                  if (!dict_all_pass) {
                    bool dict_ok = true;
                    for (const LSTableAggDictFilter& df :
                         lstable_agg_dict_filters) {
                      const LSTableColumnBlock& dcol =
                          blk.columns[df.full_column];
                      if (dcol.dict_codes.empty() ||
                          df.pass[dcol.dict_codes[r]] == 0) {
                        dict_ok = false;
                        break;
                      }
                    }
                    if (!dict_ok) {
                      continue;
                    }
                  }
                  if (!peek_all_pass && peek_ptr != nullptr &&
                      !peek_ptr->empty()) {
                    bool peek_ok = true;
                    for (const IntegerPeekCompare& pred : *peek_ptr) {
                      if (!MatchIntegerCompare(
                              blk.columns[pred.column].int_cells[r], pred.op,
                              pred.constant)) {
                        peek_ok = false;
                        break;
                      }
                    }
                    if (!peek_ok) {
                      continue;
                    }
                  }
                  if (mship_cells != nullptr &&
                      !stream_prebuilt_membership->values->Contains(
                          mship_cells[r])) {
                    continue;
                  }
                  ++blk_matched;
                  const double v0 = ptrs.codes0 != nullptr
                                        ? ptrs.dict0[ptrs.codes0[r]]
                                        : std::bit_cast<double>(ptrs.col0[r]);
                  double val = 0.0;
                  switch (term.shape) {
                    case TypedDoubleExprInput::Shape::kSingleDoubleCol:
                      val = v0;
                      break;
                    case TypedDoubleExprInput::Shape::kMulTwoDoubleCols: {
                      const double v1 =
                          ptrs.codes1 != nullptr
                              ? ptrs.dict1[ptrs.codes1[r]]
                              : std::bit_cast<double>(ptrs.col1[r]);
                      val = v0 * v1;
                      break;
                    }
                    case TypedDoubleExprInput::Shape::kMulConstMinusCol: {
                      const double f1 =
                          ptrs.dict1_omd != nullptr
                              ? ptrs.dict1_omd[ptrs.codes1[r]]
                              : (term.k0 -
                                 (ptrs.codes1 != nullptr
                                      ? ptrs.dict1[ptrs.codes1[r]]
                                      : std::bit_cast<double>(ptrs.col1[r])));
                      val = v0 * f1;
                      break;
                    }
                    case TypedDoubleExprInput::Shape::
                        kMulConstMinusColMulConstPlusCol: {
                      const double f1 =
                          ptrs.dict1_omd != nullptr
                              ? ptrs.dict1_omd[ptrs.codes1[r]]
                              : (term.k0 -
                                 (ptrs.codes1 != nullptr
                                      ? ptrs.dict1[ptrs.codes1[r]]
                                      : std::bit_cast<double>(ptrs.col1[r])));
                      const double f2 =
                          ptrs.dict2_opt != nullptr
                              ? ptrs.dict2_opt[ptrs.codes2[r]]
                              : (term.k1 +
                                 (ptrs.codes2 != nullptr
                                      ? ptrs.dict2[ptrs.codes2[r]]
                                      : std::bit_cast<double>(ptrs.col2[r])));
                      val = v0 * f1 * f2;
                      break;
                    }
                    default:
                      break;
                  }
                  const auto idx =
                      static_cast<size_t>(key_cells[r] - bounded_int_min);
                  w_sums[idx] += val;
                  ++w_cnts[idx];
                }
              }
              if (blk_matched > 0) {
                scanned[worker] += blk_matched;
                emitted[worker] += blk_matched;
                aggregate_updates[worker] += blk_matched;
              }
              continue;
            } else {
              const slot_t key_full0 = proj_to_full[plan.key_offsets[0]];
              const uint8_t* codes0 = blk.columns[key_full0].dict_codes.data();
              const uint8_t* codes1 =
                  plan.key_offsets.size() == 2
                      ? blk.columns[proj_to_full[plan.key_offsets[1]]]
                            .dict_codes.data()
                      : nullptr;
              for (uint32_t r = 0; r < nrows; ++r) {
                if (!dict_all_pass) {
                  bool dict_ok = true;
                  for (const LSTableAggDictFilter& df :
                       lstable_agg_dict_filters) {
                    const LSTableColumnBlock& dcol =
                        blk.columns[df.full_column];
                    if (dcol.dict_codes.empty() ||
                        df.pass[dcol.dict_codes[r]] == 0) {
                      dict_ok = false;
                      break;
                    }
                  }
                  if (!dict_ok) {
                    continue;
                  }
                }
                if (!peek_all_pass && peek_ptr != nullptr &&
                    !peek_ptr->empty()) {
                  bool peek_ok = true;
                  for (const IntegerPeekCompare& pred : *peek_ptr) {
                    if (!MatchIntegerCompare(
                            blk.columns[pred.column].int_cells[r], pred.op,
                            pred.constant)) {
                      peek_ok = false;
                      break;
                    }
                  }
                  if (!peek_ok) {
                    continue;
                  }
                }
                if (mship_cells != nullptr &&
                    !stream_prebuilt_membership->values->Contains(
                        mship_cells[r])) {
                  continue;
                }
                ++blk_matched;
                const size_t pair_code = codes1 == nullptr
                                             ? static_cast<size_t>(codes0[r])
                                             : (static_cast<size_t>(codes0[r]) *
                                                dict_group_stride1) +
                                                   codes1[r];
                uint16_t g = dict_pair_to_group[pair_code];
                if (g == 0xFFFFU) [[unlikely]] {
                  scratch.clear();
                  for (const slot_t offset : plan.key_offsets) {
                    const LSTableColumnBlock& kcol =
                        blk.columns[proj_to_full[offset]];
                    Value kv;
                    kv.type = ValueType::kVarChar;
                    kv.value.varchar_value = kcol.VarcharAt(r);
                    scratch.push_back(CanonicalDistinctValue(kv));
                  }
                  bool inserted = false;
                  const size_t new_g =
                      local.FindOrInsert(scratch, 0, plan.aggregates.size(),
                                         aggregate_expressions, &inserted);
                  g = static_cast<uint16_t>(new_g);
                  dict_pair_to_group[pair_code] = g;
                }
                ++blk_group_counts[g];
                double prev_val = 0.0;
                for (size_t ti = 0; ti < nterms; ++ti) {
                  const FastDictDoubleTerm& term = fast_dict_terms[ti];
                  const BlockTermPtrs& ptrs = term_ptrs[ti];
                  const double v0 = ptrs.codes0 != nullptr
                                        ? ptrs.dict0[ptrs.codes0[r]]
                                        : std::bit_cast<double>(ptrs.col0[r]);
                  double val = 0.0;
                  switch (term.shape) {
                    case TypedDoubleExprInput::Shape::kSingleDoubleCol:
                      val = v0;
                      break;
                    case TypedDoubleExprInput::Shape::kMulTwoDoubleCols: {
                      const double v1 =
                          ptrs.codes1 != nullptr
                              ? ptrs.dict1[ptrs.codes1[r]]
                              : std::bit_cast<double>(ptrs.col1[r]);
                      val = v0 * v1;
                      break;
                    }
                    case TypedDoubleExprInput::Shape::kMulConstMinusCol: {
                      const double f1 =
                          ptrs.dict1_omd != nullptr
                              ? ptrs.dict1_omd[ptrs.codes1[r]]
                              : (term.k0 -
                                 (ptrs.codes1 != nullptr
                                      ? ptrs.dict1[ptrs.codes1[r]]
                                      : std::bit_cast<double>(ptrs.col1[r])));
                      val = v0 * f1;
                      break;
                    }
                    case TypedDoubleExprInput::Shape::
                        kMulConstMinusColMulConstPlusCol: {
                      const double f2 =
                          ptrs.dict2_opt != nullptr
                              ? ptrs.dict2_opt[ptrs.codes2[r]]
                              : (term.k1 +
                                 (ptrs.codes2 != nullptr
                                      ? ptrs.dict2[ptrs.codes2[r]]
                                      : std::bit_cast<double>(ptrs.col2[r])));
                      if (term.reuse_prev_mul_const_minus) {
                        val = prev_val * f2;
                      } else {
                        const double f1 =
                            ptrs.dict1_omd != nullptr
                                ? ptrs.dict1_omd[ptrs.codes1[r]]
                                : (term.k0 -
                                   (ptrs.codes1 != nullptr
                                        ? ptrs.dict1[ptrs.codes1[r]]
                                        : std::bit_cast<double>(ptrs.col1[r])));
                        val = v0 * f1 * f2;
                      }
                      break;
                    }
                    default:
                      break;
                  }
                  prev_val = val;
                  local.SlimDoubleTotal(g, term.primary_agg_idx) += val;
                }
              }
            }
            if (blk_matched > 0) {
              scanned[worker] += blk_matched;
              emitted[worker] += blk_matched;
              aggregate_updates[worker] += blk_matched;
              for (size_t g = 0; g < local.GroupCount() && g < 256; ++g) {
                const uint32_t cnt = blk_group_counts[g];
                if (cnt == 0) {
                  continue;
                }
                for (const auto& [dup_i, prim_i] : fast_dict_dup_aggs) {
                  if ((*local.slim_kinds)[dup_i] == SlimAggKind::kSumInt64) {
                    local.SlimIntTotal(g, dup_i) =
                        local.SlimIntTotal(g, prim_i);
                  } else if ((*local.slim_kinds)[prim_i] ==
                             SlimAggKind::kSumInt64) {
                    local.SlimDoubleTotal(g, dup_i) =
                        static_cast<double>(local.SlimIntTotal(g, prim_i));
                  } else {
                    local.SlimDoubleTotal(g, dup_i) =
                        local.SlimDoubleTotal(g, prim_i);
                  }
                }
                for (size_t i = 0; i < plan.aggregates.size(); ++i) {
                  local.SlimCount(g, i) += cnt;
                  if ((*local.slim_kinds)[i] == SlimAggKind::kSumDouble &&
                      !std::isfinite(local.SlimDoubleTotal(g, i)))
                      [[unlikely]] {
                    for (uint32_t r = 0; r < nrows; ++r) {
                      double addend = 0.0;
                      if (plan.aggregates[i].column.has_value()) {
                        addend = std::bit_cast<double>(
                            blk.columns
                                [proj_to_full[*plan.aggregates[i].column]]
                                    .int_cells[r]);
                      } else if (plan.aggregates[i].double_expr.has_value()) {
                        const TypedDoubleExprInput& de =
                            *plan.aggregates[i].double_expr;
                        const auto c0 = std::bit_cast<double>(
                            blk.columns[proj_to_full[de.col0]].int_cells[r]);
                        if (de.shape ==
                            TypedDoubleExprInput::Shape::kSingleDoubleCol) {
                          addend = c0;
                        } else {
                          const auto c1 = std::bit_cast<double>(
                              blk.columns[proj_to_full[de.col1]].int_cells[r]);
                          if (de.shape ==
                              TypedDoubleExprInput::Shape::kMulTwoDoubleCols) {
                            addend = c0 * c1;
                          } else if (de.shape == TypedDoubleExprInput::Shape::
                                                     kMulConstMinusCol) {
                            addend = c0 * (de.k0 - c1);
                          } else {
                            const auto c2 = std::bit_cast<double>(
                                blk.columns[proj_to_full[de.col2]]
                                    .int_cells[r]);
                            addend = c0 * (de.k0 - c1) * (de.k1 + c2);
                          }
                        }
                      }
                      if (std::isinf(addend)) {
                        local.SlimFlags(g, i) |= 1;
                        break;
                      }
                    }
                  }
                }
              }
            }
            continue;
          }

          const LSTableColumnBlock* const rf_col =
              (lstable_rf != nullptr && active_key_col.has_value())
                  ? &blk.columns[*active_key_col]
                  : nullptr;
          const bool rf_has_dense =
              rf_col != nullptr && lstable_rf->has_dense_bits();
          const uint64_t* const rf_dense_bits =
              rf_has_dense ? lstable_rf->dense_bits_data() : nullptr;
          const size_t rf_dense_words =
              rf_has_dense ? lstable_rf->dense_bits_word_count() : 0;
          const int64_t rf_lo = rf_has_dense ? lstable_rf->Lo() : 0;
          const int64_t rf_hi = rf_has_dense ? lstable_rf->Hi() : 0;
          const bool rf_col_has_null = rf_col != nullptr && rf_col->has_null;
          const bool rf_col_monotonic =
              rf_has_dense && !rf_col_has_null &&
              *active_key_col <
                  direct_lstable_store->column_is_monotonic.size() &&
              direct_lstable_store->column_is_monotonic[*active_key_col] != 0;

          for (uint32_t r = 0; r < blk.row_count; ++r) {
            if (rf_col != nullptr) {
              if (rf_col_has_null && rf_col->IsNull(r)) [[unlikely]] {
                continue;
              }
              const int64_t key_val = rf_col->int_cells[r];
              if (rf_has_dense) {
                bool accepted = false;
                if (key_val >= rf_lo && key_val <= rf_hi) {
                  const uint64_t off = static_cast<uint64_t>(key_val) -
                                       static_cast<uint64_t>(rf_lo);
                  accepted = (rf_dense_bits[off >> 6] &
                              (uint64_t{1} << (off & 63))) != 0;
                }
                if (!accepted) {
                  if (rf_col_monotonic) {
                    if (key_val > rf_hi) {
                      break;
                    }
                    int64_t next_valid_key = rf_lo;
                    if (key_val >= rf_lo) {
                      const uint64_t next_off = static_cast<uint64_t>(key_val) -
                                                static_cast<uint64_t>(rf_lo) +
                                                1ULL;
                      auto w_idx = static_cast<size_t>(next_off >> 6);
                      if (w_idx >= rf_dense_words) {
                        break;
                      }
                      uint64_t word =
                          rf_dense_bits[w_idx] & (~0ULL << (next_off & 63));
                      while (word == 0) {
                        if (++w_idx >= rf_dense_words) {
                          break;
                        }
                        word = rf_dense_bits[w_idx];
                      }
                      if (w_idx >= rf_dense_words) {
                        break;
                      }
                      const uint64_t found_off =
                          (static_cast<uint64_t>(w_idx) << 6) +
                          static_cast<uint64_t>(std::countr_zero(word));
                      next_valid_key = rf_lo + static_cast<int64_t>(found_off);
                      if (next_valid_key > rf_hi) {
                        break;
                      }
                    }
                    if (rf_col->has_min_max &&
                        next_valid_key > rf_col->max_int) {
                      break;
                    }
                    const uint32_t row_count = blk.row_count;
                    const int64_t* const cells = rf_col->int_cells.data();
                    if (r + 1 < row_count && cells[r + 1] < next_valid_key) {
                      uint32_t lo = r + 1;
                      uint32_t step = 4;
                      while (lo + step < row_count &&
                             cells[lo + step] < next_valid_key) {
                        lo += step;
                        step <<= 1;
                      }
                      const uint32_t hi = std::min(row_count, lo + step + 1);
                      const int64_t* found = std::lower_bound(
                          cells + lo, cells + hi, next_valid_key);
                      r = static_cast<uint32_t>(found - cells) - 1;
                    }
                  }
                  continue;
                }
              } else {
                if (!have_rf_cache || key_val != last_rf_key) {
                  have_rf_cache = true;
                  last_rf_key = key_val;
                  last_rf_rejected = lstable_rf->Reject(key_val);
                }
                if (last_rf_rejected) {
                  continue;
                }
              }
            }
            if (stream_prebuilt_membership != nullptr) {
              const LSTableColumnBlock& mcol =
                  blk.columns[proj_to_full[stream_prebuilt_membership->column]];
              if (mcol.IsNull(r) ||
                  !stream_prebuilt_membership->values->Contains(
                      mcol.int_cells[r])) {
                continue;
              }
            }
            if (!peek_all_pass && peek_ptr != nullptr && !peek_ptr->empty()) {
              bool peek_ok = true;
              for (const IntegerPeekCompare& pred : *peek_ptr) {
                const LSTableColumnBlock& pcol = blk.columns[pred.column];
                if (pcol.IsNull(r) ||
                    !MatchIntegerCompare(pcol.int_cells[r], pred.op,
                                         pred.constant)) {
                  peek_ok = false;
                  break;
                }
              }
              if (!peek_ok) {
                continue;
              }
            }
            ++scanned[worker];

            const auto is_null = [&](slot_t c) noexcept -> bool {
              return blk.has_any_null && blk.columns[proj_to_full[c]].IsNull(r);
            };
            const auto make_value = [&](slot_t c) -> Value {
              const LSTableColumnBlock& col = blk.columns[proj_to_full[c]];
              Value value;
              switch (proj_types[c]) {
                case ValueType::kDouble:
                  value.type = ValueType::kDouble;
                  value.value.double_value =
                      std::bit_cast<double>(col.int_cells[r]);
                  break;
                case ValueType::kVarChar:
                  value.type = ValueType::kVarChar;
                  value.value.varchar_value = col.VarcharAt(r);
                  break;
                default:
                  value.type = proj_types[c];
                  value.value.int_value = col.int_cells[r];
                  break;
              }
              if (value.type == ValueType::kInt64 && proj_unsigned[c] != 0) {
                value.SetUnsigned();
              }
              return value;
            };
            const auto eval_filter_compare =
                [&](const TypedFilterCompare& compare) -> bool {
              const LSTableColumnBlock& lcol =
                  blk.columns[proj_to_full[compare.left]];
              if (lcol.IsNull(r)) {
                return false;
              }
              if (compare.constant_payload) {
                if (compare.string_payload) {
                  const std::string_view sv = lcol.VarcharAt(r);
                  return compare.op == BinaryOperation::kEquals
                             ? (sv == compare.string_constant)
                             : (sv != compare.string_constant);
                }
                if (compare.is_double) {
                  const auto a = std::bit_cast<double>(lcol.int_cells[r]);
                  switch (compare.op) {
                    case BinaryOperation::kEquals:
                      return a == compare.double_constant;
                    case BinaryOperation::kNotEquals:
                      return a != compare.double_constant;
                    case BinaryOperation::kLessThan:
                      return a < compare.double_constant;
                    case BinaryOperation::kLessThanEquals:
                      return a <= compare.double_constant;
                    case BinaryOperation::kGreaterThan:
                      return a > compare.double_constant;
                    default:
                      return a >= compare.double_constant;
                  }
                }
                const int64_t a = lcol.int_cells[r];
                switch (compare.op) {
                  case BinaryOperation::kEquals:
                    return a == compare.int_constant;
                  case BinaryOperation::kNotEquals:
                    return a != compare.int_constant;
                  case BinaryOperation::kLessThan:
                    return a < compare.int_constant;
                  case BinaryOperation::kLessThanEquals:
                    return a <= compare.int_constant;
                  case BinaryOperation::kGreaterThan:
                    return a > compare.int_constant;
                  default:
                    return a >= compare.int_constant;
                }
              }
              const LSTableColumnBlock& rcol =
                  blk.columns[proj_to_full[compare.right]];
              if (rcol.IsNull(r)) {
                return false;
              }
              if (compare.is_double) {
                const auto a = std::bit_cast<double>(lcol.int_cells[r]);
                const auto b = std::bit_cast<double>(rcol.int_cells[r]);
                switch (compare.op) {
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
              }
              const int64_t a = lcol.int_cells[r];
              const int64_t b = rcol.int_cells[r];
              switch (compare.op) {
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
            const auto eval_double_expr = [&](const TypedDoubleExprInput& expr,
                                              double* out) -> bool {
              switch (expr.shape) {
                case TypedDoubleExprInput::Shape::kSingleDoubleCol: {
                  const LSTableColumnBlock& c0 =
                      blk.columns[proj_to_full[expr.col0]];
                  if (c0.IsNull(r)) {
                    return false;
                  }
                  *out = std::bit_cast<double>(c0.int_cells[r]);
                  return true;
                }
                case TypedDoubleExprInput::Shape::kMulTwoDoubleCols: {
                  const LSTableColumnBlock& c0 =
                      blk.columns[proj_to_full[expr.col0]];
                  const LSTableColumnBlock& c1 =
                      blk.columns[proj_to_full[expr.col1]];
                  if (c0.IsNull(r) || c1.IsNull(r)) {
                    return false;
                  }
                  *out = std::bit_cast<double>(c0.int_cells[r]) *
                         std::bit_cast<double>(c1.int_cells[r]);
                  return true;
                }
                case TypedDoubleExprInput::Shape::kMulConstMinusCol: {
                  const LSTableColumnBlock& c0 =
                      blk.columns[proj_to_full[expr.col0]];
                  const LSTableColumnBlock& c1 =
                      blk.columns[proj_to_full[expr.col1]];
                  if (c0.IsNull(r) || c1.IsNull(r)) {
                    return false;
                  }
                  *out = std::bit_cast<double>(c0.int_cells[r]) *
                         (expr.k0 - std::bit_cast<double>(c1.int_cells[r]));
                  return true;
                }
                case TypedDoubleExprInput::Shape::
                    kMulConstMinusColMulConstPlusCol: {
                  const LSTableColumnBlock& c0 =
                      blk.columns[proj_to_full[expr.col0]];
                  const LSTableColumnBlock& c1 =
                      blk.columns[proj_to_full[expr.col1]];
                  const LSTableColumnBlock& c2 =
                      blk.columns[proj_to_full[expr.col2]];
                  if (c0.IsNull(r) || c1.IsNull(r) || c2.IsNull(r)) {
                    return false;
                  }
                  *out = std::bit_cast<double>(c0.int_cells[r]) *
                         (expr.k0 - std::bit_cast<double>(c1.int_cells[r])) *
                         (expr.k1 + std::bit_cast<double>(c2.int_cells[r]));
                  return true;
                }
                case TypedDoubleExprInput::Shape::kGeneric:
                  break;
              }
              std::array<double, 16> stack{};
              size_t sp = 0;
              for (const TypedDoubleExprOp& op : expr.ops) {
                switch (op.kind) {
                  case TypedDoubleExprOp::Kind::kLoadDoubleCol: {
                    const LSTableColumnBlock& c =
                        blk.columns[proj_to_full[op.column]];
                    if (c.IsNull(r)) {
                      return false;
                    }
                    stack[sp++] = std::bit_cast<double>(c.int_cells[r]);
                    break;
                  }
                  case TypedDoubleExprOp::Kind::kLoadIntColAsDouble: {
                    const LSTableColumnBlock& c =
                        blk.columns[proj_to_full[op.column]];
                    if (c.IsNull(r)) {
                      return false;
                    }
                    stack[sp++] = static_cast<double>(c.int_cells[r]);
                    break;
                  }
                  case TypedDoubleExprOp::Kind::kConstant:
                    stack[sp++] = op.constant;
                    break;
                  case TypedDoubleExprOp::Kind::kAdd:
                    stack[sp - 2] += stack[sp - 1];
                    --sp;
                    break;
                  case TypedDoubleExprOp::Kind::kSub:
                    stack[sp - 2] -= stack[sp - 1];
                    --sp;
                    break;
                  case TypedDoubleExprOp::Kind::kMul:
                    stack[sp - 2] *= stack[sp - 1];
                    --sp;
                    break;
                }
              }
              *out = stack[0];
              return true;
            };
            const auto eval_int_expr = [&](const TypedIntExprInput& expr,
                                           int64_t* out,
                                           Status* error) -> bool {
              std::array<int64_t, 16> stack{};
              size_t sp = 0;
              for (const TypedIntExprOp& op : expr.ops) {
                switch (op.kind) {
                  case TypedIntExprOp::Kind::kLoadIntCol: {
                    const LSTableColumnBlock& c =
                        blk.columns[proj_to_full[op.column]];
                    if (c.IsNull(r)) {
                      return false;
                    }
                    stack[sp++] = c.int_cells[r];
                    break;
                  }
                  case TypedIntExprOp::Kind::kConstant:
                    stack[sp++] = op.constant;
                    break;
                  case TypedIntExprOp::Kind::kAdd: {
                    int64_t v = 0;
                    if (__builtin_add_overflow(stack[sp - 2], stack[sp - 1],
                                               &v)) {
                      *error = StatusError(StatusCode::kIsInfinity,
                                           "integer overflow on '+'");
                      return false;
                    }
                    stack[sp - 2] = v;
                    --sp;
                    break;
                  }
                  case TypedIntExprOp::Kind::kSub: {
                    int64_t v = 0;
                    if (__builtin_sub_overflow(stack[sp - 2], stack[sp - 1],
                                               &v)) {
                      *error = StatusError(StatusCode::kIsInfinity,
                                           "integer overflow on '-'");
                      return false;
                    }
                    stack[sp - 2] = v;
                    --sp;
                    break;
                  }
                  case TypedIntExprOp::Kind::kMul: {
                    int64_t v = 0;
                    if (__builtin_mul_overflow(stack[sp - 2], stack[sp - 1],
                                               &v)) {
                      *error = StatusError(StatusCode::kIsInfinity,
                                           "integer overflow on '*'");
                      return false;
                    }
                    stack[sp - 2] = v;
                    --sp;
                    break;
                  }
                }
              }
              *out = stack[0];
              return true;
            };

            if (!block_all_pass) {
              bool keep = true;
              for (const LSTableAggDictFilter& df : lstable_agg_dict_filters) {
                const LSTableColumnBlock& dcol = blk.columns[df.full_column];
                if (dcol.dict_codes.empty() ||
                    df.pass[dcol.dict_codes[r]] == 0) {
                  keep = false;
                  break;
                }
              }
              if (!keep) {
                continue;
              }
              const bool filter_compares_done_in_peek =
                  peek_ptr != nullptr &&
                  peek_compares.size() == plan.filter_compares.size();
              if (!filter_compares_done_in_peek) {
                for (size_t fi = 0; fi < plan.filter_compares.size(); ++fi) {
                  if (filter_compare_covered[fi] != 0) {
                    continue;
                  }
                  if (!eval_filter_compare(plan.filter_compares[fi])) {
                    keep = false;
                    break;
                  }
                }
                if (!keep) {
                  continue;
                }
              }
              for (size_t li = 0; li < plan.inlist_compares.size(); ++li) {
                if (inlist_compare_covered[li] != 0) {
                  continue;
                }
                const TypedInListCompare& list = plan.inlist_compares[li];
                const LSTableColumnBlock& lcol =
                    blk.columns[proj_to_full[list.column]];
                if (lcol.IsNull(r)) {
                  keep = false;
                  break;
                }
                bool matched = false;
                if (list.is_string) {
                  matched = MatchTypedStringInList(lcol.VarcharAt(r), list);
                } else if (list.is_double) {
                  const auto v = std::bit_cast<double>(lcol.int_cells[r]);
                  for (const double item : list.double_values) {
                    if (v == item) {
                      matched = true;
                      break;
                    }
                  }
                } else {
                  const int64_t v = lcol.int_cells[r];
                  for (const int64_t item : list.int_values) {
                    if (v == item) {
                      matched = true;
                      break;
                    }
                  }
                }
                if (!matched) {
                  keep = false;
                  break;
                }
              }
              if (!keep) {
                continue;
              }
            }

            ++emitted[worker];
            size_t group_index = 0;
            if (plan.key_offsets.empty()) {
              if (!has_ungrouped) {
                bool inserted = false;
                ungrouped_idx =
                    local.FindOrInsert({}, 0, plan.aggregates.size(),
                                       aggregate_expressions, &inserted);
                has_ungrouped = true;
              }
              group_index = ungrouped_idx;
            } else if (monotonic_int_key) {
              const int64_t raw_key =
                  blk.columns[mono_key_full_col].int_cells[r];
              if (!has_mono_key) {
                has_mono_key = true;
                cur_mono_key = raw_key;
                cur_mono_group = local.AppendMonotonicIntGroup(
                    raw_key, mono_key_type, mono_key_unsigned);
                bool emp = false;
                *local.int_index.TryEmplace(raw_key, &emp) = cur_mono_group;
              } else if (raw_key != cur_mono_key) {
                if (cur_mono_group > 0 && having_expr != nullptr &&
                    !positional_having.empty()) {
                  const bool passed =
                      check_slim_group_having(local, cur_mono_group, &err);
                  if (err != Status::kSuccess) {
                    return;
                  }
                  if (!passed) {
                    local.ResetMonotonicIntGroup(cur_mono_group, raw_key,
                                                 mono_key_type,
                                                 mono_key_unsigned);
                    cur_mono_key = raw_key;
                    ++reclaimed_groups[worker];
                  } else {
                    cur_mono_group = local.AppendMonotonicIntGroup(
                        raw_key, mono_key_type, mono_key_unsigned);
                    cur_mono_key = raw_key;
                  }
                } else {
                  cur_mono_group = local.AppendMonotonicIntGroup(
                      raw_key, mono_key_type, mono_key_unsigned);
                  cur_mono_key = raw_key;
                }
              }
              group_index = cur_mono_group;
            } else if (dict_group_key) {
              const slot_t full0 = proj_to_full[plan.key_offsets[0]];
              const uint8_t c0 = blk.columns[full0].dict_codes[r];
              size_t pair_code = c0;
              if (plan.key_offsets.size() == 2) {
                const slot_t full1 = proj_to_full[plan.key_offsets[1]];
                const uint8_t c1 = blk.columns[full1].dict_codes[r];
                pair_code = (static_cast<size_t>(c0) * dict_group_stride1) + c1;
              }
              uint16_t g = dict_pair_to_group[pair_code];
              if (g == 0xFFFFU) [[unlikely]] {
                scratch.clear();
                for (const slot_t offset : plan.key_offsets) {
                  scratch.push_back(CanonicalDistinctValue(make_value(offset)));
                }
                int64_t raw_key = 0;
                bool inserted = false;
                const size_t new_g =
                    local.FindOrInsert(scratch, raw_key, plan.aggregates.size(),
                                       aggregate_expressions, &inserted);
                g = static_cast<uint16_t>(new_g);
                dict_pair_to_group[pair_code] = g;
              }
              group_index = g;
            } else if (int_key && !plan.key_offsets.empty() &&
                       !is_null(plan.key_offsets[0])) {
              const int64_t raw_key =
                  blk.columns[proj_to_full[plan.key_offsets[0]]].int_cells[r];
              const size_t mru_idx = TypedWorkerGroups::MruSlot(raw_key);
              if (local.mru_groups[mru_idx] != SIZE_MAX &&
                  local.mru_keys[mru_idx] == raw_key) {
                group_index = local.mru_groups[mru_idx];
              } else {
                bool inserted = false;
                size_t* slot = local.RawIntSlot(raw_key, &inserted);
                if (inserted) {
                  scratch.clear();
                  for (const slot_t offset : plan.key_offsets) {
                    scratch.push_back(
                        CanonicalDistinctValue(make_value(offset)));
                  }
                  group_index = local.ClaimSlotForNewGroup(
                      slot, scratch, plan.aggregates.size(),
                      aggregate_expressions);
                } else {
                  group_index = *slot;
                }
                local.mru_keys[mru_idx] = raw_key;
                local.mru_groups[mru_idx] = group_index;
              }
            } else if (packed_varchar_key) {
              uint64_t packed = 0;
              bool can_pack = true;
              for (size_t k = 0; k < plan.key_offsets.size(); ++k) {
                const slot_t offset = plan.key_offsets[k];
                uint32_t part = 0;
                if (is_null(offset) ||
                    !TryPackShortVarcharSpan(
                        blk.columns[proj_to_full[offset]].VarcharAt(r),
                        &part)) {
                  can_pack = false;
                  break;
                }
                packed |= static_cast<uint64_t>(part) << (k * 32);
              }
              if (can_pack) {
                const auto raw_packed = static_cast<int64_t>(packed);
                const size_t mru_idx = TypedWorkerGroups::MruSlot(raw_packed);
                if (local.mru_groups[mru_idx] != SIZE_MAX &&
                    local.mru_keys[mru_idx] == raw_packed) {
                  group_index = local.mru_groups[mru_idx];
                } else {
                  bool inserted = false;
                  size_t* slot = local.RawIntSlot(raw_packed, &inserted);
                  if (inserted) {
                    scratch.clear();
                    for (const slot_t offset : plan.key_offsets) {
                      scratch.push_back(
                          CanonicalDistinctValue(make_value(offset)));
                    }
                    group_index = local.ClaimSlotForNewGroup(
                        slot, scratch, plan.aggregates.size(),
                        aggregate_expressions);
                  } else {
                    group_index = *slot;
                  }
                  local.mru_keys[mru_idx] = raw_packed;
                  local.mru_groups[mru_idx] = group_index;
                }
              } else {
                scratch.clear();
                for (const slot_t offset : plan.key_offsets) {
                  if (is_null(offset)) {
                    scratch.emplace_back();
                  } else {
                    scratch.push_back(
                        CanonicalDistinctValue(make_value(offset)));
                  }
                }
                bool inserted = false;
                group_index =
                    local.FindOrInsert(scratch, 0, plan.aggregates.size(),
                                       aggregate_expressions, &inserted);
              }
            } else {
              scratch.clear();
              int64_t raw_key = 0;
              for (const slot_t offset : plan.key_offsets) {
                if (is_null(offset)) {
                  scratch.emplace_back();
                } else {
                  scratch.push_back(CanonicalDistinctValue(make_value(offset)));
                }
                if (scratch.size() == 1 && int_key) {
                  raw_key = scratch.back().value.int_value;
                }
              }
              bool inserted = false;
              group_index =
                  local.FindOrInsert(scratch, raw_key, plan.aggregates.size(),
                                     aggregate_expressions, &inserted);
            }

            ++aggregate_updates[worker];
            if (local.slim) {
              const std::vector<SlimAggKind>& kinds = *local.slim_kinds;
              for (size_t i = 0; i < plan.aggregates.size(); ++i) {
                const TypedAggregateInput& input = plan.aggregates[i];
                const SlimAggKind kind = kinds[i];
                if (input.double_expr.has_value()) {
                  double addend = 0.0;
                  if (!eval_double_expr(*input.double_expr, &addend)) {
                    continue;
                  }
                  if (kind == SlimAggKind::kSumDouble) {
                    if (std::isinf(addend)) {
                      local.SlimFlags(group_index, i) |= 1;
                    }
                    local.SlimDoubleTotal(group_index, i) += addend;
                    ++local.SlimCount(group_index, i);
                    continue;
                  }
                  if (kind == SlimAggKind::kAvgDouble) {
                    local.SlimDoubleTotal(group_index, i) += addend;
                    ++local.SlimCount(group_index, i);
                    continue;
                  }
                  fold_row_values[i] = Value(addend);
                  if (!ApplySlimValue(&local, group_index, i, kind,
                                      &fold_row_values[i], &err)) {
                    return;
                  }
                  continue;
                }
                if (input.int_expr.has_value()) {
                  int64_t addend = 0;
                  if (!eval_int_expr(*input.int_expr, &addend, &err)) {
                    if (err != Status::kSuccess) {
                      return;
                    }
                    continue;
                  }
                  if (kind == SlimAggKind::kSumInt64) {
                    int64_t updated = 0;
                    if (__builtin_add_overflow(
                            local.SlimIntTotal(group_index, i), addend,
                            &updated)) {
                      err = StatusError(StatusCode::kIsInfinity,
                                        "integer overflow in SUM");
                      return;
                    }
                    local.SlimIntTotal(group_index, i) = updated;
                    ++local.SlimCount(group_index, i);
                    continue;
                  }
                  if (kind == SlimAggKind::kAvgDouble) {
                    local.SlimDoubleTotal(group_index, i) +=
                        static_cast<double>(addend);
                    ++local.SlimCount(group_index, i);
                    continue;
                  }
                  fold_row_values[i] = Value(addend);
                  if (!ApplySlimValue(&local, group_index, i, kind,
                                      &fold_row_values[i], &err)) {
                    return;
                  }
                  continue;
                }
                ValueType cell_type = ValueType::kNull;
                int64_t cell = 0;
                bool cell_unsigned = false;
                std::optional<slot_t> fallback_col;
                if (input.conditional.has_value()) {
                  if (!eval_filter_compare(input.conditional->condition)) {
                    continue;
                  }
                  if (input.conditional->then_column.has_value()) {
                    const slot_t col = *input.conditional->then_column;
                    if (is_null(col)) {
                      continue;
                    }
                    cell_type = proj_types[col];
                    cell = blk.columns[proj_to_full[col]].int_cells[r];
                    cell_unsigned = proj_unsigned[col] != 0;
                    fallback_col = col;
                  } else {
                    cell_type = input.conditional->result_type;
                    cell = input.conditional->then_int_constant;
                  }
                } else if (input.column.has_value()) {
                  const slot_t column = *input.column;
                  if (is_null(column)) {
                    continue;
                  }
                  cell_type = proj_types[column];
                  cell = blk.columns[proj_to_full[column]].int_cells[r];
                  cell_unsigned = proj_unsigned[column] != 0;
                  fallback_col = column;
                } else {
                  if (kind == SlimAggKind::kCount) {
                    ++local.SlimCount(group_index, i);
                    continue;
                  }
                  if (!ApplySlimValue(&local, group_index, i, kind, nullptr,
                                      &err)) {
                    return;
                  }
                  continue;
                }
                bool handled = true;
                switch (kind) {
                  case SlimAggKind::kCount:
                    ++local.SlimCount(group_index, i);
                    break;
                  case SlimAggKind::kSumInt64:
                    if (cell_type == ValueType::kInt64 && !cell_unsigned) {
                      int64_t updated = 0;
                      if (__builtin_add_overflow(
                              local.SlimIntTotal(group_index, i), cell,
                              &updated)) {
                        err = StatusError(StatusCode::kIsInfinity,
                                          "integer overflow in SUM");
                        return;
                      }
                      local.SlimIntTotal(group_index, i) = updated;
                      ++local.SlimCount(group_index, i);
                    } else {
                      handled = false;
                    }
                    break;
                  case SlimAggKind::kSumDouble:
                    if (cell_type == ValueType::kDouble) {
                      const auto addend = std::bit_cast<double>(cell);
                      if (std::isinf(addend)) {
                        local.SlimFlags(group_index, i) |= 1;
                      }
                      local.SlimDoubleTotal(group_index, i) += addend;
                      ++local.SlimCount(group_index, i);
                    } else {
                      handled = false;
                    }
                    break;
                  case SlimAggKind::kAvgDouble:
                    if (cell_type == ValueType::kDouble) {
                      local.SlimDoubleTotal(group_index, i) +=
                          std::bit_cast<double>(cell);
                      ++local.SlimCount(group_index, i);
                    } else if (cell_type == ValueType::kInt64) {
                      local.SlimDoubleTotal(group_index, i) +=
                          cell_unsigned
                              ? static_cast<double>(static_cast<uint64_t>(cell))
                              : static_cast<double>(cell);
                      ++local.SlimCount(group_index, i);
                    } else {
                      handled = false;
                    }
                    break;
                  case SlimAggKind::kCountDistinctInt64:
                    if (cell_type == ValueType::kInt64 ||
                        cell_type == ValueType::kDate) {
                      static_cast<void>(
                          local.SlimInsertDistinct(group_index, i, cell));
                    } else {
                      handled = false;
                    }
                    break;
                  default:
                    handled = false;
                    break;
                }
                if (handled) {
                  continue;
                }
                if (fallback_col.has_value()) {
                  fold_row_values[i] = make_value(*fallback_col);
                } else {
                  fold_row_values[i] = Value(cell);
                  fold_row_values[i].type = cell_type;
                }
                if (!ApplySlimValue(&local, group_index, i, kind,
                                    &fold_row_values[i], &err)) {
                  return;
                }
              }
              continue;
            }

            for (size_t i = 0; i < plan.aggregates.size(); ++i) {
              const TypedAggregateInput& input = plan.aggregates[i];
              AggregateAccumulator& accumulator = local.State(group_index, i);
              if (input.column.has_value()) {
                const Value value = is_null(*input.column)
                                        ? Value()
                                        : make_value(*input.column);
                if (Status st = accumulator.Add(value);
                    st != Status::kSuccess) {
                  err = st;
                  return;
                }
                continue;
              }
              if (input.conditional.has_value()) {
                Value value;
                if (eval_filter_compare(input.conditional->condition)) {
                  if (input.conditional->then_column.has_value()) {
                    const slot_t col = *input.conditional->then_column;
                    if (!is_null(col)) {
                      value = make_value(col);
                    }
                  } else {
                    value = Value(input.conditional->then_int_constant);
                    value.type = input.conditional->result_type;
                  }
                }
                if (Status st = accumulator.Add(value);
                    st != Status::kSuccess) {
                  err = st;
                  return;
                }
                continue;
              }
              if (input.double_expr.has_value()) {
                double dbl = 0.0;
                const Value value = eval_double_expr(*input.double_expr, &dbl)
                                        ? Value(dbl)
                                        : Value();
                if (Status st = accumulator.Add(value);
                    st != Status::kSuccess) {
                  err = st;
                  return;
                }
                continue;
              }
              if (input.int_expr.has_value()) {
                int64_t iv = 0;
                const bool ok = eval_int_expr(*input.int_expr, &iv, &err);
                if (err != Status::kSuccess) {
                  return;
                }
                const Value value = ok ? Value(iv) : Value();
                if (Status st = accumulator.Add(value);
                    st != Status::kSuccess) {
                  err = st;
                  return;
                }
                continue;
              }
              if (Status st = accumulator.Add(Value(1));
                  st != Status::kSuccess) {
                err = st;
                return;
              }
            }
          }
        }
        if (monotonic_int_key && local.group_count > 1) {
          bool emp = false;
          *local.int_index.TryEmplace(cur_mono_key, &emp) =
              local.group_count - 1;
        }
        return;
      }
      for (size_t morsel = morsel_begin; morsel < morsel_end;
           morsel += morsel_step) {
        if (worker_errors[worker] != Status::kSuccess) {
          break;
        }
        Iterator iterator = table.BeginMorselScan(
            context.txn_, morsels[morsel], projection_opt, nullptr,
            active_key_col, peek_ptr, active_rf, active_rf_type);
        if (typed_fill) {
          // Typed fill: decode schema-typed cell batches (no per-cell Value),
          // fold from raw cells, mirror every row into the morsel's arena run.
          // The published cache then holds the arena WITHOUT Rows; consumers
          // that cannot stream cells materialize rows from the arena on
          // demand (see the cache-hit path above).
          TypedCellBatch batch;
          MorselFillBuffer fill_buffer;
          fill_buffer.mirror.stride = fill_arena->stride;
          fill_buffer.mirror.slot_types = fill_arena->slot_types;
          fill_buffer.mirror.null_bits.resize(fill_arena->stride);
          fill_buffer.mirror.varchars.resize(fill_arena->varchars.size());
          for (;;) {
            const size_t decoded = iterator.DecodeNextTypedBatch(&batch, 2048);
            if (decoded == 0) {
              break;
            }
            scanned[worker] += decoded;
            iterator.DropPageLatch();
            if (fill_cache) {
              AppendBatchToArena(batch, 0, decoded, &fill_buffer.mirror);
            }
            if (bulk_count) {
              bool inserted = false;
              const size_t group = workers[worker].FindOrInsert(
                  {}, 0, plan.aggregates.size(), aggregate_expressions,
                  &inserted);
              for (size_t aggregate = 0; aggregate < plan.aggregates.size();
                   ++aggregate) {
                auto& count = workers[worker].SlimCount(group, aggregate);
                if (std::cmp_greater(
                        decoded, std::numeric_limits<int64_t>::max() - count)) {
                  worker_errors[worker] = StatusError(
                      StatusCode::kIsInfinity, "integer overflow in COUNT");
                  break;
                }
                count += static_cast<int64_t>(decoded);
              }
              emitted[worker] += decoded;
              aggregate_updates[worker] += decoded;
              if (!worker_errors[worker].ok()) {
                break;
              }
              continue;
            }
            const bool direct_fold =
                plan.filter_programs.empty() &&
                std::ranges::none_of(plan.aggregates, [](const auto& input) {
                  return input.child.has_value();
                });
            Row decoded_row(std::vector<Value>(batch.stride));
            for (size_t r = 0; r < decoded; ++r) {
              if (!direct_fold) {
                size_t varchar_slot = 0;
                for (size_t c = 0; c < batch.stride; ++c) {
                  Value value;
                  if (!batch.nulls[(r * batch.stride) + c]) {
                    if (batch.slot_types[c] == ValueType::kVarChar) {
                      value =
                          Value(std::string(batch.Varchar(r, varchar_slot)));
                    } else if (batch.slot_types[c] == ValueType::kDouble) {
                      value = Value(std::bit_cast<double>(
                          batch.cells[(r * batch.stride) + c]));
                    } else {
                      value = Value(batch.cells[(r * batch.stride) + c]);
                      value.type = batch.slot_types[c];
                      if (batch.slot_unsigned[c]) {
                        value = value.WithUnsigned();
                      }
                    }
                  }
                  if (batch.slot_types[c] == ValueType::kVarChar) {
                    ++varchar_slot;
                  }
                  decoded_row.values_[c] = std::move(value);
                }
                accumulate_row(decoded_row, &workers[worker],
                               &worker_errors[worker], &emitted[worker],
                               &aggregate_updates[worker], nullptr, &scratch);
                if (!worker_errors[worker].ok()) {
                  break;
                }
                continue;
              }
              // Warm the int-key group slot a few rows ahead; the probe in
              // fold_cell_row below is otherwise a cold cache miss on the
              // first touch of each hot group's line.
              constexpr size_t kKeyPrefetchDistance = 4;
              if (int_key && r + kKeyPrefetchDistance < decoded &&
                  batch.nulls[((r + kKeyPrefetchDistance) * batch.stride) +
                              plan.key_offsets[0]] == 0) {
                workers[worker].PrefetchGroupSlot(
                    batch.cells[((r + kKeyPrefetchDistance) * batch.stride) +
                                plan.key_offsets[0]]);
              }
              fold_cell_row(batch, r, &workers[worker], &worker_errors[worker],
                            &emitted[worker], &aggregate_updates[worker],
                            &scratch, &fold_row_values);
              if (worker_errors[worker] != Status::kSuccess) {
                break;
              }
            }
            if (worker_errors[worker] != Status::kSuccess) {
              break;
            }
          }
          if (worker_errors[worker] == Status::kSuccess &&
              iterator.GetStatus() != Status::kSuccess) {
            worker_errors[worker] = iterator.GetStatus();
          }
          if (fill_cache) {
            publish_morsel(morsel, &fill_buffer);
          }
          if (worker_errors[worker] != Status::kSuccess) {
            break;
          }
          continue;
        }
        MorselFillBuffer fill_buffer;
        if (fill_cache && fill_arena != nullptr) {
          fill_buffer.arena_stride = fill_arena->stride;
        }
        MorselFillBuffer* fill_target = fill_cache ? &fill_buffer : nullptr;
        for (; iterator.IsValid(); ++iterator) {
          ++scanned[worker];
          accumulate_row(*iterator, &workers[worker], &worker_errors[worker],
                         &emitted[worker], &aggregate_updates[worker],
                         fill_target, &scratch);
          if (worker_errors[worker] != Status::kSuccess) {
            break;
          }
        }
        if (fill_cache) {
          publish_morsel(morsel, &fill_buffer);
        }
      }
    } catch (...) {
      if (!worker_exception) {
        worker_exception = std::current_exception();
      }
    }
  };
  std::vector<std::jthread> threads;
  for (size_t worker = 1; worker < worker_count; ++worker) {
    try {
      threads.emplace_back([&, worker] { run_worker(worker); });
    } catch (...) {
      break;  // Resource exhaustion: worker 0 drains the rest serially.
    }
  }
  run_worker(0);
  threads.clear();
  if (worker_exception != nullptr) {
    std::rethrow_exception(worker_exception);
  }
  for (const Status& error : worker_errors) {
    if (error != Status::kSuccess) {
      return error;
    }
  }

  // Publish the cache: the in-order morsel moves above already built the
  // relation while workers ran; once every worker joined, all morsels of a
  // successful run are guaranteed published.
  if (fill_cache) {
    cache_owner->FinishSpill();
    const std::string cache_key = BaseRelationCacheKey(
        table_name, projection.empty() ? nullptr : &projection);
    runtime->base_relations.emplace(cache_key, cache_owner);
    if (fill_arena != nullptr && fill_arena->rows == published_rows) {
      fill_arena->published_rowsless = typed_fill;
      runtime->base_relation_arenas.emplace(cache_key, fill_arena);
    }
  } else if (publish_lstable_marker && runtime != nullptr) {
    auto marker = std::make_shared<Relation>(runtime);
    marker->schema = qualified_schema;
    runtime->base_relations.emplace(fold_cache_key, std::move(marker));
  }

  // Merge worker states in worker order (deterministic group creation order).
  if (fast_dict_double_agg && bounded_int_group_key) {
    const ValueType ktype = proj_types[plan.key_offsets[0]];
    const bool kunsigned = proj_unsigned[plan.key_offsets[0]] != 0;
    const size_t emit_workers = (bounded_int_span >= 4096 && worker_count > 1 &&
                                 plan.order_items.empty() && !bounded &&
                                 (!having_expr || !positional_having.empty()))
                                    ? worker_count
                                    : 1;
    for (size_t ew = 0; ew < emit_workers; ++ew) {
      const size_t idx_begin = (bounded_int_span * ew) / emit_workers;
      const size_t idx_end = (bounded_int_span * (ew + 1)) / emit_workers;
      TypedWorkerGroups& dst = workers[ew];
      for (size_t idx = idx_begin; idx < idx_end; ++idx) {
        uint64_t total_cnt = 0;
        double total_sum = 0.0;
        int64_t total_isum = 0;
        for (size_t w = 0; w < worker_count; ++w) {
          if (!bounded_worker_counts[w].empty()) {
            const uint32_t c = bounded_worker_counts[w][idx];
            if (c != 0) {
              total_cnt += c;
              if (bounded_has_double_term) {
                total_sum += bounded_worker_sums[w][idx];
              }
              if (bounded_has_int_term) {
                total_isum += bounded_worker_int_sums[w][idx];
              }
            }
          }
        }
        if (total_cnt == 0) {
          continue;
        }
        const int64_t raw_key = bounded_int_min + static_cast<int64_t>(idx);
        const size_t g = dst.AppendMonotonicIntGroup(raw_key, ktype, kunsigned);
        for (size_t i = 0; i < plan.aggregates.size(); ++i) {
          dst.SlimCount(g, i) = static_cast<int64_t>(total_cnt);
          const SlimAggKind kind = (*dst.slim_kinds)[i];
          if (kind == SlimAggKind::kSumInt64) {
            dst.SlimIntTotal(g, i) = total_isum;
          } else if (kind == SlimAggKind::kSumDouble ||
                     kind == SlimAggKind::kAvgDouble) {
            const double val = bounded_has_int_term
                                   ? static_cast<double>(total_isum)
                                   : total_sum;
            dst.SlimDoubleTotal(g, i) = val;
            if (std::isinf(val)) [[unlikely]] {
              dst.SlimFlags(g, i) |= 1;
            }
          }
        }
      }
    }
  }
  TypedWorkerGroups merged;
  merged.int_key = int_key;
  merged.packed_varchar_key = packed_varchar_key;
  merged.shapes = &aggregate_shapes;
  merged.Configure(!flat_fold_disabled, plan.key_offsets.size(),
                   plan.aggregates.size());
  merged.ConfigureSlim(plan);
  size_t total_worker_groups = 0;
  size_t total_reclaimed_groups = 0;
  for (size_t worker = 0; worker < worker_count; ++worker) {
    total_worker_groups += workers[worker].GroupCount();
    total_reclaimed_groups += reclaimed_groups[worker];
  }
  bool can_parallel_disjoint_emit =
      int_key && plan.key_offsets.size() == 1 && !flat_fold_disabled &&
      plan.slim &&
      (total_worker_groups + total_reclaimed_groups >= 4096 ||
       monotonic_int_key ||
       (fast_dict_double_agg && bounded_int_group_key &&
        bounded_int_span >= 4096)) &&
      plan.order_items.empty() && !bounded &&
      (!having_expr || !positional_having.empty());
  if (can_parallel_disjoint_emit) {
    size_t prev_w = worker_count;
    for (size_t w = 0; w < worker_count; ++w) {
      if (workers[w].GroupCount() == 0) {
        continue;
      }
      if (workers[w].has_non_int_key || !workers[w].row_index.empty() ||
          workers[w].min_int_key > workers[w].max_int_key) {
        can_parallel_disjoint_emit = false;
        break;
      }
      if (prev_w < worker_count &&
          workers[prev_w].max_int_key > workers[w].min_int_key) {
        can_parallel_disjoint_emit = false;
        break;
      }
      prev_w = w;
    }
  }
  if (can_parallel_disjoint_emit) {
    std::vector<size_t> skipped_group(worker_count, SIZE_MAX);
    size_t boundary_merged_groups = 0;
    size_t prev_w = worker_count;
    for (size_t w = 0; w < worker_count; ++w) {
      if (workers[w].GroupCount() == 0) {
        continue;
      }
      if (prev_w < worker_count &&
          workers[prev_w].max_int_key == workers[w].min_int_key) {
        const int64_t bkey = workers[w].min_int_key;
        const size_t* dst_slot = workers[prev_w].int_index.Find(bkey);
        const size_t* src_slot = workers[w].int_index.Find(bkey);
        if (dst_slot != nullptr && src_slot != nullptr) {
          const size_t target = *dst_slot;
          const size_t group = *src_slot;
          TypedWorkerGroups& dst = workers[prev_w];
          TypedWorkerGroups& src = workers[w];
          for (size_t i = 0; i < plan.aggregates.size(); ++i) {
            const SlimAggKind kind = (*dst.slim_kinds)[i];
            if (kind == SlimAggKind::kCountDistinctInt64) {
              if (src.SlimDistinctTotal(group, i) == 0) {
                continue;
              }
              const size_t src_state = (group * src.agg_stride) + i;
              const uint8_t n = src.slim_distinct_n[src_state];
              const int64_t* slots =
                  &src.slim_distinct[src_state * kSlimDistinctSlots];
              for (size_t s = 0; s < n; ++s) {
                static_cast<void>(dst.SlimInsertDistinct(target, i, slots[s]));
              }
              if (src_state < src.slim_distinct_overflow.size() &&
                  src.slim_distinct_overflow[src_state]) {
                for (const int64_t raw :
                     *src.slim_distinct_overflow[src_state]) {
                  static_cast<void>(dst.SlimInsertDistinct(target, i, raw));
                }
              }
              continue;
            }
            if (src.SlimCount(group, i) == 0) {
              continue;
            }
            switch (kind) {
              case SlimAggKind::kCount:
                dst.SlimCount(target, i) += src.SlimCount(group, i);
                break;
              case SlimAggKind::kSumInt64: {
                int64_t updated = 0;
                if (__builtin_add_overflow(dst.SlimIntTotal(target, i),
                                           src.SlimIntTotal(group, i),
                                           &updated)) {
                  return StatusError(StatusCode::kIsInfinity,
                                     "integer overflow in SUM");
                }
                dst.SlimIntTotal(target, i) = updated;
                dst.SlimCount(target, i) += src.SlimCount(group, i);
                break;
              }
              case SlimAggKind::kSumDouble:
                dst.SlimDoubleTotal(target, i) += src.SlimDoubleTotal(group, i);
                dst.SlimFlags(target, i) |= src.SlimFlags(group, i);
                dst.SlimCount(target, i) += src.SlimCount(group, i);
                break;
              case SlimAggKind::kAvgDouble:
                dst.SlimDoubleTotal(target, i) += src.SlimDoubleTotal(group, i);
                dst.SlimCount(target, i) += src.SlimCount(group, i);
                break;
              default:
                return StatusError(StatusCode::kInvalidArgument,
                                   "aggregate has no slim state");
            }
          }
          skipped_group[w] = group;
          ++boundary_merged_groups;
        }
      }
      prev_w = w;
    }

    size_t total_scan_rows = 0;
    size_t total_scan_output = 0;
    for (size_t worker = 0; worker < worker_count; ++worker) {
      total_scan_rows += scanned[worker];
      total_scan_output += emitted[worker];
    }
    if (runtime != nullptr) {
      runtime->scan_ms += ElapsedMs(scan_begin);
      runtime->filter_ms += ElapsedMs(scan_begin);
      runtime->scan_rows += total_scan_rows;
      runtime->scan_output_rows += total_scan_output;
      runtime->scan_values_decoded +=
          total_scan_rows * qualified_schema.ColumnCount();
      runtime->scan_values_available +=
          total_scan_rows * table_schema.ColumnCount();
      runtime->typed_parallel_aggregations += 1;
      runtime->aggregate_groups +=
          total_worker_groups + total_reclaimed_groups - boundary_merged_groups;
      runtime->aggregate_input_rows += total_scan_output;
      for (const size_t worker_updates : aggregate_updates) {
        runtime->aggregate_updates += worker_updates;
      }
    }

    const auto aggregate_begin = std::chrono::steady_clock::now();
    std::vector<std::vector<Row>> worker_outputs(worker_count);
    std::vector<Status> emit_errors(worker_count, Status::kSuccess);
    std::exception_ptr emit_exception;
    const auto emit_worker = [&](size_t w) {
      try {
        TypedWorkerGroups& local = workers[w];
        const size_t gcount = local.GroupCount();
        if (gcount > 0) {
          std::vector<Value> local_finished;
          local_finished.reserve(plan.aggregates.size());
          auto& out_rows = worker_outputs[w];
          if (!having_expr) {
            out_rows.reserve(gcount);
          }
          for (size_t group = 0; group < gcount; ++group) {
            if (group == skipped_group[w]) {
              continue;
            }
            local_finished.clear();
            for (size_t i = 0; i < plan.aggregates.size(); ++i) {
              const int64_t count = local.SlimCount(group, i);
              Value finished;
              switch ((*local.slim_kinds)[i]) {
                case SlimAggKind::kCount:
                  finished = Value(count);
                  break;
                case SlimAggKind::kSumInt64:
                  finished = count == 0 ? Value()
                                        : Value(local.SlimIntTotal(group, i));
                  break;
                case SlimAggKind::kSumDouble: {
                  if (count == 0) {
                    break;
                  }
                  const double sum_result = local.SlimDoubleTotal(group, i);
                  if ((local.SlimFlags(group, i) & 1) == 0 &&
                      std::isinf(sum_result)) {
                    emit_errors[w] =
                        StatusError(StatusCode::kIsInfinity,
                                    "SUM overflow: result is infinite");
                    return;
                  }
                  finished = Value(sum_result);
                  break;
                }
                case SlimAggKind::kAvgDouble:
                  finished = count == 0
                                 ? Value()
                                 : Value(local.SlimDoubleTotal(group, i) /
                                         static_cast<double>(count));
                  break;
                case SlimAggKind::kCountDistinctInt64:
                  finished = Value(
                      static_cast<int64_t>(local.SlimDistinctTotal(group, i)));
                  break;
                default:
                  emit_errors[w] = StatusError(StatusCode::kInvalidArgument,
                                               "aggregate has no slim state");
                  return;
              }
              local_finished.push_back(std::move(finished));
            }
            if (having_expr) {
              bool keep_group = true;
              for (const PositionalHaving& clause : positional_having) {
                const Value& left_value = local_finished[clause.agg_index];
                Value having_value;
                if (clause.has_constant) {
                  StatusOr<Value> compared =
                      TryBinary(clause.op, left_value, clause.constant);
                  if (!compared.HasValue()) {
                    emit_errors[w] = compared.GetStatus();
                    return;
                  }
                  having_value = compared.MoveValue();
                } else {
                  having_value = left_value;
                }
                if (!Truthy(having_value)) {
                  keep_group = false;
                  break;
                }
              }
              if (!keep_group) {
                continue;
              }
            }
            std::vector<Value> values;
            values.reserve(plan.emit_items.size());
            for (const TypedEmitItem& item : plan.emit_items) {
              if (item.is_key) {
                values.push_back(local.GroupKey(group, item.index));
              } else {
                values.push_back(local_finished[item.index]);
              }
            }
            out_rows.emplace_back(std::move(values));
          }
        }
        local = TypedWorkerGroups{};
      } catch (...) {
        if (!emit_exception) {
          emit_exception = std::current_exception();
        }
      }
    };
    std::vector<std::jthread> emit_threads;
    for (size_t w = 1; w < worker_count; ++w) {
      try {
        emit_threads.emplace_back([&, w] { emit_worker(w); });
      } catch (...) {
        emit_worker(w);
      }
    }
    emit_worker(0);
    emit_threads.clear();
    if (emit_exception != nullptr) {
      std::rethrow_exception(emit_exception);
    }
    for (const Status& err : emit_errors) {
      if (err != Status::kSuccess) {
        return err;
      }
    }
    std::vector<Column> output_columns;
    output_columns.reserve(stmt.SelectList().size());
    for (size_t i = 0; i < stmt.SelectList().size(); ++i) {
      output_columns.emplace_back(ProjectionName(stmt.SelectList()[i], i),
                                  ValueType::kNull);
    }
    Relation output(runtime);
    size_t total_emitted = 0;
    for (const auto& out_rows : worker_outputs) {
      total_emitted += out_rows.size();
    }
    output.rows.reserve(total_emitted);
    for (auto& out_rows : worker_outputs) {
      for (Row& r : out_rows) {
        output.AddRow(std::move(r));
      }
    }
    if (!output.rows.empty()) {
      for (size_t i = 0; i < output_columns.size(); ++i) {
        output_columns[i] =
            Column(output_columns[i].Name(), ValueTypeOf(output.rows[0][i]));
        output_columns[i].SetUnsigned(output.rows[0][i].IsUnsigned());
      }
    }
    if (runtime != nullptr) {
      runtime->aggregate_ms += ElapsedMs(aggregate_begin);
    }
    output.schema = Schema("", std::move(output_columns));
    return output;
  }
  if (monotonic_int_key && !can_parallel_disjoint_emit) {
    for (TypedWorkerGroups& local : workers) {
      if (local.GroupCount() > 0) {
        for (size_t g = 0; g < local.GroupCount(); ++g) {
          bool emp = false;
          *local.int_index.TryEmplace(local.GroupKey(g, 0).value.int_value,
                                      &emp) = g;
        }
        break;
      }
    }
  }
  size_t total_scan_rows = 0;
  size_t total_scan_output = 0;
  for (size_t worker = 0; worker < worker_count; ++worker) {
    total_scan_rows += scanned[worker];
    total_scan_output += emitted[worker];
    TypedWorkerGroups& local = workers[worker];
    if (local.GroupCount() == 0) {
      continue;
    }
    if (merged.GroupCount() == 0) {
      merged.int_index = std::move(local.int_index);
      merged.row_index = std::move(local.row_index);
      merged.flat_group_keys = std::move(local.flat_group_keys);
      merged.chunked_states.AdoptChunksFrom(std::move(local.chunked_states));
      merged.group_keys = std::move(local.group_keys);
      merged.group_states = std::move(local.group_states);
      merged.slim_counts = std::move(local.slim_counts);
      merged.slim_int_totals = std::move(local.slim_int_totals);
      merged.slim_double_totals = std::move(local.slim_double_totals);
      merged.slim_flags = std::move(local.slim_flags);
      merged.slim_distinct = std::move(local.slim_distinct);
      merged.slim_distinct_n = std::move(local.slim_distinct_n);
      merged.slim_distinct_overflow = std::move(local.slim_distinct_overflow);
      merged.group_count = local.group_count;
      local.group_count = 0;
      if (total_worker_groups > merged.group_count) {
        merged.ReserveGroups(total_worker_groups);
      }
      continue;
    }
    std::vector<Value> canonical_keys;
    canonical_keys.reserve(plan.key_offsets.size());
    for (size_t group = 0; group < local.GroupCount(); ++group) {
      int64_t raw_key = 0;
      bool inserted = false;
      size_t target = 0;
      // Int-key merge fast path: probe the merged table with the raw key and
      // build canonical keys only for groups that actually open here.  Every
      // worker's hit (the overwhelmingly common case: the same orderkey
      // appears in several workers' partials) then costs one map probe with
      // no Value copies at all.
      if (merged.int_key && !local.GroupKey(group, 0).IsNull()) {
        const Value& local_key0 = local.GroupKey(group, 0);
        size_t* slot = merged.RawIntSlot(local_key0.value.int_value, &inserted);
        if (inserted) {
          canonical_keys.clear();
          for (size_t k = 0; k < plan.key_offsets.size(); ++k) {
            canonical_keys.push_back(local.GroupKey(group, k));
          }
          target = merged.ClaimSlotForNewGroup(slot, canonical_keys,
                                               plan.aggregates.size(),
                                               aggregate_expressions);
        } else {
          target = *slot;
        }
      } else {
        canonical_keys.clear();
        for (size_t k = 0; k < plan.key_offsets.size(); ++k) {
          canonical_keys.push_back(local.GroupKey(group, k));
        }
        if (merged.int_key) {
          raw_key = canonical_keys[0].value.int_value;
        }
        target =
            merged.FindOrInsert(canonical_keys, raw_key, plan.aggregates.size(),
                                aggregate_expressions, &inserted);
      }
      if (inserted) {
        // FindOrInsert created empty states; adopt this worker's partials.
        for (size_t i = 0; i < plan.aggregates.size(); ++i) {
          if (merged.slim) {
            merged.SlimCount(target, i) = local.SlimCount(group, i);
            merged.SlimIntTotal(target, i) = local.SlimIntTotal(group, i);
            merged.SlimDoubleTotal(target, i) = local.SlimDoubleTotal(group, i);
            merged.SlimFlags(target, i) = local.SlimFlags(group, i);
            if ((*merged.slim_kinds)[i] == SlimAggKind::kCountDistinctInt64) {
              const size_t dst_state = (target * merged.agg_stride) + i;
              const size_t src_state = (group * local.agg_stride) + i;
              merged.slim_distinct_n[dst_state] =
                  local.slim_distinct_n[src_state];
              std::copy_n(
                  &local.slim_distinct[src_state * kSlimDistinctSlots],
                  kSlimDistinctSlots,
                  &merged.slim_distinct[dst_state * kSlimDistinctSlots]);
              if (src_state < local.slim_distinct_overflow.size() &&
                  local.slim_distinct_overflow[src_state]) {
                if (dst_state >= merged.slim_distinct_overflow.size()) {
                  merged.slim_distinct_overflow.resize(
                      merged.slim_distinct_n.size());
                }
                merged.slim_distinct_overflow[dst_state] =
                    std::move(local.slim_distinct_overflow[src_state]);
              }
            }
            continue;
          }
          merged.State(target, i) = std::move(local.State(group, i));
        }
        continue;
      }
      for (size_t i = 0; i < plan.aggregates.size(); ++i) {
        if (merged.slim) {
          // Slim merge: mirror AggregateAccumulator::Merge for the kinds the
          // slim layout admits (counts add; INT64 sums keep the overflow
          // raise; DOUBLE totals add and OR the saw-infinite-input flag;
          // COUNT(DISTINCT) unions sets).
          const SlimAggKind kind = (*merged.slim_kinds)[i];
          if (kind == SlimAggKind::kCountDistinctInt64) {
            if (local.SlimDistinctTotal(group, i) == 0) {
              continue;
            }
            const size_t src_state = (group * local.agg_stride) + i;
            const uint8_t n = local.slim_distinct_n[src_state];
            const int64_t* slots =
                &local.slim_distinct[src_state * kSlimDistinctSlots];
            for (size_t s = 0; s < n; ++s) {
              static_cast<void>(merged.SlimInsertDistinct(target, i, slots[s]));
            }
            if (src_state < local.slim_distinct_overflow.size() &&
                local.slim_distinct_overflow[src_state]) {
              for (const int64_t raw :
                   *local.slim_distinct_overflow[src_state]) {
                static_cast<void>(merged.SlimInsertDistinct(target, i, raw));
              }
            }
            continue;
          }
          if (local.SlimCount(group, i) == 0) {
            continue;
          }
          switch (kind) {
            case SlimAggKind::kCount:
              merged.SlimCount(target, i) += local.SlimCount(group, i);
              break;
            case SlimAggKind::kSumInt64: {
              int64_t updated = 0;
              if (__builtin_add_overflow(merged.SlimIntTotal(target, i),
                                         local.SlimIntTotal(group, i),
                                         &updated)) {
                return StatusError(StatusCode::kIsInfinity,
                                   "integer overflow in SUM");
              }
              merged.SlimIntTotal(target, i) = updated;
              merged.SlimCount(target, i) += local.SlimCount(group, i);
              break;
            }
            case SlimAggKind::kSumDouble:
              merged.SlimDoubleTotal(target, i) +=
                  local.SlimDoubleTotal(group, i);
              merged.SlimFlags(target, i) |= local.SlimFlags(group, i);
              merged.SlimCount(target, i) += local.SlimCount(group, i);
              break;
            case SlimAggKind::kAvgDouble:
              merged.SlimDoubleTotal(target, i) +=
                  local.SlimDoubleTotal(group, i);
              merged.SlimCount(target, i) += local.SlimCount(group, i);
              break;
            default:
              return StatusError(StatusCode::kInvalidArgument,
                                 "aggregate has no slim state");
          }
          continue;
        }
        if (local.State(group, i).count == 0) {
          continue;
        }
        if (Status st = merged.State(target, i).Merge(local.State(group, i));
            st != Status::kSuccess) {
          return st;
        }
      }
    }
  }
  if (runtime != nullptr) {
    runtime->scan_ms += ElapsedMs(scan_begin);
    runtime->filter_ms += ElapsedMs(scan_begin);
    runtime->scan_rows += total_scan_rows;
    runtime->scan_output_rows += total_scan_output;
    runtime->scan_values_decoded +=
        total_scan_rows * qualified_schema.ColumnCount();
    runtime->scan_values_available +=
        total_scan_rows * table_schema.ColumnCount();
    runtime->typed_parallel_aggregations += 1;
    runtime->aggregate_groups += merged.GroupCount();
    runtime->aggregate_input_rows += total_scan_output;
    for (const size_t worker_updates : aggregate_updates) {
      runtime->aggregate_updates += worker_updates;
    }
  }

  // Global aggregate over an empty input produces one group of NULL/0.
  if (merged.GroupCount() == 0 && plan.key_offsets.empty()) {
    std::vector<Value> canonical_keys;
    bool inserted = false;
    const size_t target =
        merged.FindOrInsert(canonical_keys, 0, plan.aggregates.size(),
                            aggregate_expressions, &inserted);
    (void)target;
    (void)inserted;
  }

  const auto aggregate_begin = std::chrono::steady_clock::now();
  // Minimal representative row for HAVING evaluation: NULLs everywhere
  // except the group-key offsets (HAVING eligibility guarantees it reads
  // nothing else from the base row).
  Row representative;
  std::vector<Column> output_columns;
  output_columns.reserve(stmt.SelectList().size());
  for (size_t i = 0; i < stmt.SelectList().size(); ++i) {
    output_columns.emplace_back(ProjectionName(stmt.SelectList()[i], i),
                                ValueType::kNull);
  }
  Relation output(runtime);
  struct KeyedRow {
    std::vector<Value> keys;
    Row row;
    size_t sequence{0};
  };
  std::vector<KeyedRow> sortable;
  const auto precedes = [&plan](const KeyedRow& left, const KeyedRow& right) {
    for (size_t i = 0; i < plan.order_items.size(); ++i) {
      const Value& a = left.keys[i];
      const Value& b = right.keys[i];
      const int c = CompareForOrderBy(a, b);
      if (c == 0) {
        continue;
      }
      const bool nulls_first = plan.order_nulls_first[i];
      const bool a_less = c < 0;
      if (a.IsNull()) {
        return nulls_first;
      }
      if (b.IsNull()) {
        return !nulls_first;
      }
      return plan.order_ascending[i] ? a_less : !a_less;
    }
    return left.sequence < right.sequence;
  };
  // The group count is final before emit; reserving up front keeps the rows
  // vector from re-moving every emitted Row on geometric growth.
  output.rows.reserve(bounded
                          ? std::min(topn_capacity, merged.GroupCount())
                          : (having_expr != nullptr
                                 ? std::min<size_t>(merged.GroupCount(), 1024)
                                 : merged.GroupCount()));
  size_t next_sequence = 0;
  const auto retain = [&](KeyedRow keyed) {
    keyed.sequence = next_sequence++;
    if (!bounded) {
      sortable.push_back(std::move(keyed));
      return;
    }
    if (sortable.size() < topn_capacity) {
      sortable.push_back(std::move(keyed));
      std::push_heap(sortable.begin(), sortable.end(), precedes);
      return;
    }
    if (topn_capacity != 0 && precedes(keyed, sortable.front())) {
      std::pop_heap(sortable.begin(), sortable.end(), precedes);
      sortable.back() = std::move(keyed);
      std::push_heap(sortable.begin(), sortable.end(), precedes);
    }
  };
  std::vector<Value> finished_values;
  const size_t merged_group_count = merged.GroupCount();
  for (size_t group = 0; group < merged_group_count; ++group) {
    finished_values.clear();
    finished_values.reserve(plan.aggregates.size());
    if (merged.slim) {
      for (size_t i = 0; i < plan.aggregates.size(); ++i) {
        const int64_t count = merged.SlimCount(group, i);
        Value finished;
        switch ((*merged.slim_kinds)[i]) {
          case SlimAggKind::kCount:
            finished = Value(count);
            break;
          case SlimAggKind::kSumInt64:
            finished =
                count == 0 ? Value() : Value(merged.SlimIntTotal(group, i));
            break;
          case SlimAggKind::kSumDouble: {
            if (count == 0) {
              break;
            }
            const double sum_result = merged.SlimDoubleTotal(group, i);
            if ((merged.SlimFlags(group, i) & 1) == 0 &&
                std::isinf(sum_result)) {
              // Finite inputs whose sum leaves the double range overflow:
              // GoogleSQL raises instead of returning inf.
              return StatusError(StatusCode::kIsInfinity,
                                 "SUM overflow: result is infinite");
            }
            finished = Value(sum_result);
            break;
          }
          case SlimAggKind::kAvgDouble:
            finished = count == 0 ? Value()
                                  : Value(merged.SlimDoubleTotal(group, i) /
                                          static_cast<double>(count));
            break;
          case SlimAggKind::kCountDistinctInt64:
            finished =
                Value(static_cast<int64_t>(merged.SlimDistinctTotal(group, i)));
            break;
          default:
            return StatusError(StatusCode::kInvalidArgument,
                               "aggregate has no slim state");
        }
        finished_values.push_back(std::move(finished));
      }
    } else {
      for (size_t i = 0; i < plan.aggregates.size(); ++i) {
        ASSIGN_OR_RETURN(Value, finished, merged.State(group, i).TryFinish());
        finished_values.push_back(std::move(finished));
      }
    }
    // Only HAVING reads the aggregate map; building an unordered_map per
    // group cost one allocation per group on multi-million-group folds.
    AggregateResultMap aggregate_results;
    if (having_expr && !positional_having.empty()) {
      // `AGG op literal` / bare `AGG` (or a conjunction of them): decide
      // straight from the finished values.  A NULL aggregate result compares
      // to UNKNOWN, so the group drops.
      bool keep_group = true;
      for (const PositionalHaving& clause : positional_having) {
        const Value& left_value = finished_values[clause.agg_index];
        Value having_value;
        if (clause.has_constant) {
          StatusOr<Value> compared =
              TryBinary(clause.op, left_value, clause.constant);
          if (!compared.HasValue()) {
            return compared.GetStatus();
          }
          having_value = compared.MoveValue();
        } else {
          having_value = left_value;
        }
        if (!Truthy(having_value)) {
          keep_group = false;
          break;
        }
      }
      if (!keep_group) {
        continue;
      }
    } else if (having_expr) {
      aggregate_results.reserve(plan.aggregates.size());
      for (size_t i = 0; i < plan.aggregates.size(); ++i) {
        aggregate_results.emplace(plan.aggregates[i].aggregate,
                                  finished_values[i]);
      }
      if (representative.values_.size() != qualified_schema.ColumnCount()) {
        representative =
            Row(std::vector<Value>(qualified_schema.ColumnCount(), Value()));
      }
      for (size_t k = 0; k < plan.key_offsets.size(); ++k) {
        representative.values_[plan.key_offsets[k]] = merged.GroupKey(group, k);
      }
      Scope scope{.row = &representative,
                  .schema = &qualified_schema,
                  .outer = nullptr};
      scope.aggregates = &aggregate_results;
      ASSIGN_OR_RETURN(
          Value, having_value,
          (TryEvaluate(having_expr, scope, &aggregate_results, context, ctes)));
      if (!Truthy(having_value)) {
        continue;
      }
    }
    std::vector<Value> values;
    values.reserve(plan.emit_items.size());
    for (const TypedEmitItem& item : plan.emit_items) {
      if (item.is_key) {
        values.push_back(merged.GroupKey(group, item.index));
      } else {
        values.push_back(finished_values[item.index]);
      }
    }
    Row output_row(std::move(values));
    if (output.rows.empty() && sortable.empty()) {
      for (size_t i = 0; i < output_columns.size(); ++i) {
        output_columns[i] =
            Column(output_columns[i].Name(), ValueTypeOf(output_row[i]));
        output_columns[i].SetUnsigned(output_row[i].IsUnsigned());
      }
    }
    if (!plan.order_items.empty()) {
      std::vector<Value> keys;
      keys.reserve(plan.order_items.size());
      for (const TypedEmitItem& item : plan.order_items) {
        if (item.is_key) {
          keys.push_back(merged.GroupKey(group, item.index));
        } else {
          keys.push_back(finished_values[item.index]);
        }
      }
      retain(KeyedRow{.keys = std::move(keys), .row = std::move(output_row)});
    } else {
      output.AddRow(std::move(output_row));
    }
  }
  if (bounded && runtime != nullptr) {
    ++runtime->topn_finish_uses;
    runtime->topn_finish_capacity = topn_capacity;
    runtime->topn_finish_input_rows = next_sequence;
  }
  if (!sortable.empty()) {
    if (bounded) {
      std::ranges::sort(sortable, precedes);
    } else {
      std::ranges::stable_sort(sortable, precedes);
    }
    for (KeyedRow& keyed : sortable) {
      output.AddRow(std::move(keyed.row));
    }
  }
  if (runtime != nullptr) {
    runtime->aggregate_ms += ElapsedMs(aggregate_begin);
  }
  output.schema = Schema("", std::move(output_columns));
  return output;
}

}  // namespace tinylamb::relational_detail
