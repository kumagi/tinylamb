/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/detail/typed_stream_aggregate_partitioned.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/parallel_work.hpp"
#include "common/query_parallelism.hpp"
#include "common/spill/spill_store.hpp"
#include "common/status_or.hpp"
#include "executor/detail/columnar_tlcs_fast_path.hpp"
#include "executor/detail/columnar_tlcs_internal.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/detail/planning_heuristics.hpp"
#include "executor/query_memory.hpp"
#include "table/tlcs_file.hpp"
#include "type/collation.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb::relational_detail {
namespace {

inline void CopyNonDistinctAggState(const RuntimeAggState& src,
                                    RuntimeAggState& dst) noexcept {
  dst.int_val = src.int_val;
  dst.double_val = src.double_val;
  dst.count = src.count;
  dst.str_ptr = src.str_ptr;
  dst.str_len = src.str_len;
  dst.has_value = src.has_value;
}

inline int64_t CanonicalDistinctIntKey(ColAggKind kind, int64_t raw) noexcept {
  if (kind == ColAggKind::kCountDistinctDouble) {
    const auto d = std::bit_cast<double>(raw);
    if (d == 0.0) {
      return 0;
    }
    if (std::isnan(d)) {
      return int64_t{0x7ff8000000000000LL};
    }
  }
  return raw;
}

inline int64_t CanonicalDoubleBits(double d) noexcept {
  if (d == 0.0) {
    d = 0.0;
  }
  if (std::isnan(d)) {
    return int64_t{0x7ff8000000000000LL};
  }
  return std::bit_cast<int64_t>(d);
}

inline bool IsVarCharKeyKind(ColKeyKind kind) noexcept {
  return kind == ColKeyKind::kColVarChar ||
         kind == ColKeyKind::kCaseWhenVarChar ||
         kind == ColKeyKind::kRegexReplaceVarChar;
}

inline bool IsFixedNumericKeyKind(ColKeyKind kind) noexcept {
  return !IsVarCharKeyKind(kind);
}

inline bool IsDistinctAggKind(ColAggKind kind) noexcept {
  return kind == ColAggKind::kCountDistinctInt64 ||
         kind == ColAggKind::kCountDistinctDate ||
         kind == ColAggKind::kCountDistinctDouble ||
         kind == ColAggKind::kCountDistinctVarChar;
}

inline bool IsFixedNumericAggKind(ColAggKind kind) noexcept {
  return kind == ColAggKind::kCountStar || kind == ColAggKind::kCountCol ||
         kind == ColAggKind::kCountByteLen || kind == ColAggKind::kSumInt64 ||
         kind == ColAggKind::kSumDouble || kind == ColAggKind::kSumByteLen ||
         kind == ColAggKind::kAvgDouble || kind == ColAggKind::kAvgByteLen ||
         kind == ColAggKind::kMinInt64 || kind == ColAggKind::kMinDate ||
         kind == ColAggKind::kMinDouble || kind == ColAggKind::kMinByteLen ||
         kind == ColAggKind::kMaxInt64 || kind == ColAggKind::kMaxDate ||
         kind == ColAggKind::kMaxDouble || kind == ColAggKind::kMaxByteLen;
}

// Reconstructs full key specifications (minus redundant functionally dependent
// `kColInt64Delta` or `kConstInt64` keys when collapsed) -> physical keys.
struct KeyProjectionMap {
  std::vector<ColGroupKeySpec> phys_specs;
  // Maps full key index i -> physical index, or -1 if derived.
  std::vector<int32_t> full_to_phys;
  // If full_to_phys[i] < 0 and key_specs[i].kind == kColInt64Delta, base phys
  // slot whose int value + key_specs[i].const_int yields full key i.
  std::vector<int32_t> delta_base_phys;

  void ReconstructFull(const std::vector<ColGroupKeySpec>& full_specs,
                       const RuntimeKeyCell* phys_cells,
                       RuntimeKeyCell* out_full) const noexcept {
    for (size_t i = 0; i < full_specs.size(); ++i) {
      const int32_t p = full_to_phys[i];
      if (p >= 0) {
        out_full[i] = phys_cells[p];
      } else if (full_specs[i].kind == ColKeyKind::kConstInt64) {
        out_full[i] = RuntimeKeyCell{.int_val = full_specs[i].const_int};
      } else {
        const RuntimeKeyCell& base = phys_cells[delta_base_phys[i]];
        if (base.is_null) {
          out_full[i] = RuntimeKeyCell{.is_null = true};
        } else {
          out_full[i] =
              RuntimeKeyCell{.int_val = base.int_val + full_specs[i].const_int};
        }
      }
    }
  }
};

StatusOr<KeyProjectionMap> BuildKeyProjectionMap(
    const TlcsFile& tlcs, const std::vector<uint32_t>& candidate_blocks,
    const std::vector<ColGroupKeySpec>& key_specs) {
  KeyProjectionMap km;
  km.full_to_phys.assign(key_specs.size(), -1);
  km.delta_base_phys.assign(key_specs.size(), -1);

  bool any_delta = false;
  for (const auto& ks : key_specs) {
    if (ks.kind == ColKeyKind::kColInt64Delta) {
      any_delta = true;
      break;
    }
  }
  bool delta_safe = false;
  if (any_delta) {
    ASSIGN_OR_RETURN(
        bool, ok,
        ValidateDeltaKeysNoOverflow(tlcs, candidate_blocks, key_specs));
    delta_safe = ok;
  }

  // Record base kColInt64 column indices so overflow-safe delta keys reuse
  // them.
  std::vector<std::pair<slot_t, int32_t>> int64_bases;
  for (size_t i = 0; i < key_specs.size(); ++i) {
    const auto& ks = key_specs[i];
    if (ks.kind == ColKeyKind::kConstInt64) {
      continue;
    }
    if (ks.kind == ColKeyKind::kColInt64) {
      const auto p = static_cast<int32_t>(km.phys_specs.size());
      km.phys_specs.push_back(ks);
      km.full_to_phys[i] = p;
      int64_bases.emplace_back(ks.full_col, p);
    }
  }
  // Rebuild preserving original order when no deltas collapsed, or keeping
  // stable physical mapping.
  if (int64_bases.empty() || !delta_safe) {
    km.phys_specs.clear();
    std::fill(km.full_to_phys.begin(), km.full_to_phys.end(), -1);
    for (size_t i = 0; i < key_specs.size(); ++i) {
      if (key_specs[i].kind == ColKeyKind::kConstInt64) {
        continue;
      }
      km.full_to_phys[i] = static_cast<int32_t>(km.phys_specs.size());
      km.phys_specs.push_back(key_specs[i]);
    }
    return km;
  }

  for (size_t i = 0; i < key_specs.size(); ++i) {
    const auto& ks = key_specs[i];
    if (ks.kind == ColKeyKind::kConstInt64 || km.full_to_phys[i] >= 0) {
      continue;
    }
    if (ks.kind == ColKeyKind::kColInt64Delta) {
      int32_t base_p = -1;
      for (const auto& [col, p] : int64_bases) {
        if (col == ks.full_col) {
          base_p = p;
          break;
        }
      }
      if (base_p >= 0) {
        km.delta_base_phys[i] = base_p;
        continue;
      }
    }
    km.full_to_phys[i] = static_cast<int32_t>(km.phys_specs.size());
    km.phys_specs.push_back(ks);
  }
  return km;
}

// Single-owner append chunk stream for one `(worker, shard)` bucket. Chunks
// grow 4 KB -> 16 KB -> 64 KB lazily and bill exclusively to one
// `QueryMemoryCharge` per `(shard, worker)` so freeing shard `s` returns all
// memory via RAII.
struct SlabChunk {
  // `make_unique_for_overwrite` default-initializes: records are memcpy'd in
  // right after, so the vector's zero-fill would double every byte written.
  // A null `data` means the chunk was spilled: `used` bytes live at
  // `spill_offset` inside the owning writer's SpillStore segment.
  std::unique_ptr<uint8_t[]> data;
  uint32_t used{0};
  uint32_t cap{0};
  uint64_t spill_offset{0};
};

class WorkerShardWriter {
 public:
  WorkerShardWriter() = default;

  Status Init(
      std::vector<SlabChunk>* out_chunks, QueryMemoryCharge* charge,
      size_t initial_bytes = 4096, SpillStore* spill_store = nullptr,
      SpillSegmentId* out_segment = nullptr,
      uint32_t resident_chunk_cap = std::numeric_limits<uint32_t>::max()) {
    out_chunks_ = out_chunks;
    charge_ = charge;
    initial_bytes_ = initial_bytes;
    spill_store_ = spill_store;
    out_segment_ = out_segment;
    resident_chunk_cap_ = std::max<uint32_t>(resident_chunk_cap, 1);
    // `TINYLAMB_ALWAYS_SPILL` / `SetTestAlwaysSpill` is the shared fault-
    // injection knob for the OOC paths: keep only the active tail chunk in
    // RAM so every completed chunk exercises the spill + read-back path.
    if (ProcessMemoryBudget::Global().AlwaysSpillEnabled()) {
      resident_chunk_cap_ = 1;
    }
    return Status::kSuccess;
  }

  // Ensures at least `bytes` contiguous bytes are available in the active tail
  // chunk and returns writable pointer without advancing `used`.
  StatusOr<uint8_t*> Reserve(size_t bytes) {
    if (bytes > std::numeric_limits<uint32_t>::max()) {
      return StatusError(StatusCode::kMemoryLimitExceeded,
                         "partitioned record exceeds chunk limit");
    }
    if (!out_chunks_->empty()) {
      SlabChunk& tail = out_chunks_->back();
      if (tail.data != nullptr && tail.used + bytes <= tail.cap) {
        return tail.data.get() + tail.used;
      }
    }
    return GrowChunk(bytes);
  }

  void Commit(size_t bytes) noexcept {
    out_chunks_->back().used += static_cast<uint32_t>(bytes);
    total_bytes_ += bytes;
  }

  Status AppendBytes(const void* src, size_t bytes) {
    ASSIGN_OR_RETURN(uint8_t*, dst, Reserve(bytes));
    std::memcpy(dst, src, bytes);
    Commit(bytes);
    return Status::kSuccess;
  }

  // Seals the spill segment (if this writer ever spilled) and publishes its
  // id through `out_segment_` so Pass 2 can read the spilled chunks back.
  // No-op for writers that stayed fully resident.
  Status Finalize() {
    if (seg_writer_ == nullptr) {
      return Status::kSuccess;
    }
    ASSIGN_OR_RETURN(SpillSegmentId, seg, seg_writer_->Finish(false));
    if (out_segment_ != nullptr) {
      *out_segment_ = seg;
    }
    return Status::kSuccess;
  }

  [[nodiscard]] size_t TotalBytes() const noexcept { return total_bytes_; }

 private:
  // Once the query budget refuses a new chunk, the writer spills its oldest
  // resident chunk into its own `SpillStore` segment and refunds the charge
  // instead of failing — the shard slabs then outgrow RAM on disk (ADR 0005).
  StatusOr<uint8_t*> GrowChunk(size_t min_bytes) {
    size_t target = initial_bytes_;
    if (out_chunks_->size() >= 4) {
      target = 65536;
    } else if (!out_chunks_->empty()) {
      target = std::min<size_t>(
          65536, std::max(initial_bytes_,
                          static_cast<size_t>(out_chunks_->back().cap) * 4));
    }
    const size_t alloc = std::max(target, min_bytes);
    if (charge_ != nullptr && !charge_->TryAdd(alloc)) {
      if (spill_store_ == nullptr || out_segment_ == nullptr) {
        return StatusError(StatusCode::kMemoryLimitExceeded,
                           "partitioned shard slab exceeded query budget");
      }
      while (!charge_->TryAdd(alloc)) {
        RETURN_IF_FAIL(SpillOldestChunk());
      }
    }
    SlabChunk next;
    next.data = std::make_unique_for_overwrite<uint8_t[]>(alloc);
    next.cap = static_cast<uint32_t>(alloc);
    next.used = 0;
    out_chunks_->push_back(std::move(next));
    if (spill_store_ != nullptr && out_segment_ != nullptr) {
      // Proactive cap: Pass 1 may not let resident slabs grow past
      // `resident_chunk_cap_` per writer — Pass 2 still needs budget headroom
      // for the per-shard group tables, so the oldest chunk spills now rather
      // than waiting for the whole query pool to saturate.
      while (out_chunks_->size() - spill_cursor_ > resident_chunk_cap_) {
        RETURN_IF_FAIL(SpillOldestChunk());
      }
    }
    return out_chunks_->back().data.get();
  }

  // Appends the oldest still-resident chunk to this writer's spill segment,
  // releases its memory charge, and marks it spilled. Resident chunks always
  // form a suffix of `out_chunks_`, so `spill_cursor_` only moves forward.
  Status SpillOldestChunk() {
    for (; spill_cursor_ < out_chunks_->size(); ++spill_cursor_) {
      SlabChunk& c = (*out_chunks_)[spill_cursor_];
      if (c.data == nullptr) {
        continue;
      }
      if (seg_writer_ == nullptr) {
        seg_writer_ = std::make_unique<SpillSegmentWriter>(spill_store_, 0);
      }
      const uint64_t off = seg_writer_->BytesWritten();
      if (c.used > 0) {
        RETURN_IF_FAIL(seg_writer_->Write(c.data.get(), c.used));
      }
      if (charge_ != nullptr) {
        charge_->Release(c.cap);
      }
      c.data.reset();
      c.cap = 0;
      c.spill_offset = off;
      ++spill_cursor_;
      return Status::kSuccess;
    }
    return StatusError(StatusCode::kMemoryLimitExceeded,
                       "partitioned shard slab exceeded query budget");
  }

  std::vector<SlabChunk>* out_chunks_{nullptr};
  QueryMemoryCharge* charge_{nullptr};
  SpillStore* spill_store_{nullptr};
  SpillSegmentId* out_segment_{nullptr};
  std::unique_ptr<SpillSegmentWriter> seg_writer_;
  size_t spill_cursor_{0};
  uint32_t resident_chunk_cap_{std::numeric_limits<uint32_t>::max()};
  size_t initial_bytes_{4096};
  size_t total_bytes_{0};
};

// Pass-2 view over one `(shard, worker)` SlabChunk list. Resident chunks are
// yielded in place; spilled chunks (`data == nullptr`) are read back from the
// writer's `SpillStore` segment into `scratch_`, so each chunk still resolves
// to one contiguous byte range for the record parse loops.
class SlabChunkReader {
 public:
  SlabChunkReader(const SpillStore* store, SpillSegmentId segment)
      : store_(store), segment_(segment) {}

  Status Resolve(const SlabChunk& c, const uint8_t** out) {
    if (c.data != nullptr) {
      *out = c.data.get();
      return Status::kSuccess;
    }
    scratch_.resize(std::max<size_t>(c.used, 1));
    if (c.used == 0) {
      // Degenerate spilled chunk (e.g. an empty tail slab): no bytes to read
      // and the writer may hold no segment at all.
      *out = scratch_.data();
      return Status::kSuccess;
    }
    if (segment_ == kInvalidSpillSegmentId) {
      return StatusError(StatusCode::kCorrupt,
                         "spilled slab chunk without a spill segment");
    }
    if (!reader_.has_value()) {
      reader_.emplace(store_, segment_, /*buffer_bytes=*/0);
      RETURN_IF_FAIL(reader_->Open());
    }
    RETURN_IF_FAIL(reader_->Seek(c.spill_offset));
    RETURN_IF_FAIL(reader_->ReadExact(scratch_.data(), c.used));
    *out = scratch_.data();
    return Status::kSuccess;
  }

 private:
  const SpillStore* store_{nullptr};
  SpillSegmentId segment_{kInvalidSpillSegmentId};
  std::optional<SpillSegmentReader> reader_;
  std::vector<uint8_t> scratch_;
};

// Shared fast single-column ORDER BY classification + final Relation emission
// helper for materialized `FinalMaterializedRow` survivors.
enum class PartOrderKind : uint8_t { kNone, kInt, kDouble };

PartOrderKind ClassifyPartOrderKind(
    const std::vector<ColumnarOrderBinding>& order_bindings,
    const std::vector<ColGroupKeySpec>& key_specs,
    const std::vector<ColAggSpec>& agg_specs) noexcept {
  if (order_bindings.size() != 1) {
    return PartOrderKind::kNone;
  }
  const OutputSlotBinding& b0 = order_bindings[0].binding;
  if (b0.kind == OutputSlotBinding::Kind::kAgg) {
    switch (agg_specs[b0.index].kind) {
      case ColAggKind::kCountStar:
      case ColAggKind::kCountCol:
      case ColAggKind::kCountByteLen:
      case ColAggKind::kSumInt64:
      case ColAggKind::kSumByteLen:
      case ColAggKind::kMinInt64:
      case ColAggKind::kMaxInt64:
      case ColAggKind::kMinByteLen:
      case ColAggKind::kMaxByteLen:
      case ColAggKind::kMinDate:
      case ColAggKind::kMaxDate:
      case ColAggKind::kCountDistinctInt64:
      case ColAggKind::kCountDistinctDate:
      case ColAggKind::kCountDistinctDouble:
      case ColAggKind::kCountDistinctVarChar:
        return PartOrderKind::kInt;
      case ColAggKind::kAvgDouble:
      case ColAggKind::kAvgByteLen:
      case ColAggKind::kSumDouble:
      case ColAggKind::kMinDouble:
      case ColAggKind::kMaxDouble:
        return PartOrderKind::kDouble;
      default:
        return PartOrderKind::kNone;
    }
  }
  if (b0.kind == OutputSlotBinding::Kind::kKey) {
    switch (key_specs[b0.index].kind) {
      case ColKeyKind::kColInt64:
      case ColKeyKind::kConstInt64:
      case ColKeyKind::kColInt64Delta:
      case ColKeyKind::kColDate:
      case ColKeyKind::kUtcTimestampExtract:
        return PartOrderKind::kInt;
      case ColKeyKind::kColDouble:
        return PartOrderKind::kDouble;
      default:
        return PartOrderKind::kNone;
    }
  }
  return PartOrderKind::kNone;
}

struct SurvivorGroupRow {
  bool num_null{false};
  int64_t num_int{0};
  double num_double{0.0};
  std::vector<Value> order_keys;
  std::vector<Value> emit_vals;
};

struct SurvivorComparator {
  PartOrderKind fast_kind{PartOrderKind::kNone};
  const std::vector<ColumnarOrderBinding>* order_bindings{nullptr};

  bool operator()(const SurvivorGroupRow& a,
                  const SurvivorGroupRow& b) const noexcept {
    if (fast_kind != PartOrderKind::kNone) {
      const SortKeyOrder& ord0 = (*order_bindings)[0].order;
      if (a.num_null || b.num_null) {
        if (a.num_null != b.num_null) {
          return a.num_null ? ord0.NullsFirst() : !ord0.NullsFirst();
        }
      } else if (fast_kind == PartOrderKind::kInt) {
        if (a.num_int != b.num_int) {
          return ord0.Ascending() ? (a.num_int < b.num_int)
                                  : (a.num_int > b.num_int);
        }
      } else {
        const bool na = std::isnan(a.num_double);
        const bool nb = std::isnan(b.num_double);
        if (na != nb) {
          return ord0.Ascending() ? na : !na;
        }
        if (!na && a.num_double != b.num_double) {
          return ord0.Ascending() ? (a.num_double < b.num_double)
                                  : (a.num_double > b.num_double);
        }
      }
      return false;
    }
    for (size_t k = 0; k < order_bindings->size(); ++k) {
      const int c =
          (*order_bindings)[k].order.Compare(a.order_keys[k], b.order_keys[k]);
      if (c != 0) {
        return c < 0;
      }
    }
    return false;
  }
};

// Approximate heap footprint of one retained survivor group. `SurvivorGroupRow`
// owns its `Value` vectors (and any VarChar payload) outside `Relation`, so
// these bytes are invisible to the budget unless they are charged explicitly.
size_t EstimateSurvivorGroupRowBytes(const SurvivorGroupRow& row) noexcept {
  size_t bytes = sizeof(SurvivorGroupRow);
  bytes += row.order_keys.capacity() * sizeof(Value);
  bytes += row.emit_vals.capacity() * sizeof(Value);
  for (const Value& value : row.order_keys) {
    if (value.type == ValueType::kVarChar) {
      bytes += value.value.varchar_value.size();
    }
  }
  for (const Value& value : row.emit_vals) {
    if (value.type == ValueType::kVarChar) {
      bytes += value.value.varchar_value.size();
    }
  }
  return bytes;
}

// Per-shard charge for the retained survivor rows. Exactly one worker builds
// each shard, so the lazily created charge is touched by that worker only; the
// rows outlive the shard loop until the merge, so the charges live in the
// caller's vector too and are released once the merge adopts the rows.
QueryMemoryCharge& SurvivorChargeFor(
    std::vector<std::unique_ptr<QueryMemoryCharge>>& charges, size_t shard) {
  if (charges[shard] == nullptr) {
    charges[shard] = std::make_unique<QueryMemoryCharge>();
  }
  return *charges[shard];
}

void PushChargedSurvivor(std::vector<SurvivorGroupRow>* rows,
                         QueryMemoryCharge* charge, SurvivorGroupRow row) {
  charge->Add(EstimateSurvivorGroupRowBytes(row));
  rows->push_back(std::move(row));
}

Status EvaluateNumericOrderValue(const OutputSlotBinding& b,
                                 const ColAggSpec* agg_spec,
                                 const ColGroupKeySpec* key_spec,
                                 const RuntimeAggState* agg_st,
                                 const RuntimeKeyCell* key_cell,
                                 bool* out_is_null, int64_t* out_int,
                                 double* out_double) noexcept {
  if (b.kind == OutputSlotBinding::Kind::kAgg) {
    const RuntimeAggState& st = *agg_st;
    switch (agg_spec->kind) {
      case ColAggKind::kCountStar:
      case ColAggKind::kCountCol:
      case ColAggKind::kCountByteLen:
        *out_is_null = false;
        *out_int = st.count;
        return Status::kSuccess;
      case ColAggKind::kSumInt64:
      case ColAggKind::kSumByteLen:
      case ColAggKind::kMinInt64:
      case ColAggKind::kMaxInt64:
      case ColAggKind::kMinByteLen:
      case ColAggKind::kMaxByteLen:
      case ColAggKind::kMinDate:
      case ColAggKind::kMaxDate:
        *out_is_null = !st.has_value;
        *out_int = st.int_val;
        return Status::kSuccess;
      case ColAggKind::kAvgDouble:
      case ColAggKind::kAvgByteLen:
        *out_is_null = (st.count == 0);
        *out_double = st.count == 0
                          ? 0.0
                          : (st.double_val / static_cast<double>(st.count));
        return Status::kSuccess;
      case ColAggKind::kSumDouble:
      case ColAggKind::kMinDouble:
      case ColAggKind::kMaxDouble:
        *out_is_null = !st.has_value;
        *out_double = st.double_val;
        return Status::kSuccess;
      default:
        *out_is_null = false;
        *out_int = FinishAggState(*agg_spec, st).value.int_value;
        return Status::kSuccess;
    }
  }
  if (key_cell == nullptr) {
    *out_is_null = true;
    return Status::kSuccess;
  }
  *out_is_null = key_cell->is_null;
  if (key_spec->kind == ColKeyKind::kColDouble) {
    *out_double = key_cell->double_val;
  } else {
    *out_int = key_cell->int_val;
  }
  return Status::kSuccess;
}

Relation FinalizePartitionedRelation(
    const PartitionedColumnarAggRequest& req,
    std::vector<std::vector<SurvivorGroupRow>>& shard_survivors,
    size_t total_groups, uint64_t total_scanned, uint64_t total_matching,
    std::chrono::steady_clock::time_point merge_begin, const char* path_label,
    std::vector<std::unique_ptr<QueryMemoryCharge>>* survivor_charges =
        nullptr) {
  const auto fast_order_kind =
      ClassifyPartOrderKind(req.order_bindings, req.key_specs, req.agg_specs);
  const SurvivorComparator cmp_rows{.fast_kind = fast_order_kind,
                                    .order_bindings = &req.order_bindings};

  size_t total_surv = 0;
  for (const auto& sv : shard_survivors) {
    total_surv += sv.size();
  }
  const size_t emit_cap =
      req.stmt.HasLimit()
          ? (req.stmt.Offset() <=
                     std::numeric_limits<size_t>::max() - req.stmt.Limit()
                 ? req.stmt.Offset() + req.stmt.Limit()
                 : std::numeric_limits<size_t>::max())
          : std::numeric_limits<size_t>::max();
  // Each shard already kept only its local top-`emit_cap` rows. The global
  // top-`emit_cap` is a subset of their union, so merge through a fixed-size
  // heap instead of materializing num_shards * emit_cap rows and sorting all
  // of them: with 512 shards and LIMIT 1000000 that would otherwise sort
  // hundreds of millions of rows in RAM. This uses the exact same comparator
  // the shard-local heaps used, so the surviving set (and tie behaviour) is
  // unchanged.
  const bool bounded_topk = req.stmt.HasLimit() &&
                            !req.order_bindings.empty() && emit_cap > 0 &&
                            emit_cap < total_surv;
  std::vector<SurvivorGroupRow> survivors;
  if (bounded_topk) {
    survivors.reserve(emit_cap);
    for (auto& sv : shard_survivors) {
      for (auto& r : sv) {
        if (survivors.size() < emit_cap) {
          survivors.push_back(std::move(r));
          std::push_heap(survivors.begin(), survivors.end(), cmp_rows);
        } else if (cmp_rows(r, survivors.front())) {
          std::pop_heap(survivors.begin(), survivors.end(), cmp_rows);
          survivors.back() = std::move(r);
          std::push_heap(survivors.begin(), survivors.end(), cmp_rows);
        }
      }
    }
    std::sort_heap(survivors.begin(), survivors.end(), cmp_rows);
  } else {
    survivors.reserve(total_surv);
    for (auto& sv : shard_survivors) {
      for (auto& r : sv) {
        survivors.push_back(std::move(r));
      }
    }
  }
  // The moved rows' `Value` payloads now live in `survivors` and are charged
  // again as `output.AddRow` adopts them; drop the per-shard charges here so
  // the same bytes are not counted twice.
  if (survivor_charges != nullptr) {
    for (auto& charge : *survivor_charges) {
      if (charge != nullptr) {
        charge->ReleaseAll();
      }
    }
  }

  size_t emit_count = survivors.size();
  if (req.stmt.HasLimit()) {
    emit_count = std::min(emit_count, emit_cap);
  }
  if (!bounded_topk && !req.order_bindings.empty() && !survivors.empty()) {
    if (emit_count < survivors.size() && emit_count * 2 < survivors.size()) {
      std::partial_sort(
          survivors.begin(),
          survivors.begin() + static_cast<std::ptrdiff_t>(emit_count),
          survivors.end(), cmp_rows);
    } else {
      std::sort(survivors.begin(), survivors.end(), cmp_rows);
    }
  }

  if (req.publish_lstable_marker && req.runtime != nullptr) {
    auto marker = std::make_shared<Relation>(req.runtime);
    marker->schema = req.qualified_schema;
    req.runtime->base_relations.emplace(req.fold_cache_key, std::move(marker));
  }

  Relation output(req.runtime);
  output.rows.reserve(emit_count);
  std::vector<Column> out_cols;
  out_cols.reserve(req.stmt.SelectList().size());
  for (size_t i = 0; i < req.stmt.SelectList().size(); ++i) {
    ValueType vt = ValueType::kInt64;
    if (req.stmt.SelectList()[i].expression) {
      auto rt =
          req.stmt.SelectList()[i].expression->ResultType(req.qualified_schema);
      vt = rt.GetType() == TypeTag::kDouble    ? ValueType::kDouble
           : rt.GetType() == TypeTag::kDate    ? ValueType::kDate
           : rt.GetType() == TypeTag::kVarChar ? ValueType::kVarChar
                                               : ValueType::kInt64;
    }
    out_cols.emplace_back(ProjectionName(req.stmt.SelectList()[i], i), vt);
  }
  std::vector<uint8_t> refined(out_cols.size(), 0);
  for (size_t r = 0; r < emit_count; ++r) {
    auto& vals = survivors[r].emit_vals;
    for (size_t i = 0; i < vals.size(); ++i) {
      if (refined[i] == 0U && !vals[i].IsNull()) {
        out_cols[i] = Column(out_cols[i].Name(), ValueTypeOf(vals[i]));
        out_cols[i].SetUnsigned(vals[i].IsUnsigned());
        refined[i] = 1;
      }
    }
    output.AddRow(Row(std::move(vals)));
  }
  output.schema = Schema("", std::move(out_cols));

  if (std::getenv("TINYLAMB_DEBUG_TLCS") != nullptr) {
    (void)std::fprintf(stderr,
                       "[TLCS-AGG-OK] partitioned-%s dir=%.2fms scan=%.2fms "
                       "merge=%.2fms cands=%zu/%u rows=%zu/%zu groups=%zu\n",
                       path_label, req.dir_ms, ColumnarMsSince(req.scan_begin),
                       ColumnarMsSince(merge_begin),
                       req.candidate_blocks.size(), req.tlcs->BlockCount(),
                       static_cast<size_t>(total_scanned),
                       static_cast<size_t>(total_matching), total_groups);
  }
  if (req.runtime != nullptr) {
    const double scan_ms = ColumnarMsSince(req.scan_begin);
    req.runtime->scan_ms += scan_ms;
    req.runtime->filter_ms += scan_ms;
    req.runtime->scan_rows += total_scanned;
    req.runtime->scan_output_rows += total_matching;
    req.runtime->typed_parallel_aggregations += 1;
    req.runtime->aggregate_groups += total_groups;
    req.runtime->aggregate_input_rows += total_matching;
    req.runtime->aggregate_ms += ColumnarMsSince(merge_begin);
  }
  return output;
}

// Upper bound for pre-sizing Pass 2 shard tables. `shard_bytes` counts
// *records* — up to one per input row whenever no L1 pre-aggregator ran — but
// the tables hold *groups*, so record-based sizing zero-fills and
// budget-charges a `~2*rows` table per shard (GB-scale memset that also
// defeats the L3-shard intent). All of these tables grow geometrically on
// insert, so cap the speculative reservation; real group counts still get
// every slot.
constexpr size_t kMaxPresizedShardGroups = 4096;

// Budget-charged open-addressed set of `(uint32_t gid, int64_t distinct_val)`.
// Uses `gid_plus_one == 0` as empty sentinel so any uint32_t `gid` and int64_t
// `val` works cleanly without sentinels.
class FlatGidIntPairSet {
 public:
  explicit FlatGidIntPairSet(QueryMemoryCharge* charge) noexcept
      : charge_(charge) {}

  Status Reserve(size_t expected_entries) {
    const size_t want =
        std::max<size_t>(256, std::bit_ceil(expected_entries * 2));
    if (slots_.size() >= want) {
      return Status::kSuccess;
    }
    return Rehash(want);
  }

  void ClearKeepingCapacity() noexcept {
    if (size_ > 0) {
      std::ranges::fill(slots_, Slot{});
      size_ = 0;
    }
  }

  StatusOr<bool> Insert(uint32_t gid, int64_t val) {
    if ((size_ + 1) * 10 >= slots_.size() * 7) {
      const size_t next = slots_.empty() ? size_t{256} : slots_.size() * 2;
      RETURN_IF_FAIL(Rehash(next));
    }
    const size_t mask = slots_.size() - 1;
    const uint64_t h =
        MixHash64((static_cast<uint64_t>(gid) * 0x9e3779b97f4a7c15ULL) ^
                  MixHash64(static_cast<uint64_t>(val)));
    size_t pos = static_cast<size_t>(h) & mask;
    while (true) {
      Slot& s = slots_[pos];
      if (s.gid_plus_one == 0) {
        s.gid_plus_one = gid + 1;
        s.val = val;
        ++size_;
        return true;
      }
      if (s.gid_plus_one == gid + 1 && s.val == val) {
        return false;
      }
      pos = (pos + 1) & mask;
    }
  }

 private:
  struct Slot {
    uint32_t gid_plus_one{0};
    int64_t val{0};
  };

  Status Rehash(size_t new_cap) {
    const size_t old_bytes = slots_.size() * sizeof(Slot);
    const size_t new_bytes = new_cap * sizeof(Slot);
    if (charge_ != nullptr && new_bytes > old_bytes &&
        !charge_->TryAdd(new_bytes - old_bytes)) {
      return StatusError(StatusCode::kMemoryLimitExceeded,
                         "distinct pair set exceeded budget");
    }
    std::vector<Slot> next(new_cap);
    const size_t mask = new_cap - 1;
    for (const Slot& s : slots_) {
      if (s.gid_plus_one == 0) {
        continue;
      }
      uint64_t h = MixHash64(
          (static_cast<uint64_t>(s.gid_plus_one - 1) * 0x9e3779b97f4a7c15ULL) ^
          MixHash64(static_cast<uint64_t>(s.val)));
      size_t pos = static_cast<size_t>(h) & mask;
      while (next[pos].gid_plus_one != 0) {
        pos = (pos + 1) & mask;
      }
      next[pos] = s;
    }
    slots_.swap(next);
    return Status::kSuccess;
  }

  QueryMemoryCharge* charge_{nullptr};
  std::vector<Slot> slots_;
  size_t size_{0};
};

// ============================================================================
// PATH A: Distinct Partitioned Aggregation (Q09, Q10, Q11, Q12, Q14, ...)
// ============================================================================

StatusOr<std::optional<Relation>> ExecutePartitionedDistinct(
    const PartitionedColumnarAggRequest& req, const KeyProjectionMap& km,
    SpillStore* spill_store) {
  if (req.key_specs.empty()) {
    return std::optional<Relation>{};
  }
  int distinct_agg_idx = -1;
  std::vector<size_t> non_distinct_idxs;
  for (size_t a = 0; a < req.agg_specs.size(); ++a) {
    const auto k = req.agg_specs[a].kind;
    if (IsDistinctAggKind(k)) {
      if (distinct_agg_idx >= 0 || k == ColAggKind::kCountDistinctVarChar) {
        return std::optional<Relation>{};
      }
      distinct_agg_idx = static_cast<int>(a);
    } else if (!IsFixedNumericAggKind(k)) {
      return std::optional<Relation>{};
    } else {
      non_distinct_idxs.push_back(a);
    }
  }
  if (distinct_agg_idx < 0) {
    return std::optional<Relation>{};
  }
  const ColAggSpec& dist_spec =
      req.agg_specs[static_cast<size_t>(distinct_agg_idx)];
  const size_t n_phys = km.phys_specs.size();

  // Sample up to 4 blocks to classify group cardinality (low-card skewed
  // Q09/Q10/Q11/Q12 vs high-card Q14).
  const size_t sample_blocks = std::min<size_t>(4, req.candidate_blocks.size());
  QueryMemoryCharge sample_charge;
  WorkerGroupTable sample_gt(&km.phys_specs, &req.agg_specs, &sample_charge);
  {
    const ColumnarStageSchedule ksched = BuildColumnarStageSchedule(
        req.table_schema, req.preds, km.phys_specs, {});
    BlockDecodeScratch dec_scratch;
    std::vector<const TlcsFile::DecodedColumnChunk*> col_ptrs(
        req.table_schema.ColumnCount(), nullptr);
    std::vector<uint32_t> cand_rows;
    std::vector<RuntimeKeyCell> probe_keys(n_phys);
    std::string regex_scratch;
    TimestampPrefixCache ts_cache;
    uint64_t dummy_scanned = 0;

    for (size_t bi = 0; bi < sample_blocks; ++bi) {
      ASSIGN_OR_RETURN(bool, any_rows,
                       DecodeAndFilterBlock(*req.tlcs, req.candidate_blocks[bi],
                                            req.table_schema, req.preds, ksched,
                                            &dec_scratch, &col_ptrs, &cand_rows,
                                            &dummy_scanned));
      if (!any_rows) {
        continue;
      }
      for (const uint32_t r : cand_rows) {
        Status kst = Status::kSuccess;
        for (size_t k = 0; k < n_phys; ++k) {
          if (!ExtractSingleKeyCell(km.phys_specs[k], col_ptrs, r,
                                    &probe_keys[k], &regex_scratch, &ts_cache,
                                    &kst)) {
            if (kst == StatusCode::kInvalidArgument) {
              return std::optional<Relation>{};
            }
            return kst;
          }
        }
        const uint64_t kh =
            GroupKeySliceHasher::HashTyped(probe_keys.data(), km.phys_specs);
        auto gid_or = sample_gt.FindOrInsert(probe_keys.data(), kh);
        if (!gid_or.HasValue()) {
          if (gid_or.GetStatus() == StatusCode::kMemoryLimitExceeded) {
            return std::optional<Relation>{};
          }
          return gid_or.GetStatus();
        }
        if (sample_gt.NumGroups() > 8192) {
          break;
        }
      }
      if (sample_gt.NumGroups() > 8192) {
        break;
      }
    }
  }
  const bool use_pair_sharded = (sample_gt.NumGroups() <= 8192);

  // Stage schedule for full Pass 1 decode.
  const ColumnarStageSchedule sched = BuildColumnarStageSchedule(
      req.table_schema, req.preds, req.key_specs, req.agg_specs);
  const size_t ncols = req.table_schema.ColumnCount();
  const size_t workers = std::max<size_t>(
      1, std::min<size_t>(
             {QueryParallelWorkerLimit(std::thread::hardware_concurrency()),
              req.candidate_blocks.size(), size_t{16}}));

  if (use_pair_sharded) {
    // Pass 1 collects non-distinct aggs into compact per-worker
    // `WorkerGroupTable` (no distinct sets) AND buffers unique pairs via an
    // 8192-slot exact L1
    // `(local_gid, dist_val)` dedup filter into S=64 `(group_hash ^ dist_hash)`
    // shards.
    const size_t num_shards = req.candidate_blocks.size() <= 16 ? 16 : 64;
    const uint64_t shard_mask = num_shards - 1;

    struct L1PairSlot {
      uint32_t gid_plus_one{0};
      int64_t val{0};
    };
    struct __attribute__((packed)) LocalPairRecord {
      uint32_t local_gid;
      int64_t dist_val;
    };

    // Pass only non-distinct aggs to worker_tables so no distinct structures
    // are allocated on workers.
    std::vector<ColAggSpec> non_distinct_specs;
    non_distinct_specs.reserve(non_distinct_idxs.size());
    for (const size_t a : non_distinct_idxs) {
      non_distinct_specs.push_back(req.agg_specs[a]);
    }

    std::vector<QueryMemoryCharge> worker_table_charges(workers);
    std::vector<std::unique_ptr<WorkerGroupTable>> worker_tables;
    worker_tables.reserve(workers);
    for (size_t w = 0; w < workers; ++w) {
      worker_tables.push_back(std::make_unique<WorkerGroupTable>(
          &km.phys_specs, &non_distinct_specs, &worker_table_charges[w]));
    }

    std::vector<std::vector<std::vector<SlabChunk>>> shard_worker_chunks(
        num_shards);
    std::vector<std::vector<std::unique_ptr<QueryMemoryCharge>>>
        shard_worker_charges(num_shards);
    for (size_t s = 0; s < num_shards; ++s) {
      shard_worker_chunks[s].resize(workers);
      shard_worker_charges[s].resize(workers);
    }
    std::vector<std::vector<SpillSegmentId>> shard_worker_segments(
        num_shards,
        std::vector<SpillSegmentId>(workers, kInvalidSpillSegmentId));

    std::atomic<size_t> next_blk{0};
    std::atomic<uint64_t> total_scanned{0};
    std::atomic<uint64_t> total_matching{0};
    std::atomic<bool> cardinality_overflow{false};
    std::vector<Status> worker_status(workers, Status::kSuccess);

    const bool single_int_group_key =
        (n_phys == 1 && (km.phys_specs[0].kind == ColKeyKind::kColInt64 ||
                         km.phys_specs[0].kind == ColKeyKind::kColDate));
    const bool single_dict_varchar_key =
        (n_phys == 1 && km.phys_specs[0].kind == ColKeyKind::kColVarChar);

    const size_t init_chunk_bytes =
        req.candidate_blocks.size() <= 32 ? size_t{1024} : size_t{4096};

    // Pass 2 re-opens one shard group table at a time and needs it to fit in
    // the remaining query budget, so each writer keeps at most
    // `remaining/2 / (S*W*64KiB)` resident slab chunks and spills older ones
    // into its own SpillStore segment instead of saturating the pool.
    const uint32_t writer_chunk_cap = static_cast<uint32_t>(std::max<size_t>(
        1, (QueryMemoryBudget::Global().Remaining() / 2) /
               (SpillStore::kBlockSize * num_shards * workers)));

    RunParallelTasks(workers, [&](size_t tid) {
      std::vector<WorkerShardWriter> writers(num_shards);
      for (size_t s = 0; s < num_shards; ++s) {
        shard_worker_charges[s][tid] = std::make_unique<QueryMemoryCharge>();
        writers[s].Init(&shard_worker_chunks[s][tid],
                        shard_worker_charges[s][tid].get(), init_chunk_bytes,
                        spill_store, &shard_worker_segments[s][tid],
                        writer_chunk_cap);
      }
      WorkerGroupTable& wt = *worker_tables[tid];
      QueryMemoryCharge l1_charge;
      if (!l1_charge.TryAdd(8192 * sizeof(L1PairSlot))) {
        worker_status[tid] = StatusError(StatusCode::kMemoryLimitExceeded,
                                         "L1 pair filter exceeded budget");
        return;
      }
      std::vector<L1PairSlot> l1(8192);

      BlockDecodeScratch dec_scratch;
      std::vector<const TlcsFile::DecodedColumnChunk*> col_ptrs(ncols, nullptr);
      std::vector<uint32_t> cand_rows;
      std::vector<RuntimeKeyCell> probe_keys(n_phys);
      std::string regex_scratch;
      TimestampPrefixCache ts_cache;
      uint64_t local_scanned = 0;
      uint64_t local_matching = 0;

      // Scratch for single-block dict fast lookup (`Q11`).
      std::array<uint32_t, 257> dict_gid_cache{};

      // Small 1-slot group key cache for repeated adjacent
      // `RegionID`/`MobilePhone`.
      bool has_last_int = false;
      int64_t last_int_val = 0;
      uint32_t last_int_gid = 0;

      while (!cardinality_overflow.load(std::memory_order_relaxed)) {
        const size_t bi = next_blk.fetch_add(1, std::memory_order_relaxed);
        if (bi >= req.candidate_blocks.size()) {
          break;
        }
        const uint32_t bidx = req.candidate_blocks[bi];

        auto dec_or = DecodeAndFilterBlock(
            *req.tlcs, bidx, req.table_schema, req.preds, sched, &dec_scratch,
            &col_ptrs, &cand_rows, &local_scanned);
        if (!dec_or.HasValue()) {
          worker_status[tid] = dec_or.GetStatus();
          return;
        }
        if (!dec_or.Value()) {
          continue;
        }
        local_matching += cand_rows.size();

        const auto* dch = col_ptrs[dist_spec.full_col];
        const bool dist_no_null = !dch->has_null;

        const auto* first_kch = col_ptrs[km.phys_specs[0].full_col];
        const bool use_dict_gid_block =
            single_dict_varchar_key && first_kch->dict_encoded &&
            first_kch->dict != nullptr &&
            first_kch->dict->StringEntryCount() <= 256 &&
            first_kch->dict_codes.size() >= cand_rows.back() + 1;
        uint32_t dict_null_gid = UINT32_MAX;
        if (use_dict_gid_block) {
          const auto* kch = first_kch;
          const size_t nd = kch->dict->StringEntryCount();
          // The dictionary is FILE-wide, not block-wide: eagerly inserting
          // every entry creates "ghost" groups for values that survive in no
          // candidate row of this block (or of any candidate block). Those
          // groups later reach the output with COUNT(DISTINCT ...) = 0 but a
          // non-NULL key. Register only codes actually referenced by this
          // block's surviving rows, and only the NULL key if it is used.
          std::array<bool, 257> dict_code_used{};
          for (const uint32_t r : cand_rows) {
            if (kch->has_null && kch->IsNull(r)) {
              dict_code_used[nd] = true;  // NULL sentinel slot
            } else if (kch->dict_codes[r] < nd) {
              dict_code_used[kch->dict_codes[r]] = true;
            }
          }
          for (size_t d = 0; d < nd; ++d) {
            if (!dict_code_used[d]) {
              continue;
            }
            const std::string_view dv = kch->dict->StringEntryAt(d);
            probe_keys[0] =
                RuntimeKeyCell{.is_null = false,
                               .str_ptr = dv.data(),
                               .str_len = static_cast<uint32_t>(dv.size())};
            const uint64_t kh = GroupKeySliceHasher::HashTyped(
                probe_keys.data(), km.phys_specs);
            auto gid_or = wt.FindOrInsert(probe_keys.data(), kh);
            if (!gid_or.HasValue()) {
              worker_status[tid] = gid_or.GetStatus();
              return;
            }
            dict_gid_cache[d] = gid_or.Value();
          }
          if (dict_code_used[nd]) {
            probe_keys[0] = RuntimeKeyCell{.is_null = true};
            const uint64_t kh = GroupKeySliceHasher::HashTyped(
                probe_keys.data(), km.phys_specs);
            auto gid_or = wt.FindOrInsert(probe_keys.data(), kh);
            if (!gid_or.HasValue()) {
              worker_status[tid] = gid_or.GetStatus();
              return;
            }
            dict_null_gid = gid_or.Value();
          }
        }

        const auto* int_kch = single_int_group_key
                                  ? col_ptrs[km.phys_specs[0].full_col]
                                  : nullptr;

        for (const uint32_t r : cand_rows) {
          uint32_t local_gid = 0;
          if (use_dict_gid_block) {
            const auto* kch = col_ptrs[km.phys_specs[0].full_col];
            local_gid = (!kch->has_null || !kch->IsNull(r))
                            ? dict_gid_cache[kch->dict_codes[r]]
                            : dict_null_gid;
          } else if (single_int_group_key) {
            if (!int_kch->has_null || !int_kch->IsNull(r)) {
              const int64_t kv = int_kch->int_cells[r];
              if (has_last_int && kv == last_int_val) {
                local_gid = last_int_gid;
              } else {
                probe_keys[0] = RuntimeKeyCell{.int_val = kv};
                const uint64_t kh =
                    MixHash64(0x517cc1b727220a95ULL ^
                              MixHash64(static_cast<uint64_t>(kv)));
                auto gid_or = wt.FindOrInsert(probe_keys.data(), kh);
                if (!gid_or.HasValue()) {
                  worker_status[tid] = gid_or.GetStatus();
                  return;
                }
                local_gid = gid_or.Value();
                has_last_int = true;
                last_int_val = kv;
                last_int_gid = local_gid;
              }
            } else {
              probe_keys[0] = RuntimeKeyCell{.is_null = true};
              const uint64_t kh = GroupKeySliceHasher::HashTyped(
                  probe_keys.data(), km.phys_specs);
              auto gid_or = wt.FindOrInsert(probe_keys.data(), kh);
              if (!gid_or.HasValue()) {
                worker_status[tid] = gid_or.GetStatus();
                return;
              }
              local_gid = gid_or.Value();
            }
          } else {
            Status kst = Status::kSuccess;
            for (size_t k = 0; k < n_phys; ++k) {
              if (!ExtractSingleKeyCell(km.phys_specs[k], col_ptrs, r,
                                        &probe_keys[k], &regex_scratch,
                                        &ts_cache, &kst)) {
                worker_status[tid] = std::move(kst);
                return;
              }
            }
            const uint64_t kh = GroupKeySliceHasher::HashTyped(
                probe_keys.data(), km.phys_specs);
            auto gid_or = wt.FindOrInsert(probe_keys.data(), kh);
            if (!gid_or.HasValue()) {
              worker_status[tid] = gid_or.GetStatus();
              return;
            }
            local_gid = gid_or.Value();
          }

          if (wt.NumGroups() > 65536) {
            cardinality_overflow.store(true, std::memory_order_relaxed);
            return;
          }

          if (!non_distinct_specs.empty()) {
            RuntimeAggState* ast = wt.AggsAt(local_gid);
            for (size_t na = 0; na < non_distinct_specs.size(); ++na) {
              Status fst = FoldRowIntoAggState(non_distinct_specs[na], col_ptrs,
                                               r, ast[na], wt.Arena());
              if (fst != Status::kSuccess) {
                worker_status[tid] = std::move(fst);
                return;
              }
            }
          }

          if (!dist_no_null && dch->IsNull(r)) {
            continue;
          }
          const int64_t dv =
              CanonicalDistinctIntKey(dist_spec.kind, dch->int_cells[r]);
          const uint64_t dhash = MixHash64(static_cast<uint64_t>(dv));
          const uint64_t l1h = MixHash64(
              (static_cast<uint64_t>(local_gid) * 0x9e3779b97f4a7c15ULL) ^
              dhash);
          L1PairSlot& slot = l1[l1h & 8191];
          if (slot.gid_plus_one == local_gid + 1 && slot.val == dv) {
            continue;
          }
          slot.gid_plus_one = local_gid + 1;
          slot.val = dv;

          const size_t s =
              static_cast<size_t>(wt.HashAt(local_gid) ^ dhash) & shard_mask;
          LocalPairRecord rec{.local_gid = local_gid, .dist_val = dv};
          Status ast = writers[s].AppendBytes(&rec, sizeof(rec));
          if (ast != Status::kSuccess) {
            worker_status[tid] = std::move(ast);
            return;
          }
        }
      }
      for (size_t s = 0; s < num_shards; ++s) {
        Status finst = writers[s].Finalize();
        if (finst != Status::kSuccess) {
          worker_status[tid] = std::move(finst);
          return;
        }
      }
      total_scanned.fetch_add(local_scanned, std::memory_order_relaxed);
      total_matching.fetch_add(local_matching, std::memory_order_relaxed);
    });

    for (const Status& st : worker_status) {
      if (st == StatusCode::kInvalidArgument ||
          st == StatusCode::kMemoryLimitExceeded) {
        return std::optional<Relation>{};
      }
      RETURN_IF_FAIL(st);
    }

    if (!cardinality_overflow.load(std::memory_order_relaxed)) {
      const auto merge_begin = std::chrono::steady_clock::now();

      // Merge tiny worker tables into `global_gt` and construct
      // `remap[w][lgid]`.
      QueryMemoryCharge global_charge;
      WorkerGroupTable global_gt(&km.phys_specs, &non_distinct_specs,
                                 &global_charge);
      std::vector<std::vector<uint32_t>> remap(workers);
      for (size_t w = 0; w < workers; ++w) {
        WorkerGroupTable& src = *worker_tables[w];
        remap[w].resize(src.NumGroups());
        for (uint32_t lg = 0; lg < src.NumGroups(); ++lg) {
          auto gid_or = global_gt.FindOrInsert(src.KeysAt(lg), src.HashAt(lg),
                                               /*borrow_strings=*/false);
          if (!gid_or.HasValue()) {
            if (gid_or.GetStatus() == StatusCode::kMemoryLimitExceeded) {
              return std::optional<Relation>{};
            }
            return gid_or.GetStatus();
          }
          const uint32_t gg = gid_or.Value();
          remap[w][lg] = gg;
          if (!non_distinct_specs.empty()) {
            RuntimeAggState* dst_ast = global_gt.AggsAt(gg);
            RuntimeAggState* src_ast = src.AggsAt(lg);
            for (size_t na = 0; na < non_distinct_specs.size(); ++na) {
              Status mst = MergeAggState(non_distinct_specs[na], dst_ast[na],
                                         src_ast[na], global_gt.Arena());
              if (mst == StatusCode::kMemoryLimitExceeded) {
                return std::optional<Relation>{};
              }
              RETURN_IF_FAIL(mst);
            }
          }
        }
        worker_tables[w].reset();
        worker_table_charges[w] = QueryMemoryCharge{};
      }

      const size_t total_groups = global_gt.NumGroups();
      if (total_groups == 0) {
        std::vector<std::vector<SurvivorGroupRow>> empty_surv;
        return FinalizePartitionedRelation(
            req, empty_surv, 0, total_scanned.load(), total_matching.load(),
            merge_begin, "pair-distinct");
      }

      // Pass 2: deduplicate `(global_gid, dist_val)` inside each shard `s` and
      // count distinct occurrences per thread. Keep only the groups each worker
      // actually touches (a sparse map) rather than a dense `workers x
      // total_groups` matrix: the dense form is O(workers x groups) even when a
      // worker sees a handful of groups, and it was uncharged. Charge the
      // sparse nodes so the count state stays inside the query budget like the
      // pair set does, falling back to the general path when the budget is
      // tight.
      constexpr size_t kCountNodeBytes = 64;
      std::vector<std::unordered_map<uint32_t, uint64_t>> thread_counts(
          workers);
      std::vector<QueryMemoryCharge> count_charges(workers);
      std::vector<Status> p2_status(workers, Status::kSuccess);
      std::atomic<size_t> next_s{0};

      RunParallelTasks(std::min(workers, num_shards), [&](size_t tid) {
        std::unordered_map<uint32_t, uint64_t>& my_counts = thread_counts[tid];
        QueryMemoryCharge& my_charge = count_charges[tid];
        QueryMemoryCharge pair_charge;
        FlatGidIntPairSet pset(&pair_charge);

        while (true) {
          const size_t s = next_s.fetch_add(1, std::memory_order_relaxed);
          if (s >= num_shards) {
            break;
          }

          size_t shard_bytes = 0;
          for (size_t w = 0; w < workers; ++w) {
            for (const SlabChunk& c : shard_worker_chunks[s][w]) {
              shard_bytes += c.used;
            }
          }
          if (shard_bytes > 0) {
            const size_t est_pairs = std::min<size_t>(
                std::max<size_t>(128, shard_bytes / sizeof(LocalPairRecord)),
                kMaxPresizedShardGroups);
            Status rst = pset.Reserve(est_pairs);
            if (rst != Status::kSuccess) {
              p2_status[tid] = std::move(rst);
              return;
            }
            pset.ClearKeepingCapacity();

            for (size_t w = 0; w < workers; ++w) {
              const std::vector<uint32_t>& wremap = remap[w];
              SlabChunkReader chunk_reader(spill_store,
                                           shard_worker_segments[s][w]);
              for (const SlabChunk& c : shard_worker_chunks[s][w]) {
                const uint8_t* ptr = nullptr;
                Status cst = chunk_reader.Resolve(c, &ptr);
                if (cst != Status::kSuccess) {
                  p2_status[tid] = std::move(cst);
                  return;
                }
                const uint8_t* end = ptr + c.used;
                while (ptr + sizeof(LocalPairRecord) <= end) {
                  LocalPairRecord rec{};
                  std::memcpy(&rec, ptr, sizeof(rec));
                  ptr += sizeof(rec);
                  const uint32_t gg = wremap[rec.local_gid];
                  auto ins_or = pset.Insert(gg, rec.dist_val);
                  if (!ins_or.HasValue()) {
                    p2_status[tid] = ins_or.GetStatus();
                    return;
                  }
                  if (ins_or.Value()) {
                    auto [it, inserted] = my_counts.try_emplace(gg, 0);
                    if (inserted && !my_charge.TryAdd(kCountNodeBytes)) {
                      p2_status[tid] = StatusError(
                          StatusCode::kMemoryLimitExceeded,
                          "pair-distinct count map exceeded budget");
                      return;
                    }
                    ++it->second;
                  }
                }
              }
            }
          }
          // Immediate RAII refund of shard `s` Pass 1 slabs + charges.
          shard_worker_chunks[s].clear();
          shard_worker_charges[s].clear();
          for (const SpillSegmentId seg : shard_worker_segments[s]) {
            spill_store->DropSegment(seg);
          }
        }
      });

      for (const Status& st : p2_status) {
        if (st == StatusCode::kMemoryLimitExceeded) {
          return std::optional<Relation>{};
        }
        RETURN_IF_FAIL(st);
      }

      std::vector<uint64_t> global_distinct(total_groups, 0);
      for (size_t tid = 0; tid < workers; ++tid) {
        for (const auto& [g, cnt] : thread_counts[tid]) {
          global_distinct[g] += cnt;
        }
      }

      // Evaluate HAVING / ORDER BY / emit bindings across `global_gt`.
      const PartOrderKind fast_order_kind = ClassifyPartOrderKind(
          req.order_bindings, req.key_specs, req.agg_specs);
      std::vector<std::vector<SurvivorGroupRow>> single_surv(1);
      std::vector<std::unique_ptr<QueryMemoryCharge>> survivor_charges(1);
      QueryMemoryCharge& survivor_charge =
          SurvivorChargeFor(survivor_charges, 0);
      auto& out_rows = single_surv[0];
      std::vector<RuntimeAggState> full_agg_states(req.agg_specs.size());
      std::vector<Value> finished_aggs(req.agg_specs.size());
      std::vector<Value> having_scratch(req.agg_specs.size());
      std::vector<RuntimeKeyCell> full_keys(req.key_specs.size());
      std::vector<Value> cached_keys(req.key_specs.size());

      for (size_t g = 0; g < total_groups; ++g) {
        const RuntimeAggState* nd_ast =
            non_distinct_specs.empty() ? nullptr : global_gt.AggsAt(g);
        for (size_t na = 0; na < non_distinct_idxs.size(); ++na) {
          CopyNonDistinctAggState(nd_ast[na],
                                  full_agg_states[non_distinct_idxs[na]]);
        }
        RuntimeAggState& dstate =
            full_agg_states[static_cast<size_t>(distinct_agg_idx)];
        dstate.count = static_cast<int64_t>(global_distinct[g]);
        dstate.has_value = true;

        ASSIGN_OR_RETURN(bool, passes,
                         EvalPositionalHavingOnStates(
                             req.positional_having, req.agg_specs,
                             full_agg_states.data(), &having_scratch));
        if (!passes) {
          continue;
        }

        for (size_t a = 0; a < req.agg_specs.size(); ++a) {
          if (std::cmp_equal(a, distinct_agg_idx)) {
            finished_aggs[a] = Value(static_cast<int64_t>(global_distinct[g]));
          } else {
            finished_aggs[a] =
                FinishAggState(req.agg_specs[a], full_agg_states[a]);
          }
        }

        km.ReconstructFull(req.key_specs, global_gt.KeysAt(g),
                           full_keys.data());
        std::fill(cached_keys.begin(), cached_keys.end(), Value());

        SurvivorGroupRow row;
        if (fast_order_kind != PartOrderKind::kNone) {
          const auto& ob0 = req.order_bindings[0].binding;
          if (ob0.kind == OutputSlotBinding::Kind::kAgg &&
              std::cmp_equal(ob0.index, distinct_agg_idx)) {
            row.num_null = false;
            row.num_int = static_cast<int64_t>(global_distinct[g]);
          } else {
            RETURN_IF_FAIL(EvaluateNumericOrderValue(
                ob0,
                ob0.kind == OutputSlotBinding::Kind::kAgg
                    ? &req.agg_specs[ob0.index]
                    : nullptr,
                ob0.kind == OutputSlotBinding::Kind::kKey
                    ? &req.key_specs[ob0.index]
                    : nullptr,
                ob0.kind == OutputSlotBinding::Kind::kAgg
                    ? &full_agg_states[ob0.index]
                    : nullptr,
                ob0.kind == OutputSlotBinding::Kind::kKey
                    ? &full_keys[ob0.index]
                    : nullptr,
                &row.num_null, &row.num_int, &row.num_double));
          }
        } else if (!req.order_bindings.empty()) {
          row.order_keys.resize(req.order_bindings.size());
          for (size_t k = 0; k < req.order_bindings.size(); ++k) {
            ASSIGN_OR_RETURN(
                Value, ov,
                EvaluateBindingValue(req.order_bindings[k].binding,
                                     req.key_specs, full_keys.data(),
                                     finished_aggs, &cached_keys));
            row.order_keys[k] = std::move(ov);
          }
        }
        row.emit_vals.reserve(req.emit_bindings.size());
        for (const auto& eb : req.emit_bindings) {
          ASSIGN_OR_RETURN(
              Value, ev,
              EvaluateBindingValue(eb, req.key_specs, full_keys.data(),
                                   finished_aggs, &cached_keys));
          row.emit_vals.push_back(std::move(ev));
        }
        PushChargedSurvivor(&out_rows, &survivor_charge, std::move(row));
      }

      return FinalizePartitionedRelation(req, single_surv, total_groups,
                                         total_scanned.load(),
                                         total_matching.load(), merge_begin,
                                         "pair-distinct", &survivor_charges);
    }
    // Fall through to High-Card Group-Sharded if cardinality unexpectedly
    // jumped past 65K local groups in a worker.
  }

  // --------------------------------------------------------------------------
  // High-Card Group-Sharded Distinct (`Q14`, or fallback when > 65K groups)
  // --------------------------------------------------------------------------
  if (!non_distinct_idxs.empty()) {
    // Keep Group-Sharded clean & generic for distinct+non-distinct or distinct
    // alone by falling back if mixed VarChar non-distinct exists.
    for (const size_t a : non_distinct_idxs) {
      if (req.agg_specs[a].kind == ColAggKind::kMinVarChar ||
          req.agg_specs[a].kind == ColAggKind::kMaxVarChar) {
        return std::optional<Relation>{};
      }
    }
  }

  const size_t num_shards =
      req.candidate_blocks.size() <= 16
          ? 16
          : (req.candidate_blocks.size() <= 256 ? 64 : 256);
  const uint64_t shard_mask = num_shards - 1;

  std::vector<std::vector<std::vector<SlabChunk>>> shard_worker_chunks(
      num_shards);
  std::vector<std::vector<std::unique_ptr<QueryMemoryCharge>>>
      shard_worker_charges(num_shards);
  for (size_t s = 0; s < num_shards; ++s) {
    shard_worker_chunks[s].resize(workers);
    shard_worker_charges[s].resize(workers);
  }

  std::vector<std::vector<SpillSegmentId>> shard_worker_segments(
      num_shards, std::vector<SpillSegmentId>(workers, kInvalidSpillSegmentId));

  std::atomic<size_t> next_blk{0};
  std::atomic<uint64_t> total_scanned{0};
  std::atomic<uint64_t> total_matching{0};
  std::vector<Status> p1_status(workers, Status::kSuccess);
  const size_t init_chunk_bytes =
      req.candidate_blocks.size() <= 32 ? size_t{1024} : size_t{4096};
  const size_t n_extra = non_distinct_idxs.size();

  const uint32_t writer_chunk_cap = static_cast<uint32_t>(
      std::max<size_t>(1, (QueryMemoryBudget::Global().Remaining() / 2) /
                              (SpillStore::kBlockSize * num_shards * workers)));

  RunParallelTasks(workers, [&](size_t tid) {
    std::vector<WorkerShardWriter> writers(num_shards);
    for (size_t s = 0; s < num_shards; ++s) {
      shard_worker_charges[s][tid] = std::make_unique<QueryMemoryCharge>();
      writers[s].Init(&shard_worker_chunks[s][tid],
                      shard_worker_charges[s][tid].get(), init_chunk_bytes,
                      spill_store, &shard_worker_segments[s][tid],
                      writer_chunk_cap);
    }

    BlockDecodeScratch dec_scratch;
    std::vector<const TlcsFile::DecodedColumnChunk*> col_ptrs(ncols, nullptr);
    std::vector<uint32_t> cand_rows;
    std::vector<RuntimeKeyCell> probe_keys(n_phys);
    std::string regex_scratch;
    TimestampPrefixCache ts_cache;
    uint64_t local_scanned = 0;
    uint64_t local_matching = 0;

    while (true) {
      const size_t bi = next_blk.fetch_add(1, std::memory_order_relaxed);
      if (bi >= req.candidate_blocks.size()) {
        break;
      }
      const uint32_t bidx = req.candidate_blocks[bi];

      auto dec_or = DecodeAndFilterBlock(*req.tlcs, bidx, req.table_schema,
                                         req.preds, sched, &dec_scratch,
                                         &col_ptrs, &cand_rows, &local_scanned);
      if (!dec_or.HasValue()) {
        p1_status[tid] = dec_or.GetStatus();
        return;
      }
      if (!dec_or.Value()) {
        continue;
      }
      local_matching += cand_rows.size();

      const auto* dch = col_ptrs[dist_spec.full_col];
      const bool dist_no_null = !dch->has_null;

      for (const uint32_t r : cand_rows) {
        Status kst = Status::kSuccess;
        uint8_t key_null_mask = 0;
        size_t payload_bytes =
            8 + 1 + 1 + 8 + (n_extra * 9);  // hash+masks+dv+ex
        for (size_t k = 0; k < n_phys; ++k) {
          if (!ExtractSingleKeyCell(km.phys_specs[k], col_ptrs, r,
                                    &probe_keys[k], &regex_scratch, &ts_cache,
                                    &kst)) {
            p1_status[tid] = std::move(kst);
            return;
          }
          if (probe_keys[k].is_null) {
            key_null_mask |= static_cast<uint8_t>(1U << k);
          } else if (IsVarCharKeyKind(km.phys_specs[k].kind)) {
            payload_bytes += 4 + probe_keys[k].str_len;
          } else {
            payload_bytes += 8;
          }
        }
        const uint64_t kh =
            GroupKeySliceHasher::HashTyped(probe_keys.data(), km.phys_specs);
        const size_t s = static_cast<size_t>(kh) & shard_mask;

        auto dst_or = writers[s].Reserve(payload_bytes);
        if (!dst_or.HasValue()) {
          p1_status[tid] = dst_or.GetStatus();
          return;
        }
        uint8_t* p = dst_or.Value();
        uint8_t* start = p;
        std::memcpy(p, &kh, 8);
        p += 8;
        *p++ = key_null_mask;
        const bool has_dist = dist_no_null || !dch->IsNull(r);
        *p++ = has_dist ? 1 : 0;
        if (has_dist) {
          const int64_t dv =
              CanonicalDistinctIntKey(dist_spec.kind, dch->int_cells[r]);
          std::memcpy(p, &dv, 8);
          p += 8;
        }
        for (size_t k = 0; k < n_phys; ++k) {
          if (probe_keys[k].is_null) {
            continue;
          }
          if (IsVarCharKeyKind(km.phys_specs[k].kind)) {
            const uint32_t sl = probe_keys[k].str_len;
            std::memcpy(p, &sl, 4);
            p += 4;
            if (sl > 0) {
              std::memcpy(p, probe_keys[k].str_ptr, sl);
              p += sl;
            }
          } else if (km.phys_specs[k].kind == ColKeyKind::kColDouble) {
            std::memcpy(p, &probe_keys[k].double_val, 8);
            p += 8;
          } else {
            std::memcpy(p, &probe_keys[k].int_val, 8);
            p += 8;
          }
        }
        for (size_t na = 0; na < n_extra; ++na) {
          const ColAggSpec& ex = req.agg_specs[non_distinct_idxs[na]];
          if (ex.kind == ColAggKind::kCountStar) {
            continue;
          }
          const auto* ech = col_ptrs[ex.full_col];
          if (ech->IsNull(r)) {
            *p++ = 1;
          } else {
            *p++ = 0;
            if (ex.kind == ColAggKind::kCountCol ||
                ex.kind == ColAggKind::kCountByteLen) {
              // Existence bit is sufficient for COUNT(col).
            } else if (ex.kind == ColAggKind::kSumByteLen ||
                       ex.kind == ColAggKind::kAvgByteLen ||
                       ex.kind == ColAggKind::kMinByteLen ||
                       ex.kind == ColAggKind::kMaxByteLen) {
              const auto bl = static_cast<int64_t>(ech->VarcharLength(r));
              std::memcpy(p, &bl, 8);
              p += 8;
            } else {
              std::memcpy(p, &ech->int_cells[r], 8);
              p += 8;
            }
          }
        }
        writers[s].Commit(static_cast<size_t>(p - start));
      }
    }
    for (size_t s = 0; s < num_shards; ++s) {
      Status finst = writers[s].Finalize();
      if (finst != Status::kSuccess) {
        p1_status[tid] = std::move(finst);
        return;
      }
    }
    total_scanned.fetch_add(local_scanned, std::memory_order_relaxed);
    total_matching.fetch_add(local_matching, std::memory_order_relaxed);
  });

  for (const Status& st : p1_status) {
    if (st == StatusCode::kInvalidArgument ||
        st == StatusCode::kMemoryLimitExceeded) {
      return std::optional<Relation>{};
    }
    RETURN_IF_FAIL(st);
  }

  const auto merge_begin = std::chrono::steady_clock::now();
  const PartOrderKind fast_order_kind =
      ClassifyPartOrderKind(req.order_bindings, req.key_specs, req.agg_specs);
  const bool bounded_topk = req.stmt.HasLimit() && !req.order_bindings.empty();
  const size_t topk_cap =
      req.stmt.HasLimit()
          ? (req.stmt.Offset() <=
                     std::numeric_limits<size_t>::max() - req.stmt.Limit()
                 ? req.stmt.Offset() + req.stmt.Limit()
                 : std::numeric_limits<size_t>::max())
          : 0;

  std::vector<ColAggSpec> non_distinct_specs;
  non_distinct_specs.reserve(n_extra);
  for (const size_t na : non_distinct_idxs) {
    non_distinct_specs.push_back(req.agg_specs[na]);
  }

  std::vector<std::vector<SurvivorGroupRow>> shard_survivors(num_shards);
  // Survivor rows are plain vectors outside `Relation`; keep their charge
  // alive until the merge adopts the rows (see FinalizePartitionedRelation).
  std::vector<std::unique_ptr<QueryMemoryCharge>> shard_survivor_charges(
      num_shards);
  std::vector<Status> p2_status(workers, Status::kSuccess);
  std::atomic<size_t> total_groups_out{0};
  std::atomic<size_t> next_s{0};

  RunParallelTasks(std::min(workers, num_shards), [&](size_t tid) {
    std::vector<RuntimeKeyCell> probe_keys(n_phys);
    std::vector<RuntimeKeyCell> full_keys(req.key_specs.size());
    std::vector<RuntimeAggState> full_agg_states(req.agg_specs.size());
    std::vector<Value> finished_aggs(req.agg_specs.size());
    std::vector<Value> cached_keys(req.key_specs.size());
    std::vector<Value> having_scratch(req.agg_specs.size());

    QueryMemoryCharge reuse_pair_charge;
    FlatGidIntPairSet pset(&reuse_pair_charge);
    struct TempPair {
      uint32_t gid;
      int64_t val;
    };
    std::vector<TempPair> pairs;
    std::vector<uint64_t> distinct_counts;

    while (true) {
      const size_t s = next_s.fetch_add(1, std::memory_order_relaxed);
      if (s >= num_shards) {
        break;
      }

      size_t shard_bytes = 0;
      for (size_t w = 0; w < workers; ++w) {
        for (const SlabChunk& c : shard_worker_chunks[s][w]) {
          shard_bytes += c.used;
        }
      }
      if (shard_bytes > 0) {
        QueryMemoryCharge shard_ht_charge;
        WorkerGroupTable wt(&km.phys_specs, &non_distinct_specs,
                            &shard_ht_charge);
        Status rst = wt.Reserve(std::min<size_t>(
            std::max<size_t>(128, shard_bytes / 64), kMaxPresizedShardGroups));
        if (rst != Status::kSuccess) {
          p2_status[tid] = std::move(rst);
          return;
        }
        pairs.clear();
        pairs.reserve(shard_bytes / 24);

        for (size_t w = 0; w < workers; ++w) {
          SlabChunkReader chunk_reader(spill_store,
                                       shard_worker_segments[s][w]);
          for (const SlabChunk& c : shard_worker_chunks[s][w]) {
            const uint8_t* p = nullptr;
            Status cst = chunk_reader.Resolve(c, &p);
            if (cst != Status::kSuccess) {
              p2_status[tid] = std::move(cst);
              return;
            }
            const uint8_t* end = p + c.used;
            while (p < end) {
              uint64_t kh = 0;
              std::memcpy(&kh, p, 8);
              p += 8;
              const uint8_t kmask = *p++;
              const uint8_t has_dist = *p++;
              int64_t dv = 0;
              if (has_dist) {
                std::memcpy(&dv, p, 8);
                p += 8;
              }
              for (size_t k = 0; k < n_phys; ++k) {
                probe_keys[k] = RuntimeKeyCell{};
                if ((kmask & (1U << k)) != 0) {
                  probe_keys[k].is_null = true;
                } else if (IsVarCharKeyKind(km.phys_specs[k].kind)) {
                  uint32_t sl = 0;
                  std::memcpy(&sl, p, 4);
                  p += 4;
                  probe_keys[k].str_ptr = reinterpret_cast<const char*>(p);
                  probe_keys[k].str_len = sl;
                  p += sl;
                } else if (km.phys_specs[k].kind == ColKeyKind::kColDouble) {
                  std::memcpy(&probe_keys[k].double_val, p, 8);
                  p += 8;
                } else {
                  std::memcpy(&probe_keys[k].int_val, p, 8);
                  p += 8;
                }
              }
              // Borrowed VARCHAR cells are only stable while the source
              // bytes live: resident chunks outlive the shard table, but a
              // spilled chunk resolves into the reader's scratch and must be
              // interned into the table arena instead.
              auto gid_or =
                  wt.FindOrInsert(probe_keys.data(), kh,
                                  /*borrow_strings=*/c.data != nullptr);
              if (!gid_or.HasValue()) {
                p2_status[tid] = gid_or.GetStatus();
                return;
              }
              const uint32_t gid = gid_or.Value();
              if (has_dist) {
                pairs.push_back({.gid = gid, .val = dv});
              }

              RuntimeAggState* ast = n_extra > 0 ? wt.AggsAt(gid) : nullptr;
              for (size_t na = 0; na < n_extra; ++na) {
                const ColAggSpec& ex = non_distinct_specs[na];
                RuntimeAggState& st = ast[na];
                if (ex.kind == ColAggKind::kCountStar) {
                  ++st.count;
                  st.has_value = true;
                  continue;
                }
                const uint8_t is_null = *p++;
                if (is_null) {
                  continue;
                }
                if (ex.kind == ColAggKind::kCountCol ||
                    ex.kind == ColAggKind::kCountByteLen) {
                  ++st.count;
                  st.has_value = true;
                } else if (ex.kind == ColAggKind::kSumByteLen ||
                           ex.kind == ColAggKind::kAvgByteLen ||
                           ex.kind == ColAggKind::kMinByteLen ||
                           ex.kind == ColAggKind::kMaxByteLen ||
                           ex.kind == ColAggKind::kSumInt64 ||
                           ex.kind == ColAggKind::kMinInt64 ||
                           ex.kind == ColAggKind::kMaxInt64 ||
                           ex.kind == ColAggKind::kMinDate ||
                           ex.kind == ColAggKind::kMaxDate) {
                  int64_t val = 0;
                  std::memcpy(&val, p, 8);
                  p += 8;
                  if (ex.kind == ColAggKind::kSumInt64) {
                    int64_t nx = 0;
                    if (__builtin_add_overflow(st.int_val, val, &nx)) {
                      p2_status[tid] = StatusError(StatusCode::kIsInfinity,
                                                   "integer overflow on SUM");
                      return;
                    }
                    st.int_val = nx;
                    st.has_value = true;
                  } else if (ex.kind == ColAggKind::kSumByteLen) {
                    st.int_val += val;
                    st.has_value = true;
                  } else if (ex.kind == ColAggKind::kAvgByteLen) {
                    st.double_val += static_cast<double>(val);
                    ++st.count;
                    st.has_value = true;
                  } else if (ex.kind == ColAggKind::kMinInt64 ||
                             ex.kind == ColAggKind::kMinDate ||
                             ex.kind == ColAggKind::kMinByteLen) {
                    if (!st.has_value || val < st.int_val) {
                      st.int_val = val;
                      st.has_value = true;
                    }
                  } else {
                    if (!st.has_value || val > st.int_val) {
                      st.int_val = val;
                      st.has_value = true;
                    }
                  }
                } else {
                  int64_t bits = 0;
                  std::memcpy(&bits, p, 8);
                  p += 8;
                  const bool is_double_col =
                      req.table_schema.GetColumn(ex.full_col).Type() ==
                      ValueType::kDouble;
                  const double d = is_double_col ? std::bit_cast<double>(bits)
                                                 : static_cast<double>(bits);
                  if (ex.kind == ColAggKind::kAvgDouble) {
                    st.double_val += d;
                    ++st.count;
                    st.has_value = true;
                  } else if (ex.kind == ColAggKind::kSumDouble) {
                    st.double_val += d;
                    st.has_value = true;
                  } else if (ex.kind == ColAggKind::kMinDouble) {
                    if (!st.has_value ||
                        CompareForOrderBy(Value(d), Value(st.double_val)) < 0) {
                      st.double_val = d;
                      st.has_value = true;
                    }
                  } else {
                    if (!st.has_value ||
                        CompareForOrderBy(Value(d), Value(st.double_val)) > 0) {
                      st.double_val = d;
                      st.has_value = true;
                    }
                  }
                }
              }
            }
          }
        }

        const size_t ng = wt.NumGroups();
        total_groups_out.fetch_add(ng, std::memory_order_relaxed);
        distinct_counts.assign(ng, 0);
        if (!pairs.empty()) {
          Status prst =
              pset.Reserve(std::min(pairs.size(), kMaxPresizedShardGroups));
          if (prst != Status::kSuccess) {
            p2_status[tid] = std::move(prst);
            return;
          }
          pset.ClearKeepingCapacity();
          for (const TempPair& pr : pairs) {
            auto ins_or = pset.Insert(pr.gid, pr.val);
            if (!ins_or.HasValue()) {
              p2_status[tid] = ins_or.GetStatus();
              return;
            }
            if (ins_or.Value()) {
              ++distinct_counts[pr.gid];
            }
          }
        }

        auto& out_vec = shard_survivors[s];
        QueryMemoryCharge& survivor_charge =
            SurvivorChargeFor(shard_survivor_charges, s);
        const bool use_shard_heap =
            bounded_topk && topk_cap > 0 && ng > topk_cap;
        const SurvivorComparator local_cmp{
            .fast_kind = fast_order_kind,
            .order_bindings = &req.order_bindings};

        for (uint32_t g = 0; g < ng; ++g) {
          const RuntimeAggState* nd_ast = n_extra > 0 ? wt.AggsAt(g) : nullptr;
          for (size_t na = 0; na < n_extra; ++na) {
            CopyNonDistinctAggState(nd_ast[na],
                                    full_agg_states[non_distinct_idxs[na]]);
          }
          RuntimeAggState& dstate =
              full_agg_states[static_cast<size_t>(distinct_agg_idx)];
          dstate.count = static_cast<int64_t>(distinct_counts[g]);
          dstate.has_value = true;

          auto h_or = EvalPositionalHavingOnStates(
              req.positional_having, req.agg_specs, full_agg_states.data(),
              &having_scratch);
          if (!h_or.HasValue()) {
            p2_status[tid] = h_or.GetStatus();
            return;
          }
          if (!h_or.Value()) {
            continue;
          }

          if (bounded_topk && topk_cap == 0) {
            continue;
          }

          SurvivorGroupRow row;
          km.ReconstructFull(req.key_specs, wt.KeysAt(g), full_keys.data());
          if (fast_order_kind != PartOrderKind::kNone) {
            const auto& ob0 = req.order_bindings[0].binding;
            if (ob0.kind == OutputSlotBinding::Kind::kAgg &&
                std::cmp_equal(ob0.index, distinct_agg_idx)) {
              row.num_null = false;
              row.num_int = static_cast<int64_t>(distinct_counts[g]);
            } else {
              Status nst = EvaluateNumericOrderValue(
                  ob0,
                  ob0.kind == OutputSlotBinding::Kind::kAgg
                      ? &req.agg_specs[ob0.index]
                      : nullptr,
                  ob0.kind == OutputSlotBinding::Kind::kKey
                      ? &req.key_specs[ob0.index]
                      : nullptr,
                  ob0.kind == OutputSlotBinding::Kind::kAgg
                      ? &full_agg_states[ob0.index]
                      : nullptr,
                  ob0.kind == OutputSlotBinding::Kind::kKey
                      ? &full_keys[ob0.index]
                      : nullptr,
                  &row.num_null, &row.num_int, &row.num_double);
              if (nst != Status::kSuccess) {
                p2_status[tid] = std::move(nst);
                return;
              }
            }
            if (use_shard_heap && out_vec.size() >= topk_cap &&
                !local_cmp(row, out_vec.front())) {
              continue;
            }
          }

          for (size_t a = 0; a < req.agg_specs.size(); ++a) {
            if (std::cmp_equal(a, distinct_agg_idx)) {
              finished_aggs[a] =
                  Value(static_cast<int64_t>(distinct_counts[g]));
            } else {
              finished_aggs[a] =
                  FinishAggState(req.agg_specs[a], full_agg_states[a]);
            }
          }
          std::fill(cached_keys.begin(), cached_keys.end(), Value());
          if (fast_order_kind == PartOrderKind::kNone &&
              !req.order_bindings.empty()) {
            row.order_keys.resize(req.order_bindings.size());
            for (size_t k = 0; k < req.order_bindings.size(); ++k) {
              auto ov_or = EvaluateBindingValue(req.order_bindings[k].binding,
                                                req.key_specs, full_keys.data(),
                                                finished_aggs, &cached_keys);
              if (!ov_or.HasValue()) {
                p2_status[tid] = ov_or.GetStatus();
                return;
              }
              row.order_keys[k] = std::move(ov_or.Value());
            }
            if (use_shard_heap && out_vec.size() >= topk_cap &&
                !local_cmp(row, out_vec.front())) {
              continue;
            }
          }
          row.emit_vals.reserve(req.emit_bindings.size());
          for (const auto& eb : req.emit_bindings) {
            auto ev_or =
                EvaluateBindingValue(eb, req.key_specs, full_keys.data(),
                                     finished_aggs, &cached_keys);
            if (!ev_or.HasValue()) {
              p2_status[tid] = ev_or.GetStatus();
              return;
            }
            row.emit_vals.push_back(std::move(ev_or.Value()));
          }

          if (use_shard_heap) {
            if (out_vec.size() < topk_cap) {
              PushChargedSurvivor(&out_vec, &survivor_charge, std::move(row));
              std::push_heap(out_vec.begin(), out_vec.end(), local_cmp);
            } else {
              std::pop_heap(out_vec.begin(), out_vec.end(), local_cmp);
              survivor_charge.Release(
                  EstimateSurvivorGroupRowBytes(out_vec.back()));
              PushChargedSurvivor(&out_vec, &survivor_charge, std::move(row));
              std::push_heap(out_vec.begin(), out_vec.end(), local_cmp);
            }
          } else {
            PushChargedSurvivor(&out_vec, &survivor_charge, std::move(row));
            if (req.order_bindings.empty() && req.stmt.HasLimit() &&
                out_vec.size() >= topk_cap) {
              break;
            }
          }
        }
      }
      // Free Pass 1 slabs and charges for shard `s` before moving to next
      // shard.
      shard_worker_chunks[s].clear();
      shard_worker_charges[s].clear();
      for (const SpillSegmentId seg : shard_worker_segments[s]) {
        spill_store->DropSegment(seg);
      }
    }
  });

  for (const Status& st : p2_status) {
    if (st == StatusCode::kMemoryLimitExceeded) {
      return std::optional<Relation>{};
    }
    RETURN_IF_FAIL(st);
  }

  return FinalizePartitionedRelation(
      req, shard_survivors, total_groups_out.load(), total_scanned.load(),
      total_matching.load(), merge_begin, "group-distinct",
      &shard_survivor_charges);
}

// ============================================================================
// PATH B: Pure Fixed-Width Numeric Partitioned Aggregation
// (`Q16, Q31, Q32, Q33, Q36`)
// ============================================================================

struct NumericAggCell {
  int64_t val_bits{0};
  int64_t cnt{0};
};

inline Status MergeNumericRawValue(const ColAggSpec& spec, bool val_is_null,
                                   int64_t val_i, double val_d,
                                   int64_t multiplicity,
                                   NumericAggCell& st) noexcept {
  if (spec.kind == ColAggKind::kCountStar ||
      ((spec.kind == ColAggKind::kCountCol ||
        spec.kind == ColAggKind::kCountByteLen) &&
       !val_is_null)) {
    st.cnt += multiplicity;
    return Status::kSuccess;
  }
  if (val_is_null) {
    return Status::kSuccess;
  }
  switch (spec.kind) {
    case ColAggKind::kSumInt64: {
      int64_t next = 0;
      if (__builtin_add_overflow(st.val_bits, val_i, &next)) {
        return StatusError(StatusCode::kIsInfinity, "integer overflow on SUM");
      }
      st.val_bits = next;
      st.cnt += multiplicity;
      break;
    }
    case ColAggKind::kSumByteLen:
      st.val_bits += val_i;
      st.cnt += multiplicity;
      break;
    case ColAggKind::kAvgDouble:
    case ColAggKind::kAvgByteLen:
    case ColAggKind::kSumDouble: {
      const double d = std::bit_cast<double>(st.val_bits) + val_d;
      st.val_bits = std::bit_cast<int64_t>(d);
      st.cnt += multiplicity;
      break;
    }
    case ColAggKind::kMinInt64:
    case ColAggKind::kMinDate:
    case ColAggKind::kMinByteLen:
      if (st.cnt == 0 || val_i < st.val_bits) {
        st.val_bits = val_i;
      }
      st.cnt += multiplicity;
      break;
    case ColAggKind::kMaxInt64:
    case ColAggKind::kMaxDate:
    case ColAggKind::kMaxByteLen:
      if (st.cnt == 0 || val_i > st.val_bits) {
        st.val_bits = val_i;
      }
      st.cnt += multiplicity;
      break;
    case ColAggKind::kMinDouble: {
      const auto cur = std::bit_cast<double>(st.val_bits);
      if (st.cnt == 0 || CompareForOrderBy(Value(val_d), Value(cur)) < 0) {
        st.val_bits = std::bit_cast<int64_t>(val_d);
      }
      st.cnt += multiplicity;
      break;
    }
    case ColAggKind::kMaxDouble: {
      const auto cur = std::bit_cast<double>(st.val_bits);
      if (st.cnt == 0 || CompareForOrderBy(Value(val_d), Value(cur)) > 0) {
        st.val_bits = std::bit_cast<int64_t>(val_d);
      }
      st.cnt += multiplicity;
      break;
    }
    default:
      break;
  }
  return Status::kSuccess;
}

inline RuntimeAggState ToRuntimeAggState(const ColAggSpec& spec,
                                         const NumericAggCell& nc) noexcept {
  RuntimeAggState st;
  st.count = nc.cnt;
  st.has_value = (nc.cnt > 0);
  if (spec.kind == ColAggKind::kSumDouble ||
      spec.kind == ColAggKind::kAvgDouble ||
      spec.kind == ColAggKind::kAvgByteLen ||
      spec.kind == ColAggKind::kMinDouble ||
      spec.kind == ColAggKind::kMaxDouble) {
    st.double_val = std::bit_cast<double>(nc.val_bits);
  } else {
    st.int_val = nc.val_bits;
  }
  return st;
}

StatusOr<std::optional<Relation>> ExecutePartitionedFixedKeys(
    const PartitionedColumnarAggRequest& req, const KeyProjectionMap& km,
    SpillStore* spill_store) {
  const size_t n_phys = km.phys_specs.size();
  const size_t naggs = req.agg_specs.size();
  if (n_phys == 0 || n_phys > 8) {
    return std::optional<Relation>{};
  }
  for (const auto& ks : km.phys_specs) {
    if (!IsFixedNumericKeyKind(ks.kind)) {
      return std::optional<Relation>{};
    }
  }
  for (const auto& as : req.agg_specs) {
    if (!IsFixedNumericAggKind(as.kind)) {
      return std::optional<Relation>{};
    }
  }
  if (naggs > 8) {
    return std::optional<Relation>{};
  }

  const ColumnarStageSchedule sched = BuildColumnarStageSchedule(
      req.table_schema, req.preds, km.phys_specs, req.agg_specs);
  const size_t ncols = req.table_schema.ColumnCount();
  const size_t workers = std::max<size_t>(
      1, std::min<size_t>(
             {QueryParallelWorkerLimit(std::thread::hardware_concurrency()),
              req.candidate_blocks.size(), size_t{16}}));

  const bool only_count_star =
      (naggs == 1 && req.agg_specs[0].kind == ColAggKind::kCountStar);
  const bool single_key = (n_phys == 1);

  // Use 512 shards so 100M rows (`~195K`/shard) fit comfortably in CPU L3.
  const size_t num_shards =
      req.candidate_blocks.size() <= 16
          ? 16
          : (req.candidate_blocks.size() <= 256 ? 64 : 512);
  const uint64_t shard_mask = num_shards - 1;

  std::vector<std::vector<std::vector<SlabChunk>>> shard_worker_chunks(
      num_shards);
  std::vector<std::vector<std::unique_ptr<QueryMemoryCharge>>>
      shard_worker_charges(num_shards);
  for (size_t s = 0; s < num_shards; ++s) {
    shard_worker_chunks[s].resize(workers);
    shard_worker_charges[s].resize(workers);
  }
  std::vector<std::vector<SpillSegmentId>> shard_worker_segments(
      num_shards, std::vector<SpillSegmentId>(workers, kInvalidSpillSegmentId));

  std::atomic<size_t> next_blk{0};
  std::atomic<uint64_t> total_scanned{0};
  std::atomic<uint64_t> total_matching{0};
  std::vector<Status> p1_status(workers, Status::kSuccess);
  const size_t init_chunk_bytes =
      req.candidate_blocks.size() <= 32 ? size_t{1024} : size_t{4096};

  const uint32_t writer_chunk_cap = static_cast<uint32_t>(
      std::max<size_t>(1, (QueryMemoryBudget::Global().Remaining() / 2) /
                              (SpillStore::kBlockSize * num_shards * workers)));

  // Identify which aggs actually read a column vs `COUNT(*)`.
  std::vector<size_t> active_agg_cols;
  for (size_t a = 0; a < naggs; ++a) {
    if (req.agg_specs[a].kind != ColAggKind::kCountStar) {
      active_agg_cols.push_back(a);
    }
  }
  const size_t n_agg_vals = active_agg_cols.size();

  RunParallelTasks(workers, [&](size_t tid) {
    std::vector<WorkerShardWriter> writers(num_shards);
    for (size_t s = 0; s < num_shards; ++s) {
      shard_worker_charges[s][tid] = std::make_unique<QueryMemoryCharge>();
      writers[s].Init(&shard_worker_chunks[s][tid],
                      shard_worker_charges[s][tid].get(), init_chunk_bytes,
                      spill_store, &shard_worker_segments[s][tid],
                      writer_chunk_cap);
    }

    // Compact self-tuning L1 combiner for single-key COUNT(*) (`Q16, Q36`)
    // that auto-disables on low hit rates (`< 8%`) so unique WatchIDs bypass
    // it.
    struct L1SingleIntCount {
      int64_t key{0};
      uint32_t count{0};
      uint8_t occupied{0};
      bool is_null{false};
      uint64_t hash{0};
    };
    QueryMemoryCharge l1_charge;
    std::vector<L1SingleIntCount> l1_single;
    static constexpr size_t kFixedL1Slots = 8192;
    bool use_l1 = (only_count_star && single_key &&
                   IsFixedNumericKeyKind(km.phys_specs[0].kind));
    if (use_l1) {
      if (!l1_charge.TryAdd(kFixedL1Slots * sizeof(L1SingleIntCount))) {
        use_l1 = false;
      } else {
        l1_single.resize(kFixedL1Slots);
      }
    }
    uint64_t l1_probes = 0;
    uint64_t l1_hits = 0;

    auto flush_l1_single = [&]() -> Status {
      for (L1SingleIntCount& slot : l1_single) {
        if (!slot.occupied) {
          continue;
        }
        const size_t s = static_cast<size_t>(slot.hash) & shard_mask;
        const size_t rec_bytes = 8 + 1 + 8 + (slot.is_null ? 0 : 8);
        ASSIGN_OR_RETURN(uint8_t*, dst, writers[s].Reserve(rec_bytes));
        uint8_t* p = dst;
        std::memcpy(p, &slot.hash, 8);
        p += 8;
        *p++ = slot.is_null ? 1 : 0;
        const auto cnt = static_cast<int64_t>(slot.count);
        std::memcpy(p, &cnt, 8);
        p += 8;
        if (!slot.is_null) {
          std::memcpy(p, &slot.key, 8);
          p += 8;
        }
        writers[s].Commit(static_cast<size_t>(p - dst));
        slot.occupied = 0;
      }
      return Status::kSuccess;
    };

    BlockDecodeScratch dec_scratch;
    std::vector<const TlcsFile::DecodedColumnChunk*> col_ptrs(ncols, nullptr);
    std::vector<uint32_t> cand_rows;
    std::vector<RuntimeKeyCell> probe_keys(n_phys);
    std::string regex_scratch;
    TimestampPrefixCache ts_cache;
    uint64_t local_scanned = 0;
    uint64_t local_matching = 0;

    std::array<int64_t, 8> key_words{};
    std::array<int64_t, 8> agg_words{};

    while (true) {
      const size_t bi = next_blk.fetch_add(1, std::memory_order_relaxed);
      if (bi >= req.candidate_blocks.size()) {
        break;
      }
      const uint32_t bidx = req.candidate_blocks[bi];

      auto dec_or = DecodeAndFilterBlock(*req.tlcs, bidx, req.table_schema,
                                         req.preds, sched, &dec_scratch,
                                         &col_ptrs, &cand_rows, &local_scanned);
      if (!dec_or.HasValue()) {
        p1_status[tid] = dec_or.GetStatus();
        return;
      }
      if (!dec_or.Value()) {
        continue;
      }
      local_matching += cand_rows.size();

      // Ultra-fast branch for 1 fixed-width key + COUNT(*) (`Q16, Q36`).
      if (only_count_star && single_key &&
          IsFixedNumericKeyKind(km.phys_specs[0].kind)) {
        const ColKeyKind key_kind = km.phys_specs[0].kind;
        const bool direct_cells = (key_kind == ColKeyKind::kColInt64 ||
                                   key_kind == ColKeyKind::kColDate);
        const auto* kch =
            direct_cells ? col_ptrs[km.phys_specs[0].full_col] : nullptr;
        const bool no_null = kch != nullptr && !kch->has_null;
        for (const uint32_t r : cand_rows) {
          bool is_null = false;
          int64_t kv = 0;
          if (direct_cells) {
            is_null = !no_null && kch->IsNull(r);
            kv = is_null ? 0 : kch->int_cells[r];
          } else {
            Status kst = Status::kSuccess;
            if (!ExtractSingleKeyCell(km.phys_specs[0], col_ptrs, r,
                                      &probe_keys[0], &regex_scratch, &ts_cache,
                                      &kst)) {
              p1_status[tid] = std::move(kst);
              return;
            }
            is_null = probe_keys[0].is_null;
            kv = (key_kind == ColKeyKind::kColDouble)
                     ? CanonicalDoubleBits(probe_keys[0].double_val)
                     : probe_keys[0].int_val;
          }
          const uint64_t kh =
              is_null ? 0xa341316c01574b68ULL
                      : MixHash64(0x517cc1b727220a95ULL ^
                                  MixHash64(static_cast<uint64_t>(kv)));
          if (use_l1) {
            ++l1_probes;
            L1SingleIntCount& slot = l1_single[kh & (kFixedL1Slots - 1)];
            if (slot.occupied && slot.is_null == is_null &&
                (is_null || slot.key == kv) &&
                slot.count < std::numeric_limits<uint32_t>::max()) {
              ++slot.count;
              ++l1_hits;
              continue;
            }
            if (slot.occupied) {
              const size_t s = static_cast<size_t>(slot.hash) & shard_mask;
              const size_t rec_bytes = 8 + 1 + 8 + (slot.is_null ? 0 : 8);
              auto dst_or = writers[s].Reserve(rec_bytes);
              if (!dst_or.HasValue()) {
                p1_status[tid] = dst_or.GetStatus();
                return;
              }
              uint8_t* p = dst_or.Value();
              std::memcpy(p, &slot.hash, 8);
              p += 8;
              *p++ = slot.is_null ? 1 : 0;
              const auto cnt = static_cast<int64_t>(slot.count);
              std::memcpy(p, &cnt, 8);
              p += 8;
              if (!slot.is_null) {
                std::memcpy(p, &slot.key, 8);
                p += 8;
              }
              writers[s].Commit(static_cast<size_t>(p - dst_or.Value()));
            }
            slot.key = kv;
            slot.count = 1;
            slot.occupied = 1;
            slot.is_null = is_null;
            slot.hash = kh;
            if (l1_probes == 8192 && l1_hits * 16 < l1_probes) {
              Status fst = flush_l1_single();
              if (fst != Status::kSuccess) {
                p1_status[tid] = std::move(fst);
                return;
              }
              use_l1 = false;
              l1_single.clear();
              l1_charge = QueryMemoryCharge{};
            }
          } else {
            const size_t s = static_cast<size_t>(kh) & shard_mask;
            const size_t rec_bytes = 8 + 1 + 8 + (is_null ? 0 : 8);
            auto dst_or = writers[s].Reserve(rec_bytes);
            if (!dst_or.HasValue()) {
              p1_status[tid] = dst_or.GetStatus();
              return;
            }
            uint8_t* p = dst_or.Value();
            std::memcpy(p, &kh, 8);
            p += 8;
            *p++ = is_null ? 1 : 0;
            const int64_t one = 1;
            std::memcpy(p, &one, 8);
            p += 8;
            if (!is_null) {
              std::memcpy(p, &kv, 8);
              p += 8;
            }
            writers[s].Commit(static_cast<size_t>(p - dst_or.Value()));
          }
        }
        continue;
      }

      // Fast direct path for 2 int64/date keys (`Q31, Q32, Q33`) & general
      // fixed keys.
      const bool all_direct_int =
          std::all_of(km.phys_specs.begin(), km.phys_specs.end(),
                      [](const ColGroupKeySpec& ks) {
                        return ks.kind == ColKeyKind::kColInt64 ||
                               ks.kind == ColKeyKind::kColDate;
                      });

      for (const uint32_t r : cand_rows) {
        uint8_t key_null_mask = 0;
        uint64_t kh = 0x517cc1b727220a95ULL;
        if (all_direct_int) {
          for (size_t k = 0; k < n_phys; ++k) {
            const auto* kch = col_ptrs[km.phys_specs[k].full_col];
            uint64_t elem_h = 0;
            if (kch->has_null && kch->IsNull(r)) {
              key_null_mask |= static_cast<uint8_t>(1U << k);
              elem_h = 0x243f6a8885a308d3ULL;
            } else {
              key_words[k] = kch->int_cells[r];
              elem_h = MixHash64(static_cast<uint64_t>(key_words[k]));
            }
            kh = MixHash64(kh ^ (elem_h + k * 0x9e3779b97f4a7c15ULL));
          }
        } else {
          Status kst = Status::kSuccess;
          for (size_t k = 0; k < n_phys; ++k) {
            if (!ExtractSingleKeyCell(km.phys_specs[k], col_ptrs, r,
                                      &probe_keys[k], &regex_scratch, &ts_cache,
                                      &kst)) {
              p1_status[tid] = std::move(kst);
              return;
            }
            uint64_t elem_h = 0;
            if (probe_keys[k].is_null) {
              key_null_mask |= static_cast<uint8_t>(1U << k);
              elem_h = 0x243f6a8885a308d3ULL;
            } else if (km.phys_specs[k].kind == ColKeyKind::kColDouble) {
              key_words[k] = CanonicalDoubleBits(probe_keys[k].double_val);
              elem_h = MixHash64(static_cast<uint64_t>(key_words[k]));
            } else {
              key_words[k] = probe_keys[k].int_val;
              elem_h = MixHash64(static_cast<uint64_t>(key_words[k]));
            }
            kh = MixHash64(kh ^ (elem_h + k * 0x9e3779b97f4a7c15ULL));
          }
        }

        uint8_t agg_null_mask = 0;
        size_t non_null_agg_words = 0;
        for (size_t idx = 0; idx < n_agg_vals; ++idx) {
          const ColAggSpec& spec = req.agg_specs[active_agg_cols[idx]];
          const auto* ach = col_ptrs[spec.full_col];
          if (ach->has_null && ach->IsNull(r)) {
            agg_null_mask |= static_cast<uint8_t>(1U << idx);
          } else {
            if (spec.kind == ColAggKind::kCountCol ||
                spec.kind == ColAggKind::kCountByteLen) {
              // Presence in !agg_null_mask counts 1; 0 extra payload bytes!
            } else if (spec.kind == ColAggKind::kSumByteLen ||
                       spec.kind == ColAggKind::kAvgByteLen ||
                       spec.kind == ColAggKind::kMinByteLen ||
                       spec.kind == ColAggKind::kMaxByteLen) {
              agg_words[non_null_agg_words++] =
                  static_cast<int64_t>(ach->VarcharLength(r));
            } else {
              agg_words[non_null_agg_words++] = ach->int_cells[r];
            }
          }
        }

        const size_t non_null_key_words =
            n_phys - static_cast<size_t>(std::popcount(key_null_mask));
        const size_t need_bytes =
            8 + (only_count_star ? 9 : (2 + (non_null_agg_words * 8))) +
            (non_null_key_words * 8);
        const size_t s = static_cast<size_t>(kh) & shard_mask;
        auto dst_or = writers[s].Reserve(need_bytes);
        if (!dst_or.HasValue()) {
          p1_status[tid] = dst_or.GetStatus();
          return;
        }
        uint8_t* p = dst_or.Value();
        uint8_t* start = p;
        std::memcpy(p, &kh, 8);
        p += 8;
        *p++ = key_null_mask;
        if (only_count_star) {
          const int64_t one = 1;
          std::memcpy(p, &one, 8);
          p += 8;
        } else {
          *p++ = agg_null_mask;
        }
        for (size_t k = 0; k < n_phys; ++k) {
          if ((key_null_mask & (1U << k)) == 0) {
            std::memcpy(p, &key_words[k], 8);
            p += 8;
          }
        }
        for (size_t widx = 0; widx < non_null_agg_words; ++widx) {
          std::memcpy(p, &agg_words[widx], 8);
          p += 8;
        }
        writers[s].Commit(static_cast<size_t>(p - start));
      }
    }

    if (use_l1) {
      Status fst = flush_l1_single();
      if (fst != Status::kSuccess) {
        p1_status[tid] = std::move(fst);
        return;
      }
    }
    for (size_t s = 0; s < num_shards; ++s) {
      Status finst = writers[s].Finalize();
      if (finst != Status::kSuccess) {
        p1_status[tid] = std::move(finst);
        return;
      }
    }
    total_scanned.fetch_add(local_scanned, std::memory_order_relaxed);
    total_matching.fetch_add(local_matching, std::memory_order_relaxed);
  });

  for (const Status& st : p1_status) {
    if (st == StatusCode::kInvalidArgument ||
        st == StatusCode::kMemoryLimitExceeded) {
      return std::optional<Relation>{};
    }
    RETURN_IF_FAIL(st);
  }

  // Pass 2: aggregate each L3-sized shard with flat open-addressed group table
  // (`uint64_t` counts when `only_count_star`, compact POD `NumericAggCell`s
  // otherwise), extract HAVING/Top-K survivors, and immediately release shard!
  const auto merge_begin = std::chrono::steady_clock::now();
  const PartOrderKind fast_order_kind =
      ClassifyPartOrderKind(req.order_bindings, req.key_specs, req.agg_specs);
  const bool bounded_topk = req.stmt.HasLimit() && !req.order_bindings.empty();
  const size_t topk_cap =
      req.stmt.HasLimit()
          ? (req.stmt.Offset() <=
                     std::numeric_limits<size_t>::max() - req.stmt.Limit()
                 ? req.stmt.Offset() + req.stmt.Limit()
                 : std::numeric_limits<size_t>::max())
          : 0;

  std::vector<std::vector<SurvivorGroupRow>> shard_survivors(num_shards);
  // Survivor rows are plain vectors outside `Relation`; keep their charge
  // alive until the merge adopts the rows (see FinalizePartitionedRelation).
  std::vector<std::unique_ptr<QueryMemoryCharge>> shard_survivor_charges(
      num_shards);
  std::vector<Status> p2_status(workers, Status::kSuccess);
  std::atomic<size_t> total_groups_out{0};
  std::atomic<size_t> next_s{0};

  struct FixedSlot {
    uint64_t hash{0};
    uint32_t gid_plus_one{0};
  };

  const size_t rec_min_bytes = 8 + (only_count_star ? 9 : 2) + (n_phys * 8);

  RunParallelTasks(std::min(workers, num_shards), [&](size_t tid) {
    std::vector<RuntimeKeyCell> phys_cells(n_phys);
    std::vector<RuntimeKeyCell> full_keys(req.key_specs.size());
    std::vector<RuntimeAggState> full_aggs(naggs);
    std::vector<Value> finished_aggs(naggs);
    std::vector<Value> cached_keys(req.key_specs.size());
    std::vector<Value> having_scratch(naggs);

    std::array<int64_t, 8> probe_words{};

    while (true) {
      const size_t s = next_s.fetch_add(1, std::memory_order_relaxed);
      if (s >= num_shards) {
        break;
      }

      size_t shard_bytes = 0;
      for (size_t w = 0; w < workers; ++w) {
        for (const SlabChunk& c : shard_worker_chunks[s][w]) {
          shard_bytes += c.used;
        }
      }
      if (shard_bytes > 0) {
        QueryMemoryCharge ht_charge;
        const size_t est_groups = std::min<size_t>(
            std::max<size_t>(64,
                             shard_bytes / std::max<size_t>(16, rec_min_bytes)),
            kMaxPresizedShardGroups);
        const size_t init_cap =
            std::max<size_t>(256, std::bit_ceil(est_groups * 2));
        const size_t gbytes =
            (n_phys * sizeof(int64_t)) + 1 +
            (only_count_star ? sizeof(int64_t)
                             : (naggs * sizeof(NumericAggCell)));
        size_t reserved_groups_cap = std::max<size_t>(64, est_groups);
        if (!ht_charge.TryAdd((init_cap * sizeof(FixedSlot)) +
                              (reserved_groups_cap * gbytes))) {
          p2_status[tid] =
              StatusError(StatusCode::kMemoryLimitExceeded,
                          "fixed shard slot table exceeded budget");
          return;
        }
        std::vector<FixedSlot> slots(init_cap);
        std::vector<int64_t> keys_flat;
        std::vector<uint8_t> key_nulls;
        std::vector<int64_t> counts_only;
        std::vector<NumericAggCell> aggs_flat;
        keys_flat.reserve(reserved_groups_cap * n_phys);
        key_nulls.reserve(reserved_groups_cap);
        if (only_count_star) {
          counts_only.reserve(reserved_groups_cap);
        } else {
          aggs_flat.reserve(reserved_groups_cap * naggs);
        }
        size_t ng = 0;

        auto rehash_slots = [&](size_t next_cap) -> Status {
          const size_t delta = (next_cap - slots.size()) * sizeof(FixedSlot);
          if (!ht_charge.TryAdd(delta)) {
            return StatusError(StatusCode::kMemoryLimitExceeded,
                               "fixed shard slot grow exceeded budget");
          }
          std::vector<FixedSlot> next(next_cap);
          const size_t mask = next_cap - 1;
          for (const FixedSlot& old : slots) {
            if (old.gid_plus_one == 0) {
              continue;
            }
            size_t pos = static_cast<size_t>(old.hash) & mask;
            while (next[pos].gid_plus_one != 0) {
              pos = (pos + 1) & mask;
            }
            next[pos] = old;
          }
          slots.swap(next);
          return Status::kSuccess;
        };

        auto find_or_insert = [&](uint64_t kh, uint8_t kmask,
                                  const int64_t* kw) -> StatusOr<uint32_t> {
          if ((ng + 1) * 10 >= slots.size() * 7) {
            RETURN_IF_FAIL(rehash_slots(slots.size() * 2));
          }
          const size_t mask = slots.size() - 1;
          size_t pos = static_cast<size_t>(kh) & mask;
          while (true) {
            FixedSlot& sl = slots[pos];
            if (sl.gid_plus_one == 0) {
              if (ng >= reserved_groups_cap) {
                const size_t next_cap = reserved_groups_cap * 2;
                if (!ht_charge.TryAdd((next_cap - reserved_groups_cap) *
                                      gbytes)) {
                  return StatusError(
                      StatusCode::kMemoryLimitExceeded,
                      "fixed shard group payload exceeded budget");
                }
                reserved_groups_cap = next_cap;
              }
              const auto gid = static_cast<uint32_t>(ng++);
              const size_t kbase = keys_flat.size();
              keys_flat.resize(kbase + n_phys);
              for (size_t k = 0; k < n_phys; ++k) {
                keys_flat[kbase + k] = kw[k];
              }
              key_nulls.push_back(kmask);
              if (only_count_star) {
                counts_only.push_back(0);
              } else {
                aggs_flat.resize(aggs_flat.size() + naggs);
              }
              sl.hash = kh;
              sl.gid_plus_one = gid + 1;
              return gid;
            }
            if (sl.hash == kh) {
              const uint32_t eg = sl.gid_plus_one - 1;
              if (key_nulls[eg] == kmask) {
                const int64_t* ek =
                    &keys_flat[static_cast<size_t>(eg) * n_phys];
                bool eq = true;
                for (size_t k = 0; k < n_phys; ++k) {
                  if ((kmask & (1U << k)) == 0 && ek[k] != kw[k]) {
                    eq = false;
                    break;
                  }
                }
                if (eq) {
                  return eg;
                }
              }
            }
            pos = (pos + 1) & mask;
          }
        };

        for (size_t w = 0; w < workers; ++w) {
          SlabChunkReader chunk_reader(spill_store,
                                       shard_worker_segments[s][w]);
          for (const SlabChunk& c : shard_worker_chunks[s][w]) {
            const uint8_t* p = nullptr;
            Status cst = chunk_reader.Resolve(c, &p);
            if (cst != Status::kSuccess) {
              p2_status[tid] = std::move(cst);
              return;
            }
            const uint8_t* end = p + c.used;
            while (p < end) {
              uint64_t kh = 0;
              std::memcpy(&kh, p, 8);
              p += 8;
              const uint8_t kmask = *p++;
              if (only_count_star) {
                int64_t cnt = 0;
                std::memcpy(&cnt, p, 8);
                p += 8;
                for (size_t k = 0; k < n_phys; ++k) {
                  if ((kmask & (1U << k)) == 0) {
                    std::memcpy(&probe_words[k], p, 8);
                    p += 8;
                  } else {
                    probe_words[k] = 0;
                  }
                }
                auto gid_or = find_or_insert(kh, kmask, probe_words.data());
                if (!gid_or.HasValue()) {
                  p2_status[tid] = gid_or.GetStatus();
                  return;
                }
                counts_only[gid_or.Value()] += cnt;
              } else {
                const uint8_t amask = *p++;
                for (size_t k = 0; k < n_phys; ++k) {
                  if ((kmask & (1U << k)) == 0) {
                    std::memcpy(&probe_words[k], p, 8);
                    p += 8;
                  } else {
                    probe_words[k] = 0;
                  }
                }
                auto gid_or = find_or_insert(kh, kmask, probe_words.data());
                if (!gid_or.HasValue()) {
                  p2_status[tid] = gid_or.GetStatus();
                  return;
                }
                NumericAggCell* dst =
                    &aggs_flat[static_cast<size_t>(gid_or.Value()) * naggs];
                size_t aidx_word = 0;
                for (size_t a = 0; a < naggs; ++a) {
                  const ColAggSpec& spec = req.agg_specs[a];
                  if (spec.kind == ColAggKind::kCountStar) {
                    dst[a].cnt += 1;
                    continue;
                  }
                  const bool is_null = (amask & (1U << aidx_word)) != 0;
                  ++aidx_word;
                  if (is_null) {
                    continue;
                  }
                  int64_t vi = 0;
                  double vd = 0.0;
                  if (spec.kind != ColAggKind::kCountCol &&
                      spec.kind != ColAggKind::kCountByteLen) {
                    std::memcpy(&vi, p, 8);
                    p += 8;
                    if (spec.kind == ColAggKind::kAvgDouble) {
                      const bool is_double_col =
                          req.table_schema.GetColumn(spec.full_col).Type() ==
                          ValueType::kDouble;
                      vd = is_double_col ? std::bit_cast<double>(vi)
                                         : static_cast<double>(vi);
                    } else if (spec.kind == ColAggKind::kSumDouble ||
                               spec.kind == ColAggKind::kMinDouble ||
                               spec.kind == ColAggKind::kMaxDouble) {
                      vd = std::bit_cast<double>(vi);
                    } else if (spec.kind == ColAggKind::kAvgByteLen) {
                      vd = static_cast<double>(vi);
                    }
                  }
                  Status mst =
                      MergeNumericRawValue(spec, false, vi, vd, 1, dst[a]);
                  if (mst != Status::kSuccess) {
                    p2_status[tid] = std::move(mst);
                    return;
                  }
                }
              }
            }
          }
        }

        total_groups_out.fetch_add(ng, std::memory_order_relaxed);
        auto& out_vec = shard_survivors[s];
        QueryMemoryCharge& survivor_charge =
            SurvivorChargeFor(shard_survivor_charges, s);
        const bool use_shard_heap =
            bounded_topk && topk_cap > 0 && ng > topk_cap;
        const SurvivorComparator local_cmp{
            .fast_kind = fast_order_kind,
            .order_bindings = &req.order_bindings};

        auto materialize_fixed_group =
            [&](size_t g) -> StatusOr<SurvivorGroupRow> {
          if (only_count_star) {
            full_aggs[0].int_val = 0;
            full_aggs[0].double_val = 0.0;
            full_aggs[0].count = counts_only[g];
            full_aggs[0].has_value = true;
          } else {
            const NumericAggCell* src = &aggs_flat[g * naggs];
            for (size_t a = 0; a < naggs; ++a) {
              full_aggs[a] = ToRuntimeAggState(req.agg_specs[a], src[a]);
            }
          }
          const uint8_t kmask = key_nulls[g];
          const int64_t* gk = keys_flat.data() + (g * n_phys);
          for (size_t k = 0; k < n_phys; ++k) {
            phys_cells[k] = RuntimeKeyCell{};
            if ((kmask & (1U << k)) != 0) {
              phys_cells[k].is_null = true;
            } else if (km.phys_specs[k].kind == ColKeyKind::kColDouble) {
              phys_cells[k].double_val = std::bit_cast<double>(gk[k]);
            } else {
              phys_cells[k].int_val = gk[k];
            }
          }
          km.ReconstructFull(req.key_specs, phys_cells.data(),
                             full_keys.data());
          SurvivorGroupRow row;
          if (fast_order_kind != PartOrderKind::kNone) {
            const auto& ob0 = req.order_bindings[0].binding;
            RETURN_IF_FAIL(EvaluateNumericOrderValue(
                ob0,
                ob0.kind == OutputSlotBinding::Kind::kAgg
                    ? &req.agg_specs[ob0.index]
                    : nullptr,
                ob0.kind == OutputSlotBinding::Kind::kKey
                    ? &req.key_specs[ob0.index]
                    : nullptr,
                ob0.kind == OutputSlotBinding::Kind::kAgg
                    ? &full_aggs[ob0.index]
                    : nullptr,
                ob0.kind == OutputSlotBinding::Kind::kKey
                    ? &full_keys[ob0.index]
                    : nullptr,
                &row.num_null, &row.num_int, &row.num_double));
          }
          for (size_t a = 0; a < naggs; ++a) {
            finished_aggs[a] = FinishAggState(req.agg_specs[a], full_aggs[a]);
          }
          std::fill(cached_keys.begin(), cached_keys.end(), Value());
          if (fast_order_kind == PartOrderKind::kNone &&
              !req.order_bindings.empty()) {
            row.order_keys.resize(req.order_bindings.size());
            for (size_t k = 0; k < req.order_bindings.size(); ++k) {
              ASSIGN_OR_RETURN(
                  Value, ov,
                  EvaluateBindingValue(req.order_bindings[k].binding,
                                       req.key_specs, full_keys.data(),
                                       finished_aggs, &cached_keys));
              row.order_keys[k] = std::move(ov);
            }
          }
          row.emit_vals.reserve(req.emit_bindings.size());
          for (const auto& eb : req.emit_bindings) {
            ASSIGN_OR_RETURN(
                Value, ev,
                EvaluateBindingValue(eb, req.key_specs, full_keys.data(),
                                     finished_aggs, &cached_keys));
            row.emit_vals.push_back(std::move(ev));
          }
          return row;
        };

        // Fast index pre-filter for Top-K ordered queries without HAVING:
        // scans only the single sort scalar across all groups (`O(N)`) and
        // materializes full `Value` rows ONLY for the `<= topk_cap` winners.
        if (use_shard_heap && fast_order_kind != PartOrderKind::kNone &&
            req.positional_having.empty()) {
          const SortKeyOrder& ord0 = req.order_bindings[0].order;
          const OutputSlotBinding& ob0 = req.order_bindings[0].binding;
          const bool order_is_agg = (ob0.kind == OutputSlotBinding::Kind::kAgg);
          const int32_t key_phys_idx =
              (!order_is_agg && ob0.kind == OutputSlotBinding::Kind::kKey)
                  ? km.full_to_phys[ob0.index]
                  : -1;
          const bool direct_fast_prefilter = order_is_agg || key_phys_idx >= 0;

          if (direct_fast_prefilter) {
            struct FastWin {
              bool is_null{false};
              int64_t iv{0};
              double dv{0.0};
              size_t gid{0};
            };
            auto win_cmp = [&](const FastWin& a, const FastWin& b) noexcept {
              if (a.is_null || b.is_null) {
                if (a.is_null != b.is_null) {
                  return a.is_null ? ord0.NullsFirst() : !ord0.NullsFirst();
                }
                return false;
              }
              if (fast_order_kind == PartOrderKind::kInt) {
                return ord0.Ascending() ? (a.iv < b.iv) : (a.iv > b.iv);
              }
              const bool na = std::isnan(a.dv);
              const bool nb = std::isnan(b.dv);
              if (na != nb) {
                return ord0.Ascending() ? na : !na;
              }
              return !na && (ord0.Ascending() ? (a.dv < b.dv) : (a.dv > b.dv));
            };
            std::vector<FastWin> winners;
            winners.reserve(topk_cap + 1);
            const ColAggSpec* oagg =
                order_is_agg ? &req.agg_specs[ob0.index] : nullptr;

            for (size_t g = 0; g < ng; ++g) {
              FastWin w{.gid = g};
              if (only_count_star && order_is_agg) {
                w.iv = counts_only[g];
              } else if (order_is_agg) {
                const NumericAggCell& nc = aggs_flat[(g * naggs) + ob0.index];
                switch (oagg->kind) {
                  case ColAggKind::kCountStar:
                  case ColAggKind::kCountCol:
                  case ColAggKind::kCountByteLen:
                    w.iv = nc.cnt;
                    break;
                  case ColAggKind::kSumInt64:
                  case ColAggKind::kSumByteLen:
                  case ColAggKind::kMinInt64:
                  case ColAggKind::kMaxInt64:
                  case ColAggKind::kMinByteLen:
                  case ColAggKind::kMaxByteLen:
                  case ColAggKind::kMinDate:
                  case ColAggKind::kMaxDate:
                    w.is_null = (nc.cnt == 0);
                    w.iv = nc.val_bits;
                    break;
                  case ColAggKind::kAvgDouble:
                  case ColAggKind::kAvgByteLen:
                    w.is_null = (nc.cnt == 0);
                    w.dv = nc.cnt == 0 ? 0.0
                                       : (std::bit_cast<double>(nc.val_bits) /
                                          static_cast<double>(nc.cnt));
                    break;
                  default:
                    w.is_null = (nc.cnt == 0);
                    w.dv = std::bit_cast<double>(nc.val_bits);
                    break;
                }
              } else {
                const auto kp = static_cast<size_t>(key_phys_idx);
                w.is_null = ((key_nulls[g] & (1U << kp)) != 0);
                const int64_t raw = keys_flat[(g * n_phys) + kp];
                if (fast_order_kind == PartOrderKind::kInt) {
                  w.iv = raw;
                } else {
                  w.dv = std::bit_cast<double>(raw);
                }
              }
              if (winners.size() < topk_cap) {
                winners.push_back(w);
                std::push_heap(winners.begin(), winners.end(), win_cmp);
              } else if (win_cmp(w, winners.front())) {
                std::pop_heap(winners.begin(), winners.end(), win_cmp);
                winners.back() = w;
                std::push_heap(winners.begin(), winners.end(), win_cmp);
              }
            }
            out_vec.reserve(winners.size());
            for (const FastWin& fw : winners) {
              auto row_or = materialize_fixed_group(fw.gid);
              if (!row_or.HasValue()) {
                p2_status[tid] = row_or.GetStatus();
                return;
              }
              PushChargedSurvivor(&out_vec, &survivor_charge,
                                  std::move(row_or.Value()));
            }
            shard_worker_chunks[s].clear();
            shard_worker_charges[s].clear();
            for (const SpillSegmentId seg : shard_worker_segments[s]) {
              spill_store->DropSegment(seg);
            }
            continue;
          }
        }

        for (size_t g = 0; g < ng; ++g) {
          if (only_count_star) {
            full_aggs[0].int_val = 0;
            full_aggs[0].double_val = 0.0;
            full_aggs[0].count = counts_only[g];
            full_aggs[0].has_value = true;
          } else {
            const NumericAggCell* src = &aggs_flat[g * naggs];
            for (size_t a = 0; a < naggs; ++a) {
              full_aggs[a] = ToRuntimeAggState(req.agg_specs[a], src[a]);
            }
          }
          auto h_or =
              EvalPositionalHavingOnStates(req.positional_having, req.agg_specs,
                                           full_aggs.data(), &having_scratch);
          if (!h_or.HasValue()) {
            p2_status[tid] = h_or.GetStatus();
            return;
          }
          if (!h_or.Value()) {
            continue;
          }
          if (bounded_topk && topk_cap == 0) {
            continue;
          }

          const uint8_t kmask = key_nulls[g];
          const int64_t* gk = keys_flat.data() + (g * n_phys);
          for (size_t k = 0; k < n_phys; ++k) {
            phys_cells[k] = RuntimeKeyCell{};
            if ((kmask & (1U << k)) != 0) {
              phys_cells[k].is_null = true;
            } else if (km.phys_specs[k].kind == ColKeyKind::kColDouble) {
              phys_cells[k].double_val = std::bit_cast<double>(gk[k]);
            } else {
              phys_cells[k].int_val = gk[k];
            }
          }
          km.ReconstructFull(req.key_specs, phys_cells.data(),
                             full_keys.data());

          SurvivorGroupRow row;
          if (fast_order_kind != PartOrderKind::kNone) {
            const auto& ob0 = req.order_bindings[0].binding;
            Status nst = EvaluateNumericOrderValue(
                ob0,
                ob0.kind == OutputSlotBinding::Kind::kAgg
                    ? &req.agg_specs[ob0.index]
                    : nullptr,
                ob0.kind == OutputSlotBinding::Kind::kKey
                    ? &req.key_specs[ob0.index]
                    : nullptr,
                ob0.kind == OutputSlotBinding::Kind::kAgg
                    ? &full_aggs[ob0.index]
                    : nullptr,
                ob0.kind == OutputSlotBinding::Kind::kKey
                    ? &full_keys[ob0.index]
                    : nullptr,
                &row.num_null, &row.num_int, &row.num_double);
            if (nst != Status::kSuccess) {
              p2_status[tid] = std::move(nst);
              return;
            }
            if (use_shard_heap && out_vec.size() >= topk_cap &&
                !local_cmp(row, out_vec.front())) {
              continue;
            }
          }

          for (size_t a = 0; a < naggs; ++a) {
            finished_aggs[a] = FinishAggState(req.agg_specs[a], full_aggs[a]);
          }
          std::fill(cached_keys.begin(), cached_keys.end(), Value());
          if (fast_order_kind == PartOrderKind::kNone &&
              !req.order_bindings.empty()) {
            row.order_keys.resize(req.order_bindings.size());
            for (size_t k = 0; k < req.order_bindings.size(); ++k) {
              auto ov_or = EvaluateBindingValue(req.order_bindings[k].binding,
                                                req.key_specs, full_keys.data(),
                                                finished_aggs, &cached_keys);
              if (!ov_or.HasValue()) {
                p2_status[tid] = ov_or.GetStatus();
                return;
              }
              row.order_keys[k] = std::move(ov_or.Value());
            }
            if (use_shard_heap && out_vec.size() >= topk_cap &&
                !local_cmp(row, out_vec.front())) {
              continue;
            }
          }
          row.emit_vals.reserve(req.emit_bindings.size());
          for (const auto& eb : req.emit_bindings) {
            auto ev_or =
                EvaluateBindingValue(eb, req.key_specs, full_keys.data(),
                                     finished_aggs, &cached_keys);
            if (!ev_or.HasValue()) {
              p2_status[tid] = ev_or.GetStatus();
              return;
            }
            row.emit_vals.push_back(std::move(ev_or.Value()));
          }

          if (use_shard_heap) {
            if (out_vec.size() < topk_cap) {
              PushChargedSurvivor(&out_vec, &survivor_charge, std::move(row));
              std::push_heap(out_vec.begin(), out_vec.end(), local_cmp);
            } else {
              std::pop_heap(out_vec.begin(), out_vec.end(), local_cmp);
              survivor_charge.Release(
                  EstimateSurvivorGroupRowBytes(out_vec.back()));
              PushChargedSurvivor(&out_vec, &survivor_charge, std::move(row));
              std::push_heap(out_vec.begin(), out_vec.end(), local_cmp);
            }
          } else {
            PushChargedSurvivor(&out_vec, &survivor_charge, std::move(row));
            if (req.order_bindings.empty() && req.stmt.HasLimit() &&
                out_vec.size() >= topk_cap) {
              break;
            }
          }
        }
      }
      // Immediately release shard `s` slabs + memory charges!
      shard_worker_chunks[s].clear();
      shard_worker_charges[s].clear();
      for (const SpillSegmentId seg : shard_worker_segments[s]) {
        spill_store->DropSegment(seg);
      }
    }
  });

  for (const Status& st : p2_status) {
    if (st == StatusCode::kMemoryLimitExceeded) {
      return std::optional<Relation>{};
    }
    RETURN_IF_FAIL(st);
  }

  return FinalizePartitionedRelation(
      req, shard_survivors, total_groups_out.load(), total_scanned.load(),
      total_matching.load(), merge_begin, "fixed-sharded",
      &shard_survivor_charges);
}

// ============================================================================
// PATH C: Mixed / VarChar / Computed-Key Partitioned Aggregation
// (`Q13, Q15, Q17, Q18, Q19, Q34, Q35`)
// ============================================================================

StatusOr<std::optional<Relation>> ExecutePartitionedMixedKeysInternal(
    const PartitionedColumnarAggRequest& req, const KeyProjectionMap& km,
    bool shard0_only_attempt, SpillStore* spill_store) {
  const size_t n_phys = km.phys_specs.size();
  const size_t naggs = req.agg_specs.size();
  if (n_phys == 0 || n_phys > 8) {
    return std::optional<Relation>{};
  }
  for (const auto& as : req.agg_specs) {
    if (IsDistinctAggKind(as.kind)) {
      return std::optional<Relation>{};
    }
  }
  const bool only_count_star =
      (naggs == 1 && req.agg_specs[0].kind == ColAggKind::kCountStar);

  // Only pure VarChar/mixed fixed+VarChar COUNT(*) queries currently enter Path
  // C; check fixed+non-distinct general aggs by falling back only on
  // unsupported VarChar MIN/MAX combinations if needed.
  if (!only_count_star) {
    for (const auto& as : req.agg_specs) {
      if (!IsFixedNumericAggKind(as.kind)) {
        return std::optional<Relation>{};
      }
    }
    if (naggs > 8) {
      return std::optional<Relation>{};
    }
  }

  const ColumnarStageSchedule sched = BuildColumnarStageSchedule(
      req.table_schema, req.preds, km.phys_specs, req.agg_specs);
  const size_t ncols = req.table_schema.ColumnCount();
  const size_t workers = std::max<size_t>(
      1, std::min<size_t>(
             {QueryParallelWorkerLimit(std::thread::hardware_concurrency()),
              req.candidate_blocks.size(), size_t{16}}));
  const size_t num_shards =
      req.candidate_blocks.size() <= 16
          ? 16
          : (req.candidate_blocks.size() <= 256 ? 64 : 256);
  const uint64_t shard_mask = num_shards - 1;

  std::vector<std::vector<std::vector<SlabChunk>>> shard_worker_chunks(
      num_shards);
  std::vector<std::vector<std::unique_ptr<QueryMemoryCharge>>>
      shard_worker_charges(num_shards);
  for (size_t s = 0; s < num_shards; ++s) {
    shard_worker_chunks[s].resize(workers);
    shard_worker_charges[s].resize(workers);
  }
  std::vector<std::vector<SpillSegmentId>> shard_worker_segments(
      num_shards, std::vector<SpillSegmentId>(workers, kInvalidSpillSegmentId));

  std::vector<size_t> active_agg_cols;
  for (size_t a = 0; a < naggs; ++a) {
    if (req.agg_specs[a].kind != ColAggKind::kCountStar) {
      active_agg_cols.push_back(a);
    }
  }
  const size_t n_agg_vals = active_agg_cols.size();
  const size_t init_chunk_bytes =
      req.candidate_blocks.size() <= 32 ? size_t{1024} : size_t{4096};

  const uint32_t writer_chunk_cap = static_cast<uint32_t>(
      std::max<size_t>(1, (QueryMemoryBudget::Global().Remaining() / 2) /
                              (SpillStore::kBlockSize * num_shards * workers)));

  std::atomic<size_t> next_blk{0};
  std::atomic<uint64_t> total_scanned{0};
  std::atomic<uint64_t> total_matching{0};
  std::vector<Status> p1_status(workers, Status::kSuccess);

  RunParallelTasks(workers, [&](size_t tid) {
    std::vector<WorkerShardWriter> writers(num_shards);
    for (size_t s = 0; s < num_shards; ++s) {
      if (shard0_only_attempt && s != 0) {
        continue;
      }
      shard_worker_charges[s][tid] = std::make_unique<QueryMemoryCharge>();
      writers[s].Init(&shard_worker_chunks[s][tid],
                      shard_worker_charges[s][tid].get(), init_chunk_bytes,
                      spill_store, &shard_worker_segments[s][tid],
                      writer_chunk_cap);
    }

    // Self-tuning L2-resident 2048-slot L1 combiner with 128 KB byte ring for
    // `only_count_star` queries (`Q13, Q15, Q17, Q19, Q34, Q35`).
    static constexpr size_t kL1Slots = 2048;
    static constexpr size_t kL1RingBytes = size_t{128} << 10;
    struct L1Slot {
      uint32_t gen{0};
      uint64_t hash{0};
      uint32_t str_offset{0};
      int64_t count{0};
    };
    QueryMemoryCharge l1_charge;
    bool use_l1 = only_count_star && !shard0_only_attempt;
    std::vector<L1Slot> l1_slots;
    std::vector<RuntimeKeyCell> l1_keys;
    std::vector<char> l1_ring;
    size_t l1_ring_pos = 0;
    uint32_t l1_gen = 1;
    uint64_t l1_probes = 0;
    uint64_t l1_hits = 0;

    if (use_l1) {
      const size_t need = (kL1Slots * sizeof(L1Slot)) +
                          (kL1Slots * n_phys * sizeof(RuntimeKeyCell)) +
                          kL1RingBytes;
      if (!l1_charge.TryAdd(need)) {
        use_l1 = false;
      } else {
        l1_slots.resize(kL1Slots);
        l1_keys.resize(kL1Slots * n_phys);
        l1_ring.resize(kL1RingBytes);
      }
    }

    auto emit_group_record = [&](uint64_t kh, const RuntimeKeyCell* keys,
                                 int64_t count, const uint8_t* raw_agg_info,
                                 uint8_t agg_mask, size_t agg_payload_words,
                                 const int64_t* agg_words) -> Status {
      const size_t s = static_cast<size_t>(kh) & shard_mask;
      if (shard0_only_attempt && s != 0) {
        return Status::kSuccess;
      }

      uint8_t kmask = 0;
      size_t payload =
          8 + (only_count_star ? 9 : (2 + (agg_payload_words * 8)));
      for (size_t k = 0; k < n_phys; ++k) {
        if (keys[k].is_null) {
          kmask |= static_cast<uint8_t>(1U << k);
        } else if (IsVarCharKeyKind(km.phys_specs[k].kind)) {
          payload += 4 + keys[k].str_len;
        } else {
          payload += 8;
        }
      }
      ASSIGN_OR_RETURN(uint8_t*, dst, writers[s].Reserve(payload));
      uint8_t* p = dst;
      std::memcpy(p, &kh, 8);
      p += 8;
      *p++ = kmask;
      if (only_count_star) {
        std::memcpy(p, &count, 8);
        p += 8;
      } else {
        (void)raw_agg_info;
        *p++ = agg_mask;
      }
      for (size_t k = 0; k < n_phys; ++k) {
        if (keys[k].is_null) {
          continue;
        }
        if (IsVarCharKeyKind(km.phys_specs[k].kind)) {
          const uint32_t sl = keys[k].str_len;
          std::memcpy(p, &sl, 4);
          p += 4;
          if (sl > 0) {
            std::memcpy(p, keys[k].str_ptr, sl);
            p += sl;
          }
        } else if (km.phys_specs[k].kind == ColKeyKind::kColDouble) {
          std::memcpy(p, &keys[k].double_val, 8);
          p += 8;
        } else {
          std::memcpy(p, &keys[k].int_val, 8);
          p += 8;
        }
      }
      if (!only_count_star && agg_payload_words > 0) {
        std::memcpy(p, agg_words, agg_payload_words * 8);
        p += agg_payload_words * 8;
      }
      writers[s].Commit(static_cast<size_t>(p - dst));
      return Status::kSuccess;
    };

    auto flush_l1 = [&]() -> Status {
      for (size_t i = 0; i < kL1Slots; ++i) {
        if (l1_slots[i].gen == l1_gen && l1_slots[i].count > 0) {
          RETURN_IF_FAIL(
              emit_group_record(l1_slots[i].hash, &l1_keys[i * n_phys],
                                l1_slots[i].count, nullptr, 0, 0, nullptr));
        }
      }
      ++l1_gen;
      if (l1_gen == 0) {
        std::ranges::fill(l1_slots, L1Slot{});
        l1_gen = 1;
      }
      l1_ring_pos = 0;
      return Status::kSuccess;
    };

    BlockDecodeScratch dec_scratch;
    std::vector<const TlcsFile::DecodedColumnChunk*> col_ptrs(ncols, nullptr);
    std::vector<uint32_t> cand_rows;
    std::vector<RuntimeKeyCell> probe_keys(n_phys);
    std::string regex_scratch;
    TimestampPrefixCache ts_cache;
    uint64_t local_scanned = 0;
    uint64_t local_matching = 0;
    std::array<int64_t, 8> agg_words{};

    while (true) {
      const size_t bi = next_blk.fetch_add(1, std::memory_order_relaxed);
      if (bi >= req.candidate_blocks.size()) {
        break;
      }
      const uint32_t bidx = req.candidate_blocks[bi];

      auto dec_or = DecodeAndFilterBlock(*req.tlcs, bidx, req.table_schema,
                                         req.preds, sched, &dec_scratch,
                                         &col_ptrs, &cand_rows, &local_scanned);
      if (!dec_or.HasValue()) {
        p1_status[tid] = dec_or.GetStatus();
        return;
      }
      if (!dec_or.Value()) {
        continue;
      }
      local_matching += cand_rows.size();

      for (const uint32_t r : cand_rows) {
        Status kst = Status::kSuccess;
        size_t row_str_bytes = 0;
        for (size_t k = 0; k < n_phys; ++k) {
          if (!ExtractSingleKeyCell(km.phys_specs[k], col_ptrs, r,
                                    &probe_keys[k], &regex_scratch, &ts_cache,
                                    &kst)) {
            p1_status[tid] = std::move(kst);
            return;
          }
          if (!probe_keys[k].is_null &&
              IsVarCharKeyKind(km.phys_specs[k].kind)) {
            row_str_bytes += probe_keys[k].str_len;
          }
        }
        const uint64_t kh =
            GroupKeySliceHasher::HashTyped(probe_keys.data(), km.phys_specs);

        if (shard0_only_attempt && (kh & shard_mask) != 0) {
          continue;
        }

        if (use_l1 && row_str_bytes <= kL1RingBytes / 4) {
          ++l1_probes;
          const size_t idx = static_cast<size_t>(kh) & (kL1Slots - 1);
          L1Slot& slot = l1_slots[idx];
          RuntimeKeyCell* slot_keys = &l1_keys[idx * n_phys];
          if (slot.gen == l1_gen && slot.hash == kh &&
              GroupKeySliceHasher::Equals(slot_keys, probe_keys.data(),
                                          km.phys_specs)) {
            ++slot.count;
            ++l1_hits;
            continue;
          }
          if (l1_ring_pos + row_str_bytes > kL1RingBytes) {
            Status fst = flush_l1();
            if (fst != Status::kSuccess) {
              p1_status[tid] = std::move(fst);
              return;
            }
          } else if (slot.gen == l1_gen && slot.count > 0) {
            Status est = emit_group_record(slot.hash, slot_keys, slot.count,
                                           nullptr, 0, 0, nullptr);
            if (est != Status::kSuccess) {
              p1_status[tid] = std::move(est);
              return;
            }
          }
          slot.gen = l1_gen;
          slot.hash = kh;
          slot.count = 1;
          for (size_t k = 0; k < n_phys; ++k) {
            slot_keys[k] = probe_keys[k];
            if (!probe_keys[k].is_null &&
                IsVarCharKeyKind(km.phys_specs[k].kind) &&
                probe_keys[k].str_len > 0) {
              char* rd = l1_ring.data() + l1_ring_pos;
              std::memcpy(rd, probe_keys[k].str_ptr, probe_keys[k].str_len);
              slot_keys[k].str_ptr = rd;
              l1_ring_pos += probe_keys[k].str_len;
            }
          }
          if (l1_probes == 4096 && l1_hits * 8 < l1_probes) {
            Status fst = flush_l1();
            if (fst != Status::kSuccess) {
              p1_status[tid] = std::move(fst);
              return;
            }
            use_l1 = false;
            l1_slots.clear();
            l1_keys.clear();
            l1_ring.clear();
            l1_ring.shrink_to_fit();
            l1_charge = QueryMemoryCharge{};
          }
          continue;
        }

        uint8_t agg_mask = 0;
        size_t non_null_agg_words = 0;
        if (!only_count_star) {
          for (size_t idx = 0; idx < n_agg_vals; ++idx) {
            const ColAggSpec& spec = req.agg_specs[active_agg_cols[idx]];
            const auto* ach = col_ptrs[spec.full_col];
            if (ach->has_null && ach->IsNull(r)) {
              agg_mask |= static_cast<uint8_t>(1U << idx);
            } else if (spec.kind == ColAggKind::kCountCol ||
                       spec.kind == ColAggKind::kCountByteLen) {
            } else if (spec.kind == ColAggKind::kSumByteLen ||
                       spec.kind == ColAggKind::kAvgByteLen ||
                       spec.kind == ColAggKind::kMinByteLen ||
                       spec.kind == ColAggKind::kMaxByteLen) {
              agg_words[non_null_agg_words++] =
                  static_cast<int64_t>(ach->VarcharLength(r));
            } else {
              agg_words[non_null_agg_words++] = ach->int_cells[r];
            }
          }
        }
        Status est =
            emit_group_record(kh, probe_keys.data(), 1, nullptr, agg_mask,
                              non_null_agg_words, agg_words.data());
        if (est != Status::kSuccess) {
          p1_status[tid] = std::move(est);
          return;
        }
      }
    }

    if (use_l1) {
      Status fst = flush_l1();
      if (fst != Status::kSuccess) {
        p1_status[tid] = std::move(fst);
        return;
      }
    }
    for (size_t s = 0; s < num_shards; ++s) {
      Status finst = writers[s].Finalize();
      if (finst != Status::kSuccess) {
        p1_status[tid] = std::move(finst);
        return;
      }
    }
    total_scanned.fetch_add(local_scanned, std::memory_order_relaxed);
    total_matching.fetch_add(local_matching, std::memory_order_relaxed);
  });

  for (const Status& st : p1_status) {
    if (st == StatusCode::kInvalidArgument ||
        st == StatusCode::kMemoryLimitExceeded) {
      return std::optional<Relation>{};
    }
    RETURN_IF_FAIL(st);
  }

  // Pass 2: build `WorkerGroupTable` per shard with `borrow_strings = true`
  // referencing shard `s`'s own chunks, extract materialized Top-K/survivors,
  // and immediately free shard `s`.
  const auto merge_begin = std::chrono::steady_clock::now();
  const PartOrderKind fast_order_kind =
      ClassifyPartOrderKind(req.order_bindings, req.key_specs, req.agg_specs);
  const bool bounded_topk = req.stmt.HasLimit() && !req.order_bindings.empty();
  const size_t topk_cap =
      req.stmt.HasLimit()
          ? (req.stmt.Offset() <=
                     std::numeric_limits<size_t>::max() - req.stmt.Limit()
                 ? req.stmt.Offset() + req.stmt.Limit()
                 : std::numeric_limits<size_t>::max())
          : 0;

  std::vector<std::vector<SurvivorGroupRow>> shard_survivors(num_shards);
  // Survivor rows are plain vectors outside `Relation`; keep their charge
  // alive until the merge adopts the rows (see FinalizePartitionedRelation).
  std::vector<std::unique_ptr<QueryMemoryCharge>> shard_survivor_charges(
      num_shards);
  std::vector<Status> p2_status(workers, Status::kSuccess);
  std::atomic<size_t> total_groups_out{0};
  std::atomic<size_t> next_s{0};

  const size_t active_shards = shard0_only_attempt ? 1 : num_shards;
  RunParallelTasks(std::min(workers, active_shards), [&](size_t tid) {
    std::vector<RuntimeKeyCell> probe_keys(n_phys);
    std::vector<RuntimeKeyCell> full_keys(req.key_specs.size());
    std::vector<Value> finished_aggs(naggs);
    std::vector<Value> cached_keys(req.key_specs.size());
    std::vector<Value> having_scratch(naggs);

    while (true) {
      const size_t s = next_s.fetch_add(1, std::memory_order_relaxed);
      if (s >= active_shards) {
        break;
      }

      size_t shard_bytes = 0;
      for (size_t w = 0; w < workers; ++w) {
        for (const SlabChunk& c : shard_worker_chunks[s][w]) {
          shard_bytes += c.used;
        }
      }
      if (shard_bytes > 0) {
        if (only_count_star) {
          // Dedicated POD open-addressed hash table with borrowed string
          // views, batch-charged `QueryMemoryCharge`, and zero `unique_ptr`s!
          struct MixedSlot {
            uint64_t hash{0};
            uint32_t gid_plus_one{0};
          };
          QueryMemoryCharge ht_charge;
          // The mixed-path record layout mirrors emit_group_record: an 8-byte
          // hash, a 1-byte key NULL mask, 9 bytes for the count payload, then
          // 4+len for each VARCHAR key and 8 for each fixed-width key. The old
          // /24 guess overpresized the table by ~3x for several 10-char
          // VARCHAR keys, wasting memory charged to the query budget.
          constexpr size_t kAssumedVarcharKeyBytes = 12;
          size_t mixed_rec_bytes = 8 + (only_count_star ? 9 : 2);
          for (size_t k = 0; k < n_phys; ++k) {
            mixed_rec_bytes += IsVarCharKeyKind(km.phys_specs[k].kind)
                                   ? (4 + kAssumedVarcharKeyBytes)
                                   : 8;
          }
          const size_t est_groups = std::min<size_t>(
              std::max<size_t>(
                  64, shard_bytes / std::max<size_t>(16, mixed_rec_bytes)),
              kMaxPresizedShardGroups);
          const size_t init_cap =
              std::max<size_t>(256, std::bit_ceil(est_groups * 2));
          const size_t gbytes =
              (n_phys * sizeof(RuntimeKeyCell)) + sizeof(int64_t);
          size_t reserved_group_cap = est_groups;
          if (!ht_charge.TryAdd((init_cap * sizeof(MixedSlot)) +
                                (reserved_group_cap * gbytes))) {
            p2_status[tid] =
                StatusError(StatusCode::kMemoryLimitExceeded,
                            "mixed shard count table exceeded budget");
            return;
          }
          std::vector<MixedSlot> slots(init_cap);
          std::vector<RuntimeKeyCell> keys_flat;
          std::vector<int64_t> counts_flat;
          // Group keys copied out of spilled chunks must outlive the reusable
          // scratch buffer they were read into — intern their VARCHAR bytes in
          // a deque (stable element addresses) for the shard's lifetime.
          std::deque<std::string> spilled_key_arena;
          keys_flat.reserve(reserved_group_cap * n_phys);
          counts_flat.reserve(reserved_group_cap);
          size_t ng = 0;

          auto rehash_slots = [&](size_t next_cap) -> Status {
            const size_t delta = (next_cap - slots.size()) * sizeof(MixedSlot);
            if (!ht_charge.TryAdd(delta)) {
              return StatusError(StatusCode::kMemoryLimitExceeded,
                                 "mixed shard count grow exceeded budget");
            }
            std::vector<MixedSlot> next(next_cap);
            const size_t mask = next_cap - 1;
            for (const MixedSlot& old : slots) {
              if (old.gid_plus_one == 0) {
                continue;
              }
              size_t pos = static_cast<size_t>(old.hash) & mask;
              while (next[pos].gid_plus_one != 0) {
                pos = (pos + 1) & mask;
              }
              next[pos] = old;
            }
            slots.swap(next);
            return Status::kSuccess;
          };

          for (size_t w = 0; w < workers; ++w) {
            SlabChunkReader chunk_reader(spill_store,
                                         shard_worker_segments[s][w]);
            for (const SlabChunk& c : shard_worker_chunks[s][w]) {
              const uint8_t* p = nullptr;
              Status cst = chunk_reader.Resolve(c, &p);
              if (cst != Status::kSuccess) {
                p2_status[tid] = std::move(cst);
                return;
              }
              const uint8_t* end = p + c.used;
              while (p < end) {
                uint64_t kh = 0;
                std::memcpy(&kh, p, 8);
                p += 8;
                const uint8_t kmask = *p++;
                int64_t count_val = 1;
                std::memcpy(&count_val, p, 8);
                p += 8;
                for (size_t k = 0; k < n_phys; ++k) {
                  probe_keys[k] = RuntimeKeyCell{};
                  if ((kmask & (1U << k)) != 0) {
                    probe_keys[k].is_null = true;
                  } else if (IsVarCharKeyKind(km.phys_specs[k].kind)) {
                    uint32_t sl = 0;
                    std::memcpy(&sl, p, 4);
                    p += 4;
                    probe_keys[k].str_ptr = reinterpret_cast<const char*>(p);
                    probe_keys[k].str_len = sl;
                    p += sl;
                  } else if (km.phys_specs[k].kind == ColKeyKind::kColDouble) {
                    std::memcpy(&probe_keys[k].double_val, p, 8);
                    p += 8;
                  } else {
                    std::memcpy(&probe_keys[k].int_val, p, 8);
                    p += 8;
                  }
                }
                if ((ng + 1) * 10 >= slots.size() * 7) {
                  Status rst = rehash_slots(slots.size() * 2);
                  if (rst != Status::kSuccess) {
                    p2_status[tid] = std::move(rst);
                    return;
                  }
                }
                const size_t mask = slots.size() - 1;
                size_t pos = static_cast<size_t>(kh) & mask;
                uint32_t gid = 0;
                while (true) {
                  MixedSlot& sl = slots[pos];
                  if (sl.gid_plus_one == 0) {
                    if (ng >= reserved_group_cap) {
                      const size_t next_cap = reserved_group_cap * 2;
                      if (!ht_charge.TryAdd((next_cap - reserved_group_cap) *
                                            gbytes)) {
                        p2_status[tid] = StatusError(
                            StatusCode::kMemoryLimitExceeded,
                            "mixed shard group payload exceeded budget");
                        return;
                      }
                      reserved_group_cap = next_cap;
                    }
                    gid = static_cast<uint32_t>(ng++);
                    const size_t kbase = keys_flat.size();
                    keys_flat.resize(kbase + n_phys);
                    for (size_t k = 0; k < n_phys; ++k) {
                      keys_flat[kbase + k] = probe_keys[k];
                      if (c.data == nullptr && !probe_keys[k].is_null &&
                          IsVarCharKeyKind(km.phys_specs[k].kind)) {
                        spilled_key_arena.emplace_back(probe_keys[k].StrView());
                        keys_flat[kbase + k].str_ptr =
                            spilled_key_arena.back().data();
                      }
                    }
                    counts_flat.push_back(count_val);
                    sl.hash = kh;
                    sl.gid_plus_one = gid + 1;
                    break;
                  }
                  if (sl.hash == kh) {
                    const uint32_t eg = sl.gid_plus_one - 1;
                    if (GroupKeySliceHasher::Equals(
                            &keys_flat[static_cast<size_t>(eg) * n_phys],
                            probe_keys.data(), km.phys_specs)) {
                      counts_flat[eg] += count_val;
                      gid = eg;
                      break;
                    }
                  }
                  pos = (pos + 1) & mask;
                }
              }
            }
          }

          total_groups_out.fetch_add(ng, std::memory_order_relaxed);
          auto& out_vec = shard_survivors[s];
          QueryMemoryCharge& survivor_charge =
              SurvivorChargeFor(shard_survivor_charges, s);
          const bool use_shard_heap =
              bounded_topk && topk_cap > 0 && ng > topk_cap;
          const SurvivorComparator local_cmp{
              .fast_kind = fast_order_kind,
              .order_bindings = &req.order_bindings};

          auto materialize_mixed_count_group =
              [&](size_t g) -> StatusOr<SurvivorGroupRow> {
            RuntimeAggState count_st;
            count_st.count = counts_flat[g];
            count_st.has_value = true;
            km.ReconstructFull(req.key_specs, keys_flat.data() + (g * n_phys),
                               full_keys.data());
            SurvivorGroupRow row;
            if (fast_order_kind != PartOrderKind::kNone) {
              const auto& ob0 = req.order_bindings[0].binding;
              RETURN_IF_FAIL(EvaluateNumericOrderValue(
                  ob0,
                  ob0.kind == OutputSlotBinding::Kind::kAgg
                      ? req.agg_specs.data()
                      : nullptr,
                  ob0.kind == OutputSlotBinding::Kind::kKey
                      ? &req.key_specs[ob0.index]
                      : nullptr,
                  ob0.kind == OutputSlotBinding::Kind::kAgg ? &count_st
                                                            : nullptr,
                  ob0.kind == OutputSlotBinding::Kind::kKey
                      ? &full_keys[ob0.index]
                      : nullptr,
                  &row.num_null, &row.num_int, &row.num_double));
            }
            finished_aggs[0] = Value(counts_flat[g]);
            std::fill(cached_keys.begin(), cached_keys.end(), Value());
            if (fast_order_kind == PartOrderKind::kNone &&
                !req.order_bindings.empty()) {
              row.order_keys.resize(req.order_bindings.size());
              for (size_t k = 0; k < req.order_bindings.size(); ++k) {
                ASSIGN_OR_RETURN(
                    Value, ov,
                    EvaluateBindingValue(req.order_bindings[k].binding,
                                         req.key_specs, full_keys.data(),
                                         finished_aggs, &cached_keys));
                row.order_keys[k] = std::move(ov);
              }
            }
            row.emit_vals.reserve(req.emit_bindings.size());
            for (const auto& eb : req.emit_bindings) {
              ASSIGN_OR_RETURN(
                  Value, ev,
                  EvaluateBindingValue(eb, req.key_specs, full_keys.data(),
                                       finished_aggs, &cached_keys));
              row.emit_vals.push_back(std::move(ev));
            }
            return row;
          };

          if (use_shard_heap && fast_order_kind == PartOrderKind::kInt &&
              req.positional_having.empty() &&
              req.order_bindings[0].binding.kind ==
                  OutputSlotBinding::Kind::kAgg) {
            const bool asc = req.order_bindings[0].order.Ascending();
            struct WinCount {
              int64_t cnt{0};
              size_t gid{0};
            };
            auto wcmp = [asc](const WinCount& a, const WinCount& b) noexcept {
              return asc ? (a.cnt < b.cnt) : (a.cnt > b.cnt);
            };
            std::vector<WinCount> winners;
            winners.reserve(topk_cap + 1);
            for (size_t g = 0; g < ng; ++g) {
              const int64_t cnt = counts_flat[g];
              WinCount wc{.cnt = cnt, .gid = g};
              if (winners.size() < topk_cap) {
                winners.push_back(wc);
                std::push_heap(winners.begin(), winners.end(), wcmp);
              } else if (wcmp(wc, winners.front())) {
                std::pop_heap(winners.begin(), winners.end(), wcmp);
                winners.back() = wc;
                std::push_heap(winners.begin(), winners.end(), wcmp);
              }
            }
            out_vec.reserve(winners.size());
            for (const WinCount& wc : winners) {
              auto row_or = materialize_mixed_count_group(wc.gid);
              if (!row_or.HasValue()) {
                p2_status[tid] = row_or.GetStatus();
                return;
              }
              PushChargedSurvivor(&out_vec, &survivor_charge,
                                  std::move(row_or.Value()));
            }
            shard_worker_chunks[s].clear();
            shard_worker_charges[s].clear();
            for (const SpillSegmentId seg : shard_worker_segments[s]) {
              spill_store->DropSegment(seg);
            }
            continue;
          }

          for (size_t g = 0; g < ng; ++g) {
            RuntimeAggState count_st;
            count_st.count = counts_flat[g];
            count_st.has_value = true;
            auto h_or = EvalPositionalHavingOnStates(req.positional_having,
                                                     req.agg_specs, &count_st,
                                                     &having_scratch);
            if (!h_or.HasValue()) {
              p2_status[tid] = h_or.GetStatus();
              return;
            }
            if (!h_or.Value()) {
              continue;
            }
            if (bounded_topk && topk_cap == 0) {
              continue;
            }

            if (use_shard_heap && fast_order_kind == PartOrderKind::kInt &&
                req.order_bindings[0].binding.kind ==
                    OutputSlotBinding::Kind::kAgg) {
              SurvivorGroupRow probe_dummy;
              probe_dummy.num_int = counts_flat[g];
              if (out_vec.size() >= topk_cap &&
                  !local_cmp(probe_dummy, out_vec.front())) {
                continue;
              }
            }

            auto row_or = materialize_mixed_count_group(g);
            if (!row_or.HasValue()) {
              p2_status[tid] = row_or.GetStatus();
              return;
            }
            SurvivorGroupRow row = std::move(row_or.Value());
            if (use_shard_heap) {
              if (out_vec.size() < topk_cap) {
                PushChargedSurvivor(&out_vec, &survivor_charge, std::move(row));
                std::push_heap(out_vec.begin(), out_vec.end(), local_cmp);
              } else if (local_cmp(row, out_vec.front())) {
                std::pop_heap(out_vec.begin(), out_vec.end(), local_cmp);
                survivor_charge.Release(
                    EstimateSurvivorGroupRowBytes(out_vec.back()));
                PushChargedSurvivor(&out_vec, &survivor_charge, std::move(row));
                std::push_heap(out_vec.begin(), out_vec.end(), local_cmp);
              }
            } else {
              PushChargedSurvivor(&out_vec, &survivor_charge, std::move(row));
              if (req.order_bindings.empty() && req.stmt.HasLimit() &&
                  out_vec.size() >= topk_cap) {
                break;
              }
            }
          }
          shard_worker_chunks[s].clear();
          shard_worker_charges[s].clear();
          for (const SpillSegmentId seg : shard_worker_segments[s]) {
            spill_store->DropSegment(seg);
          }
          continue;
        }

        QueryMemoryCharge ht_charge;
        WorkerGroupTable wt(&km.phys_specs, &req.agg_specs, &ht_charge);
        Status rst = wt.Reserve(std::min<size_t>(
            std::max<size_t>(64, shard_bytes / 96), kMaxPresizedShardGroups));
        if (rst != Status::kSuccess) {
          p2_status[tid] = std::move(rst);
          return;
        }

        for (size_t w = 0; w < workers; ++w) {
          SlabChunkReader chunk_reader(spill_store,
                                       shard_worker_segments[s][w]);
          for (const SlabChunk& c : shard_worker_chunks[s][w]) {
            const uint8_t* p = nullptr;
            Status cst = chunk_reader.Resolve(c, &p);
            if (cst != Status::kSuccess) {
              p2_status[tid] = std::move(cst);
              return;
            }
            const uint8_t* end = p + c.used;
            while (p < end) {
              uint64_t kh = 0;
              std::memcpy(&kh, p, 8);
              p += 8;
              const uint8_t kmask = *p++;
              int64_t count_val = 1;
              uint8_t amask = 0;
              if (only_count_star) {
                std::memcpy(&count_val, p, 8);
                p += 8;
              } else {
                amask = *p++;
              }
              for (size_t k = 0; k < n_phys; ++k) {
                probe_keys[k] = RuntimeKeyCell{};
                if ((kmask & (1U << k)) != 0) {
                  probe_keys[k].is_null = true;
                } else if (IsVarCharKeyKind(km.phys_specs[k].kind)) {
                  uint32_t sl = 0;
                  std::memcpy(&sl, p, 4);
                  p += 4;
                  probe_keys[k].str_ptr = reinterpret_cast<const char*>(p);
                  probe_keys[k].str_len = sl;
                  p += sl;
                } else if (km.phys_specs[k].kind == ColKeyKind::kColDouble) {
                  std::memcpy(&probe_keys[k].double_val, p, 8);
                  p += 8;
                } else {
                  std::memcpy(&probe_keys[k].int_val, p, 8);
                  p += 8;
                }
              }
              // Same borrowed-pointer rule as the distinct path: spilled
              // chunks resolve into reusable scratch, so their VARCHAR cells
              // must be interned into the table arena rather than borrowed.
              auto gid_or =
                  wt.FindOrInsert(probe_keys.data(), kh,
                                  /*borrow_strings=*/c.data != nullptr);
              if (!gid_or.HasValue()) {
                p2_status[tid] = gid_or.GetStatus();
                return;
              }
              RuntimeAggState* ast = wt.AggsAt(gid_or.Value());
              if (only_count_star) {
                ast[0].count += count_val;
                ast[0].has_value = true;
              } else {
                size_t aidx_word = 0;
                for (size_t a = 0; a < naggs; ++a) {
                  const ColAggSpec& spec = req.agg_specs[a];
                  RuntimeAggState& st = ast[a];
                  if (spec.kind == ColAggKind::kCountStar) {
                    ++st.count;
                    st.has_value = true;
                    continue;
                  }
                  const bool is_null = (amask & (1U << aidx_word)) != 0;
                  ++aidx_word;
                  if (is_null) {
                    continue;
                  }
                  if (spec.kind == ColAggKind::kCountCol ||
                      spec.kind == ColAggKind::kCountByteLen) {
                    ++st.count;
                    st.has_value = true;
                  } else {
                    int64_t vi = 0;
                    std::memcpy(&vi, p, 8);
                    p += 8;
                    if (spec.kind == ColAggKind::kSumInt64) {
                      int64_t nx = 0;
                      if (__builtin_add_overflow(st.int_val, vi, &nx)) {
                        p2_status[tid] = StatusError(StatusCode::kIsInfinity,
                                                     "integer overflow on SUM");
                        return;
                      }
                      st.int_val = nx;
                      st.has_value = true;
                    } else if (spec.kind == ColAggKind::kSumByteLen) {
                      st.int_val += vi;
                      st.has_value = true;
                    } else if (spec.kind == ColAggKind::kAvgByteLen) {
                      st.double_val += static_cast<double>(vi);
                      ++st.count;
                      st.has_value = true;
                    } else if (spec.kind == ColAggKind::kAvgDouble) {
                      const bool is_double_col =
                          req.table_schema.GetColumn(spec.full_col).Type() ==
                          ValueType::kDouble;
                      const double d = is_double_col ? std::bit_cast<double>(vi)
                                                     : static_cast<double>(vi);
                      st.double_val += d;
                      ++st.count;
                      st.has_value = true;
                    } else if (spec.kind == ColAggKind::kSumDouble) {
                      st.double_val += std::bit_cast<double>(vi);
                      st.has_value = true;
                    } else if (spec.kind == ColAggKind::kMinInt64 ||
                               spec.kind == ColAggKind::kMinDate ||
                               spec.kind == ColAggKind::kMinByteLen) {
                      if (!st.has_value || vi < st.int_val) {
                        st.int_val = vi;
                        st.has_value = true;
                      }
                    } else if (spec.kind == ColAggKind::kMaxInt64 ||
                               spec.kind == ColAggKind::kMaxDate ||
                               spec.kind == ColAggKind::kMaxByteLen) {
                      if (!st.has_value || vi > st.int_val) {
                        st.int_val = vi;
                        st.has_value = true;
                      }
                    } else if (spec.kind == ColAggKind::kMinDouble) {
                      const auto d = std::bit_cast<double>(vi);
                      if (!st.has_value ||
                          CompareForOrderBy(Value(d), Value(st.double_val)) <
                              0) {
                        st.double_val = d;
                        st.has_value = true;
                      }
                    } else if (spec.kind == ColAggKind::kMaxDouble) {
                      const auto d = std::bit_cast<double>(vi);
                      if (!st.has_value ||
                          CompareForOrderBy(Value(d), Value(st.double_val)) >
                              0) {
                        st.double_val = d;
                        st.has_value = true;
                      }
                    }
                  }
                }
              }
            }
          }
        }

        const size_t ng = wt.NumGroups();
        total_groups_out.fetch_add(ng, std::memory_order_relaxed);
        auto& out_vec = shard_survivors[s];
        QueryMemoryCharge& survivor_charge =
            SurvivorChargeFor(shard_survivor_charges, s);
        const bool use_shard_heap =
            bounded_topk && topk_cap > 0 && ng > topk_cap;
        const SurvivorComparator local_cmp{
            .fast_kind = fast_order_kind,
            .order_bindings = &req.order_bindings};

        for (size_t g = 0; g < ng; ++g) {
          const RuntimeAggState* ast = wt.AggsAt(g);
          auto h_or = EvalPositionalHavingOnStates(
              req.positional_having, req.agg_specs, ast, &having_scratch);
          if (!h_or.HasValue()) {
            p2_status[tid] = h_or.GetStatus();
            return;
          }
          if (!h_or.Value()) {
            continue;
          }
          if (bounded_topk && topk_cap == 0) {
            continue;
          }

          km.ReconstructFull(req.key_specs, wt.KeysAt(g), full_keys.data());
          SurvivorGroupRow row;
          if (fast_order_kind != PartOrderKind::kNone) {
            const auto& ob0 = req.order_bindings[0].binding;
            Status nst = EvaluateNumericOrderValue(
                ob0,
                ob0.kind == OutputSlotBinding::Kind::kAgg
                    ? &req.agg_specs[ob0.index]
                    : nullptr,
                ob0.kind == OutputSlotBinding::Kind::kKey
                    ? &req.key_specs[ob0.index]
                    : nullptr,
                ob0.kind == OutputSlotBinding::Kind::kAgg ? &ast[ob0.index]
                                                          : nullptr,
                ob0.kind == OutputSlotBinding::Kind::kKey
                    ? &full_keys[ob0.index]
                    : nullptr,
                &row.num_null, &row.num_int, &row.num_double);
            if (nst != Status::kSuccess) {
              p2_status[tid] = std::move(nst);
              return;
            }
            if (use_shard_heap && out_vec.size() >= topk_cap &&
                !local_cmp(row, out_vec.front())) {
              continue;
            }
          }

          for (size_t a = 0; a < naggs; ++a) {
            finished_aggs[a] = FinishAggState(req.agg_specs[a], ast[a]);
          }
          std::fill(cached_keys.begin(), cached_keys.end(), Value());
          if (fast_order_kind == PartOrderKind::kNone &&
              !req.order_bindings.empty()) {
            row.order_keys.resize(req.order_bindings.size());
            for (size_t k = 0; k < req.order_bindings.size(); ++k) {
              auto ov_or = EvaluateBindingValue(req.order_bindings[k].binding,
                                                req.key_specs, full_keys.data(),
                                                finished_aggs, &cached_keys);
              if (!ov_or.HasValue()) {
                p2_status[tid] = ov_or.GetStatus();
                return;
              }
              row.order_keys[k] = std::move(ov_or.Value());
            }
            if (use_shard_heap && out_vec.size() >= topk_cap &&
                !local_cmp(row, out_vec.front())) {
              continue;
            }
          }
          row.emit_vals.reserve(req.emit_bindings.size());
          for (const auto& eb : req.emit_bindings) {
            auto ev_or =
                EvaluateBindingValue(eb, req.key_specs, full_keys.data(),
                                     finished_aggs, &cached_keys);
            if (!ev_or.HasValue()) {
              p2_status[tid] = ev_or.GetStatus();
              return;
            }
            row.emit_vals.push_back(std::move(ev_or.Value()));
          }

          if (use_shard_heap) {
            if (out_vec.size() < topk_cap) {
              PushChargedSurvivor(&out_vec, &survivor_charge, std::move(row));
              std::push_heap(out_vec.begin(), out_vec.end(), local_cmp);
            } else {
              std::pop_heap(out_vec.begin(), out_vec.end(), local_cmp);
              survivor_charge.Release(
                  EstimateSurvivorGroupRowBytes(out_vec.back()));
              PushChargedSurvivor(&out_vec, &survivor_charge, std::move(row));
              std::push_heap(out_vec.begin(), out_vec.end(), local_cmp);
            }
          } else {
            PushChargedSurvivor(&out_vec, &survivor_charge, std::move(row));
            if (req.order_bindings.empty() && req.stmt.HasLimit() &&
                out_vec.size() >= topk_cap) {
              break;
            }
          }
        }
      }
      shard_worker_chunks[s].clear();
      shard_worker_charges[s].clear();
      for (const SpillSegmentId seg : shard_worker_segments[s]) {
        spill_store->DropSegment(seg);
      }
    }
  });

  for (const Status& st : p2_status) {
    if (st == StatusCode::kMemoryLimitExceeded) {
      return std::optional<Relation>{};
    }
    RETURN_IF_FAIL(st);
  }

  if (shard0_only_attempt && shard_survivors[0].size() < topk_cap) {
    return std::optional<Relation>{};
  }

  return FinalizePartitionedRelation(
      req, shard_survivors, total_groups_out.load(), total_scanned.load(),
      total_matching.load(), merge_begin,
      shard0_only_attempt ? "mixed-shard0" : "mixed-sharded",
      &shard_survivor_charges);
}

}  // namespace

StatusOr<std::optional<Relation>> TryRunPartitionedColumnarTlcsAggregate(
    const PartitionedColumnarAggRequest& req) {
  if (req.candidate_blocks.empty() || req.key_specs.empty()) {
    return std::optional<Relation>{};
  }
  ASSIGN_OR_RETURN(
      KeyProjectionMap, km,
      BuildKeyProjectionMap(*req.tlcs, req.candidate_blocks, req.key_specs));
  if (km.phys_specs.empty()) {
    // Only constant keys (`GROUP BY 1`), handled cleanly by non-partitioned
    // small path.
    return std::optional<Relation>{};
  }

  // One lazily-opened unlinked backing file shared by every Pass-1 shard
  // writer below: once the query memory budget refuses a new slab chunk, the
  // writer spills its oldest resident chunk into its own segment and keeps
  // going instead of abandoning the whole fast path (ADR 0005).
  SpillStore spill_store;

  if (req.any_scalar_distinct) {
    return ExecutePartitionedDistinct(req, km, &spill_store);
  }

  bool all_fixed_keys = true;
  for (const auto& ks : km.phys_specs) {
    if (!IsFixedNumericKeyKind(ks.kind)) {
      all_fixed_keys = false;
      break;
    }
  }
  bool all_fixed_aggs = true;
  for (const auto& as : req.agg_specs) {
    if (!IsFixedNumericAggKind(as.kind)) {
      all_fixed_aggs = false;
      break;
    }
  }
  if (all_fixed_keys && all_fixed_aggs) {
    return ExecutePartitionedFixedKeys(req, km, &spill_store);
  }

  // Unordered LIMIT early-stop via Shard-0 capture when safe (`Q18`).
  if (req.order_bindings.empty() && req.positional_having.empty() &&
      req.stmt.HasLimit() && req.candidate_blocks.size() > 64 &&
      req.stmt.Offset() + req.stmt.Limit() <= 1024 && req.stmt.Limit() > 0) {
    ASSIGN_OR_RETURN(std::optional<Relation>, s0_res,
                     ExecutePartitionedMixedKeysInternal(
                         req, km, /*shard0_only_attempt=*/true, &spill_store));
    if (s0_res.has_value()) {
      return s0_res;
    }
  }

  return ExecutePartitionedMixedKeysInternal(req, km,
                                             /*shard0_only_attempt=*/false,
                                             &spill_store);
}

}  // namespace tinylamb::relational_detail
