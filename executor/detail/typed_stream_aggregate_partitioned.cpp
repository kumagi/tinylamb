/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "common/platform/process.hpp"
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
#include <ranges>
#include <span>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/parallel_work.hpp"
#include "common/query_parallelism.hpp"
#include "common/spill/spill_store.hpp"
#include "common/status_or.hpp"
#include "executor/detail/analytics_factorized_join.hpp"
#include "executor/detail/charged_vector.hpp"
#include "executor/detail/columnar_tlcs_fast_path.hpp"
#include "executor/detail/columnar_tlcs_internal.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/detail/planning_heuristics.hpp"
#include "executor/query_memory.hpp"
#include "expression/collation.hpp"
#include "table/tlcs_file.hpp"
#include "type/collation.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb::relational_detail {
namespace {

void PrefetchBitmapWord(const uint64_t* word) {
#if defined(__GNUC__) || defined(__clang__)
  __builtin_prefetch(word, 0, 1);
#else
  (void)word;
#endif
}

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

// A checked SUM(INT64) is order-sensitive: whether the running sum wraps
// depends on the sequence a group's rows reach the accumulator in, and the
// partitioned kernels fold in row order only when Pass 1 runs a single worker
// (with the dynamic block hand-off, the worker-major merge of the per-shard
// buffers is an arbitrary row order otherwise). Keep plans with a checked SUM
// on one worker; COUNT/MIN/MAX plans stay parallel. Same contract as the
// typed-stream aggregate gate and the non-partitioned TLCS grouped gate.
inline bool HasCheckedIntSum(const std::vector<ColAggSpec>& agg_specs) {
  return std::any_of(
      agg_specs.begin(), agg_specs.end(),
      [](const ColAggSpec& s) { return s.kind == ColAggKind::kSumInt64; });
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

// Single-owner append chunk stream for one `(worker, shard)` bucket. Chunks
// grow 4 KB -> 16 KB -> 64 KB lazily and bill exclusively to one
// `QueryMemoryCharge` per `(shard, worker)` so freeing shard `s` returns all
// memory via RAII.
struct SlabChunk {
  // `make_unique_for_overwrite` default-initializes: records are memcpy'd in
  // right after, so the vector's zero-fill would double every byte written.
  // A null `data` means the chunk was spilled: `used` bytes live at
  // `spill_offset` inside the owning writer's SpillStore segment.
  // NOLINTNEXTLINE(modernize-avoid-c-arrays) runtime-sized buffer
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
    // NOLINTNEXTLINE(modernize-avoid-c-arrays) runtime-sized buffer
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
// helper for materialized row survivors.
enum class PartOrderKind : uint8_t { kNone, kInt, kDouble };

PartOrderKind ClassifyPartOrderKind(
    const std::vector<ColumnarOrderBinding>& order_bindings,
    const std::vector<ColGroupKeySpec>& key_specs,
    const std::vector<ColAggSpec>& agg_specs) noexcept {
  if (order_bindings.size() != 1 ||
      order_bindings[0].order.KeyCollation() == Collation::kCaseInsensitive) {
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
      case ColKeyKind::kUtcTimestampTrunc:
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

void PushChargedSurvivor(std::vector<SurvivorGroupRow>* rows,
                         ChunkedCharge* charge, SurvivorGroupRow row) {
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
        *out_is_null = !st.has_value;
        // P603: finite inputs that overflowed the running sum raise, same
        // policy as the canonical accumulator and FinishAggState.
        if (st.has_value && !st.saw_inf && std::isinf(st.double_val)) {
          return StatusError(StatusCode::kIsInfinity,
                             "SUM overflow: result is infinite");
        }
        *out_double = st.double_val;
        return Status::kSuccess;
      case ColAggKind::kMinDouble:
      case ColAggKind::kMaxDouble:
        *out_is_null = !st.has_value;
        // GoogleSQL: any NaN in the group makes MIN/MAX NaN.
        *out_double = st.saw_nan ? std::numeric_limits<double>::quiet_NaN()
                                 : st.double_val;
        return Status::kSuccess;
      default: {
        *out_is_null = false;
        StatusOr<Value> finished = FinishAggState(*agg_spec, st);
        if (!finished.HasValue()) {
          return finished.GetStatus();
        }
        *out_int = finished.Value().value.int_value;
        return Status::kSuccess;
      }
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
    } else if (
        ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_RADIX_SORT") == nullptr &&
        fast_order_kind == PartOrderKind::kInt &&
        req.order_bindings.size() == 1 && survivors.size() >= 1024 &&
        // Lean proof:
        // lean/TinylambOpt/ResearchProofs/SortRadixKeyPartitionSoundness.lean
        // (sound)
        TryStableRadixSortNullableInt64(
            survivors, req.order_bindings[0].order.Ascending(),
            req.order_bindings[0].order.NullsFirst(),
            [](const SurvivorGroupRow& r) noexcept { return r.num_null; },
            [](const SurvivorGroupRow& r) noexcept { return r.num_int; })) {
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

  if (::tinylamb::platform::GetEnv("TINYLAMB_DEBUG_TLCS") != nullptr) {
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
  non_distinct_idxs.reserve(req.agg_specs.size());
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
    RegexReplaceCache regex_cache;
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
                                    &kst, &regex_cache)) {
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
  const size_t workers =
      HasCheckedIntSum(req.agg_specs)
          ? size_t{1}
          : std::max<size_t>(
                1, std::min<size_t>({QueryParallelWorkerLimit(
                                         std::thread::hardware_concurrency()),
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

    RETURN_IF_FAIL(RunParallelTasks(workers, [&](size_t tid) {
      std::vector<WorkerShardWriter> writers(num_shards);
      for (size_t s = 0; s < num_shards; ++s) {
        shard_worker_charges[s][tid] = std::make_unique<QueryMemoryCharge>();
        Status init_status = writers[s].Init(
            &shard_worker_chunks[s][tid], shard_worker_charges[s][tid].get(),
            init_chunk_bytes, spill_store, &shard_worker_segments[s][tid],
            writer_chunk_cap);
        if (!init_status.ok()) {
          worker_status[tid] = std::move(init_status);
          return;
        }
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
    }));

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

      RETURN_IF_FAIL(
          RunParallelTasks(std::min(workers, num_shards), [&](size_t tid) {
            std::unordered_map<uint32_t, uint64_t>& my_counts =
                thread_counts[tid];
            ChunkedCharge my_charge(&count_charges[tid]);
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
                    std::max<size_t>(128,
                                     shard_bytes / sizeof(LocalPairRecord)),
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
          }));

      for (const Status& st : p2_status) {
        if (st == StatusCode::kMemoryLimitExceeded) {
          return std::optional<Relation>{};
        }
        RETURN_IF_FAIL(st);
      }

      QueryMemoryCharge distinct_charge;
      std::vector<uint64_t> global_distinct;
      if (!ReserveChargedVector(global_distinct, distinct_charge,
                                total_groups)) {
        return std::optional<Relation>{};
      }
      global_distinct.resize(total_groups, 0);
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
        dstate.distinct_count_final = true;
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
            ASSIGN_OR_RETURN(
                Value, finished_value,
                (FinishAggState(req.agg_specs[a], full_agg_states[a])));
            finished_aggs[a] = std::move(finished_value);
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

  RETURN_IF_FAIL(RunParallelTasks(workers, [&](size_t tid) {
    std::vector<WorkerShardWriter> writers(num_shards);
    for (size_t s = 0; s < num_shards; ++s) {
      shard_worker_charges[s][tid] = std::make_unique<QueryMemoryCharge>();
      Status init_status = writers[s].Init(
          &shard_worker_chunks[s][tid], shard_worker_charges[s][tid].get(),
          init_chunk_bytes, spill_store, &shard_worker_segments[s][tid],
          writer_chunk_cap);
      if (!init_status.ok()) {
        p1_status[tid] = std::move(init_status);
        return;
      }
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
  }));

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

  RETURN_IF_FAIL(RunParallelTasks(std::min(workers, num_shards), [&](size_t
                                                                         tid) {
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
    QueryMemoryCharge pairs_charge;
    std::vector<TempPair> pairs;
    QueryMemoryCharge counts_charge;
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
        if (!ReserveChargedVector(pairs, pairs_charge, shard_bytes / 24)) {
          p2_status[tid] = StatusError(StatusCode::kMemoryLimitExceeded,
                                       "distinct pairs exceeded budget");
          return;
        }
        // Records of one group arrive back to back when the scan is
        // clustered on the group key (lineitem by l_orderkey: ~4 per order,
        // all hashed to this shard in scan order), so the previous record's
        // group answers most lookups without touching the hash table.
        uint32_t last_gid = std::numeric_limits<uint32_t>::max();
        uint64_t last_kh = 0;

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
              uint32_t gid = 0;
              if (last_gid != std::numeric_limits<uint32_t>::max() &&
                  kh == last_kh &&
                  GroupKeySliceHasher::Equals(
                      wt.KeysAt(last_gid), probe_keys.data(), km.phys_specs)) {
                gid = last_gid;
              } else {
                auto gid_or =
                    wt.FindOrInsert(probe_keys.data(), kh,
                                    /*borrow_strings=*/c.data != nullptr);
                if (!gid_or.HasValue()) {
                  p2_status[tid] = gid_or.GetStatus();
                  return;
                }
                gid = gid_or.Value();
                last_gid = gid;
                last_kh = kh;
              }
              if (has_dist) {
                if (pairs.size() == pairs.capacity()) {
                  const size_t next =
                      pairs.capacity() == 0 ? 1 : pairs.capacity() * 2;
                  if (next <= pairs.capacity() ||
                      !ReserveChargedVector(pairs, pairs_charge, next)) {
                    p2_status[tid] =
                        StatusError(StatusCode::kMemoryLimitExceeded,
                                    "distinct pairs exceeded budget");
                    return;
                  }
                }
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
                    if (std::isinf(d)) {
                      st.saw_inf = true;  // P603 infinity provenance
                    }
                    st.double_val += d;
                    st.has_value = true;
                  } else if (ex.kind == ColAggKind::kMinDouble) {
                    if (std::isnan(d)) {
                      st.saw_nan = true;
                    }
                    if (!st.has_value ||
                        CompareForOrderBy(Value(d), Value(st.double_val)) < 0) {
                      st.double_val = d;
                      st.has_value = true;
                    }
                  } else {
                    if (std::isnan(d)) {
                      st.saw_nan = true;
                    }
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
        if (!ReserveChargedVector(distinct_counts, counts_charge, ng)) {
          p2_status[tid] = StatusError(StatusCode::kMemoryLimitExceeded,
                                       "distinct counts exceeded budget");
          return;
        }
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
        ChunkedCharge survivor_batch(&survivor_charge);
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
          dstate.distinct_count_final = true;
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
              StatusOr<Value> finished_value =
                  FinishAggState(req.agg_specs[a], full_agg_states[a]);
              if (!finished_value.HasValue()) {
                p2_status[tid] = finished_value.GetStatus();
                return;
              }
              finished_aggs[a] = finished_value.MoveValue();
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
              PushChargedSurvivor(&out_vec, &survivor_batch, std::move(row));
              std::push_heap(out_vec.begin(), out_vec.end(), local_cmp);
            } else {
              std::pop_heap(out_vec.begin(), out_vec.end(), local_cmp);
              survivor_charge.Release(
                  EstimateSurvivorGroupRowBytes(out_vec.back()));
              PushChargedSurvivor(&out_vec, &survivor_batch, std::move(row));
              std::push_heap(out_vec.begin(), out_vec.end(), local_cmp);
            }
          } else {
            PushChargedSurvivor(&out_vec, &survivor_batch, std::move(row));
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
  }));

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
  // GoogleSQL MIN/MAX NaN poisoning (see RuntimeAggState::saw_nan).
  bool saw_nan{false};
  // P603 infinity provenance for kSumDouble (see RuntimeAggState::saw_inf).
  bool saw_inf{false};
};

inline Status MergeNumericRawValue(const ColAggSpec& spec, bool val_is_null,
                                   int64_t val_i, double val_d,
                                   int64_t multiplicity, NumericAggCell& st) {
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
      if (spec.kind == ColAggKind::kSumDouble && std::isinf(val_d)) {
        st.saw_inf = true;  // P603 infinity provenance
      }
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
      if (std::isnan(val_d)) {
        st.saw_nan = true;
      }
      const auto cur = std::bit_cast<double>(st.val_bits);
      if (st.cnt == 0 || CompareForOrderBy(Value(val_d), Value(cur)) < 0) {
        st.val_bits = std::bit_cast<int64_t>(val_d);
      }
      st.cnt += multiplicity;
      break;
    }
    case ColAggKind::kMaxDouble: {
      if (std::isnan(val_d)) {
        st.saw_nan = true;
      }
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
  st.saw_nan = nc.saw_nan;
  st.saw_inf = nc.saw_inf;
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
  const size_t workers =
      HasCheckedIntSum(req.agg_specs)
          ? size_t{1}
          : std::max<size_t>(
                1, std::min<size_t>({QueryParallelWorkerLimit(
                                         std::thread::hardware_concurrency()),
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
  active_agg_cols.reserve(naggs);
  for (size_t a = 0; a < naggs; ++a) {
    if (req.agg_specs[a].kind != ColAggKind::kCountStar) {
      active_agg_cols.push_back(a);
    }
  }
  const size_t n_agg_vals = active_agg_cols.size();

  RETURN_IF_FAIL(RunParallelTasks(workers, [&](size_t tid) {
    std::vector<WorkerShardWriter> writers(num_shards);
    for (size_t s = 0; s < num_shards; ++s) {
      shard_worker_charges[s][tid] = std::make_unique<QueryMemoryCharge>();
      Status init_status = writers[s].Init(
          &shard_worker_chunks[s][tid], shard_worker_charges[s][tid].get(),
          init_chunk_bytes, spill_store, &shard_worker_segments[s][tid],
          writer_chunk_cap);
      if (!init_status.ok()) {
        p1_status[tid] = std::move(init_status);
        return;
      }
    }

    // Compact self-tuning L1 combiner for single-key COUNT(*) (`Q16, Q36`)
    // that auto-disables on low hit rates (`< 6.25%`) so unique WatchIDs bypass
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
        if (direct_cells && kch == nullptr) {
          p1_status[tid] = StatusError(StatusCode::kCorrupt,
                                       "missing decoded group key column");
          return;
        }
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
                                      probe_keys.data(), &regex_scratch,
                                      &ts_cache, &kst)) {
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
            if (l1_probes >= 8192 && l1_hits * 16 < l1_probes) {
              // P625: `==` let a worker whose threshold-crossing probe was a
              // hit (which `continue`s above) skip self-disable forever.
              // Re-evaluate on every miss once the floor is reached; a
              // stream whose locality recovers keeps L1 because l1_hits
              // grows with the hits.
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
  }));

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

  RETURN_IF_FAIL(RunParallelTasks(std::min(workers, num_shards), [&](size_t
                                                                         tid) {
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
            FixedSlot& slot = slots[pos];
            if (slot.gid_plus_one == 0) {
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
              slot.hash = kh;
              slot.gid_plus_one = gid + 1;
              return gid;
            }
            if (slot.hash == kh) {
              const uint32_t eg = slot.gid_plus_one - 1;
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
        ChunkedCharge survivor_batch(&survivor_charge);
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
            ASSIGN_OR_RETURN(Value, finished_value,
                             (FinishAggState(req.agg_specs[a], full_aggs[a])));
            finished_aggs[a] = std::move(finished_value);
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
                    w.dv =
                        nc.saw_nan && (oagg->kind == ColAggKind::kMinDouble ||
                                       oagg->kind == ColAggKind::kMaxDouble)
                            ? std::numeric_limits<double>::quiet_NaN()
                            : std::bit_cast<double>(nc.val_bits);
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
              PushChargedSurvivor(&out_vec, &survivor_batch,
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
            StatusOr<Value> finished_value =
                FinishAggState(req.agg_specs[a], full_aggs[a]);
            if (!finished_value.HasValue()) {
              p2_status[tid] = finished_value.GetStatus();
              return;
            }
            finished_aggs[a] = finished_value.MoveValue();
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
  }));

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
  const size_t workers =
      HasCheckedIntSum(req.agg_specs)
          ? size_t{1}
          : std::max<size_t>(
                1, std::min<size_t>({QueryParallelWorkerLimit(
                                         std::thread::hardware_concurrency()),
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
  active_agg_cols.reserve(naggs);
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

  RETURN_IF_FAIL(RunParallelTasks(workers, [&](size_t tid) {
    std::vector<WorkerShardWriter> writers(num_shards);
    for (size_t s = 0; s < num_shards; ++s) {
      if (shard0_only_attempt && s != 0) {
        continue;
      }
      shard_worker_charges[s][tid] = std::make_unique<QueryMemoryCharge>();
      Status init_status = writers[s].Init(
          &shard_worker_chunks[s][tid], shard_worker_charges[s][tid].get(),
          init_chunk_bytes, spill_store, &shard_worker_segments[s][tid],
          writer_chunk_cap);
      if (!init_status.ok()) {
        p1_status[tid] = std::move(init_status);
        return;
      }
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
    RegexReplaceCache regex_cache;
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
                                    &kst, &regex_cache)) {
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
          if (l1_probes >= 4096 && l1_hits * 8 < l1_probes) {
            // P625: same `==`/hit-`continue` trap as PATH B; re-evaluate on
            // every miss past the floor instead.
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
  }));

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
  RETURN_IF_FAIL(
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
              // views, batch-charged `QueryMemoryCharge`, and zero
              // `unique_ptr`s!
              struct MixedSlot {
                uint64_t hash{0};
                uint32_t gid_plus_one{0};
              };
              QueryMemoryCharge ht_charge;
              // The mixed-path record layout mirrors emit_group_record: an
              // 8-byte hash, a 1-byte key NULL mask, 9 bytes for the count
              // payload, then 4+len for each VARCHAR key and 8 for each
              // fixed-width key. The old /24 guess oversized the table by ~3x
              // for several 10-char VARCHAR keys, wasting memory charged to the
              // query budget.
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
              // Group keys copied out of spilled chunks must outlive the
              // reusable scratch buffer they were read into — intern their
              // VARCHAR bytes in a deque (stable element addresses) for the
              // shard's lifetime.
              std::deque<std::string> spilled_key_arena;
              keys_flat.reserve(reserved_group_cap * n_phys);
              counts_flat.reserve(reserved_group_cap);
              size_t ng = 0;

              auto rehash_slots = [&](size_t next_cap) -> Status {
                const size_t delta =
                    (next_cap - slots.size()) * sizeof(MixedSlot);
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
                        probe_keys[k].str_ptr =
                            reinterpret_cast<const char*>(p);
                        probe_keys[k].str_len = sl;
                        p += sl;
                      } else if (km.phys_specs[k].kind ==
                                 ColKeyKind::kColDouble) {
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
                      MixedSlot& slot = slots[pos];
                      if (slot.gid_plus_one == 0) {
                        if (ng >= reserved_group_cap) {
                          const size_t next_cap = reserved_group_cap * 2;
                          if (!ht_charge.TryAdd(
                                  (next_cap - reserved_group_cap) * gbytes)) {
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
                            spilled_key_arena.emplace_back(
                                probe_keys[k].StrView());
                            keys_flat[kbase + k].str_ptr =
                                spilled_key_arena.back().data();
                          }
                        }
                        counts_flat.push_back(count_val);
                        slot.hash = kh;
                        slot.gid_plus_one = gid + 1;
                        break;
                      }
                      if (slot.hash == kh) {
                        const uint32_t eg = slot.gid_plus_one - 1;
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
                km.ReconstructFull(req.key_specs,
                                   keys_flat.data() + (g * n_phys),
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
                auto wcmp = [asc](const WinCount& a,
                                  const WinCount& b) noexcept {
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
                auto h_or = EvalPositionalHavingOnStates(
                    req.positional_having, req.agg_specs, &count_st,
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
                    PushChargedSurvivor(&out_vec, &survivor_charge,
                                        std::move(row));
                    std::push_heap(out_vec.begin(), out_vec.end(), local_cmp);
                  } else if (local_cmp(row, out_vec.front())) {
                    std::pop_heap(out_vec.begin(), out_vec.end(), local_cmp);
                    survivor_charge.Release(
                        EstimateSurvivorGroupRowBytes(out_vec.back()));
                    PushChargedSurvivor(&out_vec, &survivor_charge,
                                        std::move(row));
                    std::push_heap(out_vec.begin(), out_vec.end(), local_cmp);
                  }
                } else {
                  PushChargedSurvivor(&out_vec, &survivor_charge,
                                      std::move(row));
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
            Status rst = wt.Reserve(
                std::min<size_t>(std::max<size_t>(64, shard_bytes / 96),
                                 kMaxPresizedShardGroups));
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
                    } else if (km.phys_specs[k].kind ==
                               ColKeyKind::kColDouble) {
                      std::memcpy(&probe_keys[k].double_val, p, 8);
                      p += 8;
                    } else {
                      std::memcpy(&probe_keys[k].int_val, p, 8);
                      p += 8;
                    }
                  }
                  // Same borrowed-pointer rule as the distinct path: spilled
                  // chunks resolve into reusable scratch, so their VARCHAR
                  // cells must be interned into the table arena rather than
                  // borrowed.
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
                            p2_status[tid] =
                                StatusError(StatusCode::kIsInfinity,
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
                              req.table_schema.GetColumn(spec.full_col)
                                  .Type() == ValueType::kDouble;
                          const double d = is_double_col
                                               ? std::bit_cast<double>(vi)
                                               : static_cast<double>(vi);
                          st.double_val += d;
                          ++st.count;
                          st.has_value = true;
                        } else if (spec.kind == ColAggKind::kSumDouble) {
                          const auto d = std::bit_cast<double>(vi);
                          if (std::isinf(d)) {
                            st.saw_inf = true;  // P603 provenance
                          }
                          st.double_val += d;
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
                          if (std::isnan(d)) {
                            st.saw_nan = true;
                          }
                          if (!st.has_value ||
                              CompareForOrderBy(Value(d),
                                                Value(st.double_val)) < 0) {
                            st.double_val = d;
                            st.has_value = true;
                          }
                        } else if (spec.kind == ColAggKind::kMaxDouble) {
                          const auto d = std::bit_cast<double>(vi);
                          if (std::isnan(d)) {
                            st.saw_nan = true;
                          }
                          if (!st.has_value ||
                              CompareForOrderBy(Value(d),
                                                Value(st.double_val)) > 0) {
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
                StatusOr<Value> finished_value =
                    FinishAggState(req.agg_specs[a], ast[a]);
                if (!finished_value.HasValue()) {
                  p2_status[tid] = finished_value.GetStatus();
                  return;
                }
                finished_aggs[a] = finished_value.MoveValue();
              }
              std::fill(cached_keys.begin(), cached_keys.end(), Value());
              if (fast_order_kind == PartOrderKind::kNone &&
                  !req.order_bindings.empty()) {
                row.order_keys.resize(req.order_bindings.size());
                for (size_t k = 0; k < req.order_bindings.size(); ++k) {
                  auto ov_or = EvaluateBindingValue(
                      req.order_bindings[k].binding, req.key_specs,
                      full_keys.data(), finished_aggs, &cached_keys);
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
                  PushChargedSurvivor(&out_vec, &survivor_charge,
                                      std::move(row));
                  std::push_heap(out_vec.begin(), out_vec.end(), local_cmp);
                } else {
                  std::pop_heap(out_vec.begin(), out_vec.end(), local_cmp);
                  survivor_charge.Release(
                      EstimateSurvivorGroupRowBytes(out_vec.back()));
                  PushChargedSurvivor(&out_vec, &survivor_charge,
                                      std::move(row));
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
      }));

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

// Lean proof: lean/TinylambOpt/ClickBenchDiscovery.lean
// Lean proofs:
//   - lean/TinylambOpt/ClickBenchRound2.lean
//     (unordered_group_by_key_fiber_subset_sound,
//      unordered_group_by_nonkey_filter_corrupts_count_bug)
//   - lean/TinylambOpt/ResearchProofs/Wave9ClickBenchHighCardAndFilter.lean
//     (GroupByLimitAnyOrderEarlyStop_sound)
// CB-2: Key-fiber subset aggregation for unordered `GROUP BY ... LIMIT K`
// (ClickBench Q18: `SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY
// UserID, SearchPhrase LIMIT 10`). Filtering input rows by a predicate `P(k1)`
// on an integer/date prefix key `k1` alone preserves 100% exact full-table
// aggregates for every surviving group while skipping decompression of heavy
// VARCHAR keys (`SearchPhrase`) on >99% of blocks!
StatusOr<std::optional<Relation>> TryRunKeyFiberUnorderedLimitAggregate(
    const PartitionedColumnarAggRequest& req, const KeyProjectionMap& km) {
  if (!req.order_bindings.empty() || !req.stmt.HasLimit() ||
      req.stmt.Limit() == 0 || req.any_scalar_distinct ||
      req.candidate_blocks.size() <= 8 || HasCheckedIntSum(req.agg_specs)) {
    return std::optional<Relation>{};
  }
  if (req.stmt.Offset() >
      std::numeric_limits<size_t>::max() - req.stmt.Limit()) {
    return std::optional<Relation>{};
  }
  const size_t topk_cap = req.stmt.Offset() + req.stmt.Limit();
  if (topk_cap == 0 || topk_cap > 512) {
    return std::optional<Relation>{};
  }

  // Every physical key must be infallible so skipping non-fiber rows can never
  // mask a runtime expression error.
  size_t fiber_phys_idx = SIZE_MAX;
  for (size_t p = 0; p < km.phys_specs.size(); ++p) {
    const ColKeyKind kind = km.phys_specs[p].kind;
    if (kind != ColKeyKind::kColInt64 && kind != ColKeyKind::kColDate &&
        kind != ColKeyKind::kColDouble && kind != ColKeyKind::kColVarChar &&
        kind != ColKeyKind::kCaseWhenVarChar) {
      return std::optional<Relation>{};
    }
    if (fiber_phys_idx == SIZE_MAX &&
        (kind == ColKeyKind::kColInt64 || kind == ColKeyKind::kColDate)) {
      fiber_phys_idx = p;
    }
  }
  if (fiber_phys_idx == SIZE_MAX) {
    return std::optional<Relation>{};
  }
  for (const ColAggSpec& as : req.agg_specs) {
    if (IsDistinctAggKind(as.kind)) {
      return std::optional<Relation>{};
    }
  }

  // Step 1: Seed up to `target_fiber_keys` distinct values of `fiber_col` from
  // the leading candidate blocks (decoding ONLY `preds` + `fiber_col`!).
  const size_t target_fiber_keys =
      std::min<size_t>(64, std::max<size_t>(16, topk_cap * 2));
  const ColGroupKeySpec& fiber_spec = km.phys_specs[fiber_phys_idx];
  const ColumnarStageSchedule seed_sched =
      BuildColumnarStageSchedule(req.table_schema, req.preds, {fiber_spec}, {});
  const size_t ncols = req.table_schema.ColumnCount();

  static constexpr size_t kFiberSlots = 256;
  static constexpr size_t kFiberMask = kFiberSlots - 1;
  struct FiberSlot {
    uint64_t hash{0};
    int64_t key{0};
    bool occupied{false};
  };
  std::array<FiberSlot, kFiberSlots> fiber_ht{};
  size_t fiber_count = 0;
  bool fiber_has_null = false;
  int64_t fiber_min = std::numeric_limits<int64_t>::max();
  int64_t fiber_max = std::numeric_limits<int64_t>::min();
  bool exhausted_all_blocks = true;

  auto fiber_contains_nonnull = [&](int64_t v) noexcept -> bool {
    if (v < fiber_min || v > fiber_max) {
      return false;
    }
    const uint64_t h = MixHash64(static_cast<uint64_t>(v));
    size_t pos = static_cast<size_t>(h) & kFiberMask;
    while (fiber_ht[pos].occupied) {
      if (fiber_ht[pos].hash == h && fiber_ht[pos].key == v) {
        return true;
      }
      pos = (pos + 1) & kFiberMask;
    }
    return false;
  };

  auto fiber_insert_nonnull = [&](int64_t v) noexcept -> bool {
    const uint64_t h = MixHash64(static_cast<uint64_t>(v));
    size_t pos = static_cast<size_t>(h) & kFiberMask;
    while (fiber_ht[pos].occupied) {
      if (fiber_ht[pos].hash == h && fiber_ht[pos].key == v) {
        return false;
      }
      pos = (pos + 1) & kFiberMask;
    }
    fiber_ht[pos] = {.hash = h, .key = v, .occupied = true};
    fiber_min = std::min(fiber_min, v);
    fiber_max = std::max(fiber_max, v);
    ++fiber_count;
    return true;
  };

  {
    BlockDecodeScratch seed_scratch;
    std::vector<const TlcsFile::DecodedColumnChunk*> col_ptrs(ncols, nullptr);
    std::vector<uint32_t> cand_rows;
    uint64_t dummy_scanned = 0;
    const size_t max_seed_blocks =
        std::min<size_t>(8, req.candidate_blocks.size());
    size_t bi = 0;
    for (; bi < max_seed_blocks; ++bi) {
      ASSIGN_OR_RETURN(
          bool, any_rows,
          DecodeAndFilterBlock(*req.tlcs, req.candidate_blocks[bi],
                               req.table_schema, req.preds, seed_sched,
                               &seed_scratch, &col_ptrs, &cand_rows,
                               &dummy_scanned));
      if (!any_rows) {
        continue;
      }
      const auto* fch = col_ptrs[fiber_spec.full_col];
      const bool no_null = !fch->has_null;
      for (const uint32_t r : cand_rows) {
        if (!no_null && fch->IsNull(r)) {
          if (!fiber_has_null) {
            fiber_has_null = true;
            ++fiber_count;
          }
        } else {
          (void)fiber_insert_nonnull(fch->int_cells[r]);
        }
        if (fiber_count >= target_fiber_keys) {
          exhausted_all_blocks = false;
          break;
        }
      }
      if (fiber_count >= target_fiber_keys) {
        break;
      }
    }
    if (bi < req.candidate_blocks.size() && fiber_count < target_fiber_keys) {
      exhausted_all_blocks = false;
    }
  }

  if (fiber_count == 0) {
    return std::optional<Relation>{};
  }
  if (!exhausted_all_blocks && fiber_count < std::min<size_t>(4, topk_cap)) {
    return std::optional<Relation>{};
  }

  // Step 2: Zone-map prune `candidate_blocks` on `fiber_col`!
  std::vector<uint32_t> fiber_blocks;
  if (!fiber_has_null && fiber_min <= fiber_max) {
    const uint32_t dg_count = req.tlcs->DirGroupCount();
    const uint32_t dg_size = req.tlcs->DirGroupSize();
    std::vector<std::shared_ptr<const TlcsFile::DirGroup>> dir_groups(dg_count);
    for (uint32_t g = 0; g < dg_count; ++g) {
      ASSIGN_OR_RETURN(std::shared_ptr<const TlcsFile::DirGroup>, grp,
                       req.tlcs->GetDirGroup(g));
      dir_groups[g] = std::move(grp);
    }
    const slot_t fcol = fiber_spec.full_col;
    fiber_blocks.reserve(req.candidate_blocks.size());
    for (const uint32_t bidx : req.candidate_blocks) {
      const auto& blk = dir_groups[bidx / dg_size]->blocks[bidx % dg_size];
      const auto& m = blk.columns[fcol];
      if (m.encoding == TlcsColumnEncoding::kConstNull) {
        continue;
      }
      if (m.has_min_max && (m.max_int < fiber_min || m.min_int > fiber_max)) {
        continue;
      }
      fiber_blocks.push_back(bidx);
    }
  } else {
    fiber_blocks = req.candidate_blocks;
  }

  // Determine remaining payload columns (`SearchPhrase`, `agg_specs`, etc.)
  // decoded ONLY on blocks where `fiber_rows` is non-empty!
  std::vector<uint8_t> in_seed(ncols, 0);
  for (const slot_t c : seed_sched.stage1a_cols) {
    in_seed[c] = 1;
  }
  for (const slot_t c : seed_sched.stage1b_cols) {
    in_seed[c] = 1;
  }
  for (const slot_t c : seed_sched.stage2_cols) {
    in_seed[c] = 1;
  }
  for (const slot_t c : seed_sched.cond_cols) {
    in_seed[c] = 1;
  }
  std::vector<uint8_t> extra_need(ncols, 0);
  std::vector<uint8_t> extra_bytes(ncols, 0);
  for (const auto& ks : km.phys_specs) {
    if (ks.kind == ColKeyKind::kConstInt64) {
      continue;
    }
    extra_need[ks.full_col] = 1;
    if (req.table_schema.GetColumn(ks.full_col).Type() == ValueType::kVarChar) {
      extra_bytes[ks.full_col] = 1;
    }
    for (const auto& cc : ks.case_conditions) {
      extra_need[cc.left] = 1;
      if (!cc.constant_payload) {
        extra_need[cc.right] = 1;
      }
    }
  }
  for (const ColAggSpec& as : req.agg_specs) {
    if (as.kind == ColAggKind::kCountStar) {
      continue;
    }
    extra_need[as.full_col] = 1;
    if (as.needs_bytes) {
      extra_bytes[as.full_col] = 1;
    }
  }
  ColumnarStageSchedule scan_sched = seed_sched;
  for (size_t i = 0; i < scan_sched.stage1b_cols.size(); ++i) {
    if (extra_bytes[scan_sched.stage1b_cols[i]] != 0U) {
      scan_sched.stage1b_len_only[i] = 0;
    }
  }
  for (size_t i = 0; i < scan_sched.stage2_cols.size(); ++i) {
    if (extra_bytes[scan_sched.stage2_cols[i]] != 0U) {
      scan_sched.stage2_len_only[i] = 0;
    }
  }
  std::vector<slot_t> extra_cols;
  std::vector<uint8_t> extra_len_only;
  extra_cols.reserve(ncols);
  extra_len_only.reserve(ncols);
  for (size_t c = 0; c < ncols; ++c) {
    if (extra_need[c] != 0U && in_seed[c] == 0U) {
      extra_cols.push_back(static_cast<slot_t>(c));
      extra_len_only.push_back(extra_bytes[c] != 0U ? 0 : 1);
    }
  }

  const size_t workers = std::max<size_t>(
      1, std::min<size_t>(
             {QueryParallelWorkerLimit(std::thread::hardware_concurrency()),
              std::max<size_t>(1, fiber_blocks.size()), size_t{16}}));
  std::vector<QueryMemoryCharge> w_charges(workers);
  std::vector<std::unique_ptr<WorkerGroupTable>> w_tables;
  w_tables.reserve(workers);
  for (size_t w = 0; w < workers; ++w) {
    w_tables.push_back(std::make_unique<WorkerGroupTable>(
        &km.phys_specs, &req.agg_specs, &w_charges[w]));
  }
  std::vector<Status> w_status(workers, Status::kSuccess);
  std::atomic<size_t> next_b{0};
  std::atomic<uint64_t> total_scanned{0};
  std::atomic<uint64_t> total_matching{0};
  const size_t n_phys = km.phys_specs.size();

  RETURN_IF_FAIL(RunParallelTasks(workers, [&](size_t tid) {
    BlockDecodeScratch scratch;
    TlcsFile::DecodedBlock blk_extra;
    std::vector<uint8_t> io_extra;
    std::vector<const TlcsFile::DecodedColumnChunk*> col_ptrs(ncols, nullptr);
    std::vector<uint32_t> cand_rows;
    std::vector<uint32_t> fiber_rows;
    std::vector<RuntimeKeyCell> probe_keys(n_phys);
    std::string regex_scratch;
    TimestampPrefixCache ts_cache;
    RegexReplaceCache regex_cache;
    uint64_t local_scanned = 0;
    uint64_t local_matching = 0;
    WorkerGroupTable& wt = *w_tables[tid];

    while (true) {
      const size_t bi = next_b.fetch_add(1, std::memory_order_relaxed);
      if (bi >= fiber_blocks.size()) {
        break;
      }
      const uint32_t bidx = fiber_blocks[bi];
      auto dec_or = DecodeAndFilterBlock(*req.tlcs, bidx, req.table_schema,
                                         req.preds, scan_sched, &scratch,
                                         &col_ptrs, &cand_rows, &local_scanned);
      if (!dec_or.HasValue()) {
        w_status[tid] = dec_or.GetStatus();
        return;
      }
      if (!dec_or.Value()) {
        continue;
      }
      local_matching += cand_rows.size();

      const auto* fch = col_ptrs[fiber_spec.full_col];
      const bool no_null = !fch->has_null;
      fiber_rows.clear();
      for (const uint32_t r : cand_rows) {
        if (!no_null && fch->IsNull(r)) {
          if (fiber_has_null) {
            fiber_rows.push_back(r);
          }
        } else if (fiber_contains_nonnull(fch->int_cells[r])) {
          fiber_rows.push_back(r);
        }
      }
      if (fiber_rows.empty()) {
        continue;
      }

      if (!extra_cols.empty()) {
        Status est = req.tlcs->DecodeBlockColumnsWithOptions(
            bidx, extra_cols, req.table_schema, &blk_extra, true, &io_extra,
            extra_len_only);
        if (est != Status::kSuccess) {
          w_status[tid] = std::move(est);
          return;
        }
        for (size_t i = 0; i < extra_cols.size(); ++i) {
          col_ptrs[extra_cols[i]] = &blk_extra.columns[i];
        }
      }

      for (const uint32_t r : fiber_rows) {
        Status kst = Status::kSuccess;
        for (size_t k = 0; k < n_phys; ++k) {
          if (!ExtractSingleKeyCell(km.phys_specs[k], col_ptrs, r,
                                    &probe_keys[k], &regex_scratch, &ts_cache,
                                    &kst, &regex_cache)) {
            w_status[tid] = std::move(kst);
            return;
          }
        }
        const uint64_t kh =
            GroupKeySliceHasher::HashTyped(probe_keys.data(), km.phys_specs);
        auto gid_or = wt.FindOrInsert(probe_keys.data(), kh);
        if (!gid_or.HasValue()) {
          w_status[tid] = gid_or.GetStatus();
          return;
        }
        RuntimeAggState* ast = wt.AggsAt(gid_or.Value());
        for (size_t a = 0; a < req.agg_specs.size(); ++a) {
          Status fst = FoldRowIntoAggState(req.agg_specs[a], col_ptrs, r,
                                           ast[a], wt.Arena());
          if (fst != Status::kSuccess) {
            w_status[tid] = std::move(fst);
            return;
          }
        }
      }
    }
    total_scanned.fetch_add(local_scanned, std::memory_order_relaxed);
    total_matching.fetch_add(local_matching, std::memory_order_relaxed);
  }));

  for (const Status& st : w_status) {
    if (st == StatusCode::kInvalidArgument ||
        st == StatusCode::kMemoryLimitExceeded) {
      return std::optional<Relation>{};
    }
    RETURN_IF_FAIL(st);
  }

  const auto merge_begin = std::chrono::steady_clock::now();
  QueryMemoryCharge merge_charge;
  WorkerGroupTable merged(&km.phys_specs, &req.agg_specs, &merge_charge);
  size_t est_total = 0;
  for (const auto& wt : w_tables) {
    est_total += wt->NumGroups();
  }
  Status rst = merged.Reserve(std::max<size_t>(16, est_total));
  if (rst == StatusCode::kMemoryLimitExceeded) {
    return std::optional<Relation>{};
  }
  RETURN_IF_FAIL(rst);
  for (size_t w = 0; w < workers; ++w) {
    WorkerGroupTable& src = *w_tables[w];
    for (size_t g = 0; g < src.NumGroups(); ++g) {
      Status mst = merged.MergeFromGroup(src.KeysAt(g), src.HashAt(g),
                                         {src.AggsAt(g), req.agg_specs.size()},
                                         /*borrow_strings=*/true);
      if (mst == StatusCode::kMemoryLimitExceeded) {
        return std::optional<Relation>{};
      }
      RETURN_IF_FAIL(mst);
    }
  }

  std::vector<std::vector<SurvivorGroupRow>> single_surv(1);
  std::vector<std::unique_ptr<QueryMemoryCharge>> survivor_charges(1);
  QueryMemoryCharge& survivor_charge = SurvivorChargeFor(survivor_charges, 0);
  auto& out_vec = single_surv[0];
  std::vector<RuntimeKeyCell> full_keys(req.key_specs.size());
  std::vector<Value> finished_aggs(req.agg_specs.size());
  std::vector<Value> cached_keys(req.key_specs.size());
  std::vector<Value> having_scratch(req.agg_specs.size());

  const size_t ng = merged.NumGroups();
  for (size_t g = 0; g < ng; ++g) {
    const RuntimeAggState* ast = merged.AggsAt(g);
    ASSIGN_OR_RETURN(
        bool, passes,
        EvalPositionalHavingOnStates(req.positional_having, req.agg_specs, ast,
                                     &having_scratch));
    if (!passes) {
      continue;
    }
    km.ReconstructFull(req.key_specs, merged.KeysAt(g), full_keys.data());
    for (size_t a = 0; a < req.agg_specs.size(); ++a) {
      ASSIGN_OR_RETURN(Value, finished_value,
                       (FinishAggState(req.agg_specs[a], ast[a])));
      finished_aggs[a] = std::move(finished_value);
    }
    std::fill(cached_keys.begin(), cached_keys.end(), Value());
    SurvivorGroupRow row;
    row.emit_vals.reserve(req.emit_bindings.size());
    for (const auto& eb : req.emit_bindings) {
      ASSIGN_OR_RETURN(Value, ev,
                       EvaluateBindingValue(eb, req.key_specs, full_keys.data(),
                                            finished_aggs, &cached_keys));
      row.emit_vals.push_back(std::move(ev));
    }
    PushChargedSurvivor(&out_vec, &survivor_charge, std::move(row));
    if (out_vec.size() >= topk_cap) {
      break;
    }
  }

  if (out_vec.size() < topk_cap && !exhausted_all_blocks) {
    return std::optional<Relation>{};
  }

  return FinalizePartitionedRelation(req, single_surv, ng, total_scanned.load(),
                                     total_matching.load(), merge_begin,
                                     "key-fiber-limit", &survivor_charges);
}

// Lean proof: lean/TinylambOpt/ClickBenchRound2.lean
//   (quotient_bucket_count_ge_group_count, quotient_bucket_topk_pruning_sound,
//    quotient_bucket_asc_order_pruning_bug)
// CB-7: Exact heavy-hitter pruning via quotient hash-bucket upper bounds for
// `GROUP BY keys ORDER BY COUNT(*) DESC LIMIT K` (ClickBench Q13, Q15, Q17,
// Q19, Q34, Q35). Eliminates multi-GB VARCHAR slab serialization and disk
// spilling by counting 2^20 quotient buckets in L3 cache first, then
// aggregating exact key tuples only for the <= 2,048 heaviest buckets and
// certifying `tau > max_bucket_outside_S` before returning!
StatusOr<std::optional<Relation>> TryRunQuotientBucketHeavyHittersTopK(
    const PartitionedColumnarAggRequest& req, const KeyProjectionMap& km) {
  if (!req.stmt.HasLimit() || req.stmt.Limit() == 0 ||
      !req.positional_having.empty() || req.any_scalar_distinct ||
      req.agg_specs.size() != 1 ||
      req.agg_specs[0].kind != ColAggKind::kCountStar ||
      req.order_bindings.empty() ||
      req.order_bindings[0].binding.kind != OutputSlotBinding::Kind::kAgg ||
      req.order_bindings[0].binding.index != 0 ||
      req.order_bindings[0].order.Ascending() ||
      req.candidate_blocks.size() < 128) {
    return std::optional<Relation>{};
  }
  if (req.stmt.Offset() >
      std::numeric_limits<size_t>::max() - req.stmt.Limit()) {
    return std::optional<Relation>{};
  }
  const size_t topk_cap = req.stmt.Offset() + req.stmt.Limit();
  if (topk_cap == 0 || topk_cap > 256) {
    return std::optional<Relation>{};
  }

  bool any_varchar_key = false;
  for (const ColGroupKeySpec& ks : km.phys_specs) {
    if (IsVarCharKeyKind(ks.kind)) {
      any_varchar_key = true;
      break;
    }
  }
  const bool single_raw_int_key =
      km.phys_specs.size() == 1 &&
      (km.phys_specs[0].kind == ColKeyKind::kColInt64 ||
       km.phys_specs[0].kind == ColKeyKind::kColDate);
  const bool numeric_pair =
      km.phys_specs.size() == 2 &&
      std::ranges::all_of(km.phys_specs, [&](const auto& key) {
        return (key.kind == ColKeyKind::kColInt64 ||
                key.kind == ColKeyKind::kColDate) &&
               !req.table_schema.GetColumn(key.full_col).IsUnsigned();
      });
  const bool try_duplicate_sieve =
      numeric_pair && req.order_bindings.size() == 1 &&
      req.candidate_blocks.size() >= 512 &&
      std::ranges::none_of(req.emit_bindings,
                           [](const auto& item) {
                             return item.kind ==
                                    OutputSlotBinding::Kind::kPostAgg;
                           }) &&
      ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_TLCS_DUPLICATE_SIEVE") == nullptr;
  bool near_unique = false;
  if (try_duplicate_sieve) {
    // The sampled column is sufficient to estimate pair uniqueness. A
    // sparse sample may miss duplicates, so it only selects the algorithm.
    constexpr size_t kBlocks = 8;
    constexpr size_t kRowsPerBlock = 1024;
    const std::array columns{km.phys_specs[0].full_col};
    TlcsFile::DecodedBlock sample;
    std::vector<uint8_t> scratch;
    std::vector<int64_t> keys;
    keys.reserve(kBlocks * kRowsPerBlock);
    size_t sampled_rows = 0;
    for (size_t i = 0; i < kBlocks; ++i) {
      const uint32_t block =
          req.candidate_blocks[(i * req.candidate_blocks.size()) / kBlocks];
      RETURN_IF_FAIL(req.tlcs->DecodeBlockColumnsWithOptions(
          block, columns, req.table_schema, &sample, true, &scratch, {}, true));
      if (sample.row_count == 0) {
        continue;
      }
      const auto& col = sample.columns[0];
      const size_t rows = std::min<size_t>(sample.row_count, kRowsPerBlock);
      sampled_rows += rows;
      for (size_t r = 0; r < rows; ++r) {
        if (!col.IsNull(r)) {
          keys.push_back(col.int_cells[r]);
        }
      }
    }
    std::ranges::sort(keys);
    const auto duplicates = std::ranges::unique(keys);
    const size_t nulls = sampled_rows - keys.size();
    near_unique =
        keys.size() >= kRowsPerBlock &&
        nulls + static_cast<size_t>(duplicates.size()) <= (sampled_rows / 100);
  }
  const bool try_numeric_quotient =
      numeric_pair && !near_unique && req.order_bindings.size() == 1 &&
      req.candidate_blocks.size() >= 512 &&
      ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_QUOTIENT_FUSION") == nullptr;
  if (!any_varchar_key && !single_raw_int_key && !near_unique &&
      !try_numeric_quotient) {
    return std::optional<Relation>{};
  }

  static constexpr size_t kBucketBits = 20;
  static constexpr size_t kNumBuckets = size_t{1} << kBucketBits;
  static constexpr size_t kBucketMask = kNumBuckets - 1;
  static constexpr size_t kHeavyBuckets = 2048;

  const size_t workers = std::max<size_t>(
      1, std::min<size_t>(
             {QueryParallelWorkerLimit(std::thread::hardware_concurrency()),
              req.candidate_blocks.size(), size_t{16}}));

  QueryMemoryCharge sketch_charge;
  const size_t reserve_bytes = (workers * kNumBuckets * sizeof(uint32_t)) +
                               (kNumBuckets * sizeof(uint64_t)) +
                               (kNumBuckets / 8);
  if (!sketch_charge.TryAdd(reserve_bytes)) {
    return std::optional<Relation>{};
  }

  const size_t n_phys = km.phys_specs.size();
  const size_t ncols = req.table_schema.ColumnCount();
  const ColumnarStageSchedule sched = BuildColumnarStageSchedule(
      req.table_schema, req.preds, km.phys_specs, req.agg_specs);
  const ColumnarStageSchedule leading_key_sched =
      near_unique
          ? BuildColumnarStageSchedule(req.table_schema, req.preds,
                                       {km.phys_specs[0]}, req.agg_specs)
          : ColumnarStageSchedule{};
  const bool single_raw_varchar_key =
      n_phys == 1 && km.phys_specs[0].kind == ColKeyKind::kColVarChar;

  // The directory counts also cover repeated block references. Prove the
  // largest possible counter up front, so every increment in either pass
  // is exact without a saturation check in the row loop.
  // Lean proof: lean/TinylambOpt/QuotientFusion.lean (bucket_increment_fits)
  uint64_t input_bound = 0;
  std::vector<uint64_t> block_row_starts;
  const bool store_block_starts = near_unique && req.preds.Empty();
  if (store_block_starts) {
    block_row_starts.reserve(req.candidate_blocks.size() + 1);
  }
  for (uint32_t block : req.candidate_blocks) {
    if (store_block_starts) {
      block_row_starts.push_back(input_bound);
    }
    ASSIGN_OR_RETURN(uint32_t, count, req.tlcs->BlockRowCount(block));
    input_bound += count;
    if (input_bound > UINT32_MAX) {
      return std::optional<Relation>{};
    }
  }

  using GroupTables = std::vector<std::unique_ptr<WorkerGroupTable>>;
  const auto make_tables = [&](std::vector<QueryMemoryCharge>& charges) {
    GroupTables tables;
    tables.reserve(workers);
    for (auto& charge : charges) {
      tables.push_back(std::make_unique<WorkerGroupTable>(
          &km.phys_specs, &req.agg_specs, &charge));
    }
    return tables;
  };
  const auto has_bucket = [](std::span<const uint64_t> mask, size_t bucket) {
    return ((mask[bucket / 64] >> (bucket % 64)) & 1) != 0;
  };
  const auto bucket_of = [](uint64_t hash) {
    return static_cast<size_t>((hash >> 32U) ^ hash) & kBucketMask;
  };
  const bool direct = req.preds.Empty() && sched.cond_cols.empty() &&
                      sched.stage1a_cols.empty() &&
                      sched.stage1c_cols.empty() && sched.stage2_cols.empty() &&
                      !sched.stage1b_cols.empty();
  const bool specialize =
      direct && ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_QUOTIENT_FUSION") == nullptr;
  const auto string_fingerprint_jit =
      specialize && single_raw_varchar_key && input_bound >= (1U << 24)
          ? JitStringFingerprint::Compile()
          : std::nullopt;
  const auto count_group = [&](WorkerGroupTable& table,
                               const RuntimeKeyCell* keys,
                               uint64_t bucket_hash) -> Status {
    // The shortlist may deliberately use a coarse fingerprint. Rehash the
    // selected bytes or key tuple for the exact table to keep probes short.
    const uint64_t hash =
        string_fingerprint_jit || near_unique
            ? GroupKeySliceHasher::HashTyped(keys, km.phys_specs)
            : bucket_hash;
    ASSIGN_OR_RETURN(uint32_t, group, table.FindOrInsert(keys, hash));
    auto* state = table.AggsAt(group);
    ++state[0].count;
    state[0].has_value = true;
    return Status::kSuccess;
  };
  const RuntimeKeyCell null_cell{.is_null = true};
  const RuntimeKeyCell empty_cell;
  const uint64_t null_hash =
      single_raw_varchar_key || single_raw_int_key || near_unique
          ? GroupKeySliceHasher::HashTyped(&null_cell, {km.phys_specs.front()})
          : 0;
  const uint64_t empty_hash =
      string_fingerprint_jit
          ? JitStringFingerprint::Scalar({}, GroupKeySliceHasher::kSeed)
      : single_raw_varchar_key
          ? GroupKeySliceHasher::HashTyped(&empty_cell, km.phys_specs)
          : 0;

  std::atomic<uint64_t> total_scanned{0};
  std::atomic<uint64_t> total_matching{0};
  const bool read_projected_streams_directly =
      specialize && sched.stage1b_cols.size() <= 4;
  const auto group_hash_jit =
      direct && near_unique ? JitGroupHash::Compile() : std::nullopt;
  std::optional<JitDatePartHash> date_part_jit;
  std::shared_ptr<const std::vector<TlcsFile::VarcharBlockBound>> date_bounds;
  if (specialize && n_phys == 3 && req.candidate_blocks.size() >= 512 &&
      (km.phys_specs[0].kind == ColKeyKind::kColInt64 ||
       km.phys_specs[0].kind == ColKeyKind::kColDate) &&
      km.phys_specs[1].kind == ColKeyKind::kUtcTimestampExtract &&
      km.phys_specs[2].kind == ColKeyKind::kColVarChar) {
    const auto& date = km.phys_specs[1];
    date_part_jit = JitDatePartHash::Compile(
        date.trunc_unit == UtcTimestampTruncUnit::kHour ? 11 : 14);
    if (date_part_jit) {
      ASSIGN_OR_RETURN(decltype(date_bounds), bounds,
                       req.tlcs->GetOrComputeVarcharBlockBounds(
                           date.full_col, req.table_schema));
      date_bounds = std::move(bounds);
    }
  }
  struct SavedSelection {
    std::vector<uint16_t> rows;
    bool complete{false};
  };
  std::vector<SavedSelection> selections;
  QueryMemoryCharge selection_charge;
  std::atomic<size_t> selection_bytes{0};
  constexpr size_t kSelectionBudget = size_t{64} << 20;
  if (near_unique && !req.preds.Empty() &&
      selection_charge.TryAdd(kSelectionBudget + (req.candidate_blocks.size() *
                                                  sizeof(SavedSelection)))) {
    selections.resize(req.candidate_blocks.size());
  }
  const auto scan_keys =
      [&](std::span<const uint32_t> blocks, bool report, auto&& emit,
          bool leading_only = false,
          std::span<const uint64_t> prefetch_words = {},
          std::span<const uint64_t> replay_rows = {}) -> Status {
    const size_t active_workers = std::min(workers, blocks.size());
    const auto& scan_sched = leading_only ? leading_key_sched : sched;
    std::vector<Status> errors(active_workers, Status::kSuccess);
    std::atomic<size_t> next{0};
    RETURN_IF_FAIL(RunParallelTasks(active_workers, [&](size_t worker) {
      BlockDecodeScratch scratch;
      std::vector<const TlcsFile::DecodedColumnChunk*> columns(ncols, nullptr);
      std::vector<uint32_t> candidates;
      std::vector<RuntimeKeyCell> keys(n_phys);
      TimestampPrefixCache timestamps;
      RegexReplaceCache regexes;
      std::string regex_scratch;
      uint64_t scanned = 0;
      uint64_t matching = 0;
      constexpr size_t kHashBatch = 128;
      std::array<uint64_t, kHashBatch> hashes{};
      std::array<int64_t, kHashBatch> date_parts{};

      for (size_t i = next.fetch_add(1, std::memory_order_relaxed);
           i < blocks.size();
           i = next.fetch_add(1, std::memory_order_relaxed)) {
        const uint32_t block = blocks[i];
        const auto emit_row = [&](const RuntimeKeyCell* cells, uint64_t hash,
                                  uint32_t row) {
          if constexpr (std::is_invocable_v<decltype(emit), size_t,
                                            const RuntimeKeyCell*, uint64_t,
                                            size_t, uint32_t>) {
            return emit(worker, cells, hash, i, row);
          } else {
            return emit(worker, cells, hash);
          }
        };
        const auto visit = [&](auto rows) -> Status {
          if (single_raw_varchar_key) {
            const auto& chunk = *columns[km.phys_specs[0].full_col];
            const auto visit_text =
                [&]<bool Nullable, bool Encoded>() -> Status {
              // Encoded rows can reuse the hash by code. For raw strings, a
              // pointer-cache probe would only pay off for repeated empties.
              std::array<std::optional<uint64_t>, Encoded ? 256 : 0> hashes{};
              for (uint32_t row : rows) {
                if constexpr (Nullable) {
                  if (chunk.IsNull(row)) {
                    RETURN_IF_FAIL(emit_row(&null_cell, null_hash,
                                            static_cast<uint32_t>(row)));
                    continue;
                  }
                }
                const auto text = chunk.VarcharAt(row);
                const RuntimeKeyCell key{
                    .str_ptr = text.data(),
                    .str_len = static_cast<uint32_t>(text.size())};
                const auto hash_text = [&] {
                  if (text.empty()) {
                    return empty_hash;
                  }
                  return string_fingerprint_jit
                             ? JitStringFingerprint::Scalar(
                                   text, GroupKeySliceHasher::kSeed)
                             : GroupKeySliceHasher::HashSingleVarChar(text);
                };
                uint64_t hash = 0;
                if constexpr (Encoded) {
                  if (row < chunk.dict_codes.size()) {
                    auto& cached = hashes[chunk.dict_codes[row]];
                    if (!cached) cached = hash_text();
                    hash = *cached;
                  } else {
                    hash = empty_hash;
                  }
                } else {
                  hash = hash_text();
                }
                RETURN_IF_FAIL(emit_row(&key, hash, row));
              }
              return Status::kSuccess;
            };
            if (chunk.dict_encoded && chunk.dict != nullptr) {
              return chunk.has_null
                         ? visit_text.template operator()<true, true>()
                         : visit_text.template operator()<false, true>();
            }
            return chunk.has_null
                       ? visit_text.template operator()<true, false>()
                       : visit_text.template operator()<false, false>();
          }
          if (single_raw_int_key || leading_only) {
            const auto& chunk = *columns[km.phys_specs[0].full_col];
            const auto visit_int = [&]<bool Nullable>() -> Status {
              for (uint32_t row : rows) {
                if constexpr (Nullable) {
                  if (chunk.IsNull(row)) {
                    RETURN_IF_FAIL(emit_row(&null_cell, null_hash,
                                            static_cast<uint32_t>(row)));
                    continue;
                  }
                }
                const RuntimeKeyCell key{.int_val = chunk.int_cells[row]};
                const uint64_t hash =
                    MixHash64(GroupKeySliceHasher::kSeed ^
                              MixHash64(static_cast<uint64_t>(key.int_val)));
                RETURN_IF_FAIL(emit_row(&key, hash, row));
              }
              return Status::kSuccess;
            };
            return chunk.has_null ? visit_int.template operator()<true>()
                                  : visit_int.template operator()<false>();
          }
          if (numeric_pair) {
            const auto& a = *columns[km.phys_specs[0].full_col];
            const auto& b = *columns[km.phys_specs[1].full_col];
            const auto visit_pair = [&]<bool Nullable>() -> Status {
              for (const uint32_t row : rows) {
                const std::array<RuntimeKeyCell, 2> pair{{
                    {.is_null = Nullable && a.IsNull(row),
                     .int_val = a.int_cells[row]},
                    {.is_null = Nullable && b.IsNull(row),
                     .int_val = b.int_cells[row]},
                }};
                // The first key alone is a sound bucket key. The resident
                // table below still tests both cells for equality.
                const uint64_t h =
                    !near_unique
                        ? GroupKeySliceHasher::HashTyped(pair.data(), km.phys_specs)
                    : pair[0].is_null
                        ? null_hash
                        : MixHash64(GroupKeySliceHasher::kSeed ^
                                    MixHash64(static_cast<uint64_t>(
                                        pair[0].int_val)));
                RETURN_IF_FAIL(emit_row(pair.data(), h, row));
              }
              return Status::kSuccess;
            };
            return a.has_null || b.has_null
                       ? visit_pair.template operator()<true>()
                       : visit_pair.template operator()<false>();
          }
          for (uint32_t row : rows) {
            Status status = Status::kSuccess;
            for (size_t k = 0; k < n_phys; ++k) {
              if (!ExtractSingleKeyCell(km.phys_specs[k], columns, row,
                                        &keys[k], &regex_scratch, &timestamps,
                                        &status, &regexes)) {
                return status;
              }
            }
            const uint64_t hash =
                GroupKeySliceHasher::HashTyped(keys.data(), km.phys_specs);
            RETURN_IF_FAIL(emit_row(keys.data(), hash, row));
          }
          return Status::kSuccess;
        };

        Status status = Status::kSuccess;
        if (!leading_only && !selections.empty() && selections[i].complete) {
          // The first scan saved the exact predicate survivors from this
          // snapshot. Decode only their keys during the replay.
          const auto& saved = selections[i].rows;
          if (saved.empty()) {
            continue;
          }
          const std::array key_columns{km.phys_specs[0].full_col,
                                       km.phys_specs[1].full_col};
          status = req.tlcs->DecodeBlockColumnsWithOptions(
              block, key_columns, req.table_schema, &scratch.blk1b, true,
              &scratch.io1b, {}, read_projected_streams_directly);
          if (status.ok()) {
            for (size_t col = 0; col < key_columns.size(); ++col) {
              columns[key_columns[col]] = &scratch.blk1b.columns[col];
            }
            status = visit(std::span<const uint16_t>(saved));
          }
        } else if (direct) {
          // Drive the key loop with an implicit row-index range. For an
          // unfiltered query we can omit the identity selection vector.
          status = req.tlcs->DecodeBlockColumnsWithOptions(
              block, scan_sched.stage1b_cols, req.table_schema, &scratch.blk1b,
              true, &scratch.io1b, scan_sched.stage1b_len_only,
              read_projected_streams_directly);
          if (status.ok()) {
            for (size_t col = 0; col < scan_sched.stage1b_cols.size(); ++col) {
              columns[scan_sched.stage1b_cols[col]] =
                  &scratch.blk1b.columns[col];
            }
            const uint32_t count = scratch.blk1b.row_count;
            scanned += count;
            matching += count;
            const auto* a = columns[km.phys_specs[0].full_col];
            const auto* b = near_unique && !leading_only
                                ? columns[km.phys_specs[1].full_col]
                                : nullptr;
            if (!replay_rows.empty() && count != 0) {
              candidates.clear();
              const uint64_t start = block_row_starts[i];
              const uint64_t last = start + count - 1;
              for (size_t word = start / 64; word <= last / 64; ++word) {
                uint64_t mask = replay_rows[word];
                if (word == start / 64) {
                  mask &= UINT64_MAX << (start % 64);
                }
                if (word == last / 64) {
                  mask &= UINT64_MAX >> (63 - (last % 64));
                }
                while (mask != 0) {
                  candidates.push_back(static_cast<uint32_t>(
                      (word * 64) + std::countr_zero(mask) - start));
                  mask &= mask - 1;
                }
              }
              status = visit(std::span<const uint32_t>(candidates));
            } else if (string_fingerprint_jit && !a->dict_encoded &&
                       a->varchar_ends.size() == count) {
              constexpr uint32_t first = 0;
              for (size_t base = 0; base < count && status.ok();) {
                const size_t n =
                    base == 0 ? 1 : std::min(kHashBatch, count - base);
                const uint32_t* starts =
                    base == 0 ? &first : a->varchar_ends.data() + base - 1;
                string_fingerprint_jit->Hash(
                    reinterpret_cast<const char*>(a->varchar_bytes.data()),
                    a->varchar_bytes.size(), starts,
                    a->varchar_ends.data() + base, hashes.data(), n,
                    GroupKeySliceHasher::kSeed);
                for (size_t lane = 0; lane < n; ++lane) {
                  const auto row = base + lane;
                  if (a->has_null && a->IsNull(row)) {
                    status = emit_row(&null_cell, null_hash,
                                      static_cast<uint32_t>(row));
                  } else {
                    const auto value = a->VarcharAt(row);
                    const RuntimeKeyCell cell{
                        .str_ptr = value.data(),
                        .str_len = static_cast<uint32_t>(value.size())};
                    status = emit_row(&cell, hashes[lane],
                                      static_cast<uint32_t>(row));
                  }
                  if (!status.ok()) {
                    break;
                  }
                }
                base += n;
              }
            } else if (date_bounds &&
                       (*date_bounds)[block].all_canonical_utc_ts &&
                       !a->has_null) {
              const auto* date = columns[km.phys_specs[1].full_col];
              const auto* text = columns[km.phys_specs[2].full_col];
              if (!date->has_null && !date->dict_encoded && !text->has_null &&
                  date->varchar_ends.size() == count) {
                const uint32_t first = 0;
                const uint64_t empty = HashView64({});
                size_t base = 0;
                while (base < count && status.ok()) {
                  const size_t n =
                      base == 0 ? 1 : std::min(kHashBatch, count - base);
                  const uint32_t* starts =
                      base == 0 ? &first : date->varchar_ends.data() + base - 1;
                  date_part_jit->Hash(
                      a->int_cells.data() + base,
                      reinterpret_cast<const char*>(date->varchar_bytes.data()),
                      starts, date_parts.data(), hashes.data(), n,
                      GroupKeySliceHasher::kSeed);
                  for (size_t lane = 0; lane < n; ++lane) {
                    const auto row = base + lane;
                    const auto value = text->VarcharAt(row);
                    const uint64_t entry =
                        value.empty() ? empty : HashView64(value);
                    const uint64_t hash = MixHash64(
                        hashes[lane] ^ (entry + 2 * 0x9e3779b97f4a7c15ULL));
                    const std::array<RuntimeKeyCell, 3> cells{{
                        {.int_val = a->int_cells[row]},
                        {.int_val = date_parts[lane]},
                        // NOLINTNEXTLINE(bugprone-suspicious-stringview-data-usage): RuntimeKeyCell carries str_len.
                        {.str_ptr = value.data(),
                         .str_len = static_cast<uint32_t>(value.size())}}};
                    status = emit_row(cells.data(), hash,
                                      static_cast<uint32_t>(row));
                    if (!status.ok()) {
                      break;
                    }
                  }
                  base += n;
                }
              } else {
                status = visit(std::views::iota(uint32_t{0}, count));
              }
            } else if (group_hash_jit && !a->has_null &&
                       (b == nullptr || !b->has_null)) {
              for (size_t base = 0; base < count && status.ok();
                   base += kHashBatch) {
                const size_t n = std::min(kHashBatch, count - base);
                group_hash_jit->Hash(a->int_cells.data() + base, hashes.data(),
                                     n, GroupKeySliceHasher::kSeed);
                for (size_t lane = 0; lane < n; ++lane) {
                  constexpr size_t kAhead = 16;
                  if (!prefetch_words.empty() && lane + kAhead < n) {
                    const size_t word = (hashes[lane + kAhead] >> 16) &
                                        (prefetch_words.size() - 1);
                    PrefetchBitmapWord(prefetch_words.data() + word);
                  }
                  const size_t row = base + lane;
                  const std::array<RuntimeKeyCell, 2> pair{{
                      {.int_val = a->int_cells[row]},
                      {.int_val = b == nullptr ? 0 : b->int_cells[row]}}};
                  status = emit_row(pair.data(), hashes[lane],
                                    static_cast<uint32_t>(row));
                  if (!status.ok()) {
                    break;
                  }
                }
              }
            } else {
              status = visit(std::views::iota(uint32_t{0}, count));
            }
          }
        } else {
          auto decoded = DecodeAndFilterBlock(
              *req.tlcs, block, req.table_schema, req.preds, scan_sched,
              &scratch, &columns, &candidates, &scanned);
          if (!decoded.HasValue()) {
            status = decoded.GetStatus();
          } else {
            if (leading_only && !selections.empty()) {
              const size_t n = decoded.Value() ? candidates.size() : 0;
              const size_t bytes = n * sizeof(uint16_t);
              const size_t old =
                  selection_bytes.fetch_add(bytes, std::memory_order_relaxed);
              if (old <= kSelectionBudget && bytes <= kSelectionBudget - old &&
                  (n == 0 || candidates.back() <= UINT16_MAX)) {
                auto& saved = selections[i];
                saved.rows.assign(candidates.begin(), candidates.begin() + n);
                saved.complete = true;
              }
            }
            if (decoded.Value()) {
              matching += candidates.size();
              status = visit(std::span<const uint32_t>(candidates));
            }
          }
        }
        if (!status.ok()) {
          errors[worker] = std::move(status);
          return;
        }
      }
      if (report) {
        total_scanned.fetch_add(scanned, std::memory_order_relaxed);
        total_matching.fetch_add(matching, std::memory_order_relaxed);
      }
    }));
    for (const Status& error : errors) {
      RETURN_IF_FAIL(error);
    }
    return Status::kSuccess;
  };

  const auto finish =
      [&](GroupTables& w_tables, uint32_t rest_max_bucket,
          const char* label) -> StatusOr<std::optional<Relation>> {
    const auto merge_begin = std::chrono::steady_clock::now();
    WorkerGroupTable& merged = *w_tables[0];
    for (size_t w = 1; w < workers; ++w) {
      WorkerGroupTable& src = *w_tables[w];
      for (size_t g = 0; g < src.NumGroups(); ++g) {
        const RuntimeKeyCell* gk = src.KeysAt(g);
        const uint64_t kh = src.HashAt(g);
        auto dst_or = merged.FindOrInsert(gk, kh, /*borrow_strings=*/false);
        if (!dst_or.HasValue()) {
          if (dst_or.GetStatus() == StatusCode::kMemoryLimitExceeded) {
            return std::optional<Relation>{};
          }
          return dst_or.GetStatus();
        }
        RuntimeAggState* d_ast = merged.AggsAt(dst_or.Value());
        d_ast[0].count += src.AggsAt(g)[0].count;
        d_ast[0].has_value = true;
      }
      w_tables[w].reset();
    }

    const size_t ng = merged.NumGroups();
    int64_t tau = 0;
    if (rest_max_bucket > 0) {
      if (ng < topk_cap) {
        return std::optional<Relation>{};
      }
      std::vector<int64_t> all_cnts(ng);
      for (size_t g = 0; g < ng; ++g) {
        all_cnts[g] = merged.AggsAt(g)[0].count;
      }
      std::nth_element(
        all_cnts.begin(), std::next(all_cnts.begin(),
                                    static_cast<std::ptrdiff_t>(topk_cap - 1)),
        all_cnts.end(), std::greater<int64_t>{});
      tau = all_cnts[topk_cap - 1];
      // Soundness certificate (`quotient_bucket_topk_pruning_sound`): every
      // bucket outside `heavy_bits` has total row count `<= rest_max_bucket <
      // tau`, so NO group outside `heavy_bits` can reach or tie `tau`!
      if (tau <= 0 || std::cmp_less_equal(tau, rest_max_bucket)) {
        return std::optional<Relation>{};
      }
    }

    const PartOrderKind fast_order_kind =
        ClassifyPartOrderKind(req.order_bindings, req.key_specs, req.agg_specs);
    std::vector<std::vector<SurvivorGroupRow>> single_surv(1);
    std::vector<std::unique_ptr<QueryMemoryCharge>> surv_charges(1);
    auto& out_vec = single_surv[0];
    QueryMemoryCharge& survivor_charge = SurvivorChargeFor(surv_charges, 0);

    std::vector<RuntimeKeyCell> full_keys(req.key_specs.size());
    std::vector<Value> finished_aggs(1);
    std::vector<Value> cached_keys(req.key_specs.size());

    std::vector<size_t> shortlist;
    if (near_unique && ng > topk_cap) {
      shortlist.resize(ng);
      for (size_t g = 0; g < ng; ++g) {
        shortlist[g] = g;
      }
      std::nth_element(shortlist.begin(),
                       std::next(shortlist.begin(),
                                 static_cast<std::ptrdiff_t>(topk_cap)),
                       shortlist.end(), [&](size_t a, size_t b) {
                         return merged.AggsAt(a)[0].count >
                                merged.AggsAt(b)[0].count;
                       });
      shortlist.resize(topk_cap);
    }
    const size_t emit_count = shortlist.empty() ? ng : shortlist.size();
    for (size_t i = 0; i < emit_count; ++i) {
      const size_t g = shortlist.empty() ? i : shortlist[i];
      const RuntimeAggState* ast = merged.AggsAt(g);
      if (rest_max_bucket > 0 && ast[0].count < tau) {
        continue;
      }
      km.ReconstructFull(req.key_specs, merged.KeysAt(g), full_keys.data());
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
            ob0.kind == OutputSlotBinding::Kind::kAgg ? &ast[ob0.index]
                                                      : nullptr,
            ob0.kind == OutputSlotBinding::Kind::kKey ? &full_keys[ob0.index]
                                                      : nullptr,
            &row.num_null, &row.num_int, &row.num_double));
      }
      finished_aggs[0] = Value(ast[0].count);
      std::fill(cached_keys.begin(), cached_keys.end(), Value());
      if (fast_order_kind == PartOrderKind::kNone &&
          !req.order_bindings.empty()) {
        row.order_keys.resize(req.order_bindings.size());
        for (size_t k = 0; k < req.order_bindings.size(); ++k) {
          ASSIGN_OR_RETURN(Value, ov,
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
      PushChargedSurvivor(&out_vec, &survivor_charge, std::move(row));
    }

    return FinalizePartitionedRelation(
        req, single_surv, ng, total_scanned.load(), total_matching.load(),
        merge_begin, label, &surv_charges);
  };
  if (near_unique) {
    // The estimate only chooses whether to try this layout. Every hit is
    // still checked by the full key table, including hash collisions.
    const size_t wanted = static_cast<size_t>(std::clamp<uint64_t>(
        input_bound * 8, uint64_t{1} << 22, uint64_t{1} << 30));
    const size_t bits = std::bit_ceil(wanted);
    const bool use_two_bits = req.preds.Empty();
    const size_t words = (bits / 64) * (use_two_bits ? 2 : 1);
    QueryMemoryCharge bitmap_charge;
    // NOLINTNEXTLINE(modernize-avoid-c-arrays): dynamically sized uninitialized buffer is zeroed in parallel below.
    std::unique_ptr<uint64_t[]> bitmaps;
    const auto tag = [&](uint64_t hash) {
      const size_t word = (hash >> 16) & (words - 1);
      // Keep two independent bit positions in the same memory word, so a
      // single atomic read/modify/write publishes both sightings.
      const uint64_t mask = use_two_bits
                                ? ((uint64_t{1} << (hash & 31)) |
                                   (uint64_t{1} << (32 + ((hash >> 5) & 31))))
                                : (uint64_t{1} << (hash & 63));
      return std::pair{word, mask};
    };
    // Lean: duplicate_key_retained and word_partition_sound. All sightings
    // for a word go to the same worker in the bitmap reduction.
    constexpr size_t kShards = 256;
    const size_t shard_words = words / kShards;
    QueryMemoryCharge shard_charge;
    QueryMemoryCharge replay_charge;
    std::vector<uint64_t> replay;
    const size_t replay_words = (input_bound + 63) / 64;
    constexpr size_t kTagBits = 35;
    constexpr uint64_t kTagMask = (uint64_t{1} << kTagBits) - 1;
    const bool shard_sightings =
        group_hash_jit && input_bound >= (uint64_t{1} << 22) &&
        input_bound <= (uint64_t{1} << (64 - kTagBits)) &&
        sizeof(size_t) >= 8 &&
        ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_SHARDED_DUPLICATE_SIEVE") == nullptr &&
        replay_charge.TryAdd(replay_words * sizeof(uint64_t)) &&
        shard_charge.TryAdd(
            (input_bound * 2 + workers * (kShards * 64 + 2 * shard_words)) *
            sizeof(uint64_t));
    if (shard_sightings) {
      replay.resize(replay_words);
      std::vector<std::array<std::vector<uint64_t>, kShards>> queues(workers);
      RETURN_IF_FAIL(scan_keys(
          req.candidate_blocks, true,
          [&](size_t worker, const RuntimeKeyCell*, uint64_t hash, size_t block,
              uint32_t row) {
            const size_t word = (hash >> 16) & (words - 1);
            auto& queue = queues[worker][word / shard_words];
            if (queue.size() == queue.capacity()) {
              queue.reserve(std::max<size_t>(64, 2 * queue.capacity()));
            }
            const uint64_t ordinal = block_row_starts[block] + row;
            // Lean: packet_replay_roundtrip. Preserve the 25-bit word
            // number, the two lane positions and the ordinal independently.
            queue.push_back((ordinal << kTagBits) |
                            (static_cast<uint64_t>(word) << 10) |
                            (hash & 1023));
            return Status::kSuccess;
          },
          /*leading_only=*/true));
      std::atomic<size_t> next_shard{0};
      RETURN_IF_FAIL(RunParallelTasks(workers, [&](size_t) {
        std::vector<uint64_t> seen(shard_words);
        std::vector<uint64_t> repeated(shard_words);
        const auto local_tag = [&](uint64_t packet) {
          const uint64_t compact = packet & kTagMask;
          const uint64_t hash = ((compact >> 10) << 16) | (compact & 1023);
          const auto [word, mask] = tag(hash);
          return std::pair{word & (shard_words - 1), mask};
        };
        for (size_t shard = next_shard.fetch_add(1, std::memory_order_relaxed);
             shard < kShards;
             shard = next_shard.fetch_add(1, std::memory_order_relaxed)) {
          std::ranges::fill(seen, 0);
          std::ranges::fill(repeated, 0);
          for (const auto& worker : queues) {
            for (const uint64_t packet : worker[shard]) {
              const auto [word, mask] = local_tag(packet);
              repeated[word] |= seen[word] & mask;
              seen[word] |= mask;
            }
          }
          size_t singletons = 0;
          for (const auto& worker : queues) {
            for (const uint64_t packet : worker[shard]) {
              const auto [word, mask] = local_tag(packet);
              if ((repeated[word] & mask) != mask && singletons++ >= topk_cap) {
                continue;
              }
              const uint64_t ordinal = packet >> kTagBits;
              std::atomic_ref(replay[ordinal / 64])
                  .fetch_or(uint64_t{1} << (ordinal % 64),
                            std::memory_order_relaxed);
            }
          }
        }
      }));
    } else {
      replay_charge.ReleaseAll();
      if (!bitmap_charge.TryAdd(2 * words * sizeof(uint64_t))) {
        return std::nullopt;
      }
      // NOLINTNEXTLINE(modernize-avoid-c-arrays): uninitialized dynamic buffer is zeroed in parallel.
      bitmaps = std::make_unique_for_overwrite<uint64_t[]>(2 * words);
      RETURN_IF_FAIL(RunParallelTasks(workers, [&](size_t w) {
        const size_t size = 2 * words;
        // NOLINTNEXTLINE(modernize-avoid-c-arrays): dynamically indexed raw bitmap.
        std::fill(bitmaps.get() + ((size * w) / workers),
                  bitmaps.get() + ((size * (w + 1)) / workers), uint64_t{0});
      }));
      RETURN_IF_FAIL(scan_keys(
          req.candidate_blocks, true,
          [&](size_t, const RuntimeKeyCell*, uint64_t hash) {
            const auto [word, mask] = tag(hash);
            // NOLINTNEXTLINE(modernize-avoid-c-arrays): dynamically indexed raw bitmap.
            const uint64_t old = std::atomic_ref(bitmaps[word])
                                     .fetch_or(mask, std::memory_order_relaxed);
            if ((old & mask) != 0) {
              // NOLINTNEXTLINE(modernize-avoid-c-arrays): dynamically indexed raw bitmap.
              std::atomic_ref(bitmaps[word + words])
                  .fetch_or(old & mask, std::memory_order_relaxed);
            }
            return Status::kSuccess;
          },
          /*leading_only=*/true, std::span(bitmaps.get(), words)));
    }
    shard_charge.ReleaseAll();
    std::vector<QueryMemoryCharge> charges(workers);
    auto tables = make_tables(charges);
    std::vector<size_t> singletons(workers, 0);
    const Status status = scan_keys(
        req.candidate_blocks, false,
        [&](size_t w, const RuntimeKeyCell* keys, uint64_t hash) -> Status {
          if (!shard_sightings) {
            const auto [word, mask] = tag(hash);
            // NOLINTNEXTLINE(modernize-avoid-c-arrays): dynamically indexed raw bitmap.
            const bool repeated = (bitmaps[word + words] & mask) == mask;
            // All dropped singleton groups tie on the only ORDER BY term.
            // Retain enough witnesses to supply LIMIT plus OFFSET.
            if (!repeated && singletons[w]++ >= topk_cap) {
              return Status::kSuccess;
            }
          }
          return count_group(*tables[w], keys, hash);
        },
        /*leading_only=*/false,
        shard_sightings
            ? std::span<const uint64_t>{}
            : std::span<const uint64_t>(bitmaps.get() + words, words),
        replay);
    if (status == StatusCode::kMemoryLimitExceeded) {
      return std::nullopt;
    }
    RETURN_IF_FAIL(status);
    bitmaps.reset();
    bitmap_charge.ReleaseAll();
    return finish(tables, 0,
                  shard_sightings ? "sharded-duplicate-sieve-topk"
                                  : "duplicate-sieve-topk");
  }

  std::vector<std::vector<uint32_t>> worker_buckets(
      workers, std::vector<uint32_t>(kNumBuckets, 0));
  const auto sketch = [&](size_t worker, const RuntimeKeyCell*, uint64_t hash) {
    ++worker_buckets[worker][bucket_of(hash)];
    return Status::kSuccess;
  };
  const auto sum_buckets = [&] {
    std::vector<uint32_t> result(kNumBuckets, 0);
    for (const auto& local : worker_buckets) {
      for (size_t b = 0; b < kNumBuckets; ++b) {
        result[b] += local[b];
      }
    }
    return result;
  };
  const auto choose_buckets = [&](const std::vector<uint32_t>& counts,
                                  size_t capacity) {
    auto ranked = counts;
    std::nth_element(
        ranked.begin(), std::next(ranked.begin(),
                                  static_cast<std::ptrdiff_t>(capacity - 1)),
        ranked.end(), std::greater<uint32_t>{});
    const uint32_t cutoff = ranked[capacity - 1];
    std::vector<uint64_t> mask(kNumBuckets / 64, 0);
    // Give strictly higher counts priority. Break ties by bucket index
    // only when building the speculative mask.
    size_t remaining = capacity;
    for (size_t b = 0; b < kNumBuckets; ++b) {
      if (counts[b] > cutoff) {
        mask[b >> 6U] |= uint64_t{1} << (b & 63U);
        --remaining;
      }
    }
    if (cutoff != 0) {
      for (size_t b = 0; remaining != 0 && b < kNumBuckets; ++b) {
        if (counts[b] == cutoff) {
          mask[b >> 6U] |= uint64_t{1} << (b & 63U);
          --remaining;
        }
      }
    }
    return mask;
  };
  const auto outside_max = [&](const std::vector<uint32_t>& counts,
                               std::span<const uint64_t> mask) {
    uint32_t result = 0;
    for (size_t b = 0; b < kNumBuckets; ++b) {
      if (!has_bucket(mask, b)) {
        result = std::max(result, counts[b]);
      }
    }
    return result;
  };

  // A small stratified sample proposes buckets. Its estimates only
  // choose the mask. Every count used for the result or certificate comes
  // from the full, fused sweep below.
  const bool can_seed =
      (specialize && (single_raw_varchar_key || single_raw_int_key)) ||
      try_numeric_quotient || date_bounds;
  constexpr size_t group_cap = 8192;
  QueryMemoryCharge speculation_charge;
  std::vector<uint64_t> seeded;
  if (can_seed && speculation_charge.TryAdd(4 * workers * group_cap *
                                            (sizeof(RuntimeAggState) +
                                             n_phys * sizeof(RuntimeKeyCell) +
                                             sizeof(uint64_t)))) {
    const size_t blocks = req.candidate_blocks.size();
    const size_t samples = std::clamp<size_t>(blocks / 128, 8, 256);
    std::vector<uint32_t> sample;
    sample.reserve(samples);
    for (size_t i = 0; i < samples; ++i) {
      sample.push_back(req.candidate_blocks[(i * (blocks / samples)) +
                                            std::min(i, blocks % samples)]);
    }
    if (scan_keys(sample, false, sketch).ok()) {
      auto counts = sum_buckets();
      if (!try_numeric_quotient ||
          std::ranges::count_if(counts, [](uint32_t v) { return v >= 8; }) >=
              static_cast<std::ptrdiff_t>(topk_cap)) {
        seeded = choose_buckets(counts, std::max<size_t>(128, topk_cap));
      }
    }
    for (auto& counts : worker_buckets) {
      std::ranges::fill(counts, 0);
    }
  }
  if (try_numeric_quotient && seeded.empty()) {
    return std::nullopt;
  }

  std::vector<uint32_t> global_buckets;
  if (!seeded.empty()) {
    std::vector<QueryMemoryCharge> charges(workers);
    auto tables = make_tables(charges);
    std::vector<uint8_t> complete(workers, 1);
    // Lean proof: lean/TinylambOpt/QuotientFusion.lean
    // (fused_bucket_count, fused_group_count, fused_topk_certificate)
    const auto fused = [&](size_t worker, const RuntimeKeyCell* keys,
                           uint64_t hash) -> Status {
      const size_t bucket = bucket_of(hash);
      ++worker_buckets[worker][bucket];
      if (complete[worker] != 0 && has_bucket(seeded, bucket)) {
        auto& table = *tables[worker];
        if (table.NumGroups() == group_cap ||
            !count_group(table, keys, hash).ok()) {
          // Continue building the entire sketch so the exact replay can
          // reuse it even after a speculative memory refusal.
          complete[worker] = 0;
        }
      }
      return Status::kSuccess;
    };
    const Status status = scan_keys(req.candidate_blocks, true, fused);
    if (status == StatusCode::kInvalidArgument ||
        status == StatusCode::kMemoryLimitExceeded) {
      return std::optional<Relation>{};
    }
    RETURN_IF_FAIL(status);
    global_buckets = sum_buckets();
    if (std::ranges::all_of(complete, [](uint8_t done) { return done != 0; })) {
      ASSIGN_OR_RETURN(std::optional<Relation>, result,
                       finish(tables, outside_max(global_buckets, seeded),
                              "quotient-fused-topk"));
      if (result) {
        return result;
      }
    }
  } else {
    const Status status = scan_keys(req.candidate_blocks, true, sketch);
    if (status == StatusCode::kInvalidArgument ||
        status == StatusCode::kMemoryLimitExceeded) {
      return std::optional<Relation>{};
    }
    RETURN_IF_FAIL(status);
    global_buckets = sum_buckets();
  }
  std::vector<uint64_t>().swap(seeded);
  speculation_charge.ReleaseAll();
  worker_buckets.clear();

  auto heavy_bits = choose_buckets(global_buckets, kHeavyBuckets);
  const uint32_t rest_max_bucket = outside_max(global_buckets, heavy_bits);
  if (rest_max_bucket > 0 &&
      *std::ranges::max_element(global_buckets) <= rest_max_bucket) {
    return std::optional<Relation>{};
  }
  std::vector<uint32_t>().swap(global_buckets);
  std::vector<QueryMemoryCharge> charges(workers);
  auto tables = make_tables(charges);
  const Status status = scan_keys(
      req.candidate_blocks, false,
      [&](size_t worker, const RuntimeKeyCell* keys, uint64_t hash) -> Status {
        if (!has_bucket(heavy_bits, bucket_of(hash))) {
          return Status::kSuccess;
        }
        return count_group(*tables[worker], keys, hash);
      });
  if (status == StatusCode::kInvalidArgument ||
      status == StatusCode::kMemoryLimitExceeded) {
    return std::optional<Relation>{};
  }
  RETURN_IF_FAIL(status);
  return finish(tables, rest_max_bucket, "quotient-heavy-topk");
}

// Lean proof:
// lean/TinylambOpt/ResearchProofs/UnboundedSumInt64OverflowGuard.lean (sound)
bool CanFoldAggsInParallel(
    const PartitionedColumnarAggRequest& req,
    const std::vector<std::shared_ptr<const TlcsFile::DirGroup>>& dir_groups) {
  const uint32_t dg_size = req.tlcs->DirGroupSize();
  for (const ColAggSpec& spec : req.agg_specs) {
    if (spec.kind != ColAggKind::kSumInt64) {
      continue;
    }
    __int128 max_pos_sum = 0;
    __int128 min_neg_sum = 0;
    for (const uint32_t bidx : req.candidate_blocks) {
      const auto& blk = dir_groups[bidx / dg_size]->blocks[bidx % dg_size];
      const auto& meta = blk.columns[spec.full_col];
      if (meta.encoding == TlcsColumnEncoding::kConstNull ||
          blk.row_count == 0) {
        continue;
      }
      if (!meta.has_min_max) {
        return false;
      }
      if (meta.max_int > 0) {
        max_pos_sum += static_cast<__int128>(meta.max_int) *
                       static_cast<__int128>(blk.row_count);
        if (max_pos_sum >
            static_cast<__int128>(std::numeric_limits<int64_t>::max())) {
          return false;
        }
      }
      if (meta.min_int < 0) {
        min_neg_sum += static_cast<__int128>(meta.min_int) *
                       static_cast<__int128>(blk.row_count);
        if (min_neg_sum <
            static_cast<__int128>(std::numeric_limits<int64_t>::min())) {
          return false;
        }
      }
    }
  }
  return true;
}

Status BuildSurvivorRowFromStates(const PartitionedColumnarAggRequest& req,
                                  PartOrderKind fast_order_kind,
                                  const RuntimeKeyCell* full_keys,
                                  const RuntimeAggState* agg_states,
                                  std::vector<Value>* finished_aggs,
                                  std::vector<Value>* cached_keys,
                                  SurvivorGroupRow* row_out) {
  row_out->order_keys.clear();
  row_out->emit_vals.clear();
  if (fast_order_kind != PartOrderKind::kNone) {
    const auto& ob0 = req.order_bindings[0].binding;
    RETURN_IF_FAIL(EvaluateNumericOrderValue(
        ob0,
        ob0.kind == OutputSlotBinding::Kind::kAgg ? &req.agg_specs[ob0.index]
                                                  : nullptr,
        ob0.kind == OutputSlotBinding::Kind::kKey ? &req.key_specs[ob0.index]
                                                  : nullptr,
        ob0.kind == OutputSlotBinding::Kind::kAgg ? &agg_states[ob0.index]
                                                  : nullptr,
        ob0.kind == OutputSlotBinding::Kind::kKey ? &full_keys[ob0.index]
                                                  : nullptr,
        &row_out->num_null, &row_out->num_int, &row_out->num_double));
  }
  for (size_t a = 0; a < req.agg_specs.size(); ++a) {
    ASSIGN_OR_RETURN(Value, fv,
                     FinishAggState(req.agg_specs[a], agg_states[a]));
    (*finished_aggs)[a] = std::move(fv);
  }
  std::fill(cached_keys->begin(), cached_keys->end(), Value());
  if (fast_order_kind == PartOrderKind::kNone && !req.order_bindings.empty()) {
    row_out->order_keys.resize(req.order_bindings.size());
    for (size_t k = 0; k < req.order_bindings.size(); ++k) {
      ASSIGN_OR_RETURN(
          Value, ov,
          EvaluateBindingValue(req.order_bindings[k].binding, req.key_specs,
                               full_keys, *finished_aggs, cached_keys));
      row_out->order_keys[k] = std::move(ov);
    }
  }
  row_out->emit_vals.reserve(req.emit_bindings.size());
  for (const auto& eb : req.emit_bindings) {
    ASSIGN_OR_RETURN(Value, ev,
                     EvaluateBindingValue(eb, req.key_specs, full_keys,
                                          *finished_aggs, cached_keys));
    row_out->emit_vals.push_back(std::move(ev));
  }
  return Status::kSuccess;
}

struct TransparentStringViewHash {
  using is_transparent = void;
  size_t operator()(std::string_view sv) const noexcept {
    return static_cast<size_t>(HashView64(sv));
  }
};

struct VarCharLocalGroupTable {
  QueryMemoryCharge charge;
  PagedStringArena arena{&charge};
  bool has_null{false};
  std::vector<RuntimeAggState> null_aggs;
  // Lean proof:
  // lean/TinylambOpt/ResearchProofs/EmptyStringKeyPartitionSplit.lean (sound)
  // Dedicated O(1) slot for empty string "" bypassing hash/pack7 lookups.
  bool has_empty{false};
  uint32_t empty_gid{0};
  FlatIntKeyMap<uint32_t> short_map;
  std::unordered_map<std::string_view, uint32_t, TransparentStringViewHash,
                     std::equal_to<>>
      long_map;
  std::vector<std::string_view> keys;
  std::vector<uint64_t> hashes;
  std::vector<RuntimeAggState> aggs;
  size_t max_groups{65536};
  bool freeze_new_keys{false};
  bool bail{false};

  RuntimeAggState* GetOrCreateNull(size_t naggs) {
    if (!has_null) {
      const size_t need = (naggs * sizeof(RuntimeAggState)) + 64;
      if (!charge.TryAdd(need)) {
        bail = true;
        return nullptr;
      }
      null_aggs.clear();
      null_aggs.resize(naggs);
      has_null = true;
    }
    return null_aggs.data();
  }

  std::optional<uint32_t> FindOrInsertKey(std::string_view sv, size_t naggs) {
    if (sv.empty()) {
      if (has_empty) {
        return empty_gid;
      }
      if (freeze_new_keys) {
        return std::nullopt;
      }
      const size_t new_gid = keys.size();
      if (new_gid >= max_groups) {
        bail = true;
        return std::nullopt;
      }
      const size_t row_bytes = sizeof(std::string_view) + sizeof(uint64_t) +
                               (naggs * sizeof(RuntimeAggState)) + 48;
      if (!charge.TryAdd(row_bytes)) {
        bail = true;
        return std::nullopt;
      }
      empty_gid = static_cast<uint32_t>(new_gid);
      has_empty = true;
      keys.emplace_back();
      hashes.push_back(HashView64(std::string_view{}));
      if (naggs > 0) {
        aggs.resize((new_gid + 1) * naggs);
      }
      return empty_gid;
    }
    if (const auto packed = PackShortString7(sv)) {
      const auto key = static_cast<int64_t>(*packed);
      if (const uint32_t* found = short_map.Find(key)) {
        return *found;
      }
      if (freeze_new_keys) {
        return std::nullopt;
      }
      const size_t new_gid = keys.size();
      if (new_gid >= max_groups) {
        bail = true;
        return std::nullopt;
      }
      const size_t row_bytes = sizeof(std::string_view) + sizeof(uint64_t) +
                               (naggs * sizeof(RuntimeAggState)) + 48;
      if (!charge.TryAdd(row_bytes)) {
        bail = true;
        return std::nullopt;
      }
      auto interned = arena.Intern(sv);
      if (!interned.HasValue()) {
        bail = true;
        return std::nullopt;
      }
      bool inserted = false;
      uint32_t* slot = short_map.TryEmplace(key, &inserted);
      *slot = static_cast<uint32_t>(new_gid);
      keys.push_back(interned.Value());
      hashes.push_back(GroupKeySliceHasher::HashSingleVarChar(sv));
      aggs.resize((new_gid + 1) * naggs);
      return static_cast<uint32_t>(new_gid);
    }

    auto it = long_map.find(sv);
    if (it != long_map.end()) {
      return it->second;
    }
    const size_t new_gid = keys.size();
    if (new_gid >= max_groups) {
      bail = true;
      return std::nullopt;
    }
    const size_t row_bytes = sizeof(std::string_view) + sizeof(uint64_t) +
                             (naggs * sizeof(RuntimeAggState)) + 64;
    if (!charge.TryAdd(row_bytes)) {
      bail = true;
      return std::nullopt;
    }
    auto interned = arena.Intern(sv);
    if (!interned.HasValue()) {
      bail = true;
      return std::nullopt;
    }
    const std::string_view perm = interned.Value();
    long_map.emplace(perm, static_cast<uint32_t>(new_gid));
    keys.push_back(perm);
    hashes.push_back(GroupKeySliceHasher::HashSingleVarChar(sv));
    aggs.resize((new_gid + 1) * naggs);
    return static_cast<uint32_t>(new_gid);
  }

  RuntimeAggState* GetOrCreate(std::string_view sv, size_t naggs) {
    const auto gid = FindOrInsertKey(sv, naggs);
    return gid ? (aggs.data() + (static_cast<size_t>(*gid) * naggs)) : nullptr;
  }
};

}  // namespace

StatusOr<std::optional<Relation>> TryRunDirectTwoKeyBoundedIntAggregate(
    const PartitionedColumnarAggRequest& req,
    const KeyProjectionMap* precomputed_km) {
  // Lean proof:
  // lean/TinylambOpt/ResearchProofs/DirectTwoKeyArrayIndexBijection.lean
  // (sound)
  if (::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_TWO_KEY_DIRECT_AGG") != nullptr ||
      req.candidate_blocks.empty() || req.key_specs.size() != 2 ||
      req.stmt.Distinct() || req.any_scalar_distinct) {
    return std::optional<Relation>{};
  }
  const auto valid_int_key = [&](const ColGroupKeySpec& ks) {
    return (ks.kind == ColKeyKind::kColInt64 ||
            ks.kind == ColKeyKind::kColDate) &&
           ks.full_col < req.table_schema.ColumnCount() &&
           !req.table_schema.GetColumn(ks.full_col).IsUnsigned();
  };
  if (!valid_int_key(req.key_specs[0]) || !valid_int_key(req.key_specs[1])) {
    return std::optional<Relation>{};
  }
  for (const ColAggSpec& as : req.agg_specs) {
    if (IsDistinctAggKind(as.kind) || as.double_arith != nullptr) {
      return std::optional<Relation>{};
    }
  }
  std::optional<KeyProjectionMap> owned_km;
  const KeyProjectionMap* km_ptr = precomputed_km;
  if (km_ptr == nullptr) {
    ASSIGN_OR_RETURN(
        KeyProjectionMap, built_km,
        BuildKeyProjectionMap(*req.tlcs, req.candidate_blocks, req.key_specs));
    owned_km.emplace(std::move(built_km));
    km_ptr = &owned_km.value();
  }
  const KeyProjectionMap& km = *km_ptr;
  if (km.phys_specs.size() != 2) {
    return std::optional<Relation>{};
  }

  const uint32_t dg_count = req.tlcs->DirGroupCount();
  const uint32_t dg_size = req.tlcs->DirGroupSize();
  std::vector<std::shared_ptr<const TlcsFile::DirGroup>> dir_groups(dg_count);
  for (uint32_t g = 0; g < dg_count; ++g) {
    ASSIGN_OR_RETURN(std::shared_ptr<const TlcsFile::DirGroup>, grp,
                     req.tlcs->GetDirGroup(g));
    dir_groups[g] = std::move(grp);
  }

  const slot_t col0 = req.key_specs[0].full_col;
  const slot_t col1 = req.key_specs[1].full_col;
  int64_t min0 = std::numeric_limits<int64_t>::max();
  int64_t max0 = std::numeric_limits<int64_t>::min();
  bool nullable0 = false;
  bool saw_val0 = false;
  int64_t min1 = std::numeric_limits<int64_t>::max();
  int64_t max1 = std::numeric_limits<int64_t>::min();
  bool nullable1 = false;
  bool saw_val1 = false;

  for (const uint32_t bidx : req.candidate_blocks) {
    const auto& blk = dir_groups[bidx / dg_size]->blocks[bidx % dg_size];
    if (blk.row_count == 0) {
      continue;
    }
    const auto& m0 = blk.columns[col0];
    if (m0.encoding == TlcsColumnEncoding::kConstNull) {
      nullable0 = true;
    } else if (!m0.has_min_max) {
      return std::optional<Relation>{};
    } else {
      if (m0.has_null) {
        nullable0 = true;
      }
      min0 = std::min(min0, m0.min_int);
      max0 = std::max(max0, m0.max_int);
      saw_val0 = true;
    }
    const auto& m1 = blk.columns[col1];
    if (m1.encoding == TlcsColumnEncoding::kConstNull) {
      nullable1 = true;
    } else if (!m1.has_min_max) {
      return std::optional<Relation>{};
    } else {
      if (m1.has_null) {
        nullable1 = true;
      }
      min1 = std::min(min1, m1.min_int);
      max1 = std::max(max1, m1.max_int);
      saw_val1 = true;
    }
  }
  constexpr size_t kMaxTwoKeySlots = 262144;
  if (saw_val0 && (static_cast<uint64_t>(max0) - static_cast<uint64_t>(min0)) >=
                      kMaxTwoKeySlots) {
    return std::optional<Relation>{};
  }
  if (saw_val1 && (static_cast<uint64_t>(max1) - static_cast<uint64_t>(min1)) >=
                      kMaxTwoKeySlots) {
    return std::optional<Relation>{};
  }
  const size_t nval0 = saw_val0
                           ? static_cast<size_t>((static_cast<uint64_t>(max0) -
                                                  static_cast<uint64_t>(min0)) +
                                                 1ULL)
                           : 0;
  const size_t nval1 = saw_val1
                           ? static_cast<size_t>((static_cast<uint64_t>(max1) -
                                                  static_cast<uint64_t>(min1)) +
                                                 1ULL)
                           : 0;
  const size_t s0 = nval0 + (nullable0 ? 1 : 0);
  const size_t s1 = nval1 + (nullable1 ? 1 : 0);
  if (s0 == 0 || s1 == 0 || s0 > kMaxTwoKeySlots || s1 > kMaxTwoKeySlots / s0) {
    return std::optional<Relation>{};
  }
  const size_t total_slots = s0 * s1;
  const size_t naggs = req.agg_specs.size();
  // Lean proofs:
  //   - lean/TinylambOpt/ResearchProofs/BoundedMultiKeyDirectArrayAggregate.lean
  //     (sound)
  //   - lean/TinylambOpt/ResearchProofs/Wave8H2o.lean (EpochAverage_sound)
  //   - lean/TinylambOpt/ResearchProofs/TwoLevelPartitionedAggMerge.lean
  //     (sound)
  // For single-agg COUNT(*) / AVG / SUM(double) over bounded 2-key integer
  // grids (H2, H4), use a reusable 24-byte generation-stamped slot buffer per
  // worker (zero per-query memset, ~8x smaller footprint -> full parallelism)
  // merged across disjoint slot stripes in parallel.
  const bool use_compact_epoch =
      naggs == 1 &&
      (req.agg_specs[0].kind == ColAggKind::kCountStar ||
       req.agg_specs[0].kind == ColAggKind::kAvgDouble ||
       req.agg_specs[0].kind == ColAggKind::kSumDouble) &&
      ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_EPOCH_TWO_KEY_AGG") == nullptr;

  struct CompactEpochSlot {
    uint32_t stamp{0};
    uint64_t count{0};
    double sum{0.0};
  };
  struct CompactEpochBuffer {
    uint32_t epoch{0};
    std::vector<CompactEpochSlot> slots;
  };
  static std::mutex epoch_pool_mu;
  static std::vector<std::unique_ptr<CompactEpochBuffer>> epoch_pool;

  const size_t bytes_per_worker =
      use_compact_epoch
          ? total_slots * sizeof(CompactEpochSlot)
          : total_slots * (sizeof(uint8_t) + naggs * sizeof(RuntimeAggState));
  const bool parallel_ok = CanFoldAggsInParallel(req, dir_groups);
  const size_t max_hw =
      QueryParallelWorkerLimit(std::thread::hardware_concurrency());
  const size_t mem_capped_workers = std::max<size_t>(
      1, (size_t{32} << 20) / std::max<size_t>(1, bytes_per_worker));
  const size_t workers_used =
      parallel_ok
          ? std::max<size_t>(
                1, std::min<size_t>({max_hw, req.candidate_blocks.size(),
                                     mem_capped_workers, size_t{16}}))
          : 1;

  struct alignas(64) TwoKeyWorkerState {
    QueryMemoryCharge charge;
    PagedStringArena arena{&charge};
    std::vector<uint8_t> present;
    std::vector<RuntimeAggState> aggs;
    std::unique_ptr<CompactEpochBuffer> epoch_buf;
    bool epoch_active{false};
    uint64_t scanned{0};
    uint64_t matching{0};
    Status err{Status::kSuccess};
  };
  std::vector<TwoKeyWorkerState> states(workers_used);
  struct EpochPoolReturnGuard {
    explicit EpochPoolReturnGuard(std::vector<TwoKeyWorkerState>* states)
        : states_ptr(states) {}
    EpochPoolReturnGuard(const EpochPoolReturnGuard&) = delete;
    EpochPoolReturnGuard& operator=(const EpochPoolReturnGuard&) = delete;
    EpochPoolReturnGuard(EpochPoolReturnGuard&&) = delete;
    EpochPoolReturnGuard& operator=(EpochPoolReturnGuard&&) = delete;
    std::vector<TwoKeyWorkerState>* states_ptr{nullptr};
    ~EpochPoolReturnGuard() {
      if (states_ptr == nullptr) {
        return;
      }
      std::scoped_lock lock(epoch_pool_mu);
      for (auto& st : *states_ptr) {
        if (st.epoch_buf && epoch_pool.size() < 16) {
          epoch_pool.push_back(std::move(st.epoch_buf));
        }
      }
    }
  } epoch_return_guard{use_compact_epoch ? &states : nullptr};
  if (use_compact_epoch) {
    std::scoped_lock lock(epoch_pool_mu);
    for (size_t w = 0; w < workers_used; ++w) {
      if (!epoch_pool.empty()) {
        states[w].epoch_buf = std::move(epoch_pool.back());
        epoch_pool.pop_back();
      } else {
        states[w].epoch_buf = std::make_unique<CompactEpochBuffer>();
      }
    }
  }
  std::atomic<size_t> next_blk{0};
  std::atomic<bool> budget_fallback{false};

  const ColumnarStageSchedule sched = BuildColumnarStageSchedule(
      req.table_schema, req.preds, req.key_specs, req.agg_specs);
  const size_t ncols = req.table_schema.ColumnCount();

  RETURN_IF_FAIL(RunParallelTasks(workers_used, [&](size_t w) {
    TwoKeyWorkerState& wst = states[w];
    BlockDecodeScratch scratch;
    std::vector<const TlcsFile::DecodedColumnChunk*> col_ptrs(ncols, nullptr);
    std::vector<uint32_t> cand_rows;

    for (;;) {
      if (budget_fallback.load(std::memory_order_relaxed)) {
        return;
      }
      const size_t bi = next_blk.fetch_add(1, std::memory_order_relaxed);
      if (bi >= req.candidate_blocks.size()) {
        return;
      }
      const uint32_t bidx = req.candidate_blocks[bi];
      uint64_t bscanned = 0;
      auto pass_or = DecodeAndFilterBlock(*req.tlcs, bidx, req.table_schema,
                                          req.preds, sched, &scratch, &col_ptrs,
                                          &cand_rows, &bscanned);
      wst.scanned += bscanned;
      if (!pass_or.HasValue()) {
        wst.err = pass_or.GetStatus();
        return;
      }
      if (!pass_or.Value() || cand_rows.empty()) {
        continue;
      }
      if (use_compact_epoch) {
        if (!wst.epoch_active) {
          if (!wst.charge.TryAdd(bytes_per_worker)) {
            budget_fallback.store(true, std::memory_order_relaxed);
            return;
          }
          CompactEpochBuffer& eb = *wst.epoch_buf;
          if (eb.slots.size() < total_slots ||
              eb.epoch == std::numeric_limits<uint32_t>::max()) {
            eb.slots.assign(total_slots, CompactEpochSlot{});
            eb.epoch = 1;
          } else {
            ++eb.epoch;
          }
          wst.epoch_active = true;
        }
        wst.matching += cand_rows.size();
        const auto* kch0 = col_ptrs[col0];
        const auto* kch1 = col_ptrs[col1];
        CompactEpochSlot* __restrict__ eslots = wst.epoch_buf->slots.data();
        const uint32_t ep = wst.epoch_buf->epoch;
        const ColAggKind ak = req.agg_specs[0].kind;
        if (ak == ColAggKind::kCountStar) {
          if (!kch0->has_null && !kch1->has_null) {
            const int64_t* __restrict__ c0 = kch0->int_cells.data();
            const int64_t* __restrict__ c1 = kch1->int_cells.data();
            for (const uint32_t r : cand_rows) {
              const size_t gi =
                  ((static_cast<size_t>(static_cast<uint64_t>(c0[r]) - static_cast<uint64_t>(min0)) * s1) + static_cast<size_t>(static_cast<uint64_t>(c1[r]) - static_cast<uint64_t>(min1)));
              CompactEpochSlot& slot = eslots[gi];
              if (slot.stamp != ep) {
                slot.stamp = ep;
                slot.count = 1;
              } else {
                ++slot.count;
              }
            }
          } else {
            for (const uint32_t r : cand_rows) {
              const size_t idx0 =
                  kch0->IsNull(r)
                      ? nval0
                      : static_cast<size_t>(
                            static_cast<uint64_t>(kch0->int_cells[r]) -
                            static_cast<uint64_t>(min0));
              const size_t idx1 =
                  kch1->IsNull(r)
                      ? nval1
                      : static_cast<size_t>(
                            static_cast<uint64_t>(kch1->int_cells[r]) -
                            static_cast<uint64_t>(min1));
              CompactEpochSlot& slot = eslots[(idx0 * s1) + idx1];
              if (slot.stamp != ep) {
                slot.stamp = ep;
                slot.count = 1;
              } else {
                ++slot.count;
              }
            }
          }
          continue;
        }
        const auto* vch = col_ptrs[req.agg_specs[0].full_col];
        if (vch == nullptr || (vch->type != ValueType::kInt64 &&
                               vch->type != ValueType::kDouble)) {
          budget_fallback.store(true, std::memory_order_relaxed);
          return;
        }
        const bool is_int_v = (vch->type == ValueType::kInt64);
        if (!kch0->has_null && !kch1->has_null && !vch->has_null) {
          const int64_t* __restrict__ c0 = kch0->int_cells.data();
          const int64_t* __restrict__ c1 = kch1->int_cells.data();
          if (is_int_v) {
            const int64_t* __restrict__ vals = vch->int_cells.data();
            for (const uint32_t r : cand_rows) {
              const size_t gi =
                  ((static_cast<size_t>(static_cast<uint64_t>(c0[r]) - static_cast<uint64_t>(min0)) * s1) + static_cast<size_t>(static_cast<uint64_t>(c1[r]) - static_cast<uint64_t>(min1)));
              CompactEpochSlot& slot = eslots[gi];
              if (slot.stamp != ep) {
                slot.stamp = ep;
                slot.count = 1;
                slot.sum = static_cast<double>(vals[r]);
              } else {
                ++slot.count;
                slot.sum += static_cast<double>(vals[r]);
              }
            }
          } else {
            const int64_t* __restrict__ raw_vals = vch->int_cells.data();
            for (const uint32_t r : cand_rows) {
              const size_t gi =
                  ((static_cast<size_t>(static_cast<uint64_t>(c0[r]) - static_cast<uint64_t>(min0)) * s1) + static_cast<size_t>(static_cast<uint64_t>(c1[r]) - static_cast<uint64_t>(min1)));
              const auto dv = std::bit_cast<double>(raw_vals[r]);
              CompactEpochSlot& slot = eslots[gi];
              if (slot.stamp != ep) {
                slot.stamp = ep;
                slot.count = 1;
                slot.sum = dv;
              } else {
                ++slot.count;
                slot.sum += dv;
              }
            }
          }
        } else {
          for (const uint32_t r : cand_rows) {
            const size_t idx0 =
                kch0->IsNull(r)
                    ? nval0
                    : static_cast<size_t>(
                          static_cast<uint64_t>(kch0->int_cells[r]) -
                          static_cast<uint64_t>(min0));
            const size_t idx1 =
                kch1->IsNull(r)
                    ? nval1
                    : static_cast<size_t>(
                          static_cast<uint64_t>(kch1->int_cells[r]) -
                          static_cast<uint64_t>(min1));
            CompactEpochSlot& slot = eslots[(idx0 * s1) + idx1];
            if (slot.stamp != ep) {
              slot.stamp = ep;
              slot.count = 0;
              slot.sum = 0.0;
            }
            if (!vch->IsNull(r)) {
              ++slot.count;
              slot.sum += is_int_v ? static_cast<double>(vch->int_cells[r])
                                 : std::bit_cast<double>(vch->int_cells[r]);
            }
          }
        }
        continue;
      }
      if (wst.present.empty()) {
        if (!wst.charge.TryAdd(bytes_per_worker)) {
          budget_fallback.store(true, std::memory_order_relaxed);
          return;
        }
        wst.present.assign(total_slots, 0);
        if (naggs > 0) {
          wst.aggs.clear();
          wst.aggs.resize(total_slots * naggs);
        }
      }
      wst.matching += cand_rows.size();
      const auto* kch0 = col_ptrs[col0];
      const auto* kch1 = col_ptrs[col1];
      uint8_t* __restrict__ pres = wst.present.data();
      RuntimeAggState* __restrict__ aggs = wst.aggs.data();

      if (!kch0->has_null && !kch1->has_null && naggs == 1 &&
          req.agg_specs[0].kind == ColAggKind::kCountStar) {
        const int64_t* __restrict__ c0 = kch0->int_cells.data();
        const int64_t* __restrict__ c1 = kch1->int_cells.data();
        for (const uint32_t r : cand_rows) {
          const size_t gi = ((static_cast<size_t>(static_cast<uint64_t>(c0[r]) - static_cast<uint64_t>(min0)) * s1) + static_cast<size_t>(static_cast<uint64_t>(c1[r]) - static_cast<uint64_t>(min1)));
          pres[gi] = 1;
          ++aggs[gi].count;
          aggs[gi].has_value = true;
        }
        continue;
      }
      if (!kch0->has_null && !kch1->has_null && naggs == 1 &&
          (req.agg_specs[0].kind == ColAggKind::kAvgDouble ||
           req.agg_specs[0].kind == ColAggKind::kSumDouble)) {
        const auto* vch = col_ptrs[req.agg_specs[0].full_col];
        if (vch != nullptr && vch->type == ValueType::kInt64 &&
            !vch->has_null) {
          const int64_t* __restrict__ c0 = kch0->int_cells.data();
          const int64_t* __restrict__ c1 = kch1->int_cells.data();
          const int64_t* __restrict__ vals = vch->int_cells.data();
          for (const uint32_t r : cand_rows) {
            const size_t gi = ((static_cast<size_t>(static_cast<uint64_t>(c0[r]) - static_cast<uint64_t>(min0)) * s1) + static_cast<size_t>(static_cast<uint64_t>(c1[r]) - static_cast<uint64_t>(min1)));
            pres[gi] = 1;
            ++aggs[gi].count;
            aggs[gi].double_val += static_cast<double>(vals[r]);
            aggs[gi].has_value = true;
          }
          continue;
        }
      }

      for (const uint32_t r : cand_rows) {
        const size_t idx0 =
            kch0->IsNull(r) ? nval0
                            : static_cast<size_t>(
                                  static_cast<uint64_t>(kch0->int_cells[r]) -
                                  static_cast<uint64_t>(min0));
        const size_t idx1 =
            kch1->IsNull(r) ? nval1
                            : static_cast<size_t>(
                                  static_cast<uint64_t>(kch1->int_cells[r]) -
                                  static_cast<uint64_t>(min1));
        const size_t gi = (idx0 * s1) + idx1;
        pres[gi] = 1;
        for (size_t a = 0; a < naggs; ++a) {
          Status fst = FoldRowIntoAggState(req.agg_specs[a], col_ptrs, r,
                                           aggs[(gi * naggs) + a], wst.arena);
          if (fst != Status::kSuccess) {
            if (fst == StatusCode::kMemoryLimitExceeded) {
              budget_fallback.store(true, std::memory_order_relaxed);
              return;
            }
            wst.err = std::move(fst);
            return;
          }
        }
      }
    }
  }));

  if (budget_fallback.load(std::memory_order_relaxed)) {
    return std::optional<Relation>{};
  }
  uint64_t total_scanned = 0;
  uint64_t total_matching = 0;
  std::vector<size_t> active_workers;
  for (size_t w = 0; w < workers_used; ++w) {
    RETURN_IF_FAIL(states[w].err);
    total_scanned += states[w].scanned;
    total_matching += states[w].matching;
    if (use_compact_epoch ? states[w].epoch_active
                          : !states[w].present.empty()) {
      active_workers.push_back(w);
    }
  }

  const auto merge_begin = std::chrono::steady_clock::now();
  const PartOrderKind fast_order_kind =
      ClassifyPartOrderKind(req.order_bindings, req.key_specs, req.agg_specs);
  const size_t emit_shards =
      (active_workers.empty() || total_slots < 2048 || max_hw <= 1)
          ? 1
          : std::min<size_t>(max_hw, 16);
  std::vector<std::vector<SurvivorGroupRow>> shard_survivors(emit_shards);
  std::vector<std::unique_ptr<QueryMemoryCharge>> surv_charges(emit_shards);
  std::vector<Status> emit_status(emit_shards, Status::kSuccess);
  std::atomic<size_t> total_groups{0};
  std::atomic<bool> merge_budget_fallback{false};

  RETURN_IF_FAIL(RunParallelTasks(emit_shards, [&](size_t s) {
    if (active_workers.empty()) {
      return;
    }
    const size_t idx_begin = (total_slots * s) / emit_shards;
    const size_t idx_end = (total_slots * (s + 1)) / emit_shards;
    QueryMemoryCharge merge_charge;
    PagedStringArena merge_arena(&merge_charge);
    QueryMemoryCharge& surv_charge = SurvivorChargeFor(surv_charges, s);
    ChunkedCharge surv_batch(&surv_charge);
    auto& out_vec = shard_survivors[s];
    std::vector<RuntimeAggState> merged_aggs(naggs);
    std::vector<Value> having_scratch(naggs);
    std::vector<Value> finished_aggs(naggs);
    std::vector<Value> cached_keys(2);
    std::array<RuntimeKeyCell, 2> full_keys{};
    size_t local_groups = 0;

    for (size_t idx = idx_begin; idx < idx_end; ++idx) {
      if (use_compact_epoch) {
        bool any = false;
        uint64_t total_cnt = 0;
        double total_sum = 0.0;
        for (const size_t w : active_workers) {
          const CompactEpochBuffer& eb = *states[w].epoch_buf;
          const CompactEpochSlot& slot = eb.slots[idx];
          if (slot.stamp == eb.epoch) {
            any = true;
            total_cnt += slot.count;
            total_sum += slot.sum;
          }
        }
        if (!any) {
          continue;
        }
        ++local_groups;
        merged_aggs[0] = RuntimeAggState{};
        merged_aggs[0].count = total_cnt;
        merged_aggs[0].double_val = total_sum;
        merged_aggs[0].has_value =
            (req.agg_specs[0].kind == ColAggKind::kCountStar) ? true
                                                              : (total_cnt > 0);
      } else {
        bool any = false;
        for (const size_t w : active_workers) {
          if (states[w].present[idx] != 0) {
            any = true;
            break;
          }
        }
        if (!any) {
          continue;
        }
        ++local_groups;
        for (size_t a = 0; a < naggs; ++a) {
          merged_aggs[a] = RuntimeAggState{};
        }
        for (const size_t w : active_workers) {
          if (states[w].present[idx] == 0) {
            continue;
          }
          RuntimeAggState* src_aggs = states[w].aggs.data() + (idx * naggs);
          for (size_t a = 0; a < naggs; ++a) {
            Status mst = MergeAggState(req.agg_specs[a], merged_aggs[a],
                                       src_aggs[a], merge_arena);
            if (mst != Status::kSuccess) {
              if (mst == StatusCode::kMemoryLimitExceeded) {
                merge_budget_fallback.store(true, std::memory_order_relaxed);
                return;
              }
              emit_status[s] = std::move(mst);
              return;
            }
          }
        }
      }
      auto h_or =
          EvalPositionalHavingOnStates(req.positional_having, req.agg_specs,
                                       merged_aggs.data(), &having_scratch);
      if (!h_or.HasValue()) {
        emit_status[s] = h_or.GetStatus();
        return;
      }
      if (!h_or.Value()) {
        continue;
      }
      const size_t idx0 = idx / s1;
      const size_t idx1 = idx % s1;
      full_keys[0] = RuntimeKeyCell{};
      if (idx0 == nval0) {
        full_keys[0].is_null = true;
      } else {
        full_keys[0].int_val = min0 + static_cast<int64_t>(idx0);
      }
      full_keys[1] = RuntimeKeyCell{};
      if (idx1 == nval1) {
        full_keys[1].is_null = true;
      } else {
        full_keys[1].int_val = min1 + static_cast<int64_t>(idx1);
      }
      SurvivorGroupRow row;
      Status bst = BuildSurvivorRowFromStates(
          req, fast_order_kind, full_keys.data(), merged_aggs.data(),
          &finished_aggs, &cached_keys, &row);
      if (bst != Status::kSuccess) {
        emit_status[s] = std::move(bst);
        return;
      }
      PushChargedSurvivor(&out_vec, &surv_batch, std::move(row));
    }
    total_groups.fetch_add(local_groups, std::memory_order_relaxed);
  }));

  if (merge_budget_fallback.load(std::memory_order_relaxed)) {
    return std::optional<Relation>{};
  }
  for (const Status& est : emit_status) {
    RETURN_IF_FAIL(est);
  }
  return FinalizePartitionedRelation(req, shard_survivors, total_groups.load(),
                                     total_scanned, total_matching, merge_begin,
                                     "two-key-direct", &surv_charges);
}

StatusOr<std::optional<Relation>> TryRunSingleVarCharLocalDictAggregate(
    const PartitionedColumnarAggRequest& req,
    const KeyProjectionMap* precomputed_km) {
  // Lean proofs:
  //   - lean/TinylambOpt/ResearchProofs/LocalDictTwoStageAggEquiv.lean (sound)
  //   - lean/TinylambOpt/ResearchProofs/MultiBlockLocalDictTwoStageAggregate.lean
  //     (sound)
  //   - lean/TinylambOpt/ResearchProofs/ShortStringPrefix7Bijection.lean
  //   (sound)
  //   - lean/TinylambOpt/ResearchProofs/UnboundedSumInt64OverflowGuard.lean
  //   (sound)
  if (::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_LOCAL_DICT_TWO_STAGE_AGG") != nullptr ||
      req.candidate_blocks.empty() || req.candidate_blocks.size() > 256 ||
      req.key_specs.size() != 1 ||
      req.key_specs[0].kind != ColKeyKind::kColVarChar || req.stmt.Distinct() ||
      req.any_scalar_distinct || req.stmt.GroupBy().size() != 1 ||
      CollationOfExpression(req.stmt.GroupBy()[0]) ==
          Collation::kCaseInsensitive) {
    return std::optional<Relation>{};
  }
  const slot_t key_col = req.key_specs[0].full_col;
  if (key_col >= req.table_schema.ColumnCount() ||
      req.table_schema.GetColumn(key_col).IsUnsigned()) {
    return std::optional<Relation>{};
  }
  for (const ColAggSpec& as : req.agg_specs) {
    if (IsDistinctAggKind(as.kind) || as.double_arith != nullptr) {
      return std::optional<Relation>{};
    }
  }
  std::optional<KeyProjectionMap> owned_km;
  const KeyProjectionMap* km_ptr = precomputed_km;
  if (km_ptr == nullptr) {
    ASSIGN_OR_RETURN(
        KeyProjectionMap, built_km,
        BuildKeyProjectionMap(*req.tlcs, req.candidate_blocks, req.key_specs));
    owned_km.emplace(std::move(built_km));
    km_ptr = &owned_km.value();
  }
  const KeyProjectionMap& km = *km_ptr;
  if (km.phys_specs.size() != 1) {
    return std::optional<Relation>{};
  }

  const uint32_t dg_count = req.tlcs->DirGroupCount();
  std::vector<std::shared_ptr<const TlcsFile::DirGroup>> dir_groups(dg_count);
  for (uint32_t g = 0; g < dg_count; ++g) {
    ASSIGN_OR_RETURN(std::shared_ptr<const TlcsFile::DirGroup>, grp,
                     req.tlcs->GetDirGroup(g));
    dir_groups[g] = std::move(grp);
  }
  const bool parallel_ok = CanFoldAggsInParallel(req, dir_groups);
  const size_t max_hw =
      QueryParallelWorkerLimit(std::thread::hardware_concurrency());
  const size_t workers_used =
      parallel_ok
          ? std::max<size_t>(
                1, std::min<size_t>(
                       {max_hw, req.candidate_blocks.size(), size_t{16}}))
          : 1;

  struct WorkerState {
    VarCharLocalGroupTable table;
    uint64_t scanned{0};
    uint64_t matching{0};
    Status err{Status::kSuccess};
  };
  std::vector<WorkerState> workers(workers_used);
  std::atomic<size_t> next_blk{0};
  std::atomic<bool> bail_fallback{false};

  const ColumnarStageSchedule sched = BuildColumnarStageSchedule(
      req.table_schema, req.preds, req.key_specs, req.agg_specs);
  const size_t ncols = req.table_schema.ColumnCount();
  const size_t naggs = req.agg_specs.size();

  RETURN_IF_FAIL(RunParallelTasks(workers_used, [&](size_t w) {
    WorkerState& ws = workers[w];
    BlockDecodeScratch scratch;
    std::vector<const TlcsFile::DecodedColumnChunk*> col_ptrs(ncols, nullptr);
    std::vector<uint32_t> cand_rows;
    std::vector<uint8_t> local_seen;
    std::vector<RuntimeAggState> local_aggs;
    bool has_last = false;
    std::string_view last_sv;
    uint32_t last_gid = 0;

    for (;;) {
      if (bail_fallback.load(std::memory_order_relaxed)) {
        return;
      }
      const size_t bi = next_blk.fetch_add(1, std::memory_order_relaxed);
      if (bi >= req.candidate_blocks.size()) {
        return;
      }
      const uint32_t bidx = req.candidate_blocks[bi];
      uint64_t bscanned = 0;
      auto pass_or = DecodeAndFilterBlock(*req.tlcs, bidx, req.table_schema,
                                          req.preds, sched, &scratch, &col_ptrs,
                                          &cand_rows, &bscanned);
      ws.scanned += bscanned;
      if (!pass_or.HasValue()) {
        ws.err = pass_or.GetStatus();
        return;
      }
      if (!pass_or.Value() || cand_rows.empty()) {
        continue;
      }
      ws.matching += cand_rows.size();
      const auto* kch = col_ptrs[key_col];

      if (kch->dict_encoded && kch->dict != nullptr) {
        const size_t nd = kch->dict->StringEntryCount();
        local_seen.assign(nd + 1, 0);
        if (naggs > 0) {
          local_aggs.clear();
          local_aggs.resize((nd + 1) * naggs);
        }
        bool dict_ok = true;
        for (const uint32_t r : cand_rows) {
          size_t code = nd;
          if (!kch->IsNull(r)) {
            if (r >= kch->dict_codes.size() || kch->dict_codes[r] >= nd) {
              dict_ok = false;
              break;
            }
            code = kch->dict_codes[r];
          }
          local_seen[code] = 1;
          if (naggs == 1 && req.agg_specs[0].kind == ColAggKind::kCountStar) {
            ++local_aggs[code].count;
            local_aggs[code].has_value = true;
          } else {
            RuntimeAggState* dst = local_aggs.data() + (code * naggs);
            for (size_t a = 0; a < naggs; ++a) {
              Status fst = FoldRowIntoAggState(req.agg_specs[a], col_ptrs, r,
                                               dst[a], ws.table.arena);
              if (fst != Status::kSuccess) {
                if (fst == StatusCode::kMemoryLimitExceeded) {
                  bail_fallback.store(true, std::memory_order_relaxed);
                  return;
                }
                ws.err = std::move(fst);
                return;
              }
            }
          }
        }
        if (dict_ok) {
          for (size_t c = 0; c <= nd; ++c) {
            if (local_seen[c] == 0) {
              continue;
            }
            RuntimeAggState* dst =
                (c == nd)
                    ? ws.table.GetOrCreateNull(naggs)
                    : ws.table.GetOrCreate(kch->dict->StringEntryAt(c), naggs);
            if (ws.table.bail) {
              bail_fallback.store(true, std::memory_order_relaxed);
              return;
            }
            RuntimeAggState* src = local_aggs.data() + (c * naggs);
            for (size_t a = 0; a < naggs; ++a) {
              Status mst = MergeAggState(req.agg_specs[a], dst[a], src[a],
                                         ws.table.arena);
              if (mst != Status::kSuccess) {
                if (mst == StatusCode::kMemoryLimitExceeded) {
                  bail_fallback.store(true, std::memory_order_relaxed);
                  return;
                }
                ws.err = std::move(mst);
                return;
              }
            }
          }
          has_last = false;
          continue;
        }
      }

      for (const uint32_t r : cand_rows) {
        RuntimeAggState* dst = nullptr;
        if (kch->IsNull(r)) {
          dst = ws.table.GetOrCreateNull(naggs);
        } else {
          const std::string_view sv = kch->VarcharAt(r);
          uint32_t gid = 0;
          if (has_last && sv == last_sv) {
            gid = last_gid;
          } else {
            const auto found_gid = ws.table.FindOrInsertKey(sv, naggs);
            if (!found_gid) {
              bail_fallback.store(true, std::memory_order_relaxed);
              return;
            }
            gid = *found_gid;
            has_last = true;
            last_sv = ws.table.keys[gid];
            last_gid = gid;
          }
          dst = ws.table.aggs.data() + (static_cast<size_t>(gid) * naggs);
        }
        if (ws.table.bail) {
          bail_fallback.store(true, std::memory_order_relaxed);
          return;
        }
        for (size_t a = 0; a < naggs; ++a) {
          Status fst = FoldRowIntoAggState(req.agg_specs[a], col_ptrs, r,
                                           dst[a], ws.table.arena);
          if (fst != Status::kSuccess) {
            if (fst == StatusCode::kMemoryLimitExceeded) {
              bail_fallback.store(true, std::memory_order_relaxed);
              return;
            }
            ws.err = std::move(fst);
            return;
          }
        }
      }
      has_last = false;
    }
  }));

  if (bail_fallback.load(std::memory_order_relaxed)) {
    return std::optional<Relation>{};
  }
  uint64_t total_scanned = 0;
  uint64_t total_matching = 0;
  size_t total_worker_groups = 0;
  for (size_t w = 0; w < workers_used; ++w) {
    RETURN_IF_FAIL(workers[w].err);
    total_scanned += workers[w].scanned;
    total_matching += workers[w].matching;
    total_worker_groups +=
        workers[w].table.keys.size() + (workers[w].table.has_null ? 1 : 0);
  }

  const auto merge_begin = std::chrono::steady_clock::now();
  const PartOrderKind fast_order_kind =
      ClassifyPartOrderKind(req.order_bindings, req.key_specs, req.agg_specs);

  if (workers_used == 1) {
    std::vector<std::vector<SurvivorGroupRow>> single_surv(1);
    std::vector<std::unique_ptr<QueryMemoryCharge>> surv_charges(1);
    QueryMemoryCharge& surv_charge = SurvivorChargeFor(surv_charges, 0);
    ChunkedCharge surv_batch(&surv_charge);
    auto& out_vec = single_surv[0];
    std::vector<Value> having_scratch(naggs);
    std::vector<Value> finished_aggs(naggs);
    std::vector<Value> cached_keys(1);
    VarCharLocalGroupTable& wt = workers[0].table;
    auto emit_one = [&](bool is_null, std::string_view sv,
                        const RuntimeAggState* ast) -> Status {
      ASSIGN_OR_RETURN(
          bool, pass_having,
          EvalPositionalHavingOnStates(req.positional_having, req.agg_specs,
                                       ast, &having_scratch));
      if (!pass_having) {
        return Status::kSuccess;
      }
      RuntimeKeyCell kc{};
      if (is_null) {
        kc.is_null = true;
      } else {
        kc.str_ptr = sv.data();
        kc.str_len = static_cast<uint32_t>(sv.size());
      }
      SurvivorGroupRow row;
      RETURN_IF_FAIL(BuildSurvivorRowFromStates(
          req, fast_order_kind, &kc, ast, &finished_aggs, &cached_keys, &row));
      PushChargedSurvivor(&out_vec, &surv_batch, std::move(row));
      return Status::kSuccess;
    };
    if (wt.has_null) {
      RETURN_IF_FAIL(emit_one(true, {}, wt.null_aggs.data()));
    }
    for (size_t g = 0; g < wt.keys.size(); ++g) {
      RETURN_IF_FAIL(emit_one(false, wt.keys[g], wt.aggs.data() + (g * naggs)));
    }
    return FinalizePartitionedRelation(
        req, single_surv, total_worker_groups, total_scanned, total_matching,
        merge_begin, "single-varchar-local-dict", &surv_charges);
  }

  size_t merge_shards = std::bit_floor(
      std::min<size_t>({workers_used, size_t{16},
                        std::max<size_t>(1, total_worker_groups / 256 + 1)}));
  if (merge_shards == 0) {
    merge_shards = 1;
  }
  const uint64_t shard_mask = merge_shards - 1;
  struct ShardEntryRef {
    uint32_t worker_id{0};
    uint32_t gid{0};
    bool is_null{false};
  };
  std::vector<std::vector<ShardEntryRef>> shard_refs(merge_shards);
  for (size_t w = 0; w < workers_used; ++w) {
    const auto& wt = workers[w].table;
    if (wt.has_null) {
      shard_refs[0].push_back(
          {.worker_id = static_cast<uint32_t>(w), .gid = 0, .is_null = true});
    }
    for (size_t g = 0; g < wt.keys.size(); ++g) {
      const auto s = static_cast<size_t>((wt.hashes[g] >> 32) & shard_mask);
      shard_refs[s].push_back({.worker_id = static_cast<uint32_t>(w),
                               .gid = static_cast<uint32_t>(g),
                               .is_null = false});
    }
  }

  std::vector<std::vector<SurvivorGroupRow>> shard_survivors(merge_shards);
  std::vector<std::unique_ptr<QueryMemoryCharge>> surv_charges(merge_shards);
  std::vector<Status> merge_status(merge_shards, Status::kSuccess);
  std::atomic<size_t> total_groups{0};
  std::atomic<bool> merge_bail{false};

  RETURN_IF_FAIL(RunParallelTasks(merge_shards, [&](size_t s) {
    if (shard_refs[s].empty()) {
      return;
    }
    VarCharLocalGroupTable merged;
    merged.max_groups = 262144;
    for (const ShardEntryRef& ref : shard_refs[s]) {
      auto& src_wt = workers[ref.worker_id].table;
      RuntimeAggState* dst =
          ref.is_null ? merged.GetOrCreateNull(naggs)
                      : merged.GetOrCreate(src_wt.keys[ref.gid], naggs);
      if (merged.bail) {
        merge_bail.store(true, std::memory_order_relaxed);
        return;
      }
      RuntimeAggState* src =
          ref.is_null
              ? src_wt.null_aggs.data()
              : (src_wt.aggs.data() + (static_cast<size_t>(ref.gid) * naggs));
      for (size_t a = 0; a < naggs; ++a) {
        Status mst =
            MergeAggState(req.agg_specs[a], dst[a], src[a], merged.arena);
        if (mst != Status::kSuccess) {
          if (mst == StatusCode::kMemoryLimitExceeded) {
            merge_bail.store(true, std::memory_order_relaxed);
            return;
          }
          merge_status[s] = std::move(mst);
          return;
        }
      }
    }
    total_groups.fetch_add(merged.keys.size() + (merged.has_null ? 1 : 0),
                           std::memory_order_relaxed);

    QueryMemoryCharge& surv_charge = SurvivorChargeFor(surv_charges, s);
    ChunkedCharge surv_batch(&surv_charge);
    auto& out_vec = shard_survivors[s];
    std::vector<Value> having_scratch(naggs);
    std::vector<Value> finished_aggs(naggs);
    std::vector<Value> cached_keys(1);

    auto emit_merged = [&](bool is_null, std::string_view sv,
                           const RuntimeAggState* ast) -> Status {
      ASSIGN_OR_RETURN(
          bool, pass_having,
          EvalPositionalHavingOnStates(req.positional_having, req.agg_specs,
                                       ast, &having_scratch));
      if (!pass_having) {
        return Status::kSuccess;
      }
      RuntimeKeyCell kc{};
      if (is_null) {
        kc.is_null = true;
      } else {
        kc.str_ptr = sv.data();
        kc.str_len = static_cast<uint32_t>(sv.size());
      }
      SurvivorGroupRow row;
      RETURN_IF_FAIL(BuildSurvivorRowFromStates(
          req, fast_order_kind, &kc, ast, &finished_aggs, &cached_keys, &row));
      PushChargedSurvivor(&out_vec, &surv_batch, std::move(row));
      return Status::kSuccess;
    };
    if (merged.has_null) {
      Status est = emit_merged(true, {}, merged.null_aggs.data());
      if (est != Status::kSuccess) {
        merge_status[s] = std::move(est);
        return;
      }
    }
    for (size_t g = 0; g < merged.keys.size(); ++g) {
      Status est =
          emit_merged(false, merged.keys[g], merged.aggs.data() + (g * naggs));
      if (est != Status::kSuccess) {
        merge_status[s] = std::move(est);
        return;
      }
    }
  }));

  if (merge_bail.load(std::memory_order_relaxed)) {
    return std::optional<Relation>{};
  }
  for (const Status& mst : merge_status) {
    RETURN_IF_FAIL(mst);
  }
  return FinalizePartitionedRelation(
      req, shard_survivors, total_groups.load(), total_scanned, total_matching,
      merge_begin, "single-varchar-local-dict", &surv_charges);
}

StatusOr<std::optional<Relation>> TryRunPartitionedColumnarTlcsAggregate(
    const PartitionedColumnarAggRequest& req) {
  if (req.candidate_blocks.empty() || req.key_specs.empty() ||
      std::ranges::any_of(req.agg_specs, [](const ColAggSpec& s) {
        return s.double_arith != nullptr;
      })) {
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
  if (req.key_specs.size() == 2) {
    ASSIGN_OR_RETURN(std::optional<Relation>, two_key_res,
                     TryRunDirectTwoKeyBoundedIntAggregate(req, &km));
    if (two_key_res) {
      return two_key_res;
    }
  }
  if (req.key_specs.size() == 1 &&
      req.key_specs[0].kind == ColKeyKind::kColVarChar) {
    ASSIGN_OR_RETURN(std::optional<Relation>, local_dict_res,
                     TryRunSingleVarCharLocalDictAggregate(req, &km));
    if (local_dict_res) {
      return local_dict_res;
    }
  }

  // CB-2: Unordered `GROUP BY ... LIMIT K` key-fiber prefix exact subset
  // (`ClickBench Q18`). Runs before allocating multi-shard SpillStore slabs!
  if (req.order_bindings.empty() && req.stmt.HasLimit() &&
      !req.any_scalar_distinct && req.candidate_blocks.size() > 8) {
    ASSIGN_OR_RETURN(std::optional<Relation>, fiber_res,
                     TryRunKeyFiberUnorderedLimitAggregate(req, km));
    if (fiber_res.has_value()) {
      return fiber_res;
    }
  }

  // CB-7: Quotient hash-bucket upper-bound exact heavy-hitter pruning
  // (`ClickBench Q13, Q15, Q17, Q19, Q34, Q35`).
  if (!req.order_bindings.empty() && req.stmt.HasLimit() &&
      req.positional_having.empty() && !req.any_scalar_distinct &&
      req.agg_specs.size() == 1 &&
      req.agg_specs[0].kind == ColAggKind::kCountStar &&
      req.candidate_blocks.size() >= 128) {
    ASSIGN_OR_RETURN(std::optional<Relation>, hh_res,
                     TryRunQuotientBucketHeavyHittersTopK(req, km));
    if (hh_res.has_value()) {
      return hh_res;
    }
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
