/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_DETAIL_COLUMNAR_TLCS_INTERNAL_HPP
#define TINYLAMB_EXECUTOR_DETAIL_COLUMNAR_TLCS_INTERNAL_HPP

#include <algorithm>
#include <bit>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <unordered_set>
#include <vector>

#include "common/regex_guard.hpp"
#include "common/status_or.hpp"
#include "executor/detail/columnar_tlcs_fast_path.hpp"
#include "executor/detail/flat_int_key_map.hpp"
#include "executor/detail/timestamp_trunc_kernel.hpp"
#include "executor/detail/typed_stream_aggregate.hpp"
#include "executor/query_memory.hpp"
#include "expression/aggregate_expression.hpp"
#include "table/tlcs_file.hpp"
#include "type/collation.hpp"
#include "type/value.hpp"

namespace tinylamb::relational_detail {

inline double ColumnarMsSince(
    std::chrono::steady_clock::time_point begin) noexcept {
  return std::chrono::duration<double, std::milli>(
             std::chrono::steady_clock::now() - begin)
      .count();
}

inline uint64_t MixHash64(uint64_t x) noexcept {
  x += 0x9e3779b97f4a7c15ULL;
  x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
  x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
  return x ^ (x >> 31);
}

inline uint64_t HashView64(std::string_view sv) noexcept {
  return MixHash64(std::hash<std::string_view>{}(sv));
}

// Append-only paged arena storing string group keys and MIN/MAX payloads with
// stable pointers and explicit QueryMemoryCharge accounting.
class PagedStringArena {
 public:
  explicit PagedStringArena(QueryMemoryCharge* charge = nullptr) noexcept
      : charge_(charge) {}

  StatusOr<std::string_view> Intern(std::string_view sv) {
    if (sv.empty()) {
      return std::string_view("");
    }
    const size_t need = sv.size();
    if (current_pos_ + need > current_cap_) {
      const size_t alloc_bytes = std::max(kChunkBytes, need);
      if (charge_ != nullptr && !charge_->TryAdd(alloc_bytes)) {
        return StatusError(StatusCode::kMemoryLimitExceeded,
                           "columnar fast path string arena exceeded budget");
      }
      chunks_.emplace_back(alloc_bytes);
      total_bytes_ += alloc_bytes;
      current_cap_ = alloc_bytes;
      current_pos_ = 0;
    }
    char* dst = chunks_.back().data() + current_pos_;
    std::memcpy(dst, sv.data(), need);
    current_pos_ += need;
    return std::string_view(dst, need);
  }

  [[nodiscard]] size_t AllocatedBytes() const noexcept { return total_bytes_; }

 private:
  static constexpr size_t kChunkBytes = size_t{256} << 10;
  std::vector<std::vector<char>> chunks_;
  size_t current_pos_{0};
  size_t current_cap_{0};
  size_t total_bytes_{0};
  QueryMemoryCharge* charge_{nullptr};
};

enum class ColAggKind : uint8_t {
  kCountStar = 0,
  kCountCol,
  kCountByteLen,
  kSumInt64,
  kSumDouble,
  kSumByteLen,
  kAvgDouble,
  kAvgByteLen,
  kMinInt64,
  kMinDate,
  kMinDouble,
  kMinVarChar,
  kMinByteLen,
  kMaxInt64,
  kMaxDate,
  kMaxDouble,
  kMaxVarChar,
  kMaxByteLen,
  kCountDistinctInt64,
  kCountDistinctDate,
  kCountDistinctDouble,
  kCountDistinctVarChar,
};

struct ColDoubleArithExpr {
  enum class Op : uint8_t {
    kColDouble = 0,
    kColInt64,
    kConst,
    kAdd,
    kSub,
    kMul,
    kNeg,
  };
  struct Instruction {
    Op op{Op::kConst};
    slot_t full_col{0};
    double constant{0.0};
  };
  std::vector<Instruction> rpn;
  std::vector<slot_t> referenced_cols;
};

struct ColAggSpec {
  const AggregateExpression* expr{nullptr};
  ColAggKind kind{ColAggKind::kCountStar};
  slot_t full_col{0};
  bool needs_bytes{false};
  bool needs_lengths_only{false};
  std::shared_ptr<const ColDoubleArithExpr> double_arith{};
};

enum class ColKeyKind : uint8_t {
  kColInt64 = 0,
  kColDate,
  kColDouble,
  kColVarChar,
  kConstInt64,
  kColInt64Delta,
  kUtcTimestampTrunc,
  kUtcTimestampExtract,
  kCaseWhenVarChar,
  kRegexReplaceVarChar,
};

struct ColGroupKeySpec {
  ColKeyKind kind{ColKeyKind::kColInt64};
  slot_t full_col{0};
  UtcTimestampTruncUnit trunc_unit{UtcTimestampTruncUnit::kMinute};
  // Const integer key (`GROUP BY 1`) or offset delta (`col +/- delta`).
  int64_t const_int{0};
  // Simple `CASE WHEN (<constant conjuncts>) THEN var_col ELSE const_str END`.
  std::vector<TypedFilterCompare> case_conditions;
  std::string case_else_str;
  bool case_else_is_null{false};
  // Constant `REGEXP_REPLACE(var_col, pattern, replacement)`.
  std::string regex_pattern;
  std::string regex_replacement;
  std::optional<JitDelimitedCapture> regex_capture_jit;
  std::string original_expr_str;
};

struct RuntimeKeyCell {
  bool is_null{false};
  int64_t int_val{0};
  double double_val{0.0};
  const char* str_ptr{nullptr};
  uint32_t str_len{0};

  [[nodiscard]] std::string_view StrView() const noexcept {
    return str_ptr == nullptr ? std::string_view{}
                              : std::string_view(str_ptr, str_len);
  }
};

// Takes budget from `charge` in kChunk steps so per-element accounting does
// not hit the process-wide reservation atomic on every insert: with 16
// workers that contended cache line cost TPC-H Q21 about a sixth of its time.
// The unused remainder goes back when this object dies, so the extra
// reservation is bounded by kChunk per live instance (one per worker).
class ChunkedCharge {
 public:
  explicit ChunkedCharge(QueryMemoryCharge* charge) noexcept
      : charge_(charge) {}
  ChunkedCharge(const ChunkedCharge&) = delete;
  ChunkedCharge& operator=(const ChunkedCharge&) = delete;
  ChunkedCharge(ChunkedCharge&&) = delete;
  ChunkedCharge& operator=(ChunkedCharge&&) = delete;
  ~ChunkedCharge() {
    if (charge_ != nullptr && slack_ != 0) {
      charge_->Release(slack_);
    }
  }
  [[nodiscard]] bool TryAdd(size_t bytes) {
    if (charge_ == nullptr) {
      return true;
    }
    if (bytes <= slack_) {
      slack_ -= bytes;
      return true;
    }
    const size_t want = std::max(bytes, kChunk);
    if (charge_->TryAdd(want)) {
      slack_ += want - bytes;
      return true;
    }
    return charge_->TryAdd(bytes);  // close to the limit: stay exact
  }
  void Add(size_t bytes) {
    if (charge_ == nullptr) {
      return;
    }
    if (bytes <= slack_) {
      slack_ -= bytes;
      return;
    }
    const size_t want = std::max(bytes, kChunk);
    charge_->Add(want);
    slack_ += want - bytes;
  }

 private:
  static constexpr size_t kChunk = size_t{256} << 10;
  QueryMemoryCharge* charge_;
  size_t slack_{0};
};

struct DistinctIntegerValues {
  QueryMemoryCharge charge;
  FlatIntKeyMap<uint8_t> values;
};

struct DistinctStringValues {
  QueryMemoryCharge charge;
  std::unordered_set<std::string_view> values;
};

struct RuntimeAggState {
  int64_t int_val{0};
  double double_val{0.0};
  int64_t count{0};
  const char* str_ptr{nullptr};
  uint32_t str_len{0};
  bool has_value{false};
  // GoogleSQL MIN/MAX NaN poisoning: any NaN input sets this bit and the
  // emitted MIN/MAX double is NaN, regardless of the stored extreme.
  bool saw_nan{false};
  // SUM double infinity provenance (P603, same policy as the canonical
  // accumulator's sum_saw_infinite_input_): set only when an actually
  // infinite input entered the accumulator.  A kSumDouble result that is
  // inf while this bit is clear came from finite inputs overflowing, which
  // GoogleSQL raises instead of returning inf.  Partial sums that overflow
  // must NOT set it, or MergeAggState would silence the finish-time raise.
  bool saw_inf{false};
  // Order-preserving / table-owned dictionary tracking for `kMinVarChar` /
  // `kMaxVarChar` (Lean: ClickBenchDiscovery.lean
  // order_preserving_dict_min_code_sound, unsorted_dict_min_code_bug).
  const LSTableColumnDictionary* dict_ptr{nullptr};
  uint8_t dict_code{0};
  bool dict_byte_sorted{false};
  // Set by paths that count distinct values outside `distinct_ints` (e.g.
  // the pair-sharded partitioned COUNT(DISTINCT)): `count` then holds the
  // final distinct count and FinishAggState must report it.
  bool distinct_count_final{false};
  std::unique_ptr<DistinctIntegerValues> distinct_ints;
  // Keys are string_views into PagedStringArena storage (interned once per
  // distinct value), so counting distinct VARCHARs does not allocate a
  // std::string for every input row.
  std::unique_ptr<DistinctStringValues> distinct_strs;
};

struct GroupKeySliceHasher {
  static constexpr uint64_t kSeed = 0x517cc1b727220a95ULL;

  static uint64_t HashSingleVarChar(std::string_view value) noexcept {
    return MixHash64(kSeed ^ HashView64(value));
  }

  static uint64_t HashCell(const RuntimeKeyCell& c,
                           const ColGroupKeySpec& spec) noexcept {
    if (c.is_null) {
      return 0x243f6a8885a308d3ULL;
    }
    switch (spec.kind) {
      case ColKeyKind::kColVarChar:
      case ColKeyKind::kCaseWhenVarChar:
      case ColKeyKind::kRegexReplaceVarChar:
        return HashView64(c.StrView());
      case ColKeyKind::kColDouble: {
        double d = c.double_val;
        if (d == 0.0) {
          d = 0.0;
        }
        uint64_t bits = std::isnan(d) ? 0x7ff8000000000000ULL
                                      : std::bit_cast<uint64_t>(d);
        return MixHash64(bits);
      }
      default:
        return MixHash64(static_cast<uint64_t>(c.int_val));
    }
  }

  static uint64_t HashSingle(const RuntimeKeyCell& c,
                             const ColGroupKeySpec& spec) noexcept {
    return MixHash64(kSeed ^ HashCell(c, spec));
  }

  static uint64_t HashTyped(
      const RuntimeKeyCell* cells,
      const std::vector<ColGroupKeySpec>& specs) noexcept {
    uint64_t h = kSeed;
    for (size_t i = 0; i < specs.size(); ++i) {
      const uint64_t k = HashCell(cells[i], specs[i]);
      h = MixHash64(h ^ (k + (i * 0x9e3779b97f4a7c15ULL)));
    }
    return h;
  }

  static bool Equals(const RuntimeKeyCell* a, const RuntimeKeyCell* b,
                     const std::vector<ColGroupKeySpec>& specs) noexcept {
    for (size_t i = 0; i < specs.size(); ++i) {
      if (a[i].is_null != b[i].is_null) {
        return false;
      }
      if (a[i].is_null) {
        continue;
      }
      switch (specs[i].kind) {
        case ColKeyKind::kColVarChar:
        case ColKeyKind::kCaseWhenVarChar:
        case ColKeyKind::kRegexReplaceVarChar:
          if (a[i].str_len != b[i].str_len) {
            return false;
          }
          if (a[i].str_len > 0 &&
              std::memcmp(a[i].str_ptr, b[i].str_ptr, a[i].str_len) != 0) {
            return false;
          }
          break;
        case ColKeyKind::kColDouble:
          if (std::isnan(a[i].double_val) && std::isnan(b[i].double_val)) {
            continue;
          }
          if (a[i].double_val != b[i].double_val) {
            return false;
          }
          break;
        default:
          if (a[i].int_val != b[i].int_val) {
            return false;
          }
          break;
      }
    }
    return true;
  }
};

struct OutputSlotBinding {
  enum class Kind : uint8_t { kKey, kAgg, kConstValue, kPostAgg };
  Kind kind{Kind::kKey};
  size_t index{0};
  Value constant;
  std::shared_ptr<const PostAggExpr> post_agg;
};

struct ColumnarOrderBinding {
  OutputSlotBinding binding;
  SortKeyOrder order{true, false, Collation::kNone};
};

// Flat open-addressed worker/shard group table supporting 0..N mixed keys and
// interned string payloads inside `PagedStringArena`.
class WorkerGroupTable {
 public:
  WorkerGroupTable(const std::vector<ColGroupKeySpec>* key_specs,
                   const std::vector<ColAggSpec>* agg_specs,
                   QueryMemoryCharge* charge)
      : key_specs_(key_specs),
        agg_specs_(agg_specs),
        charge_(charge),
        arena_(charge) {}

  Status Reserve(size_t expected_groups);
  StatusOr<uint32_t> FindOrInsert(const RuntimeKeyCell* probe_keys,
                                  uint64_t hash, bool borrow_strings = false);
  Status MergeFromGroup(const RuntimeKeyCell* src_keys, uint64_t hash,
                        std::span<RuntimeAggState> src_aggs,
                        bool borrow_strings = false);

  [[nodiscard]] size_t NumGroups() const noexcept { return num_groups_; }
  [[nodiscard]] uint64_t HashAt(size_t gid) const noexcept {
    return group_hashes_[gid];
  }
  // Pointer arithmetic, not operator[]: with zero keys or zero aggregates
  // (`SELECT c FROM t GROUP BY c`) the backing vector is empty and callers
  // never dereference the result.
  [[nodiscard]] const RuntimeKeyCell* KeysAt(size_t gid) const noexcept {
    return keys_.data() + (gid * key_specs_->size());
  }
  [[nodiscard]] RuntimeAggState* AggsAt(size_t gid) noexcept {
    return aggs_.data() + (gid * agg_specs_->size());
  }
  [[nodiscard]] PagedStringArena& Arena() noexcept { return arena_; }

 private:
  struct Slot {
    uint64_t hash{0};
    uint32_t group_plus_one{0};
  };

  Status Rehash(size_t new_cap);
  Status ReserveGroupStorage(size_t groups);

  const std::vector<ColGroupKeySpec>* key_specs_;
  const std::vector<ColAggSpec>* agg_specs_;
  QueryMemoryCharge* charge_;
  PagedStringArena arena_;
  std::vector<Slot> slots_;
  std::vector<RuntimeKeyCell> keys_;
  std::vector<RuntimeAggState> aggs_;
  std::vector<uint64_t> group_hashes_;
  size_t num_groups_{0};
};

// Shared execution primitives defined in columnar_tlcs_fast_path.cpp and
// reused by both the resident/pruned fast path and the partitioned Wave B2
// engine.
Status FoldRowIntoAggState(
    const ColAggSpec& spec,
    const std::vector<const TlcsFile::DecodedColumnChunk*>& col_ptrs,
    uint32_t row, RuntimeAggState& st, PagedStringArena& arena);

Status MergeAggState(const ColAggSpec& spec, RuntimeAggState& dst,
                     RuntimeAggState& src, PagedStringArena& arena);

// Returns a Status error when a kSumDouble accumulator overflowed to
// infinity on finite inputs (see RuntimeAggState::saw_inf, P603).
StatusOr<Value> FinishAggState(const ColAggSpec& spec,
                               const RuntimeAggState& st);

StatusOr<Value> MaterializeKeyCellValue(const ColGroupKeySpec& spec,
                                        const RuntimeKeyCell& cell);

StatusOr<Value> EvaluateBindingValue(
    const OutputSlotBinding& b, const std::vector<ColGroupKeySpec>& key_specs,
    const RuntimeKeyCell* keys, const std::vector<Value>& finished_aggs,
    std::vector<Value>* cached_key_values);

// Evaluates PositionalHaving conjuncts over raw `RuntimeAggState`, finishing
// Value objects only for non-constant-COUNT clauses.
StatusOr<bool> EvalPositionalHavingOnStates(
    const std::vector<PositionalHaving>& clauses,
    const std::vector<ColAggSpec>& agg_specs, const RuntimeAggState* ast,
    std::vector<Value>* finished_scratch);

struct ColumnarStageSchedule {
  std::vector<slot_t> stage1a_cols;
  std::vector<slot_t> stage1b_cols;
  std::vector<uint8_t> stage1b_len_only;
  // Full-string residual filter columns without a positive substring precheck
  // (e.g. `URL NOT LIKE '%.google.%'` alongside `Title LIKE '%Google%'`),
  // decoded only after `stage1b` and `stage1a` survive!
  std::vector<slot_t> stage1c_cols;
  std::vector<uint8_t> col_in_payload;
  std::vector<slot_t> stage2_cols;
  std::vector<uint8_t> stage2_len_only;
  // Conditional `CASE WHEN` target columns decoded only when at least one
  // surviving row in the block satisfies the guard conjunction (Lean:
  // ClickBenchDiscovery.lean conditional_key_disjoint_partition_sound).
  std::vector<slot_t> cond_cols;
  std::vector<std::vector<TypedFilterCompare>> cond_guards;
};

struct BlockDecodeScratch {
  TlcsFile::DecodedBlock blk1a;
  TlcsFile::DecodedBlock blk1b;
  TlcsFile::DecodedBlock blk1c;
  TlcsFile::DecodedBlock blk2;
  TlcsFile::DecodedBlock blk_cond;
  std::vector<uint8_t> io1a;
  std::vector<uint8_t> io1b;
  std::vector<uint8_t> io1c;
  std::vector<uint8_t> io2;
  std::vector<uint8_t> io_cond;
};

// Reconstructs full key specifications (minus redundant functionally dependent
// `kColInt64Delta` or `kConstInt64` keys when collapsed) -> physical keys
// (Lean: ClickBenchDiscovery.lean group_by_functional_expr_reduction_sound,
// group_by_overflow_expr_drop_bug).
struct KeyProjectionMap {
  std::vector<ColGroupKeySpec> phys_specs;
  std::vector<int32_t> full_to_phys;
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
    const std::vector<ColGroupKeySpec>& key_specs);

ColumnarStageSchedule BuildColumnarStageSchedule(
    const Schema& table_schema, const ColumnarPredicateSet& preds,
    const std::vector<ColGroupKeySpec>& key_specs,
    const std::vector<ColAggSpec>& agg_specs);

// Decodes and filters `bidx` into `col_ptrs` and `cand_rows`. Returns `true`
// when at least one row matched, `false` when the block yielded 0 matches.
StatusOr<bool> DecodeAndFilterBlock(
    const TlcsFile& tlcs, uint32_t bidx, const Schema& table_schema,
    const ColumnarPredicateSet& preds, const ColumnarStageSchedule& sched,
    BlockDecodeScratch* scratch,
    std::vector<const TlcsFile::DecodedColumnChunk*>* col_ptrs,
    std::vector<uint32_t>* cand_rows, uint64_t* local_scanned);

// Validates via directory metadata that every candidate block has bounded
// non-overflowing `(col + const_int)` values so delta keys can collapse onto
// the base column.
StatusOr<bool> ValidateDeltaKeysNoOverflow(
    const TlcsFile& tlcs, const std::vector<uint32_t>& candidate_blocks,
    const std::vector<ColGroupKeySpec>& key_specs);

// Lightweight per-worker 64-slot verified `YYYY-MM-DD` date cache for
// canonical 19-byte UTC `YYYY-MM-DD HH:MM:SS` strings truncated/extracted
// without re-running full calendar validation per row.
struct TimestampPrefixCache {
  struct DateSlot {
    uint64_t w0{0};
    uint16_t w1{0};
    bool valid{false};
    int64_t yyyymmdd{0};
  };
  std::array<DateSlot, 64> dates{};
  bool last_trunc_valid{false};
  UtcTimestampTruncUnit last_trunc_unit{UtcTimestampTruncUnit::kMinute};
  std::array<char, 16> last_trunc_prefix{};
  int64_t last_trunc_val{0};
  bool last_ext_valid{false};
  UtcTimestampTruncUnit last_ext_unit{UtcTimestampTruncUnit::kMinute};
  std::array<char, 16> last_ext_prefix{};
  int64_t last_ext_val{0};

  static bool ParseCanonicalTime(std::string_view p, int64_t* hh, int64_t* mm,
                                 int64_t* ss) noexcept {
    if (p.size() < 19 || p[4] != '-' || p[7] != '-' || p[10] != ' ' ||
        p[13] != ':' || p[16] != ':') {
      return false;
    }
    const auto d0 = static_cast<unsigned>(p[11] - '0');
    const auto d1 = static_cast<unsigned>(p[12] - '0');
    const auto d2 = static_cast<unsigned>(p[14] - '0');
    const auto d3 = static_cast<unsigned>(p[15] - '0');
    const auto d4 = static_cast<unsigned>(p[17] - '0');
    const auto d5 = static_cast<unsigned>(p[18] - '0');
    if (d0 > 9 || d1 > 9 || d2 > 5 || d3 > 9 || d4 > 5 || d5 > 9) {
      return false;
    }
    const unsigned h = (d0 * 10) + d1;
    if (h > 23) {
      return false;
    }
    *hh = static_cast<int64_t>(h);
    *mm = (static_cast<int64_t>(d2) * 10) + static_cast<int64_t>(d3);
    *ss = (static_cast<int64_t>(d4) * 10) + static_cast<int64_t>(d5);
    return true;
  }

  static bool HasCanonicalUtcLen(std::string_view sv) noexcept {
    return sv.size() == 19 ||
           (sv.size() == 22 && sv[19] == '+' && sv[20] == '0' && sv[21] == '0');
  }

  bool LookupOrVerifyDate(std::string_view sv, int64_t* yyyymmdd_out,
                          int64_t* hh_out, int64_t* mm_out,
                          int64_t* ss_out) noexcept {
    if (!HasCanonicalUtcLen(sv) ||
        !ParseCanonicalTime(sv, hh_out, mm_out, ss_out)) {
      return false;
    }
    uint64_t w0 = 0;
    uint16_t w1 = 0;
    std::memcpy(&w0, sv.data(), 8);
    std::memcpy(&w1, sv.data() + 8, 2);
    const size_t idx = static_cast<size_t>(MixHash64(w0 ^ w1)) & 63;
    DateSlot& sl = dates[idx];
    if (sl.valid && sl.w0 == w0 && sl.w1 == w1) {
      *yyyymmdd_out = sl.yyyymmdd;
      return true;
    }
    const auto day_key =
        TryUtcTimestampTruncKey(sv, UtcTimestampTruncUnit::kDay);
    if (!day_key.has_value()) {
      return false;
    }
    const int64_t ymd = *day_key / 1000000LL;
    sl.w0 = w0;
    sl.w1 = w1;
    sl.yyyymmdd = ymd;
    sl.valid = true;
    *yyyymmdd_out = ymd;
    return true;
  }

  std::optional<int64_t> Trunc(std::string_view sv,
                               UtcTimestampTruncUnit unit) noexcept {
    if (unit == UtcTimestampTruncUnit::kMinute && last_trunc_valid &&
        last_trunc_unit == unit && HasCanonicalUtcLen(sv) &&
        std::memcmp(sv.data(), last_trunc_prefix.data(), 16) == 0 &&
        sv[16] == ':' && sv[17] >= '0' && sv[17] <= '5' && sv[18] >= '0' &&
        sv[18] <= '9') {
      return last_trunc_val;
    }
    int64_t ymd = 0, hh = 0, mm = 0, ss = 0;
    if (LookupOrVerifyDate(sv, &ymd, &hh, &mm, &ss)) {
      int64_t res = 0;
      switch (unit) {
        case UtcTimestampTruncUnit::kDay:
          res = ymd * 1000000LL;
          break;
        case UtcTimestampTruncUnit::kHour:
          res = (ymd * 1000000LL) + (hh * 10000LL);
          break;
        case UtcTimestampTruncUnit::kMinute:
          res = (ymd * 1000000LL) + (hh * 10000LL) + (mm * 100LL);
          std::memcpy(last_trunc_prefix.data(), sv.data(), 16);
          last_trunc_unit = unit;
          last_trunc_val = res;
          last_trunc_valid = true;
          break;
        case UtcTimestampTruncUnit::kSecond:
          res = (ymd * 1000000LL) + (hh * 10000LL) + (mm * 100LL) + ss;
          break;
        default:
          return std::nullopt;
      }
      return res;
    }
    return TryUtcTimestampTruncKey(sv, unit);
  }

  std::optional<int64_t> Extract(std::string_view sv,
                                 UtcTimestampTruncUnit unit) noexcept {
    if ((unit == UtcTimestampTruncUnit::kMinute ||
         unit == UtcTimestampTruncUnit::kHour) &&
        last_ext_valid && last_ext_unit == unit && HasCanonicalUtcLen(sv) &&
        std::memcmp(sv.data(), last_ext_prefix.data(), 16) == 0 &&
        sv[16] == ':' && sv[17] >= '0' && sv[17] <= '5' && sv[18] >= '0' &&
        sv[18] <= '9') {
      return last_ext_val;
    }
    int64_t ymd = 0, hh = 0, mm = 0, ss = 0;
    if (LookupOrVerifyDate(sv, &ymd, &hh, &mm, &ss)) {
      if (unit == UtcTimestampTruncUnit::kMinute ||
          unit == UtcTimestampTruncUnit::kHour) {
        const int64_t res = (unit == UtcTimestampTruncUnit::kMinute) ? mm : hh;
        std::memcpy(last_ext_prefix.data(), sv.data(), 16);
        last_ext_unit = unit;
        last_ext_val = res;
        last_ext_valid = true;
        return res;
      }
      return std::nullopt;
    }
    return TryUtcTimestampExtractKey(sv, unit);
  }
};

// Per-worker dictionary homomorphism + direct-mapped session memo cache for
// `REGEXP_REPLACE(col, pat, rep)` group keys (ClickBench Q29; Lean:
// ClickBenchDiscovery.lean dict_code_homomorphism_group_agg_sound).
struct RegexReplaceCache {
  static constexpr size_t kMemoSlots = 1024;
  static constexpr size_t kMemoRingBytes = size_t{128} << 10;

  struct DictCache {
    const LSTableColumnDictionary* dict{nullptr};
    const ColGroupKeySpec* spec{nullptr};
    size_t entry_count{0};
    std::array<uint32_t, 256> offsets{};
    std::array<uint32_t, 256> lengths{};
    std::string arena;
  };

  struct MemoSlot {
    uint32_t gen{0};
    uint64_t hash{0};
    uint32_t in_off{0};
    uint32_t in_len{0};
    uint32_t out_off{0};
    uint32_t out_len{0};
  };

  DictCache dict_cache;
  const ColGroupKeySpec* memo_spec{nullptr};
  uint32_t memo_gen{1};
  size_t ring_pos{0};
  std::vector<MemoSlot> slots;
  std::vector<char> ring;

  bool Eval(const ColGroupKeySpec& ks, const TlcsFile::DecodedColumnChunk& ch,
            uint32_t row, std::string* fallback_scratch,
            std::string_view* out_sv, Status* status_out) {
    if (ch.dict_encoded && ch.dict != nullptr) {
      const size_t nd = ch.dict->StringEntryCount();
      if (nd <= 256 && row < ch.dict_codes.size() && ch.dict_codes[row] < nd) {
        if (dict_cache.dict != ch.dict || dict_cache.spec != &ks) {
          dict_cache.arena.clear();
          dict_cache.arena.reserve(nd * 24);
          for (size_t d = 0; d < nd; ++d) {
            auto rep = ReplaceCheckedRegexView(
                ch.dict->StringEntryAt(d), ks.regex_pattern,
                ks.regex_replacement, fallback_scratch);
            if (!rep.HasValue()) {
              *status_out = rep.GetStatus();
              return false;
            }
            const std::string_view rsv = rep.Value();
            dict_cache.offsets[d] =
                static_cast<uint32_t>(dict_cache.arena.size());
            dict_cache.lengths[d] = static_cast<uint32_t>(rsv.size());
            dict_cache.arena.append(rsv.data(), rsv.size());
          }
          dict_cache.dict = ch.dict;
          dict_cache.spec = &ks;
          dict_cache.entry_count = nd;
        }
        const uint8_t code = ch.dict_codes[row];
        *out_sv =
            std::string_view(dict_cache.arena.data() + dict_cache.offsets[code],
                             dict_cache.lengths[code]);
        return true;
      }
    }

    const std::string_view in_sv = ch.VarcharAt(row);
    if (in_sv.size() <= 4096) {
      if (slots.empty()) {
        slots.resize(kMemoSlots);
        ring.resize(kMemoRingBytes);
      }
      if (memo_spec != &ks) {
        memo_spec = &ks;
        ++memo_gen;
        if (memo_gen == 0) {
          std::fill(slots.begin(), slots.end(), MemoSlot{});
          memo_gen = 1;
        }
        ring_pos = 0;
      }
      const uint64_t h = HashView64(in_sv);
      MemoSlot& sl = slots[static_cast<size_t>(h) & (kMemoSlots - 1)];
      if (sl.gen == memo_gen && sl.hash == h && sl.in_len == in_sv.size() &&
          (sl.in_len == 0 || std::memcmp(ring.data() + sl.in_off, in_sv.data(),
                                         sl.in_len) == 0)) {
        *out_sv = std::string_view(ring.data() + sl.out_off, sl.out_len);
        return true;
      }
      auto rep = ReplaceCheckedRegexView(
          in_sv, ks.regex_pattern, ks.regex_replacement, fallback_scratch);
      if (!rep.HasValue()) {
        *status_out = rep.GetStatus();
        return false;
      }
      const std::string_view rsv = rep.Value();
      const size_t need = in_sv.size() + rsv.size();
      if (need <= kMemoRingBytes) {
        if (ring_pos + need > kMemoRingBytes) {
          ++memo_gen;
          if (memo_gen == 0) {
            std::fill(slots.begin(), slots.end(), MemoSlot{});
            memo_gen = 1;
          }
          ring_pos = 0;
        }
        const auto ioff = static_cast<uint32_t>(ring_pos);
        if (!in_sv.empty()) {
          std::memcpy(ring.data() + ring_pos, in_sv.data(), in_sv.size());
          ring_pos += in_sv.size();
        }
        const auto ooff = static_cast<uint32_t>(ring_pos);
        if (!rsv.empty()) {
          std::memcpy(ring.data() + ring_pos, rsv.data(), rsv.size());
          ring_pos += rsv.size();
        }
        sl = {.gen = memo_gen,
              .hash = h,
              .in_off = ioff,
              .in_len = static_cast<uint32_t>(in_sv.size()),
              .out_off = ooff,
              .out_len = static_cast<uint32_t>(rsv.size())};
        *out_sv = std::string_view(ring.data() + ooff, sl.out_len);
        return true;
      }
      *out_sv = rsv;
      return true;
    }

    auto rep = ReplaceCheckedRegexView(in_sv, ks.regex_pattern,
                                       ks.regex_replacement, fallback_scratch);
    if (!rep.HasValue()) {
      *status_out = rep.GetStatus();
      return false;
    }
    *out_sv = rep.Value();
    return true;
  }
};

inline bool RowMatchesCaseConditions(
    const std::vector<TypedFilterCompare>& conds,
    const std::vector<const TlcsFile::DecodedColumnChunk*>& col_ptrs,
    uint32_t row) noexcept {
  for (const auto& cc : conds) {
    const auto* cch = col_ptrs[cc.left];
    if (cch == nullptr || cch->IsNull(row)) {
      return false;
    }
    const int64_t lv = cch->int_cells[row];
    bool cmp_ok = false;
    switch (cc.op) {
      case BinaryOperation::kEquals:
        cmp_ok = lv == cc.int_constant;
        break;
      case BinaryOperation::kNotEquals:
        cmp_ok = lv != cc.int_constant;
        break;
      case BinaryOperation::kLessThan:
        cmp_ok = lv < cc.int_constant;
        break;
      case BinaryOperation::kLessThanEquals:
        cmp_ok = lv <= cc.int_constant;
        break;
      case BinaryOperation::kGreaterThan:
        cmp_ok = lv > cc.int_constant;
        break;
      case BinaryOperation::kGreaterThanEquals:
        cmp_ok = lv >= cc.int_constant;
        break;
      default:
        cmp_ok = false;
        break;
    }
    if (!cmp_ok) {
      return false;
    }
  }
  return true;
}

inline bool ExtractSingleKeyCell(
    const ColGroupKeySpec& ks,
    const std::vector<const TlcsFile::DecodedColumnChunk*>& col_ptrs,
    uint32_t row, RuntimeKeyCell* out_cell, std::string* regex_scratch,
    TimestampPrefixCache* ts_cache, Status* status_out,
    RegexReplaceCache* regex_cache = nullptr) {
  *out_cell = RuntimeKeyCell{};
  if (ks.kind == ColKeyKind::kConstInt64) {
    out_cell->int_val = ks.const_int;
    return true;
  }
  // Lean proof: lean/TinylambOpt/ClickBenchDiscovery.lean
  // (conditional_key_disjoint_partition_sound): evaluate `CASE WHEN` guard
  // columns BEFORE touching `col_ptrs[ks.full_col]` so blocks where 0 surviving
  // rows satisfy `ks.case_conditions` never need to decode `ks.full_col`, and
  // `CASE WHEN false THEN NULL ELSE '' END` evaluates to `''`, not `NULL`.
  if (ks.kind == ColKeyKind::kCaseWhenVarChar) {
    if (!RowMatchesCaseConditions(ks.case_conditions, col_ptrs, row)) {
      if (ks.case_else_is_null) {
        out_cell->is_null = true;
      } else {
        out_cell->str_ptr = ks.case_else_str.data();
        out_cell->str_len = static_cast<uint32_t>(ks.case_else_str.size());
      }
      return true;
    }
    const auto* ch = col_ptrs[ks.full_col];
    if (ch->IsNull(row)) {
      out_cell->is_null = true;
      return true;
    }
    const auto sv = ch->VarcharAt(row);
    out_cell->str_ptr = sv.data();
    out_cell->str_len = static_cast<uint32_t>(sv.size());
    return true;
  }
  const auto* ch = col_ptrs[ks.full_col];
  if (ch->IsNull(row)) {
    out_cell->is_null = true;
    return true;
  }
  switch (ks.kind) {
    case ColKeyKind::kColInt64:
    case ColKeyKind::kColDate:
      out_cell->int_val = ch->int_cells[row];
      return true;
    case ColKeyKind::kColInt64Delta: {
      int64_t next = 0;
      if (__builtin_add_overflow(ch->int_cells[row], ks.const_int, &next)) {
        *status_out = StatusError(StatusCode::kIsInfinity,
                                  "integer overflow in group key");
        return false;
      }
      out_cell->int_val = next;
      return true;
    }
    case ColKeyKind::kColDouble:
      out_cell->double_val = std::bit_cast<double>(ch->int_cells[row]);
      return true;
    case ColKeyKind::kColVarChar: {
      const auto sv = ch->VarcharAt(row);
      out_cell->str_ptr = sv.data();
      out_cell->str_len = static_cast<uint32_t>(sv.size());
      return true;
    }
    case ColKeyKind::kUtcTimestampTrunc: {
      const auto packed =
          ts_cache != nullptr
              ? ts_cache->Trunc(ch->VarcharAt(row), ks.trunc_unit)
              : TryUtcTimestampTruncKey(ch->VarcharAt(row), ks.trunc_unit);
      if (!packed.has_value()) {
        *status_out = StatusError(StatusCode::kInvalidArgument,
                                  "timestamp key fell back to generic path");
        return false;
      }
      out_cell->int_val = *packed;
      return true;
    }
    case ColKeyKind::kUtcTimestampExtract: {
      const auto packed =
          ts_cache != nullptr
              ? ts_cache->Extract(ch->VarcharAt(row), ks.trunc_unit)
              : TryUtcTimestampExtractKey(ch->VarcharAt(row), ks.trunc_unit);
      if (!packed.has_value()) {
        *status_out = StatusError(StatusCode::kInvalidArgument,
                                  "timestamp key fell back to generic path");
        return false;
      }
      out_cell->int_val = *packed;
      return true;
    }
    case ColKeyKind::kCaseWhenVarChar:
      return true;
    case ColKeyKind::kRegexReplaceVarChar: {
      if (ks.regex_capture_jit && !ch->dict_encoded) {
        const auto key = ks.regex_capture_jit->Replace(ch->VarcharAt(row));
        out_cell->str_ptr = key.data();
        out_cell->str_len = static_cast<uint32_t>(key.size());
        return true;
      }
      if (regex_cache != nullptr) {
        std::string_view sv;
        if (!regex_cache->Eval(ks, *ch, row, regex_scratch, &sv, status_out)) {
          return false;
        }
        out_cell->str_ptr = sv.data();
        out_cell->str_len = static_cast<uint32_t>(sv.size());
        return true;
      }
      auto rep = ReplaceCheckedRegexView(ch->VarcharAt(row), ks.regex_pattern,
                                         ks.regex_replacement, regex_scratch);
      if (!rep.HasValue()) {
        *status_out = rep.GetStatus();
        return false;
      }
      const std::string_view sv = rep.Value();
      out_cell->str_ptr = sv.data();
      out_cell->str_len = static_cast<uint32_t>(sv.size());
      return true;
    }
    case ColKeyKind::kConstInt64:
      return true;
  }
  return true;
}

bool CompilePredicatesFromTypedPlan(const TypedAggregationPlan& plan,
                                    const TlcsFile& tlcs,
                                    std::span<const slot_t> proj_to_full,
                                    const Schema& table_schema,
                                    ColumnarPredicateSet* out);

inline slot_t MapSlot(size_t slot, std::span<const slot_t> proj_to_full) noexcept {
  return proj_to_full.empty() ? static_cast<slot_t>(slot) : proj_to_full[slot];
}

bool TryClassifyAggregate(const AggregateExpression* agg,
                          const Schema& qualified_schema,
                          std::span<const slot_t> proj_to_full,
                          const Schema& table_schema, ColAggSpec* out);

bool TryClassifyGroupKey(const Expression& expr, const Schema& qualified_schema,
                         std::span<const slot_t> proj_to_full,
                         const Schema& table_schema, ColGroupKeySpec* out);

bool BindExpressionToOutputSlot(const Expression& expr,
                                const std::vector<ColGroupKeySpec>& key_specs,
                                const std::vector<ColAggSpec>& agg_specs,
                                const std::vector<NamedExpression>* select_list,
                                OutputSlotBinding* out);

StatusOr<std::optional<Relation>> TryFoldTlcsBoundaryAggregate(
    const TlcsFile& tlcs, const SelectStatement& stmt, const Schema& schema,
    const Schema& qualified_schema, const ColumnarPredicateSet& preds,
    const std::vector<ColGroupKeySpec>& keys,
    const std::vector<ColAggSpec>& aggregates,
    const std::vector<OutputSlotBinding>& emit,
    const std::vector<ColumnarOrderBinding>& order,
    std::span<const NamedExpression> output_names, ExecutionRuntime* runtime);

}  // namespace tinylamb::relational_detail

#endif  // TINYLAMB_EXECUTOR_DETAIL_COLUMNAR_TLCS_INTERNAL_HPP
