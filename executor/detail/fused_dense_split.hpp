/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#pragma once

#include <span>

#include "executor/detail/fused_grouped_join_internal.hpp"
#include "expression/jit.hpp"

namespace tinylamb::relational_detail {

struct RightColLocator {
  const ColumnarTable* table{nullptr};
  std::vector<uint32_t> chunk_bases;
  uint32_t fixed_chunk_rows{0};
  uint32_t chunk_shift{0};
  uint32_t chunk_mask{0};
  bool single_chunk{false};
  bool uniform_pow2{false};
  bool uniform_fixed{false};

  void Init(const ColumnarTable* tbl) {
    table = tbl;
    chunk_bases.clear();
    fixed_chunk_rows = 0;
    chunk_shift = 0;
    chunk_mask = 0;
    single_chunk = false;
    uniform_pow2 = false;
    uniform_fixed = false;
    if (tbl == nullptr || tbl->chunks.empty()) {
      return;
    }
    const size_t n = tbl->chunks.size();
    chunk_bases.resize(n + 1);
    uint64_t acc = 0;
    for (size_t i = 0; i < n; ++i) {
      chunk_bases[i] = static_cast<uint32_t>(acc);
      acc += tbl->chunks[i].rows;
    }
    chunk_bases[n] = static_cast<uint32_t>(acc);
    if (n == 1) {
      single_chunk = true;
      return;
    }
    const size_t first_rows = tbl->chunks[0].rows;
    if (first_rows > 0 && first_rows <= std::numeric_limits<uint32_t>::max()) {
      bool all_interior_same = true;
      for (size_t i = 1; i + 1 < n; ++i) {
        if (tbl->chunks[i].rows != first_rows) {
          all_interior_same = false;
          break;
        }
      }
      if (all_interior_same && tbl->chunks.back().rows <= first_rows) {
        fixed_chunk_rows = static_cast<uint32_t>(first_rows);
        if (std::has_single_bit(first_rows)) {
          uniform_pow2 = true;
          chunk_shift = static_cast<uint32_t>(std::countr_zero(first_rows));
          chunk_mask = fixed_chunk_rows - 1;
        } else {
          uniform_fixed = true;
        }
      }
    }
  }

  [[nodiscard]] [[gnu::always_inline]] std::pair<uint32_t, uint32_t> Locate(
      uint32_t r_idx) const noexcept {
    if (single_chunk) [[likely]] {
      return {0, r_idx};
    }
    if (uniform_pow2) [[likely]] {
      return {r_idx >> chunk_shift, r_idx & chunk_mask};
    }
    if (uniform_fixed) {
      return {r_idx / fixed_chunk_rows, r_idx % fixed_chunk_rows};
    }
    const auto it =
        std::upper_bound(chunk_bases.begin() + 1, chunk_bases.end(), r_idx);
    const auto ci = static_cast<uint32_t>((it - chunk_bases.begin()) - 1);
    return {ci, r_idx - chunk_bases[ci]};
  }

  void WriteValue(size_t col, uint32_t r_idx, Value* out) const {
    const auto [ci, ri] = Locate(r_idx);
    table->chunks[ci].columns[col].WriteValueInto(ri, out);
  }

  [[nodiscard]] bool ReadInt(size_t col, uint32_t r_idx, int64_t* out,
                             ValueType* out_type = nullptr,
                             bool* out_unsigned = nullptr) const noexcept {
    const auto [ci, ri] = Locate(r_idx);
    const ColumnarColumn& cc = table->chunks[ci].columns[col];
    const ColumnarColumn* leaf = &cc;
    size_t lr = ri;
    // Lean proof:
    // lean/TinylambOpt/ResearchProofs/GatheredSmallDictFastPath.lean (sound)
    if (leaf->encoding == ColumnarColumn::Encoding::kGathered) [[unlikely]] {
      if (!cc.ResolveLeaf(ri, &leaf, &lr)) {
        return false;
      }
    } else if (leaf->NonGatheredIsNull(lr)) {
      return false;
    }
    if (leaf->is_uniform) [[likely]] {
      if (leaf->type != ValueType::kInt64 && leaf->type != ValueType::kDate) {
        return false;
      }
      if (out_type != nullptr) {
        *out_type = leaf->type;
      }
      if (out_unsigned != nullptr) {
        *out_unsigned = leaf->is_unsigned;
      }
      *out = leaf->IntAt(lr);
      return true;
    }
    const Value& fv = leaf->fallback[lr];
    if (fv.IsNull() ||
        (fv.type != ValueType::kInt64 && fv.type != ValueType::kDate)) {
      return false;
    }
    if (out_type != nullptr) {
      *out_type = fv.type;
    }
    if (out_unsigned != nullptr) {
      *out_unsigned = fv.IsUnsigned();
    }
    *out = fv.value.int_value;
    return true;
  }

  [[nodiscard]] bool IsNull(size_t col, uint32_t r_idx) const noexcept {
    const auto [ci, ri] = Locate(r_idx);
    return table->chunks[ci].columns[col].IsNull(ri);
  }
};

struct PreparedDeferredPkBlock {
  int64_t min_k{0};
  int64_t max_k{0};
  uint32_t row_count{0};
  uint8_t dense_contiguous{0};
  uint8_t has_null_payload{0};
  const int64_t* pk_cells{nullptr};
  const int64_t* pay_cells{nullptr};
  const LSTableColumnBlock* pay_blk{nullptr};
};
struct PreparedDeferredPkLookup {
  enum : size_t {
    kDirShift = 12U,
    kDirBuckets = size_t{1} << kDirShift,
  };
  slot_t pk_col{0};
  slot_t store_payload_col{0};
  slot_t target_proj_col{0};
  size_t group_idx{0};
  ValueType payload_type{ValueType::kNull};
  bool payload_unsigned{false};
  int64_t min_k{0};
  int64_t max_k{0};
  uint64_t span{1};
  std::array<uint32_t, kDirBuckets + 1> bucket_lo{};
  std::vector<int64_t> blk_max_keys;
  std::vector<PreparedDeferredPkBlock> blocks;
};
struct PreparedDeferredPkGroup {
  slot_t pk_col{0};
  const LSTableStore* store{nullptr};
  slot_t store_pk_col{0};
  int64_t min_k{0};
  int64_t max_k{0};
  uint64_t span{1};
  std::array<uint32_t, PreparedDeferredPkLookup::kDirBuckets + 1> bucket_lo{};
  std::vector<int64_t> blk_max_keys;
  struct Block {
    int64_t min_k{0};
    int64_t max_k{0};
    uint32_t row_count{0};
    uint32_t row_base{0};
    uint8_t dense_contiguous{0};
    const int64_t* pk_cells{nullptr};
  };
  std::vector<Block> blocks;
  uint32_t total_rows{0};
  using DensePkEntry = SharedBaseRelationCache::DensePkEntry;
  std::vector<DensePkEntry> dense_index;
  const DensePkEntry* dense_ptr{nullptr};
  size_t dense_size{0};
  std::shared_ptr<const std::vector<DensePkEntry>> shared_dense;
  std::shared_ptr<const SharedBaseRelationCache::CachedPkDict> shared_pk_dict;
  std::shared_ptr<const void> store_owner;
  std::shared_ptr<QueryMemoryCharge> dense_index_charge;
  std::vector<size_t> lookup_indices;
  std::vector<uint32_t> subgid;
  const uint32_t* subgid_ptr{nullptr};
  std::vector<std::pair<uint32_t, uint32_t>> subgid_rep_loc;
  const std::pair<uint32_t, uint32_t>* subgid_rep_ptr{nullptr};
  uint32_t subgid_count{0};
  uint8_t subgid_shift{0};
  bool use_subgid{false};
  bool lazy_payloads{false};
};

struct JitDenseSplitSelItem {
  uint8_t kind{0};  // 0 = agg0, 1 = const, 2 = right_col, 3 = pk0_col
  Value constant;
  size_t idx{0};
  std::string name;
  ValueType vt{ValueType::kNull};
  bool is_unsigned{false};
  bool is_interval{false};
  bool has_null{false};
  bool is_uniform{true};
};
inline constexpr uint8_t kDenseSplitPartShift = 15;
struct JitDenseSplitPlan {
  bool active{false};
  uint64_t dom{0};
  uint32_t pk0_card{0};
  JitDenseSplitRadixAgg::ExprOp expr_op{
      JitDenseSplitRadixAgg::ExprOp::kColDouble};
  std::array<size_t, 4> v_cols{};
  std::array<bool, 4> need_v{false, false, false, false};
  double div_const{1.0};
  bool is_avg{false};
  bool raise_inf{false};
  std::shared_ptr<QueryMemoryCharge> mem_charge;
  std::optional<JitDenseSplitRadixAgg::ScatterKernel> scatter_k;
  std::optional<JitDenseSplitRadixAgg::AccumKernel> accum_k;
  std::vector<JitDenseSplitSelItem> sel_items;
};

void SortUniqueSlots(std::vector<slot_t>* cols);

bool DenseIntBlockMayMatch(const LSTablePageBlock& meta_blk, slot_t full_key,
                           int64_t dense_int_min,
                           std::span<const uint32_t> dense_int_idxs,
                           uint32_t invalid_idx) noexcept;

// Register-tight, out-of-line dense RF pre-filter over one LSTablePageBlock.
// Keeps the 28M-row rejection loop out of TryFusedGroupedJoin's large stack
// frame (zero stack spills) and checks the unsigned range test BEFORE null
// bitmaps or bitset loads.
// Lean proof: lean/TinylambOpt/VectorizedJoinAndPruning.lean
//   (dense_rf_prefilter_block_sound)
[[gnu::noinline]] size_t PrefilterDenseRfBlock(
    const int64_t* __restrict__ vals, uint32_t nrows, int64_t rf_lo,
    int64_t rf_hi, const uint64_t* __restrict__ bits, bool all_bits_set,
    const LSTableColumnBlock* rf_col, bool has_null,
    uint32_t* __restrict__ out_sel) noexcept;

struct DenseSplitExecution {
  TransactionContext& context;
  const Relation& left;
  const Relation& right;
  GroupedJoinFusionSpec& fusion;
  const DeferredLSTableProbe& probe;
  const std::shared_ptr<const ColumnarTable>& right_col_owner;
  const RightColLocator& right_locator;
  std::chrono::steady_clock::time_point join_begin;
  Relation& result;
  std::span<const slot_t> left_columns;
  std::span<const int64_t> left_key_offsets;
  std::span<const PreparedDeferredPkLookup> prepared_pk_lookups;
  const PreparedDeferredPkGroup& pk0;
  bool split_uses_right_subgid;
  const uint32_t* split_right_subgid_ptr;
  const uint32_t* split_right_rep_ptr;
  size_t workers;
  std::span<const uint32_t> dense_int_idxs;
  int64_t dense_int_min;
  bool packed_2x32_int_key;
  const JitDenseSplitPlan& jit_dense_split;

  [[nodiscard]] StatusOr<std::optional<Relation>> Run() const;
};

}  // namespace tinylamb::relational_detail
