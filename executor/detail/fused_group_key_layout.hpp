/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#pragma once
#include "executor/detail/fused_group_storage.hpp"
#include "executor/detail/subquery_runtime.hpp"

namespace tinylamb::relational_detail {

inline int64_t ColumnarIntValue(const ColumnarColumn& col, size_t r) noexcept {
  if (col.is_uniform) {
    return col.IntAt(r);
  }
  Value tmp;
  col.WriteValueInto(r, &tmp);
  return tmp.value.int_value;
}

inline uint64_t HashFusedKeyValue(const Value& value) {
  if (value.IsNull()) {
    return 0;
  }
  if (value.type == ValueType::kVarChar && !value.IsInterval() &&
      !value.IsCaseInsensitive()) {
    return std::hash<std::string_view>{}(value.value.varchar_value);
  }
  if ((value.type == ValueType::kInt64 || value.type == ValueType::kDate) &&
      !value.IsUnsigned()) {
    uint64_t x =
        static_cast<uint64_t>(value.value.int_value) + 0x9e3779b97f4a7c15ULL;
    x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
    x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
    return x ^ (x >> 31);
  }
  return (value.IsCaseInsensitive() || value.type == ValueType::kDouble)
             ? DistinctValueHash{}(value)
             : std::hash<Value>{}(value);
}

struct FusedKeyInputs {
  const Relation& left;
  const Relation& right_arg;
  JoinType join_type;
  GroupedJoinFusionSpec* fusion;
  const DeferredLSTableProbe* deferred_left_probe;
  size_t total_right_rows;
  bool right_col_only;
  const std::shared_ptr<const ColumnarTable>& right_col_owner;
  const RightColLocator& right_locator;
  const Relation* right_ptr;
  const Relation& right;
  const Relation& result;
  bool singleton_right_relation;
  const std::vector<EqualityKey>& equality_keys;
  size_t left_width;
  const std::vector<BytecodeProgram>& key_programs;
  const std::vector<ProgramShape>& key_shapes;
  bool count_star_only;
  bool raw_count_fusion;
  bool raw_sum_fusion;
  bool all_direct_group_keys;
  bool int_group_key;
  const FusedRightCellReader& read_right_cell;
  std::vector<PreparedDeferredPkLookup>& prepared_pk_lookups;
  std::vector<PreparedDeferredPkGroup>& def_pk_groups;
  const std::array<int8_t, 16>& def_col_to_pk_lookup;
  SharedBaseRelationCache* shared_pk_cache;
  const PreparedPkCellReader& read_pk_cell;
};

struct FusedGroupKeyLayout {
  bool use_split_key_gid{};
  bool split_has_left_probe{};
  bool split_uses_right_subgid{};
  bool split_uses_pk_subgid{};
  uint8_t split_right_shift{};
  size_t split_left_k{};
  size_t split_right_subgid_count{};
  std::vector<uint32_t> split_right_subgid{};
  const uint32_t* split_right_subgid_ptr{};
  std::vector<uint32_t> split_right_rep_idx{};
  const uint32_t* split_right_rep_ptr{};
  size_t split_right_rep_count{};
  std::shared_ptr<const SharedBaseRelationCache::CachedSplitRightDict>
      shared_right_dict{};
  size_t right_row_count{};
  std::shared_ptr<QueryMemoryCharge> split_dictionary_charge{};
  bool use_fast_2key_year_lut{};
  bool packed_2x32_group_key{};
  slot_t packed_2x32_slot0{};
  slot_t packed_2x32_slot1{};
  ValueType packed_2x32_type0{};
  ValueType packed_2x32_type1{};
  bool packed_2x32_unpack_only{};
  std::optional<slot_t> split_left_slot{};
  ValueType split_left_type{};
  std::optional<int64_t> compact_integer_threshold{};
  bool split_key_unpack_only{};
  bool use_left_packed_key{};
  bool left_pack_unpack_only{};
  bool left_pack_uses_right{};
  bool left_pack_uses_dict{};
  uint64_t left_pack_right_mask{};
  uint64_t left_pack_dict_mask{};
  uint8_t left_pack_dict_shift{};
  std::vector<uint32_t> left_pack_dict_subgid{};
  std::vector<size_t> left_pack_dict_cols{};
  std::vector<uint32_t> left_pack_dict_rep_rows{};
  std::vector<std::pair<uint32_t, uint32_t>> left_pack_dict_rep_col_loc{};
  std::vector<size_t> left_pack_chunk_offsets{};
  std::vector<LeftPackedIntCol> left_pack_ints{};
  std::shared_ptr<QueryMemoryCharge> left_pack_charge{};
  bool left_col_only{};
  Status Prepare(const FusedKeyInputs& input);
};

}  // namespace tinylamb::relational_detail
