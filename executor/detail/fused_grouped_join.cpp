/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/detail/fused_grouped_join.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <cctype>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <functional>
#include <iostream>
#include <iterator>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/parallel_work.hpp"
#include "common/platform/extended_float.hpp"
#include "common/platform/int128_conversion.hpp"
#include "common/query_parallelism.hpp"
#include "common/status_or.hpp"
#include "common/string_util.hpp"
#include "database/transaction_context.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/detail/flat_int_key_map.hpp"
#include "executor/detail/fused_grouped_join_internal.hpp"
#include "executor/detail/planning_columnar_keys.hpp"
#include "executor/detail/relation.hpp"
#include "executor/detail/scan_filter.hpp"
#include "executor/detail/subquery_runtime.hpp"
#include "executor/hash_join_mode.hpp"
#include "executor/query_memory.hpp"
#include "executor/spill_file.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/binary_expression.hpp"
#include "expression/bytecode.hpp"
#include "expression/case_expression.hpp"
#include "expression/cast_expression.hpp"
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
#include "table/lstable_bridge.hpp"
#include "table/table.hpp"
#include "table/table_statistics.hpp"
#include "type/collation.hpp"
#include "type/column_name.hpp"
#include "type/date.hpp"
#include "type/schema.hpp"
#include "type/type.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb::relational_detail {

namespace {

constexpr size_t kRawAggregateCapacity = 8;

inline bool ColumnarCellIsNullOrNaN(const ColumnarColumn& col,
                                    size_t r) noexcept {
  if (col.IsNull(r)) {
    return true;
  }
  if (col.is_uniform) {
    return col.type == ValueType::kDouble && std::isnan(col.DoubleAt(r));
  }
  Value tmp;
  col.WriteValueInto(r, &tmp);
  return tmp.IsNull() ||
         (tmp.type == ValueType::kDouble && std::isnan(tmp.value.double_value));
}

inline int64_t ColumnarIntValue(const ColumnarColumn& col, size_t r) noexcept {
  if (col.is_uniform) {
    return col.IntAt(r);
  }
  Value tmp;
  col.WriteValueInto(r, &tmp);
  return tmp.value.int_value;
}

inline bool ColumnarColMayBeDouble(const ColumnarTable& table, slot_t col) {
  for (const ColumnarChunk& ch : table.chunks) {
    if (col >= ch.columns.size()) {
      continue;
    }
    const ColumnarColumn& cc = ch.columns[col];
    if (cc.is_uniform) {
      if (cc.type == ValueType::kDouble) {
        return true;
      }
      continue;
    }
    for (size_t r = 0; r < ch.rows; ++r) {
      Value tmp;
      cc.WriteValueInto(r, &tmp);
      if (tmp.type == ValueType::kDouble) {
        return true;
      }
    }
  }
  return false;
}

}  // namespace

namespace {

// One probe row updates all aggregate cells under the same group lock.
// Release on every Status/error path as well as normal completion.
class FusedGroupLock {
 public:
  explicit FusedGroupLock(uint8_t* state) noexcept : state_(state) {
    if (state_ != nullptr) {
      while (__atomic_exchange_n(state_, 2, __ATOMIC_ACQUIRE) == 2) {
      }
    }
  }
  ~FusedGroupLock() {
    if (state_ != nullptr) {
      __atomic_store_n(state_, 1, __ATOMIC_RELEASE);
    }
  }
  FusedGroupLock(const FusedGroupLock&) = delete;
  FusedGroupLock& operator=(const FusedGroupLock&) = delete;
  FusedGroupLock(FusedGroupLock&&) = delete;
  FusedGroupLock& operator=(FusedGroupLock&&) = delete;

 private:
  uint8_t* state_;
};

struct FusedRawBlock {
  static constexpr size_t kGroups = 1024;
  explicit FusedRawBlock(size_t aggregates, bool has_stat = false,
                         bool has_double = true, bool has_int = true)
      : doubles(has_double ? kGroups * aggregates : 0),
        ints(has_int ? kGroups * aggregates : 0),
        counts(kGroups * aggregates),
        flags(kGroups * aggregates),
        ext_sx(has_stat ? kGroups * aggregates : 0),
        ext_sxx(has_stat ? kGroups * aggregates : 0) {}
  std::vector<double> doubles;
  std::vector<int64_t> ints;
  std::vector<int64_t> counts;
  std::vector<uint8_t> flags;
  std::vector<platform::ExtendedFloat> ext_sx;
  std::vector<platform::ExtendedFloat> ext_sxx;
};

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

struct FusedGroup {
  // Canonical group key, kept so the merge re-probes the shared map without
  // re-evaluating the key programs.
  Row key;
  std::unique_ptr<Row> representative;
  const Row* rep_left{nullptr};
  uint32_t rep_col_chunk{kInvalidRightIdx};
  uint32_t rep_col_row{0};
  uint32_t rep_right_idx{kInvalidRightIdx};
  size_t state_offset{0};
  // count-star-only fusions accumulate here instead of through
  // AggregateAccumulator states: the per-pair work is one integer increment
  // and the merge is an integer add.
  int64_t raw_count{0};
  uint64_t key_hash{0};
  bool has_key_hash{false};
  // COUNT/group-presence needs none of these arrays. Other aggregations use
  // exactly their aggregate count in stable blocks instead of eight cells
  // in every group, including every count-only group.
  // The worker/merge arena owns the block through result materialization.
  FusedRawBlock* raw_owner{nullptr};
  size_t raw_base{0};
  [[nodiscard]] double* RawDoubles() const {
    return raw_owner->doubles.data() + raw_base;
  }
  [[nodiscard]] int64_t* RawInts() const {
    return raw_owner->ints.data() + raw_base;
  }
  [[nodiscard]] int64_t* RawCounts() const {
    return raw_owner->counts.data() + raw_base;
  }
  [[nodiscard]] uint8_t* RawInfFlags() const {
    return raw_owner->flags.data() + raw_base;
  }
  [[nodiscard]] platform::ExtendedFloat* RawStatX() const {
    return raw_owner->ext_sx.data() + raw_base;
  }
  [[nodiscard]] platform::ExtendedFloat* RawStatXX() const {
    return raw_owner->ext_sxx.data() + raw_base;
  }
  std::unique_ptr<FlatIntKeyMap<uint8_t>> raw_int_set;
};

struct FusedRawArena {
  std::vector<std::unique_ptr<FusedRawBlock>> blocks;
  size_t used{FusedRawBlock::kGroups};
  bool has_stat{false};
  bool has_double{true};
  bool has_int{true};
  void Initialize(FusedGroup& group, size_t aggregates) {
    if (group.raw_owner != nullptr || aggregates == 0) {
      return;
    }
    if (used == FusedRawBlock::kGroups) {
      blocks.push_back(std::make_unique<FusedRawBlock>(aggregates, has_stat,
                                                       has_double, has_int));
      used = 0;
    }
    const size_t base = used++ * aggregates;
    group.raw_owner = blocks.back().get();
    group.raw_base = base;
  }
};

// Bulk raw-count state: one map entry per distinct build key replaces the
// whole bucket chain.  `r_idx` keeps the most recently inserted build row for
// that key -- the row a bucket-head walk would have filled a new group's
// representative from.
struct BulkCountValue {
  int64_t count{0};
  size_t pairs{0};
  uint32_t r_idx{kInvalidRightIdx};
};

struct CompactCountGroup {
  uint64_t hash{0};
  size_t next{SIZE_MAX};
  int64_t count{0};
  const Row* left{nullptr};
  uint32_t right_idx{kInvalidRightIdx};
};

struct FusedWorkerLocal {
  FusedRawArena raw_arena;
  GroupKeyMap offsets;
  // Single-INT64/DATE group-key fast path: raw-payload probes skip the
  // Value-vector hashing the generic map pays per pair.  Key spaces of the
  // two maps are disjoint (NULL keys route to the generic map, which groups
  // NULLs), so a group lives in exactly one of them.
  FlatIntKeyMap<size_t> int_offsets;
  std::vector<FusedGroup> groups;
  std::vector<CompactCountGroup> compact_counts;
  std::vector<std::vector<uint64_t>> compact_split_shards;
  size_t compact_count_inputs{0};
  bool compact_count_log{false};
  std::deque<AggregateAccumulator> states;
  // Reusable probe key: avoids a fresh vector per probe row (see
  // accumulate_into).  The transparent group-key hash/equal pair accepts the
  // bare value vector, so probes never construct a Row.
  std::vector<Value> key_scratch;
  std::vector<const Value*> key_ptrs;
  std::deque<Row> owned_rep_left;
  std::array<uint32_t, 8192> local_2key_gid{};
  bool local_2key_gid_init{false};
  std::array<uint32_t, 4> cur_pk_subgids{};
  bool have_cur_pk_subgids{false};
  Status error{Status::kSuccess};
  size_t comparisons{0};
};

}  // namespace

ProgramShape ClassifyProgram(const BytecodeProgram& program,
                             const Expression& expr, const Schema* schema) {
  ProgramShape shape;
  shape.fast = program.FastInterpretable();
  const std::vector<BytecodeInstruction>& instructions = program.Instructions();
  if (instructions.size() == 1 &&
      instructions[0].opcode == BytecodeOp::kLoadColumn) {
    shape.direct_column = instructions[0].operand;
  } else if (instructions.size() == 2 &&
             instructions[0].opcode == BytecodeOp::kLoadColumn &&
             instructions[1].opcode == BytecodeOp::kExtractDatePart) {
    shape.extract_date_col = static_cast<int32_t>(instructions[0].operand);
    shape.extract_date_part = instructions[1].operand;
  } else if (instructions.size() == 3 &&
             instructions[0].opcode == BytecodeOp::kLoadColumn &&
             instructions[1].opcode == BytecodeOp::kLoadColumn &&
             instructions[2].opcode == BytecodeOp::kBinaryDouble &&
             instructions[2].binary == BinaryOperation::kMultiply) {
    shape.mul_col0 = static_cast<int32_t>(instructions[0].operand);
    shape.mul_col1 = static_cast<int32_t>(instructions[1].operand);
  } else if (instructions.size() == 5 &&
             instructions[0].opcode == BytecodeOp::kLoadColumn &&
             instructions[1].opcode == BytecodeOp::kLoadConstant &&
             instructions[2].opcode == BytecodeOp::kLoadColumn &&
             instructions[3].opcode == BytecodeOp::kBinaryDouble &&
             instructions[3].binary == BinaryOperation::kSubtract &&
             instructions[4].opcode == BytecodeOp::kBinaryDouble &&
             instructions[4].binary == BinaryOperation::kMultiply) {
    const Value& k = program.Constants()[instructions[1].operand];
    if (!k.IsNull() && (k.type == ValueType::kDouble ||
                        (k.type == ValueType::kInt64 && !k.IsUnsigned()))) {
      shape.mul_one_minus_col0 = static_cast<int32_t>(instructions[0].operand);
      shape.mul_one_minus_col1 = static_cast<int32_t>(instructions[2].operand);
      shape.mul_one_minus_const = k.type == ValueType::kDouble
                                      ? k.value.double_value
                                      : static_cast<double>(k.value.int_value);
    }
  } else if (instructions.size() == 9 &&
             instructions[0].opcode == BytecodeOp::kLoadColumn &&
             instructions[1].opcode == BytecodeOp::kLoadConstant &&
             instructions[2].opcode == BytecodeOp::kLoadColumn &&
             instructions[3].opcode == BytecodeOp::kBinaryDouble &&
             instructions[3].binary == BinaryOperation::kSubtract &&
             instructions[4].opcode == BytecodeOp::kBinaryDouble &&
             instructions[4].binary == BinaryOperation::kMultiply &&
             instructions[5].opcode == BytecodeOp::kLoadColumn &&
             instructions[6].opcode == BytecodeOp::kLoadColumn &&
             instructions[7].opcode == BytecodeOp::kBinaryDouble &&
             instructions[7].binary == BinaryOperation::kMultiply &&
             instructions[8].opcode == BytecodeOp::kBinaryDouble &&
             instructions[8].binary == BinaryOperation::kSubtract) {
    const Value& k = program.Constants()[instructions[1].operand];
    if (!k.IsNull() && (k.type == ValueType::kDouble ||
                        (k.type == ValueType::kInt64 && !k.IsUnsigned()))) {
      shape.profit_col0 = static_cast<int32_t>(instructions[0].operand);
      shape.profit_col1 = static_cast<int32_t>(instructions[2].operand);
      shape.profit_col2 = static_cast<int32_t>(instructions[5].operand);
      shape.profit_col3 = static_cast<int32_t>(instructions[6].operand);
      shape.profit_const = k.type == ValueType::kDouble
                               ? k.value.double_value
                               : static_cast<double>(k.value.int_value);
    }
  }
  if (instructions.size() == 3 &&
      instructions[2].opcode == BytecodeOp::kBinaryVarchar &&
      (instructions[2].binary == BinaryOperation::kLike ||
       instructions[2].binary == BinaryOperation::kNotLike) &&
      instructions[0].opcode == BytecodeOp::kLoadColumn &&
      instructions[1].opcode == BytecodeOp::kLoadConstant) {
    // The stack pops right then left, so the constant is the pattern only
    // when it was pushed second.  A collated pattern never reaches the fast
    // interpreter (ProgramIsFastInterpretable keeps such programs canonical);
    // mirror that gate so both paths agree.
    const Value& pattern = program.Constants()[instructions[1].operand];
    if (pattern.type == ValueType::kVarChar && pattern.Collation() == 0) {
      shape.like_column = static_cast<int32_t>(instructions[0].operand);
      shape.like_pattern = pattern.value.varchar_value;
      shape.like_negated = instructions[2].binary == BinaryOperation::kNotLike;
    }
  }
  if (expr && schema != nullptr && expr->Type() == TypeTag::kCaseExp &&
      !shape.HasPairDirect()) {
    const CaseExpression& ce = expr->AsCaseExpression();
    if (ce.when_clauses_.size() == 1 &&
        (!ce.else_clause_ ||
         ce.else_clause_->Type() == TypeTag::kConstantValue)) {
      const Value null_else;
      const Value& ev = ce.else_clause_
                            ? ce.else_clause_->AsConstantValue().GetValue()
                            : null_else;
      const bool else_zero_int = !ev.IsNull() && ev.type == ValueType::kInt64 &&
                                 !ev.IsUnsigned() && ev.value.int_value == 0;
      const bool else_zero_double = !ev.IsNull() &&
                                    ev.type == ValueType::kDouble &&
                                    ev.value.double_value == 0.0;
      if (else_zero_int || else_zero_double || ev.IsNull()) {
        const Expression& cond = ce.when_clauses_[0].first;
        const Expression& val = ce.when_clauses_[0].second;
        ProgramShape::CaseCondKind ck = ProgramShape::CaseCondKind::kNone;
        int32_t ccol = -1;
        int64_t cint = 0;
        std::string_view cstr0;
        std::string_view cstr1;
        if (cond && cond->Type() == TypeTag::kBinaryExp) {
          const BinaryExpression& cb = cond->AsBinaryExpression();
          if (cb.Op() == BinaryOperation::kEquals && cb.Left() && cb.Right() &&
              cb.Left()->Type() == TypeTag::kColumnValue &&
              cb.Right()->Type() == TypeTag::kConstantValue) {
            const auto off = LocalColumnOffset(
                *schema, cb.Left()->AsColumnValue().GetColumnName());
            const Value& cv = cb.Right()->AsConstantValue().GetValue();
            if (off.has_value() && !cv.IsNull()) {
              const Column& sc = schema->GetColumn(*off);
              if (sc.Type() == ValueType::kInt64 && !sc.IsUnsigned() &&
                  cv.type == ValueType::kInt64 && !cv.IsUnsigned()) {
                ck = ProgramShape::CaseCondKind::kColEqInt;
                ccol = static_cast<int32_t>(*off);
                cint = cv.value.int_value;
              } else if (sc.Type() == ValueType::kVarChar &&
                         cv.type == ValueType::kVarChar &&
                         cv.Collation() == 0 && !cv.IsCaseInsensitive()) {
                ck = ProgramShape::CaseCondKind::kColEqStr;
                ccol = static_cast<int32_t>(*off);
                cstr0 = cv.value.varchar_value;
              }
            }
          } else if (cb.Op() == BinaryOperation::kLike && cb.Left() &&
                     cb.Right() && cb.Left()->Type() == TypeTag::kColumnValue &&
                     cb.Right()->Type() == TypeTag::kConstantValue) {
            const auto off = LocalColumnOffset(
                *schema, cb.Left()->AsColumnValue().GetColumnName());
            const Value& cv = cb.Right()->AsConstantValue().GetValue();
            if (off.has_value() &&
                schema->GetColumn(*off).Type() == ValueType::kVarChar &&
                !cv.IsNull() && cv.type == ValueType::kVarChar &&
                cv.Collation() == 0 && !cv.IsCaseInsensitive()) {
              const std::string_view pat = cv.value.varchar_value;
              if (pat.size() >= 2 && pat.back() == '%' &&
                  pat.substr(0, pat.size() - 1).find_first_of("%_") ==
                      std::string_view::npos) {
                ck = ProgramShape::CaseCondKind::kColLikePrefix;
                ccol = static_cast<int32_t>(*off);
                cstr0 = pat.substr(0, pat.size() - 1);
              }
            }
          } else if ((cb.Op() == BinaryOperation::kOr ||
                      cb.Op() == BinaryOperation::kAnd) &&
                     cb.Left() && cb.Right() &&
                     cb.Left()->Type() == TypeTag::kBinaryExp &&
                     cb.Right()->Type() == TypeTag::kBinaryExp) {
            const BinaryExpression& b0 = cb.Left()->AsBinaryExpression();
            const BinaryExpression& b1 = cb.Right()->AsBinaryExpression();
            const BinaryOperation exp_op = (cb.Op() == BinaryOperation::kOr)
                                               ? BinaryOperation::kEquals
                                               : BinaryOperation::kNotEquals;
            if (b0.Op() == exp_op && b1.Op() == exp_op && b0.Left() &&
                b0.Right() && b1.Left() && b1.Right() &&
                b0.Left()->Type() == TypeTag::kColumnValue &&
                b0.Right()->Type() == TypeTag::kConstantValue &&
                b1.Left()->Type() == TypeTag::kColumnValue &&
                b1.Right()->Type() == TypeTag::kConstantValue) {
              const auto off0 = LocalColumnOffset(
                  *schema, b0.Left()->AsColumnValue().GetColumnName());
              const auto off1 = LocalColumnOffset(
                  *schema, b1.Left()->AsColumnValue().GetColumnName());
              const Value& v0 = b0.Right()->AsConstantValue().GetValue();
              const Value& v1 = b1.Right()->AsConstantValue().GetValue();
              if (off0.has_value() && off1.has_value() && *off0 == *off1 &&
                  schema->GetColumn(*off0).Type() == ValueType::kVarChar &&
                  !v0.IsNull() && !v1.IsNull() &&
                  v0.type == ValueType::kVarChar &&
                  v1.type == ValueType::kVarChar && v0.Collation() == 0 &&
                  v1.Collation() == 0 && !v0.IsCaseInsensitive() &&
                  !v1.IsCaseInsensitive()) {
                ck = (cb.Op() == BinaryOperation::kOr)
                         ? ProgramShape::CaseCondKind::kColEq2StrOr
                         : ProgramShape::CaseCondKind::kColNe2StrAnd;
                ccol = static_cast<int32_t>(*off0);
                cstr0 = v0.value.varchar_value;
                cstr1 = v1.value.varchar_value;
              }
            }
          }
        }
        if (ck == ProgramShape::CaseCondKind::kNone && cond) {
          const auto terms = SplitConjuncts(cond);
          bool valid = !terms.empty() && terms.size() <= 2;
          for (const auto& term : terms) {
            if (!valid || term->Type() != TypeTag::kBinaryExp) {
              valid = false;
              break;
            }
            const auto& comparison = term->AsBinaryExpression();
            const auto op = comparison.Op();
            if (op != BinaryOperation::kEquals &&
                op != BinaryOperation::kLessThan &&
                op != BinaryOperation::kLessThanEquals &&
                op != BinaryOperation::kGreaterThan &&
                op != BinaryOperation::kGreaterThanEquals) {
              valid = false;
              break;
            }
            auto constant =
                comparison.Right() &&
                        comparison.Right()->Type() == TypeTag::kConstantValue
                    ? std::optional<Value>(
                          comparison.Right()->AsConstantValue().GetValue())
                    : TryDecideConstantConjunct(comparison.Right());
            if (!constant || constant->type != ValueType::kInt64 ||
                constant->IsUnsigned()) {
              valid = false;
              break;
            }
            Expression lhs = comparison.Left(), rhs;
            if (lhs && lhs->Type() == TypeTag::kBinaryExp &&
                lhs->AsBinaryExpression().Op() == BinaryOperation::kSubtract) {
              rhs = lhs->AsBinaryExpression().Right();
              lhs = lhs->AsBinaryExpression().Left();
            }
            const auto column = [&](const Expression& e) -> int32_t {
              if (!e || e->Type() != TypeTag::kColumnValue) {
                return -1;
              }
              auto offset = LocalColumnOffset(
                  *schema, e->AsColumnValue().GetColumnName());
              if (!offset ||
                  schema->GetColumn(*offset).Type() != ValueType::kInt64 ||
                  schema->GetColumn(*offset).IsUnsigned()) {
                return -1;
              }
              return static_cast<int32_t>(*offset);
            };
            const int32_t left_col = column(lhs);
            const int32_t right_col = column(rhs);
            if (left_col < 0 || (rhs && right_col < 0)) {
              valid = false;
              break;
            }
            shape.case_int_comparisons.push_back(
                {left_col, right_col, op, constant->value.int_value});
          }
          if (valid) {
            ck = ProgramShape::CaseCondKind::kIntComparisons;
          } else {
            shape.case_int_comparisons.clear();
          }
        }
        ProgramShape::CaseValKind vk = ProgramShape::CaseValKind::kNone;
        int32_t vcol0 = -1;
        int32_t vcol1 = -1;
        double vconst = 1.0;
        if (ck != ProgramShape::CaseCondKind::kNone && val) {
          if (else_zero_int && val->Type() == TypeTag::kConstantValue) {
            const Value& tv = val->AsConstantValue().GetValue();
            if (!tv.IsNull() && tv.type == ValueType::kInt64 &&
                !tv.IsUnsigned() && tv.value.int_value == 1) {
              vk = ProgramShape::CaseValKind::kIntConst1;
            }
          } else if (val->Type() == TypeTag::kColumnValue) {
            const auto voff = LocalColumnOffset(
                *schema, val->AsColumnValue().GetColumnName());
            if (voff.has_value() &&
                schema->GetColumn(*voff).Type() == ValueType::kDouble) {
              vk = ProgramShape::CaseValKind::kDirectDoubleCol;
              vcol0 = static_cast<int32_t>(*voff);
            }
          } else if (val->Type() == TypeTag::kBinaryExp) {
            std::optional<BytecodeProgram> vprog =
                BytecodeCompiler::CompileRowCached(
                    val, *schema, BytecodeCompiler::Context::kValue);
            if (vprog.has_value()) {
              const ProgramShape vs = ClassifyProgram(*vprog);
              if (vs.mul_one_minus_col0 >= 0) {
                vk = ProgramShape::CaseValKind::kMulOneMinus;
                vcol0 = vs.mul_one_minus_col0;
                vcol1 = vs.mul_one_minus_col1;
                vconst = vs.mul_one_minus_const;
              }
            }
          }
        }
        if (ck != ProgramShape::CaseCondKind::kNone &&
            vk != ProgramShape::CaseValKind::kNone) {
          shape.case_cond = ck;
          shape.case_val = vk;
          shape.case_else_null = ev.IsNull();
          shape.case_cond_col = ccol;
          shape.case_cond_int = cint;
          shape.case_cond_str0 = cstr0;
          shape.case_cond_str1 = cstr1;
          shape.case_val_col0 = vcol0;
          shape.case_val_col1 = vcol1;
          shape.case_val_const = vconst;
        }
      }
    }
  }
  return shape;
}

// Column cells referenced by a program's loads (the scratch-mask copy set).
void CollectLoadCells(const BytecodeProgram& program,
                      std::vector<slot_t>* cells) {
  for (const BytecodeInstruction& instruction : program.Instructions()) {
    if (instruction.opcode == BytecodeOp::kLoadColumn) {
      cells->push_back(instruction.operand);
    }
  }
}

StatusOr<Value> EvaluateProgramValue(const BytecodeProgram& program,
                                     const ProgramShape& shape,
                                     const Row& scratch) {
  if (shape.direct_column >= 0) {
    return scratch[static_cast<slot_t>(shape.direct_column)];
  }
  if (shape.like_column >= 0) {
    const Value& cell = scratch[static_cast<slot_t>(shape.like_column)];
    // Collated operands keep the canonical verdict (case folding and the
    // '_' collation raise live in the evaluator); NULL propagates as the
    // comparison NULL it is, so both fall through to the program.
    if (!cell.IsNull() && cell.type == ValueType::kVarChar &&
        cell.Collation() == 0 && !cell.IsCaseInsensitive()) {
      const bool matched = Like(cell.value.varchar_value, shape.like_pattern);
      return Value(shape.like_negated ? !matched : matched);
    }
  }
  if (shape.fast) {
    Value out;
    Status error{Status::kSuccess};
    switch (program.TryEvaluateRowFast(scratch, &out, &error)) {
      case BytecodeProgram::FastEval::kDone:
        return out;
      case BytecodeProgram::FastEval::kError:
        return error;
      case BytecodeProgram::FastEval::kUnsupported:
        break;
    }
  }
  return program.TryEvaluateRow(scratch);
}

namespace {

// A scalar aggregate over NULL tests depends on a small nullness signature,
// not on the joined payload. Count signatures during a FULL join, preserving
// duplicate matches and each unmatched build/probe row. Only nonnegative
// integer SUM inputs qualify, so folding repeated inputs cannot hide overflow.
StatusOr<std::optional<Relation>> TryNullSignatureFullAggregate(
    TransactionContext& context, const Relation& left, const Relation& right,
    const std::vector<Expression>& predicates, const Scope* outer,
    const CteMap& ctes, const GroupedJoinFusionSpec& fusion) {
  if (!fusion.group_keys.empty() || fusion.having ||
      fusion.post_having_active || fusion.rollup_count_distribution ||
      left.HasSpill() || right.HasSpill() || left.using_columns ||
      right.using_columns || right.rows.size() >= UINT32_MAX) {
    return std::optional<Relation>{};
  }
  const Schema schema = left.schema + right.schema;
  const auto keys = EqualityKeys(left.schema, right.schema, predicates);
  if (keys.empty() || keys.size() > CompositeIntKey::kMaxParts ||
      HasMixedKeySignednessOrType(left, right, keys) ||
      !ResidualJoinPredicates(left.schema, right.schema, predicates).empty()) {
    return std::optional<Relation>{};
  }
  std::vector<slot_t> left_keys, right_keys;
  for (const auto& key : keys) {
    const auto type = left.schema.GetColumn(key.left).Type();
    if (key.null_safe || key.left_offset != 0 || key.right_offset != 0 ||
        type != right.schema.GetColumn(key.right).Type() ||
        (type != ValueType::kInt64 && type != ValueType::kDate)) {
      return std::optional<Relation>{};
    }
    left_keys.push_back(static_cast<slot_t>(key.left));
    right_keys.push_back(static_cast<slot_t>(key.right));
  }
  std::vector<slot_t> null_columns;
  const auto null_expression = [&](const auto& self,
                                   const Expression& expression) -> bool {
    if (!expression || ContainsParameterSlot(expression)) {
      return false;
    }
    if (expression->Type() == TypeTag::kConstantValue) {
      const auto& value = expression->AsConstantValue().GetValue();
      return value.IsNull() ||
             (value.type == ValueType::kInt64 && !value.IsUnsigned() &&
              value.value.int_value >= 0);
    }
    if (expression->Type() == TypeTag::kUnaryExp) {
      const auto& unary = expression->AsUnaryExpression();
      if (unary.Op() == UnaryOperation::kNot) {
        return self(self, unary.Child());
      }
      if ((unary.Op() != UnaryOperation::kIsNull &&
           unary.Op() != UnaryOperation::kIsNotNull) ||
          !unary.Child() || unary.Child()->Type() != TypeTag::kColumnValue) {
        return false;
      }
      const auto slot = LocalColumnOffset(
          schema, unary.Child()->AsColumnValue().GetColumnName());
      if (!slot) {
        return false;
      }
      if (std::ranges::find(null_columns, *slot) == null_columns.end()) {
        null_columns.push_back(static_cast<slot_t>(*slot));
      }
      return null_columns.size() <= 8;
    }
    if (expression->Type() == TypeTag::kBinaryExp) {
      const auto& binary = expression->AsBinaryExpression();
      return (binary.Op() == BinaryOperation::kAnd ||
              binary.Op() == BinaryOperation::kOr) &&
             self(self, binary.Left()) && self(self, binary.Right());
    }
    if (expression->Type() == TypeTag::kCaseExp) {
      const auto& branch = expression->AsCaseExpression();
      for (const auto& [condition, value] : branch.when_clauses_) {
        if (!self(self, condition) || !self(self, value)) {
          return false;
        }
      }
      return !branch.else_clause_ || self(self, branch.else_clause_);
    }
    return false;
  };
  for (size_t i = 0; i < fusion.aggregates.size(); ++i) {
    const auto* aggregate = fusion.aggregates[i];
    if ((aggregate->GetType() != AggregationType::kSum &&
         aggregate->GetType() != AggregationType::kCount) ||
        aggregate->Distinct() ||
        (!fusion.count_star[i] &&
         !null_expression(null_expression, aggregate->Child()))) {
      return std::optional<Relation>{};
    }
  }
  // The ordinary grouped emitter can evaluate scalar expressions of the
  // finished aggregates; no input column may escape the aggregate boundary.
  const size_t signature_count = size_t{1} << null_columns.size();
  const size_t bytes = (right.rows.size() * 128) + (signature_count * 16);
  if (!QueryMemoryBudget::Global().CanReserve(bytes)) {
    return std::optional<Relation>{};
  }
  QueryMemoryCharge charge(bytes);
  const auto started = std::chrono::steady_clock::now();
  const size_t left_width = left.schema.ColumnCount();
  const auto signature = [&](const Row* lhs, const Row* rhs) {
    size_t mask = 0;
    for (size_t i = 0; i < null_columns.size(); ++i) {
      const size_t column = null_columns[i];
      const bool is_null =
          column < left_width
              ? lhs == nullptr || (*lhs)[column].IsNull()
              : rhs == nullptr || (*rhs)[column - left_width].IsNull();
      mask |= static_cast<size_t>(is_null) << i;
    }
    return mask;
  };
  FlatJoinBuckets<CompositeIntKey, CompositeIntKeyHash> buckets;
  buckets.reserve(right.rows.size());
  for (const Row& row : right.rows) {
    if (!HasNullKey(row, right_keys)) {
      buckets.emplace(EncodeCompositeIntKey(row, right_keys), &row);
    }
  }
  std::vector<uint64_t> counts(signature_count, 0);
  std::vector<uint8_t> matched(right.rows.size(), 0);
  uint64_t comparisons = 0;
  for (const Row& row : left.rows) {
    bool found = false;
    if (!HasNullKey(row, left_keys)) {
      const auto [begin, end] =
          buckets.equal_range(EncodeCompositeIntKey(row, left_keys));
      for (auto iter = begin; iter != end; ++iter) {
        ++comparisons;
        ++counts[signature(&row, iter->second)];
        matched[static_cast<size_t>(iter->second - right.rows.data())] = 1;
        found = true;
      }
    }
    if (!found) {
      ++counts[signature(&row, nullptr)];
    }
  }
  for (size_t i = 0; i < right.rows.size(); ++i) {
    if (matched[i] == 0) {
      ++counts[signature(nullptr, &right.rows[i])];
    }
  }
  Row representative(std::vector<Value>(schema.ColumnCount(), Value(1)));
  Scope scope{.row = &representative, .schema = &schema, .outer = outer};
  AggregateResultMap finished;
  for (size_t i = 0; i < fusion.aggregates.size(); ++i) {
    const auto* aggregate = fusion.aggregates[i];
    const bool is_count = aggregate->GetType() == AggregationType::kCount;
    int64_t sum = 0;
    bool any = false;
    for (size_t mask = 0; mask < signature_count; ++mask) {
      if (counts[mask] == 0) {
        continue;
      }
      for (size_t c = 0; c < null_columns.size(); ++c) {
        representative.values_[null_columns[c]] =
            (mask & (size_t{1} << c)) != 0 ? Value() : Value(1);
      }
      Value value(1);
      if (!fusion.count_star[i]) {
        ASSIGN_OR_RETURN(
            Value, evaluated,
            TryEvaluate(aggregate->Child(), scope, nullptr, context, ctes));
        value = std::move(evaluated);
      }
      if (value.IsNull()) {
        continue;
      }
      const int64_t contribution = is_count ? 1 : value.value.int_value;
      int64_t increment = 0;
      if (counts[mask] > static_cast<uint64_t>(INT64_MAX) ||
          __builtin_mul_overflow(
              contribution, static_cast<int64_t>(counts[mask]), &increment) ||
          __builtin_add_overflow(sum, increment, &sum)) {
        return StatusError(StatusCode::kIsInfinity,
                           "integer overflow in aggregate");
      }
      any = true;
    }
    finished.emplace(aggregate, any || is_count ? Value(sum) : Value());
  }
  Row output;
  std::vector<Column> columns;
  for (size_t i = 0; i < fusion.select_items.size(); ++i) {
    ASSIGN_OR_RETURN(
        Value, value,
        TryEvaluate(fusion.select_items[i], scope, &finished, context, ctes));
    columns.emplace_back(fusion.select_names[i], ValueTypeOf(value));
    columns.back().SetUnsigned(value.IsUnsigned());
    output.values_.push_back(std::move(value));
  }
  Relation result(context.execution_runtime());
  result.schema = Schema("", std::move(columns));
  result.hash_joins = left.hash_joins + right.hash_joins + 1;
  result.in_memory_hash_joins =
      left.in_memory_hash_joins + right.in_memory_hash_joins + 1;
  result.hybrid_hash_joins = left.hybrid_hash_joins + right.hybrid_hash_joins;
  result.nested_loop_joins = left.nested_loop_joins + right.nested_loop_joins;
  result.join_comparisons =
      left.join_comparisons + right.join_comparisons + comparisons;
  result.peak_intermediate_rows =
      std::max(left.peak_intermediate_rows, right.peak_intermediate_rows);
  result.AddRow(std::move(output));
  RETURN_IF_FAIL(result.FinishSpill());
  RETURN_IF_FAIL(result.GetStatus());
  if (auto* runtime = context.execution_runtime()) {
    ++runtime->grouped_join_fusions;
    runtime->join_ms += ElapsedMs(started);
  }
  return std::optional<Relation>(std::move(result));
}

}  // namespace

// Tries to fuse the final join of a grouped statement with the grouping
// itself: matched rows (and, for LEFT joins, unmatched left rows padded with
// NULLs) feed group keys and aggregate inputs straight into per-worker
// accumulators, and the joined relation is never materialized.  The emit
// mirrors Project's generic grouped path: per group, accumulators finish
// into an AggregateResultMap and the select list evaluates against the
// representative row.
//
// An empty optional means "shape not fusable" and the caller falls back to
// the materializing join; a real Status propagates.  Gates that need the
// joined schema live here: a resident build side, no null-safe keys, an
// equality join, and every per-row expression (join residuals, group keys,
// aggregate children) compiled to context-free bytecode.
StatusOr<std::optional<Relation>> TryFusedGroupedJoin(
    TransactionContext& context, const Relation& left,
    const Relation& right_arg, const std::vector<Expression>& predicates,
    JoinType join_type, const Scope* outer, const CteMap& ctes,
    GroupedJoinFusionSpec* fusion,
    const DeferredLSTableProbe* deferred_left_probe) {
  static const bool disabled =
      std::getenv("TINYLAMB_DISABLE_GROUPED_JOIN_FUSION") != nullptr;
  const bool debug_fusion = std::getenv("TINYLAMB_FUSION_DEBUG") != nullptr;
  const auto not_fusable = [&](const char* why) {
    if (debug_fusion) {
      std::cerr << "[fusion] not fusable: " << why << "\n";
    }
    return std::optional<Relation>{};
  };
  const size_t effective_left_rows =
      deferred_left_probe != nullptr && deferred_left_probe->store != nullptr
          ? deferred_left_probe->store->total_rows
          : left.TotalRows();
  if (debug_fusion) {
    std::cerr << "[fusion] try: eligible="
              << (fusion != nullptr && fusion->eligible)
              << " join_type=" << static_cast<int>(join_type)
              << " left_rows=" << effective_left_rows << "\n";
  }
  auto materialize_columnar_side =
      [&](const Relation& side,
          std::optional<Relation>& holder) -> StatusOr<const Relation*> {
    if (!side.HasColumnarOnly()) {
      return &side;
    }
    Relation mat;
    mat.schema = side.schema;
    mat.rows.reserve(side.TotalRows());
    bool uniform_ok = true;
    RETURN_IF_FAIL(MaterializeColumnarTableToRows(*side.columnar, side.schema,
                                                  &mat.rows, &uniform_ok));
    mat.uniform_cell_types = uniform_ok && !mat.rows.empty();
    holder.emplace(std::move(mat));
    return &*holder;
  };
  if (!disabled && fusion != nullptr && fusion->eligible &&
      !fusion->aggregates.empty() && !fusion->require_exact_integer_totals &&
      join_type == JoinType::kFull && deferred_left_probe == nullptr) {
    std::optional<Relation> left_full_holder;
    std::optional<Relation> right_full_holder;
    ASSIGN_OR_RETURN(const Relation*, full_left_ptr,
                     materialize_columnar_side(left, left_full_holder));
    ASSIGN_OR_RETURN(const Relation*, full_right_ptr,
                     materialize_columnar_side(right_arg, right_full_holder));
    const Relation& full_left = *full_left_ptr;
    const Relation& full_right = *full_right_ptr;
    ASSIGN_OR_RETURN(
        std::optional<Relation>, full_res,
        (TryNullSignatureFullAggregate(context, full_left, full_right,
                                       predicates, outer, ctes, *fusion)));
    if (full_res.has_value()) {
      for (Row& r : full_res->rows) {
        for (Value& v : r.values_) {
          if (v.type == ValueType::kVarChar && v.owned_data.empty() &&
              !v.value.varchar_value.empty()) {
            v.owned_data.assign(v.value.varchar_value);
            v.value.varchar_value = v.owned_data;
          }
        }
      }
    }
    return full_res;
  }
  if (disabled || fusion == nullptr || !fusion->eligible ||
      (fusion->aggregates.empty() && fusion->group_keys.empty()) ||
      (join_type != JoinType::kInner && join_type != JoinType::kLeft)) {
    return not_fusable("head");
  }
  if (right_arg.HasSpill() || effective_left_rows == 0) {
    return not_fusable("spill");
  }
  std::optional<Relation> right_holder;
  const size_t total_right_rows = right_arg.TotalRows();
  bool right_col_only = right_arg.HasColumnarOnly() &&
                        total_right_rows < std::numeric_limits<uint32_t>::max();
  std::shared_ptr<const ColumnarTable> right_col_owner =
      right_col_only ? right_arg.columnar : nullptr;
  RightColLocator right_locator;
  if (right_col_only) {
    right_locator.Init(right_arg.columnar.get());
  }
  const Relation* right_ptr = &right_arg;
  // Build rows pointer list (populated if `right` is row-backed or falls back
  // from native columnar mode).
  std::vector<const Row*> build_rows;
  const auto fallback_to_right_rows = [&]() -> StatusOr<const Relation*> {
    if (right_col_only) {
      ASSIGN_OR_RETURN(const Relation*, rmat,
                       materialize_columnar_side(right_arg, right_holder));
      right_ptr = rmat;
      right_col_only = false;
      right_col_owner.reset();
      build_rows.reserve(right_ptr->rows.size());
      for (const Row& row : right_ptr->rows) {
        build_rows.push_back(&row);
      }
    }
    return right_ptr;
  };
  if (right_arg.HasColumnarOnly() && !right_col_only) {
    ASSIGN_OR_RETURN(const Relation*, rmat,
                     materialize_columnar_side(right_arg, right_holder));
    right_ptr = rmat;
  }
  const Relation& right = right_arg;
  if (!right_col_only) {
    build_rows.reserve(right_ptr->rows.size());
    for (const Row& row : right_ptr->rows) {
      build_rows.push_back(&row);
    }
  }
  const auto join_begin = std::chrono::steady_clock::now();
  Relation result(context.execution_runtime());
  result.schema = left.schema + right.schema;
  // A grouping without accumulators observes the set of joined tuples, not
  // their multiplicities. Repeated integral build rows therefore need only
  // one bucket entry, including when residual predicates reject every pair.
  // Keep only direct column projections and comparisons: arbitrary scalar
  // functions or SQL string/float equality can distinguish equivalent keys.
  const auto direct_column = [](const Expression& expression) {
    return expression && expression->Type() == TypeTag::kColumnValue;
  };
  const auto integral_schema = [&] {
    for (size_t i = 0; i < right.schema.ColumnCount(); ++i) {
      const auto type = right.schema.GetColumn(i).Type();
      if (type != ValueType::kInt64 && type != ValueType::kDate) {
        return false;
      }
    }
    return true;
  };
  if (fusion->aggregates.empty() && total_right_rows >= 1024 &&
      integral_schema() &&
      std::ranges::all_of(fusion->group_keys, direct_column) &&
      std::ranges::all_of(fusion->select_items, direct_column) &&
      std::ranges::all_of(predicates, [&](const Expression& predicate) {
        if (!predicate || predicate->Type() != TypeTag::kBinaryExp) {
          return false;
        }
        const auto& binary = predicate->AsBinaryExpression();
        const auto op = binary.Op();
        return (op == BinaryOperation::kEquals ||
                op == BinaryOperation::kNotEquals ||
                op == BinaryOperation::kLessThan ||
                op == BinaryOperation::kLessThanEquals ||
                op == BinaryOperation::kGreaterThan ||
                op == BinaryOperation::kGreaterThanEquals) &&
               direct_column(binary.Left()) && direct_column(binary.Right());
      })) {
    const auto hash_row = [](const Row* row) { return std::hash<Row>{}(*row); };
    const auto equal_row = [](const Row* a, const Row* b) { return *a == *b; };
    if (total_right_rows <= SIZE_MAX / 64) {
      if (auto charge = TryReserveQueryMemory(total_right_rows * 64)) {
        constexpr size_t kSampleRows = 256;
        bool sample_dup = false;
        if (right_col_only) {
          std::vector<Row> sample_storage(kSampleRows);
          const size_t ncols = right.schema.ColumnCount();
          std::unordered_set<const Row*, decltype(hash_row),
                             decltype(equal_row)>
              seen(0, hash_row, equal_row);
          for (size_t i = 0; i < kSampleRows; ++i) {
            sample_storage[i].values_.resize(ncols);
            for (size_t c = 0; c < ncols; ++c) {
              right_locator.WriteValue(c, static_cast<uint32_t>(i),
                                       &sample_storage[i].values_[c]);
            }
            seen.insert(&sample_storage[i]);
          }
          sample_dup = seen.size() <= kSampleRows * 3 / 4;
        } else {
          std::unordered_set<const Row*, decltype(hash_row),
                             decltype(equal_row)>
              seen(0, hash_row, equal_row);
          for (size_t i = 0; i < kSampleRows; ++i) {
            seen.insert(build_rows[i]);
          }
          sample_dup = seen.size() <= kSampleRows * 3 / 4;
        }
        if (sample_dup) {
          ASSIGN_OR_RETURN(const Relation*, rmat_ptr, fallback_to_right_rows());
          const Relation& rmat = *rmat_ptr;
          if (std::ranges::all_of(rmat.rows, [](const Row& row) {
                return std::ranges::all_of(row.values_, [](const Value& value) {
                  return value.IsNull() || value.type == ValueType::kInt64 ||
                         value.type == ValueType::kDate;
                });
              })) {
            std::unordered_set<const Row*, decltype(hash_row),
                               decltype(equal_row)>
                seen(0, hash_row, equal_row);
            seen.reserve(rmat.rows.size());
            size_t kept = 0;
            for (const Row* row : build_rows) {
              if (seen.insert(row).second) {
                build_rows[kept++] = row;
              }
            }
            build_rows.resize(kept);
          }
        }
      }
    }
  }
  const bool singleton_right_relation =
      (join_type == JoinType::kInner && total_right_rows == 1 &&
       predicates.empty() && !left.HasSpill());
  if (singleton_right_relation) {
    RETURN_IF_FAIL(fallback_to_right_rows().GetStatus());
  }
  const std::vector<EqualityKey> equality_keys =
      EqualityKeys(left.schema, right.schema, predicates);
  const std::vector<Expression> residuals =
      HasMixedKeySignednessOrType(left, right, equality_keys)
          ? predicates
          : ResidualJoinPredicates(left.schema, right.schema, predicates);
  if (equality_keys.empty() && !singleton_right_relation) {
    return not_fusable("no equality key");
  }
  const bool has_null_safe_key = std::ranges::any_of(
      equality_keys, [](const EqualityKey& key) { return key.null_safe; });
  if (has_null_safe_key) {
    return not_fusable("null-safe key");
  }

  const size_t left_width = left.schema.ColumnCount();
  const size_t right_width = right.schema.ColumnCount();
  const auto compile_or = [&](const Expression& expression,
                              BytecodeCompiler::Context context_kind)
      -> std::optional<BytecodeProgram> {
    if (!expression) {
      return BytecodeProgram{};
    }
    return BytecodeCompiler::CompileRowCached(expression, result.schema,
                                              context_kind);
  };
  std::vector<BytecodeProgram> residual_programs;
  for (const Expression& predicate : residuals) {
    std::optional<BytecodeProgram> program =
        compile_or(predicate, BytecodeCompiler::Context::kFilter);
    if (!program.has_value()) {
      return not_fusable("residual compile");
    }
    residual_programs.push_back(std::move(*program));
  }
  std::vector<BytecodeProgram> key_programs;
  for (const Expression& key : fusion->group_keys) {
    std::optional<BytecodeProgram> program =
        compile_or(key, BytecodeCompiler::Context::kValue);
    if (!program.has_value()) {
      return not_fusable("key compile");
    }
    key_programs.push_back(std::move(*program));
  }
  std::vector<BytecodeProgram> child_programs;
  for (size_t aggregate_index = 0; aggregate_index < fusion->aggregates.size();
       ++aggregate_index) {
    if (fusion->count_star[aggregate_index]) {
      child_programs.emplace_back();
      continue;
    }
    std::optional<BytecodeProgram> program =
        compile_or(fusion->aggregates[aggregate_index]->Child(),
                   BytecodeCompiler::Context::kValue);
    if (!program.has_value()) {
      return not_fusable("child compile");
    }
    child_programs.push_back(std::move(*program));
  }

  // Pair-loop shape classification (see ProgramShape): direct-column reads,
  // boxing-free fast programs, and canonical fallbacks.
  std::vector<ProgramShape> residual_shapes;
  residual_shapes.reserve(residual_programs.size());
  for (const BytecodeProgram& program : residual_programs) {
    residual_shapes.push_back(ClassifyProgram(program));
  }
  // Build-side residual hoist: a residual conjunct that loads only
  // right-side cells (Q13's `o_comment NOT LIKE ...` under a customer
  // LEFT JOIN orders) does not depend on the probe row, so it can be
  // evaluated once per build row -- inside the worker-parallel relation
  // filter -- instead of once per candidate pair in the fused probe loop.
  // Soundness: removing a right row whose right-only conjunct fails is
  // equivalent to leaving it in and rejecting every pair it forms; for
  // kInner those pairs vanish either way, and for kLeft the matched-false
  // contract null-extends the probe row exactly when all of its bucket
  // entries are gone.  The throw-safe gate keeps the hoisted evaluation
  // from surfacing errors the per-pair path would only reach on
  // hash-matched rows.
  static const bool hoist_disabled =
      std::getenv("TINYLAMB_DISABLE_BUILD_RESIDUAL_HOIST") != nullptr;
  if (!hoist_disabled && !residual_programs.empty() && right.TotalRows() > 0) {
    // Evaluate each right-only residual once per build row (Q13's
    // `o_comment NOT LIKE ...` is row-invariant on the probe side) and drop
    // failing rows from the build pointer list -- no relation copy, and the
    // pair loop loses the per-pair residual dispatch.  Soundness: a dropped
    // right row is one whose every candidate pair would be residual-
    // rejected; kInner loses nothing and kLeft's matched-false contract
    // null-extends the probe row exactly the same.  Any evaluation error
    // abandons the hoist (the per-pair path keeps its exact error behavior).
    std::vector<size_t> hoisted_indices;
    for (size_t i = 0; i < residual_programs.size(); ++i) {
      if (!ExpressionCannotThrowForPlanner(residuals[i])) {
        continue;
      }
      std::vector<slot_t> loads;
      CollectLoadCells(residual_programs[i], &loads);
      if (!loads.empty() &&
          std::all_of(loads.begin(), loads.end(),
                      [&](slot_t cell) { return cell >= left_width; })) {
        hoisted_indices.push_back(i);
      }
    }
    if (!hoisted_indices.empty()) {
      ASSIGN_OR_RETURN(const Relation*, rmat_ptr, fallback_to_right_rows());
      const Relation& rmat = *rmat_ptr;
      std::vector<slot_t> hoist_cells;
      std::vector<ProgramShape> hoist_shapes;
      for (const size_t i : hoisted_indices) {
        CollectLoadCells(residual_programs[i], &hoist_cells);
        hoist_shapes.push_back(residual_shapes[i]);
      }
      Row hoist_scratch;
      hoist_scratch.values_.resize(left_width + right_width);
      bool hoist_error = false;
      std::vector<bool> keep(rmat.rows.size(), true);
      for (size_t ri = 0; ri < rmat.rows.size(); ++ri) {
        const Row& row = rmat.rows[ri];
        for (const slot_t cell : hoist_cells) {
          hoist_scratch.values_[cell] = row.values_[cell - left_width];
        }
        for (size_t i = 0; i < hoisted_indices.size(); ++i) {
          StatusOr<Value> verdict =
              EvaluateProgramValue(residual_programs[hoisted_indices[i]],
                                   hoist_shapes[i], hoist_scratch);
          if (!verdict.HasValue()) {
            hoist_error = true;
            break;
          }
          if (!Truthy(verdict.Value())) {
            keep[ri] = false;
            break;
          }
        }
        if (hoist_error) {
          break;
        }
      }
      if (!hoist_error) {
        std::vector<const Row*> filtered;
        filtered.reserve(build_rows.size());
        for (auto & build_row : build_rows) {
          // `keep` is indexed by the original row position in `rmat.rows`;
          // `build_rows` may have been compacted by an earlier dedup pass, so
          // its own index is a different space. Recover the original position
          // from the row pointer before consulting the verdict.
          const auto ri =
              static_cast<size_t>(build_row - rmat.rows.data());
          if (ri < keep.size() && keep[ri]) {
            filtered.push_back(build_row);
          }
        }
        build_rows = std::move(filtered);
        // Drop the hoisted programs (and their shapes), highest index first.
        for (size_t i = hoisted_indices.size(); i-- > 0;) {
          residual_programs.erase(
              residual_programs.begin() +
              static_cast<std::ptrdiff_t>(hoisted_indices[i]));
          residual_shapes.erase(
              residual_shapes.begin() +
              static_cast<std::ptrdiff_t>(hoisted_indices[i]));
        }
      }
    }
  }
  const auto right_build_empty = [&]() noexcept -> bool {
    return right_col_only ? total_right_rows == 0 : build_rows.empty();
  };
  const auto right_build_count = [&]() noexcept -> size_t {
    return right_col_only ? total_right_rows : build_rows.size();
  };
  std::vector<ProgramShape> key_shapes;
  key_shapes.reserve(key_programs.size());
  for (size_t k = 0; k < key_programs.size(); ++k) {
    key_shapes.push_back(ClassifyProgram(key_programs[k], fusion->group_keys[k],
                                         &result.schema));
  }
  bool verified_unique_right_join_key = false;
  if (join_type == JoinType::kInner &&
      !HasMixedKeySignednessOrType(left, right, equality_keys) &&
      equality_keys.size() == 1 && !right.HasSpill() && !right_build_empty()) {
    const EqualityKey& eq = equality_keys[0];
    if (!eq.null_safe && eq.left_offset == 0 && eq.right_offset == 0) {
      std::optional<size_t> determining_key;
      for (size_t k = 0; k < key_programs.size(); ++k) {
        if (std::cmp_equal(key_shapes[k].direct_column,
                           left_width + eq.right) ||
            std::cmp_equal(key_shapes[k].direct_column, eq.left)) {
          determining_key = k;
          if (std::cmp_equal(key_shapes[k].direct_column,
                             left_width + eq.right)) {
            break;
          }
        }
      }
      if (determining_key.has_value()) {
        const Column& rcol = right.schema.GetColumn(eq.right);
        const bool is_int_col =
            !rcol.IsUnsigned() && (rcol.Type() == ValueType::kInt64 ||
                                   rcol.Type() == ValueType::kDate);
        bool right_key_unique =
            rcol.GetConstraint().ctype == Constraint::kPrimaryKey;
        if (!right_key_unique && is_int_col && right_build_count() <= 131072) {
          FlatIntKeyMap<uint8_t> seen_keys;
          right_key_unique = true;
          if (right_col_only) {
            for (const ColumnarChunk& rch : right_arg.columnar->chunks) {
              const ColumnarColumn& rcc = rch.columns[eq.right];
              for (size_t ri = 0; ri < rch.rows; ++ri) {
                const ColumnarColumn* leaf = &rcc;
                size_t lr = ri;
                if (leaf->encoding == ColumnarColumn::Encoding::kGathered)
                    [[unlikely]] {
                  if (!rcc.ResolveLeaf(ri, &leaf, &lr)) {
                    continue;
                  }
                } else if (leaf->NonGatheredIsNull(lr)) {
                  continue;
                }
                int64_t kv = 0;
                if (leaf->is_uniform) [[likely]] {
                  if (leaf->type != rcol.Type() || leaf->is_unsigned) {
                    right_key_unique = false;
                    break;
                  }
                  kv = leaf->IntAt(lr);
                } else {
                  const Value& v = leaf->fallback[lr];
                  if (v.IsNull()) {
                    continue;
                  }
                  if (v.type != rcol.Type() || v.IsUnsigned()) {
                    right_key_unique = false;
                    break;
                  }
                  kv = v.value.int_value;
                }
                bool inserted = false;
                seen_keys.TryEmplace(kv, &inserted);
                if (!inserted) {
                  right_key_unique = false;
                  break;
                }
              }
              if (!right_key_unique) {
                break;
              }
            }
          } else {
            for (const Row* r : build_rows) {
              const Value& v = r->values_[eq.right];
              if (v.IsNull()) {
                continue;
              }
              if (v.type != rcol.Type() || v.IsUnsigned()) {
                right_key_unique = false;
                break;
              }
              bool inserted = false;
              seen_keys.TryEmplace(v.value.int_value, &inserted);
              if (!inserted) {
                right_key_unique = false;
                break;
              }
            }
          }
        }
        if (right_key_unique) {
          if (is_int_col) {
            verified_unique_right_join_key = true;
          }
          if (key_programs.size() >= 2) {
            std::vector<uint8_t> elide_key(key_programs.size(), 0);
            bool any_elided = false;
            for (size_t k = 0; k < key_programs.size(); ++k) {
              if (k == *determining_key) {
                continue;
              }
              const Expression& expr = fusion->group_keys[k];
              if (!expr || ContainsQuery(expr) || ContainsParameterSlot(expr) ||
                  !ExpressionCannotThrowForPlanner(expr)) {
                continue;
              }
              std::vector<slot_t> loads;
              CollectLoadCells(key_programs[k], &loads);
              if (std::all_of(loads.begin(), loads.end(),
                              [&](slot_t c) { return c >= left_width; })) {
                elide_key[k] = 1;
                any_elided = true;
              }
            }
            if (any_elided) {
              std::vector<BytecodeProgram> kept_programs;
              std::vector<ProgramShape> kept_shapes;
              kept_programs.reserve(key_programs.size());
              kept_shapes.reserve(key_shapes.size());
              for (size_t k = 0; k < key_programs.size(); ++k) {
                if (elide_key[k] == 0) {
                  kept_programs.push_back(std::move(key_programs[k]));
                  kept_shapes.push_back(std::move(key_shapes[k]));
                }
              }
              key_programs = std::move(kept_programs);
              key_shapes = std::move(kept_shapes);
            }
          }
        }
      }
    }
  }
  std::vector<ProgramShape> child_shapes;
  child_shapes.reserve(child_programs.size());
  for (size_t i = 0; i < child_programs.size(); ++i) {
    const Expression child_expr =
        fusion->count_star[i] ? Expression() : fusion->aggregates[i]->Child();
    child_shapes.push_back(
        ClassifyProgram(child_programs[i], child_expr, &result.schema));
  }

  // Count-only fusions (Q13: COUNT(o_orderkey) per customer with every other
  // aggregate absent) accumulate a raw per-group integer instead of driving
  // the accumulator machinery 915K times: no per-pair Value boxing, no
  // per-group state construction, and an integer-add merge.  Two shapes
  // qualify, and they differ on unmatched LEFT rows: COUNT(*) counts the
  // null-extended row (+1), while COUNT(not-null column) counts nothing
  // (+0).  A not-null column child (bare load of a kNotNull/kPrimaryKey
  // column) can never contribute a NULL, so its COUNT equals COUNT(*) over
  // matched pairs.  Mixed shapes stay on the generic path.
  // A grouping-only join needs the same key-presence state as COUNT(*).
  // Its count is internal; no aggregate column is emitted.
  bool count_star_only = true;
  for (const bool count_star : fusion->count_star) {
    count_star_only = count_star_only && count_star;
  }
  bool count_nonnull_only = false;
  std::optional<slot_t> count_right_column;
  {
    bool all_count_nonnull = !fusion->aggregates.empty();
    bool any_nonnull = false;
    bool unconditional_matched_count = false;
    for (size_t i = 0; i < fusion->aggregates.size(); ++i) {
      if (fusion->count_star[i]) {
        all_count_nonnull = false;
        break;
      }
      const AggregateExpression* aggregate = fusion->aggregates[i];
      const bool plain_column_child =
          aggregate->GetType() == AggregationType::kCount &&
          child_shapes[i].direct_column >= 0 && !aggregate->Distinct() &&
          !aggregate->WhereFilter() &&
          aggregate->Having() == AggregateHavingModifier::kNone &&
          aggregate->InnerOrderBy().empty() && !aggregate->SecondaryArg() &&
          aggregate->ExtraArgs().empty();
      bool eligible_column = false;
      if (plain_column_child) {
        const auto col_idx = static_cast<size_t>(child_shapes[i].direct_column);
        const Column& column = result.schema.GetColumn(col_idx);
        const Constraint::ConstraintType ctype = column.GetConstraint().ctype;
        if ((ctype == Constraint::kNotNull ||
             ctype == Constraint::kPrimaryKey) &&
            (col_idx < left_width || join_type == JoinType::kInner)) {
          // A shared counter requires identical NULL behavior. A left
          // not-null column also counts the unmatched row of a LEFT JOIN,
          // but a right-side column can still be NULL-padded by an outer
          // join, so its declared kNotNull is not proof there.
          eligible_column = !count_right_column.has_value();
          unconditional_matched_count = true;
        } else if (col_idx >= left_width && !unconditional_matched_count) {
          const auto right_col = static_cast<slot_t>(col_idx - left_width);
          if (!count_right_column.has_value() ||
              *count_right_column == right_col) {
            count_right_column = right_col;
            eligible_column = true;
          }
        }
      }
      if (!eligible_column) {
        all_count_nonnull = false;
        break;
      }
      any_nonnull = true;
    }
    count_nonnull_only = all_count_nonnull && any_nonnull;
  }
  static const bool count_fusion_disabled =
      std::getenv("TINYLAMB_DISABLE_COUNT_STAR_FUSION") != nullptr;
  const bool raw_count_fusion =
      !count_fusion_disabled && (count_star_only || count_nonnull_only);
  enum class RawAggKind : uint8_t {
    kNone = 0,
    kSumDouble,
    kSumInt64,
    kAvgDouble,
    kStddevSamp,
    kStddevPop,
    kVarSamp,
    kVarPop,
    kCountStar,
    kCountValue,
    kConstantExtremum,
    kCountDistinctInt64,
  };
  const auto is_stat_raw_kind = [](RawAggKind kind) noexcept {
    return kind == RawAggKind::kStddevSamp || kind == RawAggKind::kStddevPop ||
           kind == RawAggKind::kVarSamp || kind == RawAggKind::kVarPop;
  };
  std::array<RawAggKind, kRawAggregateCapacity> raw_agg_kinds{};
  std::array<Value, kRawAggregateCapacity> raw_constants{};
  bool raw_sum_fusion = !raw_count_fusion && !fusion->aggregates.empty() &&
                        fusion->aggregates.size() <= kRawAggregateCapacity;
  bool has_count_distinct = false;
  // Several aggregates can read the same column (SUM(c), AVG(c), MIN(c)...).
  // The DOUBLE-admixture proof below scans the whole side once per column;
  // memoize it so a column is scanned only once per fused-join planning pass.
  std::vector<int8_t> left_double_cache(left_width, -1);
  std::vector<int8_t> right_double_cache(right_width, -1);
  const auto column_may_be_double = [&](bool is_left, size_t offset) -> bool {
    auto& cache = is_left ? left_double_cache : right_double_cache;
    if (offset >= cache.size()) {
      return false;
    }
    int8_t& cached = cache[offset];
    if (cached >= 0) {
      return cached != 0;
    }
    bool may = false;
    if (is_left) {
      if (left.HasColumnarOnly() && left.columnar != nullptr) {
        may = ColumnarColMayBeDouble(*left.columnar, static_cast<slot_t>(offset));
      } else {
        for (const Row& r : left.rows) {
          if (offset < r.Count() && r[offset].type == ValueType::kDouble) {
            may = true;
            break;
          }
        }
      }
    } else if (right_col_only && right_arg.columnar != nullptr) {
      may = ColumnarColMayBeDouble(*right_arg.columnar, static_cast<slot_t>(offset));
    } else {
      for (const Row& r : right_ptr->rows) {
        if (offset < r.Count() && r[offset].type == ValueType::kDouble) {
          may = true;
          break;
        }
      }
    }
    cached = may ? 1 : 0;
    return may;
  };
  if (raw_sum_fusion) {
    for (size_t i = 0; i < fusion->aggregates.size(); ++i) {
      if (fusion->count_star[i]) {
        raw_agg_kinds[i] = RawAggKind::kCountStar;
        continue;
      }
      const AggregateExpression* agg = fusion->aggregates[i];
      if (agg->WhereFilter() ||
          agg->Having() != AggregateHavingModifier::kNone ||
          !agg->InnerOrderBy().empty() || agg->SecondaryArg() ||
          !agg->ExtraArgs().empty() || !agg->Child()) {
        raw_sum_fusion = false;
        break;
      }
      // A uniform numeric input column makes MIN/MAX an existence test.
      // Prove uniformity from this snapshot's resident rows, preserving NULL
      // padding and empty groups; no catalog statistic is trusted as a value.
      if (!agg->Distinct() &&
          (agg->GetType() == AggregationType::kMin ||
           agg->GetType() == AggregationType::kMax) &&
          child_shapes[i].direct_column >= 0) {
        const auto column = static_cast<size_t>(child_shapes[i].direct_column);
        const Relation& side = column < left_width ? left : right;
        const size_t offset =
            column < left_width ? column : column - left_width;
        const auto valid_const_type = [](const Value& v) noexcept {
          return (v.type == ValueType::kInt64 || v.type == ValueType::kDouble ||
                  v.IsNull()) &&
                 (v.type != ValueType::kDouble ||
                  !std::isnan(v.value.double_value));
        };
        const auto matches_first = [](const Value& first_val,
                                      const Value& other) noexcept {
          return first_val.type == other.type &&
                 first_val.IsUnsigned() == other.IsUnsigned() &&
                 first_val == other &&
                 (first_val.type != ValueType::kDouble ||
                  std::signbit(first_val.value.double_value) ==
                      std::signbit(other.value.double_value));
        };
        if (!side.HasSpill() && !side.HasColumnarOnly() && !side.rows.empty() &&
            side.rows.size() <= 500000) {
          const Value& value = side.rows.front()[offset];
          if (valid_const_type(value) &&
              std::ranges::all_of(side.rows, [&](const Row& row) {
                return matches_first(value, row[offset]);
              })) {
            raw_agg_kinds[i] = RawAggKind::kConstantExtremum;
            raw_constants[i] = value;
            continue;
          }
        } else if (!side.HasSpill() && side.HasColumnarOnly() &&
                   side.columnar != nullptr) {
          const size_t total_col_rows = side.TotalRows();
          if (total_col_rows > 0 && total_col_rows <= 500000) {
            bool uniform_ok = true;
            bool has_first = false;
            Value first_val;
            for (const ColumnarChunk& chunk : side.columnar->chunks) {
              if (chunk.rows == 0) {
                continue;
              }
              if (offset >= chunk.columns.size()) {
                uniform_ok = false;
                break;
              }
              const ColumnarColumn& col = chunk.columns[offset];
              if (col.encoding == ColumnarColumn::Encoding::kConstant) {
                Value cv;
                col.WriteValueInto(0, &cv);
                if (!has_first) {
                  if (!valid_const_type(cv)) {
                    uniform_ok = false;
                    break;
                  }
                  first_val = cv;
                  has_first = true;
                } else if (!matches_first(first_val, cv)) {
                  uniform_ok = false;
                  break;
                }
                continue;
              }
              for (size_t r = 0; r < chunk.rows; ++r) {
                Value rv;
                col.WriteValueInto(r, &rv);
                if (!has_first) {
                  if (!valid_const_type(rv)) {
                    uniform_ok = false;
                    break;
                  }
                  first_val = rv;
                  has_first = true;
                } else if (!matches_first(first_val, rv)) {
                  uniform_ok = false;
                  break;
                }
              }
              if (!uniform_ok) {
                break;
              }
            }
            if (uniform_ok && has_first) {
              raw_agg_kinds[i] = RawAggKind::kConstantExtremum;
              raw_constants[i] = first_val;
              continue;
            }
          }
        }
      }
      const TypeTag child_tag =
          agg->Child()->ResultType(result.schema).GetType();
      bool child_may_unsigned = false;
      bool child_has_mixed_double = false;
      for (const BytecodeInstruction& inst : child_programs[i].Instructions()) {
        if (inst.opcode == BytecodeOp::kLoadColumn &&
            inst.operand < result.schema.ColumnCount()) {
          if (result.schema.GetColumn(inst.operand).IsUnsigned()) {
            child_may_unsigned = true;
            break;
          }
          if (result.schema.GetColumn(inst.operand).Type() ==
              ValueType::kInt64) {
            child_has_mixed_double =
                child_has_mixed_double ||
                column_may_be_double(
                    inst.operand < left_width,
                    inst.operand < left_width
                        ? static_cast<size_t>(inst.operand)
                        : static_cast<size_t>(inst.operand - left_width));
          }
        }
      }
      if (child_has_mixed_double) {
        raw_sum_fusion = false;
        break;
      }
      for (const Value& cval : child_programs[i].Constants()) {
        if (!cval.IsNull() && cval.IsUnsigned()) {
          child_may_unsigned = true;
          break;
        }
      }
      if (agg->Distinct()) {
        if (agg->GetType() == AggregationType::kCount && !has_count_distinct &&
            (child_tag == TypeTag::kBigInt || child_tag == TypeTag::kInteger) &&
            !child_may_unsigned) {
          raw_agg_kinds[i] = RawAggKind::kCountDistinctInt64;
          has_count_distinct = true;
          continue;
        }
        raw_sum_fusion = false;
        break;
      }
      if (agg->GetType() == AggregationType::kCount) {
        raw_agg_kinds[i] = RawAggKind::kCountValue;
      } else if (agg->GetType() == AggregationType::kSum) {
        if (child_tag == TypeTag::kDouble) {
          raw_agg_kinds[i] = RawAggKind::kSumDouble;
        } else if ((child_tag == TypeTag::kBigInt ||
                    child_tag == TypeTag::kInteger) &&
                   !child_may_unsigned) {
          raw_agg_kinds[i] = RawAggKind::kSumInt64;
        } else {
          raw_sum_fusion = false;
          break;
        }
      } else if (agg->GetType() == AggregationType::kAvg ||
                 agg->GetType() == AggregationType::kStddevSamp ||
                 agg->GetType() == AggregationType::kStddevPop ||
                 agg->GetType() == AggregationType::kVarSamp ||
                 agg->GetType() == AggregationType::kVarPop) {
        if (child_tag == TypeTag::kDouble ||
            ((child_tag == TypeTag::kBigInt ||
              child_tag == TypeTag::kInteger) &&
             !child_may_unsigned)) {
          switch (agg->GetType()) {
            case AggregationType::kAvg:
              raw_agg_kinds[i] = RawAggKind::kAvgDouble;
              break;
            case AggregationType::kStddevSamp:
              raw_agg_kinds[i] = RawAggKind::kStddevSamp;
              break;
            case AggregationType::kStddevPop:
              raw_agg_kinds[i] = RawAggKind::kStddevPop;
              break;
            case AggregationType::kVarSamp:
              raw_agg_kinds[i] = RawAggKind::kVarSamp;
              break;
            case AggregationType::kVarPop:
              raw_agg_kinds[i] = RawAggKind::kVarPop;
              break;
            default:
              break;
          }
        } else {
          raw_sum_fusion = false;
          break;
        }
      } else {
        raw_sum_fusion = false;
        break;
      }
    }
  }
  // A parent rollup may reassociate only exact partials. The Cartesian join
  // bound covers duplicate build keys and LEFT padding without catalog PK
  // assumptions; immutable scan bounds cover every possible filtered subset.
  const auto exact_integer_total = [&](size_t aggregate) {
    constexpr uint64_t kExactLimit = uint64_t{1} << 53;
    const size_t build_bound = std::max<size_t>(1, right_build_count());
    if (effective_left_rows == 0 ||
        effective_left_rows > kExactLimit / build_bound ||
        child_shapes[aggregate].direct_column < 0) {
      return false;
    }
    const uint64_t pairs =
        static_cast<uint64_t>(effective_left_rows) * build_bound;
    const auto slot =
        static_cast<size_t>(child_shapes[aggregate].direct_column);
    if (result.schema.GetColumn(slot).Type() != ValueType::kInt64 ||
        result.schema.GetColumn(slot).IsUnsigned()) {
      return false;
    }
    uint64_t maximum = 0;
    const auto magnitude = [](int64_t value) {
      const auto bits = static_cast<uint64_t>(value);
      return value < 0 ? uint64_t{0} - bits : bits;
    };
    if (slot < left_width && deferred_left_probe != nullptr) {
      const auto& probe = *deferred_left_probe;
      if (!probe.store || slot >= probe.projection.size() ||
          std::ranges::any_of(probe.pk_lookups, [&](const auto& lookup) {
            return lookup.target_proj_col == slot;
          })) {
        return false;
      }
      const auto& store = *probe.store;
      const slot_t column = probe.projection[slot];
      if (!store.HasColumn(column) || store.blocks.empty() ||
          column >= store.column_min_int.size() ||
          column >= store.column_max_int.size() ||
          column >= store.blocks.front().columns.size() ||
          store.blocks.front().columns[column].type != ValueType::kInt64 ||
          store.blocks.front().columns[column].is_unsigned ||
          store.column_min_int[column] > store.column_max_int[column]) {
        return false;
      }
      maximum = std::max(magnitude(store.column_min_int[column]),
                         magnitude(store.column_max_int[column]));
    } else {
      const Relation& side = slot < left_width ? left : *right_ptr;
      const size_t column = slot < left_width ? slot : slot - left_width;
      if (side.HasSpill()) {
        return false;
      }
      if (side.HasColumnarOnly()) {
        for (const ColumnarChunk& ch : side.columnar->chunks) {
          if (column >= ch.columns.size()) {
            return false;
          }
          const ColumnarColumn& cc = ch.columns[column];
          if (cc.encoding == ColumnarColumn::Encoding::kConstant &&
              ch.rows > 0) {
            if (cc.IsNull(0)) {
              continue;
            }
            if (cc.is_uniform) {
              if (cc.type != ValueType::kInt64 || cc.is_unsigned) {
                return false;
              }
              maximum = std::max(maximum, magnitude(cc.IntAt(0)));
            } else {
              Value tmp;
              cc.WriteValueInto(0, &tmp);
              if (!tmp.IsNull()) {
                if (tmp.type != ValueType::kInt64 || tmp.IsUnsigned()) {
                  return false;
                }
                maximum = std::max(maximum, magnitude(tmp.value.int_value));
              }
            }
            continue;
          }
          for (size_t r = 0; r < ch.rows; ++r) {
            if (cc.IsNull(r)) {
              continue;
            }
            if (cc.is_uniform) {
              if (cc.type != ValueType::kInt64 || cc.is_unsigned) {
                return false;
              }
              maximum = std::max(maximum, magnitude(cc.IntAt(r)));
            } else {
              Value tmp;
              cc.WriteValueInto(r, &tmp);
              if (tmp.IsNull()) {
                continue;
              }
              if (tmp.type != ValueType::kInt64 || tmp.IsUnsigned()) {
                return false;
              }
              maximum = std::max(maximum, magnitude(tmp.value.int_value));
            }
          }
        }
      } else {
        for (const Row& row : side.rows) {
          const Value& value = row[column];
          if (value.IsNull()) {
            continue;
          }
          if (value.type != ValueType::kInt64 || value.IsUnsigned()) {
            return false;
          }
          maximum = std::max(maximum, magnitude(value.value.int_value));
        }
      }
    }
    // Division avoids overflow even for INT64_MIN and excessive cardinality.
    return maximum <= kExactLimit / pairs;
  };
  if (fusion->require_exact_integer_totals) {
    const size_t build_bound = std::max<size_t>(1, right_build_count());
    if (!raw_sum_fusion ||
        effective_left_rows > (uint64_t{1} << 53) / build_bound) {
      return not_fusable("unproved partial totals");
    }
    for (size_t a = 0; a < fusion->aggregates.size(); ++a) {
      if (raw_agg_kinds[a] == RawAggKind::kCountStar ||
          raw_agg_kinds[a] == RawAggKind::kCountValue) {
        continue;
      }
      if (raw_agg_kinds[a] != RawAggKind::kSumInt64 ||
          !exact_integer_total(a)) {
        return not_fusable("unproved partial integer sum");
      }
    }
  }
  std::array<int8_t, kRawAggregateCapacity> raw_agg_slot{};
  raw_agg_slot.fill(-1);
  std::array<uint8_t, kRawAggregateCapacity> raw_slot_origin{};
  std::array<RawAggKind, kRawAggregateCapacity> slot_agg_kinds{};
  std::array<bool, kRawAggregateCapacity> raw_const_folded{};
  size_t raw_slot_count = 0;
  if (raw_sum_fusion) {
    const bool allow_const_extremum_fold = !fusion->group_keys.empty() &&
                                           join_type == JoinType::kInner &&
                                           !fusion->rollup_count_distribution;
    const auto raw_family = [](RawAggKind kind) noexcept -> uint8_t {
      switch (kind) {
        case RawAggKind::kSumDouble:
        case RawAggKind::kAvgDouble:
          return 1;
        case RawAggKind::kStddevSamp:
        case RawAggKind::kStddevPop:
        case RawAggKind::kVarSamp:
        case RawAggKind::kVarPop:
          return 2;
        case RawAggKind::kSumInt64:
          return 3;
        case RawAggKind::kCountStar:
          return 4;
        case RawAggKind::kCountValue:
          return 5;
        case RawAggKind::kConstantExtremum:
          return 6;
        case RawAggKind::kCountDistinctInt64:
          return 7;
        case RawAggKind::kNone:
          return 0;
      }
      return 0;
    };
    const auto same_value_bits = [](const Value& a,
                                    const Value& b) noexcept -> bool {
      if (a.type != b.type || a.IsUnsigned() != b.IsUnsigned() ||
          a.IsInterval() != b.IsInterval()) {
        return false;
      }
      if (a.type == ValueType::kDouble) {
        return std::bit_cast<uint64_t>(a.value.double_value) ==
               std::bit_cast<uint64_t>(b.value.double_value);
      }
      return a == b;
    };
    const auto same_child_program = [&](size_t a, size_t b) -> bool {
      if (fusion->count_star[a] != fusion->count_star[b]) {
        return false;
      }
      if (fusion->count_star[a]) {
        return true;
      }
      const BytecodeProgram& pa = child_programs[a];
      const BytecodeProgram& pb = child_programs[b];
      if (!pa.Casts().empty() || !pb.Casts().empty()) {
        return false;
      }
      const auto& ia = pa.Instructions();
      const auto& ib = pb.Instructions();
      if (ia.size() != ib.size()) {
        return false;
      }
      for (size_t k = 0; k < ia.size(); ++k) {
        if (ia[k].opcode != ib[k].opcode || ia[k].operand != ib[k].operand ||
            ia[k].binary != ib[k].binary || ia[k].unary != ib[k].unary ||
            ia[k].jump_target != ib[k].jump_target) {
          return false;
        }
      }
      const auto& ca = pa.Constants();
      const auto& cb = pb.Constants();
      if (ca.size() != cb.size()) {
        return false;
      }
      for (size_t k = 0; k < ca.size(); ++k) {
        if (!same_value_bits(ca[k], cb[k])) {
          return false;
        }
      }
      return true;
    };
    for (size_t i = 0; i < fusion->aggregates.size(); ++i) {
      if (allow_const_extremum_fold &&
          raw_agg_kinds[i] == RawAggKind::kConstantExtremum &&
          !raw_constants[i].IsNull()) {
        raw_const_folded[i] = true;
        raw_agg_slot[i] = -1;
        continue;
      }
      int8_t matched_slot = -1;
      const uint8_t fam = raw_family(raw_agg_kinds[i]);
      if (fam != 0 && fam != 7) {
        for (size_t s = 0; s < raw_slot_count; ++s) {
          const size_t src = raw_slot_origin[s];
          if (raw_family(raw_agg_kinds[src]) == fam &&
              same_child_program(i, src)) {
            matched_slot = static_cast<int8_t>(s);
            break;
          }
        }
      }
      if (matched_slot >= 0) {
        raw_agg_slot[i] = matched_slot;
      } else {
        const size_t s = raw_slot_count++;
        raw_slot_origin[s] = static_cast<uint8_t>(i);
        slot_agg_kinds[s] = raw_agg_kinds[i];
        raw_agg_slot[i] = static_cast<int8_t>(s);
      }
    }
  }
  // SUM raises kIsInfinity on an infinite total but AVG returns it as a value
  // (aggregate_accumulator.cpp), so a slot consumed only by AVG(DOUBLE) must
  // not turn its sticky inf flag into a query error.  A slot shared with any
  // other aggregate kind keeps raising.
  std::array<bool, kRawAggregateCapacity> slot_raises_inf{};
  for (size_t i = 0; i < fusion->aggregates.size(); ++i) {
    if (raw_const_folded[i] || raw_agg_slot[i] < 0) {
      continue;
    }
    if (raw_agg_kinds[i] != RawAggKind::kAvgDouble) {
      slot_raises_inf[static_cast<size_t>(
          static_cast<unsigned char>(raw_agg_slot[i]))] = true;
    }
  }
  const bool skip_accumulator_states =
      raw_count_fusion || raw_sum_fusion || fusion->aggregates.empty();
  // For INNER joins over exact INT64/DATE equality keys, align join-key group
  // columns with the side that owns the remaining group keys so either
  // right-only pre-assigned build groups (Q3, Q18) or left-only group caching
  // applies.
  if (join_type == JoinType::kInner &&
      !HasMixedKeySignednessOrType(left, right, equality_keys)) {
    bool non_join_keys_all_right = !right.HasSpill();
    bool has_non_join_right_key = false;
    for (size_t k = 0; k < key_programs.size(); ++k) {
      bool is_join_eq_col = false;
      if (key_shapes[k].direct_column >= 0) {
        const auto c = static_cast<size_t>(key_shapes[k].direct_column);
        for (const EqualityKey& eq : equality_keys) {
          if (eq.left_offset == 0 && eq.right_offset == 0 && !eq.null_safe &&
              (c == eq.left || c == left_width + eq.right)) {
            const Column& lc = left.schema.GetColumn(eq.left);
            const Column& rc = right.schema.GetColumn(eq.right);
            if ((lc.Type() == ValueType::kInt64 ||
                 lc.Type() == ValueType::kDate) &&
                lc.Type() == rc.Type() && lc.IsUnsigned() == rc.IsUnsigned()) {
              is_join_eq_col = true;
              break;
            }
          }
        }
      }
      if (is_join_eq_col) {
        continue;
      }
      if (key_shapes[k].direct_column >= 0) {
        if (std::cmp_less(key_shapes[k].direct_column, left_width)) {
          non_join_keys_all_right = false;
        } else {
          has_non_join_right_key = true;
        }
      } else if (key_shapes[k].extract_date_col >= 0) {
        if (std::cmp_less(key_shapes[k].extract_date_col, left_width)) {
          non_join_keys_all_right = false;
        } else {
          has_non_join_right_key = true;
        }
      } else {
        std::vector<slot_t> loads;
        CollectLoadCells(key_programs[k], &loads);
        if (std::any_of(loads.begin(), loads.end(),
                        [&](slot_t c) { return c < left_width; })) {
          non_join_keys_all_right = false;
        } else if (!loads.empty()) {
          has_non_join_right_key = true;
        }
      }
    }
    const bool prefer_right_group_key =
        non_join_keys_all_right &&
        (has_non_join_right_key ||
         (skip_accumulator_states && residual_programs.empty() &&
          right.TotalRows() * 2 <= effective_left_rows));
    for (ProgramShape& kshape : key_shapes) {
      if (prefer_right_group_key) {
        if (kshape.direct_column >= 0 &&
            std::cmp_less(kshape.direct_column, left_width)) {
          const auto lcol = static_cast<size_t>(kshape.direct_column);
          for (const EqualityKey& eq : equality_keys) {
            if (eq.left == lcol && eq.left_offset == 0 &&
                eq.right_offset == 0 && !eq.null_safe) {
              const Column& lc = left.schema.GetColumn(eq.left);
              const Column& rc = right.schema.GetColumn(eq.right);
              if ((lc.Type() == ValueType::kInt64 ||
                   lc.Type() == ValueType::kDate) &&
                  lc.Type() == rc.Type() &&
                  lc.IsUnsigned() == rc.IsUnsigned()) {
                kshape.direct_column =
                    static_cast<int32_t>(left_width + eq.right);
                break;
              }
            }
          }
        }
      } else if (std::cmp_greater_equal(kshape.direct_column, left_width)) {
        const size_t rcol =
            static_cast<size_t>(kshape.direct_column) - left_width;
        for (const EqualityKey& eq : equality_keys) {
          if (eq.right == rcol && eq.left_offset == 0 && eq.right_offset == 0 &&
              !eq.null_safe) {
            const Column& lc = left.schema.GetColumn(eq.left);
            const Column& rc = right.schema.GetColumn(eq.right);
            if ((lc.Type() == ValueType::kInt64 ||
                 lc.Type() == ValueType::kDate) &&
                lc.Type() == rc.Type() && lc.IsUnsigned() == rc.IsUnsigned()) {
              kshape.direct_column = static_cast<int32_t>(eq.left);
              break;
            }
          }
        }
      }
    }
  }
  bool left_only_group_keys = !key_programs.empty();
  bool right_only_group_keys = !right.HasSpill();
  for (size_t k = 0; k < key_programs.size(); ++k) {
    if (key_shapes[k].direct_column >= 0) {
      if (std::cmp_greater_equal(key_shapes[k].direct_column, left_width)) {
        left_only_group_keys = false;
      } else {
        right_only_group_keys = false;
      }
    } else if (key_shapes[k].extract_date_col >= 0) {
      if (std::cmp_greater_equal(key_shapes[k].extract_date_col, left_width)) {
        left_only_group_keys = false;
      } else {
        right_only_group_keys = false;
      }
    } else {
      std::vector<slot_t> loads;
      CollectLoadCells(key_programs[k], &loads);
      if (std::any_of(loads.begin(), loads.end(),
                      [&](slot_t c) { return c >= left_width; })) {
        left_only_group_keys = false;
      }
      if (std::any_of(loads.begin(), loads.end(),
                      [&](slot_t c) { return c < left_width; })) {
        right_only_group_keys = false;
      }
    }
  }
  bool all_direct_group_keys = !left.HasSpill() && !key_programs.empty();
  for (size_t k = 0; k < key_programs.size(); ++k) {
    if (key_shapes[k].direct_column < 0) {
      all_direct_group_keys = false;
      break;
    }
  }
  std::vector<size_t> int_key_indices;
  bool has_unique_int_key = false;
  for (size_t k = 0; k < key_programs.size(); ++k) {
    if (key_shapes[k].direct_column >= 0) {
      const auto col_idx = static_cast<size_t>(key_shapes[k].direct_column);
      if (col_idx < result.schema.ColumnCount()) {
        const Column& col = result.schema.GetColumn(col_idx);
        if ((col.Type() == ValueType::kInt64 ||
             col.Type() == ValueType::kDate) &&
            !col.IsUnsigned()) {
          int_key_indices.push_back(k);
          if (col.GetConstraint().ctype == Constraint::kPrimaryKey) {
            has_unique_int_key = true;
          } else if (join_type == JoinType::kInner &&
                     !HasMixedKeySignednessOrType(left, right, equality_keys)) {
            for (const EqualityKey& eq : equality_keys) {
              if (eq.left_offset == 0 && eq.right_offset == 0 &&
                  !eq.null_safe) {
                if (col_idx == eq.left &&
                    (verified_unique_right_join_key ||
                     right.schema.GetColumn(eq.right).GetConstraint().ctype ==
                         Constraint::kPrimaryKey)) {
                  has_unique_int_key = true;
                  break;
                }
                if (col_idx == left_width + eq.right &&
                    (verified_unique_right_join_key ||
                     left.schema.GetColumn(eq.left).GetConstraint().ctype ==
                         Constraint::kPrimaryKey)) {
                  has_unique_int_key = true;
                  break;
                }
              }
            }
          }
        }
      }
    }
  }
  const bool hash_int_keys_only =
      !int_key_indices.empty() &&
      (int_key_indices.size() == key_programs.size() || has_unique_int_key);
  // Scratch-mask copy: the pair loop fills only the cells the residual,
  // non-direct key, and non-direct child programs actually load (group
  // representatives — which the final select list reads in full — are re-filled
  // completely on the rare new-group path).  Direct-column keys and children
  // read straight from left_row/right_row without touching scratch, avoiding
  // per-pair VARCHAR copies on group hits (Q10, Q9, Q3, Q5, Q7, Q18, Q21).
  std::vector<slot_t> scratch_cells;
  for (const BytecodeProgram& program : residual_programs) {
    CollectLoadCells(program, &scratch_cells);
  }
  for (size_t k = 0; k < key_programs.size(); ++k) {
    if (!key_shapes[k].HasPairDirect()) {
      CollectLoadCells(key_programs[k], &scratch_cells);
    }
  }
  for (size_t i = 0; i < child_programs.size(); ++i) {
    if (!fusion->count_star[i] && !child_shapes[i].HasPairDirect()) {
      CollectLoadCells(child_programs[i], &scratch_cells);
    }
  }
  std::sort(scratch_cells.begin(), scratch_cells.end());
  scratch_cells.erase(std::unique(scratch_cells.begin(), scratch_cells.end()),
                      scratch_cells.end());
  const bool mask_scratch = scratch_cells.size() < left_width + right_width;

  std::vector<slot_t> left_columns;
  std::vector<slot_t> right_columns;
  left_columns.reserve(equality_keys.size());
  right_columns.reserve(equality_keys.size());
  for (const EqualityKey& key : equality_keys) {
    left_columns.push_back(static_cast<slot_t>(key.left));
    right_columns.push_back(static_cast<slot_t>(key.right));
  }
  const bool integer_key = SingleIntegerJoinKey(left.schema, left_columns) &&
                           SingleIntegerJoinKey(right.schema, right_columns);
  const bool composite_int_key =
      !integer_key && CompositeIntJoinKey(left.schema, left_columns,
                                          right.schema, right_columns);
  if (debug_fusion) {
    std::cerr << "[fusion] join_keys=" << equality_keys.size()
              << " integer=" << integer_key
              << " composite=" << composite_int_key
              << " right_columnar=" << right_col_only << "\n";
    for (const auto& eq : equality_keys) {
      std::cerr << "[fusion] equality=" << left.schema.GetColumn(eq.left).Name()
                << " -> " << right.schema.GetColumn(eq.right).Name() << "\n";
}
  }
  // Biased keys hash with their constants folded in; the fused residual
  // programs still verify every matched pair against the original
  // predicates, so the bias only prunes buckets.
  std::vector<int64_t> left_key_offsets;
  std::vector<int64_t> right_key_offsets;
  left_key_offsets.reserve(equality_keys.size());
  right_key_offsets.reserve(equality_keys.size());
  for (const EqualityKey& key : equality_keys) {
    left_key_offsets.push_back(key.left_offset);
    right_key_offsets.push_back(key.right_offset);
  }
  constexpr auto kInvalidFusedPacked2x32 = static_cast<int64_t>(
      (static_cast<uint64_t>(0x80000000ULL) << 32) | 0x80000000ULL);
  const auto fits_fused_i32 = [](int64_t v) noexcept {
    return v >= static_cast<int64_t>(std::numeric_limits<int32_t>::min()) &&
           v <= static_cast<int64_t>(std::numeric_limits<int32_t>::max());
  };
  const auto pack_fused_2x32 = [](int64_t k0, int64_t k1) noexcept -> int64_t {
    return static_cast<int64_t>(
        (static_cast<uint64_t>(static_cast<uint32_t>(static_cast<int32_t>(k0)))
         << 32) |
        static_cast<uint64_t>(static_cast<uint32_t>(static_cast<int32_t>(k1))));
  };
  std::vector<int64_t> validated_packed_keys;
  std::shared_ptr<QueryMemoryCharge> validated_packed_keys_charge;
  if (right_col_only) {
    auto program_loads_right = [&](const BytecodeProgram& prog) {
      std::vector<slot_t> cells;
      CollectLoadCells(prog, &cells);
      return std::ranges::any_of(cells,
                                 [&](slot_t c) { return c >= left_width; });
    };
    bool need_right_rows = left.HasSpill() || total_right_rows == 0;
    if (!need_right_rows) {
      for (const BytecodeProgram& rp : residual_programs) {
        if (program_loads_right(rp)) {
          need_right_rows = true;
          break;
        }
      }
    }
    if (!need_right_rows) {
      for (const BytecodeProgram& cp : child_programs) {
        if (program_loads_right(cp)) {
          need_right_rows = true;
          break;
        }
      }
    }
    if (!need_right_rows && !all_direct_group_keys) {
      for (size_t k = 0; k < key_programs.size(); ++k) {
        if (key_shapes[k].direct_column < 0 &&
            program_loads_right(key_programs[k])) {
          need_right_rows = true;
          break;
        }
      }
    }
    if (!need_right_rows && !integer_key) {
      if (!composite_int_key) {
        need_right_rows = true;
      } else if (right_columns.size() == 2) {
        const slot_t rc0 = right_columns[0];
        const slot_t rc1 = right_columns[1];
        const int64_t roff0 = right_key_offsets[0];
        const int64_t roff1 = right_key_offsets[1];
        bool can_pack = true;
        const size_t count = right_build_count();
        if (count > 0 && count <= 4 * 1024 * 1024) {
          validated_packed_keys_charge =
              TryReserveQueryMemory(count * sizeof(int64_t));
          if (validated_packed_keys_charge) {
            validated_packed_keys.assign(count, kInvalidFusedPacked2x32);
}
        }
        size_t base_row = 0;
        for (const ColumnarChunk& rch : right_arg.columnar->chunks) {
          const ColumnarColumn& c0 = rch.columns[rc0];
          const ColumnarColumn& c1 = rch.columns[rc1];
          for (size_t r = 0; r < rch.rows; ++r) {
            int64_t v0 = 0;
            int64_t v1 = 0;
            if (!c0.TryReadNonNullInt(r, &v0) ||
                !c1.TryReadNonNullInt(r, &v1)) {
              continue;
            }
            const int64_t k0 = AddJoinKeyBias(v0, roff0);
            const int64_t k1 = AddJoinKeyBias(v1, roff1);
            if (!fits_fused_i32(k0) || !fits_fused_i32(k1) ||
                pack_fused_2x32(k0, k1) == kInvalidFusedPacked2x32) {
              can_pack = false;
              break;
            }
            if (!validated_packed_keys.empty()) {
              validated_packed_keys[base_row + r] = pack_fused_2x32(k0, k1);
}
          }
          if (!can_pack) {
            break;
          }
          base_row += rch.rows;
        }
        if (!can_pack) {
          std::vector<int64_t>().swap(validated_packed_keys);
          validated_packed_keys_charge.reset();
          need_right_rows = true;
        }
      }
    }
    if (need_right_rows) {
      RETURN_IF_FAIL(fallback_to_right_rows().GetStatus());
    }
  }

  const size_t total_left = effective_left_rows;
  // Single-INT64/DATE group-key fast path: when the fusion groups by one
  // plain column of that domain, the pair loop probes a flat int table with
  // the raw cell payload (NULL keys keep the generic map, which groups
  // NULLs).  Q13's customer-key grouping pays the generic vector hash once
  // per matched pair otherwise.
  bool int_group_key = false;
  slot_t int_group_slot = 0;
  ValueType int_group_type = ValueType::kNull;
  if (key_programs.size() == 1 && key_shapes[0].direct_column >= 0) {
    const Column& column = result.schema.GetColumn(
        static_cast<size_t>(key_shapes[0].direct_column));
    const ValueType type = column.Type();
    if ((type == ValueType::kInt64 && !column.IsUnsigned()) ||
        type == ValueType::kDate) {
      int_group_key = true;
      int_group_slot = static_cast<slot_t>(key_shapes[0].direct_column);
      int_group_type = type;
    }
  }
  // Classification shared by every per-group accumulator construction below;
  // identical for all groups of one aggregate.
  std::vector<AggregateAccumulator::Shape> aggregate_shapes;
  aggregate_shapes.reserve(fusion->aggregates.size());
  for (const AggregateExpression* aggregate : fusion->aggregates) {
    aggregate_shapes.push_back(
        AggregateAccumulator::AnalyzeShape(aggregate, nullptr));
  }
  // Bulk raw-count: once every residual is hoisted off the pair path and the
  // group key is one probe-side INT64/DATE column, a count-only fusion needs
  // no per-pair work at all -- each surviving bucket entry contributes
  // exactly +1 (COUNT(*) and COUNT(not-null column) coincide inside a
  // bucket) -- so the probe collapses to one equal_range, one group-map
  // probe and one integer add per outer row.  The per-pair scratch fills,
  // flat-map probes and build-row dereferences all disappear (Q13's customer
  // LEFT JOIN orders COUNT(o_orderkey) walked ~915K pairs at ~330 ns each).
  static const bool bulk_count_disabled =
      std::getenv("TINYLAMB_DISABLE_BULK_COUNT") != nullptr;
  const bool anti_unmatched_only =
      join_type == JoinType::kLeft && fusion->anti_unmatched_only;
  const bool bulk_raw_count = !singleton_right_relation && !left.HasSpill() &&
                              !bulk_count_disabled && !anti_unmatched_only &&
                              raw_count_fusion &&
                              residual_programs.empty() && int_group_key &&
                              int_group_slot < left_width && integer_key;
  static const Value kNullCell{};
  const auto right_row_ptr = [&](uint32_t r_idx) noexcept -> const Row* {
    return (!right_col_only && r_idx != kInvalidRightIdx)
               ? &right_ptr->rows[r_idx]
               : nullptr;
  };
  const auto read_right_cell = [&](uint32_t r_idx, size_t rcol,
                                   Value* scratch) -> const Value& {
    if (r_idx == kInvalidRightIdx) {
      return kNullCell;
    }
    if (right_col_only) {
      right_locator.WriteValue(rcol, r_idx, scratch);
      return *scratch;
    }
    return right_ptr->rows[r_idx].values_[rcol];
  };
  const auto right_cell_is_null = [&](uint32_t r_idx,
                                      size_t rcol) noexcept -> bool {
    if (r_idx == kInvalidRightIdx) {
      return true;
    }
    if (right_col_only) {
      return right_locator.IsNull(rcol, r_idx);
    }
    return right_ptr->rows[r_idx].values_[rcol].IsNull();
  };
  const auto get_pair_cell = [&](const Row* left_row, const Row* right_row,
                                 slot_t col) -> const Value& {
    if (col < left_width) {
      return left_row != nullptr ? left_row->values_[col] : kNullCell;
    }
    return right_row != nullptr ? right_row->values_[col - left_width]
                                : kNullCell;
  };
  auto cell_as_double = [](const Value& v, double* out) noexcept -> bool {
    if (v.type == ValueType::kDouble) {
      *out = v.value.double_value;
      return true;
    }
    if (v.type == ValueType::kInt64 && !v.IsUnsigned()) {
      *out = static_cast<double>(v.value.int_value);
      return true;
    }
    return false;
  };
  const auto eval_case_cond = [&](const ProgramShape& shape,
                                  const Row* left_row, const Row* right_row,
                                  bool* cond_true) noexcept -> bool {
    if (shape.case_cond == ProgramShape::CaseCondKind::kIntComparisons) {
      bool matches = true;
      for (const auto& test : shape.case_int_comparisons) {
        const auto& a =
            get_pair_cell(left_row, right_row, static_cast<slot_t>(test.left));
        const auto& b = test.subtract >= 0
                            ? get_pair_cell(left_row, right_row,
                                            static_cast<slot_t>(test.subtract))
                            : kNullCell;
        if (a.IsNull() || (test.subtract >= 0 && b.IsNull())) {
          matches = false;
          continue;
        }
        if (a.type != ValueType::kInt64 || a.IsUnsigned() ||
            (test.subtract >= 0 &&
             (b.type != ValueType::kInt64 || b.IsUnsigned()))) {
          return false;
        }
        int64_t value = a.value.int_value;
        if (test.subtract >= 0 &&
            __builtin_sub_overflow(value, b.value.int_value, &value)) {
          return false;
        }
        switch (test.op) {
          case BinaryOperation::kEquals:
            matches &= value == test.constant;
            break;
          case BinaryOperation::kLessThan:
            matches &= value < test.constant;
            break;
          case BinaryOperation::kLessThanEquals:
            matches &= value <= test.constant;
            break;
          case BinaryOperation::kGreaterThan:
            matches &= value > test.constant;
            break;
          case BinaryOperation::kGreaterThanEquals:
            matches &= value >= test.constant;
            break;
          default:
            return false;
        }
      }
      *cond_true = matches;
      return true;
    }
    const Value& cc = get_pair_cell(left_row, right_row,
                                    static_cast<slot_t>(shape.case_cond_col));
    if (cc.IsNull()) {
      *cond_true = false;
      return true;
    }
    if (shape.case_cond == ProgramShape::CaseCondKind::kColEqInt) {
      if (cc.type == ValueType::kInt64 && !cc.IsUnsigned()) [[likely]] {
        *cond_true = (cc.value.int_value == shape.case_cond_int);
        return true;
      }
      return false;
    }
    if (cc.type == ValueType::kVarChar && cc.Collation() == 0 &&
        !cc.IsCaseInsensitive()) [[likely]] {
      const std::string_view sv = cc.value.varchar_value;
      switch (shape.case_cond) {
        case ProgramShape::CaseCondKind::kColEqStr:
          *cond_true = (sv == shape.case_cond_str0);
          return true;
        case ProgramShape::CaseCondKind::kColEq2StrOr:
          *cond_true =
              (sv == shape.case_cond_str0 || sv == shape.case_cond_str1);
          return true;
        case ProgramShape::CaseCondKind::kColNe2StrAnd:
          *cond_true =
              (sv != shape.case_cond_str0 && sv != shape.case_cond_str1);
          return true;
        case ProgramShape::CaseCondKind::kColLikePrefix:
          *cond_true = (sv.size() >= shape.case_cond_str0.size() &&
                        std::memcmp(sv.data(), shape.case_cond_str0.data(),
                                    shape.case_cond_str0.size()) == 0);
          return true;
        case ProgramShape::CaseCondKind::kNone:
        case ProgramShape::CaseCondKind::kColEqInt:
        case ProgramShape::CaseCondKind::kIntComparisons:
          break;
      }
    }
    return false;
  };
  std::shared_ptr<QueryMemoryCharge> right_case_charge;
  std::array<std::vector<uint8_t>, kRawAggregateCapacity> right_case_conditions;
  std::array<bool, kRawAggregateCapacity> right_case_complete{};
  if (!right_col_only && raw_sum_fusion && effective_left_rows >= 1024 &&
      effective_left_rows >= right_ptr->rows.size() &&
      !right_ptr->rows.empty() &&
      right_ptr->rows.size() <= SIZE_MAX / fusion->aggregates.size() &&
      std::ranges::any_of(child_shapes, [](const auto& shape) {
        return shape.case_cond != ProgramShape::CaseCondKind::kNone;
      })) {
    right_case_charge = TryReserveQueryMemory(right_ptr->rows.size() *
                                              fusion->aggregates.size());
    if (right_case_charge) {
      for (size_t i = 0; i < child_shapes.size(); ++i) {
        const auto& shape = child_shapes[i];
        bool right_only =
            shape.case_cond != ProgramShape::CaseCondKind::kNone &&
            shape.case_cond_col >= 0 &&
            std::cmp_greater_equal(shape.case_cond_col, left_width);
        if (shape.case_cond == ProgramShape::CaseCondKind::kIntComparisons) {
          right_only = std::ranges::all_of(
              shape.case_int_comparisons, [&](const auto& test) {
                return std::cmp_greater_equal(test.left, left_width) &&
                       (test.subtract < 0 ||
                        std::cmp_greater_equal(test.subtract, left_width));
              });
        }
        if (!right_only) {
          continue;
        }
        auto& conditions = right_case_conditions[i];
        conditions.reserve(right_ptr->rows.size());
        bool all_known = true;
        for (const auto& row : right_ptr->rows) {
          bool value = false;
          const uint8_t cond = eval_case_cond(shape, nullptr, &row, &value)
                                   ? static_cast<uint8_t>(value)
                                   : uint8_t{2};
          all_known &= (cond < 2);
          conditions.push_back(cond);
        }
        right_case_complete[i] = all_known;
      }
    }
  }
  const auto cached_case_cond = [&](size_t aggregate, const Row* left_row,
                                    const Row* right_row, bool* value) {
    const auto& conditions = right_case_conditions[aggregate];
    if (!right_col_only && right_row != nullptr && !conditions.empty()) {
      const auto index =
          static_cast<size_t>(right_row - right_ptr->rows.data());
      if (conditions[index] < 2) {
        *value = conditions[index] != 0;
        return true;
      }
    }
    return eval_case_cond(child_shapes[aggregate], left_row, right_row, value);
  };
  const auto case_always_null = [&](size_t aggregate, const Row* right_row) {
    const auto& conditions = right_case_conditions[aggregate];
    return !right_col_only && child_shapes[aggregate].case_else_null &&
           right_row != nullptr && !conditions.empty() &&
           conditions[static_cast<size_t>(right_row -
                                          right_ptr->rows.data())] == 0;
  };
  static const auto kFastYearLut = [] {
    std::array<int16_t, 25568> lut{};
    for (int d = 0; d < 25568; ++d) {
      const std::chrono::year_month_day ymd{
          std::chrono::sys_days{std::chrono::days{d}}};
      lut[static_cast<size_t>(d)] =
          static_cast<int16_t>(static_cast<int>(ymd.year()));
    }
    return lut;
  }();
  const auto eval_pair_program = [&](const BytecodeProgram& program,
                                     const ProgramShape& shape, Row& scratch,
                                     const Row* left_row,
                                     const Row* right_row) -> StatusOr<Value> {
    if (shape.direct_column >= 0) {
      return get_pair_cell(left_row, right_row,
                           static_cast<slot_t>(shape.direct_column));
    }
    if (shape.extract_date_col >= 0) {
      const Value& c = get_pair_cell(
          left_row, right_row, static_cast<slot_t>(shape.extract_date_col));
      if (c.IsNull()) {
        return Value();
      }
      if (c.type == ValueType::kDate && c.value.int_value >= kDateMinDays &&
          c.value.int_value <= kDateMaxDays) [[likely]] {
        if (shape.extract_date_part == 0 && c.value.int_value >= 0 &&
            std::cmp_less(c.value.int_value, kFastYearLut.size())) [[likely]] {
          return Value(static_cast<int64_t>(
              kFastYearLut[static_cast<size_t>(c.value.int_value)]));
        }
        const std::chrono::year_month_day ymd{std::chrono::sys_days{
            std::chrono::days{static_cast<int>(c.value.int_value)}}};
        if (shape.extract_date_part == 0) {
          const int64_t year = static_cast<int>(ymd.year());
          return Value(year);
        }
        if (shape.extract_date_part == 1) {
          return Value(
              static_cast<int64_t>(static_cast<unsigned>(ymd.month())));
        }
        return Value(static_cast<int64_t>(static_cast<unsigned>(ymd.day())));
      }
      scratch.values_[static_cast<size_t>(shape.extract_date_col)] = c;
      return EvaluateProgramValue(program, shape, scratch);
    }
    if (shape.mul_col0 >= 0) {
      const Value& c0 = get_pair_cell(left_row, right_row,
                                      static_cast<slot_t>(shape.mul_col0));
      const Value& c1 = get_pair_cell(left_row, right_row,
                                      static_cast<slot_t>(shape.mul_col1));
      if (c0.IsNull() || c1.IsNull()) {
        return Value();
      }
      double d0 = 0.0, d1 = 0.0;
      if (cell_as_double(c0, &d0) && cell_as_double(c1, &d1)) [[likely]] {
        return Value(d0 * d1);
      }
      scratch.values_[static_cast<size_t>(shape.mul_col0)] = c0;
      scratch.values_[static_cast<size_t>(shape.mul_col1)] = c1;
      return EvaluateProgramValue(program, shape, scratch);
    }
    if (shape.mul_one_minus_col0 >= 0) {
      const Value& c0 = get_pair_cell(
          left_row, right_row, static_cast<slot_t>(shape.mul_one_minus_col0));
      const Value& c1 = get_pair_cell(
          left_row, right_row, static_cast<slot_t>(shape.mul_one_minus_col1));
      if (c0.IsNull() || c1.IsNull()) {
        return Value();
      }
      double d0 = 0.0, d1 = 0.0;
      if (cell_as_double(c0, &d0) && cell_as_double(c1, &d1)) [[likely]] {
        return Value(d0 * (shape.mul_one_minus_const - d1));
      }
      scratch.values_[static_cast<size_t>(shape.mul_one_minus_col0)] = c0;
      scratch.values_[static_cast<size_t>(shape.mul_one_minus_col1)] = c1;
      return EvaluateProgramValue(program, shape, scratch);
    }
    if (shape.profit_col0 >= 0) {
      const Value& c0 = get_pair_cell(left_row, right_row,
                                      static_cast<slot_t>(shape.profit_col0));
      const Value& c1 = get_pair_cell(left_row, right_row,
                                      static_cast<slot_t>(shape.profit_col1));
      const Value& c2 = get_pair_cell(left_row, right_row,
                                      static_cast<slot_t>(shape.profit_col2));
      const Value& c3 = get_pair_cell(left_row, right_row,
                                      static_cast<slot_t>(shape.profit_col3));
      if (c0.IsNull() || c1.IsNull() || c2.IsNull() || c3.IsNull()) {
        return Value();
      }
      double d0 = 0.0, d1 = 0.0, d2 = 0.0, d3 = 0.0;
      if (cell_as_double(c0, &d0) && cell_as_double(c1, &d1) &&
          cell_as_double(c2, &d2) && cell_as_double(c3, &d3)) [[likely]] {
        return Value((d0 * (shape.profit_const - d1)) - (d2 * d3));
      }
      scratch.values_[static_cast<size_t>(shape.profit_col0)] = c0;
      scratch.values_[static_cast<size_t>(shape.profit_col1)] = c1;
      scratch.values_[static_cast<size_t>(shape.profit_col2)] = c2;
      scratch.values_[static_cast<size_t>(shape.profit_col3)] = c3;
      return EvaluateProgramValue(program, shape, scratch);
    }
    if (shape.case_cond != ProgramShape::CaseCondKind::kNone &&
        shape.case_val != ProgramShape::CaseValKind::kNone) {
      bool cond_true = false;
      if (eval_case_cond(shape, left_row, right_row, &cond_true)) [[likely]] {
        if (!cond_true) {
          if (shape.case_else_null) {
            return Value();
          }
          return shape.case_val == ProgramShape::CaseValKind::kIntConst1
                     ? Value(int64_t{0})
                     : Value(0.0);
        }
        if (shape.case_val == ProgramShape::CaseValKind::kIntConst1) {
          return Value(int64_t{1});
        }
        if (shape.case_val == ProgramShape::CaseValKind::kDirectDoubleCol) {
          const Value& v0 = get_pair_cell(
              left_row, right_row, static_cast<slot_t>(shape.case_val_col0));
          if (v0.IsNull()) {
            return Value();
          }
          double d0 = 0.0;
          if (cell_as_double(v0, &d0)) [[likely]] {
            return Value(d0);
          }
        } else if (shape.case_val == ProgramShape::CaseValKind::kMulOneMinus) {
          const Value& v0 = get_pair_cell(
              left_row, right_row, static_cast<slot_t>(shape.case_val_col0));
          const Value& v1 = get_pair_cell(
              left_row, right_row, static_cast<slot_t>(shape.case_val_col1));
          if (v0.IsNull() || v1.IsNull()) {
            return Value();
          }
          double d0 = 0.0, d1 = 0.0;
          if (cell_as_double(v0, &d0) && cell_as_double(v1, &d1)) [[likely]] {
            return Value(d0 * (shape.case_val_const - d1));
          }
        }
      }
      if (shape.case_cond_col >= 0) {
        scratch.values_[static_cast<size_t>(shape.case_cond_col)] =
            get_pair_cell(left_row, right_row,
                          static_cast<slot_t>(shape.case_cond_col));
      }
      for (const auto& test : shape.case_int_comparisons) {
        scratch.values_[static_cast<size_t>(test.left)] =
            get_pair_cell(left_row, right_row, static_cast<slot_t>(test.left));
        if (test.subtract >= 0) {
          scratch.values_[static_cast<size_t>(test.subtract)] = get_pair_cell(
              left_row, right_row, static_cast<slot_t>(test.subtract));
        }
      }
      if (shape.case_val_col0 >= 0) {
        scratch.values_[static_cast<size_t>(shape.case_val_col0)] =
            get_pair_cell(left_row, right_row,
                          static_cast<slot_t>(shape.case_val_col0));
      }
      if (shape.case_val_col1 >= 0) {
        scratch.values_[static_cast<size_t>(shape.case_val_col1)] =
            get_pair_cell(left_row, right_row,
                          static_cast<slot_t>(shape.case_val_col1));
      }
      return EvaluateProgramValue(program, shape, scratch);
    }
    return EvaluateProgramValue(program, shape, scratch);
  };
  std::vector<std::vector<double>> right_profit_doubles(child_shapes.size());
  if (!right_col_only && raw_sum_fusion && !right_ptr->HasSpill() &&
      !right_ptr->rows.empty() && right_ptr->rows.size() <= 262144) {
    for (size_t i = 0; i < child_shapes.size(); ++i) {
      const ProgramShape& sh = child_shapes[i];
      if (sh.profit_col0 < 0 ||
          std::cmp_greater_equal(sh.profit_col0, left_width) ||
          std::cmp_greater_equal(sh.profit_col1, left_width)) {
        continue;
      }
      const bool col2_right =
          std::cmp_greater_equal(sh.profit_col2, left_width) &&
          std::cmp_less(sh.profit_col3, left_width);
      const bool col3_right =
          std::cmp_greater_equal(sh.profit_col3, left_width) &&
          std::cmp_less(sh.profit_col2, left_width);
      if (!col2_right && !col3_right) {
        continue;
      }
      const auto rcol =
          static_cast<size_t>((col2_right ? sh.profit_col2 : sh.profit_col3) -
                              static_cast<int32_t>(left_width));
      if (rcol >= right.schema.ColumnCount()) {
        continue;
      }
      std::vector<double> vals(right_ptr->rows.size());
      bool all_ok = true;
      for (size_t ri = 0; ri < right_ptr->rows.size(); ++ri) {
        const Value& rv = right_ptr->rows[ri].values_[rcol];
        double dv = 0.0;
        if (rv.IsNull() || !cell_as_double(rv, &dv)) {
          all_ok = false;
          break;
        }
        vals[ri] = dv;
      }
      if (all_ok) {
        right_profit_doubles[i] = std::move(vals);
      }
    }
  }
  // Straight DOUBLE arithmetic needs neither a Value stack nor a joined
  // scratch row. Keep the exact expression order and NULL flags, and leave
  // integer subexpressions, functions and control flow to the canonical VM.
  std::vector<bool> double_arithmetic(child_programs.size(), false);
  for (size_t i = 0; i < child_programs.size(); ++i) {
    const auto& program = child_programs[i];
    size_t depth = 0;
    bool eligible = program.ResultType() == ValueType::kDouble;
    for (const auto& instruction : program.Instructions()) {
      if (instruction.opcode == BytecodeOp::kLoadColumn) {
        eligible = eligible &&
                   instruction.operand < result.schema.ColumnCount() &&
                   result.schema.GetColumn(instruction.operand).Type() ==
                       ValueType::kDouble;
        ++depth;
      } else if (instruction.opcode == BytecodeOp::kLoadConstant) {
        const auto& value = program.Constants()[instruction.operand];
        eligible = eligible && !value.IsNull() && !value.IsUnsigned() &&
                   (value.type == ValueType::kDouble ||
                    value.type == ValueType::kInt64);
        ++depth;
      } else if (instruction.opcode == BytecodeOp::kBinaryDouble &&
                 depth >= 2 &&
                 (instruction.binary == BinaryOperation::kAdd ||
                  instruction.binary == BinaryOperation::kSubtract ||
                  instruction.binary == BinaryOperation::kMultiply ||
                  instruction.binary == BinaryOperation::kDivide)) {
        --depth;
      } else {
        eligible = false;
      }
      if (!eligible || depth > 16) {
        eligible = false;
        break;
      }
    }
    double_arithmetic[i] = eligible && depth == 1;
  }
  const auto eval_pair_double = [&](size_t agg_idx, Row& scratch,
                                    const Row* left_row, const Row* right_row,
                                    double* out, bool* is_null) -> Status {
    const ProgramShape& shape = child_shapes[agg_idx];
    if (shape.direct_column >= 0) {
      const Value& c = get_pair_cell(left_row, right_row,
                                     static_cast<slot_t>(shape.direct_column));
      if (c.IsNull()) {
        *is_null = true;
        return Status::kSuccess;
      }
      if (cell_as_double(c, out)) [[likely]] {
        *is_null = false;
        return Status::kSuccess;
      }
    } else if (shape.mul_col0 >= 0) {
      const Value& c0 = get_pair_cell(left_row, right_row,
                                      static_cast<slot_t>(shape.mul_col0));
      const Value& c1 = get_pair_cell(left_row, right_row,
                                      static_cast<slot_t>(shape.mul_col1));
      if (c0.IsNull() || c1.IsNull()) {
        *is_null = true;
        return Status::kSuccess;
      }
      double d0 = 0.0, d1 = 0.0;
      if (cell_as_double(c0, &d0) && cell_as_double(c1, &d1)) [[likely]] {
        *out = d0 * d1;
        *is_null = false;
        return Status::kSuccess;
      }
    } else if (shape.mul_one_minus_col0 >= 0) {
      const Value& c0 = get_pair_cell(
          left_row, right_row, static_cast<slot_t>(shape.mul_one_minus_col0));
      const Value& c1 = get_pair_cell(
          left_row, right_row, static_cast<slot_t>(shape.mul_one_minus_col1));
      if (c0.IsNull() || c1.IsNull()) {
        *is_null = true;
        return Status::kSuccess;
      }
      double d0 = 0.0, d1 = 0.0;
      if (cell_as_double(c0, &d0) && cell_as_double(c1, &d1)) [[likely]] {
        *out = d0 * (shape.mul_one_minus_const - d1);
        *is_null = false;
        return Status::kSuccess;
      }
    } else if (shape.profit_col0 >= 0) {
      if (!right_col_only && !right_profit_doubles[agg_idx].empty() &&
          left_row != nullptr && right_row != nullptr) [[likely]] {
        const auto r_idx =
            static_cast<size_t>(right_row - right_ptr->rows.data());
        const Value& c0 =
            left_row->values_[static_cast<size_t>(shape.profit_col0)];
        const Value& c1 =
            left_row->values_[static_cast<size_t>(shape.profit_col1)];
        const bool col2_right =
            std::cmp_greater_equal(shape.profit_col2, left_width);
        const Value& left_factor = left_row->values_[static_cast<size_t>(
            col2_right ? shape.profit_col3 : shape.profit_col2)];
        if (c0.IsNull() || c1.IsNull() || left_factor.IsNull()) [[unlikely]] {
          *is_null = true;
          return Status::kSuccess;
        }
        double d0 = 0.0, d1 = 0.0, left_number = 0.0;
        if (cell_as_double(c0, &d0) && cell_as_double(c1, &d1) &&
            cell_as_double(left_factor, &left_number)) [[likely]] {
          const double dr = right_profit_doubles[agg_idx][r_idx];
          *out = (d0 * (shape.profit_const - d1)) -
                 (col2_right ? (dr * left_number) : (left_number * dr));
          *is_null = false;
          return Status::kSuccess;
        }
      }
      const Value& c0 = get_pair_cell(left_row, right_row,
                                      static_cast<slot_t>(shape.profit_col0));
      const Value& c1 = get_pair_cell(left_row, right_row,
                                      static_cast<slot_t>(shape.profit_col1));
      const Value& c2 = get_pair_cell(left_row, right_row,
                                      static_cast<slot_t>(shape.profit_col2));
      const Value& c3 = get_pair_cell(left_row, right_row,
                                      static_cast<slot_t>(shape.profit_col3));
      if (c0.IsNull() || c1.IsNull() || c2.IsNull() || c3.IsNull()) {
        *is_null = true;
        return Status::kSuccess;
      }
      double d0 = 0.0, d1 = 0.0, d2 = 0.0, d3 = 0.0;
      if (cell_as_double(c0, &d0) && cell_as_double(c1, &d1) &&
          cell_as_double(c2, &d2) && cell_as_double(c3, &d3)) [[likely]] {
        *out = (d0 * (shape.profit_const - d1)) - (d2 * d3);
        *is_null = false;
        return Status::kSuccess;
      }
    } else if (shape.case_cond != ProgramShape::CaseCondKind::kNone &&
               shape.case_val != ProgramShape::CaseValKind::kNone) {
      bool cond_true = false;
      if (cached_case_cond(agg_idx, left_row, right_row, &cond_true))
          [[likely]] {
        if (!cond_true) {
          *out = 0.0;
          *is_null = shape.case_else_null;
          return Status::kSuccess;
        }
        if (shape.case_val == ProgramShape::CaseValKind::kDirectDoubleCol) {
          const Value& v0 = get_pair_cell(
              left_row, right_row, static_cast<slot_t>(shape.case_val_col0));
          if (v0.IsNull()) {
            *is_null = true;
            return Status::kSuccess;
          }
          if (cell_as_double(v0, out)) [[likely]] {
            *is_null = false;
            return Status::kSuccess;
          }
        } else if (shape.case_val == ProgramShape::CaseValKind::kMulOneMinus) {
          const Value& v0 = get_pair_cell(
              left_row, right_row, static_cast<slot_t>(shape.case_val_col0));
          const Value& v1 = get_pair_cell(
              left_row, right_row, static_cast<slot_t>(shape.case_val_col1));
          if (v0.IsNull() || v1.IsNull()) {
            *is_null = true;
            return Status::kSuccess;
          }
          double d0 = 0.0, d1 = 0.0;
          if (cell_as_double(v0, &d0) && cell_as_double(v1, &d1)) [[likely]] {
            *out = d0 * (shape.case_val_const - d1);
            *is_null = false;
            return Status::kSuccess;
          }
        }
      }
    }
    if (double_arithmetic[agg_idx]) {
      std::array<double, 16> values{};
      std::array<bool, 16> nulls{};
      size_t depth = 0;
      bool supported = true;
      const auto& program = child_programs[agg_idx];
      for (const auto& instruction : program.Instructions()) {
        if (instruction.opcode == BytecodeOp::kLoadColumn) {
          const auto& value =
              get_pair_cell(left_row, right_row, instruction.operand);
          nulls[depth] = value.IsNull();
          if (!nulls[depth]) {
            if (value.type != ValueType::kDouble) {
              supported = false;
              break;
            }
            values[depth] = value.value.double_value;
          }
          ++depth;
        } else if (instruction.opcode == BytecodeOp::kLoadConstant) {
          const auto& value = program.Constants()[instruction.operand];
          values[depth] = value.type == ValueType::kDouble
                              ? value.value.double_value
                              : static_cast<double>(value.value.int_value);
          nulls[depth++] = false;
        } else {
          const size_t rhs = --depth;
          const size_t lhs = depth - 1;
          nulls[lhs] = nulls[lhs] || nulls[rhs];
          if (nulls[lhs]) {
            continue;
          }
          switch (instruction.binary) {
            case BinaryOperation::kAdd:
              values[lhs] += values[rhs];
              break;
            case BinaryOperation::kSubtract:
              values[lhs] -= values[rhs];
              break;
            case BinaryOperation::kMultiply:
              values[lhs] *= values[rhs];
              break;
            case BinaryOperation::kDivide: {
              if (values[rhs] == 0.0) {
                return StatusError(StatusCode::kIsInfinity, "division by zero");
              }
              const double quotient = values[lhs] / values[rhs];
              if (std::isfinite(values[lhs]) && std::isfinite(values[rhs]) &&
                  std::isinf(quotient)) {
                return StatusError(StatusCode::kIsInfinity, "double overflow");
              }
              values[lhs] = quotient;
              break;
            }
            default:
              break;
          }
        }
      }
      if (supported) {
        *out = values[0];
        *is_null = nulls[0];
        return Status::kSuccess;
      }
    }
    ASSIGN_OR_RETURN(Value, child,
                     eval_pair_program(child_programs[agg_idx], shape, scratch,
                                       left_row, right_row));
    if (child.IsNull()) {
      *is_null = true;
      return Status::kSuccess;
    }
    if (cell_as_double(child, out)) {
      *is_null = false;
      return Status::kSuccess;
    }
    return StatusError(StatusCode::kInvalidArgument,
                       "numeric aggregate operand is not double/int64");
  };
  const auto eval_pair_ext = [&](size_t agg_idx, Row& scratch,
                                 const Row* left_row, const Row* right_row,
                                 platform::ExtendedFloat* out,
                                 bool* is_null) -> Status {
    const ProgramShape& shape = child_shapes[agg_idx];
    if (shape.direct_column >= 0) {
      const Value& c = get_pair_cell(left_row, right_row,
                                     static_cast<slot_t>(shape.direct_column));
      if (c.IsNull()) {
        *is_null = true;
        return Status::kSuccess;
      }
      if (c.type == ValueType::kDouble) {
        *out = static_cast<platform::ExtendedFloat>(c.value.double_value);
        *is_null = false;
        return Status::kSuccess;
      }
      if ((c.type == ValueType::kInt64 || c.type == ValueType::kDate) &&
          !c.IsUnsigned()) {
        *out = static_cast<platform::ExtendedFloat>(c.value.int_value);
        *is_null = false;
        return Status::kSuccess;
      }
    }
    double dv = 0.0;
    RETURN_IF_FAIL(
        eval_pair_double(agg_idx, scratch, left_row, right_row, &dv, is_null));
    *out = static_cast<platform::ExtendedFloat>(dv);
    return Status::kSuccess;
  };
  const auto read_group_pair_cell = [&](const FusedGroup& grp, size_t col_idx,
                                        Value* col_scratch) -> const Value& {
    if (col_idx < left_width) {
      if (grp.rep_left != nullptr) {
        return grp.rep_left->values_[col_idx];
      }
      if (grp.rep_col_chunk != std::numeric_limits<uint32_t>::max()) {
        left.columnar->chunks[grp.rep_col_chunk]
            .columns[col_idx]
            .WriteValueInto(grp.rep_col_row, col_scratch);
        return *col_scratch;
      }
      return kNullCell;
    }
    return read_right_cell(grp.rep_right_idx, col_idx - left_width,
                           col_scratch);
  };
  const auto ensure_group_key = [&](FusedGroup& grp) {
    if (!grp.key.values_.empty() || !all_direct_group_keys ||
        (grp.rep_left == nullptr &&
         grp.rep_col_chunk == std::numeric_limits<uint32_t>::max() &&
         grp.rep_right_idx == kInvalidRightIdx)) {
      return;
    }
    grp.key.values_.resize(key_programs.size());
    Value col_tmp;
    for (size_t k = 0; k < key_programs.size(); ++k) {
      const Value& raw = read_group_pair_cell(
          grp, static_cast<size_t>(key_shapes[k].direct_column), &col_tmp);
      grp.key.values_[k] = CanonicalDistinctValue(raw);
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
  std::vector<ValueType> def_base_types;
  std::vector<uint8_t> def_overwritten_by_pk;
  std::vector<PreparedDeferredPkLookup> prepared_pk_lookups;
  std::vector<PreparedDeferredPkGroup> def_pk_groups;
  std::array<int8_t, 16> def_col_to_pk_lookup{};
  def_col_to_pk_lookup.fill(-1);
  SharedBaseRelationCache* const shared_pk_cache =
      context.execution_runtime() != nullptr
          ? context.execution_runtime()->shared_base_cache.get()
          : nullptr;
  if (deferred_left_probe != nullptr) {
    if (join_type != JoinType::kInner || left.HasSpill() ||
        deferred_left_probe->store == nullptr ||
        deferred_left_probe->pk_lookups.size() > 4) {
      return not_fusable("deferred_left_probe unsupported");
    }
    const size_t base_proj_cols = deferred_left_probe->projection.size();
    if (base_proj_cols == 0 || base_proj_cols > 16 ||
        base_proj_cols > left_width) {
      return not_fusable("deferred_left_probe width");
    }
    def_base_types.resize(base_proj_cols, ValueType::kNull);
    def_overwritten_by_pk.assign(base_proj_cols, 0);
    for (const DeferredLSTablePkLookup& lkp : deferred_left_probe->pk_lookups) {
      if (lkp.store == nullptr || lkp.pk_col >= base_proj_cols ||
          lkp.target_proj_col >= left_width ||
          lkp.store_pk_col >= lkp.store->column_min_int.size() ||
          lkp.payload_col >= lkp.store->column_min_int.size() ||
          (lkp.payload_type != ValueType::kInt64 &&
           lkp.payload_type != ValueType::kDate &&
           lkp.payload_type != ValueType::kDouble &&
           lkp.payload_type != ValueType::kVarChar)) {
        return not_fusable("deferred_left_probe pk_lookup");
      }
      if (lkp.target_proj_col < base_proj_cols) {
        def_overwritten_by_pk[lkp.target_proj_col] = 1;
      }
      PreparedDeferredPkLookup prep;
      prep.pk_col = lkp.pk_col;
      prep.target_proj_col = lkp.target_proj_col;
      prep.payload_type = lkp.payload_type;
      prep.payload_unsigned = lkp.payload_unsigned;
      prep.blocks.reserve(lkp.store->blocks.size());
      prep.blk_max_keys.reserve(lkp.store->blocks.size());
      for (const LSTablePageBlock& blk : lkp.store->blocks) {
        if (blk.row_count > 0) {
          if (lkp.store_pk_col >= blk.columns.size() ||
              lkp.payload_col >= blk.columns.size()) {
            return not_fusable("deferred_left_probe pk blk");
          }
          const LSTableColumnBlock& pk_b = blk.columns[lkp.store_pk_col];
          const LSTableColumnBlock& pay_b = blk.columns[lkp.payload_col];
          const bool is_dense =
              (static_cast<uint64_t>(pk_b.max_int) -
                   static_cast<uint64_t>(pk_b.min_int) + 1ULL ==
               blk.row_count);
          prep.blocks.push_back(PreparedDeferredPkBlock{
              .min_k = pk_b.min_int,
              .max_k = pk_b.max_int,
              .row_count = blk.row_count,
              .dense_contiguous = static_cast<uint8_t>(is_dense),
              .has_null_payload = static_cast<uint8_t>(pay_b.has_null),
              .pk_cells = pk_b.int_cells.data(),
              .pay_cells = pay_b.int_cells.data(),
              .pay_blk = &pay_b});
          prep.blk_max_keys.push_back(pk_b.max_int);
        }
      }
      if (prep.blocks.empty()) {
        return not_fusable("deferred_left_probe empty pk blocks");
      }
      prep.min_k = lkp.store->column_min_int[lkp.store_pk_col];
      prep.max_k = lkp.store->column_max_int[lkp.store_pk_col];
      prep.span = std::max<uint64_t>(
          1ULL, static_cast<uint64_t>(prep.max_k) -
                    static_cast<uint64_t>(prep.min_k) + 1ULL);
      const auto nb_count = static_cast<uint32_t>(prep.blocks.size());
      uint32_t cur_nb = 0;
      for (size_t b = 0; b < PreparedDeferredPkLookup::kDirBuckets; ++b) {
        const int64_t b_min =
            prep.min_k +
            static_cast<int64_t>((static_cast<__uint128_t>(prep.span) * b) >>
                                 PreparedDeferredPkLookup::kDirShift);
        while (cur_nb + 1 < nb_count && prep.blk_max_keys[cur_nb] < b_min) {
          ++cur_nb;
        }
        prep.bucket_lo[b] = cur_nb;
      }
      prep.bucket_lo[PreparedDeferredPkLookup::kDirBuckets] = nb_count - 1;
      prep.store_payload_col = lkp.payload_col;
      const size_t li = prepared_pk_lookups.size();
      size_t gi = def_pk_groups.size();
      for (size_t g = 0; g < def_pk_groups.size(); ++g) {
        if (def_pk_groups[g].pk_col == lkp.pk_col &&
            def_pk_groups[g].store == lkp.store.get() &&
            def_pk_groups[g].store_pk_col == lkp.store_pk_col) {
          gi = g;
          break;
        }
      }
      if (gi == def_pk_groups.size()) {
        PreparedDeferredPkGroup grp;
        grp.pk_col = lkp.pk_col;
        grp.store = lkp.store.get();
        grp.store_owner = lkp.store;
        grp.store_pk_col = lkp.store_pk_col;
        grp.min_k = prep.min_k;
        grp.max_k = prep.max_k;
        grp.span = prep.span;
        grp.bucket_lo = prep.bucket_lo;
        grp.blk_max_keys = prep.blk_max_keys;
        grp.blocks.reserve(prep.blocks.size());
        uint32_t running_rows = 0;
        for (const auto& pb : prep.blocks) {
          grp.blocks.push_back(PreparedDeferredPkGroup::Block{
              .min_k = pb.min_k,
              .max_k = pb.max_k,
              .row_count = pb.row_count,
              .row_base = running_rows,
              .dense_contiguous = pb.dense_contiguous,
              .pk_cells = pb.pk_cells});
          running_rows += pb.row_count;
        }
        grp.total_rows = running_rows;
        def_pk_groups.push_back(std::move(grp));
      }
      prep.group_idx = gi;
      def_pk_groups[gi].lookup_indices.push_back(li);
      if (prep.target_proj_col < 16) {
        def_col_to_pk_lookup[prep.target_proj_col] = static_cast<int8_t>(li);
      }
      prepared_pk_lookups.push_back(std::move(prep));
    }
    constexpr uint64_t kMaxDensePkSpan = 262144;
    for (PreparedDeferredPkGroup& grp : def_pk_groups) {
      if (grp.total_rows == 0 || grp.max_k < grp.min_k ||
          grp.span > kMaxDensePkSpan) {
        continue;
      }
      std::string dense_key;
      struct DenseSlotGuard {
        SharedBaseRelationCache* cache{nullptr};
        std::string key;
        bool active{false};
        DenseSlotGuard() = default;
        DenseSlotGuard(SharedBaseRelationCache* c, std::string k, bool a)
            : cache(c), key(std::move(k)), active(a) {}
        DenseSlotGuard(const DenseSlotGuard&) = delete;
        DenseSlotGuard& operator=(const DenseSlotGuard&) = delete;
        DenseSlotGuard(DenseSlotGuard&&) = delete;
        DenseSlotGuard& operator=(DenseSlotGuard&&) = delete;
        ~DenseSlotGuard() {
          if (active && cache != nullptr) {
            cache->AbandonPkDense(key);
          }
        }
      } dense_guard(shared_pk_cache, {}, false);
      if (shared_pk_cache != nullptr && grp.store != nullptr) {
        dense_key =
            "D:" + std::to_string(reinterpret_cast<uintptr_t>(grp.store)) +
            ":" + std::to_string(grp.store->layout_generation) + ":" +
            std::to_string(grp.store_pk_col) + ":" + std::to_string(grp.min_k) +
            ":" + std::to_string(grp.max_k) + ":" +
            std::to_string(grp.total_rows);
        std::shared_ptr<
            const std::vector<PreparedDeferredPkGroup::DensePkEntry>>
            hit;
        if (!shared_pk_cache->AcquirePkDenseSlot(dense_key, &hit)) {
          if (hit != nullptr && hit->size() == static_cast<size_t>(grp.span)) {
            grp.shared_dense = std::move(hit);
            grp.dense_ptr = grp.shared_dense->data();
            grp.dense_size = grp.shared_dense->size();
            continue;
          }
        } else {
          dense_guard.key = dense_key;
          dense_guard.active = true;
        }
      }
      const size_t dense_bytes = static_cast<size_t>(grp.span) *
                                 sizeof(PreparedDeferredPkGroup::DensePkEntry);
      grp.dense_index_charge = TryReserveQueryMemory(dense_bytes);
      if (!grp.dense_index_charge) {
        continue;
      }
      grp.dense_index.resize(static_cast<size_t>(grp.span));
      bool dense_ok = true;
      bool blocks_disjoint_sorted =
          grp.blocks.size() >= 2 && grp.total_rows >= 16384;
      int64_t prev_max_k = std::numeric_limits<int64_t>::min();
      bool have_prev_blk = false;
      for (const auto& bm : grp.blocks) {
        if (bm.pk_cells == nullptr) {
          dense_ok = false;
          break;
        }
        if (bm.row_count > 0) {
          if (have_prev_blk && bm.min_k <= prev_max_k) {
            blocks_disjoint_sorted = false;
          }
          prev_max_k = bm.max_k;
          have_prev_blk = true;
        }
      }
      if (dense_ok) {
        if (blocks_disjoint_sorted) {
          const size_t dense_workers = std::min<size_t>(
              ParallelWorkerCount(grp.total_rows / 8192), grp.blocks.size());
          if (dense_workers > 1) {
            std::atomic<uint32_t> next_blk{0};
            const auto blk_count = static_cast<uint32_t>(grp.blocks.size());
            (void)RunParallelTasks(dense_workers, [&](size_t) {
              uint32_t nb = 0;
              while ((nb = next_blk.fetch_add(1, std::memory_order_relaxed)) <
                     blk_count) {
                const auto& bm = grp.blocks[nb];
                const int64_t* const pks = bm.pk_cells;
                for (uint32_t rin = 0; rin < bm.row_count; ++rin) {
                  const int64_t pk = pks[rin];
                  if (pk >= grp.min_k && pk <= grp.max_k) {
                    const auto idx =
                        static_cast<size_t>(static_cast<uint64_t>(pk) -
                                            static_cast<uint64_t>(grp.min_k));
                    auto& entry = grp.dense_index[idx];
                    if (entry.nb == std::numeric_limits<uint32_t>::max()) {
                      entry.nb = nb;
                      entry.rin = rin;
                      entry.subgid = bm.row_base + rin;
                    }
                  }
                }
              }
            });
          } else {
            blocks_disjoint_sorted = false;
          }
        }
        if (!blocks_disjoint_sorted) {
          for (uint32_t nb = 0; nb < grp.blocks.size(); ++nb) {
            const auto& bm = grp.blocks[nb];
            const int64_t* const pks = bm.pk_cells;
            for (uint32_t rin = 0; rin < bm.row_count; ++rin) {
              const int64_t pk = pks[rin];
              if (pk >= grp.min_k && pk <= grp.max_k) {
                const auto idx =
                    static_cast<size_t>(static_cast<uint64_t>(pk) -
                                        static_cast<uint64_t>(grp.min_k));
                auto& entry = grp.dense_index[idx];
                if (entry.nb == std::numeric_limits<uint32_t>::max()) {
                  entry.nb = nb;
                  entry.rin = rin;
                  // Stash row_base + rin temporarily; remapped to subgid if
                  // built.
                  entry.subgid = bm.row_base + rin;
                }
              }
            }
          }
        }
      }
      if (!dense_ok) {
        grp.dense_index.clear();
        grp.dense_index_charge.reset();
      } else if (dense_guard.active && shared_pk_cache != nullptr) {
        auto shared = std::make_shared<
            std::vector<PreparedDeferredPkGroup::DensePkEntry>>(
            std::move(grp.dense_index));
        shared_pk_cache->PublishPkDense(dense_key, shared);
        dense_guard.active = false;
        grp.shared_dense = std::move(shared);
        grp.dense_ptr = grp.shared_dense->data();
        grp.dense_size = grp.shared_dense->size();
      } else {
        grp.dense_ptr = grp.dense_index.data();
        grp.dense_size = grp.dense_index.size();
      }
    }
    for (const slot_t lk : left_columns) {
      if (lk >= base_proj_cols || def_overwritten_by_pk[lk] != 0) {
        return not_fusable("deferred_left_probe join key overwritten");
      }
    }
    for (size_t c = 0; c < base_proj_cols; ++c) {
      if (def_overwritten_by_pk[c] == 0) {
        const Column& col = left.schema.GetColumn(c);
        if (col.IsUnsigned() || (col.Type() != ValueType::kInt64 &&
                                 col.Type() != ValueType::kDate &&
                                 col.Type() != ValueType::kDouble)) {
          return not_fusable("deferred_left_probe base col type");
        }
        def_base_types[c] = col.Type();
      }
    }
  }
  bool use_split_key_gid = false;
  bool split_has_left_probe = false;
  bool split_uses_right_subgid = false;
  bool split_uses_pk_subgid = false;
  uint8_t split_right_shift = 32;
  size_t split_left_k = 1;
  size_t split_right_subgid_count = 0;
  std::vector<uint32_t> split_right_subgid;
  const uint32_t* split_right_subgid_ptr = nullptr;
  std::vector<uint32_t> split_right_rep_idx;
  const uint32_t* split_right_rep_ptr = nullptr;
  size_t split_right_rep_count = 0;
  std::shared_ptr<const SharedBaseRelationCache::CachedSplitRightDict>
      shared_right_dict;
  const auto clear_split_right_dict = [&]() noexcept {
    split_right_subgid.clear();
    split_right_rep_idx.clear();
    shared_right_dict.reset();
    split_right_subgid_ptr = nullptr;
    split_right_rep_ptr = nullptr;
    split_right_rep_count = 0;
    split_right_subgid_count = 0;
  };
  const auto fast_hash_val = [](const Value& value) -> uint64_t {
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
  };
  const auto splitmix_i64 = [](int64_t iv) noexcept -> uint64_t {
    uint64_t x = static_cast<uint64_t>(iv) + 0x9e3779b97f4a7c15ULL;
    x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
    x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
    return x ^ (x >> 31);
  };
  const auto read_pk_cell = [&](size_t li, uint32_t nb, uint32_t rin,
                                Value* out) {
    const PreparedDeferredPkLookup& m = prepared_pk_lookups[li];
    const PreparedDeferredPkBlock& pb = m.blocks[nb];
    if (pb.has_null_payload != 0 && pb.pay_blk->IsNull(rin)) {
      *out = Value();
      return;
    }
    if (m.payload_type == ValueType::kVarChar) {
      out->type = ValueType::kVarChar;
      out->SetCollation(0);
      out->value.varchar_value = pb.pay_blk->VarcharAt(rin);
    } else if (m.payload_type == ValueType::kDouble) {
      const auto dv = std::bit_cast<double>(pb.pay_cells[rin]);
      out->type = ValueType::kDouble;
      out->SetCollation(0);
      out->value.double_value = dv == 0.0 ? 0.0 : dv;
    } else {
      out->type = m.payload_type;
      out->SetCollation(0);
      if (m.payload_unsigned) {
        out->SetUnsigned();
      }
      out->value.int_value = pb.pay_cells[rin];
    }
  };
  const auto build_flat_subgid_dict =
      [&](size_t row_count, size_t max_unique, auto&& fill_hashes,
          auto&& eq_rows, std::vector<uint32_t>* out_subgid,
          uint32_t* out_count, const std::vector<uint8_t>* row_active = nullptr,
          std::vector<uint32_t>* out_rep_rows = nullptr) -> bool {
    if (row_count == 0) {
      out_subgid->clear();
      *out_count = 0;
      if (out_rep_rows != nullptr) {
        out_rep_rows->clear();
      }
      return true;
    }
    std::vector<uint64_t> row_hashes(row_count);
    fill_hashes(row_hashes);
    const size_t dict_workers = ParallelWorkerCount(row_count / 16384);
    if (dict_workers > 1 && row_count >= 16384 && max_unique <= 4096) {
      out_subgid->resize(row_count);
      const size_t local_cap =
          std::bit_ceil(std::max<size_t>(32ULL, max_unique * 2ULL));
      const size_t local_mask = local_cap - 1;
      std::vector<std::vector<uint32_t>> local_reps(dict_workers);
      std::vector<std::vector<uint64_t>> local_hashes(dict_workers);
      std::atomic<bool> overflowed{false};
      (void)RunParallelTasks(dict_workers, [&](size_t w) {
        const size_t begin = (row_count * w) / dict_workers;
        const size_t end = (row_count * (w + 1)) / dict_workers;
        std::vector<uint32_t> buckets(local_cap,
                                      std::numeric_limits<uint32_t>::max());
        auto& l_reps = local_reps[w];
        auto& l_hashes = local_hashes[w];
        l_reps.reserve(std::min<size_t>(max_unique, 1024ULL));
        l_hashes.reserve(std::min<size_t>(max_unique, 1024ULL));
        for (size_t r = begin; r < end; ++r) {
          if (row_active != nullptr && (*row_active)[r] == 0) {
            (*out_subgid)[r] = std::numeric_limits<uint32_t>::max();
            continue;
          }
          const uint64_t h = row_hashes[r];
          size_t slot = static_cast<size_t>(h ^ (h >> 32)) & local_mask;
          bool found = false;
          uint32_t found_id = 0;
          while (buckets[slot] != std::numeric_limits<uint32_t>::max()) {
            const uint32_t id = buckets[slot];
            if (l_hashes[id] == h && eq_rows(l_reps[id], r)) {
              found = true;
              found_id = id;
              break;
            }
            slot = (slot + 1) & local_mask;
          }
          if (!found) {
            if (l_reps.size() >= max_unique) {
              overflowed.store(true, std::memory_order_relaxed);
              return;
            }
            found_id = static_cast<uint32_t>(l_reps.size());
            l_reps.push_back(static_cast<uint32_t>(r));
            l_hashes.push_back(h);
            buckets[slot] = found_id;
          }
          (*out_subgid)[r] = found_id;
        }
      });
      if (overflowed.load(std::memory_order_relaxed)) {
        out_subgid->clear();
        *out_count = 0;
        if (out_rep_rows != nullptr) {
          out_rep_rows->clear();
        }
        return false;
      }
      size_t total_local = 0;
      for (const auto& lr : local_reps) {
        total_local += lr.size();
      }
      const size_t g_cap =
          std::bit_ceil(std::max<size_t>(32ULL, total_local * 2ULL));
      const size_t g_mask = g_cap - 1;
      std::vector<uint32_t> g_buckets(g_cap,
                                      std::numeric_limits<uint32_t>::max());
      std::vector<uint32_t> rep_rows;
      std::vector<uint64_t> rep_hashes;
      rep_rows.reserve(std::min(max_unique, total_local));
      rep_hashes.reserve(std::min(max_unique, total_local));
      std::vector<std::vector<uint32_t>> remaps(dict_workers);
      for (size_t w = 0; w < dict_workers; ++w) {
        const auto& lr = local_reps[w];
        const auto& lh = local_hashes[w];
        auto& rm = remaps[w];
        rm.resize(lr.size());
        for (size_t lid = 0; lid < lr.size(); ++lid) {
          const uint32_t rr = lr[lid];
          const uint64_t h = lh[lid];
          size_t slot = static_cast<size_t>(h ^ (h >> 32)) & g_mask;
          bool found = false;
          uint32_t gid = 0;
          while (g_buckets[slot] != std::numeric_limits<uint32_t>::max()) {
            const uint32_t cand = g_buckets[slot];
            if (rep_hashes[cand] == h && eq_rows(rep_rows[cand], rr)) {
              found = true;
              gid = cand;
              break;
            }
            slot = (slot + 1) & g_mask;
          }
          if (!found) {
            if (rep_rows.size() >= max_unique) {
              out_subgid->clear();
              *out_count = 0;
              if (out_rep_rows != nullptr) {
                out_rep_rows->clear();
              }
              return false;
            }
            gid = static_cast<uint32_t>(rep_rows.size());
            rep_rows.push_back(rr);
            rep_hashes.push_back(h);
            g_buckets[slot] = gid;
          }
          rm[lid] = gid;
        }
      }
      (void)RunParallelTasks(dict_workers, [&](size_t w) {
        const size_t begin = (row_count * w) / dict_workers;
        const size_t end = (row_count * (w + 1)) / dict_workers;
        const auto& rm = remaps[w];
        for (size_t r = begin; r < end; ++r) {
          const uint32_t lid = (*out_subgid)[r];
          if (lid != std::numeric_limits<uint32_t>::max()) {
            (*out_subgid)[r] = rm[lid];
          }
        }
      });
      *out_count = static_cast<uint32_t>(rep_rows.size());
      if (out_rep_rows != nullptr) {
        *out_rep_rows = std::move(rep_rows);
      }
      return true;
    }
    if (dict_workers > 1 && row_count >= 16384 &&
        max_unique == std::numeric_limits<size_t>::max()) {
      constexpr size_t kDictParts = 64;
      const auto mix64 = [](uint64_t x) noexcept -> uint64_t {
        x ^= x >> 30;
        x *= 0xbf58476d1ce4e5b9ULL;
        x ^= x >> 27;
        x *= 0x94d049bb133111ebULL;
        return x ^ (x >> 31);
      };
      out_subgid->resize(row_count);
      std::vector<std::array<uint32_t, kDictParts>> worker_counts(dict_workers);
      (void)RunParallelTasks(dict_workers, [&](size_t w) {
        auto& counts = worker_counts[w];
        counts.fill(0);
        const size_t begin = (row_count * w) / dict_workers;
        const size_t end = (row_count * (w + 1)) / dict_workers;
        for (size_t r = begin; r < end; ++r) {
          if (row_active != nullptr && (*row_active)[r] == 0) {
            (*out_subgid)[r] = std::numeric_limits<uint32_t>::max();
            continue;
          }
          const auto p = static_cast<size_t>(mix64(row_hashes[r]) >> 58);
          ++counts[p];
        }
      });
      std::array<uint32_t, kDictParts> part_sizes{};
      size_t active_total = 0;
      for (size_t p = 0; p < kDictParts; ++p) {
        uint32_t sum = 0;
        for (size_t w = 0; w < dict_workers; ++w) {
          sum += worker_counts[w][p];
        }
        part_sizes[p] = sum;
        active_total += sum;
      }
      if (active_total == 0) {
        *out_count = 0;
        if (out_rep_rows != nullptr) {
          out_rep_rows->clear();
        }
        return true;
      }
      uint32_t max_part_size = 0;
      for (size_t p = 0; p < kDictParts; ++p) {
        max_part_size = std::max(max_part_size, part_sizes[p]);
      }
      if (max_part_size <= (active_total / 2) + 64) {
        std::array<size_t, kDictParts> part_offsets{};
        std::vector<std::array<size_t, kDictParts>> worker_offsets(
            dict_workers);
        size_t running = 0;
        for (size_t p = 0; p < kDictParts; ++p) {
          part_offsets[p] = running;
          for (size_t w = 0; w < dict_workers; ++w) {
            worker_offsets[w][p] = running;
            running += worker_counts[w][p];
          }
        }
        std::vector<uint32_t> part_rows(active_total);
        (void)RunParallelTasks(dict_workers, [&](size_t w) {
          auto cursors = worker_offsets[w];
          const size_t begin = (row_count * w) / dict_workers;
          const size_t end = (row_count * (w + 1)) / dict_workers;
          for (size_t r = begin; r < end; ++r) {
            if (row_active != nullptr && (*row_active)[r] == 0) {
              continue;
            }
            const auto p = static_cast<size_t>(mix64(row_hashes[r]) >> 58);
            part_rows[cursors[p]++] = static_cast<uint32_t>(r);
          }
        });
        std::vector<uint32_t> part_local_ids(active_total);
        std::array<std::vector<uint32_t>, kDictParts> part_rep_rows;
        std::atomic<size_t> next_p{0};
        (void)RunParallelTasks(dict_workers, [&](size_t) {
          size_t p = 0;
          while ((p = next_p.fetch_add(1, std::memory_order_relaxed)) <
                 kDictParts) {
            const uint32_t p_cnt = part_sizes[p];
            if (p_cnt == 0) {
              continue;
            }
            const size_t p_begin = part_offsets[p];
            const size_t p_cap =
                std::bit_ceil(std::max<size_t>(16ULL, p_cnt * 2ULL));
            const size_t p_mask = p_cap - 1;
            std::vector<uint32_t> buckets(p_cap,
                                          std::numeric_limits<uint32_t>::max());
            auto& preps = part_rep_rows[p];
            preps.reserve(std::min<size_t>(p_cnt, 4096ULL));
            std::vector<uint64_t> phashes;
            phashes.reserve(std::min<size_t>(p_cnt, 4096ULL));
            for (size_t idx = p_begin; idx < p_begin + p_cnt; ++idx) {
              const uint32_t r = part_rows[idx];
              const uint64_t h = row_hashes[r];
              size_t slot = static_cast<size_t>(mix64(h)) & p_mask;
              bool found = false;
              uint32_t lid = 0;
              while (buckets[slot] != std::numeric_limits<uint32_t>::max()) {
                const uint32_t id = buckets[slot];
                if (phashes[id] == h && eq_rows(preps[id], r)) {
                  found = true;
                  lid = id;
                  break;
                }
                slot = (slot + 1) & p_mask;
              }
              if (!found) {
                lid = static_cast<uint32_t>(preps.size());
                preps.push_back(r);
                phashes.push_back(h);
                buckets[slot] = lid;
              }
              part_local_ids[idx] = lid;
            }
          }
        });
        std::array<uint32_t, kDictParts> base_ids{};
        uint32_t total_unique = 0;
        for (size_t p = 0; p < kDictParts; ++p) {
          base_ids[p] = total_unique;
          total_unique += static_cast<uint32_t>(part_rep_rows[p].size());
        }
        if (out_rep_rows != nullptr) {
          out_rep_rows->clear();
          out_rep_rows->reserve(total_unique);
          for (size_t p = 0; p < kDictParts; ++p) {
            out_rep_rows->insert(out_rep_rows->end(), part_rep_rows[p].begin(),
                                 part_rep_rows[p].end());
          }
        }
        std::atomic<size_t> next_wp{0};
        (void)RunParallelTasks(dict_workers, [&](size_t) {
          size_t p = 0;
          while ((p = next_wp.fetch_add(1, std::memory_order_relaxed)) <
                 kDictParts) {
            const uint32_t p_cnt = part_sizes[p];
            const size_t p_begin = part_offsets[p];
            const uint32_t bid = base_ids[p];
            for (size_t idx = p_begin; idx < p_begin + p_cnt; ++idx) {
              (*out_subgid)[part_rows[idx]] = bid + part_local_ids[idx];
            }
          }
        });
        *out_count = total_unique;
        return true;
      }
    }
    const size_t cap = std::bit_ceil(std::max<size_t>(16ULL, row_count * 2ULL));
    const size_t mask = cap - 1;
    std::vector<uint32_t> buckets(cap, std::numeric_limits<uint32_t>::max());
    std::vector<uint32_t> rep_rows;
    std::vector<uint64_t> rep_hashes;
    const size_t reserve_est = std::min<size_t>(row_count, 65536ULL);
    rep_rows.reserve(reserve_est);
    rep_hashes.reserve(reserve_est);
    out_subgid->resize(row_count);
    for (size_t r = 0; r < row_count; ++r) {
      if (row_active != nullptr && (*row_active)[r] == 0) {
        (*out_subgid)[r] = std::numeric_limits<uint32_t>::max();
        continue;
      }
      const uint64_t h = row_hashes[r];
      size_t slot = static_cast<size_t>(h ^ (h >> 32)) & mask;
      bool found = false;
      uint32_t found_id = 0;
      while (buckets[slot] != std::numeric_limits<uint32_t>::max()) {
        const uint32_t id = buckets[slot];
        if (rep_hashes[id] == h && eq_rows(rep_rows[id], r)) {
          found = true;
          found_id = id;
          break;
        }
        slot = (slot + 1) & mask;
      }
      if (!found) {
        if (rep_rows.size() >= max_unique) {
          out_subgid->clear();
          *out_count = 0;
          if (out_rep_rows != nullptr) {
            out_rep_rows->clear();
          }
          return false;
        }
        found_id = static_cast<uint32_t>(rep_rows.size());
        rep_rows.push_back(static_cast<uint32_t>(r));
        rep_hashes.push_back(h);
        buckets[slot] = found_id;
      }
      (*out_subgid)[r] = found_id;
    }
    *out_count = static_cast<uint32_t>(rep_rows.size());
    if (out_rep_rows != nullptr) {
      *out_rep_rows = std::move(rep_rows);
    }
    return true;
  };
  const size_t right_row_count =
      right_col_only ? total_right_rows : right_ptr->rows.size();
  const auto build_or_reuse_split_right_dict =
      [&](const std::vector<size_t>& col_indices, uint32_t* out_card) -> bool {
    *out_card = 0;
    clear_split_right_dict();
    struct RightDictSlotGuard {
      SharedBaseRelationCache* cache{nullptr};
      std::string key;
      bool active{false};
      RightDictSlotGuard() = default;
      RightDictSlotGuard(SharedBaseRelationCache* c, std::string k, bool a)
          : cache(c), key(std::move(k)), active(a) {}
      RightDictSlotGuard(const RightDictSlotGuard&) = delete;
      RightDictSlotGuard& operator=(const RightDictSlotGuard&) = delete;
      RightDictSlotGuard(RightDictSlotGuard&&) = delete;
      RightDictSlotGuard& operator=(RightDictSlotGuard&&) = delete;
      ~RightDictSlotGuard() {
        if (active && cache != nullptr) {
          cache->AbandonSplitRightDict(key);
        }
      }
    } right_dict_guard(shared_pk_cache, {}, false);
    std::string right_dict_key;
    if (shared_pk_cache != nullptr && right_col_only &&
        right_col_owner != nullptr) {
      right_dict_key =
          "R:" +
          std::to_string(reinterpret_cast<uintptr_t>(right_col_owner.get())) +
          ":" + std::to_string(right_row_count) + ":" +
          std::to_string(right_arg.columnar->chunks.size());
      for (const size_t col : col_indices) {
        right_dict_key += ":";
        right_dict_key += std::to_string(col);
      }
      std::shared_ptr<const SharedBaseRelationCache::CachedSplitRightDict> hit;
      if (!shared_pk_cache->AcquireSplitRightDictSlot(right_dict_key, &hit)) {
        if (hit != nullptr) {
          shared_right_dict = std::move(hit);
          split_right_subgid_ptr = shared_right_dict->subgid.data();
          split_right_rep_ptr = shared_right_dict->rep_idx.data();
          split_right_rep_count = shared_right_dict->rep_idx.size();
          *out_card = shared_right_dict->subgid_count;
          return true;
        }
      } else {
        right_dict_guard.key = right_dict_key;
        right_dict_guard.active = true;
      }
    }
    const auto fill_right_hashes = [&](std::vector<uint64_t>& hashes) {
      const auto mix_step = [](uint64_t& h, uint64_t v) noexcept {
        h = std::rotl(h, 7);
        h ^= v;
        h *= 0x100000001b3ULL;
      };
      if (right_col_only && right_arg.columnar != nullptr) {
        const auto& chunks = right_arg.columnar->chunks;
        struct HashSlice {
          size_t ci;
          size_t rb;
          size_t re;
          size_t out_base;
        };
        std::vector<HashSlice> slices;
        slices.reserve(chunks.size());
        size_t base = 0;
        for (size_t ci = 0; ci < chunks.size(); ++ci) {
          const size_t rows = chunks[ci].rows;
          if (rows == 0) {
            continue;
          }
          constexpr size_t kSliceRows = 4096;
          for (size_t rb = 0; rb < rows; rb += kSliceRows) {
            const size_t re = std::min(rows, rb + kSliceRows);
            slices.push_back(
                HashSlice{.ci = ci, .rb = rb, .re = re, .out_base = base + rb});
          }
          base += rows;
        }
        auto run_slice_worker = [&](size_t s_begin, size_t s_end) {
          std::vector<uint64_t> dict_hash;
          std::vector<uint8_t> dict_valid;
          Value v_tmp;
          for (size_t si = s_begin; si < s_end; ++si) {
            const HashSlice& slice = slices[si];
            const size_t n = slice.re - slice.rb;
            uint64_t* dst = hashes.data() + slice.out_base;
            std::fill_n(dst, n, uint64_t{0x9e3779b97f4a7c15ULL});
            const auto& cols = chunks[slice.ci].columns;
            for (const size_t column : col_indices) {
              const ColumnarColumn& sc = cols[column];
              if (sc.encoding == ColumnarColumn::Encoding::kConstant &&
                  sc.is_uniform) {
                uint64_t ch_hash = 0;
                if (!sc.const_is_null && sc.type != ValueType::kNull) {
                  if (sc.type == ValueType::kVarChar && !sc.is_interval) {
                    ch_hash = std::hash<std::string_view>{}(sc.const_string);
                  } else if ((sc.type == ValueType::kInt64 ||
                              sc.type == ValueType::kDate) &&
                             !sc.is_unsigned) {
                    ch_hash = splitmix_i64(sc.const_int);
                  } else {
                    sc.WriteValueInto(slice.rb, &v_tmp);
                    ch_hash = fast_hash_val(v_tmp);
                  }
                }
                for (size_t i = 0; i < n; ++i) {
                  mix_step(dst[i], ch_hash);
                }
              } else if (sc.encoding == ColumnarColumn::Encoding::kDictionary &&
                         sc.is_uniform && !sc.is_interval &&
                         sc.type == ValueType::kVarChar && sc.dict != nullptr) {
                const size_t entry_count = sc.DictEntryCount();
                const bool cache_dict = entry_count <= 8192;
                if (cache_dict) {
                  dict_valid.assign(entry_count, 0);
                  if (dict_hash.size() < entry_count) {
                    dict_hash.resize(entry_count);
                  }
                }
                if (!sc.has_null && sc.borrowed_dict_u8 != nullptr) {
                  const uint8_t* codes = sc.borrowed_dict_u8 + slice.rb;
                  for (size_t i = 0; i < n; ++i) {
                    const uint32_t code = codes[i];
                    uint64_t hv = 0;
                    if (code < entry_count) [[likely]] {
                      if (cache_dict) {
                        if (dict_valid[code] != 0) [[likely]] {
                          hv = dict_hash[code];
                        } else {
                          hv = std::hash<std::string_view>{}(
                              sc.dict->StringEntryAt(code));
                          dict_hash[code] = hv;
                          dict_valid[code] = 1;
                        }
                      } else {
                        hv = std::hash<std::string_view>{}(
                            sc.dict->StringEntryAt(code));
                      }
                    }
                    mix_step(dst[i], hv);
                  }
                } else {
                  for (size_t i = 0; i < n; ++i) {
                    const uint32_t code = sc.DictCodeAt(slice.rb + i);
                    uint64_t hv = 0;
                    if ((!sc.has_null ||
                         code != ColumnarColumn::kNullDictCode) &&
                        code < entry_count) [[likely]] {
                      if (cache_dict) {
                        if (dict_valid[code] != 0) [[likely]] {
                          hv = dict_hash[code];
                        } else {
                          hv = std::hash<std::string_view>{}(
                              sc.dict->StringEntryAt(code));
                          dict_hash[code] = hv;
                          dict_valid[code] = 1;
                        }
                      } else {
                        hv = std::hash<std::string_view>{}(
                            sc.dict->StringEntryAt(code));
                      }
                    }
                    mix_step(dst[i], hv);
                  }
                }
              } else if (sc.encoding == ColumnarColumn::Encoding::kDictionary &&
                         sc.is_uniform && !sc.is_unsigned &&
                         (sc.type == ValueType::kInt64 ||
                          sc.type == ValueType::kDate) &&
                         sc.dict != nullptr) {
                const size_t entry_count = sc.DictEntryCount();
                const bool cache_dict = entry_count <= 8192;
                if (cache_dict) {
                  dict_valid.assign(entry_count, 0);
                  if (dict_hash.size() < entry_count) {
                    dict_hash.resize(entry_count);
                  }
                }
                for (size_t i = 0; i < n; ++i) {
                  const uint32_t code = sc.DictCodeAt(slice.rb + i);
                  uint64_t hv = 0;
                  if ((!sc.has_null || code != ColumnarColumn::kNullDictCode) &&
                      code < entry_count) [[likely]] {
                    if (cache_dict) {
                      if (dict_valid[code] != 0) [[likely]] {
                        hv = dict_hash[code];
                      } else {
                        hv = splitmix_i64(sc.dict->IntEntryAt(code));
                        dict_hash[code] = hv;
                        dict_valid[code] = 1;
                      }
                    } else {
                      hv = splitmix_i64(sc.dict->IntEntryAt(code));
                    }
                  }
                  mix_step(dst[i], hv);
                }
              } else if (sc.encoding == ColumnarColumn::Encoding::kFlat &&
                         sc.is_uniform && sc.type == ValueType::kVarChar &&
                         !sc.is_interval && !sc.strings.empty()) {
                const std::string_view* strs = sc.strings.data() + slice.rb;
                if (!sc.has_null || sc.nulls.empty()) {
                  for (size_t i = 0; i < n; ++i) {
                    mix_step(dst[i], std::hash<std::string_view>{}(strs[i]));
                  }
                } else {
                  const uint8_t* nul = sc.nulls.data() + slice.rb;
                  for (size_t i = 0; i < n; ++i) {
                    const uint64_t hv =
                        nul[i] != 0 ? 0ULL
                                    : std::hash<std::string_view>{}(strs[i]);
                    mix_step(dst[i], hv);
                  }
                }
              } else if (sc.is_uniform && !sc.is_unsigned &&
                         (sc.type == ValueType::kInt64 ||
                          sc.type == ValueType::kDate) &&
                         ((sc.encoding == ColumnarColumn::Encoding::kFlat &&
                           !sc.ints.empty()) ||
                          sc.encoding ==
                              ColumnarColumn::Encoding::kFrameOfReference)) {
                if (sc.encoding == ColumnarColumn::Encoding::kFlat &&
                    (!sc.has_null || sc.nulls.empty())) {
                  const int64_t* ivs = sc.ints.data() + slice.rb;
                  for (size_t i = 0; i < n; ++i) {
                    mix_step(dst[i], splitmix_i64(ivs[i]));
                  }
                } else {
                  for (size_t i = 0; i < n; ++i) {
                    const uint64_t hv =
                        sc.NonGatheredIsNull(slice.rb + i)
                            ? 0ULL
                            : splitmix_i64(
                                  sc.encoding == ColumnarColumn::Encoding::kFlat
                                      ? sc.ints[slice.rb + i]
                                      : sc.ForIntAt(slice.rb + i));
                    mix_step(dst[i], hv);
                  }
                }
              } else {
                for (size_t i = 0; i < n; ++i) {
                  sc.WriteValueInto(slice.rb + i, &v_tmp);
                  mix_step(dst[i], fast_hash_val(v_tmp));
                }
              }
            }
          }
        };
        if (right_row_count >= 16384 && slices.size() > 1) {
          const size_t hw = std::min(
              slices.size(), ParallelWorkerCount(right_row_count / 4096));
          if (hw > 1) {
            std::atomic<size_t> next_slice{0};
            (void)RunParallelTasks(hw, [&](size_t) {
              while (true) {
                const size_t si =
                    next_slice.fetch_add(1, std::memory_order_relaxed);
                if (si >= slices.size()) {
                  break;
                }
                run_slice_worker(si, si + 1);
              }
            });
            return;
          }
        }
        run_slice_worker(0, slices.size());
        return;
      }
      const auto hash_right_row = [&](size_t r) {
        Value v_tmp;
        uint64_t hash = 0x9e3779b97f4a7c15ULL;
        for (const size_t column : col_indices) {
          const auto& value =
              read_right_cell(static_cast<uint32_t>(r), column, &v_tmp);
          mix_step(hash, fast_hash_val(value));
        }
        return hash;
      };
      if (right_row_count >= 16384) {
        const size_t hw = ParallelWorkerCount(right_row_count / 4096);
        (void)RunParallelTasks(hw, [&](size_t worker) {
          const size_t b = (right_row_count * worker) / hw;
          const size_t e = (right_row_count * (worker + 1)) / hw;
          for (size_t r = b; r < e; ++r) {
            hashes[r] = hash_right_row(r);
          }
        });
      } else {
        for (size_t r = 0; r < right_row_count; ++r) {
          hashes[r] = hash_right_row(r);
        }
      }
    };
    const auto eq_right = [&](size_t a, size_t b) {
      if (right_col_only && right_arg.columnar != nullptr) {
        const auto [ca, ra] = right_locator.Locate(static_cast<uint32_t>(a));
        const auto [cb, rb] = right_locator.Locate(static_cast<uint32_t>(b));
        const auto& cols_a = right_arg.columnar->chunks[ca].columns;
        const auto& cols_b = right_arg.columnar->chunks[cb].columns;
        Value x_tmp;
        Value y_tmp;
        for (const size_t column : col_indices) {
          const ColumnarColumn& sca = cols_a[column];
          const ColumnarColumn& scb = cols_b[column];
          if (sca.is_uniform && scb.is_uniform &&
              sca.encoding != ColumnarColumn::Encoding::kGathered &&
              scb.encoding != ColumnarColumn::Encoding::kGathered) {
            const bool na = sca.NonGatheredIsNull(ra);
            const bool nb = scb.NonGatheredIsNull(rb);
            if (na != nb) {
              return false;
            }
            if (na) {
              continue;
            }
            if (sca.type == ValueType::kVarChar &&
                scb.type == ValueType::kVarChar && !sca.is_interval &&
                !scb.is_interval) {
              if (sca.StringAt(ra) != scb.StringAt(rb)) {
                return false;
              }
              continue;
            }
            if ((sca.type == ValueType::kInt64 ||
                 sca.type == ValueType::kDate) &&
                sca.type == scb.type && sca.is_unsigned == scb.is_unsigned) {
              if (sca.IntAt(ra) != scb.IntAt(rb)) {
                return false;
              }
              continue;
            }
          }
          sca.WriteValueInto(ra, &x_tmp);
          scb.WriteValueInto(rb, &y_tmp);
          const bool eq =
              x_tmp.IsCaseInsensitive() || y_tmp.IsCaseInsensitive()
                  ? DistinctValueEqual{}(CanonicalDistinctValue(x_tmp),
                                         CanonicalDistinctValue(y_tmp))
                  : DistinctValueEqual{}(x_tmp, y_tmp);
          if (!eq) {
            return false;
          }
        }
        return true;
      }
      Value x_tmp;
      Value y_tmp;
      return std::ranges::all_of(col_indices, [&](size_t column) {
        const auto& x =
            read_right_cell(static_cast<uint32_t>(a), column, &x_tmp);
        const auto& y =
            read_right_cell(static_cast<uint32_t>(b), column, &y_tmp);
        return x.IsCaseInsensitive() || y.IsCaseInsensitive()
                   ? DistinctValueEqual{}(CanonicalDistinctValue(x),
                                          CanonicalDistinctValue(y))
                   : DistinctValueEqual{}(x, y);
      });
    };
    std::vector<uint32_t> rep_idxs;
    if (!build_flat_subgid_dict(
            right_row_count, std::numeric_limits<size_t>::max(),
            fill_right_hashes, eq_right, &split_right_subgid, out_card, nullptr,
            &rep_idxs)) {
      clear_split_right_dict();
      return false;
    }
    if (right_dict_guard.active && shared_pk_cache != nullptr) {
      auto cached =
          std::make_shared<SharedBaseRelationCache::CachedSplitRightDict>();
      cached->subgid_count = *out_card;
      cached->subgid = std::move(split_right_subgid);
      cached->rep_idx = std::move(rep_idxs);
      std::shared_ptr<const SharedBaseRelationCache::CachedSplitRightDict>
          shared_rd = std::move(cached);
      shared_pk_cache->PublishSplitRightDict(right_dict_key, shared_rd);
      right_dict_guard.active = false;
      shared_right_dict = std::move(shared_rd);
      split_right_subgid_ptr = shared_right_dict->subgid.data();
      split_right_rep_ptr = shared_right_dict->rep_idx.data();
      split_right_rep_count = shared_right_dict->rep_idx.size();
    } else {
      split_right_rep_idx = std::move(rep_idxs);
      split_right_subgid_ptr = split_right_subgid.data();
      split_right_rep_ptr = split_right_rep_idx.data();
      split_right_rep_count = split_right_rep_idx.size();
    }
    return true;
  };
  if (!int_group_key && !left.HasSpill() && !right_ptr->HasSpill() &&
      key_programs.size() == 2 && right_row_count > 0 &&
      right_row_count <= 262144) {
    for (size_t rk = 0; rk < 2; ++rk) {
      const size_t lk = 1 - rk;
      if (std::cmp_less(key_shapes[rk].direct_column, left_width)) {
        continue;
      }
      const bool left_is_extract =
          key_shapes[lk].extract_date_col >= 0 &&
          std::cmp_less(key_shapes[lk].extract_date_col, left_width);
      bool left_is_int_col = false;
      if (key_shapes[lk].direct_column >= 0 &&
          std::cmp_less(key_shapes[lk].direct_column, left_width)) {
        const Column& lcol = left.schema.GetColumn(
            static_cast<size_t>(key_shapes[lk].direct_column));
        left_is_int_col = (lcol.Type() == ValueType::kInt64 ||
                           lcol.Type() == ValueType::kDate) &&
                          !lcol.IsUnsigned();
      }
      if (!left_is_extract && !left_is_int_col) {
        continue;
      }
      const auto rcol = static_cast<slot_t>(
          static_cast<size_t>(key_shapes[rk].direct_column) - left_width);
      if (rcol >= right.schema.ColumnCount()) {
        continue;
      }
      const Column& rcol_def = right.schema.GetColumn(rcol);
      Value rv_tmp;
      if (rcol_def.Type() == ValueType::kVarChar) {
        split_right_subgid.assign(right_row_count,
                                  std::numeric_limits<uint32_t>::max());
        // Columnar reads may use rv_tmp's owned short-string buffer.
        // Keep dictionary keys and the consecutive-key shortcut independent
        // of that scratch value.
        std::unordered_map<std::string, uint32_t> sv_to_gid;
        sv_to_gid.reserve(64);
        std::vector<uint32_t> local_rep_idxs;
        local_rep_idxs.reserve(64);
        std::string_view last_sv;
        uint32_t last_gid = std::numeric_limits<uint32_t>::max();
        bool all_ok = true;
        for (size_t ri = 0; ri < right_row_count; ++ri) {
          const Value& rv =
              read_right_cell(static_cast<uint32_t>(ri), rcol, &rv_tmp);
          if (rv.IsNull() || rv.type != ValueType::kVarChar ||
              rv.IsCaseInsensitive()) {
            all_ok = false;
            break;
          }
          const std::string_view sv = rv.value.varchar_value;
          if (last_gid != std::numeric_limits<uint32_t>::max() &&
              sv == last_sv) {
            split_right_subgid[ri] = last_gid;
            continue;
          }
          if (!sv.empty() && ((sv[0] >= '0' && sv[0] <= '9') || sv[0] == '-' ||
                              sv[0] == '+' || sv[0] == ' ')) {
            all_ok = false;
            break;
          }
          const auto next_id = static_cast<uint32_t>(sv_to_gid.size());
          const auto [it, ins] = sv_to_gid.emplace(std::string(sv), next_id);
          if (sv_to_gid.size() > 65535) {
            all_ok = false;
            break;
          }
          if (ins) {
            local_rep_idxs.push_back(static_cast<uint32_t>(ri));
          }
          last_sv = it->first;
          last_gid = it->second;
          split_right_subgid[ri] = last_gid;
        }
        if (all_ok) {
          use_split_key_gid = true;
          split_has_left_probe = true;
          split_uses_right_subgid = true;
          split_right_shift = 32;
          split_left_k = lk;
          split_right_subgid_count = sv_to_gid.size();
          split_right_rep_idx = std::move(local_rep_idxs);
          split_right_subgid_ptr = split_right_subgid.data();
          split_right_rep_ptr = split_right_rep_idx.data();
          split_right_rep_count = split_right_rep_idx.size();
          break;
        }
        clear_split_right_dict();
      } else if ((rcol_def.Type() == ValueType::kInt64 ||
                  rcol_def.Type() == ValueType::kDate) &&
                 !rcol_def.IsUnsigned()) {
        split_right_subgid.assign(right_row_count,
                                  std::numeric_limits<uint32_t>::max());
        FlatIntKeyMap<uint32_t> int_to_gid;
        int_to_gid.Reserve(256);
        std::vector<uint32_t> local_rep_idxs;
        local_rep_idxs.reserve(256);
        uint32_t next_id = 0;
        bool all_ok = true;
        for (size_t ri = 0; ri < right_row_count; ++ri) {
          const Value& rv =
              read_right_cell(static_cast<uint32_t>(ri), rcol, &rv_tmp);
          if (rv.IsNull() || rv.IsUnsigned()) {
            all_ok = false;
            break;
          }
          bool ins = false;
          uint32_t* slot = int_to_gid.TryEmplace(rv.value.int_value, &ins);
          if (ins) {
            if (next_id >= 65535) {
              all_ok = false;
              break;
            }
            *slot = next_id++;
            local_rep_idxs.push_back(static_cast<uint32_t>(ri));
          }
          split_right_subgid[ri] = *slot;
        }
        if (all_ok) {
          use_split_key_gid = true;
          split_has_left_probe = true;
          split_uses_right_subgid = true;
          split_right_shift = 32;
          split_left_k = lk;
          split_right_subgid_count = next_id;
          split_right_rep_idx = std::move(local_rep_idxs);
          split_right_subgid_ptr = split_right_subgid.data();
          split_right_rep_ptr = split_right_rep_idx.data();
          split_right_rep_count = split_right_rep_idx.size();
          break;
        }
        clear_split_right_dict();
      }
    }
  }
  std::shared_ptr<QueryMemoryCharge> split_dictionary_charge;
  if (!use_split_key_gid && !left.HasSpill() && !right_ptr->HasSpill() &&
      !int_group_key && !key_programs.empty() && key_programs.size() <= 16) {
    // Factor finite right and deferred-PK dimension tuples once per query so
    // every surviving probe row probes one exact packed 64-bit integer key.
    std::vector<size_t> dimension_columns;
    std::vector<std::vector<size_t>> pk_group_key_lookups(def_pk_groups.size());
    std::optional<size_t> probe_key;
    size_t left_key_count = 0;
    for (const auto& shape : key_shapes) {
      if ((shape.direct_column >= 0 &&
           std::cmp_less(shape.direct_column, left_width)) ||
          (shape.extract_date_col >= 0 &&
           std::cmp_less(shape.extract_date_col, left_width))) {
        ++left_key_count;
      }
    }
    const bool right_available =
        right_row_count > 0 && right_row_count <= 262144;
    bool has_any_right_key = false;
    bool supported = true;
    for (size_t k = 0; k < key_shapes.size(); ++k) {
      const auto& shape = key_shapes[k];
      if (shape.direct_column >= 0 &&
          std::cmp_greater_equal(shape.direct_column, left_width)) {
        if (!right_available) {
          supported = false;
          break;
        }
        dimension_columns.push_back(static_cast<size_t>(shape.direct_column) -
                                    left_width);
        has_any_right_key = true;
        continue;
      }
      if (shape.direct_column >= 0 &&
          std::cmp_less(shape.direct_column, left_width)) {
        const auto lcol = static_cast<size_t>(shape.direct_column);
        const Column& column = left.schema.GetColumn(lcol);
        const bool integer_probe =
            !column.IsUnsigned() && (column.Type() == ValueType::kInt64 ||
                                     column.Type() == ValueType::kDate);
        const int li = deferred_left_probe != nullptr && lcol < 16
                           ? static_cast<int>(def_col_to_pk_lookup[lcol])
                           : -1;
        const bool pk_eligible =
            li >= 0 &&
            def_pk_groups[prepared_pk_lookups[static_cast<size_t>(li)]
                              .group_idx]
                    .total_rows <= 262144;
        // Keep a sole INT64/DATE left key paired with right keys in the low 32
        // bits so split_key_unpack_only still avoids cloning left rep rows.
        const bool prefer_probe_slot = integer_probe && !probe_key &&
                                       left_key_count == 1 && has_any_right_key;
        if (pk_eligible && !prefer_probe_slot) {
          const auto uli = static_cast<size_t>(li);
          pk_group_key_lookups[prepared_pk_lookups[uli].group_idx].push_back(
              uli);
          continue;
        }
        if (li < 0 && right_available && join_type == JoinType::kInner &&
            integer_probe) {
          const auto eq_it =
              std::ranges::find_if(equality_keys, [&](const EqualityKey& eq) {
                if (eq.left != lcol || eq.left_offset != 0 ||
                    eq.right_offset != 0 || eq.null_safe ||
                    eq.right >= right.schema.ColumnCount()) {
                  return false;
                }
                const Column& rc = right.schema.GetColumn(eq.right);
                return rc.Type() == column.Type() && !rc.IsUnsigned();
              });
          if (eq_it != equality_keys.end()) {
            if (std::ranges::find(dimension_columns, eq_it->right) ==
                dimension_columns.end()) {
              dimension_columns.push_back(eq_it->right);
            }
            has_any_right_key = true;
            continue;
          }
        }
        if (!probe_key && integer_probe) {
          probe_key = k;
          continue;
        }
        supported = false;
        break;
      }
      if (shape.extract_date_col >= 0 &&
          std::cmp_less(shape.extract_date_col, left_width) && !probe_key) {
        probe_key = k;
        continue;
      }
      supported = false;
      break;
    }
    const bool any_pk_dict = std::ranges::any_of(
        pk_group_key_lookups,
        [](const std::vector<size_t>& v) { return !v.empty(); });
    if (supported && (!dimension_columns.empty() || any_pk_dict)) {
      size_t charge_bytes = 0;
      if (!dimension_columns.empty()) {
        charge_bytes +=
            (right_col_only ? 0 : right_ptr->charged_bytes_) +
            (right_row_count * (64 + dimension_columns.size() * sizeof(Value)));
      }
      for (size_t gi = 0; gi < def_pk_groups.size(); ++gi) {
        if (!pk_group_key_lookups[gi].empty()) {
          charge_bytes += static_cast<size_t>(def_pk_groups[gi].total_rows) *
                          (32 + pk_group_key_lookups[gi].size() * 8);
        }
      }
      split_dictionary_charge = TryReserveQueryMemory(charge_bytes);
      if (split_dictionary_charge) {
        bool dict_ok = true;
        uint32_t right_card = 0;
        if (!dimension_columns.empty()) {
          dict_ok =
              build_or_reuse_split_right_dict(dimension_columns, &right_card);
        }
        std::vector<std::vector<std::pair<uint32_t, uint32_t>>> pk_locs(
            def_pk_groups.size());
        for (size_t gi = 0; dict_ok && gi < def_pk_groups.size(); ++gi) {
          if (pk_group_key_lookups[gi].empty()) {
            continue;
          }
          PreparedDeferredPkGroup& grp = def_pk_groups[gi];
          const auto& lkp_idxs = pk_group_key_lookups[gi];
          const size_t nb_count = grp.blocks.size();
          std::vector<const RuntimeFilterGroup*> pk_rf_groups;
          if (deferred_left_probe != nullptr &&
              grp.pk_col < left.schema.ColumnCount() &&
              !left.schema.GetColumn(grp.pk_col).IsUnsigned()) {
            const ValueType col_type = left.schema.GetColumn(grp.pk_col).Type();
            const ScanRuntimeFilter& prf = deferred_left_probe->runtime_filter;
            if (prf.enabled() && prf.column == grp.pk_col &&
                prf.type == col_type) {
              pk_rf_groups.push_back(prf.group.get());
            }
            for (const ScanRuntimeFilter& ext : prf.extra) {
              if (ext.enabled() && ext.column == grp.pk_col &&
                  ext.type == col_type) {
                pk_rf_groups.push_back(ext.group.get());
              }
            }
          }
          std::string pk_dict_key;
          struct PkDictSlotGuard {
            SharedBaseRelationCache* cache{nullptr};
            std::string key;
            bool active{false};
            PkDictSlotGuard() = default;
            PkDictSlotGuard(SharedBaseRelationCache* c, std::string k, bool a)
                : cache(c), key(std::move(k)), active(a) {}
            PkDictSlotGuard(const PkDictSlotGuard&) = delete;
            PkDictSlotGuard& operator=(const PkDictSlotGuard&) = delete;
            PkDictSlotGuard(PkDictSlotGuard&&) = delete;
            PkDictSlotGuard& operator=(PkDictSlotGuard&&) = delete;
            ~PkDictSlotGuard() {
              if (active && cache != nullptr) {
                cache->AbandonPkDict(key);
              }
            }
          } pk_dict_guard(shared_pk_cache, {}, false);
          if (pk_rf_groups.empty() && shared_pk_cache != nullptr &&
              grp.store != nullptr) {
            pk_dict_key =
                "P:" + std::to_string(reinterpret_cast<uintptr_t>(grp.store)) +
                ":" + std::to_string(grp.store->layout_generation) + ":" +
                std::to_string(grp.store_pk_col) + ":" +
                std::to_string(grp.total_rows);
            for (const size_t li : lkp_idxs) {
              const auto& prep = prepared_pk_lookups[li];
              pk_dict_key +=
                  ":" + std::to_string(prep.store_payload_col) + "," +
                  std::to_string(static_cast<uint32_t>(prep.payload_type)) +
                  "," + (prep.payload_unsigned ? "1" : "0");
            }
            const bool want_dense = grp.dense_ptr != nullptr;
            pk_dict_key += ":" + std::to_string(want_dense ? 1 : 0);
            if (want_dense) {
              pk_dict_key += ":" + std::to_string(grp.min_k) + ":" +
                             std::to_string(grp.max_k) + ":" +
                             std::to_string(grp.dense_size);
            }
            std::shared_ptr<const SharedBaseRelationCache::CachedPkDict> hit;
            if (!shared_pk_cache->AcquirePkDictSlot(pk_dict_key, &hit)) {
              if (hit != nullptr) {
                grp.shared_pk_dict = std::move(hit);
                grp.use_subgid = true;
                grp.subgid_count = grp.shared_pk_dict->subgid_count;
                grp.subgid_ptr = grp.shared_pk_dict->subgid.data();
                grp.subgid_rep_ptr = grp.shared_pk_dict->subgid_rep_loc.data();
                if (grp.shared_pk_dict->has_dense) {
                  grp.dense_ptr = grp.shared_pk_dict->remapped_dense.data();
                  grp.dense_size = grp.shared_pk_dict->remapped_dense.size();
                }
                continue;
              }
            } else {
              pk_dict_guard.key = pk_dict_key;
              pk_dict_guard.active = true;
            }
          }
          pk_locs[gi].resize(grp.total_rows);
          std::vector<uint8_t> pk_active;
          if (!pk_rf_groups.empty()) {
            pk_active.assign(grp.total_rows, 1);
          }
          const auto fill_pk_hashes = [&](std::vector<uint64_t>& hashes) {
            const auto fill_blk = [&](uint32_t nb, Value& tmp) {
              const auto& gb = grp.blocks[nb];
              const uint32_t base = gb.row_base;
              for (uint32_t rin = 0; rin < gb.row_count; ++rin) {
                pk_locs[gi][base + rin] = {nb, rin};
                if (!pk_active.empty()) {
                  const int64_t pk_key = gb.pk_cells[rin];
                  bool rejected = false;
                  for (const RuntimeFilterGroup* rf_g : pk_rf_groups) {
                    if (rf_g->Reject(pk_key)) {
                      rejected = true;
                      break;
                    }
                  }
                  if (rejected) {
                    pk_active[base + rin] = 0;
                    continue;
                  }
                }
                uint64_t hash = 0x9e3779b97f4a7c15ULL;
                for (const size_t li : lkp_idxs) {
                  read_pk_cell(li, nb, rin, &tmp);
                  hash = std::rotl(hash, 7);
                  hash ^= fast_hash_val(tmp);
                  hash *= 0x100000001b3ULL;
                }
                hashes[base + rin] = hash;
              }
            };
            const size_t pk_workers = std::min<size_t>(
                ParallelWorkerCount(grp.total_rows / 16384), nb_count);
            if (pk_workers > 1 && grp.total_rows >= 16384 && nb_count >= 2) {
              std::atomic<uint32_t> next_nb{0};
              const auto total_nb = static_cast<uint32_t>(nb_count);
              (void)RunParallelTasks(pk_workers, [&](size_t) {
                Value tmp;
                uint32_t nb = 0;
                while ((nb = next_nb.fetch_add(1, std::memory_order_relaxed)) <
                       total_nb) {
                  fill_blk(nb, tmp);
                }
              });
            } else {
              Value tmp;
              for (uint32_t nb = 0; nb < nb_count; ++nb) {
                fill_blk(nb, tmp);
              }
            }
          };
          const auto eq_pk = [&](size_t a, size_t b) {
            const auto [nba, ra] = pk_locs[gi][a];
            const auto [nbb, rb] = pk_locs[gi][b];
            Value va;
            Value vb;
            for (const size_t li : lkp_idxs) {
              read_pk_cell(li, nba, ra, &va);
              read_pk_cell(li, nbb, rb, &vb);
              if (!DistinctValueEqual{}(va, vb)) {
                return false;
              }
            }
            return true;
          };
          std::vector<uint32_t> pk_rep_rows;
          dict_ok = build_flat_subgid_dict(
              grp.total_rows, std::numeric_limits<size_t>::max(),
              fill_pk_hashes, eq_pk, &grp.subgid, &grp.subgid_count,
              pk_active.empty() ? nullptr : &pk_active, &pk_rep_rows);
          if (dict_ok) {
            grp.use_subgid = true;
            grp.subgid_rep_loc.resize(pk_rep_rows.size());
            for (size_t id = 0; id < pk_rep_rows.size(); ++id) {
              grp.subgid_rep_loc[id] = pk_locs[gi][pk_rep_rows[id]];
            }
            if (pk_dict_guard.active && shared_pk_cache != nullptr) {
              auto cached =
                  std::make_shared<SharedBaseRelationCache::CachedPkDict>();
              cached->store_owner = grp.store_owner;
              cached->subgid_count = grp.subgid_count;
              cached->subgid = std::move(grp.subgid);
              cached->subgid_rep_loc = std::move(grp.subgid_rep_loc);
              if (grp.dense_ptr != nullptr && grp.dense_size > 0) {
                cached->has_dense = true;
                cached->min_k = grp.min_k;
                cached->max_k = grp.max_k;
                cached->span = grp.span;
                cached->remapped_dense.assign(grp.dense_ptr,
                                              grp.dense_ptr + grp.dense_size);
                for (auto& entry : cached->remapped_dense) {
                  if (entry.nb != std::numeric_limits<uint32_t>::max()) {
                    entry.subgid = cached->subgid[entry.subgid];
                  }
                }
              }
              std::shared_ptr<const SharedBaseRelationCache::CachedPkDict>
                  shared_pd = std::move(cached);
              shared_pk_cache->PublishPkDict(pk_dict_key, shared_pd);
              pk_dict_guard.active = false;
              grp.shared_pk_dict = std::move(shared_pd);
              grp.subgid_ptr = grp.shared_pk_dict->subgid.data();
              grp.subgid_rep_ptr = grp.shared_pk_dict->subgid_rep_loc.data();
              if (grp.shared_pk_dict->has_dense) {
                grp.dense_ptr = grp.shared_pk_dict->remapped_dense.data();
                grp.dense_size = grp.shared_pk_dict->remapped_dense.size();
              }
            } else {
              grp.subgid_ptr = grp.subgid.data();
              grp.subgid_rep_ptr = grp.subgid_rep_loc.data();
            }
          }
        }
        uint32_t bit_cursor = probe_key.has_value() ? 32U : 0U;
        if (dict_ok && !dimension_columns.empty()) {
          const uint32_t r_bits =
              right_card <= 1
                  ? 0U
                  : static_cast<uint32_t>(std::bit_width(right_card - 1));
          if (bit_cursor + r_bits <= 64U) {
            split_uses_right_subgid = true;
            split_right_shift = static_cast<uint8_t>(bit_cursor);
            split_right_subgid_count = right_card;
            bit_cursor += r_bits;
          } else {
            dict_ok = false;
          }
        }
        for (size_t gi = 0; dict_ok && gi < def_pk_groups.size(); ++gi) {
          if (!def_pk_groups[gi].use_subgid) {
            continue;
          }
          const uint32_t p_card = def_pk_groups[gi].subgid_count;
          const uint32_t p_bits =
              p_card <= 1 ? 0U
                          : static_cast<uint32_t>(std::bit_width(p_card - 1));
          if (bit_cursor + p_bits <= 64U) {
            def_pk_groups[gi].subgid_shift = static_cast<uint8_t>(bit_cursor);
            split_uses_pk_subgid = true;
            bit_cursor += p_bits;
          } else {
            dict_ok = false;
          }
        }
        if (dict_ok && bit_cursor <= 64U) {
          use_split_key_gid = true;
          split_has_left_probe = probe_key.has_value();
          split_left_k = probe_key.value_or(key_shapes.size());
          for (PreparedDeferredPkGroup& grp : def_pk_groups) {
            if (!grp.use_subgid || grp.dense_ptr == nullptr ||
                grp.shared_pk_dict != nullptr) {
              continue;
            }
            if (grp.shared_dense != nullptr) {
              grp.dense_index = *grp.shared_dense;
              grp.shared_dense.reset();
              grp.dense_ptr = grp.dense_index.data();
            }
            for (auto& entry : grp.dense_index) {
              if (entry.nb != std::numeric_limits<uint32_t>::max()) {
                entry.subgid = grp.subgid_ptr[entry.subgid];
              }
            }
          }
        } else {
          clear_split_right_dict();
          split_uses_right_subgid = false;
          split_uses_pk_subgid = false;
          for (auto& grp : def_pk_groups) {
            grp.subgid.clear();
            grp.subgid_rep_loc.clear();
            grp.shared_pk_dict.reset();
            grp.subgid_ptr = nullptr;
            grp.subgid_rep_ptr = nullptr;
            grp.subgid_count = 0;
            grp.use_subgid = false;
            if (grp.shared_dense != nullptr) {
              grp.dense_ptr = grp.shared_dense->data();
              grp.dense_size = grp.shared_dense->size();
            } else {
              grp.dense_ptr =
                  grp.dense_index.empty() ? nullptr : grp.dense_index.data();
              grp.dense_size = grp.dense_index.size();
            }
          }
          split_dictionary_charge.reset();
        }
      }
    }
  }
  const bool use_fast_2key_year_lut =
      use_split_key_gid && split_has_left_probe && !split_uses_pk_subgid &&
      split_uses_right_subgid && split_right_subgid_count <= 64 &&
      key_shapes[split_left_k].extract_date_col >= 0 &&
      key_shapes[split_left_k].extract_date_part == 0;
  bool packed_2x32_group_key = false;
  slot_t packed_2x32_slot0 = 0;
  slot_t packed_2x32_slot1 = 0;
  ValueType packed_2x32_type0 = ValueType::kNull;
  ValueType packed_2x32_type1 = ValueType::kNull;
  if (!int_group_key && !use_split_key_gid && !left.HasSpill() &&
      !right_ptr->HasSpill() && key_programs.size() == 2 &&
      key_shapes[0].direct_column >= 0 && key_shapes[1].direct_column >= 0) {
    const auto s0 = static_cast<slot_t>(key_shapes[0].direct_column);
    const auto s1 = static_cast<slot_t>(key_shapes[1].direct_column);
    const Column& c0 = result.schema.GetColumn(s0);
    const Column& c1 = result.schema.GetColumn(s1);
    const auto is_packable_type = [](const Column& col) {
      return !col.IsUnsigned() && (col.Type() == ValueType::kInt64 ||
                                   col.Type() == ValueType::kDate);
    };
    if (is_packable_type(c0) && is_packable_type(c1)) {
      packed_2x32_group_key = true;
      packed_2x32_slot0 = s0;
      packed_2x32_slot1 = s1;
      packed_2x32_type0 = c0.Type();
      packed_2x32_type1 = c1.Type();
    }
  }
  bool packed_2x32_unpack_only =
      packed_2x32_group_key && result.using_columns == nullptr &&
      !fusion->having && !fusion->post_having_active &&
      fusion->topn_limit == 0 && !fusion->rollup_count_distribution;
  if (packed_2x32_unpack_only) {
    for (const Expression& item : fusion->select_items) {
      if (!item) {
        packed_2x32_unpack_only = false;
        break;
      }
      if (item->Type() == TypeTag::kConstantValue &&
          !ContainsParameterSlot(item)) {
        continue;
      }
      if (item->Type() == TypeTag::kAggregateExp) {
        if (std::ranges::find(fusion->aggregates,
                              &item->AsAggregateExpression()) ==
            fusion->aggregates.end()) {
          packed_2x32_unpack_only = false;
          break;
        }
        continue;
      }
      if (item->Type() == TypeTag::kColumnValue) {
        const auto col_off = LocalColumnOffset(
            result.schema, item->AsColumnValue().GetColumnName());
        if (col_off.has_value() &&
            (*col_off == packed_2x32_slot0 || *col_off == packed_2x32_slot1)) {
          continue;
        }
      }
      packed_2x32_unpack_only = false;
      break;
    }
  }
  std::optional<slot_t> split_left_slot;
  ValueType split_left_type = ValueType::kNull;
  std::optional<int64_t> compact_integer_threshold;
  if (fusion->having && fusion->having->Type() == TypeTag::kBinaryExp) {
    const auto& predicate = fusion->having->AsBinaryExpression();
    if (predicate.Op() == BinaryOperation::kGreaterThan && predicate.Left() &&
        predicate.Left()->Type() == TypeTag::kAggregateExp &&
        predicate.Right() &&
        predicate.Right()->Type() == TypeTag::kConstantValue &&
        !ContainsParameterSlot(predicate.Right()) &&
        std::ranges::find(fusion->aggregates,
                          &predicate.Left()->AsAggregateExpression()) !=
            fusion->aggregates.end()) {
      const auto& threshold = predicate.Right()->AsConstantValue().GetValue();
      if (threshold.type == ValueType::kInt64 && !threshold.IsUnsigned() &&
          threshold.value.int_value >= 1) {
        compact_integer_threshold = threshold.value.int_value;
      }
    }
  }
  const bool compact_having_count_gt =
      raw_count_fusion && count_star_only &&
      (!fusion->having || compact_integer_threshold.has_value()) &&
      (!fusion->post_having_active ||
       (!fusion->post_having_is_self_sum_factor &&
        fusion->post_having_agg_slot == 0 &&
        fusion->post_having_op == BinaryOperation::kGreaterThan &&
        fusion->post_having_constant_or_factor >= 1.0)) &&
      (compact_integer_threshold.has_value() || fusion->post_having_active);
  bool split_key_unpack_only =
      use_split_key_gid && (split_uses_right_subgid || split_uses_pk_subgid) &&
      (split_has_left_probe || join_type == JoinType::kInner) &&
      result.using_columns == nullptr &&
      ((!fusion->having && !fusion->post_having_active) ||
       compact_having_count_gt) &&
      fusion->topn_limit == 0 && !fusion->rollup_count_distribution &&
      (raw_sum_fusion || raw_count_fusion || fusion->aggregates.empty());
  if (split_key_unpack_only && split_has_left_probe) {
    if (key_shapes[split_left_k].direct_column >= 0 &&
        std::cmp_less(key_shapes[split_left_k].direct_column, left_width)) {
      const auto slot =
          static_cast<slot_t>(key_shapes[split_left_k].direct_column);
      const Column& lcol = left.schema.GetColumn(slot);
      if (!lcol.IsUnsigned() && (lcol.Type() == ValueType::kInt64 ||
                                 lcol.Type() == ValueType::kDate)) {
        split_left_slot = slot;
        split_left_type = lcol.Type();
      } else {
        split_key_unpack_only = false;
      }
    } else if (key_shapes[split_left_k].extract_date_col < 0 ||
               std::cmp_greater_equal(key_shapes[split_left_k].extract_date_col,
                                      left_width)) {
      split_key_unpack_only = false;
    }
  }
  if (split_key_unpack_only) {
    auto col_unpackable = [&](size_t col_off) -> bool {
      if ((col_off >= left_width &&
           (split_uses_right_subgid || singleton_right_relation)) ||
          (split_has_left_probe && split_left_slot.has_value() &&
           col_off == *split_left_slot)) {
        return true;
      }
      if (col_off < 16 && def_col_to_pk_lookup[col_off] >= 0 &&
          def_pk_groups[prepared_pk_lookups[static_cast<size_t>(
                                                def_col_to_pk_lookup[col_off])]
                            .group_idx]
              .use_subgid) {
        return true;
      }
      if (join_type == JoinType::kInner &&
          std::ranges::any_of(equality_keys, [&](const EqualityKey& eq) {
            if (eq.left != col_off || eq.left_offset != 0 ||
                eq.right_offset != 0 || eq.null_safe ||
                eq.right >= right.schema.ColumnCount()) {
              return false;
            }
            const Column& lc = left.schema.GetColumn(eq.left);
            const Column& rc = right.schema.GetColumn(eq.right);
            return lc.Type() == rc.Type() &&
                   lc.IsUnsigned() == rc.IsUnsigned() &&
                   lc.Type() != ValueType::kVarChar;
          })) {
        return true;
      }
      return false;
    };
    const std::function<bool(const Expression&)> expr_unpackable =
        [&](const Expression& expr) -> bool {
      if (!expr || ContainsParameterSlot(expr)) {
        return false;
      }
      if (expr->Type() == TypeTag::kConstantValue) {
        return true;
      }
      if (expr->Type() == TypeTag::kAggregateExp) {
        return std::ranges::find(fusion->aggregates,
                                 &expr->AsAggregateExpression()) !=
               fusion->aggregates.end();
      }
      if (expr->Type() == TypeTag::kColumnValue) {
        const auto col_off = LocalColumnOffset(
            result.schema, expr->AsColumnValue().GetColumnName());
        return col_off.has_value() && col_unpackable(*col_off);
      }
      if (expr->Type() == TypeTag::kUnaryExp) {
        return expr_unpackable(expr->AsUnaryExpression().Child());
      }
      if (expr->Type() == TypeTag::kBinaryExp) {
        const auto& bin = expr->AsBinaryExpression();
        return expr_unpackable(bin.Left()) && expr_unpackable(bin.Right());
      }
      return false;
    };
    if (!std::ranges::all_of(fusion->select_items, expr_unpackable)) {
      split_key_unpack_only = false;
    }
  }
  struct LeftPackedIntCol {
    slot_t col{0};
    ValueType type{ValueType::kNull};
    int64_t min_val{0};
    uint64_t null_code{0};
    uint64_t mask{0};
    uint8_t shift{0};
  };
  bool use_left_packed_key = false;
  bool left_pack_unpack_only = false;
  bool left_pack_uses_right = false;
  bool left_pack_uses_dict = false;
  uint64_t left_pack_right_mask = 0;
  uint64_t left_pack_dict_mask = 0;
  uint8_t left_pack_dict_shift = 0;
  std::vector<uint32_t> left_pack_dict_subgid;
  std::vector<size_t> left_pack_dict_cols;
  std::vector<size_t> left_pack_right_cols;
  std::vector<uint32_t> left_pack_dict_rep_rows;
  std::vector<std::pair<uint32_t, uint32_t>> left_pack_dict_rep_col_loc;
  std::vector<size_t> left_pack_chunk_offsets;
  std::vector<LeftPackedIntCol> left_pack_ints;
  std::shared_ptr<QueryMemoryCharge> left_pack_charge;
  const bool left_col_only = left.HasColumnarOnly();
  const size_t left_total_rows = left.TotalRows();
  if (!use_split_key_gid && !int_group_key && !packed_2x32_group_key &&
      deferred_left_probe == nullptr && !left.HasSpill() &&
      !right_ptr->HasSpill() && left_total_rows >= 8192 &&
      left_total_rows <= 1'000'000 && all_direct_group_keys &&
      key_programs.size() >= 2 && key_programs.size() <= 8 &&
      !(raw_count_fusion && count_star_only &&
        (fusion->having || fusion->post_having_active))) {
    std::vector<size_t> lp_right_cols;
    std::vector<size_t> lp_dict_cols;
    std::vector<LeftPackedIntCol> lp_ints;
    const bool right_ok = right_row_count > 0 && right_row_count <= 262144;
    bool ok = true;
    for (const auto& shape : key_shapes) {
      if (shape.direct_column < 0) {
        ok = false;
        break;
      }
      const auto col = static_cast<size_t>(shape.direct_column);
      if (col >= left_width) {
        if (!right_ok) {
          ok = false;
          break;
        }
        lp_right_cols.push_back(col - left_width);
      } else {
        const Column& cdef = left.schema.GetColumn(col);
        if (!cdef.IsUnsigned() && (cdef.Type() == ValueType::kInt64 ||
                                   cdef.Type() == ValueType::kDate)) {
          lp_ints.push_back(LeftPackedIntCol{.col = static_cast<slot_t>(col),
                                             .type = cdef.Type()});
        } else if (cdef.Type() == ValueType::kVarChar) {
          lp_dict_cols.push_back(col);
        } else {
          ok = false;
          break;
        }
      }
    }
    std::vector<size_t> lp_col_offsets;
    std::vector<uint32_t> lp_ne_chunks;
    std::vector<size_t> lp_ne_bases;
    const auto locate_left_col = [&](size_t li) -> std::pair<size_t, size_t> {
      const auto it =
          std::upper_bound(lp_ne_bases.begin(), lp_ne_bases.end(), li);
      const auto idx = static_cast<size_t>((it - lp_ne_bases.begin()) - 1);
      return {lp_ne_chunks[idx], li - lp_ne_bases[idx]};
    };
    if (ok && left_col_only) {
      const auto& chunks = left.columnar->chunks;
      lp_col_offsets.resize(chunks.size(), 0);
      lp_ne_chunks.reserve(chunks.size());
      lp_ne_bases.reserve(chunks.size());
      size_t base = 0;
      for (size_t ci = 0; ci < chunks.size(); ++ci) {
        lp_col_offsets[ci] = base;
        if (chunks[ci].rows > 0) {
          lp_ne_chunks.push_back(static_cast<uint32_t>(ci));
          lp_ne_bases.push_back(base);
          base += chunks[ci].rows;
        }
      }
    }
    if (ok && !lp_dict_cols.empty()) {
      constexpr size_t kSample = 256;
      std::unordered_set<uint64_t> seen_hashes;
      seen_hashes.reserve(256);
      Value sample_tmp;
      for (size_t s = 0; s < kSample; ++s) {
        const size_t ri = (s * left_total_rows) / kSample;
        uint64_t h = 0x9e3779b97f4a7c15ULL;
        if (left_col_only) {
          const auto [ci, r] = locate_left_col(ri);
          const ColumnarChunk& ch = left.columnar->chunks[ci];
          for (const size_t dc : lp_dict_cols) {
            ch.columns[dc].WriteValueInto(r, &sample_tmp);
            h = std::rotl(h, 7) ^ fast_hash_val(sample_tmp);
            h *= 0x100000001b3ULL;
          }
        } else {
          for (const size_t dc : lp_dict_cols) {
            const Value& v = left.rows[ri].values_[dc];
            h = std::rotl(h, 7) ^ std::hash<Value>{}(v);
            h *= 0x100000001b3ULL;
          }
        }
        seen_hashes.insert(h);
        if (seen_hashes.size() > 128) {
          ok = false;
          break;
        }
      }
    }
    if (ok && (!lp_dict_cols.empty() || !lp_right_cols.empty() ||
               lp_ints.size() > 2)) {
      const size_t charge_bytes =
          (left_total_rows * (lp_dict_cols.empty() ? 1 : 20)) +
          (lp_right_cols.empty() ? 0 : right_row_count * 16);
      left_pack_charge = TryReserveQueryMemory(charge_bytes);
      if (left_pack_charge) {
        std::vector<uint64_t> left_hashes;
        if (!lp_dict_cols.empty()) {
          left_hashes.resize(left_total_rows);
        }
        const size_t scan_workers =
            ParallelWorkerCount(left_total_rows / 32768);
        struct WorkerRange {
          std::array<int64_t, 8> min_v{};
          std::array<int64_t, 8> max_v{};
          std::array<uint8_t, 8> has_non_null{};
          std::array<uint8_t, 8> has_null{};
          bool valid{true};
        };
        std::vector<WorkerRange> wranges(scan_workers);
        for (auto& wr : wranges) {
          wr.min_v.fill(std::numeric_limits<int64_t>::max());
          wr.max_v.fill(std::numeric_limits<int64_t>::min());
        }
        const size_t n_ints = lp_ints.size();
        (void)RunParallelTasks(scan_workers, [&](size_t worker) {
          WorkerRange& wr = wranges[worker];
          if (left_col_only) {
            const auto& chunks = left.columnar->chunks;
            const size_t cb = (chunks.size() * worker) / scan_workers;
            const size_t ce = (chunks.size() * (worker + 1)) / scan_workers;
            Value tmp;
            for (size_t ci = cb; ci < ce; ++ci) {
              const ColumnarChunk& ch = chunks[ci];
              if (ch.rows == 0) {
                continue;
              }
              for (size_t ji = 0; ji < n_ints; ++ji) {
                const ColumnarColumn& cc = ch.columns[lp_ints[ji].col];
                if (cc.encoding == ColumnarColumn::Encoding::kConstant) {
                  if (cc.IsNull(0)) {
                    wr.has_null[ji] = 1;
                  } else if (cc.is_uniform && !cc.is_unsigned &&
                             cc.type == lp_ints[ji].type) {
                    const int64_t iv = cc.IntAt(0);
                    wr.has_non_null[ji] = 1;
                    wr.min_v[ji] = std::min(wr.min_v[ji], iv);
                    wr.max_v[ji] = std::max(wr.max_v[ji], iv);
                  } else {
                    cc.WriteValueInto(0, &tmp);
                    if (tmp.IsNull()) {
                      wr.has_null[ji] = 1;
                    } else if (tmp.type == lp_ints[ji].type &&
                               !tmp.IsUnsigned()) {
                      const int64_t iv = tmp.value.int_value;
                      wr.has_non_null[ji] = 1;
                      wr.min_v[ji] = std::min(wr.min_v[ji], iv);
                      wr.max_v[ji] = std::max(wr.max_v[ji], iv);
                    } else {
                      wr.valid = false;
                      return;
                    }
                  }
                } else if (cc.is_uniform && !cc.is_unsigned &&
                           cc.type == lp_ints[ji].type) {
                  for (size_t r = 0; r < ch.rows; ++r) {
                    if (cc.IsNull(r)) {
                      wr.has_null[ji] = 1;
                    } else {
                      const int64_t iv = cc.IntAt(r);
                      wr.has_non_null[ji] = 1;
                      wr.min_v[ji] = std::min(wr.min_v[ji], iv);
                      wr.max_v[ji] = std::max(wr.max_v[ji], iv);
                    }
                  }
                } else if (cc.is_uniform && cc.type == ValueType::kNull) {
                  wr.has_null[ji] = 1;
                } else {
                  for (size_t r = 0; r < ch.rows; ++r) {
                    cc.WriteValueInto(r, &tmp);
                    if (tmp.IsNull()) {
                      wr.has_null[ji] = 1;
                    } else if (tmp.type == lp_ints[ji].type &&
                               !tmp.IsUnsigned()) {
                      const int64_t iv = tmp.value.int_value;
                      wr.has_non_null[ji] = 1;
                      wr.min_v[ji] = std::min(wr.min_v[ji], iv);
                      wr.max_v[ji] = std::max(wr.max_v[ji], iv);
                    } else {
                      wr.valid = false;
                      return;
                    }
                  }
                }
              }
              if (!lp_dict_cols.empty()) {
                const size_t base_li = lp_col_offsets[ci];
                for (size_t r = 0; r < ch.rows; ++r) {
                  uint64_t h = 0x9e3779b97f4a7c15ULL;
                  for (const size_t dc : lp_dict_cols) {
                    ch.columns[dc].WriteValueInto(r, &tmp);
                    if (tmp.IsCaseInsensitive()) {
                      wr.valid = false;
                      return;
                    }
                    h = std::rotl(h, 7) ^ fast_hash_val(tmp);
                    h *= 0x100000001b3ULL;
                  }
                  left_hashes[base_li + r] = h;
                }
              }
            }
          } else {
            const size_t b = (left.rows.size() * worker) / scan_workers;
            const size_t e = (left.rows.size() * (worker + 1)) / scan_workers;
            for (size_t ri = b; ri < e; ++ri) {
              const Row& row = left.rows[ri];
              for (size_t ji = 0; ji < n_ints; ++ji) {
                const Value& v = row.values_[lp_ints[ji].col];
                if (v.IsNull()) {
                  wr.has_null[ji] = 1;
                } else if (v.type == lp_ints[ji].type && !v.IsUnsigned()) {
                  const int64_t iv = v.value.int_value;
                  wr.has_non_null[ji] = 1;
                  wr.min_v[ji] = std::min(wr.min_v[ji], iv);
                  wr.max_v[ji] = std::max(wr.max_v[ji], iv);
                } else {
                  wr.valid = false;
                  return;
                }
              }
              if (!lp_dict_cols.empty()) {
                uint64_t h = 0x9e3779b97f4a7c15ULL;
                for (const size_t dc : lp_dict_cols) {
                  const Value& v = row.values_[dc];
                  if (v.IsCaseInsensitive()) {
                    wr.valid = false;
                    return;
                  }
                  h = std::rotl(h, 7) ^ std::hash<Value>{}(v);
                  h *= 0x100000001b3ULL;
                }
                left_hashes[ri] = h;
              }
            }
          }
        });
        for (const WorkerRange& wr : wranges) {
          if (!wr.valid) {
            ok = false;
            break;
          }
        }
        std::array<int64_t, 8> g_min{};
        std::array<int64_t, 8> g_max{};
        std::array<uint8_t, 8> g_non_null{};
        std::array<uint8_t, 8> g_null{};
        g_min.fill(std::numeric_limits<int64_t>::max());
        g_max.fill(std::numeric_limits<int64_t>::min());
        if (ok) {
          for (const WorkerRange& wr : wranges) {
            for (size_t ji = 0; ji < n_ints; ++ji) {
              if (wr.has_non_null[ji] != 0) {
                g_non_null[ji] = 1;
                g_min[ji] = std::min(g_min[ji], wr.min_v[ji]);
                g_max[ji] = std::max(g_max[ji], wr.max_v[ji]);
              }
              g_null[ji] |= wr.has_null[ji];
            }
          }
        }
        uint32_t dict_card = 0;
        std::vector<uint32_t> lp_dict_rep_rows;
        if (ok && !lp_dict_cols.empty()) {
          ok = build_flat_subgid_dict(
              left_total_rows, 1024,
              [&](std::vector<uint64_t>& hashes) {
                hashes = std::move(left_hashes);
              },
              [&](size_t a, size_t b) {
                if (left_col_only) {
                  const auto [cia, ra] = locate_left_col(a);
                  const auto [cib, rb] = locate_left_col(b);
                  const ColumnarChunk& ca = left.columnar->chunks[cia];
                  const ColumnarChunk& cb = left.columnar->chunks[cib];
                  Value va;
                  Value vb;
                  for (const size_t dc : lp_dict_cols) {
                    ca.columns[dc].WriteValueInto(ra, &va);
                    cb.columns[dc].WriteValueInto(rb, &vb);
                    if (!DistinctValueEqual{}(va, vb)) {
                      return false;
                    }
                  }
                  return true;
                }
                const Row& ra = left.rows[a];
                const Row& rb = left.rows[b];
                return std::ranges::all_of(lp_dict_cols, [&](size_t dc) {
                  return DistinctValueEqual{}(ra.values_[dc], rb.values_[dc]);
                });
              },
              &left_pack_dict_subgid, &dict_card, nullptr, &lp_dict_rep_rows);
          if (ok && left_col_only) {
            left_pack_chunk_offsets = std::move(lp_col_offsets);
          }
        }
        uint32_t right_card = 0;
        if (ok && !lp_right_cols.empty()) {
          ok = build_or_reuse_split_right_dict(lp_right_cols, &right_card);
        }
        uint32_t bit_cursor = 0;
        if (ok && !lp_right_cols.empty()) {
          const uint32_t r_bits =
              right_card <= 1
                  ? 0U
                  : static_cast<uint32_t>(std::bit_width(right_card - 1));
          if (bit_cursor + r_bits <= 64U) {
            left_pack_uses_right = true;
            split_right_shift = static_cast<uint8_t>(bit_cursor);
            split_right_subgid_count = right_card;
            left_pack_right_mask =
                r_bits == 0 ? 0ULL : ((uint64_t{1} << r_bits) - 1ULL);
            bit_cursor += r_bits;
          } else {
            ok = false;
          }
        }
        if (ok && !lp_dict_cols.empty()) {
          const uint32_t d_bits =
              dict_card <= 1
                  ? 0U
                  : static_cast<uint32_t>(std::bit_width(dict_card - 1));
          if (bit_cursor + d_bits <= 64U) {
            left_pack_uses_dict = true;
            left_pack_dict_shift = static_cast<uint8_t>(bit_cursor);
            left_pack_dict_mask =
                d_bits == 0 ? 0ULL : ((uint64_t{1} << d_bits) - 1ULL);
            bit_cursor += d_bits;
          } else {
            ok = false;
          }
        }
        for (size_t ji = 0; ok && ji < n_ints; ++ji) {
          if (g_non_null[ji] == 0) {
            lp_ints[ji].min_val = 0;
            lp_ints[ji].null_code = 0;
            lp_ints[ji].mask = 0;
            lp_ints[ji].shift = static_cast<uint8_t>(bit_cursor);
            continue;
          }
          const uint64_t span = static_cast<uint64_t>(g_max[ji]) -
                                static_cast<uint64_t>(g_min[ji]);
          if (span >= std::numeric_limits<uint64_t>::max() - 2ULL) {
            ok = false;
            break;
          }
          const uint64_t domain = span + 1ULL + (g_null[ji] != 0 ? 1ULL : 0ULL);
          const uint32_t bits =
              domain <= 1ULL
                  ? 0U
                  : static_cast<uint32_t>(std::bit_width(domain - 1ULL));
          if (bit_cursor + bits > 64U) {
            ok = false;
            break;
          }
          lp_ints[ji].min_val = g_min[ji];
          lp_ints[ji].null_code = span + 1ULL;
          lp_ints[ji].mask = bits == 0 ? 0ULL : ((uint64_t{1} << bits) - 1ULL);
          lp_ints[ji].shift = static_cast<uint8_t>(bit_cursor);
          bit_cursor += bits;
        }
        if (ok && bit_cursor <= 64U) {
          use_left_packed_key = true;
          left_pack_ints = std::move(lp_ints);
          left_pack_dict_cols = std::move(lp_dict_cols);
          left_pack_right_cols = std::move(lp_right_cols);
          if (left_pack_uses_dict) {
            if (left_col_only) {
              left_pack_dict_rep_col_loc.reserve(lp_dict_rep_rows.size());
              for (const uint32_t rr : lp_dict_rep_rows) {
                const auto [ci, r] = locate_left_col(rr);
                left_pack_dict_rep_col_loc.emplace_back(
                    static_cast<uint32_t>(ci), static_cast<uint32_t>(r));
              }
            } else {
              left_pack_dict_rep_rows = std::move(lp_dict_rep_rows);
            }
          }
          auto col_unpackable = [&](size_t col_off) -> bool {
            if (std::ranges::any_of(left_pack_ints, [&](const auto& lpi) {
                  return lpi.col == col_off;
                })) {
              return true;
            }
            if (std::ranges::find(left_pack_dict_cols, col_off) !=
                left_pack_dict_cols.end()) {
              return true;
            }
            if (col_off >= left_width) {
              return std::ranges::find(left_pack_right_cols,
                                       col_off - left_width) !=
                     left_pack_right_cols.end();
            }
            if (join_type == JoinType::kInner) {
              return std::ranges::any_of(
                  equality_keys, [&](const EqualityKey& eq) {
                    if (eq.left != col_off || eq.left_offset != 0 ||
                        eq.right_offset != 0 || eq.null_safe ||
                        eq.right >= right.schema.ColumnCount()) {
                      return false;
                    }
                    const Column& lc = left.schema.GetColumn(eq.left);
                    const Column& rc = right.schema.GetColumn(eq.right);
                    return lc.Type() == rc.Type() &&
                           lc.IsUnsigned() == rc.IsUnsigned() &&
                           lc.Type() != ValueType::kVarChar &&
                           std::ranges::find(left_pack_right_cols, eq.right) !=
                               left_pack_right_cols.end();
                  });
            }
            return false;
          };
          const std::function<bool(const Expression&)> expr_unpackable =
              [&](const Expression& expr) -> bool {
            if (!expr || ContainsParameterSlot(expr)) {
              return false;
            }
            if (expr->Type() == TypeTag::kConstantValue) {
              return true;
            }
            if (expr->Type() == TypeTag::kAggregateExp) {
              return std::ranges::find(fusion->aggregates,
                                       &expr->AsAggregateExpression()) !=
                     fusion->aggregates.end();
            }
            if (expr->Type() == TypeTag::kColumnValue) {
              const auto off = LocalColumnOffset(
                  result.schema, expr->AsColumnValue().GetColumnName());
              return off.has_value() && col_unpackable(*off);
            }
            if (expr->Type() == TypeTag::kUnaryExp) {
              return expr_unpackable(expr->AsUnaryExpression().Child());
            }
            if (expr->Type() == TypeTag::kBinaryExp) {
              const auto& bin = expr->AsBinaryExpression();
              return expr_unpackable(bin.Left()) &&
                     expr_unpackable(bin.Right());
            }
            return false;
          };
          left_pack_unpack_only =
              !left.HasSpill() && !right_ptr->HasSpill() &&
              result.using_columns == nullptr && !fusion->having &&
              !fusion->post_having_active && fusion->topn_limit == 0 &&
              !fusion->rollup_count_distribution &&
              (raw_sum_fusion || raw_count_fusion ||
               fusion->aggregates.empty()) &&
              std::ranges::all_of(fusion->select_items, expr_unpackable);
        } else {
          left_pack_uses_right = false;
          left_pack_uses_dict = false;
          left_pack_dict_subgid.clear();
          clear_split_right_dict();
          left_pack_charge.reset();
        }
      }
    }
  }
  // A selective COUNT HAVING needs only a counter and borrowed key owners
  // while probing. Delay the much larger general aggregate state until the
  // worker counts have merged and the HAVING predicate has accepted a group.
  const bool compact_split_key_eligible =
      use_split_key_gid && split_key_unpack_only && !split_has_left_probe &&
      right_row_count > 0 &&
      (!split_uses_right_subgid ||
       split_right_rep_count == split_right_subgid_count);
  const bool compact_counts =
      raw_count_fusion && count_star_only && !bulk_raw_count &&
      all_direct_group_keys && !right_ptr->HasSpill() &&
      join_type == JoinType::kInner &&
      ((deferred_left_probe == nullptr && !left.HasColumnarOnly()) ||
       (integer_key && compact_split_key_eligible)) &&
      (compact_integer_threshold.has_value() ||
       (fusion->post_having_active && !fusion->post_having_is_self_sum_factor &&
        fusion->post_having_agg_slot == 0 &&
        fusion->post_having_op == BinaryOperation::kGreaterThan &&
        fusion->post_having_constant_or_factor >= 1.0));
  const bool compact_split_key_only =
      compact_counts && compact_split_key_eligible;
  constexpr size_t kCompactSplitShards = 256;
  const auto compact_count_passes = [&](int64_t count) {
    return compact_integer_threshold
               ? count > *compact_integer_threshold
               : static_cast<double>(count) >
                     fusion->post_having_constant_or_factor;
  };
  const auto compact_key_equal = [&](const CompactCountGroup& a,
                                     const CompactCountGroup& b) {
    Value ax_tmp;
    Value ay_tmp;
    return std::ranges::all_of(key_shapes, [&](const ProgramShape& shape) {
      const auto column = static_cast<slot_t>(shape.direct_column);
      const auto& x =
          column < left_width
              ? get_pair_cell(a.left, nullptr, column)
              : read_right_cell(a.right_idx, column - left_width, &ax_tmp);
      const auto& y =
          column < left_width
              ? get_pair_cell(b.left, nullptr, column)
              : read_right_cell(b.right_idx, column - left_width, &ay_tmp);
      return x.IsCaseInsensitive() || y.IsCaseInsensitive()
                 ? DistinctValueEqual{}(CanonicalDistinctValue(x),
                                        CanonicalDistinctValue(y))
                 : DistinctValueEqual{}(x, y);
    });
  };
  if (debug_fusion && compact_counts) {
    std::cerr << "[fusion] compact counts rows=" << left.rows.size() << '\n';
  }
  constexpr size_t kNoCachedGroup = std::numeric_limits<size_t>::max();
  const auto read_pair_cell = [&](const Row* left_row, uint32_t r_idx,
                                  slot_t col,
                                  Value* col_scratch) -> const Value& {
    if (col < left_width) {
      return left_row != nullptr ? left_row->values_[col] : kNullCell;
    }
    return read_right_cell(r_idx, col - left_width, col_scratch);
  };
  const auto resolve_group_index = [&](FusedWorkerLocal& local, Row& scratch,
                                       const Row* left_row, uint32_t r_idx,
                                       bool masked, size_t* cached_group,
                                       size_t* out_group_index) -> Status {
    const Row* right_row = right_row_ptr(r_idx);
    size_t group_index = 0;
    if (cached_group != nullptr && *cached_group != kNoCachedGroup) {
      group_index = *cached_group;
    } else {
      Value fast_key_tmp;
      const Value* fast_key = nullptr;
      if (int_group_key) {
        const Value& cell =
            read_pair_cell(left_row, r_idx, int_group_slot, &fast_key_tmp);
        if (!cell.IsNull() && cell.type == int_group_type &&
            !cell.IsUnsigned()) {
          fast_key = &cell;
        }
      }
      size_t* fast_slot = nullptr;
      uint32_t* fast_lut_slot = nullptr;
      bool new_group = false;
      uint64_t computed_key_hash = 0;
      bool has_computed_key_hash = false;
      if (fast_key != nullptr) {
        bool inserted = false;
        fast_slot =
            local.int_offsets.TryEmplace(fast_key->value.int_value, &inserted);
        if (!inserted) {
          group_index = *fast_slot;
        } else {
          if (!all_direct_group_keys) {
            local.key_scratch.assign(1, *fast_key);
          }
          new_group = true;
        }
      } else if (use_split_key_gid) {
        const uint32_t r_subgid =
            !split_uses_right_subgid
                ? 0U
                : (r_idx == kInvalidRightIdx
                       ? std::numeric_limits<uint32_t>::max()
                       : split_right_subgid_ptr[r_idx]);
        const bool pk_ok = !split_uses_pk_subgid || local.have_cur_pk_subgids;
        int64_t l_val = 0;
        bool l_ok = !split_has_left_probe;
        if (split_has_left_probe) {
          const ProgramShape& l_shape = key_shapes[split_left_k];
          if (left_row != nullptr && l_shape.direct_column >= 0) {
            const Value& lv =
                left_row->values_[static_cast<size_t>(l_shape.direct_column)];
            if (!lv.IsNull() &&
                (lv.type == ValueType::kInt64 || lv.type == ValueType::kDate) &&
                !lv.IsUnsigned() &&
                lv.value.int_value >=
                    static_cast<int64_t>(std::numeric_limits<int32_t>::min()) &&
                lv.value.int_value <=
                    static_cast<int64_t>(std::numeric_limits<int32_t>::max()))
                [[likely]] {
              l_val = lv.value.int_value;
              l_ok = true;
            }
          } else if (left_row != nullptr && l_shape.extract_date_col >= 0) {
            const Value& lv =
                left_row
                    ->values_[static_cast<size_t>(l_shape.extract_date_col)];
            if (!lv.IsNull() && lv.type == ValueType::kDate &&
                lv.value.int_value >= kDateMinDays &&
                lv.value.int_value <= kDateMaxDays) [[likely]] {
              if (l_shape.extract_date_part == 0 && lv.value.int_value >= 0 &&
                  std::cmp_less(lv.value.int_value, kFastYearLut.size()))
                  [[likely]] {
                l_val = kFastYearLut[static_cast<size_t>(lv.value.int_value)];
              } else {
                const std::chrono::year_month_day ymd{std::chrono::sys_days{
                    std::chrono::days{static_cast<int>(lv.value.int_value)}}};
                if (l_shape.extract_date_part == 0) {
                  const int64_t year = static_cast<int>(ymd.year());
                  l_val = year;
                } else if (l_shape.extract_date_part == 1) {
                  l_val =
                      static_cast<int64_t>(static_cast<unsigned>(ymd.month()));
                } else {
                  l_val =
                      static_cast<int64_t>(static_cast<unsigned>(ymd.day()));
                }
              }
              l_ok = true;
            }
          }
        }
        if (r_subgid != std::numeric_limits<uint32_t>::max() && pk_ok && l_ok)
            [[likely]] {
          if (use_fast_2key_year_lut && r_subgid < 64 && l_val >= 1900 &&
              l_val < 2028) [[likely]] {
            if (!local.local_2key_gid_init) [[unlikely]] {
              local.local_2key_gid.fill(std::numeric_limits<uint32_t>::max());
              local.local_2key_gid_init = true;
            }
            const size_t lut_idx = (static_cast<size_t>(r_subgid) << 7) |
                                   static_cast<size_t>(l_val - 1900);
            const uint32_t cached_gid = local.local_2key_gid[lut_idx];
            if (cached_gid != std::numeric_limits<uint32_t>::max()) [[likely]] {
              *out_group_index = cached_gid;
              return Status::kSuccess;
            }
            fast_lut_slot = &local.local_2key_gid[lut_idx];
          }
          uint64_t packed_u64 = 0;
          if (split_uses_right_subgid) {
            packed_u64 |= static_cast<uint64_t>(r_subgid) << split_right_shift;
          }
          if (split_uses_pk_subgid) {
            for (size_t gi = 0; gi < def_pk_groups.size(); ++gi) {
              if (def_pk_groups[gi].use_subgid) {
                packed_u64 |= static_cast<uint64_t>(local.cur_pk_subgids[gi])
                              << def_pk_groups[gi].subgid_shift;
              }
            }
          }
          if (split_has_left_probe) {
            packed_u64 |= static_cast<uint64_t>(
                static_cast<uint32_t>(static_cast<int32_t>(l_val)));
          }
          const auto packed_gk = std::bit_cast<int64_t>(packed_u64);
          bool inserted = false;
          fast_slot = local.int_offsets.TryEmplace(packed_gk, &inserted);
          if (!inserted) [[likely]] {
            if (fast_lut_slot != nullptr) {
              *fast_lut_slot = static_cast<uint32_t>(*fast_slot);
            }
            *out_group_index = *fast_slot;
            return Status::kSuccess;
          }
          if (!all_direct_group_keys) {
            local.key_scratch.resize(key_programs.size());
            Value rv_tmp;
            for (size_t k = 0; k < key_shapes.size(); ++k) {
              if (split_has_left_probe && k == split_left_k) {
                const ProgramShape& l_shape = key_shapes[split_left_k];
                if (l_shape.direct_column >= 0 &&
                    left_row->values_[static_cast<size_t>(
                                          l_shape.direct_column)]
                            .type == ValueType::kDate) {
                  local.key_scratch[split_left_k] = Value::DateFromDays(l_val);
                } else {
                  local.key_scratch[split_left_k] = Value(l_val);
                }
              } else if (std::cmp_greater_equal(key_shapes[k].direct_column,
                                                left_width)) {
                const auto column =
                    static_cast<size_t>(key_shapes[k].direct_column) -
                    left_width;
                local.key_scratch[k] = CanonicalDistinctValue(
                    read_right_cell(r_idx, column, &rv_tmp));
              } else {
                local.key_scratch[k] = CanonicalDistinctValue(
                    left_row->values_[static_cast<size_t>(
                        key_shapes[k].direct_column)]);
              }
            }
          }
          computed_key_hash = packed_u64;
          has_computed_key_hash = true;
          new_group = true;
        } else {
          local.key_scratch.resize(key_programs.size());
          for (size_t k = 0; k < key_programs.size(); ++k) {
            // A NULL or wide probe key cannot use the packed key. The build
            // side may still be columnar, so its absent Row is not SQL NULL.
            if (key_shapes[k].direct_column >= 0) {
              Value cell_scratch;
              local.key_scratch[k] = CanonicalDistinctValue(read_pair_cell(
                  left_row, r_idx,
                  static_cast<slot_t>(key_shapes[k].direct_column),
                  &cell_scratch));
            } else {
              ASSIGN_OR_RETURN(Value, kv,
                               eval_pair_program(key_programs[k], key_shapes[k],
                                                 scratch, left_row, right_row));
              local.key_scratch[k] = CanonicalDistinctValue(kv);
            }
          }
          if (auto found = local.offsets.find(local.key_scratch);
              found != local.offsets.end()) {
            group_index = found->second;
          } else {
            new_group = true;
          }
        }
      } else if (use_left_packed_key && left_row != nullptr &&
                 (!left_pack_uses_right || r_idx != kInvalidRightIdx) &&
                 left_row >= left.rows.data() &&
                 left_row < left.rows.data() + left.rows.size()) {
        uint64_t packed_u64 = 0;
        if (left_pack_uses_right) {
          const uint32_t r_subgid = split_right_subgid_ptr[r_idx];
          packed_u64 |= static_cast<uint64_t>(r_subgid) << split_right_shift;
        }
        if (left_pack_uses_dict) {
          const auto li = static_cast<size_t>(left_row - left.rows.data());
          packed_u64 |= static_cast<uint64_t>(left_pack_dict_subgid[li])
                        << left_pack_dict_shift;
        }
        for (const LeftPackedIntCol& lpi : left_pack_ints) {
          const Value& v = left_row->values_[lpi.col];
          const uint64_t code =
              v.IsNull()
                  ? lpi.null_code
                  : static_cast<uint64_t>(v.value.int_value - lpi.min_val);
          packed_u64 |= code << lpi.shift;
        }
        bool inserted = false;
        fast_slot = local.int_offsets.TryEmplace(
            std::bit_cast<int64_t>(packed_u64), &inserted);
        if (!inserted) [[likely]] {
          *out_group_index = *fast_slot;
          return Status::kSuccess;
        }
        computed_key_hash = packed_u64;
        has_computed_key_hash = true;
        new_group = true;
      } else if (packed_2x32_group_key) {
        Value v0_tmp;
        Value v1_tmp;
        const Value& v0 =
            read_pair_cell(left_row, r_idx, packed_2x32_slot0, &v0_tmp);
        const Value& v1 =
            read_pair_cell(left_row, r_idx, packed_2x32_slot1, &v1_tmp);
        const auto fits_i32 = [](const Value& v, ValueType expected) noexcept {
          return !v.IsNull() && v.type == expected && !v.IsUnsigned() &&
                 v.value.int_value >=
                     static_cast<int64_t>(
                         std::numeric_limits<int32_t>::min()) &&
                 v.value.int_value <=
                     static_cast<int64_t>(std::numeric_limits<int32_t>::max());
        };
        if (fits_i32(v0, packed_2x32_type0) && fits_i32(v1, packed_2x32_type1))
            [[likely]] {
          const uint64_t u0 =
              static_cast<uint32_t>(static_cast<int32_t>(v0.value.int_value));
          const uint64_t u1 =
              static_cast<uint32_t>(static_cast<int32_t>(v1.value.int_value));
          const uint64_t packed_u64 = (u0 << 32) | u1;
          bool inserted = false;
          fast_slot = local.int_offsets.TryEmplace(
              std::bit_cast<int64_t>(packed_u64), &inserted);
          if (!inserted) [[likely]] {
            *out_group_index = *fast_slot;
            return Status::kSuccess;
          }
          computed_key_hash = packed_u64;
          has_computed_key_hash = true;
          new_group = true;
        } else {
          if (local.key_scratch.size() != 2) {
            local.key_scratch.resize(2);
          }
          local.key_scratch[0] = v0;
          local.key_scratch[1] = v1;
          if (auto found = local.offsets.find(local.key_scratch);
              found != local.offsets.end()) {
            group_index = found->second;
          } else {
            new_group = true;
          }
        }
      } else if (!key_programs.empty()) {
        uint64_t key_hash = 0x9e3779b97f4a7c15ULL;
        const bool fast_direct_int_hash =
            hash_int_keys_only && all_direct_group_keys && !int_group_key;
        if (fast_direct_int_hash) {
          Value kv_tmp;
          for (const size_t ik : int_key_indices) {
            const Value& kv = read_pair_cell(
                left_row, r_idx,
                static_cast<slot_t>(key_shapes[ik].direct_column), &kv_tmp);
            const uint64_t h = kv.IsNull()
                                   ? 0x517cc1b727220a95ULL
                                   : static_cast<uint64_t>(kv.value.int_value) *
                                         0x9e3779b97f4a7c15ULL;
            key_hash = (key_hash << 7) | (key_hash >> 57);
            key_hash ^= h ^ (h >> 32);
            key_hash *= 0x100000001b3ULL;
          }
        } else {
          if (local.key_scratch.size() != key_programs.size()) {
            local.key_scratch.resize(key_programs.size());
          }
          if (local.key_ptrs.size() != key_programs.size()) {
            local.key_ptrs.resize(key_programs.size());
          }
          Value extract_tmp;
          for (size_t key_index = 0; key_index < key_programs.size();
               ++key_index) {
            const Value* kp = nullptr;
            if (key_shapes[key_index].direct_column >= 0) {
              const Value& raw = read_pair_cell(
                  left_row, r_idx,
                  static_cast<slot_t>(key_shapes[key_index].direct_column),
                  &local.key_scratch[key_index]);
              if (raw.IsCaseInsensitive() ||
                  (raw.type == ValueType::kDouble &&
                   (std::isnan(raw.value.double_value) ||
                    raw.value.double_value == 0.0))) [[unlikely]] {
                local.key_scratch[key_index] = CanonicalDistinctValue(raw);
                kp = &local.key_scratch[key_index];
              } else {
                kp = &raw;
              }
            } else if (key_shapes[key_index].extract_date_col >= 0) {
              const Value& c = read_pair_cell(
                  left_row, r_idx,
                  static_cast<slot_t>(key_shapes[key_index].extract_date_col),
                  &extract_tmp);
              if (!c.IsNull() && c.type == ValueType::kDate &&
                  c.value.int_value >= kDateMinDays &&
                  c.value.int_value <= kDateMaxDays) [[likely]] {
                int64_t extracted = 0;
                if (key_shapes[key_index].extract_date_part == 0 &&
                    c.value.int_value >= 0 &&
                    std::cmp_less(c.value.int_value, kFastYearLut.size()))
                    [[likely]] {
                  extracted =
                      kFastYearLut[static_cast<size_t>(c.value.int_value)];
                } else {
                  const std::chrono::year_month_day ymd{std::chrono::sys_days{
                      std::chrono::days{static_cast<int>(c.value.int_value)}}};
                  if (key_shapes[key_index].extract_date_part == 0) {
                    const int64_t year = static_cast<int>(ymd.year());
                    extracted = year;
                  } else if (key_shapes[key_index].extract_date_part == 1) {
                    extracted = static_cast<int64_t>(
                        static_cast<unsigned>(ymd.month()));
                  } else {
                    extracted =
                        static_cast<int64_t>(static_cast<unsigned>(ymd.day()));
                  }
                }
                local.key_scratch[key_index] = Value(extracted);
                kp = &local.key_scratch[key_index];
              } else {
                ASSIGN_OR_RETURN(
                    Value, key_value,
                    eval_pair_program(key_programs[key_index],
                                      key_shapes[key_index], scratch, left_row,
                                      right_row));
                local.key_scratch[key_index] =
                    CanonicalDistinctValue(key_value);
                kp = &local.key_scratch[key_index];
              }
            } else {
              ASSIGN_OR_RETURN(Value, key_value,
                               eval_pair_program(key_programs[key_index],
                                                 key_shapes[key_index], scratch,
                                                 left_row, right_row));
              local.key_scratch[key_index] = CanonicalDistinctValue(key_value);
              kp = &local.key_scratch[key_index];
            }
            local.key_ptrs[key_index] = kp;
            if (!hash_int_keys_only) {
              key_hash = (key_hash << 7) | (key_hash >> 57);
              uint64_t vh = 0;
              if (!kp->IsNull() &&
                  (kp->type == ValueType::kInt64 ||
                   kp->type == ValueType::kDate) &&
                  !kp->IsUnsigned()) [[likely]] {
                vh = static_cast<uint64_t>(kp->value.int_value) *
                     0x9e3779b97f4a7c15ULL;
                vh ^= vh >> 32;
              } else {
                // Shared dimension strings already retain their canonical hash.
                // Reuse it instead of hashing the bytes for every fact match.
                vh = std::hash<Value>()(*kp);
              }
              key_hash ^= vh;
              key_hash *= 0x100000001b3ULL;
            }
          }
          if (hash_int_keys_only) {
            for (const size_t ik : int_key_indices) {
              const Value& kv = *local.key_ptrs[ik];
              const uint64_t h =
                  kv.IsNull() ? 0x517cc1b727220a95ULL
                              : static_cast<uint64_t>(kv.value.int_value) *
                                    0x9e3779b97f4a7c15ULL;
              key_hash = (key_hash << 7) | (key_hash >> 57);
              key_hash ^= h ^ (h >> 32);
              key_hash *= 0x100000001b3ULL;
            }
          }
        }
        computed_key_hash = key_hash;
        has_computed_key_hash = true;
        if (!int_group_key) {
          bool hash_inserted = false;
          size_t* hash_slot = local.int_offsets.TryEmplace(
              static_cast<int64_t>(key_hash), &hash_inserted);
          if (!hash_inserted) {
            const size_t cand_group = *hash_slot;
            FusedGroup& cand_grp = local.groups[cand_group];
            const bool lazy_cand =
                cand_grp.key.values_.empty() && all_direct_group_keys;
            bool match = lazy_cand ||
                         (cand_grp.key.values_.size() == key_programs.size());
            Value cand_col_tmp;
            Value probe_col_tmp;
            auto cand_key_cell = [&](size_t k) -> const Value& {
              if (!lazy_cand) {
                return cand_grp.key.values_[k];
              }
              return read_group_pair_cell(
                  cand_grp, static_cast<size_t>(key_shapes[k].direct_column),
                  &cand_col_tmp);
            };
            auto probe_key_cell = [&](size_t k) -> const Value& {
              if (fast_direct_int_hash) {
                return read_pair_cell(
                    left_row, r_idx,
                    static_cast<slot_t>(key_shapes[k].direct_column),
                    &probe_col_tmp);
              }
              return *local.key_ptrs[k];
            };
            if (match && !int_key_indices.empty()) {
              for (const size_t ik : int_key_indices) {
                const Value& x = probe_key_cell(ik);
                const Value& y = cand_key_cell(ik);
                if (x.type != y.type ||
                    (!x.IsNull() && x.value.int_value != y.value.int_value)) {
                  match = false;
                  break;
                }
              }
            }
            if (match && int_key_indices.size() < key_programs.size()) {
              for (size_t k = 0; k < key_programs.size(); ++k) {
                const Value& x = probe_key_cell(k);
                const Value& y = cand_key_cell(k);
                if (&x == &y) {
                  continue;
                }
                if (x.type != y.type) {
                  match = false;
                  break;
                }
                if (x.type == ValueType::kDouble) {
                  const double p = x.value.double_value;
                  const double q = y.value.double_value;
                  if (std::isnan(p) && std::isnan(q)) {
                    continue;
                  }
                  if (!(p == q)) {
                    match = false;
                    break;
                  }
                  continue;
                }
                if (!(x == y)) {
                  match = false;
                  break;
                }
              }
            }
            if (match) [[likely]] {
              group_index = cand_group;
            } else {
              ensure_group_key(cand_grp);
              if (local.key_scratch.size() != key_programs.size()) {
                local.key_scratch.resize(key_programs.size());
              }
              Value p_tmp;
              for (size_t k = 0; k < key_programs.size(); ++k) {
                if (fast_direct_int_hash) {
                  local.key_scratch[k] = CanonicalDistinctValue(read_pair_cell(
                      left_row, r_idx,
                      static_cast<slot_t>(key_shapes[k].direct_column),
                      &p_tmp));
                } else if (local.key_ptrs[k] != &local.key_scratch[k]) {
                  local.key_scratch[k] = *local.key_ptrs[k];
                }
              }
              if (auto found = local.offsets.find(local.key_scratch);
                  found != local.offsets.end()) {
                group_index = found->second;
              } else {
                new_group = true;
              }
            }
          } else {
            if (!all_direct_group_keys) {
              for (size_t k = 0; k < local.key_ptrs.size(); ++k) {
                if (local.key_ptrs[k] != &local.key_scratch[k]) {
                  local.key_scratch[k] = *local.key_ptrs[k];
                }
              }
            }
            new_group = true;
            fast_slot = hash_slot;
          }
        } else {
          for (size_t k = 0; k < local.key_ptrs.size(); ++k) {
            if (local.key_ptrs[k] != &local.key_scratch[k]) {
              local.key_scratch[k] = *local.key_ptrs[k];
            }
          }
          if (auto found = local.offsets.find(local.key_scratch);
              found != local.offsets.end()) {
            group_index = found->second;
          } else {
            new_group = true;
          }
        }
      } else {
        local.key_scratch.clear();
        if (auto found = local.offsets.find(local.key_scratch);
            found != local.offsets.end()) {
          group_index = found->second;
        } else {
          new_group = true;
        }
      }
      if (new_group) {
        group_index = local.groups.size();
        FusedGroup group;
        if (raw_sum_fusion) {
          local.raw_arena.Initialize(group, raw_slot_count);
        }
        if (!skip_accumulator_states) {
          group.state_offset = local.states.size();
          for (size_t i = 0; i < fusion->aggregates.size(); ++i) {
            local.states.emplace_back(fusion->aggregates[i],
                                      aggregate_shapes[i]);
          }
        }
        group.key_hash = computed_key_hash;
        group.has_key_hash = has_computed_key_hash;
        if (left.HasSpill()) {
          if (masked && left_row != nullptr) {
            for (size_t i = 0; i < left_width; ++i) {
              scratch.values_[i] = left_row->values_[i];
            }
            Value right_tmp;
            for (size_t i = 0; i < right_width; ++i) {
              scratch.values_[left_width + i] =
                  read_right_cell(r_idx, i, &right_tmp);
            }
          }
          group.representative = std::make_unique<Row>(scratch);
        } else {
          if ((deferred_left_probe != nullptr || left.HasColumnarOnly()) &&
              left_row != nullptr &&
              !((packed_2x32_unpack_only || split_key_unpack_only) &&
                has_computed_key_hash)) {
            local.owned_rep_left.push_back(*left_row);
            group.rep_left = &local.owned_rep_left.back();
          } else {
            group.rep_left =
                (deferred_left_probe != nullptr || left.HasColumnarOnly())
                    ? nullptr
                    : left_row;
          }
          group.rep_right_idx = r_idx;
        }
        if (fast_slot != nullptr) {
          if (!all_direct_group_keys) {
            group.key = Row(std::move(local.key_scratch));
          }
          local.groups.push_back(std::move(group));
          *fast_slot = group_index;
          if (fast_lut_slot != nullptr) {
            *fast_lut_slot = static_cast<uint32_t>(group_index);
          }
        } else {
          group.key = Row(local.key_scratch);
          local.groups.push_back(std::move(group));
          local.offsets.emplace(Row(std::move(local.key_scratch)), group_index);
        }
      }
      if (cached_group != nullptr && left_only_group_keys) {
        *cached_group = group_index;
      }
    }
    *out_group_index = group_index;
    return Status::kSuccess;
  };
  bool compact_log_allowed = false;
  const auto record_compact_split = [&](FusedWorkerLocal& local,
                                        uint64_t packed_u64) -> Status {
    if (compact_log_allowed && local.compact_count_inputs == 1024) {
      local.compact_count_log = local.compact_counts.size() >= 820;
    }
    ++local.compact_count_inputs;
    if (local.compact_count_log) {
      if (local.compact_split_shards.empty()) {
        local.compact_split_shards.resize(kCompactSplitShards);
      }
      uint64_t h = packed_u64 + 0x9e3779b97f4a7c15ULL;
      h = (h ^ (h >> 30U)) * 0xbf58476d1ce4e5b9ULL;
      h = (h ^ (h >> 27U)) * 0x94d049bb133111ebULL;
      h ^= h >> 31U;
      local.compact_split_shards[h & (kCompactSplitShards - 1)].push_back(
          packed_u64);
      return Status::kSuccess;
    }
    bool inserted = false;
    size_t* slot = local.int_offsets.TryEmplace(
        std::bit_cast<int64_t>(packed_u64), &inserted);
    if (!inserted) {
      ++local.compact_counts[*slot].count;
      return Status::kSuccess;
    }
    CompactCountGroup probe;
    probe.hash = packed_u64;
    probe.count = 1;
    *slot = local.compact_counts.size();
    local.compact_counts.push_back(probe);
    return Status::kSuccess;
  };
  const auto accumulate_into = [&](FusedWorkerLocal& local, Row& scratch,
                                   const Row* left_row, uint32_t r_idx,
                                   bool masked,
                                   size_t* cached_group = nullptr) -> Status {
    const Row* right_row = right_row_ptr(r_idx);
    if (compact_counts) {
      if (compact_split_key_only && r_idx != kInvalidRightIdx) {
        uint64_t packed_u64 = 0;
        bool pk_ok = true;
        if (split_uses_pk_subgid) {
          pk_ok = local.have_cur_pk_subgids;
          for (size_t gi = 0; pk_ok && gi < def_pk_groups.size(); ++gi) {
            if (def_pk_groups[gi].use_subgid) {
              if (local.cur_pk_subgids[gi] ==
                  std::numeric_limits<uint32_t>::max()) [[unlikely]] {
                pk_ok = false;
              } else {
                packed_u64 |= static_cast<uint64_t>(local.cur_pk_subgids[gi])
                              << def_pk_groups[gi].subgid_shift;
              }
            }
          }
        }
        const uint32_t r_subgid =
            !split_uses_right_subgid ? 0U : split_right_subgid_ptr[r_idx];
        if (pk_ok && r_subgid != std::numeric_limits<uint32_t>::max()) {
          if (split_uses_right_subgid) {
            packed_u64 |= static_cast<uint64_t>(r_subgid) << split_right_shift;
          }
          return record_compact_split(local, packed_u64);
        }
      }
      CompactCountGroup probe;
      probe.left = left_row;
      probe.right_idx = r_idx;
      bool packed = false;
      if (use_split_key_gid && split_has_left_probe && !split_uses_pk_subgid &&
          split_uses_right_subgid &&
          key_shapes[split_left_k].direct_column >= 0 && left_row != nullptr &&
          r_idx != kInvalidRightIdx) {
        const auto& value = (*left_row)[static_cast<size_t>(
            key_shapes[split_left_k].direct_column)];
        if (!value.IsNull() && !value.IsUnsigned() &&
            (value.type == ValueType::kInt64 ||
             value.type == ValueType::kDate) &&
            std::in_range<int32_t>(value.value.int_value)) {
          const uint32_t dimension = split_right_subgid_ptr[r_idx];
          if (dimension != std::numeric_limits<uint32_t>::max()) {
            probe.hash = (static_cast<uint64_t>(dimension) << 32U) |
                         static_cast<uint32_t>(value.value.int_value);
            packed = true;
          }
        }
      }
      if (!packed) {
        probe.hash = 0x9e3779b97f4a7c15ULL;
        Value v_tmp;
        for (const auto& shape : key_shapes) {
          const auto& value =
              read_pair_cell(left_row, r_idx,
                             static_cast<slot_t>(shape.direct_column), &v_tmp);
          probe.hash = std::rotl(probe.hash, 7);
          probe.hash ^= fast_hash_val(value);
          probe.hash *= 0x100000001b3ULL;
        }
      }
      // When nearly every pair is a new group, local hashing barely reduces
      // the transfer. Keep the sampled partials, then append records for the
      // owning merge partition to fold exactly once. The sample only chooses
      // an algorithm; full key equality still decides every final group.
      if (compact_log_allowed && local.compact_count_inputs == 1024) {
        local.compact_count_log = local.compact_counts.size() >= 820;
      }
      ++local.compact_count_inputs;
      if (local.compact_count_log) {
        probe.count = 1;
        local.compact_counts.push_back(probe);
        return Status::kSuccess;
      }
      bool inserted = false;
      size_t* slot = local.int_offsets.TryEmplace(
          std::bit_cast<int64_t>(probe.hash), &inserted);
      if (!inserted) {
        for (size_t index = *slot; index != SIZE_MAX;
             index = local.compact_counts[index].next) {
          if (compact_key_equal(local.compact_counts[index], probe)) {
            ++local.compact_counts[index].count;
            return Status::kSuccess;
          }
        }
        probe.next = *slot;
      }
      probe.count = 1;
      *slot = local.compact_counts.size();
      local.compact_counts.push_back(probe);
      return Status::kSuccess;
    }
    size_t group_index = 0;
    RETURN_IF_FAIL(resolve_group_index(local, scratch, left_row, r_idx, masked,
                                       cached_group, &group_index));
    FusedGroup& group = local.groups[group_index];
    if (count_star_only) {
      group.raw_count += 1;
      return Status::kSuccess;
    }
    if (count_nonnull_only) {
      // COUNT(right column): unmatched LEFT rows and NULL cells contribute 0.
      if (r_idx != kInvalidRightIdx &&
          (!count_right_column.has_value() ||
           !right_cell_is_null(r_idx, *count_right_column))) {
        group.raw_count += 1;
      }
      return Status::kSuccess;
    }
    if (raw_sum_fusion) {
      Value v_tmp;
      for (size_t s = 0; s < raw_slot_count; ++s) {
        const size_t src = raw_slot_origin[s];
        if (case_always_null(src, right_row)) {
          continue;
        }
        switch (slot_agg_kinds[s]) {
          case RawAggKind::kCountStar:
            group.RawCounts()[s] += 1;
            break;
          case RawAggKind::kConstantExtremum:
          case RawAggKind::kCountValue: {
            if (child_shapes[src].direct_column >= 0) {
              const Value& v = read_pair_cell(
                  left_row, r_idx,
                  static_cast<slot_t>(child_shapes[src].direct_column), &v_tmp);
              if (!v.IsNull()) {
                group.RawCounts()[s] += 1;
              }
            } else {
              ASSIGN_OR_RETURN(
                  Value, child,
                  eval_pair_program(child_programs[src], child_shapes[src],
                                    scratch, left_row, right_row));
              if (!child.IsNull()) {
                group.RawCounts()[s] += 1;
              }
            }
            break;
          }
          case RawAggKind::kSumInt64: {
            int64_t iv = 0;
            bool is_null = false;
            if (child_shapes[src].direct_column >= 0) {
              const Value& v = read_pair_cell(
                  left_row, r_idx,
                  static_cast<slot_t>(child_shapes[src].direct_column), &v_tmp);
              is_null = v.IsNull();
              if (!is_null) {
                iv = v.value.int_value;
              }
            } else if (child_shapes[src].case_cond !=
                           ProgramShape::CaseCondKind::kNone &&
                       child_shapes[src].case_val ==
                           ProgramShape::CaseValKind::kIntConst1) {
              bool cond_true = false;
              if (cached_case_cond(src, left_row, right_row, &cond_true))
                  [[likely]] {
                iv = cond_true ? 1 : 0;
              } else {
                ASSIGN_OR_RETURN(
                    Value, child,
                    eval_pair_program(child_programs[src], child_shapes[src],
                                      scratch, left_row, right_row));
                is_null = child.IsNull();
                if (!is_null) {
                  iv = child.value.int_value;
                }
              }
            } else {
              ASSIGN_OR_RETURN(
                  Value, child,
                  eval_pair_program(child_programs[src], child_shapes[src],
                                    scratch, left_row, right_row));
              is_null = child.IsNull();
              if (!is_null) {
                iv = child.value.int_value;
              }
            }
            if (!is_null) {
              int64_t next = 0;
              if (__builtin_add_overflow(group.RawInts()[s], iv, &next)) {
                group.RawInfFlags()[s] = 1;
              } else {
                group.RawInts()[s] = next;
              }
              group.RawCounts()[s] += 1;
            }
            break;
          }
          case RawAggKind::kSumDouble:
          case RawAggKind::kAvgDouble: {
            double dv = 0.0;
            bool is_null = false;
            RETURN_IF_FAIL(eval_pair_double(src, scratch, left_row, right_row,
                                            &dv, &is_null));
            if (!is_null) {
              const double next = group.RawDoubles()[s] + dv;
              if (std::isinf(next) && !std::isinf(group.RawDoubles()[s]) &&
                  !std::isinf(dv)) {
                group.RawInfFlags()[s] = 1;
              }
              group.RawDoubles()[s] = next;
              group.RawCounts()[s] += 1;
            }
            break;
          }
          case RawAggKind::kStddevSamp:
          case RawAggKind::kStddevPop:
          case RawAggKind::kVarSamp:
          case RawAggKind::kVarPop: {
            platform::ExtendedFloat x = 0;
            bool is_null = false;
            RETURN_IF_FAIL(
                eval_pair_ext(src, scratch, left_row, right_row, &x, &is_null));
            if (!is_null) {
              group.RawStatX()[s] += x;
              group.RawStatXX()[s] += x * x;
              group.RawCounts()[s] += 1;
            }
            break;
          }
          case RawAggKind::kCountDistinctInt64: {
            int64_t iv = 0;
            bool is_null = false;
            if (child_shapes[src].direct_column >= 0) {
              const Value& v = read_pair_cell(
                  left_row, r_idx,
                  static_cast<slot_t>(child_shapes[src].direct_column), &v_tmp);
              is_null = v.IsNull();
              if (!is_null) {
                iv = v.value.int_value;
              }
            } else {
              ASSIGN_OR_RETURN(
                  Value, child,
                  eval_pair_program(child_programs[src], child_shapes[src],
                                    scratch, left_row, right_row));
              is_null = child.IsNull();
              if (!is_null) {
                iv = child.value.int_value;
              }
            }
            if (!is_null) {
              if (!group.raw_int_set) {
                group.raw_int_set = std::make_unique<FlatIntKeyMap<uint8_t>>();
              }
              group.raw_int_set->TryEmplace(iv);
            }
            break;
          }
          case RawAggKind::kNone:
            break;
        }
      }
      return Status::kSuccess;
    }
    Value v_tmp;
    for (size_t i = 0; i < fusion->aggregates.size(); ++i) {
      AggregateAccumulator& accumulator = local.states[group.state_offset + i];
      if (fusion->count_star[i]) {
        RETURN_IF_FAIL(accumulator.Add(Value(1)));
        continue;
      }
      if (child_shapes[i].direct_column >= 0) {
        RETURN_IF_FAIL(accumulator.Add(read_pair_cell(
            left_row, r_idx, static_cast<slot_t>(child_shapes[i].direct_column),
            &v_tmp)));
        continue;
      }
      ASSIGN_OR_RETURN(Value, child,
                       eval_pair_program(child_programs[i], child_shapes[i],
                                         scratch, left_row, right_row));
      RETURN_IF_FAIL(accumulator.Add(child));
    }
    return Status::kSuccess;
  };
  // Fills the pair scratch from the two sides.  `r_idx == kInvalidRightIdx`
  // pads the right half with NULLs (unmatched left rows).  Masked fills touch
  // only the cells the compiled programs load.
  const auto fill_scratch = [&](Row& scratch, const Row& left_row,
                                uint32_t r_idx) {
    if (mask_scratch) {
      Value tmp;
      for (const slot_t cell : scratch_cells) {
        scratch.values_[cell] =
            cell < left_width ? left_row.values_[cell]
                              : read_right_cell(r_idx, cell - left_width, &tmp);
      }
      return;
    }
    for (size_t i = 0; i < left_width; ++i) {
      scratch.values_[i] = left_row.values_[i];
    }
    Value tmp;
    for (size_t i = 0; i < right_width; ++i) {
      scratch.values_[left_width + i] = read_right_cell(r_idx, i, &tmp);
    }
  };
  // Returns true when the pair passed every residual and was accumulated.
  // A residual-rejected pair is NOT a match: LEFT semantics must still
  // null-extend the outer row when every bucket pair for it was rejected.
  // Direct raw aggregates read the probe and build rows themselves. Copying
  // every joined cell into the scratch row is then pure overhead.
  const bool scratch_unused =
      residual_programs.empty() && all_direct_group_keys && raw_sum_fusion &&
      std::ranges::all_of(child_shapes, [](const ProgramShape& shape) {
        return shape.direct_column >= 0;
      });
  const auto process_pair =
      [&](FusedWorkerLocal& local, const Row& left_row, uint32_t r_idx,
          Row& scratch, size_t* cached_group = nullptr) -> StatusOr<bool> {
    if (anti_unmatched_only && residual_programs.empty()) {
      return true;
    }
    if (!scratch_unused) {
      fill_scratch(scratch, left_row, r_idx);
    }
    for (size_t i = 0; i < residual_programs.size(); ++i) {
      ASSIGN_OR_RETURN(Value, verdict,
                       EvaluateProgramValue(residual_programs[i],
                                            residual_shapes[i], scratch));
      if (!Truthy(verdict)) {
        return false;
      }
    }
    if (anti_unmatched_only) {
      // Lean proof: lean/TinylambOpt/TpcdsExecCodegen.lean
      //   (fused_antijoin_groupby_sound)
      // Matched pair under LEFT JOIN ... WHERE right.key IS NULL is rejected.
      return true;
    }
    RETURN_IF_FAIL(accumulate_into(local, scratch, &left_row, r_idx,
                                   mask_scratch, cached_group));
    return true;
  };
  const auto unmatched_left = [&](FusedWorkerLocal& local, Row& scratch,
                                  const Row& left_row) -> Status {
    fill_scratch(scratch, left_row, kInvalidRightIdx);
    return accumulate_into(local, scratch, &left_row, kInvalidRightIdx,
                           mask_scratch);
  };

  bool has_double_raw_agg = false;
  bool has_int_raw_agg = false;
  bool has_stat_raw_agg = false;
  if (raw_sum_fusion) {
    for (size_t s = 0; s < raw_slot_count && s < slot_agg_kinds.size(); ++s) {
      if (slot_agg_kinds[s] == RawAggKind::kSumDouble ||
          slot_agg_kinds[s] == RawAggKind::kAvgDouble) {
        has_double_raw_agg = true;
      } else if (is_stat_raw_kind(slot_agg_kinds[s])) {
        has_stat_raw_agg = true;
      } else if (slot_agg_kinds[s] == RawAggKind::kSumInt64) {
        has_int_raw_agg = true;
      }
    }
  }
  const size_t workers =
      (left.HasSpill() || (int_group_key && int_group_slot < left_width &&
                           !bulk_raw_count && total_left < 96000))
          ? 1
          : ParallelWorkerCount((total_left / 16384) + 1);
  std::vector<FusedWorkerLocal> locals(workers);
  for (FusedWorkerLocal& l : locals) {
    l.raw_arena.has_stat = has_stat_raw_agg;
    l.raw_arena.has_double = !raw_sum_fusion || has_double_raw_agg;
    l.raw_arena.has_int = !raw_sum_fusion || has_int_raw_agg;
  }
  std::shared_ptr<QueryMemoryCharge> group_capacity_charge;
  if (all_direct_group_keys && !right_only_group_keys &&
      deferred_left_probe == nullptr && !left.HasSpill() &&
      !left.HasColumnarOnly() && left.rows.size() >= 32768) {
    // Estimate high-cardinality probe keys from a small, distributed sample.
    // Only capacity depends on this estimate: collisions, repeated keys and
    // build-side multiplicity still go through the normal equality checks.
    std::array<uint64_t, 1024> hashes{};
    for (size_t sample = 0; sample < hashes.size(); ++sample) {
      const auto& row = left.rows[(sample * left.rows.size()) / hashes.size()];
      uint64_t hash = 0x9e3779b97f4a7c15ULL;
      for (const auto& shape : key_shapes) {
        const auto column = static_cast<size_t>(shape.direct_column);
        if (column < left_width) {
          hash = std::rotl(hash, 7) ^ DistinctValueHash {}(row[column]);
          hash *= 0x100000001b3ULL;
        }
      }
      hashes[sample] = hash;
    }
    std::ranges::sort(hashes);
    const auto distinct = static_cast<size_t>(
        std::unique(hashes.begin(), hashes.end()) - hashes.begin());
    if (distinct >= (hashes.size() * 4) / 5) {
      const size_t capacity =
          std::min<size_t>(262144, (left.rows.size() / workers) + 1);
      // Include open-addressing slack and the group-vector storage. Raw
      // aggregate blocks retain their existing incremental allocation path.
      const size_t bytes = workers * capacity *
                           (64 + (compact_counts ? sizeof(CompactCountGroup)
                                                 : sizeof(FusedGroup)));
      group_capacity_charge = TryReserveQueryMemory(bytes);
      if (group_capacity_charge) {
        for (auto& local : locals) {
          local.int_offsets.Reserve(capacity);
          if (compact_counts) {
            local.compact_counts.reserve(capacity);
          } else {
            local.groups.reserve(capacity);
          }
        }
      }
    }
  }
  std::vector<Row> scratch_storage(locals.size());
  for (Row& scratch : scratch_storage) {
    scratch.values_.resize(left_width + right_width);
  }
  std::atomic<size_t> next_chunk{0};
  std::atomic<size_t> next_col_chunk{0};
  constexpr size_t kProbeChunkRows = 8192;
  const size_t chunk_count =
      (total_left + kProbeChunkRows - 1) / kProbeChunkRows;

  // Build the hash table over the right side once, before the workers spawn
  // (the build side is read-only during the probe).
  std::optional<FlatJoinIdxBuckets<int64_t>> int_buckets;
  std::optional<FlatJoinIdxBuckets<CompositeIntKey, CompositeIntKeyHash>>
      composite_buckets;
  // A unique, compact leading component can index a composite key directly.
  // Keep all components for exact equality checks; uniqueness is verified
  // below.
  std::vector<CompositeIntKey> dense_composite_keys;
  std::vector<uint32_t> dense_composite_rows;
  std::shared_ptr<QueryMemoryCharge> composite_keys_charge,
      composite_rows_charge;
  int64_t dense_composite_min = 0;
  bool use_dense_composite = false;
  std::optional<FlatJoinIdxBuckets<std::string>> key_buckets;
  std::optional<FlatIntKeyMap<BulkCountValue>> bulk_counts;
  std::vector<BulkCountValue> dense_bulk_counts;
  int64_t dense_bulk_min = 0;
  bool use_dense_bulk = false;
  struct BuildKey {
    int64_t key;
    uint32_t row;
  };
  std::vector<std::vector<BuildKey>> key_cache;
  std::shared_ptr<QueryMemoryCharge> key_cache_charge;
  std::vector<uint32_t> dense_int_idxs;
  std::shared_ptr<QueryMemoryCharge> dense_int_charge;
  std::vector<uint64_t> ranked_int_bits;
  std::vector<uint32_t> ranked_int_prefix, ranked_int_rows;
  uint64_t ranked_int_span = 0;
  int64_t dense_int_min = 0;
  bool use_dense_int_rows = false;
  bool build_int_keys_unique = true;
  bool packed_2x32_int_key = false;
  std::vector<UniqueIntSlot> unique_int_table;
  size_t unique_int_mask = 0;
  bool use_unique_int_table = false;
  auto hash_unique_int_key = HashUniqueIntKey;
  const auto read_col_int_cell = [&](const ColumnarColumn& kc, size_t r,
                                     bool fast_uniform,
                                     int64_t* out) noexcept -> bool {
    if (fast_uniform) {
      return kc.TryReadNonNullInt(r, out);
    }
    if (ColumnarCellIsNullOrNaN(kc, r)) {
      return false;
    }
    *out = ColumnarIntValue(kc, r);
    return true;
  };
  // Decode each component once, including gathered columns and NULLs.
  const auto read_col_composite_key = [&](const ColumnarChunk& chunk,
                                          const std::vector<slot_t>& columns,
                                          const std::vector<int64_t>& biases,
                                          size_t row, CompositeIntKey* key) {
    *key = CompositeIntKey{};
    for (size_t i = 0; i < columns.size(); ++i) {
      const auto& column = chunk.columns[columns[i]];
      const bool fast =
          column.is_uniform && !column.is_unsigned &&
          (column.type == ValueType::kInt64 || column.type == ValueType::kDate);
      int64_t raw = 0;
      if (!read_col_int_cell(column, row, fast, &raw)) { return false;
}
      key->parts[key->count++] = AddJoinKeyBias(raw, biases[i]);
    }
    return true;
  };
  const auto visit_right_int_keys = [&](auto&& on_row) {
    if (right_col_only) {
      const slot_t rc0 = right_columns[0];
      const int64_t bias0 = right_key_offsets[0];
      uint32_t base_r = 0;
      for (const ColumnarChunk& ch : right_arg.columnar->chunks) {
        const ColumnarColumn& kc = ch.columns[rc0];
        const bool fast =
            kc.is_uniform && !kc.is_unsigned &&
            (kc.type == ValueType::kInt64 || kc.type == ValueType::kDate);
        for (size_t r = 0; r < ch.rows; ++r) {
          int64_t raw_k = 0;
          if (!read_col_int_cell(kc, r, fast, &raw_k)) {
            continue;
          }
          if (!on_row(base_r + static_cast<uint32_t>(r),
                      AddJoinKeyBias(raw_k, bias0))) {
            return;
          }
        }
        base_r += static_cast<uint32_t>(ch.rows);
      }
      return;
    }
    for (const Row* row_ptr : build_rows) {
      if (HasNullKey(*row_ptr, right_columns)) {
        continue;
      }
      const int64_t bkey = AddJoinKeyBias(
          IntegerJoinKey(*row_ptr, right_columns[0]), right_key_offsets[0]);
      if (!on_row(static_cast<uint32_t>(row_ptr - right_ptr->rows.data()),
                  bkey)) {
        return;
      }
    }
  };
  const auto visit_right_packed_2x32_keys = [&](auto&& on_row) {
    if (!validated_packed_keys.empty()) {
      for (size_t row = 0; row < validated_packed_keys.size(); ++row) {
        const int64_t key = validated_packed_keys[row];
        if (key != kInvalidFusedPacked2x32 &&
            !on_row(static_cast<uint32_t>(row), key)) {
          return;
}
      }
      return;
    }
    if (right_col_only) {
      const slot_t rc0 = right_columns[0];
      const slot_t rc1 = right_columns[1];
      const int64_t bias0 = right_key_offsets[0];
      const int64_t bias1 = right_key_offsets[1];
      uint32_t base_r = 0;
      for (const ColumnarChunk& ch : right_arg.columnar->chunks) {
        const ColumnarColumn& kc0 = ch.columns[rc0];
        const ColumnarColumn& kc1 = ch.columns[rc1];
        const bool fast0 =
            kc0.is_uniform && !kc0.is_unsigned &&
            (kc0.type == ValueType::kInt64 || kc0.type == ValueType::kDate);
        const bool fast1 =
            kc1.is_uniform && !kc1.is_unsigned &&
            (kc1.type == ValueType::kInt64 || kc1.type == ValueType::kDate);
        for (size_t r = 0; r < ch.rows; ++r) {
          int64_t v0 = 0;
          int64_t v1 = 0;
          if (!read_col_int_cell(kc0, r, fast0, &v0) ||
              !read_col_int_cell(kc1, r, fast1, &v1)) {
            continue;
          }
          const int64_t bkey = pack_fused_2x32(AddJoinKeyBias(v0, bias0),
                                               AddJoinKeyBias(v1, bias1));
          if (!on_row(base_r + static_cast<uint32_t>(r), bkey)) {
            return;
          }
        }
        base_r += static_cast<uint32_t>(ch.rows);
      }
      return;
    }
    for (const Row* row_ptr : build_rows) {
      const Row& row = *row_ptr;
      if (HasNullKey(row, right_columns)) {
        continue;
      }
      const int64_t k0 = AddJoinKeyBias(IntegerJoinKey(row, right_columns[0]),
                                        right_key_offsets[0]);
      const int64_t k1 = AddJoinKeyBias(IntegerJoinKey(row, right_columns[1]),
                                        right_key_offsets[1]);
      if (!on_row(static_cast<uint32_t>(row_ptr - right_ptr->rows.data()),
                  pack_fused_2x32(k0, k1))) {
        return;
      }
    }
  };
  const auto build_buckets = [&]() {
    const size_t b_count = right_build_count();
    if (bulk_raw_count) {
      const auto visit_bulk = [&](auto&& on_bulk) {
        if (right_col_only) {
          const slot_t rc0 = right_columns[0];
          const int64_t bias0 = right_key_offsets[0];
          const std::optional<slot_t> count_rc = count_right_column;
          uint32_t base_r = 0;
          for (const ColumnarChunk& ch : right_arg.columnar->chunks) {
            const ColumnarColumn& kc = ch.columns[rc0];
            const ColumnarColumn* cnt_c =
                count_rc.has_value() ? &ch.columns[*count_rc] : nullptr;
            const bool fast =
                kc.is_uniform && !kc.is_unsigned &&
                (kc.type == ValueType::kInt64 || kc.type == ValueType::kDate);
            for (size_t r = 0; r < ch.rows; ++r) {
              int64_t raw_k = 0;
              if (!read_col_int_cell(kc, r, fast, &raw_k)) {
                continue;
              }
              const int64_t add =
                  (cnt_c == nullptr || !cnt_c->IsNull(r)) ? 1 : 0;
              on_bulk(base_r + static_cast<uint32_t>(r),
                      AddJoinKeyBias(raw_k, bias0), add);
            }
            base_r += static_cast<uint32_t>(ch.rows);
          }
          return;
        }
        for (const Row* row_ptr : build_rows) {
          const Row& row = *row_ptr;
          if (HasNullKey(row, right_columns)) {
            continue;
          }
          const int64_t bkey = AddJoinKeyBias(
              IntegerJoinKey(row, right_columns[0]), right_key_offsets[0]);
          const int64_t add = (!count_right_column.has_value() ||
                               !row.values_[*count_right_column].IsNull())
                                  ? 1
                                  : 0;
          on_bulk(static_cast<uint32_t>(row_ptr - right_ptr->rows.data()), bkey,
                  add);
        }
      };
      if (!left.HasSpill() && !left.HasColumnarOnly() && !left.rows.empty()) {
        int64_t min_k = std::numeric_limits<int64_t>::max();
        int64_t max_k = std::numeric_limits<int64_t>::min();
        for (const Row& lrow : left.rows) {
          if (HasNullKey(lrow, left_columns)) {
            continue;
          }
          const int64_t k = AddJoinKeyBias(
              IntegerJoinKey(lrow, left_columns[0]), left_key_offsets[0]);
          min_k = std::min(min_k, k);
          max_k = std::max(max_k, k);
        }
        if (min_k <= max_k) {
          const uint64_t span = static_cast<uint64_t>(max_k) -
                                static_cast<uint64_t>(min_k) + 1ULL;
          if (span <= 2'000'000ULL &&
              span <=
                  (static_cast<uint64_t>(left.rows.size()) * 8ULL) + 1024ULL) {
            dense_bulk_min = min_k;
            dense_bulk_counts.resize(static_cast<size_t>(span));
            use_dense_bulk = true;
            visit_bulk([&](uint32_t r_idx, int64_t build_key, int64_t add) {
              if (build_key < min_k || build_key > max_k) {
                return;
              }
              BulkCountValue& slot = dense_bulk_counts[static_cast<size_t>(
                  static_cast<uint64_t>(build_key) -
                  static_cast<uint64_t>(min_k))];
              ++slot.pairs;
              slot.count += add;
              slot.r_idx = r_idx;
            });
            return;
          }
        }
      }
      // The duplicate count and the bucket-head row stand in for the whole
      // bucket chain: the probe never touches an Entry or a build Row.
      bulk_counts.emplace();
      bulk_counts->Reserve(b_count);
      visit_bulk([&](uint32_t r_idx, int64_t build_key, int64_t add) {
        bool inserted = false;
        BulkCountValue* slot = bulk_counts->TryEmplace(build_key, &inserted);
        slot->count = inserted ? add : slot->count + add;
        slot->pairs = inserted ? 1 : slot->pairs + 1;
        slot->r_idx = r_idx;
      });
      return;
    }
    if (integer_key) {
      // Gathered columns can require several indirections per cell. Decode
      // build keys once when range discovery and direct-index construction
      // will make multiple passes over the same column.
      std::vector<std::pair<int64_t, int64_t>> key_bounds;
      if (deferred_left_probe != nullptr && right_col_only &&
          b_count >= 262144 && b_count <= 4194304) {
        key_cache_charge = TryReserveQueryMemory(b_count * sizeof(BuildKey));
        if (key_cache_charge) {
          const auto& chunks = right_arg.columnar->chunks;
          std::vector<uint32_t> starts(chunks.size() + 1, 0);
          for (size_t i = 0; i < chunks.size(); ++i) {
            starts[i + 1] = starts[i] + chunks[i].rows;
}
          const size_t tasks =
              std::min(workers, std::max<size_t>(1, chunks.size()));
          key_cache.resize(tasks);
          key_bounds.assign(tasks, {INT64_MAX, INT64_MIN});
          const Status decoded = RunParallelTasks(tasks, [&](size_t w) {
            const size_t begin = chunks.size() * w / tasks;
            const size_t end = chunks.size() * (w + 1) / tasks;
            auto& keys = key_cache[w];
            auto& [low, high] = key_bounds[w];
            keys.reserve(starts[end] - starts[begin]);
            for (size_t ci = begin; ci < end; ++ci) {
              const auto& column = chunks[ci].columns[right_columns[0]];
              const bool fast = column.is_uniform && !column.is_unsigned &&
                                (column.type == ValueType::kInt64 ||
                                 column.type == ValueType::kDate);
              for (size_t r = 0; r < chunks[ci].rows; ++r) {
                int64_t raw = 0;
                if (!read_col_int_cell(column, r, fast, &raw)) { continue;
}
                const int64_t key = AddJoinKeyBias(raw, right_key_offsets[0]);
                keys.push_back({key, starts[ci] + static_cast<uint32_t>(r)});
                low = std::min(low, key);
                high = std::max(high, key);
              }
            }
          });
          if (!decoded.ok()) {
            key_cache.clear();
            key_cache_charge.reset();
          }
        }
      }
      const auto visit_build_int_keys = [&](auto&& on_row) {
        if (!key_cache_charge) {
          visit_right_int_keys(on_row);
          return;
        }
        for (const auto& chunk : key_cache) {
          for (const auto& entry : chunk) {
            if (!on_row(entry.row, entry.key)) { return;
}
          }
        }
      };
      if (!left.HasSpill() && b_count > 0) {
        int64_t min_k = std::numeric_limits<int64_t>::max();
        int64_t max_k = std::numeric_limits<int64_t>::min();
        if (key_cache_charge) {
          for (const auto& [low, high] : key_bounds) {
            min_k = std::min(min_k, low);
            max_k = std::max(max_k, high);
          }
        } else {
          visit_build_int_keys([&](uint32_t, int64_t bkey) {
            min_k = std::min(min_k, bkey);
            max_k = std::max(max_k, bkey);
            return true;
          });
        }
        if (min_k <= max_k) {
          const uint64_t span = static_cast<uint64_t>(max_k) -
                                static_cast<uint64_t>(min_k) + 1ULL;
          // A wide sparse domain needs a membership bitmap and ranks, not
          // one row slot for every possible key. Rank selects the row index
          // with one popcount while keeping this direct lookup cache-sized.
          if (deferred_left_probe != nullptr && !fusion->group_keys.empty() &&
              !fusion->aggregates.empty() && span > 1'048'576ULL &&
              span <= 134'217'728ULL &&
              span <= static_cast<uint64_t>(b_count) * 64ULL) {
            const auto words = static_cast<size_t>((span + 63) / 64);
            const size_t bytes = (words * 12) + (b_count * 4);
            dense_int_charge = TryReserveQueryMemory(bytes);
            if (dense_int_charge) {
              ranked_int_bits.assign(words, 0);
              bool unique = true;
              if (key_cache_charge && key_cache.size() > 1) {
                // Adjacent chunk partitions may share bitmap words, and
                // duplicate keys can occur in different partitions. Atomic
                // OR preserves both membership and the uniqueness proof.
                std::atomic<bool> duplicate{false};
                const Status indexed =
                    RunParallelTasks(key_cache.size(), [&](size_t w) {
                      for (const auto& entry : key_cache[w]) {
                        const uint64_t off = static_cast<uint64_t>(entry.key) -
                                             static_cast<uint64_t>(min_k);
                        const uint64_t bit = uint64_t{1} << (off & 63);
                        std::atomic_ref<uint64_t> word(
                            ranked_int_bits[off >> 6]);
                        if ((word.fetch_or(bit, std::memory_order_relaxed) &
                             bit) != 0) {
                          duplicate.store(true, std::memory_order_relaxed);
}
                      }
                    });
                unique =
                    indexed.ok() && !duplicate.load(std::memory_order_relaxed);
              } else {
                visit_build_int_keys([&](uint32_t, int64_t key) {
                  const uint64_t off =
                      static_cast<uint64_t>(key) - static_cast<uint64_t>(min_k);
                  auto& word = ranked_int_bits[off >> 6];
                  const uint64_t bit = uint64_t{1} << (off & 63);
                  if ((word & bit) != 0) {
                    unique = false;
                    return false;
                  }
                  word |= bit;
                  return true;
                });
              }
              if (unique) {
                dense_int_min = min_k;
                ranked_int_span = span;
                use_dense_int_rows = true;
                build_int_keys_unique = true;
                return;
              }
              std::vector<uint64_t>().swap(ranked_int_bits);
              dense_int_charge.reset();
            }
          }
          if (span <= 1'048'576ULL) {
            dense_int_min = min_k;
            dense_int_idxs.assign(static_cast<size_t>(span), kInvalidRightIdx);
            bool unique = true;
            visit_build_int_keys([&](uint32_t r_idx, int64_t bkey) {
              const auto idx = static_cast<size_t>(
                  static_cast<uint64_t>(bkey) - static_cast<uint64_t>(min_k));
              if (dense_int_idxs[idx] != kInvalidRightIdx) {
                unique = false;
                return false;
              }
              dense_int_idxs[idx] = r_idx;
              return true;
            });
            if (unique) {
              use_dense_int_rows = true;
              build_int_keys_unique = true;
              return;
            }
            std::vector<uint32_t>().swap(dense_int_idxs);
            dense_int_charge.reset();
          }
        }
        if (!right_ptr->HasSpill() && b_count <= 524288) {
          size_t cap = 16;
          while (cap < b_count * 2) {
            cap <<= 1;
          }
          unique_int_table.assign(cap, UniqueIntSlot{});
          unique_int_mask = cap - 1;
          bool unique = true;
          visit_build_int_keys([&](uint32_t r_idx, int64_t bkey) {
            size_t slot = hash_unique_int_key(bkey) & unique_int_mask;
            while (unique_int_table[slot].r_idx !=
                   std::numeric_limits<uint32_t>::max()) {
              if (unique_int_table[slot].key == bkey) {
                unique = false;
                return false;
              }
              slot = (slot + 1) & unique_int_mask;
            }
            unique_int_table[slot].key = bkey;
            unique_int_table[slot].r_idx = r_idx;
            return true;
          });
          if (unique) {
            use_unique_int_table = true;
            build_int_keys_unique = true;
            return;
          }
          unique_int_table.clear();
        }
      }
      int_buckets.emplace();
      int_buckets->reserve(b_count);
      visit_build_int_keys([&](uint32_t r_idx, int64_t bkey) {
        if (!int_buckets->emplace(bkey, r_idx)) {
          build_int_keys_unique = false;
        }
        return true;
      });
    } else if (composite_int_key) {
      bool can_pack = (right_columns.size() == 2);
      if (can_pack && !right_col_only) {
        for (const Row* row_ptr : build_rows) {
          const Row& row = *row_ptr;
          if (HasNullKey(row, right_columns)) {
            continue;
          }
          const int64_t k0 = AddJoinKeyBias(
              IntegerJoinKey(row, right_columns[0]), right_key_offsets[0]);
          const int64_t k1 = AddJoinKeyBias(
              IntegerJoinKey(row, right_columns[1]), right_key_offsets[1]);
          if (!fits_fused_i32(k0) || !fits_fused_i32(k1) ||
              pack_fused_2x32(k0, k1) == kInvalidFusedPacked2x32) {
            can_pack = false;
            break;
          }
        }
      }
      if (can_pack) {
        packed_2x32_int_key = true;
        if (!left.HasSpill() && !right_ptr->HasSpill() && b_count > 0 &&
            b_count <= 524288) {
          size_t cap = 16;
          while (cap < b_count * 2) {
            cap <<= 1;
          }
          unique_int_table.assign(cap, UniqueIntSlot{});
          unique_int_mask = cap - 1;
          bool unique = true;
          visit_right_packed_2x32_keys([&](uint32_t r_idx, int64_t bkey) {
            size_t slot = hash_unique_int_key(bkey) & unique_int_mask;
            while (unique_int_table[slot].r_idx !=
                   std::numeric_limits<uint32_t>::max()) {
              if (unique_int_table[slot].key == bkey) {
                unique = false;
                return false;
              }
              slot = (slot + 1) & unique_int_mask;
            }
            unique_int_table[slot].key = bkey;
            unique_int_table[slot].r_idx = r_idx;
            return true;
          });
          if (unique) {
            use_unique_int_table = true;
            build_int_keys_unique = true;
            return;
          }
          unique_int_table.clear();
        }
        int_buckets.emplace();
        int_buckets->reserve(b_count);
        visit_right_packed_2x32_keys([&](uint32_t r_idx, int64_t bkey) {
          int_buckets->emplace(bkey, r_idx);
          return true;
        });
      } else {
        if (right_col_only && left.HasColumnarOnly() && b_count > 0 &&
            b_count <= 4 * 1024 * 1024) {
          composite_keys_charge =
              TryReserveQueryMemory(b_count * sizeof(CompositeIntKey));
          if (composite_keys_charge) {
            dense_composite_keys.resize(b_count);
            int64_t minimum = std::numeric_limits<int64_t>::max();
            int64_t maximum = std::numeric_limits<int64_t>::min();
            size_t base = 0;
            bool any = false;
            for (const auto& chunk : right_arg.columnar->chunks) {
              for (size_t r = 0; r < chunk.rows; ++r) {
                CompositeIntKey key;
                if (!read_col_composite_key(chunk, right_columns,
                                            right_key_offsets, r, &key)) {
                  continue;
}
                dense_composite_keys[base + r] = key;
                minimum = std::min(minimum, key.parts[0]);
                maximum = std::max(maximum, key.parts[0]);
                any = true;
              }
              base += chunk.rows;
            }
            const uint64_t span =
                static_cast<uint64_t>(maximum) - static_cast<uint64_t>(minimum);
            if (any && span < 16 * 1024 * 1024 && span < b_count * 16) {
              composite_rows_charge =
                  TryReserveQueryMemory((span + 1) * sizeof(uint32_t));
              if (composite_rows_charge) {
                dense_composite_rows.assign(span + 1, kInvalidRightIdx);
                bool unique = true;
                for (size_t r = 0; r < b_count; ++r) {
                  const auto& key = dense_composite_keys[r];
                  if (key.count == 0) { continue;
}
                  const uint64_t offset = static_cast<uint64_t>(key.parts[0]) -
                                          static_cast<uint64_t>(minimum);
                  if (dense_composite_rows[offset] != kInvalidRightIdx) {
                    unique = false;
                    break;
                  }
                  dense_composite_rows[offset] = r;
                }
                if (unique) {
                  dense_composite_min = minimum;
                  use_dense_composite = true;
                  return;
                }
              }
            }
            std::vector<uint32_t>().swap(dense_composite_rows);
            composite_rows_charge.reset();
          }
        }
        composite_buckets.emplace();
        composite_buckets->reserve(b_count);
        if (!dense_composite_keys.empty()) {
          // Reuse decoded keys when the optional dense layout was unsuitable.
          for (size_t r = 0; r < dense_composite_keys.size(); ++r) {
            if (dense_composite_keys[r].count != 0) {
              composite_buckets->emplace(dense_composite_keys[r], r);
}
          }
          std::vector<CompositeIntKey>().swap(dense_composite_keys);
          composite_keys_charge.reset();
        } else if (right_col_only) {
          uint32_t base = 0;
          for (const auto& chunk : right_arg.columnar->chunks) {
            for (size_t r = 0; r < chunk.rows; ++r) {
              CompositeIntKey key;
              if (!read_col_composite_key(chunk, right_columns,
                                          right_key_offsets, r, &key)) {
                continue;
}
              composite_buckets->emplace(key, base + static_cast<uint32_t>(r));
            }
            base += chunk.rows;
          }
        } else {
          for (const Row* row_ptr : build_rows) {
            const Row& row = *row_ptr;
            if (HasNullKey(row, right_columns)) { continue;
}
            composite_buckets->emplace(
                EncodeCompositeIntKey(row, right_columns, &right_key_offsets),
                static_cast<uint32_t>(row_ptr - right_ptr->rows.data()));
          }
        }
      }
    } else {
      key_buckets.emplace();
      key_buckets->reserve(b_count);
      for (const Row* row_ptr : build_rows) {
        const Row& row = *row_ptr;
        if (HasNullKey(row, right_columns)) {
          continue;
        }
        key_buckets->emplace(
            EncodeJoinKey(row, right_columns, &right_key_offsets),
            static_cast<uint32_t>(row_ptr - right_ptr->rows.data()));
      }
    }
  };
  if (!singleton_right_relation) {
    build_buckets();
  }

  const auto phase_buckets_done = std::chrono::steady_clock::now();

  // A proven unique build bounds the append log by the number of probe
  // rows. Charge vector growth slack before enabling it, and retain local
  // aggregation when the query memory budget cannot cover the log.
  static const bool compact_log_disabled =
      std::getenv("TINYLAMB_DISABLE_COMPACT_COUNT_LOG") != nullptr;
  const size_t log_entry_bytes =
      compact_split_key_only ? sizeof(uint64_t) : sizeof(CompactCountGroup);
  auto compact_log_charge =
      compact_counts && !compact_log_disabled && workers > 1 &&
              !left.HasSpill() &&
              (use_dense_int_rows || use_unique_int_table) &&
              total_left <= SIZE_MAX / (2 * log_entry_bytes)
          ? TryReserveQueryMemory(total_left * 2 * log_entry_bytes)
          : nullptr;
  compact_log_allowed = compact_log_charge != nullptr;

  constexpr uint32_t kInvalidGid = std::numeric_limits<uint32_t>::max();
  std::vector<uint32_t> right_row_gid;
  FusedRawArena pre_raw_arena;
  pre_raw_arena.has_stat = has_stat_raw_agg;
  pre_raw_arena.has_double = !raw_sum_fusion || has_double_raw_agg;
  pre_raw_arena.has_int = !raw_sum_fusion || has_int_raw_agg;
  std::vector<FusedGroup> pre_groups;
  bool identity_right_gid = false;
  const size_t active_build_count = right_build_count();
  // Dense per-worker states scale with bytes, not a fixed number of groups.
  // Larger unique build domains used to construct millions of FusedGroups
  // only to discard them at the fixed 512K cutoff below.
  const size_t direct_slots =
      raw_sum_fusion ? raw_slot_count : fusion->aggregates.size();
  const size_t direct_state_bytes =
      raw_count_fusion
          ? 9
          : 1 + (direct_slots *
                    (9 + (has_double_raw_agg ? 8 : 0) +
                     (has_int_raw_agg ? 8 : 0) +
                     (has_stat_raw_agg ? 2 * sizeof(platform::ExtendedFloat)
                                       : 0)));
  size_t direct_group_limit = 524288;
  std::shared_ptr<QueryMemoryCharge> extended_direct_charge;
  if (right_row_count > direct_group_limit && workers > 0 &&
      right_row_count <= (size_t{256} << 20) / workers / direct_state_bytes &&
      integer_key && build_int_keys_unique && right_only_group_keys &&
      all_direct_group_keys && !has_count_distinct) {
    extended_direct_charge =
        TryReserveQueryMemory(workers * right_row_count * direct_state_bytes);
    if (extended_direct_charge) { direct_group_limit = right_row_count;
}
  }
  bool use_preassigned_build_groups =
      join_type == JoinType::kInner && !left.HasSpill() &&
      !right_ptr->HasSpill() && right_only_group_keys &&
      skip_accumulator_states && residual_programs.empty() && !bulk_raw_count &&
      active_build_count > 0;
  const auto for_each_build_nonnull_idx = [&](auto&& on_idx) -> Status {
    if (right_col_only) {
      if (integer_key) {
        const slot_t rc0 = right_columns[0];
        uint32_t base_r = 0;
        for (const ColumnarChunk& ch : right_arg.columnar->chunks) {
          const ColumnarColumn& kc = ch.columns[rc0];
          const bool fast =
              kc.is_uniform && !kc.is_unsigned &&
              (kc.type == ValueType::kInt64 || kc.type == ValueType::kDate);
          for (size_t r = 0; r < ch.rows; ++r) {
            int64_t dummy = 0;
            if (read_col_int_cell(kc, r, fast, &dummy)) {
              RETURN_IF_FAIL(on_idx(base_r + static_cast<uint32_t>(r)));
            }
          }
          base_r += static_cast<uint32_t>(ch.rows);
        }
        return Status::kSuccess;
      }
      if (right_columns.size() > 2) {
        uint32_t base = 0;
        Value key_scratch;
        for (const auto& chunk : right_arg.columnar->chunks) {
          for (size_t r = 0; r < chunk.rows; ++r) {
            if (!ColumnarHasNullOrNaN(chunk, right_columns, r, &key_scratch))
              RETURN_IF_FAIL(on_idx(base + static_cast<uint32_t>(r)));
          }
          base += chunk.rows;
        }
        return Status::kSuccess;
      }
      const slot_t rc0 = right_columns[0];
      const slot_t rc1 = right_columns[1];
      uint32_t base_r = 0;
      for (const ColumnarChunk& ch : right_arg.columnar->chunks) {
        const ColumnarColumn& kc0 = ch.columns[rc0];
        const ColumnarColumn& kc1 = ch.columns[rc1];
        const bool fast0 =
            kc0.is_uniform && !kc0.is_unsigned &&
            (kc0.type == ValueType::kInt64 || kc0.type == ValueType::kDate);
        const bool fast1 =
            kc1.is_uniform && !kc1.is_unsigned &&
            (kc1.type == ValueType::kInt64 || kc1.type == ValueType::kDate);
        for (size_t r = 0; r < ch.rows; ++r) {
          int64_t d0 = 0;
          int64_t d1 = 0;
          if (read_col_int_cell(kc0, r, fast0, &d0) &&
              read_col_int_cell(kc1, r, fast1, &d1)) {
            RETURN_IF_FAIL(on_idx(base_r + static_cast<uint32_t>(r)));
          }
        }
        base_r += static_cast<uint32_t>(ch.rows);
      }
      return Status::kSuccess;
    }
    for (const Row* row_ptr : build_rows) {
      if (!HasNullKey(*row_ptr, right_columns)) {
        RETURN_IF_FAIL(
            on_idx(static_cast<uint32_t>(row_ptr - right_ptr->rows.data())));
      }
    }
    return Status::kSuccess;
  };
  if (use_preassigned_build_groups) {
    if (key_programs.empty()) {
      right_row_gid.assign(right_row_count, kInvalidGid);
      FusedGroup g;
      g.rep_left = (deferred_left_probe != nullptr || left.HasColumnarOnly() ||
                    left.rows.empty())
                       ? nullptr
                       : left.rows.data();
      g.rep_right_idx =
          right_col_only
              ? 0U
              : static_cast<uint32_t>(build_rows[0] - right_ptr->rows.data());
      pre_groups.push_back(std::move(g));
      (void)for_each_build_nonnull_idx([&](uint32_t r_idx) -> Status {
        right_row_gid[r_idx] = 0;
        return Status::kSuccess;
      });
    } else if (integer_key && build_int_keys_unique && all_direct_group_keys &&
               !has_count_distinct && right_row_count <= direct_group_limit &&
               std::ranges::any_of(key_shapes, [&](const ProgramShape& ks) {
                 return std::cmp_equal(ks.direct_column,
                                       left_width + right_columns[0]);
               })) {
      identity_right_gid = true;
    } else if (integer_key && build_int_keys_unique && all_direct_group_keys &&
               std::ranges::any_of(key_shapes, [&](const ProgramShape& ks) {
                 return std::cmp_equal(ks.direct_column,
                                       left_width + right_columns[0]);
               })) {
      right_row_gid.assign(right_row_count, kInvalidGid);
      pre_groups.reserve(active_build_count);
      (void)for_each_build_nonnull_idx([&](uint32_t r_idx) -> Status {
        const auto gid = static_cast<uint32_t>(pre_groups.size());
        right_row_gid[r_idx] = gid;
        FusedGroup g;
        g.rep_right_idx = r_idx;
        pre_groups.push_back(std::move(g));
        return Status::kSuccess;
      });
    } else {
      right_row_gid.assign(right_row_count, kInvalidGid);
      FusedWorkerLocal pre_local;
      pre_local.raw_arena.has_stat = has_stat_raw_agg;
      pre_local.raw_arena.has_double = !raw_sum_fusion || has_double_raw_agg;
      pre_local.raw_arena.has_int = !raw_sum_fusion || has_int_raw_agg;
      pre_local.int_offsets.Reserve(active_build_count);
      pre_local.groups.reserve(active_build_count);
      Row& pre_scratch = scratch_storage[0];
      const Status pre_st =
          for_each_build_nonnull_idx([&](uint32_t r_idx) -> Status {
            if (!all_direct_group_keys) {
              Value pre_tmp;
              for (const slot_t cell : scratch_cells) {
                if (cell >= left_width) {
                  pre_scratch.values_[cell] =
                      read_right_cell(r_idx, cell - left_width, &pre_tmp);
                }
              }
            }
            size_t gid = 0;
            RETURN_IF_FAIL(resolve_group_index(pre_local, pre_scratch, nullptr,
                                               r_idx, true, nullptr, &gid));
            right_row_gid[r_idx] = static_cast<uint32_t>(gid);
            return Status::kSuccess;
          });
      if (!pre_st.ok()) {
        use_preassigned_build_groups = false;
        right_row_gid.clear();
        pre_groups.clear();
      } else {
        pre_groups = std::move(pre_local.groups);
        pre_raw_arena = std::move(pre_local.raw_arena);
      }
    }
  }
  const size_t pre_group_count =
      identity_right_gid ? right_row_count : pre_groups.size();
  const bool use_direct_gid_agg = use_preassigned_build_groups &&
                                  !has_count_distinct && pre_group_count > 0 &&
                                  pre_group_count <= direct_group_limit;
  const bool use_partitioned_count_distinct =
      use_preassigned_build_groups && has_count_distinct &&
      fusion->aggregates.size() == 1 &&
      raw_agg_kinds[0] == RawAggKind::kCountDistinctInt64 &&
      pre_group_count > 0;
  if (use_preassigned_build_groups && !use_direct_gid_agg &&
      !use_partitioned_count_distinct) {
    use_preassigned_build_groups = false;
    identity_right_gid = false;
    right_row_gid.clear();
    pre_groups.clear();
  }
  struct DirectDoubleCell {
    double sum{0.0};
    uint64_t meta{0};  // bit 0: matched, bit 1: inf, bits 2..63: count
  };
  struct DirectGidWorker {
    std::vector<uint8_t> matched;
    std::vector<double> doubles;
    std::vector<int64_t> ints;
    std::vector<int64_t> counts;
    std::vector<uint8_t> inf_flags;
    std::vector<platform::ExtendedFloat> ext_sx;
    std::vector<platform::ExtendedFloat> ext_sxx;
    std::vector<DirectDoubleCell> double_cells;
  };
  struct GidValPair {
    uint32_t gid{0};
    int64_t val{0};
    bool operator<(const GidValPair& o) const noexcept {
      return gid != o.gid ? gid < o.gid : val < o.val;
    }
    bool operator==(const GidValPair& o) const noexcept {
      return gid == o.gid && val == o.val;
    }
  };
  const size_t agg_count =
      raw_sum_fusion ? raw_slot_count : fusion->aggregates.size();
  bool all_slot0_sum_double = raw_sum_fusion && agg_count == 1 &&
                              slot_agg_kinds[0] == RawAggKind::kSumDouble;
  if (all_slot0_sum_double) {
    for (size_t oi = 0; oi < fusion->aggregates.size(); ++oi) {
      if (raw_agg_slot[oi] == 0 &&
          raw_agg_kinds[oi] != RawAggKind::kSumDouble) {
        all_slot0_sum_double = false;
        break;
      }
    }
  }
  const bool single_double_sum =
      all_slot0_sum_double &&
      (child_shapes[raw_slot_origin[0]].direct_column >= 0 ||
       child_shapes[raw_slot_origin[0]].mul_col0 >= 0 ||
       (raw_slot_origin[0] < double_arithmetic.size() &&
        double_arithmetic[raw_slot_origin[0]]));
  // One shared group array takes a lock on every probe row. Multi-aggregate
  // probes keep a private partial per worker and fold them once. A lone
  // SUM(double) stays on the shared input-ordered log so its prefixes do
  // not change. Huge domains keep the shared array.
  const bool many_direct_groups =
      use_direct_gid_agg && pre_group_count > 4096 && workers > 1;
  const size_t private_partial_bytes =
      workers * pre_group_count * direct_state_bytes;
  // A lone SUM(double) keeps one shared array when the probe must follow
  // input order. A deferred fact probe has no row order to preserve, so each
  // worker keeps its own partial and the cells are added without a lock.
  // Floating-point SUM/AVG partials are not associative: with dynamic chunk
  // scheduling each worker folds a different subset, so the merge order (and
  // thus the answer) changes per run. Keep every DOUBLE aggregate on the
  // single shared, input-ordered fold so the result is deterministic and
  // matches the canonical accumulator. `single_double_sum` below is the lone
  // SUM(double) special case; `has_double_raw_agg` covers AVG/multi-agg shapes.
  const bool private_partials =
      many_direct_groups && !has_double_raw_agg &&
      (!single_double_sum || deferred_left_probe != nullptr ||
       left.HasColumnarOnly()) &&
      private_partial_bytes <= (size_t{256} << 20);
  const bool use_shared_direct_gid = many_direct_groups && !private_partials;
  const size_t direct_gid_worker_count =
      use_direct_gid_agg ? (use_shared_direct_gid ? 1 : workers) : 0;
  std::vector<DirectGidWorker> direct_gid_workers;
  const auto init_direct_gid_workers = [&](size_t worker_count,
                                           size_t domain_count,
                                           bool packed_double = false) {
    direct_gid_workers.resize(worker_count);
    const auto init_one = [&](DirectGidWorker& dw) {
      if (packed_double) {
        dw.double_cells.assign(domain_count, DirectDoubleCell{});
        return;
      }
      dw.matched.assign(domain_count, 0);
      if (raw_count_fusion) {
        dw.counts.assign(domain_count, 0);
      } else {
        if (has_double_raw_agg) {
          dw.doubles.assign(domain_count * agg_count, 0.0);
        }
        if (has_int_raw_agg) {
          dw.ints.assign(domain_count * agg_count, 0);
        }
        if (has_stat_raw_agg) {
          dw.ext_sx.assign(domain_count * agg_count,
                           platform::ExtendedFloat{0});
          dw.ext_sxx.assign(domain_count * agg_count,
                            platform::ExtendedFloat{0});
        }
        dw.counts.assign(domain_count * agg_count, 0);
        dw.inf_flags.assign(domain_count * agg_count, 0);
      }
    };
    if (worker_count > 1 && domain_count >= 8192) {
      (void)RunParallelTasks(
          worker_count, [&](size_t wi) { init_one(direct_gid_workers[wi]); });
    } else {
      for (DirectGidWorker& dw : direct_gid_workers) {
        init_one(dw);
      }
    }
  };
  std::vector<std::vector<std::vector<GidValPair>>> cd_worker_parts(
      use_partitioned_count_distinct ? workers : 0);
  std::vector<std::vector<uint8_t>> cd_worker_matched(
      use_partitioned_count_distinct ? workers : 0);
  if (use_partitioned_count_distinct) {
    for (size_t w = 0; w < workers; ++w) {
      cd_worker_parts[w].resize(workers);
      cd_worker_matched[w].assign(pre_group_count, 0);
    }
  }
  struct OrderedSumInput {
    double value{0};
    uint32_t gid{kInvalidGid};
    bool nonnull{false};
  };
  std::shared_ptr<QueryMemoryCharge> ordered_sum_charge;
  std::vector<OrderedSumInput> ordered_sum_inputs;
  if (use_shared_direct_gid && single_double_sum && integer_key &&
      build_int_keys_unique && deferred_left_probe == nullptr &&
      !left.HasColumnarOnly() &&
      left.rows.size() <= SIZE_MAX / sizeof(OrderedSumInput)) {
    ordered_sum_charge =
        TryReserveQueryMemory(left.rows.size() * sizeof(OrderedSumInput));
    if (ordered_sum_charge) {
      ordered_sum_inputs.resize(left.rows.size());
    }
  }
  const auto accumulate_preassigned_pair = [&](size_t w, Row& scratch,
                                               const Row& left_row,
                                               uint32_t r_idx) -> Status {
    const Row* right_row = right_row_ptr(r_idx);
    const uint32_t gid = identity_right_gid ? r_idx : right_row_gid[r_idx];
    if (gid == kInvalidGid) [[unlikely]] {
      return Status::kSuccess;
    }
    // The group id is already the build row's. COUNT(*) never reads a cell,
    // so copying key strings into the scratch row on every fact row is wasted
    // (Q23's item-description grouping).
    if (!count_star_only && (!mask_scratch || !scratch_cells.empty())) {
      Value cell_tmp;
      for (const slot_t cell : scratch_cells) {
        scratch.values_[cell] =
            cell < left_width
                ? left_row.values_[cell]
                : read_right_cell(r_idx, cell - left_width, &cell_tmp);
      }
    }
    if (use_partitioned_count_distinct) {
      cd_worker_matched[w][gid] = 1;
      int64_t iv = 0;
      bool is_null = false;
      if (child_shapes[0].direct_column >= 0) {
        Value v_tmp;
        const Value& v = read_pair_cell(
            &left_row, r_idx,
            static_cast<slot_t>(child_shapes[0].direct_column), &v_tmp);
        is_null = v.IsNull();
        if (!is_null) {
          iv = v.value.int_value;
        }
      } else {
        ASSIGN_OR_RETURN(Value, child,
                         eval_pair_program(child_programs[0], child_shapes[0],
                                           scratch, &left_row, right_row));
        is_null = child.IsNull();
        if (!is_null) {
          iv = child.value.int_value;
        }
      }
      if (!is_null) {
        cd_worker_parts[w][gid % workers].push_back(
            GidValPair{.gid = gid, .val = iv});
      }
      return Status::kSuccess;
    }
    if (ordered_sum_charge) {
      // Unique build keys produce at most one entry for this input row.
      // Publish by input position, independently of morsel scheduling.
      auto& item =
          ordered_sum_inputs[static_cast<size_t>(&left_row - left.rows.data())];
      bool is_null = false;
      RETURN_IF_FAIL(eval_pair_double(raw_slot_origin[0], scratch, &left_row,
                                      right_row, &item.value, &is_null));
      item.gid = gid;
      item.nonnull = !is_null;
      return Status::kSuccess;
    }
    DirectGidWorker& dw = direct_gid_workers[use_shared_direct_gid ? 0 : w];
    if (fusion->aggregates.empty()) {
      if (use_shared_direct_gid) {
        __atomic_store_n(&dw.matched[gid], 1, __ATOMIC_RELAXED);
      } else {
        dw.matched[gid] = 1;
      }
      return Status::kSuccess;
    }
    if (count_star_only) {
      if (use_shared_direct_gid) {
        __atomic_store_n(&dw.matched[gid], 1, __ATOMIC_RELAXED);
        __atomic_fetch_add(&dw.counts[gid], 1, __ATOMIC_RELAXED);
      } else {
        dw.matched[gid] = 1;
        dw.counts[gid] += 1;
      }
      return Status::kSuccess;
    }
    if (count_nonnull_only) {
      const bool non_null = !count_right_column.has_value() ||
                            !right_cell_is_null(r_idx, *count_right_column);
      if (use_shared_direct_gid) {
        __atomic_store_n(&dw.matched[gid], 1, __ATOMIC_RELAXED);
        if (non_null) {
          __atomic_fetch_add(&dw.counts[gid], 1, __ATOMIC_RELAXED);
        }
      } else {
        dw.matched[gid] = 1;
        if (non_null) {
          dw.counts[gid] += 1;
        }
      }
      return Status::kSuccess;
    }
    FusedGroupLock group_lock(use_shared_direct_gid ? &dw.matched[gid]
                                                    : nullptr);
    if (!use_shared_direct_gid) {
      dw.matched[gid] = 1;
    }
    if (single_double_sum) {
      double value = 0.0;
      bool is_null = false;
      RETURN_IF_FAIL(eval_pair_double(raw_slot_origin[0], scratch, &left_row,
                                      right_row, &value, &is_null));
      if (!is_null) {
        const double previous = dw.doubles[gid];
        const double next = previous + value;
        if (std::isinf(next) && !std::isinf(previous) && !std::isinf(value)) {
          dw.inf_flags[gid] = 1;
        }
        dw.doubles[gid] = next;
        // A lone SUM observes presence only; AVG and COUNT keep exact counts
        // in the general loop. The merge also needs only zero/nonzero here.
        dw.counts[gid] = 1;
      }
      return Status::kSuccess;
    }
    const size_t base = static_cast<size_t>(gid) * agg_count;
    Value v_tmp;
    for (size_t s = 0; s < agg_count; ++s) {
      const size_t src = raw_slot_origin[s];
      if (case_always_null(src, right_row)) {
        continue;
      }
      switch (slot_agg_kinds[s]) {
        case RawAggKind::kCountStar:
          dw.counts[base + s] += 1;
          break;
        case RawAggKind::kConstantExtremum:
        case RawAggKind::kCountValue: {
          bool non_null = false;
          if (child_shapes[src].direct_column >= 0) {
            const Value& v = read_pair_cell(
                &left_row, r_idx,
                static_cast<slot_t>(child_shapes[src].direct_column), &v_tmp);
            non_null = !v.IsNull();
          } else {
            ASSIGN_OR_RETURN(
                Value, child,
                eval_pair_program(child_programs[src], child_shapes[src],
                                  scratch, &left_row, right_row));
            non_null = !child.IsNull();
          }
          if (non_null) {
            dw.counts[base + s] += 1;
          }
          break;
        }
        case RawAggKind::kSumInt64: {
          int64_t iv = 0;
          bool is_null = false;
          if (child_shapes[src].direct_column >= 0) {
            const Value& v = read_pair_cell(
                &left_row, r_idx,
                static_cast<slot_t>(child_shapes[src].direct_column), &v_tmp);
            is_null = v.IsNull();
            if (!is_null) {
              iv = v.value.int_value;
            }
          } else if (child_shapes[src].case_cond !=
                         ProgramShape::CaseCondKind::kNone &&
                     child_shapes[src].case_val ==
                         ProgramShape::CaseValKind::kIntConst1) {
            bool cond_true = false;
            if (cached_case_cond(src, &left_row, right_row, &cond_true))
                [[likely]] {
              iv = cond_true ? 1 : 0;
            } else {
              ASSIGN_OR_RETURN(
                  Value, child,
                  eval_pair_program(child_programs[src], child_shapes[src],
                                    scratch, &left_row, right_row));
              is_null = child.IsNull();
              if (!is_null) {
                iv = child.value.int_value;
              }
            }
          } else {
            ASSIGN_OR_RETURN(
                Value, child,
                eval_pair_program(child_programs[src], child_shapes[src],
                                  scratch, &left_row, right_row));
            is_null = child.IsNull();
            if (!is_null) {
              iv = child.value.int_value;
            }
          }
          if (!is_null) {
            int64_t next = 0;
            if (__builtin_add_overflow(dw.ints[base + s], iv, &next)) {
              dw.inf_flags[base + s] = 1;
            } else {
              dw.ints[base + s] = next;
            }
            dw.counts[base + s] += 1;
          }
          break;
        }
        case RawAggKind::kSumDouble:
        case RawAggKind::kAvgDouble: {
          double dv = 0.0;
          bool is_null = false;
          RETURN_IF_FAIL(eval_pair_double(src, scratch, &left_row, right_row,
                                          &dv, &is_null));
          if (!is_null) {
            const double prev = dw.doubles[base + s];
            const double next = prev + dv;
            if (std::isinf(next) && !std::isinf(prev) && !std::isinf(dv)) {
              dw.inf_flags[base + s] = 1;
            }
            dw.doubles[base + s] = next;
            dw.counts[base + s] += 1;
          }
          break;
        }
        case RawAggKind::kStddevSamp:
        case RawAggKind::kStddevPop:
        case RawAggKind::kVarSamp:
        case RawAggKind::kVarPop: {
          platform::ExtendedFloat x = 0;
          bool is_null = false;
          RETURN_IF_FAIL(
              eval_pair_ext(src, scratch, &left_row, right_row, &x, &is_null));
          if (!is_null) {
            dw.ext_sx[base + s] += x;
            dw.ext_sxx[base + s] += x * x;
            dw.counts[base + s] += 1;
          }
          break;
        }
        case RawAggKind::kCountDistinctInt64:
        case RawAggKind::kNone:
          break;
      }
    }
    return Status::kSuccess;
  };

  std::atomic<size_t> next_def_block{0};
  // A global numeric aggregate whose dimension was absorbed into the
  // runtime filter has no join key left. The fact stays columnar and every
  // surviving row charges the single group. Grouped queries keep the keyed
  // probe; a failed proof materializes the fact as before.
  const bool def_local_preds_ok =
      deferred_left_probe != nullptr &&
      (deferred_left_probe->local_preds.empty() ||
       deferred_left_probe->local_preds_covered_by_peeks);
  const bool scalar_fact_charge =
      deferred_left_probe != nullptr && singleton_right_relation &&
      fusion->group_keys.empty() && key_programs.empty() &&
      equality_keys.empty() && deferred_left_probe->pk_lookups.empty() &&
      def_local_preds_ok && deferred_left_probe->store != nullptr;
  const bool distinct_fact_singleton =
      deferred_left_probe != nullptr && singleton_right_relation &&
      equality_keys.empty() && deferred_left_probe->pk_lookups.empty() &&
      def_local_preds_ok && deferred_left_probe->store != nullptr &&
      fusion->aggregates.empty() && join_type == JoinType::kInner &&
      !fusion->having && !fusion->post_having_active &&
      fusion->topn_limit == 0 && !fusion->rollup_count_distribution &&
      fusion->output_order.empty() && result.using_columns == nullptr &&
      residual_programs.empty() && !key_programs.empty() &&
      key_programs.size() <= 2;
  if (deferred_left_probe != nullptr) {
    if ((singleton_right_relation && !scalar_fact_charge &&
         !distinct_fact_singleton) ||
        (!integer_key && !packed_2x32_int_key && !scalar_fact_charge &&
         !distinct_fact_singleton)) {
      return not_fusable("deferred_left_probe unsupported");
    }
  }

  // Preassigned groups already name the build row. Aggregates that only read
  // left-side numeric columns can charge those cells without a scratch row
  // or a bytecode evaluation.
  struct LeftNumericAgg {
    int32_t col0{-1};
    int32_t col1{-1};
    int32_t orig_agg{-1};
    uint8_t op{255};
    bool is_int_col{false};
    bool case_else_null{false};
    uint8_t case_val_kind{0};
    const uint8_t* right_cond{nullptr};
    double scale{0};
  };
  std::array<LeftNumericAgg, kRawAggregateCapacity> left_numeric{};
  bool left_cell_agg_ready = raw_sum_fusion && !ordered_sum_charge &&
                             !use_partitioned_count_distinct && agg_count > 0 &&
                             agg_count <= kRawAggregateCapacity;
  bool uses_double_arith_cell_op = false;
  struct CompiledDefDoubleArith {
    enum class Pattern : uint8_t {
      kGeneral = 0,
      kSub = 1,
      kAdd = 2,
      kSubDivConst = 3,
      kSubSubAddDivConst = 4,
    };
    struct Step {
      // 0: load def col, 1: load right col, 2: load const,
      // 3: add, 4: sub, 5: mul, 6: div, 7: div by finite |c| >= 1.0
      uint8_t kind{0};
      uint32_t operand{0};
      double cst{0.0};
    };
    Pattern pattern{Pattern::kGeneral};
    std::array<uint32_t, 4> fast_cols{};
    double fast_div{1.0};
    std::vector<Step> steps;
  };
  std::vector<CompiledDefDoubleArith> compiled_double_arith(
      fusion->aggregates.size());
  const auto numeric_left_col = [&](int32_t col) {
    if (col < 0 ||
        std::cmp_greater_equal(static_cast<size_t>(col), left_width)) {
      return false;
    }
    const Column& column = left.schema.GetColumn(static_cast<size_t>(col));
    if (column.IsUnsigned()) {
      return false;
    }
    const ValueType type = column.Type();
    return type == ValueType::kInt64 || type == ValueType::kDouble;
  };
  const auto integer_left_col = [&](int32_t col) {
    if (col < 0 ||
        std::cmp_greater_equal(static_cast<size_t>(col), left_width)) {
      return false;
    }
    const Column& column = left.schema.GetColumn(static_cast<size_t>(col));
    return !column.IsUnsigned() && column.Type() == ValueType::kInt64;
  };
  if (left_cell_agg_ready) {
    for (size_t s = 0; s < agg_count; ++s) {
      const size_t i = raw_slot_origin[s];
      if (child_shapes[i].profit_col0 >= 0) {
        left_cell_agg_ready = false;
        break;
      }
      LeftNumericAgg& op = left_numeric[s];
      op.orig_agg = static_cast<int32_t>(i);
      if (child_shapes[i].case_cond != ProgramShape::CaseCondKind::kNone) {
        if (right_case_conditions[i].empty() || !right_case_complete[i]) {
          left_cell_agg_ready = false;
          break;
        }
        const ProgramShape& sh = child_shapes[i];
        op.case_else_null = sh.case_else_null;
        op.right_cond = right_case_conditions[i].data();
        bool val_ok = false;
        switch (sh.case_val) {
          case ProgramShape::CaseValKind::kDirectDoubleCol:
            if (numeric_left_col(sh.case_val_col0)) {
              op.col0 = sh.case_val_col0;
              op.case_val_kind = 2;
              val_ok = true;
            }
            break;
          case ProgramShape::CaseValKind::kMulOneMinus:
            if (numeric_left_col(sh.case_val_col0) &&
                numeric_left_col(sh.case_val_col1)) {
              op.col0 = sh.case_val_col0;
              op.col1 = sh.case_val_col1;
              op.scale = sh.case_val_const;
              op.case_val_kind = 3;
              val_ok = true;
            }
            break;
          case ProgramShape::CaseValKind::kIntConst1:
            op.case_val_kind = 1;
            val_ok = true;
            break;
          case ProgramShape::CaseValKind::kNone:
            break;
        }
        if (!val_ok) {
          left_cell_agg_ready = false;
          break;
        }
        switch (slot_agg_kinds[s]) {
          case RawAggKind::kCountValue:
            op.op = 13;
            break;
          case RawAggKind::kSumInt64:
            if (op.case_val_kind == 1) {
              op.op = 12;
            } else {
              left_cell_agg_ready = false;
            }
            break;
          case RawAggKind::kSumDouble:
          case RawAggKind::kAvgDouble:
            if (op.case_val_kind == 2) {
              op.op = 10;
            } else if (op.case_val_kind == 3) {
              op.op = 11;
            } else {
              left_cell_agg_ready = false;
            }
            break;
          default:
            left_cell_agg_ready = false;
            break;
        }
        if (!left_cell_agg_ready) {
          break;
        }
        continue;
      }
      switch (slot_agg_kinds[s]) {
        case RawAggKind::kCountStar:
          op.op = 0;
          break;
        case RawAggKind::kConstantExtremum:
          op.op = raw_constants[i].IsNull() ? 5 : 0;
          break;
        case RawAggKind::kCountValue:
          if (numeric_left_col(child_shapes[i].direct_column)) {
            op.op = 4;
            op.col0 = child_shapes[i].direct_column;
          } else if (numeric_left_col(child_shapes[i].mul_col0) &&
                     numeric_left_col(child_shapes[i].mul_col1)) {
            // COUNT(a * b): the product is NULL exactly when either factor is.
            op.op = 7;
            op.col0 = child_shapes[i].mul_col0;
            op.col1 = child_shapes[i].mul_col1;
          } else {
            left_cell_agg_ready = false;
          }
          break;
        case RawAggKind::kSumInt64:
          if (!integer_left_col(child_shapes[i].direct_column)) {
            left_cell_agg_ready = false;
          } else {
            op.op = 3;
            op.col0 = child_shapes[i].direct_column;
          }
          break;
        case RawAggKind::kSumDouble:
        case RawAggKind::kAvgDouble:
          if (child_shapes[i].mul_col0 >= 0 &&
              numeric_left_col(child_shapes[i].mul_col0) &&
              numeric_left_col(child_shapes[i].mul_col1)) {
            op.op = 1;
            op.col0 = child_shapes[i].mul_col0;
            op.col1 = child_shapes[i].mul_col1;
          } else if (child_shapes[i].mul_one_minus_col0 >= 0 &&
                     numeric_left_col(child_shapes[i].mul_one_minus_col0) &&
                     numeric_left_col(child_shapes[i].mul_one_minus_col1)) {
            op.op = 6;
            op.col0 = child_shapes[i].mul_one_minus_col0;
            op.col1 = child_shapes[i].mul_one_minus_col1;
            op.scale = child_shapes[i].mul_one_minus_const;
          } else if (numeric_left_col(child_shapes[i].direct_column)) {
            op.op = 2;
            op.col0 = child_shapes[i].direct_column;
          } else if (deferred_left_probe != nullptr &&
                     i < double_arithmetic.size() && double_arithmetic[i]) {
            bool operands_ok = true;
            for (const auto& inst : child_programs[i].Instructions()) {
              if (inst.opcode != BytecodeOp::kLoadColumn) {
                continue;
              }
              if (inst.operand < def_base_types.size()) {
                if (def_overwritten_by_pk[inst.operand] != 0 ||
                    def_base_types[inst.operand] != ValueType::kDouble) {
                  operands_ok = false;
                  break;
                }
              } else if (!right_ptr->HasSpill() && inst.operand >= left_width &&
                         (inst.operand - left_width) <
                             right.schema.ColumnCount() &&
                         right.schema.GetColumn(inst.operand - left_width)
                                 .Type() == ValueType::kDouble) {
                continue;
              } else {
                operands_ok = false;
                break;
              }
            }
            if (operands_ok) {
              op.op = 9;
              uses_double_arith_cell_op = true;
              CompiledDefDoubleArith& plan = compiled_double_arith[i];
              const auto& insts = child_programs[i].Instructions();
              const auto& consts = child_programs[i].Constants();
              plan.steps.reserve(insts.size());
              bool can_simplify = true;
              for (const auto& inst : insts) {
                CompiledDefDoubleArith::Step st;
                if (inst.opcode == BytecodeOp::kLoadColumn) {
                  if (inst.operand < def_base_types.size()) {
                    st.kind = 0;
                    st.operand = inst.operand;
                  } else {
                    st.kind = 1;
                    st.operand =
                        static_cast<uint32_t>(inst.operand - left_width);
                  }
                } else if (inst.opcode == BytecodeOp::kLoadConstant) {
                  st.kind = 2;
                  const Value& cv = consts[inst.operand];
                  st.cst = cv.type == ValueType::kDouble
                               ? cv.value.double_value
                               : static_cast<double>(cv.value.int_value);
                } else if (inst.opcode == BytecodeOp::kBinaryDouble ||
                           inst.opcode == BytecodeOp::kBinaryInt64) {
                  switch (inst.binary) {
                    case BinaryOperation::kAdd:
                      st.kind = 3;
                      break;
                    case BinaryOperation::kSubtract:
                      st.kind = 4;
                      break;
                    case BinaryOperation::kMultiply:
                      st.kind = 5;
                      break;
                    case BinaryOperation::kDivide:
                      st.kind = 6;
                      break;
                    default:
                      can_simplify = false;
                      break;
                  }
                } else {
                  can_simplify = false;
                }
                if (st.kind == 6 && !plan.steps.empty() &&
                    plan.steps.back().kind == 2 &&
                    std::isfinite(plan.steps.back().cst) &&
                    std::abs(plan.steps.back().cst) >= 1.0) {
                  st.kind = 7;
                  st.cst = plan.steps.back().cst;
                  plan.steps.pop_back();
                }
                plan.steps.push_back(st);
              }
              if (can_simplify && plan.steps.size() == 8 &&
                  plan.steps[0].kind == 0 && plan.steps[1].kind == 0 &&
                  plan.steps[2].kind == 4 && plan.steps[3].kind == 0 &&
                  plan.steps[4].kind == 4 && plan.steps[5].kind == 0 &&
                  plan.steps[6].kind == 3 && plan.steps[7].kind == 7) {
                plan.pattern =
                    CompiledDefDoubleArith::Pattern::kSubSubAddDivConst;
                plan.fast_cols = {plan.steps[0].operand, plan.steps[1].operand,
                                  plan.steps[3].operand, plan.steps[5].operand};
                plan.fast_div = plan.steps[7].cst;
              } else if (can_simplify && plan.steps.size() == 4 &&
                         plan.steps[0].kind == 0 && plan.steps[1].kind == 0 &&
                         plan.steps[2].kind == 4 && plan.steps[3].kind == 7) {
                plan.pattern = CompiledDefDoubleArith::Pattern::kSubDivConst;
                plan.fast_cols[0] = plan.steps[0].operand;
                plan.fast_cols[1] = plan.steps[1].operand;
                plan.fast_div = plan.steps[3].cst;
              } else if (can_simplify && plan.steps.size() == 3 &&
                         plan.steps[0].kind == 0 && plan.steps[1].kind == 0 &&
                         (plan.steps[2].kind == 4 || plan.steps[2].kind == 3)) {
                plan.pattern = plan.steps[2].kind == 4
                                   ? CompiledDefDoubleArith::Pattern::kSub
                                   : CompiledDefDoubleArith::Pattern::kAdd;
                plan.fast_cols[0] = plan.steps[0].operand;
                plan.fast_cols[1] = plan.steps[1].operand;
              }
            } else {
              left_cell_agg_ready = false;
            }
          } else {
            left_cell_agg_ready = false;
          }
          break;
        case RawAggKind::kStddevSamp:
        case RawAggKind::kStddevPop:
        case RawAggKind::kVarSamp:
        case RawAggKind::kVarPop:
          if (numeric_left_col(child_shapes[i].direct_column)) {
            op.op = 8;
            op.col0 = child_shapes[i].direct_column;
            op.is_int_col = integer_left_col(child_shapes[i].direct_column);
          } else {
            left_cell_agg_ready = false;
          }
          break;
        default:
          left_cell_agg_ready = false;
          break;
      }
      if (!left_cell_agg_ready) {
        break;
      }
    }
  }
  bool left_numeric_probe = left_cell_agg_ready && !uses_double_arith_cell_op;
  if (!use_direct_gid_agg || use_shared_direct_gid) {
    left_numeric_probe = false;
  }
  if (scalar_fact_charge) {
    bool columns_ready = left_numeric_probe && prepared_pk_lookups.empty();
    for (size_t i = 0; columns_ready && i < agg_count; ++i) {
      const LeftNumericAgg& op = left_numeric[i];
      const auto in_proj = [&](int32_t col) {
        return col < 0 ||
               (static_cast<size_t>(col) <
                    deferred_left_probe->projection.size() &&
                def_overwritten_by_pk[static_cast<size_t>(col)] == 0);
      };
      columns_ready = in_proj(op.col0) && in_proj(op.col1);
    }
    if (!columns_ready) {
      return not_fusable("scalar fact charge");
    }
  }
  const bool def_compact_split_probe = deferred_left_probe != nullptr &&
                                       compact_split_key_only &&
                                       residual_programs.empty();
  bool def_group_cell_probe =
      deferred_left_probe != nullptr && !use_preassigned_build_groups &&
      !compact_counts && residual_programs.empty() && raw_sum_fusion &&
      left_cell_agg_ready && !has_count_distinct && all_direct_group_keys &&
      (int_group_key || packed_2x32_group_key ||
       (use_split_key_gid && !use_fast_2key_year_lut));
  if (def_group_cell_probe) {
    const size_t base_proj_n = deferred_left_probe->projection.size();
    for (size_t i = 0; i < agg_count; ++i) {
      const LeftNumericAgg& op = left_numeric[i];
      if ((op.col0 >= 0 &&
           (std::cmp_greater_equal(op.col0, base_proj_n) ||
            def_overwritten_by_pk[static_cast<size_t>(op.col0)] != 0)) ||
          (op.col1 >= 0 &&
           (std::cmp_greater_equal(op.col1, base_proj_n) ||
            def_overwritten_by_pk[static_cast<size_t>(op.col1)] != 0))) {
        def_group_cell_probe = false;
        break;
      }
    }
  }
  if (def_group_cell_probe || def_compact_split_probe) {
    std::array<uint8_t, 16> eager_col{};
    for (const slot_t lk : left_columns) {
      if (lk < 16) {
        eager_col[lk] = 1;
      }
    }
    if (def_group_cell_probe) {
      for (size_t i = 0; i < agg_count; ++i) {
        const LeftNumericAgg& op = left_numeric[i];
        if (op.col0 >= 0 && op.col0 < 16) {
          eager_col[static_cast<size_t>(op.col0)] = 1;
        }
        if (op.col1 >= 0 && op.col1 < 16) {
          eager_col[static_cast<size_t>(op.col1)] = 1;
        }
        if (op.op == 9) {
          for (const auto& inst :
               child_programs[static_cast<size_t>(op.orig_agg)]
                   .Instructions()) {
            if (inst.opcode == BytecodeOp::kLoadColumn && inst.operand < 16) {
              eager_col[inst.operand] = 1;
            }
          }
        }
      }
    }
    for (PreparedDeferredPkGroup& grp : def_pk_groups) {
      if (!grp.use_subgid) {
        continue;
      }
      bool all_lazy = true;
      for (const size_t li : grp.lookup_indices) {
        const slot_t tc = prepared_pk_lookups[li].target_proj_col;
        if (tc < 16 && eager_col[tc] != 0) {
          all_lazy = false;
          break;
        }
      }
      grp.lazy_payloads = all_lazy;
    }
  }
  std::vector<size_t> dense_pk_group_indices;
  size_t dense_split_domain = 0;
  std::shared_ptr<QueryMemoryCharge> dense_split_charge;
  bool use_dense_split_direct_gid =
      def_group_cell_probe && use_split_key_gid && !split_has_left_probe &&
      split_key_unpack_only && join_type == JoinType::kInner &&
      !fusion->aggregates.empty() && right_row_count > 0 &&
      (!split_uses_right_subgid ||
       split_right_rep_count == split_right_subgid_count);
  bool use_packed_double_cells = use_dense_split_direct_gid &&
                                 !use_direct_gid_agg && !raw_count_fusion &&
                                 agg_count == 1 &&
                                 (slot_agg_kinds[0] == RawAggKind::kSumDouble ||
                                  slot_agg_kinds[0] == RawAggKind::kAvgDouble);
  if (use_dense_split_direct_gid) {
    uint64_t dom = split_uses_right_subgid ? split_right_subgid_count : 1ULL;
    for (size_t gi = 0; gi < def_pk_groups.size(); ++gi) {
      if (!def_pk_groups[gi].use_subgid) {
        continue;
      }
      const uint32_t card = def_pk_groups[gi].subgid_count;
      if (card == 0 || dom > 524288ULL / card) {
        dom = 0;
        break;
      }
      dom *= card;
      dense_pk_group_indices.push_back(gi);
    }
    if (dom == 0 || dom > 524288ULL) {
      use_dense_split_direct_gid = false;
      use_packed_double_cells = false;
    } else {
      const size_t per_group_bytes =
          use_packed_double_cells
              ? sizeof(DirectDoubleCell)
              : (1 + (agg_count *
                      (8 + 1 + (has_double_raw_agg ? 8 : 0) +
                       (has_int_raw_agg ? 8 : 0) +
                       (has_stat_raw_agg ? 2 * sizeof(platform::ExtendedFloat)
                                         : 0))));
      const size_t bytes = workers * static_cast<size_t>(dom) *
                           std::max<size_t>(per_group_bytes, 16);
      const size_t dense_byte_cap = (!raw_count_fusion && has_stat_raw_agg)
                                        ? (size_t{16} << 20)
                                        : (size_t{256} << 20);
      if (bytes <= dense_byte_cap) {
        dense_split_charge = TryReserveQueryMemory(bytes);
      }
      if (dense_split_charge) {
        dense_split_domain = static_cast<size_t>(dom);
        init_direct_gid_workers(workers, dense_split_domain,
                                use_packed_double_cells);
      } else {
        use_dense_split_direct_gid = false;
        use_packed_double_cells = false;
      }
    }
  }
  struct alignas(8) RadixTuple {
    uint64_t key;
    uint64_t bits;
    uint8_t valid;
  };
  constexpr size_t kRadixParts = 64;
  const auto radix_bucket_of = [](uint64_t ukey) noexcept -> size_t {
    uint64_t x = ukey + 0x9e3779b97f4a7c15ULL;
    x = (x ^ (x >> 30U)) * 0xbf58476d1ce4e5b9ULL;
    return static_cast<size_t>((x ^ (x >> 27U)) >> 58U);
  };
  bool use_radix_agg = false;
  size_t radix_primary = 0;
  bool radix_tuple_is_int = false;
  if (workers > 1 && effective_left_rows >= 2000000 && raw_sum_fusion &&
      !use_preassigned_build_groups && !use_direct_gid_agg &&
      !use_dense_split_direct_gid && !ordered_sum_charge &&
      !scalar_fact_charge && !compact_counts && !has_count_distinct &&
      residual_programs.empty() && all_direct_group_keys &&
      !fusion->group_keys.empty() && !fusion->having &&
      !fusion->post_having_active && fusion->topn_limit == 0 &&
      !fusion->rollup_count_distribution && left_cell_agg_ready &&
      agg_count >= 1 && join_type == JoinType::kInner &&
      !right_ptr->HasSpill() && !use_fast_2key_year_lut &&
      ((use_left_packed_key && left_pack_unpack_only) ||
       (use_split_key_gid && split_key_unpack_only) ||
       (packed_2x32_group_key && packed_2x32_unpack_only)) &&
      std::getenv("TINYLAMB_DISABLE_RADIX_AGG") == nullptr) {
    const bool radix_def_path =
        (deferred_left_probe != nullptr && def_group_cell_probe &&
         (def_pk_groups.empty() ||
          (!uses_double_arith_cell_op && !use_dense_split_direct_gid)));
    const bool radix_col_path =
        (deferred_left_probe == nullptr && left.HasColumnarOnly() &&
         !singleton_right_relation && !uses_double_arith_cell_op &&
         (integer_key || packed_2x32_int_key) && !bulk_raw_count &&
         (use_left_packed_key ||
          (use_split_key_gid && !split_uses_pk_subgid &&
           (!split_has_left_probe ||
            key_shapes[split_left_k].direct_column >= 0)) ||
          packed_2x32_group_key));
    if (radix_def_path || radix_col_path) {
      bool found_any_primary = false;
      for (size_t s = 0; s < agg_count; ++s) {
        const uint8_t sop = left_numeric[s].op;
        if (sop == 1 || sop == 2 || sop == 3 || sop == 6 || sop == 8 ||
            sop == 9) {
          radix_primary = s;
          break;
        }
        if (!found_any_primary && sop != 0 && sop != 5) {
          radix_primary = s;
          found_any_primary = true;
        }
      }
      const LeftNumericAgg& pop = left_numeric[radix_primary];
      bool ops_ok = (pop.op <= 9);
      bool any_int_req = (pop.op == 3 || (pop.op == 8 && pop.is_int_col));
      for (size_t s = 0; ops_ok && s < agg_count; ++s) {
        const LeftNumericAgg& sop = left_numeric[s];
        if (sop.op == 0 || sop.op == 5) {
          continue;
        }
        if (sop.op >= 10) {
          ops_ok = false;
          break;
        }
        if (s == radix_primary) {
          continue;
        }
        if ((pop.op == 2 || pop.op == 3 || pop.op == 4 || pop.op == 8) &&
            (sop.op == 2 || sop.op == 3 || sop.op == 4 || sop.op == 8) &&
            sop.col0 == pop.col0 && sop.col0 >= 0) {
          if (sop.op == 3 || (sop.op == 8 && sop.is_int_col)) {
            any_int_req = true;
          }
          continue;
        }
        ops_ok = false;
      }
      if (ops_ok && any_int_req && !integer_left_col(pop.col0)) {
        ops_ok = false;
      }
      if (ops_ok) {
        use_radix_agg = true;
        radix_tuple_is_int = any_int_req;
      }
    }
  }
  std::vector<std::array<std::vector<RadixTuple>, kRadixParts>>
      worker_radix_buckets;
  if (use_radix_agg) {
    worker_radix_buckets.resize(workers);
  } else if (!group_capacity_charge &&
             (deferred_left_probe != nullptr || left.HasColumnarOnly()) &&
             !left.HasSpill() && effective_left_rows >= 32768 &&
             (int_group_key || use_split_key_gid || packed_2x32_group_key)) {
    const size_t max_reserve =
        effective_left_rows >= 2000000 ? size_t{65536} : size_t{8192};
    const size_t capacity =
        std::min<size_t>(max_reserve, (effective_left_rows / workers) + 1);
    const size_t bytes = workers * capacity *
                         (64 + (compact_counts ? sizeof(CompactCountGroup)
                                               : sizeof(FusedGroup)));
    group_capacity_charge = TryReserveQueryMemory(bytes);
    if (group_capacity_charge) {
      for (auto& local : locals) {
        local.int_offsets.Reserve(
            compact_counts ? std::min<size_t>(1024, capacity) : capacity);
        if (compact_counts) {
          local.compact_counts.reserve(std::min<size_t>(1024, capacity));
        } else {
          local.groups.reserve(capacity);
        }
      }
    }
  }
  std::vector<FusedRawArena> radix_part_arenas;
  std::vector<std::vector<FusedGroup>> radix_part_groups;

  // Decide the partitioning method once, before workers start. A failed
  // optional allocation must not make some workers split rows while others
  // split keys, which would overlap or omit parts of the probe.
  std::shared_ptr<const LSTableIntegerRuns> flat_run_index;
  if (deferred_left_probe != nullptr && left_numeric_probe &&
      single_double_sum && !ranked_int_bits.empty() && integer_key &&
      prepared_pk_lookups.empty() && deferred_left_probe->null_peeks.empty() &&
      left_key_offsets[0] == 0 &&
      (left_numeric[0].op == 1 || left_numeric[0].op == 2 ||
       left_numeric[0].op == 6)) {
    const auto& store = *deferred_left_probe->store;
    const auto flat_column_ready = [&](size_t col) {
      return col < store.column_has_null.size() &&
             !store.column_has_null[col] &&
             col < store.integer_columns.size() &&
             store.integer_columns[col].size() == store.total_rows;
    };
    const auto op_column_ready = [&](int32_t col) {
      return col >= 0 &&
             static_cast<size_t>(col) <
                 deferred_left_probe->projection.size() &&
             !def_overwritten_by_pk[static_cast<size_t>(col)];
    };
    if (op_column_ready(left_numeric[0].col0) &&
        (left_numeric[0].op == 2 || op_column_ready(left_numeric[0].col1)) &&
        std::ranges::all_of(deferred_left_probe->projection,
                            flat_column_ready) &&
        std::ranges::all_of(
            deferred_left_probe->integer_peeks,
            [&](const auto& pred) { return flat_column_ready(pred.column); })) {
      flat_run_index =
          store.IntegerRuns(deferred_left_probe->projection[left_columns[0]]);
    }
  }
  std::shared_ptr<const LSTableIntegerRunBounds> flat_run_bounds;
  if (flat_run_index && deferred_left_probe->integer_peeks.size() == 1 &&
      deferred_left_probe->integer_peeks[0].op ==
          BinaryOperation::kGreaterThan) {
    flat_run_bounds = deferred_left_probe->store->IntegerRunBounds(
        flat_run_index->column, deferred_left_probe->integer_peeks[0].column);
  }
  // Indexed fact probes consume the already decoded build keys directly.
  // Only the scanning fallback needs rank-to-row arrays over the bitmap.
  if (!ranked_int_bits.empty() && (!flat_run_index || !key_cache_charge)) {
    ranked_int_prefix.resize(ranked_int_bits.size());
    uint32_t count = 0;
    for (size_t i = 0; i < ranked_int_bits.size(); ++i) {
      ranked_int_prefix[i] = count;
      count += static_cast<uint32_t>(std::popcount(ranked_int_bits[i]));
    }
    ranked_int_rows.resize(count);
    const auto add = [&](uint32_t row, int64_t key) {
      const uint64_t off =
          static_cast<uint64_t>(key) - static_cast<uint64_t>(dense_int_min);
      const uint64_t lower = (uint64_t{1} << (off & 63)) - 1;
      const uint32_t rank =
          ranked_int_prefix[off >> 6] + static_cast<uint32_t>(std::popcount(
                                            ranked_int_bits[off >> 6] & lower));
      ranked_int_rows[rank] = row;
      return true;
    };
    if (key_cache_charge) {
      for (const auto& chunk : key_cache) {
        for (const auto& entry : chunk) { add(entry.row, entry.key);
}
}
    } else {
      visit_right_int_keys(add);
    }
  }
  if (!flat_run_index) {
    key_cache.clear();
    key_cache_charge.reset();
  }
  // The indexed probe assigns each unique build key (and its entire fact
  // run) to one worker. Identity groups therefore have exactly one writer:
  // one state array suffices without locks or a merge of worker partials.
  const bool disjoint_direct_groups = flat_run_index && identity_right_gid;
  if (use_direct_gid_agg) {
    const size_t state_workers =
        disjoint_direct_groups ? 1 : direct_gid_worker_count;
    if (disjoint_direct_groups && extended_direct_charge && workers > 1) {
      extended_direct_charge->Release((workers - 1) * pre_group_count *
                                      direct_state_bytes);
    }
    init_direct_gid_workers(state_workers, pre_group_count);
  }
  const auto phase_states_done = std::chrono::steady_clock::now();

  auto run_probe = [&](size_t w, FusedWorkerLocal& local,
                       Row& scratch) -> Status {
    const auto add_dw_double = [](DirectGidWorker& dw, size_t slot, double dv) {
      const double prev = dw.doubles[slot];
      const double next = prev + dv;
      if (std::isinf(next) && !std::isinf(prev) && !std::isinf(dv)) {
        dw.inf_flags[slot] = 1;
      }
      dw.doubles[slot] = next;
      dw.counts[slot] += 1;
    };
    const auto add_dw_int = [](DirectGidWorker& dw, size_t slot, int64_t iv) {
      int64_t next = 0;
      if (__builtin_add_overflow(dw.ints[slot], iv, &next)) {
        dw.inf_flags[slot] = 1;
      } else {
        dw.ints[slot] = next;
      }
      dw.counts[slot] += 1;
    };
    const auto charge_right_case_op = [&](const LeftNumericAgg& op,
                                          size_t r_idx, bool has_right,
                                          auto&& add_double, auto&& add_int,
                                          auto&& add_count, auto&& as_double,
                                          auto&& present) {
      const uint8_t rc = has_right ? op.right_cond[r_idx] : uint8_t{0};
      switch (op.op) {
        case 10: {
          if (rc == 1) {
            double d0 = 0.0;
            if (as_double(op.col0, &d0)) [[likely]] {
              add_double(d0);
            }
          } else if (!op.case_else_null) {
            add_double(0.0);
          }
          break;
        }
        case 11: {
          if (rc == 1) {
            double d0 = 0.0;
            double d1 = 0.0;
            if (as_double(op.col0, &d0) && as_double(op.col1, &d1)) [[likely]] {
              add_double(d0 * (op.scale - d1));
            }
          } else if (!op.case_else_null) {
            add_double(0.0);
          }
          break;
        }
        case 12:
          if (rc == 1) {
            add_int(1);
          } else if (!op.case_else_null) {
            add_count();
          }
          break;
        case 13:
          if (rc == 1) {
            if (op.case_val_kind == 1 ||
                (op.case_val_kind == 2 && present(op.col0)) ||
                (op.case_val_kind == 3 && present(op.col0) &&
                 present(op.col1))) {
              add_count();
            }
          } else if (!op.case_else_null) {
            add_count();
          }
          break;
        default:
          break;
      }
    };
    const auto charge_numeric_with = [&](size_t r_idx, bool has_right,
                                         auto&& add_double_fn,
                                         auto&& add_int_fn, auto&& inc_count_fn,
                                         auto&& add_stat_fn, auto&& as_double,
                                         auto&& as_int, auto&& present) {
      for (size_t i = 0; i < agg_count; ++i) {
        const LeftNumericAgg& op = left_numeric[i];
        switch (op.op) {
          case 0:
            inc_count_fn(i);
            break;
          case 1:
          case 6: {
            double d0 = 0;
            double d1 = 0;
            if (!as_double(op.col0, &d0) || !as_double(op.col1, &d1)) {
              break;
            }
            add_double_fn(i, op.op == 1 ? d0 * d1 : d0 * (op.scale - d1));
            break;
          }
          case 2: {
            double d0 = 0;
            if (!as_double(op.col0, &d0)) {
              break;
            }
            add_double_fn(i, d0);
            break;
          }
          case 3: {
            int64_t iv = 0;
            if (!as_int(op.col0, &iv)) {
              break;
            }
            add_int_fn(i, iv);
            break;
          }
          case 4:
            if (present(op.col0)) {
              inc_count_fn(i);
            }
            break;
          case 7:
            if (present(op.col0) && present(op.col1)) {
              inc_count_fn(i);
            }
            break;
          case 8: {
            platform::ExtendedFloat x = 0;
            if (op.is_int_col) {
              int64_t iv = 0;
              if (!as_int(op.col0, &iv)) {
                break;
              }
              x = static_cast<platform::ExtendedFloat>(iv);
            } else {
              double d0 = 0;
              if (!as_double(op.col0, &d0)) {
                break;
              }
              x = static_cast<platform::ExtendedFloat>(d0);
            }
            add_stat_fn(i, x);
            break;
          }
          case 10:
          case 11:
          case 12:
          case 13:
            charge_right_case_op(
                op, r_idx, has_right, [&](double dv) { add_double_fn(i, dv); },
                [&](int64_t iv) { add_int_fn(i, iv); },
                [&]() { inc_count_fn(i); }, as_double, present);
            break;
          default:
            break;
        }
      }
    };
    const auto charge_numeric = [&](DirectGidWorker& dw, uint32_t gid,
                                    size_t r_idx, auto&& as_double,
                                    auto&& as_int, auto&& present) {
      dw.matched[gid] = 1;
      if (single_double_sum) {
        const LeftNumericAgg& op = left_numeric[0];
        if (op.op == 1 || op.op == 6) {
          double d0 = 0.0;
          double d1 = 0.0;
          if (as_double(op.col0, &d0) && as_double(op.col1, &d1)) {
            add_dw_double(dw, gid, op.op == 1 ? d0 * d1 : d0 * (op.scale - d1));
          }
          return;
        }
        if (op.op == 2) {
          double d0 = 0.0;
          if (as_double(op.col0, &d0)) {
            add_dw_double(dw, gid, d0);
          }
          return;
        }
      }
      const size_t base = static_cast<size_t>(gid) * agg_count;
      charge_numeric_with(
          r_idx, true,
          [&](size_t i, double dv) { add_dw_double(dw, base + i, dv); },
          [&](size_t i, int64_t iv) { add_dw_int(dw, base + i, iv); },
          [&](size_t i) { dw.counts[base + i] += 1; },
          [&](size_t i, platform::ExtendedFloat x) {
            const size_t slot = base + i;
            dw.ext_sx[slot] += x;
            dw.ext_sxx[slot] += x * x;
            dw.counts[slot] += 1;
          },
          as_double, as_int, present);
    };
    const auto charge_group_numeric = [&](FusedGroup& group, size_t r_idx,
                                          bool has_right, auto&& as_double,
                                          auto&& as_int, auto&& present) {
      charge_numeric_with(
          r_idx, has_right,
          [&](size_t i, double dv) {
            const double prev = group.RawDoubles()[i];
            const double next = prev + dv;
            if (std::isinf(next) && !std::isinf(prev) && !std::isinf(dv)) {
              group.RawInfFlags()[i] = 1;
            }
            group.RawDoubles()[i] = next;
            group.RawCounts()[i] += 1;
          },
          [&](size_t i, int64_t iv) {
            int64_t next = 0;
            if (__builtin_add_overflow(group.RawInts()[i], iv, &next)) {
              group.RawInfFlags()[i] = 1;
            } else {
              group.RawInts()[i] = next;
            }
            group.RawCounts()[i] += 1;
          },
          [&](size_t i) { group.RawCounts()[i] += 1; },
          [&](size_t i, platform::ExtendedFloat x) {
            group.RawStatX()[i] += x;
            group.RawStatXX()[i] += x * x;
            group.RawCounts()[i] += 1;
          },
          as_double, as_int, present);
    };
    const auto scatter_radix_tuple =
        [&](size_t worker_id, uint64_t ukey, auto&& as_double, auto&& as_int,
            auto&& present, auto&& eval_op9) -> Status {
      const LeftNumericAgg& rop = left_numeric[radix_primary];
      uint64_t t_bits = 0;
      uint8_t t_valid = 0;
      if (radix_tuple_is_int) {
        int64_t iv = 0;
        if (as_int(rop.col0, &iv)) [[likely]] {
          t_bits = static_cast<uint64_t>(iv);
          t_valid = 1;
        }
      } else {
        switch (rop.op) {
          case 0:
            t_valid = 1;
            break;
          case 1:
          case 6: {
            double d0 = 0.0;
            double d1 = 0.0;
            if (as_double(rop.col0, &d0) && as_double(rop.col1, &d1))
                [[likely]] {
              const double dv = rop.op == 1 ? d0 * d1 : d0 * (rop.scale - d1);
              t_bits = std::bit_cast<uint64_t>(dv);
              t_valid = 1;
            }
            break;
          }
          case 2:
          case 8: {
            double dv = 0.0;
            if (as_double(rop.col0, &dv)) [[likely]] {
              t_bits = std::bit_cast<uint64_t>(dv);
              t_valid = 1;
            }
            break;
          }
          case 4:
            if (present(rop.col0)) [[likely]] {
              t_valid = 1;
            }
            break;
          case 5:
            break;
          case 7:
            if (present(rop.col0) && present(rop.col1)) [[likely]] {
              t_valid = 1;
            }
            break;
          case 9: {
            double dv = 0.0;
            bool is_null = false;
            RETURN_IF_FAIL(eval_op9(rop.orig_agg, &dv, &is_null));
            if (!is_null) [[likely]] {
              t_bits = std::bit_cast<uint64_t>(dv);
              t_valid = 1;
            }
            break;
          }
          default:
            break;
        }
      }
      worker_radix_buckets[worker_id][radix_bucket_of(ukey)].push_back(
          RadixTuple{.key = ukey, .bits = t_bits, .valid = t_valid});
      return Status::kSuccess;
    };
    const uint32_t* const right_gids =
        identity_right_gid ? nullptr : right_row_gid.data();
    const auto gid_of = [&](uint32_t r_idx) -> uint32_t {
      return right_gids == nullptr ? r_idx : right_gids[r_idx];
    };
    if (deferred_left_probe != nullptr) {
      const LSTableStore& store = *deferred_left_probe->store;
      const size_t total_blocks = store.blocks.size();
      size_t block_cursor = 0, block_end = 0;
      const auto next_block = [&]() {
        if (block_cursor == block_end) {
          block_cursor =
              next_def_block.fetch_add(64, std::memory_order_relaxed);
          block_end = block_cursor + 64;
        }
        return block_cursor++;
      };
      const size_t base_proj_cols = deferred_left_probe->projection.size();
      const auto* peek_ptr = deferred_left_probe->integer_peeks.empty()
                                 ? nullptr
                                 : &deferred_left_probe->integer_peeks;
      const ScanRuntimeFilter& primary_rf = deferred_left_probe->runtime_filter;
      const RuntimeFilterGroup* active_rf_group =
          primary_rf.enabled() ? primary_rf.group.get() : nullptr;
      const std::optional<slot_t> full_rf_col =
          (active_rf_group != nullptr && primary_rf.column < base_proj_cols)
              ? std::optional<slot_t>(
                    deferred_left_probe->projection[primary_rf.column])
              : std::nullopt;
      const bool rf_has_dense =
          active_rf_group != nullptr && active_rf_group->has_dense_bits();
      const uint64_t* const rf_dense_bits =
          rf_has_dense ? active_rf_group->dense_bits_data() : nullptr;
      const size_t rf_dense_words =
          rf_has_dense ? active_rf_group->dense_bits_word_count() : 0;
      const int64_t rf_lo = rf_has_dense ? active_rf_group->Lo() : 0;
      const int64_t rf_hi = rf_has_dense ? active_rf_group->Hi() : 0;
      const bool rf_col_monotonic =
          rf_has_dense && full_rf_col.has_value() &&
          *full_rf_col < store.column_is_monotonic.size() &&
          store.column_is_monotonic[*full_rf_col] != 0;
      Row left_row;
      left_row.values_.resize(left_width);
      for (size_t c = 0; c < base_proj_cols; ++c) {
        if (def_overwritten_by_pk[c] == 0) {
          left_row.values_[c].type = def_base_types[c];
        }
      }
      struct DefPkGroupCursor {
        int64_t last_key{std::numeric_limits<int64_t>::min()};
        bool have_last{false};
        bool last_matched{false};
        uint32_t last_nb{0};
        uint32_t last_row_in_blk{0};
        uint32_t last_subgid{0};
      };
      std::array<DefPkGroupCursor, 4> pk_group_cursors{};
      std::array<const LSTableColumnBlock*, 16> proj_cols{};
      std::array<const int64_t*, 16> proj_int_ptrs{};
      std::array<uint8_t, 16> proj_has_null{};
      const auto write_pk_group_payloads = [&](size_t gi,
                                               const DefPkGroupCursor& cur) {
        const uint32_t nb = cur.last_nb;
        const uint32_t rin = cur.last_row_in_blk;
        for (const size_t li : def_pk_groups[gi].lookup_indices) {
          const PreparedDeferredPkLookup& lkp = prepared_pk_lookups[li];
          const PreparedDeferredPkBlock& bm = lkp.blocks[nb];
          Value& dst = left_row.values_[lkp.target_proj_col];
          if (bm.has_null_payload != 0 && bm.pay_blk->IsNull(rin))
              [[unlikely]] {
            dst = Value();
          } else if (lkp.payload_type == ValueType::kVarChar) {
            dst.type = ValueType::kVarChar;
            dst.SetCollation(0);
            dst.owned_data.clear();
            dst.value.varchar_value = bm.pay_blk->VarcharAt(rin);
          } else if (lkp.payload_type == ValueType::kDouble) {
            dst.type = ValueType::kDouble;
            dst.SetCollation(0);
            dst.value.double_value = std::bit_cast<double>(bm.pay_cells[rin]);
          } else {
            dst.type = lkp.payload_type;
            dst.SetCollation(0);
            if (lkp.payload_unsigned) {
              dst.SetUnsigned();
            }
            dst.value.int_value = bm.pay_cells[rin];
          }
        }
      };
      const auto resolve_def_pk_groups = [&](uint32_t r,
                                             bool write_all_payloads) -> bool {
        local.have_cur_pk_subgids = false;
        for (size_t gi = 0; gi < def_pk_groups.size(); ++gi) {
          const PreparedDeferredPkGroup& grp = def_pk_groups[gi];
          DefPkGroupCursor& cur = pk_group_cursors[gi];
          if (proj_has_null[grp.pk_col] != 0 &&
              proj_cols[grp.pk_col]->IsNull(r)) [[unlikely]] {
            return false;
          }
          const int64_t k = proj_int_ptrs[grp.pk_col][r];
          if (k < grp.min_k || k > grp.max_k) [[unlikely]] {
            return false;
          }
          if (!cur.have_last || k != cur.last_key) {
            if (grp.dense_ptr != nullptr) {
              const auto& entry = grp.dense_ptr[static_cast<size_t>(
                  static_cast<uint64_t>(k) - static_cast<uint64_t>(grp.min_k))];
              cur.last_key = k;
              cur.have_last = true;
              if (entry.nb == std::numeric_limits<uint32_t>::max())
                  [[unlikely]] {
                cur.last_matched = false;
                return false;
              }
              cur.last_nb = entry.nb;
              cur.last_row_in_blk = entry.rin;
              cur.last_matched = true;
              if (grp.use_subgid) {
                cur.last_subgid = entry.subgid;
              }
            } else {
              const auto nb_count = static_cast<uint32_t>(grp.blocks.size());
              const auto* const meta_ptr = grp.blocks.data();
              uint32_t nb = cur.last_nb;
              if (nb >= nb_count || k > meta_ptr[nb].max_k ||
                  k < meta_ptr[nb].min_k) {
                if (nb + 1 < nb_count && k <= meta_ptr[nb + 1].max_k &&
                    k >= meta_ptr[nb + 1].min_k) {
                  nb = nb + 1;
                } else {
                  const auto b = static_cast<size_t>(
                      (static_cast<__uint128_t>(
                           static_cast<uint64_t>(k) -
                           static_cast<uint64_t>(grp.min_k))
                       << PreparedDeferredPkLookup::kDirShift) /
                      grp.span);
                  const uint32_t lo = grp.bucket_lo[b];
                  const uint32_t hi = std::min<uint32_t>(
                      nb_count - 1, grp.bucket_lo[b + 1] + 1);
                  if (hi - lo <= 4) {
                    nb = lo;
                    while (nb < hi && meta_ptr[nb].max_k < k) {
                      ++nb;
                    }
                  } else {
                    const auto it =
                        std::lower_bound(grp.blk_max_keys.begin() + lo,
                                         grp.blk_max_keys.begin() + hi + 1, k);
                    nb = static_cast<uint32_t>(
                        std::distance(grp.blk_max_keys.begin(), it));
                  }
                }
              }
              const bool same_blk_monotonic =
                  cur.have_last && nb == cur.last_nb && k > cur.last_key;
              if (nb != cur.last_nb) {
                cur.last_row_in_blk = 0;
              }
              cur.last_nb = nb;
              cur.last_key = k;
              cur.have_last = true;
              const auto& bm = meta_ptr[nb];
              if (k < bm.min_k || k > bm.max_k) [[unlikely]] {
                cur.last_matched = false;
                return false;
              }
              uint32_t row_in_blk = bm.row_count;
              if (bm.dense_contiguous != 0) {
                row_in_blk = static_cast<uint32_t>(
                    static_cast<uint64_t>(k) - static_cast<uint64_t>(bm.min_k));
                cur.last_row_in_blk = row_in_blk;
              } else {
                const int64_t* const begin_ptr = bm.pk_cells;
                const int64_t* const end_ptr = begin_ptr + bm.row_count;
                const int64_t* search_begin = begin_ptr;
                const int64_t* search_end = end_ptr;
                if (same_blk_monotonic && cur.last_row_in_blk < bm.row_count) {
                  const uint32_t cur_r = cur.last_row_in_blk;
                  if (begin_ptr[cur_r] == k) {
                    row_in_blk = cur_r;
                    search_begin = end_ptr;
                  } else if (begin_ptr[cur_r] < k) {
                    if (cur_r + 1 < bm.row_count && begin_ptr[cur_r + 1] == k) {
                      row_in_blk = cur_r + 1;
                      search_begin = end_ptr;
                    } else {
                      search_begin = begin_ptr + cur_r + 1;
                      if (cur_r + 16 < bm.row_count &&
                          begin_ptr[cur_r + 16] >= k) {
                        search_end = begin_ptr + cur_r + 17;
                      } else if (cur_r + 64 < bm.row_count &&
                                 begin_ptr[cur_r + 64] >= k) {
                        search_begin = begin_ptr + cur_r + 16;
                        search_end = begin_ptr + cur_r + 65;
                      }
                    }
                  }
                }
                if (search_begin < search_end) {
                  const int64_t* const found_ptr =
                      std::lower_bound(search_begin, search_end, k);
                  const auto off = static_cast<uint32_t>(found_ptr - begin_ptr);
                  cur.last_row_in_blk = off;
                  if (found_ptr != search_end && *found_ptr == k) [[likely]] {
                    row_in_blk = off;
                  }
                } else {
                  cur.last_row_in_blk = row_in_blk;
                }
              }
              if (row_in_blk >= bm.row_count) [[unlikely]] {
                cur.last_matched = false;
                return false;
              }
              cur.last_matched = true;
              if (grp.use_subgid) {
                cur.last_subgid = grp.subgid_ptr[bm.row_base + row_in_blk];
              }
            }
          } else if (!cur.last_matched) [[unlikely]] {
            return false;
          }
          if (grp.use_subgid) {
            local.cur_pk_subgids[gi] = cur.last_subgid;
          }
          if (write_all_payloads) {
            write_pk_group_payloads(gi, cur);
          }
        }
        local.have_cur_pk_subgids = true;
        return true;
      };
      const auto write_base_left_cols = [&](uint32_t r) {
        for (size_t c = 0; c < base_proj_cols; ++c) {
          if (def_overwritten_by_pk[c] != 0) {
            continue;
          }
          Value& dst = left_row.values_[c];
          if (proj_has_null[c] != 0 && proj_cols[c]->IsNull(r)) [[unlikely]] {
            dst = Value();
            continue;
          }
          const ValueType vt = def_base_types[c];
          dst.type = vt;
          const int64_t raw = proj_int_ptrs[c][r];
          if (vt == ValueType::kDouble) {
            dst.value.double_value = std::bit_cast<double>(raw);
          } else {
            dst.value.int_value = raw;
          }
        }
      };
      const auto finish_lazy_left_row = [&](uint32_t r) {
        for (size_t gi = 0; gi < def_pk_groups.size(); ++gi) {
          write_pk_group_payloads(gi, pk_group_cursors[gi]);
        }
        write_base_left_cols(r);
      };
      const auto materialize_current_left_row = [&](uint32_t r) -> bool {
        if (!def_pk_groups.empty() && !resolve_def_pk_groups(r, true)) {
          return false;
        }
        write_base_left_cols(r);
        return true;
      };
      const auto read_def_left_raw = [&](size_t c, uint32_t r, int64_t* out_raw,
                                         ValueType* out_type,
                                         bool* out_unsigned) -> bool {
        const int8_t li = c < 16 ? def_col_to_pk_lookup[c] : int8_t{-1};
        if (li >= 0) {
          const PreparedDeferredPkLookup& lkp =
              prepared_pk_lookups[static_cast<size_t>(li)];
          if (lkp.payload_type == ValueType::kVarChar) {
            return false;
          }
          const DefPkGroupCursor& cur = pk_group_cursors[lkp.group_idx];
          const PreparedDeferredPkBlock& bm = lkp.blocks[cur.last_nb];
          if (bm.has_null_payload != 0 &&
              bm.pay_blk->IsNull(cur.last_row_in_blk)) [[unlikely]] {
            return false;
          }
          *out_raw = bm.pay_cells[cur.last_row_in_blk];
          *out_type = lkp.payload_type;
          *out_unsigned = lkp.payload_unsigned;
          return true;
        }
        if (proj_has_null[c] != 0 && proj_cols[c]->IsNull(r)) [[unlikely]] {
          return false;
        }
        *out_raw = proj_int_ptrs[c][r];
        *out_type = def_base_types[c];
        *out_unsigned = false;
        return true;
      };
      const auto read_pair_raw_int = [&](slot_t col, uint32_t r, uint32_t r_idx,
                                         int64_t* out_val, ValueType* out_type,
                                         bool* out_unsigned) -> bool {
        if (col < left_width) {
          return read_def_left_raw(col, r, out_val, out_type, out_unsigned);
        }
        Value rv_tmp;
        const Value& v = read_right_cell(r_idx, col - left_width, &rv_tmp);
        if (v.IsNull()) [[unlikely]] {
          return false;
        }
        *out_val = v.value.int_value;
        *out_type = v.type;
        *out_unsigned = v.IsUnsigned();
        return true;
      };
      const auto eval_double_arith_cells = [&](size_t agg_idx, uint32_t r,
                                               uint32_t r_idx, double* out,
                                               bool* is_null) -> Status {
        const CompiledDefDoubleArith& plan = compiled_double_arith[agg_idx];
        switch (plan.pattern) {
          case CompiledDefDoubleArith::Pattern::kSubSubAddDivConst: {
            const uint32_t c0 = plan.fast_cols[0];
            const uint32_t c1 = plan.fast_cols[1];
            const uint32_t c2 = plan.fast_cols[2];
            const uint32_t c3 = plan.fast_cols[3];
            if ((proj_has_null[c0] | proj_has_null[c1] | proj_has_null[c2] |
                 proj_has_null[c3]) == 0) [[likely]] {
              const auto v0 = std::bit_cast<double>(proj_int_ptrs[c0][r]);
              const auto v1 = std::bit_cast<double>(proj_int_ptrs[c1][r]);
              const auto v2 = std::bit_cast<double>(proj_int_ptrs[c2][r]);
              const auto v3 = std::bit_cast<double>(proj_int_ptrs[c3][r]);
              *out = (((v0 - v1) - v2) + v3) / plan.fast_div;
              *is_null = false;
              return Status::kSuccess;
            }
            if ((proj_has_null[c0] != 0 && proj_cols[c0]->IsNull(r)) ||
                (proj_has_null[c1] != 0 && proj_cols[c1]->IsNull(r)) ||
                (proj_has_null[c2] != 0 && proj_cols[c2]->IsNull(r)) ||
                (proj_has_null[c3] != 0 && proj_cols[c3]->IsNull(r))) {
              *is_null = true;
              return Status::kSuccess;
            }
            const auto v0 = std::bit_cast<double>(proj_int_ptrs[c0][r]);
            const auto v1 = std::bit_cast<double>(proj_int_ptrs[c1][r]);
            const auto v2 = std::bit_cast<double>(proj_int_ptrs[c2][r]);
            const auto v3 = std::bit_cast<double>(proj_int_ptrs[c3][r]);
            *out = (((v0 - v1) - v2) + v3) / plan.fast_div;
            *is_null = false;
            return Status::kSuccess;
          }
          case CompiledDefDoubleArith::Pattern::kSubDivConst: {
            const uint32_t c0 = plan.fast_cols[0];
            const uint32_t c1 = plan.fast_cols[1];
            if ((proj_has_null[c0] | proj_has_null[c1]) != 0 &&
                ((proj_has_null[c0] != 0 && proj_cols[c0]->IsNull(r)) ||
                 (proj_has_null[c1] != 0 && proj_cols[c1]->IsNull(r)))) {
              *is_null = true;
              return Status::kSuccess;
            }
            const auto v0 = std::bit_cast<double>(proj_int_ptrs[c0][r]);
            const auto v1 = std::bit_cast<double>(proj_int_ptrs[c1][r]);
            *out = (v0 - v1) / plan.fast_div;
            *is_null = false;
            return Status::kSuccess;
          }
          case CompiledDefDoubleArith::Pattern::kSub:
          case CompiledDefDoubleArith::Pattern::kAdd: {
            const uint32_t c0 = plan.fast_cols[0];
            const uint32_t c1 = plan.fast_cols[1];
            if ((proj_has_null[c0] | proj_has_null[c1]) != 0 &&
                ((proj_has_null[c0] != 0 && proj_cols[c0]->IsNull(r)) ||
                 (proj_has_null[c1] != 0 && proj_cols[c1]->IsNull(r)))) {
              *is_null = true;
              return Status::kSuccess;
            }
            const auto v0 = std::bit_cast<double>(proj_int_ptrs[c0][r]);
            const auto v1 = std::bit_cast<double>(proj_int_ptrs[c1][r]);
            *out = plan.pattern == CompiledDefDoubleArith::Pattern::kSub
                       ? (v0 - v1)
                       : (v0 + v1);
            *is_null = false;
            return Status::kSuccess;
          }
          case CompiledDefDoubleArith::Pattern::kGeneral:
            break;
        }
        std::array<double, 16> values{};
        std::array<uint8_t, 16> nulls{};
        size_t depth = 0;
        for (const CompiledDefDoubleArith::Step& st : plan.steps) {
          switch (st.kind) {
            case 0: {
              const uint32_t col = st.operand;
              if (proj_has_null[col] != 0 && proj_cols[col]->IsNull(r))
                  [[unlikely]] {
                nulls[depth] = 1;
              } else {
                nulls[depth] = 0;
                values[depth] = std::bit_cast<double>(proj_int_ptrs[col][r]);
              }
              ++depth;
              break;
            }
            case 1: {
              Value rv_tmp;
              const Value& v = read_right_cell(r_idx, st.operand, &rv_tmp);
              nulls[depth] = static_cast<uint8_t>(v.IsNull());
              if (!nulls[depth]) {
                values[depth] = v.value.double_value;
              }
              ++depth;
              break;
            }
            case 2:
              values[depth] = st.cst;
              nulls[depth++] = 0;
              break;
            case 3: {
              const size_t rhs = --depth;
              const size_t lhs = depth - 1;
              nulls[lhs] |= nulls[rhs];
              if (!nulls[lhs]) [[likely]] {
                values[lhs] += values[rhs];
              }
              break;
            }
            case 4: {
              const size_t rhs = --depth;
              const size_t lhs = depth - 1;
              nulls[lhs] |= nulls[rhs];
              if (!nulls[lhs]) [[likely]] {
                values[lhs] -= values[rhs];
              }
              break;
            }
            case 5: {
              const size_t rhs = --depth;
              const size_t lhs = depth - 1;
              nulls[lhs] |= nulls[rhs];
              if (!nulls[lhs]) [[likely]] {
                values[lhs] *= values[rhs];
              }
              break;
            }
            case 6: {
              const size_t rhs = --depth;
              const size_t lhs = depth - 1;
              nulls[lhs] |= nulls[rhs];
              if (!nulls[lhs]) [[likely]] {
                if (values[rhs] == 0.0) {
                  return StatusError(StatusCode::kIsInfinity,
                                     "division by zero");
                }
                const double quotient = values[lhs] / values[rhs];
                if (std::isfinite(values[lhs]) && std::isfinite(values[rhs]) &&
                    std::isinf(quotient)) {
                  return StatusError(StatusCode::kIsInfinity,
                                     "double overflow");
                }
                values[lhs] = quotient;
              }
              break;
            }
            case 7: {
              const size_t lhs = depth - 1;
              if (!nulls[lhs]) [[likely]] {
                values[lhs] /= st.cst;
              }
              break;
            }
            default:
              break;
          }
        }
        *out = values[0];
        *is_null = nulls[0] != 0;
        return Status::kSuccess;
      };
      const auto read_def_agg_double = [&](int32_t col, uint32_t r,
                                           double* out) noexcept -> bool {
        const auto c = static_cast<size_t>(col);
        if (proj_has_null[c] != 0 && proj_cols[c]->IsNull(r)) [[unlikely]] {
          return false;
        }
        const int64_t raw = proj_int_ptrs[c][r];
        *out = def_base_types[c] == ValueType::kDouble
                   ? std::bit_cast<double>(raw)
                   : static_cast<double>(raw);
        return true;
      };
      const auto read_def_agg_int = [&](int32_t col, uint32_t r,
                                        int64_t* out) noexcept -> bool {
        const auto c = static_cast<size_t>(col);
        if (proj_has_null[c] != 0 && proj_cols[c]->IsNull(r)) [[unlikely]] {
          return false;
        }
        *out = proj_int_ptrs[c][r];
        return true;
      };
      const auto process_def_match = [&](uint32_t r, uint32_t r_idx) -> Status {
        if (def_compact_split_probe) {
          if (!def_pk_groups.empty() && !resolve_def_pk_groups(r, false)) {
            return Status::kSuccess;
          }
          const uint32_t r_subgid =
              !split_uses_right_subgid ? 0U : split_right_subgid_ptr[r_idx];
          uint64_t pk_packed = 0;
          bool pk_ok = true;
          if (split_uses_pk_subgid) {
            for (size_t gi = 0; gi < def_pk_groups.size(); ++gi) {
              if (def_pk_groups[gi].use_subgid) {
                if (local.cur_pk_subgids[gi] ==
                    std::numeric_limits<uint32_t>::max()) [[unlikely]] {
                  pk_ok = false;
                  break;
                }
                pk_packed |= static_cast<uint64_t>(local.cur_pk_subgids[gi])
                             << def_pk_groups[gi].subgid_shift;
              }
            }
          }
          if (r_subgid != std::numeric_limits<uint32_t>::max() && pk_ok)
              [[likely]] {
            uint64_t packed_u64 = pk_packed;
            if (split_uses_right_subgid) {
              packed_u64 |= static_cast<uint64_t>(r_subgid)
                            << split_right_shift;
            }
            ++local.comparisons;
            return record_compact_split(local, packed_u64);
          }
          return Status::kSuccess;
        }
        if (def_group_cell_probe) {
          if (!def_pk_groups.empty() && !resolve_def_pk_groups(r, false)) {
            return Status::kSuccess;
          }
          if (use_dense_split_direct_gid) {
            const uint32_t r_subgid =
                !split_uses_right_subgid ? 0U : split_right_subgid_ptr[r_idx];
            if (r_subgid == std::numeric_limits<uint32_t>::max()) [[unlikely]] {
              return Status::kSuccess;
            }
            uint32_t gid = r_subgid;
            for (const size_t gi : dense_pk_group_indices) {
              const uint32_t psg = local.cur_pk_subgids[gi];
              if (psg == std::numeric_limits<uint32_t>::max()) [[unlikely]] {
                return Status::kSuccess;
              }
              gid = (gid * def_pk_groups[gi].subgid_count) + psg;
            }
            ++local.comparisons;
            DirectGidWorker& dw = direct_gid_workers[w];
            if (use_packed_double_cells) {
              DirectDoubleCell& cell = dw.double_cells[gid];
              cell.meta |= 1ULL;
              const auto charge_packed = [&](double dv) noexcept {
                const double prev = cell.sum;
                const double next = prev + dv;
                uint64_t meta = (cell.meta + 4ULL) | 1ULL;
                if (std::isinf(next) && !std::isinf(prev) && !std::isinf(dv))
                    [[unlikely]] {
                  meta |= 2ULL;
                }
                cell.sum = next;
                cell.meta = meta;
              };
              const LeftNumericAgg& op = left_numeric[0];
              switch (op.op) {
                case 1:
                case 6: {
                  double d0 = 0.0;
                  double d1 = 0.0;
                  if (read_def_agg_double(op.col0, r, &d0) &&
                      read_def_agg_double(op.col1, r, &d1)) [[likely]] {
                    charge_packed(op.op == 1 ? d0 * d1 : d0 * (op.scale - d1));
                  }
                  break;
                }
                case 2: {
                  double dv = 0.0;
                  if (read_def_agg_double(op.col0, r, &dv)) [[likely]] {
                    charge_packed(dv);
                  }
                  break;
                }
                case 9: {
                  double dv = 0.0;
                  bool is_null = false;
                  RETURN_IF_FAIL(
                      eval_double_arith_cells(static_cast<size_t>(op.orig_agg),
                                              r, r_idx, &dv, &is_null));
                  if (!is_null) [[likely]] {
                    charge_packed(dv);
                  }
                  break;
                }
                case 10:
                case 11:
                  charge_right_case_op(
                      op, r_idx, true, [&](double dv) { charge_packed(dv); },
                      [&](int64_t) {}, []() {},
                      [&](int32_t col, double* out) {
                        return read_def_agg_double(col, r, out);
                      },
                      [&](int32_t col) {
                        const auto c = static_cast<size_t>(col);
                        return proj_has_null[c] == 0 ||
                               !proj_cols[c]->IsNull(r);
                      });
                  break;
                default:
                  break;
              }
              return Status::kSuccess;
            }
            dw.matched[gid] = 1;
            const size_t base = static_cast<size_t>(gid) * agg_count;
            for (size_t i = 0; i < agg_count; ++i) {
              const LeftNumericAgg& op = left_numeric[i];
              const size_t slot = base + i;
              switch (op.op) {
                case 0:
                  dw.counts[slot] += 1;
                  break;
                case 1:
                case 6: {
                  double d0 = 0.0;
                  double d1 = 0.0;
                  if (read_def_agg_double(op.col0, r, &d0) &&
                      read_def_agg_double(op.col1, r, &d1)) [[likely]] {
                    add_dw_double(dw, slot,
                                  op.op == 1 ? d0 * d1 : d0 * (op.scale - d1));
                  }
                  break;
                }
                case 2: {
                  double dv = 0.0;
                  if (read_def_agg_double(op.col0, r, &dv)) [[likely]] {
                    add_dw_double(dw, slot, dv);
                  }
                  break;
                }
                case 3: {
                  int64_t iv = 0;
                  if (read_def_agg_int(op.col0, r, &iv)) [[likely]] {
                    add_dw_int(dw, slot, iv);
                  }
                  break;
                }
                case 4: {
                  const auto c0 = static_cast<size_t>(op.col0);
                  if (proj_has_null[c0] == 0 || !proj_cols[c0]->IsNull(r)) {
                    dw.counts[slot] += 1;
                  }
                  break;
                }
                case 7: {
                  const auto c0 = static_cast<size_t>(op.col0);
                  const auto c1 = static_cast<size_t>(op.col1);
                  if ((proj_has_null[c0] == 0 || !proj_cols[c0]->IsNull(r)) &&
                      (proj_has_null[c1] == 0 || !proj_cols[c1]->IsNull(r))) {
                    dw.counts[slot] += 1;
                  }
                  break;
                }
                case 8: {
                  platform::ExtendedFloat x = 0;
                  bool ok_x = false;
                  if (op.is_int_col) {
                    int64_t iv = 0;
                    if (read_def_agg_int(op.col0, r, &iv)) [[likely]] {
                      x = static_cast<platform::ExtendedFloat>(iv);
                      ok_x = true;
                    }
                  } else {
                    double dv = 0.0;
                    if (read_def_agg_double(op.col0, r, &dv)) [[likely]] {
                      x = static_cast<platform::ExtendedFloat>(dv);
                      ok_x = true;
                    }
                  }
                  if (ok_x) [[likely]] {
                    dw.ext_sx[slot] += x;
                    dw.ext_sxx[slot] += x * x;
                    dw.counts[slot] += 1;
                  }
                  break;
                }
                case 9: {
                  double dv = 0.0;
                  bool is_null = false;
                  RETURN_IF_FAIL(
                      eval_double_arith_cells(static_cast<size_t>(op.orig_agg),
                                              r, r_idx, &dv, &is_null));
                  if (!is_null) [[likely]] {
                    add_dw_double(dw, slot, dv);
                  }
                  break;
                }
                case 10:
                case 11:
                case 12:
                case 13:
                  charge_right_case_op(
                      op, r_idx, true,
                      [&](double dv) { add_dw_double(dw, slot, dv); },
                      [&](int64_t iv) { add_dw_int(dw, slot, iv); },
                      [&]() { dw.counts[slot] += 1; },
                      [&](int32_t col, double* out) {
                        return read_def_agg_double(col, r, out);
                      },
                      [&](int32_t col) {
                        const auto c = static_cast<size_t>(col);
                        return proj_has_null[c] == 0 ||
                               !proj_cols[c]->IsNull(r);
                      });
                  break;
                default:
                  break;
              }
            }
            return Status::kSuccess;
          }
          bool key_ok = false;
          int64_t lookup_key = 0;
          uint64_t computed_key_hash = 0;
          bool has_computed_key_hash = false;
          bool need_rep_left = true;
          if (use_split_key_gid) {
            const uint32_t r_subgid =
                !split_uses_right_subgid ? 0U : split_right_subgid_ptr[r_idx];
            int64_t l_val = 0;
            bool l_ok = !split_has_left_probe;
            if (split_has_left_probe) {
              const auto& l_shape = key_shapes[split_left_k];
              if (l_shape.direct_column >= 0) {
                ValueType ltype = ValueType::kNull;
                bool lunsigned = false;
                if (read_def_left_raw(
                        static_cast<size_t>(l_shape.direct_column), r, &l_val,
                        &ltype, &lunsigned) &&
                    !lunsigned &&
                    (ltype == ValueType::kInt64 || ltype == ValueType::kDate) &&
                    l_val >= static_cast<int64_t>(
                                 std::numeric_limits<int32_t>::min()) &&
                    l_val <= static_cast<int64_t>(
                                 std::numeric_limits<int32_t>::max()))
                    [[likely]] {
                  l_ok = true;
                }
              }
            }
            bool pk_ok = true;
            uint64_t pk_packed = 0;
            if (split_uses_pk_subgid) {
              for (size_t gi = 0; gi < def_pk_groups.size(); ++gi) {
                if (def_pk_groups[gi].use_subgid) {
                  if (local.cur_pk_subgids[gi] ==
                      std::numeric_limits<uint32_t>::max()) [[unlikely]] {
                    pk_ok = false;
                    break;
                  }
                  pk_packed |= static_cast<uint64_t>(local.cur_pk_subgids[gi])
                               << def_pk_groups[gi].subgid_shift;
                }
              }
            }
            if (r_subgid != std::numeric_limits<uint32_t>::max() && l_ok &&
                pk_ok) [[likely]] {
              uint64_t packed_u64 = pk_packed;
              if (split_uses_right_subgid) {
                packed_u64 |= static_cast<uint64_t>(r_subgid)
                              << split_right_shift;
              }
              if (split_has_left_probe) {
                packed_u64 |= static_cast<uint64_t>(
                    static_cast<uint32_t>(static_cast<int32_t>(l_val)));
              }
              lookup_key = std::bit_cast<int64_t>(packed_u64);
              computed_key_hash = packed_u64;
              has_computed_key_hash = true;
              need_rep_left = !split_key_unpack_only;
              key_ok = true;
            }
          } else if (packed_2x32_group_key) {
            int64_t v0 = 0;
            int64_t v1 = 0;
            ValueType t0 = ValueType::kNull;
            ValueType t1 = ValueType::kNull;
            bool u0_uns = false;
            bool u1_uns = false;
            if (read_pair_raw_int(packed_2x32_slot0, r, r_idx, &v0, &t0,
                                  &u0_uns) &&
                read_pair_raw_int(packed_2x32_slot1, r, r_idx, &v1, &t1,
                                  &u1_uns) &&
                t0 == packed_2x32_type0 && !u0_uns && t1 == packed_2x32_type1 &&
                !u1_uns &&
                v0 >=
                    static_cast<int64_t>(std::numeric_limits<int32_t>::min()) &&
                v0 <=
                    static_cast<int64_t>(std::numeric_limits<int32_t>::max()) &&
                v1 >=
                    static_cast<int64_t>(std::numeric_limits<int32_t>::min()) &&
                v1 <= static_cast<int64_t>(std::numeric_limits<int32_t>::max()))
                [[likely]] {
              const uint64_t packed_u64 =
                  (static_cast<uint64_t>(
                       static_cast<uint32_t>(static_cast<int32_t>(v0)))
                   << 32) |
                  static_cast<uint32_t>(static_cast<int32_t>(v1));
              lookup_key = std::bit_cast<int64_t>(packed_u64);
              computed_key_hash = packed_u64;
              has_computed_key_hash = true;
              need_rep_left = !packed_2x32_unpack_only;
              key_ok = true;
            }
          } else {
            ValueType kt = ValueType::kNull;
            bool k_uns = false;
            if (read_pair_raw_int(int_group_slot, r, r_idx, &lookup_key, &kt,
                                  &k_uns) &&
                kt == int_group_type && !k_uns) [[likely]] {
              key_ok = true;
            }
          }
          if (key_ok) [[likely]] {
            ++local.comparisons;
            if (use_radix_agg) {
              RETURN_IF_FAIL(scatter_radix_tuple(
                  w, std::bit_cast<uint64_t>(lookup_key),
                  [&](int32_t col, double* out) {
                    return read_def_agg_double(col, r, out);
                  },
                  [&](int32_t col, int64_t* out) {
                    return read_def_agg_int(col, r, out);
                  },
                  [&](int32_t col) {
                    const auto c = static_cast<size_t>(col);
                    return proj_has_null[c] == 0 || !proj_cols[c]->IsNull(r);
                  },
                  [&](int32_t orig_agg, double* out_dv, bool* out_null) {
                    return eval_double_arith_cells(
                        static_cast<size_t>(orig_agg), r, r_idx, out_dv,
                        out_null);
                  }));
              return Status::kSuccess;
            }
            bool inserted = false;
            size_t* slot = local.int_offsets.TryEmplace(lookup_key, &inserted);
            size_t group_index = 0;
            if (!inserted) [[likely]] {
              group_index = *slot;
            } else {
              group_index = local.groups.size();
              FusedGroup group;
              local.raw_arena.Initialize(group, agg_count);
              if (!skip_accumulator_states) {
                group.state_offset = local.states.size();
                for (size_t i = 0; i < fusion->aggregates.size(); ++i) {
                  local.states.emplace_back(fusion->aggregates[i],
                                            aggregate_shapes[i]);
                }
              }
              group.key_hash = computed_key_hash;
              group.has_key_hash = has_computed_key_hash;
              if (need_rep_left) {
                finish_lazy_left_row(r);
                local.owned_rep_left.push_back(left_row);
                group.rep_left = &local.owned_rep_left.back();
              }
              group.rep_right_idx = r_idx;
              local.groups.push_back(std::move(group));
              *slot = group_index;
            }
            FusedGroup& group = local.groups[group_index];
            for (size_t i = 0; i < agg_count; ++i) {
              const LeftNumericAgg& op = left_numeric[i];
              switch (op.op) {
                case 0:
                  group.RawCounts()[i] += 1;
                  break;
                case 1: {
                  double d0 = 0.0;
                  double d1 = 0.0;
                  if (read_def_agg_double(op.col0, r, &d0) &&
                      read_def_agg_double(op.col1, r, &d1)) [[likely]] {
                    const double dv = d0 * d1;
                    const double prev = group.RawDoubles()[i];
                    const double next = prev + dv;
                    if (std::isinf(next) && !std::isinf(prev) &&
                        !std::isinf(dv)) {
                      group.RawInfFlags()[i] = 1;
                    }
                    group.RawDoubles()[i] = next;
                    group.RawCounts()[i] += 1;
                  }
                  break;
                }
                case 2: {
                  double dv = 0.0;
                  if (read_def_agg_double(op.col0, r, &dv)) [[likely]] {
                    const double prev = group.RawDoubles()[i];
                    const double next = prev + dv;
                    if (std::isinf(next) && !std::isinf(prev) &&
                        !std::isinf(dv)) {
                      group.RawInfFlags()[i] = 1;
                    }
                    group.RawDoubles()[i] = next;
                    group.RawCounts()[i] += 1;
                  }
                  break;
                }
                case 3: {
                  int64_t iv = 0;
                  if (read_def_agg_int(op.col0, r, &iv)) [[likely]] {
                    int64_t next = 0;
                    if (__builtin_add_overflow(group.RawInts()[i], iv, &next)) {
                      group.RawInfFlags()[i] = 1;
                    } else {
                      group.RawInts()[i] = next;
                    }
                    group.RawCounts()[i] += 1;
                  }
                  break;
                }
                case 4: {
                  const auto c0 = static_cast<size_t>(op.col0);
                  if (proj_has_null[c0] == 0 || !proj_cols[c0]->IsNull(r)) {
                    group.RawCounts()[i] += 1;
                  }
                  break;
                }
                case 5:
                  break;
                case 6: {
                  double d0 = 0.0;
                  double d1 = 0.0;
                  if (read_def_agg_double(op.col0, r, &d0) &&
                      read_def_agg_double(op.col1, r, &d1)) [[likely]] {
                    const double dv = d0 * (op.scale - d1);
                    const double prev = group.RawDoubles()[i];
                    const double next = prev + dv;
                    if (std::isinf(next) && !std::isinf(prev) &&
                        !std::isinf(dv)) {
                      group.RawInfFlags()[i] = 1;
                    }
                    group.RawDoubles()[i] = next;
                    group.RawCounts()[i] += 1;
                  }
                  break;
                }
                case 7: {
                  const auto c0 = static_cast<size_t>(op.col0);
                  const auto c1 = static_cast<size_t>(op.col1);
                  if ((proj_has_null[c0] == 0 || !proj_cols[c0]->IsNull(r)) &&
                      (proj_has_null[c1] == 0 || !proj_cols[c1]->IsNull(r))) {
                    group.RawCounts()[i] += 1;
                  }
                  break;
                }
                case 8: {
                  platform::ExtendedFloat x = 0;
                  bool ok_x = false;
                  if (op.is_int_col) {
                    int64_t iv = 0;
                    if (read_def_agg_int(op.col0, r, &iv)) [[likely]] {
                      x = static_cast<platform::ExtendedFloat>(iv);
                      ok_x = true;
                    }
                  } else {
                    double dv = 0.0;
                    if (read_def_agg_double(op.col0, r, &dv)) [[likely]] {
                      x = static_cast<platform::ExtendedFloat>(dv);
                      ok_x = true;
                    }
                  }
                  if (ok_x) [[likely]] {
                    group.RawStatX()[i] += x;
                    group.RawStatXX()[i] += x * x;
                    group.RawCounts()[i] += 1;
                  }
                  break;
                }
                case 9: {
                  double dv = 0.0;
                  bool is_null = false;
                  RETURN_IF_FAIL(
                      eval_double_arith_cells(static_cast<size_t>(op.orig_agg),
                                              r, r_idx, &dv, &is_null));
                  if (!is_null) [[likely]] {
                    const double prev = group.RawDoubles()[i];
                    const double next = prev + dv;
                    if (std::isinf(next) && !std::isinf(prev) &&
                        !std::isinf(dv)) {
                      group.RawInfFlags()[i] = 1;
                    }
                    group.RawDoubles()[i] = next;
                    group.RawCounts()[i] += 1;
                  }
                  break;
                }
                case 10:
                case 11:
                case 12:
                case 13:
                  charge_right_case_op(
                      op, r_idx, r_idx != kInvalidRightIdx,
                      [&](double dv) {
                        const double prev = group.RawDoubles()[i];
                        const double next = prev + dv;
                        if (std::isinf(next) && !std::isinf(prev) &&
                            !std::isinf(dv)) {
                          group.RawInfFlags()[i] = 1;
                        }
                        group.RawDoubles()[i] = next;
                        group.RawCounts()[i] += 1;
                      },
                      [&](int64_t iv) {
                        int64_t next = 0;
                        if (__builtin_add_overflow(group.RawInts()[i], iv,
                                                   &next)) {
                          group.RawInfFlags()[i] = 1;
                        } else {
                          group.RawInts()[i] = next;
                        }
                        group.RawCounts()[i] += 1;
                      },
                      [&]() { group.RawCounts()[i] += 1; },
                      [&](int32_t col, double* out) {
                        return read_def_agg_double(col, r, out);
                      },
                      [&](int32_t col) {
                        const auto c = static_cast<size_t>(col);
                        return proj_has_null[c] == 0 ||
                               !proj_cols[c]->IsNull(r);
                      });
                  break;
                default:
                  break;
              }
            }
            return Status::kSuccess;
          }
        }
        if (!materialize_current_left_row(r)) {
          return Status::kSuccess;
        }
        ++local.comparisons;
        if (use_preassigned_build_groups) {
          return accumulate_preassigned_pair(w, scratch, left_row, r_idx);
        }
        size_t cached_group = kNoCachedGroup;
        ASSIGN_OR_RETURN(
            bool, accumulated,
            process_pair(local, left_row, r_idx, scratch, &cached_group));
        (void)accumulated;
        return Status::kSuccess;
      };
      auto eval_int_peek = [](BinaryOperation op, int64_t a,
                              int64_t b) noexcept -> bool {
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
      };
      if (scalar_fact_charge) {
        DirectGidWorker& dw = direct_gid_workers[w];
        while (true) {
          const size_t bi = next_block();
          if (bi >= total_blocks) {
            return Status::kSuccess;
          }
          const LSTablePageBlock& blk = store.blocks[bi];
          if (blk.row_count == 0 ||
              !blk.MatchesZoneMaps(peek_ptr, active_rf_group, full_rf_col)) {
            continue;
          }
          for (size_t c = 0; c < base_proj_cols; ++c) {
            const LSTableColumnBlock& cblk =
                blk.columns[deferred_left_probe->projection[c]];
            proj_cols[c] = &cblk;
            proj_int_ptrs[c] = cblk.int_cells.data();
            proj_has_null[c] = static_cast<uint8_t>(cblk.has_null);
          }
          const LSTableColumnBlock* const rf_col =
              full_rf_col.has_value() ? &blk.columns[*full_rf_col] : nullptr;
          struct ScalarExtraRf {
            const LSTableColumnBlock* col{nullptr};
            const uint64_t* dense_bits{nullptr};
            int64_t lo{0};
            int64_t hi{0};
            bool has_null{false};
            // ScanRuntimeFilter's contract (common/runtime_filter.hpp): a
            // value whose decoded type differs from the filter's domain is
            // never rejected. int_cells only describes INT64/DATE blocks, so
            // the row-level reads below must be skipped for anything else
            // instead of comparing incomparable bit patterns.
            bool type_ok{true};
            const RuntimeFilterGroup* group{nullptr};
          };
          std::array<ScalarExtraRf, 4> extra_rfs{};
          size_t extra_rf_count = 0;
          for (const ScanRuntimeFilter& ext : primary_rf.extra) {
            if (!ext.enabled() || ext.column >= base_proj_cols ||
                extra_rf_count >= extra_rfs.size()) {
              continue;
            }
            const LSTableColumnBlock* ecol = proj_cols[ext.column];
            const bool hd = ext.group->has_dense_bits();
            extra_rfs[extra_rf_count++] = ScalarExtraRf{
                .col = ecol,
                .dense_bits = hd ? ext.group->dense_bits_data() : nullptr,
                .lo = hd ? ext.group->Lo() : 0,
                .hi = hd ? ext.group->Hi() : 0,
                .has_null = ecol->has_null,
                .type_ok = (ecol->type == ext.type),
                .group = ext.group.get(),
            };
          }
          for (uint32_t r = 0; r < blk.row_count; ++r) {
            if (std::ranges::any_of(deferred_left_probe->null_peeks,
                                    [&](const NullPeekCompare& test) {
                                      return blk.columns[test.column].IsNull(
                                                 r) != test.expect_null;
                                    })) {
              continue;
            }
            if (rf_col != nullptr) {
              if (rf_col->has_null && rf_col->IsNull(r)) {
                continue;
              }
              const int64_t kv = rf_col->int_cells[r];
              if (rf_has_dense) {
                const uint64_t off =
                    static_cast<uint64_t>(kv) - static_cast<uint64_t>(rf_lo);
                if (kv < rf_lo || kv > rf_hi ||
                    (rf_dense_bits[off >> 6] & (uint64_t{1} << (off & 63))) ==
                        0) {
                  continue;
                }
              } else if (active_rf_group->Reject(kv)) {
                continue;
              }
            }
            bool extra_ok = true;
            for (size_t ei = 0; ei < extra_rf_count; ++ei) {
              const ScalarExtraRf& erf = extra_rfs[ei];
              if (erf.has_null && erf.col->IsNull(r)) {
                extra_ok = false;
                break;
              }
              if (!erf.type_ok) {
                continue;
              }
              const int64_t kv = erf.col->int_cells[r];
              if (erf.dense_bits != nullptr) {
                if (kv < erf.lo || kv > erf.hi) {
                  extra_ok = false;
                  break;
                }
                const uint64_t off =
                    static_cast<uint64_t>(kv) - static_cast<uint64_t>(erf.lo);
                if ((erf.dense_bits[off >> 6] & (uint64_t{1} << (off & 63))) ==
                    0) {
                  extra_ok = false;
                  break;
                }
              } else if (erf.group->Reject(kv)) {
                extra_ok = false;
                break;
              }
            }
            if (!extra_ok) {
              continue;
            }
            if (peek_ptr != nullptr) {
              bool peek_ok = true;
              for (const IntegerPeekCompare& pred : *peek_ptr) {
                const LSTableColumnBlock& pcol = blk.columns[pred.column];
                if ((pcol.has_null && pcol.IsNull(r)) ||
                    !eval_int_peek(pred.op, pcol.int_cells[r], pred.constant)) {
                  peek_ok = false;
                  break;
                }
              }
              if (!peek_ok) {
                continue;
              }
            }
            ++local.comparisons;
            charge_numeric(
                dw, 0, 0,
                [&](int32_t col, double* out) {
                  const auto c = static_cast<size_t>(col);
                  if (proj_has_null[c] != 0 && proj_cols[c]->IsNull(r)) {
                    return false;
                  }
                  const int64_t raw = proj_int_ptrs[c][r];
                  if (def_base_types[c] == ValueType::kDouble) {
                    *out = std::bit_cast<double>(raw);
                  } else {
                    *out = static_cast<double>(raw);
                  }
                  return true;
                },
                [&](int32_t col, int64_t* out) {
                  const auto c = static_cast<size_t>(col);
                  if (proj_has_null[c] != 0 && proj_cols[c]->IsNull(r)) {
                    return false;
                  }
                  *out = proj_int_ptrs[c][r];
                  return true;
                },
                [&](int32_t col) {
                  const auto c = static_cast<size_t>(col);
                  return proj_has_null[c] == 0 || !proj_cols[c]->IsNull(r);
                });
          }
        }
      }
      const slot_t jk0 = left_columns[0];
      const slot_t jk1 = packed_2x32_int_key ? left_columns[1] : 0;
      const int64_t off0 = left_key_offsets[0];
      const int64_t off1 = packed_2x32_int_key ? left_key_offsets[1] : 0;
      bool charge_from_cells = left_numeric_probe &&
                               prepared_pk_lookups.empty() &&
                               use_dense_int_rows;
      if (charge_from_cells) {
        for (size_t ai = 0; ai < agg_count; ++ai) {
          const LeftNumericAgg& op = left_numeric[ai];
          if ((op.col0 >= 0 &&
               (std::cmp_greater_equal(op.col0, base_proj_cols) ||
                def_overwritten_by_pk[static_cast<size_t>(op.col0)] != 0)) ||
              (op.col1 >= 0 &&
               (std::cmp_greater_equal(op.col1, base_proj_cols) ||
                def_overwritten_by_pk[static_cast<size_t>(op.col1)] != 0))) {
            charge_from_cells = false;
            break;
          }
        }
      }
      // The cache owns contiguous numeric columns as well as page views.
      // A monotonic unique-build probe can scan those columns without doing
      // projection and zone-map setup for every small heap page. NULL-bearing
      // columns and unsupported expressions retain the page-aware path.
      const auto flat_column = [&](size_t c) -> const int64_t* {
        if (c >= store.integer_columns.size() ||
            c >= store.column_has_null.size() || store.column_has_null[c] ||
            store.integer_columns[c].size() != store.total_rows) {
          return nullptr;
}
        return store.integer_columns[c].data();
      };
      const size_t full_jk = deferred_left_probe->projection[jk0];
      bool flat_ready = charge_from_cells && single_double_sum && integer_key &&
                        !ranked_int_bits.empty() && off0 == 0 &&
                        deferred_left_probe->null_peeks.empty() &&
                        full_jk < store.column_is_monotonic.size() &&
                        store.column_is_monotonic[full_jk] != 0 &&
                        (left_numeric[0].op == 1 || left_numeric[0].op == 2 ||
                         left_numeric[0].op == 6);
      const int64_t* flat_keys = flat_ready ? flat_column(full_jk) : nullptr;
      const int64_t* flat_a = nullptr;
      const int64_t* flat_b = nullptr;
      struct FlatPredicate {
        const int64_t* cells;
        BinaryOperation op;
        int64_t value;
      };
      struct FlatFilter {
        const int64_t* cells;
        const RuntimeFilterGroup* group;
      };
      std::vector<FlatPredicate> flat_predicates;
      std::vector<FlatFilter> flat_filters;
      if (flat_ready) {
        const auto& op = left_numeric[0];
        flat_a = flat_column(deferred_left_probe->projection[op.col0]);
        if (op.op != 2) {
          flat_b = flat_column(deferred_left_probe->projection[op.col1]);
}
        flat_ready = flat_keys && flat_a && (op.op == 2 || flat_b);
        if (peek_ptr) {
          for (const auto& pred : *peek_ptr) {
            const auto* cells = flat_column(pred.column);
            flat_ready = flat_ready && cells;
            flat_predicates.push_back({cells, pred.op, pred.constant});
          }
        }
        const auto add_filter = [&](const ScanRuntimeFilter& filter) {
          if (!filter.enabled() || filter.column >= base_proj_cols) { return;
}
          const auto* cells =
              flat_column(deferred_left_probe->projection[filter.column]);
          flat_ready = flat_ready && cells;
          flat_filters.push_back({cells, filter.group.get()});
        };
        add_filter(primary_rf);
        for (const auto& filter : primary_rf.extra) { add_filter(filter);
}
      }
      if (flat_ready) {
        DirectGidWorker& dw =
            direct_gid_workers[disjoint_direct_groups ? 0 : w];
        const auto& op = left_numeric[0];
        const bool a_double = def_base_types[op.col0] == ValueType::kDouble;
        const bool b_double =
            op.op != 2 && def_base_types[op.col1] == ValueType::kDouble;
        const auto add_row = [&](uint32_t gid, size_t r) {
          ++local.comparisons;
          dw.matched[gid] = 1;
          const double a = a_double ? std::bit_cast<double>(flat_a[r])
                                    : static_cast<double>(flat_a[r]);
          const double b = op.op == 2
                               ? 0
                               : (b_double ? std::bit_cast<double>(flat_b[r])
                                           : static_cast<double>(flat_b[r]));
          add_dw_double(
              dw, gid,
              op.op == 2 ? a : (op.op == 1 ? a * b : a * (op.scale - b)));
        };
        const bool single_greater =
            flat_predicates.size() == 1 &&
            flat_predicates[0].op == BinaryOperation::kGreaterThan &&
            std::ranges::all_of(flat_filters, [&](const auto& filter) {
              return filter.cells == flat_keys;
            });
        const auto consume_range = [&](int64_t key, uint32_t gid, size_t begin,
                                       size_t end) {
          if (gid == kInvalidGid) { return;
}
          for (const auto& filter : flat_filters) {
            if (filter.cells == flat_keys && filter.group->Reject(key)) { return;
}
          }
          // A single range predicate over a non-NULL flat column needs one
          // comparison per candidate, with no per-row predicate dispatch or
          // membership loop. Other shapes retain the general evaluator.
          if (single_greater) {
            const auto* cells = flat_predicates[0].cells;
            const int64_t threshold = flat_predicates[0].value;
            for (size_t r = begin; r < end; ++r) {
              if (cells[r] > threshold) { add_row(gid, r);
}
}
            return;
          }
          for (size_t r = begin; r < end; ++r) {
            bool selected = true;
            for (const auto& pred : flat_predicates) {
              if (!eval_int_peek(pred.op, pred.cells[r], pred.value)) {
                selected = false;
                break;
              }
            }
            if (selected) {
              for (const auto& filter : flat_filters) {
                if (filter.cells != flat_keys &&
                    filter.group->Reject(filter.cells[r])) {
                  selected = false;
                  break;
                }
              }
            }
            if (selected) { add_row(gid, r);
}
          }
        };
        if (flat_run_index) {
          struct PendingRun {
            int64_t key;
            uint32_t gid;
            uint32_t begin;
            uint32_t end;
          };
          std::array<PendingRun, 128> pending{};
          size_t pending_count = 0;
          const auto flush_runs = [&]() {
            // Separate index reads and fact reads so several independent
            // cache misses can be in flight instead of one per matching key.
            for (size_t i = 0; i < pending_count; ++i) {
              const auto off = static_cast<uint64_t>(pending[i].key) -
                               static_cast<uint64_t>(flat_run_index->minimum);
              if (flat_run_bounds && off < flat_run_bounds->packed.size()) {
                __builtin_prefetch(flat_run_bounds->packed.data() + off, 0, 1);
              } else if (off < flat_run_index->offsets.size() - 1) {
                __builtin_prefetch(flat_run_index->offsets.data() + off, 0, 1);
                __builtin_prefetch(flat_run_index->offsets.data() + off + 1, 0,
                                   1);
              }
            }
            for (size_t i = 0; i < pending_count; ++i) {
              auto& run = pending[i];
              if (flat_run_bounds && !flat_run_bounds->MayExceed(
                                         run.key, flat_predicates[0].value)) {
                run.begin = run.end = 0;
                continue;
              }
              std::tie(run.begin, run.end) = flat_run_index->Find(run.key);
              if (run.begin == run.end) { continue;
}
              for (const auto& pred : flat_predicates) {
                __builtin_prefetch(pred.cells + run.begin, 0, 1);
                if (run.end - run.begin > 8) {
                  __builtin_prefetch(pred.cells + run.begin + 8, 0, 1);
}
              }
              for (const auto& filter : flat_filters) {
                if (filter.cells != flat_keys) {
                  __builtin_prefetch(filter.cells + run.begin, 0, 1);
}
              }
            }
            for (size_t i = 0; i < pending_count; ++i) {
              const auto& run = pending[i];
              if (run.begin != run.end) {
                consume_range(run.key, run.gid, run.begin, run.end);
}
            }
            pending_count = 0;
          };
          if (key_cache_charge) {
            for (size_t ci = w; ci < key_cache.size(); ci += workers) {
              for (const auto& entry : key_cache[ci]) {
                pending[pending_count++] = {.key=entry.key, .gid=gid_of(entry.row), .begin=0, .end=0};
                if (pending_count == pending.size()) { flush_runs();
}
              }
            }
          } else {
            const size_t first_word = ranked_int_bits.size() * w / workers;
            const size_t last_word = ranked_int_bits.size() * (w + 1) / workers;
            for (size_t wi = first_word; wi < last_word; ++wi) {
              uint64_t bits = ranked_int_bits[wi];
              uint32_t rank = ranked_int_prefix[wi];
              while (bits != 0) {
                const int64_t key =
                    dense_int_min +
                    static_cast<int64_t>((wi * 64) + std::countr_zero(bits));
                const uint32_t gid = gid_of(ranked_int_rows[rank++]);
                pending[pending_count++] = {.key=key, .gid=gid, .begin=0, .end=0};
                if (pending_count == pending.size()) { flush_runs();
}
                bits &= bits - 1;
              }
            }
          }
          flush_runs();
          return Status::kSuccess;
        }
        size_t r = store.total_rows * w / workers;
        const size_t end = store.total_rows * (w + 1) / workers;
        while (r < end) {
          const int64_t key = flat_keys[r];
          uint64_t idx =
              static_cast<uint64_t>(key) - static_cast<uint64_t>(dense_int_min);
          if (key >= dense_int_min && idx >= ranked_int_span) { break;
}
          if (key < dense_int_min ||
              (ranked_int_bits[idx >> 6] & (uint64_t{1} << (idx & 63))) == 0) {
            idx = key < dense_int_min ? 0 : idx + 1;
            size_t wi = idx >> 6;
            if (wi >= ranked_int_bits.size()) { break;
}
            uint64_t bits = ranked_int_bits[wi] & (~uint64_t{0} << (idx & 63));
            while (bits == 0 && ++wi < ranked_int_bits.size()) {
              bits = ranked_int_bits[wi];
}
            if (wi == ranked_int_bits.size()) { break;
}
            const int64_t next_key =
                dense_int_min +
                static_cast<int64_t>((wi * 64) + std::countr_zero(bits));
            size_t lo = r + 1, step = 4;
            while (step < end - lo && flat_keys[lo + step] < next_key) {
              lo += step;
              step = std::min(step * 2, end - lo);
            }
            const size_t hi = lo + std::min(step + 1, end - lo);
            r = static_cast<size_t>(
                std::lower_bound(flat_keys + lo, flat_keys + hi, next_key) -
                flat_keys);
            continue;
          }
          const uint64_t word = ranked_int_bits[idx >> 6];
          const uint64_t lower = (uint64_t{1} << (idx & 63)) - 1;
          const uint32_t rank =
              ranked_int_prefix[idx >> 6] +
              static_cast<uint32_t>(std::popcount(word & lower));
          const uint32_t gid = gid_of(ranked_int_rows[rank]);
          const size_t begin = r;
          do {
            ++r;
          } while (r < end && flat_keys[r] == key);
          consume_range(key, gid, begin, r);
        }
        return Status::kSuccess;
      }
      while (true) {
        const size_t bi = next_block();
        if (bi >= total_blocks) {
          return Status::kSuccess;
        }
        const LSTablePageBlock& blk = store.blocks[bi];
        if (blk.row_count == 0) {
          continue;
        }
        if (!blk.MatchesZoneMaps(peek_ptr, active_rf_group, full_rf_col)) {
          continue;
        }
        bool extra_zm_reject = false;
        for (const ScanRuntimeFilter& ext : primary_rf.extra) {
          if (!ext.enabled() || ext.column >= base_proj_cols) {
            continue;
          }
          if (ext.group->RejectsAll()) {
            extra_zm_reject = true;
            break;
          }
          const slot_t ext_full = deferred_left_probe->projection[ext.column];
          if (ext_full < blk.columns.size()) {
            const LSTableColumnBlock& mcol = blk.columns[ext_full];
            // RejectRange speaks the int64 key domain; a block column of
            // any other type must not feed it (same guard as the sister
            // delayed-scan loop and RuntimeFilterGroup::Reject).
            if ((mcol.type == ValueType::kInt64 ||
                 mcol.type == ValueType::kDate) &&
                mcol.type == ext.type &&
                (!mcol.has_min_max ||
                 ext.group->RejectRange(mcol.min_int, mcol.max_int))) {
              extra_zm_reject = true;
              break;
            }
          }
        }
        if (extra_zm_reject) {
          continue;
        }
        // A dense unique build key occupies [dense_int_min, dense_int_min +
        // span). A block whose join key is entirely outside that range cannot
        // match an inner probe, including NULL keys (NULL never matches).
        if (use_dense_int_rows && integer_key && !dense_int_idxs.empty() &&
            jk0 < base_proj_cols && left_key_offsets[0] == 0) {
          const slot_t full_key = deferred_left_probe->projection[jk0];
          if (full_key < blk.columns.size()) {
            const LSTableColumnBlock& jk = blk.columns[full_key];
            if (jk.has_min_max) {
              const uint64_t span = dense_int_idxs.size();
              const bool above = jk.min_int >= dense_int_min &&
                                 static_cast<uint64_t>(jk.min_int) -
                                         static_cast<uint64_t>(dense_int_min) >=
                                     span;
              if (jk.max_int < dense_int_min || above) {
                continue;
              }
            }
          }
        }
        bool peek_all_pass = true;
        if (peek_ptr != nullptr) {
          for (const IntegerPeekCompare& pred : *peek_ptr) {
            const LSTableColumnBlock& pcol = blk.columns[pred.column];
            if (pcol.has_null || !pcol.has_min_max) {
              peek_all_pass = false;
              break;
            }
            bool all_ok = false;
            switch (pred.op) {
              case BinaryOperation::kEquals:
                all_ok = (pcol.min_int == pred.constant &&
                          pcol.max_int == pred.constant);
                break;
              case BinaryOperation::kNotEquals:
                all_ok = (pred.constant < pcol.min_int ||
                          pred.constant > pcol.max_int);
                break;
              case BinaryOperation::kLessThan:
                all_ok = (pcol.max_int < pred.constant);
                break;
              case BinaryOperation::kLessThanEquals:
                all_ok = (pcol.max_int <= pred.constant);
                break;
              case BinaryOperation::kGreaterThan:
                all_ok = (pcol.min_int > pred.constant);
                break;
              case BinaryOperation::kGreaterThanEquals:
                all_ok = (pcol.min_int >= pred.constant);
                break;
              default:
                break;
            }
            if (!all_ok) {
              peek_all_pass = false;
              break;
            }
          }
        }
        for (size_t c = 0; c < base_proj_cols; ++c) {
          const LSTableColumnBlock& cblk =
              blk.columns[deferred_left_probe->projection[c]];
          proj_cols[c] = &cblk;
          proj_int_ptrs[c] = cblk.int_cells.data();
          proj_has_null[c] = static_cast<uint8_t>(cblk.has_null);
        }
        const LSTableColumnBlock* const rf_col =
            full_rf_col.has_value() ? &blk.columns[*full_rf_col] : nullptr;
        const bool rf_col_has_null = rf_col != nullptr && rf_col->has_null;
        const bool blk_rf_monotonic = rf_col_monotonic && !rf_col_has_null;
        struct DefExtraRf {
          const LSTableColumnBlock* col{nullptr};
          const uint64_t* dense_bits{nullptr};
          int64_t lo{0};
          int64_t hi{0};
          bool has_null{false};
          // See ScalarExtraRf: the decoded type must match the filter's
          // domain before int_cells is read.
          bool type_ok{true};
          const RuntimeFilterGroup* group{nullptr};
        };
        std::array<DefExtraRf, 4> extra_rfs{};
        size_t extra_rf_count = 0;
        for (const ScanRuntimeFilter& ext : primary_rf.extra) {
          if (!ext.enabled() || ext.column >= base_proj_cols ||
              extra_rf_count >= extra_rfs.size()) {
            continue;
          }
          const LSTableColumnBlock* ecol = proj_cols[ext.column];
          const bool hd = ext.group->has_dense_bits();
          extra_rfs[extra_rf_count++] = DefExtraRf{
              .col = ecol,
              .dense_bits = hd ? ext.group->dense_bits_data() : nullptr,
              .lo = hd ? ext.group->Lo() : 0,
              .hi = hd ? ext.group->Hi() : 0,
              .has_null = ecol->has_null,
              .type_ok = (ecol->type == ext.type),
              .group = ext.group.get(),
          };
        }
        const int64_t* const jk0_ints = proj_int_ptrs[jk0];
        const int64_t* const jk1_ints =
            packed_2x32_int_key ? proj_int_ptrs[jk1] : nullptr;
        const bool jk_has_null =
            proj_has_null[jk0] != 0 ||
            (packed_2x32_int_key && proj_has_null[jk1] != 0);
        const bool fast_1pk_dense =
            (use_dense_int_rows || use_unique_int_table) &&
            !packed_2x32_int_key && !bulk_raw_count &&
            (use_dense_int_rows || (!count_star_only && !charge_from_cells)) &&
            !(count_star_only && use_direct_gid_agg &&
              prepared_pk_lookups.empty()) &&
            !charge_from_cells && def_pk_groups.size() == 1;
        const size_t split_lcol =
            (fast_1pk_dense && split_has_left_probe &&
             split_left_k < key_shapes.size() &&
             key_shapes[split_left_k].direct_column >= 0 &&
             std::cmp_less(key_shapes[split_left_k].direct_column, left_width))
                ? static_cast<size_t>(key_shapes[split_left_k].direct_column)
                : base_proj_cols;
        const int8_t split_left_li =
            split_lcol < 16 ? def_col_to_pk_lookup[split_lcol] : int8_t{-1};
        const bool split_left_from_pk0 =
            fast_1pk_dense && split_has_left_probe && split_left_li >= 0 &&
            prepared_pk_lookups[static_cast<size_t>(split_left_li)].group_idx ==
                0;
        const bool split_left_direct = fast_1pk_dense && split_has_left_probe &&
                                       split_left_li < 0 &&
                                       split_lcol < base_proj_cols;
        const bool fast_split_left_ok =
            !split_has_left_probe || split_left_from_pk0 || split_left_direct;
        const bool fast_split_arith_agg =
            fast_1pk_dense && fast_split_left_ok && def_group_cell_probe &&
            use_split_key_gid && split_key_unpack_only &&
            !use_dense_split_direct_gid && !use_radix_agg &&
            !packed_2x32_group_key && skip_accumulator_states &&
            agg_count == 1 && left_numeric[0].op == 9 &&
            compiled_double_arith[static_cast<size_t>(left_numeric[0].orig_agg)]
                    .pattern != CompiledDefDoubleArith::Pattern::kGeneral;
        const bool fast_split_col_agg =
            fast_1pk_dense && fast_split_left_ok && def_group_cell_probe &&
            use_split_key_gid && split_key_unpack_only &&
            !use_dense_split_direct_gid && !use_radix_agg &&
            !packed_2x32_group_key && skip_accumulator_states &&
            agg_count == 1 && left_numeric[0].op == 2;
        const bool fast_split_count_or_distinct =
            fast_1pk_dense && fast_split_left_ok && !def_group_cell_probe &&
            !def_compact_split_probe &&
            (count_star_only || fusion->aggregates.empty()) &&
            !use_preassigned_build_groups && !compact_counts &&
            !raw_sum_fusion && residual_programs.empty() && use_split_key_gid &&
            split_key_unpack_only && !packed_2x32_group_key &&
            join_type == JoinType::kInner &&
            !fusion->rollup_count_distribution && all_direct_group_keys &&
            !left.HasSpill();
        const int64_t* const split_lcol_ints =
            split_left_direct ? proj_int_ptrs[split_lcol] : nullptr;
        const LSTableColumnBlock* const split_lcol_blk =
            split_left_direct ? proj_cols[split_lcol] : nullptr;
        const uint8_t split_lcol_has_null =
            split_left_direct ? proj_has_null[split_lcol] : uint8_t{0};
        const ValueType split_lcol_vt =
            split_left_direct ? def_base_types[split_lcol] : ValueType::kNull;
        const bool split_lcol_type_ok =
            split_left_direct && (split_lcol_vt == ValueType::kInt64 ||
                                  split_lcol_vt == ValueType::kDate);
        bool have_cached_pk_lval = false;
        int64_t cached_pk_key = 0;
        bool cached_pk_lval_ok = !split_has_left_probe;
        uint32_t cached_pk_lval_bits = 0;
        const PreparedDeferredPkGroup* const pk0_ptr =
            fast_1pk_dense ? def_pk_groups.data() : nullptr;
        DefPkGroupCursor* const cur0_ptr =
            fast_1pk_dense ? pk_group_cursors.data() : nullptr;
        const LSTableColumnBlock* const pk0_col =
            fast_1pk_dense ? proj_cols[pk0_ptr->pk_col] : nullptr;
        const int64_t* const pk0_ints =
            fast_1pk_dense ? proj_int_ptrs[pk0_ptr->pk_col] : nullptr;
        const uint8_t pk0_has_null =
            fast_1pk_dense ? proj_has_null[pk0_ptr->pk_col] : uint8_t{0};
        const int64_t pk0_min_k = fast_1pk_dense ? pk0_ptr->min_k : 0;
        const int64_t pk0_max_k = fast_1pk_dense ? pk0_ptr->max_k : -1;
        const bool pk0_use_subgid =
            fast_1pk_dense && pk0_ptr->use_subgid && split_uses_pk_subgid;
        const uint8_t pk0_shift =
            fast_1pk_dense ? pk0_ptr->subgid_shift : uint8_t{0};
        const bool fast_dense_split_probe =
            use_dense_split_direct_gid && !use_shared_direct_gid &&
            !use_direct_gid_agg &&
            (use_dense_int_rows || use_unique_int_table) &&
            !def_compact_split_probe && !def_pk_groups.empty() &&
            def_pk_groups.size() <= 3 && agg_count == 1 &&
            (left_numeric[0].op == 0 || left_numeric[0].op == 2 ||
             left_numeric[0].op == 3 || left_numeric[0].op == 9) &&
            (left_numeric[0].op != 9 ||
             compiled_double_arith[static_cast<size_t>(
                                       left_numeric[0].orig_agg)]
                     .pattern != CompiledDefDoubleArith::Pattern::kGeneral) &&
            std::ranges::all_of(def_pk_groups, [](const auto& g) {
              return g.dense_ptr != nullptr && g.total_rows > 0 &&
                     g.max_k >= g.min_k && g.dense_size == g.span;
            });
        const bool want_da_arith =
            fast_split_arith_agg ||
            (fast_dense_split_probe && left_numeric[0].op == 9);
        const bool want_da_col =
            fast_split_col_agg ||
            (fast_dense_split_probe && left_numeric[0].op == 2);
        const bool want_da_int =
            fast_dense_split_probe && left_numeric[0].op == 3;
        const size_t dpk_n = fast_dense_split_probe ? def_pk_groups.size() : 0;
        uint32_t dense_right_stride = 1;
        std::array<uint32_t, 3> dpk_stride{};
        std::array<int64_t, 3> dpk_min{};
        std::array<uint64_t, 3> dpk_span{};
        std::array<const decltype(def_pk_groups)::value_type::DensePkEntry*, 3>
            dpk_dense{};
        std::array<uint8_t, 3> dpk_use_subgid{};
        std::array<const LSTableColumnBlock*, 3> dpk_col{};
        std::array<const int64_t*, 3> dpk_ints{};
        std::array<uint8_t, 3> dpk_has_null{};
        if (fast_dense_split_probe) {
          for (size_t idx = dense_pk_group_indices.size(); idx > 0; --idx) {
            const size_t gi = dense_pk_group_indices[idx - 1];
            dpk_stride[gi] = dense_right_stride;
            dense_right_stride *= def_pk_groups[gi].subgid_count;
          }
          for (size_t gi = 0; gi < dpk_n; ++gi) {
            const auto& g = def_pk_groups[gi];
            dpk_min[gi] = g.min_k;
            dpk_span[gi] = static_cast<uint64_t>(g.dense_size);
            dpk_dense[gi] = g.dense_ptr;
            dpk_use_subgid[gi] = static_cast<uint8_t>(g.use_subgid ? 1 : 0);
            dpk_col[gi] = proj_cols[static_cast<size_t>(g.pk_col)];
            dpk_ints[gi] = proj_int_ptrs[static_cast<size_t>(g.pk_col)];
            dpk_has_null[gi] = proj_has_null[static_cast<size_t>(g.pk_col)];
          }
        }
        DirectGidWorker* const dw_fast_ptr =
            fast_dense_split_probe ? &direct_gid_workers[w] : nullptr;
        DirectDoubleCell* const dw_packed_cells =
            (fast_dense_split_probe && use_packed_double_cells)
                ? dw_fast_ptr->double_cells.data()
                : nullptr;
        const CompiledDefDoubleArith* const da_plan =
            want_da_arith ? &compiled_double_arith[static_cast<size_t>(
                                left_numeric[0].orig_agg)]
                          : nullptr;
        const auto da_pattern = want_da_arith
                                    ? da_plan->pattern
                                    : CompiledDefDoubleArith::Pattern::kGeneral;
        const uint32_t da_c0 =
            want_da_arith
                ? da_plan->fast_cols[0]
                : (want_da_col
                       ? static_cast<uint32_t>(left_numeric[0].col0)
                       : (want_da_int
                              ? static_cast<uint32_t>(left_numeric[0].col0)
                              : 0U));
        const uint32_t da_c1 = want_da_arith ? da_plan->fast_cols[1] : 0U;
        const uint32_t da_c2 = want_da_arith ? da_plan->fast_cols[2] : 0U;
        const uint32_t da_c3 = want_da_arith ? da_plan->fast_cols[3] : 0U;
        const double da_div = want_da_arith ? da_plan->fast_div : 1.0;
        const int64_t* const da_p0 =
            (want_da_arith || want_da_col || want_da_int) ? proj_int_ptrs[da_c0]
                                                          : nullptr;
        const int64_t* const da_p1 =
            want_da_arith ? proj_int_ptrs[da_c1] : nullptr;
        const int64_t* const da_p2 =
            want_da_arith ? proj_int_ptrs[da_c2] : nullptr;
        const int64_t* const da_p3 =
            want_da_arith ? proj_int_ptrs[da_c3] : nullptr;
        const bool da_c0_is_double =
            want_da_col && def_base_types[da_c0] == ValueType::kDouble;
        const bool da_all_nonnull =
            want_da_arith
                ? ((da_pattern ==
                    CompiledDefDoubleArith::Pattern::kSubSubAddDivConst)
                       ? ((proj_has_null[da_c0] | proj_has_null[da_c1] |
                           proj_has_null[da_c2] | proj_has_null[da_c3]) == 0)
                       : ((proj_has_null[da_c0] | proj_has_null[da_c1]) == 0))
                : ((want_da_col || want_da_int) ? (proj_has_null[da_c0] == 0)
                                                : false);
        for (uint32_t r = 0; r < blk.row_count; ++r) {
          if (std::ranges::any_of(deferred_left_probe->null_peeks,
                                  [&](const NullPeekCompare& test) {
                                    return blk.columns[test.column].IsNull(r) !=
                                           test.expect_null;
                                  })) {
            continue;
          }
          if (rf_col != nullptr) {
            if (rf_col_has_null && rf_col->IsNull(r)) [[unlikely]] {
              continue;
            }
            const int64_t kv = rf_col->int_cells[r];
            if (rf_has_dense) {
              bool accepted = false;
              if (kv >= rf_lo && kv <= rf_hi) {
                const uint64_t off =
                    static_cast<uint64_t>(kv) - static_cast<uint64_t>(rf_lo);
                accepted = (rf_dense_bits[off >> 6] &
                            (uint64_t{1} << (off & 63))) != 0;
              }
              if (!accepted) {
                if (blk_rf_monotonic) {
                  if (kv > rf_hi) {
                    break;
                  }
                  int64_t next_valid_key = rf_lo;
                  if (kv >= rf_lo) {
                    const uint64_t next_off = static_cast<uint64_t>(kv) -
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
                  if (rf_col->has_min_max && next_valid_key > rf_col->max_int) {
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
            } else if (active_rf_group->Reject(kv)) {
              continue;
            }
          }
          if (extra_rf_count != 0) {
            bool extra_ok = true;
            for (size_t ei = 0; ei < extra_rf_count; ++ei) {
              const DefExtraRf& erf = extra_rfs[ei];
              if (erf.has_null && erf.col->IsNull(r)) [[unlikely]] {
                extra_ok = false;
                break;
              }
              if (!erf.type_ok) {
                continue;
              }
              const int64_t kv = erf.col->int_cells[r];
              if (erf.dense_bits != nullptr) {
                if (kv < erf.lo || kv > erf.hi) {
                  extra_ok = false;
                  break;
                }
                const uint64_t off =
                    static_cast<uint64_t>(kv) - static_cast<uint64_t>(erf.lo);
                if ((erf.dense_bits[off >> 6] & (uint64_t{1} << (off & 63))) ==
                    0) {
                  extra_ok = false;
                  break;
                }
              } else if (erf.group->Reject(kv)) {
                extra_ok = false;
                break;
              }
            }
            if (!extra_ok) {
              continue;
            }
          }
          if (!peek_all_pass && peek_ptr != nullptr) {
            bool peek_ok = true;
            for (const IntegerPeekCompare& pred : *peek_ptr) {
              const LSTableColumnBlock& pcol = blk.columns[pred.column];
              if ((pcol.has_null && pcol.IsNull(r)) ||
                  !eval_int_peek(pred.op, pcol.int_cells[r], pred.constant)) {
                peek_ok = false;
                break;
              }
            }
            if (!peek_ok) {
              continue;
            }
          }
          if (jk_has_null) [[unlikely]] {
            if ((proj_has_null[jk0] != 0 && proj_cols[jk0]->IsNull(r)) ||
                (packed_2x32_int_key && proj_has_null[jk1] != 0 &&
                 proj_cols[jk1]->IsNull(r))) {
              continue;
            }
          }
          int64_t key = 0;
          if (!packed_2x32_int_key) {
            key = AddJoinKeyBias(jk0_ints[r], off0);
          } else {
            const int64_t k0 = AddJoinKeyBias(jk0_ints[r], off0);
            const int64_t k1 = AddJoinKeyBias(jk1_ints[r], off1);
            if (!fits_fused_i32(k0) || !fits_fused_i32(k1)) [[unlikely]] {
              continue;
            }
            key = pack_fused_2x32(k0, k1);
          }
          if (bulk_raw_count) {
            const BulkCountValue* bulk = nullptr;
            if (use_dense_bulk) {
              const uint64_t idx = static_cast<uint64_t>(key) -
                                   static_cast<uint64_t>(dense_bulk_min);
              if (idx < dense_bulk_counts.size() &&
                  dense_bulk_counts[idx].r_idx != kInvalidRightIdx) {
                bulk = &dense_bulk_counts[idx];
              }
            } else {
              bulk = bulk_counts->Find(key);
            }
            if (bulk == nullptr || !materialize_current_left_row(r)) {
              continue;
            }
            local.comparisons += bulk->pairs;
            const Value& bulk_cell = left_row.values_[int_group_slot];
            size_t* bulk_slot = nullptr;
            bool new_group = false;
            size_t group_index = 0;
            if (!bulk_cell.IsNull() && bulk_cell.type == int_group_type &&
                !bulk_cell.IsUnsigned()) {
              bool inserted = false;
              bulk_slot = local.int_offsets.TryEmplace(
                  bulk_cell.value.int_value, &inserted);
              if (!inserted) {
                group_index = *bulk_slot;
              } else {
                local.key_scratch.assign(1, bulk_cell);
                new_group = true;
              }
            } else {
              local.key_scratch.clear();
              local.key_scratch.push_back(CanonicalDistinctValue(bulk_cell));
              if (auto found = local.offsets.find(local.key_scratch);
                  found != local.offsets.end()) {
                group_index = found->second;
              } else {
                new_group = true;
              }
            }
            if (new_group) {
              group_index = local.groups.size();
              FusedGroup group;
              local.owned_rep_left.push_back(left_row);
              group.rep_left = &local.owned_rep_left.back();
              group.rep_right_idx = bulk->r_idx;
              if (bulk_slot != nullptr) {
                if (!all_direct_group_keys) {
                  group.key = Row(local.key_scratch);
                }
                local.groups.push_back(std::move(group));
                *bulk_slot = group_index;
              } else {
                group.key = Row(local.key_scratch);
                local.groups.push_back(std::move(group));
                local.offsets.emplace(Row(local.key_scratch), group_index);
              }
            }
            local.groups[group_index].raw_count += bulk->count;
            continue;
          }
          if (use_dense_int_rows || use_unique_int_table) {
            uint32_t r_idx = kInvalidRightIdx;
            if (use_dense_int_rows) {
              const uint64_t idx = static_cast<uint64_t>(key) -
                                   static_cast<uint64_t>(dense_int_min);
              if (!ranked_int_bits.empty()) {
                if (idx < ranked_int_span) {
                  const uint64_t word = ranked_int_bits[idx >> 6];
                  const uint64_t bit = uint64_t{1} << (idx & 63);
                  if ((word & bit) != 0) {
                    const uint32_t rank =
                        ranked_int_prefix[idx >> 6] +
                        static_cast<uint32_t>(std::popcount(word & (bit - 1)));
                    r_idx = ranked_int_rows[rank];
                  }
                }
              } else if (idx < dense_int_idxs.size()) {
                r_idx = dense_int_idxs[idx];
              }
            } else {
              size_t slot = hash_unique_int_key(key) & unique_int_mask;
              while (unique_int_table[slot].r_idx != kInvalidRightIdx) {
                if (unique_int_table[slot].key == key) [[likely]] {
                  r_idx = unique_int_table[slot].r_idx;
                  break;
                }
                slot = (slot + 1) & unique_int_mask;
              }
            }
            if (r_idx == kInvalidRightIdx) {
              continue;
            }
            // COUNT(*) and existence-only fusions never read a fact cell.
            // The build row already carries the group, so a matched gid is
            // the whole update.
            if (use_dense_int_rows && count_star_only && use_direct_gid_agg &&
                prepared_pk_lookups.empty()) {
              const uint32_t gid = gid_of(r_idx);
              if (gid != kInvalidGid) {
                DirectGidWorker& dw =
                    direct_gid_workers[use_shared_direct_gid ? 0 : w];
                ++local.comparisons;
                if (use_shared_direct_gid) {
                  __atomic_store_n(&dw.matched[gid], 1, __ATOMIC_RELAXED);
                  __atomic_fetch_add(&dw.counts[gid], 1, __ATOMIC_RELAXED);
                } else {
                  dw.matched[gid] = 1;
                  dw.counts[gid] += 1;
                }
                continue;
              }
            }
            if (use_dense_int_rows && charge_from_cells) {
              const uint32_t gid = gid_of(r_idx);
              if (gid != kInvalidGid) {
                ++local.comparisons;
                DirectGidWorker& dw = direct_gid_workers[w];
                charge_numeric(
                    dw, gid, r_idx,
                    [&](int32_t col, double* out) {
                      const auto c = static_cast<size_t>(col);
                      if (proj_has_null[c] != 0 && proj_cols[c]->IsNull(r)) {
                        return false;
                      }
                      const int64_t raw = proj_int_ptrs[c][r];
                      if (def_base_types[c] == ValueType::kDouble) {
                        *out = std::bit_cast<double>(raw);
                      } else {
                        *out = static_cast<double>(raw);
                      }
                      return true;
                    },
                    [&](int32_t col, int64_t* out) {
                      const auto c = static_cast<size_t>(col);
                      if (proj_has_null[c] != 0 && proj_cols[c]->IsNull(r)) {
                        return false;
                      }
                      *out = proj_int_ptrs[c][r];
                      return true;
                    },
                    [&](int32_t col) {
                      const auto c = static_cast<size_t>(col);
                      return proj_has_null[c] == 0 || !proj_cols[c]->IsNull(r);
                    });
                continue;
              }
            }
            if (fast_dense_split_probe) {
              const uint32_t r_subgid =
                  !split_uses_right_subgid ? 0U : split_right_subgid_ptr[r_idx];
              if (r_subgid == std::numeric_limits<uint32_t>::max())
                  [[unlikely]] {
                continue;
              }
              uint32_t gid = split_uses_right_subgid
                                 ? (r_subgid * dense_right_stride)
                                 : 0U;
              bool pk_ok = true;
              for (size_t gi = 0; gi < dpk_n; ++gi) {
                if (dpk_has_null[gi] != 0 && dpk_col[gi]->IsNull(r))
                    [[unlikely]] {
                  pk_ok = false;
                  break;
                }
                const uint64_t off = static_cast<uint64_t>(dpk_ints[gi][r]) -
                                     static_cast<uint64_t>(dpk_min[gi]);
                if (off >= dpk_span[gi]) [[unlikely]] {
                  pk_ok = false;
                  break;
                }
                const auto& entry = dpk_dense[gi][off];
                if (entry.nb == std::numeric_limits<uint32_t>::max())
                    [[unlikely]] {
                  pk_ok = false;
                  break;
                }
                if (dpk_use_subgid[gi] != 0) {
                  if (entry.subgid == std::numeric_limits<uint32_t>::max())
                      [[unlikely]] {
                    pk_ok = false;
                    break;
                  }
                  gid += entry.subgid * dpk_stride[gi];
                }
              }
              if (!pk_ok) {
                continue;
              }
              ++local.comparisons;
              if (use_packed_double_cells) {
                DirectDoubleCell& cell = dw_packed_cells[gid];
                double dv = 0.0;
                bool is_null = false;
                if (want_da_col) {
                  if (!da_all_nonnull && proj_cols[da_c0]->IsNull(r))
                      [[unlikely]] {
                    is_null = true;
                  } else {
                    const int64_t raw = da_p0[r];
                    dv = da_c0_is_double ? std::bit_cast<double>(raw)
                                         : static_cast<double>(raw);
                  }
                } else {
                  if (!da_all_nonnull) {
                    if ((proj_has_null[da_c0] != 0 &&
                         proj_cols[da_c0]->IsNull(r)) ||
                        (proj_has_null[da_c1] != 0 &&
                         proj_cols[da_c1]->IsNull(r)) ||
                        (da_pattern == CompiledDefDoubleArith::Pattern::
                                           kSubSubAddDivConst &&
                         ((proj_has_null[da_c2] != 0 &&
                           proj_cols[da_c2]->IsNull(r)) ||
                          (proj_has_null[da_c3] != 0 &&
                           proj_cols[da_c3]->IsNull(r))))) {
                      is_null = true;
                    }
                  }
                  if (!is_null) [[likely]] {
                    const auto v0 = std::bit_cast<double>(da_p0[r]);
                    const auto v1 = std::bit_cast<double>(da_p1[r]);
                    switch (da_pattern) {
                      case CompiledDefDoubleArith::Pattern::kSubSubAddDivConst:
                        dv = (((v0 - v1) - std::bit_cast<double>(da_p2[r])) +
                              std::bit_cast<double>(da_p3[r])) /
                             da_div;
                        break;
                      case CompiledDefDoubleArith::Pattern::kSubDivConst:
                        dv = (v0 - v1) / da_div;
                        break;
                      case CompiledDefDoubleArith::Pattern::kSub:
                        dv = v0 - v1;
                        break;
                      case CompiledDefDoubleArith::Pattern::kAdd:
                        dv = v0 + v1;
                        break;
                      default:
                        break;
                    }
                  }
                }
                if (is_null) [[unlikely]] {
                  cell.meta |= 1ULL;
                } else {
                  const double prev = cell.sum;
                  const double next = prev + dv;
                  uint64_t meta = (cell.meta + 4ULL) | 1ULL;
                  if (std::isinf(next) && !std::isinf(prev) && !std::isinf(dv))
                      [[unlikely]] {
                    meta |= 2ULL;
                  }
                  cell.sum = next;
                  cell.meta = meta;
                }
              } else if (left_numeric[0].op == 0) {
                dw_fast_ptr->matched[gid] = 1;
                dw_fast_ptr->counts[gid] += 1;
              } else {
                dw_fast_ptr->matched[gid] = 1;
                if (da_all_nonnull || !proj_cols[da_c0]->IsNull(r)) [[likely]] {
                  const int64_t iv = da_p0[r];
                  int64_t next = 0;
                  if (__builtin_add_overflow(dw_fast_ptr->ints[gid], iv,
                                             &next)) {
                    dw_fast_ptr->inf_flags[gid] = 1;
                  } else {
                    dw_fast_ptr->ints[gid] = next;
                  }
                  dw_fast_ptr->counts[gid] += 1;
                }
              }
              continue;
            }
            if (fast_1pk_dense &&
                (def_compact_split_probe || fast_split_arith_agg ||
                 fast_split_col_agg || fast_split_count_or_distinct)) {
              if (pk0_has_null != 0 && pk0_col->IsNull(r)) [[unlikely]] {
                local.have_cur_pk_subgids = false;
                continue;
              }
              const int64_t pk_k = pk0_ints[r];
              if (pk_k < pk0_min_k || pk_k > pk0_max_k) [[unlikely]] {
                local.have_cur_pk_subgids = false;
                continue;
              }
              if (!cur0_ptr->have_last || pk_k != cur0_ptr->last_key)
                  [[unlikely]] {
                if (!resolve_def_pk_groups(r, false)) [[unlikely]] {
                  have_cached_pk_lval = false;
                  continue;
                }
                if (split_left_from_pk0) {
                  int64_t lv = 0;
                  ValueType ltype = ValueType::kNull;
                  bool luns = false;
                  cached_pk_lval_ok =
                      read_def_left_raw(split_lcol, r, &lv, &ltype, &luns) &&
                      !luns &&
                      (ltype == ValueType::kInt64 ||
                       ltype == ValueType::kDate) &&
                      lv >= static_cast<int64_t>(
                                std::numeric_limits<int32_t>::min()) &&
                      lv <= static_cast<int64_t>(
                                std::numeric_limits<int32_t>::max());
                  cached_pk_lval_bits =
                      cached_pk_lval_ok
                          ? static_cast<uint32_t>(static_cast<int32_t>(lv))
                          : 0U;
                  cached_pk_key = pk_k;
                  have_cached_pk_lval = true;
                }
              } else {
                if (!cur0_ptr->last_matched) [[unlikely]] {
                  local.have_cur_pk_subgids = false;
                  continue;
                }
                if (pk0_ptr->use_subgid) {
                  local.cur_pk_subgids[0] = cur0_ptr->last_subgid;
                }
                local.have_cur_pk_subgids = true;
                if (split_left_from_pk0 &&
                    (!have_cached_pk_lval || cached_pk_key != pk_k))
                    [[unlikely]] {
                  int64_t lv = 0;
                  ValueType ltype = ValueType::kNull;
                  bool luns = false;
                  cached_pk_lval_ok =
                      read_def_left_raw(split_lcol, r, &lv, &ltype, &luns) &&
                      !luns &&
                      (ltype == ValueType::kInt64 ||
                       ltype == ValueType::kDate) &&
                      lv >= static_cast<int64_t>(
                                std::numeric_limits<int32_t>::min()) &&
                      lv <= static_cast<int64_t>(
                                std::numeric_limits<int32_t>::max());
                  cached_pk_lval_bits =
                      cached_pk_lval_ok
                          ? static_cast<uint32_t>(static_cast<int32_t>(lv))
                          : 0U;
                  cached_pk_key = pk_k;
                  have_cached_pk_lval = true;
                }
              }
              const uint32_t r_subgid =
                  !split_uses_right_subgid ? 0U : split_right_subgid_ptr[r_idx];
              if (r_subgid == std::numeric_limits<uint32_t>::max())
                  [[unlikely]] {
                if (def_compact_split_probe) {
                  continue;
                }
                RETURN_IF_FAIL(process_def_match(r, r_idx));
                continue;
              }
              bool lval_ok = !split_has_left_probe || cached_pk_lval_ok;
              uint32_t lval_bits =
                  split_left_from_pk0 ? cached_pk_lval_bits : 0U;
              if (split_left_direct) {
                if (!split_lcol_type_ok ||
                    (split_lcol_has_null != 0 && split_lcol_blk->IsNull(r)))
                    [[unlikely]] {
                  lval_ok = false;
                } else {
                  const int64_t lv = split_lcol_ints[r];
                  lval_ok = lv >= static_cast<int64_t>(
                                      std::numeric_limits<int32_t>::min()) &&
                            lv <= static_cast<int64_t>(
                                      std::numeric_limits<int32_t>::max());
                  lval_bits = static_cast<uint32_t>(static_cast<int32_t>(lv));
                }
              }
              if (!lval_ok) [[unlikely]] {
                RETURN_IF_FAIL(process_def_match(r, r_idx));
                continue;
              }
              uint64_t packed_u64 =
                  split_uses_right_subgid
                      ? (static_cast<uint64_t>(r_subgid) << split_right_shift)
                      : 0ULL;
              if (pk0_use_subgid) {
                const uint32_t psg = local.cur_pk_subgids[0];
                if (psg == std::numeric_limits<uint32_t>::max()) [[unlikely]] {
                  if (def_compact_split_probe) {
                    continue;
                  }
                  RETURN_IF_FAIL(process_def_match(r, r_idx));
                  continue;
                }
                packed_u64 |= static_cast<uint64_t>(psg) << pk0_shift;
              }
              if (split_has_left_probe) {
                packed_u64 |= static_cast<uint64_t>(lval_bits);
              }
              ++local.comparisons;
              if (def_compact_split_probe) {
                if (compact_log_allowed && local.compact_count_inputs == 1024)
                    [[unlikely]] {
                  RETURN_IF_FAIL(record_compact_split(local, packed_u64));
                } else if (local.compact_count_log) {
                  ++local.compact_count_inputs;
                  uint64_t h = packed_u64 + 0x9e3779b97f4a7c15ULL;
                  h = (h ^ (h >> 30U)) * 0xbf58476d1ce4e5b9ULL;
                  h = (h ^ (h >> 27U)) * 0x94d049bb133111ebULL;
                  h ^= h >> 31U;
                  local.compact_split_shards[h & (kCompactSplitShards - 1)]
                      .push_back(packed_u64);
                } else {
                  ++local.compact_count_inputs;
                  bool inserted = false;
                  size_t* slot = local.int_offsets.TryEmplace(
                      std::bit_cast<int64_t>(packed_u64), &inserted);
                  if (!inserted) [[likely]] {
                    ++local.compact_counts[*slot].count;
                  } else {
                    CompactCountGroup probe;
                    probe.hash = packed_u64;
                    probe.count = 1;
                    *slot = local.compact_counts.size();
                    local.compact_counts.push_back(probe);
                  }
                }
                continue;
              }
              bool inserted = false;
              size_t* slot = local.int_offsets.TryEmplace(
                  std::bit_cast<int64_t>(packed_u64), &inserted);
              if (fast_split_count_or_distinct) {
                if (!inserted) [[likely]] {
                  if (count_star_only) {
                    ++local.groups[*slot].raw_count;
                  }
                } else {
                  const size_t group_index = local.groups.size();
                  FusedGroup group;
                  group.key_hash = packed_u64;
                  group.has_key_hash = true;
                  group.rep_right_idx = r_idx;
                  group.raw_count = count_star_only ? 1 : 0;
                  local.groups.push_back(std::move(group));
                  *slot = group_index;
                }
                continue;
              }
              size_t group_index = 0;
              if (!inserted) [[likely]] {
                group_index = *slot;
              } else {
                group_index = local.groups.size();
                FusedGroup group;
                local.raw_arena.Initialize(group, agg_count);
                group.key_hash = packed_u64;
                group.has_key_hash = true;
                group.rep_right_idx = r_idx;
                local.groups.push_back(std::move(group));
                *slot = group_index;
              }
              FusedGroup& group = local.groups[group_index];
              double dv = 0.0;
              bool is_null = false;
              if (fast_split_arith_agg) {
                if (!da_all_nonnull) {
                  if ((proj_has_null[da_c0] != 0 &&
                       proj_cols[da_c0]->IsNull(r)) ||
                      (proj_has_null[da_c1] != 0 &&
                       proj_cols[da_c1]->IsNull(r)) ||
                      (da_pattern == CompiledDefDoubleArith::Pattern::
                                         kSubSubAddDivConst &&
                       ((proj_has_null[da_c2] != 0 &&
                         proj_cols[da_c2]->IsNull(r)) ||
                        (proj_has_null[da_c3] != 0 &&
                         proj_cols[da_c3]->IsNull(r))))) {
                    is_null = true;
                  }
                }
                if (!is_null) [[likely]] {
                  const auto v0 = std::bit_cast<double>(da_p0[r]);
                  const auto v1 = std::bit_cast<double>(da_p1[r]);
                  switch (da_pattern) {
                    case CompiledDefDoubleArith::Pattern::kSubSubAddDivConst:
                      dv = (((v0 - v1) - std::bit_cast<double>(da_p2[r])) +
                            std::bit_cast<double>(da_p3[r])) /
                           da_div;
                      break;
                    case CompiledDefDoubleArith::Pattern::kSubDivConst:
                      dv = (v0 - v1) / da_div;
                      break;
                    case CompiledDefDoubleArith::Pattern::kSub:
                      dv = v0 - v1;
                      break;
                    case CompiledDefDoubleArith::Pattern::kAdd:
                      dv = v0 + v1;
                      break;
                    default:
                      break;
                  }
                }
              } else {
                if (!da_all_nonnull && proj_cols[da_c0]->IsNull(r))
                    [[unlikely]] {
                  is_null = true;
                } else {
                  const int64_t raw = da_p0[r];
                  dv = da_c0_is_double ? std::bit_cast<double>(raw)
                                       : static_cast<double>(raw);
                }
              }
              if (!is_null) [[likely]] {
                const double prev = group.RawDoubles()[0];
                const double next = prev + dv;
                if (std::isinf(next) && !std::isinf(prev) && !std::isinf(dv))
                    [[unlikely]] {
                  group.RawInfFlags()[0] = 1;
                }
                group.RawDoubles()[0] = next;
                group.RawCounts()[0] += 1;
              }
              continue;
            }
            RETURN_IF_FAIL(process_def_match(r, r_idx));
            continue;
          }
          const auto [first, last] = int_buckets->equal_range(key);
          if (first == last) {
            continue;
          }
          if (def_group_cell_probe || def_compact_split_probe) {
            for (auto iter = first; iter != last; ++iter) {
              RETURN_IF_FAIL(process_def_match(r, iter->second));
            }
          } else {
            if (!materialize_current_left_row(r)) {
              continue;
            }
            if (use_preassigned_build_groups) {
              for (auto iter = first; iter != last; ++iter) {
                ++local.comparisons;
                RETURN_IF_FAIL(accumulate_preassigned_pair(w, scratch, left_row,
                                                           iter->second));
              }
            } else {
              size_t cached_group = kNoCachedGroup;
              for (auto iter = first; iter != last; ++iter) {
                ++local.comparisons;
                ASSIGN_OR_RETURN(bool, accumulated,
                                 process_pair(local, left_row, iter->second,
                                              scratch, &cached_group));
                (void)accumulated;
              }
            }
          }
        }
      }
    }
    if (left.HasColumnarOnly()) {
      const auto& chunks = left.columnar->chunks;
      const size_t n_col_chunks = chunks.size();
      Row col_left_row;
      if (singleton_right_relation) {
        while (true) {
          const size_t ci = next_col_chunk.fetch_add(1);
          if (ci >= n_col_chunks) {
            return Status::kSuccess;
          }
          const ColumnarChunk& ch = chunks[ci];
          for (size_t r = 0; r < ch.rows; ++r) {
            ch.MaterializeRowInto(r, &col_left_row);
            ++local.comparisons;
            if (use_preassigned_build_groups) {
              RETURN_IF_FAIL(accumulate_preassigned_pair(
                  w, scratch, col_left_row, uint32_t{0}));
            } else {
              size_t cached_group = kNoCachedGroup;
              ASSIGN_OR_RETURN(bool, accumulated,
                               process_pair(local, col_left_row, uint32_t{0},
                                            scratch, &cached_group));
              (void)accumulated;
            }
          }
        }
      }
      const bool col_raw_fast =
          !use_preassigned_build_groups && !compact_counts &&
          residual_programs.empty() && raw_sum_fusion && !has_count_distinct &&
          all_direct_group_keys && left_cell_agg_ready &&
          !uses_double_arith_cell_op && !right_ptr->HasSpill() &&
          (use_left_packed_key ||
           (use_split_key_gid && !split_uses_pk_subgid &&
            (!split_has_left_probe ||
             key_shapes[split_left_k].direct_column >= 0)) ||
           packed_2x32_group_key || int_group_key);
      const bool col_raw_unmatched_ok =
          col_raw_fast && !left_pack_uses_right && !split_uses_right_subgid;
      const auto col_read_raw_int =
          [&](slot_t slot_col, const ColumnarChunk& ch, size_t r,
              uint32_t r_idx, int64_t* out, ValueType* out_t,
              bool* out_uns) noexcept -> bool {
        if (slot_col < left_width) {
          const ColumnarColumn& cc = ch.columns[slot_col];
          if (cc.IsNull(r)) [[unlikely]] {
            return false;
          }
          if (cc.is_uniform) [[likely]] {
            *out_t = cc.type;
            *out_uns = cc.is_unsigned;
            *out = cc.IntAt(r);
            return true;
          }
          Value tmp;
          cc.WriteValueInto(r, &tmp);
          if (tmp.IsNull()) {
            return false;
          }
          *out_t = tmp.type;
          *out_uns = tmp.IsUnsigned();
          *out = tmp.value.int_value;
          return true;
        }
        if (r_idx == kInvalidRightIdx) {
          return false;
        }
        Value rv_tmp;
        const Value& rv =
            read_right_cell(r_idx, slot_col - left_width, &rv_tmp);
        if (rv.IsNull()) [[unlikely]] {
          return false;
        }
        *out_t = rv.type;
        *out_uns = rv.IsUnsigned();
        *out = rv.value.int_value;
        return true;
      };
      const auto process_col_raw_match = [&](size_t ci, const ColumnarChunk& ch,
                                             size_t r,
                                             uint32_t r_idx) -> Status {
        bool key_ok = false;
        int64_t lookup_key = 0;
        uint64_t computed_key_hash = 0;
        bool has_computed_key_hash = false;
        bool need_rep_left = true;
        if (use_left_packed_key) {
          uint64_t packed_u64 = 0;
          if (left_pack_uses_right) {
            const uint32_t r_subgid = split_right_subgid_ptr[r_idx];
            packed_u64 |= static_cast<uint64_t>(r_subgid) << split_right_shift;
          }
          if (left_pack_uses_dict) {
            const size_t li = left_pack_chunk_offsets[ci] + r;
            packed_u64 |= static_cast<uint64_t>(left_pack_dict_subgid[li])
                          << left_pack_dict_shift;
          }
          for (const LeftPackedIntCol& lpi : left_pack_ints) {
            const ColumnarColumn& cc = ch.columns[lpi.col];
            const uint64_t code =
                cc.IsNull(r) ? lpi.null_code
                             : static_cast<uint64_t>(
                                   (cc.is_uniform ? cc.IntAt(r)
                                                  : ColumnarIntValue(cc, r)) -
                                   lpi.min_val);
            packed_u64 |= code << lpi.shift;
          }
          lookup_key = std::bit_cast<int64_t>(packed_u64);
          computed_key_hash = packed_u64;
          has_computed_key_hash = true;
          need_rep_left = !left_pack_unpack_only;
          key_ok = true;
        } else if (use_split_key_gid) {
          const uint32_t r_subgid =
              !split_uses_right_subgid ? 0U : split_right_subgid_ptr[r_idx];
          int64_t l_val = 0;
          bool l_ok = !split_has_left_probe;
          if (split_has_left_probe) {
            const auto& l_shape = key_shapes[split_left_k];
            ValueType ltype = ValueType::kNull;
            bool luns = false;
            if (l_shape.direct_column >= 0 &&
                col_read_raw_int(static_cast<slot_t>(l_shape.direct_column), ch,
                                 r, r_idx, &l_val, &ltype, &luns) &&
                !luns &&
                (ltype == ValueType::kInt64 || ltype == ValueType::kDate) &&
                std::in_range<int32_t>(l_val)) [[likely]] {
              l_ok = true;
            }
          }
          if (r_subgid != std::numeric_limits<uint32_t>::max() && l_ok)
              [[likely]] {
            uint64_t packed_u64 = 0;
            if (split_uses_right_subgid) {
              packed_u64 |= static_cast<uint64_t>(r_subgid)
                            << split_right_shift;
            }
            if (split_has_left_probe) {
              packed_u64 |= static_cast<uint64_t>(
                  static_cast<uint32_t>(static_cast<int32_t>(l_val)));
            }
            lookup_key = std::bit_cast<int64_t>(packed_u64);
            computed_key_hash = packed_u64;
            has_computed_key_hash = true;
            need_rep_left = !split_key_unpack_only;
            key_ok = true;
          }
        } else if (packed_2x32_group_key) {
          int64_t v0 = 0;
          int64_t v1 = 0;
          ValueType t0 = ValueType::kNull;
          ValueType t1 = ValueType::kNull;
          bool u0 = false;
          bool u1 = false;
          if (col_read_raw_int(packed_2x32_slot0, ch, r, r_idx, &v0, &t0,
                               &u0) &&
              col_read_raw_int(packed_2x32_slot1, ch, r, r_idx, &v1, &t1,
                               &u1) &&
              t0 == packed_2x32_type0 && !u0 && t1 == packed_2x32_type1 &&
              !u1 && std::in_range<int32_t>(v0) && std::in_range<int32_t>(v1))
              [[likely]] {
            const uint64_t packed_u64 =
                (static_cast<uint64_t>(
                     static_cast<uint32_t>(static_cast<int32_t>(v0)))
                 << 32) |
                static_cast<uint32_t>(static_cast<int32_t>(v1));
            lookup_key = std::bit_cast<int64_t>(packed_u64);
            computed_key_hash = packed_u64;
            has_computed_key_hash = true;
            need_rep_left = !packed_2x32_unpack_only;
            key_ok = true;
          }
        } else {
          ValueType kt = ValueType::kNull;
          bool kuns = false;
          if (col_read_raw_int(int_group_slot, ch, r, r_idx, &lookup_key, &kt,
                               &kuns) &&
              kt == int_group_type && !kuns) [[likely]] {
            key_ok = true;
          }
        }
        if (!key_ok) [[unlikely]] {
          ch.MaterializeRowInto(r, &col_left_row);
          if (r_idx == kInvalidRightIdx) {
            return unmatched_left(local, scratch, col_left_row);
          }
          size_t cached_group = kNoCachedGroup;
          ASSIGN_OR_RETURN(
              bool, accumulated,
              process_pair(local, col_left_row, r_idx, scratch, &cached_group));
          (void)accumulated;
          return Status::kSuccess;
        }
        const auto col_agg_double = [&](int32_t col, double* out) {
          const ColumnarColumn& cc = ch.columns[static_cast<size_t>(col)];
          if (cc.IsNull(r)) [[unlikely]] {
            return false;
          }
          if (cc.is_uniform) [[likely]] {
            if (cc.type == ValueType::kDouble) {
              *out = cc.DoubleAt(r);
              return true;
            }
            if (cc.type == ValueType::kInt64 && !cc.is_unsigned) {
              *out = static_cast<double>(cc.IntAt(r));
              return true;
            }
            return false;
          }
          Value tmp;
          cc.WriteValueInto(r, &tmp);
          return !tmp.IsNull() && cell_as_double(tmp, out);
        };
        const auto col_agg_int = [&](int32_t col, int64_t* out) {
          const ColumnarColumn& cc = ch.columns[static_cast<size_t>(col)];
          if (cc.IsNull(r)) [[unlikely]] {
            return false;
          }
          if (cc.is_uniform) [[likely]] {
            if (cc.type != ValueType::kInt64 || cc.is_unsigned) {
              return false;
            }
            *out = cc.IntAt(r);
            return true;
          }
          Value tmp;
          cc.WriteValueInto(r, &tmp);
          if (tmp.IsNull() || tmp.type != ValueType::kInt64 ||
              tmp.IsUnsigned()) {
            return false;
          }
          *out = tmp.value.int_value;
          return true;
        };
        const auto col_agg_present = [&](int32_t col) {
          return !ch.columns[static_cast<size_t>(col)].IsNull(r);
        };
        if (use_radix_agg) {
          return scatter_radix_tuple(
              w, std::bit_cast<uint64_t>(lookup_key), col_agg_double,
              col_agg_int, col_agg_present,
              [](int32_t, double*, bool*) { return Status::kSuccess; });
        }
        bool inserted = false;
        size_t* slot = local.int_offsets.TryEmplace(lookup_key, &inserted);
        size_t group_index = 0;
        if (!inserted) [[likely]] {
          group_index = *slot;
        } else {
          group_index = local.groups.size();
          FusedGroup group;
          local.raw_arena.Initialize(group, agg_count);
          group.key_hash = computed_key_hash;
          group.has_key_hash = has_computed_key_hash;
          if (need_rep_left) {
            group.rep_col_chunk = static_cast<uint32_t>(ci);
            group.rep_col_row = static_cast<uint32_t>(r);
          }
          group.rep_right_idx = r_idx;
          local.groups.push_back(std::move(group));
          *slot = group_index;
        }
        charge_group_numeric(
            local.groups[group_index],
            r_idx != kInvalidRightIdx ? static_cast<size_t>(r_idx) : size_t{0},
            r_idx != kInvalidRightIdx, col_agg_double, col_agg_int,
            col_agg_present);
        return Status::kSuccess;
      };
      if (integer_key || packed_2x32_int_key) {
        if (integer_key && count_star_only && use_dense_int_rows &&
            use_preassigned_build_groups && join_type == JoinType::kInner &&
            !direct_gid_workers.empty()) {
          const slot_t key_col = left_columns[0];
          const int64_t bias = left_key_offsets[0];
          DirectGidWorker& dw =
              direct_gid_workers[use_shared_direct_gid ? 0 : w];
          const uint32_t* dense = dense_int_idxs.data();
          const size_t dense_n = dense_int_idxs.size();
          const int64_t dense_base = dense_int_min;
          while (true) {
            const size_t ci = next_col_chunk.fetch_add(1);
            if (ci >= n_col_chunks) {
              return Status::kSuccess;
            }
            const ColumnarChunk& ch = chunks[ci];
            const ColumnarColumn& kc = ch.columns[key_col];
            if (kc.encoding == ColumnarColumn::Encoding::kConstant &&
                ch.rows > 0 && kc.IsNull(0)) {
              continue;
            }
            const bool kc_fast =
                kc.is_uniform && !kc.is_unsigned &&
                (kc.type == ValueType::kInt64 || kc.type == ValueType::kDate);
            for (size_t r = 0; r < ch.rows; ++r) {
              if (kc_fast ? kc.IsNull(r) : ColumnarCellIsNullOrNaN(kc, r)) {
                continue;
              }
              const int64_t raw_k =
                  kc_fast ? kc.IntAt(r) : ColumnarIntValue(kc, r);
              const int64_t key = AddJoinKeyBias(raw_k, bias);
              const uint64_t idx = static_cast<uint64_t>(key) -
                                   static_cast<uint64_t>(dense_base);
              if (idx >= dense_n) {
                continue;
              }
              const uint32_t r_idx = dense[idx];
              if (r_idx == kInvalidRightIdx) {
                continue;
              }
              const uint32_t gid = gid_of(r_idx);
              if (gid == kInvalidGid) {
                continue;
              }
              ++local.comparisons;
              if (use_shared_direct_gid) {
                __atomic_store_n(&dw.matched[gid], 1, __ATOMIC_RELAXED);
                __atomic_fetch_add(&dw.counts[gid], 1, __ATOMIC_RELAXED);
              } else {
                dw.matched[gid] = 1;
                dw.counts[gid] += 1;
              }
            }
          }
        }
        if (integer_key && left_numeric_probe && use_dense_int_rows) {
          const slot_t key_col = left_columns[0];
          const int64_t bias = left_key_offsets[0];
          DirectGidWorker& dw = direct_gid_workers[w];
          const uint32_t* dense = dense_int_idxs.data();
          const size_t dense_n = dense_int_idxs.size();
          const int64_t dense_base = dense_int_min;
          while (true) {
            const size_t ci = next_col_chunk.fetch_add(1);
            if (ci >= n_col_chunks) {
              return Status::kSuccess;
            }
            const ColumnarChunk& ch = chunks[ci];
            const ColumnarColumn& kc = ch.columns[key_col];
            if (kc.encoding == ColumnarColumn::Encoding::kConstant &&
                ch.rows > 0 && kc.IsNull(0)) {
              continue;
            }
            const bool kc_fast =
                kc.is_uniform && !kc.is_unsigned &&
                (kc.type == ValueType::kInt64 || kc.type == ValueType::kDate);
            for (size_t r = 0; r < ch.rows; ++r) {
              if (kc_fast ? kc.IsNull(r) : ColumnarCellIsNullOrNaN(kc, r)) {
                continue;
              }
              const int64_t raw_k =
                  kc_fast ? kc.IntAt(r) : ColumnarIntValue(kc, r);
              const int64_t key = AddJoinKeyBias(raw_k, bias);
              const uint64_t idx = static_cast<uint64_t>(key) -
                                   static_cast<uint64_t>(dense_base);
              if (idx >= dense_n) {
                continue;
              }
              const uint32_t r_idx = dense[idx];
              if (r_idx == kInvalidRightIdx) {
                continue;
              }
              const uint32_t gid = gid_of(r_idx);
              if (gid == kInvalidGid) {
                continue;
              }
              ++local.comparisons;
              charge_numeric(
                  dw, gid, r_idx,
                  [&](int32_t col, double* out) {
                    const ColumnarColumn& cc =
                        ch.columns[static_cast<size_t>(col)];
                    if (cc.IsNull(r)) {
                      return false;
                    }
                    if (cc.is_uniform) {
                      if (cc.type == ValueType::kDouble) {
                        *out = cc.DoubleAt(r);
                        return true;
                      }
                      if (cc.type == ValueType::kInt64 && !cc.is_unsigned) {
                        *out = static_cast<double>(cc.IntAt(r));
                        return true;
                      }
                      return false;
                    }
                    Value tmp;
                    cc.WriteValueInto(r, &tmp);
                    return !tmp.IsNull() && cell_as_double(tmp, out);
                  },
                  [&](int32_t col, int64_t* out) {
                    const ColumnarColumn& cc =
                        ch.columns[static_cast<size_t>(col)];
                    if (cc.IsNull(r)) {
                      return false;
                    }
                    if (cc.is_uniform) {
                      if (cc.type != ValueType::kInt64 || cc.is_unsigned) {
                        return false;
                      }
                      *out = cc.IntAt(r);
                      return true;
                    }
                    Value tmp;
                    cc.WriteValueInto(r, &tmp);
                    if (tmp.IsNull() || tmp.type != ValueType::kInt64 ||
                        tmp.IsUnsigned()) {
                      return false;
                    }
                    *out = tmp.value.int_value;
                    return true;
                  },
                  [&](int32_t col) {
                    return !ch.columns[static_cast<size_t>(col)].IsNull(r);
                  });
            }
          }
        }
        while (true) {
          const size_t ci = next_col_chunk.fetch_add(1);
          if (ci >= n_col_chunks) {
            return Status::kSuccess;
          }
          const ColumnarChunk& ch = chunks[ci];
          const ColumnarColumn& kc0 = ch.columns[left_columns[0]];
          const ColumnarColumn* kc1 =
              packed_2x32_int_key ? &ch.columns[left_columns[1]] : nullptr;
          const bool kc0_fast =
              kc0.is_uniform && !kc0.is_unsigned &&
              (kc0.type == ValueType::kInt64 || kc0.type == ValueType::kDate);
          const bool kc1_fast =
              kc1 != nullptr && kc1->is_uniform && !kc1->is_unsigned &&
              (kc1->type == ValueType::kInt64 || kc1->type == ValueType::kDate);
          for (size_t r = 0; r < ch.rows; ++r) {
            int64_t raw0 = 0, raw1 = 0;
            if (!read_col_int_cell(kc0, r, kc0_fast, &raw0) ||
                (kc1 != nullptr &&
                 !read_col_int_cell(*kc1, r, kc1_fast, &raw1))) {
              if (join_type == JoinType::kLeft) {
                if (col_raw_unmatched_ok) {
                  RETURN_IF_FAIL(
                      process_col_raw_match(ci, ch, r, kInvalidRightIdx));
                } else {
                  ch.MaterializeRowInto(r, &col_left_row);
                  RETURN_IF_FAIL(unmatched_left(local, scratch, col_left_row));
                }
              }
              continue;
            }
            int64_t key = 0;
            if (integer_key) {
              key = AddJoinKeyBias(raw0, left_key_offsets[0]);
            } else {
              const int64_t k0 = AddJoinKeyBias(raw0, left_key_offsets[0]);
              const int64_t k1 = AddJoinKeyBias(raw1, left_key_offsets[1]);
              if (!fits_fused_i32(k0) || !fits_fused_i32(k1)) {
                if (join_type == JoinType::kLeft) {
                  if (col_raw_unmatched_ok) {
                    RETURN_IF_FAIL(
                        process_col_raw_match(ci, ch, r, kInvalidRightIdx));
                  } else {
                    ch.MaterializeRowInto(r, &col_left_row);
                    RETURN_IF_FAIL(
                        unmatched_left(local, scratch, col_left_row));
                  }
                }
                continue;
              }
              key = pack_fused_2x32(k0, k1);
            }
            if (bulk_raw_count) {
              const BulkCountValue* bulk = nullptr;
              if (use_dense_bulk) {
                const uint64_t idx = static_cast<uint64_t>(key) -
                                     static_cast<uint64_t>(dense_bulk_min);
                if (idx < dense_bulk_counts.size() &&
                    dense_bulk_counts[idx].r_idx != kInvalidRightIdx) {
                  bulk = &dense_bulk_counts[idx];
                }
              } else {
                bulk = bulk_counts->Find(key);
              }
              if (bulk == nullptr) {
                if (join_type == JoinType::kLeft) {
                  ch.MaterializeRowInto(r, &col_left_row);
                  RETURN_IF_FAIL(unmatched_left(local, scratch, col_left_row));
                }
                continue;
              }
              local.comparisons += bulk->pairs;
              const ColumnarColumn& gcc = ch.columns[int_group_slot];
              if (gcc.is_uniform && !gcc.is_unsigned &&
                  gcc.type == int_group_type && !gcc.IsNull(r)) {
                bool inserted = false;
                size_t* bulk_slot =
                    local.int_offsets.TryEmplace(gcc.IntAt(r), &inserted);
                size_t group_index = 0;
                if (!inserted) {
                  group_index = *bulk_slot;
                } else {
                  group_index = local.groups.size();
                  FusedGroup group;
                  group.rep_col_chunk = static_cast<uint32_t>(ci);
                  group.rep_col_row = static_cast<uint32_t>(r);
                  group.rep_right_idx = bulk->r_idx;
                  if (!all_direct_group_keys) {
                    Value bulk_k;
                    gcc.WriteValueInto(r, &bulk_k);
                    group.key = Row(std::vector<Value>{std::move(bulk_k)});
                  }
                  local.groups.push_back(std::move(group));
                  *bulk_slot = group_index;
                }
                local.groups[group_index].raw_count += bulk->count;
                continue;
              }
              ch.MaterializeRowInto(r, &col_left_row);
              const Value& bulk_cell = col_left_row.values_[int_group_slot];
              size_t* bulk_slot = nullptr;
              bool new_group = false;
              size_t group_index = 0;
              if (!bulk_cell.IsNull() && bulk_cell.type == int_group_type &&
                  !bulk_cell.IsUnsigned()) {
                bool inserted = false;
                bulk_slot = local.int_offsets.TryEmplace(
                    bulk_cell.value.int_value, &inserted);
                if (!inserted) {
                  group_index = *bulk_slot;
                } else {
                  local.key_scratch.assign(1, bulk_cell);
                  new_group = true;
                }
              } else {
                local.key_scratch.clear();
                local.key_scratch.push_back(CanonicalDistinctValue(bulk_cell));
                if (auto found = local.offsets.find(local.key_scratch);
                    found != local.offsets.end()) {
                  group_index = found->second;
                } else {
                  new_group = true;
                }
              }
              if (new_group) {
                group_index = local.groups.size();
                local.owned_rep_left.push_back(col_left_row);
                FusedGroup group;
                group.rep_left = &local.owned_rep_left.back();
                group.rep_right_idx = bulk->r_idx;
                if (bulk_slot != nullptr) {
                  if (!all_direct_group_keys) {
                    group.key = Row(local.key_scratch);
                  }
                  local.groups.push_back(std::move(group));
                  *bulk_slot = group_index;
                } else {
                  group.key = Row(local.key_scratch);
                  local.groups.push_back(std::move(group));
                  local.offsets.emplace(Row(local.key_scratch), group_index);
                }
              }
              local.groups[group_index].raw_count += bulk->count;
              continue;
            }
            if (use_dense_int_rows) {
              const uint64_t idx = static_cast<uint64_t>(key) -
                                   static_cast<uint64_t>(dense_int_min);
              const uint32_t r_idx = idx < dense_int_idxs.size()
                                         ? dense_int_idxs[idx]
                                         : kInvalidRightIdx;
              bool matched = false;
              if (r_idx != kInvalidRightIdx) {
                ++local.comparisons;
                if (anti_unmatched_only && residual_programs.empty()) {
                  matched = true;
                } else if (col_raw_fast) {
                  RETURN_IF_FAIL(process_col_raw_match(ci, ch, r, r_idx));
                  matched = true;
                } else {
                  ch.MaterializeRowInto(r, &col_left_row);
                  if (use_preassigned_build_groups) {
                    RETURN_IF_FAIL(accumulate_preassigned_pair(
                        w, scratch, col_left_row, r_idx));
                    matched = true;
                  } else {
                    size_t cached_group = kNoCachedGroup;
                    ASSIGN_OR_RETURN(bool, accumulated,
                                     process_pair(local, col_left_row, r_idx,
                                                  scratch, &cached_group));
                    matched = accumulated;
                  }
                }
              }
              if (!matched && join_type == JoinType::kLeft) {
                if (col_raw_unmatched_ok) {
                  RETURN_IF_FAIL(
                      process_col_raw_match(ci, ch, r, kInvalidRightIdx));
                } else {
                  if (r_idx == kInvalidRightIdx) {
                    ch.MaterializeRowInto(r, &col_left_row);
                  }
                  RETURN_IF_FAIL(unmatched_left(local, scratch, col_left_row));
                }
              }
              continue;
            }
            if (use_unique_int_table) {
              size_t slot = hash_unique_int_key(key) & unique_int_mask;
              uint32_t r_idx = kInvalidRightIdx;
              while (true) {
                const UniqueIntSlot& entry = unique_int_table[slot];
                if (entry.r_idx == kInvalidRightIdx) {
                  break;
                }
                if (entry.key == key) [[likely]] {
                  r_idx = entry.r_idx;
                  break;
                }
                slot = (slot + 1) & unique_int_mask;
              }
              bool matched = false;
              if (r_idx != kInvalidRightIdx) [[likely]] {
                ++local.comparisons;
                if (anti_unmatched_only && residual_programs.empty()) {
                  matched = true;
                } else if (col_raw_fast) {
                  RETURN_IF_FAIL(process_col_raw_match(ci, ch, r, r_idx));
                  matched = true;
                } else {
                  ch.MaterializeRowInto(r, &col_left_row);
                  if (use_preassigned_build_groups) {
                    RETURN_IF_FAIL(accumulate_preassigned_pair(
                        w, scratch, col_left_row, r_idx));
                    matched = true;
                  } else {
                    size_t cached_group = kNoCachedGroup;
                    ASSIGN_OR_RETURN(bool, accumulated,
                                     process_pair(local, col_left_row, r_idx,
                                                  scratch, &cached_group));
                    matched = accumulated;
                  }
                }
              }
              if (!matched && join_type == JoinType::kLeft) {
                if (col_raw_unmatched_ok) {
                  RETURN_IF_FAIL(
                      process_col_raw_match(ci, ch, r, kInvalidRightIdx));
                } else {
                  if (r_idx == kInvalidRightIdx) {
                    ch.MaterializeRowInto(r, &col_left_row);
                  }
                  RETURN_IF_FAIL(unmatched_left(local, scratch, col_left_row));
                }
              }
              continue;
            }
            bool matched = false;
            const auto [first, last] = int_buckets->equal_range(key);
            if (first != last) {
              if (anti_unmatched_only && residual_programs.empty()) {
                ++local.comparisons;
                matched = true;
              } else if (col_raw_fast) {
                for (auto iter = first; iter != last; ++iter) {
                  ++local.comparisons;
                  RETURN_IF_FAIL(
                      process_col_raw_match(ci, ch, r, iter->second));
                }
                matched = true;
              } else {
                ch.MaterializeRowInto(r, &col_left_row);
                size_t cached_group = kNoCachedGroup;
                for (auto iter = first; iter != last; ++iter) {
                  ++local.comparisons;
                  if (use_preassigned_build_groups) {
                    RETURN_IF_FAIL(accumulate_preassigned_pair(
                        w, scratch, col_left_row, iter->second));
                    matched = true;
                  } else {
                    ASSIGN_OR_RETURN(
                        bool, accumulated,
                        process_pair(local, col_left_row, iter->second, scratch,
                                     &cached_group));
                    matched = matched || accumulated;
                    if (matched && anti_unmatched_only) {
                      break;
                    }
                  }
                }
              }
            }
            if (!matched && join_type == JoinType::kLeft) {
              if (col_raw_unmatched_ok) {
                RETURN_IF_FAIL(
                    process_col_raw_match(ci, ch, r, kInvalidRightIdx));
              } else {
                if (first == last) {
                  ch.MaterializeRowInto(r, &col_left_row);
                }
                RETURN_IF_FAIL(unmatched_left(local, scratch, col_left_row));
              }
            }
          }
        }
      }
      if (use_dense_composite) {
        while (true) {
          const size_t ci = next_col_chunk.fetch_add(1);
          if (ci >= n_col_chunks) { return Status::kSuccess;
}
          const auto& chunk = chunks[ci];
          for (size_t r = 0; r < chunk.rows; ++r) {
            CompositeIntKey key;
            uint32_t right_row = kInvalidRightIdx;
            if (read_col_composite_key(chunk, left_columns, left_key_offsets, r,
                                       &key)) {
              const uint64_t offset =
                  static_cast<uint64_t>(key.parts[0]) -
                  static_cast<uint64_t>(dense_composite_min);
              if (offset < dense_composite_rows.size()) {
                const uint32_t candidate = dense_composite_rows[offset];
                if (candidate != kInvalidRightIdx &&
                    dense_composite_keys[candidate] == key) {
                  right_row = candidate;
}
              }
            }
            if (right_row == kInvalidRightIdx && join_type == JoinType::kInner) {
              continue;
}
            chunk.MaterializeRowInto(r, &col_left_row);
            bool matched = false;
            if (right_row != kInvalidRightIdx) {
              ++local.comparisons;
              if (use_preassigned_build_groups) {
                RETURN_IF_FAIL(accumulate_preassigned_pair(
                    w, scratch, col_left_row, right_row));
                matched = true;
              } else {
                ASSIGN_OR_RETURN(
                    bool, accumulated,
                    process_pair(local, col_left_row, right_row, scratch));
                matched = accumulated;
              }
            }
            if (!matched && join_type == JoinType::kLeft)
              RETURN_IF_FAIL(unmatched_left(local, scratch, col_left_row));
          }
        }
      }
      if (composite_int_key) {
        const FlatJoinIdxBuckets<CompositeIntKey, CompositeIntKeyHash>&
            buckets = *composite_buckets;
        while (true) {
          const size_t ci = next_col_chunk.fetch_add(1);
          if (ci >= n_col_chunks) {
            return Status::kSuccess;
          }
          const ColumnarChunk& ch = chunks[ci];
          for (size_t r = 0; r < ch.rows; ++r) {
            CompositeIntKey key;
            if (!read_col_composite_key(ch, left_columns, left_key_offsets, r,
                                        &key)) {
              if (join_type == JoinType::kLeft) {
                ch.MaterializeRowInto(r, &col_left_row);
                RETURN_IF_FAIL(unmatched_left(local, scratch, col_left_row));
              }
              continue;
            }
            bool matched = false;
            size_t cached_group = kNoCachedGroup;
            const auto [first, last] = buckets.equal_range(key);
            if (first == last && join_type == JoinType::kInner) { continue;
}
            ch.MaterializeRowInto(r, &col_left_row);
            for (auto iter = first; iter != last; ++iter) {
              ++local.comparisons;
              if (use_preassigned_build_groups) {
                RETURN_IF_FAIL(accumulate_preassigned_pair(
                    w, scratch, col_left_row, iter->second));
                matched = true;
              } else {
                ASSIGN_OR_RETURN(bool, accumulated,
                                 process_pair(local, col_left_row, iter->second,
                                              scratch, &cached_group));
                matched = matched || accumulated;
              }
            }
            if (!matched && join_type == JoinType::kLeft) {
              RETURN_IF_FAIL(unmatched_left(local, scratch, col_left_row));
            }
          }
        }
      }
      const FlatJoinIdxBuckets<std::string>& buckets = *key_buckets;
      while (true) {
        const size_t ci = next_col_chunk.fetch_add(1);
        if (ci >= n_col_chunks) {
          return Status::kSuccess;
        }
        const ColumnarChunk& ch = chunks[ci];
        for (size_t r = 0; r < ch.rows; ++r) {
          ch.MaterializeRowInto(r, &col_left_row);
          if (HasNullKey(col_left_row, left_columns)) {
            if (join_type == JoinType::kLeft) {
              RETURN_IF_FAIL(unmatched_left(local, scratch, col_left_row));
            }
            continue;
          }
          const std::string key =
              EncodeJoinKey(col_left_row, left_columns, &left_key_offsets);
          bool matched = false;
          size_t cached_group = kNoCachedGroup;
          const auto [first, last] = buckets.equal_range(key);
          for (auto iter = first; iter != last; ++iter) {
            ++local.comparisons;
            if (use_preassigned_build_groups) {
              RETURN_IF_FAIL(accumulate_preassigned_pair(
                  w, scratch, col_left_row, iter->second));
              matched = true;
            } else {
              ASSIGN_OR_RETURN(bool, accumulated,
                               process_pair(local, col_left_row, iter->second,
                                            scratch, &cached_group));
              matched = matched || accumulated;
            }
          }
          if (!matched && join_type == JoinType::kLeft) {
            RETURN_IF_FAIL(unmatched_left(local, scratch, col_left_row));
          }
        }
      }
    }
    if (left.HasSpill()) {
      // Spill readers own one row at a time. Feed it directly into groups;
      // neither the probe nor its potentially huge join output is copied.
      const auto probe_row = [&](const Row& left_row) -> Status {
        bool matched = false;
        const auto visit = [&](const auto& buckets, const auto& key) -> Status {
          const auto [first, last] = buckets.equal_range(key);
          for (auto iter = first; iter != last; ++iter) {
            ++local.comparisons;
            ASSIGN_OR_RETURN(
                bool, accumulated,
                process_pair(local, left_row, iter->second, scratch));
            matched = matched || accumulated;
          }
          return Status::kSuccess;
        };
        if (!HasNullKey(left_row, left_columns)) {
          if (integer_key) {
            RETURN_IF_FAIL(
                visit(*int_buckets,
                      AddJoinKeyBias(IntegerJoinKey(left_row, left_columns[0]),
                                     left_key_offsets[0])));
          } else if (packed_2x32_int_key) {
            const int64_t k0 = AddJoinKeyBias(
                IntegerJoinKey(left_row, left_columns[0]), left_key_offsets[0]);
            const int64_t k1 = AddJoinKeyBias(
                IntegerJoinKey(left_row, left_columns[1]), left_key_offsets[1]);
            if (fits_fused_i32(k0) && fits_fused_i32(k1)) {
              RETURN_IF_FAIL(visit(*int_buckets, pack_fused_2x32(k0, k1)));
            }
          } else if (composite_int_key) {
            RETURN_IF_FAIL(visit(*composite_buckets,
                                 EncodeCompositeIntKey(left_row, left_columns,
                                                       &left_key_offsets)));
          } else {
            RETURN_IF_FAIL(visit(
                *key_buckets,
                EncodeJoinKey(left_row, left_columns, &left_key_offsets)));
          }
        }
        if (!matched && join_type == JoinType::kLeft) {
          RETURN_IF_FAIL(unmatched_left(local, scratch, left_row));
        }
        return Status::kSuccess;
      };
      Status evaluation = Status::kSuccess;
      RETURN_IF_FAIL(left.ForEachRow([&](const Row& row) {
        if (evaluation.ok()) {
          evaluation = probe_row(row);
        }
      }));
      return evaluation;
    }
    if (singleton_right_relation) {
      while (true) {
        const size_t chunk = next_chunk.fetch_add(1);
        if (chunk >= chunk_count) {
          return Status::kSuccess;
        }
        const size_t begin = chunk * kProbeChunkRows;
        const size_t end = std::min(total_left, begin + kProbeChunkRows);
        for (size_t ri = begin; ri < end; ++ri) {
          const Row& left_row = left.rows[ri];
          ++local.comparisons;
          if (use_preassigned_build_groups) {
            RETURN_IF_FAIL(
                accumulate_preassigned_pair(w, scratch, left_row, uint32_t{0}));
          } else {
            size_t cached_group = kNoCachedGroup;
            ASSIGN_OR_RETURN(bool, accumulated,
                             process_pair(local, left_row, uint32_t{0}, scratch,
                                          &cached_group));
            (void)accumulated;
          }
        }
      }
    }
    if (integer_key || packed_2x32_int_key) {
      // COUNT(*) with a dense unique build key only needs the foreign key.
      // The group id was assigned on the build row, so the fact row's other
      // cells and the scratch copy are not read.
      if (integer_key && count_star_only && use_dense_int_rows &&
          use_preassigned_build_groups && join_type == JoinType::kInner &&
          !left.HasSpill() && !direct_gid_workers.empty()) {
        const slot_t key_col = left_columns[0];
        const int64_t bias = left_key_offsets[0];
        DirectGidWorker& dw = direct_gid_workers[use_shared_direct_gid ? 0 : w];
        const uint32_t* dense = dense_int_idxs.data();
        const size_t dense_n = dense_int_idxs.size();
        const int64_t dense_base = dense_int_min;
        while (true) {
          const size_t chunk = next_chunk.fetch_add(1);
          if (chunk >= chunk_count) {
            return Status::kSuccess;
          }
          const size_t begin = chunk * kProbeChunkRows;
          const size_t end = std::min(total_left, begin + kProbeChunkRows);
          for (size_t ri = begin; ri < end; ++ri) {
            const Value& cell = left.rows[ri].values_[key_col];
            if (cell.IsNull()) {
              continue;
            }
            const int64_t key = AddJoinKeyBias(cell.value.int_value, bias);
            const uint64_t idx =
                static_cast<uint64_t>(key) - static_cast<uint64_t>(dense_base);
            if (idx >= dense_n) {
              continue;
            }
            const uint32_t r_idx = dense[idx];
            if (r_idx == kInvalidRightIdx) {
              continue;
            }
            const uint32_t gid = gid_of(r_idx);
            if (gid == kInvalidGid) {
              continue;
            }
            ++local.comparisons;
            if (use_shared_direct_gid) {
              __atomic_store_n(&dw.matched[gid], 1, __ATOMIC_RELAXED);
              __atomic_fetch_add(&dw.counts[gid], 1, __ATOMIC_RELAXED);
            } else {
              dw.matched[gid] = 1;
              dw.counts[gid] += 1;
            }
          }
        }
      }
      if (integer_key && left_numeric_probe && use_dense_int_rows &&
          !left.HasSpill()) {
        const slot_t key_col = left_columns[0];
        const int64_t bias = left_key_offsets[0];
        DirectGidWorker& dw = direct_gid_workers[w];
        const uint32_t* dense = dense_int_idxs.data();
        const size_t dense_n = dense_int_idxs.size();
        const int64_t dense_base = dense_int_min;
        while (true) {
          const size_t chunk = next_chunk.fetch_add(1);
          if (chunk >= chunk_count) {
            return Status::kSuccess;
          }
          const size_t begin = chunk * kProbeChunkRows;
          const size_t end = std::min(total_left, begin + kProbeChunkRows);
          for (size_t ri = begin; ri < end; ++ri) {
            const Row& left_row = left.rows[ri];
            const Value& cell = left_row.values_[key_col];
            if (cell.IsNull()) {
              continue;
            }
            const int64_t key = AddJoinKeyBias(cell.value.int_value, bias);
            const uint64_t idx =
                static_cast<uint64_t>(key) - static_cast<uint64_t>(dense_base);
            if (idx >= dense_n) {
              continue;
            }
            const uint32_t r_idx = dense[idx];
            if (r_idx == kInvalidRightIdx) {
              continue;
            }
            const uint32_t gid = gid_of(r_idx);
            if (gid == kInvalidGid) {
              continue;
            }
            ++local.comparisons;
            charge_numeric(
                dw, gid, r_idx,
                [&](int32_t col, double* out) {
                  const Value& value =
                      left_row.values_[static_cast<size_t>(col)];
                  return !value.IsNull() && cell_as_double(value, out);
                },
                [&](int32_t col, int64_t* out) {
                  const Value& value =
                      left_row.values_[static_cast<size_t>(col)];
                  if (value.IsNull() || value.type != ValueType::kInt64 ||
                      value.IsUnsigned()) {
                    return false;
                  }
                  *out = value.value.int_value;
                  return true;
                },
                [&](int32_t col) {
                  return !left_row.values_[static_cast<size_t>(col)].IsNull();
                });
          }
        }
      }
      while (true) {
        const size_t chunk = next_chunk.fetch_add(1);
        if (chunk >= chunk_count) {
          return Status::kSuccess;
        }
        const size_t begin = chunk * kProbeChunkRows;
        const size_t end = std::min(total_left, begin + kProbeChunkRows);
        for (size_t ri = begin; ri < end; ++ri) {
          const Row& left_row = left.rows[ri];
          if (HasNullKey(left_row, left_columns)) {
            if (join_type == JoinType::kLeft) {
              RETURN_IF_FAIL(unmatched_left(local, scratch, left_row));
            }
            continue;
          }
          int64_t key = 0;
          if (integer_key) {
            key = AddJoinKeyBias(IntegerJoinKey(left_row, left_columns[0]),
                                 left_key_offsets[0]);
          } else {
            const int64_t k0 = AddJoinKeyBias(
                IntegerJoinKey(left_row, left_columns[0]), left_key_offsets[0]);
            const int64_t k1 = AddJoinKeyBias(
                IntegerJoinKey(left_row, left_columns[1]), left_key_offsets[1]);
            if (!fits_fused_i32(k0) || !fits_fused_i32(k1)) {
              if (join_type == JoinType::kLeft) {
                RETURN_IF_FAIL(unmatched_left(local, scratch, left_row));
              }
              continue;
            }
            key = pack_fused_2x32(k0, k1);
          }
          if (bulk_raw_count) {
            // One open-addressing probe answers the whole join step: the
            // duplicate count stands in for the bucket walk (whose Entry
            // chain and per-pair build-row dereferences dominated this
            // loop), and the group key is read straight off the outer row.
            const BulkCountValue* bulk = nullptr;
            if (use_dense_bulk) {
              const uint64_t idx = static_cast<uint64_t>(key) -
                                   static_cast<uint64_t>(dense_bulk_min);
              if (idx < dense_bulk_counts.size() &&
                  dense_bulk_counts[idx].r_idx != kInvalidRightIdx) {
                bulk = &dense_bulk_counts[idx];
              }
            } else {
              bulk = bulk_counts->Find(key);
            }
            if (bulk == nullptr) {
              if (join_type == JoinType::kLeft) {
                RETURN_IF_FAIL(unmatched_left(local, scratch, left_row));
              }
              continue;
            }
            local.comparisons += bulk->pairs;
            const Value& bulk_cell = left_row.values_[int_group_slot];
            size_t* bulk_slot = nullptr;
            bool new_group = false;
            size_t group_index = 0;
            if (!bulk_cell.IsNull() && bulk_cell.type == int_group_type &&
                !bulk_cell.IsUnsigned()) {
              bool inserted = false;
              bulk_slot = local.int_offsets.TryEmplace(
                  bulk_cell.value.int_value, &inserted);
              if (!inserted) {
                group_index = *bulk_slot;
              } else {
                local.key_scratch.assign(1, bulk_cell);
                new_group = true;
              }
            } else {
              local.key_scratch.clear();
              local.key_scratch.push_back(CanonicalDistinctValue(bulk_cell));
              if (auto found = local.offsets.find(local.key_scratch);
                  found != local.offsets.end()) {
                group_index = found->second;
              } else {
                new_group = true;
              }
            }
            if (new_group) {
              group_index = local.groups.size();
              FusedGroup group;
              group.rep_left = &left_row;
              group.rep_right_idx = bulk->r_idx;
              if (bulk_slot != nullptr) {
                if (!all_direct_group_keys) {
                  group.key = Row(local.key_scratch);
                }
                local.groups.push_back(std::move(group));
                *bulk_slot = group_index;
              } else {
                group.key = Row(local.key_scratch);
                local.groups.push_back(std::move(group));
                local.offsets.emplace(Row(local.key_scratch), group_index);
              }
            }
            local.groups[group_index].raw_count += bulk->count;
            continue;
          }
          if (use_dense_int_rows) {
            const uint64_t idx = static_cast<uint64_t>(key) -
                                 static_cast<uint64_t>(dense_int_min);
            const uint32_t r_idx = idx < dense_int_idxs.size()
                                       ? dense_int_idxs[idx]
                                       : kInvalidRightIdx;
            bool matched = false;
            if (r_idx != kInvalidRightIdx) {
              ++local.comparisons;
              if (use_preassigned_build_groups) {
                RETURN_IF_FAIL(
                    accumulate_preassigned_pair(w, scratch, left_row, r_idx));
                matched = true;
              } else {
                size_t cached_group = kNoCachedGroup;
                ASSIGN_OR_RETURN(bool, accumulated,
                                 process_pair(local, left_row, r_idx, scratch,
                                              &cached_group));
                matched = accumulated;
              }
            }
            if (!matched && join_type == JoinType::kLeft) {
              RETURN_IF_FAIL(unmatched_left(local, scratch, left_row));
            }
            continue;
          }
          if (use_unique_int_table) {
            if (ri + 8 < end) {
              const Row& next_row = left.rows[ri + 8];
              if (!HasNullKey(next_row, left_columns)) [[likely]] {
                if (integer_key) {
                  const int64_t nk =
                      AddJoinKeyBias(IntegerJoinKey(next_row, left_columns[0]),
                                     left_key_offsets[0]);
                  __builtin_prefetch(&unique_int_table[hash_unique_int_key(nk) &
                                                       unique_int_mask],
                                     0, 1);
                } else {
                  const int64_t nk0 =
                      AddJoinKeyBias(IntegerJoinKey(next_row, left_columns[0]),
                                     left_key_offsets[0]);
                  const int64_t nk1 =
                      AddJoinKeyBias(IntegerJoinKey(next_row, left_columns[1]),
                                     left_key_offsets[1]);
                  if (fits_fused_i32(nk0) && fits_fused_i32(nk1)) [[likely]] {
                    __builtin_prefetch(
                        &unique_int_table[hash_unique_int_key(
                                              pack_fused_2x32(nk0, nk1)) &
                                          unique_int_mask],
                        0, 1);
                  }
                }
              }
            }
            size_t slot = hash_unique_int_key(key) & unique_int_mask;
            uint32_t r_idx = kInvalidRightIdx;
            while (true) {
              const UniqueIntSlot& entry = unique_int_table[slot];
              if (entry.r_idx == kInvalidRightIdx) {
                break;
              }
              if (entry.key == key) [[likely]] {
                r_idx = entry.r_idx;
                break;
              }
              slot = (slot + 1) & unique_int_mask;
            }
            bool matched = false;
            if (r_idx != kInvalidRightIdx) [[likely]] {
              ++local.comparisons;
              if (use_preassigned_build_groups) {
                RETURN_IF_FAIL(
                    accumulate_preassigned_pair(w, scratch, left_row, r_idx));
                matched = true;
              } else {
                size_t cached_group = kNoCachedGroup;
                ASSIGN_OR_RETURN(bool, accumulated,
                                 process_pair(local, left_row, r_idx, scratch,
                                              &cached_group));
                matched = accumulated;
              }
            }
            if (!matched && join_type == JoinType::kLeft) {
              RETURN_IF_FAIL(unmatched_left(local, scratch, left_row));
            }
            continue;
          }
          if (ri + 4 < end) {
            const Row& next_row = left.rows[ri + 4];
            if (!HasNullKey(next_row, left_columns)) [[likely]] {
              if (integer_key) {
                int_buckets->PrefetchBucket(
                    AddJoinKeyBias(IntegerJoinKey(next_row, left_columns[0]),
                                   left_key_offsets[0]));
              } else {
                const int64_t nk0 =
                    AddJoinKeyBias(IntegerJoinKey(next_row, left_columns[0]),
                                   left_key_offsets[0]);
                const int64_t nk1 =
                    AddJoinKeyBias(IntegerJoinKey(next_row, left_columns[1]),
                                   left_key_offsets[1]);
                if (fits_fused_i32(nk0) && fits_fused_i32(nk1)) [[likely]] {
                  int_buckets->PrefetchBucket(pack_fused_2x32(nk0, nk1));
                }
              }
            }
          }
          bool matched = false;
          size_t cached_group = kNoCachedGroup;
          const auto [first, last] = int_buckets->equal_range(key);
          for (auto iter = first; iter != last; ++iter) {
            ++local.comparisons;
            if (use_preassigned_build_groups) {
              RETURN_IF_FAIL(accumulate_preassigned_pair(w, scratch, left_row,
                                                         iter->second));
              matched = true;
            } else {
              ASSIGN_OR_RETURN(bool, accumulated,
                               process_pair(local, left_row, iter->second,
                                            scratch, &cached_group));
              matched = matched || accumulated;
            }
          }
          if (!matched && join_type == JoinType::kLeft) {
            RETURN_IF_FAIL(unmatched_left(local, scratch, left_row));
          }
        }
      }
    }
    if (composite_int_key) {
      const FlatJoinIdxBuckets<CompositeIntKey, CompositeIntKeyHash>& buckets =
          *composite_buckets;
      while (true) {
        const size_t chunk = next_chunk.fetch_add(1);
        if (chunk >= chunk_count) {
          return Status::kSuccess;
        }
        const size_t begin = chunk * kProbeChunkRows;
        const size_t end = std::min(total_left, begin + kProbeChunkRows);
        for (size_t ri = begin; ri < end; ++ri) {
          const Row& left_row = left.rows[ri];
          if (HasNullKey(left_row, left_columns)) {
            if (join_type == JoinType::kLeft) {
              RETURN_IF_FAIL(unmatched_left(local, scratch, left_row));
            }
            continue;
          }
          const CompositeIntKey key =
              EncodeCompositeIntKey(left_row, left_columns, &left_key_offsets);
          bool matched = false;
          size_t cached_group = kNoCachedGroup;
          const auto [first, last] = buckets.equal_range(key);
          for (auto iter = first; iter != last; ++iter) {
            ++local.comparisons;
            if (use_preassigned_build_groups) {
              RETURN_IF_FAIL(accumulate_preassigned_pair(w, scratch, left_row,
                                                         iter->second));
              matched = true;
            } else {
              ASSIGN_OR_RETURN(bool, accumulated,
                               process_pair(local, left_row, iter->second,
                                            scratch, &cached_group));
              matched = matched || accumulated;
            }
          }
          if (!matched && join_type == JoinType::kLeft) {
            RETURN_IF_FAIL(unmatched_left(local, scratch, left_row));
          }
        }
      }
    }
    const FlatJoinIdxBuckets<std::string>& buckets = *key_buckets;
    while (true) {
      const size_t chunk = next_chunk.fetch_add(1);
      if (chunk >= chunk_count) {
        return Status::kSuccess;
      }
      const size_t begin = chunk * kProbeChunkRows;
      const size_t end = std::min(total_left, begin + kProbeChunkRows);
      for (size_t ri = begin; ri < end; ++ri) {
        const Row& left_row = left.rows[ri];
        if (HasNullKey(left_row, left_columns)) {
          if (join_type == JoinType::kLeft) {
            RETURN_IF_FAIL(unmatched_left(local, scratch, left_row));
          }
          continue;
        }
        const std::string key =
            EncodeJoinKey(left_row, left_columns, &left_key_offsets);
        bool matched = false;
        size_t cached_group = kNoCachedGroup;
        const auto [first, last] = buckets.equal_range(key);
        for (auto iter = first; iter != last; ++iter) {
          ++local.comparisons;
          if (use_preassigned_build_groups) {
            RETURN_IF_FAIL(accumulate_preassigned_pair(w, scratch, left_row,
                                                       iter->second));
            matched = true;
          } else {
            ASSIGN_OR_RETURN(bool, accumulated,
                             process_pair(local, left_row, iter->second,
                                          scratch, &cached_group));
            matched = matched || accumulated;
          }
        }
        if (!matched && join_type == JoinType::kLeft) {
          RETURN_IF_FAIL(unmatched_left(local, scratch, left_row));
        }
      }
    }
  };
  {
    ASSIGN_OR_RETURN(
        DistinctLSTableProbeOutcome, distinct_probe_outcome,
        TryDistinctLSTableProbe(DistinctLSTableProbeContext{
            .context = context,
            .left = left,
            .right = right,
            .fusion = fusion,
            .result = result,
            .int_buckets = int_buckets,
            .equality_keys = equality_keys,
            .residual_programs = residual_programs,
            .deferred_left_probe = deferred_left_probe,
            .key_programs = key_programs,
            .key_shapes = key_shapes,
            .left_width = left_width,
            .def_overwritten_by_pk = def_overwritten_by_pk,
            .left_columns = left_columns,
            .left_key_offsets = left_key_offsets,
            .join_begin = join_begin,
            .dense_int_idxs = dense_int_idxs,
            .dense_int_min = dense_int_min,
            .unique_int_table = unique_int_table,
            .unique_int_mask = unique_int_mask,
            .worker_count = locals.size(),
            .join_type = join_type,
            .use_dense_int_rows = use_dense_int_rows,
            .use_unique_int_table = use_unique_int_table,
            .singleton_right_relation = singleton_right_relation,
            .integer_key = integer_key,
            .packed_2x32_int_key = packed_2x32_int_key,
            .prepared_pk_lookups_empty = prepared_pk_lookups.empty(),
            .def_local_preds_ok = def_local_preds_ok,
            .distinct_fact_singleton = distinct_fact_singleton,
            .debug_fusion = debug_fusion}));
    if (distinct_probe_outcome.handled) {
      return std::move(distinct_probe_outcome.result);
    }
  }
  const bool track_group_phases =
      std::getenv("TINYLAMB_GROUP_PHASES") != nullptr &&
      effective_left_rows >= 100000;
  std::chrono::steady_clock::time_point phase_probe_begin = join_begin;
  std::chrono::steady_clock::time_point phase_merge_begin = join_begin;
  std::chrono::steady_clock::time_point phase_merge_done = join_begin;
  std::chrono::steady_clock::time_point phase_emit_begin = join_begin;
  phase_probe_begin = std::chrono::steady_clock::now();
  if (locals.size() == 1) {
    // Small grouped joins need no helper or transfer. Besides thread startup,
    // this preserves the caller's expression scratch capacity across queries.
    locals[0].error = run_probe(0, locals[0], scratch_storage[0]);
  } else {
    RETURN_IF_FAIL(RunParallelTasks(locals.size(), [&](size_t w) {
      locals[w].error = run_probe(w, locals[w], scratch_storage[w]);
    }));
  }
  for (const FusedWorkerLocal& local : locals) {
    RETURN_IF_FAIL(local.error);
  }
  if (ordered_sum_charge) {
    // A group belongs to one fold worker. Visiting entries in input order
    // preserves every floating-point prefix while avoiding a lock per row.
    RETURN_IF_FAIL(RunParallelTasks(workers, [&](size_t worker) {
      auto& state = direct_gid_workers[0];
      const size_t chunk = (pre_group_count + workers - 1) / workers;
      const size_t begin = worker * chunk;
      const size_t end = std::min(pre_group_count, begin + chunk);
      for (const auto& item : ordered_sum_inputs) {
        if (item.gid < begin || item.gid >= end) {
          continue;
        }
        const size_t gid = item.gid;
        state.matched[gid] = 1;
        if (!item.nonnull) {
          continue;
        }
        const double previous = state.doubles[gid];
        const double next = previous + item.value;
        if (std::isinf(next) && !std::isinf(previous) &&
            !std::isinf(item.value)) {
          state.inf_flags[gid] = 1;
        }
        state.doubles[gid] = next;
        state.counts[gid] = 1;
      }
    }));
  }
  result.hash_joins = left.hash_joins + right.hash_joins + 1;
  result.in_memory_hash_joins =
      left.in_memory_hash_joins + right.in_memory_hash_joins + 1;
  result.nested_loop_joins = left.nested_loop_joins + right.nested_loop_joins;
  result.hybrid_hash_joins = left.hybrid_hash_joins + right.hybrid_hash_joins;
  result.join_comparisons = left.join_comparisons + right.join_comparisons;
  for (const FusedWorkerLocal& local : locals) {
    result.join_comparisons += local.comparisons;
  }
  phase_merge_begin = std::chrono::steady_clock::now();

  // Merge partials in worker order.  Groups are visited through each
  // worker's insertion-ordered vector (not its hash map) so the emission
  // order is deterministic; a group first seen by an earlier worker merges
  // later partials into its adopted state.
  const auto same_group_key = [&](FusedGroup& a, FusedGroup& b) -> bool {
    if (!a.key.values_.empty() && !b.key.values_.empty()) {
      return GroupKeyEqual{}(a.key, b.key);
    }
    if (!all_direct_group_keys) {
      ensure_group_key(a);
      ensure_group_key(b);
      return GroupKeyEqual{}(a.key, b.key);
    }
    Value tmp_a;
    Value tmp_b;
    const auto key_cell_of = [&](const FusedGroup& g, size_t k,
                                 Value* scratch_v) -> const Value& {
      if (!g.key.values_.empty()) {
        return g.key.values_[k];
      }
      return read_group_pair_cell(
          g, static_cast<size_t>(key_shapes[k].direct_column), scratch_v);
    };
    if (!int_key_indices.empty()) {
      for (const size_t ik : int_key_indices) {
        const Value& x = key_cell_of(a, ik, &tmp_a);
        const Value& y = key_cell_of(b, ik, &tmp_b);
        if (x.type != y.type ||
            (!x.IsNull() && x.value.int_value != y.value.int_value)) {
          return false;
        }
      }
    }
    if (int_key_indices.size() < key_programs.size()) {
      for (size_t k = 0; k < key_programs.size(); ++k) {
        const Value& x = key_cell_of(a, k, &tmp_a);
        const Value& y = key_cell_of(b, k, &tmp_b);
        if (&x == &y) {
          continue;
        }
        if (x.type != y.type) {
          return false;
        }
        if (x.type == ValueType::kDouble) {
          const double p = x.value.double_value;
          const double q = y.value.double_value;
          if (std::isnan(p) && std::isnan(q)) {
            continue;
          }
          if (!(p == q)) {
            return false;
          }
          continue;
        }
        if (!(x == y)) {
          return false;
        }
      }
    }
    return true;
  };
  GroupKeyMap offsets;
  FlatIntKeyMap<size_t> int_merge_offsets;
  std::vector<FusedGroup> groups;
  FusedRawArena merged_raw_arena;
  merged_raw_arena.has_stat = has_stat_raw_agg;
  merged_raw_arena.has_double = !raw_sum_fusion || has_double_raw_agg;
  merged_raw_arena.has_int = !raw_sum_fusion || has_int_raw_agg;
  std::vector<FusedRawArena> extra_merged_arenas;
  std::deque<AggregateAccumulator> states;
  if (compact_counts) {
    if (compact_split_key_only) {
      const uint64_t r_mask =
          (!split_uses_right_subgid || split_right_subgid_count <= 1)
              ? 0ULL
              : ((uint64_t{1} << std::bit_width(split_right_subgid_count - 1)) -
                 1ULL);
      const auto make_split_group = [&](uint64_t packed_u64, int64_t cnt) {
        FusedGroup group;
        group.key_hash = packed_u64;
        group.has_key_hash = true;
        group.rep_left = nullptr;
        if (split_uses_right_subgid) {
          const auto r_subgid =
              static_cast<size_t>((packed_u64 >> split_right_shift) & r_mask);
          group.rep_right_idx = split_right_rep_ptr[r_subgid];
        } else if (right_row_count > 0) {
          group.rep_right_idx = 0;
        }
        group.raw_count = cnt;
        return group;
      };
      const bool any_sharded =
          std::ranges::any_of(locals, [](const auto& local) {
            return !local.compact_split_shards.empty();
          });
      if (!any_sharded) {
        if (locals.size() == 1) {
          for (const auto& entry : locals.front().compact_counts) {
            if (compact_count_passes(entry.count)) {
              groups.push_back(make_split_group(entry.hash, entry.count));
            }
          }
        } else {
          std::vector<uint64_t> merged_keys;
          std::vector<int64_t> merged_counts;
          FlatIntKeyMap<size_t> compact_offsets;
          size_t hint = 0;
          for (const auto& local : locals) {
            hint += local.compact_counts.size();
          }
          merged_keys.reserve(hint);
          merged_counts.reserve(hint);
          compact_offsets.Reserve(hint);
          for (const auto& local : locals) {
            for (const auto& entry : local.compact_counts) {
              bool inserted = false;
              size_t* slot = compact_offsets.TryEmplace(
                  std::bit_cast<int64_t>(entry.hash), &inserted);
              if (!inserted) {
                merged_counts[*slot] += entry.count;
              } else {
                *slot = merged_keys.size();
                merged_keys.push_back(entry.hash);
                merged_counts.push_back(entry.count);
              }
            }
          }
          for (size_t i = 0; i < merged_keys.size(); ++i) {
            if (compact_count_passes(merged_counts[i])) {
              groups.push_back(
                  make_split_group(merged_keys[i], merged_counts[i]));
            }
          }
        }
      } else {
        struct PreSplitEntry {
          uint64_t key;
          int64_t count;
        };
        std::array<std::vector<PreSplitEntry>, kCompactSplitShards>
            pre_entries{};
        size_t total_entries = 0;
        for (auto& local : locals) {
          for (const auto& entry : local.compact_counts) {
            uint64_t h = entry.hash + 0x9e3779b97f4a7c15ULL;
            h = (h ^ (h >> 30U)) * 0xbf58476d1ce4e5b9ULL;
            h = (h ^ (h >> 27U)) * 0x94d049bb133111ebULL;
            h ^= h >> 31U;
            pre_entries[h & (kCompactSplitShards - 1)].push_back(
                PreSplitEntry{.key = entry.hash, .count = entry.count});
            ++total_entries;
          }
          std::vector<CompactCountGroup>().swap(local.compact_counts);
          local.int_offsets = FlatIntKeyMap<size_t>();
          for (const auto& shard : local.compact_split_shards) {
            total_entries += shard.size();
          }
        }
        const size_t shard_workers = std::min<size_t>(
            kCompactSplitShards,
            ParallelWorkerCount(std::max<size_t>(1, total_entries / 2048)));
        std::array<std::vector<PreSplitEntry>, kCompactSplitShards>
            shard_accepted{};
        RETURN_IF_FAIL(RunParallelTasks(shard_workers, [&](size_t sw) {
          if (total_entries < std::numeric_limits<uint32_t>::max()) {
            struct CompactShardSlot {
              uint64_t key{0};
              uint32_t gen{0};
              uint32_t count{0};
            };
            std::vector<CompactShardSlot> table;
            std::vector<uint32_t> occupied;
            uint32_t gen = 0;
            for (size_t s = sw; s < kCompactSplitShards; s += shard_workers) {
              size_t shard_n = pre_entries[s].size();
              for (const auto& local : locals) {
                if (s < local.compact_split_shards.size()) {
                  shard_n += local.compact_split_shards[s].size();
                }
              }
              if (shard_n == 0) {
                continue;
              }
              const size_t min_cap =
                  std::max<size_t>(64, shard_n + (shard_n / 2) + 16);
              if (min_cap > table.size()) {
                table.assign(std::bit_ceil(min_cap), CompactShardSlot{});
                gen = 1;
              } else {
                ++gen;
                if (gen == 0) {
                  std::fill(table.begin(), table.end(), CompactShardSlot{});
                  gen = 1;
                }
              }
              const size_t mask = table.size() - 1;
              occupied.clear();
              const auto add_key = [&](uint64_t key, uint32_t add) {
                uint64_t h =
                    (key + 0x9e3779b97f4a7c15ULL) * 0xbf58476d1ce4e5b9ULL;
                h ^= h >> 30U;
                size_t idx = static_cast<size_t>(h) & mask;
                while (true) {
                  CompactShardSlot& slot_entry = table[idx];
                  if (slot_entry.gen != gen) {
                    slot_entry.gen = gen;
                    slot_entry.key = key;
                    slot_entry.count = add;
                    occupied.push_back(static_cast<uint32_t>(idx));
                    return;
                  }
                  if (slot_entry.key == key) {
                    slot_entry.count += add;
                    return;
                  }
                  idx = (idx + 1) & mask;
                }
              };
              for (const auto& pre : pre_entries[s]) {
                add_key(pre.key, static_cast<uint32_t>(pre.count));
              }
              for (auto& local : locals) {
                if (s >= local.compact_split_shards.size()) {
                  continue;
                }
                for (const uint64_t k : local.compact_split_shards[s]) {
                  add_key(k, 1U);
                }
                std::vector<uint64_t>().swap(local.compact_split_shards[s]);
              }
              for (const uint32_t idx : occupied) {
                const auto signed_cnt = static_cast<int64_t>(table[idx].count);
                if (compact_count_passes(signed_cnt)) {
                  shard_accepted[s].push_back(PreSplitEntry{
                      .key = table[idx].key, .count = signed_cnt});
                }
              }
            }
            return;
          }
          FlatIntKeyMap<uint64_t> counts_map;
          for (size_t s = sw; s < kCompactSplitShards; s += shard_workers) {
            size_t shard_n = pre_entries[s].size();
            for (const auto& local : locals) {
              if (s < local.compact_split_shards.size()) {
                shard_n += local.compact_split_shards[s].size();
              }
            }
            if (shard_n == 0) {
              continue;
            }
            counts_map.Clear();
            counts_map.Reserve(shard_n);
            for (const auto& pre : pre_entries[s]) {
              *counts_map.TryEmplace(std::bit_cast<int64_t>(pre.key)) +=
                  static_cast<uint64_t>(pre.count);
            }
            for (auto& local : locals) {
              if (s >= local.compact_split_shards.size()) {
                continue;
              }
              for (const uint64_t k : local.compact_split_shards[s]) {
                ++(*counts_map.TryEmplace(std::bit_cast<int64_t>(k)));
              }
              std::vector<uint64_t>().swap(local.compact_split_shards[s]);
            }
            counts_map.ForEach([&](int64_t k, uint64_t cnt) {
              const auto signed_cnt = static_cast<int64_t>(cnt);
              if (compact_count_passes(signed_cnt)) {
                shard_accepted[s].push_back(PreSplitEntry{
                    .key = std::bit_cast<uint64_t>(k), .count = signed_cnt});
              }
            });
          }
        }));
        for (size_t s = 0; s < kCompactSplitShards; ++s) {
          for (const auto& entry : shard_accepted[s]) {
            groups.push_back(make_split_group(entry.key, entry.count));
          }
        }
      }
      fusion->post_having_applied = true;
    } else {
      size_t partial_count = 0;
      for (const auto& local : locals) {
        partial_count += local.compact_counts.size();
      }
      const size_t merge_workers =
          std::min(locals.size(), ParallelWorkerCount(partial_count / 4096));
      static const bool merge_disabled =
          std::getenv("TINYLAMB_DISABLE_FUSED_PARTITION_MERGE") != nullptr;
      auto merge_charge = !merge_disabled && merge_workers > 1 &&
                                  partial_count <= SIZE_MAX / 256
                              ? TryReserveQueryMemory(partial_count * 256)
                              : nullptr;
      if (merge_charge) {
        static const bool prefilter_disabled =
            std::getenv("TINYLAMB_DISABLE_COUNT_PREFILTER") != nullptr;
        uint8_t prefilter_limit = 0;
        if (!prefilter_disabled) {
          if (compact_integer_threshold && *compact_integer_threshold >= 3 &&
              *compact_integer_threshold <= 254) {
            prefilter_limit =
                static_cast<uint8_t>(*compact_integer_threshold + 1);
          } else if (!compact_integer_threshold &&
                     fusion->post_having_constant_or_factor >= 3.0 &&
                     fusion->post_having_constant_or_factor < 255.0) {
            prefilter_limit = static_cast<uint8_t>(
                static_cast<unsigned>(fusion->post_having_constant_or_factor) +
                1U);
          }
        }
        struct Ref {
          const CompactCountGroup* group;
          size_t rank;
        };
        struct RankedCount {
          CompactCountGroup group;
          size_t rank;
        };
        std::vector<size_t> rank_bases(locals.size(), 0);
        for (size_t w = 1; w < locals.size(); ++w) {
          rank_bases[w] =
              rank_bases[w - 1] + locals[w - 1].compact_counts.size();
        }
        std::vector<std::vector<std::vector<Ref>>> worker_buckets(
            locals.size());
        RETURN_IF_FAIL(RunParallelTasks(locals.size(), [&](size_t w) {
          const auto& counts = locals[w].compact_counts;
          auto& wb = worker_buckets[w];
          wb.resize(merge_workers);
          const size_t approx = (counts.size() / merge_workers) + 16;
          for (auto& b : wb) {
            b.reserve(approx);
          }
          size_t r = rank_bases[w];
          for (const auto& group : counts) {
            const size_t partition =
                (group.hash ^ (group.hash >> 32U)) % merge_workers;
            wb[partition].push_back(Ref{.group = &group, .rank = r++});
          }
        }));
        std::vector<std::vector<RankedCount>> accepted(merge_workers);
        RETURN_IF_FAIL(RunParallelTasks(merge_workers, [&](size_t p) {
          size_t part_size = 0;
          for (size_t w = 0; w < locals.size(); ++w) {
            part_size += worker_buckets[w][p].size();
          }
          std::vector<Ref> bucket_p;
          bucket_p.reserve(part_size);
          for (size_t w = 0; w < locals.size(); ++w) {
            const auto& wb = worker_buckets[w][p];
            bucket_p.insert(bucket_p.end(), wb.begin(), wb.end());
          }
          std::vector<Ref> candidates;
          const auto* refs = &bucket_p;
          if (prefilter_limit != 0 && bucket_p.size() >= 8192) {
            // Counter collisions can only increase a frequency. Saturating at
            // the acceptance bound therefore rejects only groups that cannot
            // satisfy COUNT's HAVING predicate. Every candidate still uses
            // the exact key comparison, exact count and original final
            // predicate. The four arrays use at most four bytes per input
            // reference, covered by the merge reservation above.
            const size_t width = std::bit_ceil(bucket_p.size() / 2);
            const size_t mask = width - 1;
            std::vector<uint8_t> counts(width * 4);
            const auto mixed_hash = [](uint64_t hash) {
              hash ^= hash >> 30U;
              hash *= 0xbf58476d1ce4e5b9ULL;
              hash ^= hash >> 27U;
              hash *= 0x94d049bb133111ebULL;
              return hash ^ (hash >> 31U);
            };
            for (const auto& ref : bucket_p) {
              uint64_t hash = mixed_hash(ref.group->hash);
              const auto add = static_cast<unsigned>(
                  std::min<int64_t>(ref.group->count, prefilter_limit));
              for (size_t lane = 0; lane < 4; ++lane) {
                auto& cell = counts[(lane * width) + (hash & mask)];
                cell = static_cast<uint8_t>(std::min<unsigned>(
                    prefilter_limit, static_cast<unsigned>(cell) + add));
                hash = std::rotl(hash, 16);
              }
            }
            candidates.reserve(std::min<size_t>(4096, bucket_p.size()));
            for (const auto& ref : bucket_p) {
              uint64_t hash = mixed_hash(ref.group->hash);
              bool possible = true;
              for (size_t lane = 0; lane < 4; ++lane) {
                if (counts[(lane * width) + (hash & mask)] < prefilter_limit) {
                  possible = false;
                  break;
                }
                hash = std::rotl(hash, 16);
              }
              if (possible) {
                candidates.push_back(ref);
              }
            }
            refs = &candidates;
          }
          FlatIntKeyMap<size_t> index;
          std::vector<RankedCount> merged;
          merged.reserve(refs->size());
          index.Reserve(refs->size());
          for (const auto& ref : *refs) {
            const auto& group = *ref.group;
            bool inserted = false;
            size_t* slot =
                index.TryEmplace(std::bit_cast<int64_t>(group.hash), &inserted);
            size_t matching = SIZE_MAX;
            if (!inserted) {
              for (size_t candidate = *slot; candidate != SIZE_MAX;
                   candidate = merged[candidate].group.next) {
                if (compact_key_equal(merged[candidate].group, group)) {
                  matching = candidate;
                  break;
                }
              }
            }
            if (matching != SIZE_MAX) {
              merged[matching].group.count += group.count;
            } else {
              auto added = group;
              added.next = inserted ? SIZE_MAX : *slot;
              *slot = merged.size();
              merged.push_back(RankedCount{.group = added, .rank = ref.rank});
            }
          }
          for (auto& group : merged) {
            if (compact_count_passes(group.group.count)) {
              accepted[p].push_back(group);
            }
          }
        }));
        std::vector<RankedCount> ordered;
        for (auto& part : accepted) {
          ordered.insert(ordered.end(), std::make_move_iterator(part.begin()),
                         std::make_move_iterator(part.end()));
        }
        std::ranges::sort(ordered, {}, &RankedCount::rank);
        for (const auto& entry : ordered) {
          FusedGroup group;
          group.rep_left = entry.group.left;
          group.rep_right_idx = entry.group.right_idx;
          group.raw_count = entry.group.count;
          groups.push_back(std::move(group));
        }
      } else {
        // A logged first worker can contain repeated keys. Rebuild its index
        // too when the parallel merge cannot reserve memory.
        const bool logged = std::ranges::any_of(
            locals, [](const auto& local) { return local.compact_count_log; });
        auto merged = logged ? std::vector<CompactCountGroup>{}
                             : std::move(locals.front().compact_counts);
        auto compact_offsets = logged ? FlatIntKeyMap<size_t>{}
                                      : std::move(locals.front().int_offsets);
        for (size_t w = logged ? 0 : 1; w < locals.size(); ++w) {
          for (const auto& group : locals[w].compact_counts) {
            bool inserted = false;
            size_t* slot = compact_offsets.TryEmplace(
                std::bit_cast<int64_t>(group.hash), &inserted);
            size_t matching = SIZE_MAX;
            if (!inserted) {
              for (size_t index = *slot; index != SIZE_MAX;
                   index = merged[index].next) {
                if (compact_key_equal(merged[index], group)) {
                  matching = index;
                  break;
                }
              }
            }
            if (matching != SIZE_MAX) {
              merged[matching].count += group.count;
            } else {
              auto added = group;
              added.next = inserted ? SIZE_MAX : *slot;
              *slot = merged.size();
              merged.push_back(added);
            }
          }
        }
        for (const auto& count : merged) {
          if (compact_count_passes(count.count)) {
            FusedGroup group;
            group.rep_left = count.left;
            group.rep_right_idx = count.right_idx;
            group.raw_count = count.count;
            groups.push_back(std::move(group));
          }
        }
      }
    }
    fusion->post_having_applied = true;
  }
  if (use_direct_gid_agg || use_dense_split_direct_gid) {
    const size_t gid_domain =
        use_dense_split_direct_gid ? dense_split_domain : pre_group_count;
    const size_t dw_count = direct_gid_workers.size();
    const auto merge_gid_range = [&](size_t begin, size_t end,
                                     FusedRawArena& arena,
                                     std::vector<FusedGroup>& out_groups) {
      out_groups.reserve(std::min<size_t>(end - begin, 16384));
      if (use_packed_double_cells) {
        for (size_t gid = begin; gid < end; ++gid) {
          double sum = 0.0;
          uint64_t total_cnt = 0;
          uint8_t any_matched = 0;
          uint8_t any_inf = 0;
          for (size_t w = 0; w < dw_count; ++w) {
            const DirectDoubleCell& c = direct_gid_workers[w].double_cells[gid];
            if ((c.meta & 1ULL) == 0) {
              continue;
            }
            if (!any_matched) {
              sum = c.sum;
              any_matched = 1;
            } else {
              const double next = sum + c.sum;
              if (std::isinf(next) && !std::isinf(sum) && !std::isinf(c.sum)) {
                any_inf = 1;
              }
              sum = next;
            }
            total_cnt += (c.meta >> 2);
            any_inf |= static_cast<uint8_t>((c.meta >> 1) & 1ULL);
          }
          if (any_matched == 0) {
            continue;
          }
          FusedGroup grp;
          auto tmp_gid = static_cast<uint32_t>(gid);
          uint64_t packed_u64 = 0;
          for (size_t idx = dense_pk_group_indices.size(); idx > 0; --idx) {
            const size_t gi = dense_pk_group_indices[idx - 1];
            const uint32_t card = def_pk_groups[gi].subgid_count;
            const uint32_t psg = tmp_gid % card;
            tmp_gid /= card;
            packed_u64 |= static_cast<uint64_t>(psg)
                          << def_pk_groups[gi].subgid_shift;
          }
          const uint32_t r_subgid = tmp_gid;
          if (split_uses_right_subgid) {
            packed_u64 |= static_cast<uint64_t>(r_subgid) << split_right_shift;
          }
          grp.key_hash = packed_u64;
          grp.has_key_hash = true;
          grp.rep_left = nullptr;
          grp.rep_right_idx =
              split_uses_right_subgid
                  ? split_right_rep_ptr[r_subgid]
                  : (right_row_count > 0 ? 0U : kInvalidRightIdx);
          arena.Initialize(grp, raw_slot_count);
          grp.RawDoubles()[0] = sum;
          grp.RawCounts()[0] = static_cast<int64_t>(total_cnt);
          grp.RawInfFlags()[0] = any_inf;
          out_groups.push_back(std::move(grp));
        }
        return;
      }
      for (size_t gid = begin; gid < end; ++gid) {
        bool any_matched = false;
        for (size_t w = 0; w < dw_count; ++w) {
          if (direct_gid_workers[w].matched[gid] != 0) {
            any_matched = true;
            break;
          }
        }
        if (!any_matched) {
          continue;
        }
        FusedGroup grp;
        if (use_dense_split_direct_gid) {
          auto tmp_gid = static_cast<uint32_t>(gid);
          uint64_t packed_u64 = 0;
          for (size_t idx = dense_pk_group_indices.size(); idx > 0; --idx) {
            const size_t gi = dense_pk_group_indices[idx - 1];
            const uint32_t card = def_pk_groups[gi].subgid_count;
            const uint32_t psg = tmp_gid % card;
            tmp_gid /= card;
            packed_u64 |= static_cast<uint64_t>(psg)
                          << def_pk_groups[gi].subgid_shift;
          }
          const uint32_t r_subgid = tmp_gid;
          if (split_uses_right_subgid) {
            packed_u64 |= static_cast<uint64_t>(r_subgid) << split_right_shift;
          }
          grp.key_hash = packed_u64;
          grp.has_key_hash = true;
          grp.rep_left = nullptr;
          grp.rep_right_idx =
              split_uses_right_subgid
                  ? split_right_rep_ptr[r_subgid]
                  : (right_row_count > 0 ? 0U : kInvalidRightIdx);
        } else if (identity_right_gid) {
          grp.rep_right_idx = static_cast<uint32_t>(gid);
        } else {
          grp = std::move(pre_groups[gid]);
        }
        if (raw_sum_fusion) {
          arena.Initialize(grp, raw_slot_count);
        }
        if (raw_count_fusion) {
          int64_t total_cnt = 0;
          for (size_t w = 0; w < dw_count; ++w) {
            total_cnt += direct_gid_workers[w].counts[gid];
          }
          grp.raw_count = total_cnt;
        } else {
          const size_t base = gid * agg_count;
          for (size_t w = 0; w < dw_count; ++w) {
            const DirectGidWorker& dw = direct_gid_workers[w];
            if (dw.matched[gid] == 0) {
              continue;
            }
            for (size_t s = 0; s < agg_count; ++s) {
              switch (slot_agg_kinds[s]) {
                case RawAggKind::kCountStar:
                case RawAggKind::kConstantExtremum:
                case RawAggKind::kCountValue:
                  grp.RawCounts()[s] += dw.counts[base + s];
                  break;
                case RawAggKind::kSumInt64: {
                  int64_t next = 0;
                  if (__builtin_add_overflow(grp.RawInts()[s],
                                             dw.ints[base + s], &next)) {
                    grp.RawInfFlags()[s] = 1;
                  } else {
                    grp.RawInts()[s] = next;
                  }
                  grp.RawCounts()[s] += dw.counts[base + s];
                  grp.RawInfFlags()[s] |= dw.inf_flags[base + s];
                  break;
                }
                case RawAggKind::kSumDouble:
                case RawAggKind::kAvgDouble: {
                  const double next =
                      grp.RawDoubles()[s] + dw.doubles[base + s];
                  if (std::isinf(next) && !std::isinf(grp.RawDoubles()[s]) &&
                      !std::isinf(dw.doubles[base + s])) {
                    grp.RawInfFlags()[s] = 1;
                  }
                  grp.RawDoubles()[s] = next;
                  grp.RawCounts()[s] += dw.counts[base + s];
                  grp.RawInfFlags()[s] |= dw.inf_flags[base + s];
                  break;
                }
                case RawAggKind::kStddevSamp:
                case RawAggKind::kStddevPop:
                case RawAggKind::kVarSamp:
                case RawAggKind::kVarPop:
                  grp.RawStatX()[s] += dw.ext_sx[base + s];
                  grp.RawStatXX()[s] += dw.ext_sxx[base + s];
                  grp.RawCounts()[s] += dw.counts[base + s];
                  grp.RawInfFlags()[s] |= dw.inf_flags[base + s];
                  break;
                case RawAggKind::kCountDistinctInt64:
                case RawAggKind::kNone:
                  break;
              }
            }
          }
        }
        out_groups.push_back(std::move(grp));
      }
    };
    const size_t gid_workers =
        gid_domain >= 16384 && workers > 1
            ? std::min(workers, ParallelWorkerCount(gid_domain / 4096))
            : 1;
    if (gid_workers > 1) {
      extra_merged_arenas.resize(gid_workers);
      for (FusedRawArena& a : extra_merged_arenas) {
        a.has_stat = has_stat_raw_agg;
        a.has_double = !raw_sum_fusion || has_double_raw_agg;
        a.has_int = !raw_sum_fusion || has_int_raw_agg;
      }
      std::vector<std::vector<FusedGroup>> slice_groups(gid_workers);
      RETURN_IF_FAIL(RunParallelTasks(gid_workers, [&](size_t mw) {
        const size_t begin = (gid_domain * mw) / gid_workers;
        const size_t end = (gid_domain * (mw + 1)) / gid_workers;
        merge_gid_range(begin, end, extra_merged_arenas[mw], slice_groups[mw]);
      }));
      size_t total_merged = 0;
      for (const auto& sg : slice_groups) {
        total_merged += sg.size();
      }
      groups.reserve(total_merged);
      for (auto& sg : slice_groups) {
        groups.insert(groups.end(), std::make_move_iterator(sg.begin()),
                      std::make_move_iterator(sg.end()));
      }
    } else {
      merge_gid_range(0, gid_domain, merged_raw_arena, groups);
    }
    if (dw_count > 1) {
      (void)RunParallelTasks(dw_count, [&](size_t w) {
        DirectGidWorker{}.matched.swap(direct_gid_workers[w].matched);
        DirectGidWorker{}.doubles.swap(direct_gid_workers[w].doubles);
        DirectGidWorker{}.ints.swap(direct_gid_workers[w].ints);
        DirectGidWorker{}.counts.swap(direct_gid_workers[w].counts);
        DirectGidWorker{}.inf_flags.swap(direct_gid_workers[w].inf_flags);
        DirectGidWorker{}.ext_sx.swap(direct_gid_workers[w].ext_sx);
        DirectGidWorker{}.ext_sxx.swap(direct_gid_workers[w].ext_sxx);
        std::vector<DirectDoubleCell>{}.swap(
            direct_gid_workers[w].double_cells);
      });
    }
    std::vector<DirectGidWorker>().swap(direct_gid_workers);
  } else if (use_partitioned_count_distinct) {
    std::vector<int64_t> cd_counts(pre_group_count, 0);
    auto sort_partition = [&](size_t p) {
      size_t total_pairs = 0;
      for (size_t w = 0; w < workers; ++w) {
        total_pairs += cd_worker_parts[w][p].size();
      }
      if (total_pairs == 0) {
        return;
      }
      std::vector<GidValPair> buf;
      buf.reserve(total_pairs);
      for (size_t w = 0; w < workers; ++w) {
        auto& vec = cd_worker_parts[w][p];
        buf.insert(buf.end(), vec.begin(), vec.end());
        std::vector<GidValPair>().swap(vec);
      }
      std::sort(buf.begin(), buf.end());
      uint32_t cur_gid = buf[0].gid;
      int64_t cur_val = buf[0].val;
      int64_t cur_cnt = 1;
      for (size_t i = 1; i < buf.size(); ++i) {
        if (buf[i].gid != cur_gid) {
          cd_counts[cur_gid] = cur_cnt;
          cur_gid = buf[i].gid;
          cur_val = buf[i].val;
          cur_cnt = 1;
        } else if (buf[i].val != cur_val) {
          cur_val = buf[i].val;
          ++cur_cnt;
        }
      }
      cd_counts[cur_gid] = cur_cnt;
    };
    if (workers <= 1) {
      sort_partition(0);
    } else {
      std::vector<std::jthread> cd_threads;
      cd_threads.reserve(workers - 1);
      for (size_t p = 1; p < workers; ++p) {
        cd_threads.emplace_back([&, p] { sort_partition(p); });
      }
      sort_partition(0);
    }
    groups.reserve(pre_group_count);
    for (size_t gid = 0; gid < pre_group_count; ++gid) {
      bool any_matched = (cd_counts[gid] > 0);
      if (!any_matched) {
        for (size_t w = 0; w < workers; ++w) {
          if (cd_worker_matched[w][gid] != 0) {
            any_matched = true;
            break;
          }
        }
      }
      if (!any_matched) {
        continue;
      }
      FusedGroup grp = std::move(pre_groups[gid]);
      merged_raw_arena.Initialize(grp, fusion->aggregates.size());
      grp.RawCounts()[0] = cd_counts[gid];
      groups.push_back(std::move(grp));
    }
  }
  const auto merge_raw = [&](FusedGroup& destination, FusedGroup& group) {
    for (size_t s = 0; s < raw_slot_count; ++s) {
      switch (slot_agg_kinds[s]) {
        case RawAggKind::kCountStar:
        case RawAggKind::kConstantExtremum:
        case RawAggKind::kCountValue:
          destination.RawCounts()[s] += group.RawCounts()[s];
          break;
        case RawAggKind::kSumInt64: {
          int64_t next = 0;
          if (__builtin_add_overflow(destination.RawInts()[s],
                                     group.RawInts()[s], &next)) {
            destination.RawInfFlags()[s] = 1;
          } else {
            destination.RawInts()[s] = next;
          }
          destination.RawCounts()[s] += group.RawCounts()[s];
          destination.RawInfFlags()[s] |= group.RawInfFlags()[s];
          break;
        }
        case RawAggKind::kSumDouble:
        case RawAggKind::kAvgDouble: {
          const double next =
              destination.RawDoubles()[s] + group.RawDoubles()[s];
          if (std::isinf(next) && !std::isinf(destination.RawDoubles()[s]) &&
              !std::isinf(group.RawDoubles()[s])) {
            destination.RawInfFlags()[s] = 1;
          }
          destination.RawDoubles()[s] = next;
          destination.RawCounts()[s] += group.RawCounts()[s];
          destination.RawInfFlags()[s] |= group.RawInfFlags()[s];
          break;
        }
        case RawAggKind::kStddevSamp:
        case RawAggKind::kStddevPop:
        case RawAggKind::kVarSamp:
        case RawAggKind::kVarPop:
          destination.RawStatX()[s] += group.RawStatX()[s];
          destination.RawStatXX()[s] += group.RawStatXX()[s];
          destination.RawCounts()[s] += group.RawCounts()[s];
          destination.RawInfFlags()[s] |= group.RawInfFlags()[s];
          break;
        case RawAggKind::kCountDistinctInt64:
          if (group.raw_int_set) {
            if (!destination.raw_int_set) {
              destination.raw_int_set = std::move(group.raw_int_set);
            } else {
              group.raw_int_set->ForEach([&](int64_t v, uint8_t) {
                destination.raw_int_set->TryEmplace(v);
              });
            }
          }
          break;
        case RawAggKind::kNone:
          break;
      }
    }
  };
  std::vector<FusedGroup*> group_ptrs;
  bool use_group_ptrs = false;
  if (use_radix_agg) {
    radix_part_arenas.resize(kRadixParts);
    radix_part_groups.resize(kRadixParts);
    for (size_t p = 0; p < kRadixParts; ++p) {
      radix_part_arenas[p].has_stat = has_stat_raw_agg;
      radix_part_arenas[p].has_double = !raw_sum_fusion || has_double_raw_agg;
      radix_part_arenas[p].has_int = !raw_sum_fusion || has_int_raw_agg;
    }
    const bool has_split_rep_right =
        split_right_rep_ptr != nullptr &&
        ((split_key_unpack_only && split_uses_right_subgid) ||
         (left_pack_unpack_only && left_pack_uses_right));
    const uint64_t radix_r_mask =
        !has_split_rep_right
            ? 0ULL
            : ((left_pack_unpack_only && left_pack_uses_right)
                   ? left_pack_right_mask
                   : (split_right_subgid_count <= 1
                          ? 0ULL
                          : ((uint64_t{1}
                              << std::bit_width(split_right_subgid_count - 1)) -
                             1ULL)));
    std::atomic<size_t> next_part{0};
    const size_t part_workers = std::min<size_t>(workers, kRadixParts);
    RETURN_IF_FAIL(RunParallelTasks(part_workers, [&](size_t) {
      while (true) {
        const size_t p = next_part.fetch_add(1, std::memory_order_relaxed);
        if (p >= kRadixParts) {
          break;
        }
        size_t part_total = 0;
        for (auto& worker_radix_bucket : worker_radix_buckets) {
          part_total += worker_radix_bucket[p].size();
        }
        if (part_total == 0) {
          continue;
        }
        const size_t part_init =
            std::min<size_t>(std::max<size_t>(part_total / 8, 64), 512);
        FlatIntKeyMap<size_t> part_map;
        part_map.Reserve(part_init);
        FusedRawArena& arena = radix_part_arenas[p];
        std::vector<FusedGroup>& pgroups = radix_part_groups[p];
        pgroups.reserve(part_init);
        for (auto& worker_radix_bucket : worker_radix_buckets) {
          const auto& bucket = worker_radix_bucket[p];
          for (const RadixTuple& tuple : bucket) {
            bool inserted = false;
            size_t* slot = part_map.TryEmplace(
                std::bit_cast<int64_t>(tuple.key), &inserted);
            size_t gi = 0;
            if (!inserted) [[likely]] {
              gi = *slot;
            } else {
              gi = pgroups.size();
              FusedGroup grp;
              arena.Initialize(grp, raw_slot_count);
              grp.key_hash = tuple.key;
              grp.has_key_hash = true;
              if (has_split_rep_right) {
                const auto r_subgid = static_cast<size_t>(
                    (tuple.key >> split_right_shift) & radix_r_mask);
                if (r_subgid < split_right_rep_count) {
                  grp.rep_right_idx = split_right_rep_ptr[r_subgid];
                }
              } else if (right_row_count > 0) {
                grp.rep_right_idx = 0;
              }
              pgroups.push_back(std::move(grp));
              *slot = gi;
            }
            FusedGroup& g = pgroups[gi];
            for (size_t s = 0; s < agg_count; ++s) {
              const uint8_t sop = left_numeric[s].op;
              if (sop == 0) {
                g.RawCounts()[s] += 1;
                continue;
              }
              if (tuple.valid == 0 || sop == 5) {
                continue;
              }
              if (radix_tuple_is_int) {
                const auto iv = static_cast<int64_t>(tuple.bits);
                if (sop == 3) {
                  int64_t next = 0;
                  if (__builtin_add_overflow(g.RawInts()[s], iv, &next)) {
                    g.RawInfFlags()[s] = 1;
                  } else {
                    g.RawInts()[s] = next;
                  }
                  g.RawCounts()[s] += 1;
                } else if (sop == 2) {
                  const auto dv = static_cast<double>(iv);
                  const double prev = g.RawDoubles()[s];
                  const double next = prev + dv;
                  if (std::isinf(next) && !std::isinf(prev) &&
                      !std::isinf(dv)) {
                    g.RawInfFlags()[s] = 1;
                  }
                  g.RawDoubles()[s] = next;
                  g.RawCounts()[s] += 1;
                } else if (sop == 4) {
                  g.RawCounts()[s] += 1;
                } else if (sop == 8) {
                  const auto x = static_cast<platform::ExtendedFloat>(iv);
                  g.RawStatX()[s] += x;
                  g.RawStatXX()[s] += x * x;
                  g.RawCounts()[s] += 1;
                }
              } else {
                const auto dv = std::bit_cast<double>(tuple.bits);
                if (sop == 1 || sop == 2 || sop == 6 || sop == 9) {
                  const double prev = g.RawDoubles()[s];
                  const double next = prev + dv;
                  if (std::isinf(next) && !std::isinf(prev) &&
                      !std::isinf(dv)) {
                    g.RawInfFlags()[s] = 1;
                  }
                  g.RawDoubles()[s] = next;
                  g.RawCounts()[s] += 1;
                } else if (sop == 4 || sop == 7) {
                  g.RawCounts()[s] += 1;
                } else if (sop == 8) {
                  const auto x = static_cast<platform::ExtendedFloat>(dv);
                  g.RawStatX()[s] += x;
                  g.RawStatXX()[s] += x * x;
                  g.RawCounts()[s] += 1;
                }
              }
            }
          }
        }
      }
    }));
    std::vector<std::array<std::vector<RadixTuple>, kRadixParts>>().swap(
        worker_radix_buckets);
    size_t fallback_worker_groups = 0;
    for (const FusedWorkerLocal& local : locals) {
      fallback_worker_groups += local.groups.size();
    }
    if (fallback_worker_groups == 0) {
      size_t radix_total_groups = 0;
      for (const auto& pgroups : radix_part_groups) {
        radix_total_groups += pgroups.size();
      }
      group_ptrs.reserve(radix_total_groups);
      for (auto& pgroups : radix_part_groups) {
        for (FusedGroup& g : pgroups) {
          group_ptrs.push_back(&g);
        }
      }
      use_group_ptrs = true;
      if (result.runtime() != nullptr) {
        ++result.runtime()->partitioned_aggregate_merges;
      }
    } else {
      for (auto& pgroups : radix_part_groups) {
        for (FusedGroup& g : pgroups) {
          locals.front().groups.push_back(std::move(g));
        }
        std::vector<FusedGroup>().swap(pgroups);
      }
    }
  }
  size_t total_worker_groups = 0;
  if (!use_group_ptrs) {
    for (const FusedWorkerLocal& local : locals) {
      total_worker_groups += local.groups.size();
    }
    auto extract_single_int_key = [&](const FusedGroup& group,
                                      int64_t* out_key) -> bool {
      Value col_tmp;
      const Value* cell = nullptr;
      if (group.key.values_.size() == 1) {
        cell = group.key.values_.data();
      } else if (all_direct_group_keys &&
                 (group.rep_left != nullptr ||
                  group.rep_col_chunk != std::numeric_limits<uint32_t>::max() ||
                  group.rep_right_idx != kInvalidRightIdx)) {
        cell = &read_group_pair_cell(group, int_group_slot, &col_tmp);
      }
      if (cell != nullptr && !cell->IsNull() && cell->type == int_group_type &&
          !cell->IsUnsigned()) {
        *out_key = cell->value.int_value;
        return true;
      }
      return false;
    };
    auto mix_int_partition = [](int64_t key) noexcept -> uint64_t {
      auto x = static_cast<uint64_t>(key) + 0x9e3779b97f4a7c15ULL;
      x = (x ^ (x >> 30U)) * 0xbf58476d1ce4e5b9ULL;
      x = (x ^ (x >> 27U)) * 0x94d049bb133111ebULL;
      return x ^ (x >> 31U);
    };
    const size_t merge_workers = std::min(
        locals.size(), ParallelWorkerCount(total_worker_groups / 8192));
    const bool partitionable_hash =
        !int_group_key &&
        std::ranges::all_of(locals, [](const FusedWorkerLocal& local) {
          return std::ranges::all_of(local.groups, [](const FusedGroup& group) {
            return group.has_key_hash;
          });
        });
    const bool partitionable_int = int_group_key && all_direct_group_keys;
    const bool partitionable =
        merge_workers > 1 && groups.empty() && all_direct_group_keys &&
        (raw_sum_fusion || raw_count_fusion || fusion->aggregates.empty()) &&
        (partitionable_hash || partitionable_int) &&
        std::getenv("TINYLAMB_DISABLE_RAW_PARTITION_MERGE") == nullptr;
    auto partition_charge =
        partitionable && total_worker_groups <= SIZE_MAX / 128
            ? TryReserveQueryMemory(total_worker_groups * 128)
            : nullptr;
    if (partition_charge) {
      struct GroupRef {
        FusedGroup* group;
        size_t rank;
      };
      struct MergedRef {
        GroupRef ref;
        size_t next;
      };
      const size_t local_count = locals.size();
      std::vector<size_t> rank_bases(local_count + 1, 0);
      for (size_t w = 0; w < local_count; ++w) {
        rank_bases[w + 1] = rank_bases[w] + locals[w].groups.size();
      }
      const auto group_partition = [&](const FusedGroup& group) -> size_t {
        if (int_group_key) {
          int64_t k = 0;
          if (!extract_single_int_key(group, &k)) {
            return 0;
          }
          return static_cast<size_t>(mix_int_partition(k) % merge_workers);
        }
        return (group.key_hash ^ (group.key_hash >> 32U)) % merge_workers;
      };
      std::vector<size_t> worker_part_counts(local_count * merge_workers, 0);
      RETURN_IF_FAIL(RunParallelTasks(local_count, [&](size_t w) {
        size_t* counts = worker_part_counts.data() + (w * merge_workers);
        for (const auto& group : locals[w].groups) {
          ++counts[group_partition(group)];
        }
      }));
      std::vector<size_t> worker_part_offsets(local_count * merge_workers, 0);
      std::vector<std::vector<GroupRef>> partitions(merge_workers);
      for (size_t p = 0; p < merge_workers; ++p) {
        size_t off = 0;
        for (size_t w = 0; w < local_count; ++w) {
          worker_part_offsets[(w * merge_workers) + p] = off;
          off += worker_part_counts[(w * merge_workers) + p];
        }
        partitions[p].resize(off);
      }
      RETURN_IF_FAIL(RunParallelTasks(local_count, [&](size_t w) {
        size_t* offsets_w = worker_part_offsets.data() + (w * merge_workers);
        const size_t base_rank = rank_bases[w];
        auto& wgroups = locals[w].groups;
        for (size_t i = 0; i < wgroups.size(); ++i) {
          auto& group = wgroups[i];
          const size_t p = group_partition(group);
          partitions[p][offsets_w[p]++] = {.group = &group,
                                           .rank = base_rank + i};
        }
      }));
      std::vector<std::vector<MergedRef>> merged(merge_workers);
      std::vector<FusedGroup*> first_groups(total_worker_groups, nullptr);
      const bool exact_packed_hash =
          use_split_key_gid || packed_2x32_group_key || use_left_packed_key;
      RETURN_IF_FAIL(RunParallelTasks(merge_workers, [&](size_t p) {
        FlatIntKeyMap<size_t> index;
        index.Reserve(partitions[p].size());
        GroupKeyMap fallback_offsets;
        auto& output = merged[p];
        output.reserve(partitions[p].size());
        for (const auto& ref : partitions[p]) {
          bool inserted = false;
          size_t match = SIZE_MAX;
          size_t next = SIZE_MAX;
          size_t* head = nullptr;
          if (int_group_key) {
            int64_t k = 0;
            if (extract_single_int_key(*ref.group, &k)) {
              head = index.TryEmplace(k, &inserted);
              if (!inserted) {
                match = *head;
              }
            } else {
              ensure_group_key(*ref.group);
              auto [it, ins] =
                  fallback_offsets.emplace(ref.group->key, output.size());
              inserted = ins;
              if (!ins) {
                match = it->second;
              }
            }
          } else {
            head = index.TryEmplace(std::bit_cast<int64_t>(ref.group->key_hash),
                                    &inserted);
            if (!inserted) {
              if (exact_packed_hash) {
                match = *head;
              } else {
                for (size_t i = *head; i != SIZE_MAX; i = output[i].next) {
                  if (same_group_key(*output[i].ref.group, *ref.group)) {
                    match = i;
                    break;
                  }
                }
              }
            }
            next = inserted ? SIZE_MAX : *head;
          }
          if (match == SIZE_MAX) {
            if (head != nullptr) {
              *head = output.size();
            }
            output.push_back({.ref = ref, .next = next});
            first_groups[ref.rank] = ref.group;
          } else if (raw_count_fusion) {
            output[match].ref.group->raw_count += ref.group->raw_count;
          } else if (raw_sum_fusion) {
            merge_raw(*output[match].ref.group, *ref.group);
          }
        }
      }));
      // Preserve the historical first-worker/first-group emission order as
      // well as the accumulation order within each group (including FLOAT
      // prefixes). Keep FusedGroup storage in worker locals until after
      // emission so each worker frees its own groups in parallel without
      // move-copying structs.
      std::vector<size_t> chunk_counts(merge_workers, 0);
      RETURN_IF_FAIL(RunParallelTasks(merge_workers, [&](size_t c) {
        const size_t begin = (total_worker_groups * c) / merge_workers;
        const size_t end = (total_worker_groups * (c + 1)) / merge_workers;
        size_t cnt = 0;
        for (size_t i = begin; i < end; ++i) {
          if (first_groups[i] != nullptr) {
            ++cnt;
          }
        }
        chunk_counts[c] = cnt;
      }));
      std::vector<size_t> chunk_offsets(merge_workers, 0);
      size_t unique_groups = 0;
      for (size_t c = 0; c < merge_workers; ++c) {
        chunk_offsets[c] = unique_groups;
        unique_groups += chunk_counts[c];
      }
      group_ptrs.resize(unique_groups);
      use_group_ptrs = true;
      RETURN_IF_FAIL(RunParallelTasks(merge_workers, [&](size_t c) {
        const size_t begin = (total_worker_groups * c) / merge_workers;
        const size_t end = (total_worker_groups * (c + 1)) / merge_workers;
        size_t out = chunk_offsets[c];
        for (size_t i = begin; i < end; ++i) {
          if (first_groups[i] != nullptr) {
            group_ptrs[out++] = first_groups[i];
          }
        }
      }));
      if (locals.size() > 1) {
        RETURN_IF_FAIL(RunParallelTasks(locals.size(), [&](size_t w) {
          locals[w].offsets = GroupKeyMap();
          locals[w].int_offsets = FlatIntKeyMap<size_t>();
          std::vector<CompactCountGroup>().swap(locals[w].compact_counts);
        }));
      }
      // Group structs and raw state blocks remain owned by locals until
      // emission completes.
      total_worker_groups = 0;
      if (result.runtime() != nullptr) {
        ++result.runtime()->partitioned_aggregate_merges;
      }
    }
  }
  if (!use_group_ptrs) {
    if (total_worker_groups > 0) {
      groups.reserve(total_worker_groups);
    }
    for (FusedWorkerLocal& local : locals) {
      if (groups.empty() && offsets.empty() && local.offsets.empty() &&
          !local.groups.empty()) {
        groups = std::move(local.groups);
        int_merge_offsets = std::move(local.int_offsets);
        if (!skip_accumulator_states) {
          states = std::move(local.states);
        } else {
          local.states.clear();
        }
        if (total_worker_groups > groups.size()) {
          int_merge_offsets.Reserve(total_worker_groups);
          groups.reserve(total_worker_groups);
        }
        continue;
      }
      if (groups.empty() && total_worker_groups > 0) {
        int_merge_offsets.Reserve(total_worker_groups);
      }
      for (FusedGroup& group : local.groups) {
        bool inserted = false;
        size_t target_idx = 0;
        Value single_int_tmp;
        const Value* single_int_cell = nullptr;
        if (int_group_key) {
          if (group.key.values_.size() == 1) {
            single_int_cell = group.key.values_.data();
          } else if (all_direct_group_keys &&
                     (group.rep_left != nullptr ||
                      group.rep_col_chunk !=
                          std::numeric_limits<uint32_t>::max() ||
                      group.rep_right_idx != kInvalidRightIdx)) {
            single_int_cell =
                &read_group_pair_cell(group, int_group_slot, &single_int_tmp);
          }
        }
        if (single_int_cell != nullptr && !single_int_cell->IsNull() &&
            single_int_cell->type == int_group_type &&
            !single_int_cell->IsUnsigned()) {
          size_t* slot = int_merge_offsets.TryEmplace(
              single_int_cell->value.int_value, &inserted);
          if (inserted) {
            *slot = groups.size();
          }
          target_idx = *slot;
        } else if (!int_group_key && group.has_key_hash) {
          bool hash_inserted = false;
          size_t* slot = int_merge_offsets.TryEmplace(
              static_cast<int64_t>(group.key_hash), &hash_inserted);
          if (hash_inserted) {
            *slot = groups.size();
            inserted = true;
            target_idx = *slot;
            // Split keys pack an exact dictionary ID and a signed 32-bit
            // probe value. Their integer slots are collision-free; fallback
            // keys live exclusively in offsets and never enter this map.
          } else if (use_split_key_gid || packed_2x32_group_key ||
                     use_left_packed_key ||
                     same_group_key(groups[*slot], group)) [[likely]] {
            inserted = false;
            target_idx = *slot;
          } else {
            ensure_group_key(groups[*slot]);
            ensure_group_key(group);
            auto [iter, ins] = offsets.emplace(group.key, groups.size());
            inserted = ins;
            target_idx = iter->second;
          }
        } else {
          ensure_group_key(group);
          auto [iter, ins] = offsets.emplace(group.key, groups.size());
          inserted = ins;
          target_idx = iter->second;
        }
        if (inserted) {
          if (!skip_accumulator_states) {
            const size_t local_offset = group.state_offset;
            for (size_t i = 0; i < fusion->aggregates.size(); ++i) {
              states.push_back(std::move(local.states[local_offset + i]));
            }
            group.state_offset = states.size() - fusion->aggregates.size();
          }
          groups.push_back(std::move(group));
        } else if (raw_count_fusion) {
          groups[target_idx].raw_count += group.raw_count;
        } else if (raw_sum_fusion) {
          merge_raw(groups[target_idx], group);
        } else {
          FusedGroup& destination = groups[target_idx];
          for (size_t i = 0; i < fusion->aggregates.size(); ++i) {
            AggregateAccumulator& target = states[destination.state_offset + i];
            RETURN_IF_FAIL(target.Merge(local.states[group.state_offset + i]));
          }
        }
      }
      local.groups.clear();
      local.states.clear();
    }
  }
  // A keyless (global) aggregate over an empty join output still emits one
  // row: accumulators finish to their empty-input values (SUM -> NULL,
  // COUNT -> 0), exactly like the scalar AggregateExecutor.  Without this the
  // fused shape would drop the row entirely.
  if (fusion->group_keys.empty() && groups.empty() && !use_group_ptrs) {
    FusedGroup group;
    if (raw_sum_fusion) {
      merged_raw_arena.Initialize(group, raw_slot_count);
    }
    if (!skip_accumulator_states) {
      group.state_offset = 0;
      for (size_t i = 0; i < fusion->aggregates.size(); ++i) {
        states.emplace_back(fusion->aggregates[i], aggregate_shapes[i]);
      }
    }
    group.representative = std::make_unique<Row>(
        std::vector<Value>(result.schema.ColumnCount(), Value()));
    groups.push_back(std::move(group));
  }
  const auto active_group_count = [&]() noexcept -> size_t {
    return use_group_ptrs ? group_ptrs.size() : groups.size();
  };
  const auto active_group_at = [&](size_t idx) noexcept -> const FusedGroup& {
    return use_group_ptrs ? *group_ptrs[idx] : groups[idx];
  };

  // Emit: finish the accumulators into an AggregateResultMap keyed by the
  // aggregate node, then evaluate the select list against the representative
  // row — the same contract as Project's generic grouped emit.
  std::vector<Column> output_columns;
  output_columns.reserve(fusion->select_items.size());
  for (const std::string& name : fusion->select_names) {
    output_columns.emplace_back(name, ValueType::kNull);
  }
  struct DirectEmitItem {
    bool is_agg{false};
    size_t index{0};
    std::optional<Value> constant{std::nullopt};
    std::optional<BytecodeProgram> program{std::nullopt};
  };
  std::vector<DirectEmitItem> direct_emit;
  std::vector<slot_t> emit_needed_cols;
  std::vector<size_t> emit_needed_aggs;
  bool has_emit_program = false;
  size_t emit_row_width = result.schema.ColumnCount();
  std::optional<BytecodeProgram> having_prog;
  std::vector<slot_t> having_cols;
  std::vector<size_t> having_aggs;
  size_t having_row_width = result.schema.ColumnCount();
  bool can_direct_emit = result.using_columns == nullptr;
  if (can_direct_emit) {
    const auto static_agg_type = [&](size_t ai) -> std::optional<ValueType> {
      if (raw_count_fusion) {
        return ValueType::kInt64;
      }
      if (raw_sum_fusion) {
        switch (raw_agg_kinds[ai]) {
          case RawAggKind::kCountStar:
          case RawAggKind::kCountValue:
          case RawAggKind::kCountDistinctInt64:
          case RawAggKind::kSumInt64:
            return ValueType::kInt64;
          case RawAggKind::kSumDouble:
          case RawAggKind::kAvgDouble:
          case RawAggKind::kStddevSamp:
          case RawAggKind::kStddevPop:
          case RawAggKind::kVarSamp:
          case RawAggKind::kVarPop:
            return ValueType::kDouble;
          case RawAggKind::kConstantExtremum:
            if (!raw_constants[ai].IsNull()) {
              return raw_constants[ai].type;
            }
            if (fusion->aggregates[ai]->Child()) {
              const TypeTag ct = fusion->aggregates[ai]
                                     ->Child()
                                     ->ResultType(result.schema)
                                     .GetType();
              return ct == TypeTag::kDouble ? ValueType::kDouble
                                            : ValueType::kInt64;
            }
            return ValueType::kDouble;
          default:
            return std::nullopt;
        }
      }
      switch (fusion->aggregates[ai]->GetType()) {
        case AggregationType::kCount:
          return ValueType::kInt64;
        case AggregationType::kAvg:
        case AggregationType::kStddevSamp:
        case AggregationType::kStddevPop:
        case AggregationType::kVarSamp:
        case AggregationType::kVarPop:
          return ValueType::kDouble;
        default:
          return std::nullopt;
      }
    };
    std::optional<Schema> emit_schema_opt;
    std::vector<std::string> agg_col_names;
    const auto ensure_emit_schema = [&]() -> const Schema* {
      if (!emit_schema_opt.has_value()) {
        std::vector<Column> cols;
        cols.reserve(result.schema.ColumnCount() + fusion->aggregates.size());
        for (size_t ci = 0; ci < result.schema.ColumnCount(); ++ci) {
          cols.push_back(result.schema.GetColumn(ci));
        }
        agg_col_names.resize(fusion->aggregates.size());
        for (size_t ai = 0; ai < fusion->aggregates.size(); ++ai) {
          const auto t = static_agg_type(ai);
          if (!t.has_value()) {
            return nullptr;
          }
          std::string nm = "__fused_agg_" + std::to_string(ai);
          if (LocalColumnOffset(result.schema, ColumnName(nm)).has_value()) {
            return nullptr;
          }
          agg_col_names[ai] = nm;
          cols.emplace_back(std::move(nm), *t);
        }
        emit_schema_opt = Schema("", std::move(cols));
        emit_row_width = emit_schema_opt->ColumnCount();
      }
      return &*emit_schema_opt;
    };
    const std::function<Expression(const Expression&, std::vector<slot_t>*,
                                   std::vector<size_t>*)>
        rewrite_for_emit = [&](const Expression& expr,
                               std::vector<slot_t>* used_cols,
                               std::vector<size_t>* used_aggs) -> Expression {
      if (!expr || ContainsQuery(expr) || ContainsParameterSlot(expr)) {
        return {};
      }
      if (expr->Type() == TypeTag::kConstantValue) {
        return expr;
      }
      if (expr->Type() == TypeTag::kColumnValue) {
        const auto off = LocalColumnOffset(
            result.schema, expr->AsColumnValue().GetColumnName());
        if (!off.has_value()) {
          return {};
        }
        used_cols->push_back(static_cast<slot_t>(*off));
        return expr;
      }
      if (expr->Type() == TypeTag::kAggregateExp) {
        const auto* agg_ptr = &expr->AsAggregateExpression();
        auto found = std::ranges::find(fusion->aggregates, agg_ptr);
        if (found == fusion->aggregates.end()) {
          return {};
        }
        const auto ai = static_cast<size_t>(
            std::distance(fusion->aggregates.begin(), found));
        if (raw_sum_fusion && raw_const_folded[ai]) {
          return ConstantValueExp(raw_constants[ai]);
        }
        if (!static_agg_type(ai).has_value() ||
            ensure_emit_schema() == nullptr) {
          return {};
        }
        used_aggs->push_back(ai);
        return ColumnValueExp(agg_col_names[ai]);
      }
      if (expr->Type() == TypeTag::kUnaryExp) {
        auto child = rewrite_for_emit(expr->AsUnaryExpression().Child(),
                                      used_cols, used_aggs);
        if (!child) {
          return {};
        }
        return UnaryExpressionExp(std::move(child),
                                  expr->AsUnaryExpression().Op());
      }
      if (expr->Type() == TypeTag::kBinaryExp) {
        const auto& bin = expr->AsBinaryExpression();
        auto lhs = rewrite_for_emit(bin.Left(), used_cols, used_aggs);
        auto rhs = rewrite_for_emit(bin.Right(), used_cols, used_aggs);
        if (!lhs || !rhs) {
          return {};
        }
        return BinaryExpressionExp(std::move(lhs), bin.Op(), std::move(rhs));
      }
      return {};
    };
    direct_emit.reserve(fusion->select_items.size());
    for (const Expression& item : fusion->select_items) {
      if (!item) {
        can_direct_emit = false;
        break;
      }
      if (item->Type() == TypeTag::kConstantValue &&
          !ContainsParameterSlot(item)) {
        direct_emit.push_back({.constant = item->AsConstantValue().GetValue()});
      } else if (item->Type() == TypeTag::kColumnValue) {
        const auto col_off = LocalColumnOffset(
            result.schema, item->AsColumnValue().GetColumnName());
        if (!col_off.has_value()) {
          can_direct_emit = false;
          break;
        }
        direct_emit.push_back({.is_agg = false, .index = *col_off});
      } else if (item->Type() == TypeTag::kAggregateExp) {
        const auto* agg_ptr = &item->AsAggregateExpression();
        auto found = std::ranges::find(fusion->aggregates, agg_ptr);
        if (found == fusion->aggregates.end()) {
          can_direct_emit = false;
          break;
        }
        const auto ai = static_cast<size_t>(
            std::distance(fusion->aggregates.begin(), found));
        if (raw_sum_fusion && raw_const_folded[ai]) {
          direct_emit.push_back({.constant = raw_constants[ai]});
        } else {
          direct_emit.push_back({.is_agg = true, .index = ai});
        }
      } else {
        std::vector<slot_t> cols_used;
        std::vector<size_t> aggs_used;
        Expression rw = rewrite_for_emit(item, &cols_used, &aggs_used);
        const Schema* es = rw ? ensure_emit_schema() : nullptr;
        std::optional<BytecodeProgram> prog =
            es != nullptr ? BytecodeCompiler::CompileRowCached(
                                rw, *es, BytecodeCompiler::Context::kValue)
                          : std::nullopt;
        if (!prog.has_value()) {
          can_direct_emit = false;
          break;
        }
        emit_needed_cols.insert(emit_needed_cols.end(), cols_used.begin(),
                                cols_used.end());
        emit_needed_aggs.insert(emit_needed_aggs.end(), aggs_used.begin(),
                                aggs_used.end());
        has_emit_program = true;
        DirectEmitItem di;
        di.program = std::move(prog);
        direct_emit.push_back(std::move(di));
      }
    }
    if (has_emit_program) {
      std::ranges::sort(emit_needed_cols);
      emit_needed_cols.erase(
          std::unique(emit_needed_cols.begin(), emit_needed_cols.end()),
          emit_needed_cols.end());
      std::ranges::sort(emit_needed_aggs);
      emit_needed_aggs.erase(
          std::unique(emit_needed_aggs.begin(), emit_needed_aggs.end()),
          emit_needed_aggs.end());
    }
    if (can_direct_emit && fusion->having) {
      Expression rw_having =
          rewrite_for_emit(fusion->having, &having_cols, &having_aggs);
      const Schema* es = rw_having ? ensure_emit_schema() : nullptr;
      having_prog = es != nullptr
                        ? BytecodeCompiler::CompileRowCached(
                              rw_having, *es, BytecodeCompiler::Context::kValue)
                        : std::nullopt;
      if (!having_prog.has_value()) {
        can_direct_emit = false;
      } else {
        having_row_width = emit_row_width;
        std::ranges::sort(having_cols);
        having_cols.erase(std::unique(having_cols.begin(), having_cols.end()),
                          having_cols.end());
        std::ranges::sort(having_aggs);
        having_aggs.erase(std::unique(having_aggs.begin(), having_aggs.end()),
                          having_aggs.end());
      }
    }
  }
  const auto finish_raw_agg = [&](const FusedGroup& group,
                                  size_t i) -> StatusOr<Value> {
    if (raw_const_folded[i]) {
      return raw_constants[i];
    }
    const auto slot =
        static_cast<size_t>(static_cast<unsigned char>(raw_agg_slot[i]));
    if (group.RawInfFlags()[slot] && slot_raises_inf[slot]) {
      return StatusError(StatusCode::kIsInfinity, "infinity");
    }
    switch (raw_agg_kinds[i]) {
      case RawAggKind::kConstantExtremum:
        return group.RawCounts()[slot] == 0 ? Value() : raw_constants[i];
      case RawAggKind::kCountStar:
      case RawAggKind::kCountValue:
        return Value(group.RawCounts()[slot]);
      case RawAggKind::kSumInt64:
        return group.RawCounts()[slot] == 0 ? Value()
                                            : Value(group.RawInts()[slot]);
      case RawAggKind::kSumDouble:
        return group.RawCounts()[slot] == 0 ? Value()
                                            : Value(group.RawDoubles()[slot]);
      case RawAggKind::kAvgDouble:
        return group.RawCounts()[slot] == 0
                   ? Value()
                   : Value(group.RawDoubles()[slot] /
                           static_cast<double>(group.RawCounts()[slot]));
      case RawAggKind::kStddevSamp:
      case RawAggKind::kStddevPop:
      case RawAggKind::kVarSamp:
      case RawAggKind::kVarPop: {
        const int64_t n = group.RawCounts()[slot];
        const bool is_samp = raw_agg_kinds[i] == RawAggKind::kStddevSamp ||
                             raw_agg_kinds[i] == RawAggKind::kVarSamp;
        if (n < (is_samp ? 2 : 1)) {
          return Value();
        }
        const platform::ExtendedFloat mean = group.RawStatX()[slot] / n;
        const platform::ExtendedFloat ssd =
            group.RawStatXX()[slot] - (n * mean * mean);
        const platform::ExtendedFloat variance = ssd / (is_samp ? n - 1 : n);
        const bool is_stddev = raw_agg_kinds[i] == RawAggKind::kStddevSamp ||
                               raw_agg_kinds[i] == RawAggKind::kStddevPop;
        return Value(static_cast<double>(is_stddev ? platform::Sqrt(variance)
                                                   : variance));
      }
      case RawAggKind::kCountDistinctInt64:
        return Value(static_cast<int64_t>(
            group.raw_int_set ? group.raw_int_set->Size()
                              : static_cast<size_t>(group.RawCounts()[slot])));
      case RawAggKind::kNone:
        break;
    }
    return Value();
  };
  const auto try_unpack_key_cell = [&](const FusedGroup& grp, size_t col_idx,
                                       bool own_pk_strings,
                                       Value* out) -> bool {
    if (packed_2x32_unpack_only && grp.has_key_hash) {
      if (col_idx == packed_2x32_slot0) {
        const auto v0 =
            static_cast<int64_t>(static_cast<int32_t>(grp.key_hash >> 32));
        *out = packed_2x32_type0 == ValueType::kDate ? Value::DateFromDays(v0)
                                                     : Value(v0);
        return true;
      }
      if (col_idx == packed_2x32_slot1) {
        const auto v1 = static_cast<int64_t>(
            static_cast<int32_t>(grp.key_hash & 0xffffffffULL));
        *out = packed_2x32_type1 == ValueType::kDate ? Value::DateFromDays(v1)
                                                     : Value(v1);
        return true;
      }
    }
    if (split_key_unpack_only && grp.has_key_hash) {
      if (split_has_left_probe && split_left_slot.has_value() &&
          col_idx == *split_left_slot) {
        const auto v1 = static_cast<int64_t>(
            static_cast<int32_t>(grp.key_hash & 0xffffffffULL));
        *out = split_left_type == ValueType::kDate ? Value::DateFromDays(v1)
                                                   : Value(v1);
        return true;
      }
      if (col_idx < left_width && col_idx < 16 &&
          def_col_to_pk_lookup[col_idx] >= 0) {
        const auto li = static_cast<size_t>(
            static_cast<unsigned char>(def_col_to_pk_lookup[col_idx]));
        const PreparedDeferredPkGroup& pk_grp =
            def_pk_groups[prepared_pk_lookups[li].group_idx];
        if (pk_grp.use_subgid) {
          const uint32_t p_card = pk_grp.subgid_count;
          const uint64_t mask =
              p_card <= 1
                  ? 0ULL
                  : ((uint64_t{1} << std::bit_width(p_card - 1)) - 1ULL);
          const auto sg = static_cast<uint32_t>(
              (grp.key_hash >> pk_grp.subgid_shift) & mask);
          const auto [nb, rin] = pk_grp.subgid_rep_ptr[sg];
          read_pk_cell(li, nb, rin, out);
          if (own_pk_strings && out->type == ValueType::kVarChar &&
              !out->IsNull()) {
            *out = Value(std::string(out->value.varchar_value));
          }
          return true;
        }
      }
    }
    if (left_pack_unpack_only && grp.has_key_hash) {
      for (const LeftPackedIntCol& lpi : left_pack_ints) {
        if (lpi.col == col_idx) {
          const uint64_t code = (grp.key_hash >> lpi.shift) & lpi.mask;
          if (code == lpi.null_code) {
            *out = Value();
          } else {
            const auto iv =
                static_cast<int64_t>(static_cast<uint64_t>(lpi.min_val) + code);
            *out = lpi.type == ValueType::kDate ? Value::DateFromDays(iv)
                                                : Value(iv);
          }
          return true;
        }
      }
      if (left_pack_uses_dict) {
        for (const size_t dc : left_pack_dict_cols) {
          if (dc == col_idx) {
            const auto d_id = static_cast<uint32_t>(
                (grp.key_hash >> left_pack_dict_shift) & left_pack_dict_mask);
            if (left_col_only) {
              const auto [ci, r] = left_pack_dict_rep_col_loc[d_id];
              left.columnar->chunks[ci].columns[col_idx].WriteValueInto(r, out);
              if (own_pk_strings && out->type == ValueType::kVarChar &&
                  !out->IsNull()) {
                *out = Value(std::string(out->value.varchar_value));
              }
            } else {
              *out = left.rows[left_pack_dict_rep_rows[d_id]].values_[col_idx];
            }
            return true;
          }
        }
      }
      if (left_pack_uses_right && split_right_rep_ptr != nullptr) {
        const auto r_subgid = static_cast<uint32_t>(
            (grp.key_hash >> split_right_shift) & left_pack_right_mask);
        const uint32_t rep_idx = split_right_rep_ptr[r_subgid];
        if (col_idx >= left_width) {
          const Value& v = read_right_cell(rep_idx, col_idx - left_width, out);
          if (&v != out) {
            *out = v;
          }
          if (own_pk_strings && out->type == ValueType::kVarChar &&
              !out->IsNull() && out->owned_data.empty()) {
            *out = Value(std::string(out->value.varchar_value));
          }
          return true;
        }
        if (join_type == JoinType::kInner) {
          for (const EqualityKey& eq : equality_keys) {
            if (eq.left == col_idx && eq.left_offset == 0 &&
                eq.right_offset == 0 && !eq.null_safe) {
              const Value& v = read_right_cell(rep_idx, eq.right, out);
              if (&v != out) {
                *out = v;
              }
              if (own_pk_strings && out->type == ValueType::kVarChar &&
                  !out->IsNull() && out->owned_data.empty()) {
                *out = Value(std::string(out->value.varchar_value));
              }
              return true;
            }
          }
        }
      }
    }
    return false;
  };
  const auto rep_cell = [&](const FusedGroup& grp, size_t col_idx,
                            Value* col_scratch) -> const Value& {
    if (grp.representative != nullptr) {
      return grp.representative->values_[col_idx];
    }
    if (grp.rep_left == nullptr &&
        grp.rep_col_chunk == std::numeric_limits<uint32_t>::max() &&
        try_unpack_key_cell(grp, col_idx, false, col_scratch)) {
      return *col_scratch;
    }
    if (col_idx < left_width && grp.rep_left == nullptr &&
        grp.rep_col_chunk != std::numeric_limits<uint32_t>::max()) {
      left.columnar->chunks[grp.rep_col_chunk].columns[col_idx].WriteValueInto(
          grp.rep_col_row, col_scratch);
      return *col_scratch;
    }
    if (col_idx < left_width && grp.rep_left == nullptr &&
        grp.rep_right_idx != kInvalidRightIdx) {
      for (const EqualityKey& eq : equality_keys) {
        if (eq.left == col_idx && eq.left_offset == 0 && eq.right_offset == 0 &&
            !eq.null_safe) {
          return read_right_cell(grp.rep_right_idx, eq.right, col_scratch);
        }
      }
      return kNullCell;
    }
    if (col_idx < left_width) {
      return grp.rep_left != nullptr ? grp.rep_left->values_[col_idx]
                                     : kNullCell;
    }
    return read_right_cell(grp.rep_right_idx, col_idx - left_width,
                           col_scratch);
  };
  const auto direct_key_value = [&](const FusedGroup& grp, size_t col_idx,
                                    bool own_pk_strings = true) -> Value {
    Value col_tmp;
    if (try_unpack_key_cell(grp, col_idx, own_pk_strings, &col_tmp)) {
      return CanonicalDistinctValue(col_tmp);
    }
    return CanonicalDistinctValue(rep_cell(grp, col_idx, &col_tmp));
  };
  // Reject count groups before materializing their representative and output
  // row. A selective HAVING can discard almost every joined group.
  if (fusion->post_having_active && !fusion->post_having_is_self_sum_factor &&
      fusion->post_having_agg_slot < fusion->aggregates.size()) {
    const slot_t orig_slot = fusion->post_having_agg_slot;
    const bool count_only = raw_count_fusion && orig_slot == 0;
    const bool count_slot =
        raw_sum_fusion && (raw_agg_kinds[orig_slot] == RawAggKind::kCountStar ||
                           raw_agg_kinds[orig_slot] == RawAggKind::kCountValue);
    if (count_only || count_slot) {
      const size_t slot =
          count_only ? 0 : static_cast<size_t>(raw_agg_slot[orig_slot]);
      const double threshold = fusion->post_having_constant_or_factor;
      const auto rejected = [&](const FusedGroup& group) {
        const auto count = static_cast<double>(
            count_only ? group.raw_count : group.RawCounts()[slot]);
        switch (fusion->post_having_op) {
          case BinaryOperation::kEquals:
            return !(count == threshold);
          case BinaryOperation::kNotEquals:
            return !(count != threshold);
          case BinaryOperation::kLessThan:
            return !(count < threshold);
          case BinaryOperation::kLessThanEquals:
            return !(count <= threshold);
          case BinaryOperation::kGreaterThan:
            return !(count > threshold);
          case BinaryOperation::kGreaterThanEquals:
            return !(count >= threshold);
          default:
            return false;
        }
      };
      // Compact once in encounter order. Keeping this loop explicit avoids
      // forcing the library remove_if search loop through an unroll request
      // that ThinLTO cannot satisfy for this large group state.
      size_t retained = 0;
      if (use_group_ptrs) {
        for (size_t index = 0; index < group_ptrs.size(); ++index) {
          if (!rejected(*group_ptrs[index])) {
            group_ptrs[retained++] = group_ptrs[index];
          }
        }
        group_ptrs.resize(retained);
      } else {
        for (size_t index = 0; index < groups.size(); ++index) {
          if (!rejected(groups[index])) {
            if (retained != index) {
              groups[retained] = std::move(groups[index]);
            }
            ++retained;
          }
        }
        groups.erase(groups.begin() + static_cast<std::ptrdiff_t>(retained),
                     groups.end());
      }
      fusion->post_having_applied = true;
    }
  }
  phase_merge_done = std::chrono::steady_clock::now();
  if (fusion->post_having_active && raw_sum_fusion &&
      fusion->post_having_agg_slot < fusion->aggregates.size()) {
    const slot_t orig_agg_slot = fusion->post_having_agg_slot;
    const RawAggKind akind = raw_agg_kinds[orig_agg_slot];
    if (raw_agg_slot[orig_agg_slot] >= 0 &&
        (akind == RawAggKind::kSumDouble || akind == RawAggKind::kSumInt64)) {
      const auto agg_slot = static_cast<slot_t>(
          static_cast<unsigned char>(raw_agg_slot[orig_agg_slot]));
      bool any_inf = false;
      const size_t group_count = active_group_count();
      for (size_t gi = 0; gi < group_count; ++gi) {
        if (active_group_at(gi).RawInfFlags()[agg_slot] != 0) {
          any_inf = true;
          break;
        }
      }
      // INT64 sums compare as doubles below; that is only exact while every
      // group sum and the grand total stay within +-2^53. Otherwise leave
      // HAVING to the canonical integer comparison (post_having_applied
      // stays false).
      if (!any_inf && akind == RawAggKind::kSumInt64) {
        constexpr platform::Int128 kExactLimit = platform::Int128{1} << 53;
        platform::Int128 grand = 0;
        for (size_t gi = 0; gi < group_count; ++gi) {
          const FusedGroup& grp = active_group_at(gi);
          if (grp.RawCounts()[agg_slot] <= 0) {
            continue;
          }
          const platform::Int128 v = grp.RawInts()[agg_slot];
          grand += v;
          if (v > kExactLimit || v < -kExactLimit || grand > kExactLimit ||
              grand < -kExactLimit) {
            any_inf = true;  // Reuse the "skip the shortcut" exit.
            break;
          }
        }
      }
      if (!any_inf) {
        auto group_sum_val = [&](const FusedGroup& grp) noexcept -> double {
          return akind == RawAggKind::kSumDouble
                     ? grp.RawDoubles()[agg_slot]
                     : static_cast<double>(grp.RawInts()[agg_slot]);
        };
        double threshold = fusion->post_having_constant_or_factor;
        bool valid_threshold = true;
        if (fusion->post_having_is_self_sum_factor) {
          double grand_total = 0.0;
          bool any_nonnull = false;
          for (size_t gi = 0; gi < group_count; ++gi) {
            const FusedGroup& grp = active_group_at(gi);
            if (grp.RawCounts()[agg_slot] > 0) {
              any_nonnull = true;
              grand_total += group_sum_val(grp);
            }
          }
          if (!any_nonnull) {
            valid_threshold = false;
          } else {
            threshold = grand_total * fusion->post_having_constant_or_factor;
            if (!std::isfinite(threshold)) {
              valid_threshold = false;
            }
          }
        }
        if (valid_threshold) {
          size_t out = 0;
          for (size_t gi = 0; gi < group_count; ++gi) {
            const FusedGroup& grp = active_group_at(gi);
            if (grp.RawCounts()[agg_slot] <= 0) {
              continue;
            }
            const double val = group_sum_val(grp);
            bool pass = false;
            switch (fusion->post_having_op) {
              case BinaryOperation::kEquals:
                pass = (val == threshold);
                break;
              case BinaryOperation::kNotEquals:
                pass = (val != threshold);
                break;
              case BinaryOperation::kLessThan:
                pass = (val < threshold);
                break;
              case BinaryOperation::kLessThanEquals:
                pass = (val <= threshold);
                break;
              case BinaryOperation::kGreaterThan:
                pass = (val > threshold);
                break;
              case BinaryOperation::kGreaterThanEquals:
                pass = (val >= threshold);
                break;
              default:
                break;
            }
            if (pass) {
              if (use_group_ptrs) {
                group_ptrs[out++] = group_ptrs[gi];
              } else {
                if (out != gi) {
                  groups[out] = std::move(groups[gi]);
                }
                ++out;
              }
            }
          }
          if (use_group_ptrs) {
            group_ptrs.resize(out);
          } else {
            groups.resize(out);
          }
          fusion->post_having_applied = true;
        }
      }
    } else if (fusion->post_having_is_self_sum_factor &&
               active_group_count() == 0) {
      fusion->post_having_applied = true;
    }
  }
  bool direct_having_applied = false;
  if (can_direct_emit && having_prog.has_value() &&
      !fusion->post_having_applied) {
    const size_t group_count = active_group_count();
    if (group_count > 0) {
      const size_t having_workers = ParallelWorkerCount(group_count / 4096);
      std::vector<std::vector<uint32_t>> kept_shards(having_workers);
      std::vector<Status> having_errors(having_workers, Status::kSuccess);
      RETURN_IF_FAIL(RunParallelTasks(having_workers, [&](size_t hw) {
        const size_t begin = (group_count * hw) / having_workers;
        const size_t end = (group_count * (hw + 1)) / having_workers;
        std::vector<uint32_t>& kept = kept_shards[hw];
        kept.reserve(end - begin);
        Row eval_row;
        eval_row.values_.resize(having_row_width);
        for (size_t gi = begin; gi < end; ++gi) {
          const FusedGroup& grp = active_group_at(gi);
          if (raw_sum_fusion) {
            for (size_t s = 0; s < raw_slot_count; ++s) {
              if (grp.RawInfFlags()[s] != 0 && slot_raises_inf[s]) {
                having_errors[hw] =
                    StatusError(StatusCode::kIsInfinity, "infinity");
                return;
              }
            }
          }
          for (const slot_t col : having_cols) {
            eval_row.values_[col] = direct_key_value(grp, col, false);
          }
          for (const size_t ai : having_aggs) {
            if (raw_count_fusion) {
              eval_row.values_[left_width + right_width + ai] =
                  Value(grp.raw_count);
            } else if (raw_sum_fusion) {
              auto fin = finish_raw_agg(grp, ai);
              if (!fin.HasValue()) {
                having_errors[hw] = fin.GetStatus();
                return;
              }
              eval_row.values_[left_width + right_width + ai] =
                  std::move(fin.Value());
            } else {
              auto fin = states[grp.state_offset + ai].TryFinish();
              if (!fin.HasValue()) {
                having_errors[hw] = fin.GetStatus();
                return;
              }
              eval_row.values_[left_width + right_width + ai] =
                  std::move(fin.Value());
            }
          }
          Value hv;
          Status hst;
          const auto fast =
              having_prog->TryEvaluateRowFast(eval_row, &hv, &hst);
          if (fast == BytecodeProgram::FastEval::kError) {
            having_errors[hw] = std::move(hst);
            return;
          }
          if (fast == BytecodeProgram::FastEval::kUnsupported) {
            auto slow = having_prog->TryEvaluateRow(eval_row);
            if (!slow.HasValue()) {
              having_errors[hw] = slow.GetStatus();
              return;
            }
            hv = std::move(slow.Value());
          }
          if (Truthy(hv)) {
            kept.push_back(static_cast<uint32_t>(gi));
          }
        }
      }));
      for (const Status& err : having_errors) {
        RETURN_IF_FAIL(err);
      }
      size_t total_kept = 0;
      for (const auto& s : kept_shards) {
        total_kept += s.size();
      }
      if (use_group_ptrs) {
        std::vector<FusedGroup*> next_ptrs;
        next_ptrs.reserve(total_kept);
        for (const auto& shard : kept_shards) {
          for (const uint32_t idx : shard) {
            next_ptrs.push_back(group_ptrs[idx]);
          }
        }
        group_ptrs = std::move(next_ptrs);
      } else {
        std::vector<FusedGroup> next_groups;
        next_groups.reserve(total_kept);
        for (const auto& shard : kept_shards) {
          for (const uint32_t idx : shard) {
            next_groups.push_back(std::move(groups[idx]));
          }
        }
        groups = std::move(next_groups);
      }
    }
    direct_having_applied = true;
  }
  if (fusion->topn_limit > 0 && fusion->topn_limit < active_group_count() &&
      !fusion->output_order.empty() && can_direct_emit &&
      (raw_count_fusion || raw_sum_fusion)) {
    struct FastTopNTerm {
      size_t col_idx{0};
      SortKeyOrder order;
    };
    std::vector<FastTopNTerm> topn_terms;
    topn_terms.reserve(fusion->output_order.size());
    bool can_topn = true;
    const size_t group_count = active_group_count();
    if (raw_sum_fusion) {
      for (size_t gi = 0; gi < group_count; ++gi) {
        const FusedGroup& g = active_group_at(gi);
        for (size_t s = 0; s < raw_slot_count; ++s) {
          if (g.RawInfFlags()[s] != 0) {
            can_topn = false;
            break;
          }
        }
        if (!can_topn) {
          break;
        }
      }
    }
    for (const SelectStatement::OrderByTerm& term : fusion->output_order) {
      if (!can_topn || !term.expression) {
        can_topn = false;
        break;
      }
      std::optional<size_t> resolved_col;
      if (term.expression->Type() == TypeTag::kConstantValue) {
        const Value& cv = term.expression->AsConstantValue().GetValue();
        if (cv.type == ValueType::kInt64 && !cv.IsNull() &&
            cv.value.int_value >= 1 &&
            static_cast<size_t>(cv.value.int_value) <= direct_emit.size()) {
          resolved_col = static_cast<size_t>(cv.value.int_value - 1);
        }
      } else if (term.expression->Type() == TypeTag::kColumnValue) {
        const ColumnName& cn = term.expression->AsColumnValue().GetColumnName();
        if (cn.schema.empty()) {
          for (size_t si = 0; si < fusion->select_names.size(); ++si) {
            if (!fusion->select_names[si].empty() &&
                IdentifierEquals(fusion->select_names[si], cn.name)) {
              resolved_col = si;
              break;
            }
          }
        }
      }
      if (!resolved_col.has_value() || *resolved_col >= direct_emit.size()) {
        can_topn = false;
        break;
      }
      topn_terms.push_back(
          FastTopNTerm{.col_idx = *resolved_col,
                       .order = SortKeyOrder(term.ascending, term.nulls_first,
                                             Collation::kNone)});
    }
    if (can_topn && !topn_terms.empty()) {
      std::vector<size_t> group_order(group_count);
      for (size_t gi = 0; gi < group_count; ++gi) {
        group_order[gi] = gi;
      }
      const auto cmp_groups = [&](size_t ga, size_t gb) -> bool {
        const FusedGroup& a = active_group_at(ga);
        const FusedGroup& b = active_group_at(gb);
        for (const FastTopNTerm& t : topn_terms) {
          const DirectEmitItem& d = direct_emit[t.col_idx];
          int cmp = 0;
          if (d.constant) {
            continue;
          }
          if (!d.is_agg) {
            Value va;
            Value vb;
            cmp = t.order.Compare(rep_cell(a, d.index, &va),
                                  rep_cell(b, d.index, &vb));
          } else if (raw_count_fusion) {
            cmp = t.order.Compare(Value(a.raw_count), Value(b.raw_count));
          } else {
            auto fa = finish_raw_agg(a, d.index);
            auto fb = finish_raw_agg(b, d.index);
            cmp = t.order.Compare(fa.Value(), fb.Value());
          }
          if (cmp != 0) {
            return cmp < 0;
          }
        }
        return ga < gb;
      };
      std::partial_sort(
          group_order.begin(),
          group_order.begin() + static_cast<std::ptrdiff_t>(fusion->topn_limit),
          group_order.end(), cmp_groups);
      if (use_group_ptrs) {
        std::vector<FusedGroup*> top_ptrs;
        top_ptrs.reserve(fusion->topn_limit);
        for (size_t k = 0; k < fusion->topn_limit; ++k) {
          top_ptrs.push_back(group_ptrs[group_order[k]]);
        }
        group_ptrs = std::move(top_ptrs);
      } else {
        std::vector<FusedGroup> top_groups;
        top_groups.reserve(fusion->topn_limit);
        for (size_t k = 0; k < fusion->topn_limit; ++k) {
          top_groups.push_back(std::move(groups[group_order[k]]));
        }
        groups = std::move(top_groups);
      }
    }
  }
  if (!can_direct_emit ||
      (!fusion->columnar_output && fusion->output_sink == nullptr)) {
    result.rows.reserve(active_group_count());
  }
  if (track_group_phases) {
    phase_emit_begin = std::chrono::steady_clock::now();
    std::cerr << "[fusion] pre_emit_ms=" << ElapsedMs(join_begin)
              << " setup_ms="
              << std::chrono::duration<double, std::milli>(phase_probe_begin -
                                                           join_begin)
                     .count()
              << " index_ms="
              << std::chrono::duration<double, std::milli>(phase_buckets_done -
                                                           join_begin)
                     .count()
              << " states_ms="
              << std::chrono::duration<double, std::milli>(phase_states_done -
                                                           phase_buckets_done)
                     .count()
              << " probe_ms="
              << std::chrono::duration<double, std::milli>(phase_merge_begin -
                                                           phase_probe_begin)
                     .count()
              << " merge_ms="
              << std::chrono::duration<double, std::milli>(phase_merge_done -
                                                           phase_merge_begin)
                     .count()
              << " post_ms="
              << std::chrono::duration<double, std::milli>(phase_emit_begin -
                                                           phase_merge_done)
                     .count()
              << " groups=" << active_group_count()
              << " left=" << effective_left_rows << "\n";
  }
  const size_t total_out_groups = active_group_count();
  bool pushed_output = false;
  std::shared_ptr<QueryMemoryCharge> push_batch_charge;
  if (fusion->output_sink != nullptr && can_direct_emit && !has_emit_program &&
      (raw_count_fusion || raw_sum_fusion || fusion->aggregates.empty()) &&
      (!fusion->having || direct_having_applied ||
       fusion->post_having_applied) &&
      !fusion->post_having_active && fusion->topn_limit == 0 &&
      !fusion->rollup_count_distribution) {
    const size_t emit_workers = ParallelWorkerCount(total_out_groups / 8192);
    // Preserve the ordinary output's first-row schema, including all-null
    // columns. Actual cell tags remain in the batches; consumers must check
    // their own narrower representation before accepting a build.
    if (total_out_groups > 0) {
      const auto eval_sink_cell =
          [&](size_t gi, const DirectEmitItem& d) -> StatusOr<Value> {
        const FusedGroup& grp = active_group_at(gi);
        if (d.constant) {
          return *d.constant;
        }
        if (!d.is_agg) {
          return direct_key_value(grp, d.index);
        }
        if (raw_count_fusion) {
          return Value(grp.raw_count);
        }
        return finish_raw_agg(grp, d.index);
      };
      for (size_t c = 0; c < direct_emit.size(); ++c) {
        const auto& d = direct_emit[c];
        ASSIGN_OR_RETURN(Value, first_val, eval_sink_cell(0, d));
        std::optional<Value> sample;
        if (!first_val.IsNull()) {
          sample = std::move(first_val);
        } else if (!d.constant) {
          for (size_t gi = 1; gi < total_out_groups; ++gi) {
            ASSIGN_OR_RETURN(Value, cand, eval_sink_cell(gi, d));
            if (cand.IsNull()) {
              continue;
            }
            if (!sample.has_value()) {
              sample = std::move(cand);
              break;
            }
          }
          // Preserve homogeneous vs heterogeneous nullable type semantics
          // when group 0 was NULL by verifying remaining non-null values.
          if (sample.has_value()) {
            const ValueType expected_t = ValueTypeOf(*sample);
            const bool expected_u = sample->IsUnsigned();
            const bool expected_iv = sample->IsInterval();
            for (size_t gi = 1; gi < total_out_groups; ++gi) {
              ASSIGN_OR_RETURN(Value, cand, eval_sink_cell(gi, d));
              if (cand.IsNull()) {
                continue;
              }
              if (ValueTypeOf(cand) != expected_t ||
                  cand.IsUnsigned() != expected_u ||
                  cand.IsInterval() != expected_iv) {
                sample.reset();
                break;
              }
            }
          }
        }
        if (sample.has_value()) {
          output_columns[c] =
              Column(output_columns[c].Name(), ValueTypeOf(*sample));
          output_columns[c].SetUnsigned(sample->IsUnsigned());
          output_columns[c].SetInterval(sample->IsInterval());
        } else {
          output_columns[c] =
              Column(output_columns[c].Name(), ValueType::kNull);
        }
      }
    }
    push_batch_charge =
        TryReserveQueryMemory(emit_workers * GroupedOutputSink::kBatchRows *
                              direct_emit.size() * sizeof(Value));
    bool accepted = false;
    if (push_batch_charge) {
      Status begin_status = Status::kSuccess;
      RETURN_IF_FAIL(RunParallelTasks(1, [&](size_t) {
        auto begun = fusion->output_sink->begin(Schema("", output_columns),
                                                total_out_groups, emit_workers);
        begin_status = begun.GetStatus();
        if (begun.HasValue()) accepted = begun.Value();
      }));
      RETURN_IF_FAIL(begin_status);
    }
    if (accepted) {
      fusion->output_sink->used = true;
      pushed_output = true;
      std::vector<Status> errors(emit_workers, Status::kSuccess);
      RETURN_IF_FAIL(RunParallelTasks(emit_workers, [&](size_t worker) {
        const auto emit = [&]() -> Status {
          const size_t end = total_out_groups * (worker + 1) / emit_workers;
          const size_t width = direct_emit.size();
          std::vector<Value> batch(GroupedOutputSink::kBatchRows * width);
          for (size_t first = total_out_groups * worker / emit_workers;
               first < end; first += GroupedOutputSink::kBatchRows) {
            const size_t count =
                std::min(GroupedOutputSink::kBatchRows, end - first);
            for (size_t r = 0; r < count; ++r) {
              const auto& group = active_group_at(first + r);
              if (raw_sum_fusion) {
                for (size_t s = 0; s < raw_slot_count; ++s) {
                  if (group.RawInfFlags()[s] && slot_raises_inf[s]) {
                    return StatusError(StatusCode::kIsInfinity, "infinity");
                  }
                }
              }
              for (size_t c = 0; c < width; ++c) {
                const auto& d = direct_emit[c];
                Value& cell = batch[(r * width) + c];
                if (d.constant) {
                  cell = *d.constant;
                } else if (!d.is_agg) {
                  cell = direct_key_value(group, d.index);
                } else if (raw_count_fusion) {
                  cell = Value(group.raw_count);
                } else {
                  ASSIGN_OR_RETURN(Value, value,
                                   finish_raw_agg(group, d.index));
                  cell = std::move(value);
                }
              }
            }
            RETURN_IF_FAIL(fusion->output_sink->push(
                worker, first,
                std::span<const Value>(batch.data(), count * width)));
          }
          return Status::kSuccess;
        };
        errors[worker] = emit();
      }));
      for (const Status& error : errors) {
        RETURN_IF_FAIL(error);
      }
      if (auto* runtime = context.execution_runtime()) {
        runtime->pushed_group_rows += total_out_groups;
      }
    }
  }
  const size_t flush_granularity = Relation::ChargeFlushGranularity();
  const auto eval_direct_group =
      [&](const FusedGroup& group, const std::vector<uint8_t>* skip_cols,
          bool own_pk_strings, std::vector<Value>* finished_aggs, Row* eval_row,
          auto&& emit_cell_fn) -> Status {
    const auto prepare_eval_row = [&]() -> Status {
      if (!has_emit_program) {
        return Status::kSuccess;
      }
      for (const slot_t col : emit_needed_cols) {
        eval_row->values_[col] = direct_key_value(group, col, own_pk_strings);
      }
      for (const size_t ai : emit_needed_aggs) {
        if (raw_count_fusion) {
          eval_row->values_[left_width + right_width + ai] =
              Value(group.raw_count);
        } else if (raw_sum_fusion) {
          ASSIGN_OR_RETURN(Value, finished, finish_raw_agg(group, ai));
          eval_row->values_[left_width + right_width + ai] =
              std::move(finished);
        } else {
          eval_row->values_[left_width + right_width + ai] =
              (*finished_aggs)[ai];
        }
      }
      return Status::kSuccess;
    };
    const auto eval_direct_item =
        [&](const DirectEmitItem& d) -> StatusOr<Value> {
      Value v;
      Status eval_st;
      const auto fast = d.program->TryEvaluateRowFast(*eval_row, &v, &eval_st);
      if (fast == BytecodeProgram::FastEval::kError) {
        return eval_st;
      }
      if (fast == BytecodeProgram::FastEval::kUnsupported) {
        ASSIGN_OR_RETURN(Value, fallback_v,
                         d.program->TryEvaluateRow(*eval_row));
        v = std::move(fallback_v);
      }
      return v;
    };
    if (raw_count_fusion) {
      RETURN_IF_FAIL(prepare_eval_row());
      for (size_t c = 0; c < direct_emit.size(); ++c) {
        if (skip_cols != nullptr && (*skip_cols)[c] != 0) {
          continue;
        }
        const DirectEmitItem& d = direct_emit[c];
        if (d.program.has_value()) {
          ASSIGN_OR_RETURN(Value, v, eval_direct_item(d));
          emit_cell_fn(c, std::move(v));
        } else {
          emit_cell_fn(
              c, d.constant ? *d.constant
                 : d.is_agg ? Value(group.raw_count)
                            : direct_key_value(group, d.index, own_pk_strings));
        }
      }
      return Status::kSuccess;
    }
    if (raw_sum_fusion) {
      for (size_t s = 0; s < raw_slot_count; ++s) {
        if (group.RawInfFlags()[s] && slot_raises_inf[s]) {
          return StatusError(StatusCode::kIsInfinity, "infinity");
        }
      }
      RETURN_IF_FAIL(prepare_eval_row());
      for (size_t c = 0; c < direct_emit.size(); ++c) {
        if (skip_cols != nullptr && (*skip_cols)[c] != 0) {
          continue;
        }
        const DirectEmitItem& d = direct_emit[c];
        if (d.program.has_value()) {
          ASSIGN_OR_RETURN(Value, v, eval_direct_item(d));
          emit_cell_fn(c, std::move(v));
          continue;
        }
        if (d.constant || !d.is_agg) {
          emit_cell_fn(
              c, d.constant ? *d.constant
                            : direct_key_value(group, d.index, own_pk_strings));
          continue;
        }
        const size_t i = d.index;
        if (raw_const_folded[i]) {
          emit_cell_fn(c, raw_constants[i]);
          continue;
        }
        const auto slot =
            static_cast<size_t>(static_cast<unsigned char>(raw_agg_slot[i]));
        const int64_t count = group.RawCounts()[slot];
        switch (raw_agg_kinds[i]) {
          case RawAggKind::kCountStar:
          case RawAggKind::kCountValue:
            emit_cell_fn(c, Value(count));
            break;
          case RawAggKind::kSumInt64:
            emit_cell_fn(c,
                         count == 0 ? Value() : Value(group.RawInts()[slot]));
            break;
          case RawAggKind::kSumDouble:
          case RawAggKind::kAvgDouble:
            emit_cell_fn(
                c, count == 0 ? Value()
                              : Value(raw_agg_kinds[i] == RawAggKind::kAvgDouble
                                          ? group.RawDoubles()[slot] /
                                                static_cast<double>(count)
                                          : group.RawDoubles()[slot]));
            break;
          default: {
            ASSIGN_OR_RETURN(Value, finished, finish_raw_agg(group, i));
            emit_cell_fn(c, std::move(finished));
            break;
          }
        }
      }
      return Status::kSuccess;
    }
    finished_aggs->clear();
    for (size_t i = 0; i < fusion->aggregates.size(); ++i) {
      AggregateAccumulator& accumulator = states[group.state_offset + i];
      ASSIGN_OR_RETURN(Value, finished, accumulator.TryFinish());
      finished_aggs->push_back(std::move(finished));
    }
    RETURN_IF_FAIL(prepare_eval_row());
    for (size_t c = 0; c < direct_emit.size(); ++c) {
      if (skip_cols != nullptr && (*skip_cols)[c] != 0) {
        continue;
      }
      const DirectEmitItem& d = direct_emit[c];
      if (d.program.has_value()) {
        ASSIGN_OR_RETURN(Value, v, eval_direct_item(d));
        emit_cell_fn(c, std::move(v));
      } else {
        emit_cell_fn(c, d.constant ? *d.constant
                        : d.is_agg
                            ? (*finished_aggs)[d.index]
                            : direct_key_value(group, d.index, own_pk_strings));
      }
    }
    return Status::kSuccess;
  };
  const bool emit_own_pk_strings = context.execution_runtime() == nullptr;
  const auto emit_direct =
      [&](size_t begin, size_t end, Relation* output,
          std::vector<ColumnTypeTracker>* col_tracks) -> Status {
    if (!output->HasSpill() && output->rows.empty() && end > begin) {
      output->rows.reserve(end - begin);
    }
    std::vector<Value> finished_aggs;
    finished_aggs.reserve(fusion->aggregates.size());
    Row eval_row;
    if (has_emit_program) {
      eval_row.values_.resize(emit_row_width);
    }
    for (size_t index = begin; index < end; ++index) {
      const FusedGroup& group = active_group_at(index);
      Row output_row;
      output_row.values_.reserve(direct_emit.size());
      size_t row_bytes = 64;
      RETURN_IF_FAIL(eval_direct_group(
          group, nullptr, emit_own_pk_strings, &finished_aggs, &eval_row,
          [&](size_t c, Value cell) {
            if (col_tracks != nullptr && c < col_tracks->size()) {
              (*col_tracks)[c].Observe(cell);
            }
            row_bytes += cell.type == ValueType::kArray
                             ? EstimateValueBytes(cell)
                             : EstimateScalarValueBytes(cell);
            output_row.values_.push_back(std::move(cell));
          }));
      output->AddRowWithKnownBytes(std::move(output_row), row_bytes,
                                   flush_granularity);
    }
    RETURN_IF_FAIL(output->FinishSpill());
    return output->GetStatus();
  };
  const bool fast_col_emit =
      (raw_count_fusion || raw_sum_fusion || fusion->aggregates.empty()) &&
      !has_emit_program &&
      std::ranges::none_of(direct_emit, [](const DirectEmitItem& item) {
        return item.program.has_value();
      });
  const auto emit_direct_columnar =
      [&](size_t begin, size_t end, std::vector<ColumnarChunk>* out_chunks,
          std::vector<ColumnTypeTracker>* col_tracks) -> Status {
    if (end <= begin) {
      return Status::kSuccess;
    }
    std::vector<Value> finished_aggs;
    finished_aggs.reserve(fusion->aggregates.size());
    Row eval_row;
    if (has_emit_program) {
      eval_row.values_.resize(emit_row_width);
    }
    const auto track_uniform_col = [&](size_t c, const ColumnarColumn& col,
                                       bool non_null_seen) {
      if (!non_null_seen || col_tracks == nullptr || c >= col_tracks->size()) {
        return;
      }
      ColumnTypeTracker& ct = (*col_tracks)[c];
      if (!ct.seen_non_null) {
        ct.seen_non_null = true;
        ct.type = col.type;
        ct.is_unsigned = col.is_unsigned;
        ct.is_interval = col.is_interval;
      } else if (ct.type != col.type || ct.is_unsigned != col.is_unsigned ||
                 ct.is_interval != col.is_interval) {
        ct.mixed = true;
      }
    };
    const auto track_fallback_col = [&](size_t c, const ColumnarColumn& col) {
      if (col_tracks == nullptr || c >= col_tracks->size()) {
        return;
      }
      ColumnTypeTracker& ct = (*col_tracks)[c];
      for (const Value& v : col.fallback) {
        ct.Observe(v);
      }
    };
    out_chunks->reserve((end - begin + kColumnarChunkRows - 1) /
                        kColumnarChunkRows);
    std::vector<uint32_t> chunk_rch;
    std::vector<size_t> chunk_rr;
    for (size_t c_begin = begin; c_begin < end; c_begin += kColumnarChunkRows) {
      const size_t c_end = std::min(end, c_begin + kColumnarChunkRows);
      const size_t chunk_rows = c_end - c_begin;
      ColumnarChunk chunk;
      chunk.rows = chunk_rows;
      chunk.columns.resize(direct_emit.size());
      std::vector<uint8_t> is_const_col(direct_emit.size(), 0);
      for (size_t c = 0; c < direct_emit.size(); ++c) {
        const DirectEmitItem& d = direct_emit[c];
        if (!d.program.has_value() && d.constant.has_value() &&
            TrySetConstantColumnarColumn(&chunk.columns[c], ValueType::kNull,
                                         false, false, *d.constant, chunk_rows,
                                         &chunk)) {
          is_const_col[c] = 1;
          if (col_tracks != nullptr && c < col_tracks->size()) {
            (*col_tracks)[c].Observe(*d.constant);
          }
          continue;
        }
        ValueType init_vt = ValueType::kNull;
        bool init_unsigned = false;
        if (!d.program.has_value() && d.is_agg) {
          if (raw_count_fusion) {
            init_vt = ValueType::kInt64;
          } else if (raw_sum_fusion && d.index < raw_agg_kinds.size()) {
            switch (raw_agg_kinds[d.index]) {
              case RawAggKind::kCountStar:
              case RawAggKind::kCountValue:
              case RawAggKind::kCountDistinctInt64:
              case RawAggKind::kSumInt64:
                init_vt = ValueType::kInt64;
                break;
              case RawAggKind::kSumDouble:
              case RawAggKind::kAvgDouble:
              case RawAggKind::kStddevSamp:
              case RawAggKind::kStddevPop:
              case RawAggKind::kVarSamp:
              case RawAggKind::kVarPop:
                init_vt = ValueType::kDouble;
                break;
              default:
                break;
            }
          }
        } else if (!d.program.has_value() && !d.is_agg) {
          if (d.index < left_width) {
            if (d.index < 16 && def_col_to_pk_lookup[d.index] >= 0) {
              const auto& lkp = prepared_pk_lookups[static_cast<size_t>(
                  def_col_to_pk_lookup[d.index])];
              init_vt = lkp.payload_type;
              init_unsigned = lkp.payload_unsigned;
            } else if ((left.uniform_cell_types || left.HasColumnarOnly() ||
                        deferred_left_probe != nullptr) &&
                       (d.index >= def_overwritten_by_pk.size() ||
                        def_overwritten_by_pk[d.index] == 0)) {
              const Column& src_col = left.schema.GetColumn(d.index);
              if (!src_col.IsInterval() &&
                  (src_col.Type() == ValueType::kInt64 ||
                   src_col.Type() == ValueType::kDate ||
                   src_col.Type() == ValueType::kDouble ||
                   src_col.Type() == ValueType::kVarChar)) {
                init_vt = src_col.Type();
                init_unsigned = src_col.IsUnsigned();
              }
            }
          } else if (d.index - left_width < right_width &&
                     (right.uniform_cell_types || right.HasColumnarOnly())) {
            const Column& src_col =
                right.schema.GetColumn(d.index - left_width);
            if (!src_col.IsInterval() &&
                (src_col.Type() == ValueType::kInt64 ||
                 src_col.Type() == ValueType::kDate ||
                 src_col.Type() == ValueType::kDouble ||
                 src_col.Type() == ValueType::kVarChar)) {
              init_vt = src_col.Type();
              init_unsigned = src_col.IsUnsigned();
            }
          }
        }
        chunk.columns[c].Init(
            init_vt, init_unsigned, false,
            (!fast_col_emit && init_vt != ValueType::kNull) ? chunk_rows : 0);
      }
      ChunkStringArena* arena = chunk.string_arena.get();
      if (fast_col_emit) {
        if (raw_sum_fusion &&
            (has_double_raw_agg || has_int_raw_agg || has_stat_raw_agg)) {
          for (size_t index = c_begin; index < c_end; ++index) {
            const FusedGroup& group = active_group_at(index);
            for (size_t s = 0; s < raw_slot_count; ++s) {
              if (group.RawInfFlags()[s] && slot_raises_inf[s]) {
                return StatusError(StatusCode::kIsInfinity, "infinity");
              }
            }
          }
        }
        bool all_hash_only = true;
        for (size_t r = 0; r < chunk_rows; ++r) {
          const FusedGroup& grp = active_group_at(c_begin + r);
          if (grp.representative != nullptr || grp.rep_left != nullptr ||
              grp.rep_col_chunk != std::numeric_limits<uint32_t>::max() ||
              !grp.has_key_hash) {
            all_hash_only = false;
            break;
          }
        }
        bool have_chunk_rlocs = false;
        bool chunk_rlocs_ok = false;
        bool right_keepalive_added = false;
        for (size_t c = 0; c < direct_emit.size(); ++c) {
          if (is_const_col[c] != 0) {
            continue;
          }
          const DirectEmitItem& d = direct_emit[c];
          ColumnarColumn& col = chunk.columns[c];
          auto emit_col_via_rep = [&](size_t start_r) -> Status {
            for (size_t r = start_r; r < chunk_rows; ++r) {
              Value cell = direct_key_value(active_group_at(c_begin + r),
                                            d.index, false);
              if (col_tracks != nullptr && c < col_tracks->size()) {
                (*col_tracks)[c].Observe(cell);
              }
              if (cell.type == ValueType::kVarChar && arena == nullptr) {
                arena = chunk.EnsureArena();
              }
              col.AppendValue(cell, arena, r);
            }
            return Status::kSuccess;
          };
          if (d.is_agg) {
            if (raw_count_fusion) {
              col.ints.resize(chunk_rows);
              int64_t* dst = col.ints.data();
              for (size_t r = 0; r < chunk_rows; ++r) {
                dst[r] = active_group_at(c_begin + r).raw_count;
              }
              track_uniform_col(c, col, true);
              continue;
            }
            const size_t i = d.index;
            if (!raw_const_folded[i]) {
              const auto slot = static_cast<size_t>(
                  static_cast<unsigned char>(raw_agg_slot[i]));
              const RawAggKind kind = raw_agg_kinds[i];
              if (kind == RawAggKind::kCountStar ||
                  kind == RawAggKind::kCountValue) {
                col.ints.resize(chunk_rows);
                int64_t* dst = col.ints.data();
                for (size_t r = 0; r < chunk_rows; ++r) {
                  dst[r] = active_group_at(c_begin + r).RawCounts()[slot];
                }
                track_uniform_col(c, col, true);
                continue;
              }
              if (kind == RawAggKind::kSumInt64) {
                col.ints.resize(chunk_rows);
                int64_t* dst = col.ints.data();
                bool non_null_seen = false;
                for (size_t r = 0; r < chunk_rows; ++r) {
                  const FusedGroup& g = active_group_at(c_begin + r);
                  if (g.RawCounts()[slot] == 0) {
                    if (!col.has_null) {
                      col.has_null = true;
                      col.nulls.assign(chunk_rows, 0);
                    }
                    col.nulls[r] = 1;
                    dst[r] = 0;
                  } else {
                    dst[r] = g.RawInts()[slot];
                    non_null_seen = true;
                  }
                }
                track_uniform_col(c, col, non_null_seen);
                continue;
              }
              if (kind == RawAggKind::kSumDouble ||
                  kind == RawAggKind::kAvgDouble) {
                col.doubles.resize(chunk_rows);
                double* dst = col.doubles.data();
                const bool is_avg = (kind == RawAggKind::kAvgDouble);
                bool non_null_seen = false;
                for (size_t r = 0; r < chunk_rows; ++r) {
                  const FusedGroup& g = active_group_at(c_begin + r);
                  const int64_t cnt = g.RawCounts()[slot];
                  if (cnt == 0) {
                    if (!col.has_null) {
                      col.has_null = true;
                      col.nulls.assign(chunk_rows, 0);
                    }
                    col.nulls[r] = 1;
                    dst[r] = 0.0;
                  } else {
                    dst[r] =
                        is_avg
                            ? (g.RawDoubles()[slot] / static_cast<double>(cnt))
                            : g.RawDoubles()[slot];
                    non_null_seen = true;
                  }
                }
                track_uniform_col(c, col, non_null_seen);
                continue;
              }
            }
            for (size_t r = 0; r < chunk_rows; ++r) {
              Value cell;
              if (raw_const_folded[i]) {
                cell = raw_constants[i];
              } else {
                ASSIGN_OR_RETURN(
                    Value, agg_cell,
                    finish_raw_agg(active_group_at(c_begin + r), i));
                cell = std::move(agg_cell);
              }
              if (col_tracks != nullptr && c < col_tracks->size()) {
                (*col_tracks)[c].Observe(cell);
              }
              if (cell.type == ValueType::kVarChar && arena == nullptr) {
                arena = chunk.EnsureArena();
              }
              col.AppendValue(cell, arena, r);
            }
            continue;
          }
          if (all_hash_only && packed_2x32_unpack_only &&
              (d.index == packed_2x32_slot0 || d.index == packed_2x32_slot1) &&
              col.type != ValueType::kNull) {
            col.ints.resize(chunk_rows);
            int64_t* dst = col.ints.data();
            const bool first = (d.index == packed_2x32_slot0);
            for (size_t r = 0; r < chunk_rows; ++r) {
              const uint64_t kh = active_group_at(c_begin + r).key_hash;
              dst[r] = static_cast<int64_t>(static_cast<int32_t>(
                  first ? (kh >> 32) : (kh & 0xffffffffULL)));
            }
            track_uniform_col(c, col, true);
            continue;
          }
          if (all_hash_only && split_key_unpack_only && split_has_left_probe &&
              split_left_slot.has_value() && d.index == *split_left_slot &&
              col.type != ValueType::kNull) {
            col.ints.resize(chunk_rows);
            int64_t* dst = col.ints.data();
            for (size_t r = 0; r < chunk_rows; ++r) {
              dst[r] = static_cast<int64_t>(static_cast<int32_t>(
                  active_group_at(c_begin + r).key_hash & 0xffffffffULL));
            }
            track_uniform_col(c, col, true);
            continue;
          }
          if (all_hash_only && split_key_unpack_only && d.index < left_width &&
              d.index < 16 && def_col_to_pk_lookup[d.index] >= 0 &&
              col.type != ValueType::kNull) {
            const auto li = static_cast<size_t>(
                static_cast<unsigned char>(def_col_to_pk_lookup[d.index]));
            const PreparedDeferredPkLookup& lkp = prepared_pk_lookups[li];
            const PreparedDeferredPkGroup& pk_grp =
                def_pk_groups[lkp.group_idx];
            if (pk_grp.use_subgid && pk_grp.subgid_rep_ptr != nullptr &&
                (col.type != ValueType::kVarChar ||
                 pk_grp.store_owner != nullptr)) {
              const uint32_t p_card = pk_grp.subgid_count;
              const uint64_t p_mask =
                  p_card <= 1
                      ? 0ULL
                      : ((uint64_t{1} << std::bit_width(p_card - 1)) - 1ULL);
              const uint8_t p_shift = pk_grp.subgid_shift;
              const auto* rep_ptrs = pk_grp.subgid_rep_ptr;
              const auto* blk_ptr = lkp.blocks.data();
              bool non_null_seen = false;
              if (col.type == ValueType::kVarChar) {
                col.encoding_owner = pk_grp.store_owner;
                chunk.keepalives.push_back(pk_grp.store_owner);
                col.strings.resize(chunk_rows);
                std::string_view* dst = col.strings.data();
                for (size_t r = 0; r < chunk_rows; ++r) {
                  const auto sg = static_cast<uint32_t>(
                      (active_group_at(c_begin + r).key_hash >> p_shift) &
                      p_mask);
                  const auto [nb, rin] = rep_ptrs[sg];
                  const PreparedDeferredPkBlock& pb = blk_ptr[nb];
                  if (pb.has_null_payload != 0 && pb.pay_blk->IsNull(rin)) {
                    if (!col.has_null) {
                      col.has_null = true;
                      col.nulls.assign(chunk_rows, 0);
                    }
                    col.nulls[r] = 1;
                    dst[r] = {};
                  } else {
                    dst[r] = pb.pay_blk->VarcharAt(rin);
                    non_null_seen = true;
                  }
                }
                track_uniform_col(c, col, non_null_seen);
                continue;
              }
              if (col.type == ValueType::kDouble) {
                col.doubles.resize(chunk_rows);
                double* dst = col.doubles.data();
                for (size_t r = 0; r < chunk_rows; ++r) {
                  const auto sg = static_cast<uint32_t>(
                      (active_group_at(c_begin + r).key_hash >> p_shift) &
                      p_mask);
                  const auto [nb, rin] = rep_ptrs[sg];
                  const PreparedDeferredPkBlock& pb = blk_ptr[nb];
                  if (pb.has_null_payload != 0 && pb.pay_blk->IsNull(rin)) {
                    if (!col.has_null) {
                      col.has_null = true;
                      col.nulls.assign(chunk_rows, 0);
                    }
                    col.nulls[r] = 1;
                    dst[r] = 0.0;
                  } else {
                    const auto dv = std::bit_cast<double>(pb.pay_cells[rin]);
                    dst[r] = dv == 0.0 ? 0.0 : dv;
                    non_null_seen = true;
                  }
                }
                track_uniform_col(c, col, non_null_seen);
                continue;
              }
              col.ints.resize(chunk_rows);
              int64_t* dst = col.ints.data();
              for (size_t r = 0; r < chunk_rows; ++r) {
                const auto sg = static_cast<uint32_t>(
                    (active_group_at(c_begin + r).key_hash >> p_shift) &
                    p_mask);
                const auto [nb, rin] = rep_ptrs[sg];
                const PreparedDeferredPkBlock& pb = blk_ptr[nb];
                if (pb.has_null_payload != 0 && pb.pay_blk->IsNull(rin)) {
                  if (!col.has_null) {
                    col.has_null = true;
                    col.nulls.assign(chunk_rows, 0);
                  }
                  col.nulls[r] = 1;
                  dst[r] = 0;
                } else {
                  dst[r] = pb.pay_cells[rin];
                  non_null_seen = true;
                }
              }
              track_uniform_col(c, col, non_null_seen);
              continue;
            }
          }
          std::optional<size_t> eq_right_col;
          if (d.index < left_width && join_type == JoinType::kInner &&
              col.type != ValueType::kNull && col.type != ValueType::kVarChar &&
              d.index < left.schema.ColumnCount()) {
            const Column& lc = left.schema.GetColumn(d.index);
            for (const EqualityKey& eq : equality_keys) {
              if (eq.left == d.index && eq.left_offset == 0 &&
                  eq.right_offset == 0 && !eq.null_safe &&
                  eq.right < right.schema.ColumnCount()) {
                const Column& rc = right.schema.GetColumn(eq.right);
                if (lc.Type() == rc.Type() &&
                    lc.IsUnsigned() == rc.IsUnsigned() &&
                    lc.Type() != ValueType::kVarChar) {
                  eq_right_col = eq.right;
                  break;
                }
              }
            }
          }
          if ((d.index >= left_width || eq_right_col.has_value()) &&
              col.type != ValueType::kNull) {
            const size_t rc =
                d.index >= left_width ? (d.index - left_width) : *eq_right_col;
            const bool can_split_right_rep =
                all_hash_only && split_right_rep_ptr != nullptr &&
                ((split_key_unpack_only && split_uses_right_subgid) ||
                 (left_pack_unpack_only && left_pack_uses_right));
            const uint64_t r_mask =
                !can_split_right_rep
                    ? 0ULL
                    : ((left_pack_unpack_only && left_pack_uses_right)
                           ? left_pack_right_mask
                           : (split_right_subgid_count <= 1
                                  ? 0ULL
                                  : ((uint64_t{1} << std::bit_width(
                                          split_right_subgid_count - 1)) -
                                     1ULL)));
            const auto get_right_idx =
                [&](const FusedGroup& grp) noexcept -> uint32_t {
              if (grp.rep_right_idx != kInvalidRightIdx) {
                return grp.rep_right_idx;
              }
              if (can_split_right_rep && grp.has_key_hash) {
                const auto r_subgid = static_cast<size_t>(
                    (grp.key_hash >> split_right_shift) & r_mask);
                if (r_subgid < split_right_rep_count) {
                  return split_right_rep_ptr[r_subgid];
                }
              }
              return kInvalidRightIdx;
            };
            const auto get_right_val =
                [&](const FusedGroup& grp) noexcept -> const Value* {
              if (grp.representative != nullptr) {
                return &grp.representative->values_[d.index];
              }
              const uint32_t r_idx = get_right_idx(grp);
              if (r_idx != kInvalidRightIdx) {
                return &right_ptr->rows[r_idx].values_[rc];
              }
              return nullptr;
            };
            if (right_col_only && !have_chunk_rlocs) {
              have_chunk_rlocs = true;
              chunk_rlocs_ok = true;
              chunk_rch.resize(chunk_rows);
              chunk_rr.resize(chunk_rows);
              for (size_t r = 0; r < chunk_rows; ++r) {
                const FusedGroup& grp = active_group_at(c_begin + r);
                if (grp.representative != nullptr) {
                  chunk_rlocs_ok = false;
                  break;
                }
                const uint32_t r_idx = get_right_idx(grp);
                if (r_idx == kInvalidRightIdx) {
                  chunk_rlocs_ok = false;
                  break;
                }
                const auto [rch, rr] = right_locator.Locate(r_idx);
                chunk_rch[r] = static_cast<uint32_t>(rch);
                chunk_rr[r] = rr;
              }
            }
            bool non_null_seen = false;
            bool demoted = false;
            size_t resume_r = chunk_rows;
            const auto demote_at = [&](size_t r) {
              col.DemoteToFallback(r);
              demoted = true;
              resume_r = r;
            };
            const auto set_null_row = [&](size_t r) {
              if (!col.has_null) {
                col.has_null = true;
                col.nulls.assign(chunk_rows, 0);
              }
              col.nulls[r] = 1;
            };
            const auto emit_uniform_loop = [&](auto col_tag, ValueType exp_vt,
                                               bool exp_uns, auto* dst_ptr,
                                               auto zero_val,
                                               auto&& store_from_leaf,
                                               auto&& store_from_val) {
              constexpr bool kRightColOnly = decltype(col_tag)::value;
              if constexpr (kRightColOnly) {
                if (chunk_rlocs_ok) {
                  const ColumnarChunk* const right_chunks =
                      right_arg.columnar->chunks.data();
                  const uint32_t* const rchs = chunk_rch.data();
                  const size_t* const rrs = chunk_rr.data();
                  for (size_t r = 0; r < chunk_rows; ++r) {
                    const ColumnarColumn& sc =
                        right_chunks[rchs[r]].columns[rc];
                    const ColumnarColumn* leaf = &sc;
                    size_t lr = rrs[r];
                    const bool is_null =
                        (leaf->encoding == ColumnarColumn::Encoding::kGathered)
                            ? !sc.ResolveLeaf(lr, &leaf, &lr)
                            : leaf->NonGatheredIsNull(lr);
                    if (is_null) {
                      set_null_row(r);
                      dst_ptr[r] = zero_val;
                    } else if (leaf->is_uniform && leaf->type == exp_vt &&
                               leaf->is_unsigned == exp_uns &&
                               !leaf->is_interval) {
                      dst_ptr[r] = store_from_leaf(leaf, lr);
                      non_null_seen = true;
                    } else {
                      demote_at(r);
                      break;
                    }
                  }
                  return;
                }
              }
              for (size_t r = 0; r < chunk_rows; ++r) {
                const FusedGroup& grp = active_group_at(c_begin + r);
                if constexpr (kRightColOnly) {
                  if (grp.representative != nullptr) {
                    demote_at(r);
                    break;
                  }
                  const uint32_t r_idx = get_right_idx(grp);
                  if (r_idx == kInvalidRightIdx) {
                    demote_at(r);
                    break;
                  }
                  const auto [rch, rr] = right_locator.Locate(r_idx);
                  const ColumnarColumn& sc =
                      right_arg.columnar->chunks[rch].columns[rc];
                  const ColumnarColumn* leaf = &sc;
                  size_t lr = rr;
                  const bool is_null =
                      (leaf->encoding == ColumnarColumn::Encoding::kGathered)
                          ? !sc.ResolveLeaf(rr, &leaf, &lr)
                          : leaf->NonGatheredIsNull(lr);
                  if (is_null) {
                    set_null_row(r);
                    dst_ptr[r] = zero_val;
                  } else if (leaf->is_uniform && leaf->type == exp_vt &&
                             leaf->is_unsigned == exp_uns &&
                             !leaf->is_interval) {
                    dst_ptr[r] = store_from_leaf(leaf, lr);
                    non_null_seen = true;
                  } else {
                    demote_at(r);
                    break;
                  }
                } else {
                  const Value* cp = get_right_val(grp);
                  if (cp == nullptr) {
                    demote_at(r);
                    break;
                  }
                  if (cp->IsNull()) {
                    set_null_row(r);
                    dst_ptr[r] = zero_val;
                  } else if (cp->type == exp_vt &&
                             cp->IsUnsigned() == exp_uns && !cp->IsInterval() &&
                             (exp_uns || cp->Collation() == 0)) {
                    dst_ptr[r] = store_from_val(cp);
                    non_null_seen = true;
                  } else {
                    demote_at(r);
                    break;
                  }
                }
              }
            };
            const auto run_right_emit = [&](auto&& emit_with_tag) {
              if (right_col_only) {
                emit_with_tag(std::true_type{});
              } else {
                emit_with_tag(std::false_type{});
              }
            };
            if (col.type == ValueType::kVarChar) {
              col.strings.resize(chunk_rows);
              std::string_view* dst = col.strings.data();
              if (right_col_only) {
                col.encoding_owner = right_col_owner;
                if (!right_keepalive_added) {
                  chunk.keepalives.push_back(right_col_owner);
                  right_keepalive_added = true;
                }
              }
              run_right_emit([&](auto col_tag) {
                emit_uniform_loop(
                    col_tag, ValueType::kVarChar, false, dst,
                    std::string_view{},
                    [](const ColumnarColumn* leaf, size_t lr) {
                      switch (leaf->encoding) {
                        case ColumnarColumn::Encoding::kDictionary: {
                          const uint32_t code =
                              leaf->borrowed_dict_u8 != nullptr
                                  ? leaf->borrowed_dict_u8[lr]
                                  : leaf->dict_codes[lr];
                          return leaf->dict->StringEntryAt(code);
                        }
                        case ColumnarColumn::Encoding::kFlat:
                          return leaf->strings[lr];
                        case ColumnarColumn::Encoding::kConstant:
                          return leaf->const_string;
                        default:
                          return leaf->StringAt(lr);
                      }
                    },
                    [&](const Value* cp) {
                      if (arena == nullptr) {
                        arena = chunk.EnsureArena();
                      }
                      return arena->Store(cp->value.varchar_value);
                    });
              });
            } else if (col.type == ValueType::kDouble) {
              col.doubles.resize(chunk_rows);
              double* dst = col.doubles.data();
              run_right_emit([&](auto col_tag) {
                emit_uniform_loop(
                    col_tag, ValueType::kDouble, false, dst, 0.0,
                    [](const ColumnarColumn* leaf, size_t lr) {
                      return leaf->DoubleAt(lr);
                    },
                    [](const Value* cp) { return cp->value.double_value; });
              });
            } else {
              col.ints.resize(chunk_rows);
              int64_t* dst = col.ints.data();
              const ValueType exp_vt = col.type;
              const bool exp_uns = col.is_unsigned;
              run_right_emit([&](auto col_tag) {
                emit_uniform_loop(
                    col_tag, exp_vt, exp_uns, dst, int64_t{0},
                    [](const ColumnarColumn* leaf, size_t lr) {
                      return leaf->IntAt(lr);
                    },
                    [](const Value* cp) { return cp->value.int_value; });
              });
            }
            if (!demoted) {
              track_uniform_col(c, col, non_null_seen);
              continue;
            }
            track_fallback_col(c, col);
            RETURN_IF_FAIL(emit_col_via_rep(resume_r));
            continue;
          }
          RETURN_IF_FAIL(emit_col_via_rep(0));
        }
        out_chunks->push_back(std::move(chunk));
        continue;
      }
      for (size_t index = c_begin, local_r = 0; index < c_end;
           ++index, ++local_r) {
        const FusedGroup& group = active_group_at(index);
        RETURN_IF_FAIL(eval_direct_group(
            group, &is_const_col, false, &finished_aggs, &eval_row,
            [&](size_t c, const Value& cell) {
              if (col_tracks != nullptr && c < col_tracks->size()) {
                (*col_tracks)[c].Observe(cell);
              }
              if (cell.type == ValueType::kVarChar && arena == nullptr) {
                arena = chunk.EnsureArena();
              }
              chunk.columns[c].AppendValue(cell, arena, local_r);
            }));
      }
      out_chunks->push_back(std::move(chunk));
    }
    return Status::kSuccess;
  };
  bool finalized_output_types = false;
  bool all_uniform_columns = false;
  std::shared_ptr<ColumnarTable> out_columnar;
  if (can_direct_emit && !pushed_output) {
    const size_t emit_denom = direct_emit.size() >= 6 ? 2048 : 8192;
    const size_t emit_workers =
        (raw_count_fusion || raw_sum_fusion || fusion->aggregates.empty()) &&
                std::getenv("TINYLAMB_DISABLE_PARALLEL_FUSED_EMIT") == nullptr
            ? ParallelWorkerCount(total_out_groups / emit_denom)
            : 1;
    std::vector<std::vector<ColumnTypeTracker>> emit_col_tracks(
        emit_workers, std::vector<ColumnTypeTracker>(output_columns.size()));
    if (fusion->columnar_output) {
      out_columnar = std::make_shared<ColumnarTable>();
      if (emit_workers > 1) {
        std::vector<std::vector<ColumnarChunk>> shard_chunks(emit_workers);
        std::vector<Status> errors(emit_workers, Status::kSuccess);
        RETURN_IF_FAIL(RunParallelTasks(emit_workers, [&](size_t w) {
          errors[w] =
              emit_direct_columnar((total_out_groups * w) / emit_workers,
                                   (total_out_groups * (w + 1)) / emit_workers,
                                   &shard_chunks[w], &emit_col_tracks[w]);
        }));
        for (const Status& err : errors) {
          RETURN_IF_FAIL(err);
        }
        for (auto& chunks : shard_chunks) {
          for (ColumnarChunk& chunk : chunks) {
            out_columnar->AppendChunk(std::move(chunk));
          }
        }
      } else {
        std::vector<ColumnarChunk> chunks;
        RETURN_IF_FAIL(emit_direct_columnar(0, total_out_groups, &chunks,
                                            emit_col_tracks.data()));
        for (ColumnarChunk& chunk : chunks) {
          out_columnar->AppendChunk(std::move(chunk));
        }
      }
      if (right_col_owner != nullptr) {
        out_columnar->AppendKeepalive(right_col_owner);
      }
    } else if (emit_workers > 1) {
      // Finishing independent raw states does not reassociate their inputs.
      // Contiguous shards retain the historical group order after the merge.
      std::vector<ExecutionRuntime> runtimes(emit_workers);
      std::vector<Relation> shards(emit_workers);
      std::vector<Status> errors(emit_workers, Status::kSuccess);
      for (size_t w = 0; w < emit_workers; ++w) {
        shards[w].set_runtime(&runtimes[w]);
      }
      RETURN_IF_FAIL(RunParallelTasks(emit_workers, [&](size_t w) {
        errors[w] = emit_direct((total_out_groups * w) / emit_workers,
                                (total_out_groups * (w + 1)) / emit_workers,
                                &shards[w], &emit_col_tracks[w]);
      }));
      for (size_t w = 0; w < emit_workers; ++w) {
        RETURN_IF_FAIL(errors[w]);
        if (result.runtime() != nullptr) {
          result.runtime()->relation_spills += runtimes[w].relation_spills;
        }
      }
      RETURN_IF_FAIL(Relation::MergeShardsInto(shards, &result));
    } else {
      RETURN_IF_FAIL(
          emit_direct(0, total_out_groups, &result, emit_col_tracks.data()));
    }
    all_uniform_columns =
        ColumnTypeTracker::FinalizeColumns(emit_col_tracks, output_columns);
    finalized_output_types = true;
  }
  Row eval_rep_row;
  std::vector<ColumnTypeTracker> fallback_col_tracks;
  if (!can_direct_emit) {
    eval_rep_row.values_.resize(result.schema.ColumnCount());
    fallback_col_tracks.resize(output_columns.size());
  }
  for (size_t group_index = can_direct_emit ? total_out_groups : 0;
       group_index < total_out_groups; ++group_index) {
    const FusedGroup& group = active_group_at(group_index);
    AggregateResultMap aggregate_results;
    aggregate_results.reserve(fusion->aggregates.size());
    if (raw_count_fusion) {
      for (auto& aggregate : fusion->aggregates) {
        aggregate_results.emplace(aggregate, Value(group.raw_count));
      }
    } else if (raw_sum_fusion) {
      for (size_t i = 0; i < fusion->aggregates.size(); ++i) {
        ASSIGN_OR_RETURN(Value, finished, finish_raw_agg(group, i));
        aggregate_results.emplace(fusion->aggregates[i], std::move(finished));
      }
    } else {
      for (size_t i = 0; i < fusion->aggregates.size(); ++i) {
        AggregateAccumulator& accumulator = states[group.state_offset + i];
        ASSIGN_OR_RETURN(Value, finished, accumulator.TryFinish());
        aggregate_results.emplace(accumulator.expression, std::move(finished));
      }
    }
    const Row* scope_row = group.representative.get();
    if (scope_row == nullptr) {
      Value col_tmp;
      for (size_t c = 0; c < result.schema.ColumnCount(); ++c) {
        eval_rep_row.values_[c] = rep_cell(group, c, &col_tmp);
      }
      scope_row = &eval_rep_row;
    }
    Scope scope{.row = scope_row, .schema = &result.schema, .outer = outer};
    if (fusion->having) {
      ASSIGN_OR_RETURN(Value, keep,
                       TryEvaluate(fusion->having, scope, &aggregate_results,
                                   context, ctes));
      if (!Truthy(keep)) {
        continue;
      }
    }
    Row output_row;
    output_row.values_.reserve(fusion->select_items.size());
    size_t row_bytes = 64;
    for (size_t ci = 0; ci < fusion->select_items.size(); ++ci) {
      ASSIGN_OR_RETURN(Value, value,
                       (TryEvaluate(fusion->select_items[ci], scope,
                                    &aggregate_results, context, ctes)));
      if (ci < fallback_col_tracks.size()) {
        fallback_col_tracks[ci].Observe(value);
      }
      row_bytes += value.type == ValueType::kArray
                       ? EstimateValueBytes(value)
                       : EstimateScalarValueBytes(value);
      output_row.values_.push_back(std::move(value));
    }
    result.AddRowWithKnownBytes(std::move(output_row), row_bytes,
                                flush_granularity);
  }
  if (!can_direct_emit) {
    all_uniform_columns = ColumnTypeTracker::FinalizeColumns(
        std::span<const std::vector<ColumnTypeTracker>>(&fallback_col_tracks,
                                                        1),
        output_columns);
    finalized_output_types = true;
  }
  result.schema = Schema("", std::move(output_columns));
  RETURN_IF_FAIL(result.FinishSpill());
  RETURN_IF_FAIL(result.GetStatus());
  if (finalized_output_types) {
    result.uniform_cell_types =
        !result.HasSpill() && all_uniform_columns && !result.rows.empty();
  }
  if (out_columnar != nullptr) {
    const bool non_empty = out_columnar->total_rows > 0;
    result.SetColumnarTable(std::move(out_columnar),
                            all_uniform_columns && non_empty);
  } else if (fusion->columnar_output && !pushed_output && !result.HasSpill()) {
    const bool non_empty = !result.rows.empty();
    std::shared_ptr<ColumnarTable> fallback_col;
    if (non_empty) {
      ASSIGN_OR_RETURN(std::shared_ptr<ColumnarTable>, built,
                       BuildColumnarTableFromRows(result.schema, result.rows));
      fallback_col = std::move(built);
    } else {
      fallback_col = std::make_shared<ColumnarTable>();
    }
    if (right_col_owner != nullptr) {
      fallback_col->AppendKeepalive(right_col_owner);
    }
    result.SetColumnarTable(std::move(fallback_col),
                            all_uniform_columns && non_empty);
  }
  if (!result.HasColumnarOnly() && !result.HasSpill() && !result.rows.empty() &&
      (left.HasColumnarOnly() || right_holder.has_value() ||
       right_col_owner != nullptr ||
       (deferred_left_probe != nullptr &&
        !deferred_left_probe->pk_lookups.empty()))) {
    if (context.execution_runtime() != nullptr) {
      if (left.HasColumnarOnly() && left.columnar != nullptr) {
        context.execution_runtime()->retained_scan_stores.push_back(
            left.columnar);
      }
      if (right_col_owner != nullptr) {
        context.execution_runtime()->retained_scan_stores.push_back(
            right_col_owner);
      } else if (right_holder.has_value() && right_arg.columnar != nullptr) {
        context.execution_runtime()->retained_scan_stores.push_back(
            right_arg.columnar);
      }
      if (deferred_left_probe != nullptr) {
        for (const DeferredLSTablePkLookup& lkp :
             deferred_left_probe->pk_lookups) {
          if (lkp.store != nullptr) {
            context.execution_runtime()->retained_scan_stores.push_back(
                lkp.store);
          }
        }
      }
    } else {
      for (Row& row : result.rows) {
        for (Value& cell : row.values_) {
          if (cell.type == ValueType::kVarChar && cell.owned_data.empty() &&
              !cell.value.varchar_value.empty()) {
            cell.owned_data.assign(cell.value.varchar_value);
            cell.value.varchar_value = cell.owned_data;
          }
        }
      }
    }
  }
  const size_t final_group_count = active_group_count();
  if (!use_group_ptrs && workers > 1 && groups.size() >= 16384) {
    (void)RunParallelTasks(workers, [&](size_t w) {
      const size_t begin = (groups.size() * w) / workers;
      const size_t end = (groups.size() * (w + 1)) / workers;
      for (size_t gi = begin; gi < end; ++gi) {
        std::vector<Value>().swap(groups[gi].key.values_);
        groups[gi].representative.reset();
      }
    });
  }
  std::vector<FusedGroup*>().swap(group_ptrs);
  std::vector<FusedGroup>().swap(groups);
  std::deque<AggregateAccumulator>().swap(states);
  int_merge_offsets = FlatIntKeyMap<size_t>();
  offsets.clear();
  if (locals.size() > 1) {
    (void)RunParallelTasks(locals.size(), [&](size_t w) {
      locals[w].raw_arena.blocks.clear();
      std::deque<AggregateAccumulator>().swap(locals[w].states);
      std::deque<Row>().swap(locals[w].owned_rep_left);
      std::vector<FusedGroup>().swap(locals[w].groups);
      locals[w].offsets = GroupKeyMap();
      locals[w].int_offsets = FlatIntKeyMap<size_t>();
      std::vector<CompactCountGroup>().swap(locals[w].compact_counts);
      std::vector<std::vector<uint64_t>>().swap(locals[w].compact_split_shards);
    });
  }
  if (!radix_part_groups.empty() && workers > 1) {
    (void)RunParallelTasks(workers, [&](size_t w) {
      const size_t begin = (radix_part_groups.size() * w) / workers;
      const size_t end = (radix_part_groups.size() * (w + 1)) / workers;
      for (size_t p = begin; p < end; ++p) {
        std::vector<FusedGroup>().swap(radix_part_groups[p]);
        radix_part_arenas[p].blocks.clear();
      }
    });
  }
  result.peak_intermediate_rows =
      std::max(result.peak_intermediate_rows, result.TotalRows());
  if (context.execution_runtime() != nullptr) {
    ExecutionRuntime& runtime = *context.execution_runtime();
    runtime.join_ms += ElapsedMs(join_begin);
    ++runtime.grouped_join_fusions;
  }
  if (track_group_phases) {
    std::cerr << "[fusion] emit_ms=" << ElapsedMs(phase_emit_begin)
              << " total_ms=" << ElapsedMs(join_begin)
              << " groups=" << final_group_count << "\n";
  }
  if (debug_fusion) {
    std::cerr << "[fusion] fused groups=" << final_group_count
              << " dense=" << use_dense_int_rows
              << " unique=" << use_unique_int_table
              << " radix=" << use_radix_agg
              << " def_cell=" << def_group_cell_probe
              << " dense_split=" << use_dense_split_direct_gid
              << " split_unpack=" << split_key_unpack_only
              << " left_pack_unpack=" << left_pack_unpack_only
              << " packed2x32_unpack=" << packed_2x32_unpack_only
              << " ordered_sum=" << static_cast<bool>(ordered_sum_charge)
              << " shared_gid=" << use_shared_direct_gid
              << " workers=" << workers
              << " single_double=" << single_double_sum
              << " count_star=" << count_star_only
              << " preassigned=" << use_preassigned_build_groups
              << " right_keys=" << right_only_group_keys
              << " residual=" << residual_programs.size()
              << " numprobe=" << left_numeric_probe
              << " deferred=" << (deferred_left_probe != nullptr) << " pk="
              << (deferred_left_probe == nullptr
                      ? 0
                      : deferred_left_probe->pk_lookups.size())
              << " raw=" << raw_sum_fusion << " aggs=" << agg_count
              << " darith="
              << (!double_arithmetic.empty() && double_arithmetic[0])
              << " width=" << left_width << " keys=";
    for (size_t ki = 0; ki < key_programs.size() && ki < 10; ++ki) {
      std::cerr << key_shapes[ki].direct_column << ",";
    }
    std::cerr << " kinds=";
    for (size_t ai = 0; ai < agg_count && ai < 4; ++ai) {
      std::cerr << static_cast<int>(raw_agg_kinds[ai]) << ":"
                << child_shapes[ai].mul_col0 << "/" << child_shapes[ai].mul_col1
                << "/" << child_shapes[ai].direct_column << ",";
    }
    std::cerr << " pre_groups=" << pre_group_count
              << " ranked=" << ranked_int_rows.size() << " monotonic="
              << (deferred_left_probe != nullptr && !left_columns.empty() &&
                  left_columns[0] < deferred_left_probe->projection.size() &&
                  deferred_left_probe->projection[left_columns[0]] <
                      deferred_left_probe->store->column_is_monotonic.size() &&
                  deferred_left_probe->store->column_is_monotonic
                          [deferred_left_probe->projection[left_columns[0]]] !=
                      0)
              << " left=" << effective_left_rows << "\n";
  }
  return std::optional<Relation>(std::move(result));
}

}  // namespace tinylamb::relational_detail
