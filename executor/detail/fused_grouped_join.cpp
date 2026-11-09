/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/detail/fused_grouped_join.hpp"

#include <algorithm>
#include <array>
#include <atomic>
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
#include "common/query_parallelism.hpp"
#include "common/status_or.hpp"
#include "common/string_util.hpp"
#include "database/transaction_context.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/detail/flat_int_key_map.hpp"
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

constexpr size_t kParallelProbeMaxWorkers = 16;

}  // namespace

namespace {

struct FusedGroup {
  // Canonical group key, kept so the merge re-probes the shared map without
  // re-evaluating the key programs.
  Row key;
  Row representative;
  const Row* rep_left{nullptr};
  const Row* rep_right{nullptr};
  size_t state_offset{0};
  // count-star-only fusions accumulate here instead of through
  // AggregateAccumulator states: the per-pair work is one integer increment
  // and the merge is an integer add.
  int64_t raw_count{0};
  uint64_t key_hash{0};
  bool has_key_hash{false};
  std::array<double, 4> raw_doubles{};
  std::array<int64_t, 4> raw_ints{};
  std::array<int64_t, 4> raw_counts{};
  std::array<uint8_t, 4> raw_inf_flags{};
  std::unique_ptr<FlatIntKeyMap<uint8_t>> raw_int_set;
};

// Bulk raw-count state: one map entry per distinct build key replaces the
// whole bucket chain.  `row` keeps the most recently inserted build row for
// that key -- the row a bucket-head walk would have filled a new group's
// representative from.
struct BulkCountValue {
  int64_t count{0};
  size_t pairs{0};
  const Row* row{nullptr};
};

struct FusedWorkerLocal {
  GroupKeyMap offsets;
  // Single-INT64/DATE group-key fast path: raw-payload probes skip the
  // Value-vector hashing the generic map pays per pair.  Key spaces of the
  // two maps are disjoint (NULL keys route to the generic map, which groups
  // NULLs), so a group lives in exactly one of them.
  FlatIntKeyMap<size_t> int_offsets;
  std::vector<FusedGroup> groups;
  std::deque<AggregateAccumulator> states;
  // Reusable probe key: avoids a fresh vector per probe row (see
  // accumulate_into).  The transparent group-key hash/equal pair accepts the
  // bare value vector, so probes never construct a Row.
  std::vector<Value> key_scratch;
  std::vector<const Value*> key_ptrs;
  std::deque<Row> owned_rep_left;
  std::array<uint32_t, 8192> local_2key_gid{};
  bool local_2key_gid_init{false};
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
    if (ce.when_clauses_.size() == 1 && ce.else_clause_ &&
        ce.else_clause_->Type() == TypeTag::kConstantValue) {
      const Value& ev = ce.else_clause_->AsConstantValue().GetValue();
      const bool else_zero_int = !ev.IsNull() && ev.type == ValueType::kInt64 &&
                                 !ev.IsUnsigned() && ev.value.int_value == 0;
      const bool else_zero_double = !ev.IsNull() &&
                                    ev.type == ValueType::kDouble &&
                                    ev.value.double_value == 0.0;
      if (else_zero_int || else_zero_double) {
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
    TransactionContext& context, const Relation& left, const Relation& right,
    const std::vector<Expression>& predicates, JoinType join_type,
    const Scope* outer, const CteMap& ctes, GroupedJoinFusionSpec* fusion,
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
  if (disabled || fusion == nullptr || !fusion->eligible ||
      fusion->aggregates.empty() ||
      (join_type != JoinType::kInner && join_type != JoinType::kLeft)) {
    return not_fusable("head");
  }
  if (right.HasSpill() || effective_left_rows == 0) {
    return not_fusable("spill");
  }
  const auto join_begin = std::chrono::steady_clock::now();
  Relation result(context.execution_runtime());
  result.schema = left.schema + right.schema;
  // The build-side rows the hash table is built over; the residual hoist
  // below may drop rows whose right-only residual conjuncts already fail.
  std::vector<const Row*> build_rows;
  build_rows.reserve(right.rows.size());
  for (const Row& row : right.rows) {
    build_rows.push_back(&row);
  }
  const bool single_absorbed_relation =
      (join_type == JoinType::kInner && right.schema.ColumnCount() == 0 &&
       right.rows.size() == 1 && predicates.empty() && !left.HasSpill());
  const std::vector<EqualityKey> equality_keys =
      EqualityKeys(left.schema, right.schema, predicates);
  const std::vector<Expression> residuals =
      HasMixedKeySignednessOrType(left, right, equality_keys)
          ? predicates
          : ResidualJoinPredicates(left.schema, right.schema, predicates);
  if (equality_keys.empty() && !single_absorbed_relation) {
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
      std::vector<slot_t> hoist_cells;
      std::vector<ProgramShape> hoist_shapes;
      for (const size_t i : hoisted_indices) {
        CollectLoadCells(residual_programs[i], &hoist_cells);
        hoist_shapes.push_back(residual_shapes[i]);
      }
      Row hoist_scratch;
      hoist_scratch.values_.resize(left_width + right_width);
      bool hoist_error = false;
      std::vector<bool> keep(right.rows.size(), true);
      for (size_t ri = 0; ri < right.rows.size(); ++ri) {
        const Row& row = right.rows[ri];
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
        for (size_t ri = 0; ri < build_rows.size(); ++ri) {
          if (keep[ri]) {
            filtered.push_back(build_rows[ri]);
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
  std::vector<ProgramShape> key_shapes;
  key_shapes.reserve(key_programs.size());
  for (size_t k = 0; k < key_programs.size(); ++k) {
    key_shapes.push_back(ClassifyProgram(key_programs[k], fusion->group_keys[k],
                                         &result.schema));
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
  bool count_star_only = !fusion->aggregates.empty();
  for (const bool count_star : fusion->count_star) {
    count_star_only = count_star_only && count_star;
  }
  bool count_nonnull_only = false;
  std::optional<slot_t> count_right_column;
  {
    bool all_count_nonnull = !fusion->aggregates.empty();
    bool any_nonnull = false;
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
        if (ctype == Constraint::kNotNull || ctype == Constraint::kPrimaryKey) {
          eligible_column = true;
        } else if (col_idx >= left_width) {
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
    kCountStar,
    kCountValue,
    kCountDistinctInt64,
  };
  std::array<RawAggKind, 4> raw_agg_kinds{};
  bool raw_sum_fusion = !raw_count_fusion && !fusion->aggregates.empty() &&
                        fusion->aggregates.size() <= 4;
  bool has_count_distinct = false;
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
            if (inst.operand < left_width) {
              const slot_t col = inst.operand;
              for (const Row& r : left.rows) {
                if (col < r.Size() && r[col].type == ValueType::kDouble) {
                  child_has_mixed_double = true;
                  break;
                }
              }
            } else {
              const auto col = static_cast<slot_t>(inst.operand - left_width);
              for (const Row& r : right.rows) {
                if (col < r.Size() && r[col].type == ValueType::kDouble) {
                  child_has_mixed_double = true;
                  break;
                }
              }
            }
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
      } else if (agg->GetType() == AggregationType::kAvg) {
        if (child_tag == TypeTag::kDouble ||
            ((child_tag == TypeTag::kBigInt ||
              child_tag == TypeTag::kInteger) &&
             !child_may_unsigned)) {
          raw_agg_kinds[i] = RawAggKind::kAvgDouble;
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
  const bool skip_accumulator_states = raw_count_fusion || raw_sum_fusion;
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
                    right.schema.GetColumn(eq.right).GetConstraint().ctype ==
                        Constraint::kPrimaryKey) {
                  has_unique_int_key = true;
                  break;
                }
                if (col_idx == left_width + eq.right &&
                    left.schema.GetColumn(eq.left).GetConstraint().ctype ==
                        Constraint::kPrimaryKey) {
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
  const bool bulk_raw_count = !single_absorbed_relation && !left.HasSpill() &&
                              !bulk_count_disabled && raw_count_fusion &&
                              residual_programs.empty() && int_group_key &&
                              int_group_slot < left_width;
  static const Value kNullCell{};
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
          break;
      }
    }
    return false;
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
          return Value(year >= -999 && year <= 9999 ? year : year / 10);
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
  if (raw_sum_fusion && !right.HasSpill() && !right.rows.empty() &&
      right.rows.size() <= 262144) {
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
      std::vector<double> vals(right.rows.size());
      bool all_ok = true;
      for (size_t ri = 0; ri < right.rows.size(); ++ri) {
        const Value& rv = right.rows[ri].values_[rcol];
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
      if (!right_profit_doubles[agg_idx].empty() && left_row != nullptr &&
          right_row != nullptr) [[likely]] {
        const auto r_idx = static_cast<size_t>(right_row - right.rows.data());
        const Value& c0 =
            left_row->values_[static_cast<size_t>(shape.profit_col0)];
        const Value& c1 =
            left_row->values_[static_cast<size_t>(shape.profit_col1)];
        const bool col2_right =
            std::cmp_greater_equal(shape.profit_col2, left_width);
        const Value& cl = left_row->values_[static_cast<size_t>(
            col2_right ? shape.profit_col3 : shape.profit_col2)];
        if (c0.IsNull() || c1.IsNull() || cl.IsNull()) [[unlikely]] {
          *is_null = true;
          return Status::kSuccess;
        }
        double d0 = 0.0, d1 = 0.0, dl = 0.0;
        if (cell_as_double(c0, &d0) && cell_as_double(c1, &d1) &&
            cell_as_double(cl, &dl)) [[likely]] {
          const double dr = right_profit_doubles[agg_idx][r_idx];
          *out = (d0 * (shape.profit_const - d1)) -
                 (col2_right ? (dr * dl) : (dl * dr));
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
      if (eval_case_cond(shape, left_row, right_row, &cond_true)) [[likely]] {
        if (!cond_true) {
          *out = 0.0;
          *is_null = false;
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
  const auto ensure_group_key = [&](FusedGroup& grp) {
    if (!grp.key.values_.empty() || !all_direct_group_keys ||
        (grp.rep_left == nullptr && grp.rep_right == nullptr)) {
      return;
    }
    grp.key.values_.resize(key_programs.size());
    for (size_t k = 0; k < key_programs.size(); ++k) {
      const Value& raw =
          get_pair_cell(grp.rep_left, grp.rep_right,
                        static_cast<slot_t>(key_shapes[k].direct_column));
      grp.key.values_[k] = CanonicalDistinctValue(raw);
    }
  };
  bool use_split_2key_gid = false;
  size_t split_right_k = 0;
  size_t split_left_k = 1;
  slot_t split_right_col = 0;
  size_t split_right_subgid_count = 0;
  std::vector<uint32_t> split_right_subgid;
  if (!int_group_key && !left.HasSpill() && !right.HasSpill() &&
      key_programs.size() == 2 && !right.rows.empty() &&
      right.rows.size() <= 262144) {
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
      const auto rcol =
          static_cast<slot_t>(key_shapes[rk].direct_column - left_width);
      if (rcol >= right.schema.ColumnCount()) {
        continue;
      }
      const Column& rcol_def = right.schema.GetColumn(rcol);
      if (rcol_def.Type() == ValueType::kVarChar) {
        split_right_subgid.assign(right.rows.size(),
                                  std::numeric_limits<uint32_t>::max());
        std::unordered_map<std::string_view, uint32_t> sv_to_gid;
        sv_to_gid.reserve(64);
        std::string_view last_sv;
        uint32_t last_gid = std::numeric_limits<uint32_t>::max();
        bool all_ok = true;
        for (size_t ri = 0; ri < right.rows.size(); ++ri) {
          const Value& rv = right.rows[ri].values_[rcol];
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
          const auto [it, ins] = sv_to_gid.emplace(sv, next_id);
          if (sv_to_gid.size() > 65535) {
            all_ok = false;
            break;
          }
          last_sv = sv;
          last_gid = it->second;
          split_right_subgid[ri] = last_gid;
        }
        if (all_ok) {
          use_split_2key_gid = true;
          split_right_k = rk;
          split_left_k = lk;
          split_right_col = rcol;
          split_right_subgid_count = sv_to_gid.size();
          break;
        }
        split_right_subgid.clear();
      } else if ((rcol_def.Type() == ValueType::kInt64 ||
                  rcol_def.Type() == ValueType::kDate) &&
                 !rcol_def.IsUnsigned()) {
        split_right_subgid.assign(right.rows.size(),
                                  std::numeric_limits<uint32_t>::max());
        FlatIntKeyMap<uint32_t> int_to_gid;
        int_to_gid.Reserve(256);
        uint32_t next_id = 0;
        bool all_ok = true;
        for (size_t ri = 0; ri < right.rows.size(); ++ri) {
          const Value& rv = right.rows[ri].values_[rcol];
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
          }
          split_right_subgid[ri] = *slot;
        }
        if (all_ok) {
          use_split_2key_gid = true;
          split_right_k = rk;
          split_left_k = lk;
          split_right_col = rcol;
          split_right_subgid_count = next_id;
          break;
        }
        split_right_subgid.clear();
      }
    }
  }
  const bool use_fast_2key_year_lut =
      use_split_2key_gid && split_right_subgid_count <= 64 &&
      key_shapes[split_left_k].extract_date_col >= 0 &&
      key_shapes[split_left_k].extract_date_part == 0;
  constexpr size_t kNoCachedGroup = std::numeric_limits<size_t>::max();
  const auto resolve_group_index =
      [&](FusedWorkerLocal& local, Row& scratch, const Row* left_row,
          const Row* right_row, bool masked, size_t* cached_group,
          size_t* out_group_index) -> Status {
    size_t group_index = 0;
    if (cached_group != nullptr && *cached_group != kNoCachedGroup) {
      group_index = *cached_group;
    } else {
      const Value* fast_key = nullptr;
      if (int_group_key) {
        const Value& cell = get_pair_cell(left_row, right_row, int_group_slot);
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
      } else if (use_split_2key_gid && left_row != nullptr &&
                 right_row != nullptr) {
        const auto r_idx = static_cast<size_t>(right_row - right.rows.data());
        const uint32_t r_subgid = split_right_subgid[r_idx];
        int64_t l_val = 0;
        bool l_ok = false;
        const ProgramShape& l_shape = key_shapes[split_left_k];
        if (l_shape.direct_column >= 0) {
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
        } else if (l_shape.extract_date_col >= 0) {
          const Value& lv =
              left_row->values_[static_cast<size_t>(l_shape.extract_date_col)];
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
                l_val = year >= -999 && year <= 9999 ? year : year / 10;
              } else if (l_shape.extract_date_part == 1) {
                l_val =
                    static_cast<int64_t>(static_cast<unsigned>(ymd.month()));
              } else {
                l_val = static_cast<int64_t>(static_cast<unsigned>(ymd.day()));
              }
            }
            l_ok = true;
          }
        }
        if (r_subgid != std::numeric_limits<uint32_t>::max() && l_ok)
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
          const int64_t packed_gk =
              (static_cast<int64_t>(r_subgid) << 32) |
              static_cast<uint32_t>(static_cast<int32_t>(l_val));
          bool inserted = false;
          fast_slot = local.int_offsets.TryEmplace(packed_gk, &inserted);
          if (!inserted) [[likely]] {
            if (fast_lut_slot != nullptr) {
              *fast_lut_slot = static_cast<uint32_t>(*fast_slot);
            }
            *out_group_index = *fast_slot;
            return Status::kSuccess;
          }
          local.key_scratch.resize(2);
          local.key_scratch[split_right_k] =
              CanonicalDistinctValue(right_row->values_[split_right_col]);
          if (l_shape.direct_column >= 0 &&
              left_row->values_[static_cast<size_t>(l_shape.direct_column)]
                      .type == ValueType::kDate) {
            local.key_scratch[split_left_k] = Value::DateFromDays(l_val);
          } else {
            local.key_scratch[split_left_k] = Value(l_val);
          }
          computed_key_hash = static_cast<uint64_t>(packed_gk);
          has_computed_key_hash = true;
          new_group = true;
        } else {
          local.key_scratch.resize(2);
          for (size_t k = 0; k < 2; ++k) {
            ASSIGN_OR_RETURN(Value, kv,
                             eval_pair_program(key_programs[k], key_shapes[k],
                                               scratch, left_row, right_row));
            local.key_scratch[k] = CanonicalDistinctValue(kv);
          }
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
          for (const size_t ik : int_key_indices) {
            const Value& kv = get_pair_cell(
                left_row, right_row,
                static_cast<slot_t>(key_shapes[ik].direct_column));
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
          for (size_t key_index = 0; key_index < key_programs.size();
               ++key_index) {
            const Value* kp = nullptr;
            if (key_shapes[key_index].direct_column >= 0) {
              const Value& raw = get_pair_cell(
                  left_row, right_row,
                  static_cast<slot_t>(key_shapes[key_index].direct_column));
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
              const Value& c = get_pair_cell(
                  left_row, right_row,
                  static_cast<slot_t>(key_shapes[key_index].extract_date_col));
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
                    extracted = year >= -999 && year <= 9999 ? year : year / 10;
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
              } else if (!kp->IsNull() && kp->type == ValueType::kVarChar &&
                         !kp->IsCaseInsensitive()) [[likely]] {
                const std::string_view sv = kp->value.varchar_value;
                if (sv.empty() ||
                    ((sv[0] < '0' || sv[0] > '9') && sv[0] != '-' &&
                     sv[0] != '+' && sv[0] != ' ')) [[likely]] {
                  vh = std::hash<std::string_view>()(sv);
                } else {
                  vh = std::hash<Value>()(*kp);
                }
              } else {
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
            auto cand_key_cell = [&](size_t k) -> const Value& {
              if (!lazy_cand) {
                return cand_grp.key.values_[k];
              }
              return get_pair_cell(
                  cand_grp.rep_left, cand_grp.rep_right,
                  static_cast<slot_t>(key_shapes[k].direct_column));
            };
            auto probe_key_cell = [&](size_t k) -> const Value& {
              if (fast_direct_int_hash) {
                return get_pair_cell(
                    left_row, right_row,
                    static_cast<slot_t>(key_shapes[k].direct_column));
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
              for (size_t k = 0; k < key_programs.size(); ++k) {
                if (fast_direct_int_hash) {
                  local.key_scratch[k] =
                      CanonicalDistinctValue(probe_key_cell(k));
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
            for (size_t i = 0; i < right_width; ++i) {
              scratch.values_[left_width + i] =
                  right_row != nullptr ? right_row->values_[i] : Value();
            }
          }
          group.representative = scratch;
        } else {
          if (deferred_left_probe != nullptr && left_row != nullptr) {
            local.owned_rep_left.push_back(*left_row);
            group.rep_left = &local.owned_rep_left.back();
          } else {
            group.rep_left = left_row;
          }
          group.rep_right = right_row;
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
  const auto accumulate_into = [&](FusedWorkerLocal& local, Row& scratch,
                                   const Row* left_row, const Row* right_row,
                                   bool masked,
                                   size_t* cached_group = nullptr) -> Status {
    size_t group_index = 0;
    RETURN_IF_FAIL(resolve_group_index(local, scratch, left_row, right_row,
                                       masked, cached_group, &group_index));
    FusedGroup& group = local.groups[group_index];
    if (count_star_only) {
      group.raw_count += 1;
      return Status::kSuccess;
    }
    if (count_nonnull_only) {
      // COUNT(right column): unmatched LEFT rows and NULL cells contribute 0.
      if (right_row != nullptr &&
          (!count_right_column.has_value() ||
           !right_row->values_[*count_right_column].IsNull())) {
        group.raw_count += 1;
      }
      return Status::kSuccess;
    }
    if (raw_sum_fusion) {
      for (size_t i = 0; i < fusion->aggregates.size(); ++i) {
        switch (raw_agg_kinds[i]) {
          case RawAggKind::kCountStar:
            group.raw_counts[i] += 1;
            break;
          case RawAggKind::kCountValue: {
            if (child_shapes[i].direct_column >= 0) {
              const Value& v = get_pair_cell(
                  left_row, right_row,
                  static_cast<slot_t>(child_shapes[i].direct_column));
              if (!v.IsNull()) {
                group.raw_counts[i] += 1;
              }
            } else {
              ASSIGN_OR_RETURN(
                  Value, child,
                  eval_pair_program(child_programs[i], child_shapes[i], scratch,
                                    left_row, right_row));
              if (!child.IsNull()) {
                group.raw_counts[i] += 1;
              }
            }
            break;
          }
          case RawAggKind::kSumInt64: {
            int64_t iv = 0;
            bool is_null = false;
            if (child_shapes[i].direct_column >= 0) {
              const Value& v = get_pair_cell(
                  left_row, right_row,
                  static_cast<slot_t>(child_shapes[i].direct_column));
              is_null = v.IsNull();
              if (!is_null) {
                iv = v.value.int_value;
              }
            } else if (child_shapes[i].case_cond !=
                           ProgramShape::CaseCondKind::kNone &&
                       child_shapes[i].case_val ==
                           ProgramShape::CaseValKind::kIntConst1) {
              bool cond_true = false;
              if (eval_case_cond(child_shapes[i], left_row, right_row,
                                 &cond_true)) [[likely]] {
                iv = cond_true ? 1 : 0;
              } else {
                ASSIGN_OR_RETURN(
                    Value, child,
                    eval_pair_program(child_programs[i], child_shapes[i],
                                      scratch, left_row, right_row));
                is_null = child.IsNull();
                if (!is_null) {
                  iv = child.value.int_value;
                }
              }
            } else {
              ASSIGN_OR_RETURN(
                  Value, child,
                  eval_pair_program(child_programs[i], child_shapes[i], scratch,
                                    left_row, right_row));
              is_null = child.IsNull();
              if (!is_null) {
                iv = child.value.int_value;
              }
            }
            if (!is_null) {
              int64_t next = 0;
              if (__builtin_add_overflow(group.raw_ints[i], iv, &next)) {
                group.raw_inf_flags[i] = 1;
              } else {
                group.raw_ints[i] = next;
              }
              group.raw_counts[i] += 1;
            }
            break;
          }
          case RawAggKind::kSumDouble:
          case RawAggKind::kAvgDouble: {
            double dv = 0.0;
            bool is_null = false;
            RETURN_IF_FAIL(eval_pair_double(i, scratch, left_row, right_row,
                                            &dv, &is_null));
            if (!is_null) {
              const double next = group.raw_doubles[i] + dv;
              if (std::isinf(next) && !std::isinf(group.raw_doubles[i]) &&
                  !std::isinf(dv)) {
                group.raw_inf_flags[i] = 1;
              }
              group.raw_doubles[i] = next;
              group.raw_counts[i] += 1;
            }
            break;
          }
          case RawAggKind::kCountDistinctInt64: {
            int64_t iv = 0;
            bool is_null = false;
            if (child_shapes[i].direct_column >= 0) {
              const Value& v = get_pair_cell(
                  left_row, right_row,
                  static_cast<slot_t>(child_shapes[i].direct_column));
              is_null = v.IsNull();
              if (!is_null) {
                iv = v.value.int_value;
              }
            } else {
              ASSIGN_OR_RETURN(
                  Value, child,
                  eval_pair_program(child_programs[i], child_shapes[i], scratch,
                                    left_row, right_row));
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
    for (size_t i = 0; i < fusion->aggregates.size(); ++i) {
      AggregateAccumulator& accumulator = local.states[group.state_offset + i];
      if (fusion->count_star[i]) {
        RETURN_IF_FAIL(accumulator.Add(Value(1)));
        continue;
      }
      if (child_shapes[i].direct_column >= 0) {
        RETURN_IF_FAIL(accumulator.Add(
            get_pair_cell(left_row, right_row,
                          static_cast<slot_t>(child_shapes[i].direct_column))));
        continue;
      }
      ASSIGN_OR_RETURN(Value, child,
                       eval_pair_program(child_programs[i], child_shapes[i],
                                         scratch, left_row, right_row));
      RETURN_IF_FAIL(accumulator.Add(child));
    }
    return Status::kSuccess;
  };
  // Fills the pair scratch from the two sides.  `right_row == nullptr` pads
  // the right half with NULLs (unmatched left rows).  Masked fills touch
  // only the cells the compiled programs load.
  const auto fill_scratch = [&](Row& scratch, const Row& left_row,
                                const Row* right_row) {
    if (mask_scratch) {
      for (const slot_t cell : scratch_cells) {
        scratch.values_[cell] =
            cell < left_width
                ? left_row.values_[cell]
                : (right_row != nullptr ? right_row->values_[cell - left_width]
                                        : Value());
      }
      return;
    }
    for (size_t i = 0; i < left_width; ++i) {
      scratch.values_[i] = left_row.values_[i];
    }
    for (size_t i = 0; i < right_width; ++i) {
      scratch.values_[left_width + i] =
          right_row != nullptr ? right_row->values_[i] : Value();
    }
  };
  // Returns true when the pair passed every residual and was accumulated.
  // A residual-rejected pair is NOT a match: LEFT semantics must still
  // null-extend the outer row when every bucket pair for it was rejected.
  const auto process_pair =
      [&](FusedWorkerLocal& local, const Row& left_row, const Row& right_row,
          Row& scratch, size_t* cached_group = nullptr) -> StatusOr<bool> {
    fill_scratch(scratch, left_row, &right_row);
    for (size_t i = 0; i < residual_programs.size(); ++i) {
      ASSIGN_OR_RETURN(Value, verdict,
                       EvaluateProgramValue(residual_programs[i],
                                            residual_shapes[i], scratch));
      if (!Truthy(verdict)) {
        return false;
      }
    }
    RETURN_IF_FAIL(accumulate_into(local, scratch, &left_row, &right_row,
                                   mask_scratch, cached_group));
    return true;
  };
  const auto unmatched_left = [&](FusedWorkerLocal& local, Row& scratch,
                                  const Row& left_row) -> Status {
    fill_scratch(scratch, left_row, nullptr);
    return accumulate_into(local, scratch, &left_row, nullptr, mask_scratch);
  };

  const size_t workers =
      (left.HasSpill() || (int_group_key && int_group_slot < left_width &&
                           !bulk_raw_count && total_left < 96000))
          ? 1
          : std::max<size_t>(
                1, std::min<size_t>({kParallelProbeMaxWorkers,
                                     std::thread::hardware_concurrency(),
                                     ((total_left / 16384)) + 1}));
  std::vector<FusedWorkerLocal> locals(workers);
  std::vector<Row> scratch_storage(locals.size());
  for (Row& scratch : scratch_storage) {
    scratch.values_.resize(left_width + right_width);
  }
  std::atomic<size_t> next_chunk{0};
  const size_t chunk_rows = 8192;
  const size_t chunk_count = (total_left + chunk_rows - 1) / chunk_rows;

  // Build the hash table over the right side once, before the workers spawn
  // (the build side is read-only during the probe).
  std::optional<FlatJoinBuckets<int64_t>> int_buckets;
  std::optional<FlatJoinBuckets<CompositeIntKey, CompositeIntKeyHash>>
      composite_buckets;
  std::optional<FlatJoinBuckets<std::string>> key_buckets;
  std::optional<FlatIntKeyMap<BulkCountValue>> bulk_counts;
  std::vector<BulkCountValue> dense_bulk_counts;
  int64_t dense_bulk_min = 0;
  bool use_dense_bulk = false;
  std::vector<const Row*> dense_int_rows;
  int64_t dense_int_min = 0;
  bool use_dense_int_rows = false;
  bool build_int_keys_unique = true;
  bool packed_2x32_int_key = false;
  struct UniqueIntSlot {
    int64_t key{0};
    uint32_t r_idx{std::numeric_limits<uint32_t>::max()};
    uint32_t pad{0};
  };
  std::vector<UniqueIntSlot> unique_int_table;
  size_t unique_int_mask = 0;
  bool use_unique_int_table = false;
  auto hash_unique_int_key = [](int64_t k) noexcept -> size_t {
    uint64_t h = static_cast<uint64_t>(k) * 0x9e3779b97f4a7c15ULL;
    h ^= h >> 32;
    return static_cast<size_t>(h);
  };
  constexpr auto kInvalidFusedPacked2x32 = static_cast<int64_t>(
      (static_cast<uint64_t>(0x80000000ULL) << 32) | 0x80000000ULL);
  auto fits_fused_i32 = [](int64_t v) noexcept {
    return v >= static_cast<int64_t>(std::numeric_limits<int32_t>::min()) &&
           v <= static_cast<int64_t>(std::numeric_limits<int32_t>::max());
  };
  auto pack_fused_2x32 = [](int64_t k0, int64_t k1) noexcept -> int64_t {
    return static_cast<int64_t>(
        (static_cast<uint64_t>(static_cast<uint32_t>(static_cast<int32_t>(k0)))
         << 32) |
        static_cast<uint64_t>(static_cast<uint32_t>(static_cast<int32_t>(k1))));
  };
  const auto build_buckets = [&]() {
    if (bulk_raw_count) {
      if (!left.HasSpill() && !left.rows.empty()) {
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
            for (const Row* row_ptr : build_rows) {
              const Row& row = *row_ptr;
              if (HasNullKey(row, right_columns)) {
                continue;
              }
              const int64_t build_key = AddJoinKeyBias(
                  IntegerJoinKey(row, right_columns[0]), right_key_offsets[0]);
              if (build_key < min_k || build_key > max_k) {
                continue;
              }
              BulkCountValue& slot = dense_bulk_counts[static_cast<size_t>(
                  static_cast<uint64_t>(build_key) -
                  static_cast<uint64_t>(min_k))];
              ++slot.pairs;
              if (!count_right_column.has_value() ||
                  !row.values_[*count_right_column].IsNull()) {
                ++slot.count;
              }
              slot.row = row_ptr;
            }
            return;
          }
        }
      }
      // The duplicate count and the bucket-head row stand in for the whole
      // bucket chain: the probe never touches an Entry or a build Row.
      bulk_counts.emplace();
      bulk_counts->Reserve(build_rows.size());
      for (const Row* row_ptr : build_rows) {
        const Row& row = *row_ptr;
        if (HasNullKey(row, right_columns)) {
          continue;
        }
        const int64_t build_key = AddJoinKeyBias(
            IntegerJoinKey(row, right_columns[0]), right_key_offsets[0]);
        bool inserted = false;
        BulkCountValue* slot = bulk_counts->TryEmplace(build_key, &inserted);
        const int64_t add = (!count_right_column.has_value() ||
                             !row.values_[*count_right_column].IsNull())
                                ? 1
                                : 0;
        slot->count = inserted ? add : slot->count + add;
        slot->pairs = inserted ? 1 : slot->pairs + 1;
        slot->row = row_ptr;
      }
      return;
    }
    if (integer_key) {
      if (!left.HasSpill() && !build_rows.empty()) {
        int64_t min_k = std::numeric_limits<int64_t>::max();
        int64_t max_k = std::numeric_limits<int64_t>::min();
        for (const Row* row_ptr : build_rows) {
          if (HasNullKey(*row_ptr, right_columns)) {
            continue;
          }
          const int64_t bkey = AddJoinKeyBias(
              IntegerJoinKey(*row_ptr, right_columns[0]), right_key_offsets[0]);
          min_k = std::min(min_k, bkey);
          max_k = std::max(max_k, bkey);
        }
        if (min_k <= max_k) {
          const uint64_t span = static_cast<uint64_t>(max_k) -
                                static_cast<uint64_t>(min_k) + 1ULL;
          if (span <= 1'048'576ULL) {
            dense_int_min = min_k;
            dense_int_rows.assign(static_cast<size_t>(span), nullptr);
            bool unique = true;
            for (const Row* row_ptr : build_rows) {
              if (HasNullKey(*row_ptr, right_columns)) {
                continue;
              }
              const int64_t bkey =
                  AddJoinKeyBias(IntegerJoinKey(*row_ptr, right_columns[0]),
                                 right_key_offsets[0]);
              const auto idx = static_cast<size_t>(
                  static_cast<uint64_t>(bkey) - static_cast<uint64_t>(min_k));
              if (dense_int_rows[idx] != nullptr) {
                unique = false;
                break;
              }
              dense_int_rows[idx] = row_ptr;
            }
            if (unique) {
              use_dense_int_rows = true;
              build_int_keys_unique = true;
              return;
            }
            dense_int_rows.clear();
          }
        }
        if (!right.HasSpill() && build_rows.size() <= 524288) {
          size_t cap = 16;
          while (cap < build_rows.size() * 2) {
            cap <<= 1;
          }
          unique_int_table.assign(cap, UniqueIntSlot{});
          unique_int_mask = cap - 1;
          bool unique = true;
          for (const Row* row_ptr : build_rows) {
            if (HasNullKey(*row_ptr, right_columns)) {
              continue;
            }
            const int64_t bkey =
                AddJoinKeyBias(IntegerJoinKey(*row_ptr, right_columns[0]),
                               right_key_offsets[0]);
            size_t slot = hash_unique_int_key(bkey) & unique_int_mask;
            while (unique_int_table[slot].r_idx !=
                   std::numeric_limits<uint32_t>::max()) {
              if (unique_int_table[slot].key == bkey) {
                unique = false;
                break;
              }
              slot = (slot + 1) & unique_int_mask;
            }
            if (!unique) {
              break;
            }
            unique_int_table[slot].key = bkey;
            unique_int_table[slot].r_idx =
                static_cast<uint32_t>(row_ptr - right.rows.data());
          }
          if (unique) {
            use_unique_int_table = true;
            build_int_keys_unique = true;
            return;
          }
          unique_int_table.clear();
        }
      }
      int_buckets.emplace();
      int_buckets->reserve(build_rows.size());
      for (const Row* row_ptr : build_rows) {
        const Row& row = *row_ptr;
        if (HasNullKey(row, right_columns)) {
          continue;
        }
        const int64_t bkey = AddJoinKeyBias(
            IntegerJoinKey(row, right_columns[0]), right_key_offsets[0]);
        if (!int_buckets->emplace(bkey, &row)) {
          build_int_keys_unique = false;
        }
      }
    } else if (composite_int_key) {
      bool can_pack = (right_columns.size() == 2);
      if (can_pack) {
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
        if (!left.HasSpill() && !right.HasSpill() && !build_rows.empty() &&
            build_rows.size() <= 524288) {
          size_t cap = 16;
          while (cap < build_rows.size() * 2) {
            cap <<= 1;
          }
          unique_int_table.assign(cap, UniqueIntSlot{});
          unique_int_mask = cap - 1;
          bool unique = true;
          for (const Row* row_ptr : build_rows) {
            const Row& row = *row_ptr;
            if (HasNullKey(row, right_columns)) {
              continue;
            }
            const int64_t k0 = AddJoinKeyBias(
                IntegerJoinKey(row, right_columns[0]), right_key_offsets[0]);
            const int64_t k1 = AddJoinKeyBias(
                IntegerJoinKey(row, right_columns[1]), right_key_offsets[1]);
            const int64_t bkey = pack_fused_2x32(k0, k1);
            size_t slot = hash_unique_int_key(bkey) & unique_int_mask;
            while (unique_int_table[slot].r_idx !=
                   std::numeric_limits<uint32_t>::max()) {
              if (unique_int_table[slot].key == bkey) {
                unique = false;
                break;
              }
              slot = (slot + 1) & unique_int_mask;
            }
            if (!unique) {
              break;
            }
            unique_int_table[slot].key = bkey;
            unique_int_table[slot].r_idx =
                static_cast<uint32_t>(row_ptr - right.rows.data());
          }
          if (unique) {
            use_unique_int_table = true;
            build_int_keys_unique = true;
            return;
          }
          unique_int_table.clear();
        }
        int_buckets.emplace();
        int_buckets->reserve(build_rows.size());
        for (const Row* row_ptr : build_rows) {
          const Row& row = *row_ptr;
          if (HasNullKey(row, right_columns)) {
            continue;
          }
          const int64_t k0 = AddJoinKeyBias(
              IntegerJoinKey(row, right_columns[0]), right_key_offsets[0]);
          const int64_t k1 = AddJoinKeyBias(
              IntegerJoinKey(row, right_columns[1]), right_key_offsets[1]);
          int_buckets->emplace(pack_fused_2x32(k0, k1), &row);
        }
      } else {
        composite_buckets.emplace();
        composite_buckets->reserve(build_rows.size());
        for (const Row* row_ptr : build_rows) {
          const Row& row = *row_ptr;
          if (HasNullKey(row, right_columns)) {
            continue;
          }
          composite_buckets->emplace(
              EncodeCompositeIntKey(row, right_columns, &right_key_offsets),
              &row);
        }
      }
    } else {
      key_buckets.emplace();
      key_buckets->reserve(build_rows.size());
      for (const Row* row_ptr : build_rows) {
        const Row& row = *row_ptr;
        if (HasNullKey(row, right_columns)) {
          continue;
        }
        key_buckets->emplace(
            EncodeJoinKey(row, right_columns, &right_key_offsets), &row);
      }
    }
  };
  if (!single_absorbed_relation) {
    build_buckets();
  }

  constexpr uint32_t kInvalidGid = std::numeric_limits<uint32_t>::max();
  std::vector<uint32_t> right_row_gid;
  std::vector<FusedGroup> pre_groups;
  bool identity_right_gid = false;
  bool use_preassigned_build_groups =
      join_type == JoinType::kInner && !left.HasSpill() && !right.HasSpill() &&
      right_only_group_keys && skip_accumulator_states &&
      residual_programs.empty() && !bulk_raw_count && !build_rows.empty();
  if (use_preassigned_build_groups) {
    if (key_programs.empty()) {
      right_row_gid.assign(right.rows.size(), kInvalidGid);
      FusedGroup g;
      g.rep_left = (deferred_left_probe != nullptr || left.rows.empty())
                       ? nullptr
                       : left.rows.data();
      g.rep_right = build_rows[0];
      pre_groups.push_back(std::move(g));
      for (const Row* row_ptr : build_rows) {
        if (!HasNullKey(*row_ptr, right_columns)) {
          right_row_gid[static_cast<size_t>(row_ptr - right.rows.data())] = 0;
        }
      }
    } else if (integer_key && build_int_keys_unique && all_direct_group_keys &&
               !has_count_distinct && right.rows.size() <= 524288 &&
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
      right_row_gid.assign(right.rows.size(), kInvalidGid);
      pre_groups.reserve(build_rows.size());
      for (const Row* row_ptr : build_rows) {
        if (HasNullKey(*row_ptr, right_columns)) {
          continue;
        }
        const auto gid = static_cast<uint32_t>(pre_groups.size());
        right_row_gid[static_cast<size_t>(row_ptr - right.rows.data())] = gid;
        FusedGroup g;
        g.rep_right = row_ptr;
        pre_groups.push_back(std::move(g));
      }
    } else {
      right_row_gid.assign(right.rows.size(), kInvalidGid);
      FusedWorkerLocal pre_local;
      pre_local.int_offsets.Reserve(build_rows.size());
      pre_local.groups.reserve(build_rows.size());
      Row& pre_scratch = scratch_storage[0];
      for (const Row* row_ptr : build_rows) {
        if (HasNullKey(*row_ptr, right_columns)) {
          continue;
        }
        if (!all_direct_group_keys) {
          for (const slot_t cell : scratch_cells) {
            if (cell >= left_width) {
              pre_scratch.values_[cell] = row_ptr->values_[cell - left_width];
            }
          }
        }
        size_t gid = 0;
        const Status st = resolve_group_index(pre_local, pre_scratch, nullptr,
                                              row_ptr, true, nullptr, &gid);
        if (!st.ok()) {
          use_preassigned_build_groups = false;
          right_row_gid.clear();
          pre_groups.clear();
          break;
        }
        right_row_gid[static_cast<size_t>(row_ptr - right.rows.data())] =
            static_cast<uint32_t>(gid);
      }
      if (use_preassigned_build_groups) {
        pre_groups = std::move(pre_local.groups);
      }
    }
  }
  const size_t pre_group_count =
      identity_right_gid ? right.rows.size() : pre_groups.size();
  const bool use_direct_gid_agg = use_preassigned_build_groups &&
                                  !has_count_distinct && pre_group_count > 0 &&
                                  pre_group_count <= 524288;
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
  struct DirectGidWorker {
    std::vector<uint8_t> matched;
    std::vector<double> doubles;
    std::vector<int64_t> ints;
    std::vector<int64_t> counts;
    std::vector<uint8_t> inf_flags;
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
  const size_t agg_count = fusion->aggregates.size();
  bool has_double_raw_agg = false;
  bool has_int_raw_agg = false;
  if (raw_sum_fusion) {
    for (size_t i = 0; i < agg_count && i < raw_agg_kinds.size(); ++i) {
      if (raw_agg_kinds[i] == RawAggKind::kSumDouble ||
          raw_agg_kinds[i] == RawAggKind::kAvgDouble) {
        has_double_raw_agg = true;
      } else if (raw_agg_kinds[i] == RawAggKind::kSumInt64) {
        has_int_raw_agg = true;
      }
    }
  }
  const bool use_shared_direct_gid =
      use_direct_gid_agg && pre_group_count > 4096 && workers > 1;
  const size_t direct_gid_worker_count =
      use_direct_gid_agg ? (use_shared_direct_gid ? 1 : workers) : 0;
  std::vector<DirectGidWorker> direct_gid_workers(direct_gid_worker_count);
  if (use_direct_gid_agg) {
    for (DirectGidWorker& dw : direct_gid_workers) {
      dw.matched.assign(pre_group_count, 0);
      if (raw_count_fusion) {
        dw.counts.assign(pre_group_count, 0);
      } else {
        if (has_double_raw_agg) {
          dw.doubles.assign(pre_group_count * agg_count, 0.0);
        }
        if (has_int_raw_agg) {
          dw.ints.assign(pre_group_count * agg_count, 0);
        }
        dw.counts.assign(pre_group_count * agg_count, 0);
        dw.inf_flags.assign(pre_group_count * agg_count, 0);
      }
    }
  }
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
  const auto accumulate_preassigned_pair = [&](size_t w, Row& scratch,
                                               const Row& left_row,
                                               const Row& right_row) -> Status {
    const auto r_idx = static_cast<size_t>(&right_row - right.rows.data());
    const uint32_t gid = identity_right_gid ? static_cast<uint32_t>(r_idx)
                                            : right_row_gid[r_idx];
    if (gid == kInvalidGid) [[unlikely]] {
      return Status::kSuccess;
    }
    if (!mask_scratch || !scratch_cells.empty()) {
      for (const slot_t cell : scratch_cells) {
        scratch.values_[cell] = cell < left_width
                                    ? left_row.values_[cell]
                                    : right_row.values_[cell - left_width];
      }
    }
    if (use_partitioned_count_distinct) {
      cd_worker_matched[w][gid] = 1;
      int64_t iv = 0;
      bool is_null = false;
      if (child_shapes[0].direct_column >= 0) {
        const Value& v =
            get_pair_cell(&left_row, &right_row,
                          static_cast<slot_t>(child_shapes[0].direct_column));
        is_null = v.IsNull();
        if (!is_null) {
          iv = v.value.int_value;
        }
      } else {
        ASSIGN_OR_RETURN(Value, child,
                         eval_pair_program(child_programs[0], child_shapes[0],
                                           scratch, &left_row, &right_row));
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
    DirectGidWorker& dw = direct_gid_workers[use_shared_direct_gid ? 0 : w];
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
                            !right_row.values_[*count_right_column].IsNull();
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
    if (!use_shared_direct_gid) {
      dw.matched[gid] = 1;
    }
    const size_t base = static_cast<size_t>(gid) * agg_count;
    for (size_t i = 0; i < agg_count; ++i) {
      switch (raw_agg_kinds[i]) {
        case RawAggKind::kCountStar:
          if (use_shared_direct_gid) {
            while (__atomic_exchange_n(&dw.matched[gid], 2, __ATOMIC_ACQUIRE) ==
                   2) {
            }
            dw.counts[base + i] += 1;
            __atomic_store_n(&dw.matched[gid], 1, __ATOMIC_RELEASE);
          } else {
            dw.counts[base + i] += 1;
          }
          break;
        case RawAggKind::kCountValue: {
          bool non_null = false;
          if (child_shapes[i].direct_column >= 0) {
            const Value& v = get_pair_cell(
                &left_row, &right_row,
                static_cast<slot_t>(child_shapes[i].direct_column));
            non_null = !v.IsNull();
          } else {
            ASSIGN_OR_RETURN(
                Value, child,
                eval_pair_program(child_programs[i], child_shapes[i], scratch,
                                  &left_row, &right_row));
            non_null = !child.IsNull();
          }
          if (use_shared_direct_gid) {
            while (__atomic_exchange_n(&dw.matched[gid], 2, __ATOMIC_ACQUIRE) ==
                   2) {
            }
            if (non_null) {
              dw.counts[base + i] += 1;
            }
            __atomic_store_n(&dw.matched[gid], 1, __ATOMIC_RELEASE);
          } else if (non_null) {
            dw.counts[base + i] += 1;
          }
          break;
        }
        case RawAggKind::kSumInt64: {
          int64_t iv = 0;
          bool is_null = false;
          if (child_shapes[i].direct_column >= 0) {
            const Value& v = get_pair_cell(
                &left_row, &right_row,
                static_cast<slot_t>(child_shapes[i].direct_column));
            is_null = v.IsNull();
            if (!is_null) {
              iv = v.value.int_value;
            }
          } else if (child_shapes[i].case_cond !=
                         ProgramShape::CaseCondKind::kNone &&
                     child_shapes[i].case_val ==
                         ProgramShape::CaseValKind::kIntConst1) {
            bool cond_true = false;
            if (eval_case_cond(child_shapes[i], &left_row, &right_row,
                               &cond_true)) [[likely]] {
              iv = cond_true ? 1 : 0;
            } else {
              ASSIGN_OR_RETURN(
                  Value, child,
                  eval_pair_program(child_programs[i], child_shapes[i], scratch,
                                    &left_row, &right_row));
              is_null = child.IsNull();
              if (!is_null) {
                iv = child.value.int_value;
              }
            }
          } else {
            ASSIGN_OR_RETURN(
                Value, child,
                eval_pair_program(child_programs[i], child_shapes[i], scratch,
                                  &left_row, &right_row));
            is_null = child.IsNull();
            if (!is_null) {
              iv = child.value.int_value;
            }
          }
          if (use_shared_direct_gid) {
            while (__atomic_exchange_n(&dw.matched[gid], 2, __ATOMIC_ACQUIRE) ==
                   2) {
            }
            if (!is_null) {
              int64_t next = 0;
              if (__builtin_add_overflow(dw.ints[base + i], iv, &next)) {
                dw.inf_flags[base + i] = 1;
              } else {
                dw.ints[base + i] = next;
              }
              dw.counts[base + i] += 1;
            }
            __atomic_store_n(&dw.matched[gid], 1, __ATOMIC_RELEASE);
          } else if (!is_null) {
            int64_t next = 0;
            if (__builtin_add_overflow(dw.ints[base + i], iv, &next)) {
              dw.inf_flags[base + i] = 1;
            } else {
              dw.ints[base + i] = next;
            }
            dw.counts[base + i] += 1;
          }
          break;
        }
        case RawAggKind::kSumDouble:
        case RawAggKind::kAvgDouble: {
          double dv = 0.0;
          bool is_null = false;
          RETURN_IF_FAIL(eval_pair_double(i, scratch, &left_row, &right_row,
                                          &dv, &is_null));
          if (use_shared_direct_gid) {
            while (__atomic_exchange_n(&dw.matched[gid], 2, __ATOMIC_ACQUIRE) ==
                   2) {
            }
            if (!is_null) {
              const double prev = dw.doubles[base + i];
              const double next = prev + dv;
              if (std::isinf(next) && !std::isinf(prev) && !std::isinf(dv)) {
                dw.inf_flags[base + i] = 1;
              }
              dw.doubles[base + i] = next;
              dw.counts[base + i] += 1;
            }
            __atomic_store_n(&dw.matched[gid], 1, __ATOMIC_RELEASE);
          } else if (!is_null) {
            const double prev = dw.doubles[base + i];
            const double next = prev + dv;
            if (std::isinf(next) && !std::isinf(prev) && !std::isinf(dv)) {
              dw.inf_flags[base + i] = 1;
            }
            dw.doubles[base + i] = next;
            dw.counts[base + i] += 1;
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
    slot_t target_proj_col{0};
    ValueType payload_type{ValueType::kNull};
    bool payload_unsigned{false};
    int64_t min_k{0};
    int64_t max_k{0};
    uint64_t span{1};
    std::array<uint32_t, kDirBuckets + 1> bucket_lo{};
    std::vector<int64_t> blk_max_keys;
    std::vector<PreparedDeferredPkBlock> blocks;
  };
  std::vector<ValueType> def_base_types;
  std::vector<uint8_t> def_overwritten_by_pk;
  std::vector<PreparedDeferredPkLookup> prepared_pk_lookups;
  std::atomic<size_t> next_def_block{0};
  if (deferred_left_probe != nullptr) {
    if (join_type != JoinType::kInner || single_absorbed_relation ||
        left.HasSpill() || (!integer_key && !packed_2x32_int_key) ||
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
      prepared_pk_lookups.push_back(std::move(prep));
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

  auto run_probe = [&](size_t w, FusedWorkerLocal& local,
                       Row& scratch) -> Status {
    if (deferred_left_probe != nullptr) {
      const LSTableStore& store = *deferred_left_probe->store;
      const size_t total_blocks = store.blocks.size();
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
      struct DefPkCursor {
        int64_t last_key{std::numeric_limits<int64_t>::min()};
        bool have_last{false};
        bool last_matched{false};
        bool last_is_null{false};
        int64_t last_raw_bits{0};
        std::string_view last_sv{};
        uint32_t last_nb{0};
        uint32_t last_row_in_blk{0};
      };
      std::array<DefPkCursor, 4> pk_cursors{};
      std::array<const LSTableColumnBlock*, 16> proj_cols{};
      std::array<const int64_t*, 16> proj_int_ptrs{};
      std::array<uint8_t, 16> proj_has_null{};
      const auto resolve_def_pk_lookups = [&](uint32_t r) -> bool {
        for (size_t li = 0; li < prepared_pk_lookups.size(); ++li) {
          const PreparedDeferredPkLookup& lkp = prepared_pk_lookups[li];
          DefPkCursor& cur = pk_cursors[li];
          if (proj_has_null[lkp.pk_col] != 0 &&
              proj_cols[lkp.pk_col]->IsNull(r)) [[unlikely]] {
            return false;
          }
          const int64_t k = proj_int_ptrs[lkp.pk_col][r];
          if (k < lkp.min_k || k > lkp.max_k) [[unlikely]] {
            return false;
          }
          if (!cur.have_last || k != cur.last_key) {
            const auto nb_count = static_cast<uint32_t>(lkp.blocks.size());
            const PreparedDeferredPkBlock* const meta_ptr = lkp.blocks.data();
            uint32_t nb = cur.last_nb;
            if (nb >= nb_count || k > meta_ptr[nb].max_k ||
                k < meta_ptr[nb].min_k) {
              if (nb + 1 < nb_count && k <= meta_ptr[nb + 1].max_k &&
                  k >= meta_ptr[nb + 1].min_k) {
                nb = nb + 1;
              } else {
                const auto b = static_cast<size_t>(
                    (static_cast<__uint128_t>(static_cast<uint64_t>(k) -
                                              static_cast<uint64_t>(lkp.min_k))
                     << PreparedDeferredPkLookup::kDirShift) /
                    lkp.span);
                const uint32_t lo = lkp.bucket_lo[b];
                const uint32_t hi =
                    std::min<uint32_t>(nb_count - 1, lkp.bucket_lo[b + 1] + 1);
                if (hi - lo <= 4) {
                  nb = lo;
                  while (nb < hi && meta_ptr[nb].max_k < k) {
                    ++nb;
                  }
                } else {
                  const auto it =
                      std::lower_bound(lkp.blk_max_keys.begin() + lo,
                                       lkp.blk_max_keys.begin() + hi + 1, k);
                  nb = static_cast<uint32_t>(
                      std::distance(lkp.blk_max_keys.begin(), it));
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
            const PreparedDeferredPkBlock& bm = meta_ptr[nb];
            if (k < bm.min_k || k > bm.max_k) [[unlikely]] {
              cur.last_matched = false;
              return false;
            }
            uint32_t row_in_blk = bm.row_count;
            if (bm.dense_contiguous != 0) {
              row_in_blk = static_cast<uint32_t>(
                  static_cast<uint64_t>(k) - static_cast<uint64_t>(bm.min_k));
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
                cur.last_row_in_blk =
                    static_cast<uint32_t>(found_ptr - begin_ptr);
                if (found_ptr != search_end && *found_ptr == k) [[likely]] {
                  row_in_blk = cur.last_row_in_blk;
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
            cur.last_is_null =
                (bm.has_null_payload != 0) && bm.pay_blk->IsNull(row_in_blk);
            if (!cur.last_is_null) [[likely]] {
              if (lkp.payload_type == ValueType::kVarChar) {
                cur.last_sv = bm.pay_blk->VarcharAt(row_in_blk);
              } else {
                cur.last_raw_bits = bm.pay_cells[row_in_blk];
              }
            }
          } else if (!cur.last_matched) [[unlikely]] {
            return false;
          }
          Value& dst = left_row.values_[lkp.target_proj_col];
          if (cur.last_is_null) [[unlikely]] {
            dst = Value();
          } else if (lkp.payload_type == ValueType::kVarChar) {
            dst.type = ValueType::kVarChar;
            dst.SetCollation(0);
            dst.owned_data.clear();
            dst.value.varchar_value = cur.last_sv;
          } else if (lkp.payload_type == ValueType::kDouble) {
            dst.type = ValueType::kDouble;
            dst.SetCollation(0);
            dst.value.double_value = std::bit_cast<double>(cur.last_raw_bits);
          } else {
            dst.type = lkp.payload_type;
            dst.SetCollation(0);
            if (lkp.payload_unsigned) {
              dst.SetUnsigned();
            }
            dst.value.int_value = cur.last_raw_bits;
          }
        }
        return true;
      };
      const auto materialize_current_left_row = [&](uint32_t r) -> bool {
        if (!prepared_pk_lookups.empty() && !resolve_def_pk_lookups(r)) {
          return false;
        }
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
        return true;
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
      const slot_t jk0 = left_columns[0];
      const slot_t jk1 = packed_2x32_int_key ? left_columns[1] : 0;
      const int64_t off0 = left_key_offsets[0];
      const int64_t off1 = packed_2x32_int_key ? left_key_offsets[1] : 0;
      while (true) {
        const size_t bi =
            next_def_block.fetch_add(1, std::memory_order_relaxed);
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
          const slot_t ext_full = deferred_left_probe->projection[ext.column];
          if (ext_full < blk.columns.size()) {
            const LSTableColumnBlock& mcol = blk.columns[ext_full];
            if (!mcol.has_min_max ||
                ext.group->RejectRange(mcol.min_int, mcol.max_int)) {
              extra_zm_reject = true;
              break;
            }
          }
        }
        if (extra_zm_reject) {
          continue;
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
              .group = ext.group.get(),
          };
        }
        const int64_t* const jk0_ints = proj_int_ptrs[jk0];
        const int64_t* const jk1_ints =
            packed_2x32_int_key ? proj_int_ptrs[jk1] : nullptr;
        const bool jk_has_null =
            proj_has_null[jk0] != 0 ||
            (packed_2x32_int_key && proj_has_null[jk1] != 0);
        for (uint32_t r = 0; r < blk.row_count; ++r) {
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
          if (integer_key) {
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
                  dense_bulk_counts[idx].row != nullptr) {
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
              group.rep_right = bulk->row;
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
            const Row* rptr =
                idx < dense_int_rows.size() ? dense_int_rows[idx] : nullptr;
            if (rptr != nullptr && materialize_current_left_row(r)) {
              ++local.comparisons;
              if (use_preassigned_build_groups) {
                RETURN_IF_FAIL(
                    accumulate_preassigned_pair(w, scratch, left_row, *rptr));
              } else {
                size_t cached_group = kNoCachedGroup;
                ASSIGN_OR_RETURN(bool, accumulated,
                                 process_pair(local, left_row, *rptr, scratch,
                                              &cached_group));
                (void)accumulated;
              }
            }
            continue;
          }
          if (use_unique_int_table) {
            size_t slot = hash_unique_int_key(key) & unique_int_mask;
            const Row* rptr = nullptr;
            while (unique_int_table[slot].r_idx !=
                   std::numeric_limits<uint32_t>::max()) {
              if (unique_int_table[slot].key == key) [[likely]] {
                rptr = &right.rows[unique_int_table[slot].r_idx];
                break;
              }
              slot = (slot + 1) & unique_int_mask;
            }
            if (rptr != nullptr && materialize_current_left_row(r)) {
              ++local.comparisons;
              if (use_preassigned_build_groups) {
                RETURN_IF_FAIL(
                    accumulate_preassigned_pair(w, scratch, left_row, *rptr));
              } else {
                size_t cached_group = kNoCachedGroup;
                ASSIGN_OR_RETURN(bool, accumulated,
                                 process_pair(local, left_row, *rptr, scratch,
                                              &cached_group));
                (void)accumulated;
              }
            }
            continue;
          }
          const auto [first, last] = int_buckets->equal_range(key);
          if (first == last || !materialize_current_left_row(r)) {
            continue;
          }
          if (use_preassigned_build_groups) {
            for (auto iter = first; iter != last; ++iter) {
              ++local.comparisons;
              RETURN_IF_FAIL(accumulate_preassigned_pair(w, scratch, left_row,
                                                         *iter->second));
            }
          } else {
            size_t cached_group = kNoCachedGroup;
            for (auto iter = first; iter != last; ++iter) {
              ++local.comparisons;
              ASSIGN_OR_RETURN(bool, accumulated,
                               process_pair(local, left_row, *iter->second,
                                            scratch, &cached_group));
              (void)accumulated;
            }
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
                process_pair(local, left_row, *iter->second, scratch));
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
    if (single_absorbed_relation) {
      const Row& dummy_right = right.rows[0];
      while (true) {
        const size_t chunk = next_chunk.fetch_add(1);
        if (chunk >= chunk_count) {
          return Status::kSuccess;
        }
        const size_t begin = chunk * chunk_rows;
        const size_t end = std::min(total_left, begin + chunk_rows);
        for (size_t ri = begin; ri < end; ++ri) {
          const Row& left_row = left.rows[ri];
          ++local.comparisons;
          if (use_preassigned_build_groups) {
            RETURN_IF_FAIL(
                accumulate_preassigned_pair(w, scratch, left_row, dummy_right));
          } else {
            size_t cached_group = kNoCachedGroup;
            ASSIGN_OR_RETURN(bool, accumulated,
                             process_pair(local, left_row, dummy_right, scratch,
                                          &cached_group));
            (void)accumulated;
          }
        }
      }
    }
    if (integer_key || packed_2x32_int_key) {
      while (true) {
        const size_t chunk = next_chunk.fetch_add(1);
        if (chunk >= chunk_count) {
          return Status::kSuccess;
        }
        const size_t begin = chunk * chunk_rows;
        const size_t end = std::min(total_left, begin + chunk_rows);
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
                  dense_bulk_counts[idx].row != nullptr) {
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
              group.rep_right = bulk->row;
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
            const Row* rptr =
                idx < dense_int_rows.size() ? dense_int_rows[idx] : nullptr;
            bool matched = false;
            if (rptr != nullptr) {
              ++local.comparisons;
              if (use_preassigned_build_groups) {
                RETURN_IF_FAIL(
                    accumulate_preassigned_pair(w, scratch, left_row, *rptr));
                matched = true;
              } else {
                size_t cached_group = kNoCachedGroup;
                ASSIGN_OR_RETURN(bool, accumulated,
                                 process_pair(local, left_row, *rptr, scratch,
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
            const Row* rptr = nullptr;
            while (true) {
              const UniqueIntSlot& entry = unique_int_table[slot];
              if (entry.r_idx == std::numeric_limits<uint32_t>::max()) {
                break;
              }
              if (entry.key == key) [[likely]] {
                rptr = right.rows.data() + entry.r_idx;
                break;
              }
              slot = (slot + 1) & unique_int_mask;
            }
            bool matched = false;
            if (rptr != nullptr) [[likely]] {
              ++local.comparisons;
              if (use_preassigned_build_groups) {
                RETURN_IF_FAIL(
                    accumulate_preassigned_pair(w, scratch, left_row, *rptr));
                matched = true;
              } else {
                size_t cached_group = kNoCachedGroup;
                ASSIGN_OR_RETURN(bool, accumulated,
                                 process_pair(local, left_row, *rptr, scratch,
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
                                                         *iter->second));
              matched = true;
            } else {
              ASSIGN_OR_RETURN(bool, accumulated,
                               process_pair(local, left_row, *iter->second,
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
      const FlatJoinBuckets<CompositeIntKey, CompositeIntKeyHash>& buckets =
          *composite_buckets;
      while (true) {
        const size_t chunk = next_chunk.fetch_add(1);
        if (chunk >= chunk_count) {
          return Status::kSuccess;
        }
        const size_t begin = chunk * chunk_rows;
        const size_t end = std::min(total_left, begin + chunk_rows);
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
                                                         *iter->second));
              matched = true;
            } else {
              ASSIGN_OR_RETURN(bool, accumulated,
                               process_pair(local, left_row, *iter->second,
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
    const FlatJoinBuckets<std::string>& buckets = *key_buckets;
    while (true) {
      const size_t chunk = next_chunk.fetch_add(1);
      if (chunk >= chunk_count) {
        return Status::kSuccess;
      }
      const size_t begin = chunk * chunk_rows;
      const size_t end = std::min(total_left, begin + chunk_rows);
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
                                                       *iter->second));
            matched = true;
          } else {
            ASSIGN_OR_RETURN(bool, accumulated,
                             process_pair(local, left_row, *iter->second,
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
  if (locals.size() == 1) {
    // Small grouped joins need no helper or transfer. Besides thread startup,
    // this preserves the caller's expression scratch capacity across queries.
    locals[0].error = run_probe(0, locals[0], scratch_storage[0]);
  } else {
    std::vector<std::jthread> threads;
    threads.reserve(locals.size());
    for (size_t w = 0; w < locals.size(); ++w) {
      threads.emplace_back([&, w] {
        Status status = run_probe(w, locals[w], scratch_storage[w]);
        if (status != Status::kSuccess) {
          locals[w].error = status;
        }
      });
    }
  }
  for (const FusedWorkerLocal& local : locals) {
    RETURN_IF_FAIL(local.error);
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
    const auto key_cell_of = [&](const FusedGroup& g,
                                 size_t k) -> const Value& {
      if (!g.key.values_.empty()) {
        return g.key.values_[k];
      }
      return get_pair_cell(g.rep_left, g.rep_right,
                           static_cast<slot_t>(key_shapes[k].direct_column));
    };
    if (!int_key_indices.empty()) {
      for (const size_t ik : int_key_indices) {
        const Value& x = key_cell_of(a, ik);
        const Value& y = key_cell_of(b, ik);
        if (x.type != y.type ||
            (!x.IsNull() && x.value.int_value != y.value.int_value)) {
          return false;
        }
      }
    }
    if (int_key_indices.size() < key_programs.size()) {
      for (size_t k = 0; k < key_programs.size(); ++k) {
        const Value& x = key_cell_of(a, k);
        const Value& y = key_cell_of(b, k);
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
  std::deque<AggregateAccumulator> states;
  if (use_direct_gid_agg) {
    groups.reserve(pre_group_count);
    for (size_t gid = 0; gid < pre_group_count; ++gid) {
      bool any_matched = false;
      for (size_t w = 0; w < direct_gid_worker_count; ++w) {
        if (direct_gid_workers[w].matched[gid] != 0) {
          any_matched = true;
          break;
        }
      }
      if (!any_matched) {
        continue;
      }
      FusedGroup grp;
      if (identity_right_gid) {
        grp.rep_right = &right.rows[gid];
      } else {
        grp = std::move(pre_groups[gid]);
      }
      if (raw_count_fusion) {
        int64_t total_cnt = 0;
        for (size_t w = 0; w < direct_gid_worker_count; ++w) {
          total_cnt += direct_gid_workers[w].counts[gid];
        }
        grp.raw_count = total_cnt;
      } else {
        const size_t base = gid * agg_count;
        for (size_t w = 0; w < direct_gid_worker_count; ++w) {
          const DirectGidWorker& dw = direct_gid_workers[w];
          if (dw.matched[gid] == 0) {
            continue;
          }
          for (size_t i = 0; i < agg_count; ++i) {
            switch (raw_agg_kinds[i]) {
              case RawAggKind::kCountStar:
              case RawAggKind::kCountValue:
                grp.raw_counts[i] += dw.counts[base + i];
                break;
              case RawAggKind::kSumInt64: {
                int64_t next = 0;
                if (__builtin_add_overflow(grp.raw_ints[i], dw.ints[base + i],
                                           &next)) {
                  grp.raw_inf_flags[i] = 1;
                } else {
                  grp.raw_ints[i] = next;
                }
                grp.raw_counts[i] += dw.counts[base + i];
                grp.raw_inf_flags[i] |= dw.inf_flags[base + i];
                break;
              }
              case RawAggKind::kSumDouble:
              case RawAggKind::kAvgDouble: {
                const double next = grp.raw_doubles[i] + dw.doubles[base + i];
                if (std::isinf(next) && !std::isinf(grp.raw_doubles[i]) &&
                    !std::isinf(dw.doubles[base + i])) {
                  grp.raw_inf_flags[i] = 1;
                }
                grp.raw_doubles[i] = next;
                grp.raw_counts[i] += dw.counts[base + i];
                grp.raw_inf_flags[i] |= dw.inf_flags[base + i];
                break;
              }
              case RawAggKind::kCountDistinctInt64:
              case RawAggKind::kNone:
                break;
            }
          }
        }
      }
      groups.push_back(std::move(grp));
    }
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
      grp.raw_counts[0] = cd_counts[gid];
      groups.push_back(std::move(grp));
    }
  }
  size_t total_worker_groups = 0;
  for (const FusedWorkerLocal& local : locals) {
    total_worker_groups += local.groups.size();
  }
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
      const Value* single_int_cell = nullptr;
      if (int_group_key) {
        if (group.key.values_.size() == 1) {
          single_int_cell = group.key.values_.data();
        } else if (all_direct_group_keys &&
                   (group.rep_left != nullptr || group.rep_right != nullptr)) {
          single_int_cell =
              &get_pair_cell(group.rep_left, group.rep_right, int_group_slot);
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
        } else if (same_group_key(groups[*slot], group)) [[likely]] {
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
        FusedGroup& destination = groups[target_idx];
        for (size_t i = 0; i < fusion->aggregates.size(); ++i) {
          switch (raw_agg_kinds[i]) {
            case RawAggKind::kCountStar:
            case RawAggKind::kCountValue:
              destination.raw_counts[i] += group.raw_counts[i];
              break;
            case RawAggKind::kSumInt64: {
              int64_t next = 0;
              if (__builtin_add_overflow(destination.raw_ints[i],
                                         group.raw_ints[i], &next)) {
                destination.raw_inf_flags[i] = 1;
              } else {
                destination.raw_ints[i] = next;
              }
              destination.raw_counts[i] += group.raw_counts[i];
              destination.raw_inf_flags[i] |= group.raw_inf_flags[i];
              break;
            }
            case RawAggKind::kSumDouble:
            case RawAggKind::kAvgDouble: {
              const double next =
                  destination.raw_doubles[i] + group.raw_doubles[i];
              if (std::isinf(next) && !std::isinf(destination.raw_doubles[i]) &&
                  !std::isinf(group.raw_doubles[i])) {
                destination.raw_inf_flags[i] = 1;
              }
              destination.raw_doubles[i] = next;
              destination.raw_counts[i] += group.raw_counts[i];
              destination.raw_inf_flags[i] |= group.raw_inf_flags[i];
              break;
            }
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
  // A keyless (global) aggregate over an empty join output still emits one
  // row: accumulators finish to their empty-input values (SUM -> NULL,
  // COUNT -> 0), exactly like the scalar AggregateExecutor.  Without this the
  // fused shape would drop the row entirely.
  if (fusion->group_keys.empty() && groups.empty()) {
    FusedGroup group;
    if (!skip_accumulator_states) {
      group.state_offset = 0;
      for (size_t i = 0; i < fusion->aggregates.size(); ++i) {
        states.emplace_back(fusion->aggregates[i], aggregate_shapes[i]);
      }
    }
    group.representative =
        Row(std::vector<Value>(result.schema.ColumnCount(), Value()));
    groups.push_back(std::move(group));
  }

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
  };
  std::vector<DirectEmitItem> direct_emit;
  bool can_direct_emit = (result.using_columns == nullptr);
  if (can_direct_emit) {
    direct_emit.reserve(fusion->select_items.size());
    for (const Expression& item : fusion->select_items) {
      if (!item) {
        can_direct_emit = false;
        break;
      }
      if (item->Type() == TypeTag::kColumnValue) {
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
        direct_emit.push_back({.is_agg = true,
                               .index = static_cast<size_t>(std::distance(
                                   fusion->aggregates.begin(), found))});
      } else {
        can_direct_emit = false;
        break;
      }
    }
  }
  const auto finish_raw_agg = [&](const FusedGroup& group,
                                  size_t i) -> StatusOr<Value> {
    if (group.raw_inf_flags[i]) {
      return StatusError(StatusCode::kIsInfinity, "infinity");
    }
    switch (raw_agg_kinds[i]) {
      case RawAggKind::kCountStar:
      case RawAggKind::kCountValue:
        return Value(group.raw_counts[i]);
      case RawAggKind::kSumInt64:
        return group.raw_counts[i] == 0 ? Value() : Value(group.raw_ints[i]);
      case RawAggKind::kSumDouble:
        return group.raw_counts[i] == 0 ? Value() : Value(group.raw_doubles[i]);
      case RawAggKind::kAvgDouble:
        return group.raw_counts[i] == 0
                   ? Value()
                   : Value(group.raw_doubles[i] /
                           static_cast<double>(group.raw_counts[i]));
      case RawAggKind::kCountDistinctInt64:
        return Value(static_cast<int64_t>(
            group.raw_int_set ? group.raw_int_set->Size()
                              : static_cast<size_t>(group.raw_counts[i])));
      case RawAggKind::kNone:
        break;
    }
    return Value();
  };
  const auto rep_cell = [&](const FusedGroup& grp,
                            size_t col_idx) -> const Value& {
    if (!grp.representative.values_.empty()) {
      return grp.representative.values_[col_idx];
    }
    if (col_idx < left_width && grp.rep_left == nullptr &&
        grp.rep_right != nullptr) {
      for (const EqualityKey& eq : equality_keys) {
        if (eq.left == col_idx && eq.left_offset == 0 && eq.right_offset == 0 &&
            !eq.null_safe) {
          return grp.rep_right->values_[eq.right];
        }
      }
      return kNullCell;
    }
    return get_pair_cell(grp.rep_left, grp.rep_right,
                         static_cast<slot_t>(col_idx));
  };
  if (fusion->post_having_active && raw_sum_fusion &&
      fusion->post_having_agg_slot < fusion->aggregates.size()) {
    const slot_t agg_slot = fusion->post_having_agg_slot;
    const RawAggKind akind = raw_agg_kinds[agg_slot];
    if (akind == RawAggKind::kSumDouble || akind == RawAggKind::kSumInt64) {
      bool any_inf = false;
      for (const FusedGroup& grp : groups) {
        if (grp.raw_inf_flags[agg_slot] != 0) {
          any_inf = true;
          break;
        }
      }
      if (!any_inf) {
        auto group_sum_val = [&](const FusedGroup& grp) noexcept -> double {
          return akind == RawAggKind::kSumDouble
                     ? grp.raw_doubles[agg_slot]
                     : static_cast<double>(grp.raw_ints[agg_slot]);
        };
        double threshold = fusion->post_having_constant_or_factor;
        bool valid_threshold = true;
        if (fusion->post_having_is_self_sum_factor) {
          double grand_total = 0.0;
          bool any_nonnull = false;
          for (const FusedGroup& grp : groups) {
            if (grp.raw_counts[agg_slot] > 0) {
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
          for (size_t gi = 0; gi < groups.size(); ++gi) {
            if (groups[gi].raw_counts[agg_slot] <= 0) {
              continue;
            }
            const double val = group_sum_val(groups[gi]);
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
              if (out != gi) {
                groups[out] = std::move(groups[gi]);
              }
              ++out;
            }
          }
          groups.resize(out);
          fusion->post_having_applied = true;
        }
      }
    } else if (fusion->post_having_is_self_sum_factor && groups.empty()) {
      fusion->post_having_applied = true;
    }
  }
  if (fusion->topn_limit > 0 && fusion->topn_limit < groups.size() &&
      !fusion->output_order.empty() && can_direct_emit &&
      (raw_count_fusion || raw_sum_fusion)) {
    struct FastTopNTerm {
      size_t col_idx{0};
      SortKeyOrder order;
    };
    std::vector<FastTopNTerm> topn_terms;
    topn_terms.reserve(fusion->output_order.size());
    bool can_topn = true;
    if (raw_sum_fusion) {
      for (const FusedGroup& g : groups) {
        for (size_t i = 0; i < fusion->aggregates.size(); ++i) {
          if (g.raw_inf_flags[i] != 0) {
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
      std::vector<size_t> group_order(groups.size());
      for (size_t gi = 0; gi < groups.size(); ++gi) {
        group_order[gi] = gi;
      }
      const auto cmp_groups = [&](size_t ga, size_t gb) -> bool {
        const FusedGroup& a = groups[ga];
        const FusedGroup& b = groups[gb];
        for (const FastTopNTerm& t : topn_terms) {
          const DirectEmitItem& d = direct_emit[t.col_idx];
          int cmp = 0;
          if (!d.is_agg) {
            cmp = t.order.Compare(rep_cell(a, d.index), rep_cell(b, d.index));
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
      std::vector<FusedGroup> top_groups;
      top_groups.reserve(fusion->topn_limit);
      for (size_t k = 0; k < fusion->topn_limit; ++k) {
        top_groups.push_back(std::move(groups[group_order[k]]));
      }
      groups = std::move(top_groups);
    }
  }
  result.rows.reserve(groups.size());
  std::vector<Value> finished_aggs;
  if (can_direct_emit && !raw_count_fusion) {
    finished_aggs.reserve(fusion->aggregates.size());
  }
  Row eval_rep_row;
  if (!can_direct_emit) {
    eval_rep_row.values_.resize(result.schema.ColumnCount());
  }
  for (const FusedGroup& group : groups) {
    if (can_direct_emit) {
      Row output_row;
      output_row.values_.reserve(direct_emit.size());
      if (raw_count_fusion) {
        for (const DirectEmitItem& d : direct_emit) {
          output_row.values_.push_back(d.is_agg ? Value(group.raw_count)
                                                : rep_cell(group, d.index));
        }
      } else if (raw_sum_fusion) {
        finished_aggs.clear();
        for (size_t i = 0; i < fusion->aggregates.size(); ++i) {
          ASSIGN_OR_RETURN(Value, finished, finish_raw_agg(group, i));
          finished_aggs.push_back(std::move(finished));
        }
        for (const DirectEmitItem& d : direct_emit) {
          output_row.values_.push_back(d.is_agg ? finished_aggs[d.index]
                                                : rep_cell(group, d.index));
        }
      } else {
        finished_aggs.clear();
        for (size_t i = 0; i < fusion->aggregates.size(); ++i) {
          AggregateAccumulator& accumulator = states[group.state_offset + i];
          ASSIGN_OR_RETURN(Value, finished, accumulator.TryFinish());
          finished_aggs.push_back(std::move(finished));
        }
        for (const DirectEmitItem& d : direct_emit) {
          output_row.values_.push_back(d.is_agg ? finished_aggs[d.index]
                                                : rep_cell(group, d.index));
        }
      }
      result.AddRow(std::move(output_row));
      continue;
    }
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
    const Row* scope_row = &group.representative;
    if (group.representative.values_.empty()) {
      for (size_t c = 0; c < result.schema.ColumnCount(); ++c) {
        eval_rep_row.values_[c] = rep_cell(group, c);
      }
      scope_row = &eval_rep_row;
    }
    Scope scope{.row = scope_row, .schema = &result.schema, .outer = outer};
    Row output_row;
    output_row.values_.reserve(fusion->select_items.size());
    for (const Expression& item : fusion->select_items) {
      ASSIGN_OR_RETURN(
          Value, value,
          (TryEvaluate(item, scope, &aggregate_results, context, ctes)));
      output_row.values_.push_back(std::move(value));
    }
    result.AddRow(std::move(output_row));
  }
  if (!result.rows.empty()) {
    for (size_t i = 0; i < output_columns.size(); ++i) {
      output_columns[i] =
          Column(output_columns[i].Name(), ValueTypeOf(result.rows[0][i]));
      output_columns[i].SetUnsigned(result.rows[0][i].IsUnsigned());
    }
  }
  result.schema = Schema("", std::move(output_columns));
  RETURN_IF_FAIL(result.FinishSpill());
  RETURN_IF_FAIL(result.GetStatus());
  result.peak_intermediate_rows =
      std::max(result.peak_intermediate_rows, result.TotalRows());
  if (context.execution_runtime() != nullptr) {
    ExecutionRuntime& runtime = *context.execution_runtime();
    runtime.join_ms += ElapsedMs(join_begin);
    ++runtime.grouped_join_fusions;
  }
  if (debug_fusion) {
    std::cerr << "[fusion] fused groups=" << groups.size() << "\n";
  }
  return std::optional<Relation>(std::move(result));
}

}  // namespace tinylamb::relational_detail
