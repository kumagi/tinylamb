/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_DETAIL_TYPED_STREAM_AGGREGATE_HPP
#define TINYLAMB_EXECUTOR_DETAIL_TYPED_STREAM_AGGREGATE_HPP

#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "common/constants.hpp"
#include "common/runtime_filter.hpp"
#include "common/status_or.hpp"
#include "database/transaction_context.hpp"
#include "executor/detail/planning_heuristics.hpp"
#include "executor/detail/relation.hpp"
#include "executor/detail/scan_filter.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/binary_expression.hpp"
#include "expression/bytecode.hpp"
#include "expression/expression.hpp"
#include "query/statement.hpp"
#include "table/scan_options.hpp"
#include "table/table.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb::relational_detail {

struct TypedFilterCompare {
  enum class StringMatchKind : uint8_t { kExact, kPrefix, kSuffix, kContains };
  slot_t left{0};
  slot_t right{0};
  BinaryOperation op{BinaryOperation::kEquals};
  bool is_double{false};
  bool constant_payload{false};
  int64_t int_constant{0};
  double double_constant{0.0};
  bool string_payload{false};
  std::string string_constant{};
  StringMatchKind string_match{StringMatchKind::kExact};
};

// The compiler admits only literal LIKE segments with optional edge '%'.
// These byte comparisons match the AST's no-'_' LIKE path, without creating
// owned Values. NULL rejection remains the caller's responsibility.
inline bool MatchTypedStringCompare(
    std::string_view value, const TypedFilterCompare& compare) noexcept {
  const std::string_view literal = compare.string_constant;
  bool matched = false;
  switch (compare.string_match) {
    case TypedFilterCompare::StringMatchKind::kExact:
      matched = value == literal;
      break;
    case TypedFilterCompare::StringMatchKind::kPrefix:
      matched = value.starts_with(literal);
      break;
    case TypedFilterCompare::StringMatchKind::kSuffix:
      matched = value.ends_with(literal);
      break;
    case TypedFilterCompare::StringMatchKind::kContains:
      matched = value.find(literal) != std::string_view::npos;
      break;
  }
  return compare.op == BinaryOperation::kNotEquals ||
                 compare.op == BinaryOperation::kNotLike
             ? !matched
             : matched;
}

struct TypedConditionalInput {
  TypedFilterCompare condition;
  std::optional<slot_t> then_column;
  int64_t then_int_constant{0};
  ValueType result_type{ValueType::kNull};
};

struct TypedDoubleExprOp {
  enum class Kind : uint8_t {
    kLoadDoubleCol,
    kLoadIntColAsDouble,
    kConstant,
    kAdd,
    kSub,
    kMul,
  } kind{Kind::kConstant};
  slot_t column{0};
  double constant{0.0};
};

struct TypedDoubleExprInput {
  enum class Shape : uint8_t {
    kGeneric = 0,
    kSingleDoubleCol,
    kMulTwoDoubleCols,
    kMulConstMinusCol,
    kMulConstMinusColMulConstPlusCol,
  } shape{Shape::kGeneric};
  slot_t col0{0};
  slot_t col1{0};
  slot_t col2{0};
  double k0{0.0};
  double k1{0.0};
  std::vector<TypedDoubleExprOp> ops;
};

struct TypedIntExprOp {
  enum class Kind : uint8_t {
    kLoadIntCol,
    kConstant,
    kAdd,
    kSub,
    kMul,
  } kind{Kind::kConstant};
  slot_t column{0};
  int64_t constant{0};
};

struct TypedIntExprInput {
  enum class Shape : uint8_t {
    kGeneric = 0,
    kSingleIntCol,
    kMulTwoIntCols,
    kMulTwoIntColsPlusCol,
  } shape{Shape::kGeneric};
  slot_t col0{0};
  slot_t col1{0};
  slot_t col2{0};
  std::vector<TypedIntExprOp> ops;
};

struct TypedAggregateInput {
  const AggregateExpression* aggregate{nullptr};
  std::optional<slot_t> column;
  std::optional<TypedConditionalInput> conditional;
  std::optional<TypedDoubleExprInput> double_expr;
  std::optional<TypedIntExprInput> int_expr;
  std::optional<BytecodeProgram> child;
};

enum class SlimAggKind : uint8_t {
  kNone = 0,
  kCount,
  kSumInt64,
  kSumDouble,
  kAvgDouble,
  kCountDistinctInt64,
  kCountDistinctDouble,
};

[[nodiscard]] constexpr bool IsSlimDistinctKind(SlimAggKind kind) {
  return kind == SlimAggKind::kCountDistinctInt64 ||
         kind == SlimAggKind::kCountDistinctDouble;
}

constexpr size_t kSlimDistinctSlots = 8;

struct TypedEmitItem {
  bool is_key{false};
  size_t index{0};
  std::optional<Value> constant{std::nullopt};
  std::optional<size_t> dependent_key{std::nullopt};
};

struct TypedInListCompare {
  slot_t column{0};
  bool is_double{false};
  bool is_string{false};
  size_t prefix_len{0};
  std::vector<int64_t> int_values;
  std::vector<double> double_values;
  std::vector<std::string> strings;
};

struct TypedAggregationPlan {
  bool eligible{false};
  std::vector<slot_t> key_offsets;
  // Integer expressions determined by the plain group keys. Evaluate once
  // for every completed group, before HAVING/TopN can hide an overflow.
  std::vector<BytecodeProgram> dependent_key_programs;
  // Aligned checked integer kernels for non-NULL dictionary inputs. Bytecode
  // remains the fallback for NULL and failed fast evaluation.
  std::vector<TypedIntExprInput> dependent_key_inputs;
  std::vector<TypedAggregateInput> aggregates;
  bool slim{false};
  std::vector<SlimAggKind> slim_kinds;
  std::vector<TypedEmitItem> emit_items;
  std::vector<TypedEmitItem> order_items;
  std::vector<bool> order_ascending;
  std::vector<bool> order_nulls_first;
  std::vector<BytecodeProgram> filter_programs;
  std::vector<TypedFilterCompare> filter_compares;
  // AND of disjunctions; each disjunction is an OR of non-throwing compare
  // conjunctions. NULL comparisons cannot make an AND branch pass.
  std::vector<std::vector<std::vector<TypedFilterCompare>>> filter_disjunctions;
  std::vector<TypedInListCompare> inlist_compares;
};

struct PositionalHaving {
  size_t agg_index{0};
  BinaryOperation op{BinaryOperation::kEquals};
  Value constant;
  bool has_constant{false};
};

bool HavingReferencesOnlyKeysAndAggregates(
    const Expression& expression, const Schema& schema,
    const std::vector<slot_t>& key_offsets);

bool MatchTypedStringInList(std::string_view v,
                            const TypedInListCompare& list) noexcept;

bool DecideTypedFilterCompare(const Value& left, const Value& right,
                              const TypedFilterCompare& compare);

template <typename IsNullFn, typename MakeValueFn>
inline bool DecideTypedFilterCompareCells(const TypedFilterCompare& compare,
                                          const int64_t* cells,
                                          IsNullFn&& is_null,
                                          MakeValueFn&& make_value) {
  if (is_null(compare.left)) {
    return false;
  }
  if (compare.string_payload) {
    const Value left = make_value(compare.left);
    return DecideTypedFilterCompare(left, left, compare);
  }
  if (compare.constant_payload) {
    if (compare.is_double) {
      const auto a = std::bit_cast<double>(cells[compare.left]);
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
    const int64_t a = cells[compare.left];
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
  if (is_null(compare.right)) {
    return false;
  }
  if (compare.is_double) {
    const auto a = std::bit_cast<double>(cells[compare.left]);
    const auto b = std::bit_cast<double>(cells[compare.right]);
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
  const int64_t a = cells[compare.left];
  const int64_t b = cells[compare.right];
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

template <typename IsNullFn>
inline bool EvaluateTypedDoubleExprCells(const TypedDoubleExprInput& expr,
                                         const int64_t* cells,
                                         IsNullFn&& is_null, double* out) {
  switch (expr.shape) {
    case TypedDoubleExprInput::Shape::kSingleDoubleCol:
      if (is_null(expr.col0)) {
        return false;
      }
      *out = std::bit_cast<double>(cells[expr.col0]);
      return true;
    case TypedDoubleExprInput::Shape::kMulTwoDoubleCols:
      if (is_null(expr.col0) || is_null(expr.col1)) {
        return false;
      }
      *out = std::bit_cast<double>(cells[expr.col0]) *
             std::bit_cast<double>(cells[expr.col1]);
      return true;
    case TypedDoubleExprInput::Shape::kMulConstMinusCol:
      if (is_null(expr.col0) || is_null(expr.col1)) {
        return false;
      }
      *out = std::bit_cast<double>(cells[expr.col0]) *
             (expr.k0 - std::bit_cast<double>(cells[expr.col1]));
      return true;
    case TypedDoubleExprInput::Shape::kMulConstMinusColMulConstPlusCol:
      if (is_null(expr.col0) || is_null(expr.col1) || is_null(expr.col2)) {
        return false;
      }
      *out = std::bit_cast<double>(cells[expr.col0]) *
             (expr.k0 - std::bit_cast<double>(cells[expr.col1])) *
             (expr.k1 + std::bit_cast<double>(cells[expr.col2]));
      return true;
    case TypedDoubleExprInput::Shape::kGeneric:
      break;
  }
  std::array<double, 16> stack{};
  size_t sp = 0;
  for (const TypedDoubleExprOp& op : expr.ops) {
    switch (op.kind) {
      case TypedDoubleExprOp::Kind::kLoadDoubleCol:
        if (is_null(op.column)) {
          return false;
        }
        stack[sp++] = std::bit_cast<double>(cells[op.column]);
        break;
      case TypedDoubleExprOp::Kind::kLoadIntColAsDouble:
        if (is_null(op.column)) {
          return false;
        }
        stack[sp++] = static_cast<double>(cells[op.column]);
        break;
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
}

template <typename IsNullFn>
inline bool EvaluateTypedIntExprCells(const TypedIntExprInput& expr,
                                      const int64_t* cells, IsNullFn&& is_null,
                                      int64_t* out, Status* error) {
  std::array<int64_t, 16> stack{};
  size_t sp = 0;
  for (const TypedIntExprOp& op : expr.ops) {
    switch (op.kind) {
      case TypedIntExprOp::Kind::kLoadIntCol:
        if (is_null(op.column)) {
          return false;
        }
        stack[sp++] = cells[op.column];
        break;
      case TypedIntExprOp::Kind::kConstant:
        stack[sp++] = op.constant;
        break;
      case TypedIntExprOp::Kind::kAdd: {
        int64_t v = 0;
        if (__builtin_add_overflow(stack[sp - 2], stack[sp - 1], &v)) {
          *error =
              StatusError(StatusCode::kIsInfinity, "integer overflow on '+'");
          return false;
        }
        stack[sp - 2] = v;
        --sp;
        break;
      }
      case TypedIntExprOp::Kind::kSub: {
        int64_t v = 0;
        if (__builtin_sub_overflow(stack[sp - 2], stack[sp - 1], &v)) {
          *error =
              StatusError(StatusCode::kIsInfinity, "integer overflow on '-'");
          return false;
        }
        stack[sp - 2] = v;
        --sp;
        break;
      }
      case TypedIntExprOp::Kind::kMul: {
        int64_t v = 0;
        if (__builtin_mul_overflow(stack[sp - 2], stack[sp - 1], &v)) {
          *error =
              StatusError(StatusCode::kIsInfinity, "integer overflow on '*'");
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
}

inline bool TryPackShortVarcharSpan(std::string_view sv, uint32_t* out) {
  if (sv.size() > 3 || !PlainStringComparable(sv)) {
    return false;
  }
  auto part = static_cast<uint32_t>(sv.size() + 1);
  for (size_t i = 0; i < sv.size(); ++i) {
    part |= static_cast<uint32_t>(static_cast<uint8_t>(sv[i])) << ((i + 1) * 8);
  }
  *out = part;
  return true;
}

std::optional<TypedFilterCompare> TryCompileTypedFilterCompare(
    const Expression& predicate, const Schema& scan_schema,
    bool allow_const_fold);

std::optional<TypedConditionalInput> TryCompileTypedConditionalInput(
    const Expression& expr, const Schema& scan_schema);

std::optional<TypedDoubleExprInput> TryCompileTypedDoubleExpr(
    const Expression& expr, const Schema& scan_schema);

std::optional<TypedIntExprInput> TryCompileTypedIntExpr(
    const Expression& expr, const Schema& scan_schema);

Value EvaluateTypedConditionalOnRow(const TypedConditionalInput& cond,
                                    const Row& row);

Value EvaluateTypedDoubleExprOnRow(const TypedDoubleExprInput& expr,
                                   const Row& row);

StatusOr<Value> EvaluateTypedIntExprOnRow(const TypedIntExprInput& expr,
                                          const Row& row);

std::optional<PositionalHaving> TryCompilePositionalHaving(
    const Expression& having,
    const std::vector<TypedAggregateInput>& aggregates);

bool TryCompilePositionalHavingConjuncts(
    const Expression& having,
    const std::vector<TypedAggregateInput>& aggregates,
    std::vector<PositionalHaving>* out);

TypedAggregationPlan PlanTypedAggregation(
    const SelectStatement& stmt, const Expression& having_expr,
    const Schema& scan_schema,
    const std::vector<const AggregateExpression*>& aggregate_expressions,
    const std::vector<Expression>& scan_predicates);

StatusOr<std::optional<std::vector<Relation>>> TryCompactGroupingSets(
    TransactionContext& context,
    const std::vector<const SelectStatement*>& sets, const Relation& input);

StatusOr<std::optional<Relation>> TryCompactRelationAggregate(
    TransactionContext& context, const SelectStatement& statement,
    const Relation& input,
    const std::vector<const AggregateExpression*>& aggregates);

// Partial COUNT(*) by physical dictionary code, followed by one scalar key
// evaluation per observed value and canonical regrouping. No shared scan-cache
// publication; callers must decline reusable sources and HAVING.
StatusOr<std::optional<Relation>> TryDictionaryExpressionCountTopN(
    TransactionContext& context, const SelectStatement& statement,
    const Table& table, const std::vector<slot_t>& projection,
    const Schema& qualified_schema,
    const std::vector<const AggregateExpression*>& aggregates,
    const std::vector<Expression>& predicates, const CteMap& ctes);

// Fold a single encoded VARCHAR input into first-occurrence-ordered values
// and multiplicities. Eligibility restricts the caller's aggregates to COUNT,
// MIN/MAX and exact bounded BYTE_LENGTH averages. The callback applies the
// original row filter before evaluating any key or aggregate expression.
StatusOr<bool> TryFoldDictionaryAggregateRows(
    TransactionContext& context, const SelectStatement& statement,
    const Table& table, const std::vector<slot_t>& projection,
    const Schema& schema,
    const std::vector<const AggregateExpression*>& aggregates,
    const std::vector<Expression>& predicates,
    const std::function<Status(const Row&, int64_t)>& consume);

StatusOr<Relation> RunTypedParallelStreamAggregate(
    TransactionContext& context, const SelectStatement& stmt,
    const Expression& having_expr, const Table& table,
    const std::string& table_name, bool reusable,
    const std::vector<slot_t>& projection, const Schema& qualified_schema,
    const Schema& table_schema,
    const std::vector<IntegerPeekCompare>& peek_compares,
    const std::vector<const AggregateExpression*>& aggregate_expressions,
    const CteMap& ctes, TypedAggregationPlan plan,
    const RuntimeFilterGroup* stream_runtime_filter = nullptr,
    ValueType stream_runtime_filter_type = ValueType::kNull,
    std::optional<slot_t> stream_key_column = std::nullopt,
    const PrebuiltMembershipPredicate* stream_prebuilt_membership = nullptr);

GroupedJoinFusionSpec BuildGroupedJoinFusionSpec(
    const SelectStatement& statement);

}  // namespace tinylamb::relational_detail

#endif  // TINYLAMB_EXECUTOR_DETAIL_TYPED_STREAM_AGGREGATE_HPP
