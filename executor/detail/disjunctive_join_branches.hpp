/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_DETAIL_DISJUNCTIVE_JOIN_BRANCHES_HPP
#define TINYLAMB_EXECUTOR_DETAIL_DISJUNCTIVE_JOIN_BRANCHES_HPP

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/platform/process.hpp"
#include "common/constants.hpp"
#include "expression/binary_expression.hpp"
#include "expression/collation.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/parameter_slot.hpp"
#include "expression/rewrite.hpp"
#include "expression/unary_expression.hpp"
#include "type/collation.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb::relational_detail {

// Lean proofs:
//   - lean/TinylambOpt/OuterOrBranches.lean (outer_or_branches_sound)
//   - lean/TinylambOpt/ResearchProofs/CrossTableDisjunctiveBranchMask.lean
//     (sound)
// Splits `ON p OR q` (up to 4 error-free disjunctive equi-branches) into
// exclusive branch-by-branch hash lookups. A right row emits in branch i iff
// IS_TRUE(d_i) AND NOT IS_TRUE(d_j) for all j < i (preserving
// `p = UNKNOWN, q = TRUE` under three-valued logic) and LEFT JOIN pads left
// rows with zero matches across all branches.
inline constexpr size_t kMaxDisjunctiveJoinBranches = 4;

struct DisjunctiveJoinBranchSpec {
  Expression expr;
  size_t left_col{0};
  size_t right_col{0};
  ValueType key_type{ValueType::kNull};
  bool is_unsigned{false};
};

namespace disjunctive_detail {

inline void CollectOrDisjuncts(const Expression& expr,
                               std::vector<Expression>* out) {
  if (expr && expr->Type() == TypeTag::kBinaryExp &&
      expr->AsBinaryExpression().Op() == BinaryOperation::kOr) {
    CollectOrDisjuncts(expr->AsBinaryExpression().Left(), out);
    CollectOrDisjuncts(expr->AsBinaryExpression().Right(), out);
    return;
  }
  if (expr) {
    out->push_back(expr);
  }
}

struct ScalarLeafType {
  bool valid{false};
  bool is_null_literal{false};
  ValueType type{ValueType::kNull};
  bool is_unsigned{false};
};

inline ScalarLeafType InspectScalarLeaf(const Expression& expr,
                                        const Schema& combined_schema) {
  if (!expr) {
    return {};
  }
  if (expr->Type() == TypeTag::kColumnValue) {
    const int off =
        combined_schema.Offset(expr->AsColumnValue().GetColumnName());
    if (off < 0 || std::cmp_greater_equal(off, combined_schema.ColumnCount())) {
      return {};
    }
    const Column& col = combined_schema.GetColumn(static_cast<size_t>(off));
    if (col.IsInterval()) {
      return {};
    }
    const ValueType t = col.Type();
    if (t != ValueType::kInt64 && t != ValueType::kDate &&
        t != ValueType::kDouble && t != ValueType::kVarChar) {
      return {};
    }
    return ScalarLeafType{
        .valid = true,
        .is_null_literal = false,
        .type = t,
        .is_unsigned = col.IsUnsigned(),
    };
  }
  if (expr->Type() == TypeTag::kConstantValue &&
      dynamic_cast<const ParameterSlot*>(expr.get()) == nullptr) {
    const Value& v = expr->AsConstantValue().GetValue();
    if (v.IsNull()) {
      return ScalarLeafType{.valid = true, .is_null_literal = true};
    }
    if (v.IsCaseInsensitive() || v.IsInterval()) {
      return {};
    }
    if (v.type != ValueType::kInt64 && v.type != ValueType::kDate &&
        v.type != ValueType::kDouble && v.type != ValueType::kVarChar) {
      return {};
    }
    return ScalarLeafType{
        .valid = true,
        .is_null_literal = false,
        .type = v.type,
        .is_unsigned = v.IsUnsigned(),
    };
  }
  return {};
}

inline bool IsErrorFreeJoinExpr(const Expression& expr,
                                const Schema& combined_schema) {
  if (!expr || CollationOfExpression(expr) == Collation::kCaseInsensitive) {
    return false;
  }
  switch (expr->Type()) {
    case TypeTag::kColumnValue:
    case TypeTag::kConstantValue: {
      const ScalarLeafType leaf = InspectScalarLeaf(expr, combined_schema);
      return leaf.valid;
    }
    case TypeTag::kUnaryExp: {
      const auto& u = expr->AsUnaryExpression();
      if (u.Op() == UnaryOperation::kIsNull ||
          u.Op() == UnaryOperation::kIsNotNull) {
        return InspectScalarLeaf(u.Child(), combined_schema).valid;
      }
      if (u.Op() == UnaryOperation::kNot) {
        return IsErrorFreeJoinExpr(u.Child(), combined_schema);
      }
      return false;
    }
    case TypeTag::kBinaryExp: {
      const auto& bin = expr->AsBinaryExpression();
      switch (bin.Op()) {
        case BinaryOperation::kAnd:
        case BinaryOperation::kOr:
        case BinaryOperation::kXor:
          return IsErrorFreeJoinExpr(bin.Left(), combined_schema) &&
                 IsErrorFreeJoinExpr(bin.Right(), combined_schema);
        case BinaryOperation::kEquals:
        case BinaryOperation::kNotEquals:
        case BinaryOperation::kLessThan:
        case BinaryOperation::kLessThanEquals:
        case BinaryOperation::kGreaterThan:
        case BinaryOperation::kGreaterThanEquals:
        case BinaryOperation::kIsDistinctFrom:
        case BinaryOperation::kIsNotDistinctFrom: {
          if (CollationOfExpression(bin.Left()) ==
                  Collation::kCaseInsensitive ||
              CollationOfExpression(bin.Right()) ==
                  Collation::kCaseInsensitive) {
            return false;
          }
          const ScalarLeafType lhs =
              InspectScalarLeaf(bin.Left(), combined_schema);
          const ScalarLeafType rhs =
              InspectScalarLeaf(bin.Right(), combined_schema);
          if (!lhs.valid || !rhs.valid) {
            return false;
          }
          if (lhs.is_null_literal || rhs.is_null_literal) {
            return true;
          }
          return lhs.type == rhs.type && lhs.is_unsigned == rhs.is_unsigned;
        }
        default:
          return false;
      }
    }
    default:
      return false;
  }
}

inline bool PlainHashableVarChar(const Value& v) noexcept {
  if (v.IsCaseInsensitive() || v.IsInterval()) {
    return false;
  }
  const std::string_view s = v.value.varchar_value;
  if (s.find(' ') != std::string_view::npos) {
    return false;
  }
  if (s.size() >= 19 && s[4] == '-' && s[7] == '-' && s[10] == 'T' &&
      s[13] == ':' && s[16] == ':') {
    const auto digits = [&s](size_t begin, size_t end) {
      for (size_t i = begin; i < end; ++i) {
        if (s[i] < '0' || s[i] > '9') {
          return false;
        }
      }
      return true;
    };
    if (digits(0, 4) && digits(5, 7) && digits(8, 10) && digits(11, 13) &&
        digits(14, 16) && digits(17, 19)) {
      return false;
    }
  }
  const size_t first = s.find_first_not_of(" \t\r\n");
  return first == std::string_view::npos || s[first] != '{';
}

}  // namespace disjunctive_detail

inline bool DisjunctiveOrBranchesDisabled() noexcept {
  const char* env = ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_OUTER_OR_BRANCHES");
  return env != nullptr && env[0] == '1';
}

inline std::optional<std::vector<DisjunctiveJoinBranchSpec>>
ExtractDisjunctiveJoinBranches(const Expression& predicate,
                               const Schema& left_schema,
                               const Schema& right_schema,
                               const Schema& combined_schema) {
  if (!predicate || DisjunctiveOrBranchesDisabled()) {
    return std::nullopt;
  }
  std::vector<Expression> disjuncts;
  disjunctive_detail::CollectOrDisjuncts(predicate, &disjuncts);
  if (disjuncts.size() < 2 || disjuncts.size() > kMaxDisjunctiveJoinBranches) {
    return std::nullopt;
  }
  const size_t left_width = left_schema.ColumnCount();
  std::vector<DisjunctiveJoinBranchSpec> branches;
  branches.reserve(disjuncts.size());

  for (const Expression& d : disjuncts) {
    if (!disjunctive_detail::IsErrorFreeJoinExpr(d, combined_schema)) {
      return std::nullopt;
    }
    std::optional<DisjunctiveJoinBranchSpec> matched_spec;
    for (const Expression& conj : SplitConjuncts(d)) {
      if (!conj || conj->Type() != TypeTag::kBinaryExp) {
        continue;
      }
      const auto& bin = conj->AsBinaryExpression();
      if (bin.Op() != BinaryOperation::kEquals || !bin.Left() || !bin.Right() ||
          bin.Left()->Type() != TypeTag::kColumnValue ||
          bin.Right()->Type() != TypeTag::kColumnValue) {
        continue;
      }
      if (CollationOfExpression(conj) == Collation::kCaseInsensitive ||
          CollationOfExpression(bin.Left()) == Collation::kCaseInsensitive ||
          CollationOfExpression(bin.Right()) == Collation::kCaseInsensitive) {
        continue;
      }
      const ColumnName& cn1 = bin.Left()->AsColumnValue().GetColumnName();
      const ColumnName& cn2 = bin.Right()->AsColumnValue().GetColumnName();
      const int c1_comb = combined_schema.Offset(cn1);
      const int c2_comb = combined_schema.Offset(cn2);
      if (c1_comb < 0 || c2_comb < 0) {
        continue;
      }
      const int l1 = left_schema.Offset(cn1);
      const int r1 = right_schema.Offset(cn1);
      const int l2 = left_schema.Offset(cn2);
      const int r2 = right_schema.Offset(cn2);

      std::optional<size_t> l_slot;
      std::optional<size_t> r_slot;
      if (l1 >= 0 && std::cmp_less(l1, left_width) && c1_comb == l1 &&
          r2 >= 0 &&
          static_cast<size_t>(c2_comb) ==
              left_width + static_cast<size_t>(r2)) {
        l_slot = static_cast<size_t>(l1);
        r_slot = static_cast<size_t>(r2);
      } else if (l2 >= 0 && std::cmp_less(l2, left_width) &&
                 c2_comb == l2 && r1 >= 0 &&
                 static_cast<size_t>(c1_comb) ==
                     left_width + static_cast<size_t>(r1)) {
        l_slot = static_cast<size_t>(l2);
        r_slot = static_cast<size_t>(r1);
      } else {
        continue;
      }

      const Column& lc = left_schema.GetColumn(*l_slot);
      const Column& rc = right_schema.GetColumn(*r_slot);
      if (lc.Type() != rc.Type() || lc.IsUnsigned() != rc.IsUnsigned() ||
          lc.IsInterval() || rc.IsInterval()) {
        continue;
      }
      if (lc.Type() != ValueType::kInt64 && lc.Type() != ValueType::kDate &&
          lc.Type() != ValueType::kVarChar) {
        continue;
      }
      matched_spec = DisjunctiveJoinBranchSpec{
          .expr = d,
          .left_col = *l_slot,
          .right_col = *r_slot,
          .key_type = lc.Type(),
          .is_unsigned = lc.IsUnsigned(),
      };
      break;
    }
    if (!matched_spec.has_value()) {
      return std::nullopt;
    }
    branches.push_back(std::move(*matched_spec));
  }
  return branches;
}

class DisjunctiveBranchBuckets {
 public:
  explicit DisjunctiveBranchBuckets(DisjunctiveJoinBranchSpec spec)
      : spec_(std::move(spec)) {}

  // Right rows MUST stay pinned in memory while `str_buckets_` holds
  // string_views referencing right VARCHAR cells.
  [[nodiscard]] bool AddRightRow(uint32_t right_idx, const Row& row) {
    if (spec_.right_col >= row.values_.size()) {
      return false;
    }
    const Value& v = row[spec_.right_col];
    if (v.IsNull()) {
      return true;
    }
    if (v.type != spec_.key_type || v.IsUnsigned() != spec_.is_unsigned ||
        v.IsCaseInsensitive() || v.IsInterval()) {
      return false;
    }
    if (spec_.key_type == ValueType::kVarChar) {
      if (!disjunctive_detail::PlainHashableVarChar(v)) {
        return false;
      }
      str_buckets_[v.value.varchar_value].push_back(right_idx);
      return true;
    }
    int_buckets_[v.value.int_value].push_back(right_idx);
    return true;
  }

  void Reserve(size_t right_count) {
    if (spec_.key_type == ValueType::kVarChar) {
      str_buckets_.reserve(right_count);
    } else {
      int_buckets_.reserve(right_count);
    }
  }

  // Returns `true` if indexed lookup answered conclusively (`*out` may be
  // empty for NULL or unmatched keys); returns `false` only if the left cell
  // has non-plain runtime VARCHAR semantics requiring linear fallback for this
  // row.
  [[nodiscard]] bool Probe(const Row& left_row,
                           std::span<const uint32_t>* out) const noexcept {
    *out = {};
    if (spec_.left_col >= left_row.values_.size()) {
      return false;
    }
    const Value& v = left_row[spec_.left_col];
    if (v.IsNull()) {
      return true;
    }
    if (v.type != spec_.key_type || v.IsUnsigned() != spec_.is_unsigned ||
        v.IsCaseInsensitive() || v.IsInterval()) {
      return false;
    }
    if (spec_.key_type == ValueType::kVarChar) {
      if (!disjunctive_detail::PlainHashableVarChar(v)) {
        return false;
      }
      const auto it = str_buckets_.find(v.value.varchar_value);
      if (it != str_buckets_.end()) {
        *out = it->second;
      }
      return true;
    }
    const auto it = int_buckets_.find(v.value.int_value);
    if (it != int_buckets_.end()) {
      *out = it->second;
    }
    return true;
  }

  [[nodiscard]] const DisjunctiveJoinBranchSpec& Spec() const noexcept {
    return spec_;
  }

 private:
  DisjunctiveJoinBranchSpec spec_;
  std::unordered_map<int64_t, std::vector<uint32_t>> int_buckets_;
  std::unordered_map<std::string_view, std::vector<uint32_t>> str_buckets_;
};

}  // namespace tinylamb::relational_detail

#endif  // TINYLAMB_EXECUTOR_DETAIL_DISJUNCTIVE_JOIN_BRANCHES_HPP
