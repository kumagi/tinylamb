/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/zone_map.hpp"

#include <array>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <string>
#include <string_view>

#include "common/constants.hpp"
#include "executor/detail/execution_strategies.hpp"
#include "expression/binary_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/function_call_expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/unary_expression.hpp"
#include "table/lstable_bridge.hpp"
#include "type/collation.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

void ZoneMap::Add(const Value& value) {
  initialized_ = true;
  if (value.IsNull()) {
    ++null_count_;
    return;
  }
  ++value_count_;
  has_collated_values_ =
      has_collated_values_ || CollationOf(value) == Collation::kCaseInsensitive;
  // Arrays carry no useful scalar ordering for pruning (element-wise
  // lexicographic comparison throws across NULL/non-NULL elements), so keep
  // them out of the min/max envelope.
  if (value.IsArray()) {
    return;
  }  // PRODUCTION FIX: NaN fails every IEEE comparison, so a NaN that arrived
  // first poisoned min/max into (NaN, NaN) and MayMatch then pruned batches
  // that actually contained matching rows. Exclude NaN from the envelope and
  // keep a flag: its presence must conservatively keep the zone eligible.
  if (value.type == ValueType::kDouble &&
      std::isnan(value.value.double_value)) {
    has_nan_ = true;
    return;
  }
  if (minimum_ && minimum_->type != value.type) {
    has_nan_ = true;
    minimum_.reset();
    maximum_.reset();
    return;
  }
  if (!minimum_ || value < *minimum_) {
    minimum_ = value;
  }
  if (!maximum_ || *maximum_ < value) {
    maximum_ = value;
  }
}

void ZoneMap::AddInt(int64_t value) {
  initialized_ = true;
  ++value_count_;
  if (!minimum_ || value < minimum_->value.int_value) {
    minimum_ = Value(value);
  }
  if (!maximum_ || maximum_->value.int_value < value) {
    maximum_ = Value(value);
  }
}

void ZoneMap::AddDate(int64_t days) {
  initialized_ = true;
  ++value_count_;
  if (!minimum_ || days < minimum_->value.int_value) {
    minimum_ = Value::DateFromDays(days);
  }
  if (!maximum_ || maximum_->value.int_value < days) {
    maximum_ = Value::DateFromDays(days);
  }
}

void ZoneMap::AddDouble(double value) {
  initialized_ = true;
  ++value_count_;
  // Same NaN guard as ZoneMap::Add: a leading NaN poisoned the envelope.
  if (std::isnan(value)) {
    has_nan_ = true;
    return;
  }
  if (!minimum_ || value < minimum_->value.double_value) {
    minimum_ = Value(value);
  }
  if (!maximum_ || maximum_->value.double_value < value) {
    maximum_ = Value(value);
  }
}

void ZoneMap::AddString(std::string_view value) {
  initialized_ = true;
  ++value_count_;
  // T4171: an improving extremum reuses the stored Value's string buffer
  // (CopyFromStringView assigns in place) instead of allocating a fresh owned
  // string and dropping the old one on every new min/max.
  if (!minimum_ || value < minimum_->value.varchar_value) {
    if (minimum_) {
      minimum_->CopyFromStringView(value);
    } else {
      minimum_ = Value(std::string(value));
    }
  }
  if (!maximum_ || maximum_->value.varchar_value < value) {
    if (maximum_) {
      maximum_->CopyFromStringView(value);
    } else {
      maximum_ = Value(std::string(value));
    }
  }
}

void ZoneMap::AddNull() {
  initialized_ = true;
  ++null_count_;
}

void ZoneMap::AddNulls(size_t count) {
  if (count == 0) {
    return;
  }
  initialized_ = true;
  null_count_ += count;
}

void ZoneMap::AddIntRange(int64_t mn, int64_t mx, size_t count) {
  if (count == 0) {
    return;
  }
  initialized_ = true;
  value_count_ += count;
  if (minimum_ && minimum_->type != ValueType::kInt64) {
    has_nan_ = true;
    minimum_.reset();
    maximum_.reset();
    return;
  }
  // T1935: a reversed interval would make MayMatch reject every row (same
  // contract AddDoubleRange already enforces).
  if (mn > mx) {
    has_nan_ = true;
    minimum_.reset();
    maximum_.reset();
    return;
  }
  if (!minimum_ || mn < minimum_->value.int_value) {
    minimum_ = Value(mn);
  }
  if (!maximum_ || maximum_->value.int_value < mx) {
    maximum_ = Value(mx);
  }
}

void ZoneMap::AddDateRange(int64_t mn, int64_t mx, size_t count) {
  if (count == 0) {
    return;
  }
  initialized_ = true;
  value_count_ += count;
  if (minimum_ && minimum_->type != ValueType::kDate) {
    has_nan_ = true;
    minimum_.reset();
    maximum_.reset();
    return;
  }
  // T1935: reversed interval -> unbounded envelope (see AddIntRange).
  if (mn > mx) {
    has_nan_ = true;
    minimum_.reset();
    maximum_.reset();
    return;
  }
  if (!minimum_ || mn < minimum_->value.int_value) {
    minimum_ = Value::DateFromDays(mn);
  }
  if (!maximum_ || maximum_->value.int_value < mx) {
    maximum_ = Value::DateFromDays(mx);
  }
}

void ZoneMap::AddDoubleRange(double mn, double mx, bool has_bounds,
                             bool saw_nan, size_t count) {
  if (count == 0) {
    return;
  }
  initialized_ = true;
  value_count_ += count;
  if (saw_nan) {
    has_nan_ = true;
  }
  if (!has_bounds) {
    return;
  }
  if (minimum_ && minimum_->type != ValueType::kDouble) {
    has_nan_ = true;
    minimum_.reset();
    maximum_.reset();
    return;
  }
  // A reversed interval would make MayMatch reject every row.
  if (mn > mx) {
    has_nan_ = true;
    minimum_.reset();
    maximum_.reset();
    return;
  }
  if (!minimum_ || mn < minimum_->value.double_value) {
    minimum_ = Value(mn);
  }
  if (!maximum_ || maximum_->value.double_value < mx) {
    maximum_ = Value(mx);
  }
}

void ZoneMap::AddStringRange(std::string_view mn, std::string_view mx,
                             size_t count) {
  if (count == 0) {
    return;
  }
  initialized_ = true;
  value_count_ += count;
  if (minimum_ && minimum_->type != ValueType::kVarChar) {
    has_nan_ = true;
    minimum_.reset();
    maximum_.reset();
    return;
  }
  // T1935: reversed interval -> unbounded envelope, never a false prune.
  if (mx < mn) {
    has_nan_ = true;
    minimum_.reset();
    maximum_.reset();
    return;
  }
  if (!minimum_ || mn < minimum_->value.varchar_value) {
    minimum_ = Value(std::string(mn));
  }
  if (!maximum_ || maximum_->value.varchar_value < mx) {
    maximum_ = Value(std::string(mx));
  }
}

void ZoneMap::AddOpaque() {
  initialized_ = true;
  ++value_count_;
}

void ZoneMap::Merge(const ZoneMap& other) {
  if (!other.initialized_) {
    return;
  }
  initialized_ = true;
  null_count_ += other.null_count_;
  value_count_ += other.value_count_;
  has_nan_ = has_nan_ || other.has_nan_;
  has_collated_values_ = has_collated_values_ || other.has_collated_values_;
  if (minimum_ && other.minimum_ && minimum_->type != other.minimum_->type) {
    has_nan_ = true;
    minimum_.reset();
    maximum_.reset();
    return;
  }
  if (other.minimum_ &&
      (!minimum_ || CompareForOrderBy(*other.minimum_, *minimum_) < 0)) {
    minimum_ = other.minimum_;
  }
  if (other.maximum_ &&
      (!maximum_ || CompareForOrderBy(*maximum_, *other.maximum_) < 0)) {
    maximum_ = other.maximum_;
  }
}

void ZoneMap::Reset() {
  minimum_.reset();
  maximum_.reset();
  null_count_ = 0;
  value_count_ = 0;
  initialized_ = false;
  has_nan_ = false;
  has_collated_values_ = false;
}

bool ZoneMap::MayMatch(BinaryOperation operation, const Value& constant) const {
  // An uninitialized zone map proves nothing: the producing operator may not
  // have maintained it, so pruning would silently drop matching rows.
  if (!initialized_) {
    return true;
  }
  // T1621: a NaN-bearing zone used to keep every row ("NaN compares false
  // against everything, so envelope pruning is unsound").  But the engine's
  // AST ground truth (binary_expression.cpp) is IEEE-unordered: EVERY
  // comparison against a NaN -- ordered, `=`, and even `!=` -- evaluates to
  // FALSE, so a NaN row can never satisfy a BinaryOperation predicate.
  // AddDouble/AddDoubleRange already keep NaN out of the envelope, so the
  // finite min/max remains a sound pruning envelope for the whole zone and
  // the unconditional bail-out only cost pruning for the batch's finite
  // rows.  Zones whose envelope was dropped as unreliable (mixed types,
  // reversed ranges) or that saw ONLY NaN keep the conservative answer via
  // the empty-envelope branch below.
  if (constant.IsNull()) {
    return false;
  }
  if (!minimum_ || !maximum_) {
    // Non-NULL values were observed but excluded from the envelope (arrays,
    // NaN-only zones) or the envelope was dropped as unreliable (mixed
    // types, reversed ranges): nothing can be proven, so keep every row.
    // value_count_ is the single authority for "a non-NULL cell was seen" --
    // every path that sets has_nan_ has already incremented it, so the two
    // checks were redundant. has_nan_ only describes why the envelope is
    // empty and must not independently disable pruning.
    return value_count_ > 0;
  }
  // Cross-type comparisons (e.g. int constant vs double zone) may match after
  // implicit conversion; pruning them would silently drop rows.
  if (minimum_->type != constant.type) {
    return true;
  }
  // An interval-tagged constant compared against an envelope built from
  // untagged VARCHAR values uses non-commutative TryLess semantics
  // (untagged < tagged is always true, tagged < untagged is always false),
  // so envelope pruning is unsound.  Keep the batch.
  if (constant.IsInterval()) {
    return true;
  }
  // A collation can change the order of values inside a byte-order range:
  // e.g. ["Z", "a"] contains a match for COLLATE("z", "und:ci"). Until
  // the zone carries an envelope for each collation, retain such batches.
  if (has_collated_values_ ||
      CollationOf(constant) == Collation::kCaseInsensitive) {
    return true;
  }
  switch (operation) {
    case BinaryOperation::kEquals:
      return *minimum_ <= constant && constant <= *maximum_;
    case BinaryOperation::kNotEquals:
      return !(*minimum_ == constant && *maximum_ == constant);
    case BinaryOperation::kLessThan:
      return *minimum_ < constant;
    case BinaryOperation::kLessThanEquals:
      return *minimum_ <= constant;
    case BinaryOperation::kGreaterThan:
      return constant < *maximum_;
    case BinaryOperation::kGreaterThanEquals:
      return constant <= *maximum_;
    default:
      return true;
  }
}

namespace {

enum class Val4 : uint8_t { kT = 0, kF = 1, kU = 2, kE = 3 };

KleeneMask BitFor(Val4 v) noexcept {
  return static_cast<KleeneMask>(uint8_t{1} << static_cast<uint8_t>(v));
}

KleeneMask KleeneNot(KleeneMask m) noexcept {
  KleeneMask out = kKleeneNone;
  if ((m & kKleeneT) != 0) {
    out |= kKleeneF;
  }
  if ((m & kKleeneF) != 0) {
    out |= kKleeneT;
  }
  if ((m & kKleeneU) != 0) {
    out |= kKleeneU;
  }
  if ((m & kKleeneE) != 0) {
    out |= kKleeneE;
  }
  return out;
}

KleeneMask KleeneAnd(KleeneMask a, KleeneMask b) noexcept {
  // Exact 4-valued lift from `lean/TinylambOpt/BlockKleene.lean` (`val4_and`,
  // strict error propagation).
  if ((a & kKleeneE) != 0 || (b & kKleeneE) != 0) {
    // If one side is definitely empty (0), result is 0; if any pair can hit E,
    // propagate E cleanly alongside other reachable non-error values.
    if (a == 0 || b == 0) {
      return 0;
    }
  }
  KleeneMask out = kKleeneNone;
  const std::array<Val4, 4> vals = {Val4::kT, Val4::kF, Val4::kU, Val4::kE};
  for (Val4 va : vals) {
    if ((a & BitFor(va)) == 0) {
      continue;
    }
    for (Val4 vb : vals) {
      if ((b & BitFor(vb)) == 0) {
        continue;
      }
      if (va == Val4::kE || vb == Val4::kE) {
        out |= kKleeneE;
      } else if (va == Val4::kF || vb == Val4::kF) {
        out |= kKleeneF;
      } else if (va == Val4::kU || vb == Val4::kU) {
        out |= kKleeneU;
      } else {
        out |= kKleeneT;
      }
    }
  }
  return out;
}

KleeneMask KleeneOr(KleeneMask a, KleeneMask b) noexcept {
  KleeneMask out = kKleeneNone;
  const std::array<Val4, 4> vals = {Val4::kT, Val4::kF, Val4::kU, Val4::kE};
  for (Val4 va : vals) {
    if ((a & BitFor(va)) == 0) {
      continue;
    }
    for (Val4 vb : vals) {
      if ((b & BitFor(vb)) == 0) {
        continue;
      }
      if (va == Val4::kE || vb == Val4::kE) {
        out |= kKleeneE;
      } else if (va == Val4::kT || vb == Val4::kT) {
        out |= kKleeneT;
      } else if (va == Val4::kU || vb == Val4::kU) {
        out |= kKleeneU;
      } else {
        out |= kKleeneF;
      }
    }
  }
  return out;
}

bool ExprGuaranteedBooleanOrNull(  // NOLINT(misc-no-recursion)
    const Expression& expr, const Schema& schema) {
  if (!expr) {
    return false;
  }
  if (expr->Type() == TypeTag::kConstantValue) {
    const Value& v = expr->AsConstantValue().GetValue();
    return v.IsNull() || (v.type == ValueType::kInt64 && !v.IsUnsigned() &&
                          (v.value.int_value == 0 || v.value.int_value == 1));
  }
  if (expr->Type() == TypeTag::kUnaryExp) {
    const auto op = expr->AsUnaryExpression().Op();
    return op == UnaryOperation::kNot || op == UnaryOperation::kIsNull ||
           op == UnaryOperation::kIsNotNull || op == UnaryOperation::kIsTrue ||
           op == UnaryOperation::kIsNotTrue || op == UnaryOperation::kIsFalse ||
           op == UnaryOperation::kIsNotFalse;
  }
  if (expr->Type() == TypeTag::kInExp) {
    return true;
  }
  if (expr->Type() == TypeTag::kBinaryExp) {
    const auto op = expr->AsBinaryExpression().Op();
    return op == BinaryOperation::kEquals ||
           op == BinaryOperation::kNotEquals ||
           op == BinaryOperation::kLessThan ||
           op == BinaryOperation::kLessThanEquals ||
           op == BinaryOperation::kGreaterThan ||
           op == BinaryOperation::kGreaterThanEquals ||
           op == BinaryOperation::kIsDistinctFrom ||
           op == BinaryOperation::kIsNotDistinctFrom ||
           op == BinaryOperation::kLike || op == BinaryOperation::kNotLike ||
           op == BinaryOperation::kAnd || op == BinaryOperation::kOr ||
           op == BinaryOperation::kXor;
  }
  (void)schema;
  return false;
}

BinaryOperation FlipOp(BinaryOperation op) noexcept {
  switch (op) {
    case BinaryOperation::kLessThan:
      return BinaryOperation::kGreaterThan;
    case BinaryOperation::kLessThanEquals:
      return BinaryOperation::kGreaterThanEquals;
    case BinaryOperation::kGreaterThan:
      return BinaryOperation::kLessThan;
    case BinaryOperation::kGreaterThanEquals:
      return BinaryOperation::kLessThanEquals;
    default:
      return op;
  }
}

KleeneMask EvalColConstCompare(const LSTableColumnBlock& col,
                               BinaryOperation op, const Value& cval) noexcept {
  if (cval.IsNull()) {
    return kKleeneU;
  }
  if (col.type == ValueType::kInt64 || col.type == ValueType::kDate) {
    if (col.is_unsigned || cval.IsUnsigned() || cval.type != col.type) {
      return kKleeneAll;
    }
    KleeneMask out = col.has_null ? kKleeneU : kKleeneNone;
    if (!col.has_min_max) {
      // No non-NULL values contributed bounds. Preserve whether NULL rows
      // exist; an empty block can have neither bounds nor NULLs.
      return out;
    }
    const int64_t mn = col.min_int;
    const int64_t mx = col.max_int;
    const int64_t c = cval.value.int_value;
    bool may_t = false;
    bool may_f = false;
    switch (op) {
      case BinaryOperation::kEquals:
        may_t = (mn <= c && c <= mx);
        may_f = mn != c || mx != c;
        break;
      case BinaryOperation::kNotEquals:
        may_t = mn != c || mx != c;
        may_f = (mn <= c && c <= mx);
        break;
      case BinaryOperation::kLessThan:
        may_t = (mn < c);
        may_f = (mx >= c);
        break;
      case BinaryOperation::kLessThanEquals:
        may_t = (mn <= c);
        may_f = (mx > c);
        break;
      case BinaryOperation::kGreaterThan:
        may_t = (mx > c);
        may_f = (mn <= c);
        break;
      case BinaryOperation::kGreaterThanEquals:
        may_t = (mx >= c);
        may_f = (mn < c);
        break;
      default:
        return kKleeneAll;
    }
    if (may_t) {
      out |= kKleeneT;
    }
    if (may_f) {
      out |= kKleeneF;
    }
    return out;
  }
  if (col.type == ValueType::kDouble) {
    if (col.has_nan) {
      return kKleeneAll;
    }
    double c = 0.0;
    if (cval.type == ValueType::kDouble) {
      c = cval.value.double_value;
    } else if (cval.type == ValueType::kInt64 && !cval.IsUnsigned() &&
               cval.value.int_value >= -(int64_t{1} << 53) &&
               cval.value.int_value <= (int64_t{1} << 53)) {
      c = static_cast<double>(cval.value.int_value);
    } else {
      return kKleeneAll;
    }
    if (std::isnan(c)) {
      // Comparison with NaN is FALSE (or TRUE for !=) on non-NULL doubles, and
      // NULL on NULLs.
      KleeneMask out = col.has_null ? kKleeneU : kKleeneNone;
      if (col.has_double_min_max) {
        out |= (op == BinaryOperation::kNotEquals) ? kKleeneT : kKleeneF;
      }
      return out;
    }
    KleeneMask out = col.has_null ? kKleeneU : kKleeneNone;
    if (!col.has_double_min_max) {
      return out;
    }
    const double mn = col.min_double;
    const double mx = col.max_double;
    bool may_t = false;
    bool may_f = false;
    switch (op) {
      case BinaryOperation::kEquals:
        may_t = (mn <= c && c <= mx);
        may_f = mn != c || mx != c;
        break;
      case BinaryOperation::kNotEquals:
        may_t = mn != c || mx != c;
        may_f = (mn <= c && c <= mx);
        break;
      case BinaryOperation::kLessThan:
        may_t = (mn < c);
        may_f = (mx >= c);
        break;
      case BinaryOperation::kLessThanEquals:
        may_t = (mn <= c);
        may_f = (mx > c);
        break;
      case BinaryOperation::kGreaterThan:
        may_t = (mx > c);
        may_f = (mn <= c);
        break;
      case BinaryOperation::kGreaterThanEquals:
        may_t = (mx >= c);
        may_f = (mn < c);
        break;
      default:
        return kKleeneAll;
    }
    if (may_t) {
      out |= kKleeneT;
    }
    if (may_f) {
      out |= kKleeneF;
    }
    return out;
  }
  return kKleeneAll;
}

KleeneMask EvalKleeneImpl(  // NOLINT(misc-no-recursion)
    const Expression& expr, const Schema& schema, const LSTablePageBlock& blk) {
  if (!expr || blk.row_count == 0) {
    return blk.row_count == 0 ? kKleeneNone : kKleeneAll;
  }
  switch (expr->Type()) {
    case TypeTag::kConstantValue: {
      const Value& v = expr->AsConstantValue().GetValue();
      if (v.IsNull()) {
        return kKleeneU;
      }
      return v.Truthy() ? kKleeneT : kKleeneF;
    }
    case TypeTag::kUnaryExp: {
      const UnaryExpression& u = expr->AsUnaryExpression();
      if (u.Op() == UnaryOperation::kIsNull ||
          u.Op() == UnaryOperation::kIsNotNull) {
        if (!u.Child() || u.Child()->Type() != TypeTag::kColumnValue) {
          return kKleeneAll;
        }
        const int off =
            schema.Offset(u.Child()->AsColumnValue().GetColumnName());
        if (off < 0 || !blk.columns.Contains(static_cast<size_t>(off))) {
          return kKleeneAll;
        }
        const LSTableColumnBlock& col = blk.columns[static_cast<size_t>(off)];
        KleeneMask is_null_mask = kKleeneNone;
        if (!col.has_null) {
          is_null_mask = kKleeneF;
        } else if (((col.type == ValueType::kInt64 ||
                     col.type == ValueType::kDate) &&
                    !col.has_min_max) ||
                   (col.type == ValueType::kDouble && !col.has_double_min_max &&
                    !col.has_nan)) {
          is_null_mask = kKleeneT;
        } else {
          is_null_mask = kKleeneTF;
        }
        return u.Op() == UnaryOperation::kIsNull ? is_null_mask
                                                 : KleeneNot(is_null_mask);
      }
      const KleeneMask inner = EvalKleeneImpl(u.Child(), schema, blk);
      if (u.Op() == UnaryOperation::kNot) {
        return KleeneNot(inner);
      }
      if (!ExprGuaranteedBooleanOrNull(u.Child(), schema)) {
        return kKleeneAll;
      }
      KleeneMask out = (inner & kKleeneE);
      switch (u.Op()) {
        case UnaryOperation::kIsTrue:
          if ((inner & kKleeneT) != 0) {
            out |= kKleeneT;
          }
          if ((inner & (kKleeneF | kKleeneU)) != 0) {
            out |= kKleeneF;
          }
          return out;
        case UnaryOperation::kIsNotTrue:
          if ((inner & (kKleeneF | kKleeneU)) != 0) {
            out |= kKleeneT;
          }
          if ((inner & kKleeneT) != 0) {
            out |= kKleeneF;
          }
          return out;
        case UnaryOperation::kIsFalse:
          if ((inner & kKleeneF) != 0) {
            out |= kKleeneT;
          }
          if ((inner & (kKleeneT | kKleeneU)) != 0) {
            out |= kKleeneF;
          }
          return out;
        case UnaryOperation::kIsNotFalse:
          if ((inner & (kKleeneT | kKleeneU)) != 0) {
            out |= kKleeneT;
          }
          if ((inner & kKleeneF) != 0) {
            out |= kKleeneF;
          }
          return out;
        default:
          return kKleeneAll;
      }
    }
    case TypeTag::kBinaryExp: {
      const BinaryExpression& bin = expr->AsBinaryExpression();
      if (bin.Op() == BinaryOperation::kAnd) {
        return KleeneAnd(EvalKleeneImpl(bin.Left(), schema, blk),
                         EvalKleeneImpl(bin.Right(), schema, blk));
      }
      if (bin.Op() == BinaryOperation::kOr) {
        return KleeneOr(EvalKleeneImpl(bin.Left(), schema, blk),
                        EvalKleeneImpl(bin.Right(), schema, blk));
      }
      Expression col_expr;
      Expression const_expr;
      BinaryOperation op = bin.Op();
      if (bin.Left() && bin.Left()->Type() == TypeTag::kColumnValue &&
          bin.Right() && bin.Right()->Type() == TypeTag::kConstantValue) {
        col_expr = bin.Left();
        const_expr = bin.Right();
      } else if (bin.Left() && bin.Left()->Type() == TypeTag::kConstantValue &&
                 bin.Right() && bin.Right()->Type() == TypeTag::kColumnValue) {
        col_expr = bin.Right();
        const_expr = bin.Left();
        op = FlipOp(op);
      } else {
        // Lean-backed (ExecutionStrategies/IndexPreimages.lean:
        // CheckedDistanceBand, TruncBucket): derive exact integer preimage
        // intervals for `ABS(col - center) <= rad` (or `< rad`) and
        // `col / d = q` so zone maps prune blocks without row evaluation.
        auto eval_int_band =
            [&](const ColumnName& cn,
                const execution_strategies::IntInterval& iv) -> KleeneMask {
          const int off = schema.Offset(cn);
          if (off < 0 || !blk.columns.Contains(static_cast<size_t>(off))) {
            return kKleeneAll;
          }
          const LSTableColumnBlock& col = blk.columns[static_cast<size_t>(off)];
          if (col.type != ValueType::kInt64 || col.is_unsigned) {
            return kKleeneAll;
          }
          KleeneMask out = col.has_null ? kKleeneU : kKleeneNone;
          if (!col.has_min_max) {
            return out;
          }
          if (iv.empty || col.max_int < iv.lo || col.min_int > iv.hi) {
            return out | kKleeneF;
          }
          if (col.min_int >= iv.lo && col.max_int <= iv.hi) {
            return out | kKleeneT;
          }
          return out | kKleeneT | kKleeneF;
        };
        Expression lhs = bin.Left();
        Expression rhs = bin.Right();
        BinaryOperation bop = bin.Op();
        if (lhs && lhs->Type() == TypeTag::kConstantValue && rhs &&
            rhs->Type() != TypeTag::kConstantValue) {
          std::swap(lhs, rhs);
          bop = FlipOp(bop);
        }
        if (lhs && rhs && rhs->Type() == TypeTag::kConstantValue) {
          const Value& rv = rhs->AsConstantValue().GetValue();
          if (!rv.IsNull() && rv.type == ValueType::kInt64 &&
              !rv.IsUnsigned()) {
            // Case 1: ABS(col - center) <= rad (or < rad).
            if ((bop == BinaryOperation::kLessThanEquals ||
                 bop == BinaryOperation::kLessThan) &&
                lhs->Type() == TypeTag::kFunctionCallExp) {
              const auto& fc = lhs->AsFunctionCallExpression();
              if ((fc.FuncName() == "abs" || fc.FuncName() == "ABS") &&
                  fc.Args().size() == 1 && fc.Args()[0] &&
                  fc.Args()[0]->Type() == TypeTag::kBinaryExp &&
                  fc.Args()[0]->AsBinaryExpression().Op() ==
                      BinaryOperation::kSubtract) {
                const auto& sub = fc.Args()[0]->AsBinaryExpression();
                if (sub.Left() && sub.Left()->Type() == TypeTag::kColumnValue &&
                    sub.Right() &&
                    sub.Right()->Type() == TypeTag::kConstantValue) {
                  const Value& cv = sub.Right()->AsConstantValue().GetValue();
                  if (!cv.IsNull() && cv.type == ValueType::kInt64 &&
                      !cv.IsUnsigned()) {
                    const int64_t rad =
                        bop == BinaryOperation::kLessThanEquals
                            ? rv.value.int_value
                            : (rv.value.int_value ==
                                       std::numeric_limits<int64_t>::min()
                                   ? -1
                                   : rv.value.int_value - 1);
                    const auto iv = execution_strategies::CheckedDistanceBand(
                        cv.value.int_value, rad);
                    // Guard against signed overflow in `col - center` when
                    // min/max could overflow int64.
                    const int off = schema.Offset(
                        sub.Left()->AsColumnValue().GetColumnName());
                    if (off >= 0 &&
                        blk.columns.Contains(static_cast<size_t>(off))) {
                      const auto& col = blk.columns[static_cast<size_t>(off)];
                      int64_t tmp1 = 0;
                      int64_t tmp2 = 0;
                      if (col.has_min_max &&
                          !__builtin_sub_overflow(col.min_int,
                                                  cv.value.int_value, &tmp1) &&
                          !__builtin_sub_overflow(col.max_int,
                                                  cv.value.int_value, &tmp2) &&
                          tmp1 != std::numeric_limits<int64_t>::min() &&
                          tmp2 != std::numeric_limits<int64_t>::min()) {
                        return eval_int_band(
                            sub.Left()->AsColumnValue().GetColumnName(), iv);
                      }
                    }
                  }
                }
              }
            }
            // Case 2: DIV(col, d) = q (exact integer truncation toward zero).
            if (bop == BinaryOperation::kEquals &&
                lhs->Type() == TypeTag::kFunctionCallExp) {
              const auto& fc = lhs->AsFunctionCallExpression();
              if ((fc.FuncName() == "div" || fc.FuncName() == "DIV") &&
                  fc.Args().size() == 2 && fc.Args()[0] &&
                  fc.Args()[0]->Type() == TypeTag::kColumnValue &&
                  fc.Args()[1] &&
                  fc.Args()[1]->Type() == TypeTag::kConstantValue) {
                const Value& dv = fc.Args()[1]->AsConstantValue().GetValue();
                if (!dv.IsNull() && dv.type == ValueType::kInt64 &&
                    !dv.IsUnsigned() && dv.value.int_value > 0) {
                  const auto iv = execution_strategies::TruncBucketPreimage(
                      dv.value.int_value, rv.value.int_value);
                  return eval_int_band(
                      fc.Args()[0]->AsColumnValue().GetColumnName(), iv);
                }
              }
            }
          }
        }
        return kKleeneAll;
      }
      if (op != BinaryOperation::kEquals && op != BinaryOperation::kNotEquals &&
          op != BinaryOperation::kLessThan &&
          op != BinaryOperation::kLessThanEquals &&
          op != BinaryOperation::kGreaterThan &&
          op != BinaryOperation::kGreaterThanEquals) {
        return kKleeneAll;
      }
      const int off = schema.Offset(col_expr->AsColumnValue().GetColumnName());
      if (off < 0 || !blk.columns.Contains(static_cast<size_t>(off))) {
        return kKleeneAll;
      }
      return EvalColConstCompare(blk.columns[static_cast<size_t>(off)], op,
                                 const_expr->AsConstantValue().GetValue());
    }
    case TypeTag::kInExp: {
      const InExpression& in = expr->AsInExpression();
      if (!in.child_ || in.child_->Type() != TypeTag::kColumnValue ||
          in.list_.size() > 64) {
        return kKleeneAll;
      }
      const int off = schema.Offset(in.child_->AsColumnValue().GetColumnName());
      if (off < 0 || !blk.columns.Contains(static_cast<size_t>(off))) {
        return kKleeneAll;
      }
      if (in.list_.empty()) {
        return kKleeneF;
      }
      const LSTableColumnBlock& col = blk.columns[static_cast<size_t>(off)];
      KleeneMask acc = kKleeneF;
      for (const Expression& item : in.list_) {
        if (!item || item->Type() != TypeTag::kConstantValue) {
          return kKleeneAll;
        }
        acc = KleeneOr(acc,
                       EvalColConstCompare(col, BinaryOperation::kEquals,
                                           item->AsConstantValue().GetValue()));
      }
      return acc;
    }
    default:
      return kKleeneAll;
  }
}

}  // namespace

KleeneMask EvaluateBlockKleene(const Expression& expr, const Schema& schema,
                               const LSTablePageBlock& blk) {
  return EvalKleeneImpl(expr, schema, blk);
}

bool CanSkipLikeBlockByTrigrams(std::string_view like_pattern,
                                const TrigramBlockSummary& summary,
                                char escape_char) noexcept {
  // Parse literal runs between `%` and `_` wildcards (honoring escape_char).
  // Any literal run of length >= 3 yields mandatory trigrams that MUST appear
  // in every matching row (`TrigramSkip.trigram_skip_sound`).
  uint8_t win[3] = {0, 0, 0};
  size_t run_len = 0;
  for (size_t i = 0; i < like_pattern.size(); ++i) {
    char ch = like_pattern[i];
    if (escape_char != '\0' && ch == escape_char) {
      if (i + 1 < like_pattern.size()) {
        ch = like_pattern[++i];
      } else {
        // Trailing bare escape char: treat as literal.
      }
    } else if (ch == '%' || ch == '_') {
      run_len = 0;
      continue;
    }
    const uint8_t b = static_cast<uint8_t>(ch);
    if (run_len < 3) {
      win[run_len++] = b;
    } else {
      win[0] = win[1];
      win[1] = win[2];
      win[2] = b;
    }
    if (run_len == 3 && !summary.MayContainTrigram(win[0], win[1], win[2])) {
      return true;
    }
  }
  return false;
}

bool CanSkipBlockByBloomIntersection(const BlockBloomSignature& build_sig,
                                     const BlockBloomSignature& probe_sig,
                                     bool null_safe_eq) noexcept {
  // (H1) Seed and bit-width must match; otherwise skipping is unsound.
  if (build_sig.seed != probe_sig.seed || build_sig.words.empty() ||
      build_sig.words.size() != probe_sig.words.size()) {
    return false;
  }
  // `nullSafeSkip_sound`: NULL-safe equality (`IS NOT DISTINCT FROM`) matches
  // when both sides contain NULL even if non-NULL signatures are disjoint.
  if (null_safe_eq && build_sig.has_null && probe_sig.has_null) {
    return false;
  }
  for (size_t i = 0; i < build_sig.words.size(); ++i) {
    if ((build_sig.words[i] & probe_sig.words[i]) != 0ULL) {
      return false;
    }
  }
  return true;
}

}  // namespace tinylamb
