/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_TYPE_COLLATION_HPP
#define TINYLAMB_TYPE_COLLATION_HPP

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

#include "type/value.hpp"

namespace tinylamb {

// Collation of a comparison, as carried in Value's tag field
// (type/value.hpp): 0 = none (engine default), 1 = explicit 'binary',
// 2 = case-insensitive ('und:ci' plus every other non-binary spec the engine
// folds). Tag 3 is the unsigned marker and unknown tags are not collations;
// both degrade to kNone, which is the engine default (byte order).
//
// Scope: this engine implements *binary* and *case-insensitive* comparison
// only. There is no locale/ICU collation, so `en_US`, `czech` or
// `en_US:cs` behave as case-insensitive and anything the fold cannot reach
// (non-ASCII letters, combining marks) keeps byte order. That fallback is
// deliberate: UTF-8 code-point order and interval rendering order are
// preserved exactly as before, so a COLLATE'd key never silently reorders
// text the engine cannot interpret.
enum class Collation : uint8_t {
  kNone = 0,
  kBinary = 1,
  kCaseInsensitive = 2,
};

// The collation a single value carries, or kNone when it carries none (or
// carries a non-collation tag such as the unsigned marker). Value is not a
// literal type (owned_data / array_), so this cannot be constexpr.
[[nodiscard]] inline Collation CollationOf(const Value& value) {
  return value.Collation() <= static_cast<uint8_t>(Collation::kCaseInsensitive)
             ? static_cast<Collation>(value.Collation())
             : Collation::kNone;
}

[[nodiscard]] constexpr Collation CollationOf(uint8_t tag) {
  return tag <= static_cast<uint8_t>(Collation::kCaseInsensitive)
             ? static_cast<Collation>(tag)
             : Collation::kNone;
}

// Explicit incompatible string collations make IN membership ambiguous.
// Numeric tags (notably UINT64's tag 3) are not string collations.
[[nodiscard]] inline bool HaveConflictingCollations(const Value& left,
                                                    const Value& right) {
  if (left.type != ValueType::kVarChar || right.type != ValueType::kVarChar) {
    return false;
  }
  const Collation lhs = CollationOf(left), rhs = CollationOf(right);
  return lhs != Collation::kNone && rhs != Collation::kNone && lhs != rhs;
}

// The wire name used by PhysicalProperties::collation and its key
// serialization: "" (empty) means "engine default", which is binary order.
[[nodiscard]] std::string_view CollationName(Collation collation);

// Inverse of CollationName: "" is "no collator" (the engine default),
// otherwise the name names a collator. Only the property name uses this —
// COLLATE() itself resolves its spec through CollationOfSpec.
[[nodiscard]] Collation CollationFromName(std::string_view spec);

// The collator COLLATE(value, 'spec') attaches: 'binary' is binary and every
// other spec folds, matching the resolution the expression and batch
// evaluators have always applied. An empty spec is a degenerate collator that
// is still explicit and non-binary (the compliance corpus distinguishes
// COLLATE(x, '') from COLLATE(x, 'und:ci')), so it folds too.
[[nodiscard]] Collation CollationOfSpec(std::string_view spec);

// The collation that governs a comparison of two operands. GoogleSQL resolves
// an explicit COLLATE on either side for the whole comparison, and the engine
// folds whenever *either* side asks for it (the same rule the `=` predicate
// applies in expression/binary_expression.cpp), so ordering and filtering can
// never disagree about which rows are peers.
[[nodiscard]] constexpr Collation ResolveCollation(Collation left,
                                                   Collation right) {
  return (left == Collation::kCaseInsensitive ||
          right == Collation::kCaseInsensitive)
             ? Collation::kCaseInsensitive
             : ((left == Collation::kBinary || right == Collation::kBinary)
                    ? Collation::kBinary
                    : Collation::kNone);
}

[[nodiscard]] Collation ResolveCollation(const Value& left, const Value& right);

// ASCII case fold — the single fold rule for case-insensitive collation,
// shared by comparison (`=`), grouping/DISTINCT canonicalization
// (CanonicalDistinctValue) and the sort-key encoders.
[[nodiscard]] std::string FoldCaseForCollation(std::string_view s);

// Applies a collation to a value: a VARCHAR under a case-insensitive
// collation comes back folded, and the collator tag rides along so
// downstream comparisons and LIKE validation still see it. Every other
// (type, collation) pair is returned unchanged.
[[nodiscard]] Value ApplyCollation(const Value& value, Collation collation);

// Applies a key's static collation on top of the value's own tag, the same
// resolution SortKeyOrder::Compare performs, for the byte-encoding paths that
// fold a value before encoding it.
[[nodiscard]] Value ApplyKeyCollation(const Value& value,
                                      Collation key_collation);

// VARCHAR ordering under a resolved collation. `case_insensitive` folds both
// sides before comparing, so it is the *only* code path that changes byte
// order. The compliance catalog's synthetic ENUM members rank by a fixed
// table instead of lexically; that ranking is byte-based and therefore
// applies to the binary collation only (folding would miss every literal).
[[nodiscard]] int CompareVarCharForOrderBy(std::string_view left,
                                           std::string_view right,
                                           bool case_insensitive);

// Three-way comparison of two ORDER BY operands under their resolved
// collation. NULLs compare equal (placement is the caller's decision, see
// SortKeyOrder) and every non-VARCHAR operand, every array, and every
// collation the engine does not implement falls back to CompareForOrderBy
// (binary) unchanged.
[[nodiscard]] int CompareWithCollation(const Value& left, const Value& right);

// The same comparison under a collation the caller already resolved. A sort
// key carries a static collation (COLLATE(...)) that the operands' own tags
// cannot show, so SortKeyOrder merges the two and compares under the result.
[[nodiscard]] int CompareUnderCollation(const Value& left, const Value& right,
                                        Collation collation);

// Peer equality under the engine's ORDER BY / DISTINCT semantics: NULL equals
// NULL, FLOAT64 is canonicalized (every NaN is one peer group and -0.0 equals
// +0.0, exactly as CompareForOrderBy orders them), and a case-insensitive
// VARCHAR pair is equal when the folded forms match -- the same rule
// CanonicalDistinctValue applies before hashing a GROUP BY / DISTINCT key.
// Everything else keeps Value::operator==, so interval-equivalent text still
// compares equal.
[[nodiscard]] bool EqualUnderCollation(const Value& left, const Value& right);

// One ORDER BY term's full ordering contract: NULL placement, direction and
// collation. Every comparison-based sort path in the engine (TopNExecutor,
// PdqSort, MergeAppend, the relational ORDER BY) and the byte-encoding paths
// (SortExecutor in-memory, its external merge runs, PartialSort) resolve
// their keys through this one type, so they cannot drift apart on NULL
// placement, direction or case folding.
//
// `collation` is the key's static collation (from COLLATE()); the operands'
// own tags are resolved on top of it, so a value tagged at runtime (a
// parameter, a subquery output) is honoured even when the key is a bare
// column.
class SortKeyOrder {
 public:
  SortKeyOrder() = default;
  SortKeyOrder(bool ascending, std::optional<bool> nulls_first = std::nullopt,
               Collation collation = Collation::kNone)
      : ascending_(ascending),
        nulls_first_(nulls_first.value_or(ascending)),
        collation_(collation) {}

  // Total order over this key's operands: < 0 when `left` sorts before
  // `right`, 0 when they are peers under both the collation and the type's
  // ordering, > 0 otherwise.
  [[nodiscard]] int Compare(const Value& left, const Value& right) const;
  [[nodiscard]] bool Precedes(const Value& left, const Value& right) const {
    return Compare(left, right) < 0;
  }
  [[nodiscard]] bool Ascending() const { return ascending_; }
  [[nodiscard]] bool NullsFirst() const { return nulls_first_; }
  [[nodiscard]] Collation KeyCollation() const { return collation_; }

 private:
  bool ascending_{true};
  bool nulls_first_{true};
  Collation collation_{Collation::kNone};
};

// Builds the contract for any sort key struct that exposes the three fields
// the engine shares (SortExecutor::Key, TopNExecutor::Key, SortKey,
// TopNKey, IncrementalSortPlan's keys), so no caller re-derives the NULLS
// default.
template <typename KeyLike>
[[nodiscard]] SortKeyOrder MakeSortKeyOrder(const KeyLike& key) {
  return SortKeyOrder{key.ascending, key.nulls_first, key.collation};
}

}  // namespace tinylamb

#endif  // TINYLAMB_TYPE_COLLATION_HPP
