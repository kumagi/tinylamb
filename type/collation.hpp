/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_TYPE_COLLATION_HPP
#define TINYLAMB_TYPE_COLLATION_HPP

#include <cstdint>
#include <optional>
#include <span>
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

// Inverse of CollationName, for PhysicalProperties::collation wire names
// only: "" is "no collator" (the engine default) and every other name is
// resolved to a collator. Unknown/legacy property names (e.g. "en_US") are
// deliberately mapped to case-insensitive here — see
// CollationTest.NamesRoundTripAndSpecResolution. This is NOT the COLLATE()
// spec parser; use CollationOfSpec when an unsupported spec must be rejected.
[[nodiscard]] Collation CollationFromName(std::string_view spec);

// The strict COLLATE(value, 'spec') parser. GoogleSQL only permits 'binary'
// and 'und:ci'; every other spec (including typos such as 'und-cl') returns
// nullopt so callers can reject it with an error instead of silently folding
// it to case-insensitive. CollationFromName has a deliberately different,
// lenient policy for stored physical-property names.
[[nodiscard]] std::optional<Collation> CollationOfSpec(std::string_view spec);

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

// Allocation-free sibling of FoldCaseForCollation for hash paths (T3752):
// folds each byte through the same ASCII A-Z -> a-z map and streams the
// result into FNV-1a, so hashing a row pays no per-cell scratch string.
// Only deterministic dependence on the folded bytes matters for bucketing,
// and that matches FoldCaseForCollation byte for byte.
[[nodiscard]] inline size_t HashFoldedForCollation(
    std::string_view s) noexcept {
  uint64_t hash = 14695981039346656037ULL;  // FNV-1a offset basis
  for (const char c : s) {
    const auto uc = static_cast<unsigned char>(c);
    hash ^= uc >= 'A' && uc <= 'Z' ? static_cast<uint64_t>(uc + ('a' - 'A'))
                                   : static_cast<uint64_t>(uc);
    hash *= 1099511628211ULL;  // FNV-1a prime
  }
  return static_cast<size_t>(hash);
}

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
                                           bool case_insensitive) noexcept;

// ENUM member rank for synthetic ENUM values ("TESTENUMNEGATIVE" -> 0,
// "TESTENUM0" -> 1, ...). Returns -1 for non-ENUM strings. Used by sort-key
// encoders so ENUM members sort by rank, not raw bytes.
[[nodiscard]] int EnumRank(std::string_view value);

// Three-way comparison of two ORDER BY operands under their resolved
// collation. NULLs compare equal (placement is the caller's decision, see
// SortKeyOrder) and every non-VARCHAR operand, every array, and every
// collation the engine does not implement falls back to CompareForOrderBy
// unchanged. For FLOAT64 operands that order places all NaNs together before
// other FLOAT64 values; NULL placement and descending order are applied by
// SortKeyOrder. Different value types use CompareForOrderBy's type ranks.
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
// (SortExecutor in-memory, its external merge runs, the sort executors) resolve
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

  // Observe every input before heap admission or spill partitioning. NULL and
  // non-text keys carry no string ordering requirement.
  [[nodiscard]] Status Observe(const Value& value);
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
  Collation observed_{Collation::kNone};
};

inline Status ObserveSortKeys(std::span<SortKeyOrder> orders,
                              std::span<const Value> values) {
  for (size_t i = 0; i < orders.size(); ++i) {
    RETURN_IF_FAIL(orders[i].Observe(values[i]));
  }
  return Status::kSuccess;
}

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
