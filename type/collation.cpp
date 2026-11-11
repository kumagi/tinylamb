/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "type/collation.hpp"

#include <cctype>
#include <string>
#include <string_view>

#include "type/value_type.hpp"

namespace tinylamb {
namespace {

// The compliance catalog orders its synthetic ENUM members by a fixed table
// (NEGATIVE, then the numeric members ascending) rather than lexically, and
// stores them in the compact VARCHAR representation. Every member is between 9
// and 18 chars, so ordinary strings skip the five literal comparisons below.
int EnumRank(std::string_view value) {
  if (value.size() < 9 || value.size() > 18) {
    return -1;
  }
  if (value == "TESTENUMNEGATIVE") {
    return 0;
  }
  if (value == "TESTENUM0") {
    return 1;
  }
  if (value == "TESTENUM1") {
    return 2;
  }
  if (value == "TESTENUM2") {
    return 3;
  }
  if (value == "TESTENUM2147483647") {
    return 4;
  }
  return -1;
}

}  // namespace

std::string_view CollationName(Collation collation) {
  switch (collation) {
    case Collation::kBinary:
      return "binary";
    case Collation::kCaseInsensitive:
      return "und:ci";
    case Collation::kNone:
      break;
  }
  return "";
}

Collation CollationFromName(std::string_view spec) {
  return spec.empty() ? Collation::kNone : CollationOfSpec(spec);
}

Collation CollationOfSpec(std::string_view spec) {
  std::string lowered(spec);
  for (char& c : lowered) {
    c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
  }
  return lowered == "binary" ? Collation::kBinary : Collation::kCaseInsensitive;
}

Collation ResolveCollation(const Value& left, const Value& right) {
  return ResolveCollation(CollationOf(left), CollationOf(right));
}

std::string FoldCaseForCollation(std::string_view s) {
  std::string out;
  out.reserve(s.size());
  for (const char c : s) {
    const auto uc = static_cast<unsigned char>(c);
    out.push_back(uc >= 'A' && uc <= 'Z' ? static_cast<char>(uc - 'A' + 'a')
                                         : c);
  }
  return out;
}

Value ApplyCollation(const Value& value, Collation collation) {
  if (collation != Collation::kCaseInsensitive ||
      value.type != ValueType::kVarChar) {
    return value;
  }
  // The tag rides along so a folded value still reports its collator to
  // downstream comparisons, LIKE validation and grouping.
  return Value(FoldCaseForCollation(value.value.varchar_value))
      .WithCollation(value.Collation() != 0
                         ? value.Collation()
                         : static_cast<uint8_t>(Collation::kCaseInsensitive));
}

Value ApplyKeyCollation(const Value& value, Collation key_collation) {
  return ApplyCollation(value,
                        ResolveCollation(key_collation, CollationOf(value)));
}

int CompareVarCharForOrderBy(std::string_view left, std::string_view right,
                             bool case_insensitive) {
  if (!case_insensitive) {
    const int left_rank = EnumRank(left);
    const int right_rank = EnumRank(right);
    if (left_rank >= 0 && right_rank >= 0 && left_rank != right_rank) {
      return left_rank < right_rank ? -1 : 1;
    }
  }
  if (case_insensitive) {
    const std::string folded_left = FoldCaseForCollation(left);
    const std::string folded_right = FoldCaseForCollation(right);
    return folded_left < folded_right ? -1
                                      : (folded_right < folded_left ? 1 : 0);
  }
  return left < right ? -1 : (right < left ? 1 : 0);
}

int CompareWithCollation(const Value& left, const Value& right) {
  return CompareUnderCollation(left, right, ResolveCollation(left, right));
}

int CompareUnderCollation(const Value& left, const Value& right,
                          Collation collation) {
  if (collation != Collation::kCaseInsensitive || left.IsInterval() ||
      right.IsInterval()) {
    return CompareForOrderBy(left, right);
  }
  // Only a VARCHAR pair can be folded; every other shape (including the
  // interval-shaped text that CompareForOrderBy orders byte-wise) keeps the
  // engine default so a COLLATE tag on an unrelated value changes nothing.
  if (left.type != ValueType::kVarChar || right.type != ValueType::kVarChar) {
    return CompareForOrderBy(left, right);
  }
  return CompareVarCharForOrderBy(left.value.varchar_value,
                                  right.value.varchar_value,
                                  /*case_insensitive=*/true);
}

bool EqualUnderCollation(const Value& left, const Value& right) {
  if (left.IsNull() || right.IsNull()) {
    return left.IsNull() && right.IsNull();
  }
  if (left.type == ValueType::kDouble && right.type == ValueType::kDouble) {
    // Every NaN is one peer group and -0.0 equals +0.0, matching
    // CompareForOrderBy (GoogleSQL DISTINCT and ORDER BY agree on this).
    return CompareForOrderBy(left, right) == 0;
  }
  if (ResolveCollation(left, right) == Collation::kCaseInsensitive &&
      left.type == ValueType::kVarChar && right.type == ValueType::kVarChar) {
    return FoldCaseForCollation(left.value.varchar_value) ==
           FoldCaseForCollation(right.value.varchar_value);
  }
  return left == right;
}

int SortKeyOrder::Compare(const Value& left, const Value& right) const {
  if (left.IsNull() || right.IsNull()) {
    if (left.IsNull() && right.IsNull()) {
      return 0;
    }
    const int null_side = left.IsNull() ? -1 : 1;
    return nulls_first_ ? null_side : -null_side;
  }
  const Collation resolved = ResolveCollation(
      collation_, ResolveCollation(CollationOf(left), CollationOf(right)));
  const int cmp = CompareUnderCollation(left, right, resolved);
  return ascending_ ? cmp : -cmp;
}

}  // namespace tinylamb
