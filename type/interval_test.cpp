/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include "type/interval.hpp"

#include <gtest/gtest.h>

#include <cstdint>
#include <stdexcept>
#include <string>

#include "common/status_or.hpp"

namespace tinylamb {

namespace {
constexpr int64_t kDayNanos = 24LL * 3600LL * 1000000000LL;
constexpr int64_t kMonthNanos = 30LL * kDayNanos;

// Helper keeping gtest macro arguments comma-free.
IntervalValue Iv(int64_t months, int64_t days, int64_t nanos) {
  return IntervalValue{.months = months, .days = days, .nanos = nanos};
}
}  // namespace

// ------------------------------------------------------------- arithmetic ---

TEST(IntervalTest, TotalNanosAndFieldEquality) {
  EXPECT_EQ(IntervalValue{}.TryTotalNanos().MoveValue(), 0);
  EXPECT_EQ(Iv(0, 1, 0).TryTotalNanos().MoveValue(), kDayNanos);
  EXPECT_EQ(Iv(1, 0, 0).TryTotalNanos().MoveValue(), kMonthNanos);
  EXPECT_EQ(Iv(0, 0, 5).TryTotalNanos().MoveValue(), 5);

  // Field-wise equality: one day is not equal to 24 hours of nanos in a
  // different field shape, even though the totals agree.
  const IntervalValue one_day = Iv(0, 1, 0);
  const IntervalValue day_as_nanos = Iv(0, 0, kDayNanos);
  EXPECT_EQ(one_day.TryTotalNanos().MoveValue(),
            day_as_nanos.TryTotalNanos().MoveValue());
  EXPECT_NE(one_day, day_as_nanos);
}

TEST(IntervalTest, OrderingIsFieldWise) {
  EXPECT_GT(Iv(1, 0, 0), Iv(0, 40, 0));
  EXPECT_LT(Iv(0, 40, 0), Iv(1, 0, 0));
  EXPECT_LT(Iv(0, 1, 5), Iv(0, 1, 6));
  EXPECT_EQ(Iv(3, 2, 1) <=> Iv(3, 2, 1), std::strong_ordering::equal);
}

TEST(IntervalTest, PlusMinusNegateMultiply) {
  const IntervalValue a = Iv(1, 2, 3);
  const IntervalValue b = Iv(10, 20, 30);
  EXPECT_EQ(a.TryPlus(b).MoveValue(), Iv(11, 22, 33));
  EXPECT_EQ(b.TryMinus(a).MoveValue(), Iv(9, 18, 27));
  EXPECT_EQ(a.TryNegate().MoveValue(), Iv(-1, -2, -3));
  EXPECT_EQ(a.TryMultiply(3).MoveValue(), Iv(3, 6, 9));
  EXPECT_EQ(a.TryMultiply(0).MoveValue(), Iv(0, 0, 0));

  // Overflow paths report kInvalidArgument instead of wrapping.
  const IntervalValue huge = Iv(INT64_MAX, 0, 0);
  EXPECT_FALSE(huge.TryPlus(Iv(1, 0, 0)).HasValue());
  const IntervalValue min = Iv(0, INT64_MIN, 0);
  EXPECT_FALSE(min.TryNegate().HasValue());
  EXPECT_FALSE(Iv(1, 0, 0).TryMultiply(INT64_MAX).HasValue());
  EXPECT_FALSE(huge.TryTotalNanos().HasValue());

  // Throwing wrappers mirror the Try* forms.
  EXPECT_EQ(a + b, Iv(11, 22, 33));
  EXPECT_EQ(b - a, Iv(9, 18, 27));
  EXPECT_EQ(-a, Iv(-1, -2, -3));
  EXPECT_EQ(a * 3, Iv(3, 6, 9));
  EXPECT_THROW((void)(huge + b), std::runtime_error);
  EXPECT_THROW((void)(-min), std::runtime_error);
  EXPECT_THROW((void)huge.TotalNanos(), std::runtime_error);
}

// --------------------------------------------------------------- justify ---

TEST(IntervalTest, JustifyHours) {
  EXPECT_EQ(
      Iv(1, 2, 36LL * 3600LL * 1000000000LL).TryJustifyHours().MoveValue(),
      Iv(1, 3, 12LL * 3600LL * 1000000000LL));
  EXPECT_EQ(
      Iv(0, 1, -12LL * 3600LL * 1000000000LL).TryJustifyHours().MoveValue(),
      Iv(0, 0, 12LL * 3600LL * 1000000000LL));
  EXPECT_EQ(Iv(2, 1, 7).TryJustifyHours().MoveValue(), Iv(2, 1, 7));
  // days * day-nanos overflow is rejected.
  EXPECT_FALSE(Iv(0, 4000000000, 0).TryJustifyHours().HasValue());
  EXPECT_EQ(Iv(1, 2, 3).JustifyHours(), Iv(1, 2, 3));
}

TEST(IntervalTest, JustifyDays) {
  EXPECT_EQ(Iv(1, 45, 5).TryJustifyDays().MoveValue(), Iv(2, 15, 5));
  EXPECT_EQ(Iv(0, -31, -7).TryJustifyDays().MoveValue(), Iv(-1, -1, -7));
  EXPECT_EQ(Iv(0, 29, 0).TryJustifyDays().MoveValue(), Iv(0, 29, 0));
  // months * 30 overflow is rejected.
  EXPECT_FALSE(Iv(INT64_MAX, 0, 0).TryJustifyDays().HasValue());
  EXPECT_THROW((void)Iv(INT64_MAX, 0, 0).JustifyDays(), std::runtime_error);
  EXPECT_EQ(Iv(1, 45, 5).JustifyDays(), Iv(2, 15, 5));
}

TEST(IntervalTest, JustifyInterval) {
  EXPECT_EQ(
      Iv(0, 0, kMonthNanos + kDayNanos + 1).TryJustifyInterval().MoveValue(),
      Iv(1, 1, 1));
  EXPECT_EQ(Iv(0, 0, -(kMonthNanos + 1)).TryJustifyInterval().MoveValue(),
            Iv(-1, 0, -1));
  EXPECT_FALSE(Iv(INT64_MAX, 0, 0).TryJustifyInterval().HasValue());
  EXPECT_EQ(Iv(0, 0, kMonthNanos + kDayNanos + 1).JustifyInterval(),
            Iv(1, 1, 1));
  EXPECT_THROW((void)Iv(INT64_MAX, 0, 0).JustifyInterval(), std::runtime_error);
}

TEST(IntervalTest, JustifyPreservesMinimumInt64Components) {
  constexpr int64_t min = INT64_MIN;
  EXPECT_EQ(Iv(7, 0, min).TryJustifyHours().MoveValue(),
            Iv(7, min / kDayNanos, min % kDayNanos));
  EXPECT_EQ(Iv(0, min, 7).TryJustifyDays().MoveValue(),
            Iv(min / 30, min % 30, 7));
  EXPECT_EQ(
      Iv(0, 0, min).TryJustifyInterval().MoveValue(),
      Iv(min / kMonthNanos, (min % kMonthNanos) / kDayNanos, min % kDayNanos));
}

// ---------------------------------------------------------------- output ---

TEST(IntervalTest, ToStringShapes) {
  EXPECT_EQ(Iv(14, 3, 14706789000000).TryToString().MoveValue(),
            "1-2 3 4:5:6.789");
  EXPECT_EQ(Iv(0, 0, 500000000).TryToString().MoveValue(), "0-0 0 0:0:0.5");
  // Trailing zeros of the fractional part are trimmed.
  EXPECT_EQ(Iv(0, 0, 1500000000).TryToString().MoveValue(), "0-0 0 0:0:1.5");
  EXPECT_EQ(Iv(0, 0, 3600000000000).TryToString().MoveValue(), "0-0 0 1:0:0");
  EXPECT_EQ(Iv(-14, 3, -14706789000000).TryToString().MoveValue(),
            "-1-2 3 -4:5:6.789");
  EXPECT_EQ(Iv(0, 0, 0).TryToString().MoveValue(), "0-0 0 0:0:0");
  EXPECT_EQ(Iv(0, 0, 123000000).TryToString().MoveValue(), "0-0 0 0:0:0.123");
  EXPECT_EQ(Iv(0, 0, 0).ToString(), "0-0 0 0:0:0");
}

// ----------------------------------------------------------------- parse ---

TEST(IntervalTest, ParseIso8601) {
  EXPECT_EQ(IntervalValue::TryParse("P1Y2M3DT4H5M6.789S").MoveValue(),
            Iv(14, 3, 14706789000000));
  EXPECT_EQ(IntervalValue::TryParse("P").MoveValue(), Iv(0, 0, 0));
  EXPECT_EQ(IntervalValue::TryParse("PT").MoveValue(), Iv(0, 0, 0));
  EXPECT_EQ(IntervalValue::TryParse("P-1Y").MoveValue(), Iv(-12, 0, 0));
  EXPECT_EQ(IntervalValue::TryParse("P+1Y").MoveValue(), Iv(12, 0, 0));
  // A mid-string sign applies to the following component only.
  EXPECT_EQ(IntervalValue::TryParse("P1Y-2M").MoveValue(), Iv(10, 0, 0));
  EXPECT_EQ(IntervalValue::TryParse("P1.5Y").MoveValue().months, 12);
  EXPECT_EQ(IntervalValue::TryParse("p1d").MoveValue().days, 1);
  // Malformed / out-of-range inputs.
  EXPECT_FALSE(IntervalValue::TryParse("P-").HasValue());
  EXPECT_FALSE(IntervalValue::TryParse("P1W").HasValue());
  EXPECT_FALSE(IntervalValue::TryParse("PXY").HasValue());
  EXPECT_FALSE(IntervalValue::TryParse("P1").HasValue());
  EXPECT_FALSE(IntervalValue::TryParse("P9e300Y").HasValue());
  EXPECT_FALSE(IntervalValue::TryParse("PnanY").HasValue());
  EXPECT_FALSE(IntervalValue::TryParse("PT9e300S").HasValue());
  // Throwing wrapper.
  EXPECT_EQ(IntervalValue::Parse("P1Y").months, 12);
  EXPECT_THROW(IntervalValue::Parse("P1W"), std::runtime_error);
}

TEST(IntervalTest, ParseSingleUnitsIntegral) {
  EXPECT_EQ(IntervalValue::TryParse("2", "YEAR").MoveValue().months, 24);
  EXPECT_EQ(IntervalValue::TryParse("2", "years").MoveValue().months, 24);
  EXPECT_EQ(IntervalValue::TryParse("5", "QUARTER").MoveValue().months, 15);
  EXPECT_EQ(IntervalValue::TryParse("5", "quarters").MoveValue().months, 15);
  EXPECT_EQ(IntervalValue::TryParse("3", "month").MoveValue().months, 3);
  EXPECT_EQ(IntervalValue::TryParse("3", "MONTHS").MoveValue().months, 3);
  EXPECT_EQ(IntervalValue::TryParse("4", "week").MoveValue().days, 28);
  EXPECT_EQ(IntervalValue::TryParse("4", "WEEKS").MoveValue().days, 28);
  EXPECT_EQ(IntervalValue::TryParse("-6", "day").MoveValue().days, -6);
  EXPECT_EQ(IntervalValue::TryParse("6", "days").MoveValue().days, 6);
  EXPECT_EQ(IntervalValue::TryParse("1", "hour").MoveValue().nanos,
            3600LL * 1000000000LL);
  EXPECT_EQ(IntervalValue::TryParse("1", "hours").MoveValue().nanos,
            3600LL * 1000000000LL);
  EXPECT_EQ(IntervalValue::TryParse("2", "minute").MoveValue().nanos,
            2LL * 60LL * 1000000000LL);
  EXPECT_EQ(IntervalValue::TryParse("2", "minutes").MoveValue().nanos,
            2LL * 60LL * 1000000000LL);
  EXPECT_EQ(IntervalValue::TryParse("3", "second").MoveValue().nanos,
            3000000000LL);
  EXPECT_EQ(IntervalValue::TryParse("3", "seconds").MoveValue().nanos,
            3000000000LL);
  EXPECT_EQ(IntervalValue::TryParse("4", "millisecond").MoveValue().nanos,
            4000000);
  EXPECT_EQ(IntervalValue::TryParse("4", "milliseconds").MoveValue().nanos,
            4000000);
  EXPECT_EQ(IntervalValue::TryParse("5", "microsecond").MoveValue().nanos,
            5000);
  EXPECT_EQ(IntervalValue::TryParse("5", "microseconds").MoveValue().nanos,
            5000);
  EXPECT_EQ(IntervalValue::TryParse("6", "nanosecond").MoveValue().nanos, 6);
  EXPECT_EQ(IntervalValue::TryParse("6", "nanoseconds").MoveValue().nanos, 6);
  // Large integral values parse exactly without double rounding.
  EXPECT_EQ(
      IntervalValue::TryParse("9223372036854775806", "DAY").MoveValue().days,
      9223372036854775806LL);
  // DAY / MONTH / NANOSECOND return the raw fields without a final
  // range re-check, so an exactly-int64-max day count parses successfully
  // (using it later in arithmetic is what reports out-of-range).
  EXPECT_EQ(
      IntervalValue::TryParse("9223372036854775807", "DAY").MoveValue().days,
      9223372036854775807LL);
  EXPECT_EQ(IntervalValue::TryParse("9223372036854775807", "NANOSECOND")
                .MoveValue()
                .nanos,
            9223372036854775807LL);
  // HOUR, by contrast, goes through the checked multiply path.
  EXPECT_FALSE(
      IntervalValue::TryParse("9223372036854775807", "HOUR").HasValue());
  EXPECT_FALSE(
      IntervalValue::TryParse("9223372036854775807", "MILLISECOND").HasValue());
  EXPECT_FALSE(
      IntervalValue::TryParse("9223372036854775807", "MICROSECOND").HasValue());
  EXPECT_FALSE(
      IntervalValue::TryParse("9223372036854775807", "WEEK").HasValue());
  EXPECT_FALSE(
      IntervalValue::TryParse("9223372036854775807", "YEAR").HasValue());
  EXPECT_FALSE(
      IntervalValue::TryParse("9223372036854775807", "QUARTER").HasValue());
}

TEST(IntervalTest, ParseSingleUnitsFractional) {
  EXPECT_EQ(IntervalValue::TryParse("1.5", "YEAR").MoveValue().months, 18);
  EXPECT_EQ(IntervalValue::TryParse("1.5", "quarter").MoveValue().months, 5);
  EXPECT_EQ(IntervalValue::TryParse("1.5", "month").MoveValue().months, 2);
  EXPECT_EQ(IntervalValue::TryParse("1.5", "week").MoveValue().days, 11);
  EXPECT_EQ(IntervalValue::TryParse("1.5", "day").MoveValue().days, 2);
  EXPECT_EQ(IntervalValue::TryParse("-2.5", "hour").MoveValue().nanos,
            -9000000000000LL);
  EXPECT_EQ(IntervalValue::TryParse("1.5", "minute").MoveValue().nanos,
            90LL * 1000000000LL);
  EXPECT_EQ(IntervalValue::TryParse("0.5", "second").MoveValue().nanos,
            500000000LL);
  EXPECT_EQ(IntervalValue::TryParse("1.5", "millisecond").MoveValue().nanos,
            1500000);
  EXPECT_EQ(IntervalValue::TryParse("1.5", "microsecond").MoveValue().nanos,
            1500);
  EXPECT_EQ(IntervalValue::TryParse("1.5", "nanosecond").MoveValue().nanos, 2);
  // Literals beyond the int64 range are rejected instead of wrapping.
  EXPECT_FALSE(IntervalValue::TryParse("1e300", "YEAR").HasValue());
  EXPECT_FALSE(IntervalValue::TryParse("1e300", "NANOSECOND").HasValue());
}

TEST(IntervalTest, ParseCompositeStrings) {
  EXPECT_EQ(IntervalValue::TryParse("1-2 3 4:5:6.789").MoveValue(),
            Iv(14, 3, 14706789000000));
  // A leading sign on a composite part flips the whole part.
  EXPECT_EQ(IntervalValue::TryParse("-4:5:6.789").MoveValue().nanos,
            -14706789000000LL);
  EXPECT_EQ(IntervalValue::TryParse("-1-2").MoveValue().months, -14);
  EXPECT_EQ(IntervalValue::TryParse("25:0:0").MoveValue().nanos,
            25LL * 3600LL * 1000000000LL);
  // Both 2-part fields land in days when no unit names them.
  EXPECT_EQ(IntervalValue::TryParse("-20 30").MoveValue().days, 10);
  EXPECT_EQ(IntervalValue::TryParse("0-0 29 24:0:0").MoveValue(),
            Iv(0, 29, kDayNanos));
  EXPECT_EQ(IntervalValue::TryParse("  1-0 0 0:0:0  ").MoveValue().months, 12);
  // Out-of-range time spellings.
  EXPECT_FALSE(IntervalValue::TryParse("0:0:1e999").HasValue());
  EXPECT_FALSE(IntervalValue::TryParse("0:0:inf").HasValue());
  EXPECT_EQ(
      IntervalValue::TryParse("9223372036854775807 0:0:0").MoveValue().days,
      9223372036854775807LL);
  // An out-of-range hour field trips the checked multiply path.
  EXPECT_FALSE(IntervalValue::TryParse("0 9223372036854775807:0:0").HasValue());
}

TEST(IntervalTest, ParseUnitRanges) {
  EXPECT_EQ(IntervalValue::TryParse("1 2", "year to month").MoveValue(),
            Iv(14, 0, 0));
  EXPECT_EQ(IntervalValue::TryParse("3 4", "month to day").MoveValue(),
            Iv(3, 4, 0));
  EXPECT_EQ(IntervalValue::TryParse("1 2:3:4", "day to second").MoveValue(),
            Iv(0, 1, 7384LL * 1000000000LL));
  EXPECT_EQ(IntervalValue::TryParse("1 2", "hour to minute").MoveValue(),
            Iv(0, 0, 3720LL * 1000000000LL));
  // Wrong part counts fall back to the generic composite accumulation.
  EXPECT_EQ(IntervalValue::TryParse("1 2 3", "year to month").MoveValue(),
            Iv(12, 2, 0));
  // The range spelling with a mismatched part count defers to the generic
  // accumulation, which stores raw fields without a final re-check.
  EXPECT_EQ(
      IntervalValue::TryParse("9223372036854775807 0:0:0", "day to second")
          .MoveValue()
          .days,
      9223372036854775807LL);
  // A proper 4-part range does take the checked assignment path: the
  // components map onto day, hour, minute, second respectively.
  EXPECT_EQ(IntervalValue::TryParse("1 2 0:0:3 0", "day to second").MoveValue(),
            Iv(0, 1, 7203000000000LL));
}

TEST(IntervalTest, ParseEdgeCases) {
  EXPECT_EQ(IntervalValue::TryParse("").MoveValue(), Iv(0, 0, 0));
  EXPECT_EQ(IntervalValue::TryParse("   ").MoveValue(), Iv(0, 0, 0));
  // A bare number carries no unit information.
  EXPECT_FALSE(IntervalValue::TryParse("123").HasValue());
  // Throwing wrapper propagates parse failures.
  EXPECT_THROW(IntervalValue::Parse("123"), std::runtime_error);
}

// ----------------------------------------------------- session constants ---

TEST(IntervalTest, SessionConstants) {
  EXPECT_FALSE(HasSessionConstant("interval_test_constant"));
  SetSessionConstant("interval_test_constant", "1-2 3 4:5:6.789");
  EXPECT_TRUE(HasSessionConstant("interval_test_constant"));
  EXPECT_EQ(GetSessionConstant("interval_test_constant"), "1-2 3 4:5:6.789");
  SetSessionConstant("interval_test_constant", "updated");
  EXPECT_EQ(GetSessionConstant("interval_test_constant"), "updated");
  EXPECT_EQ(GetSessionConstant("never_set"), "");
  EXPECT_FALSE(HasSessionConstant("never_set"));
}

TEST(IntervalTest, AuditQualifiedTimeRejectsOverflow) {
  for (const char* text :
       {"3000000000000000:0:0", "-3000000000000000:0:0", "0:3000000000000000:0",
        "3000000:0:0", "3000000000000000:0:0 0 0", "3000000:0:0 0 0"}) {
    EXPECT_FALSE(IntervalValue::TryParse(text, "hour to second").HasValue())
        << text;
  }
  auto valid = IntervalValue::TryParse("1:2:3.000000001", "hour to second");
  ASSERT_TRUE(valid.HasValue());
  EXPECT_EQ(valid.Value().nanos, 3723000000001LL);
}

}  // namespace tinylamb
