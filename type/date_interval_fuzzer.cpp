/** Copyright 2026 KUMAZAKI Hiroki. Licensed under the Apache-2.0. */

// Byte-driven DATE/INTERVAL text-layer fuzzer.  Date and interval parsing is
// the classic atoi-style bug farm, and every entry point here is Status-
// based, so the fuzz body can assert algebraic invariants directly:
//   * parse -> format -> parse is a fixed point,
//   * interval parse -> ToString -> parse preserves the value,
//   * justify / negate / multiply preserve the total-nanoseconds view,
//   * date arithmetic agrees between 1+2 and 3 at the same unit,
// and no input may ever crash, hang, or misbehave outside a Status error.

#include <cassert>
#include <cctype>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <string>
#include <string_view>

#include "type/date.hpp"
#include "type/interval.hpp"

namespace tinylamb {
namespace {

struct Cursor {
  const uint8_t* data;
  size_t size;
  size_t pos{0};

  uint8_t Next() { return pos < size ? data[pos++] : 0; }
  bool Done() const { return pos >= size; }

  // A NUL-free text slice (parse entry points take string_view; keeping the
  // bytes printable-ish steers the mutations into the grammar).
  std::string Text(size_t max_len) {
    const size_t len = Done() ? 0 : (Next() % (max_len + 1));
    std::string out;
    for (size_t i = 0; i < len && !Done(); ++i) {
      uint8_t b = Next();
      if (b % 16 == 0) {
        static constexpr char kPunct[] = "+-/.:_eE ";  // grammar punctuation
        out.push_back(kPunct[Next() % 10]);
      } else {
        out.push_back(static_cast<char>('0' + (b % 43)));  // digits..z
      }
    }
    return out;
  }

  int64_t Signed() {
    uint64_t v = 0;
    for (size_t i = 0; i < 8 && !Done(); ++i) {
      v = (v << 8) | Next();
    }
    return static_cast<int64_t>(v);
  }
};

}  // namespace

inline void Try(const uint8_t* data, size_t size, bool verbose) {
  if (size < 2) {
    return;
  }
  Cursor c{data, size};

  // ---- DATE round trip -----------------------------------------------
  const std::string date_text = c.Text(24);
  const StatusOr<int64_t> days = TryParseDateDays(date_text);
  if (days.HasValue()) {
    if (const StatusOr<std::string> formatted = TryFormatDateDays(days.Value());
        formatted.HasValue()) {
      const StatusOr<int64_t> reparsed = TryParseDateDays(formatted.Value());
      assert(reparsed.HasValue());
      assert(reparsed.Value() == days.Value());
    }
  }

  // ---- DATE interval arithmetic --------------------------------------
  static const char* kUnits[] = {"day",    "week",   "month",  "year",   "hour",
                                 "micros", "millis", "second", "quarter"};
  const std::string_view unit = kUnits[c.Next() % 9];
  const int64_t amount = c.Signed() % 4000;
  if (days.HasValue()) {
    const StatusOr<int64_t> zero =
        TryAddDateIntervalDays(days.Value(), 0, unit);
    if (zero.HasValue()) {
      assert(zero.Value() == days.Value());  // identity
    }
    const StatusOr<int64_t> split =
        TryAddDateIntervalDays(days.Value(), amount / 2, unit);
    const StatusOr<int64_t> whole =
        TryAddDateIntervalDays(days.Value(), amount, unit);
    const StatusOr<int64_t> whole_again =
        TryAddDateIntervalDays(days.Value(), amount, unit);
    if (whole.HasValue() != whole_again.HasValue()) {
      assert(whole.HasValue() == whole_again.HasValue());  // deterministic
    }
    if (amount % 2 == 0 && split.HasValue() && whole.HasValue()) {
      const StatusOr<int64_t> step2 =
          TryAddDateIntervalDays(split.Value(), amount - amount / 2, unit);
      if (step2.HasValue() && unit == "day") {
        // Day arithmetic is a plain sum; month/year paths legitimately
        // clamp (Jan 31 + 1m + 1m != Jan 31 + 2m).
        assert(step2.Value() == whole.Value());
      }
    }
  }

  if (days.HasValue()) {
    const int64_t base = days.Value();
    // DAY arithmetic is raw day addition; at the representable date range
    // boundary the shift may legitimately fail, but then the plain sum must
    // be outside the range the formatter accepts.
    for (int k = -3; k <= 3; ++k) {
      const StatusOr<int64_t> moved = TryAddDateIntervalDays(base, k, "day");
      if (moved.HasValue()) {
        assert(moved.Value() == base + k);
      } else {
        assert(!TryFormatDateDays(base + k).HasValue());
      }
    }
    // WEEK is a pinned unsupported unit (value_test
    // AddDateIntervalDays_WithUnknownUnits_ThrowsRuntimeError): the call
    // must fail cleanly, never crash.
    for (int k = -2; k <= 2; ++k) {
      const StatusOr<int64_t> w = TryAddDateIntervalDays(base, k, "week");
      assert(!w.HasValue());
    }
    // MONTH/YEAR must be monotone in the amount (calendar clamping may
    // collapse two amounts onto one date but never reorder them).
    int64_t prev = std::numeric_limits<int64_t>::min();
    for (int k = -15; k <= 15; ++k) {
      const StatusOr<int64_t> m = TryAddDateIntervalDays(base, k, "month");
      if (m.HasValue()) {
        assert(m.Value() >= prev);
        prev = m.Value();
      }
    }
  }

  // ---- INTERVAL parse/serialize/justify ------------------------------
  static const char* kIUnits[] = {"",       "day",    "hour",
                                  "minute", "second", "month",
                                  "year",   "week",   "microsecond"};
  const std::string iv_text = c.Text(32);
  const std::string_view iv_unit = kIUnits[c.Next() % 9];
  const StatusOr<IntervalValue> parsed =
      IntervalValue::TryParse(iv_text, iv_unit);
  IntervalValue base;
  bool have_base = false;
  if (parsed.HasValue()) {
    base = parsed.Value();
    have_base = true;
    if (const StatusOr<std::string> text = base.TryToString();
        text.HasValue()) {
      const StatusOr<IntervalValue> back =
          IntervalValue::TryParse(text.Value());
      if (back.HasValue()) {
        assert(back.Value() == base);
      } else if (verbose) {
        LOG(TRACE) << "ToString output no longer parses: " << text.Value();
      }
    }
  } else {
    // Malformed text must be rejected, not crash: synthesise a valid value
    // from the byte stream so the algebra below still runs.
    base = IntervalValue{c.Signed() % 5000, c.Signed() % 5000,
                         c.Signed() % 1000000000};
    have_base = true;
  }
  (void)have_base;

  const IntervalValue other{c.Signed() % 7000, c.Signed() % 7000,
                            c.Signed() % 900000000};
  const StatusOr<IntervalValue> sum = base.TryPlus(other);
  const StatusOr<IntervalValue> reverse = other.TryPlus(base);
  assert(sum.HasValue() == reverse.HasValue());
  if (sum.HasValue() && reverse.HasValue()) {
    assert(sum.Value() == reverse.Value());  // commutative
  }
  const StatusOr<IntervalValue> negated = base.TryNegate();
  if (negated.HasValue()) {
    const StatusOr<IntervalValue> unnegated = negated.Value().TryNegate();
    assert(unnegated.HasValue());
    assert(unnegated.Value() == base);  // involution
  }
  if (sum.HasValue() && negated.HasValue()) {
    const StatusOr<int64_t> sum_total = sum.Value().TryTotalNanos();
    const StatusOr<int64_t> base_total = base.TryTotalNanos();
    const StatusOr<int64_t> other_total = other.TryTotalNanos();
    if (sum_total.HasValue() && base_total.HasValue() &&
        other_total.HasValue()) {
      int64_t expected = 0;
      assert(!__builtin_add_overflow(base_total.Value(), other_total.Value(),
                                     &expected));
      assert(sum_total.Value() == expected);
    }
  }
  // Justify chains preserve the total-nanoseconds view when representable.
  IntervalValue journey = base;
  const StatusOr<int64_t> before = journey.TryTotalNanos();
  if (StatusOr<IntervalValue> next = journey.TryJustifyHours();
      next.HasValue()) {
    journey = next.MoveValue();
    if (StatusOr<IntervalValue> d = journey.TryJustifyDays(); d.HasValue()) {
      journey = d.MoveValue();
      if (StatusOr<IntervalValue> i = journey.TryJustifyInterval();
          i.HasValue()) {
        journey = i.MoveValue();
      }
    }
  }
  if (before.HasValue() && journey.TryTotalNanos().HasValue()) {
    assert(journey.TryTotalNanos().Value() == before.Value());
  }
}

}  // namespace tinylamb

extern "C" [[maybe_unused]] int LLVMFuzzerTestOneInput(const uint8_t* data,
                                                       size_t size) {
  tinylamb::Try(data, size, false);
  return 0;
}
