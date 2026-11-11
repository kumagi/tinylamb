/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_TYPE_DATE_HPP
#define TINYLAMB_TYPE_DATE_HPP

#include <cstdint>
#include <string>
#include <string_view>

#include "common/exc_shim.hpp"
#include "common/status_or.hpp"

namespace tinylamb {

// sys_days counts days in a 32-bit representation and chrono::year saturates
// at ±32767; keep every day count well inside both limits.  Shared with the
// bytecode date kernels so they range-check exactly like the formatters.
inline constexpr int64_t kDateMinDays = -11000000;
inline constexpr int64_t kDateMaxDays = 11000000;

[[nodiscard]] StatusOr<int64_t> TryParseDateDays(std::string_view date);
[[nodiscard]] StatusOr<std::string> TryFormatDateDays(int64_t days);
[[nodiscard]] StatusOr<int64_t> TryAddDateIntervalDays(int64_t days,
                                                       int64_t amount,
                                                       std::string_view unit);

// EXC-SHIM: deprecated throwing wrappers (removed with the expression/query
// conversion, see common/exc_shim.hpp).
[[nodiscard]] inline int64_t ParseDateDays(std::string_view date) {
  return ExcShimUnwrap(TryParseDateDays(date), "ParseDateDays");
}
[[nodiscard]] inline std::string FormatDateDays(int64_t days) {
  return ExcShimUnwrap(TryFormatDateDays(days), "FormatDateDays");
}
[[nodiscard]] inline int64_t AddDateIntervalDays(int64_t days, int64_t amount,
                                                 std::string_view unit) {
  return ExcShimUnwrap(TryAddDateIntervalDays(days, amount, unit),
                       "AddDateIntervalDays");
}

// Resolve an IANA zone or a complete signed hour/minute offset. Unknown
// names and malformed numeric offsets are errors, never a default-zone alias.
[[nodiscard]] StatusOr<int> TryTimeZoneOffsetAtLocalTime(
    std::string_view zone, int year, unsigned month, unsigned day, int hour,
    int minute, int second);

// Per-thread SQL default; an empty setting restores UTC.
void SetDefaultTimeZone(std::string_view tz);
[[nodiscard]] std::string GetDefaultTimeZone();

}  // namespace tinylamb

#endif
