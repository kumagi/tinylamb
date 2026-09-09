/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_TYPE_DATE_HPP
#define TINYLAMB_TYPE_DATE_HPP

#include <cstdint>
#include <string>
#include <string_view>

#include "common/exc_shim.hpp"
#include "common/status_or.hpp"

namespace tinylamb {

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

void SetDefaultTimeZone(std::string_view tz);
[[nodiscard]] std::string GetDefaultTimeZone();

}  // namespace tinylamb

#endif
