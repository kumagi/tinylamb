/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_TYPE_DATE_HPP
#define TINYLAMB_TYPE_DATE_HPP

#include <cstdint>
#include <string>
#include <string_view>

namespace tinylamb {

[[nodiscard]] int64_t ParseDateDays(std::string_view date);
[[nodiscard]] std::string FormatDateDays(int64_t days);
[[nodiscard]] int64_t AddDateIntervalDays(int64_t days, int64_t amount,
                                          std::string_view unit);

void SetDefaultTimeZone(std::string_view tz);
[[nodiscard]] std::string GetDefaultTimeZone();

}  // namespace tinylamb

#endif
