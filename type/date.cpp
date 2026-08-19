/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "type/date.hpp"

#include <chrono>
#include <cstdio>
#include <stdexcept>

namespace tinylamb {

int64_t ParseDateDays(std::string_view date) {
  int year = 0;
  unsigned month = 0;
  unsigned day = 0;
  if (std::sscanf(std::string(date).c_str(), "%d-%u-%u", &year, &month,
                  &day) != 3) {
    throw std::runtime_error("invalid DATE value");
  }
  const std::chrono::year_month_day ymd{
      std::chrono::year{year}, std::chrono::month{month},
      std::chrono::day{day}};
  if (!ymd.ok()) throw std::runtime_error("invalid DATE value");
  return std::chrono::sys_days{ymd}.time_since_epoch().count();
}

std::string FormatDateDays(int64_t days) {
  const std::chrono::year_month_day ymd{
      std::chrono::sys_days{std::chrono::days{days}}};
  char buffer[16];
  std::snprintf(buffer, sizeof(buffer), "%04d-%02u-%02u", int(ymd.year()),
                unsigned(ymd.month()), unsigned(ymd.day()));
  return buffer;
}

int64_t AddDateIntervalDays(int64_t days, int64_t amount,
                            std::string_view unit) {
  using namespace std::chrono;
  sys_days value{std::chrono::days{days}};
  if (unit == "day" || unit == "days") {
    value += std::chrono::days{amount};
  } else if (unit == "month" || unit == "months") {
    year_month_day ymd{value};
    ymd += months{static_cast<int>(amount)};
    if (!ymd.ok()) {
      ymd = year_month_day{year_month_day_last{
          ymd.year(), month_day_last{ymd.month()}}};
    }
    value = sys_days{ymd};
  } else if (unit == "year" || unit == "years") {
    year_month_day ymd{value};
    ymd += years{static_cast<int>(amount)};
    if (!ymd.ok()) {
      ymd = year_month_day{year_month_day_last{
          ymd.year(), month_day_last{ymd.month()}}};
    }
    value = sys_days{ymd};
  } else {
    throw std::runtime_error("unsupported interval unit " + std::string(unit));
  }
  return value.time_since_epoch().count();
}

}  // namespace tinylamb
