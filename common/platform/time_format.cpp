/**
 * Copyright 2026 KUMAZAKI Hiroki
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "common/platform/time_format.hpp"

#include <array>
#include <cctype>
#include <cstdio>
#include <cstdlib>
#include <string>

namespace tinylamb::platform {

namespace {

int64_t FloorDiv(int64_t a, int64_t b) {
  return (a / b) - ((a % b != 0) && ((a < 0) != (b < 0)) ? 1 : 0);
}

// Howard Hinnant's days_from_civil / civil_from_days.
int64_t DaysFromCivil(int64_t y, int64_t m, int64_t d) {
  y -= m <= 2 ? 1 : 0;
  const int64_t era = FloorDiv(y, 400);
  const int64_t yoe = y - (era * 400);
  const int64_t doy = ((153 * (m + (m > 2 ? -3 : 9)) + 2) / 5) + d - 1;
  const int64_t doe = (yoe * 365) + (yoe / 4) - (yoe / 100) + doy;
  return (era * 146097) + doe - 719468;
}

void CivilFromDays(int64_t z, int64_t* y, int64_t* m, int64_t* d) {
  z += 719468;
  const int64_t era = FloorDiv(z, 146097);
  const int64_t doe = z - (era * 146097);
  const int64_t yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
  const int64_t doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
  const int64_t mp = (5 * doy + 2) / 153;
  *d = doy - ((153 * mp + 2) / 5) + 1;
  *m = mp < 10 ? mp + 3 : mp - 9;
  *y = yoe + (era * 400) + (*m <= 2 ? 1 : 0);
}

std::array<char, 4> kGmtZoneName = {'G', 'M', 'T', '\0'};

}  // namespace

void GmTime(int64_t seconds, struct tm* out) {
  const int64_t days = FloorDiv(seconds, 86400);
  const int64_t rem = seconds - (days * 86400);
  int64_t y = 0;
  int64_t m = 0;
  int64_t d = 0;
  CivilFromDays(days, &y, &m, &d);
  *out = {};
  out->tm_year = static_cast<int>(y - 1900);
  out->tm_mon = static_cast<int>(m - 1);
  out->tm_mday = static_cast<int>(d);
  out->tm_hour = static_cast<int>(rem / 3600);
  out->tm_min = static_cast<int>((rem % 3600) / 60);
  out->tm_sec = static_cast<int>(rem % 60);
  out->tm_wday = static_cast<int>(((days % 7) + 11) % 7);  // 1970-01-01: Thu
  out->tm_yday = static_cast<int>(days - DaysFromCivil(y, 1, 1));
  out->tm_isdst = 0;
  out->tm_gmtoff = 0;
  out->tm_zone = kGmtZoneName.data();
}

int64_t TimeGm(struct tm* tm) {
  const int64_t month_index = tm->tm_mon;
  const int64_t year = int64_t{tm->tm_year} + 1900 + FloorDiv(month_index, 12);
  const int64_t month = month_index - (FloorDiv(month_index, 12) * 12) + 1;
  const int64_t days = DaysFromCivil(year, month, 1) + tm->tm_mday - 1;
  const int64_t seconds = (days * 86400) + (int64_t{tm->tm_hour} * 3600) +
                          (int64_t{tm->tm_min} * 60) + tm->tm_sec;
  GmTime(seconds, tm);
  return seconds;
}

#ifdef __GLIBC__

const char* ParseTime(const char* input, const char* format, struct tm* tm) {
  return ::strptime(input, format, tm);
}

size_t FormatTime(char* out, size_t size, const char* format,
                  const struct tm* tm) noexcept {
#if defined(__GNUC__) || defined(__clang__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wformat-nonliteral"
#endif
  return std::strftime(out, size, format, tm);
#if defined(__GNUC__) || defined(__clang__)
#pragma GCC diagnostic pop
#endif
}

#else

namespace {

bool IsSpace(char c) {
  return std::isspace(static_cast<unsigned char>(c)) != 0;
}
bool IsDigit(char c) { return c >= '0' && c <= '9'; }

// glibc's "%z": optional leading blanks, then "Z", or a sign followed by
// "hh", "hhmm" or "hh:mm" (minutes < 60). Stores the offset in tm_gmtoff.
const char* ParseOffset(const char* rp, struct tm* tm) {
  while (IsSpace(*rp)) ++rp;
  if (*rp == 'Z') {
    tm->tm_gmtoff = 0;
    return rp + 1;
  }
  if (*rp != '+' && *rp != '-') return nullptr;
  const bool negative = *rp++ == '-';
  int value = 0;
  int digits = 0;
  while (digits < 4 && IsDigit(*rp)) {
    value = value * 10 + (*rp++ - '0');
    ++digits;
    if (*rp == ':' && digits == 2 && IsDigit(rp[1])) ++rp;
  }
  if (digits == 2) {
    value *= 100;
  } else if (digits != 4 || value % 100 >= 60) {
    return nullptr;
  }
  long offset = (value / 100) * 3600L + (value % 100) * 60L;
  tm->tm_gmtoff = negative ? -offset : offset;
  return rp;
}

// glibc's "%Z": skip blanks, then consume the name without interpreting it.
const char* SkipZoneName(const char* rp) {
  while (IsSpace(*rp)) ++rp;
  while (*rp != '\0' && !IsSpace(*rp)) ++rp;
  return rp;
}

}  // namespace

const char* ParseTime(const char* input, const char* format, struct tm* tm) {
  // Hand everything except "%z"/"%Z" to the system strptime, piecewise.
  std::string piece;
  const char* rp = input;
  auto flush = [&]() -> bool {
    if (piece.empty()) return true;
    rp = ::strptime(rp, piece.c_str(), tm);
    piece.clear();
    return rp != nullptr;
  };
  for (const char* fp = format; *fp != '\0'; ++fp) {
    if (*fp != '%' || fp[1] == '\0') {
      piece.push_back(*fp);
      continue;
    }
    if (fp[1] == '%') {
      piece.append("%%");
      ++fp;
      continue;
    }
    if (fp[1] == 'z' || fp[1] == 'Z') {
      if (!flush()) return nullptr;
      rp = fp[1] == 'z' ? ParseOffset(rp, tm) : SkipZoneName(rp);
      if (rp == nullptr) return nullptr;
      ++fp;
      continue;
    }
    piece.push_back(*fp);
  }
  if (!flush()) return nullptr;
  return rp;
}

size_t FormatTime(char* out, size_t size, const char* format,
                  const struct tm* tm) noexcept {
  // Expand "%z"/"%Ez"/"%Oz" and "%Z"/"%EZ"/"%OZ" from the struct itself,
  // then let strftime render the rest.
  std::string expanded;
  for (const char* fp = format; *fp != '\0'; ++fp) {
    if (*fp != '%' || fp[1] == '\0') {
      expanded.push_back(*fp);
      continue;
    }
    if (fp[1] == '%') {
      expanded.append("%%");
      ++fp;
      continue;
    }
    const char* spec = fp + 1;
    if ((*spec == 'E' || *spec == 'O') && (spec[1] == 'z' || spec[1] == 'Z')) {
      ++spec;
    }
    if (*spec == 'z') {
      const long offset = tm->tm_gmtoff;
      const long magnitude = offset < 0 ? -offset : offset;
      std::array<char, 16> text{};
      (void)std::snprintf(text.data(), text.size(), "%c%02ld%02ld",
                          offset < 0 ? '-' : '+', magnitude / 3600,
                          (magnitude / 60) % 60);
      expanded.append(text.data());
      fp = spec;
      continue;
    }
    if (*spec == 'Z') {
      for (const char* zone = tm->tm_zone; zone != nullptr && *zone != '\0';
           ++zone) {
        if (*zone == '%') expanded.push_back('%');
        expanded.push_back(*zone);
      }
      fp = spec;
      continue;
    }
    expanded.push_back(*fp);
  }
  if (expanded.empty()) {
    if (size > 0) out[0] = '\0';
    return 0;
  }
#if defined(__GNUC__) || defined(__clang__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wformat-nonliteral"
#endif
  return std::strftime(out, size, expanded.c_str(), tm);
#if defined(__GNUC__) || defined(__clang__)
#pragma GCC diagnostic pop
#endif
}

#endif

}  // namespace tinylamb::platform
