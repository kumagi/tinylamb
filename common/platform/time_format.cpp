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
#include <climits>
#include <ctime>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>

#include "common/platform/os.hpp"

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

#ifndef _WIN32
std::array<char, 4> kGmtZoneName = {'G', 'M', 'T', '\0'};
#else
struct WinTmZoneShadow {
  const struct tm* ptr = nullptr;
  long gmtoff = 0;
  const char* zone = "GMT";
};
thread_local WinTmZoneShadow tls_win_tm_shadow;
#endif

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
#ifndef _WIN32
  out->tm_gmtoff = 0;
  out->tm_zone = kGmtZoneName.data();
#else
  tls_win_tm_shadow = {.ptr = out, .gmtoff = 0, .zone = "GMT"};
#endif
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

void LocalTime(std::time_t seconds, struct tm* out) {
  *out = {};
#ifdef _WIN32
  (void)::localtime_s(out, &seconds);
#else
  (void)::localtime_r(&seconds, out);
#endif
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

void SetTmGmtoff(struct tm* tm, long offset) {
#if !defined(_WIN32)
  tm->tm_gmtoff = offset;
#else
  tls_win_tm_shadow = {.ptr = tm, .gmtoff = offset, .zone = "GMT"};
#endif
}

long GetTmGmtoff(const struct tm* tm) {
#if !defined(_WIN32)
  return tm->tm_gmtoff;
#else
  return tls_win_tm_shadow.ptr == tm ? tls_win_tm_shadow.gmtoff : 0L;
#endif
}

const char* GetTmZone(const struct tm* tm) {
#if !defined(_WIN32)
  return tm->tm_zone;
#else
  return tls_win_tm_shadow.ptr == tm ? tls_win_tm_shadow.zone : "GMT";
#endif
}

// glibc's "%z": optional leading blanks, then "Z", or a sign followed by
// "hh", "hhmm" or "hh:mm" (minutes < 60). Stores the offset in tm_gmtoff.
const char* ParseOffset(const char* rp, struct tm* tm) {
  while (IsSpace(*rp)) ++rp;
  if (*rp == 'Z') {
    SetTmGmtoff(tm, 0);
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
  SetTmGmtoff(tm, negative ? -offset : offset);
  return rp;
}

// glibc's "%Z": skip blanks, then consume the name without interpreting it.
const char* SkipZoneName(const char* rp) {
  while (IsSpace(*rp)) ++rp;
  while (*rp != '\0' && !IsSpace(*rp)) ++rp;
  return rp;
}

// Every non-glibc platform (Windows, macOS/BSD) parses through this
// glibc-compatible implementation: BSD strptime applies %p in place and
// lacks glibc extensions such as %s/%l, so the same PARSE_TIMESTAMP gave
// different answers than on Linux.
const char* ParseIntField(const char* rp, int max_digits, int min_val,
                          int max_val, int* out,
                          bool allow_leading_space = true) {
  if (allow_leading_space) {
    while (*rp == ' ' || *rp == '\t') ++rp;
  }
  bool neg = false;
  if (min_val < 0 && (*rp == '+' || *rp == '-')) {
    neg = (*rp == '-');
    ++rp;
  }
  if (!IsDigit(*rp)) return nullptr;
  int val = 0;
  int count = 0;
  while (count < max_digits && IsDigit(*rp)) {
    val = (val * 10) + (*rp - '0');
    ++rp;
    ++count;
  }
  if (neg) val = -val;
  if (val < min_val || val > max_val) return nullptr;
  *out = val;
  return rp;
}

bool StartsWithIgnoreCase(const char* s, const char* prefix, size_t* len) {
  size_t i = 0;
  while (prefix[i] != '\0') {
    if (std::tolower(static_cast<unsigned char>(s[i])) !=
        std::tolower(static_cast<unsigned char>(prefix[i]))) {
      return false;
    }
    ++i;
  }
  *len = i;
  return true;
}

const char* PortableStrptime(const char* rp, const char* fmt, struct tm* tm) {
  static constexpr std::array<const char*, 12> kMonthsFull = {
      "January", "February", "March",     "April",   "May",      "June",
      "July",    "August",   "September", "October", "November", "December"};
  static constexpr std::array<const char*, 12> kMonthsAbbr = {
      "Jan", "Feb", "Mar", "Apr", "May", "Jun",
      "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};
  static constexpr std::array<const char*, 7> kDaysFull = {
      "Sunday",   "Monday", "Tuesday", "Wednesday",
      "Thursday", "Friday", "Saturday"};
  static constexpr std::array<const char*, 7> kDaysAbbr = {
      "Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"};

  // glibc semantics: %p is a FLAG applied after the whole format parsed,
  // and only to a 12-hour (%I / %l) hour; applying it in place made
  // "%p%I" disagree with Linux ("PM 3" -> 03:00 instead of 15:00).
  bool have_twelve_hour = false;
  int meridiem = -1;  // -1 none, 0 AM, 1 PM
  for (const char* fp = fmt; *fp != '\0'; ++fp) {
    if (IsSpace(*fp)) {
      while (IsSpace(*rp)) ++rp;
      continue;
    }
    if (*fp != '%') {
      if (*rp != *fp) return nullptr;
      ++rp;
      continue;
    }
    ++fp;
    if (*fp == 'E' || *fp == 'O') ++fp;
    switch (*fp) {
      case '%':
        if (*rp != '%') return nullptr;
        ++rp;
        break;
      case 'n':
      case 't':
        while (IsSpace(*rp)) ++rp;
        break;
      case 'Y': {
        int y = 0;
        rp = ParseIntField(rp, 4, -9999, 9999, &y);
        if (rp == nullptr) return nullptr;
        tm->tm_year = y - 1900;
        break;
      }
      case 'y': {
        int y = 0;
        rp = ParseIntField(rp, 2, 0, 99, &y);
        if (rp == nullptr) return nullptr;
        tm->tm_year = (y >= 69 ? y : y + 100);
        break;
      }
      case 'C': {
        int c = 0;
        rp = ParseIntField(rp, 2, 0, 99, &c);
        if (rp == nullptr) return nullptr;
        tm->tm_year = (c * 100) - 1900 + (tm->tm_year % 100);
        break;
      }
      case 'm': {
        int m = 0;
        rp = ParseIntField(rp, 2, 1, 12, &m);
        if (rp == nullptr) return nullptr;
        tm->tm_mon = m - 1;
        break;
      }
      case 'B':
      case 'b':
      case 'h': {
        while (IsSpace(*rp)) ++rp;
        bool matched = false;
        for (int i = 0; i < 12; ++i) {
          size_t len = 0;
          if (StartsWithIgnoreCase(rp, kMonthsFull[i], &len) ||
              StartsWithIgnoreCase(rp, kMonthsAbbr[i], &len)) {
            tm->tm_mon = i;
            rp += len;
            matched = true;
            break;
          }
        }
        if (!matched) return nullptr;
        break;
      }
      case 'd':
      case 'e': {
        int d = 0;
        rp = ParseIntField(rp, 2, 1, 31, &d);
        if (rp == nullptr) return nullptr;
        tm->tm_mday = d;
        break;
      }
      case 'j': {
        int j = 0;
        rp = ParseIntField(rp, 3, 1, 366, &j);
        if (rp == nullptr) return nullptr;
        tm->tm_yday = j - 1;
        break;
      }
      case 'H': {
        int h = 0;
        rp = ParseIntField(rp, 2, 0, 23, &h);
        if (rp == nullptr) return nullptr;
        tm->tm_hour = h;
        break;
      }
      case 'k': {  // glibc: space-padded 24-hour clock
        int h = 0;
        rp = ParseIntField(rp, 2, 0, 23, &h);
        if (rp == nullptr) return nullptr;
        tm->tm_hour = h;
        break;
      }
      case 'I':
      case 'l': {  // %l: glibc space-padded 12-hour clock
        int h = 0;
        rp = ParseIntField(rp, 2, 1, 12, &h);
        if (rp == nullptr) return nullptr;
        tm->tm_hour = (h == 12) ? 0 : h;
        have_twelve_hour = true;
        break;
      }
      case 'p': {
        while (IsSpace(*rp)) ++rp;
        size_t len = 0;
        if (StartsWithIgnoreCase(rp, "AM", &len)) {
          meridiem = 0;
          rp += len;
        } else if (StartsWithIgnoreCase(rp, "PM", &len)) {
          meridiem = 1;
          rp += len;
        } else {
          return nullptr;
        }
        break;
      }
      case 's': {  // seconds since the Epoch (glibc: local time)
        while (IsSpace(*rp)) ++rp;
        bool neg = false;
        if (*rp == '-' || *rp == '+') {
          neg = (*rp == '-');
          ++rp;
        }
        if (!IsDigit(*rp)) return nullptr;
        long long secs = 0;
        while (IsDigit(*rp)) {
          if (secs > (LLONG_MAX - 9) / 10) return nullptr;
          secs = (secs * 10) + (*rp - '0');
          ++rp;
        }
        const auto t = static_cast<std::time_t>(neg ? -secs : secs);
        LocalTime(t, tm);
        break;
      }
      case 'U':
      case 'W':
      case 'V': {  // week numbers: parsed and ignored, as glibc does
        int w = 0;
        rp = ParseIntField(rp, 2, 0, 53, &w);
        if (rp == nullptr) return nullptr;
        break;
      }
      case 'G': {  // ISO week-based year: parsed and ignored
        int y = 0;
        rp = ParseIntField(rp, 4, 0, 9999, &y);
        if (rp == nullptr) return nullptr;
        break;
      }
      case 'g': {
        int y = 0;
        rp = ParseIntField(rp, 2, 0, 99, &y);
        if (rp == nullptr) return nullptr;
        break;
      }
      case 'r':  // C locale: %I:%M:%S %p
        rp = PortableStrptime(rp, "%I:%M:%S %p", tm);
        if (rp == nullptr) return nullptr;
        break;
      case 'c':  // C locale
        rp = PortableStrptime(rp, "%a %b %e %H:%M:%S %Y", tm);
        if (rp == nullptr) return nullptr;
        break;
      case 'x':
        rp = PortableStrptime(rp, "%m/%d/%y", tm);
        if (rp == nullptr) return nullptr;
        break;
      case 'X':
        rp = PortableStrptime(rp, "%H:%M:%S", tm);
        if (rp == nullptr) return nullptr;
        break;
      case 'M': {
        int m = 0;
        rp = ParseIntField(rp, 2, 0, 59, &m);
        if (rp == nullptr) return nullptr;
        tm->tm_min = m;
        break;
      }
      case 'S': {
        int s = 0;
        rp = ParseIntField(rp, 2, 0, 60, &s);
        if (rp == nullptr) return nullptr;
        tm->tm_sec = s;
        break;
      }
      case 'a':
      case 'A': {
        while (IsSpace(*rp)) ++rp;
        bool matched = false;
        for (int i = 0; i < 7; ++i) {
          size_t len = 0;
          if (StartsWithIgnoreCase(rp, kDaysFull[i], &len) ||
              StartsWithIgnoreCase(rp, kDaysAbbr[i], &len)) {
            tm->tm_wday = i;
            rp += len;
            matched = true;
            break;
          }
        }
        if (!matched) return nullptr;
        break;
      }
      case 'w': {
        int w = 0;
        rp = ParseIntField(rp, 1, 0, 6, &w);
        if (rp == nullptr) return nullptr;
        tm->tm_wday = w;
        break;
      }
      case 'u': {
        int u = 0;
        rp = ParseIntField(rp, 1, 1, 7, &u);
        if (rp == nullptr) return nullptr;
        tm->tm_wday = u % 7;
        break;
      }
      case 'F':
        rp = PortableStrptime(rp, "%Y-%m-%d", tm);
        if (rp == nullptr) return nullptr;
        break;
      case 'T':
        rp = PortableStrptime(rp, "%H:%M:%S", tm);
        if (rp == nullptr) return nullptr;
        break;
      case 'R':
        rp = PortableStrptime(rp, "%H:%M", tm);
        if (rp == nullptr) return nullptr;
        break;
      case 'D':
        rp = PortableStrptime(rp, "%m/%d/%y", tm);
        if (rp == nullptr) return nullptr;
        break;
      default:
        return nullptr;
    }
  }
  if (have_twelve_hour && meridiem == 1) {
    tm->tm_hour += 12;  // %I parsed 12 as 0, so 12 PM -> 12 and 3 PM -> 15
  }
  return rp;
}

}  // namespace

const char* ParseTime(const char* input, const char* format, struct tm* tm) {
  // Hand everything except "%z"/"%Z" to the system strptime, piecewise.
  std::string piece;
  const char* rp = input;
  auto flush = [&]() -> bool {
    if (piece.empty()) return true;
    rp = PortableStrptime(rp, piece.c_str(), tm);
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
      const long offset = GetTmGmtoff(tm);
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
      for (const char* zone = GetTmZone(tm); zone != nullptr && *zone != '\0';
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
