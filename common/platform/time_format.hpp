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

#ifndef TINYLAMB_COMMON_PLATFORM_TIME_FORMAT_HPP
#define TINYLAMB_COMMON_PLATFORM_TIME_FORMAT_HPP

#include <cstddef>
#include <cstdint>
#include <ctime>

namespace tinylamb::platform {

// timegm(3)/gmtime_r(3) over the full proleptic Gregorian range, computed
// arithmetically. macOS timegm returns -1 for any year before 1900 (and -1 is
// also a valid result), glibc handles them; neither consults the host zone.
//
// TimeGm normalizes out-of-range fields and rewrites `tm` exactly like glibc
// timegm (tm_wday, tm_yday, tm_isdst = 0, tm_gmtoff = 0, tm_zone = "GMT") and
// returns seconds since the epoch.
int64_t TimeGm(struct tm* tm);
// Fills `out` with the UTC calendar time of `seconds` since the epoch.
void GmTime(int64_t seconds, struct tm* out);
// Fills `out` with the local calendar time of `seconds` since the epoch
// (localtime_r on POSIX, localtime_s on Windows).
void LocalTime(std::time_t seconds, struct tm* out);

// strptime(3)/strftime(3) with glibc's time-zone semantics on every platform.
// The engine works on UTC-normalized `struct tm` values, so the process's
// local zone must never leak into results. BSD/Apple libc differ from glibc
// exactly there:
//   * strptime "%z" on macOS converts the parsed fields to *local* time and
//     "%Z" accepts only local/GMT names; glibc consumes both fields, records
//     a "%z" offset only in tm_gmtoff, and leaves the other fields alone.
//   * strftime "%z"/"%Z" on macOS print the process's local zone; glibc uses
//     the struct's tm_gmtoff/tm_zone.
// On glibc these are plain pass-throughs.

// Parses `input` per `format` into `tm`. Returns a pointer past the consumed
// input, or nullptr on mismatch (like strptime).
const char* ParseTime(const char* input, const char* format, struct tm* tm);

// Formats `tm` per `format` into `out` (like strftime: returns the number of
// bytes written excluding the NUL, or 0 when it does not fit).
size_t FormatTime(char* out, size_t size, const char* format,
                  const struct tm* tm) noexcept;

}  // namespace tinylamb::platform

#endif  // TINYLAMB_COMMON_PLATFORM_TIME_FORMAT_HPP
