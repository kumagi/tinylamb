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

#include "common/platform/time_zone.hpp"

#include <chrono>
#include <cstdint>
#include <string>

#ifdef __APPLE__
#include <CoreFoundation/CoreFoundation.h>
#include <dirent.h>

#include <memory>
#include <mutex>
#include <unordered_map>
#endif

namespace tinylamb::platform {
namespace {

// Seconds since the epoch of a local wall-clock time read as if it were UTC.
int64_t LocalSeconds(int year, unsigned month, unsigned day, int hour,
                     int minute, int second) {
  const std::chrono::sys_days days{std::chrono::year_month_day{
      std::chrono::year{year}, std::chrono::month{month},
      std::chrono::day{day}}};
  return (static_cast<int64_t>(days.time_since_epoch().count()) * 86400) +
         (static_cast<int64_t>(hour) * 3600) +
         (static_cast<int64_t>(minute) * 60) + second;
}

#ifdef __APPLE__

struct CfRelease {
  void operator()(CFTypeRef ref) const {
    if (ref != nullptr) CFRelease(ref);
  }
};

// Resolved zones live for the whole process: CFTimeZone objects are
// immutable and thread-safe, and the set of distinct valid names is bounded
// by the tz database. Unknown names are not cached.
// APFS is case-insensitive by default, so CoreFoundation happily resolves
// "asia/tokyo"; CoreFoundation also canonicalizes some names ("UTC" becomes
// "GMT"). The tz database, like libstdc++'s locate_zone, is exact and
// case-sensitive, so validate the identifier against the zoneinfo tree
// component by component with exact string comparison.
bool ZoneFileExistsExactCase(std::string_view name) {
  if (name.empty() || name.front() == '/' || name.back() == '/') return false;
  std::string dir = "/usr/share/zoneinfo";
  size_t begin = 0;
  while (begin <= name.size()) {
    const size_t slash = name.find('/', begin);
    const std::string_view part = name.substr(
        begin, slash == std::string_view::npos ? std::string_view::npos
                                               : slash - begin);
    if (part.empty() || part == "." || part == "..") return false;
    DIR* handle = ::opendir(dir.c_str());
    if (handle == nullptr) return false;
    bool found = false;
    while (const dirent* entry = ::readdir(handle)) {
      if (part == entry->d_name) {
        found = true;
        break;
      }
    }
    ::closedir(handle);
    if (!found) return false;
    dir += '/';
    dir += part;
    if (slash == std::string_view::npos) break;
    begin = slash + 1;
  }
  return true;
}

CFTimeZoneRef FindZone(std::string_view name) {
  static std::mutex mu;
  static std::unordered_map<std::string, CFTimeZoneRef> cache;
  std::string key(name);
  {
    std::scoped_lock lock(mu);
    const auto found = cache.find(key);
    if (found != cache.end()) return found->second;
  }
  if (!ZoneFileExistsExactCase(key)) return nullptr;
  const std::unique_ptr<const void, CfRelease> cf_name(CFStringCreateWithBytes(
      kCFAllocatorDefault, reinterpret_cast<const UInt8*>(key.data()),
      static_cast<CFIndex>(key.size()), kCFStringEncodingUTF8, false));
  if (cf_name == nullptr) return nullptr;
  CFTimeZoneRef zone = CFTimeZoneCreateWithName(
      kCFAllocatorDefault, static_cast<CFStringRef>(cf_name.get()),
      /*tryAbbrev=*/false);
  if (zone == nullptr) return nullptr;
  std::scoped_lock lock(mu);
  const auto [it, inserted] = cache.emplace(std::move(key), zone);
  if (!inserted) CFRelease(zone);
  return it->second;
}

int OffsetAtUtc(CFTimeZoneRef zone, int64_t unix_seconds) {
  const CFAbsoluteTime at = static_cast<CFAbsoluteTime>(unix_seconds) -
                            kCFAbsoluteTimeIntervalSince1970;
  return static_cast<int>(CFTimeZoneGetSecondsFromGMT(zone, at));
}

#endif

}  // namespace

#ifdef __APPLE__

bool IsKnownTimeZone(std::string_view name) {
  return !name.empty() && FindZone(name) != nullptr;
}

std::optional<int> TimeZoneOffsetAtLocalTime(std::string_view name, int year,
                                             unsigned month, unsigned day,
                                             int hour, int minute, int second) {
  if (name.empty()) return std::nullopt;
  CFTimeZoneRef zone = FindZone(name);
  if (zone == nullptr) return std::nullopt;
  const int64_t local = LocalSeconds(year, month, day, hour, minute, second);
  // UTC offsets lie within +-1 day, so the instant `local` denotes falls in
  // (local - 1d, local + 1d); the offsets at both ends bracket any single
  // transition affecting it.
  const int before = OffsetAtUtc(zone, local - 86400);
  const int after = OffsetAtUtc(zone, local + 86400);
  if (before == after) return before;
  if (OffsetAtUtc(zone, local - before) == before) {
    return before;  // unique in the earlier period, or ambiguous: earlier
  }
  if (OffsetAtUtc(zone, local - after) == after) {
    return after;  // unique in the later period
  }
  return before;  // nonexistent (gap): offset in force before the gap
}

#else

bool IsKnownTimeZone(std::string_view name) {
  if (name.empty()) {
    return false;
  }
  try {
    return std::chrono::locate_zone(name) != nullptr;
  } catch (...) {  // NOLINT(bugprone-empty-catch) runtime_error: unknown zone
    return false;
  }
}

std::optional<int> TimeZoneOffsetAtLocalTime(std::string_view name, int year,
                                             unsigned month, unsigned day,
                                             int hour, int minute, int second) {
  if (name.empty()) {
    return std::nullopt;
  }
  try {
    const std::chrono::time_zone* zone = std::chrono::locate_zone(name);
    if (zone == nullptr) {
      return std::nullopt;
    }
    const std::chrono::local_seconds local{std::chrono::seconds{
        LocalSeconds(year, month, day, hour, minute, second)}};
    return static_cast<int>(zone->get_info(local).first.offset.count());
  } catch (...) {  // NOLINT(bugprone-empty-catch) runtime_error: unknown zone
    return std::nullopt;
  }
}

#endif

}  // namespace tinylamb::platform
