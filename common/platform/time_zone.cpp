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
#include <mutex>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <unordered_set>

#ifdef __APPLE__
#include <CoreFoundation/CoreFoundation.h>
#include <dirent.h>

#include <memory>
#include <mutex>
#include <unordered_map>
#include <unordered_set>
#endif

namespace tinylamb::platform {
namespace {

// Seconds since the epoch of a local wall-clock time read as if it were UTC.
// T2094: invalid civil fields (month 0/13, day 0/32, Feb 30, years outside
// [-32767, 32767]) must not reach year_month_day -> sys_days, whose
// conversion result is unspecified there (silently normalizing to ANOTHER
// date and returning that day's offset); std::chrono::year<int> itself is
// UB beyond that range.  Callers already speak optional, so report "no
// offset" instead.
std::optional<int64_t> LocalSeconds(int year, unsigned month, unsigned day,
                                    int hour, int minute, int second) {
  if (year < -32767 || year > 32767) {
    return std::nullopt;
  }
  const std::chrono::year_month_day ymd{std::chrono::year{year},
                                        std::chrono::month{month},
                                        std::chrono::day{day}};
  if (!ymd.ok()) {
    return std::nullopt;
  }
  if (hour < 0 || hour > 23 || minute < 0 || minute > 59 || second < 0 ||
      second > 60) {  // 60 = leap second spelling
    return std::nullopt;
  }
  const std::chrono::sys_days days{ymd};
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
// by the tz database. Unknown names are negative-cached to avoid repeated
// directory scans (Linux locate_zone does its own caching).
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
  // Unknown names are cached too: otherwise each typo re-scans the whole
  // zoneinfo tree (opendir + readdir + exact-case compare).
  static std::unordered_set<std::string> unknown;
  std::string key(name);
  {
    std::scoped_lock lock(mu);
    const auto found = cache.find(key);
    if (found != cache.end()) return found->second;
    if (unknown.contains(key)) return nullptr;
  }
  if (!ZoneFileExistsExactCase(key)) {
    std::scoped_lock lock(mu);
    unknown.insert(std::move(key));
    return nullptr;
  }
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
                                             int hour, int minute, int second,
                                             bool at_utc) {
  if (name.empty()) return std::nullopt;
  // T2095: the header contract is "Never throws." The Linux branch satisfies
  // it with try/catch(...); here the std::string key built in FindZone, the
  // accumulating dir string in ZoneFileExistsExactCase, and the zone-cache
  // emplace can still leak std::bad_alloc out of a supposedly noexcept
  // lookup. Translate any escaping exception into the same "no offset"
  // result as an unknown zone.
  try {
    CFTimeZoneRef zone = FindZone(name);
    if (zone == nullptr) return std::nullopt;
    const std::optional<int64_t> local =
        LocalSeconds(year, month, day, hour, minute, second);
    if (!local.has_value()) return std::nullopt;  // T2094: invalid civil date
    if (at_utc) return OffsetAtUtc(zone, *local);
    // UTC offsets lie within +-1 day, so the instant `local` denotes falls in
    // (local - 1d, local + 1d); the offsets at both ends bracket any single
    // transition affecting it.
    const int before = OffsetAtUtc(zone, *local - 86400);
    const int after = OffsetAtUtc(zone, *local + 86400);
    if (before == after) return before;
    if (OffsetAtUtc(zone, *local - before) == before) {
      return before;  // unique in the earlier period, or ambiguous: earlier
    }
    if (OffsetAtUtc(zone, *local - after) == after) {
      return after;  // unique in the later period
    }
    return before;  // nonexistent (gap): offset in force before the gap
  } catch (...) {
    return std::nullopt;  // T2095: bad_alloc etc. -> "no offset", never throw
  }
}

#else

namespace {
// T2093: mirror the macOS branch's name cache.  std::chrono::tzdb is a
// process-wide singleton, so resolved zone pointers stay valid; unknown
// names cache as nullptr so repeated lookups of a bad name cost a hash  // NOLINT(modernize-use-scoped-lock)
// probe instead of another locate_zone search.
//
// T2096: only std::runtime_error from locate_zone means "unknown zone".
// Anything else (bad_alloc, tzdb load failure) is a transient/infra error
// and now propagates instead of being laundered into kInvalidArgument by
// callers such as type/date.cpp.
const std::chrono::time_zone* FindZoneCached(std::string_view name) {
  static std::mutex mu;
  static std::unordered_map<std::string, const std::chrono::time_zone*> cache;
  const std::string key(name);
  std::lock_guard<std::mutex> lock(mu);  // NOLINT(modernize-use-scoped-lock)
  const auto it = cache.find(key);
  if (it != cache.end()) {
    return it->second;
  }
  const std::chrono::time_zone* zone = nullptr;
  try {
    zone = std::chrono::locate_zone(key);
  } catch (const std::runtime_error&) {
    zone = nullptr;  // unknown zone name
  }
  cache.emplace(key, zone);
  return zone;
}
}  // namespace

bool IsKnownTimeZone(std::string_view name) {
  if (name.empty()) {
    return false;
  }
  return FindZoneCached(name) != nullptr;
}

std::optional<int> TimeZoneOffsetAtLocalTime(std::string_view name, int year,
                                             unsigned month, unsigned day,
                                             int hour, int minute, int second,
                                             bool at_utc) {
  if (name.empty()) {
    return std::nullopt;
  }
  const std::chrono::time_zone* zone = FindZoneCached(name);
  if (zone == nullptr) {
    return std::nullopt;
  }
  try {
    const std::optional<int64_t> local_epoch =
        LocalSeconds(year, month, day, hour, minute, second);
    if (!local_epoch.has_value()) {
      return std::nullopt;  // T2094: invalid civil date, not a zone miss
    }
    const std::chrono::seconds seconds{*local_epoch};
    if (at_utc) {
      return static_cast<int>(
          zone->get_info(std::chrono::sys_seconds{seconds}).offset.count());
    }
    const std::chrono::local_seconds local{seconds};
    return static_cast<int>(zone->get_info(local).first.offset.count());
  } catch (const std::runtime_error&) {
    // get_info out_of_range on unrepresentable civil times: no offset.
    return std::nullopt;
  }
}

#endif

}  // namespace tinylamb::platform
