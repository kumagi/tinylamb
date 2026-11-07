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

#ifndef TINYLAMB_COMMON_PLATFORM_TIME_ZONE_HPP
#define TINYLAMB_COMMON_PLATFORM_TIME_ZONE_HPP

#include <optional>
#include <string_view>

namespace tinylamb::platform {

// Access to the system IANA time-zone database. Linux uses the C++20 tzdb
// (std::chrono::locate_zone); Apple's libc++ does not ship it, so macOS
// queries CoreFoundation (CFTimeZone), which reads the same tzdata.

// Whether `name` is a known IANA zone identifier (exact, case-sensitive).
bool IsKnownTimeZone(std::string_view name);

// UTC offset in seconds (east positive) of zone `name` at the given local
// wall-clock time, i.e. `std::chrono::time_zone::get_info(local).first`:
// an ambiguous time (DST fall-back overlap) yields the earlier offset and a
// nonexistent time (spring-forward gap) yields the offset in force before the
// gap. nullopt when the zone is unknown. Never throws.
std::optional<int> TimeZoneOffsetAtLocalTime(std::string_view name, int year,
                                             unsigned month, unsigned day,
                                             int hour, int minute, int second);

}  // namespace tinylamb::platform

#endif  // TINYLAMB_COMMON_PLATFORM_TIME_ZONE_HPP
