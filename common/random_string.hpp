/**
 * Copyright 2023 KUMAZAKI Hiroki
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

#ifndef TINYLAMB_RANDOM_STRING_HPP
#define TINYLAMB_RANDOM_STRING_HPP

#include <cstddef>
#include <random>
#include <string>
#include <string_view>

namespace tinylamb {

// mt19937 is not thread safe, so every thread gets its own pair of engines.
// `device_random` seeds from random_device; `seeded_random` uses a fixed seed
// (see https://xkcd.com/221/) so tests and fuzzers can reproduce sequences.
// Function-local statics keep initialization lazy and thread-safe (a
// throwing constructor at static-init time would otherwise be uncatchable).
inline std::random_device& SeedGen() {  // Kept for existing callers seeding
                                        // their own engines (e.g. row_page
                                        // tests).
  // NOLINTNEXTLINE(cert-err58-cpp)
  static std::random_device seed_gen;
  return seed_gen;
}

inline std::mt19937& DeviceRandom() {
  // NOLINTNEXTLINE(cert-err58-cpp)
  static thread_local std::mt19937 device_random((std::random_device())());
  return device_random;
}

inline std::mt19937& SeededRandom() {
  // Deterministic by design (fixed seed); static storage init cannot throw
  // for mt19937.
  // NOLINTNEXTLINE(cert-err58-cpp,cert-msc32-c,cert-msc51-cpp)
  static thread_local std::mt19937 seeded_random(4);
  return seeded_random;
}

inline void RandomStringInitialize() { SeededRandom() = std::mt19937(4); }

inline std::string RandomString(size_t len = 16, bool use_random = true) {
  static constexpr std::string_view alphanum =
      "0123456789"
      "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
      "abcdefghijklmnopqrstuvwxyz";
  // uniform_int_distribution avoids the modulo bias of `% 62`.
  static thread_local std::uniform_int_distribution<size_t> dist(
      0, alphanum.size() - 1);
  std::string ret;
  ret.reserve(len);
  if (use_random) {
    for (size_t i = 0; i < len; ++i) {
      ret.push_back(alphanum[dist(DeviceRandom())]);
    }
  } else {
    for (size_t i = 0; i < len; ++i) {
      ret.push_back(alphanum[dist(SeededRandom())]);
    }
  }
  return ret;
}

}  // namespace tinylamb

#endif  // TINYLAMB_RANDOM_STRING_HPP
