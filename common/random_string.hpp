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

namespace tinylamb {

inline std::random_device seed_gen;
inline std::mt19937 device_random(seed_gen());
inline std::mt19937 seeded_random(4);  // see https://xkcd.com/221/

inline void RandomStringInitialize() { seeded_random = std::mt19937(4); }

inline std::string RandomString(size_t len = 16, bool use_seed = true) {
  static const char alphanum[] =
      "0123456789"
      "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
      "abcdefghijklmnopqrstuvwxyz";
  std::string ret;
  ret.reserve(len);
  if (use_seed) {
    for (size_t i = 0; i < len; ++i) {
      ret.push_back(alphanum[device_random() % (sizeof(alphanum) - 1)]);
    }
  } else {
    for (size_t i = 0; i < len; ++i) {
      ret.push_back(alphanum[seeded_random() % (sizeof(alphanum) - 1)]);
    }
  }
  return ret;
}

};  // namespace tinylamb

#endif  // TINYLAMB_RANDOM_STRING_HPP
