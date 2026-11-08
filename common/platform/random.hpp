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

#ifndef TINYLAMB_COMMON_PLATFORM_RANDOM_HPP
#define TINYLAMB_COMMON_PLATFORM_RANDOM_HPP

#include <cstdint>
#include <iterator>
#include <type_traits>
#include <utility>

namespace tinylamb::platform {

// Reproducible random draws. The engines (std::mt19937, std::mt19937_64) are
// fully specified by the standard, but std::uniform_int_distribution and
// std::shuffle are not: libstdc++ and libc++ turn the same engine output into
// different values. Seeded fuzzers, pinned regression traces and workload
// generators must produce the same cases on every platform, so they draw
// through these functions, which reproduce libstdc++ (GCC >= 11) bit for bit;
// Linux sequences are therefore unchanged.

namespace random_detail {

// Lemire's nearly-divisionless reduction, as libstdc++'s _S_nd: `Wide` is
// twice as wide as `Narrow`. Returns a value in [0, range).
template <typename Wide, typename Narrow, typename Engine>
Narrow NearlyDivisionless(Engine& engine, Narrow range) {
  constexpr int kDigits = static_cast<int>(sizeof(Narrow) * 8);
  Wide product = Wide(Narrow(engine())) * Wide(range);
  auto low = Narrow(product);
  if (low < range) {
    const Narrow threshold = Narrow(-range) % range;
    while (low < threshold) {
      product = Wide(Narrow(engine())) * Wide(range);
      low = Narrow(product);
    }
  }
  return Narrow(product >> kDigits);
}

// libstdc++ uniform_int_distribution<...>::operator() over the unsigned
// offset range [0, urange].
template <typename Engine>
uint64_t UniformOffset(Engine& engine, uint64_t urange) {
  constexpr uint64_t kMin = static_cast<uint64_t>(Engine::min());
  constexpr uint64_t kMax = static_cast<uint64_t>(Engine::max());
  static_assert(kMin < kMax);
  constexpr uint64_t kEngineRange = kMax - kMin;
  if (kEngineRange > urange) {
    const uint64_t uerange = urange + 1;
#if defined(__SIZEOF_INT128__)
    if constexpr (kEngineRange == UINT64_MAX) {
      __extension__ using uint128_t = unsigned __int128;
      return NearlyDivisionless<uint128_t, uint64_t>(engine, uerange);
    } else
#endif
        if constexpr (kEngineRange == UINT32_MAX) {
      return NearlyDivisionless<uint64_t, uint32_t>(
          engine, static_cast<uint32_t>(uerange));
    } else {
      const uint64_t scaling = kEngineRange / uerange;
      const uint64_t past = uerange * scaling;
      uint64_t ret = 0;
      do {
        ret = static_cast<uint64_t>(engine()) - kMin;
      } while (ret >= past);
      return ret / scaling;
    }
  }
  if constexpr (kEngineRange != UINT64_MAX) {  // else unreachable
    if (kEngineRange < urange) {
      // Upscaling: combine several engine draws.
      uint64_t ret = 0;
      uint64_t tmp = 0;
      do {
        constexpr uint64_t kEngineSpan = kEngineRange + 1;
        tmp = kEngineSpan * UniformOffset(engine, urange / kEngineSpan);
        ret = tmp + (static_cast<uint64_t>(engine()) - kMin);
      } while (ret > urange || ret < tmp);
      return ret;
    }
  }
  return static_cast<uint64_t>(engine()) - kMin;
}

}  // namespace random_detail

// Equivalent to std::uniform_int_distribution<Int>(lo, hi)(engine) under
// libstdc++. Requires lo <= hi.
template <typename Int, typename Engine>
Int UniformInt(Engine& engine, Int lo, Int hi) {
  static_assert(std::is_integral_v<Int> && sizeof(Int) <= sizeof(uint64_t));
  using Unsigned = std::make_unsigned_t<Int>;
  const uint64_t urange = static_cast<uint64_t>(static_cast<Unsigned>(
      static_cast<Unsigned>(hi) - static_cast<Unsigned>(lo)));
  const uint64_t offset = random_detail::UniformOffset(engine, urange);
  return static_cast<Int>(static_cast<Unsigned>(static_cast<Unsigned>(offset) +
                                                static_cast<Unsigned>(lo)));
}

// Equivalent to std::shuffle(first, last, engine) under libstdc++, including
// its "two positions per draw" optimization.
template <typename RandomIt, typename Engine>
void Shuffle(RandomIt first, RandomIt last, Engine& engine) {
  if (first == last) return;
  using std::iter_swap;
  constexpr uint64_t kEngineRange = static_cast<uint64_t>(Engine::max()) -
                                    static_cast<uint64_t>(Engine::min());
  const auto urange = static_cast<uint64_t>(last - first);
  if (kEngineRange / urange >= urange) {
    RandomIt it = first + 1;
    if ((urange % 2) == 0) {
      iter_swap(it++, first + static_cast<std::ptrdiff_t>(
                                  random_detail::UniformOffset(engine, 1)));
    }
    while (it != last) {
      const auto swap_range = static_cast<uint64_t>(it - first) + 1;
      // libstdc++ __gen_two_uniform_ints(swap_range, swap_range + 1).
      const uint64_t x = random_detail::UniformOffset(
          engine, (swap_range * (swap_range + 1)) - 1);
      const uint64_t first_pos = x / (swap_range + 1);
      const uint64_t second_pos = x % (swap_range + 1);
      iter_swap(it++, first + static_cast<std::ptrdiff_t>(first_pos));
      iter_swap(it++, first + static_cast<std::ptrdiff_t>(second_pos));
    }
    return;
  }
  for (RandomIt it = first + 1; it != last; ++it) {
    iter_swap(it,
              first + static_cast<std::ptrdiff_t>(random_detail::UniformOffset(
                          engine, static_cast<uint64_t>(it - first))));
  }
}

}  // namespace tinylamb::platform

#endif  // TINYLAMB_COMMON_PLATFORM_RANDOM_HPP
