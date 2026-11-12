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

#ifndef TINYLAMB_MATH_UTILS_HPP
#define TINYLAMB_MATH_UTILS_HPP

#include <cmath>
#include <cstdint>

namespace tinylamb {

// std::cbrt is not required to be correctly rounded, and some libm
// implementations (e.g. clang's on Linux) return 3.0000000000000004 for
// cbrt(27). One Newton-Raphson step recovers the correctly rounded result
// and makes the value identical across toolchains.
inline double AccurateCbrt(double v) {
  const double c = std::cbrt(v);
  if (v == 0.0 || !std::isfinite(c)) {
    return c;
  }
  return (2.0 * c + v / (c * c)) / 3.0;
}

// FLOAT64 -> INT64 without the [conv.fpint] UB of a bare static_cast on
// NaN, +-inf or |x| >= 2^63: NaN maps to 0, out-of-range values saturate.
inline int64_t SaturatingDoubleToInt64(double d) {
  if (std::isnan(d)) {
    return 0;
  }
  // 2^63 is exactly representable; every double below it truncates safely.
  constexpr double kTwo63 = 9223372036854775808.0;
  if (d >= kTwo63) {
    return INT64_MAX;
  }
  if (d < -kTwo63) {
    return INT64_MIN;
  }
  return static_cast<int64_t>(d);
}

// ROUND / TRUNC of a FLOAT64 to `digits` decimal places. Shared by the AST
// evaluator and the relational evaluator so both agree at the extremes:
// - a positive precision finer than the value can represent (10^digits
//   overflows, or val * 10^digits overflows) leaves the value unchanged;
// - a negative precision coarser than the value (10^-digits overflows)
//   rounds / truncates to zero.
// The naive `round(val * 10^d) / 10^d` produced inf/inf or 0/0 = NaN there.
inline double ScaleRoundDouble(double val, int64_t digits, bool truncate) {
  if (!std::isfinite(val)) {
    return val;
  }
  const auto apply = [truncate](double x) {
    return truncate ? std::trunc(x) : std::round(x);
  };
  if (digits >= 0) {
    // The smallest subnormal needs ~1074 fractional digits; finer is a no-op.
    if (digits > 1100) {
      return val;
    }
    const double factor = std::pow(10.0, static_cast<double>(digits));
    const double scaled = val * factor;
    if (!std::isfinite(factor) || !std::isfinite(scaled)) {
      return val;
    }
    const double result = apply(scaled) / factor;
    return std::isfinite(result) ? result : val;
  }
  if (digits < -400) {
    return std::copysign(0.0, val);
  }
  const double factor = std::pow(10.0, static_cast<double>(digits));
  if (factor == 0.0) {
    return std::copysign(0.0, val);
  }
  const double result = apply(val * factor) / factor;
  return std::isfinite(result) ? result : val;
}

}  // namespace tinylamb

#endif  // TINYLAMB_MATH_UTILS_HPP
