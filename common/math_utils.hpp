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

}  // namespace tinylamb

#endif  // TINYLAMB_MATH_UTILS_HPP
