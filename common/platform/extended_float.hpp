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

#ifndef TINYLAMB_COMMON_PLATFORM_EXTENDED_FLOAT_HPP
#define TINYLAMB_COMMON_PLATFORM_EXTENDED_FLOAT_HPP

#include <algorithm>
#include <cfloat>
#include <cmath>
#include <cstdint>
#include <limits>
#include <type_traits>
#include <utility>

namespace tinylamb::platform {

// Accumulator type wider than double, for sums whose intermediates exceed
// double's exponent range (Σx² of 1e304-scale inputs) or need more than 53
// significand bits (exact int64 differences).
//
// The engine was written against x86-64's `long double` (x87 double-extended:
// 64-bit significand, 15-bit exponent). AArch64 Linux's binary128 is wider
// still. Apple's arm64 ABI makes `long double` a plain double, which turns
// e.g. STDDEV over ±2.2e304 into inf. There `ExtendedFloat` is SoftExtended,
// a software double-extended: every operation is rounded to a 64-bit
// significand, nearest-even, exactly like x87 at its default precision, so
// results match x86-64 Linux bit for bit.
class SoftExtended {
  __extension__ using uint128_t = unsigned __int128;

 public:
  constexpr SoftExtended() = default;
  // Implicit, like the arithmetic conversions to long double it replaces.
  template <typename T,
            typename = std::enable_if_t<std::is_arithmetic_v<T>>>
  SoftExtended(T value) {  // NOLINT(google-explicit-constructor)
    if constexpr (std::is_floating_point_v<T>) {
      FromDouble(static_cast<double>(value));
    } else if constexpr (std::is_signed_v<T>) {
      const auto v = static_cast<int64_t>(value);
      FromMagnitude(v < 0, v < 0 ? ~static_cast<uint64_t>(v) + 1
                                 : static_cast<uint64_t>(v));
    } else {
      FromMagnitude(false, static_cast<uint64_t>(value));
    }
  }

  explicit operator double() const { return ToDouble(); }
  explicit operator float() const { return static_cast<float>(ToDouble()); }
  // Truncates toward zero, exactly (an int64 survives a round trip).
  template <typename T, typename = std::enable_if_t<std::is_integral_v<T>>>
  explicit operator T() const {
    if (kind_ != Kind::kFinite || exponent_ <= -64) return T{0};
    if (exponent_ >= 0) return static_cast<T>(ToDouble());  // out of range
    const uint64_t magnitude = mantissa_ >> (-exponent_);
    return negative_ ? static_cast<T>(~magnitude + 1)
                     : static_cast<T>(magnitude);
  }

  friend SoftExtended operator+(SoftExtended a, SoftExtended b) {
    return Add(a, b, false);
  }
  friend SoftExtended operator-(SoftExtended a, SoftExtended b) {
    return Add(a, b, true);
  }
  friend SoftExtended operator*(SoftExtended a, SoftExtended b) {
    return Mul(a, b);
  }
  friend SoftExtended operator/(SoftExtended a, SoftExtended b) {
    return Div(a, b);
  }
  SoftExtended operator-() const {
    SoftExtended r = *this;
    r.negative_ = !r.negative_;
    return r;
  }
  SoftExtended operator+() const { return *this; }
  SoftExtended& operator+=(SoftExtended o) { return *this = *this + o; }
  SoftExtended& operator-=(SoftExtended o) { return *this = *this - o; }
  SoftExtended& operator*=(SoftExtended o) { return *this = *this * o; }
  SoftExtended& operator/=(SoftExtended o) { return *this = *this / o; }

  friend bool operator==(SoftExtended a, SoftExtended b) {
    return Compare(a, b) == 0;
  }
  friend bool operator!=(SoftExtended a, SoftExtended b) { return !(a == b); }
  friend bool operator<(SoftExtended a, SoftExtended b) {
    return Compare(a, b) == -1;
  }
  friend bool operator>(SoftExtended a, SoftExtended b) { return b < a; }
  friend bool operator<=(SoftExtended a, SoftExtended b) {
    const int c = Compare(a, b);
    return c == -1 || c == 0;
  }
  friend bool operator>=(SoftExtended a, SoftExtended b) { return b <= a; }

  friend SoftExtended Sqrt(SoftExtended x);
  [[nodiscard]] bool IsNaN() const { return kind_ == Kind::kNaN; }
  [[nodiscard]] bool IsInf() const { return kind_ == Kind::kInf; }

 private:
  enum class Kind : uint8_t { kZero, kFinite, kInf, kNaN };
  // x87 double-extended exponent limits for the value's leading bit.
  static constexpr int kMaxExponent = 16383;
  static constexpr int kMinExponent = -16445;  // smallest denormal

  static SoftExtended Special(Kind kind, bool negative) {
    SoftExtended r;
    r.kind_ = kind;
    r.negative_ = negative;
    return r;
  }

  void FromMagnitude(bool negative, uint64_t magnitude) {
    negative_ = negative;
    if (magnitude == 0) {
      kind_ = Kind::kZero;
      return;
    }
    kind_ = Kind::kFinite;
    const int lead = 63 - __builtin_clzll(magnitude);
    mantissa_ = magnitude << (63 - lead);
    exponent_ = lead - 63;
  }

  void FromDouble(double value) {
    negative_ = std::signbit(value);
    if (std::isnan(value)) {
      kind_ = Kind::kNaN;
      return;
    }
    if (std::isinf(value)) {
      kind_ = Kind::kInf;
      return;
    }
    if (value == 0) {
      kind_ = Kind::kZero;
      return;
    }
    int exp = 0;
    const double frac = std::frexp(std::fabs(value), &exp);  // [0.5, 1)
    // frac * 2^64 is an exact integer with <= 53 significant bits.
    const auto bits = static_cast<uint64_t>(std::ldexp(frac, 64));
    FromMagnitude(negative_, bits);
    exponent_ += exp - 64;
  }

  // Rounds `value` (an unsigned 128-bit significand scaled by 2^exponent,
  // with `sticky` marking nonzero bits already shifted out) to 64 bits.
  static SoftExtended Round(bool negative, uint128_t value, int exponent,
                            bool sticky) {
    if (value == 0) return Special(Kind::kZero, negative);
    const auto hi = static_cast<uint64_t>(value >> 64);
    const auto lo = static_cast<uint64_t>(value);
    const int lead =
        hi != 0 ? 127 - __builtin_clzll(hi) : 63 - __builtin_clzll(lo);
    int shift = lead - 63;  // bits to drop
    uint64_t mantissa = 0;
    if (shift <= 0) {
      mantissa = static_cast<uint64_t>(value) << (-shift);
    } else {
      const uint128_t dropped_mask = (static_cast<uint128_t>(1) << shift) - 1;
      const uint128_t dropped = value & dropped_mask;
      const uint128_t half = static_cast<uint128_t>(1) << (shift - 1);
      mantissa = static_cast<uint64_t>(value >> shift);
      const bool above_half = dropped > half || (dropped == half && sticky);
      const bool exactly_half = dropped == half && !sticky;
      if (above_half || (exactly_half && (mantissa & 1U) != 0)) {
        ++mantissa;
        if (mantissa == 0) {  // carried out of 64 bits
          mantissa = uint64_t{1} << 63;
          ++shift;
        }
      }
    }
    SoftExtended r;
    r.negative_ = negative;
    r.kind_ = Kind::kFinite;
    r.mantissa_ = mantissa;
    r.exponent_ = exponent + shift;
    const int64_t lead_exponent = int64_t{r.exponent_} + 63;
    if (lead_exponent > kMaxExponent) return Special(Kind::kInf, negative);
    if (lead_exponent < kMinExponent) return Special(Kind::kZero, negative);
    return r;
  }

  static SoftExtended Add(SoftExtended a, SoftExtended b, bool subtract) {
    if (subtract) b.negative_ = !b.negative_;
    if (a.kind_ == Kind::kNaN || b.kind_ == Kind::kNaN) {
      return Special(Kind::kNaN, false);
    }
    if (a.kind_ == Kind::kInf || b.kind_ == Kind::kInf) {
      if (a.kind_ == Kind::kInf && b.kind_ == Kind::kInf &&
          a.negative_ != b.negative_) {
        return Special(Kind::kNaN, true);
      }
      return a.kind_ == Kind::kInf ? a : b;
    }
    if (a.kind_ == Kind::kZero && b.kind_ == Kind::kZero) {
      return Special(Kind::kZero, a.negative_ && b.negative_);
    }
    if (a.kind_ == Kind::kZero) return b;
    if (b.kind_ == Kind::kZero) return a;
    if (a.exponent_ < b.exponent_) std::swap(a, b);
    // Keep 63 guard bits below the larger operand's significand.
    uint128_t big = static_cast<uint128_t>(a.mantissa_) << 63;
    uint128_t small = static_cast<uint128_t>(b.mantissa_) << 63;
    const int64_t distance = int64_t{a.exponent_} - b.exponent_;
    bool sticky = false;
    if (distance >= 127) {
      sticky = true;
      small = 0;
    } else if (distance > 0) {
      const uint128_t lost =
          small & ((static_cast<uint128_t>(1) << distance) - 1);
      sticky = lost != 0;
      small >>= distance;
    }
    const int exponent = a.exponent_ - 63;
    if (a.negative_ == b.negative_) {
      return Round(a.negative_, big + small, exponent, sticky);
    }
    // Opposite signs: a sticky bit below `small` borrows one unit.
    if (big > small || (big == small && sticky)) {
      uint128_t diff = big - small;
      if (sticky) {
        diff -= 1;  // exact value is diff - epsilon: round from below
      }
      return Round(a.negative_, diff, exponent, sticky);
    }
    if (big == small) return Special(Kind::kZero, false);
    return Round(b.negative_, small - big, exponent, false);
  }

  static SoftExtended Mul(SoftExtended a, SoftExtended b) {
    const bool negative = a.negative_ != b.negative_;
    if (a.kind_ == Kind::kNaN || b.kind_ == Kind::kNaN) {
      return Special(Kind::kNaN, false);
    }
    if (a.kind_ == Kind::kInf || b.kind_ == Kind::kInf) {
      if (a.kind_ == Kind::kZero || b.kind_ == Kind::kZero) {
        return Special(Kind::kNaN, true);
      }
      return Special(Kind::kInf, negative);
    }
    if (a.kind_ == Kind::kZero || b.kind_ == Kind::kZero) {
      return Special(Kind::kZero, negative);
    }
    const uint128_t product = static_cast<uint128_t>(a.mantissa_) * b.mantissa_;
    return Round(negative, product, a.exponent_ + b.exponent_, false);
  }

  static SoftExtended Div(SoftExtended a, SoftExtended b) {
    const bool negative = a.negative_ != b.negative_;
    if (a.kind_ == Kind::kNaN || b.kind_ == Kind::kNaN) {
      return Special(Kind::kNaN, false);
    }
    if (a.kind_ == Kind::kInf) {
      return b.kind_ == Kind::kInf ? Special(Kind::kNaN, true)
                                   : Special(Kind::kInf, negative);
    }
    if (b.kind_ == Kind::kInf) return Special(Kind::kZero, negative);
    if (b.kind_ == Kind::kZero) {
      return a.kind_ == Kind::kZero ? Special(Kind::kNaN, true)
                                    : Special(Kind::kInf, negative);
    }
    if (a.kind_ == Kind::kZero) return Special(Kind::kZero, negative);
    const uint128_t numerator = static_cast<uint128_t>(a.mantissa_) << 64;
    uint128_t quotient = numerator / b.mantissa_;
    uint128_t remainder = numerator % b.mantissa_;
    int exponent = a.exponent_ - b.exponent_ - 64;
    // quotient has 64 or 65 bits; make sure one guard bit exists.
    if ((quotient >> 64) == 0) {
      const uint128_t twice = remainder << 1;
      quotient <<= 1;
      if (twice >= b.mantissa_) {
        quotient |= 1;
        remainder = twice - b.mantissa_;
      } else {
        remainder = twice;
      }
      exponent -= 1;
    }
    return Round(negative, quotient, exponent, remainder != 0);
  }

  // -1: a < b, 0: equal, 1: a > b, 2: unordered.
  static int Compare(SoftExtended a, SoftExtended b) {
    if (a.kind_ == Kind::kNaN || b.kind_ == Kind::kNaN) return 2;
    const int sa = a.Sign();
    const int sb = b.Sign();
    if (sa != sb) return sa < sb ? -1 : 1;
    if (sa == 0) return 0;
    const int magnitude = CompareMagnitude(a, b);
    return sa > 0 ? magnitude : -magnitude;
  }
  [[nodiscard]] int Sign() const {
    if (kind_ == Kind::kZero) return 0;
    return negative_ ? -1 : 1;
  }
  static int CompareMagnitude(SoftExtended a, SoftExtended b) {
    if (a.kind_ == Kind::kInf || b.kind_ == Kind::kInf) {
      if (a.kind_ == b.kind_) return 0;
      return a.kind_ == Kind::kInf ? 1 : -1;
    }
    const int64_t ea = int64_t{a.exponent_} + 63;
    const int64_t eb = int64_t{b.exponent_} + 63;
    if (ea != eb) return ea < eb ? -1 : 1;
    if (a.mantissa_ != b.mantissa_) return a.mantissa_ < b.mantissa_ ? -1 : 1;
    return 0;
  }

  [[nodiscard]] double ToDouble() const {
    switch (kind_) {
      case Kind::kNaN:
        return negative_ ? -std::numeric_limits<double>::quiet_NaN()
                         : std::numeric_limits<double>::quiet_NaN();
      case Kind::kInf:
        return negative_ ? -std::numeric_limits<double>::infinity()
                         : std::numeric_limits<double>::infinity();
      case Kind::kZero:
        return negative_ ? -0.0 : 0.0;
      case Kind::kFinite:
        break;
    }
    // value = mantissa_ * 2^exponent_; keep 53 bits (fewer when subnormal).
    const int64_t lead = int64_t{exponent_} + 63;
    const int64_t lsb = std::max<int64_t>(lead - 52, -1074);
    const int64_t shift = lsb - exponent_;  // >= 11
    uint64_t kept = 0;
    if (shift >= 64) {
      const bool above_half =
          shift == 64 ? mantissa_ > (uint64_t{1} << 63) : false;
      const bool half = shift == 64 && mantissa_ == (uint64_t{1} << 63);
      kept = above_half ? 1 : 0;
      (void)half;  // ties to even: 0
    } else {
      kept = mantissa_ >> shift;
      const uint64_t dropped = mantissa_ & ((uint64_t{1} << shift) - 1);
      const uint64_t half = uint64_t{1} << (shift - 1);
      if (dropped > half || (dropped == half && (kept & 1U) != 0)) ++kept;
    }
    const double magnitude =
        std::ldexp(static_cast<double>(kept), static_cast<int>(lsb));
    return negative_ ? -magnitude : magnitude;
  }

  uint64_t mantissa_{0};  // normalized: bit 63 set when finite
  int32_t exponent_{0};   // value = mantissa_ * 2^exponent_
  Kind kind_{Kind::kZero};
  bool negative_{false};
};

inline SoftExtended Sqrt(SoftExtended x) {
  if (x.kind_ == SoftExtended::Kind::kNaN) return x;
  if (x.kind_ == SoftExtended::Kind::kZero) return x;  // sqrt(-0) = -0
  if (x.negative_) {
    return SoftExtended::Special(SoftExtended::Kind::kNaN, true);
  }
  if (x.kind_ == SoftExtended::Kind::kInf) return x;
  // radicand = mantissa * 2^e with e even, scaled into [2^126, 2^128) so the
  // integer root carries exactly 64 significant bits.
  __extension__ using uint128_t = unsigned __int128;
  uint128_t radicand = static_cast<uint128_t>(x.mantissa_);
  int exponent = x.exponent_;
  if ((exponent & 1) != 0) {
    radicand <<= 63;
    exponent -= 63;
  } else {
    radicand <<= 64;
    exponent -= 64;
  }
  // Integer square root by bit-wise restoration (result < 2^64).
  uint128_t root = 0;
  uint128_t rem = radicand;
  uint128_t bit = static_cast<uint128_t>(1) << 126;
  while (bit > rem) bit >>= 2;
  while (bit != 0) {
    if (rem >= root + bit) {
      rem -= root + bit;
      root = (root >> 1) + bit;
    } else {
      root >>= 1;
    }
    bit >>= 2;
  }
  // root has 64 bits (radicand >= 2^126); round to nearest: up iff the
  // remainder exceeds root (exact halves are impossible).
  bool round_up = rem > root;
  uint64_t mantissa = static_cast<uint64_t>(root);
  int result_exponent = exponent / 2;
  if (round_up) {
    ++mantissa;
    if (mantissa == 0) {
      mantissa = uint64_t{1} << 63;
      ++result_exponent;
    }
  }
  SoftExtended r;
  r.kind_ = SoftExtended::Kind::kFinite;
  r.negative_ = false;
  r.mantissa_ = mantissa;
  r.exponent_ = result_exponent;
  return r;
}

#if LDBL_MANT_DIG >= 64 && LDBL_MAX_EXP > DBL_MAX_EXP
using ExtendedFloat = long double;
inline constexpr bool kExtendedFloatIsNative = true;
#else
using ExtendedFloat = SoftExtended;
inline constexpr bool kExtendedFloatIsNative = false;
#endif

inline long double Sqrt(long double x) { return std::sqrt(x); }
inline bool IsNaN(long double x) { return std::isnan(x); }
inline bool IsNaN(SoftExtended x) { return x.IsNaN(); }
inline bool IsInf(long double x) { return std::isinf(x); }
inline bool IsInf(SoftExtended x) { return x.IsInf(); }

}  // namespace tinylamb::platform

#endif  // TINYLAMB_COMMON_PLATFORM_EXTENDED_FLOAT_HPP
