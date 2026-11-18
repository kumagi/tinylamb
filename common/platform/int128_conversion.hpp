// Copyright 2026 KUMAZAKI Hiroki
// SPDX-License-Identifier: Apache-2.0
#ifndef TINYLAMB_COMMON_PLATFORM_INT128_CONVERSION_HPP
#define TINYLAMB_COMMON_PLATFORM_INT128_CONVERSION_HPP

#include <bit>
#include <cstdint>

#ifdef __SIZEOF_INT128__
namespace tinylamb::platform {

__extension__ using Int128 = __int128;
__extension__ using UInt128 = unsigned __int128;

// Magnitude truncated toward zero. Saturates overflow; NaN maps to zero.
inline UInt128 DoubleToUInt128Magnitude(double value) {
  const auto bits = std::bit_cast<uint64_t>(value);
  const uint64_t fraction = bits & ((uint64_t{1} << 52) - 1);
  const int biased_exponent = static_cast<int>((bits >> 52) & 0x7ff);
  if (biased_exponent == 0x7ff && fraction != 0) {
    return 0;
  }
  const int exponent = biased_exponent - 1023;
  if (exponent < 0) {
    return 0;
  }
  if (exponent >= 128) {
    return ~static_cast<UInt128>(0);
  }
  const uint64_t mantissa = fraction | (uint64_t{1} << 52);
  if (exponent >= 52) {
    return static_cast<UInt128>(mantissa) << (exponent - 52);
  }
  return mantissa >> (52 - exponent);
}

inline Int128 SaturatingDoubleToInt128(double value) {
  const UInt128 magnitude = DoubleToUInt128Magnitude(value);
  const UInt128 sign_bit = static_cast<UInt128>(1) << 127;
  const auto maximum = static_cast<Int128>(sign_bit - 1);
  if (value < 0) {
    if (magnitude >= sign_bit) {
      return -maximum - 1;
    }
    return -static_cast<Int128>(magnitude);
  }
  return magnitude >= sign_bit ? maximum : static_cast<Int128>(magnitude);
}

}  // namespace tinylamb::platform
#endif
#endif
