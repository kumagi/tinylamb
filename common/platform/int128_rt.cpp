// Copyright 2026 KUMAZAKI Hiroki
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Compiler-rt style builtins for 128-bit integers on targets where __int128
// is only available through -fforce-enable-int128 (32-bit arm/x86). Clang
// lowers every pure-integer ti128 operation inline there, so the only
// libcalls it emits are the float<->ti128 conversions implemented below.
// The classic integer entry points are provided too, as cheap insurance
// against inlining decisions changing across clang versions.
//
// The whole file is empty on targets with a native __int128 or when the
// type is unavailable entirely, so it is safe to compile unconditionally.

#if defined(__SIZEOF_INT128__) && !defined(__x86_64__) && \
    !defined(__aarch64__) && !defined(_WIN64)
#define TINYLAMB_INT128_SOFT 1
#endif

#ifdef TINYLAMB_INT128_SOFT

#include <cstdint>
#include <cstring>

namespace {

using ti128 = __int128;
using tu128 = unsigned __int128;

int Clz128(tu128 u) {
  const uint64_t hi = static_cast<uint64_t>(u >> 64);
  return hi ? __builtin_clzll(hi)
            : 64 + __builtin_clzll(static_cast<uint64_t>(u));
}

// Returns the 53-bit-rounded magnitude plus the exponent of the top bit.
// *exponent is the binary exponent of the leading one bit of |u| (u != 0).
uint64_t RoundToF64Mantissa(tu128 u, int* exponent) {
  int top = 127 - Clz128(u);
  tu128 m;
  if (top > 52) {
    const int drop = top - 52;
    const tu128 dropped = u & ((static_cast<tu128>(1) << drop) - 1);
    m = u >> drop;
    const tu128 half = static_cast<tu128>(1) << (drop - 1);
    // Round half to even.
    if (dropped > half || (dropped == half && (m & 1))) {
      m += 1;
      if (m >> 53) {
        m >>= 1;
        ++top;
      }
    }
  } else {
    m = u << (52 - top);
  }
  *exponent = top;
  return static_cast<uint64_t>(m);
}

double U128ToF64(tu128 u) {
  if (u == 0) {
    return 0.0;
  }
  int exponent = 0;
  const uint64_t mantissa = RoundToF64Mantissa(u, &exponent);
  const uint64_t bits = (static_cast<uint64_t>(exponent + 1023) << 52) |
                        (mantissa & ((1ULL << 52) - 1));
  double d;
  std::memcpy(&d, &bits, sizeof(d));
  return d;
}

// Truncate-to-zero conversion shared by the signed/unsigned fix routines.
tu128 F64ToU128(double d) {
  uint64_t bits;
  std::memcpy(&bits, &d, sizeof(bits));
  const int exponent = static_cast<int>((bits >> 52) & 0x7ff) - 1023;
  if (exponent < 0) {
    return 0;
  }
  const uint64_t mantissa = (bits & ((1ULL << 52) - 1)) | (1ULL << 52);
  if (exponent >= 127) {
    return ~static_cast<tu128>(0);
  }
  if (exponent >= 52) {
    return static_cast<tu128>(mantissa) << (exponent - 52);
  }
  return mantissa >> (52 - exponent);
}

float U128ToF32(tu128 u) { return static_cast<float>(U128ToF64(u)); }

}  // namespace

extern "C" {

// --- float conversions ----------------------------------------------------

double __floattidf(ti128 v) {
  if (v < 0) {
    return -U128ToF64(-static_cast<tu128>(v));
  }
  return U128ToF64(v);
}

double __floatuntidf(tu128 v) { return U128ToF64(v); }

float __floattisf(ti128 v) { return static_cast<float>(__floattidf(v)); }

float __floatuntisf(tu128 v) { return U128ToF32(v); }

ti128 __fixdfti(double d) {
  const tu128 u = F64ToU128(d);
  if (d < 0) {
    return -static_cast<ti128>(u);
  }
  return static_cast<ti128>(u);
}

tu128 __fixunsdfti(double d) { return d < 0 ? 0 : F64ToU128(d); }

ti128 __fixsfti(float f) { return __fixdfti(f); }

tu128 __fixunssfti(float f) { return __fixunsdfti(f); }

// --- integer entry points (normally inlined; kept as fallbacks) -----------

ti128 __multi3(ti128 a, ti128 b) { return a * b; }
ti128 __divti3(ti128 a, ti128 b) { return a / b; }
ti128 __modti3(ti128 a, ti128 b) { return a % b; }
tu128 __udivti3(tu128 a, tu128 b) { return a / b; }
tu128 __umodti3(tu128 a, tu128 b) { return a % b; }
ti128 __negti2(ti128 a) { return -a; }
ti128 __ashlti3(ti128 a, int b) { return a << b; }
ti128 __ashrti3(ti128 a, int b) { return a >> b; }
tu128 __lshrti3(tu128 a, int b) { return a >> b; }

}  // extern "C"

#endif  // TINYLAMB_INT128_SOFT
