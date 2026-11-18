/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#pragma once

#include <cstdint>

namespace tinylamb::platform {

// Write, in input order, positions whose signed value is in [lower, upper].
// Output has room for count positions and must not alias the input. NULL
// validity is deliberately separate: callers can intersect it afterwards.
uint32_t SelectInt64ClosedRange(const int64_t* values, uint32_t count,
                                int64_t lower, int64_t upper, uint32_t* output);

// Compact existing positions in place, preserving order. Values contain the
// bit representation of doubles. NaNs never match; endpoints may be infinite.
uint32_t CompactDoubleRange(const int64_t* values, uint32_t* positions,
                            uint32_t count, double lower, double upper,
                            bool lower_inclusive, bool upper_inclusive);

}  // namespace tinylamb::platform
