/** Copyright 2026 KUMAZAKI Hiroki. Licensed under the Apache-2.0. */

#include "page/pax_page_fuzzer.hpp"

#include <cstddef>
#include <cstdint>

extern "C" [[maybe_unused]] int LLVMFuzzerTestOneInput(const uint8_t* data,
                                                       size_t size) {
  if (size == 0) return 0;
  tinylamb::Try(data, size, false);
  return 0;
}
