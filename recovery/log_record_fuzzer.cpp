/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include "recovery/log_record_fuzzer.hpp"

#include <cstddef>
#include <cstdint>

extern "C" [[maybe_unused]] int LLVMFuzzerTestOneInput(const uint8_t* data,
                                                       size_t size) {
  tinylamb::Try(data, size, false);
  return 0;
}
