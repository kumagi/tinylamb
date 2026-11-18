/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include <cstddef>
#include <cstdint>
#include <cstdlib>

#include "query/sql_ooc_fuzzer.hpp"

extern "C" [[maybe_unused]] int LLVMFuzzerTestOneInput(const uint8_t* data,
                                                       size_t size) {
  tinylamb::OocFuzzTry(data, size,
                       std::getenv("TINYLAMB_OOC_FUZZ_VERBOSE") != nullptr ||
                           std::getenv("TINYLAMB_FUZZ_VERBOSE") != nullptr);
  return 0;
}
