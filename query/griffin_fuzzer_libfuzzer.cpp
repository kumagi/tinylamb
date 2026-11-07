/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include <cstddef>
#include <cstdint>
#include <cstdlib>

#include "query/griffin_fuzzer.hpp"

extern "C" [[maybe_unused]] int LLVMFuzzerTestOneInput(const uint8_t* data,
                                                       size_t size) {
  tinylamb::GriffinFuzzTry(data, size,
                           std::getenv("TINYLAMB_FUZZ_VERBOSE") != nullptr);
  return 0;
}
