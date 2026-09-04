/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include <cstddef>
#include <cstdint>

#include "query/sql_oracle_fuzzer.hpp"

extern "C" [[maybe_unused]] int LLVMFuzzerTestOneInput(const uint8_t* data,
                                                       size_t size) {
  tinylamb::OracleFuzzTry(data, size, false);
  return 0;
}
