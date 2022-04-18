//
// Created by kumagi on 2022/04/19.
//

#include "table/table_fuzzer.hpp"

extern "C" [[maybe_unused]] int LLVMFuzzerTestOneInput(const uint8_t* data,
                                                       size_t size) {
  if (size < 8) return 0;
  tinylamb::Try(*(uint64_t*)data, false);
  return 0;
}