//
// Created by kumagi on 22/09/22.
//

#include "page/leaf_page_fuzzer.hpp"

extern "C" [[maybe_unused]] int LLVMFuzzerTestOneInput(const uint8_t* data,
                                                       size_t size) {
  if (size < 8) return 0;
  tinylamb::Try(*(uint64_t*)data, false);
  return 0;
}