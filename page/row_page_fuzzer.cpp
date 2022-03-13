//
// Created by kumagi on 2022/02/10.
//

#include "page/row_page_fuzzer.hpp"

extern "C" [[maybe_unused]] int LLVMFuzzerTestOneInput(const uint8_t* data,
                                                       size_t size) {
  if (size < 2) return 0;
  static tinylamb::RowPageEnvironment env;
  env.Initialize();
  tinylamb::Operation op(&env);
  std::string_view input(reinterpret_cast<const char*>(data), size);
  while (!input.empty()) {
    size_t read_bytes = op.Execute(input);
    input.remove_prefix(read_bytes);
  }
  return 0;
}
