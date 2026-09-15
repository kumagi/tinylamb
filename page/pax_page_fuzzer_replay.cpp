/** Copyright 2026 KUMAZAKI Hiroki. Licensed under the Apache-2.0. */

#include <cstddef>
#include <filesystem>
#include <fstream>
#include <ios>
#include <iterator>
#include <string>
#include <string_view>
#include <vector>

#include "common/log_message.hpp"
#include "page/pax_page_fuzzer.hpp"

int main(int argc, char** argv) {
  if (argc < 2) {
    LOG(FATAL) << "set test file path";
    return 1;
  }
  std::filesystem::path file(argv[1]);
  std::ifstream case_data(file, std::ios::in | std::ios::binary);
  std::vector<char> content((std::istreambuf_iterator<char>(case_data)),
                            std::istreambuf_iterator<char>());
  LOG(INFO) << "test file: " << file << " (" << content.size() << " bytes)";
  tinylamb::Try(reinterpret_cast<const uint8_t*>(content.data()),
                content.size(), true);
  LOG(INFO) << "successfully finished.";
  return 0;
}
