/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include <cstddef>
#include <filesystem>
#include <fstream>
#include <ios>
#include <iterator>
#include <string>
#include <string_view>

#include "common/log_message.hpp"
#include "query/googlesql_ast_fuzzer.hpp"

static void TestCase(std::string_view input) {
  tinylamb::Try(reinterpret_cast<const uint8_t*>(input.data()), input.size(),
                true);
}

int main(int argc, char** argv) {
  if (argc < 2) {
    LOG(FATAL) << "set test file.";
    return 1;
  }
  std::filesystem::path target_dir(argv[1]);

  std::filesystem::directory_iterator dir(target_dir);
  for (const auto& file : dir) {
    std::ifstream case_data(file.path(), std::ios::in | std::ios::binary);
    if (!case_data) {
      LOG(ERROR) << "cannot open: " << file.path();
      continue;
    }
    // Formatted extraction stops at the first whitespace; fuzz inputs are
    // binary and must be replayed verbatim.
    std::string file_content((std::istreambuf_iterator<char>(case_data)),
                             std::istreambuf_iterator<char>());
    LOG(ERROR) << "test: " << file.path();
    TestCase(file_content);
  }
  return 0;
}
