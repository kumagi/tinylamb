/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include <cstddef>
#include <filesystem>
#include <fstream>
#include <ios>
#include <iterator>
#include <string>
#include <string_view>
#include <system_error>

#include "common/log_message.hpp"
#include "server/postgres_protocol_fuzzer.hpp"

namespace {
void TestCase(std::string_view input) {
  tinylamb::Try(reinterpret_cast<const uint8_t*>(input.data()), input.size(),
                true);
}
}  // namespace

int main(int argc, char** argv) {
  if (argc < 2) {
    LOG(FATAL) << "set test file.";
    return 1;
  }
  std::filesystem::path target_dir(argv[1]);

  // T3946: the throwing constructor turns a missing/renamed replay dir
  // into an uncaught filesystem_error stack dump; report it as the usage
  // error it is and exit nonzero.
  std::error_code dir_ec;
  std::filesystem::directory_iterator dir(target_dir, dir_ec);
  if (dir_ec) {
    LOG(ERROR) << "cannot open directory " << target_dir << ": "
               << dir_ec.message();
    return 1;
  }
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
    TestCase(file_content);
  }
  return 0;
}
