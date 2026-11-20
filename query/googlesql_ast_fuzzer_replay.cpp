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
#include "query/googlesql_ast_fuzzer.hpp"

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
  // Stable, filtered input set: collector sidecars (.report.txt/.test/.sql)
  // are not AST dumps, and directory_iterator order is filesystem-dependent.
  std::vector<std::filesystem::path> inputs;
  std::error_code iter_ec;
  for (const auto& file : dir) {
    const auto& path = file.path();
    const std::string name = path.filename().string();
    if (!file.is_regular_file(iter_ec)) {
      continue;
    }
    if (name.ends_with(".report.txt") || name.ends_with(".test") ||
        name.ends_with(".sql") || name.ends_with(".log")) {
      continue;
    }
    inputs.push_back(path);
  }
  std::sort(inputs.begin(), inputs.end());
  for (const auto& file : inputs) {
    std::ifstream case_data(file, std::ios::in | std::ios::binary);
    if (!case_data) {
      LOG(ERROR) << "cannot open: " << file;
      continue;
    }
    // Formatted extraction stops at the first whitespace; fuzz inputs are
    // binary and must be replayed verbatim.
    std::string file_content((std::istreambuf_iterator<char>(case_data)),
                             std::istreambuf_iterator<char>());
    LOG(ERROR) << "test: " << file;
    TestCase(file_content);
  }
  return 0;
}
