//
// Created by kumagi on 2022/02/17.
//

#include <filesystem>

#include "page/row_page_fuzzer.hpp"

void TestCase(std::string_view input) {
  tinylamb::RowPageEnvironment env;
  env.Initialize();
  tinylamb::Operation op(&env);
  while (!input.empty()) {
    size_t read_bytes = op.Execute(input, true);
    input.remove_prefix(read_bytes);
  }
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
    std::string file_content;
    case_data >> file_content;
    LOG(ERROR) << "test: " << file.path();
    TestCase(file_content);
  }
  return 0;
}