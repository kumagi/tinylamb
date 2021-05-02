//
// Created by kumagi on 22/09/22.
//

#include <filesystem>

#include "page/leaf_page_fuzzer.hpp"

int main(int argc, char** argv) {
  if (argc < 2) {
    std::cout << "set target file path\n";
    return 1;
  }
  std::filesystem::path file(argv[1]);
  std::ifstream case_data(file, std::ios::in | std::ios::binary);
  std::string file_content;
  file_content.resize(8);
  case_data.read(file_content.data(), 8);
  LOG(INFO) << "test file: " << file;
  tinylamb::Try(*(uint64_t*)file_content.data(), true);
  LOG(INFO) << "successfully finished.";
  return 0;
}
