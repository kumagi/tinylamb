//
// Created by kumagi on 2022/04/10.
//
#include <filesystem>

#include "index/b_plus_tree_fuzzer.hpp"

int main(int argc, char** argv) {
  if (argc < 2) {
    std::cout << "set target file path\n";
    return 1;
  }
  std::filesystem::path file(argv[1]);
  std::ifstream case_data(file, std::ios::in | std::ios::binary);
  std::string file_content;
  case_data >> file_content;
  LOG(ERROR) << "test: " << file;
  tinylamb::Try(*(uint64_t*)file_content.data(), true);
  return 0;
}
