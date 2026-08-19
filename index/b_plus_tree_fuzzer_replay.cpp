/**
 * Copyright 2023 KUMAZAKI Hiroki
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <ios>
#include <iostream>
#include <iterator>
#include <string>
#include <vector>

#include "common/log_message.hpp"
#include "index/b_plus_tree_fuzzer.hpp"

int main(int argc, char** argv) {
  if (argc < 2) {
    std::cout << "set target file path\n";
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
