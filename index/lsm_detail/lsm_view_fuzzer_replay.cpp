/**
 * Copyright 2024 KUMAZAKI Hiroki
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
#include <iomanip>
#include <ios>
#include <iostream>
#include <string>

#include "common/log_message.hpp"
#include "index/lsm_detail/lsm_view_fuzzer.hpp"

int main(int argc, char** argv) {
  if (argc < 2) {
    std::cout << "set target file path\n";
    return 1;
  }
  std::filesystem::path file(argv[1]);
  std::ifstream case_data(file, std::ios::in | std::ios::binary);
  std::string file_content(8, 0);
  case_data.read(file_content.data(), 8);
  LOG(INFO) << "test file: " << file << " : " << std::hex << std::setw(2)
            << std::setfill('0') << "0x" << ((int)file_content[0] & 0xff)
            << ", 0x" << std::hex << std::setw(2)
            << ((int)file_content[1] & 0xff) << ", 0x" << std::hex
            << std::setw(2) << ((int)file_content[2] & 0xff) << ", 0x"
            << std::hex << std::setw(2) << ((int)file_content[3] & 0xff)
            << ", 0x" << std::hex << std::setw(2)
            << ((int)file_content[4] & 0xff) << ", 0x" << std::hex
            << std::setw(2) << ((int)file_content[5] & 0xff) << ", 0x"
            << std::hex << std::setw(2) << ((int)file_content[6] & 0xff)
            << ", 0x" << std::hex << std::setw(2)
            << ((int)file_content[7] & 0xff);
  uint64_t seed = *reinterpret_cast<const uint64_t*>(file_content.data());
  tinylamb::Try(seed, true);
  LOG(INFO) << "successfully finished.";
  return 0;
}
