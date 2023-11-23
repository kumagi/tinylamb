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