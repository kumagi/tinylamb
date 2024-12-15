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

#ifndef TINYLAMB_LSM_TREE_FUZZER_HPP
#define TINYLAMB_LSM_TREE_FUZZER_HPP

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <map>
#include <random>
#include <string>

#include "common/constants.hpp"
#include "common/log_message.hpp"
#include "common/random_string.hpp"
#include "lsm_detail/lsm_view.hpp"
#include "lsm_tree.hpp"
namespace tinylamb {

inline uint64_t Generate(size_t offset) { return offset * 19937 + 2147483647; }

inline void Try(const uint64_t seed, bool verbose) {
  RandomStringInitialize();
  std::filesystem::path base_path = "lsm_tree_fuzzer-" + RandomString(20, true);
  if (std::filesystem::exists(base_path)) {
    std::filesystem::remove_all(base_path);
  }
  std::mt19937 rand(seed);
  std::map<std::string, std::string> expected;
  LSMTree tree(base_path);

  const size_t kTestSize = rand() % 1000 + 10;

  auto Scan = [&]() {
    for (const auto& it : expected) {
      if (!tree.Contains(it.first)) {
        LOG(FATAL) << it.first << "not found";
        exit(1);
      }
    }
  };

  for (size_t i = 0; i < kTestSize; ++i) {
    std::string key = RandomString(rand() % 4 + 2, false);
    std::string value = RandomString(rand() % 16 + 8, false);
    if (verbose) {
      LOG(TRACE) << "Insert: (" << (i + 1) << "/" << kTestSize << "): " << key
                 << " => " << value;
    }
    tree.Write(key, value, false);
    expected[key] = value;
    Scan();
  }
  tree.Sync();
  Scan();
  LSMView v = tree.GetView();
  if (verbose) {
    LOG(TRACE) << v;
    LOG(WARN) << v.Begin();
  }
  auto expected_it = expected.begin();
  for (auto actual_it = v.Begin(); actual_it.IsValid();
       ++actual_it, ++expected_it) {
    if (actual_it.Key() != expected_it->first) {
      LOG(ERROR) << actual_it.Key() << " != " << expected_it->first;
      exit(1);
    }
    if (actual_it.Value() != expected_it->second) {
      LOG(ERROR) << actual_it.Key() << " -- " << actual_it.Value()
                 << " != " << expected_it->second;
      exit(1);
    }
    if (!tree.Contains(actual_it.Key())) {
      LOG(ERROR) << actual_it.Key() << " not found";
      exit(1);
    }
  }
  if (expected_it != expected.end()) {
    LOG(ERROR) << expected_it->first << " not finished";
  }
  std::filesystem::remove_all(base_path);
  if (verbose) {
    LOG(INFO) << "Successfully finished.";
  }
}

}  // namespace tinylamb

#endif  // TINYLAMB_LSM_TREE_FUZZER_HPP
