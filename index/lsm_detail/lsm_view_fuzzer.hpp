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

#ifndef TINYLAMB_LSM_VIEW_FUZZER_HPP
#define TINYLAMB_LSM_VIEW_FUZZER_HPP

#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <map>
#include <random>
#include <string>
#include <vector>

#include "common/constants.hpp"
#include "common/debug.hpp"
#include "common/log_message.hpp"
#include "common/random_string.hpp"
#include "index/lsm_detail/blob_file.hpp"
#include "index/lsm_detail/sorted_run.hpp"
#include "lsm_view.hpp"

namespace tinylamb {

inline void Try(const uint64_t seed, bool verbose) {
  if (verbose) {
    LOG(INFO) << "seed: " << seed;
  }
  std::filesystem::path base = "lsm_view_fuzzer-" + RandomString(20, false);
  std::filesystem::create_directory(base);
  std::mt19937 rand(seed);
  std::string blob_path = base / "blob.db";
  BlobFile blob(blob_path, 1024 * 1024LLU, 1024LLU * 1024 * 1024 * 8);
  std::vector<std::filesystem::path> index_files;

  int files = rand() % 10 + 2;
  std::map<std::string, std::string> expected;

  for (int file = 0; file < files; ++file) {
    std::map<std::string, LSMValue> mem_value;
    int size = rand() % 1000 + 10;
    for (int i = 0; i < size; ++i) {
      std::string key = RandomString(rand() % 1000 + 1, false);

      if (rand() % 2 == 0) {
        mem_value[key] = LSMValue::Delete();
        expected.erase(key);
        if (verbose) {
          LOG(INFO) << "delete " << OmittedString(key, 20);
        }
      } else {
        std::string value = RandomString(rand() % 1000 + 1, false);
        mem_value[key] = LSMValue(value);
        expected[key] = value;
        if (verbose) {
          LOG(INFO) << "store " << OmittedString(key, 20) << " => "
                    << OmittedString(value, 10);
        }
      }
    }
    std::filesystem::path path = base / (std::to_string(file) + ".idx");
    SortedRun::Construct(path, mem_value, blob, file);
    index_files.emplace_back(path);
  }
  blob.Flush();
  if (verbose) {
    for (const auto& file : index_files) {
      SortedRun run(file);
      LOG(DEBUG) << run;
    }
  }

  LSMView view(blob, index_files);
  auto iter = view.Begin();
  auto expected_iter = expected.begin();
  while (iter.IsValid()) {
    std::string key = iter.Key();
    std::string value = iter.Value();
    if (key != expected_iter->first) {
      LOG(FATAL) << "key mismatch " << key << " vs " << expected_iter->first;
      exit(1);
    }
    if (value != expected_iter->second) {
      LOG(FATAL) << "value mismatch " << value << " vs "
                 << expected_iter->second;
      exit(1);
    }
    ++iter;
    ++expected_iter;
  }
  for (const auto& it : expected) {
    auto found = view.Find(it.first);
    if (!found.HasValue()) {
      LOG(FATAL) << "key not found: " << it.first;
      exit(1);
    }
    if (found.Value() != it.second) {
      LOG(FATAL) << "value mismatch " << found.Value() << " vs " << it.second;
      exit(1);
    }
  }

  std::filesystem::remove_all(base);
}

}  // namespace tinylamb

#endif  // TINYLAMB_LSM_VIEW_FUZZER_HPP
