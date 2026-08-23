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
#include <string>

#include "common/byte_stream.hpp"
#include "common/constants.hpp"
#include "common/log_message.hpp"
#include "common/random_string.hpp"
#include "lsm_detail/lsm_view.hpp"
#include "lsm_tree.hpp"
namespace tinylamb {

inline uint64_t Generate(size_t offset) { return offset * 19937 + 2147483647; }

// Byte-driven LSM stress test.  The input directly encodes a Write / Delete /
// Verify operation stream with key and value bytes taken from the same buffer,
// so libFuzzer can steer exact keys, tombstones, and flush points toward
// memtable freeze and merge boundaries instead of sampling a PRNG.
inline void Try(const uint8_t* data, size_t size, bool verbose) {
  ByteStream stream(data, size);
  RandomStringInitialize();
  std::filesystem::path base_path = "lsm_tree_fuzzer-" + RandomString(20, true);
  if (std::filesystem::exists(base_path)) {
    std::filesystem::remove_all(base_path);
  }
  std::map<std::string, std::string> expected;
  LSMTree tree(base_path);

  auto Scan = [&]() {
    for (const auto& it : expected) {
      if (!tree.Contains(it.first)) {
        LOG(FATAL) << it.first << "not found";
        exit(1);
      }
    }
  };

  constexpr size_t kMaxOps = 200;
  for (size_t i = 0; i < kMaxOps && stream.Remaining(); ++i) {
    std::string key(stream.Bytes(stream.Pick(64)));
    switch (stream.Pick(3)) {
      case 0: {  // Write
        std::string value(stream.Bytes(stream.Pick(64)));
        if (verbose) {
          LOG(TRACE) << "Insert: " << key << " => " << value;
        }
        tree.Write(key, value, stream.Pick(1) == 0);
        expected[key] = value;
        break;
      }
      case 1: {  // Delete (tombstone)
        if (verbose) {
          LOG(TRACE) << "Delete: " << key;
        }
        tree.Delete(key, stream.Pick(1) == 0);
        expected.erase(key);
        break;
      }
      default: {  // Verify the model against the tree.
        Scan();
        break;
      }
    }
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
    if (expected_it == expected.end()) {
      // The view enumerated more entries than the model: stop instead of
      // dereferencing past the map's end.
      LOG(ERROR) << "view yields more keys than the model: "
                 << actual_it.Key();
      exit(1);
    }
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
    exit(1);
  }
  std::filesystem::remove_all(base_path);
  if (verbose) {
    LOG(INFO) << "Successfully finished.";
  }
}

}  // namespace tinylamb

#endif  // TINYLAMB_LSM_TREE_FUZZER_HPP
