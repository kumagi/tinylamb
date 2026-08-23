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
#include <cassert>
#include <chrono>
#include <cstddef>
#include <filesystem>
#include <functional>
#include <iomanip>
#include <iostream>
#include <random>
#include <set>
#include <string>
#include <string_view>

#include "common/random_string.hpp"
#include "index/lsm_tree.hpp"
#include "lsm_view.hpp"

using namespace tinylamb;

namespace {
void Bench(size_t count, const std::function<void()>& fun,
           std::string_view name, std::string_view unit) {
  auto begin = std::chrono::steady_clock::now();
  fun();
  auto finish = std::chrono::steady_clock::now();
  int ms = std::chrono::duration_cast<std::chrono::milliseconds>(finish - begin)
               .count();
  if (ms == 0) {
    ms = 1;
  }
  std::cout << std::setw(25) << name << ": " << (double)count / ms << " "
            << unit << "\n";
}
}  // namespace

int main(int argc, char** argv) {
  std::set<char> opts;
  for (int i = 1; i < argc; ++i) {
    std::string v(argv[i]);
    for (const char& j : v) {
      opts.emplace(j);
    }
  }
  if (opts.empty()) {
    opts.emplace('k');
    opts.emplace('s');
    opts.emplace('m');
  }
  std::random_device rd;
  std::mt19937 random(rd());
  size_t kCount = 500000;

  if (opts.contains('k')) {
    std::filesystem::path path = "tmp_blob_file_test-" + RandomString();
    std::filesystem::create_directory(path);
    LSMTree tree(path);
    Bench(
        kCount,
        [&]() {
          for (size_t i = 0; i < kCount; ++i) {
            tree.Write(std::to_string(i * i), std::to_string(i));
          }
          tree.Sync();
        },
        "KDB Write", "writes/ms");
    LSMView vm = tree.GetView();

    if (opts.contains('s')) {
      Bench(
          kCount,
          [&]() {
            for (size_t i = 0; i < kCount; ++i) {
              std::string key = std::to_string(random() % kCount);
              auto v = vm.Find(key);
            }
          },
          "KDB Success Find", "reads/ms");
    }
    if (opts.contains('f')) {
      Bench(
          kCount,
          [&]() {
            for (size_t i = 0; i < kCount; ++i) {
              std::string key = std::to_string(random() % kCount) + "a";
              auto v = vm.Find(key);
            }
          },
          "KDB Failed Find", "reads/ms");
    }
    if (opts.contains('i')) {
      Bench(
          kCount,
          [&]() {
            volatile size_t sink = 0;
            for (LSMView::Iterator it = vm.Begin(); it.IsValid(); ++it) {
              std::string key = it.Key();
              std::string value = it.Value();
              sink += key.size() + value.size();
            }
          },
          "KDB Full Scan before merge", "reads/ms");
    }
    if (opts.contains('m')) {
      tree.MergeAll();
      LSMView vm2 = tree.GetView();
      Bench(
          kCount,
          [&]() {
            for (size_t i = 0; i < kCount; ++i) {
              std::string key = std::to_string(random() % kCount);
              auto v = vm2.Find(key);
            }
          },
          "KDB Merged Success Find", "reads/ms");

      if (opts.contains('f')) {
        Bench(
            kCount,
            [&]() {
              for (size_t i = 0; i < kCount; ++i) {
                std::string key = std::to_string(random() % kCount) + "a";
                auto v = vm2.Find(key);
              }
            },
            "KDB Merged Failed Find", "reads/ms");
      }
      if (opts.contains('i')) {
        Bench(
            kCount,
            [&]() {
              volatile size_t sink = 0;
              for (LSMView::Iterator it = vm2.Begin(); it.IsValid(); ++it) {
                std::string key = it.Key();
                std::string value = it.Value();
                sink += key.size() + value.size();
              }
            },
            "KDB Full Scan after merge", "reads/ms");
      }
    }
    std::filesystem::remove_all(path);
  }
}