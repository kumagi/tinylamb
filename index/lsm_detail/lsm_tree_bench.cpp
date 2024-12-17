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
#include <leveldb/db.h>
#include <leveldb/iterator.h>
#include <leveldb/status.h>
// #include <rocksdb/db.h>
// #include <rocksdb/metadata.h>
// #include <rocksdb/options.h>

#include <cassert>
#include <chrono>
#include <cstddef>
#include <filesystem>
#include <functional>
#include <iomanip>
#include <iostream>
#include <memory>
#include <random>
#include <set>
#include <string>
#include <string_view>

#include "common/constants.hpp"
#include "common/log_message.hpp"
#include "common/random_string.hpp"
#include "index/lsm_tree.hpp"
#include "lsm_view.hpp"

using namespace tinylamb;

static void Bench(size_t count, const std::function<void()>& fun,
                  std::string_view name, std::string_view unit) {
  auto begin = std::chrono::system_clock::now();
  fun();
  auto finish = std::chrono::system_clock::now();
  int us = std::chrono::duration_cast<std::chrono::milliseconds>(finish - begin)
               .count();
  std::cout << std::setw(25) << name << ": " << (double)count / us << " "
            << unit << "\n";
}

int main(int argc, char** argv) {
  std::set<char> opts;
  for (int i = 1; i < argc; ++i) {
    std::string v(argv[i]);
    for (const char& j : v) {
      opts.emplace(j);
    }
  }
  std::random_device rd;
  std::mt19937 random(rd());
  size_t kCount = 500000;

  if (opts.contains('l')) {
    std::filesystem::path ldb_path = "leveldb-" + RandomString() + "-";
    leveldb::DB* ldb = nullptr;
    std::unique_ptr<leveldb::DB> ptr;
    leveldb::Options options;
    options.create_if_missing = true;
    leveldb::Status status = leveldb::DB::Open(options, ldb_path.c_str(), &ldb);
    ptr.reset(ldb);

    if (!status.ok()) {
      LOG(FATAL) << "Unable to open/create test database " << ldb_path << " : "
                 << status.ToString();
      return -1;
    }
    Bench(
        kCount,
        [&]() {
          leveldb::WriteOptions wo;
          for (size_t i = 0; i < kCount; ++i) {
            ldb->Put(wo, std::to_string(i), std::to_string(i));
          }
        },
        "LDB Write", "writes/ms");
    if (opts.contains('s')) {
      Bench(
          kCount,
          [&]() {
            leveldb::ReadOptions ro;
            for (size_t i = 0; i < kCount; ++i) {
              std::string val;
              std::string key = std::to_string(random() % kCount);
              auto v = ldb->Get(ro, key, &val);
            }
          },
          "LDB Success Find", "reads/ms");
    }
    if (opts.contains('f')) {
      Bench(
          kCount,
          [&]() {
            leveldb::ReadOptions ro;
            for (size_t i = 0; i < kCount; ++i) {
              std::string val;
              std::string key = std::to_string(random() % kCount) + "k";
              volatile auto v = ldb->Get(ro, key, &val);
            }
          },
          "LDB Failed Find", "reads/ms");
    }
    if (opts.contains('i')) {
      Bench(
          kCount,
          [&]() {
            leveldb::ReadOptions ro;
            leveldb::Iterator* it = ldb->NewIterator(ro);
            for (it->SeekToFirst(); it->Valid(); it->Next()) {
              volatile std::string key = it->key().ToString();
              volatile std::string value = it->value().ToString();
            }
            assert(it->status().ok());
            delete it;
          },
          "LDB Full Scan", "reads/ms");
    }
    std::filesystem::remove_all(ldb_path);
  }

  if (opts.contains('k')) {
    std::filesystem::path path = "tmp_blob_file_test-" + RandomString();
    std::filesystem::create_directory(path);
    std::string blob_path = path / "blob.db";
    LSMTree tree(blob_path);
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
            for (LSMView::Iterator it = vm.Begin(); it.IsValid(); ++it) {
              volatile std::string key = it.Key();
              volatile std::string value = it.Value();
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
              for (LSMView::Iterator it = vm2.Begin(); it.IsValid(); ++it) {
                volatile std::string key = it.Key();
                volatile std::string value = it.Value();
              }
            },
            "KDB Full Scan after merge", "reads/ms");
      }
    }
    std::filesystem::remove_all(path);
  }
}