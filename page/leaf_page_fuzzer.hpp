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

//
// Created by kumagi on 22/09/22.
//

#ifndef TINYLAMB_LEAF_PAGE_FUZZER_HPP
#define TINYLAMB_LEAF_PAGE_FUZZER_HPP

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <random>

#include "page/leaf_page.hpp"
#include "page/page_manager.hpp"
#include "recovery/logger.hpp"
#include "transaction/lock_manager.hpp"
#include "transaction/transaction.hpp"
#include "transaction/transaction_manager.hpp"
#include "type/value.hpp"

namespace tinylamb {

void Try(uint64_t seed, bool verbose) {
  std::mt19937_64 rand(seed);
  auto RandomString = [&](size_t len = 16) {
    static const char alphanum[] =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";
    std::string ret;
    ret.reserve(len);
    for (size_t i = 0; i < len; ++i) {
      ret.push_back(alphanum[rand() % (sizeof(alphanum) - 1)]);
    }
    return ret;
  };
  const uint32_t count = rand() % 200 + 200;
  std::string db_name = RandomString();
  std::string log_name = db_name + ".log";
  PageManager page_manager(db_name + ".db", 20);
  Logger logger(log_name);
  LockManager lm;
  RecoveryManager rm(log_name, page_manager.GetPool());
  TransactionManager tm(&lm, &page_manager, &logger, &rm);
  Transaction txn = tm.Begin();
  PageRef page = page_manager.AllocateNewPage(txn, PageType::kLeafPage);
  std::unordered_map<std::string, std::string> kvp;
  kvp.reserve(count);
  for (uint32_t i = 0; i < count; ++i) {
    std::string key = RandomString(rand() % 3000 + 10);
    std::string value = RandomString(rand() % 2000 + 10);
    if (verbose) {
      LOG(TRACE) << "Insert: " << key << " : " << value;
    }
    if (page->InsertLeaf(txn, key, value) == Status::kSuccess) {
      kvp.emplace(key, value);
      ASSIGN_OR_CRASH(std::string_view, val, page->Read(txn, key));
      assert(val == value);
    }
  }
  for (const auto& kv : kvp) {
    ASSIGN_OR_CRASH(std::string_view, val, page->Read(txn, kv.first));
    assert(kvp[kv.first] == val);
  }
  for (uint32_t i = 0; i < count * 4; ++i) {
    auto iter = kvp.begin();
    std::advance(iter, (i * 19937) % kvp.size());
    if (verbose) {
      LOG(TRACE) << "Delete: " << iter->first << " : " << iter->second;
    }
    assert(page->Delete(txn, iter->first) == Status::kSuccess);
    if (verbose) {
      std::cerr << *page << "\n";
    }
    kvp.erase(iter);

    for (const auto& kv : kvp) {
      ASSIGN_OR_CRASH(std::string_view, val, page->Read(txn, kv.first));
      if (kvp[kv.first] != val) {
        LOG(ERROR) << kvp[kv.first] << " vs " << val;
      }
      assert(kvp[kv.first] == val);
    }

    std::string key = RandomString(rand() % 3000 + 100);
    std::string value = RandomString(rand() % 1800 + 200);
    if (verbose) {
      LOG(TRACE) << "Insert: " << key << " : " << value;
    }
    if (page->InsertLeaf(txn, key, value) == Status::kSuccess) {
      kvp[key] = value;
      if (verbose) {
        std::cerr << *page << "\n";
      }
      for (const auto& kv : kvp) {
        ASSIGN_OR_CRASH(std::string_view, val, page->Read(txn, kv.first));
        if (kvp[kv.first] != val) {
          LOG(ERROR) << "GetKey: " << kv.first;
          LOG(ERROR) << kvp[kv.first] << " vs " << val;
        }
        assert(kvp[kv.first] == val);
      }
    }
  }
  for (const auto& kv : kvp) {
    ASSIGN_OR_CRASH(std::string_view, val, page->Read(txn, kv.first));
    assert(kvp[kv.first] == val);
    assert(page->Delete(txn, kv.first) == Status::kSuccess);
  }
  std::remove((db_name + ".db").c_str());
  std::remove(log_name.c_str());
}

}  // namespace tinylamb

#endif  // TINYLAMB_LEAF_PAGE_FUZZER_HPP
