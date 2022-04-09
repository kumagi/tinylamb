//
// Created by kumagi on 2022/04/10.
//

#ifndef TINYLAMB_B_PLUS_TREE_FUZZER_HPP
#define TINYLAMB_B_PLUS_TREE_FUZZER_HPP

#include <filesystem>

#include <cstddef>
#include <cstdint>
#include <iostream>
#include <random>

#include "type/value.hpp"
#include "transaction/transaction.hpp"
#include "page/page_manager.hpp"
#include "recovery/logger.hpp"
#include "transaction/lock_manager.hpp"
#include "index/b_plus_tree.hpp"
#include "transaction/transaction_manager.hpp"

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
  const int count = rand() % 200;
  std::string dbname = RandomString();
  std::string logname = dbname + ".log";
  PageManager page_manager(dbname + ".db", 20);
  Logger logger(logname);
  LockManager lm;
  RecoveryManager rm(logname, page_manager.GetPool());
  TransactionManager tm(&lm, &logger, &rm);
  BPlusTree bpt(1, &page_manager);

  {
    auto txn = tm.Begin();
    PageRef page_ = page_manager.AllocateNewPage(txn, PageType::kLeafPage);
    assert(txn.PreCommit() == Status::kSuccess);
  }
  Transaction txn = tm.Begin();
  std::unordered_map<std::string, std::string> kvp;
  kvp.reserve(count);
  for (int i = 0; i < count; ++i) {
    std::string key = RandomString((19937 * i) % 120 + 10);
    std::string value = RandomString((19937 * i) % 120 + 10);
    assert(bpt.Insert(txn, key, value) == Status::kSuccess);
    std::string_view val;
    assert(bpt.Read(txn, key, &val) == Status::kSuccess);
    assert(val == value);
    assert(bpt.SanityCheckForTest(&page_manager));
    kvp[key] = value;
  }
  for (const auto& kv : kvp) {
    std::string_view val;
    bpt.Read(txn, kv.first, &val);
    assert(kvp[kv.first] == val);
  }
  for (int i = 0; i < count * 4; ++i) {
    auto iter = kvp.begin();
    std::advance(iter, (i * 19937) % kvp.size());
    assert(bpt.Delete(txn, iter->first) == Status::kSuccess);
    assert(bpt.SanityCheckForTest(&page_manager));
    kvp.erase(iter);

    std::string key = RandomString((19937 * i) % 130 + 1000);
    std::string value = RandomString((19937 * i) % 320 + 5000);
    assert(bpt.Insert(txn, key, value));
    assert(bpt.SanityCheckForTest(&page_manager));
    kvp[key] = value;
  }
  for (const auto& kv : kvp) {
    std::string_view val;
    assert(bpt.Read(txn, kv.first, &val) == Status::kSuccess);
    assert(kvp[kv.first] == val);
    assert(bpt.Delete(txn, kv.first) == Status::kSuccess);
  }
  std::remove((dbname + ".db").c_str());
  std::remove(logname.c_str());
}
}  // namespace tinylamb

#endif  // TINYLAMB_B_PLUS_TREE_FUZZER_HPP
