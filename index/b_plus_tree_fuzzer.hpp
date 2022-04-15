//
// Created by kumagi on 2022/04/10.
//

#ifndef TINYLAMB_B_PLUS_TREE_FUZZER_HPP
#define TINYLAMB_B_PLUS_TREE_FUZZER_HPP

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <random>

#include "index/b_plus_tree.hpp"
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
  const uint32_t count = rand() % 200;
  std::string db_name = RandomString();
  std::string log_name = db_name + ".log";
  PageManager page_manager(db_name + ".db", 20);
  Logger logger(log_name);
  LockManager lm;
  RecoveryManager rm(log_name, page_manager.GetPool());
  TransactionManager tm(&lm, &logger, &rm);
  page_id_t root;
  {
    auto txn = tm.Begin();
    PageRef page = page_manager.AllocateNewPage(txn, PageType::kLeafPage);
    root = page->PageID();
    assert(txn.PreCommit() == Status::kSuccess);
  }
  BPlusTree bpt(root, &page_manager);
  Transaction txn = tm.Begin();
  std::unordered_map<std::string, std::string> kvp;
  kvp.reserve(count);
  for (uint32_t i = 0; i < count; ++i) {
    std::string key = RandomString((19937 * i) % 120 + 10);
    std::string value = RandomString((19937 * i) % 120 + 10);
    if (verbose) {
      LOG(TRACE) << "Insert: " << key << " : " << value;
    }
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
  for (uint32_t i = 0; i < count * 4; ++i) {
    auto iter = kvp.begin();
    std::advance(iter, (i * 19937) % kvp.size());
    if (verbose) {
      LOG(TRACE) << "Delete: " << iter->first << " : " << iter->second;
    }
    assert(bpt.Delete(txn, iter->first) == Status::kSuccess);
    assert(bpt.SanityCheckForTest(&page_manager));
    if (verbose) {
      bpt.Dump(txn, std::cerr, 0);
      std::cerr << "\n";
    }
    kvp.erase(iter);

    for (const auto& kv : kvp) {
      std::string_view val;
      bpt.Read(txn, kv.first, &val);
      assert(kvp[kv.first] == val);
    }

    std::string key = RandomString((19937 * i) % 130 + 1000);
    std::string value = RandomString((19937 * i) % 320 + 5000);
    if (verbose) {
      LOG(TRACE) << "Insert: " << key << " : " << value;
    }
    assert(bpt.Insert(txn, key, value));
    assert(bpt.SanityCheckForTest(&page_manager));
    if (verbose) {
      bpt.Dump(txn, std::cerr, 0);
      std::cerr << "\n";
    }
    kvp[key] = value;
    for (const auto& kv : kvp) {
      std::string_view val;
      bpt.Read(txn, kv.first, &val);
      assert(kvp[kv.first] == val);
    }
  }
  for (const auto& kv : kvp) {
    std::string_view val;
    assert(bpt.Read(txn, kv.first, &val) == Status::kSuccess);
    assert(kvp[kv.first] == val);
    assert(bpt.Delete(txn, kv.first) == Status::kSuccess);
  }
  std::remove((db_name + ".db").c_str());
  std::remove(log_name.c_str());
}

}  // namespace tinylamb

#endif  // TINYLAMB_B_PLUS_TREE_FUZZER_HPP