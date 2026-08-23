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

#ifndef TINYLAMB_B_PLUS_TREE_FUZZER_HPP
#define TINYLAMB_B_PLUS_TREE_FUZZER_HPP

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <map>

#include "common/byte_stream.hpp"
#include "common/random_string.hpp"
#include "index/b_plus_tree.hpp"
#include "page/page_manager.hpp"
#include "recovery/logger.hpp"
#include "transaction/lock_manager.hpp"
#include "transaction/transaction.hpp"
#include "transaction/transaction_manager.hpp"
#include "type/value.hpp"

namespace tinylamb {

// Byte-driven B+ tree stress test.  The input directly encodes an operation
// stream (Insert / Delete / Read / Verify) with key and value bytes taken from
// the same buffer, so libFuzzer can evolve exact key bytes and interleavings
// that push page splits, foster children, and rebalances.
inline void Try(const uint8_t* data, size_t size, bool verbose) {
  RandomStringInitialize();
  ByteStream stream(data, size);
  std::string db_name = RandomString();
  std::string log_name = db_name + ".log";
  PageManager page_manager(db_name + ".db", 20);
  Logger logger(log_name);
  LockManager lm;
  RecoveryManager rm(log_name, page_manager.GetPool());
  TransactionManager tm(&lm, &page_manager, &logger, &rm);
  page_id_t root;
  {
    auto txn = tm.Begin();
    PageRef page = page_manager.AllocateNewPage(txn, PageType::kLeafPage);
    root = page->PageID();
    assert(txn.PreCommit() == Status::kSuccess);
  }
  BPlusTree bpt(root);
  Transaction txn = tm.Begin();
  std::map<std::string, std::string> kvp;
  constexpr size_t kMaxOps = 300;
  for (size_t op = 0; op < kMaxOps && stream.Remaining(); ++op) {
    switch (stream.Pick(4)) {
      case 0: {  // Insert
        std::string key(stream.Bytes(stream.Pick(256)));
        std::string value(stream.Bytes(stream.Pick(256)));
        if (verbose) {
          LOG(TRACE) << "Insert: " << key << " : " << value;
        }
        if (bpt.Insert(txn, key, value) == Status::kSuccess) {
          kvp[key] = value;
        }
        assert(bpt.SanityCheckForTest(&page_manager));
        break;
      }
      case 1: {  // Delete
        std::string key(stream.Bytes(stream.Pick(256)));
        if (verbose) {
          LOG(TRACE) << "Delete: " << key;
        }
        if (bpt.Delete(txn, key) == Status::kSuccess) {
          kvp.erase(key);
        }
        assert(bpt.SanityCheckForTest(&page_manager));
        break;
      }
      case 2: {  // Read a key from the model.
        if (!kvp.empty()) {
          auto iter = kvp.begin();
          std::advance(iter, stream.Pick(kvp.size()));
          ASSIGN_OR_CRASH(std::string_view, val, bpt.Read(txn, iter->first));
          assert(val == iter->second);
        }
        break;
      }
      default: {  // Verify the whole model against the tree.
        for (const auto& [key, value] : kvp) {
          ASSIGN_OR_CRASH(std::string_view, val, bpt.Read(txn, key));
          assert(val == value);
        }
        break;
      }
    }
  }
  std::remove((db_name + ".db").c_str());
  std::remove(log_name.c_str());
}

}  // namespace tinylamb

#endif  // TINYLAMB_B_PLUS_TREE_FUZZER_HPP
