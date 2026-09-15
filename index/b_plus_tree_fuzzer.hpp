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
#include "index/b_plus_tree_iterator.hpp"
#include "page/page_manager.hpp"
#include "recovery/logger.hpp"
#include "transaction/lock_manager.hpp"
#include "transaction/transaction.hpp"
#include "transaction/transaction_manager.hpp"
#include "type/value.hpp"

namespace tinylamb {

// Byte-driven B+ tree stress test.  The input directly encodes an operation
// stream (Insert / Delete / Read / Verify / Commit / Crash+Recover) with key
// and value bytes taken from the same buffer, so libFuzzer can evolve exact
// key bytes and interleavings that push page splits, foster children,
// rebalances, and the ARIES redo/undo path across tree topologies.
inline void Try(const uint8_t* data, size_t size, bool verbose) {
  RandomStringInitialize();
  ByteStream stream(data, size);
  std::string db_name = RandomString();
  std::string log_name = db_name + ".log";
  std::remove((db_name + ".db").c_str());
  std::remove(log_name.c_str());
  std::unique_ptr<LockManager> lm;
  std::unique_ptr<PageManager> page_manager;
  std::unique_ptr<Logger> logger;
  std::unique_ptr<RecoveryManager> rm;
  std::unique_ptr<TransactionManager> tm;
  auto Open = [&]() {
    page_manager = PageManager::Create(db_name + ".db", 20).MoveValue();
    logger = Logger::Create(log_name).MoveValue();
    rm = std::make_unique<RecoveryManager>(log_name, page_manager->GetPool());
    lm = std::make_unique<LockManager>();
    tm = std::make_unique<TransactionManager>(page_manager.get(), logger.get(),
                                              rm.get());
  };
  Open();
  page_id_t root;
  {
    auto boot_txn = tm->Begin();
    PageRef page = page_manager->AllocateNewPage(boot_txn, PageType::kLeafPage)
                       .MoveValue();
    root = page->PageID();
    assert(boot_txn.PreCommit() == Status::kSuccess);
  }
  BPlusTree bpt(root);
  Transaction txn = tm->Begin();
  // `kvp` is the live model; `committed` is its value at the last successful
  // PreCommit, i.e. exactly what MUST survive the next Crash.
  std::map<std::string, std::string> kvp;
  std::map<std::string, std::string> committed;
  constexpr size_t kMaxOps = 300;
  for (size_t op = 0; op < kMaxOps && stream.Remaining(); ++op) {
    switch (stream.Pick(6)) {
      case 0: {  // Insert
        std::string key(stream.Bytes(stream.Pick(256)));
        std::string value(stream.Bytes(stream.Pick(256)));
        if (verbose) {
          LOG(TRACE) << "Insert: " << key << " : " << value;
        }
        if (bpt.Insert(txn, key, value) == Status::kSuccess) {
          kvp[key] = value;
        }
        assert(bpt.SanityCheckForTest(page_manager.get()));
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
        assert(bpt.SanityCheckForTest(page_manager.get()));
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
      case 3: {  // Verify the whole model against the tree.
        for (const auto& [key, value] : kvp) {
          ASSIGN_OR_CRASH(std::string_view, val, bpt.Read(txn, key));
          assert(val == value);
        }
        // Full ascending scan must enumerate exactly the model in order.
        size_t model_idx = 0;
        for (auto it = bpt.Begin(txn); it.IsValid(); ++it) {
          assert(model_idx < kvp.size());
          auto expect_it = kvp.begin();
          std::advance(expect_it, model_idx);
          assert(it.Key() == expect_it->first);
          assert(it.Value() == expect_it->second);
          ++model_idx;
        }
        assert(model_idx == kvp.size());
        break;
      }
      case 4: {  // Commit and continue in a fresh transaction.
        if (verbose) {
          LOG(TRACE) << "Commit";
        }
        assert(txn.PreCommit() == Status::kSuccess);
        txn = tm->Begin();
        committed = kvp;
        break;
      }
      default: {  // Crash: lose dirty pages, then run the real ARIES pass.
        if (verbose) {
          LOG(TRACE) << "Crash";
        }
        // The live txn is abandoned unfinished so recovery sees a loser:
        // redo must restore every committed split/insert/delete, undo must
        // erase the uncommitted tail of the operation stream.
        page_manager->GetPool()->DropAllPages();
        tm.reset();
        lm.reset();
        rm.reset();
        logger.reset();
        page_manager.reset();
        Open();
        const Status recovery_status = rm->RecoverFrom(0, tm.get());
        if (recovery_status != Status::kSuccess) {
          LOG(FATAL) << "RecoverFrom failed: " << recovery_status;
        }
        txn = tm->Begin();
        bpt = BPlusTree(root);
        kvp = committed;
        assert(bpt.SanityCheckForTest(page_manager.get()));
        for (const auto& [key, value] : kvp) {
          ASSIGN_OR_CRASH(std::string_view, val, bpt.Read(txn, key));
          assert(val == value);
        }
        break;
      }
    }
  }
  txn.Abort();
  std::remove((db_name + ".db").c_str());
  std::remove(log_name.c_str());
}

}  // namespace tinylamb

#endif  // TINYLAMB_B_PLUS_TREE_FUZZER_HPP
