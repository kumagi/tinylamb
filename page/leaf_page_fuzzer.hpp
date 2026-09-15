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
#include <map>

#include "common/byte_stream.hpp"
#include "common/random_string.hpp"
#include "page/leaf_page.hpp"
#include "page/page_manager.hpp"
#include "recovery/logger.hpp"
#include "transaction/lock_manager.hpp"
#include "transaction/transaction.hpp"
#include "transaction/transaction_manager.hpp"
#include "type/value.hpp"

namespace tinylamb {

// Byte-driven leaf page stress test.  The input directly encodes an operation
// stream (Insert / Delete / Verify / Commit / Crash+Recover) so libFuzzer can
// steer the exact key bytes and operation interleaving toward split /
// slot-array edge cases AND the ARIES redo/undo path instead of guessing a
// PRNG seed.
void Try(const uint8_t* data, size_t size, bool verbose) {
  ByteStream stream(data, size);
  std::string db_name = RandomString();
  std::string log_name = db_name + ".log";
  std::remove(db_name.c_str());
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
  Transaction txn = tm->Begin();
  PageRef page =
      page_manager->AllocateNewPage(txn, PageType::kLeafPage).MoveValue();
  const page_id_t page_id = page->PageID();
  // `model` tracks what the page should contain right now; `committed` is the
  // same map as of the last successful PreCommit, i.e. what MUST survive a
  // crash after ARIES redo+undo.
  std::map<std::string, std::string> model;
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
        if (page->InsertLeaf(txn, key, value) == Status::kSuccess) {
          model[key] = value;
          ASSIGN_OR_CRASH(std::string_view, val, page->Read(txn, key));
          assert(val == value);
        }
        break;
      }
      case 1: {  // Delete
        std::string key(stream.Bytes(stream.Pick(256)));
        if (verbose) {
          LOG(TRACE) << "Delete: " << key;
        }
        if (page->Delete(txn, key) == Status::kSuccess) {
          model.erase(key);
        }
        break;
      }
      case 2:
      case 3: {  // Verify the whole model against the page.
        for (const auto& [key, value] : model) {
          ASSIGN_OR_CRASH(std::string_view, val, page->Read(txn, key));
          assert(val == value);
        }
        break;
      }
      case 4: {  // Commit and continue in a fresh transaction.
        if (verbose) {
          LOG(TRACE) << "Commit";
        }
        page.PageUnlock();
        const Status commit_status = txn.PreCommit();
        if (commit_status != Status::kSuccess) {
          LOG(FATAL) << "PreCommit failed: " << commit_status;
        }
        txn = tm->Begin();
        page = page_manager->GetPage(page_id).MoveValue();
        committed = model;
        break;
      }
      default: {  // Crash: drop the pool without write-back, run real ARIES.
        if (verbose) {
          LOG(TRACE) << "Crash";
        }
        // The live txn dies unfinished on purpose so recovery replays its
        // write set: redo must restore committed state, undo must erase
        // everything that was never committed.
        page.PageUnlock();
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
        page = page_manager->GetPage(page_id).MoveValue();
        assert(!page.IsNull());
        model = committed;
        for (const auto& [key, value] : model) {
          ASSIGN_OR_CRASH(std::string_view, val, page->Read(txn, key));
          assert(val == value);
        }
        break;
      }
    }
  }
  // End the transaction explicitly instead of leaving its fate to the
  // destructor ordering.  Drop the latch first: Abort's undo path latches
  // the touched pages itself and a held exclusive latch would self-deadlock.
  page.PageUnlock();
  txn.Abort();
  // Remove the files only after every RAII handle above has closed them.
  std::remove((db_name + ".db").c_str());
  std::remove(log_name.c_str());
}

}  // namespace tinylamb

#endif  // TINYLAMB_LEAF_PAGE_FUZZER_HPP
