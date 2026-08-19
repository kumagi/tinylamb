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
// stream (Insert / Delete / Verify) so libFuzzer can steer the exact key bytes
// and operation interleaving toward split / slot-array edge cases instead of
// guessing a PRNG seed.
void Try(const uint8_t* data, size_t size, bool verbose) {
  ByteStream stream(data, size);
  std::string db_name = RandomString();
  std::string log_name = db_name + ".log";
  PageManager page_manager(db_name + ".db", 20);
  Logger logger(log_name);
  LockManager lm;
  RecoveryManager rm(log_name, page_manager.GetPool());
  TransactionManager tm(&lm, &page_manager, &logger, &rm);
  Transaction txn = tm.Begin();
  PageRef page = page_manager.AllocateNewPage(txn, PageType::kLeafPage);
  std::map<std::string, std::string> kvp;
  constexpr size_t kMaxOps = 300;
  for (size_t op = 0; op < kMaxOps && stream.Remaining(); ++op) {
    switch (stream.Pick(3)) {
      case 0: {  // Insert
        std::string key(stream.Bytes(stream.Pick(256)));
        std::string value(stream.Bytes(stream.Pick(256)));
        if (verbose) {
          LOG(TRACE) << "Insert: " << key << " : " << value;
        }
        if (page->InsertLeaf(txn, key, value) == Status::kSuccess) {
          kvp[key] = value;
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
          kvp.erase(key);
        }
        break;
      }
      default: {  // Verify the whole model against the page.
        for (const auto& [key, value] : kvp) {
          ASSIGN_OR_CRASH(std::string_view, val, page->Read(txn, key));
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

#endif  // TINYLAMB_LEAF_PAGE_FUZZER_HPP
