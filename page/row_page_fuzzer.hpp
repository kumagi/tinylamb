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

#ifndef TINYLAMB_ROW_PAGE_FUZZER_HPP
#define TINYLAMB_ROW_PAGE_FUZZER_HPP

#include <stdlib.h>

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <memory>
#include <string>
#include <string_view>

#include "common/constants.hpp"
#include "common/log_message.hpp"
#include "common/random_string.hpp"
#include "page/page_manager.hpp"
#include "page/page_ref.hpp"
#include "page/row_page.hpp"
#include "page_type.hpp"
#include "recovery/logger.hpp"
#include "transaction/lock_manager.hpp"
#include "transaction/transaction.hpp"
#include "transaction/transaction_manager.hpp"

namespace tinylamb {

class RowPageEnvironment {
 public:
  RowPageEnvironment() {
    std::string prefix = "checkpoint_test-" + RandomString();
    db_name_ = prefix + ".db";
    log_name_ = prefix + ".log";
    std::remove(db_name_.c_str());
    std::remove(log_name_.c_str());
    Recover();
    auto txn = tm_->Begin();
    PageRef page = p_->AllocateNewPage(txn, PageType::kRowPage);
    page_id_ = page->PageID();
    txn.PreCommit();
  }

  void Recover() {
    if (p_) {
      p_->GetPool()->DropAllPages();
    }
    tm_.reset();
    lm_.reset();
    l_.reset();
    p_.reset();
    p_ = std::make_unique<PageManager>(db_name_, 10);
    l_ = std::make_unique<Logger>(log_name_);
    r_ = std::make_unique<RecoveryManager>(log_name_, p_->GetPool());
    lm_ = std::make_unique<LockManager>();
    tm_ = std::make_unique<TransactionManager>(lm_.get(), p_.get(), l_.get(),
                                               r_.get());
  }

  ~RowPageEnvironment() {
    std::remove(db_name_.c_str());
    std::remove(log_name_.c_str());
  }

  std::string db_name_;
  std::string log_name_;
  std::unique_ptr<LockManager> lm_;
  std::unique_ptr<PageManager> p_;
  std::unique_ptr<Logger> l_;
  std::unique_ptr<RecoveryManager> r_;
  std::unique_ptr<TransactionManager> tm_;
  page_id_t page_id_ = 0;
};

class Operation {
 public:
  explicit Operation(RowPageEnvironment* env)
      : env_(env),
        page_(env_->p_->GetPage(env_->page_id_)),
        txn_(env_->tm_->Begin()) {}

  void StartTransaction() { txn_ = env_->tm_->Begin(); }

  size_t Execute(std::string_view input, bool verbose = false) {
    uint8_t operation = input[0];
    input.remove_prefix(1);
    switch (operation % 6) {
      case 0: {  // Read
        slot_t slot;
        if (input.size() < sizeof(slot)) {
          return 1 + input.size();
        }
        memcpy(&slot, input.data(), sizeof(slot_t));
        if (verbose) {
          LOG(TRACE) << "Read: " << slot;
        }
        page_->Read(txn_, slot);
        return 1 + sizeof(slot);
      }
      case 1: {  // InsertBranch
        bin_size_t size = 0;
        if (input.size() < sizeof(size)) {
          return 1 + input.size();
        }
        memcpy(&size, input.data(), sizeof(size));
        input.remove_prefix(sizeof(size));
        size = std::min(size, (bin_size_t)input.size());
        std::string_view str(input.data(), size);
        if (verbose) {
          LOG(TRACE) << "InsertBranch: " << str;
        }
        page_->Insert(txn_, str);
        return 1 + sizeof(size) + size;
      }
      case 2: {  // UpdateBranch
        slot_t slot;
        if (input.size() < sizeof(slot)) {
          return 1 + input.size();
        }
        memcpy(&slot, input.data(), sizeof(slot_t));
        input.remove_prefix(sizeof(slot));

        bin_size_t size;
        if (input.size() < sizeof(size)) {
          return 1 + sizeof(size) + input.size();
        }
        memcpy(&size, input.data(), sizeof(size));
        input.remove_prefix(sizeof(size));

        size = std::min(size, (bin_size_t)input.size());
        std::string_view str(input.data(), size);
        if (verbose) {
          LOG(TRACE) << "UpdateBranch: " << str << " at " << slot;
        }
        Status s = page_->Update(txn_, slot, str);
        if (verbose) {
          LOG(TRACE) << ToString(s);
        }
        return 1 + sizeof(slot) + sizeof(size) + size;
      }
      case 3: {  // Delete
        slot_t slot;
        if (input.size() < sizeof(slot)) {
          return 1 + input.size();
        }
        memcpy(&slot, input.data(), sizeof(slot_t));
        input.remove_prefix(sizeof(slot));
        if (verbose) {
          LOG(TRACE) << "Delete: " << slot;
        }
        page_->Delete(txn_, slot);
        return 1 + sizeof(slot);
      }
      case 4: {  // Commit
        if (verbose) {
          LOG(TRACE) << "Commit";
        }
        txn_.PreCommit();
        StartTransaction();
        return 1;
      }
      case 5: {  // Abort
        if (verbose) {
          LOG(TRACE) << "Commit";
        }
        page_.PageUnlock();
        txn_.Abort();
        StartTransaction();
        page_ = env_->p_->GetPage(env_->page_id_);
        return 1;
      }
      case 6: {  // Crash
        if (verbose) {
          LOG(TRACE) << "Crash";
        }
        page_.PageUnlock();
        env_->Recover();
        StartTransaction();
        page_ = env_->p_->GetPage(env_->page_id_);
        return 1;
      }
      default:
        LOG(ERROR) << "default: " << (int)operation;
        abort();
    }
  }

 private:
  RowPageEnvironment* env_;
  PageRef page_;
  Transaction txn_;
};

}  // namespace tinylamb
#endif  // TINYLAMB_ROW_PAGE_FUZZER_HPP
