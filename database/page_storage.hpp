//
// Created by kumagi on 22/05/04.
//

#ifndef TINYLAMB_PAGE_STORAGE_HPP
#define TINYLAMB_PAGE_STORAGE_HPP

#include "database/transaction_context.hpp"
#include "page/page_manager.hpp"
#include "recovery/checkpoint_manager.hpp"
#include "recovery/logger.hpp"
#include "transaction/lock_manager.hpp"
#include "transaction/transaction_manager.hpp"

namespace tinylamb {

class PageStorage {
 public:
  explicit PageStorage(std::string_view dbname);

  Transaction Begin();
  std::string DBName() const;
  std::string LogName() const;
  std::string MasterRecordName() const;

  void LostAllPageForTest();

 private:
  friend class RelationStorage;
  friend class Database;

  std::string dbname_;
  LockManager lm_;
  Logger logger_;
  PageManager pm_;
  RecoveryManager rm_;
  TransactionManager tm_;
  CheckpointManager cm_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_PAGE_STORAGE_HPP
