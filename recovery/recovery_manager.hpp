#ifndef TINYLAMB_RECOVERY_MANAGER_HPP
#define TINYLAMB_RECOVERY_MANAGER_HPP

#include <string>
#include <string_view>

#include "common/constants.hpp"

namespace tinylamb {

class LogRecord;
class Page;
class PageManager;
class PagePool;
class PageRef;
class Transaction;
class TransactionManager;

class RecoveryManager {
 public:
  RecoveryManager(std::string_view log_path, PagePool* pp);

  void SinglePageRecovery(PageRef&& page, TransactionManager* tm);

  void RecoverFrom(lsn_t checkpoint_lsn, TransactionManager* tm);

  bool ReadLog(lsn_t lsn, LogRecord* dst);

  void LogUndoWithPage(lsn_t lsn, const LogRecord& log, TransactionManager* tm);

 private:
  void RefreshMap();

 private:
  std::string log_name_;
  char* log_data_;
  PagePool* pool_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_RECOVERY_MANAGER_HPP
