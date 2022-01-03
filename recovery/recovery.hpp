#ifndef TINYLAMB_RECOVERY_HPP
#define TINYLAMB_RECOVERY_HPP

#include <string>
#include <string_view>

namespace tinylamb {

class LogRecord;
class Page;
class PageManager;
class PagePool;
class Transaction;
class TransactionManager;

class Recovery {
 public:
  Recovery(std::string_view log_path, std::string_view db_path, PagePool* pp);
  void StartFrom(size_t offset, TransactionManager* tm);
  bool ReadLog(uint64_t lsn, LogRecord* dst);

  void LogRedo(uint64_t lsn, const LogRecord& log, TransactionManager* tm);
  void LogUndo(uint64_t lsn, const LogRecord& log, TransactionManager* tm);

 private:
  void RefreshMap();

 private:
  std::string log_name_;
  char* log_data_;
  PagePool* pool_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_RECOVERY_HPP
