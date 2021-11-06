#ifndef TINYLAMB_RECOVERY_HPP
#define TINYLAMB_RECOVERY_HPP

#include <string>
#include <string_view>

namespace tinylamb {

class LogRecord;
class PageManager;
class PagePool;
class Transaction;
class TransactionManager;

class Recovery {
 public:
  Recovery(std::string_view log_path, std::string_view db_path,
           PageManager* pm, TransactionManager* tm);
  void StartFrom(size_t offset);

 private:
  void LogRedo(const LogRecord& log);
  void LogUndo(const LogRecord& log);

 private:
  std::string log_name_;
  char* log_data_;
  PagePool* pool_;
  TransactionManager* tm_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_RECOVERY_HPP
