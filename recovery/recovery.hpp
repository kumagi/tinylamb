#ifndef TINYLAMB_RECOVERY_HPP
#define TINYLAMB_RECOVERY_HPP

#include <string>
#include <string_view>

namespace tinylamb {

class LogRecord;
class PageManager;
class PagePool;
class TransactionManager;

class Recovery {
 public:
  Recovery(std::string_view log_path, std::string_view db_path,
           PageManager* pm, TransactionManager* tm);
  void StartFrom(size_t offset);
  void LogRedo(const LogRecord& log);
  void LogUndo(const LogRecord& log);

  bool ParseLogRecord(std::string_view src, LogRecord* dst);

 private:
  std::string log_name_;
  std::string db_name_;
  char* log_data_;
  PagePool* pool_;
  TransactionManager* tm_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_RECOVERY_HPP
