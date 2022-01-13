#ifndef TINYLAMB_DATABASE_HPP
#define TINYLAMB_DATABASE_HPP

#include <functional>

#include "page/page_manager.hpp"
#include "recovery/checkpoint_manager.hpp"
#include "recovery/logger.hpp"
#include "transaction/lock_manager.hpp"
#include "transaction/transaction_manager.hpp"
#include "type/catalog.hpp"

namespace tinylamb {

class Database {
 public:
  explicit Database(std::string_view dbname)
      : dbname_(dbname),
        lock_manager_(),
        logger_(dbname_ + ".log"),
        pm_(dbname_ + ".db", 1024),
        recovery_(dbname_ + ".log", pm_.GetPool()),
        tm_(&lock_manager_, &logger_, &recovery_),
        cm_(dbname_ + ".last_checkpoint", &tm_, pm_.GetPool()) {}

 private:
  std::string dbname_;
  LockManager lock_manager_;
  Logger logger_;
  PageManager pm_;
  Recovery recovery_;
  TransactionManager tm_;
  CheckpointManager cm_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_DATABASE_HPP
