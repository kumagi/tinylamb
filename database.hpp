#ifndef PEDASOS_DATABASE_HPP
#define PEDASOS_DATABASE_HPP

#include <functional>

#include "logger.hpp"
#include "operation.hpp"
#include "page_pool.hpp"

namespace pedasus {

class Database {
public:
  enum TransactionStatus {
    RUNNING,
    COMMIT,
    ABORT,
  };
  Database(std::string_view dbname) : root_(dbname, 1024) {}

  void Transaction(const Operation& task);

private:
  PagePool root_;
  logger logger_;
};

}  // namespace pedasus

#endif // PEDASOS_DATABASE_HPP
