//
// Created by kumagi on 2019/09/04.
//

#ifndef PEDASOS_DATABASE_HPP
#define PEDASOS_DATABASE_HPP

#include "Operation.hpp"
#include "PagePool.hpp"
#include <functional>
namespace pedasos {

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
};

}  // namespace pedasos

#endif // PEDASOS_DATABASE_HPP
