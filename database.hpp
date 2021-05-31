#ifndef TINYLAMB_DATABASE_HPP
#define TINYLAMB_DATABASE_HPP

#include <functional>

#include "catalog.hpp"
#include "logging.hpp"
#include "operation.hpp"
#include "page_manager.hpp"

namespace tinylamb {

class Database {
public:
    Database(std::string_view dbname)
      : dbname_(dbname), pm_(dbname_, 1024), catalog_(&pm_),
        logger_(dbname_ + ".log") {}

  void Transaction(const Operation& task);

private:
 std::string dbname_;
  PageManager pm_;
  Catalog catalog_;
  Logger logger_;

};

}  // namespace tinylamb

#endif // TINYLAMB_DATABASE_HPP
