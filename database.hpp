#ifndef TINYLAMB_DATABASE_HPP
#define TINYLAMB_DATABASE_HPP

#include <functional>

#include "page/page_manager.hpp"
#include "recovery/logger.hpp"
#include "type/catalog.hpp"

namespace tinylamb {

class Database {
 public:
  Database(std::string_view dbname)
      : dbname_(dbname),
        logger_(dbname_ + ".log"),
        pm_(dbname_, 1024),
        catalog_(&pm_) {}

 private:
  std::string dbname_;
  Logger logger_;
  PageManager pm_;
  Catalog catalog_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_DATABASE_HPP
