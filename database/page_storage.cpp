//
// Created by kumagi on 22/05/04.
//

#include "page_storage.hpp"

namespace tinylamb {

PageStorage::PageStorage(std::string_view dbname)
    : dbname_(dbname),
      logger_(LogName()),
      pm_(DBName(), 1024),
      rm_(LogName(), pm_.GetPool()),
      tm_(&lm_, &pm_, &logger_, &rm_),
      cm_(MasterRecordName(), &tm_, pm_.GetPool()) {
  rm_.RecoverFrom(0, &tm_);
}

void PageStorage::LostAllPageForTest() { pm_.GetPool()->LostAllPageForTest(); }

Transaction PageStorage::Begin() { return tm_.Begin(); }

std::string PageStorage::DBName() const { return dbname_ + ".db"; }
std::string PageStorage::LogName() const { return dbname_ + ".log"; }
std::string PageStorage::MasterRecordName() const {
  return dbname_ + ".last_checkpoint";
}

}  // namespace tinylamb