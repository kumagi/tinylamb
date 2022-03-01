//
// Created by kumagi on 2019/09/04.
//

#include "database.hpp"

namespace tinylamb {

TransactionContext Database::Begin() {
  return TransactionContext(&tm_, &pm_, &catalog_);
}

}  // namespace tinylamb