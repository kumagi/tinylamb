//
// Created by kumagi on 2022/02/06.
//

#include "index.hpp"

#include <sstream>

#include "type/row.hpp"

namespace tinylamb {

std::string Index::GenerateKey(const Row& row) const {
  std::stringstream s;
  for (const auto& k : key_) {
    s << row[k].EncodeMemcomparableFormat();
  }
  return s.str();
}

}  // namespace tinylamb