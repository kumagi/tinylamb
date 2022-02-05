//
// Created by kumagi on 2022/02/06.
//

#ifndef TINYLAMB_INDEX_HPP
#define TINYLAMB_INDEX_HPP

#include <string>
#include <vector>

#include "common/constants.hpp"

namespace tinylamb {

class Index {
 public:
  std::string name_;
  std::vector<size_t> key;
  page_id_t pid;
};

}  // namespace tinylamb

#endif  // TINYLAMB_INDEX_HPP
