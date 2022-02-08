//
// Created by kumagi on 2022/02/06.
//

#ifndef TINYLAMB_INDEX_HPP
#define TINYLAMB_INDEX_HPP

#include <string>
#include <vector>

#include "common/constants.hpp"

namespace tinylamb {
class Row;
class Index {
 public:
  std::string GenerateKey(const Row& row) const;
  Index(std::string name, std::vector<size_t> key, page_id_t pid)
      : name_(std::move(name)), key_(std::move(key)), pid_(pid) {}
  std::string name_;
  std::vector<size_t> key_;
  page_id_t pid_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_INDEX_HPP
