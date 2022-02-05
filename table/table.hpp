//
// Created by kumagi on 2022/02/06.
//

#ifndef TINYLAMB_TABLE_HPP
#define TINYLAMB_TABLE_HPP

#include "type/schema.hpp"

namespace tinylamb {

class Table {
  Schema schema_;
  page_id_t pid_{};
};

}  // namespace tinylamb

#endif  // TINYLAMB_TABLE_HPP
