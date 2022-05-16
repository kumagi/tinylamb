//
// Created by kumagi on 22/05/05.
//

#include "index_schema.hpp"

#include <sstream>

#include "common/decoder.hpp"
#include "common/encoder.hpp"
#include "type/row.hpp"

namespace tinylamb {

std::string IndexSchema::GenerateKey(const Row& row) const {
  std::stringstream s;
  for (const auto& k : key_) {
    s << row[k].EncodeMemcomparableFormat();
  }
  return s.str();
}

Encoder& operator<<(Encoder& a, const IndexSchema& idx) {
  a << idx.name_ << idx.key_;
  return a;
}

Decoder& operator>>(Decoder& e, IndexSchema& idx) {
  e >> idx.name_ >> idx.key_;
  return e;
}

}  // namespace tinylamb