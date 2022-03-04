//
// Created by kumagi on 2022/02/06.
//

#include "index.hpp"

#include <sstream>

#include "common/decoder.hpp"
#include "common/encoder.hpp"
#include "type/row.hpp"

namespace tinylamb {

std::string Index::GenerateKey(const Row& row) const {
  std::stringstream s;
  for (const auto& k : key_) {
    s << row[k].EncodeMemcomparableFormat();
  }
  return s.str();
}

Encoder& operator<<(Encoder& a, const Index& idx) {
  a << idx.name_ << idx.key_ << idx.pid_;
  return a;
}

Decoder& operator>>(Decoder& e, Index& idx) {
  e >> idx.name_ >> idx.key_ >> idx.pid_;
  return e;
}
bool Index::operator==(const Index& rhs) const {
  return name_ == rhs.name_ && key_ == rhs.key_ && pid_ == rhs.pid_;
}

}  // namespace tinylamb