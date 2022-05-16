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
  return sc_.GenerateKey(row);
}

Encoder& operator<<(Encoder& a, const Index& idx) {
  a << idx.sc_ << idx.pid_;
  return a;
}

Decoder& operator>>(Decoder& e, Index& idx) {
  e >> idx.sc_ >> idx.pid_;
  return e;
}

}  // namespace tinylamb