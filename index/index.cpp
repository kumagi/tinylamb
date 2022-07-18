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

std::unordered_set<slot_t> Index::CoveredColumns() const {
  std::unordered_set<slot_t> ret;
  for (const auto& k : sc_.key_) {
    ret.emplace(k);
  }
  for (const auto& k : sc_.include_) {
    ret.emplace(k);
  }
  return ret;
}

Encoder& operator<<(Encoder& a, const Index& idx) {
  a << idx.sc_ << idx.pid_;
  return a;
}

Decoder& operator>>(Decoder& e, Index& idx) {
  e >> idx.sc_ >> idx.pid_;
  return e;
}

void Index::Dump(std::ostream& o) const {
  o << "Index: " << sc_ << " Root: " << pid_;
}

std::ostream& operator<<(std::ostream& o, const Index& rhs) {
  rhs.Dump(o);
  return o;
}

}  // namespace tinylamb