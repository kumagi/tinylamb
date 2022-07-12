//
// Created by kumagi on 22/05/05.
//

#include "index_schema.hpp"

#include <sstream>

#include "common/decoder.hpp"
#include "common/encoder.hpp"
#include "type/row.hpp"

namespace tinylamb {

std::ostream& operator<<(std::ostream& o, const IndexMode& mode) {
  o << (mode == IndexMode::kUnique ? "Unique" : "NonUnique");
  return o;
}

std::string IndexSchema::GenerateKey(const Row& row) const {
  std::stringstream s;
  for (const auto& k : key_) {
    s << row[k].EncodeMemcomparableFormat();
  }
  return s.str();
}

Encoder& operator<<(Encoder& a, const IndexSchema& idx) {
  a << idx.name_ << idx.key_ << idx.include_ << (bool)idx.mode_;
  return a;
}

Decoder& operator>>(Decoder& e, IndexSchema& idx) {
  e >> idx.name_ >> idx.key_ >> idx.include_ >> (bool&)idx.mode_;
  return e;
}

std::ostream& operator<<(std::ostream& o, const IndexSchema& rhs) {
  o << rhs.name_ << " => [ Column: {";
  for (size_t i = 0; i < rhs.key_.size(); ++i) {
    if (0 < i) {
      o << ", ";
    }
    o << rhs.key_[i];
  }
  o << "}";
  if (!rhs.include_.empty()) {
    o << " Include: {";
    for (size_t i = 0; i < rhs.include_.size(); ++i) {
      if (0 < i) {
        o << ", ";
      }
      o << rhs.include_[i];
    }
    o << "}";
  }
  o << " " << rhs.mode_ << "]";
  return o;
}

}  // namespace tinylamb