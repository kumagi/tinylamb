//
// Created by kumagi on 2022/02/21.
//

#include "projection.hpp"

#include "type/row.hpp"
#include "type/value.hpp"

namespace tinylamb {

bool Projection::Next(Row* dst) {
  Row orig;
  dst->Clear();
  if (!src_->Next(&orig)) return false;
  std::vector<Value> extracted;
  extracted.resize(offsets_.size());
  for (unsigned long offset : offsets_) {
    extracted.push_back(orig[offset]);
  }
  *dst = Row(extracted);
  return true;
}

}  // namespace tinylamb