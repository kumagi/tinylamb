//
// Created by kumagi on 2022/02/21.
//

#include "projection.hpp"

#include "common/log_message.hpp"
#include "type/row.hpp"
#include "type/value.hpp"

namespace tinylamb {

bool Projection::Next(Row* dst) {
  Row orig;
  dst->Clear();
  if (!src_->Next(&orig)) return false;
  std::vector<Value> extracted;
  extracted.reserve(offsets_.size());
  for (unsigned long offset : offsets_) {
    extracted.push_back(orig[offset]);
  }
  *dst = Row(std::move(extracted));
  return true;
}

}  // namespace tinylamb