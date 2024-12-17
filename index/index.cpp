/**
 * Copyright 2023 KUMAZAKI Hiroki
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "index.hpp"

#include <ostream>
#include <string>
#include <unordered_set>

#include "common/constants.hpp"
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