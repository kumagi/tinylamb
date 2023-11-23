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

#ifndef TINYLAMB_ROW_POSITION_HPP
#define TINYLAMB_ROW_POSITION_HPP

#include <cstring>
#include <ostream>

#include "common/constants.hpp"
#include "common/decoder.hpp"
#include "common/encoder.hpp"
#include "common/serdes.hpp"

namespace tinylamb {

struct RowPosition {
  // Returns invalid position.
  RowPosition() = default;
  RowPosition(page_id_t pid, slot_t sl) : page_id(pid), slot(sl) {}

  // The page where the row exists.
  page_id_t page_id = ~0LLU;

  // n-th row in the page.
  slot_t slot = ~0;

  [[nodiscard]] bool IsValid() const { return page_id != ~0LLU; }

  friend std::ostream& operator<<(std::ostream& o, const RowPosition& p) {
    o << "{" << p.page_id << ": " << p.slot << "}";
    return o;
  }
  bool operator==(const RowPosition& rhs) const = default;
  bool operator!=(const RowPosition& rhs) const = default;
  friend Encoder& operator<<(Encoder& a, const RowPosition& rp) {
    a << rp.page_id << rp.slot;
    return a;
  }
  friend Decoder& operator>>(Decoder& a, RowPosition& rp) {
    a >> rp.page_id >> rp.slot;
    return a;
  }

  [[nodiscard]] std::string Serialize() const {
    std::string s(Size(), '\0');
    memcpy(s.data(), &page_id, sizeof(page_id));
    memcpy(s.data() + sizeof(page_id), &slot, sizeof(slot));
    return s;
  }
  size_t Deserialize(const char* src) {
    const char* const original_offset = src;
    src += DeserializePID(src, &page_id);
    src += DeserializeSlot(src, &slot);
    return src - original_offset;
  }

  static constexpr size_t Size() { return sizeof(page_id) + sizeof(slot); }
};

}  // namespace tinylamb

namespace std {

template <>
class hash<tinylamb::RowPosition> {
 public:
  uint64_t operator()(const tinylamb::RowPosition& target) const {
    return std::hash<tinylamb::page_id_t>()(target.page_id) +
           std::hash<tinylamb::slot_t>()(target.slot);
  }
};

}  // namespace std

#endif  // TINYLAMB_ROW_POSITION_HPP
