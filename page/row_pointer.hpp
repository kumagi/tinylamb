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

//
// Created by kumagi on 22/08/05.
//

#ifndef TINYLAMB_ROW_POINTER_HPP
#define TINYLAMB_ROW_POINTER_HPP

#include "common/constants.hpp"
namespace tinylamb {

struct RowPointer {
  // Row start position from beginning fom this page.
  bin_size_t offset = 0;

  // Physical row size in bytes (required to get exact size for logging).
  bin_size_t size = 0;

  bool operator==(const RowPointer&) const = default;
  friend std::ostream& operator<<(std::ostream& o, const RowPointer& rp) {
    o << "{" << rp.offset << ", " << rp.size << "}";
    return o;
  }
};

constexpr static RowPointer kMinusInfinity{1, 0};
constexpr static RowPointer kPlusInfinity{2, 0};
}  // namespace tinylamb

#endif  // TINYLAMB_ROW_POINTER_HPP
