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

#include "type/type.hpp"

#include <stdexcept>

namespace tinylamb {

size_t Type::Size() const {
  switch (type_) {
    case TypeTag::kInteger:
      return sizeof(int32_t);
    case TypeTag::kBigInt:
      return sizeof(int64_t);
    case TypeTag::kDouble:
      return sizeof(double);
    case TypeTag::kVarChar:
      return 0;
    default:
      throw std::runtime_error("Invalid type");
  }
}

std::string Type::ToString() const {
  switch (type_) {
    case TypeTag::kInteger:
      return "INTEGER";
    case TypeTag::kBigInt:
      return "BIGINT";
    case TypeTag::kDouble:
      return "DOUBLE";
    case TypeTag::kVarChar:
      return "VARCHAR";
    default:
      return "INVALID";
  }
}

}  // namespace tinylamb
