/**
 * Copyright 2026 KUMAZAKI Hiroki
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

#ifndef TINYLAMB_TABLE_FUZZ_HELPERS_HPP
#define TINYLAMB_TABLE_FUZZ_HELPERS_HPP

// Shared helpers for the PAX/LSM storage fuzz targets.
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "type/column.hpp"
#include "type/column_name.hpp"
#include "type/constraint.hpp"
#include "type/schema.hpp"
#include "type/value_type.hpp"

namespace tinylamb::fuzz {

// Derive a valid Schema (1..4 columns, one PK) from one seed byte.
inline Schema MakeSchemaFromByte(uint8_t b) {
  const size_t ncols = 1 + (b & 0x3);
  const size_t key = (b >> 2) % ncols;
  std::vector<Column> cols;
  cols.reserve(ncols);
  for (size_t i = 0; i < ncols; ++i) {
    const uint8_t raw = (b >> (4 + (i % 2) * 2)) & 0x3;
    const auto vt = static_cast<ValueType>((raw % 3) + 1);
    if (i == key) {
      cols.emplace_back(ColumnName{"", "c" + std::to_string(i)}, vt,
                        Constraint{Constraint::kPrimaryKey});
    } else {
      cols.emplace_back(ColumnName{"", "c" + std::to_string(i)}, vt);
    }
  }
  return Schema{"f", std::move(cols), static_cast<uint16_t>(key), 1};
}

// Entry points for deterministic smoke testing and libFuzzer harnesses.
int FuzzRowCodecOneInput(const uint8_t* data, size_t size);
int FuzzMemcmpOneInput(const uint8_t* data, size_t size);
int FuzzPaxMutateOneInput(const uint8_t* data, size_t size);
int FuzzLsmMergeOneInput(const uint8_t* data, size_t size);
int FuzzSchemaOneInput(const uint8_t* data, size_t size);
int FuzzMemtableModelOneInput(const uint8_t* data, size_t size);
int FuzzChunkFooterOneInput(const uint8_t* data, size_t size);
int FuzzRegionSchemeOneInput(const uint8_t* data, size_t size);
int FuzzNullmapVarcharOneInput(const uint8_t* data, size_t size);
int FuzzFooterSectionsOneInput(const uint8_t* data, size_t size);
int FuzzEncodeAgreeOneInput(const uint8_t* data, size_t size);

}  // namespace tinylamb::fuzz

#endif  // TINYLAMB_TABLE_FUZZ_HELPERS_HPP
