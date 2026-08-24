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

#ifndef TINYLAMB_SERDES_HPP
#define TINYLAMB_SERDES_HPP

#include <cstdint>
#include <sstream>
#include <string_view>
#include <vector>

#include "common/constants.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

// Version-1 on-disk layout. All fixed-width scalars are big-endian and every
// top-level page/WAL object carries this magic/version pair. Version 0 is
// intentionally unsupported: the v1 migration is a destructive format bump.
inline constexpr uint32_t kSerdesMagic = 0x54594231U;  // "TYB1"
inline constexpr uint32_t kSerdesVersion = 1U;

size_t SerializeU16(char* pos, uint16_t value);
size_t SerializeU32(char* pos, uint32_t value);
size_t SerializeU64(char* pos, uint64_t value);
size_t DeserializeU16(const char* pos, uint16_t* out);
size_t DeserializeU32(const char* pos, uint32_t* out);
size_t DeserializeU64(const char* pos, uint64_t* out);

// Contract: the caller must guarantee that `pos` holds at least
// SerializeSize(bin) writable/readable bytes. DeserializeStringView trusts the
// stored length prefix and cannot bounds-check without an end pointer; page
// images are validated (checksummed) before deserialization.
size_t SerializeStringView(char* pos, std::string_view bin);
size_t SerializeSlot(char* pos, slot_t slot);
size_t SerializePID(char* pos, page_id_t pid);
size_t SerializeSize(std::string_view bin);
size_t SerializeNull(char* pos);
size_t SerializeInteger(char* pos, int64_t i);
size_t SerializeDouble(char* pos, double d);

size_t DeserializeStringView(const char* pos, std::string_view* out);
// Bounds-checked variant for untrusted input: returns 0 (and leaves *out
// untouched) when the stored length would run past `end`.
size_t DeserializeStringView(const char* pos, const char* end,
                             std::string_view* out);
size_t DeserializeString(std::istream& in, std::string* out);
size_t DeserializeSlot(const char* pos, slot_t* out);
size_t DeserializePID(const char* pos, page_id_t* out);
size_t DeserializeInteger(const char* pos, int64_t* out);
size_t DeserializeDouble(const char* pos, double* out);

}  // namespace tinylamb

#endif  // TINYLAMB_SERDES_HPP
