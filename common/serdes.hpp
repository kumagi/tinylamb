//
// Created by kumagi on 2022/01/23.
//

#ifndef TINYLAMB_SERDES_HPP
#define TINYLAMB_SERDES_HPP

#include <cstdint>
#include <string_view>

#include "common/constants.hpp"

namespace tinylamb {

size_t SerializeStringView(char* pos, std::string_view bin);
size_t SerializeSlot(char* pos, slot_t slot);
size_t SerializePID(char* pos, page_id_t pid);
size_t SerializeSize(std::string_view bin);
size_t SerializeInteger(char* pos, int64_t i);
size_t SerializeDouble(char* pos, double d);

size_t DeserializeStringView(const char* pos, std::string_view* out);
size_t DeserializeSlot(const char* pos, slot_t* slot);
size_t DeserializePID(const char* pos, page_id_t* out);
size_t DeserializeInteger(const char* pos, int64_t* out);
size_t DeserializeDouble(const char* pos, double* out);

}  // namespace tinylamb

#endif  // TINYLAMB_SERDES_HPP
