//
// Created by kumagi on 2022/01/23.
//

#ifndef TINYLAMB_SERDES_HPP
#define TINYLAMB_SERDES_HPP

#include <cstdint>
#include <string_view>

#include "common/constants.hpp"

namespace tinylamb {

size_t Serialize(char* pos, std::string_view bin);
size_t SerializePID(char* pos, page_id_t pid);

std::string_view Deserialize(const char* pos);

}  // namespace tinylamb

#endif  // TINYLAMB_SERDES_HPP
