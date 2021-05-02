//
// Created by kumagi on 2022/02/07.
//

#ifndef TINYLAMB_DEBUG_HPP
#define TINYLAMB_DEBUG_HPP

#include <iomanip>
#include <iosfwd>
#include <sstream>

namespace tinylamb {

std::string Hex(std::string_view in);

std::string OmittedString(std::string_view original, int length);

std::string HeadString(std::string_view original, int length);

}  // namespace tinylamb

#endif  // TINYLAMB_DEBUG_HPP
