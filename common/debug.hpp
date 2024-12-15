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

#ifndef TINYLAMB_DEBUG_HPP
#define TINYLAMB_DEBUG_HPP

#include <string>
#include <string_view>

namespace tinylamb {

std::string Hex(std::string_view in);

std::string OmittedString(std::string_view original, int length);

std::string HeadString(std::string_view original, int length);

}  // namespace tinylamb

#endif  // TINYLAMB_DEBUG_HPP
