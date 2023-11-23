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
// Created by kumagi on 22/07/10.
//

#include "expression/named_expression.hpp"

#include <ostream>

namespace tinylamb {

std::ostream& operator<<(std::ostream& o, const NamedExpression& ne) {
  o << *ne.expression;
  if (!ne.name.empty() && ne.name != ne.expression->ToString()) {
    o << " AS " << ne.name;
  }
  return o;
}

}  // namespace tinylamb