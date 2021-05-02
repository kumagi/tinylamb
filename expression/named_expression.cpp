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