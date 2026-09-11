/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "materialize_plan.hpp"

#include <ostream>

#include "common/constants.hpp"

namespace tinylamb {

void MaterializePlan::Dump(std::ostream& output, int indent) const {
  output << Indent(static_cast<size_t>(indent)) << "Materialize\n";
  child_->Dump(output, indent + 2);
}

}  // namespace tinylamb
