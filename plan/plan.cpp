/**
 * Copyright 2024 KUMAZAKI Hiroki
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

#include "plan/plan.hpp"

#include <algorithm>
#include <cstddef>
#include <ostream>

namespace tinylamb {

void PlanBase::Explain(std::ostream& o, int indent) const {
  Dump(o, indent);
  const PlanExplainInfo& info = ExplainInfo();
  if (info.rule_name.empty() && info.physical_operator.empty() &&
      info.required_properties.empty() && info.provided_properties.empty() &&
      info.applied_rules.empty()) {
    return;
  }
  const std::string pad(static_cast<size_t>(std::max(indent, 0)), ' ');
  o << "\n"
    << pad << "Rule: " << info.rule_name << "\n"
    << pad << "Physical operator: " << info.physical_operator << "\n"
    << pad << "Required properties: " << info.required_properties << "\n"
    << pad << "Provided properties: " << info.provided_properties;
  if (!info.applied_rules.empty()) {
    o << "\n" << pad << "Applied rules: " << info.applied_rules;
  }
}

std::ostream& operator<<(std::ostream& o, const PlanBase& p) {
  p.Dump(o, 0);
  return o;
}

}  // namespace tinylamb
