#include "plan/plan.hpp"

namespace tinylamb {

std::ostream& operator<<(std::ostream& o, const PlanBase& p) {
  p.Dump(o, 0);
  return o;
}

}  // namespace tinylamb
