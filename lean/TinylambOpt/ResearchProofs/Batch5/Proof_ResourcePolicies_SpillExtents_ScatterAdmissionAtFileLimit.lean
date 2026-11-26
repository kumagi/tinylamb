import TinylambOpt.ResearchGoals.ResourcePolicies.SpillExtents

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.SpillExtents.Goal_ScatterAdmissionAtFileLimit

open TinylambOpt.ResearchGoals.ResourcePolicies.SpillExtents


/-! Scratch proof: at the file quota, reusable holes plus unused quota are
    exactly the number of new blocks a scatter-write may install. The stated
    admission test is therefore equivalent to the remaining-capacity test even
    with Nat's truncated subtraction. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

open TinylambOpt.ResearchGoals.ResourcePolicies.SpillExtents

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.SpillExtents.ScatterAdmissionAtFileLimit.Goal := by
  intro holes requested used cap hnodup hmem hcap
  omega
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.SpillExtents.Goal_ScatterAdmissionAtFileLimit
