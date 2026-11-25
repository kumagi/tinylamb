import TinylambOpt.ResearchGoals.Atlas.JoinPlans
import TinylambOpt.ResearchProofs.Atlas_JoinPlans_ReplyCycle_Remaining

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_ReplyCycle_FractionalCap

open TinylambOpt.ResearchGoals.Atlas.JoinPlans.ReplyCycle

/-- The fractional edge-cover bound for the four-cycle reply pattern: the
    result count is at most the product of support counts raised to the cover
    weights. The full proof lives in the per-model `Remaining` module: two
    opposite-edge cardinality bounds, a weighted split of the exponent, and
    zero-support degeneracies. -/
theorem sound_ReplyCycle_FractionalCap :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.ReplyCycle.FractionalCap.Goal :=
  TinylambOpt.ResearchProofs.Atlas.JoinPlans.ReplyCycle.Remaining.sound_FractionalCap

end TinylambOpt.ResearchProofs.Atlas.Goal_ReplyCycle_FractionalCap
