import TinylambOpt.ResearchGoals.Atlas.JoinPlans
import TinylambOpt.ResearchProofs.Atlas_JoinPlans_CoLikes_Remaining

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_CoLikes_FractionalCap

open TinylambOpt.ResearchGoals.Atlas.JoinPlans

/-- The fractional edge-cover (AGM) bound for the CoLikes shape.  The support
    count of the five-way join, raised to `den`, is bounded by the product of
    the per-atom support counts raised to the cover weights.  The statement is
    true; the fully elaborated proof (indicator expansion, four integral cover
    bounds and the splitting of `den` over them) lives in the per-model
    `Remaining` module and is reused verbatim here. -/
theorem sound_CoLikes_FractionalCap :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.CoLikes.FractionalCap.Goal :=
  TinylambOpt.ResearchProofs.Atlas.JoinPlans.CoLikes.Remaining.sound_FractionalCap

end TinylambOpt.ResearchProofs.Atlas.Goal_CoLikes_FractionalCap
