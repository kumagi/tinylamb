import TinylambOpt.ResearchGoals.Atlas.JoinPlans
import TinylambOpt.ResearchProofs.Atlas_JoinPlans_DoubleBilling_Remaining

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_DoubleBilling_FractionalCap

open TinylambOpt.ResearchGoals.Atlas.JoinPlans.DoubleBilling

/-- The fractional edge-cover bound for the double-billing pattern.  The
reference unfolds to `Mcount * S3count`, and the cover forces `den ≤ w2` and
`den ≤ w3`; hence the bound follows from `Mcount ≤ support(atom 1,1,2)` and
the two support factors being nonzero.  The full proof lives in the per-model
`Remaining` module and is reused here. -/
theorem sound_DoubleBilling_FractionalCap :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.DoubleBilling.FractionalCap.Goal :=
  TinylambOpt.ResearchProofs.Atlas.JoinPlans.DoubleBilling.Remaining.sound_FractionalCap

end TinylambOpt.ResearchProofs.Atlas.Goal_DoubleBilling_FractionalCap
