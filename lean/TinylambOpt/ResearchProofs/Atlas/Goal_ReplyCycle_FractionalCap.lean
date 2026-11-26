import TinylambOpt.ResearchGoals.Atlas.JoinPlans
import TinylambOpt.ResearchProofs.Atlas.JoinPlans.ReplyCycleCapsAndCollisionCorrection

/-!
Join planning can change the order or representation of matching work while preserving
the modeled result and multiplicity.

Proved

- `ReplyCycle.FractionalCap` — `∀ n db b weights den, cover pattern weights den →
  (reference pattern n (fun r x y => if db r x y == 0 then 0 else 1) b).toNat ^ den ≤
  ((pattern.atoms.zip weights).map (fun (a,w) => support n db a pattern b ^ w)).prod`
-/

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
  TinylambOpt.ResearchProofs.Atlas.JoinPlans.ReplyCycleCapsAndCollisionCorrection.sound_FractionalCap

end TinylambOpt.ResearchProofs.Atlas.Goal_ReplyCycle_FractionalCap
