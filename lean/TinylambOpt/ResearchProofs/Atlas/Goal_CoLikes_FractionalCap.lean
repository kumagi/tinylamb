import TinylambOpt.ResearchGoals.Atlas.JoinPlans
import TinylambOpt.ResearchProofs.Atlas.JoinPlans.CoLikesCapsAndCollisionCorrection

/-!
Join planning can change the order or representation of matching work while preserving
the modeled result and multiplicity.

Proved

- `CoLikes.FractionalCap` — `∀ n db b weights den, cover pattern weights den →
  (reference pattern n (fun r x y => if db r x y == 0 then 0 else 1) b).toNat ^ den ≤
  ((pattern.atoms.zip weights).map (fun (a,w) => support n db a pattern b ^ w)).prod`
-/

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
  TinylambOpt.ResearchProofs.Atlas.JoinPlans.CoLikesCapsAndCollisionCorrection.sound_FractionalCap

end TinylambOpt.ResearchProofs.Atlas.Goal_CoLikes_FractionalCap
