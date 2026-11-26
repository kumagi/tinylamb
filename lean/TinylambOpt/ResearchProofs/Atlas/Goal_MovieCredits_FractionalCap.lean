import TinylambOpt.ResearchGoals.Atlas.JoinPlans
import TinylambOpt.ResearchProofs.Atlas.JoinPlans.MovieCreditsCapsAndCollisionCorrection

/-!
Join planning can change the order or representation of matching work while preserving
the modeled result and multiplicity.

Proved

- `MovieCredits.FractionalCap` — `∀ n db b weights den, cover pattern weights den →
  (reference pattern n (fun r x y => if db r x y == 0 then 0 else 1) b).toNat ^ den ≤
  ((pattern.atoms.zip weights).map (fun (a,w) => support n db a pattern b ^ w)).prod`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_MovieCredits_FractionalCap

open TinylambOpt.ResearchGoals.Atlas.JoinPlans.MovieCredits

/-- The fractional edge-cover bound for the JOB-style movie/credit/person
triangle join: the same result is proved in the per-model `Remaining` module
(support counting, three pairwise bounds, a Cauchy-Schwarz bound on `T^2`,
and the weighted induction over `den`). -/
theorem sound_MovieCredits_FractionalCap :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.MovieCredits.FractionalCap.Goal :=
  TinylambOpt.ResearchProofs.Atlas.JoinPlans.MovieCreditsCapsAndCollisionCorrection.sound_FractionalCap

end TinylambOpt.ResearchProofs.Atlas.Goal_MovieCredits_FractionalCap
