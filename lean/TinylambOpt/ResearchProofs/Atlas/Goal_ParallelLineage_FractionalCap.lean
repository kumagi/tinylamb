import TinylambOpt.ResearchGoals.Atlas.JoinPlans
import TinylambOpt.ResearchProofs.Atlas.JoinPlans.ParallelLineageCapsAndCollisionCorrection

/-!
Join planning can change the order or representation of matching work while preserving
the modeled result and multiplicity.

Proved

- `ParallelLineage.FractionalCap` — `∀ n db b weights den, cover pattern weights den →
  (reference pattern n (fun r x y => if db r x y == 0 then 0 else 1) b).toNat ^ den ≤
  ((pattern.atoms.zip weights).map (fun (a,w) => support n db a pattern b ^ w)).prod`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_ParallelLineage_FractionalCap

open TinylambOpt.ResearchGoals.Atlas.JoinPlans
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.ParallelLineage

/-- The fractional edge-cover (AGM) bound for the `ParallelLineage` shape:
    the number of nonzero assignments of the six-atom join, raised to `den`,
    is bounded by the product of the per-atom support counts raised to the
    cover weights.  The statement was checked exhaustively on all 3^3 signed
    one-element databases, both boundary bindings, all 3^6 weight vectors and
    `den ∈ {1,2}` before proving it.  The fully elaborated argument (finite
    range-sum infrastructure, five pointwise cover bounds and the splitting
    of `den` over them) is the already type-checked per-model proof in
    `Atlas.JoinPlans.ParallelLineageCapsAndCollisionCorrection`, reused verbatim here. -/
theorem sound_ParallelLineage_FractionalCap :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.ParallelLineage.FractionalCap.Goal :=
  TinylambOpt.ResearchProofs.Atlas.JoinPlans.ParallelLineageCapsAndCollisionCorrection.sound_FractionalCap

end TinylambOpt.ResearchProofs.Atlas.Goal_ParallelLineage_FractionalCap
