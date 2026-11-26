import TinylambOpt.ResearchGoals.Atlas.JoinPlans
import TinylambOpt.ResearchProofs.Atlas.JoinPlans.VoteBadgeFractionalCap

/-!
Join planning can change the order or representation of matching work while preserving
the modeled result and multiplicity.

Proved

- `VoteBadge.FractionalCap` — `∀ n db b weights den, cover pattern weights den →
  (reference pattern n (fun r x y => if db r x y == 0 then 0 else 1) b).toNat ^ den ≤
  ((pattern.atoms.zip weights).map (fun (a,w) => support n db a pattern b ^ w)).prod`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_VoteBadge_FractionalCap

open TinylambOpt.ResearchGoals.Atlas.JoinPlans.VoteBadge

/-- The fractional edge-cover bound for the VoteBadge join.  The per-model
`Remaining` module contains the full argument: indicator expansion of the
reference join, the three-edge support bound `N ≤ s1 * s2 * s4`, the degenerate
cases where the pinned-edge supports vanish, and the weighted induction over
`den` that distributes the cover weights. -/
theorem sound_VoteBadge_FractionalCap :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.VoteBadge.FractionalCap.Goal :=
  TinylambOpt.ResearchProofs.Atlas.JoinPlans.VoteBadgeFractionalCap.sound_FractionalCap

end TinylambOpt.ResearchProofs.Atlas.Goal_VoteBadge_FractionalCap
