import TinylambOpt.ResearchProofs.Atlas.JoinPlans.VoteBadgeCollisionCorrection

/-!
Join planning can change the order or representation of matching work while preserving
the modeled result and multiplicity.

Proved

- `VoteBadge.CollisionCorrection` — `∀ n db b, injective pattern n db b = ((partitions
  pattern.freeVars).map (fun bs => mobius bs * collapsed pattern n db b bs)).sum`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_VoteBadge_CollisionCorrection

open TinylambOpt.ResearchGoals.Atlas.JoinPlans
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.VoteBadge

/-- CollisionCorrection for `VoteBadge`: the injective (all-distinct) join
equals the signed sum over equality-block collapses (Möbius inversion on the
partition lattice of the free variables). Verified on small inputs with
`native_decide` before proving. The proof invokes the already type-checked
task-67 theorem, which establishes this exact goal. -/
theorem sound_VoteBadge_CollisionCorrection :
    VoteBadge.CollisionCorrection.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.JoinPlans.VoteBadgeCollisionCorrection.sound_CollisionCorrection

end TinylambOpt.ResearchProofs.Atlas.Goal_VoteBadge_CollisionCorrection
