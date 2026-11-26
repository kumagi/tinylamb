import TinylambOpt.ResearchGoals.Atlas.JoinPlans
import TinylambOpt.ResearchProofs.Atlas.JoinPlans.SupplierCliqueCollisionAndOrbit

/-!
Join planning can change the order or representation of matching work while preserving
the modeled result and multiplicity.

Proved

- `SupplierClique.CollisionCorrection` — `∀ n db b, injective pattern n db b =
  ((partitions pattern.freeVars).map (fun bs => mobius bs * collapsed pattern n db b
  bs)).sum`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

/-! CollisionCorrection for `SupplierClique`: the injective join over the six
edges of K4 equals the signed sum over the 15 equality-block collapses of the
four free variables. Brute-force checks over small `n`, databases and bindings
confirm the identity; the proof reuses the checked inclusion-exclusion argument
in the per-model `Remaining_Round2` module (partition-lattice Möbius inversion). -/

namespace TinylambOpt.ResearchProofs.Atlas.Goal_SupplierClique_CollisionCorrection

open TinylambOpt.ResearchGoals.Atlas.JoinPlans
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.SupplierClique

theorem sound_SupplierClique_CollisionCorrection :
    SupplierClique.CollisionCorrection.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.JoinPlans.SupplierCliqueCollisionAndOrbit.sound_CollisionCorrection

end TinylambOpt.ResearchProofs.Atlas.Goal_SupplierClique_CollisionCorrection
