import TinylambOpt.ResearchGoals.Atlas.JoinPlans
import TinylambOpt.ResearchProofs.Atlas.JoinPlans.MovieJoinBoundaryMessages

/-!
Join planning can change the order or representation of matching work while preserving
the modeled result and multiplicity.

Proved

- `CoLikes.AnchoredRepair` — `∀ n old new b, reference pattern n new b - reference
  pattern n old b = (pattern.atoms.zipIdx.map (fun (a,i) => pivotTerm pattern n old
  new b i a)).sum`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_CoLikes_AnchoredRepair

open TinylambOpt.ResearchGoals.Atlas.JoinPlans
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.CoLikes

/-! Truth check.  The CoLikes pattern has three free variables `[2,3,0]` and
five atoms, none of which is a self loop, so the generic anchored-repair
argument applies.  Before using it we brute-force the claim on all boolean
databases on the 12 relevant entries for `n = 2` (with the old database zero
and the binding fixed at 0): the equality holds in every case. -/

private def dbOf (mask : Nat) : DB := fun r x y =>
  if (mask / (2 ^ (r * 4 + x * 2 + y))) % 2 == 1 then 1 else 0

private def checkSmall : Bool :=
  (List.range 4096).all fun m =>
    let old : DB := fun _ _ _ => 0
    let new := dbOf m
    let b : Bind := fun _ => 0
    reference CoLikes.pattern 2 new b - reference CoLikes.pattern 2 old b =
      (CoLikes.pattern.atoms.zipIdx.map (fun (a, i) => pivotTerm CoLikes.pattern 2 old new b i a)).sum

#eval checkSmall

example : CoLikes.pattern.freeVars.Nodup := by native_decide
example : ∀ a ∈ CoLikes.pattern.atoms, a.src ≠ a.dst := by native_decide

theorem sound_CoLikes_AnchoredRepair : CoLikes.AnchoredRepair.Goal := by
  intro n old new b
  exact TinylambOpt.ResearchProofs.Atlas.JoinPlans.MovieJoinBoundaryMessages.anchored_repair_generic
    CoLikes.pattern n old new b (by native_decide) (by native_decide)

end TinylambOpt.ResearchProofs.Atlas.Goal_CoLikes_AnchoredRepair
