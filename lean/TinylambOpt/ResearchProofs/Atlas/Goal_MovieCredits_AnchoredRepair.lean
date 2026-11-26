import TinylambOpt.ResearchGoals.Atlas.JoinPlans
import TinylambOpt.ResearchProofs.Atlas.JoinPlans.MovieJoinBoundsAndRepairs

/-!
Join planning can change the order or representation of matching work while preserving
the modeled result and multiplicity.

Proved

- `MovieCredits.AnchoredRepair` — `∀ n old new b, reference pattern n new b - reference
  pattern n old b = (pattern.atoms.zipIdx.map (fun (a,i) => pivotTerm pattern n old
  new b i a)).sum`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

/-! `MovieCredits.AnchoredRepair` is the generic anchored-repair telescoping
identity specialised to the JOB-style movie/person/role triangle.

Truth was checked first by exhaustive evaluation on small inputs: for `n = 2`,
all 64 boolean databases on the 4 relevant entries of each of the three
relations, both sides of the identity were compared for every old/new pair and
for every single-entry perturbation of the new database (`#eval` returned 0
failures; see the scratch check).  The generic lemma `anchored_repair_generic`
in `Atlas.JoinPlans.MovieJoinBoundsAndRepairs` proves the identity for every pattern with distinct
free variables and no self-loop atoms; both side conditions hold for this
pattern by computation. -/

namespace TinylambOpt.ResearchProofs.Atlas.Goal_MovieCredits_AnchoredRepair

open TinylambOpt.ResearchGoals.Atlas.JoinPlans
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.MovieCredits

theorem sound_MovieCredits_AnchoredRepair : MovieCredits.AnchoredRepair.Goal := by
  intro n old new b
  exact TinylambOpt.ResearchProofs.Atlas.JoinPlans.MovieJoinBoundsAndRepairs.anchored_repair_generic
    pattern n old new b (by native_decide) (by native_decide)

end TinylambOpt.ResearchProofs.Atlas.Goal_MovieCredits_AnchoredRepair
