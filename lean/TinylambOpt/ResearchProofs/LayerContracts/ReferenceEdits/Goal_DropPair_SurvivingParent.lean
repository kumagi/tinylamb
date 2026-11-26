import TinylambOpt.ResearchGoals.LayerContracts.ReferenceEdits
import TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Common

/-!
Foreign-key checking must respect the observation point of the constraint and the parent
keys still present after each modeled edit.

Proved

- `DropPair.SurvivingParent` — `∀ s a b, 1 < s.parents.count a → bad (finish model s a
  b) a = false`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Goal_DropPair_SurvivingParent

open TinylambOpt.ResearchGoals.LayerContracts.ReferenceEdits
open TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Common

theorem sound : DropPair.SurvivingParent.Goal := by
  intro s a b hcount
  have hpar : a ∈ s.parents.erase a := mem_erase_of_count_gt_one s.parents a hcount
  have hnot : ¬ bad (finish DropPair.model s a b) a = true := by
    intro hb
    have hh := (bad_iff _ a).mp hb
    exact hh.2 (by simpa [finish, run, step, DropPair.model] using hpar)
  cases hb : bad (finish DropPair.model s a b) a <;> simp_all

end TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Goal_DropPair_SurvivingParent
