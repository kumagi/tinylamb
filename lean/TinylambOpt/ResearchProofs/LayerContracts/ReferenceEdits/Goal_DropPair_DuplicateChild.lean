import TinylambOpt.ResearchGoals.LayerContracts.ReferenceEdits
import TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Common

/-!
Foreign-key checking must respect the observation point of the constraint and the parent
keys still present after each modeled edit.

Proved

- `DropPair.DuplicateChild` — `∀ s a b, s.parents.count a=1 → 1 < s.children.count
  (some a) → bad (finish model s a b) a`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Goal_DropPair_DuplicateChild

open TinylambOpt.ResearchGoals.LayerContracts.ReferenceEdits
open TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Common

theorem sound : DropPair.DuplicateChild.Goal := by
  intro s a b hcount hchild
  have hmem : some a ∈ s.children.erase (some a) :=
    mem_erase_of_count_gt_one s.children (some a) hchild
  have hpar : a ∉ s.parents.erase a := not_mem_erase_of_count_one s.parents a hcount
  apply (bad_iff _ a).mpr
  constructor
  · simpa [finish, run, step, DropPair.model] using hmem
  · simpa [finish, run, step, DropPair.model] using hpar

end TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Goal_DropPair_DuplicateChild
