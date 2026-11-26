import TinylambOpt.ResearchGoals.LayerContracts.ReferenceEdits
import TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Common

/-!
Foreign-key checking must respect the observation point of the constraint and the parent
keys still present after each modeled edit.

Proved

- `ReplaceParent.LastCopy` — `∀ s a b, ok s → s.parents.count a=1 → some a ∈ s.children
  → immediate model s a b = false`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Goal_ReplaceParent_LastCopy

open TinylambOpt.ResearchGoals.LayerContracts.ReferenceEdits
open TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Common

theorem sound : ReplaceParent.LastCopy.Goal := by
  intro s a b hs hcount hchild
  have hpar : a ∉ (s.parents.erase a) := not_mem_erase_of_count_one s.parents a hcount
  have hbad : bad (step s (.delP a)) a = true := by
    apply (bad_iff _ a).mpr
    constructor
    · simpa [step] using hchild
    · simpa [step] using hpar
  have hs1 : ok (step s (.delP a)) = false := by
    cases hok : ok (step s (.delP a)) with
    | false => rfl
    | true =>
        have hb := (ok_iff_no_bad _).mp hok a
        rw [hbad] at hb
        contradiction
  have hs1' : ok { parents := s.parents.erase a, children := s.children } = false := by
    simpa [step] using hs1
  simp [immediate, finish, run, step, ReplaceParent.model, hs1']

end TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Goal_ReplaceParent_LastCopy
