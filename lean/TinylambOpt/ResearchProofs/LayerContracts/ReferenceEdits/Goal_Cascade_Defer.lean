import TinylambOpt.ResearchGoals.LayerContracts.ReferenceEdits
import TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Common

/-!
Foreign-key checking must respect the observation point of the constraint and the parent
keys still present after each modeled edit.

Proved

- `Cascade.Defer` — Can an immediate checker be replaced by an end-of-statement check?
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Goal_Cascade_Defer

open TinylambOpt.ResearchGoals.LayerContracts.ReferenceEdits
open TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Common

theorem sound : Cascade.Defer.Goal := by
  intro s a b hs
  let s1 := step s (.purge a)
  let s2 := step s1 (.delP a)
  have hs1 : ok s1 = true := by simpa [s1] using ok_purge s a hs
  have hno : ¬ some a ∈ s1.children := by simpa [s1] using no_child_after_purge s a
  have hs2 : ok s2 = true := by simpa [s2] using ok_delP_of_no_children s1 a hs1 hno
  apply Bool.eq_iff_iff.mpr
  constructor
  · intro him
    have hc : ok s = true ∧ ok s1 = true ∧ ok s2 = true := by
      simpa [immediate, finish, run, step, Cascade.model, s1, s2] using him
    simpa [finish, run, step, Cascade.model, s1, s2] using hc.2.2
  · intro hf
    have hc : ok s = true ∧ ok s1 = true ∧ ok s2 = true := ⟨hs, hs1, hs2⟩
    simpa [immediate, finish, run, step, Cascade.model, s1, s2] using hc

end TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Goal_Cascade_Defer
