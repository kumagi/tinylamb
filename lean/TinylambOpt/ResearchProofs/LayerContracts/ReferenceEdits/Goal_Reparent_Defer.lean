import TinylambOpt.ResearchGoals.LayerContracts.ReferenceEdits
import TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Common

/-!
Foreign-key checking must respect the observation point of the constraint and the parent
keys still present after each modeled edit.

Proved

- `Reparent.Defer` — Can an immediate checker be replaced by an end-of-statement check?
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Goal_Reparent_Defer

open TinylambOpt.ResearchGoals.LayerContracts.ReferenceEdits
open TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Common

theorem sound : Reparent.Defer.Goal := by
  intro s a b hs
  let s1 := step s (.rewrite a (some b))
  let s2 := step s1 (.delP a)
  have hmono : ok s2 = true → ok s1 = true := by
    intro h
    exact ok_delP_mono s1 a (by simpa [s2] using h)
  apply Bool.eq_iff_iff.mpr
  constructor
  · intro him
    have hc : ok s = true ∧ ok s1 = true ∧ ok s2 = true := by
      simpa [immediate, finish, run, step, Reparent.model, s1, s2] using him
    simpa [finish, run, step, Reparent.model, s1, s2] using hc.2.2
  · intro hf
    have hs2 : ok s2 = true := by simpa [finish, run, step, Reparent.model, s1, s2] using hf
    have hs1 := hmono hs2
    have hc : ok s = true ∧ ok s1 = true ∧ ok s2 = true := ⟨hs, hs1, hs2⟩
    simpa [immediate, finish, run, step, Reparent.model, s1, s2] using hc

end TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Goal_Reparent_Defer
