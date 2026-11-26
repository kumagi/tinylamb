import TinylambOpt.ResearchGoals.LayerContracts.ReferenceEdits
import TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Common

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Goal_DropPair_Defer

open TinylambOpt.ResearchGoals.LayerContracts.ReferenceEdits
open TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Common

theorem sound : DropPair.Defer.Goal := by
  intro s a b hs
  let s1 := step s (.delC (some a))
  let s2 := step s1 (.delP a)
  have hs1 : ok s1 = true := by simpa [s1] using ok_delC s (some a) hs
  apply Bool.eq_iff_iff.mpr
  constructor
  · intro him
    have hc : ok s = true ∧ ok s1 = true ∧ ok s2 = true := by
      simpa [immediate, finish, run, step, DropPair.model, s1, s2] using him
    simpa [finish, run, step, DropPair.model, s1, s2] using hc.2.2
  · intro hf
    have hs2 : ok s2 = true := by simpa [finish, run, step, DropPair.model, s1, s2] using hf
    have hc : ok s = true ∧ ok s1 = true ∧ ok s2 = true := ⟨hs, hs1, hs2⟩
    simpa [immediate, finish, run, step, DropPair.model, s1, s2] using hc

end TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Goal_DropPair_Defer
