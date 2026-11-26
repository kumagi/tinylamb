import TinylambOpt.ResearchGoals.LayerContracts.ReferenceEdits
import TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Common

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Goal_RekeyLate_Repaired

open TinylambOpt.ResearchGoals.LayerContracts.ReferenceEdits
open TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Common

theorem sound : RekeyLate.Repaired.Goal := by
  intro s a b hs
  rw [ok_iff_prop] at hs ⊢
  intro k hk
  by_cases hkb : k = b
  · subst k
    exact hk.2 (by simp [finish, run, step, RekeyLate.model])
  · have hmap : some k ∈ s.children.map (fun x => if x = some a then some b else x) := by
      simpa [finish, run, step, RekeyLate.model] using hk.1
    rcases List.mem_map.mp hmap with ⟨x, hx, hxk⟩
    by_cases hxa : x = some a
    · have hbk : b = k := by simpa [hxa] using hxk
      exact hkb hbk.symm
    · have hxk' : x = some k := by simpa [hxa] using hxk
      have hmem : some k ∈ s.children := by simpa [hxk'] using hx
      have hpar : k ∉ s.parents := by
        intro hp
        have hka : k ≠ a := by
          intro hka
          subst k
          exact hxa hxk'
        have hpfin : k ∈ s.parents.erase a := (List.mem_erase_of_ne hka).mpr hp
        exact hk.2 (by simp [finish, run, step, RekeyLate.model, hpfin])
      exact hs k ⟨hmem, hpar⟩

end TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Goal_RekeyLate_Repaired
