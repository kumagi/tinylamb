import TinylambOpt.ResearchGoals.LayerContracts.ReferenceEdits
import TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Common

/-!
Foreign-key checking must respect the observation point of the constraint and the parent
keys still present after each modeled edit.

Proved

- `RekeyLate.SingleProbe` — Can checking just the first affected key authorize the
  complete statement?
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Goal_RekeyLate_SingleProbe

open TinylambOpt.ResearchGoals.LayerContracts.ReferenceEdits
open TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Common

theorem sound : RekeyLate.SingleProbe.Goal := by
  intro s a b hs
  have hsProp := (ok_iff_prop s).mp hs
  have ht : ok (finish RekeyLate.model s a b) = true := by
    rw [ok_iff_prop]
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
        exact hsProp k ⟨hmem, hpar⟩
  have hba : bad (finish RekeyLate.model s a b) a = false := (ok_iff_no_bad _).mp ht a
  have hiff : bad (finish RekeyLate.model s a b) a = false ↔ ok (finish RekeyLate.model s a b) = true :=
    ⟨fun _ => ht, fun _ => hba⟩
  exact singleProbe_of_head_iff RekeyLate.model (fun _ _ => rfl) s a b hiff

end TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Goal_RekeyLate_SingleProbe
