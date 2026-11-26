import TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
import TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common_FrameRetractions

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_Maximum_PhysicalCount

open TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
open TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common

theorem sound : Maximum.PhysicalCount.Goal := by
  intro rows x out hmem hlen hrem
  let vals := rows.filterMap id
  have hxvals : x ∈ vals := by simp [vals] at hmem ⊢; exact hmem
  have hrem' : x < vals.max?.getD 0 ∧ vals.max?.getD 0 = out := by
    simpa [Maximum.model, vals] using hrem
  have hlt : x < vals.max?.getD 0 := hrem'.1
  have hout : out = vals.max?.getD 0 := hrem'.2.symm
  subst out
  have hnevals : vals ≠ [] := by
    intro h
    have : x ∉ vals := by simp [h]
    exact this hxvals
  have hmaxmem : vals.max?.getD 0 ∈ vals := by
    have hh := (List.max?_eq_some_iff (xs := vals)).mp (max_eq_getD hnevals)
    exact hh.1
  have hmaxne : vals.max?.getD 0 ≠ x := by omega
  have herase := max_erase_of_lt hxvals hlt
  have hneerase : (vals.erase x) ≠ [] := by
    intro h
    have : vals.max?.getD 0 ∉ vals.erase x := by simp [h]
    exact this ((List.mem_erase_of_ne hmaxne).2 hmaxmem)
  have his := isEmpty_false_of_ne_nil hneerase
  rw [filterMap_id_erase_some hmem]
  change (if (vals.erase x).isEmpty then Maximum.model.empty
          else some (Maximum.model.result (vals.erase x))) = some (vals.max?.getD 0)
  rw [his]
  simp only [Bool.false_eq_true, ite_false, Option.some.injEq]
  change (vals.erase x).max?.getD 0 = vals.max?.getD 0
  exact herase

end TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_Maximum_PhysicalCount
