import TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
import TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common_FrameRetractions

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_RangeWidth_Translate

open TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
open TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common

theorem sound : RangeWidth.Translate.Goal := by
  intro xs k
  by_cases h : xs = []
  · subst xs
    simp [value]
  have hp := (List.min?_eq_some_iff (xs := xs)).mp (min_eq_getD h)
  have hq := (List.max?_eq_some_iff (xs := xs)).mp (max_eq_getD h)
  have hmapped : xs.map (fun v => v + k) ≠ [] := by simp [h]
  have hmin : (xs.map (fun v => v + k)).min? = some (xs.min?.getD 0 + k) :=
    (List.min?_eq_some_iff).2 ⟨by
      exact List.mem_map.mpr ⟨xs.min?.getD 0, hp.1, rfl⟩, by
      intro z hz
      simp only [List.mem_map] at hz
      rcases hz with ⟨v, hv, rfl⟩
      have := hp.2 v hv
      omega⟩
  have hmax : (xs.map (fun v => v + k)).max? = some (xs.max?.getD 0 + k) :=
    (List.max?_eq_some_iff).2 ⟨by
      exact List.mem_map.mpr ⟨xs.max?.getD 0, hq.1, rfl⟩, by
      intro z hz
      simp only [List.mem_map] at hz
      rcases hz with ⟨v, hv, rfl⟩
      have := hq.2 v hv
      omega⟩
  simp [value, RangeWidth.model, h, hmapped, hmin, hmax]
  omega

end TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_RangeWidth_Translate
