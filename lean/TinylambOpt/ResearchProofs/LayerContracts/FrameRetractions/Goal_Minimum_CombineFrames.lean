import TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
import TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common_FrameRetractions

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_Minimum_CombineFrames

open TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
open TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common

theorem sound : Minimum.CombineFrames.Goal := by
  apply combineFrames_of
  intro a b ha hb
  have h1 := (List.min?_eq_some_iff (xs := a)).mp (min_eq_getD ha)
  have h2 := (List.min?_eq_some_iff (xs := b)).mp (min_eq_getD hb)
  have happ : a ++ b ≠ [] := by simp [ha]
  have h3 : (a ++ b).min? = some (min (a.min?.getD 0) (b.min?.getD 0)) := by
    apply (List.min?_eq_some_iff).2
    constructor
    · by_cases hle : a.min?.getD 0 ≤ b.min?.getD 0
      · rw [Nat.min_eq_left hle]
        simp [h1.1]
      · rw [Nat.min_eq_right (Nat.le_of_not_ge hle)]
        simp [h2.1]
    · intro z hz
      simp at hz
      rcases hz with hz | hz
      · exact Nat.le_trans (Nat.min_le_left _ _) (h1.2 z hz)
      · exact Nat.le_trans (Nat.min_le_right _ _) (h2.2 z hz)
  simpa [Minimum.model, h3]

end TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_Minimum_CombineFrames
