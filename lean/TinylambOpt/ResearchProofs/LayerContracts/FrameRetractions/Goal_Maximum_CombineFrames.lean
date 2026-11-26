import TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
import TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common_FrameRetractions

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_Maximum_CombineFrames

open TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
open TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common

theorem sound : Maximum.CombineFrames.Goal := by
  apply combineFrames_of
  intro a b ha hb
  have h1 := (List.max?_eq_some_iff (xs := a)).mp (max_eq_getD ha)
  have h2 := (List.max?_eq_some_iff (xs := b)).mp (max_eq_getD hb)
  have h3 : (a ++ b).max? = some (max (a.max?.getD 0) (b.max?.getD 0)) := by
    apply (List.max?_eq_some_iff).2
    constructor
    · by_cases hle : a.max?.getD 0 ≤ b.max?.getD 0
      · rw [Nat.max_eq_right hle]
        simp [h2.1]
      · rw [Nat.max_eq_left (Nat.le_of_not_ge hle)]
        simp [h1.1]
    · intro z hz
      simp at hz
      rcases hz with hz | hz
      · exact Nat.le_trans (h1.2 z hz) (Nat.le_max_left _ _)
      · exact Nat.le_trans (h2.2 z hz) (Nat.le_max_right _ _)
  simpa [Maximum.model, h3]

end TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_Maximum_CombineFrames
