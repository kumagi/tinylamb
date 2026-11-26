import TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_Product_LastZero

open TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions

theorem sound : Product.LastZero.Goal := by
  intro n
  by_cases hn : n = 0
  · subst n
    simp [repaired, dropOne, value, Product.model]
  · have hnpos : 0 < n := Nat.pos_of_ne_zero hn
    simp [repaired, dropOne, value, Product.model, hnpos]

end TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_Product_LastZero
