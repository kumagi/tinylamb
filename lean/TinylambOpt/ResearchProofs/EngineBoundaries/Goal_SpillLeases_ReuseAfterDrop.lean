import TinylambOpt.ResearchGoals.EngineBoundaries.SpillLeases

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.EngineBoundaries.Goal_SpillLeases_ReuseAfterDrop

open TinylambOpt.ResearchGoals.EngineBoundaries.SpillLeases

theorem sound : ReuseAfterDrop.Goal := by
  intro s v h
  rcases s with ⟨e,live,pay,pins⟩
  simp [reuse, reclaimable, dropBlock] at h ⊢
  simp [h]

end TinylambOpt.ResearchProofs.EngineBoundaries.Goal_SpillLeases_ReuseAfterDrop
