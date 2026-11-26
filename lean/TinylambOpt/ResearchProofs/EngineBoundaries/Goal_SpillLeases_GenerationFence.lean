import TinylambOpt.ResearchGoals.EngineBoundaries.SpillLeases

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.EngineBoundaries.Goal_SpillLeases_GenerationFence

open TinylambOpt.ResearchGoals.EngineBoundaries.SpillLeases

theorem sound : GenerationFence.Goal := by
  intro s t l v hle hreuse
  by_cases hr : reclaimable s = true
  · have ht : t = ⟨s.epoch+1,true,v,[]⟩ := by
      simpa [reuse, hr] using hreuse.symm
    subst t
    simp [readLease]
  · have hnone : reuse s v = none := by simp [reuse, hr]
    rw [hnone] at hreuse
    contradiction

end TinylambOpt.ResearchProofs.EngineBoundaries.Goal_SpillLeases_GenerationFence
