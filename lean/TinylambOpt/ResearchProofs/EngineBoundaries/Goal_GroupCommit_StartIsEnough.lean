import TinylambOpt.ResearchGoals.EngineBoundaries.GroupCommit

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.EngineBoundaries.Goal_GroupCommit_StartIsEnough

open TinylambOpt.ResearchGoals.EngineBoundaries.GroupCommit

theorem refute : ¬ StartIsEnough.Goal := by
  intro h
  have hh := h ⟨1,0,0,none⟩ 0 1 (by simp [ordered]) (by decide) (by decide) (by decide)
  have hbad : ¬ (0 + 1 ≤ (⟨1,0,0,none⟩ : State).durable) := by native_decide
  exact hbad hh

end TinylambOpt.ResearchProofs.EngineBoundaries.Goal_GroupCommit_StartIsEnough
