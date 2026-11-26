import TinylambOpt.ResearchGoals.EngineBoundaries.GroupCommit

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.EngineBoundaries.Goal_GroupCommit_LateWritePiggyback

open TinylambOpt.ResearchGoals.EngineBoundaries.GroupCommit

theorem refute : ¬ LateWritePiggyback.Goal := by
  intro h
  have hh := h ⟨10,0,0,none⟩ 5 (by simp [ordered]) rfl
  have hbad : ¬ (let t := run (⟨10,0,0,none⟩ : State) [.submit, .write 5, .finish];
      t.durable = t.written) := by
    native_decide
  exact hbad hh

end TinylambOpt.ResearchProofs.EngineBoundaries.Goal_GroupCommit_LateWritePiggyback
