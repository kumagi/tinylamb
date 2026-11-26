import TinylambOpt.ResearchGoals.EngineBoundaries.GroupCommit

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.EngineBoundaries.Goal_GroupCommit_DuplicateSubmit

open TinylambOpt.ResearchGoals.EngineBoundaries.GroupCommit

theorem sound : DuplicateSubmit.Goal := by
  intro s trace
  rcases s with ⟨a,wr,d,f⟩
  simp [run, step]

end TinylambOpt.ResearchProofs.EngineBoundaries.Goal_GroupCommit_DuplicateSubmit
