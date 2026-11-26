import TinylambOpt.ResearchGoals.EngineBoundaries.GroupCommit

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.EngineBoundaries.Goal_GroupCommit_RetryLatest

open TinylambOpt.ResearchGoals.EngineBoundaries.GroupCommit

theorem sound : RetryLatest.Goal := by
  intro s n hord
  rcases s with ⟨a,wr,d,f⟩
  simp [ordered] at hord
  rcases hord with ⟨h1,h2,hf⟩
  simp [run, step]
  have hmin : d ≤ min a (wr+n) := by omega
  omega

end TinylambOpt.ResearchProofs.EngineBoundaries.Goal_GroupCommit_RetryLatest
