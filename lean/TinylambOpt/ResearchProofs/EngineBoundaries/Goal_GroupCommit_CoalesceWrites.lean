import TinylambOpt.ResearchGoals.EngineBoundaries.GroupCommit

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.EngineBoundaries.Goal_GroupCommit_CoalesceWrites

open TinylambOpt.ResearchGoals.EngineBoundaries.GroupCommit

theorem sound : CoalesceWrites.Goal := by
  intro s a b rest h
  have hs : step (step s (.write a)) (.write b) = step s (.write (a+b)) := by
    rcases s with ⟨ap, wr, du, fl⟩
    simp [step]
    omega
  simp [run, hs]

end TinylambOpt.ResearchProofs.EngineBoundaries.Goal_GroupCommit_CoalesceWrites
