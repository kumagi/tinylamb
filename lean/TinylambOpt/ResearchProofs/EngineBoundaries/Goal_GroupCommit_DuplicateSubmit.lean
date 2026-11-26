import TinylambOpt.ResearchGoals.EngineBoundaries.GroupCommit

/-!
A commit batching policy must preserve the durability and visibility guarantees of
committed SQL transactions.

Proved

- `DuplicateSubmit` — Two requests to start the same outstanding sync need only one
  completion.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.EngineBoundaries.Goal_GroupCommit_DuplicateSubmit

open TinylambOpt.ResearchGoals.EngineBoundaries.GroupCommit

theorem sound : DuplicateSubmit.Goal := by
  intro s trace
  rcases s with ⟨a,wr,d,f⟩
  simp [run, step]

end TinylambOpt.ResearchProofs.EngineBoundaries.Goal_GroupCommit_DuplicateSubmit
