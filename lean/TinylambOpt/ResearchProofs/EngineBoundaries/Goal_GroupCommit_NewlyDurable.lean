import TinylambOpt.ResearchGoals.EngineBoundaries.GroupCommit

/-!
A commit batching policy must preserve the durability and visibility guarantees of
committed SQL transactions.

Proved

- `NewlyDurable` — Replies newly eligible after a sync occupy an exact half-open/closed
  interval.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.EngineBoundaries.Goal_GroupCommit_NewlyDurable

open TinylambOpt.ResearchGoals.EngineBoundaries.GroupCommit

theorem sound : NewlyDurable.Goal := by
  intro s ws
  simp [ready, step, List.filter_filter]

end TinylambOpt.ResearchProofs.EngineBoundaries.Goal_GroupCommit_NewlyDurable
