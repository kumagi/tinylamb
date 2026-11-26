import TinylambOpt.ResearchGoals.EngineBoundaries.SpillLeases

/-!
A spillable operator's resource accounting must protect partitions still referenced by
execution.

Proved

- `PinBlocksReuse` — An admitted reader prevents reuse even after the logical segment
  is dropped.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.EngineBoundaries.Goal_SpillLeases_PinBlocksReuse

open TinylambOpt.ResearchGoals.EngineBoundaries.SpillLeases

theorem sound : PinBlocksReuse.Goal := by
  intro s t l next v hacq
  by_cases hlive : s.live = true
  · simp [acquire, hlive] at hacq
    rcases hacq with ⟨_, _, hnext⟩
    rw [← hnext]
    simp [reuse, reclaimable, dropBlock]
  · simp [acquire, hlive] at hacq

end TinylambOpt.ResearchProofs.EngineBoundaries.Goal_SpillLeases_PinBlocksReuse
