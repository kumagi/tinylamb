import TinylambOpt.ResearchGoals.EngineBoundaries.SpillLeases

/-!
A spillable operator's resource accounting must protect partitions still referenced by
execution.

Proved

- `HideBeforeFree` — Logical drop immediately blocks new readers while preserving every
  admitted lease's bytes.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.EngineBoundaries.Goal_SpillLeases_HideBeforeFree

open TinylambOpt.ResearchGoals.EngineBoundaries.SpillLeases

theorem sound : HideBeforeFree.Goal := by
  intro s t l
  constructor
  · simp [acquire, dropBlock]
  · rfl

end TinylambOpt.ResearchProofs.EngineBoundaries.Goal_SpillLeases_HideBeforeFree
