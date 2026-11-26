import TinylambOpt.ResearchGoals.EngineBoundaries.PeerWindows

/-!
Window execution must preserve peer membership and frame results at ordering boundaries.

Proved

- `MergeRank` — RANK can merge arbitrary shards by summing the two counts of strict
  predecessors.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.EngineBoundaries.Goal_PeerWindows_MergeRank

open TinylambOpt.ResearchGoals.EngineBoundaries.PeerWindows

theorem sound : MergeRank.Goal := by
  intro a b k
  simp [rankOf, lower]
  omega

end TinylambOpt.ResearchProofs.EngineBoundaries.Goal_PeerWindows_MergeRank
