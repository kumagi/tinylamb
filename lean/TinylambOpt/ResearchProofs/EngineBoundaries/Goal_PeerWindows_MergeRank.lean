import TinylambOpt.ResearchGoals.EngineBoundaries.PeerWindows

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.EngineBoundaries.Goal_PeerWindows_MergeRank

open TinylambOpt.ResearchGoals.EngineBoundaries.PeerWindows

theorem sound : MergeRank.Goal := by
  intro a b k
  simp [rankOf, lower]
  omega

end TinylambOpt.ResearchProofs.EngineBoundaries.Goal_PeerWindows_MergeRank
