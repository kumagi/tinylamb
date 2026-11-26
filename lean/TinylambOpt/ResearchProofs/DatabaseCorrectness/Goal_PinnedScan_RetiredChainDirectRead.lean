import TinylambOpt.ResearchGoals.DatabaseCorrectness.PinnedScan

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.Goal_PinnedScan_RetiredChainDirectRead

open TinylambOpt.ResearchGoals.DatabaseCorrectness.PinnedScan
open TinylambOpt.ResearchGoals.ResourcePolicies.VersionVacuum


theorem sound : RetiredChainDirectRead.Goal := by
  intro s head rest oldest snap hs hvs hord hphys hle1 hle2
  have hle : head.stamp ≤ snap := by omega
  simp [resolve, hvs, readAt, winner, hle, hphys]

end TinylambOpt.ResearchProofs.DatabaseCorrectness.Goal_PinnedScan_RetiredChainDirectRead
