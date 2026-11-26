import TinylambOpt.ResearchGoals.DatabaseCorrectness.PinnedScan

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.Goal_PinnedScan_RegisteredBeforePhysicalMutation

open TinylambOpt.ResearchGoals.DatabaseCorrectness.PinnedScan
open TinylambOpt.ResearchGoals.ResourcePolicies.VersionVacuum


theorem sound : RegisteredBeforePhysicalMutation.Goal := by
  intro s snap hreg hsave
  simp [readWithVerdict, pin, resolve, hreg, hsave]

end TinylambOpt.ResearchProofs.DatabaseCorrectness.Goal_PinnedScan_RegisteredBeforePhysicalMutation
