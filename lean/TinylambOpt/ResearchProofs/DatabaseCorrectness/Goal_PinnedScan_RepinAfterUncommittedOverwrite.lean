import TinylambOpt.ResearchGoals.DatabaseCorrectness.PinnedScan

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.Goal_PinnedScan_RepinAfterUncommittedOverwrite

open TinylambOpt.ResearchGoals.DatabaseCorrectness.PinnedScan
open TinylambOpt.ResearchGoals.ResourcePolicies.VersionVacuum


theorem sound : RepinAfterUncommittedOverwrite.Goal := by
  intro s snap v
  simp [readWithVerdict, pin, stage, resolve]

end TinylambOpt.ResearchProofs.DatabaseCorrectness.Goal_PinnedScan_RepinAfterUncommittedOverwrite
