import TinylambOpt.ResearchGoals.DatabaseCorrectness.MergeIntegrity

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.Goal_MergeIntegrity_FailedMergePhaseCannotPublishPrefix

open TinylambOpt.ResearchGoals.DatabaseCorrectness.MergeIntegrity


theorem sound : FailedMergePhaseCannotPublishPrefix.Goal := by
  intro s gone
  simp [publish, failAfterDelete]

end TinylambOpt.ResearchProofs.DatabaseCorrectness.Goal_MergeIntegrity_FailedMergePhaseCannotPublishPrefix
