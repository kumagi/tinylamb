import TinylambOpt.ResearchProofs.Frontiers.WorkloadBases

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Frontiers.Goal_WorkloadBases_DirtyWeights

theorem sound : TinylambOpt.ResearchGoals.Frontiers.WorkloadBases.DirtyWeights.Goal :=
  TinylambOpt.ResearchProofs.Frontiers.WorkloadBases.DirtyWeights_sound

end TinylambOpt.ResearchProofs.Frontiers.Goal_WorkloadBases_DirtyWeights
