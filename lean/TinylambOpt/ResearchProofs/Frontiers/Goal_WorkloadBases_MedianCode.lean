import TinylambOpt.ResearchProofs.Frontiers.WorkloadBases

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Frontiers.Goal_WorkloadBases_MedianCode

theorem sound : TinylambOpt.ResearchGoals.Frontiers.WorkloadBases.MedianCode.Goal :=
  TinylambOpt.ResearchProofs.Frontiers.WorkloadBases.MedianCode_sound

end TinylambOpt.ResearchProofs.Frontiers.Goal_WorkloadBases_MedianCode
