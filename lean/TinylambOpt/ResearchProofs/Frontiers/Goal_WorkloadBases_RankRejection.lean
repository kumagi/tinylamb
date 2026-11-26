import TinylambOpt.ResearchProofs.Frontiers.WorkloadBases

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Frontiers.Goal_WorkloadBases_RankRejection

theorem sound : TinylambOpt.ResearchGoals.Frontiers.WorkloadBases.RankRejection.Goal :=
  TinylambOpt.ResearchProofs.Frontiers.WorkloadBases.RankRejection_sound

end TinylambOpt.ResearchProofs.Frontiers.Goal_WorkloadBases_RankRejection
