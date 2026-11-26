import TinylambOpt.ResearchProofs.Frontiers.WorkloadBases

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Frontiers.Goal_WorkloadBases_SeparateMeasures

theorem sound : TinylambOpt.ResearchGoals.Frontiers.WorkloadBases.SeparateMeasures.Goal :=
  TinylambOpt.ResearchProofs.Frontiers.WorkloadBases.SeparateMeasures_sound

end TinylambOpt.ResearchProofs.Frontiers.Goal_WorkloadBases_SeparateMeasures
