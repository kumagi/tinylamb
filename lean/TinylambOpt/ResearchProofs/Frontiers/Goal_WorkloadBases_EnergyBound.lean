import TinylambOpt.ResearchProofs.Frontiers.WorkloadBases

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Frontiers.Goal_WorkloadBases_EnergyBound

theorem sound : TinylambOpt.ResearchGoals.Frontiers.WorkloadBases.EnergyBound.Goal :=
  TinylambOpt.ResearchProofs.Frontiers.WorkloadBases.EnergyBound_sound

end TinylambOpt.ResearchProofs.Frontiers.Goal_WorkloadBases_EnergyBound
