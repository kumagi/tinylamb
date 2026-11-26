import TinylambOpt.ResearchProofs.Frontiers.WorkloadBases

/-!
The modeled contracts identify when an incremental SQL operator can advance its frontier
while preserving its outputs.

Proved

- `EnergyBound` — `∀ n (row actual code : Vec), (dot n row actual - dot n row code)^2 ≤
  dot n row row * total n (fun j => (actual j-code j)^2)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Frontiers.Goal_WorkloadBases_EnergyBound

theorem sound : TinylambOpt.ResearchGoals.Frontiers.WorkloadBases.EnergyBound.Goal :=
  TinylambOpt.ResearchProofs.Frontiers.WorkloadBases.EnergyBound_sound

end TinylambOpt.ResearchProofs.Frontiers.Goal_WorkloadBases_EnergyBound
