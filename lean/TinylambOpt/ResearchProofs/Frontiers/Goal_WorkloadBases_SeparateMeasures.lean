import TinylambOpt.ResearchProofs.Frontiers.WorkloadBases

/-!
The modeled contracts identify when an incremental SQL operator can advance its frontier
while preserving its outputs.

Proved

- `SeparateMeasures` — `∀ (keys : List Nat) (lc ls rc rs : Vec), (classDot keys rc ls,
  classDot keys rs lc) = ((keys.map (fun j => ls j*rc j)).sum, (keys.map (fun j => lc
  j*rs j)).sum)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Frontiers.Goal_WorkloadBases_SeparateMeasures

theorem sound : TinylambOpt.ResearchGoals.Frontiers.WorkloadBases.SeparateMeasures.Goal :=
  TinylambOpt.ResearchProofs.Frontiers.WorkloadBases.SeparateMeasures_sound

end TinylambOpt.ResearchProofs.Frontiers.Goal_WorkloadBases_SeparateMeasures
