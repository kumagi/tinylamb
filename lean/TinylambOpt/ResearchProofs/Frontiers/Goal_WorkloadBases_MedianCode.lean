import TinylambOpt.ResearchProofs.Frontiers.WorkloadBases

/-!
The modeled contracts identify when an incremental SQL operator can advance its frontier
while preserving its outputs.

Proved

- `MedianCode` — `∀ (ws : List Int) center, ws ≠ [] → let m :=
  ws.mergeSort[ws.length/2]?.getD 0 (ws.map (fun w => (w-m).natAbs)).sum ≤ (ws.map
  (fun w => (w-center).natAbs)).sum`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Frontiers.Goal_WorkloadBases_MedianCode

theorem sound : TinylambOpt.ResearchGoals.Frontiers.WorkloadBases.MedianCode.Goal :=
  TinylambOpt.ResearchProofs.Frontiers.WorkloadBases.MedianCode_sound

end TinylambOpt.ResearchProofs.Frontiers.Goal_WorkloadBases_MedianCode
