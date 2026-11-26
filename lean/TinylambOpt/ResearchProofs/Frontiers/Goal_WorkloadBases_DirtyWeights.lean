import TinylambOpt.ResearchProofs.Frontiers.WorkloadBases

/-!
The modeled contracts identify when an incremental SQL operator can advance its frontier
while preserving its outputs.

Proved

- `DirtyWeights` — `∀ n (row old new : Vec) (dirty : List Nat), dirty.Nodup → (∀ j ∈
  dirty, j < n) → (∀ j < n, old j ≠ new j → j ∈ dirty) → dot n row new = dot n row
  old + (dirty.map (fun j => row j*(new j-old j))).sum`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Frontiers.Goal_WorkloadBases_DirtyWeights

theorem sound : TinylambOpt.ResearchGoals.Frontiers.WorkloadBases.DirtyWeights.Goal :=
  TinylambOpt.ResearchProofs.Frontiers.WorkloadBases.DirtyWeights_sound

end TinylambOpt.ResearchProofs.Frontiers.Goal_WorkloadBases_DirtyWeights
