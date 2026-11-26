import TinylambOpt.ResearchProofs.Frontiers.WorkloadBases

/-!
The modeled contracts identify when an incremental SQL operator can advance its frontier
while preserving its outputs.

Proved

- `NarrowWeights` — `∀ n bits (lo : Int) (row weight : Vec), (∀ j < n, lo ≤ weight j ∧
  weight j < lo + 2^bits) → dot n row weight = lo * total n row + total bits (fun b
  => 2^b * total n (fun j => if (weight j-lo).toNat.testBit b then row j else 0))`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Frontiers.Goal_WorkloadBases_NarrowWeights

theorem sound : TinylambOpt.ResearchGoals.Frontiers.WorkloadBases.NarrowWeights.Goal :=
  TinylambOpt.ResearchProofs.Frontiers.WorkloadBases.NarrowWeights_sound

end TinylambOpt.ResearchProofs.Frontiers.Goal_WorkloadBases_NarrowWeights
