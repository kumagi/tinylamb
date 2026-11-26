import TinylambOpt.ResearchProofs.Frontiers.WorkloadBases

/-!
The modeled contracts identify when an incremental SQL operator can advance its frontier
while preserving its outputs.

Proved

- `Indistinguishable` — `∀ n k (a b : Vec) (basis : Mat), (∀ h < k, dot n a (basis h) =
  dot n b (basis h)) ↔ ∀ c : Vec, dot n a (fun j => dot k c (fun h => basis h j)) =
  dot n b (fun j => dot k c (fun h => basis h j))`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Frontiers.Goal_WorkloadBases_Indistinguishable

theorem sound : TinylambOpt.ResearchGoals.Frontiers.WorkloadBases.Indistinguishable.Goal :=
  TinylambOpt.ResearchProofs.Frontiers.WorkloadBases.Indistinguishable_sound

end TinylambOpt.ResearchProofs.Frontiers.Goal_WorkloadBases_Indistinguishable
