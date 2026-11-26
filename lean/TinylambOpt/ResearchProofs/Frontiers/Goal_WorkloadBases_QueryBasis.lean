import TinylambOpt.ResearchProofs.Frontiers.WorkloadBases

/-!
The modeled contracts identify when an incremental SQL operator can advance its frontier
while preserving its outputs.

Proved

- `QueryBasis` — `∀ n k (row coeff : Vec) (basis : Mat), dot n row (fun j => dot k
  coeff (fun h => basis h j)) = dot k coeff (fun h => dot n row (basis h))`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Frontiers.Goal_WorkloadBases_QueryBasis

theorem sound : TinylambOpt.ResearchGoals.Frontiers.WorkloadBases.QueryBasis.Goal :=
  TinylambOpt.ResearchProofs.Frontiers.WorkloadBases.QueryBasis_sound

end TinylambOpt.ResearchProofs.Frontiers.Goal_WorkloadBases_QueryBasis
