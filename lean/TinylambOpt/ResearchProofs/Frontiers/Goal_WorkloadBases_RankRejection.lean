import TinylambOpt.ResearchProofs.Frontiers.WorkloadBases

/-!
The modeled contracts identify when an incremental SQL operator can advance its frontier
while preserving its outputs.

Proved

- `RankRejection` — `∀ n m r (l t : Mat), binaryRank n m (fun i j => dot r (l i) (fun h
  => t h j)) ≤ r`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Frontiers.Goal_WorkloadBases_RankRejection

theorem sound : TinylambOpt.ResearchGoals.Frontiers.WorkloadBases.RankRejection.Goal :=
  TinylambOpt.ResearchProofs.Frontiers.WorkloadBases.RankRejection_sound

end TinylambOpt.ResearchProofs.Frontiers.Goal_WorkloadBases_RankRejection
