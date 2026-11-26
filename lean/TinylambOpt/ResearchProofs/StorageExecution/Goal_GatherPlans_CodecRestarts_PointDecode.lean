import TinylambOpt.ResearchGoals.StorageExecution.GatherPlans

/-!
Late materialization can gather payloads at selected row positions while preserving the
selection's order and multiplicities.

Proved

- `CodecRestarts.PointDecode` — `∀ k fee x, price k fee x x = fee+1+x%(k+1)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_GatherPlans_CodecRestarts_PointDecode

open TinylambOpt.ResearchGoals.StorageExecution.GatherPlans
open TinylambOpt.ResearchGoals.StorageExecution.GatherPlans.CodecRestarts

theorem sound_PointDecode : CodecRestarts.PointDecode.Goal := by
  intro k fee x
  unfold CodecRestarts.price
  have h := Nat.div_add_mod x (k + 1)
  have hc : x / (k + 1) * (k + 1) = (k + 1) * (x / (k + 1)) := Nat.mul_comm _ _
  have hle : x / (k + 1) * (k + 1) ≤ fee + x + 1 := by omega
  apply (Nat.sub_eq_iff_eq_add hle).2
  omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_GatherPlans_CodecRestarts_PointDecode
