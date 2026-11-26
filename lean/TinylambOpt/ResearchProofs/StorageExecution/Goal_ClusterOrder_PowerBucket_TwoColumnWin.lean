import TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder

/-!
Ordered SQL scans and limits depend on preserving key order and ties across physical
clusters.

Refuted by counterexample

- `PowerBucket.TwoColumnWin` — Aggressive hypothesis: the combined RLE payload always
  compensates for the permutation on nonconstant pages.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_ClusterOrder_PowerBucket_TwoColumnWin

open TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder
open TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder.PowerBucket

-- Truth check on the smallest admissible input (confirmed by #eval/native_decide):
--   rs = [(0,0),(1,1)] has rs.eraseDups.length = 2, so 1 < 2 holds, but
--   layoutBytes PowerBucket.key rs = 10 and both rs = 8, hence 10 <= 8 is false.
--   PowerBucket.key groups by width first: width 0 = 0 and width 1 = 1, so the
--   keys are (0,0) and (1,1) and the permutation is the identity. Neither
--   column gains any RLE savings, yet the format tag (1) and the sidecar add
--   cost on top of the unchanged RLE payload. The counterexample is key
--   independent, just as for Lexicographic/AttributeFirst/Interleaved.

#eval layoutBytes PowerBucket.key [(0, 0), (1, 1)]
#eval both [(0, 0), (1, 1)]
#eval decide (1 < ([(0, 0), (1, 1)] : List Row).eraseDups.length)

theorem disproof_TwoColumnWin : ¬ PowerBucket.TwoColumnWin.Goal := by
  intro h
  have hle : layoutBytes PowerBucket.key [(0, 0), (1, 1)] ≤ both [(0, 0), (1, 1)] :=
    h [(0, 0), (1, 1)] (by native_decide)
  exact (by native_decide : ¬ (layoutBytes PowerBucket.key [(0, 0), (1, 1)] ≤ both [(0, 0), (1, 1)])) hle

end TinylambOpt.ResearchProofs.StorageExecution.Goal_ClusterOrder_PowerBucket_TwoColumnWin
