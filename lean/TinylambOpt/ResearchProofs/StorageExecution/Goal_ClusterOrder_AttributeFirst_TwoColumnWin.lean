import TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_ClusterOrder_AttributeFirst_TwoColumnWin

open TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder
open TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder.AttributeFirst

-- Truth check on the smallest admissible input (confirmed by #eval/native_decide):
--   rs = [(0,0),(1,1)] has rs.eraseDups.length = 2, so 1 < 2 holds, but
--   layoutBytes AttributeFirst.key rs = 10 and both rs = 8, hence 10 <= 8 is false.
--   Swapping each row gives keys (0,0) and (1,1), so the permutation is already
--   the identity; neither column gains any RLE savings, yet the format tag (1)
--   and the sidecar add cost on top of the unchanged RLE payload.

theorem disproof_TwoColumnWin : ¬ AttributeFirst.TwoColumnWin.Goal := by
  intro h
  have hle : layoutBytes AttributeFirst.key [(0, 0), (1, 1)] ≤ both [(0, 0), (1, 1)] :=
    h [(0, 0), (1, 1)] (by native_decide)
  exact (by native_decide : ¬ (layoutBytes AttributeFirst.key [(0, 0), (1, 1)] ≤ both [(0, 0), (1, 1)])) hle

end TinylambOpt.ResearchProofs.StorageExecution.Goal_ClusterOrder_AttributeFirst_TwoColumnWin
