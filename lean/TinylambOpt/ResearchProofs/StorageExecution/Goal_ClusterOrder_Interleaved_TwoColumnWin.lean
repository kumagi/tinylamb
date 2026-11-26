import TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_ClusterOrder_Interleaved_TwoColumnWin

open TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder
open TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder.Interleaved

-- Truth check on the smallest admissible input (confirmed by #eval/native_decide):
--   rs = [(0,0),(1,1)] has rs.eraseDups.length = 2, so 1 < 2 holds, but
--   layoutBytes Interleaved.key rs = 10 and both rs = 8, hence 10 <= 8 is false.
--   Morton codes are 0 and 3, so the permutation is the identity; neither
--   column gains any RLE savings, yet the format tag (1) and the sidecar add
--   cost on top of the unchanged RLE payload. This counterexample is key
--   independent, just as for Lexicographic/AttributeFirst.

#eval layoutBytes Interleaved.key [(0, 0), (1, 1)]
#eval both [(0, 0), (1, 1)]
#eval decide (1 < ([(0, 0), (1, 1)] : List Row).eraseDups.length)

theorem disproof_TwoColumnWin : ¬ Interleaved.TwoColumnWin.Goal := by
  intro h
  have hle : layoutBytes Interleaved.key [(0, 0), (1, 1)] ≤ both [(0, 0), (1, 1)] :=
    h [(0, 0), (1, 1)] (by native_decide)
  exact (by native_decide : ¬ (layoutBytes Interleaved.key [(0, 0), (1, 1)] ≤ both [(0, 0), (1, 1)])) hle

end TinylambOpt.ResearchProofs.StorageExecution.Goal_ClusterOrder_Interleaved_TwoColumnWin
