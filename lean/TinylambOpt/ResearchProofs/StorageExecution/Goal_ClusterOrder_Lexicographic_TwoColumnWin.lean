import TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_ClusterOrder_Lexicographic_TwoColumnWin

open TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder
open TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder.Lexicographic

-- Truth check on the smallest admissible input:
--   rs = [(0,0),(1,1)] has rs.eraseDups.length = 2, so 1 < 2 holds, but
--   layoutBytes Lexicographic.key rs = 10 and both rs = 8, hence 10 <= 8 is false.
--   (The goals module itself contains the matching #guard
--    `layoutBytes Lexicographic.key [(0,0),(1,1)] > both [(0,0),(1,1)]`.)

theorem disproof_TwoColumnWin : ¬ Lexicographic.TwoColumnWin.Goal := by
  intro h
  have hle : layoutBytes Lexicographic.key [(0, 0), (1, 1)] ≤ both [(0, 0), (1, 1)] :=
    h [(0, 0), (1, 1)] (by native_decide)
  exact (by native_decide : ¬ (layoutBytes Lexicographic.key [(0, 0), (1, 1)] ≤ both [(0, 0), (1, 1)])) hle

end TinylambOpt.ResearchProofs.StorageExecution.Goal_ClusterOrder_Lexicographic_TwoColumnWin
