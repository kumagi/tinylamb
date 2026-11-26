import TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_ClusterOrder_UnorderedPair_TwoColumnWin

open TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder
open TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder.UnorderedPair

-- Truth check on the smallest admissible input (confirmed by #eval):
--   rs = [(0,0),(1,1)] has rs.eraseDups.length = 2, so 1 < 2 holds, but
--   layoutBytes UnorderedPair.key rs = 10 and both rs = 8, hence 10 <= 8 is false.
-- The counterexample is key-independent (the same one refutes the
-- Lexicographic variant): UnorderedPair.key fixes (0,0) and (1,1).

theorem disproof_TwoColumnWin : ¬ UnorderedPair.TwoColumnWin.Goal := by
  intro h
  have hle : layoutBytes UnorderedPair.key [(0, 0), (1, 1)] ≤ both [(0, 0), (1, 1)] :=
    h [(0, 0), (1, 1)] (by native_decide)
  exact (by native_decide : ¬ (layoutBytes UnorderedPair.key [(0, 0), (1, 1)] ≤ both [(0, 0), (1, 1)])) hle

end TinylambOpt.ResearchProofs.StorageExecution.Goal_ClusterOrder_UnorderedPair_TwoColumnWin
