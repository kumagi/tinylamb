import TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder

/-!
Ordered SQL scans and limits depend on preserving key order and ties across physical
clusters.

Refuted by counterexample

- `RatioClass.TwoColumnWin` — Aggressive hypothesis: the combined RLE payload always
  compensates for the permutation on nonconstant pages.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_ClusterOrder_RatioClass_TwoColumnWin

open TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder
open TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder.RatioClass

-- Truth check on the smallest admissible input:
--   rs = [(0,0),(1,1)] has rs.eraseDups.length = 2, so 1 < 2 holds, but
--   RatioClass.key sorts this input identically to Lexicographic.key
--   (key (0,0) = (0,0) and key (1,1) = (0,1)), hence perm = [0,1] and
--   physical is unchanged.  Consequently
--     layoutBytes RatioClass.key rs = 10 and both rs = 8,
--   so 10 <= 8 is false.  (The goals module contains the matching #guard
--    `layoutBytes Lexicographic.key [(0,0),(1,1)] > both [(0,0),(1,1)]`;
--    the counterexample is key-independent for keys that leave this input
--    in its original order.)

theorem disproof_TwoColumnWin : ¬ RatioClass.TwoColumnWin.Goal := by
  intro h
  have hle : layoutBytes RatioClass.key [(0, 0), (1, 1)] ≤ both [(0, 0), (1, 1)] :=
    h [(0, 0), (1, 1)] (by native_decide)
  exact (by native_decide : ¬ (layoutBytes RatioClass.key [(0, 0), (1, 1)] ≤ both [(0, 0), (1, 1)])) hle

end TinylambOpt.ResearchProofs.StorageExecution.Goal_ClusterOrder_RatioClass_TwoColumnWin
