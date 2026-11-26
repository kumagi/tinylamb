import TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder

/-!
Ordered SQL scans and limits depend on preserving key order and ties across physical
clusters.

Refuted by counterexample

- `DyadicTile.TwoColumnWin` — Aggressive hypothesis: the combined RLE payload always
  compensates for the permutation on nonconstant pages.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_ClusterOrder_DyadicTile_TwoColumnWin

open TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder
open TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder.DyadicTile

-- Truth check on the smallest admissible input (confirmed by #eval/native_decide):
--   rs = [(0,0),(1,1)] has rs.eraseDups.length = 2, so 1 < 2 holds.
--   DyadicTile.key sends them to (0,0) and (morton 0 0, 17) = (0,17), hence
--   perm is the identity and both reordered columns are unchanged. There is no
--   RLE saving, but the format tag and the sidecar still add cost:
--   layoutBytes DyadicTile.key rs = 10 and both rs = 8, so 10 <= 8 is false.
--   The counterexample is key independent, as for Lexicographic/Interleaved.

theorem disproof_TwoColumnWin : ¬ DyadicTile.TwoColumnWin.Goal := by
  intro h
  have hle : layoutBytes DyadicTile.key [(0, 0), (1, 1)] ≤ both [(0, 0), (1, 1)] :=
    h [(0, 0), (1, 1)] (by native_decide)
  exact (by native_decide : ¬ (layoutBytes DyadicTile.key [(0, 0), (1, 1)] ≤ both [(0, 0), (1, 1)])) hle

end TinylambOpt.ResearchProofs.StorageExecution.Goal_ClusterOrder_DyadicTile_TwoColumnWin
