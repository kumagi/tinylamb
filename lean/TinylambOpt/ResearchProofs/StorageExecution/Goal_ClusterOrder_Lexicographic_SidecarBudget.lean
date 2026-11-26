import TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder

/-!
Ordered SQL scans and limits depend on preserving key order and ties across physical
clusters.

Proved

- `Lexicographic.SidecarBudget` — Decide whether RLE savings cover the materialized
  permutation and format tag.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_ClusterOrder_Lexicographic_SidecarBudget

open TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder
open TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder.Lexicographic
open TinylambOpt.ResearchGoals.Compression.Wire

-- Truth check on small inputs.
private def genRows : Nat → List (List Row)
  | 0 => [[]]
  | n + 1 => (genRows n).flatMap (fun xs => [(0, 0), (1, 0), (0, 1), (2, 3), (7, 7)].map (fun r => r :: xs))

#eval (List.range 4).all (fun n => (genRows n).all (fun rs =>
  (layoutBytes key rs < both rs) == (both (physical key rs) + orderBytes rs.length + 1 < both rs)))

private theorem perm_length (rs : List Row) : (perm key rs).length = rs.length := by
  simp [perm]

private theorem side_length (rs : List Row) : (side key rs).length = orderBytes rs.length := by
  simp [side, orderBytes, pack, little, perm_length]

private theorem layoutBytes_eq (rs : List Row) :
    layoutBytes key rs = 1 + orderBytes rs.length + both (physical key rs) := by
  unfold layoutBytes
  rw [side_length]

theorem sound_SidecarBudget : Lexicographic.SidecarBudget.Goal := by
  intro rs
  rw [layoutBytes_eq]
  omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_ClusterOrder_Lexicographic_SidecarBudget
