import TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder

/-!
Ordered SQL scans and limits depend on preserving key order and ties across physical
clusters.

Proved

- `PowerBucket.DenseBitBounds` — `∀ rs w, (((physical key rs).filter (fun p => (key
  p).1==w)).map Prod.fst).all (fun x => x<2^w)=true`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_ClusterOrder_PowerBucket_DenseBitBounds

open TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder
open TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder.PowerBucket
open TinylambOpt.ResearchGoals.Compression.Wire

/-- `width n` is the bit length of `n`, so every value fits in its own width.
    `Nat.lt_log2_self` gives the nonzero case; zero has width zero. -/
private theorem width_bound (n : Nat) : n < 2 ^ width n := by
  by_cases h : n == 0
  · have hn : n = 0 := by simpa using h
    subst n
    simp [width]
  · simp [width, h]
    exact Nat.lt_log2_self

theorem sound_DenseBitBounds : PowerBucket.DenseBitBounds.Goal := by
  intro rs w
  rw [List.all_eq_true]
  intro x hx
  rcases List.mem_map.mp hx with ⟨p, hp, rfl⟩
  have hkw : (((key p).1 == w) = true) := (List.mem_filter.mp hp).2
  have hw : width p.1 = w := by
    simpa [key] using hkw
  simpa [hw] using width_bound p.1

end TinylambOpt.ResearchProofs.StorageExecution.Goal_ClusterOrder_PowerBucket_DenseBitBounds
