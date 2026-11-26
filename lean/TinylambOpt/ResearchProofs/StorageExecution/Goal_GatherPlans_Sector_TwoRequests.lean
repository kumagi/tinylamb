import TinylambOpt.ResearchGoals.StorageExecution.GatherPlans

/-!
Late materialization can gather payloads at selected row positions while preserving the
selection's order and multiplicities.

Proved

- `Sector.TwoRequests` — `∀ (shift startup : Nat), twoRequests (price shift startup)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_GatherPlans_Sector_TwoRequests

open TinylambOpt.ResearchGoals.StorageExecution.GatherPlans
open TinylambOpt.ResearchGoals.StorageExecution.GatherPlans.Sector

private theorem points_pair (x y : Nat) (hxy : x < y) : points [x,y] = [x,y] := by
  unfold points
  have hne : y ≠ x := Nat.ne_of_gt hxy
  simp [List.eraseDups_cons, hne, List.mergeSort_pair, Nat.le_of_lt hxy]

private theorem covers_pair (x y : Nat) : covers [x,y] = [[(x,x),(y,y)], [(x,y)]] := by
  simp [covers]

theorem sound_TwoRequests : Sector.TwoRequests.Goal := by
  intro shift startup x y hxy
  have hpts : points [x,y] = [x,y] := points_pair x y hxy
  have hcov : covers [x,y] = [[(x,x),(y,y)], [(x,y)]] := covers_pair x y
  let a := price shift startup x x + price shift startup y y
  let b := price shift startup x y
  by_cases hle : a ≤ b
  · simp [choose, hpts, hcov, cost, a, b, hle, Nat.min_eq_left hle]
  · have hge : b ≤ a := Nat.le_of_not_ge hle
    simp [choose, hpts, hcov, cost, a, b, hle, Nat.min_eq_right hge]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_GatherPlans_Sector_TwoRequests
