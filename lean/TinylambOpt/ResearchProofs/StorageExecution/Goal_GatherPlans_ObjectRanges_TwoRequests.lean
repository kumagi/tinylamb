import TinylambOpt.ResearchGoals.StorageExecution.GatherPlans

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_GatherPlans_ObjectRanges_TwoRequests

open TinylambOpt.ResearchGoals.StorageExecution.GatherPlans
open TinylambOpt.ResearchGoals.StorageExecution.GatherPlans.ObjectRanges

private theorem points_pair (x y : Nat) (hxy : x < y) : points [x,y] = [x,y] := by
  unfold points
  have hne : y ≠ x := Nat.ne_of_gt hxy
  simp [List.eraseDups_cons, hne, List.mergeSort_pair, Nat.le_of_lt hxy]

private theorem covers_pair (x y : Nat) : covers [x,y] = [[(x,x),(y,y)], [(x,y)]] := by
  simp [covers]

theorem sound_TwoRequests : ObjectRanges.TwoRequests.Goal := by
  intro unit fee x y hxy
  have hpts : points [x,y] = [x,y] := points_pair x y hxy
  have hcov : covers [x,y] = [[(x,x),(y,y)], [(x,y)]] := covers_pair x y
  let a := price unit fee x x + price unit fee y y
  let b := price unit fee x y
  by_cases hle : a ≤ b
  · simp [choose, hpts, hcov, cost, a, b, hle, Nat.min_eq_left hle]
  · have hge : b ≤ a := Nat.le_of_not_ge hle
    simp [choose, hpts, hcov, cost, a, b, hle, Nat.min_eq_right hge]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_GatherPlans_ObjectRanges_TwoRequests
