import TinylambOpt.ResearchGoals.StorageExecution.GatherPlans

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_GatherPlans_Contiguous_TwoRequests

open TinylambOpt.ResearchGoals.StorageExecution.GatherPlans
open TinylambOpt.ResearchGoals.StorageExecution.GatherPlans.Contiguous

private theorem points_pair (x y : Nat) (hxy : x < y) : points [x,y] = [x,y] := by
  unfold points
  have hne : y ≠ x := Nat.ne_of_gt hxy
  simp [List.eraseDups_cons, hne, List.mergeSort_pair, Nat.le_of_lt hxy]

private theorem covers_pair (x y : Nat) : covers [x,y] = [[(x,x),(y,y)], [(x,y)]] := by
  simp [covers]

theorem sound_TwoRequests : Contiguous.TwoRequests.Goal := by
  intro fee x y hxy
  have hpts : points [x,y] = [x,y] := points_pair x y hxy
  have hcov : covers [x,y] = [[(x,x),(y,y)], [(x,y)]] := covers_pair x y
  have hpoint : price fee x x = fee + 1 := by simp [price, span]
  have hyy : price fee y y = fee + 1 := by simp [price, span]
  have hspan : price fee x y = fee + (y + 1 - x) := by simp [price, span]
  by_cases hle : (fee + 1) + (fee + 1) ≤ fee + (y + 1 - x)
  · simp [choose, hpts, hcov, cost, hpoint, hyy, hspan, hle]
  · simp [choose, hpts, hcov, cost, hpoint, hyy, hspan, hle]
    omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_GatherPlans_Contiguous_TwoRequests
