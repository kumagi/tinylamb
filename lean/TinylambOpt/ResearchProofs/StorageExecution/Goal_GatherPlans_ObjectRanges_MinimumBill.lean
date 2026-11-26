import TinylambOpt.ResearchGoals.StorageExecution.GatherPlans

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_GatherPlans_ObjectRanges_MinimumBill

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.GatherPlans
open TinylambOpt.ResearchGoals.StorageExecution.GatherPlans.ObjectRanges

-- 真偽確認用小さな入力（仮説 a ≤ b と span a b ≤ u+1 を満たすものだけを検査）
#eval (List.range 6).all (fun u => (List.range 6).all (fun f =>
  (List.range 8).all (fun a => (List.range 8).all (fun b =>
    a ≤ b → span a b ≤ u + 1 → price u f a b = f + u + 1))))

-- 1 ≤ n ≤ unit なら align unit n = unit （unit = 0 のときは仮説自体が矛盾する）
private theorem align_of_pos_le (unit n : Nat) (hn1 : 1 ≤ n) (hn2 : n ≤ unit) :
    align unit n = unit := by
  unfold align
  have hdiv : (n + unit - 1) / unit = 1 := by
    apply Nat.div_eq_of_lt_le
    · omega
    · omega
  rw [hdiv]
  omega

theorem sound_MinimumBill : ObjectRanges.MinimumBill.Goal := by
  intro u f a b hab hspan
  unfold ObjectRanges.price span at *
  rw [align_of_pos_le (u + 1) (b + 1 - a) (by omega) hspan]
  omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_GatherPlans_ObjectRanges_MinimumBill
