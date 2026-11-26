import TinylambOpt.ResearchGoals.StorageExecution.GatherPlans

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_GatherPlans_QuadraticDecode_EqualLengthSplit

open TinylambOpt.ResearchGoals.StorageExecution.GatherPlans
open TinylambOpt.ResearchGoals.StorageExecution.GatherPlans.QuadraticDecode

private theorem span_whole (a n : Nat) (hn : 0 < n) : span a (a + 2*n - 1) = 2*n := by
  unfold span
  omega

private theorem span_left (a n : Nat) (hn : 0 < n) : span a (a + n - 1) = n := by
  unfold span
  omega

private theorem span_right (a n : Nat) (hn : 0 < n) : span (a + n) (a + 2*n - 1) = n := by
  unfold span
  omega

private theorem square_twice (n : Nat) : (2*n)^2 = 4*n^2 := by
  rw [Nat.mul_pow]

theorem sound_EqualLengthSplit : QuadraticDecode.EqualLengthSplit.Goal := by
  intro fee a n hn
  unfold price
  rw [span_whole a n hn, span_left a n hn, span_right a n hn]
  rw [square_twice n]
  omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_GatherPlans_QuadraticDecode_EqualLengthSplit
