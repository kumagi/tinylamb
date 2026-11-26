import TinylambOpt.ResearchGoals.JitKernels.TierInvestment

/-! Moving the compilation cut by one batch changes the bill by that batch's net saving, provided both schedules still compile. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.TierInvestment.Goal_TierInvestment_AdjacentTierCutDifference

open TinylambOpt.ResearchGoals.JitKernels.TierInvestment
open TinylambOpt.ResearchGoals.JitKernels.TierInvestment.AdjacentTierCutDifference

theorem drop_eq_cons_drop_succ (xs : List Batch) (i : Nat) (b : Batch) (h : xs[i]? = some b) :
    xs.drop i = b :: xs.drop (i + 1) := by
  induction xs generalizing i with
  | nil => simp at h
  | cons x xs ih =>
      cases i with
      | zero =>
          simp at h
          subst b
          simp
      | succ i =>
          simp only [List.getElem?_cons_succ] at h
          simp only [List.drop]
          exact ih i h

theorem sound : AdjacentTierCutDifference.Goal := by
  intro t xs i b hlen hget
  have hi : i < xs.length := by omega
  have htake : xs.take (i + 1) = xs.take i ++ [b] := by
    rw [List.take_add_one, hget]
    rfl
  have hdrop := drop_eq_cons_drop_succ xs i b hget
  simp [bill, hlen, hi, htake, hdrop, net]
  omega

end TinylambOpt.ResearchProofs.JitKernels.TierInvestment.Goal_TierInvestment_AdjacentTierCutDifference
