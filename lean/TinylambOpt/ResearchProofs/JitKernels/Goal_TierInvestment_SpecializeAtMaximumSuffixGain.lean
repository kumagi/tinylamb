import TinylambOpt.ResearchGoals.JitKernels.TierInvestment

/-! Among schedules that compile once, the smallest bill is obtained at the suffix with the largest net gain; the comparison is an identity between bill differences and suffix-gain differences. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_TierInvestment_SpecializeAtMaximumSuffixGain

open TinylambOpt.ResearchGoals.JitKernels.TierInvestment
open TinylambOpt.ResearchGoals.JitKernels.TierInvestment.SpecializeAtMaximumSuffixGain

theorem sum_net (t : Tariff) (xs : List Batch) :
    (xs.map (net t)).sum = ((xs.map (scalar t)).sum : Int) - ((xs.map (compiled t)).sum : Int) := by
  induction xs with
  | nil => simp
  | cons b xs ih =>
      simp [net, ih]
      omega

theorem bill_sub_bill (t : Tariff) (xs : List Batch) (i j : Nat) (hi : i < xs.length) (hj : j < xs.length) :
    (bill t xs i : Int) - (bill t xs j) =
      ((xs.drop j).map (net t)).sum - ((xs.drop i).map (net t)).sum := by
  let S := xs.map (scalar t)
  let C := xs.map (compiled t)
  have hsc_i : (S.take i).sum + (S.drop i).sum = S.sum := by
    rw [← List.sum_append, List.take_append_drop]
  have hsc_j : (S.take j).sum + (S.drop j).sum = S.sum := by
    rw [← List.sum_append, List.take_append_drop]
  have hco_i : (C.take i).sum + (C.drop i).sum = C.sum := by
    rw [← List.sum_append, List.take_append_drop]
  have hco_j : (C.take j).sum + (C.drop j).sum = C.sum := by
    rw [← List.sum_append, List.take_append_drop]
  have hni : (List.drop i (xs.map (net t))).sum = ((List.drop i S).sum : Int) - ((List.drop i C).sum : Int) := by
    simpa [S, C, ← List.map_drop] using sum_net t (xs.drop i)
  have hnj : (List.drop j (xs.map (net t))).sum = ((List.drop j S).sum : Int) - ((List.drop j C).sum : Int) := by
    simpa [S, C, ← List.map_drop] using sum_net t (xs.drop j)
  simp [bill, hi, hj, S, C, List.map_drop, List.map_take] at hni hnj hsc_i hsc_j hco_i hco_j ⊢
  omega

theorem sound : TinylambOpt.ResearchGoals.JitKernels.TierInvestment.SpecializeAtMaximumSuffixGain.Goal := by
  intro t xs i j hi hj
  have hs := bill_sub_bill t xs i j hi hj
  constructor <;> intro h <;> omega

end TinylambOpt.ResearchProofs.JitKernels.Goal_TierInvestment_SpecializeAtMaximumSuffixGain
