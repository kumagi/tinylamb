import TinylambOpt.ResearchGoals.JitKernels.TierInvestment

/-! A positive-net trace amortizes any finite compilation cost after enough repetitions. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.TierInvestment.Goal_TierInvestment_CompiledSuffixDuplication

open TinylambOpt.ResearchGoals.JitKernels.TierInvestment
open TinylambOpt.ResearchGoals.JitKernels.TierInvestment.CompiledSuffixDuplication

theorem sum_net (t : Tariff) (xs : List Batch) :
    (xs.map (net t)).sum = ((xs.map (scalar t)).sum : Int) - ((xs.map (compiled t)).sum : Int) := by
  induction xs with
  | nil => simp
  | cons b xs ih =>
      simp [net, ih]
      omega

theorem bill_zero_lt_full (t : Tariff) (ys : List Batch) :
    bill t ys 0 < bill t ys ys.length ↔ (t.compile : Int) < (ys.map (net t)).sum := by
  have hn := sum_net t ys
  cases ys with
  | nil => simp [bill]
  | cons b ys =>
      simp [bill] at hn ⊢
      omega

theorem sum_flatten_replicate (ys : List Int) (n : Nat) :
    ((List.replicate n ys).flatten).sum = (n : Int) * ys.sum := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.replicate_succ]
      simp [ih, Int.add_mul]
      omega

theorem net_sum_replicate (t : Tariff) (xs : List Batch) (n : Nat) :
    (((List.replicate n xs).flatten).map (net t)).sum = (n : Int) * (xs.map (net t)).sum := by
  rw [List.map_flatten, List.map_replicate]
  exact sum_flatten_replicate (xs.map (net t)) n

theorem sound : CompiledSuffixDuplication.Goal := by
  intro t xs hpos
  have hSge : (1 : Int) ≤ (xs.map (net t)).sum := by omega
  refine ⟨t.compile + 1, ?_⟩
  rw [bill_zero_lt_full]
  rw [net_sum_replicate]
  have hnnonneg : (0 : Int) ≤ (t.compile + 1 : Nat) := by exact_mod_cast (Nat.zero_le (t.compile + 1))
  have hmul := Int.mul_le_mul_of_nonneg_left hSge hnnonneg
  have hmul' : ((t.compile + 1 : Nat) : Int) ≤ ((t.compile + 1 : Nat) : Int) * (xs.map (net t)).sum := by
    simpa using hmul
  omega

end TinylambOpt.ResearchProofs.JitKernels.TierInvestment.Goal_TierInvestment_CompiledSuffixDuplication
