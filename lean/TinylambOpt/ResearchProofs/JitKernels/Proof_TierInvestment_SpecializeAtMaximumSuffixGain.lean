import TinylambOpt.ResearchGoals.JitKernels.TierInvestment

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-!
Soundness of "select the most profitable nonempty suffix to minimize total work": among
schedules that compile exactly once (cut strictly inside the trace), `bill t xs k` is the
common quantity `all-scalar work + compile` minus the realized net savings of the
compiled suffix, so comparing two candidate cuts is exactly comparing their suffix savings
in the opposite direction. A tier chooser may therefore pick the cut maximizing suffix
gain; SQL plan cost is minimized without simulating any schedule.
-/
namespace TinylambOpt.ResearchProofs.JitKernels.TierInvestment_SpecializeAtMaximumSuffixGain
open TinylambOpt.ResearchGoals.JitKernels.TierInvestment

private theorem sum_scalar_take_drop (t : Tariff) : ∀ (xs : List Batch) (k : Nat),
    ((xs.take k).map (scalar t)).sum + ((xs.drop k).map (scalar t)).sum = (xs.map (scalar t)).sum := by
  intro xs
  induction xs with
  | nil => intro k; cases k <;> rfl
  | cons a as ih =>
      intro k
      cases k with
      | zero => simp
      | succ k =>
          have h := ih k
          rw [List.take_succ_cons, List.drop_succ_cons]
          simp only [List.map_cons, List.sum_cons]
          omega

private theorem sum_net (t : Tariff) : ∀ (l : List Batch),
    (l.map (net t)).sum = ((l.map (scalar t)).sum : Int) - ((l.map (compiled t)).sum : Int) := by
  intro l
  induction l with
  | nil => rfl
  | cons b bs ih =>
      rw [List.map_cons, List.map_cons, List.map_cons,
        List.sum_cons, List.sum_cons, List.sum_cons, ih]
      simp only [net]
      omega

private theorem bill_eq (t : Tariff) : ∀ (xs : List Batch) (k : Nat), k < xs.length →
    (bill t xs k : Int) = ((xs.map (scalar t)).sum : Int) + (t.compile : Int)
      - ((xs.drop k).map (net t)).sum := by
  intro xs k hk
  have h1 := sum_scalar_take_drop t xs k
  have h2 := sum_net t (xs.drop k)
  unfold bill
  rw [if_pos hk, h2]
  omega

theorem sound : SpecializeAtMaximumSuffixGain.Goal := by
  intro t xs i j hi hj
  rw [← Int.ofNat_le, bill_eq t xs i hi, bill_eq t xs j hj]
  omega

end TinylambOpt.ResearchProofs.JitKernels.TierInvestment_SpecializeAtMaximumSuffixGain
