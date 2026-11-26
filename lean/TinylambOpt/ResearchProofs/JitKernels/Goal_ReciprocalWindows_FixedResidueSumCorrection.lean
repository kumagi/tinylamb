import TinylambOpt.ResearchGoals.JitKernels.ReciprocalWindows

/-!
Rows sharing a divisor remainder can be aggregated from one wide sum and one
correction: subtracting the common remainder contribution leaves an exact
multiple of the divisor. This preserves quotient aggregates in a fused SQL
division kernel.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_ReciprocalWindows_FixedResidueSumCorrection

open TinylambOpt.ResearchGoals.JitKernels.ReciprocalWindows

theorem sound : FixedResidueSumCorrection.Goal := by
  intro xs d r hd hall
  have decomp : ∀ ys : List Nat, (∀ y ∈ ys, y % d = r) →
      ys.sum = d * (ys.map (fun y => y / d)).sum + ys.length * r := by
    intro ys
    induction ys with
    | nil =>
        intro _
        simp
    | cons y ys ih =>
        intro hy
        have hy' : y % d = r := hy y (by simp)
        have hys : ∀ z ∈ ys, z % d = r := by
          intro z hz
          exact hy z (by simp [hz])
        specialize ih hys
        simp only [List.sum_cons, List.map_cons, List.length_cons]
        rw [ih]
        have hq : d * (y / d) + r = y := by
          rw [← hy']
          exact Nat.div_add_mod y d
        have step : ∀ (q Q n : Nat), d * q + r = y →
            y + (d * Q + n * r) = d * (q + Q) + (n + 1) * r := by
          intro q Q n hqq
          rw [← hqq]
          simp [Nat.mul_add, Nat.succ_mul]
          omega
        exact step (y / d) _ _ hq
  have hsum := decomp xs hall
  rw [hsum]
  rw [Nat.add_sub_cancel]
  rw [Nat.mul_comm d]
  symm
  exact Nat.mul_div_left _ hd

end TinylambOpt.ResearchProofs.JitKernels.Goal_ReciprocalWindows_FixedResidueSumCorrection
