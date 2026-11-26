import TinylambOpt.ResearchGoals.JitKernels.DecimalChunks

/-! For a negative decimal value the upper range test is automatic, so the
parse succeeds exactly when the unsigned magnitude is at most `2^w`. SQL can
therefore compare the accumulated magnitude directly with the negative limit
without a separate signed endpoint check. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.DecimalChunks.Goal_NegativeAccumulatorThreshold

open TinylambOpt.ResearchGoals.JitKernels.DecimalChunks

theorem sound : NegativeAccumulatorThreshold.Goal := by
  intro w s hd hne
  have hp : (0 : Int) < (2 ^ w : Int) := by
    have hn : (0 : Nat) < 2 ^ w := Nat.pow_pos (a := 2) (n := w) (by decide)
    simpa using (Int.ofNat_lt.mpr hn)
  have hmnonneg : (0 : Int) ≤ (magnitude s : Int) := by simp
  have hneg : -(magnitude s : Int) < (2 ^ w : Int) := by omega
  by_cases hle : magnitude s ≤ 2 ^ w
  · have hleI : (magnitude s : Int) ≤ (2 ^ w : Int) := by
      simpa using (Int.ofNat_le.mpr hle)
    simp [parse, hne, hd, hleI, hneg, hle]
  · have hleI : ¬ (magnitude s : Int) ≤ (2 ^ w : Int) := by
      intro hc
      have hc' : magnitude s ≤ 2 ^ w := by
        simpa using (Int.ofNat_le.mp (by simpa using hc))
      exact hle hc'
    simp [parse, hne, hd, hleI, hle]

end TinylambOpt.ResearchProofs.JitKernels.DecimalChunks.Goal_NegativeAccumulatorThreshold
