import TinylambOpt.ResearchGoals.JitKernels.InListLowering

/-!
A biased bitset range check folds an out-of-domain probe value into the hot
interval. When the folded value is an actual build key, the compact bitset
accepts a probe that a residual comparison of the original key would reject.
`xs = [1]`, `lo = 0`, `n = 3`, `x = 4` is such a witness.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.InListLowering.Goal_BiasedBitsetOutlier

open TinylambOpt.ResearchGoals.JitKernels.InListLowering

theorem refute : ¬ BiasedBitsetOutlier.Goal := by
  intro h
  have hprem : ∀ v, some v ∈ ([some (1 : Int)] : Keys) → 0 ≤ v ∧ v < 0 + (3 : Nat) := by
    intro v hv
    simp at hv
    omega
  have t := h [some (1 : Int)] 0 3 4 (by omega) hprem
  simp [test, keep] at t

end TinylambOpt.ResearchProofs.JitKernels.InListLowering.Goal_BiasedBitsetOutlier
