import TinylambOpt.ResearchGoals.JitKernels.ReciprocalWindows

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-!
Refutation of "sum the dividends, then divide once, when every lane has the same
nonzero remainder". Shared nonzero remainders accumulate: with `xs = [1, 1]`, `d = 2`,
`r = 1` each lane floors to `1/2 = 0`, so the per-lane quotient sum is 0, while
`([1,1].sum)/2 = 2/2 = 1`. A fused aggregate must subtract the accumulated remainders
(see FixedResidueSumCorrection), not divide the raw sum.
-/
namespace TinylambOpt.ResearchProofs.JitKernels.ReciprocalWindows_FusedQuotientSum
open TinylambOpt.ResearchGoals.JitKernels.ReciprocalWindows

theorem refute : ¬ FusedQuotientSum.Goal := by
  intro h
  have hm : ∀ x ∈ ([1, 1] : List Nat), x % 2 = 1 := by
    intro x hx
    rcases List.mem_cons.mp hx with rfl | hx
    · rfl
    · rcases List.mem_cons.mp hx with rfl | hx
      · rfl
      · exact absurd hx List.not_mem_nil
  have h' := h [1, 1] 2 1 (by omega) (by omega) hm
  exact absurd h' (by decide)

end TinylambOpt.ResearchProofs.JitKernels.ReciprocalWindows_FusedQuotientSum
