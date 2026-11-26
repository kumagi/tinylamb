import TinylambOpt.ResearchGoals.JitKernels.FusionTraffic

/-! FALSE: Sharing a cache line does not amortize the page guard. Counterexample: base 0,
stride 1, width 4, rows `i = 0` and `j = 1` sit on line `0`, and row `0` is below limit 1,
but row `1` is not — the line spans the page boundary, so each speculative load in the
line still needs its own bound check. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.FusionTraffic_PageGuardAmortizedByLineHit
open TinylambOpt.ResearchGoals.JitKernels.FusionTraffic

/-- Refutation of line-level amortization of page guards. -/
theorem refute : ¬ PageGuardAmortizedByLineHit.Goal := by
  intro h
  have key := h 0 1 4 0 1 1 (by decide) (by decide) (by decide)
  exact absurd key (by decide)

end TinylambOpt.ResearchProofs.JitKernels.FusionTraffic_PageGuardAmortizedByLineHit
