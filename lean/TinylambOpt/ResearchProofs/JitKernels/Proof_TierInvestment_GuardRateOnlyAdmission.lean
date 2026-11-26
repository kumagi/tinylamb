import TinylambOpt.ResearchGoals.JitKernels.TierInvestment

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-!
Refutation of "two profiles with the same row total and successful batch count imply the
same compilation decision": `bill t xs 0` charges each batch through `compiled`, whose
cost depends on WHICH batches pass the guard, not just how many rows pass in aggregate.
Counterexample: with tariff `⟨0,0,1,0,0⟩` (slow=1, fast=0), the traces `[⟨1,true⟩,
⟨2,false⟩]` and `[⟨1,false⟩,⟨2,true⟩]` have equal length, equal row totals (3) and equal
pass counts (1), but the fast/slow split lands on different row counts: the bills are 2
and 1. A JIT admission rule keyed only on (length, rows, pass-count) misprices traces
whose guard outcomes are row-correlated; SQL tier admission must track per-batch outcomes.
-/
namespace TinylambOpt.ResearchProofs.JitKernels.TierInvestment_GuardRateOnlyAdmission
open TinylambOpt.ResearchGoals.JitKernels.TierInvestment

theorem refute : ¬ GuardRateOnlyAdmission.Goal := by
  intro h
  have h1 := h ⟨0, 0, 1, 0, 0⟩ [⟨1, true⟩, ⟨2, false⟩] [⟨1, false⟩, ⟨2, true⟩]
    (by decide) (by decide) (by decide)
  exact absurd h1 (by decide)

end TinylambOpt.ResearchProofs.JitKernels.TierInvestment_GuardRateOnlyAdmission
