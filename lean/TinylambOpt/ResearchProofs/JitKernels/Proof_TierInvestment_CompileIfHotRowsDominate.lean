import TinylambOpt.ResearchGoals.JitKernels.TierInvestment

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-!
Refutation of "a majority of rows passing the guard guarantees tier-up pays": the
hypotheses bound only ROWS (`compile ≤ total rows`, `2·passing rows > total rows`,
`fast < slow`), while the compiled tier also levies a PER-BATCH guard fee on every batch
and a replay penalty on every failing one. Counterexample: tariff `⟨0,100,2,1,100⟩` with
`xs = [⟨3,true⟩,⟨2,false⟩]` — passing rows 3 of 5 are a majority and compile=0 — yet the
bills are 103 + 204 = 307 compiled against 2·5 = 10 scalar. A row-majority admission rule
must also bound guardFee and replay per batch (or amortize over many batches).
-/
namespace TinylambOpt.ResearchProofs.JitKernels.TierInvestment_CompileIfHotRowsDominate
open TinylambOpt.ResearchGoals.JitKernels.TierInvestment

theorem refute : ¬ CompileIfHotRowsDominate.Goal := by
  intro h
  have h1 := h ⟨0, 100, 2, 1, 100⟩ [⟨3, true⟩, ⟨2, false⟩]
    (by decide) (by decide) (by decide)
  exact absurd h1 (by decide)

end TinylambOpt.ResearchProofs.JitKernels.TierInvestment_CompileIfHotRowsDominate
