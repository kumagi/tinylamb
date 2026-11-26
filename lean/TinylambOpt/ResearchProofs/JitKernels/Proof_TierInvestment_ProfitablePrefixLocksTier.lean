import TinylambOpt.ResearchGoals.JitKernels.TierInvestment

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-!
Refutation of "once an initial prefix repays compilation, continued use of the compiled
version cannot lose over the whole trace": a deopting suffix batch costs
`guardFee + replay + slow·rows`, i.e. a fixed surcharge `guardFee + replay` ABOVE its
scalar cost on every failing batch — and that surcharge is not proportional to the
prefix's (one-time) advantage. Counterexample: tariff `⟨0,0,10,1,5⟩` with
`a = [⟨1,true⟩]` — scalar 10 vs compiled 1, so compilation already "pays for itself" —
appended with `b = 10 · ⟨1,false⟩`: the full trace bills 0 + (1 + 10·15) = 151 compiled
against 10 + 10·10 = 110 scalar. A tier lock-in decision must project the SUFFIX's guard
outcomes too, not only the prefix's realized savings.
-/
namespace TinylambOpt.ResearchProofs.JitKernels.TierInvestment_ProfitablePrefixLocksTier
open TinylambOpt.ResearchGoals.JitKernels.TierInvestment

theorem refute : ¬ ProfitablePrefixLocksTier.Goal := by
  intro h
  have h1 := h ⟨0, 0, 10, 1, 5⟩ [⟨1, true⟩] (List.replicate 10 ⟨1, false⟩)
    (by decide)
  exact absurd h1 (by decide)

end TinylambOpt.ResearchProofs.JitKernels.TierInvestment_ProfitablePrefixLocksTier
