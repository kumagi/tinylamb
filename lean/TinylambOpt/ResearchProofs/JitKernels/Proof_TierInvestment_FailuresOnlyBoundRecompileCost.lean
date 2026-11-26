import TinylambOpt.ResearchGoals.JitKernels.TierInvestment

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-!
Refutation of "bounding the number of deopts also bounds excess work over the interpreter
independently of the number of tiny successful batches": every compiled batch — including
each SUCCESSFUL one — pays the guard fee, so the excess over scalar execution also grows
with the number of successful batches, which the claimed bound charges only to failures.
Counterexample: tariff `⟨0,1,1,1,0⟩` (guardFee=1, slow=fast=1, replay=0) with
`xs = [⟨1,true⟩]`: `bill 0 = 0 + (1 + 1·1) = 2` exceeds `bill len + compile +
0·(replay+guardFee) = 1` by one guard fee; padding with more tiny successful batches
widens the gap arbitrarily while the failure count stays 0. A correct bound must add
`xs.length · guardFee` (or discount successful batches separately).
-/
namespace TinylambOpt.ResearchProofs.JitKernels.TierInvestment_FailuresOnlyBoundRecompileCost
open TinylambOpt.ResearchGoals.JitKernels.TierInvestment

theorem refute : ¬ FailuresOnlyBoundRecompileCost.Goal := by
  intro h
  have h1 := h ⟨0, 1, 1, 1, 0⟩ [⟨1, true⟩] (by decide)
  exact absurd h1 (by decide)

end TinylambOpt.ResearchProofs.JitKernels.TierInvestment_FailuresOnlyBoundRecompileCost
