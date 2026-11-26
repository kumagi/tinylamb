import TinylambOpt.ResearchGoals.JitKernels.TierInvestment

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-!
Refutation of "treat a fused batch as generic on any guard miss; saved dispatch overhead
always offsets lost specialization": fusing two batches under the conjunction of their
guards forces the merged batch onto the generic path, charging the larger `slow·(ra+rb)`
for the batch `a` that WOULD have specialized. The saved per-dispatch overhead is at most
one `guardFee` plus the replay saved when `b` was already failing, so the claimed
inequality fails when `slow - fast` is large relative to `guardFee` and `replay = 0`.
Counterexample: tariff `⟨0,1,5,1,0⟩` (guardFee=1, slow=5, fast=1, replay=0) with
`a = ⟨1,true⟩`, `b = ⟨1,false⟩`: fused cost `1 + 5·2 = 11` exceeds separate cost
`(1 + 1·1) + (1 + 0 + 5·1) = 8`. Lost specialization grows with row count and is NOT
always offset by the fixed dispatch savings; the fusion win only holds when the guards
agree (as in SameShapeBatchFusion), not under a mixed conjunction.
-/
namespace TinylambOpt.ResearchProofs.JitKernels.TierInvestment_MixedShapeBatchFusion
open TinylambOpt.ResearchGoals.JitKernels.TierInvestment

theorem refute : ¬ MixedShapeBatchFusion.Goal := by
  intro h
  have h1 := h ⟨0, 1, 5, 1, 0⟩ ⟨1, true⟩ ⟨1, false⟩ (by decide)
  exact absurd h1 (by decide)

end TinylambOpt.ResearchProofs.JitKernels.TierInvestment_MixedShapeBatchFusion
