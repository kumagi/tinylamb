import TinylambOpt.ResearchGoals.JitKernels.NullWordFusion

/-!
**Proved: nullable equality mask intersection.** The word expression
`(a &&& b) ||| (~~~(a ||| b) &&& equalPayload)` accepts equal NULL/NULL pairs
and uses payload equality only where both sides are non-NULL, matching the
`IS NOT DISTINCT FROM` semantics lane by lane. SQL execution may fuse
`IS NOT DISTINCT FROM` validity into this mask without changing any lane's
result.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.NullWordFusion_NullableEqualityMaskIntersection
open TinylambOpt.ResearchGoals.JitKernels.NullWordFusion

theorem sound : NullableEqualityMaskIntersection.Goal := by
  intro a b equalPayload i hi
  simp only [BitVec.getLsbD_and, BitVec.getLsbD_or, BitVec.getLsbD_not,
    decide_eq_true hi, Bool.true_and]
  cases ha : a.getLsbD i <;> cases hb : b.getLsbD i <;>
    cases he : equalPayload.getLsbD i <;>
    simp [ha, hb, he]

end TinylambOpt.ResearchProofs.JitKernels.NullWordFusion_NullableEqualityMaskIntersection
