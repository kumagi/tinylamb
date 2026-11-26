import TinylambOpt.ResearchGoals.JitKernels.NullWordFusion

/-!
**Proved: COALESCE null intersection.** De Morgan's law turns the null mask of
the bitwise input intersection into the union of the masked validity words, so
`valid n (a &&& b) = valid n a ||| valid n b` holds for every row count. SQL
execution may derive COALESCE validity from the two input null masks without
materializing per-lane values.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.NullWordFusion_CoalesceNullIntersection
open TinylambOpt.ResearchGoals.JitKernels.NullWordFusion

theorem sound : CoalesceNullIntersection.Goal := by
  intro a b n
  apply BitVec.eq_of_getLsbD_eq_iff.mpr
  intro i hi
  simp only [valid, BitVec.getLsbD_and, BitVec.getLsbD_or, BitVec.getLsbD_not]
  rw [show decide (i < 64) = true from decide_eq_true hi]
  cases hw : (window n).getLsbD i <;> cases ha : a.getLsbD i <;>
    cases hb : b.getLsbD i <;> simp [hw, ha, hb]

end TinylambOpt.ResearchProofs.JitKernels.NullWordFusion_CoalesceNullIntersection
