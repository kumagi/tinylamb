import TinylambOpt.ResearchGoals.JitKernels.NullWordFusion

/-!
**Proved: branchless CASE null mask.** At every physical lane, selecting the
then/else null bit with the mask `truth &&& ~~~predNull` agrees with scalar
CASE evaluation, including NULL predicate lanes taking the else branch. SQL
execution may replace per-lane predicate tests with this bit select.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.NullWordFusion_BranchNullMaskMux
open TinylambOpt.ResearchGoals.JitKernels.NullWordFusion

theorem sound : BranchNullMaskMux.Goal := by
  intro truth predNull thenNull elseNull i hi
  dsimp only
  simp only [BitVec.getLsbD_and, BitVec.getLsbD_or, BitVec.getLsbD_not,
    decide_eq_true hi, Bool.true_and]
  cases ht : truth.getLsbD i <;> cases hp : predNull.getLsbD i <;>
    cases hn : thenNull.getLsbD i <;> cases he : elseNull.getLsbD i <;>
    simp_all

end TinylambOpt.ResearchProofs.JitKernels.NullWordFusion_BranchNullMaskMux
