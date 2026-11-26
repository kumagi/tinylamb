import TinylambOpt.ResearchGoals.JitFrontiers.PredicateBallots
import TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Common_PredicateBallots

/-!
Vector predicates use lane masks to carry the SQL truth or selection state into later
operators.

Proved

- `IfMissingTest.MaskNonNull` — `∀ a b, (test.emit a b).yes &&& b.nul=0`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Goal_IfMissingTest_MaskNonNull

open TinylambOpt.ResearchGoals.JitFrontiers.PredicateBallots

theorem sound : IfMissingTest.MaskNonNull.Goal := by
  intro a b
  simp [IfMissingTest.test]
  apply Common.bitvec_eq_of_lanes
  intro i hi
  simp only [BitVec.getLsbD_and, BitVec.getLsbD_or, BitVec.getLsbD_xor, BitVec.getLsbD_zero, BitVec.getLsbD_not, hi] at *
  by_cases h0 : a.nul.getLsbD i <;> by_cases h1 : b.nul.getLsbD i <;> by_cases h2 : b.err.getLsbD i <;>
  simp_all <;> decide

end TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Goal_IfMissingTest_MaskNonNull
