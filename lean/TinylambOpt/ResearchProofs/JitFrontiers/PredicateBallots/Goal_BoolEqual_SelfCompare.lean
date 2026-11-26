import TinylambOpt.ResearchGoals.JitFrontiers.PredicateBallots
import TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Common_PredicateBallots

/-!
Vector predicates use lane masks to carry the SQL truth or selection state into later
operators.

Proved

- `BoolEqual.SelfCompare` — `∀ a, well a → (test.emit a a).yes = ~~~(a.nul ||| a.err)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Goal_BoolEqual_SelfCompare

open TinylambOpt.ResearchGoals.JitFrontiers.PredicateBallots

theorem sound : BoolEqual.SelfCompare.Goal := by
  intro a ha
  simp [BoolEqual.test, strictBits, zeros, well] at ha ⊢
  apply Common.bitvec_eq_of_lanes
  intro i hi
  rw [show ((18446744073709551615#64 : W)) = ~~~(0 : W) by rfl]
  simp only [BitVec.getLsbD_and, BitVec.getLsbD_or, BitVec.getLsbD_xor, BitVec.getLsbD_zero, BitVec.getLsbD_not, hi] at *
  by_cases h0 : a.nul.getLsbD i <;> by_cases h1 : a.err.getLsbD i <;> simp_all <;> decide

end TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Goal_BoolEqual_SelfCompare
