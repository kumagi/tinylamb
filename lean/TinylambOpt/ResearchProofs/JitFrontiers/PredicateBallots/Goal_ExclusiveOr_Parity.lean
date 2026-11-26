import TinylambOpt.ResearchGoals.JitFrontiers.PredicateBallots
import TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Common_PredicateBallots

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Goal_ExclusiveOr_Parity

open TinylambOpt.ResearchGoals.JitFrontiers.PredicateBallots

theorem sound : ExclusiveOr.Parity.Goal := by
  intro a b hn hn2 he he2
  simp [ExclusiveOr.test, strictBits] at *
  apply Common.bitvec_eq_of_lanes
  intro i hi
  have hz1 := congrArg (fun w : W => w.getLsbD i) hn
  have hz2 := congrArg (fun w : W => w.getLsbD i) hn2
  have hz3 := congrArg (fun w : W => w.getLsbD i) he
  have hz4 := congrArg (fun w : W => w.getLsbD i) he2
  simp only [BitVec.getLsbD_and, BitVec.getLsbD_or, BitVec.getLsbD_xor, BitVec.getLsbD_zero, BitVec.getLsbD_not, hi] at *
  by_cases h0 : a.yes.getLsbD i <;> by_cases h1 : b.yes.getLsbD i <;> simp_all <;> decide

end TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Goal_ExclusiveOr_Parity
