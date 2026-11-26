import TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Common_PredicateBallots
import TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Lanes_PredicateBallots

/-!
Vector predicates use lane masks to carry the SQL truth or selection state into later
operators.

Proved

- `NullSafeEqual.DisjointOutputs` — Every well-formed output lane has at most one tag.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Goal_NullSafeEqual_DisjointOutputs

open TinylambOpt.ResearchGoals.JitFrontiers.PredicateBallots
open TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Common
open TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Lanes

theorem sound : NullSafeEqual.DisjointOutputs.Goal := by
  unfold NullSafeEqual.DisjointOutputs.Goal disjointOutputs
  intro a b ha hb
  simp [NullSafeEqual.test, well, zeros] at ha hb ⊢
  apply Common.bitvec_eq_of_lanes
  intro i hi
  have ha1 := congrArg (fun w : W => w.getLsbD i) ha.1
  have ha2 := congrArg (fun w : W => w.getLsbD i) ha.2.1
  have ha3 := congrArg (fun w : W => w.getLsbD i) ha.2.2
  have hb1 := congrArg (fun w : W => w.getLsbD i) hb.1
  have hb2 := congrArg (fun w : W => w.getLsbD i) hb.2.1
  have hb3 := congrArg (fun w : W => w.getLsbD i) hb.2.2
  simp only [BitVec.getLsbD_and, BitVec.getLsbD_or, BitVec.getLsbD_xor, BitVec.getLsbD_zero, BitVec.getLsbD_not, hi] at *
  by_cases h0 : a.yes.getLsbD i <;> by_cases h1 : a.nul.getLsbD i <;> by_cases h2 : a.err.getLsbD i <;>
  by_cases h3 : b.yes.getLsbD i <;> by_cases h4 : b.nul.getLsbD i <;> by_cases h5 : b.err.getLsbD i <;>
  simp_all <;> decide

end TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Goal_NullSafeEqual_DisjointOutputs
