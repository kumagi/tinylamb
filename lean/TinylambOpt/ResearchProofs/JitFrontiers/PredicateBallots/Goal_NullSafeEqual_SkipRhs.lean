import TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Common_PredicateBallots
import TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Lanes_PredicateBallots

/-!
Vector predicates use lane masks to carry the SQL truth or selection state into later
operators.

Proved

- `NullSafeEqual.SkipRhs` — Replacing arbitrary RHS flags outside the generated demand
  mask leaves all live outputs unchanged.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Goal_NullSafeEqual_SkipRhs

open TinylambOpt.ResearchGoals.JitFrontiers.PredicateBallots
open TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Common
open TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Lanes

theorem sound : NullSafeEqual.SkipRhs.Goal := by
  unfold NullSafeEqual.SkipRhs.Goal skipRhs
  intro m a b c ha hb hc h
  simp [NullSafeEqual.test, well, zeros, gate] at ha hb hc h ⊢
  refine ⟨Common.bitvec_eq_of_lanes _ _ (fun i hi => ?_), Common.bitvec_eq_of_lanes _ _ (fun i hi => ?_)⟩
  all_goals
    have ha1 := congrArg (fun w : W => w.getLsbD i) ha.1
    have ha2 := congrArg (fun w : W => w.getLsbD i) ha.2.1
    have ha3 := congrArg (fun w : W => w.getLsbD i) ha.2.2
    have hb1 := congrArg (fun w : W => w.getLsbD i) hb.1
    have hb2 := congrArg (fun w : W => w.getLsbD i) hb.2.1
    have hb3 := congrArg (fun w : W => w.getLsbD i) hb.2.2
    have hc1 := congrArg (fun w : W => w.getLsbD i) hc.1
    have hc2 := congrArg (fun w : W => w.getLsbD i) hc.2.1
    have hc3 := congrArg (fun w : W => w.getLsbD i) hc.2.2
    have hz1 := congrArg (fun w : W => w.getLsbD i) h.1
    have hz2 := congrArg (fun w : W => w.getLsbD i) h.2.1
    have hz3 := congrArg (fun w : W => w.getLsbD i) h.2.2
    simp only [BitVec.getLsbD_and, BitVec.getLsbD_or, BitVec.getLsbD_xor, BitVec.getLsbD_zero, BitVec.getLsbD_not, hi] at *
    by_cases h0 : m.getLsbD i <;> by_cases h1 : a.yes.getLsbD i <;> by_cases h2 : a.nul.getLsbD i <;>
    by_cases h3 : a.err.getLsbD i <;> simp_all <;> decide

end TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Goal_NullSafeEqual_SkipRhs
