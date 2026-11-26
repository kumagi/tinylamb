import TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Common_PredicateBallots
import TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Lanes_PredicateBallots

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Goal_Coalesce_SplitActive

open TinylambOpt.ResearchGoals.JitFrontiers.PredicateBallots
open TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Common
open TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Lanes

theorem sound : Coalesce.SplitActive.Goal := by
  unfold Coalesce.SplitActive.Goal splitActive
  intro m n a b ha hb
  simp [Coalesce.test, well, gate] at ha hb ⊢
  refine ⟨Common.bitvec_eq_of_lanes _ _ (fun i hi => ?_), Common.bitvec_eq_of_lanes _ _ (fun i hi => ?_), Common.bitvec_eq_of_lanes _ _ (fun i hi => ?_)⟩
  all_goals
    have ha1 := congrArg (fun w : W => w.getLsbD i) ha.1
    have ha2 := congrArg (fun w : W => w.getLsbD i) ha.2.1
    have ha3 := congrArg (fun w : W => w.getLsbD i) ha.2.2
    have hb1 := congrArg (fun w : W => w.getLsbD i) hb.1
    have hb2 := congrArg (fun w : W => w.getLsbD i) hb.2.1
    have hb3 := congrArg (fun w : W => w.getLsbD i) hb.2.2
    simp only [BitVec.getLsbD_and, BitVec.getLsbD_or, BitVec.getLsbD_xor, BitVec.getLsbD_zero, BitVec.getLsbD_not, hi] at *
    by_cases h0 : m.getLsbD i <;> by_cases h1 : n.getLsbD i <;> simp_all <;> decide

end TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Goal_Coalesce_SplitActive
