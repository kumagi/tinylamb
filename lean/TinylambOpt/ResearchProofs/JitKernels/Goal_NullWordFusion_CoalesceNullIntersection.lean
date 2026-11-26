import TinylambOpt.ResearchGoals.JitKernels.NullWordFusion

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_NullWordFusion_CoalesceNullIntersection

open TinylambOpt.ResearchGoals.JitKernels.NullWordFusion

theorem sound : CoalesceNullIntersection.Goal := by
  intro a b n
  apply BitVec.eq_of_getLsbD_eq
  intro i hi
  simp only [valid, BitVec.getLsbD_and, BitVec.getLsbD_or, BitVec.getLsbD_not]
  rw [show decide (i < 64) = true from decide_eq_true hi]
  cases hw : (window n).getLsbD i <;> cases ha : a.getLsbD i <;>
    cases hb : b.getLsbD i <;> simp [hw, ha, hb]

end TinylambOpt.ResearchProofs.JitKernels.Goal_NullWordFusion_CoalesceNullIntersection
