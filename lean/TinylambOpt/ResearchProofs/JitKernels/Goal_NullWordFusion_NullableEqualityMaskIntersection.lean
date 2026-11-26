import TinylambOpt.ResearchGoals.JitKernels.NullWordFusion

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_NullWordFusion_NullableEqualityMaskIntersection

open TinylambOpt.ResearchGoals.JitKernels.NullWordFusion

theorem sound : NullableEqualityMaskIntersection.Goal := by
  intro a b equalPayload i hi
  simp only [BitVec.getLsbD_and, BitVec.getLsbD_or, BitVec.getLsbD_not]
  rw [show decide (i < 64) = true from decide_eq_true hi]
  cases ha : a.getLsbD i <;> cases hb : b.getLsbD i <;>
    cases he : equalPayload.getLsbD i <;> simp [ha, hb, he]

end TinylambOpt.ResearchProofs.JitKernels.Goal_NullWordFusion_NullableEqualityMaskIntersection
