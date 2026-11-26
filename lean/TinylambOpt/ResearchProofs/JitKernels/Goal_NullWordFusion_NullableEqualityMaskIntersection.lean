import TinylambOpt.ResearchGoals.JitKernels.NullWordFusion

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_NullWordFusion_NullableEqualityMaskIntersection

open TinylambOpt.ResearchGoals.JitKernels.NullWordFusion

theorem sound : TinylambOpt.ResearchGoals.JitKernels.NullWordFusion.NullableEqualityMaskIntersection.Goal := by
  intro a b equalPayload i hi
  simp only [BitVec.getLsbD_and, BitVec.getLsbD_or, BitVec.getLsbD_not]
  cases ha : a.getLsbD i <;> cases hb : b.getLsbD i <;> simp [ha, hb, hi]

end TinylambOpt.ResearchProofs.JitKernels.Goal_NullWordFusion_NullableEqualityMaskIntersection
