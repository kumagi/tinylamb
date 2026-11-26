import TinylambOpt.ResearchGoals.JitKernels.PredicatedStores

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_PredicatedStores_SplitLoadPageCrossing

theorem sound : TinylambOpt.ResearchGoals.JitKernels.PredicatedStores.SplitLoadPageCrossing.Goal := by
  intro m a n k hk
  rw [← Nat.add_sub_of_le hk]
  simp [TinylambOpt.ResearchGoals.JitKernels.PredicatedStores.read, List.range_add, Nat.add_assoc]

end TinylambOpt.ResearchProofs.JitKernels.Goal_PredicatedStores_SplitLoadPageCrossing
