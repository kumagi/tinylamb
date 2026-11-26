import TinylambOpt.ResearchGoals.JitKernels.PredicatedStores

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_PredicatedStores_MergeAdjacentNarrowStores

open TinylambOpt.ResearchGoals.JitKernels.PredicatedStores

theorem sound : TinylambOpt.ResearchGoals.JitKernels.PredicatedStores.MergeAdjacentNarrowStores.Goal := by
  intro m a xs ys
  funext i
  simp only [store]
  by_cases hi : a ≤ i
  · simp only [if_pos hi]
    by_cases hix : i - a < xs.length
    · have hnot : ¬ a + xs.length ≤ i := by omega
      simp only [if_neg hnot]
      rw [List.getElem?_append]
      simp [hix]
    · have hge : a + xs.length ≤ i := by omega
      simp only [if_pos hge]
      rw [List.getElem?_append]
      simp [hix, Nat.sub_sub]
  · simp only [if_neg hi]
    have hnot : ¬ a + xs.length ≤ i := by omega
    simp only [if_neg hnot]

end TinylambOpt.ResearchProofs.JitKernels.Goal_PredicatedStores_MergeAdjacentNarrowStores
