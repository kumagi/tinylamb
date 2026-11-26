import TinylambOpt.ResearchGoals.JitKernels.PredicatedStores

/-! Two generated stores whose byte intervals are disjoint commute. At every
    address at most one store can replace the original byte, so SQL may emit
    the pair in either order without changing the memory image observed by a
    later read. -/

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_PredicatedStores_StorePairDisjointReorder

open TinylambOpt.ResearchGoals.JitKernels.PredicatedStores

theorem sound : TinylambOpt.ResearchGoals.JitKernels.PredicatedStores.StorePairDisjointReorder.Goal := by
  intro m a b xs ys hdisj
  funext i
  rcases hdisj with hab | hba
  · by_cases hbi : b ≤ i
    · have hai : a ≤ i := by omega
      have hxs : xs[i - a]? = none := List.getElem?_eq_none (by omega)
      simp [store, hbi, hai, hxs]
    · by_cases hai : a ≤ i
      · simp [store, hbi, hai]
      · simp [store, hbi, hai]
  · by_cases hai : a ≤ i
    · have hbi : b ≤ i := by omega
      have hys : ys[i - b]? = none := List.getElem?_eq_none (by omega)
      simp [store, hai, hbi, hys]
    · by_cases hbi : b ≤ i
      · simp [store, hai, hbi]
      · simp [store, hai, hbi]

end TinylambOpt.ResearchProofs.JitKernels.Goal_PredicatedStores_StorePairDisjointReorder
