import TinylambOpt.ResearchGoals.JitKernels.PredicatedStores

/-!
Two adjacent narrow stores can be merged into one wide store. The proof checks
each byte address: it either lies in the first byte range, in the second range,
or outside both, and in each case the merged store writes the same byte.

Proved

- `MergeAdjacentNarrowStores` — packing adjacent stores preserves the modeled
  memory, so code generation may coalesce them without changing observed data.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_PredicatedStores_MergeAdjacentNarrowStores

open TinylambOpt.ResearchGoals.JitKernels.PredicatedStores

theorem sound : MergeAdjacentNarrowStores.Goal := by
  intro m a xs ys
  funext i
  by_cases ha : a ≤ i
  · by_cases hxs : i - a < xs.length
    · have hnot : ¬ a + xs.length ≤ i := by omega
      simp [store, ha, hnot, hxs, List.getElem?_append]
    · by_cases hys : (i - a) - xs.length < ys.length
      · have hge : a + xs.length ≤ i := by omega
        simp [store, ha, hge, hxs, hys, List.getElem?_append,
          show i - (a + xs.length) = (i - a) - xs.length by omega]
      · have hge : a + xs.length ≤ i := by omega
        have hidx : i - (a + xs.length) = (i - a) - xs.length := by omega
        have hny : ys[i - (a + xs.length)]? = none := by
          rw [hidx]
          exact List.getElem?_eq_none (by omega)
        have hnxy : (xs ++ ys)[i - a]? = none := by
          apply List.getElem?_eq_none
          simp
          omega
        simp [store, ha, hge, hxs, hny, hnxy]
  · simp [store, ha, show ¬ a + xs.length ≤ i by omega]

end TinylambOpt.ResearchProofs.JitKernels.Goal_PredicatedStores_MergeAdjacentNarrowStores
