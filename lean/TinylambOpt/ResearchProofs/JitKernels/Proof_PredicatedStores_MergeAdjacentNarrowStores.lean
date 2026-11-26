import TinylambOpt.ResearchGoals.JitKernels.PredicatedStores

/-!
# MergeAdjacentNarrowStores: sound

Two adjacent stores, the second starting exactly where the first ends, are
byte-for-byte equivalent to the single store of the concatenated payload, so
SQL execution may fuse them into one wide store without touching any byte
outside either destination.
-/

set_option maxHeartbeats 4000000
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.PredicatedStores_MergeAdjacentNarrowStores
open TinylambOpt.ResearchGoals.JitKernels.PredicatedStores

private theorem store_apply (m : Mem) (base : Nat) (bs : List UInt8) (i : Nat) :
    store m base bs i = if base ≤ i then bs[i - base]?.getD (m i) else m i := rfl

theorem sound : MergeAdjacentNarrowStores.Goal := by
  intro m a xs ys
  funext i
  simp only [store_apply]
  by_cases h : a ≤ i
  · by_cases h2 : a + xs.length ≤ i
    · simp only [if_pos h, if_pos h2, List.getElem?_append,
        if_neg (by omega : ¬ i - a < xs.length),
        List.getElem?_eq_none (by omega : xs.length ≤ i - a), Option.getD_none, Nat.sub_sub]
    · simp only [if_pos h, if_neg h2, List.getElem?_append,
        if_pos (by omega : i - a < xs.length)]
  · simp only [if_neg h, if_neg (by omega : ¬ a + xs.length ≤ i)]

end TinylambOpt.ResearchProofs.JitKernels.PredicatedStores_MergeAdjacentNarrowStores
