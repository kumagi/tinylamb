import TinylambOpt.ResearchGoals.JitKernels.PredicatedStores

/-!
# StorePairDisjointReorder: sound

Two generated stores whose byte intervals are disjoint commute: for every
byte index the last-writer-wins result is the same regardless of issue
order, so SQL execution may reorder differently sized stores as long as
one interval lies entirely below the other.
-/

set_option maxHeartbeats 4000000
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.PredicatedStores_StorePairDisjointReorder
open TinylambOpt.ResearchGoals.JitKernels.PredicatedStores

private theorem store_apply (m : Mem) (base : Nat) (bs : List UInt8) (i : Nat) :
    store m base bs i = if base ≤ i then bs[i - base]?.getD (m i) else m i := rfl

/-- The lower store's bytes are all re-covered or missed by the higher store,
so the two issue orders agree byte-for-byte. -/
private theorem pair (m : Mem) (a b : Nat) (xs ys : List UInt8) (h : a + xs.length ≤ b) :
    store (store m a xs) b ys = store (store m b ys) a xs := by
  funext i
  simp only [store_apply]
  by_cases hbi : b ≤ i
  · have hai : a ≤ i := by omega
    simp only [if_pos hbi, if_pos hai, List.getElem?_eq_none (by omega : xs.length ≤ i - a),
      Option.getD_none]
  · by_cases hai : a ≤ i
    · simp only [if_neg hbi, if_pos hai]
    · simp only [if_neg hbi, if_neg hai]

theorem sound : StorePairDisjointReorder.Goal := by
  intro m a b xs ys hor
  cases hor with
  | inl h => exact pair m a b xs ys h
  | inr h => exact (pair m b a ys xs h).symm

end TinylambOpt.ResearchProofs.JitKernels.PredicatedStores_StorePairDisjointReorder
