import TinylambOpt.ResearchGoals.JitKernels.PredicatedStores

/-!
# LastWriterMaskedMerge: sound

A masked store issued on top of an earlier masked store with the same base is
byte-for-byte equivalent to one masked store of the lane-wise `last writer
wins` merge (the second store's validity lane decides; when it is inactive the
first store's lane survives), so SQL execution may merge the two predicated
stores into a single vector store.
-/

set_option maxHeartbeats 4000000
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.PredicatedStores_LastWriterMaskedMerge
open TinylambOpt.ResearchGoals.JitKernels.PredicatedStores

private theorem masked_apply (m : Mem) (a : Nat) (xs : List (Option UInt8)) (i : Nat) :
    masked m a xs i = if a ≤ i then xs[i - a]?.join.getD (m i) else m i := rfl

theorem sound : LastWriterMaskedMerge.Goal := by
  intro m a xs ys hlen
  funext i
  simp only [masked_apply]
  by_cases hai : a ≤ i
  · simp only [if_pos hai, Nat.add_sub_cancel_left, List.getElem?_zipWith]
    cases hx : xs[i - a]? with
    | none =>
      have hk : xs.length ≤ i - a := by
        by_cases hlt : i - a < xs.length
        · rw [List.getElem?_eq_getElem hlt] at hx
          cases hx
        · exact Nat.le_of_not_lt hlt
      have hy : ys[i - a]? = none := List.getElem?_eq_none (by rw [← hlen]; exact hk)
      simp [hy, hx]
    | some x =>
      cases hy : ys[i - a]? with
      | none =>
        have hlt : i - a < ys.length := by
          rw [← hlen]
          by_cases h : i - a < xs.length
          · exact h
          · rw [List.getElem?_eq_none (Nat.le_of_not_lt h)] at hx
            cases hx
        rw [List.getElem?_eq_getElem hlt] at hy
        cases hy
      | some y =>
        simp only [hx, hy]
        cases y with
        | none => cases x with
          | none => rfl
          | some v => rfl
        | some v => rfl
  · simp only [if_neg hai]

end TinylambOpt.ResearchProofs.JitKernels.PredicatedStores_LastWriterMaskedMerge
