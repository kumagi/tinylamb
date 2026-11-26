import TinylambOpt.ResearchGoals.EngineBoundaries.RadixBudget

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.EngineBoundaries.Goal_RadixBudget_LeafBag

open TinylambOpt.ResearchGoals.EngineBoundaries.RadixBudget

private theorem perm_cons_append {α} (x : α) (l t : List α) : (l ++ x :: t).Perm (x :: (l ++ t)) := by
  induction l with
  | nil => simp
  | cons y ys ih =>
      simp
      exact (ih.cons y).trans (List.Perm.swap x y (ys ++ t))

private theorem bucket_split_perm (bit : Nat) (rs : Batch) :
    (bucket bit false rs ++ bucket bit true rs).Perm rs := by
  induction rs with
  | nil => simp [bucket]
  | cons r rs ih =>
      simp [bucket] at ih ⊢
      by_cases ht : (r.hash.testBit bit) = true
      · simp [ht] at ih ⊢
        exact (perm_cons_append r (bucket bit false rs) (bucket bit true rs)).trans (ih.cons r)
      · have hf : (r.hash.testBit bit) = false := by
          cases hb : r.hash.testBit bit <;> simp_all
        simp [hf] at ih ⊢
        exact ih

theorem sound : LeafBag.Goal := by
  intro b d rs
  induction d generalizing rs with
  | zero => simp [leaves]
  | succ d ih =>
      by_cases hsmall : size rs ≤ b
      · simp [leaves, hsmall]
      · simp [leaves, hsmall]
        have side : ∀ s : Bool,
            ((if bucket d s rs = [] then [] else
                if b < size (bucket d s rs) ∧ stagnant (size (bucket d s rs)) (size rs) then
                  [⟨bucket d s rs, true⟩]
                else leaves b d (bucket d s rs)).flatMap Leaf.rows).Perm (bucket d s rs) := by
          intro s
          by_cases he : bucket d s rs = []
          · simp [he]
          · by_cases hs : b < size (bucket d s rs) ∧
                stagnant (size (bucket d s rs)) (size rs) = true
            · simp [he, hs]
            · simpa [he, hs] using ih (bucket d s rs)
        have ht := side true
        have hf := side false
        exact (List.Perm.append hf ht).trans (bucket_split_perm d rs)

end TinylambOpt.ResearchProofs.EngineBoundaries.Goal_RadixBudget_LeafBag
