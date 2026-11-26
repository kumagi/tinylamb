import TinylambOpt.ResearchGoals.EngineBoundaries.RadixBudget

/-!
Partitioning for a spillable SQL operator must satisfy the modeled size or resource
bound for its partitions.

Proved

- `LargerGrant` — Hot-key fallback pressure can only decrease when the same plan gets a
  larger grant.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.EngineBoundaries.Goal_RadixBudget_LargerGrant

open TinylambOpt.ResearchGoals.EngineBoundaries.RadixBudget

private def fbOf (ls : List Leaf) : Nat :=
  size ((ls.filter Leaf.fallback).flatMap Leaf.rows)

private theorem fbOf_append (xs ys : List Leaf) :
    fbOf (xs ++ ys) = fbOf xs + fbOf ys := by
  simp [fbOf, size, List.filter_append, List.flatMap_append, List.map_append]

private theorem fbOf_le_flatSize (ls : List Leaf) :
    fbOf ls ≤ size (ls.flatMap Leaf.rows) := by
  induction ls with
  | nil => simp [fbOf, size]
  | cons l ls ih =>
      by_cases hf : l.fallback = true
      · have hcalc : fbOf (l :: ls) = size l.rows + fbOf ls := by
          simp [fbOf, hf, size]
        have hflat : size ((l :: ls).flatMap Leaf.rows) = size l.rows + size (ls.flatMap Leaf.rows) := by
          simp [size]
        rw [hcalc, hflat]
        exact Nat.add_le_add_left ih (size l.rows)
      · have hcalc : fbOf (l :: ls) = fbOf ls := by
          simp [fbOf, hf]
        have hflat : size ((l :: ls).flatMap Leaf.rows) = size l.rows + size (ls.flatMap Leaf.rows) := by
          simp [size]
        rw [hcalc, hflat]
        exact Nat.le_trans ih (Nat.le_add_left _ _)

private theorem perm_sum {α} (f : α → Nat) : ∀ {xs ys : List α}, xs.Perm ys →
    (xs.map f).sum = (ys.map f).sum := by
  intro xs ys h
  induction h with
  | nil => rfl
  | cons x h ih => simpa using ih
  | swap x y l => simp [Nat.add_left_comm]
  | trans h1 h2 ih1 ih2 => exact ih1.trans ih2

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
      by_cases ht : r.hash.testBit bit = true
      · simp [ht] at ih ⊢
        exact (perm_cons_append r (bucket bit false rs) (bucket bit true rs)).trans (ih.cons r)
      · have hf : r.hash.testBit bit = false := by
          cases hb : r.hash.testBit bit <;> simp_all
        simp [hf] at ih ⊢
        exact ih

private theorem leaves_perm (b d : Nat) (rs : Batch) : (List.flatMap Leaf.rows (leaves b d rs)).Perm rs := by
  induction d generalizing rs with
  | zero => simp [leaves]
  | succ d ih =>
      by_cases hsmall : size rs ≤ b
      · simp [leaves, hsmall]
      · simp only [leaves, hsmall, ite_false]
        have side : ∀ s : Bool,
            ((if (bucket d s rs).isEmpty = true then [] else
                if b < size (bucket d s rs) && stagnant (size (bucket d s rs)) (size rs) then
                  [⟨bucket d s rs, true⟩]
                else leaves b d (bucket d s rs)).flatMap Leaf.rows).Perm (bucket d s rs) := by
          intro s
          by_cases he : (bucket d s rs).isEmpty = true
          · have hnil : bucket d s rs = [] := by simpa using he
            simp [hnil]
          · by_cases hs : (b < size (bucket d s rs) && stagnant (size (bucket d s rs)) (size rs)) = true
            · simp [he, hs]
            · simpa [he, hs] using ih (bucket d s rs)
        simpa [List.flatMap_cons, List.flatMap_nil, List.append_nil] using
          (List.Perm.append (side false) (side true)).trans (bucket_split_perm d rs)

private theorem fbSize_le_size (b d : Nat) (rs : Batch) : fbOf (leaves b d rs) ≤ size rs := by
  have h := fbOf_le_flatSize (leaves b d rs)
  have hsz : size ((leaves b d rs).flatMap Leaf.rows) = size rs := perm_sum Row.bytes (leaves_perm b d rs)
  omega

private theorem side_mono (small large d : Nat) (rs : Batch) (side : Bool) (hs : small ≤ large)
    (ih : ∀ (rs : Batch), fbOf (leaves large d rs) ≤ fbOf (leaves small d rs)) :
    let sub (b : Nat) := if (bucket d side rs).isEmpty = true then [] else
      if b < size (bucket d side rs) && stagnant (size (bucket d side rs)) (size rs) then
        [⟨bucket d side rs, true⟩]
      else leaves b d (bucket d side rs)
    fbOf (sub large) ≤ fbOf (sub small) := by
  dsimp
  by_cases he : (bucket d side rs).isEmpty = true
  · simp [he]
  · by_cases hssmall : (small < size (bucket d side rs) &&
        stagnant (size (bucket d side rs)) (size rs)) = true
    · by_cases hslarge : (large < size (bucket d side rs) &&
          stagnant (size (bucket d side rs)) (size rs)) = true
      · rw [ite_eq_left hslarge, ite_eq_left hssmall, ite_eq_right he]
        exact Nat.le_refl _
      · rw [ite_eq_right hslarge, ite_eq_left hssmall]
        simp [he]
        simpa [fbOf, size] using fbSize_le_size large d (bucket d side rs)
    · by_cases hslarge : (large < size (bucket d side rs) &&
          stagnant (size (bucket d side rs)) (size rs)) = true
      · have hfacts : small < size (bucket d side rs) ∧
            stagnant (size (bucket d side rs)) (size rs) = true := by
          have hlargefacts : large < size (bucket d side rs) ∧
              stagnant (size (bucket d side rs)) (size rs) = true := by
            simpa using hslarge
          exact ⟨by omega, hlargefacts.2⟩
        have hssmall' : (small < size (bucket d side rs) &&
            stagnant (size (bucket d side rs)) (size rs)) = true := by
          simp [hfacts.1, hfacts.2]
        exact False.elim (hssmall hssmall')
      · rw [ite_eq_right hslarge, ite_eq_right hssmall]
        simp [he]
        exact ih (bucket d side rs)

theorem sound : LargerGrant.Goal := by
  intro small large d rs hs
  induction d generalizing rs with
  | zero =>
      by_cases hll : large < size rs
      · have hsl : small < size rs := Nat.lt_of_le_of_lt hs hll
        simp [leaves, hsl, hll]
      · by_cases hsl : small < size rs
        · simp [leaves, hsl, hll]
          exact Nat.zero_le _
        · simp [leaves, hsl, hll]
  | succ d ih =>
      by_cases hsrs : size rs ≤ small
      · have hlrs : size rs ≤ large := Nat.le_trans hsrs hs
        simp [leaves, hsrs, hlrs]
      · by_cases hlrs : size rs ≤ large
        · simp [leaves, hsrs, hlrs]
          exact Nat.zero_le _
        · simp only [leaves, hsrs, hlrs, ite_false, List.flatMap_cons, List.flatMap_nil,
            List.append_nil]
          let sub (b : Nat) (side : Bool) := if (bucket d side rs).isEmpty = true then [] else
            if b < size (bucket d side rs) && stagnant (size (bucket d side rs)) (size rs) then
              [⟨bucket d side rs, true⟩]
            else leaves b d (bucket d side rs)
          change fbOf (sub large false ++ sub large true) ≤ fbOf (sub small false ++ sub small true)
          rw [fbOf_append, fbOf_append]
          have hf := side_mono small large d rs false hs ih
          have ht := side_mono small large d rs true hs ih
          dsimp [sub] at hf ht
          exact Nat.add_le_add hf ht

end TinylambOpt.ResearchProofs.EngineBoundaries.Goal_RadixBudget_LargerGrant
