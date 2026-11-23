import TinylambOpt.ResearchGoals.ExecutionStrategies.FrameIndexes

namespace TinylambOpt.ResearchProofs.ExecutionStrategies.FrameIndexes

open TinylambOpt.ResearchGoals.FrameIndexes

/-!
# Window/frame index lemmas

`frame xs l r = (xs.take r).drop l` is the half-open slice `[l, r)`.
The proofs below only need elementary `List.take`/`drop`/`getElem?` facts.
-/

private theorem length_take_of_le {α : Type} (xs : List α) {a : Nat} (h : a ≤ xs.length) :
    (xs.take a).length = a := by
  rw [List.length_take, Nat.min_eq_left h]

/-- `EXCLUDE GROUP`: deleting the contiguous peer interval `[a, b)` shifts the
requested surviving rank `k` by the removed width whenever `k ≥ a`. -/
theorem sound_ExcludePeerSelect : ExcludePeerSelect.Goal := by
  intro xs a b k hab hb
  have ha_len : (xs.take a).length = a := length_take_of_le xs (by omega)
  have hcat_len : (xs.take a ++ xs.drop b).length = xs.length - (b - a) := by
    rw [List.length_append, ha_len, List.length_drop]
    omega
  by_cases hk : k < a
  · -- Before the deleted interval: index unchanged.
    rw [ite_eq_left hk, List.getElem?_append,
      ite_eq_left (by rw [ha_len]; exact hk), List.getElem?_take, ite_eq_left hk]
  · -- At/after the deleted interval: shift the index by `b - a`.
    rw [ite_eq_right hk]
    by_cases hk2 : k < (xs.take a ++ xs.drop b).length
    · have htake : ¬ k < (xs.take a).length := by omega
      rw [List.getElem?_append, ite_eq_right htake, List.getElem?_drop]
      congr 1
      omega
    · rw [List.getElem?_eq_none (Nat.le_of_not_lt hk2)]
      apply List.getElem?_eq_none
      rw [hcat_len] at hk2
      omega

/-! ### Session boundary reconciliation

`sessions gap xs` counts `1` plus the number of adjacent pairs `(a, b)` with
`a + gap < b`. Splitting `xs = a ++ b` only changes the count through the
single boundary pair `(last a, head b)`, which contributes `1` exactly when
`head b ≤ last a + gap`. -/

private def apairs {α : Type} (xs : List α) : List (α × α) := xs.zip xs.tail

private def cpair {α : Type} (x : α) (w : List α) : List (α × α) :=
  match w.head? with | some y => [(x, y)] | none => []

private def bpair {α : Type} (u v : List α) : List (α × α) :=
  match u.getLast?, v.head? with
  | some x, some y => [(x, y)]
  | _, _ => []

private theorem apairs_cons {α : Type} (x : α) (w : List α) :
    apairs (x :: w) = cpair x w ++ apairs w := by
  cases w <;> rfl

private theorem bpair_singleton {α : Type} (x : α) (v : List α) :
    bpair [x] v = cpair x v := by
  cases v <;> rfl

private theorem bpair_cons_cons {α : Type} (x y : α) (ys : List α) (v : List α) :
    bpair (x :: y :: ys) v = bpair (y :: ys) v := by
  simp [bpair, List.getLast?_cons]

private theorem apairs_append {α : Type} (u v : List α) :
    apairs (u ++ v) = apairs u ++ bpair u v ++ apairs v := by
  induction u with
  | nil => rfl
  | cons x xs ih =>
    cases xs with
    | nil =>
      have hxs : apairs [x] = ([] : List (α × α)) := rfl
      show apairs (x :: v) = apairs [x] ++ bpair [x] v ++ apairs v
      rw [apairs_cons, bpair_singleton, hxs]
      simp
    | cons y ys =>
      rw [List.cons_append, apairs_cons, ih]
      rw [show cpair x ((y :: ys) ++ v) = [(x, y)] from rfl]
      rw [apairs_cons x (y :: ys), bpair_cons_cons]
      rw [show cpair x (y :: ys) = [(x, y)] from rfl]
      simp only [apairs_cons, List.append_assoc]

private theorem sessions_eq_countP {gap : Nat} {xs : List Nat} (h : xs ≠ []) :
    sessions gap xs = 1 + List.countP (fun p => match p with | (a, b) => decide (a + gap < b)) (apairs xs) := by
  cases xs with
  | nil => exact absurd rfl h
  | cons x xs' => rfl

private theorem countP_boundary_singleton {gap s y : Nat} :
    List.countP (fun p => match p with | (a, b) => decide (a + gap < b)) [(s, y)] +
      (decide (y ≤ s + gap)).toNat = 1 := by
  rw [List.countP_singleton]
  by_cases h : s + gap < y
  · have h2 : ¬ y ≤ s + gap := by omega
    simp [h, h2]
  · have h2 : y ≤ s + gap := by omega
    simp [h, h2]

/-- Only the boundary pair can change the session count when concatenating. -/
theorem sound_SessionBoundary : SessionBoundary.Goal := by
  intro gap a b
  cases a with
  | nil => simp [sessions]
  | cons x as =>
    cases b with
    | nil => simp [sessions]
    | cons y ys =>
      have hne1 : ((x :: as) ++ (y :: ys)) ≠ [] := by simp
      have hne2 : (x :: as) ≠ [] := by simp
      have hne3 : (y :: ys) ≠ [] := by simp
      rw [sessions_eq_countP hne1, sessions_eq_countP hne2, sessions_eq_countP hne3]
      rw [apairs_append, List.countP_append, List.countP_append]
      have hbpair : bpair (x :: as) (y :: ys) = [((as.getLast?).getD x, y)] := by
        simp only [bpair]
        rw [List.getLast?_cons]
        simp only [List.head?]
      rw [hbpair]
      rw [List.getLast?_cons]
      simp only [List.head?]
      generalize hB : List.countP (fun p => match p with | (a, b) => decide (a + gap < b))
          [((as.getLast?).getD x, y)] = B
      generalize hC : (decide (y ≤ (as.getLast?).getD x + gap)).toNat = C
      have key : B + C = 1 := by
        rw [← hB, ← hC]
        exact countP_boundary_singleton (gap := gap) (s := (as.getLast?).getD x) (y := y)
      omega

/-! ### Prefix nullable sums

`sums xs` is the `scanl` of `(non-null count, sum)` pairs. The `l`-th and `r`-th
prefix entries differ exactly by the frame's non-null count and value sum; the
count difference is `0` iff `filterMap id` of the frame is empty. -/

private def stepF (q : Nat × Int) (x : Option Int) : Nat × Int :=
  (q.1 + x.isSome.toNat, q.2 + x.getD 0)

private theorem stepF_none (q : Nat × Int) : stepF q none = q := by
  apply Prod.ext <;> simp [stepF] <;> omega

private theorem stepF_some (q : Nat × Int) (v : Int) : stepF q (some v) = (q.1 + 1, q.2 + v) := by
  apply Prod.ext <;> simp [stepF] <;> omega

private theorem foldl_stepF {xs : List (Option Int)} (q : Nat × Int) :
    xs.foldl stepF q =
      (q.1 + xs.countP (·.isSome), q.2 + (xs.map (·.getD 0)).sum) := by
  induction xs generalizing q with
  | nil => simp [stepF]
  | cons x xs ih =>
    rw [List.foldl_cons, ih]
    cases x with
    | none =>
      rw [stepF_none]
      apply Prod.ext
      · rw [List.countP_cons_of_neg (by simp [Option.isSome_none])]
      · simp [List.map_cons, List.sum_cons, Option.getD_none]
    | some v =>
      rw [stepF_some]
      apply Prod.ext
      · rw [List.countP_cons_of_pos (by simp [Option.isSome_some])]
        omega
      · simp [List.map_cons, List.sum_cons, Option.getD_some]
        omega

private theorem foldl_pattern_eq_stepF (xs : List (Option Int)) (q : Nat × Int) :
    xs.foldl (fun (n, s) x => (n + x.isSome.toNat, s + x.getD 0)) q =
      xs.foldl stepF q := by
  have hfun : (fun (n, s) x => (n + x.isSome.toNat, s + x.getD 0)) = stepF := by
    funext q x; cases q with | mk n s => rfl
  rw [hfun]

private theorem foldl_pattern (xs : List (Option Int)) (q : Nat × Int) :
    xs.foldl (fun (n, s) x => (n + x.isSome.toNat, s + x.getD 0)) q =
      (q.1 + xs.countP (·.isSome), q.2 + (xs.map (·.getD 0)).sum) := by
  rw [foldl_pattern_eq_stepF, foldl_stepF]

private theorem filterMap_id_sum (xs : List (Option Int)) :
    (xs.filterMap id).sum = (xs.map (·.getD 0)).sum := by
  induction xs with
  | nil => rfl
  | cons x xs ih =>
    cases x with
    | none => simp [List.map_cons, List.sum_cons, ih]
    | some v => simp [List.map_cons, List.sum_cons, ih]

private theorem filterMap_id_isEmpty_iff (xs : List (Option Int)) :
    (xs.filterMap id).isEmpty = true ↔ xs.countP (·.isSome) = 0 := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
    cases x with
    | none =>
      have hfm : List.filterMap id (none :: xs) = List.filterMap id xs := by
        simp
      have hcp : List.countP (·.isSome) (none :: xs) = List.countP (·.isSome) xs :=
        List.countP_cons_of_neg (by simp [Option.isSome_none])
      rw [hfm, hcp]
      exact ih
    | some v =>
      have hfm : List.filterMap id (some v :: xs) = v :: List.filterMap id xs := by
        simp
      have hcp : List.countP (·.isSome) (some v :: xs) = List.countP (·.isSome) xs + 1 :=
        List.countP_cons_of_pos (by simp [Option.isSome_some])
      rw [hfm, hcp]
      exact iff_of_false (by simp [List.isEmpty_cons]) (by omega)

/-- Prefix `(count, sum)` pairs distinguish "no non-null values" from "sum is 0". -/
theorem sound_PrefixNullableSum : PrefixNullableSum.Goal := by
  intro xs l r hlr hr
  have hlen_l : l ≤ xs.length := Nat.le_trans hlr hr
  have ha1 : ((sums xs)[l]?.getD (0,0)).1 = (xs.take l).countP (·.isSome) := by
    unfold sums
    rw [List.getElem?_toArray, List.getElem?_scanl, ite_eq_left hlen_l,
      Option.getD_some, foldl_pattern]
    simp [Nat.zero_add, Int.zero_add]
  have ha2 : ((sums xs)[l]?.getD (0,0)).2 = ((xs.take l).map (·.getD 0)).sum := by
    unfold sums
    rw [List.getElem?_toArray, List.getElem?_scanl, ite_eq_left hlen_l,
      Option.getD_some, foldl_pattern]
    simp [Nat.zero_add, Int.zero_add]
  have hb1 : ((sums xs)[r]?.getD (0,0)).1 = (xs.take r).countP (·.isSome) := by
    unfold sums
    rw [List.getElem?_toArray, List.getElem?_scanl, ite_eq_left hr,
      Option.getD_some, foldl_pattern]
    simp [Nat.zero_add, Int.zero_add]
  have hb2 : ((sums xs)[r]?.getD (0,0)).2 = ((xs.take r).map (·.getD 0)).sum := by
    unfold sums
    rw [List.getElem?_toArray, List.getElem?_scanl, ite_eq_left hr,
      Option.getD_some, foldl_pattern]
    simp [Nat.zero_add, Int.zero_add]
  have htake : xs.take r = xs.take l ++ frame xs l r := by
    have h1 : xs.take r = List.take l (xs.take r) ++ List.drop l (xs.take r) :=
      (List.take_append_drop l (xs.take r)).symm
    rw [h1, List.take_take, Nat.min_eq_left hlr]
    rfl
  have hcntR : (xs.take r).countP (·.isSome) =
      (xs.take l).countP (·.isSome) + (frame xs l r).countP (·.isSome) := by
    rw [htake, List.countP_append]
  have hsmR : ((xs.take r).map (·.getD 0)).sum =
      ((xs.take l).map (·.getD 0)).sum + ((frame xs l r).map (·.getD 0)).sum := by
    rw [htake, List.map_append, List.sum_append]
  show (if (((sums xs)[l]?.getD (0,0)).1 == ((sums xs)[r]?.getD (0,0)).1) = true
        then none
        else some (((sums xs)[r]?.getD (0,0)).2 - ((sums xs)[l]?.getD (0,0)).2)) =
       (if ((frame xs l r).filterMap id).isEmpty = true then none
        else some ((frame xs l r).filterMap id).sum)
  rw [ha1, ha2, hb1, hb2, hcntR, hsmR, filterMap_id_sum]
  by_cases hF : (frame xs l r).countP (·.isSome) = 0
  · have hLc : ((xs.take l).countP (·.isSome) ==
        (xs.take l).countP (·.isSome) + (frame xs l r).countP (·.isSome)) = true := by
      apply beq_iff_eq.mpr
      rw [hF, Nat.add_zero]
    have hRc : ((frame xs l r).filterMap id).isEmpty = true :=
      (filterMap_id_isEmpty_iff (frame xs l r)).mpr hF
    rw [ite_eq_left hLc, ite_eq_left hRc]
  · have hne : (xs.take l).countP (·.isSome) ≠
        (xs.take l).countP (·.isSome) + (frame xs l r).countP (·.isSome) := by
      intro h
      apply hF
      exact Nat.add_eq_left.mp h.symm
    have hLc : ¬(((xs.take l).countP (·.isSome) ==
        (xs.take l).countP (·.isSome) + (frame xs l r).countP (·.isSome)) = true) := by
      rw [beq_iff_eq]
      exact hne
    have hRc : ¬(((frame xs l r).filterMap id).isEmpty = true) := by
      rw [filterMap_id_isEmpty_iff]
      exact hF
    rw [ite_eq_right hLc, ite_eq_right hRc]
    apply congrArg
    omega

end TinylambOpt.ResearchProofs.ExecutionStrategies.FrameIndexes
