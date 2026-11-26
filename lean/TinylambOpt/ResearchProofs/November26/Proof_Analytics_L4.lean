import TinylambOpt.ResearchGoals.November26.Analytics

set_option maxHeartbeats 0
set_option maxRecDepth 10000


namespace TinylambOpt.ResearchProofs.November26.Analytics.Goal_L4
open TinylambOpt.ResearchGoals.November26

set_option linter.unusedSimpArgs false

theorem valuesAt_cons_zero {α : Type} (x : α) (xs : List α) (is : List Nat) :
    valuesAt (x :: xs) (0 :: is) = x :: valuesAt (x :: xs) is := by
  simp [valuesAt, List.getElem?_cons_zero]

theorem valuesAt_cons_succ {α : Type} (x : α) (xs : List α) (is : List Nat) :
    valuesAt (x :: xs) (is.map (fun i => i + 1)) = valuesAt xs is := by
  induction is with
  | nil => simp [valuesAt]
  | cons i is ih =>
      unfold valuesAt at ih ⊢
      simp only [List.map_cons, List.filterMap_cons] at ih ⊢
      rw [List.getElem?_cons_succ]
      cases h : xs[i]? <;> simp [h, ih]

theorem zipIdx_succ {α : Type} (xs : List α) :
    ∀ n, xs.zipIdx (n + 1) = (xs.zipIdx n).map (fun pr => (pr.1, pr.2 + 1)) := by
  induction xs with
  | nil => intro n; simp
  | cons x xs ih =>
      intro n
      simp [List.zipIdx_cons, ih, Nat.add_assoc]

theorem filter_map_snd_map_succ {α : Type} (xs : List (α × Nat)) (P : α → Bool) :
    (((xs.map (fun pr => (pr.1, pr.2 + 1))).filter (fun pr => P pr.1)).map Prod.snd) =
      (((xs.filter (fun pr => P pr.1)).map Prod.snd).map (fun i => i + 1)) := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      by_cases h : P x.1 = true <;> simp [h, ih]

theorem zipIdx_filter_snd_succ {α : Type} (xs : List α) (P : α → Bool) (n : Nat) :
    (((xs.zipIdx (n + 1)).filter (fun pr => P pr.1)).map Prod.snd) =
      ((((xs.zipIdx n).filter (fun pr => P pr.1)).map Prod.snd).map (fun i => i + 1)) := by
  rw [zipIdx_succ]
  exact filter_map_snd_map_succ _ _

theorem valuesAt_zipIdx_filter_map {α β : Type} (xs : List α) (f : α → β) (P : α → Bool) :
    valuesAt (xs.map f) (((xs.zipIdx).filter (fun pr => P pr.1)).map Prod.snd) =
      (xs.filter P).map f := by
  induction xs with
  | nil => simp [valuesAt]
  | cons x xs ih =>
      by_cases h : P x = true
      · simp only [List.zipIdx_cons, List.map_cons, h, Bool.false_eq_true, ↓reduceIte,
          List.filter_cons_of_pos, List.filter_cons_of_neg]
        rw [zipIdx_filter_snd_succ xs P 0]
        rw [valuesAt_cons_zero]
        rw [valuesAt_cons_succ]
        rw [ih]
      · have hf : P x = false := by
          cases hp : P x <;> simp_all
        simp [List.zipIdx_cons, hf, ite_false, List.filter_cons_of_neg]
        rw [zipIdx_filter_snd_succ xs P 0]
        rw [valuesAt_cons_succ]
        rw [ih]

theorem option_ne_none_eq_isSome (o : Option Nat) : (o != none) = o.isSome := by
  cases o <;> simp

theorem sound : TinylambOpt.ResearchGoals.November26.Analytics.L4.Goal := by
  intro rs
  dsimp only
  let P : (Nat × Option Nat × Option Int) → Bool := fun r => r.1 == 2012 && r.2.1.isSome
  have h := valuesAt_zipIdx_filter_map rs (fun r : Nat × Option Nat × Option Int => r.2.2) P
  simpa [P, option_ne_none_eq_isSome] using h
end TinylambOpt.ResearchProofs.November26.Analytics.Goal_L4
