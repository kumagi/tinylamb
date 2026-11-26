import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

/-!
SQL execution can use cumulative counts or ordered measures to answer range and rank
queries under the modeled bounds.

Proved

- `Drawdown.CrossExtreme` — `∀ x y, x ≠ [] → y ≠ [] → measure (x++y) = max (max
  (measure x) (measure y)) (x.max?.getD 0-y.min?.getD 0)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_Drawdown_CrossExtreme

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

/-! Brute-force sanity check on all nonempty lists of length ≤ 2 over a small range. -/
#eval do
  let vals : List Int := [-3, -2, -1, 0, 1, 2, 3]
  let lists : List (List Int) := vals.flatMap (fun a => vals.flatMap (fun b => [[a], [a, b]]))
  let mut bad : List (List Int × List Int) := []
  for x in lists do
    for y in lists do
      let lhs := Drawdown.measure (x ++ y)
      let rhs := max (max (Drawdown.measure x) (Drawdown.measure y))
        (x.max?.getD 0 - y.min?.getD 0)
      if lhs != rhs then bad := (x, y) :: bad
  pure bad

private theorem zipIdx_succ {α : Type} (l : List α) (n : Nat) :
    l.zipIdx (n + 1) = (l.zipIdx n).map (fun x => (x.1, x.2 + 1)) := by
  induction l generalizing n with
  | nil => simp
  | cons a l ih => simp [List.zipIdx, ih, Nat.add_assoc]

private theorem pairs_cons (a : Int) (xs : Series) :
    pairs (a :: xs) = xs.map (fun b => (a, b)) ++ pairs xs := by
  unfold pairs
  simp [List.zipIdx]
  rw [zipIdx_succ]
  rw [List.flatMap_map]

private theorem max0_cons (c : Int) (l : List Int) : max0 (c :: l) = max c (max0 l) := by
  unfold max0
  rw [List.max?_cons]
  cases h : l.max? with
  | none =>
      simp
      omega
  | some m =>
      simp
      omega

private theorem max0_append (l₁ l₂ : List Int) :
    max0 (l₁ ++ l₂) = max (max0 l₁) (max0 l₂) := by
  induction l₁ with
  | nil => simp [max0]
  | cons a l₁ ih =>
      rw [List.cons_append, max0_cons, ih, max0_cons]
      omega

private theorem measure_nonneg (x : Series) : 0 ≤ Drawdown.measure x := by
  unfold Drawdown.measure max0
  omega

private theorem measure_singleton (a : Int) : Drawdown.measure [a] = 0 := by
  simp [Drawdown.measure, pairs, max0]

private theorem max_singleton (a : Int) : ([a].max?.getD 0) = a := by
  simp

private theorem measure_cons (a : Int) (xs : Series) :
    Drawdown.measure (a :: xs) =
      max (Drawdown.measure xs) (max0 (xs.map (fun b => a - b))) := by
  unfold Drawdown.measure
  rw [pairs_cons]
  rw [List.map_append]
  rw [max0_append]
  rw [List.map_map]
  change max (max0 (xs.map (fun b => a - b))) (max0 ((pairs xs).map (fun p => p.1 - p.2))) =
    max (max0 ((pairs xs).map (fun p => p.1 - p.2))) (max0 (xs.map (fun b => a - b)))
  omega

private theorem min_cons_getD (a : Int) (l : Series) (hl : l ≠ []) :
    (a :: l).min?.getD 0 = min a (l.min?.getD 0) := by
  cases l with
  | nil => contradiction
  | cons b bs =>
      simp [List.min?_cons]

private theorem max_cons_getD (a : Int) (l : Series) (hl : l ≠ []) :
    (a :: l).max?.getD 0 = max a (l.max?.getD 0) := by
  cases l with
  | nil => contradiction
  | cons b bs =>
      simp [List.max?_cons]

private theorem max0_map_sub (a : Int) (l : Series) (hl : l ≠ []) :
    max0 (l.map (fun b => a - b)) = max 0 (a - l.min?.getD 0) := by
  induction l with
  | nil => contradiction
  | cons b bs ih =>
      by_cases hbs : bs = []
      · subst bs
        simp [max0]
      · rw [List.map_cons, max0_cons, ih hbs]
        rw [min_cons_getD b bs hbs]
        omega

private theorem min_append (x y : Series) (hx : x ≠ []) (hy : y ≠ []) :
    (x ++ y).min?.getD 0 = min (x.min?.getD 0) (y.min?.getD 0) := by
  induction x generalizing y with
  | nil => contradiction
  | cons a xs ih =>
      by_cases hxs : xs = []
      · subst xs
        cases y with
        | nil => contradiction
        | cons b bs =>
            rw [List.cons_append, List.nil_append]
            rw [min_cons_getD a (b :: bs) (by simp)]
            simp
      · rw [List.cons_append]
        rw [min_cons_getD a (xs ++ y) (by simp [hxs])]
        rw [ih y hxs hy]
        rw [min_cons_getD a xs hxs]
        omega

theorem sound_Drawdown_CrossExtreme : Drawdown.CrossExtreme.Goal := by
  intro x y hx hy
  induction x generalizing y with
  | nil => contradiction
  | cons a xs ih =>
      by_cases hxs : xs = []
      · subst xs
        rw [List.cons_append, List.nil_append]
        rw [measure_cons, max0_map_sub a y hy]
        rw [measure_singleton, max_singleton]
        have hnon := measure_nonneg y
        omega
      · have ih' := ih y hxs hy
        rw [List.cons_append]
        rw [measure_cons a (xs ++ y), measure_cons a xs]
        rw [ih']
        rw [max0_map_sub a (xs ++ y) (by simp [hxs])]
        rw [min_append xs y hxs hy]
        rw [max0_map_sub a xs hxs]
        rw [max_cons_getD a xs hxs]
        have h₁ := measure_nonneg xs
        have h₂ := measure_nonneg y
        omega

end TinylambOpt.ResearchProofs.Atlas.Goal_Drawdown_CrossExtreme
