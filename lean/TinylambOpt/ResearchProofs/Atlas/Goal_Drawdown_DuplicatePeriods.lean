import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

/-!
SQL execution can use cumulative counts or ordered measures to answer range and rank
queries under the modeled bounds.

Proved

- `Drawdown.DuplicatePeriods` — `∀ x n, 2 ≤ n → measure ((List.replicate n x).flatten)
  = spread x`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_Drawdown_DuplicatePeriods

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures
open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.Drawdown

/-! Exhaustive sanity check on all lists of length <= 3 over a small range,
    with n = 2, 3, 4. -/
private def rowsOf : Nat → List Int → List (List Int)
  | 0, _ => [[]]
  | n + 1, pool => pool.flatMap (fun r => (rowsOf n pool).map (r :: ·))

#eval do
  let pool : List Int := [-2, -1, 0, 1, 2]
  let lists := (List.range 4).flatMap (fun k => rowsOf k pool)
  let bad := lists.filter (fun x : List Int =>
    [2, 3, 4].any (fun n =>
      Drawdown.measure ((List.replicate n x).flatten) != spread x))
  pure bad

/-! ### List algebra -/

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

private theorem pairs_mem {z : Series} {p : Int × Int} (hp : p ∈ pairs z) :
    p.1 ∈ z ∧ p.2 ∈ z := by
  induction z with
  | nil => simp [pairs] at hp
  | cons a z ih =>
      rw [pairs_cons] at hp
      rcases List.mem_append.mp hp with hp | hp
      · rcases List.mem_map.mp hp with ⟨b, hb, rfl⟩
        exact ⟨by simp, by simp [hb]⟩
      · have h := ih hp
        exact ⟨by simp [h.1], by simp [h.2]⟩

private theorem mem_le_max_getD {x : List Int} (hx : x ≠ []) {a : Int} (ha : a ∈ x) :
    a ≤ x.max?.getD 0 := by
  induction x with
  | nil => contradiction
  | cons b bs ih =>
      by_cases hbs : bs = []
      · subst bs
        simp at ha ⊢
        omega
      · rw [List.max?_cons]
        rcases List.mem_cons.mp ha with rfl | ha
        · cases h : bs.max? with
          | none => simp at h; contradiction
          | some m => simp; omega
        · have ih' := ih hbs ha
          cases h : bs.max? with
          | none => simp at h; contradiction
          | some m => simp [h] at ih' ⊢; omega

private theorem min_getD_le_mem {x : List Int} (hx : x ≠ []) {a : Int} (ha : a ∈ x) :
    x.min?.getD 0 ≤ a := by
  induction x with
  | nil => contradiction
  | cons b bs ih =>
      by_cases hbs : bs = []
      · subst bs
        simp at ha ⊢
        omega
      · rw [List.min?_cons]
        rcases List.mem_cons.mp ha with rfl | ha
        · cases h : bs.min? with
          | none => simp at h; contradiction
          | some m => simp; omega
        · have ih' := ih hbs ha
          cases h : bs.min? with
          | none => simp at h; contradiction
          | some m => simp [h] at ih' ⊢; omega

private theorem max_getD_mem {x : List Int} (hx : x ≠ []) : x.max?.getD 0 ∈ x := by
  cases h : x.max? with
  | none => simp at h; contradiction
  | some m =>
      have hm : m ∈ x := List.max?_mem h
      simpa [h] using hm

private theorem min_getD_mem {x : List Int} (hx : x ≠ []) : x.min?.getD 0 ∈ x := by
  cases h : x.min? with
  | none => simp at h; contradiction
  | some m =>
      have hm : m ∈ x := List.min?_mem h
      simpa [h] using hm

private theorem spread_nonneg (x : Series) : 0 ≤ spread x := by
  cases x with
  | nil => simp [spread]
  | cons a xs =>
      have hmem : a ∈ a :: xs := by simp
      have hmax := mem_le_max_getD (by simp : a :: xs ≠ []) hmem
      have hmin := min_getD_le_mem (by simp : a :: xs ≠ []) hmem
      simp only [spread]
      omega

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

private theorem max0_nonneg (xs : List Int) : 0 ≤ max0 xs := by
  unfold max0
  omega

private theorem le_max0_of_mem {xs : List Int} {a : Int} (ha : a ∈ xs) : a ≤ max0 xs := by
  induction xs with
  | nil => simp at ha
  | cons c cs ih =>
      rw [max0_cons]
      rcases List.mem_cons.mp ha with rfl | ha
      · omega
      · have h := ih ha
        omega

private theorem max0_le {xs : List Int} {b : Int} (hb : 0 ≤ b) (h : ∀ a ∈ xs, a ≤ b) :
    max0 xs ≤ b := by
  induction xs with
  | nil => simpa [max0] using hb
  | cons c cs ih =>
      rw [max0_cons]
      exact Int.max_le.mpr ⟨h c (by simp), ih (by intro a ha; exact h a (by simp [ha]))⟩

/-! ### Upper bound: any pair drawn from values of `x` has gap at most `spread x`. -/

private theorem measure_le_spread_of_mem (z x : Series) (hz : ∀ v ∈ z, v ∈ x) :
    Drawdown.measure z ≤ spread x := by
  unfold Drawdown.measure
  apply max0_le (spread_nonneg x)
  intro d hd
  rcases List.mem_map.mp hd with ⟨p, hp, rfl⟩
  have hpz := pairs_mem hp
  have h1x := hz p.1 hpz.1
  have h2x := hz p.2 hpz.2
  by_cases hx : x = []
  · subst x
    simp at h1x
  · have hmax := mem_le_max_getD hx h1x
    have hmin := min_getD_le_mem hx h2x
    simp only [spread]
    omega

/-! ### Lower bound: the cross pair (max, min) occurs in `x ++ y`. -/

private theorem cart_mem_pairs_append (x y : Series) (a b : Int) (ha : a ∈ x) (hb : b ∈ y) :
    (a, b) ∈ pairs (x ++ y) := by
  induction x generalizing a b with
  | nil => simp at ha
  | cons c xs ih =>
      rw [List.cons_append, pairs_cons]
      apply List.mem_append.mpr
      rcases List.mem_cons.mp ha with rfl | ha
      · left
        apply List.mem_map.mpr
        exact ⟨b, by simp [hb], rfl⟩
      · right
        exact ih a b ha hb

private theorem spread_le_duplicate_measure (x : Series) (n : Nat) (hn : 2 ≤ n) :
    spread x ≤ Drawdown.measure ((List.replicate n x).flatten) := by
  by_cases hx : x = []
  · subst x
    simp [spread, Drawdown.measure, max0, pairs]
  · obtain ⟨k, hk⟩ := Nat.exists_eq_add_of_le hn
    subst n
    have hrep : List.replicate (2 + k) x = x :: x :: List.replicate k x := by
      rw [show 2 + k = (k + 1) + 1 by omega]
      rw [List.replicate_succ, List.replicate_succ]
    rw [hrep]
    simp only [List.flatten_cons]
    let z : Series := (List.replicate k x).flatten
    have hcart : (x.max?.getD 0, x.min?.getD 0) ∈ pairs (x ++ (x ++ z)) :=
      cart_mem_pairs_append x (x ++ z) _ _ (max_getD_mem hx) (by simp [min_getD_mem hx])
    unfold Drawdown.measure
    apply le_max0_of_mem
    apply List.mem_map.mpr
    exact ⟨(x.max?.getD 0, x.min?.getD 0), hcart, rfl⟩

private theorem mem_flatten_replicate {x : Series} {v : Int} (n : Nat) :
    v ∈ (List.replicate n x).flatten → v ∈ x := by
  intro hv
  rw [List.mem_flatten] at hv
  rcases hv with ⟨l, hl, hv⟩
  have hlx : l = x := List.eq_of_mem_replicate hl
  simpa [hlx] using hv

theorem sound_Drawdown_DuplicatePeriods : Drawdown.DuplicatePeriods.Goal := by
  intro x n hn
  apply Int.le_antisymm
  · apply measure_le_spread_of_mem
    intro v hv
    exact mem_flatten_replicate n hv
  · exact spread_le_duplicate_measure x n hn

end TinylambOpt.ResearchProofs.Atlas.Goal_Drawdown_DuplicatePeriods
