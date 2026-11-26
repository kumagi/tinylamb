import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

/-!
SQL execution can use cumulative counts or ordered measures to answer range and rank
queries under the modeled bounds.

Proved

- `CenteredScatter.OutlierCap` — `∀ x, 4*measure x ≤ size x^2 * spread x^2`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_CenteredScatter_OutlierCap

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

/-! Brute-force truth check: every series of length <= 4 with entries in
    [-3, 3] satisfies 4 * sum of squared pair differences <= n^2 * spread^2. -/
#eval
  let rec gen : Nat → List (List Int)
    | 0 => [[]]
    | n + 1 => (gen n).flatMap (fun xs => [-3, -2, -1, 0, 1, 2, 3].map (fun v => xs ++ [v]))
  (gen 4).all (fun xs => 4 * CenteredScatter.measure xs ≤ size xs ^ 2 * spread xs ^ 2)

private theorem flatMap_map {α β γ : Type} (f : β → List γ) (g : α → β) (xs : List α) :
    (xs.map g).flatMap f = xs.flatMap (fun x => f (g x)) := by
  induction xs with
  | nil => rfl
  | cons a as ih => simp [ih]

private theorem zipIdx_add {α : Type} (xs : List α) (n m : Nat) :
    xs.zipIdx (n + m) = (xs.zipIdx n).map (fun (v, i) => (v, i + m)) := by
  induction xs generalizing n with
  | nil => simp
  | cons a as ih =>
      simp only [List.zipIdx_cons, List.map_cons]
      congr 1
      simpa [Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] using ih (n + 1)

private theorem pairs_cons (a : Int) (xs : Series) :
    pairs (a :: xs) = xs.map (fun b => (a, b)) ++ pairs xs := by
  unfold pairs
  rw [List.zipIdx_cons]
  simp only [List.flatMap_cons, List.drop]
  rw [show (1 : Nat) = 0 + 1 by omega, zipIdx_add]
  rw [flatMap_map]

private theorem measure_cons (a : Int) (xs : Series) :
    CenteredScatter.measure (a :: xs) =
      (xs.map (fun b => (a - b)^2)).sum + CenteredScatter.measure xs := by
  simp [CenteredScatter.measure, pairs_cons, List.map_append, List.sum_append,
    List.map_map, Function.comp_def]

private theorem sum_map_sq_sub (a : Int) (xs : Series) :
    (xs.map (fun b => (a - b)^2)).sum =
      (xs.length : Int) * a^2 - 2 * a * xs.sum + (xs.map (fun v => v^2)).sum := by
  induction xs with
  | nil => simp
  | cons v xs ih =>
      simp only [List.map_cons, List.sum_cons, List.length_cons] at ih ⊢
      rw [ih]
      grind

private theorem moment (x : Series) :
    CenteredScatter.measure x =
      (x.length : Int) * (x.map (fun v => v^2)).sum - x.sum^2 := by
  induction x with
  | nil => simp [CenteredScatter.measure, pairs]
  | cons a xs ih =>
      rw [measure_cons, sum_map_sq_sub, ih]
      simp only [List.length_cons, List.sum_cons, List.map_cons]
      have hcast : (((xs.length + 1 : Nat) : Int)) = (xs.length : Int) + 1 := by simp
      rw [hcast]
      grind

private theorem min_le_of_mem {xs : Series} {a : Int} (ha : a ∈ xs) : xs.min?.getD 0 ≤ a := by
  cases xs with
  | nil => simp at ha
  | cons b bs =>
      cases hm : (b :: bs).min? with
      | none =>
          have heq : b :: bs = [] := List.min?_eq_none_iff.mp hm
          cases heq
      | some M =>
          have hspec := List.min?_eq_some_iff.mp hm
          simpa [hm] using hspec.2 a ha

private theorem le_max_of_mem {xs : Series} {a : Int} (ha : a ∈ xs) : a ≤ xs.max?.getD 0 := by
  cases xs with
  | nil => simp at ha
  | cons b bs =>
      cases hm : (b :: bs).max? with
      | none =>
          have heq : b :: bs = [] := List.max?_eq_none_iff.mp hm
          cases heq
      | some M =>
          have hspec := List.max?_eq_some_iff.mp hm
          simpa [hm] using hspec.2 a ha

/-! For a value between `lo` and `hi`, its centered double is at most
    the full range in absolute value. -/
private theorem centered_sq_le (lo a hi : Int) (hlo : lo ≤ a) (hhi : a ≤ hi) :
    (2 * a - (lo + hi))^2 ≤ (hi - lo)^2 := by
  have h1 : 0 ≤ a - lo := by omega
  have h2 : 0 ≤ hi - a := by omega
  have h12 : 0 ≤ (a - lo) * (hi - a) := Int.mul_nonneg h1 h2
  have h4 : 0 ≤ 4 * ((a - lo) * (hi - a)) := Int.mul_nonneg (by omega) h12
  have hid : (hi - lo)^2 - (2 * a - (lo + hi))^2 = 4 * ((a - lo) * (hi - a)) := by
    grind
  omega

private theorem sum_centered_le (lo hi : Int) (x : Series)
    (hx : ∀ a ∈ x, lo ≤ a ∧ a ≤ hi) :
    (x.map (fun a => (2 * a - (lo + hi))^2)).sum ≤ (x.length : Int) * (hi - lo)^2 := by
  induction x with
  | nil => simp
  | cons a xs ih =>
      have hmem := hx a (by simp)
      have hxs : ∀ b ∈ xs, lo ≤ b ∧ b ≤ hi := by
        intro b hb
        exact hx b (by simp [hb])
      have ha := centered_sq_le lo a hi hmem.1 hmem.2
      have hih := ih hxs
      simp only [List.map_cons, List.sum_cons, List.length_cons]
      have hcast : (((xs.length + 1 : Nat) : Int)) = (xs.length : Int) + 1 := by simp
      rw [hcast]
      have hstep : ((xs.length : Int) + 1) * (hi - lo)^2 =
          (xs.length : Int) * (hi - lo)^2 + (hi - lo)^2 := by grind
      rw [hstep]
      omega

private theorem sum_centered (lo hi : Int) (x : Series) :
    (x.map (fun a => (2 * a - (lo + hi))^2)).sum =
      4 * (x.map (fun a => a^2)).sum - 4 * (lo + hi) * x.sum +
        (x.length : Int) * (lo + hi)^2 := by
  induction x with
  | nil => simp
  | cons a xs ih =>
      simp only [List.map_cons, List.sum_cons, List.length_cons]
      have hcast : (((xs.length + 1 : Nat) : Int)) = (xs.length : Int) + 1 := by simp
      rw [hcast, ih]
      grind

private theorem bound_of_bounds (lo hi : Int) (x : Series)
    (hx : ∀ a ∈ x, lo ≤ a ∧ a ≤ hi) :
    4 * CenteredScatter.measure x ≤ (x.length : Int)^2 * (hi - lo)^2 := by
  rw [moment]
  have hsum := sum_centered_le lo hi x hx
  rw [sum_centered lo hi x] at hsum
  have hA : 0 ≤ (x.length : Int) * (hi - lo)^2 -
      (4 * (x.map (fun a => a^2)).sum - 4 * (lo + hi) * x.sum +
        (x.length : Int) * (lo + hi)^2) := by
    omega
  have hn : 0 ≤ (x.length : Int) := by omega
  have hB : 0 ≤ (2 * x.sum - (x.length : Int) * (lo + hi))^2 := Int.sq_nonneg _
  have hnon : 0 ≤ (x.length : Int) * ((x.length : Int) * (hi - lo)^2 -
      (4 * (x.map (fun a => a^2)).sum - 4 * (lo + hi) * x.sum +
        (x.length : Int) * (lo + hi)^2)) +
      (2 * x.sum - (x.length : Int) * (lo + hi))^2 :=
    Int.add_nonneg (Int.mul_nonneg hn hA) hB
  have hid : (x.length : Int) * ((x.length : Int) * (hi - lo)^2 -
      (4 * (x.map (fun a => a^2)).sum - 4 * (lo + hi) * x.sum +
        (x.length : Int) * (lo + hi)^2)) +
      (2 * x.sum - (x.length : Int) * (lo + hi))^2 =
      (x.length : Int)^2 * (hi - lo)^2 -
        4 * ((x.length : Int) * (x.map (fun a => a^2)).sum - x.sum^2) := by
    grind
  omega

theorem sound_CenteredScatter_OutlierCap : CenteredScatter.OutlierCap.Goal := by
  intro x
  cases x with
  | nil => simp [CenteredScatter.measure, pairs, spread, size]
  | cons a xs =>
      let y : Series := a :: xs
      have hy : ∀ b ∈ y, y.min?.getD 0 ≤ b ∧ b ≤ y.max?.getD 0 := by
        intro b hb
        exact ⟨min_le_of_mem hb, le_max_of_mem hb⟩
      have h := bound_of_bounds (y.min?.getD 0) (y.max?.getD 0) y hy
      simpa [y, size, spread] using h

end TinylambOpt.ResearchProofs.Atlas.Goal_CenteredScatter_OutlierCap
