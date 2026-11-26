import TinylambOpt.ResearchGoals.Atlas.EncodedPages

/-!
SQL scans and aggregates can use encoded page structure when positions and arithmetic
summaries agree with expanded rows.

Proved

- `TriangleWave.PeakPositions` — `∀ c, c.step ≠ 0 → (((rows layout c).filter (fun r =>
  r.2 == c.base+c.step*width c)).map Prod.fst) = (List.range c.n).filter (fun i =>
  i%(2*width c) == width c)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_TriangleWave_PeakPositions

open TinylambOpt.ResearchGoals.Atlas.EncodedPages
open TinylambOpt.ResearchGoals.Atlas.EncodedPages.TriangleWave

private theorem filterMap_some_eq_map {α β : Type} (l : List α) (f : α → β) :
    l.filterMap (fun x => some (f x)) = l.map f := by
  induction l with
  | nil => rfl
  | cons x xs ih => simp [ih]

private theorem range_filterMap_all {α : Type} (n : Nat) (f : Nat → Option α) (g : Nat → α)
    (h : ∀ i, i < n → f i = some (g i)) :
    (List.range n).filterMap f = (List.range n).map g := by
  rw [← filterMap_some_eq_map (List.range n) g]
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ]
      simp only [List.filterMap_append]
      rw [ih (fun i hi => h i (by omega))]
      simp [h n (by omega)]

private def val (c : Config) (i : Nat) : Int :=
  let w := width c
  let r := i % (2 * w)
  c.base + c.step * (min r (2 * w - r) : Nat)

private theorem read_eq (c : Config) (i : Nat) : layout.read c i = some (val c i) := by
  rfl

private theorem rows_eq (c : Config) :
    rows layout c = (List.range c.n).map (fun i => (i, val c i)) := by
  unfold rows
  apply range_filterMap_all
  intro i hi
  simp [read_eq]

private theorem filter_map_fst {α : Type} (l : List Nat) (f : Nat → α) (p : Nat → α → Bool) :
    ((l.map (fun i => (i, f i))).filter (fun r => p r.1 r.2)).map Prod.fst =
      l.filter (fun i => p i (f i)) := by
  induction l with
  | nil => rfl
  | cons i l ih =>
      by_cases h : p i (f i)
      · simp [h, ih]
      · simp [h, ih]

private theorem filter_eq_of_pred_eq {α : Type} (l : List α) (p q : α → Bool)
    (h : ∀ x ∈ l, p x = q x) : l.filter p = l.filter q := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      simp only [List.filter_cons]
      rw [h x (by simp)]
      rw [ih (fun y hy => h y (by simp [hy]))]

/-- A triangular-wave sample reaches its amplitude exactly at phase `w`. -/
private theorem contour_eq (w i : Nat) (hw : 0 < w) :
    min (i % (2 * w)) (2 * w - i % (2 * w)) = w ↔ i % (2 * w) = w := by
  let r := i % (2 * w)
  have hrlt : r < 2 * w := by
    dsimp [r]
    exact Nat.mod_lt _ (by omega)
  constructor
  · intro hmin
    change min r (2 * w - r) = w at hmin
    by_cases hr : r ≤ w
    · have hleft : min r (2 * w - r) = r := Nat.min_eq_left (by omega)
      rw [hleft] at hmin
      change r = w
      exact hmin
    · have hgt : w < r := by omega
      have hright : min r (2 * w - r) = 2 * w - r := Nat.min_eq_right (by omega)
      rw [hright] at hmin
      change r = w
      omega
  · intro hr
    change r = w at hr
    have hsub : 2 * w - w = w := by omega
    change min r (2 * w - r) = w
    rw [hr, hsub]
    simp

private theorem peak_pred (c : Config) (i : Nat) (hstep : c.step ≠ 0) :
    (val c i == c.base + c.step * (width c : Int)) =
      (i % (2 * width c) == width c) := by
  have hw : 0 < width c := by unfold width; omega
  have hc := contour_eq (width c) i hw
  by_cases h : i % (2 * width c) = width c
  · have hb1 : val c i == c.base + c.step * (width c : Int) := by
      have hcv : min (i % (2 * width c)) (2 * width c - i % (2 * width c)) = width c :=
        hc.mpr h
      simp [val, hcv]
    have hb2 : i % (2 * width c) == width c := by simp [h]
    rw [hb1, hb2]
  · have hcvne : min (i % (2 * width c)) (2 * width c - i % (2 * width c)) ≠ width c := by
      intro heq
      exact h (hc.mp heq)
    have hne : val c i ≠ c.base + c.step * (width c : Int) := by
      intro heq
      have hm : c.step * ((min (i % (2 * width c)) (2 * width c - i % (2 * width c)) : Nat) : Int) =
          c.step * (width c : Int) := by
        exact Int.add_left_cancel heq
      have hcast :
          ((min (i % (2 * width c)) (2 * width c - i % (2 * width c)) : Nat) : Int) =
            (width c : Int) :=
        Int.eq_of_mul_eq_mul_left hstep hm
      have heqnat : min (i % (2 * width c)) (2 * width c - i % (2 * width c)) = width c := by
        exact_mod_cast hcast
      exact hcvne heqnat
    have hb1 : (val c i == c.base + c.step * (width c : Int)) = false := by
      simp [hne]
    have hb2 : (i % (2 * width c) == width c) = false := by
      simp [h]
    rw [hb1, hb2]

theorem sound_TriangleWave_PeakPositions : TriangleWave.PeakPositions.Goal := by
  intro c hstep
  rw [rows_eq c]
  rw [filter_map_fst (List.range c.n) (val c)
    (fun _ v => v == c.base + c.step * (width c : Int))]
  apply filter_eq_of_pred_eq
  intro i hi
  exact peak_pred c i hstep

end TinylambOpt.ResearchProofs.Atlas.Goal_TriangleWave_PeakPositions
