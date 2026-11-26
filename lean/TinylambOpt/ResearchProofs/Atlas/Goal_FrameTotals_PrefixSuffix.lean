import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

/-!
SQL execution can use cumulative counts or ordered measures to answer range and rank
queries under the modeled bounds.

Proved

- `FrameTotals.PrefixSuffix` — `∀ x y, measure (x++y) = measure x + measure y + size
  y*(suffs x).sum + size x*(prefs y).sum`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_FrameTotals_PrefixSuffix

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures
open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.FrameTotals

/-! Small exhaustive sanity check before the proof: all series of length <= 3
with entries in [-2, 2], for every pair of series. -/
def vecs : Nat → List (List Int)
  | 0 => [[]]
  | n + 1 => (List.range 5).flatMap (fun i =>
      (vecs n).map (fun xs => (Int.ofNat i - 2) :: xs))

#eval! (vecs 3).all (fun x => (vecs 3).all (fun y =>
  decide (FrameTotals.measure (x ++ y) =
    FrameTotals.measure x + FrameTotals.measure y +
      size y * (suffs x).sum + size x * (prefs y).sum)))

private theorem sum_map_add_int {α : Type} (xs : List α) (f g : α → Int) :
    (xs.map (fun x => f x + g x)).sum = (xs.map f).sum + (xs.map g).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [ih]
      grind

private theorem sum_map_const_int {α : Type} (xs : List α) (c : Int) :
    (xs.map (fun _ => c)).sum = (xs.length : Int) * c := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons, List.length_cons, Int.natCast_add,
        Int.natCast_one]
      rw [ih]
      grind

private theorem sum_map_add_const_int {α : Type} (xs : List α) (f : α → Int) (c : Int) :
    (xs.map (fun x => f x + c)).sum = (xs.map f).sum + (xs.length : Int) * c := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons, List.length_cons, ih]
      push_cast
      simp only [Int.add_mul, Int.one_mul]
      ac_rfl

private theorem sum_map_const_add_int {α : Type} (xs : List α) (c : Int) (f : α → Int) :
    (xs.map (fun x => c + f x)).sum = (xs.length : Int) * c + (xs.map f).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons, List.length_cons, ih]
      push_cast
      simp only [Int.add_mul, Int.one_mul]
      ac_rfl

private theorem prefs_cons (a : Int) (xs : Series) :
    prefs (a :: xs) = a :: (prefs xs).map (fun v => a + v) := by
  simp [prefs, List.range_succ_eq_map, List.take_succ_cons, List.map_map,
    Function.comp_def]

private theorem suffs_cons (a : Int) (xs : Series) :
    suffs (a :: xs) = (a + xs.sum) :: suffs xs := by
  simp [suffs, List.range_succ_eq_map, List.drop_succ_cons, List.map_map,
    Function.comp_def]

private theorem suffs_cons_sum (a : Int) (xs : Series) :
    (suffs (a :: xs)).sum = a + xs.sum + (suffs xs).sum := by
  rw [suffs_cons]
  simp only [List.sum_cons]

private theorem prefix_exposure_cons (a : Int) (xs : Series) :
    (prefs (a :: xs)).sum = ((xs.length + 1 : Nat) : Int) * a + (prefs xs).sum := by
  rw [prefs_cons]
  simp only [List.sum_cons]
  have hsum : ((prefs xs).map (fun v => a + v)).sum =
      (xs.length : Int) * a + (prefs xs).sum := by
    simpa [prefs, Function.comp_def] using
      sum_map_const_add_int (prefs xs) a (fun v => v)
  rw [hsum]
  push_cast
  simp only [Int.add_mul, Int.one_mul]
  ac_rfl

private theorem prefs_append_sum (xs ys : Series) :
    (prefs (xs ++ ys)).sum =
      (prefs xs).sum + (ys.length : Int) * xs.sum + (prefs ys).sum := by
  induction xs with
  | nil => simp [prefs]
  | cons a xs ih =>
      rw [List.cons_append, prefix_exposure_cons, ih]
      simp only [List.length_append, List.sum_cons]
      rw [prefix_exposure_cons]
      push_cast
      simp only [Int.add_mul, Int.mul_add, Int.one_mul]
      ac_rfl

private theorem windows_cons_prefix (a : Int) (xs : Series) :
    windows (a :: xs) =
      [[a]] ++ ((List.range xs.length).map (fun i => a :: xs.take (i + 1))) ++ windows xs := by
  unfold windows
  rw [List.length_cons, List.range_succ_eq_map]
  simp only [List.flatMap_cons, List.flatMap_map, Nat.succ_eq_add_one]
  congr 2
  · simp [List.range_succ_eq_map, List.drop_zero, List.take_succ_cons, List.take_zero]
  · funext i
    simp

private theorem frameTotals_cons (a : Int) (xs : Series) :
    FrameTotals.measure (a :: xs) =
      FrameTotals.measure xs + (prefs xs).sum + ((xs.length + 1 : Nat) : Int) * a := by
  unfold FrameTotals.measure
  rw [windows_cons_prefix]
  simp only [List.map_append, List.sum_append, List.map_cons, List.sum_cons,
    List.map_map, Function.comp_def]
  rw [sum_map_add_int, sum_map_const_int]
  simp only [List.length_range, List.sum_nil, Int.add_zero]
  unfold prefs
  push_cast
  simp only [Int.add_mul, Int.one_mul]
  ac_rfl

theorem sound_FrameTotals_PrefixSuffix : FrameTotals.PrefixSuffix.Goal := by
  intro x
  induction x with
  | nil =>
      intro y
      simp [FrameTotals.measure, windows, suffs, prefs, size]
  | cons a xs ih =>
      intro y
      rw [List.cons_append, frameTotals_cons, prefs_append_sum, ih y]
      rw [frameTotals_cons, suffs_cons_sum]
      simp only [size, List.length_cons, List.length_append, Int.natCast_add, Int.natCast_one]
      push_cast
      simp only [Int.add_mul, Int.mul_add, Int.one_mul]
      ac_rfl

end TinylambOpt.ResearchProofs.Atlas.Goal_FrameTotals_PrefixSuffix
