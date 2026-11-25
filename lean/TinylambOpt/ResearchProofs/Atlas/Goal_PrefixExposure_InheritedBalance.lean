import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_PrefixExposure_InheritedBalance

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures
open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.PrefixExposure

/-! Brute-force sanity check: all pairs of series of length ≤ 3 over [-2, 2]. -/
#eval do
  let vals : List Int := [-2, -1, 0, 1, 2]
  let rec lists : Nat → List (List Int)
    | 0 => [[]]
    | n + 1 => lists n ++ vals.flatMap (fun a => (lists n).map (fun xs => a :: xs))
  let ls := lists 3
  pure ((ls.flatMap (fun x => ls.map (fun y => (x, y)))).filter (fun (x, y) =>
    measure (x ++ y) != measure x + measure y + x.sum * size y) |>.length)

private theorem prefs_cons (a : Int) (xs : Series) :
    prefs (a :: xs) = a :: (prefs xs).map (fun v => a + v) := by
  simp [prefs, List.range_succ_eq_map, List.take_succ_cons, List.map_map,
    Function.comp_def]

private theorem prefs_length (x : Series) : (prefs x).length = x.length := by
  simp [prefs]

private theorem prefs_append (xs ys : Series) :
    prefs (xs ++ ys) = prefs xs ++ (prefs ys).map (fun v => xs.sum + v) := by
  induction xs with
  | nil => simp [prefs]
  | cons a xs ih =>
      rw [List.cons_append, prefs_cons, prefs_cons, ih]
      simp only [List.map_append, List.cons_append, List.map_map,
        Function.comp_def, List.sum_cons]
      rw [show (prefs ys).map (fun x => a + (xs.sum + x)) =
          (prefs ys).map (fun v => a + xs.sum + v) by
        apply List.map_congr_left
        intro v hv
        grind]

private theorem sum_map_add_const (c : Int) (xs : List Int) :
    (xs.map (fun v => c + v)).sum = (xs.length : Int) * c + xs.sum := by
  induction xs with
  | nil => simp
  | cons v xs ih =>
      simp only [List.map_cons, List.sum_cons, List.length_cons] at ih ⊢
      rw [ih]
      grind

theorem sound_PrefixExposure_InheritedBalance : PrefixExposure.InheritedBalance.Goal := by
  intro x y
  change (prefs (x ++ y)).sum = (prefs x).sum + (prefs y).sum + x.sum * (y.length : Int)
  rw [prefs_append, List.sum_append, sum_map_add_const, prefs_length]
  grind

end TinylambOpt.ResearchProofs.Atlas.Goal_PrefixExposure_InheritedBalance
