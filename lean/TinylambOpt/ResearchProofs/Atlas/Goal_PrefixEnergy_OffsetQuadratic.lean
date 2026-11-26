import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

/-!
SQL execution can use cumulative counts or ordered measures to answer range and rank
queries under the modeled bounds.

Proved

- `PrefixEnergy.OffsetQuadratic` — `∀ x y, measure (x++y) = measure x + measure y +
  2*x.sum*PrefixExposure.measure y + size y*x.sum^2`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_PrefixEnergy_OffsetQuadratic

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

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

private theorem sum_map_sq_affine (c : Int) (xs : List Int) :
    (xs.map (fun v => (c + v) ^ 2)).sum =
      (xs.length : Int) * c ^ 2 + 2 * c * xs.sum + (xs.map (fun v => v ^ 2)).sum := by
  induction xs with
  | nil => simp
  | cons v xs ih =>
      simp only [List.map_cons, List.sum_cons, List.length_cons] at ih ⊢
      rw [ih]
      grind

theorem sound_PrefixEnergy_OffsetQuadratic : PrefixEnergy.OffsetQuadratic.Goal := by
  intro x y
  change ((prefs (x ++ y)).map (fun v => v ^ 2)).sum =
    ((prefs x).map (fun v => v ^ 2)).sum + ((prefs y).map (fun v => v ^ 2)).sum +
      2 * x.sum * (prefs y).sum + (y.length : Int) * x.sum ^ 2
  rw [prefs_append, List.map_append, List.sum_append, List.map_map, Function.comp_def]
  have hsq := sum_map_sq_affine x.sum (prefs y)
  rw [prefs_length] at hsq
  rw [hsq]
  grind

end TinylambOpt.ResearchProofs.Atlas.Goal_PrefixEnergy_OffsetQuadratic
