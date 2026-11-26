import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

/-!
SQL execution can use cumulative counts or ordered measures to answer range and rank
queries under the modeled bounds.

Proved

- `PrefixEnergy.UnitScaling` — `∀ a x, measure (scaled a x) = a^2*measure x`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_PrefixEnergy_UnitScaling

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

private theorem prefs_cons (a : Int) (xs : Series) :
    prefs (a :: xs) = a :: (prefs xs).map (fun v => a + v) := by
  simp [prefs, List.range_succ_eq_map, List.take_succ_cons, List.map_map,
    Function.comp_def]

private theorem prefs_scaled (a : Int) (xs : Series) :
    prefs (scaled a xs) = (prefs xs).map (fun v => a * v) := by
  induction xs with
  | nil => simp [prefs, scaled]
  | cons v xs ih =>
      change prefs (a * v :: scaled a xs) = (prefs (v :: xs)).map (fun v => a * v)
      rw [prefs_cons, prefs_cons, ih]
      simp only [List.map_cons]
      congr 1
      rw [List.map_map, List.map_map]
      apply List.map_congr_left
      intro p hp
      rw [Function.comp_apply, Function.comp_apply]
      grind

private theorem sum_map_mul_sq (a : Int) (xs : List Int) :
    (xs.map (fun v => (a * v) ^ 2)).sum = a ^ 2 * (xs.map (fun v => v ^ 2)).sum := by
  induction xs with
  | nil => simp
  | cons v xs ih =>
      simp only [List.map_cons, List.sum_cons] at ih ⊢
      rw [ih]
      grind

theorem sound_PrefixEnergy_UnitScaling : PrefixEnergy.UnitScaling.Goal := by
  intro a x
  change ((prefs (scaled a x)).map (fun v => v ^ 2)).sum =
    a ^ 2 * ((prefs x).map (fun v => v ^ 2)).sum
  rw [prefs_scaled, List.map_map]
  simp only [Function.comp_def]
  exact sum_map_mul_sq a (prefs x)

end TinylambOpt.ResearchProofs.Atlas.Goal_PrefixEnergy_UnitScaling
