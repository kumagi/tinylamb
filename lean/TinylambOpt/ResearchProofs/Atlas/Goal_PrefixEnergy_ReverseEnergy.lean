import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_PrefixEnergy_ReverseEnergy

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

/-! Brute-force sanity check: all series of length <= 4 over [-4, 4]. -/
#eval do
  let vals : List Int := [-4, -3, -2, -1, 0, 1, 2, 3, 4]
  let rec lists : Nat → List (List Int)
    | 0 => [[]]
    | n + 1 => lists n ++ vals.flatMap (fun a => (lists n).map (fun xs => a :: xs))
  pure ((lists 4).filter (fun x : List Int =>
    PrefixEnergy.measure x.reverse !=
      PrefixEnergy.measure x - 2 * x.sum * PrefixExposure.measure x +
        (size x + 1) * x.sum ^ 2) |>.length)

private theorem prefs_cons (a : Int) (xs : Series) :
    prefs (a :: xs) = a :: (prefs xs).map (fun s => a + s) := by
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
      rw [show (prefs ys).map (fun v => a + (xs.sum + v)) =
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

private theorem sum_map_sq_affine (c : Int) (xs : List Int) :
    (xs.map (fun v => (c + v) ^ 2)).sum =
      (xs.length : Int) * c ^ 2 + 2 * c * xs.sum + (xs.map (fun v => v ^ 2)).sum := by
  induction xs with
  | nil => simp
  | cons v xs ih =>
      simp only [List.map_cons, List.sum_cons, List.length_cons] at ih ⊢
      rw [ih]
      grind

private theorem exposure_append_singleton (xs : Series) (a : Int) :
    PrefixExposure.measure (xs ++ [a]) =
      PrefixExposure.measure xs + xs.sum + a := by
  unfold PrefixExposure.measure
  rw [prefs_append]
  simp [prefs]
  grind

private theorem exposure_cons (a : Int) (xs : Series) :
    PrefixExposure.measure (a :: xs) =
      a + (xs.length : Int) * a + PrefixExposure.measure xs := by
  unfold PrefixExposure.measure
  rw [prefs_cons, List.sum_cons, sum_map_add_const, prefs_length]
  grind

private theorem energy_append_singleton (xs : Series) (a : Int) :
    PrefixEnergy.measure (xs ++ [a]) =
      PrefixEnergy.measure xs + (xs.sum + a) ^ 2 := by
  unfold PrefixEnergy.measure
  rw [prefs_append]
  simp [prefs]

private theorem energy_cons (a : Int) (xs : Series) :
    PrefixEnergy.measure (a :: xs) =
      a ^ 2 + (xs.length : Int) * a ^ 2 + 2 * a * PrefixExposure.measure xs +
        PrefixEnergy.measure xs := by
  unfold PrefixEnergy.measure PrefixExposure.measure
  rw [prefs_cons, List.map_cons, List.sum_cons, List.map_map, Function.comp_def]
  rw [sum_map_sq_affine, prefs_length]
  grind

theorem sound_PrefixEnergy_ReverseEnergy : PrefixEnergy.ReverseEnergy.Goal := by
  intro x
  induction x with
  | nil => simp [PrefixEnergy.measure, PrefixExposure.measure, prefs, size]
  | cons a xs ih =>
      simp only [List.reverse_cons]
      rw [energy_append_singleton, ih, energy_cons]
      rw [exposure_cons]
      unfold size at *
      simp only [List.sum_cons, List.length_cons] at *
      rw [List.sum_reverse]
      push_cast at *
      grind

end TinylambOpt.ResearchProofs.Atlas.Goal_PrefixEnergy_ReverseEnergy
