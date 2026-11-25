import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_QuadraticVariation_Affine

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures
open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.QuadraticVariation

/-! Brute-force truth check over all lists of length <= 3 with entries in [-2, 2]
    and all affine parameters in [-2, 2]. -/
#eval do
  let vals : List Int := [-2, -1, 0, 1, 2]
  let rec gen : Nat → List (List Int)
    | 0 => [[]]
    | k + 1 => gen k ++ vals.flatMap (fun v => (gen k).map (fun xs => v :: xs))
  let bad := (gen 3).flatMap (fun x =>
    vals.flatMap (fun a =>
      vals.filterMap (fun b =>
        if QuadraticVariation.measure (shifted b (scaled a x)) != a^2 * QuadraticVariation.measure x
        then some (a, b, x) else none)))
  pure bad

private theorem adj_map (f : Int → Int) (x : Series) :
    adj (x.map f) = (adj x).map (fun p : Int × Int => (f p.1, f p.2)) := by
  unfold adj
  rw [← List.map_drop]
  rw [List.zip_map]
  simp [Prod.map]

private theorem shifted_scaled_eq (a b : Int) (x : Series) :
    shifted b (scaled a x) = x.map (fun v => a * v + b) := by
  unfold shifted scaled
  rw [List.map_map]
  rfl

private theorem delta_affine (a b : Int) (x : Series) :
    delta (shifted b (scaled a x)) = (delta x).map (fun d => a * d) := by
  unfold delta
  rw [shifted_scaled_eq, adj_map]
  simp only [List.map_map]
  apply List.map_congr_left
  intro p hp
  rcases p with ⟨u, v⟩
  simp only [Function.comp_apply]
  rw [Int.mul_sub]
  omega

private theorem square_mul (a d : Int) : (a * d)^2 = a^2 * d^2 := by
  grind

theorem sound_Affine : QuadraticVariation.Affine.Goal := by
  intro a b x
  unfold QuadraticVariation.measure
  rw [delta_affine]
  have hsq : ((delta x).map (fun d => a * d)).map (fun d : Int => d ^ 2) =
      (delta x).map (fun d => a^2 * d^2) := by
    simp only [List.map_map]
    apply List.map_congr_left
    intro d hd
    simp only [Function.comp_apply]
    exact square_mul a d
  rw [hsq]
  induction delta x with
  | nil => simp
  | cons d ds ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [Int.mul_add, ih]

end TinylambOpt.ResearchProofs.Atlas.Goal_QuadraticVariation_Affine
