import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_AlternatingBalance_ParityCarry

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures
open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.AlternatingBalance

/-! Brute-force sanity check: all series of length ≤ 3 over [-2, 2]. -/
#eval do
  let vals : List Int := [-2, -1, 0, 1, 2]
  let rec lists : Nat → List (List Int)
    | 0 => [[]]
    | n + 1 => lists n ++ vals.flatMap (fun a => (lists n).map (fun xs => a :: xs))
  let ls := lists 3
  pure ((ls.flatMap (fun x => ls.map (fun y => (x, y)))).filter (fun (x, y) =>
    measure (x ++ y) != measure x + (-1 : Int) ^ x.length * measure y) |>.length)

/-! #### Alternating (parity-weighted) sum helpers -/

private def alt (k : Nat) (x : Series) : Int :=
  ((x.zipIdx k).map (fun (v, i) => (-1 : Int) ^ i * v)).sum

private theorem measure_alt (x : Series) : AlternatingBalance.measure x = alt 0 x := by
  rfl

private theorem alt_cons (k : Nat) (a : Int) (xs : Series) :
    alt k (a :: xs) = (-1 : Int) ^ k * a + alt (k + 1) xs := by
  simp [alt, List.zipIdx_cons]

private theorem neg_one_pow_succ (n : Nat) : (-1 : Int) ^ (n + 1) = -((-1 : Int) ^ n) := by
  rw [Int.pow_succ]
  omega

private theorem alt_shift (k : Nat) (x : Series) : alt k x = (-1 : Int) ^ k * alt 0 x := by
  induction x generalizing k with
  | nil => simp [alt]
  | cons a xs ih =>
      rw [alt_cons, ih (k + 1)]
      rw [alt_cons, ih 1]
      rw [neg_one_pow_succ]
      simp only [Int.pow_zero]
      grind

private theorem alt_append (k : Nat) (x y : Series) :
    alt k (x ++ y) = alt k x + alt (k + x.length) y := by
  induction x generalizing k with
  | nil => simp [alt]
  | cons a xs ih =>
      rw [List.cons_append, alt_cons, alt_cons, ih (k + 1)]
      rw [show k + 1 + xs.length = k + (xs.length + 1) by omega]
      simp only [List.length_cons]
      omega

theorem sound_AlternatingBalance_ParityCarry : AlternatingBalance.ParityCarry.Goal := by
  intro x y
  rw [measure_alt, measure_alt, measure_alt]
  rw [alt_append]
  simp only [Nat.zero_add]
  rw [alt_shift (x.length) y]

end TinylambOpt.ResearchProofs.Atlas.Goal_AlternatingBalance_ParityCarry
