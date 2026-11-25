import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_AlternatingBalance_BackwardPhase

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures
open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.AlternatingBalance

/-! Brute-force sanity check: all series of length <= 4 over [-2, 2]. -/
#eval do
  let vals : List Int := [-2, -1, 0, 1, 2]
  let rec lists : Nat → List (List Int)
    | 0 => [[]]
    | n + 1 => lists n ++ vals.flatMap (fun a => (lists n).map (fun xs => a :: xs))
  pure ((lists 4).filter (fun x : Series =>
    measure x.reverse != (-1 : Int) ^ (x.length - 1) * measure x) |>.length)

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

private theorem neg_one_pow_succ' (n : Nat) : (-1 : Int) ^ (1 + n) = -((-1 : Int) ^ n) := by
  rw [show 1 + n = n + 1 by omega, neg_one_pow_succ]

private theorem alt_shift (k : Nat) (x : Series) : alt k x = (-1 : Int) ^ k * alt 0 x := by
  induction x generalizing k with
  | nil => simp [alt]
  | cons a xs ih =>
      rw [alt_cons, ih (k + 1), alt_cons, ih 1]
      rw [neg_one_pow_succ]
      simp only [Int.pow_zero]
      grind

private theorem alt_one (x : Series) : alt 1 x = -alt 0 x := by
  rw [alt_shift 1]
  simp

private theorem alt_append (k : Nat) (x y : Series) :
    alt k (x ++ y) = alt k x + alt (k + x.length) y := by
  induction x generalizing k with
  | nil => simp [alt]
  | cons a xs ih =>
      rw [List.cons_append, alt_cons, alt_cons, ih (k + 1)]
      rw [show k + 1 + xs.length = k + (xs.length + 1) by omega]
      simp only [List.length_cons]
      omega

/-! Reversing shifts the phase by one: `alt 0 x.reverse = -((-1)^length * alt 0 x)`.
    Keeping this form avoids reasoning about truncated natural subtraction. -/
private theorem alt_reverse_neg (x : Series) :
    alt 0 x.reverse = -((-1 : Int) ^ x.length * alt 0 x) := by
  have hrev : alt 1 x.reverse = (-1 : Int) ^ x.length * alt 0 x := by
    induction x with
    | nil => simp [alt]
    | cons a xs ih =>
        rw [List.reverse_cons, alt_append]
        rw [List.length_reverse]
        rw [ih, alt_cons, alt_cons, alt_one xs]
        simp [alt, neg_one_pow_succ, neg_one_pow_succ']
        grind
  calc
    alt 0 x.reverse = -alt 1 x.reverse := by
      rw [alt_one]
      simp
    _ = -((-1 : Int) ^ x.length * alt 0 x) := by rw [hrev]

theorem sound_AlternatingBalance_BackwardPhase : AlternatingBalance.BackwardPhase.Goal := by
  intro x
  rw [measure_alt, measure_alt]
  cases x with
  | nil => simp [alt]
  | cons a xs =>
      rw [alt_reverse_neg]
      simp only [List.length_cons, Nat.add_sub_cancel]
      rw [neg_one_pow_succ]
      grind

end TinylambOpt.ResearchProofs.Atlas.Goal_AlternatingBalance_BackwardPhase
