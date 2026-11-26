import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

/-!
SQL execution can use cumulative counts or ordered measures to answer range and rank
queries under the modeled bounds.

Proved

- `AlternatingBalance.PeriodicParity` — `∀ x n, measure ((List.replicate n x).flatten)
  = if x.length%2 == 0 then (n:Int)*measure x else ((n%2:Nat):Int)*measure x`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_AlternatingBalance_PeriodicParity

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures
open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.AlternatingBalance

/-! Brute-force sanity check: all series of length <= 3 over [-2, 2], n <= 6. -/
#eval do
  let vals : List Int := [-2, -1, 0, 1, 2]
  let rec lists : Nat → List (List Int)
    | 0 => [[]]
    | n + 1 => lists n ++ vals.flatMap (fun a => (lists n).map (fun xs => a :: xs))
  pure ((lists 3).filter (fun x : Series =>
    (List.range 7).any (fun n =>
      measure ((List.replicate n x).flatten) !=
        (if x.length % 2 == 0 then (n : Int) * measure x else ((n % 2 : Nat) : Int) * measure x))) |>.length)

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
      rw [alt_cons, ih (k + 1), alt_cons, ih 1]
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

private theorem neg_one_pow_two_mul (m : Nat) : (-1 : Int) ^ (2 * m) = 1 := by
  induction m with
  | zero => simp
  | succ m ih =>
      rw [Nat.mul_succ, Int.pow_add, ih]
      simp

private theorem neg_one_pow_of_mod_zero (n : Nat) (h : n % 2 = 0) : (-1 : Int) ^ n = 1 := by
  have hn : n = 2 * (n / 2) := by omega
  rw [hn, neg_one_pow_two_mul]

private theorem neg_one_pow_of_mod_one (n : Nat) (h : n % 2 = 1) : (-1 : Int) ^ n = -1 := by
  have hn : n = 2 * (n / 2) + 1 := by omega
  rw [hn, Int.pow_add, neg_one_pow_two_mul]
  simp

theorem sound_AlternatingBalance_PeriodicParity : AlternatingBalance.PeriodicParity.Goal := by
  intro x n
  rw [measure_alt]
  have hrep : ∀ n : Nat, alt 0 ((List.replicate n x).flatten) =
      if x.length % 2 == 0 then (n : Int) * alt 0 x else ((n % 2 : Nat) : Int) * alt 0 x := by
    intro n
    induction n with
    | zero => simp [alt]
    | succ n ih =>
        have happ : (List.replicate (n + 1) x).flatten = x ++ (List.replicate n x).flatten := by
          rw [List.replicate_succ, List.flatten_cons]
        rw [happ, alt_append]
        simp only [Nat.zero_add]
        rw [alt_shift x.length ((List.replicate n x).flatten)]
        rw [ih]
        by_cases he0 : x.length % 2 = 0
        · rw [neg_one_pow_of_mod_zero x.length he0]
          simp [he0]
          grind
        · have he1 : x.length % 2 = 1 := by omega
          rw [neg_one_pow_of_mod_one x.length he1]
          have hnpar : n % 2 = 0 ∨ n % 2 = 1 := by omega
          rcases hnpar with hn0 | hn1
          · have hn1' : (n + 1) % 2 = 1 := by omega
            simp [he0, hn0, hn1']
          · have hn1' : (n + 1) % 2 = 0 := by omega
            simp [he0, hn1, hn1']
            grind
  exact hrep n

end TinylambOpt.ResearchProofs.Atlas.Goal_AlternatingBalance_PeriodicParity
