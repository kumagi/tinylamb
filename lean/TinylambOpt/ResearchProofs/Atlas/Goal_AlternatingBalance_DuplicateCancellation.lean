import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

/-!
SQL execution can use cumulative counts or ordered measures to answer range and rank
queries under the modeled bounds.

Proved

- `AlternatingBalance.DuplicateCancellation` — `∀ x n, measure (x.flatMap (repeated n))
  = if n%2 == 0 then 0 else measure x`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_AlternatingBalance_DuplicateCancellation

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

/-! Brute-force sanity check: all series of length <= 4 over [-2, 2], n <= 6. -/
#eval do
  let vals : List Int := [-2, -1, 0, 1, 2]
  let rec lists : Nat → List (List Int)
    | 0 => [[]]
    | n + 1 => lists n ++ vals.flatMap (fun a => (lists n).map (fun xs => a :: xs))
  pure ((lists 4).filter (fun x : Series =>
    (List.range 7).any (fun n =>
      AlternatingBalance.measure (x.flatMap (repeated n)) !=
        (if n % 2 == 0 then 0 else AlternatingBalance.measure x))) |>.length)

/-! #### Parity-weighted alternating sums -/

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

private theorem alt_replicate (k : Nat) (a : Int) (n : Nat) :
    alt k (List.replicate n a) = if n % 2 == 0 then 0 else (-1 : Int) ^ k * a := by
  induction n generalizing k with
  | zero => simp [alt]
  | succ n ih =>
      rw [List.replicate_succ]
      rw [alt_cons, ih (k + 1)]
      by_cases hn : n % 2 = 0
      · have hn1 : (n + 1) % 2 = 1 := by omega
        simp [hn, hn1]
      · have hn1 : n % 2 = 1 := by omega
        have hn2 : (n + 1) % 2 = 0 := by omega
        simp [hn1, hn2]
        rw [neg_one_pow_succ, Int.neg_mul]
        omega

theorem sound_AlternatingBalance_DuplicateCancellation :
    AlternatingBalance.DuplicateCancellation.Goal := by
  intro x n
  rw [measure_alt]
  induction x with
  | nil => simp [alt, AlternatingBalance.measure]
  | cons a xs ih =>
      simp only [List.flatMap_cons]
      rw [alt_append]
      simp only [Nat.zero_add]
      change alt 0 (repeated n a) + alt (repeated n a).length (xs.flatMap (repeated n)) =
        if n % 2 == 0 then 0 else AlternatingBalance.measure (a :: xs)
      rw [show repeated n a = List.replicate n a by rfl]
      simp only [List.length_replicate]
      rw [alt_replicate]
      rw [alt_shift]
      rw [ih]
      by_cases hn : n % 2 = 0
      · simp [hn, neg_one_pow_of_mod_zero n hn]
      · have hn1 : n % 2 = 1 := by omega
        simp [hn1, neg_one_pow_of_mod_one n hn1]
        change a + -alt 0 xs = alt 0 (a :: xs)
        rw [alt_cons]
        rw [alt_shift 1 xs]
        simp [neg_one_pow_of_mod_one 1 (by decide)]

end TinylambOpt.ResearchProofs.Atlas.Goal_AlternatingBalance_DuplicateCancellation
