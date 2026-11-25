import TinylambOpt.ResearchGoals.Frontiers.Arithmetic

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.NewGoalsBatch.A02

open TinylambOpt.ResearchGoals.Frontiers.Arithmetic

/-! Per-goal notes:
  * FirstOverflow: the group-level `safe` predicate checks exactly all prefix
    sums of a block; since earlier blocks are safe, the first violating row is
    in the first unsafe block.
  * QuadraticJoin: true by expanding `(x-y)^2` and collecting terms.
  * QuantizedAverage: each remainder is in `[0, scale)`, so its contribution is
    at most `scale-1` when nonzero, and zero otherwise.
  * CenteredProduct: residues are the true products modulo `2^bits`; the
    magnitude bound puts the signed sum in `(-M/2, M/2)`, so emod reconstruction
    is exact.
  * ShiftedCovariance: the shift terms cancel algebraically. -/

private theorem mul_two (a : Int) : a * 2 = a + a := by omega

private theorem add_two_mul (k : Int) (a : Int) : (k + 1) * a = a + k * a := by
  simp [Int.add_mul]
  omega

/-! ## QuadraticJoin -/

private theorem flatten_sum (xss : List (List Int)) :
    xss.flatten.sum = (xss.map List.sum).sum := by
  induction xss with
  | nil => simp
  | cons xs xss ih => simp [List.sum_append, ih]

private theorem flatMap_sum (f : Int → List Int) (xs : List Int) :
    (xs.flatMap f).sum = (xs.map (fun x => (f x).sum)).sum := by
  change ((xs.map f).flatten).sum = (xs.map (fun x => (f x).sum)).sum
  rw [flatten_sum, List.map_map]
  rfl

private theorem sq_diff_sum (x : Int) (b : List Int) :
    (b.map (fun y => (x - y) ^ 2)).sum =
      (b.length : Int) * x ^ 2 - 2 * x * b.sum + (b.map (fun y => y ^ 2)).sum := by
  induction b with
  | nil => simp
  | cons y ys ih =>
      simp [ih, add_two_mul]
      simp [Int.pow_succ, Int.sub_eq_add_neg, Int.mul_add, Int.mul_comm, mul_two, Int.mul_neg]
      omega

private theorem sum_sq_diff_rows (a b : List Int) :
    (a.map (fun x => (b.map (fun y => (x - y) ^ 2)).sum)).sum =
      (b.length : Int) * (a.map (fun x => x ^ 2)).sum +
        (a.length : Int) * (b.map (fun x => x ^ 2)).sum - 2 * a.sum * b.sum := by
  induction a with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [sq_diff_sum x b, ih]
      simp [add_two_mul]
      simp [Int.pow_succ, Int.mul_add, Int.add_mul, Int.mul_comm, mul_two]
      omega

theorem QuadraticJoin_sound :
    TinylambOpt.ResearchGoals.Frontiers.Arithmetic.QuadraticJoin.Goal := by
  intro a b
  rw [flatMap_sum]
  exact sum_sq_diff_rows a b

/-! ## QuantizedAverage -/

private theorem emod_sum_bound (xs : List Int) (scale : Int) (hs : 0 < scale) :
    xs.sum ≤ scale * (xs.map (fun x => x / scale)).sum +
      (xs.countP (fun x => x % scale != 0) : Int) * (scale - 1) := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      have hsne : scale ≠ 0 := by omega
      have hnonneg : 0 ≤ x % scale := Int.emod_nonneg _ hsne
      have hlt : x % scale < scale := Int.emod_lt_of_pos _ hs
      have hdecomp : scale * (x / scale) + x % scale = x := Int.mul_ediv_add_emod x scale
      by_cases hx : x % scale = 0
      · simp [hx] at ih ⊢
        simp [Int.mul_add] at ih ⊢
        omega
      · simp [hx] at ih ⊢
        simp [Int.mul_add, Int.add_mul] at ih ⊢
        omega

theorem QuantizedAverage_sound :
    TinylambOpt.ResearchGoals.Frontiers.Arithmetic.QuantizedAverage.Goal := by
  intro xs scale cut hs hne
  dsimp
  have hb := emod_sum_bound xs scale hs
  omega

/-! ## ShiftedCovariance -/

theorem ShiftedCovariance_sound :
    TinylambOpt.ResearchGoals.Frontiers.Arithmetic.ShiftedCovariance.Goal := by
  intro rows ax ay
  induction rows with
  | nil => simp
  | cons r rs ih =>
      dsimp at ih ⊢
      simp only [Function.comp_def] at ih ⊢
      simp only [List.map_cons, List.sum_cons]
      simp [add_two_mul]
      simp [Int.sub_eq_add_neg, Int.mul_add, Int.add_mul, Int.mul_comm, Int.mul_left_comm,
        Int.mul_assoc, mul_two, Int.mul_neg] at ih ⊢
      omega

end TinylambOpt.ResearchProofs.NewGoalsBatch.A02
