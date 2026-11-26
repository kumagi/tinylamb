import TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns

/-!
# ShiftedVarianceNumerator

Subtracting a common offset from the inputs preserves the exact integer variance
numerator.

The aggregate can accumulate differences from a block reference value, reducing the
magnitude of intermediate sums when the values are clustered.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000


namespace TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.ShiftedVarianceNumerator
open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns

theorem sum_sub_const : ∀ (xs : List Int) (o : Int),
    (xs.map (fun x => x - o)).sum = xs.sum - (xs.length : Int) * o
  | [], o => by simp
  | x :: xs, o => by
      simp only [List.map_cons, List.sum_cons, List.length_cons, Int.natCast_add, Int.natCast_one]
      rw [sum_sub_const xs o]
      rw [Int.add_mul]
      omega

theorem sumsq_sub_const : ∀ (xs : List Int) (o : Int),
    total (xs.map (fun x => x - o)) (fun x => x * x) =
      total xs (fun x => x * x) - 2 * o * xs.sum + (xs.length : Int) * o * o
  | [], o => by simp [total]
  | x :: xs, o => by
      rw [show total ((x :: xs).map (fun x => x - o)) (fun x => x * x) =
          (x - o) * (x - o) + total (xs.map (fun x => x - o)) (fun x => x * x) by simp [total]]
      rw [show total (x :: xs) (fun x => x * x) = x * x + total xs (fun x => x * x) by simp [total]]
      rw [sumsq_sub_const xs o]
      simp only [total, List.sum_cons, List.length_cons, Int.natCast_add, Int.natCast_one]
      rw [Int.mul_sub, Int.sub_mul, Int.sub_mul, Int.add_mul, Int.mul_add]
      rw [Int.add_mul]
      simp only [Int.one_mul]
      have hox : o * x = x * o := by ac_rfl
      rw [hox]
      have h2ox : 2 * o * x = 2 * (x * o) := by ac_rfl
      rw [h2ox]
      omega

theorem poly_identity (n Q S o : Int) :
    n * (Q - 2 * o * S + n * o * o) - (S - n * o) * (S - n * o) = n * Q - S * S := by
  have e1 : n * (2 * o * S) = 2 * (n * o * S) := by ac_rfl
  have e2 : n * (n * o * o) = (n * o) * (n * o) := by ac_rfl
  have e3 : (S - n * o) * (S - n * o) = S * S - 2 * (S * (n * o)) + (n * o) * (n * o) := by
    rw [Int.mul_sub, Int.sub_mul, Int.sub_mul]
    have : n * o * S = S * (n * o) := by ac_rfl
    rw [this]
    omega
  rw [e3]
  rw [Int.mul_add, Int.mul_sub]
  rw [e1, e2]
  have e4 : n * o * S = S * (n * o) := by ac_rfl
  rw [e4]
  omega

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns.ShiftedVarianceNumerator.Goal := by
  intro quantities origin
  dsimp only
  let shifted : List Int := quantities.map (fun x => x - origin)
  have hlen : (shifted.length : Int) = (quantities.length : Int) := by simp [shifted]
  have hsum : shifted.sum = quantities.sum - (quantities.length : Int) * origin := by
    dsimp [shifted]
    exact sum_sub_const quantities origin
  have hsq : total shifted (fun x => x * x) =
      total quantities (fun x => x * x) - 2 * origin * quantities.sum +
        (quantities.length : Int) * origin * origin := by
    dsimp [shifted]
    exact sumsq_sub_const quantities origin
  rw [hlen, hsq, hsum]
  exact poly_identity (quantities.length : Int) (total quantities (fun x => x * x))
    quantities.sum origin
end TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.ShiftedVarianceNumerator
