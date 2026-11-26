import TinylambOpt.ResearchGoals.QueryRewrites.AnalyticalJoins
import TinylambOpt.ResearchProofs.QueryRewrites.NullableSum

/-!
# FactorizedNullableJoinSums

The two sums over matching pairs can be recovered by scaling each side's subtotal by the
other side's count for the same join keys. The executor can combine partial aggregates
without generating the pairs.
-/


set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.QueryRewrites.AnalyticalJoins.FactorizedNullableJoinSums

open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchProofs.QueryRewrites.NullableSum (nsum_flatMap_replicate nsum_flatMap_const)

theorem fst_pairs_eq (x y : List (Option Int)) :
    (x.flatMap (fun a => y.map (fun b => (a, b)))).map Prod.fst =
      x.flatMap (fun a => List.replicate y.length a) := by
  induction x with
  | nil => simp
  | cons a t ih =>
      simp only [List.flatMap_cons, List.map_append]
      rw [ih]
      congr 1
      have hmap : (y.map (fun b => (a, b))).map Prod.fst = y.map (fun _ => a) := by
        induction y with
        | nil => simp
        | cons b ys hy => simp [hy]
      rw [hmap, List.map_const']

theorem snd_pairs_eq (x y : List (Option Int)) :
    (x.flatMap (fun a => y.map (fun b => (a, b)))).map Prod.snd =
      x.flatMap (fun _ => y) := by
  induction x with
  | nil => simp
  | cons a t ih =>
      simp only [List.flatMap_cons, List.map_append]
      rw [ih]
      congr 1
      have hmap : (y.map (fun b => (a, b))).map Prod.snd = y := by
        induction y with
        | nil => simp
        | cons b ys hy => simp [hy, Function.comp_def]
      rw [hmap]

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.AnalyticalJoins.FactorizedNullableJoinSums.Goal := by
  intro x y
  dsimp only
  apply Prod.ext
  · rw [fst_pairs_eq, nsum_flatMap_replicate]
    cases y <;> simp [Int.mul_comm]
  · rw [snd_pairs_eq, nsum_flatMap_const]
    by_cases hx : x.isEmpty <;> simp [hx, Int.mul_comm]

end TinylambOpt.ResearchProofs.QueryRewrites.AnalyticalJoins.FactorizedNullableJoinSums

#print axioms TinylambOpt.ResearchProofs.QueryRewrites.AnalyticalJoins.FactorizedNullableJoinSums.sound

-- C++ references to this model or its proof.
-- executor/detail/benchmark_rematch.hpp
