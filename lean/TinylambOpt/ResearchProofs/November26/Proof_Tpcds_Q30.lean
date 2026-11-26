import TinylambOpt.ResearchGoals.November26.Tpcds

set_option maxHeartbeats 0
set_option maxRecDepth 10000


namespace TinylambOpt.ResearchProofs.November26.Tpcds.Goal_Q30
open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.November26
open TinylambOpt.ResearchGoals.November26.Tpcds

theorem sound : TinylambOpt.ResearchGoals.November26.Tpcds.Q30.Goal := by
  intro stateTotals v
  dsimp only
  intro hn hdiv
  let xs : List Int := stateTotals.filterMap id
  let n : Int := xs.length
  let d : Int := 5 * n
  let S : Int := 6 * xs.sum
  have hd : 0 < d := by
    dsimp [d, n, xs]
    exact Int.mul_pos (by decide) hn
  have h1 : (S / d) * d = S := by
    dsimp [S, d, n, xs]
    exact Int.ediv_mul_cancel hdiv
  constructor
  · intro h
    have hqd : (S / d) * d < v * d := Int.mul_lt_mul_of_pos_right h hd
    rw [h1] at hqd
    rw [show v * d = d * v by ac_rfl] at hqd
    exact hqd
  · intro h
    have hv : S < d * v := h
    rw [← h1] at hv
    rw [show d * v = v * d by ac_rfl] at hv
    exact (Int.mul_lt_mul_right hd).mp hv
end TinylambOpt.ResearchProofs.November26.Tpcds.Goal_Q30
