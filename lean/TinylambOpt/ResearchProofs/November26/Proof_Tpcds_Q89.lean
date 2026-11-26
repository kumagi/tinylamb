import TinylambOpt.ResearchGoals.November26.Tpcds

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false


namespace TinylambOpt.ResearchProofs.November26.Tpcds.Goal_Q89
open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.November26
open TinylambOpt.ResearchGoals.November26.Tpcds

theorem abs_bound (d sum : Int) (hsum : 0 < sum)
    (h1 : -sum ≤ 10 * d) (h2 : 10 * d ≤ sum) :
    ¬ 10 * d.natAbs > sum.toNat := by
  omega

theorem sound : TinylambOpt.ResearchGoals.November26.Tpcds.Q89.Goal := by
  intro months n sum lo hi hn hsum hbounds hlo hhi
  apply List.filter_eq_nil_iff.mpr
  intro x hx hbad
  have hbadP : 10 * (n * x - sum).natAbs > sum.toNat := of_decide_eq_true hbad
  have hxlo : lo ≤ x := (hbounds x hx).1
  have hxhi : x ≤ hi := (hbounds x hx).2
  have hn0 : 0 ≤ n := Int.le_of_lt hn
  have hmul_lo : n * lo ≤ n * x := Int.mul_le_mul_of_nonneg_left hxlo hn0
  have hmul_hi : n * x ≤ n * hi := Int.mul_le_mul_of_nonneg_left hxhi hn0
  have hlo' : 9 * sum ≤ 10 * (n * lo) := by
    simpa [Int.mul_assoc] using hlo
  have hhi' : 10 * (n * hi) ≤ 11 * sum := by
    simpa [Int.mul_assoc] using hhi
  have hlow : 9 * sum ≤ 10 * (n * x) := by
    exact Int.le_trans hlo' (Int.mul_le_mul_of_nonneg_left hmul_lo (by omega))
  have hhigh : 10 * (n * x) ≤ 11 * sum := by
    exact Int.le_trans (Int.mul_le_mul_of_nonneg_left hmul_hi (by omega)) hhi'
  have hneg : -sum ≤ 10 * (n * x - sum) := by omega
  have hpos : 10 * (n * x - sum) ≤ sum := by omega
  exact abs_bound (n * x - sum) sum hsum hneg hpos hbadP
end TinylambOpt.ResearchProofs.November26.Tpcds.Goal_Q89
