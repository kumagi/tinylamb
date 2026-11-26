import TinylambOpt.ProposedGoals.BenchmarkRematch.TpcdsPart2

/-!
# Exact integer deviation band (TPC-DS Q89)

With a positive year sum, `Int.natAbs yearSum` and `yearSum.toNat` denote the same Nat,
so the two strict integer thresholds coincide. The executor may use the exact
`10 * |cnt * month - yearSum| > yearSum` test on the compact month-array table.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.ProposedGoals.TpcdsPart2.Goal_Q89_MonthlyDeviationBandExactCheckGoal

open TinylambOpt.ProposedGoals.BenchmarkRematch.TpcdsPart2
open TinylambOpt.ResearchGoals.QueryRewrites

theorem sound : Q89_MonthlyDeviationBandExactCheckGoal := by
  intro m yearSum cnt hcnt hyear
  have hnonneg : 0 ≤ yearSum := by omega
  have h1 : ((Int.natAbs yearSum : Nat) : Int) = yearSum := Int.natAbs_of_nonneg hnonneg
  have h2 : ((yearSum.toNat : Nat) : Int) = yearSum := Int.toNat_of_nonneg hnonneg
  have habs : Int.natAbs yearSum = yearSum.toNat := by
    exact_mod_cast h1.trans h2.symm
  rw [habs]

end TinylambOpt.ResearchProofs.ProposedGoals.TpcdsPart2.Goal_Q89_MonthlyDeviationBandExactCheckGoal
