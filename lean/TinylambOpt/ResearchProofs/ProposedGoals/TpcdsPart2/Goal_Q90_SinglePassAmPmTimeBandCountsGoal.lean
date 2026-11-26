import TinylambOpt.ProposedGoals.BenchmarkRematch.TpcdsPart2

/-!
# Single-pass morning/evening time-band counts (TPC-DS Q90)

The morning and evening hour bands are disjoint, so counting rows in their union equals
the sum of the two individual counts. The executor may evaluate both predicates in one
scan of the filtered web-sales rows without changing the modeled `(amc, pmc)` pair.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.ProposedGoals.TpcdsPart2.Goal_Q90_SinglePassAmPmTimeBandCountsGoal

open TinylambOpt.ProposedGoals.BenchmarkRematch.TpcdsPart2
open TinylambOpt.ResearchGoals.QueryRewrites

theorem sound : Q90_SinglePassAmPmTimeBandCountsGoal := by
  intro rows isAm isPm hdisj
  simp only [countWhere]
  induction rows with
  | nil => rfl
  | cons r rows ih =>
    by_cases hr : r.2 = true <;> by_cases ha : isAm r.1 = true <;> by_cases hp : isPm r.1 = true
    · have hbad := hdisj r.1
      simp [ha, hp] at hbad
    all_goals simp [hr, ha, hp, ih] <;> omega

end TinylambOpt.ResearchProofs.ProposedGoals.TpcdsPart2.Goal_Q90_SinglePassAmPmTimeBandCountsGoal
