import TinylambOpt.ProposedGoals.BenchmarkRematch.TpcdsPart2

/-!
# State average from precomputed customer totals (TPC-DS Q81)

`countWhere` is defined as the length of the filtered list, so the state count and the
state sum both read the identical `(state, total)` customer-level partition. The executor
may derive per-state `(count, sum)` from that table instead of rescanning catalog returns.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.ProposedGoals.TpcdsPart2.Goal_Q81_StateAvgFromCustomerTotalsGoal

open TinylambOpt.ProposedGoals.BenchmarkRematch.TpcdsPart2
open TinylambOpt.ResearchGoals.QueryRewrites

theorem sound : Q81_StateAvgFromCustomerTotalsGoal := by
  intro custTotals st
  rfl

end TinylambOpt.ResearchProofs.ProposedGoals.TpcdsPart2.Goal_Q81_StateAvgFromCustomerTotalsGoal
