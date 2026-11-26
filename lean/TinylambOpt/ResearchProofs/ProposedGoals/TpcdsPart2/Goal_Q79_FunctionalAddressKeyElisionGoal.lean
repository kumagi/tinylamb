import TinylambOpt.ProposedGoals.BenchmarkRematch.TpcdsPart2

/-!
# Functional address key elision (TPC-DS Q79)

For rows satisfying the functional dependency `addr = addrOf ticket`, fixing the ticket
key also fixes the address, so the extra address equality in the grouping filter is
redundant. The executor may elide `ss_addr_sk` from the hash key when that dependency
holds, preserving the modeled grouped sum.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.ProposedGoals.TpcdsPart2.Goal_Q79_FunctionalAddressKeyElisionGoal

open TinylambOpt.ProposedGoals.BenchmarkRematch.TpcdsPart2
open TinylambOpt.ResearchGoals.QueryRewrites

theorem sound : Q79_FunctionalAddressKeyElisionGoal := by
  intro rows addrOf t hdep
  simp only [total, List.map_map, Function.comp_def]
  congr 1
  have hfilters : rows.filter (fun r => (r.1 == t) && (r.2.1 == addrOf t)) =
      rows.filter (fun r => r.1 == t) := by
    apply List.filter_congr
    intro r hr
    by_cases ht : (r.1 == t) = true
    · have hrt : r.1 = t := eq_of_beq ht
      have hra : r.2.1 = addrOf t := by
        simpa [hrt] using hdep r hr
      simp [ht, hra]
    · simp [ht]
  rw [hfilters]

end TinylambOpt.ResearchProofs.ProposedGoals.TpcdsPart2.Goal_Q79_FunctionalAddressKeyElisionGoal
