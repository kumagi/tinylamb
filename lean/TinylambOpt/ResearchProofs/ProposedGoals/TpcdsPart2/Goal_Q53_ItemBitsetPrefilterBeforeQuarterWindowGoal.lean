import TinylambOpt.ProposedGoals.BenchmarkRematch.TpcdsPart2

/-!
# Item-bitset prefilter before the quarterly window (TPC-DS Q53)

The target manufacturer's item predicate is assumed true, so an extra `itemOk` test
inside the `(manufacturer, quarter)` selection selects exactly the same sales rows
as applying it once beforehand. The executor may push the bitset test ahead of the
quarterly grouping while preserving the modeled partition totals and window average.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.ProposedGoals.TpcdsPart2.Goal_Q53_ItemBitsetPrefilterBeforeQuarterWindowGoal

open TinylambOpt.ProposedGoals.BenchmarkRematch.TpcdsPart2
open TinylambOpt.ResearchGoals.QueryRewrites

theorem sound : Q53_ItemBitsetPrefilterBeforeQuarterWindowGoal := by
  intro sales itemOk m q hm
  simp only [total, List.map_map, Function.comp_def]
  congr 1
  rw [List.filter_filter]
  have hfilters : sales.filter (fun r => itemOk r.1 && (r.1 == m) && (r.2.1 == q)) =
      sales.filter (fun r => (r.1 == m) && (r.2.1 == q) && itemOk r.1) := by
    apply List.filter_congr
    intro r _
    by_cases hkey : (r.1 == m) = true
    · have hrm : r.1 = m := eq_of_beq hkey
      rw [hrm]
      simp [hm, Bool.and_left_comm, Bool.and_assoc]
    · by_cases hq : (r.2.1 == q) = true <;> simp [hkey, hq]
  rw [hfilters]

end TinylambOpt.ResearchProofs.ProposedGoals.TpcdsPart2.Goal_Q53_ItemBitsetPrefilterBeforeQuarterWindowGoal
