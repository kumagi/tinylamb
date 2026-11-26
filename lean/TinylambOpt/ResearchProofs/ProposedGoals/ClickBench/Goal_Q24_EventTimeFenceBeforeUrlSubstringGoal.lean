import TinylambOpt.ProposedGoals.BenchmarkRematch.ClickBench

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.ProposedGoals.ClickBench.Goal_Q24_EventTimeFenceBeforeUrlSubstringGoal

open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ProposedGoals.BenchmarkRematch.ClickBench

theorem sound : Q24_EventTimeFenceBeforeUrlSubstringGoal := by
  intro rows tau h
  change (rows.filter (fun r => decide (r.1 ≤ tau) && r.2)).length = 0
  have hfilter : rows.filter (fun r => decide (r.1 ≤ tau) && r.2) = [] := by
    apply List.filter_eq_nil_iff.mpr
    intro r hr hpred
    have hleBool : decide (r.1 ≤ tau) = true := (Bool.and_eq_true_iff.mp hpred).1
    have hle : r.1 ≤ tau := by simpa using hleBool
    have hgt := h r hr
    omega
  simp [hfilter]

end TinylambOpt.ResearchProofs.ProposedGoals.ClickBench.Goal_Q24_EventTimeFenceBeforeUrlSubstringGoal
