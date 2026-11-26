/-
Scratch proof for `AverageThroughputCapsBacklog.Goal`. The claim says an
average arrival rate at or below a constant per-tick service quantum bounds
the backlog by one quantum. A trace with two idle quantum-sized service
opportunities followed by a three-unit arrival violates it: the unused
credits expire, yet the total arrivals still fit the average bound.
-/
import TinylambOpt.ResearchGoals.ResourcePolicies.MaintenanceDebt

/-!
Proof contribution for ResourcePolicies_MaintenanceDebt_AverageThroughputCapsBacklog. The theorem below
records a planning or execution decision that preserves the modeled
SQL result, or an explicit counterexample when the proposed rewrite is
unsound.
-/


set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.ResourcePolicies.MaintenanceDebt.Goal_AverageThroughputCapsBacklog

open TinylambOpt.ResearchGoals.ResourcePolicies.MaintenanceDebt


/-- Counterexample: unused capacity cannot be banked for a later burst. -/
theorem refute : ¬ AverageThroughputCapsBacklog.Goal := by
  intro h
  let ts : List Tick := [⟨0, 1⟩, ⟨0, 1⟩, ⟨3, 1⟩]
  have hcap : ∀ t ∈ ts, t.capacity = 1 := by
    simp [ts]
  have hsum : (ts.map Tick.added).sum ≤ ts.length * 1 := by
    simp [ts]
  have hb : high 0 ts ≤ 1 := h ts 1 hcap hsum
  have hbad : ¬ high 0 ts ≤ 1 := by
    decide
  exact hbad hb

end TinylambOpt.ResearchProofs.ResourcePolicies.MaintenanceDebt.Goal_AverageThroughputCapsBacklog
